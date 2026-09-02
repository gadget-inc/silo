//! The one-shot per-shard sweep that fills `enqueue_time_ms` into status
//! records and status/time index entries that lack it.

mod test_helpers;

use std::sync::Arc;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::enqueue_time_backfill_complete_key;
use silo::shard_range::ShardRange;
use test_helpers::*;

async fn enqueue_at(shard: &JobStoreShard, tenant: &str, start_at_ms: i64) -> String {
    shard
        .enqueue(
            tenant,
            None,
            10u8,
            start_at_ms,
            None,
            msgpack_payload(&serde_json::json!({"k": "v"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

async fn dequeue_one(shard: &JobStoreShard) -> String {
    let ids = dequeue_task_ids_until(shard, "w", "default", 1).await;
    ids.into_iter().next().expect("one task")
}

/// Seed one job per status kind for each tenant, strip every job's rows, and
/// return the `(tenant, job_id)` pairs.
async fn seed_stripped_jobs(shard: &JobStoreShard, tenants: &[&str]) -> Vec<(String, String)> {
    let mut jobs = Vec::new();
    for tenant in tenants {
        let far_future = now_ms() + 3_600_000;

        let scheduled = enqueue_at(shard, tenant, far_future).await;

        let running = enqueue_at(shard, tenant, 0).await;
        dequeue_one(shard).await;

        let succeeded = enqueue_at(shard, tenant, 0).await;
        let task = dequeue_one(shard).await;
        shard
            .report_attempt_outcome(&task, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("success");

        let failed = enqueue_at(shard, tenant, 0).await;
        let task = dequeue_one(shard).await;
        shard
            .report_attempt_outcome(
                &task,
                AttemptOutcome::Error {
                    error_code: "E".into(),
                    error: vec![],
                },
            )
            .await
            .expect("failure");

        let cancelled = enqueue_at(shard, tenant, far_future).await;
        shard.cancel_job(tenant, &cancelled).await.expect("cancel");

        for (job_id, kind) in [
            (scheduled, JobStatusKind::Scheduled),
            (running, JobStatusKind::Running),
            (succeeded, JobStatusKind::Succeeded),
            (failed, JobStatusKind::Failed),
            (cancelled, JobStatusKind::Cancelled),
        ] {
            let status = shard
                .get_job_status(tenant, &job_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.kind, kind, "seed {tenant}/{job_id}");
            strip_enqueue_time_from_job_rows(shard, tenant, &job_id).await;
            jobs.push((tenant.to_string(), job_id));
        }
    }
    jobs
}

async fn wait_for_completion(shard: &Arc<JobStoreShard>) {
    let complete = poll_until(
        || async { shard.enqueue_time_backfill_complete() },
        |complete| *complete,
        20_000,
    )
    .await;
    assert!(complete, "sweep did not complete in time");
}

#[silo::test]
async fn sweep_fills_missing_values_and_sets_completion_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let jobs = seed_stripped_jobs(&shard, &["-", "tenant-a", "tenant-b"]).await;
    for (tenant, job_id) in &jobs {
        assert_rows_lack_enqueue_time(&shard, tenant, job_id).await;
    }
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics, |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(4);
    })
    .await;
    wait_for_completion(&shard).await;

    for (tenant, job_id) in &jobs {
        assert_rows_carry_job_info_enqueue_time(&shard, tenant, job_id, "after sweep").await;
    }
}

fn repair_reads(metrics: &silo::metrics::Metrics, shard: &JobStoreShard) -> f64 {
    metrics.enqueue_time_repair_reads_value(shard.name())
}

/// Seed `n` future-scheduled jobs for `tenant` and strip their rows.
async fn seed_stripped_scheduled(shard: &JobStoreShard, tenant: &str, n: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let job_id = enqueue_at(shard, tenant, now_ms() + 3_600_000).await;
        strip_enqueue_time_from_job_rows(shard, tenant, &job_id).await;
        ids.push(job_id);
    }
    ids
}

#[silo::test]
async fn completed_shard_does_not_run_the_sweep_again() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let jobs = seed_stripped_scheduled(&shard, "-", 2).await;
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(1);
    })
    .await;
    wait_for_completion(&shard).await;
    let reads_after_sweep = repair_reads(&metrics, &shard);
    let progress_after_sweep = shard.enqueue_time_backfill_progress().await.unwrap();
    strip_enqueue_time_from_job_rows(&shard, "-", &jobs[0]).await;
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(1);
    })
    .await;
    assert!(
        shard.enqueue_time_backfill_complete(),
        "flag read from marker"
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        repair_reads(&metrics, &shard),
        reads_after_sweep,
        "no JOB_INFO reads on a completed shard"
    );
    assert_eq!(
        shard.enqueue_time_backfill_progress().await.unwrap(),
        progress_after_sweep,
        "progress untouched on a completed shard"
    );
    assert_rows_lack_enqueue_time(&shard, "-", &jobs[0]).await;
}

#[silo::test]
async fn interrupted_sweep_resumes_from_persisted_key() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");
    let n = 8;

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let jobs = seed_stripped_scheduled(&shard, "-", n).await;
    shard.close().await.expect("close");

    // One row per batch with a long pause: the first checkpoint lands within
    // the jitter plus one row, and the close lands inside the pause.
    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = silo::settings::EnqueueTimeBackfillConfig {
            enabled: true,
            batch_size: 1,
            pause_ms: 2_000,
        };
    })
    .await;
    let progress = poll_until(
        || async { shard.enqueue_time_backfill_progress().await.unwrap() },
        |p| p.as_ref().is_some_and(|p| p.scanned >= 1),
        10_000,
    )
    .await
    .expect("progress persisted after the first batch");
    shard.close().await.expect("close mid-sweep");
    assert!(progress.last_key.is_some(), "resume key persisted");
    assert!(
        progress.scanned < n as u64,
        "sweep must be interrupted before finishing: {progress:?}"
    );
    assert!(!shard.enqueue_time_backfill_complete());

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(1);
    })
    .await;
    wait_for_completion(&shard).await;
    let progress = shard
        .enqueue_time_backfill_progress()
        .await
        .unwrap()
        .expect("progress");
    // A close that lands inside a batch re-scans that batch's rows, so the
    // exactly-once guarantees are on repairs and reads, not on scans.
    assert!(
        progress.scanned >= n as u64,
        "every row scanned: {progress:?}"
    );
    assert_eq!(
        progress.repaired, n as u64,
        "each row repaired once: {progress:?}"
    );
    assert_eq!(repair_reads(&metrics, &shard), n as f64, "one read per row");
    for job_id in &jobs {
        assert_rows_carry_job_info_enqueue_time(&shard, "-", job_id, "after resume").await;
    }
}

/// `(status row expire_ts, index row expire_ts)` for the job's current status.
async fn row_expiries(
    shard: &JobStoreShard,
    tenant: &str,
    job_id: &str,
) -> (Option<i64>, Option<i64>) {
    use silo::keys::{idx_status_time_key, job_status_key, status_index_timestamp};

    let status = shard.get_job_status(tenant, job_id).await.unwrap().unwrap();
    let status_kv = shard
        .db()
        .get_key_value(&job_status_key(tenant, job_id))
        .await
        .unwrap()
        .expect("status row");
    let index_kv = shard
        .db()
        .get_key_value(&idx_status_time_key(
            tenant,
            status.kind.as_str(),
            status_index_timestamp(&status),
            job_id,
        ))
        .await
        .unwrap()
        .expect("index row");
    (status_kv.expire_ts, index_kv.expire_ts)
}

#[silo::test]
async fn sweep_keeps_each_rows_expiry_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.completed_job_expire_s = Some(60);
        cfg.terminal_job_expire_s = Some(60);
    })
    .await;
    let terminal = enqueue_at(&shard, "-", 0).await;
    let task = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(&task, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("success");
    let scheduled = enqueue_at(&shard, "-", now_ms() + 3_600_000).await;
    strip_enqueue_time_from_job_rows(&shard, "-", &terminal).await;
    strip_enqueue_time_from_job_rows(&shard, "-", &scheduled).await;
    let terminal_before = row_expiries(&shard, "-", &terminal).await;
    assert!(terminal_before.0.is_some() && terminal_before.1.is_some());
    assert_eq!(row_expiries(&shard, "-", &scheduled).await, (None, None));
    shard.close().await.expect("close");

    // Different expiry settings from the ones the rows were written under.
    let shard = open_shard_at_path(&path, ShardRange::full(), metrics, |cfg| {
        cfg.completed_job_expire_s = Some(3_600);
        cfg.terminal_job_expire_s = None;
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(1);
    })
    .await;
    wait_for_completion(&shard).await;

    assert_rows_carry_job_info_enqueue_time(&shard, "-", &terminal, "terminal").await;
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &scheduled, "scheduled").await;
    assert_eq!(
        row_expiries(&shard, "-", &terminal).await,
        terminal_before,
        "terminal rows keep their expiry"
    );
    assert_eq!(
        row_expiries(&shard, "-", &scheduled).await,
        (None, None),
        "rows without an expiry stay without one"
    );
}

#[silo::test]
async fn sweep_leaves_counters_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    seed_stripped_jobs(&shard, &["-", "tenant-a"]).await;
    let tenant_counters_before = shard.scan_tenant_status_counters(None).await.unwrap();
    let shard_counters_before = shard.get_counters().await.unwrap();
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics, |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(3);
    })
    .await;
    wait_for_completion(&shard).await;

    assert_eq!(
        shard.scan_tenant_status_counters(None).await.unwrap(),
        tenant_counters_before,
        "per-tenant status counters"
    );
    let shard_counters_after = shard.get_counters().await.unwrap();
    assert_eq!(
        shard_counters_after.total_jobs,
        shard_counters_before.total_jobs
    );
    assert_eq!(
        shard_counters_after.completed_jobs,
        shard_counters_before.completed_jobs
    );
}

#[silo::test]
async fn transitions_racing_the_sweep_leave_one_index_entry_per_job() {
    use silo::keys::{end_bound, idx_status_time_tenant_prefix, parse_status_time_index_key};

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");
    let n = 150;

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let jobs = seed_stripped_scheduled(&shard, "-", n).await;
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics, |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(1);
    })
    .await;
    for job_id in &jobs {
        shard.cancel_job("-", job_id).await.expect("cancel");
    }
    wait_for_completion(&shard).await;

    let mut seen = std::collections::HashMap::<String, usize>::new();
    let start = idx_status_time_tenant_prefix("-");
    let end = end_bound(&start);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .expect("scan index");
    while let Some(kv) = iter.next().await.expect("next") {
        let parsed = parse_status_time_index_key(&kv.key).expect("index key");
        assert_eq!(
            parsed.status, "Cancelled",
            "{}: stale index entry",
            parsed.job_id
        );
        *seen.entry(parsed.job_id).or_default() += 1;
    }
    assert_eq!(seen.len(), n, "one entry per job");
    for job_id in &jobs {
        assert_eq!(
            seen.get(job_id),
            Some(&1),
            "{job_id}: exactly one index entry"
        );
        let status = shard.get_job_status("-", job_id).await.unwrap().unwrap();
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        assert_rows_carry_job_info_enqueue_time(&shard, "-", job_id, "raced").await;
    }
}

#[silo::test]
async fn rows_carrying_the_value_are_skipped_without_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let stripped = seed_stripped_scheduled(&shard, "-", 3).await;
    let mut intact = Vec::new();
    for _ in 0..3 {
        intact.push(enqueue_at(&shard, "-", now_ms() + 3_600_000).await);
    }
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(2);
    })
    .await;
    let scanned_keys_before = metrics.query_scanned_keys_value(shard.name());
    let point_lookups_before = metrics.query_point_lookups_value(shard.name());
    wait_for_completion(&shard).await;

    assert_eq!(repair_reads(&metrics, &shard), stripped.len() as f64);
    let progress = shard
        .enqueue_time_backfill_progress()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.scanned, 6);
    assert_eq!(progress.repaired, 3);
    for job_id in stripped.iter().chain(&intact) {
        assert_rows_carry_job_info_enqueue_time(&shard, "-", job_id, "after sweep").await;
    }
    assert_eq!(
        metrics.query_scanned_keys_value(shard.name()),
        scanned_keys_before,
        "sweep I/O is not query scanned keys"
    );
    assert_eq!(
        metrics.query_point_lookups_value(shard.name()),
        point_lookups_before,
        "sweep reads are not query point lookups"
    );
}

#[silo::test]
async fn missing_job_info_is_skipped_and_the_sweep_completes() {
    use silo::keys::job_info_key;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let jobs = seed_stripped_scheduled(&shard, "-", 2).await;
    shard
        .db()
        .delete(&job_info_key("-", &jobs[0]))
        .await
        .expect("delete JOB_INFO");
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics, |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(1);
    })
    .await;
    wait_for_completion(&shard).await;

    let progress = shard
        .enqueue_time_backfill_progress()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.skipped_missing_job_info, 1, "{progress:?}");
    assert_eq!(progress.repaired, 1, "{progress:?}");
    assert_rows_lack_enqueue_time(&shard, "-", &jobs[0]).await;
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &jobs[1], "intact job").await;
}

#[silo::test]
async fn disabled_sweep_never_runs() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let job_id = enqueue_at(&shard, "-", now_ms() + 3_600_000).await;
    strip_enqueue_time_from_job_rows(&shard, "-", &job_id).await;

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    assert!(!shard.enqueue_time_backfill_complete());
    assert!(
        shard
            .db()
            .get(&enqueue_time_backfill_complete_key())
            .await
            .unwrap()
            .is_none(),
        "no marker"
    );
    assert_eq!(shard.enqueue_time_backfill_progress().await.unwrap(), None);
    assert_eq!(repair_reads(&metrics, &shard), 0.0);
    assert_rows_lack_enqueue_time(&shard, "-", &job_id).await;
}

#[silo::test]
async fn sweep_only_rewrites_rows_inside_the_shard_range() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");
    let range = ShardRange::new("", "8000000000000000");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let mut inside = Vec::new();
    let mut outside = Vec::new();
    for i in 0..12 {
        let tenant = format!("tenant-{i}");
        let job_id = enqueue_at(&shard, &tenant, now_ms() + 3_600_000).await;
        strip_enqueue_time_from_job_rows(&shard, &tenant, &job_id).await;
        if range.contains_tenant(&tenant) {
            inside.push((tenant, job_id));
        } else {
            outside.push((tenant, job_id));
        }
    }
    assert!(
        !inside.is_empty() && !outside.is_empty(),
        "tenants on both sides"
    );
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, range, metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(2);
    })
    .await;
    wait_for_completion(&shard).await;

    for (tenant, job_id) in &inside {
        assert_rows_carry_job_info_enqueue_time(&shard, tenant, job_id, "inside range").await;
    }
    for (tenant, job_id) in &outside {
        assert_rows_lack_enqueue_time(&shard, tenant, job_id).await;
    }
    assert_eq!(repair_reads(&metrics, &shard), inside.len() as f64);
    let progress = shard
        .enqueue_time_backfill_progress()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.scanned, inside.len() as u64, "{progress:?}");
}

/// Once the sweep has completed, index-served queries never fall back to
/// `JOB_INFO`, whichever rows lacked the value before it ran.
#[silo::test]
async fn queries_after_the_sweep_need_no_fallback() {
    use datafusion::arrow::array::{Array, Int64Array};
    use silo::query::ShardQueryEngine;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {}).await;
    let stripped = seed_stripped_scheduled(&shard, "-", 20).await;
    shard.close().await.expect("close");

    let shard = open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |cfg| {
        cfg.enqueue_time_backfill = fast_enqueue_time_backfill(4);
    })
    .await;
    wait_for_completion(&shard).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    for query in [
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-'",
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled'",
    ] {
        let fallback_before = metrics.enqueue_time_fallback_hydrations_value(shard.name());
        let lookups_before = metrics.query_point_lookups_value(shard.name());
        let batches = sql
            .sql(query)
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, stripped.len(), "{query}: row count");
        for batch in &batches {
            let times = batch
                .column_by_name("enqueue_time_ms")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(
                (0..times.len()).all(|i| times.value(i) > 0),
                "{query}: values"
            );
        }
        assert_eq!(
            metrics.enqueue_time_fallback_hydrations_value(shard.name()),
            fallback_before,
            "{query}: no fallback hydrations after the sweep"
        );
        assert_eq!(
            metrics.query_point_lookups_value(shard.name()),
            lookups_before,
            "{query}: no point lookups after the sweep"
        );
    }
}
