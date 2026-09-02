//! `enqueue_time_ms` on the status record and the status/time index entry.
//!
//! Every write path (enqueue, import, every transition) must leave both rows
//! carrying the same `enqueue_time_ms` that `JOB_INFO` reports, and a job whose
//! rows lack the value must be repaired by its next transition.

mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_store_shard::JobStoreShard;
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

#[silo::test]
async fn enqueue_writes_enqueue_time_to_status_and_index_entry() {
    let (_tmp, shard) = open_temp_shard().await;
    let cases = [
        ("immediate", 0),
        ("explicit", now_ms()),
        ("future", now_ms() + 60_000),
    ];

    for (name, start_at_ms) in cases {
        let job_id = enqueue_at(&shard, "-", start_at_ms).await;
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("status exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);
        assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_id, name).await;
    }
}

fn immediate_retry_policy(retry_count: u32) -> silo::retry::RetryPolicy {
    silo::retry::RetryPolicy {
        retry_count,
        initial_interval_ms: 0,
        max_interval_ms: 0,
        randomize_interval: false,
        backoff_factor: 1.0,
    }
}

async fn dequeue_one(shard: &JobStoreShard) -> String {
    let ids = dequeue_task_ids_until(shard, "w", "default", 1).await;
    ids.into_iter().next().expect("one task")
}

/// Every transition kind copies the enqueue time forward into the new status
/// record and index entry.
#[silo::test]
async fn transitions_copy_enqueue_time_forward() {
    use silo::job_attempt::AttemptOutcome;

    let (_tmp, shard) = open_temp_shard().await;

    // Job A: running -> retry -> running -> failed -> restart -> running -> succeeded.
    let job_a = shard
        .enqueue(
            "-",
            None,
            10u8,
            0,
            Some(immediate_retry_policy(1)),
            msgpack_payload(&serde_json::json!({"job": "a"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue a");

    let task = dequeue_one(&shard).await;
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_a, "dequeue -> Running").await;

    shard
        .report_attempt_outcome(
            &task,
            AttemptOutcome::Error {
                error_code: "E".into(),
                error: vec![],
            },
        )
        .await
        .expect("report error with retry");
    let status = shard.get_job_status("-", &job_a).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Scheduled);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_a, "failure with retry").await;

    let task = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(
            &task,
            AttemptOutcome::Error {
                error_code: "E".into(),
                error: vec![],
            },
        )
        .await
        .expect("report final error");
    let status = shard.get_job_status("-", &job_a).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Failed);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_a, "final failure").await;

    shard.restart_job("-", &job_a).await.expect("restart");
    let status = shard.get_job_status("-", &job_a).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Scheduled);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_a, "restart").await;

    let task = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(&task, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
    let status = shard.get_job_status("-", &job_a).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Succeeded);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_a, "success").await;

    // Job B: future scheduled -> expedite -> cancel.
    let job_b = enqueue_at(&shard, "-", now_ms() + 60_000).await;
    shard.expedite_job("-", &job_b).await.expect("expedite");
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_b, "expedite").await;

    shard.cancel_job("-", &job_b).await.expect("cancel");
    let status = shard.get_job_status("-", &job_b).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Cancelled);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_b, "cancel").await;
}

fn import_params(id: &str, enqueue_time_ms: i64) -> silo::job_store_shard::import::ImportJobParams {
    silo::job_store_shard::import::ImportJobParams {
        id: id.to_string(),
        priority: 50,
        enqueue_time_ms,
        start_at_ms: 0,
        retry_policy: None,
        payload: msgpack_payload(&serde_json::json!({"imported": true})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts: vec![],
    }
}

fn succeeded_attempt(finished_at_ms: i64) -> silo::job_store_shard::import::ImportedAttempt {
    silo::job_store_shard::import::ImportedAttempt {
        status: silo::job_store_shard::import::ImportedAttemptStatus::Succeeded { result: vec![] },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

fn failed_attempt(finished_at_ms: i64) -> silo::job_store_shard::import::ImportedAttempt {
    silo::job_store_shard::import::ImportedAttempt {
        status: silo::job_store_shard::import::ImportedAttemptStatus::Failed {
            error_code: "ERR".to_string(),
            error: vec![],
        },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

/// Import writes the effective enqueue time it stores in `JOB_INFO` into both
/// rows, for scheduled and terminal imports alike.
#[silo::test]
async fn import_writes_enqueue_time_to_status_and_index_entry() {
    let (_tmp, shard) = open_temp_shard().await;
    let explicit = 1_700_000_000_000;

    let mut terminal = import_params("imp-terminal", explicit);
    terminal.attempts = vec![succeeded_attempt(explicit + 5_000)];
    let cases = vec![
        (
            "scheduled explicit",
            import_params("imp-scheduled", explicit),
            JobStatusKind::Scheduled,
        ),
        (
            "scheduled clamped",
            import_params("imp-clamped", 0),
            JobStatusKind::Scheduled,
        ),
        ("terminal", terminal, JobStatusKind::Succeeded),
    ];

    for (name, params, expected_kind) in cases {
        let job_id = params.id.clone();
        let results = shard.import_jobs("-", vec![params]).await.expect("import");
        assert!(results[0].success, "{name}: import must succeed");
        let status = shard.get_job_status("-", &job_id).await.unwrap().unwrap();
        assert_eq!(status.kind, expected_kind, "{name}: status kind");
        assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_id, name).await;
    }
}

/// Reimport keeps the job's original enqueue time in both rows even when the
/// reimport request names a different one.
#[silo::test]
async fn reimport_preserves_original_enqueue_time() {
    let (_tmp, shard) = open_temp_shard().await;
    let original = 1_700_000_000_000;

    let mut first = import_params("reimp", original);
    first.attempts = vec![failed_attempt(original + 5_000)];
    let results = shard.import_jobs("-", vec![first]).await.expect("import");
    assert!(results[0].success);
    let status = shard.get_job_status("-", "reimp").await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Failed);

    let mut again = import_params("reimp", original + 99_000);
    again.attempts = vec![
        failed_attempt(original + 5_000),
        succeeded_attempt(original + 9_000),
    ];
    let results = shard.import_jobs("-", vec![again]).await.expect("reimport");
    assert!(
        results[0].success,
        "reimport must succeed: {:?}",
        results[0].error
    );

    let job = shard.get_job("-", "reimp").await.unwrap().unwrap();
    assert_eq!(
        job.enqueue_time_ms(),
        original,
        "JOB_INFO keeps the original"
    );
    let status = shard.get_job_status("-", "reimp").await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Succeeded);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", "reimp", "reimport").await;
}

fn repair_reads(metrics: &silo::metrics::Metrics, shard: &JobStoreShard) -> f64 {
    metrics.enqueue_time_repair_reads_value(shard.name())
}

/// A job whose rows lack the value is repaired by its next transition.
#[silo::test]
async fn rows_without_enqueue_time_are_repaired_by_next_transition() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let job_id = enqueue_at(&shard, "-", now_ms()).await;
    strip_enqueue_time_from_job_rows(&shard, "-", &job_id).await;
    assert_rows_lack_enqueue_time(&shard, "-", &job_id).await;

    let before = repair_reads(&metrics, &shard);
    shard.cancel_job("-", &job_id).await.expect("cancel");

    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_id, "cancel repairs").await;
    let reads = repair_reads(&metrics, &shard) - before;
    assert_eq!(reads, 1.0, "exactly one repair read, recorded {reads}");
}

/// Transitions that already hold a `JobView` repair the rows without any
/// extra `JOB_INFO` read.
#[silo::test]
async fn transitions_with_job_view_repair_without_a_read() {
    use silo::job_attempt::AttemptOutcome;

    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;

    // dequeue
    let job = enqueue_at(&shard, "-", 0).await;
    strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
    let before = repair_reads(&metrics, &shard);
    let task = dequeue_one(&shard).await;
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "dequeue").await;
    assert_eq!(
        repair_reads(&metrics, &shard) - before,
        0.0,
        "dequeue reads"
    );

    // error branch of report-outcome (no retry policy, lands Failed)
    strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
    let before = repair_reads(&metrics, &shard);
    shard
        .report_attempt_outcome(
            &task,
            AttemptOutcome::Error {
                error_code: "E".into(),
                error: vec![],
            },
        )
        .await
        .expect("report error");
    let status = shard.get_job_status("-", &job).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Failed);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "report error").await;
    assert_eq!(
        repair_reads(&metrics, &shard) - before,
        0.0,
        "report error reads"
    );

    // restart
    strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
    let before = repair_reads(&metrics, &shard);
    shard.restart_job("-", &job).await.expect("restart");
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "restart").await;
    assert_eq!(
        repair_reads(&metrics, &shard) - before,
        0.0,
        "restart reads"
    );

    // direct lease
    strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
    let before = repair_reads(&metrics, &shard);
    shard.lease_task("-", &job, "w").await.expect("lease_task");
    let status = shard.get_job_status("-", &job).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Running);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "lease_task").await;
    assert_eq!(
        repair_reads(&metrics, &shard) - before,
        0.0,
        "lease_task reads"
    );

    // expedite
    let future = enqueue_at(&shard, "-", now_ms() + 60_000).await;
    strip_enqueue_time_from_job_rows(&shard, "-", &future).await;
    let before = repair_reads(&metrics, &shard);
    shard.expedite_job("-", &future).await.expect("expedite");
    assert_rows_carry_job_info_enqueue_time(&shard, "-", &future, "expedite").await;
    assert_eq!(
        repair_reads(&metrics, &shard) - before,
        0.0,
        "expedite reads"
    );

    // reimport
    let mut first = import_params("reimp-view", 1_700_000_000_000);
    first.attempts = vec![failed_attempt(1_700_000_005_000)];
    shard.import_jobs("-", vec![first]).await.expect("import");
    strip_enqueue_time_from_job_rows(&shard, "-", "reimp-view").await;
    let before = repair_reads(&metrics, &shard);
    let mut again = import_params("reimp-view", 1_700_000_000_000);
    again.attempts = vec![
        failed_attempt(1_700_000_005_000),
        succeeded_attempt(1_700_000_009_000),
    ];
    let results = shard.import_jobs("-", vec![again]).await.expect("reimport");
    assert!(results[0].success, "{:?}", results[0].error);
    assert_rows_carry_job_info_enqueue_time(&shard, "-", "reimp-view", "reimport").await;
    assert_eq!(
        repair_reads(&metrics, &shard) - before,
        0.0,
        "reimport reads"
    );
}

/// Gauge-relevant transitions without a `JobView` in scope repair the rows
/// with exactly one `JOB_INFO` read, and leave the metric alone when the rows
/// already carry the value.
#[silo::test]
async fn gauge_path_transitions_repair_with_exactly_one_read() {
    use silo::job_attempt::AttemptOutcome;

    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;

    for stripped in [true, false] {
        let expected_reads = if stripped { 1.0 } else { 0.0 };
        let label = if stripped { "stripped" } else { "intact" };

        // cancel of a scheduled job
        let job = enqueue_at(&shard, "-", now_ms() + 60_000).await;
        if stripped {
            strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
        }
        let before = repair_reads(&metrics, &shard);
        shard.cancel_job("-", &job).await.expect("cancel");
        assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "cancel").await;
        assert_eq!(
            repair_reads(&metrics, &shard) - before,
            expected_reads,
            "{label}: cancel reads"
        );

        // success branch of report-outcome
        let job = enqueue_at(&shard, "-", 0).await;
        let task = dequeue_one(&shard).await;
        if stripped {
            strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
        }
        let before = repair_reads(&metrics, &shard);
        shard
            .report_attempt_outcome(&task, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");
        assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "success").await;
        assert_eq!(
            repair_reads(&metrics, &shard) - before,
            expected_reads,
            "{label}: success reads"
        );

        // cancelled branch of report-outcome
        let job = enqueue_at(&shard, "-", 0).await;
        let task = dequeue_one(&shard).await;
        if stripped {
            strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
        }
        let before = repair_reads(&metrics, &shard);
        shard
            .report_attempt_outcome(&task, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");
        let status = shard.get_job_status("-", &job).await.unwrap().unwrap();
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "cancelled").await;
        assert_eq!(
            repair_reads(&metrics, &shard) - before,
            expected_reads,
            "{label}: cancelled reads"
        );
    }
}

/// Retargeting a scheduled job's task key (a rate-limit retry) has no
/// `JobView` in scope and is not gauge-relevant, so a job whose rows lack the
/// value costs exactly one dedicated `JOB_INFO` read.
#[silo::test]
async fn task_key_retarget_repairs_with_exactly_one_read() {
    use silo::gubernator::{MockGubernatorClient, RateLimitClient};
    use silo::job::{GubernatorAlgorithm, GubernatorRateLimit, Limit, RateLimitRetryPolicy};

    let gubernator = MockGubernatorClient::new_arc();
    // Exhaust the limit up front so the CheckRateLimit task parks the job on a
    // retry row at a fresh start time.
    gubernator
        .check_rate_limit(
            "api",
            "retarget",
            1,
            1,
            60_000,
            silo::pb::gubernator::Algorithm::TokenBucket,
            0,
        )
        .await
        .expect("exhaust");
    let rate_limit = GubernatorRateLimit {
        name: "api".to_string(),
        unique_key: "retarget".to_string(),
        limit: 1,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 60_000,
            max_backoff_ms: 60_000,
            backoff_multiplier: 1.0,
            max_retries: 5,
        },
    };

    let tmp = tempfile::tempdir().unwrap();
    let cfg = silo::settings::DatabaseConfig {
        name: "test".to_string(),
        backend: silo::settings::Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(fast_flush_slatedb_settings()),
        ..Default::default()
    };
    let metrics = silo::metrics::init().expect("init metrics");
    let shard = JobStoreShard::open(
        &cfg,
        gubernator,
        Some(metrics.clone()),
        silo::shard_range::ShardRange::full(),
    )
    .await
    .expect("open shard");

    for stripped in [true, false] {
        let expected_reads = if stripped { 1.0 } else { 0.0 };
        let job = shard
            .enqueue(
                "-",
                None,
                10u8,
                0,
                None,
                msgpack_payload(&serde_json::json!({"rl": stripped})),
                vec![Limit::RateLimit(rate_limit.clone())],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        let scheduled = shard.get_job_status("-", &job).await.unwrap().unwrap();
        if stripped {
            strip_enqueue_time_from_job_rows(&shard, "-", &job).await;
        }

        let before = repair_reads(&metrics, &shard);
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert!(
            tasks.is_empty(),
            "over-limit check must not hand out a task"
        );

        let retargeted = shard.get_job_status("-", &job).await.unwrap().unwrap();
        assert_eq!(retargeted.kind, JobStatusKind::Scheduled);
        assert_ne!(
            retargeted.next_attempt_starts_after_ms, scheduled.next_attempt_starts_after_ms,
            "status must point at the retry row"
        );
        assert_rows_carry_job_info_enqueue_time(&shard, "-", &job, "retarget").await;
        assert_eq!(
            repair_reads(&metrics, &shard) - before,
            expected_reads,
            "stripped={stripped}: retarget reads"
        );
    }
}

async fn status_counts(
    shard: &JobStoreShard,
    tenant: &str,
) -> std::collections::HashMap<String, i64> {
    shard
        .scan_tenant_status_counters(None)
        .await
        .expect("scan counters")
        .into_iter()
        .filter(|(t, _, _)| t == tenant)
        .map(|(_, status, count)| (status, count))
        .collect()
}

/// Enqueue and import each move the tenant status counter for the written
/// status by exactly one.
#[silo::test]
async fn enqueue_and_import_move_tenant_status_counters_by_one() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_at(&shard, "-", 0).await;
    enqueue_at(&shard, "-", now_ms() + 60_000).await;
    let counts = status_counts(&shard, "-").await;
    assert_eq!(
        counts.get("Scheduled"),
        Some(&2),
        "after enqueue: {counts:?}"
    );

    let mut terminal = import_params("counter-terminal", 1_700_000_000_000);
    terminal.attempts = vec![succeeded_attempt(1_700_000_005_000)];
    shard
        .import_jobs("-", vec![terminal, import_params("counter-scheduled", 0)])
        .await
        .expect("import");
    let counts = status_counts(&shard, "-").await;
    assert_eq!(
        counts.get("Scheduled"),
        Some(&3),
        "after import: {counts:?}"
    );
    assert_eq!(
        counts.get("Succeeded"),
        Some(&1),
        "after import: {counts:?}"
    );
}

/// The public marker method sets and clears the marker key and the in-memory
/// flag together, and the getter reflects it immediately.
#[silo::test]
async fn backfill_completion_marker_and_flag_move_together() {
    use silo::keys::enqueue_time_backfill_complete_key;

    let (_tmp, shard) = open_temp_shard().await;
    let key = enqueue_time_backfill_complete_key();
    assert!(!shard.enqueue_time_backfill_complete(), "fresh shard");
    assert!(
        shard.db().get(&key).await.unwrap().is_none(),
        "fresh shard marker"
    );

    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .expect("set");
    assert!(shard.enqueue_time_backfill_complete(), "after set");
    assert!(
        shard.db().get(&key).await.unwrap().is_some(),
        "marker after set"
    );

    shard
        .set_enqueue_time_backfill_complete(false)
        .await
        .expect("clear");
    assert!(!shard.enqueue_time_backfill_complete(), "after clear");
    assert!(
        shard.db().get(&key).await.unwrap().is_none(),
        "marker after clear"
    );
}

/// A reopened shard reads the flag from the persisted marker.
#[silo::test]
async fn reopened_shard_reads_backfill_flag_from_marker() {
    use silo::shard_range::ShardRange;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let metrics = silo::metrics::init().expect("init metrics");
    let open = || open_shard_at_path(&path, ShardRange::full(), metrics.clone(), |_| {});

    let shard = open().await;
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .expect("set");
    shard.close().await.expect("close");

    let shard = open().await;
    assert!(
        shard.enqueue_time_backfill_complete(),
        "flag read from marker at open"
    );
    shard
        .set_enqueue_time_backfill_complete(false)
        .await
        .expect("clear");
    shard.close().await.expect("close");

    let shard = open().await;
    assert!(
        !shard.enqueue_time_backfill_complete(),
        "cleared marker read at open"
    );
}

/// A terminal transition on a shard with row expiry writes both rows with the
/// value and with the expiry.
#[silo::test]
async fn terminal_transition_writes_value_and_expiry_on_both_rows() {
    use silo::job_attempt::AttemptOutcome;
    use silo::keys::{idx_status_time_key, job_status_key, status_index_timestamp};

    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    let job_id = enqueue_at(&shard, "-", 0).await;
    let task = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(&task, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");

    assert_rows_carry_job_info_enqueue_time(&shard, "-", &job_id, "terminal").await;
    let status = shard.get_job_status("-", &job_id).await.unwrap().unwrap();
    assert_eq!(status.kind, JobStatusKind::Succeeded);
    let status_row = shard
        .db()
        .get_key_value(&job_status_key("-", &job_id))
        .await
        .unwrap()
        .expect("status row");
    let index_row = shard
        .db()
        .get_key_value(&idx_status_time_key(
            "-",
            status.kind.as_str(),
            status_index_timestamp(&status),
            &job_id,
        ))
        .await
        .unwrap()
        .expect("index row");
    assert!(status_row.expire_ts.is_some(), "status row carries expiry");
    assert_eq!(
        index_row.expire_ts, status_row.expire_ts,
        "index row carries the same expiry"
    );
}
