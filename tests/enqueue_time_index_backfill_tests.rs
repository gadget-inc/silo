//! The one-shot per-shard sweep that writes missing `IDX_ENQUEUE_TIME`
//! entries for jobs created before the index existed, and the completion
//! marker that gates the query path.

mod test_helpers;

use std::sync::Arc;
use std::time::Duration;

use silo::gubernator::MockGubernatorClient;
use silo::instrumented_db::InstrumentedDb;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::{
    end_bound, idx_enqueue_time_key, idx_enqueue_time_tenant_prefix, job_info_key,
    parse_enqueue_time_index_key,
};
use silo::settings::{Backend, DatabaseConfig, EnqueueTimeIndexBackfillConfig};
use silo::shard_range::ShardRange;
use test_helpers::*;

const TENANT: &str = "-";

/// Open (or reopen) a shard at `path` with the given backfill settings.
async fn open_shard_at(
    path: &std::path::Path,
    terminal_expire_s: Option<u64>,
    backfill: EnqueueTimeIndexBackfillConfig,
) -> Arc<JobStoreShard> {
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.to_string_lossy().to_string(),
        slatedb: Some(fast_flush_slatedb_settings()),
        completed_job_expire_s: terminal_expire_s,
        terminal_job_expire_s: terminal_expire_s,
        enqueue_time_index_backfill: backfill,
        ..Default::default()
    };
    JobStoreShard::open(
        &cfg,
        MockGubernatorClient::new_arc(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard")
}

async fn enqueue_many(shard: &JobStoreShard, prefix: &str, count: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let id = format!("{prefix}-{i:04}");
        shard
            .enqueue(
                TENANT,
                Some(id.clone()),
                10u8,
                now_ms() + i as i64,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        ids.push(id);
    }
    ids
}

/// Remove the index entries for `ids`, simulating jobs created before the
/// index existed.
async fn strip_entries(shard: &JobStoreShard, ids: &[String]) {
    for id in ids {
        let job = shard
            .get_job(TENANT, id)
            .await
            .expect("get_job")
            .expect("job exists");
        shard
            .db()
            .delete(idx_enqueue_time_key(TENANT, job.enqueue_time_ms(), id))
            .await
            .expect("delete entry");
    }
    shard.db().flush().await.expect("flush");
}

/// The tenant's index entries as job ids, in index order.
async fn indexed_ids(db: &InstrumentedDb) -> Vec<String> {
    let start = idx_enqueue_time_tenant_prefix(TENANT);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut ids = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        ids.push(parse_enqueue_time_index_key(&kv.key).expect("key").job_id);
    }
    ids
}

async fn expire_ts_of(db: &InstrumentedDb, key: &[u8]) -> Option<i64> {
    db.get_key_value(key)
        .await
        .expect("get_key_value")
        .unwrap_or_else(|| panic!("row {key:?} should be present"))
        .expire_ts
}

fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

#[silo::test]
async fn sweep_writes_only_missing_entries_and_reports_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 10).await;
    strip_entries(&shard, &ids[2..7]).await;
    assert_eq!(indexed_ids(shard.db()).await.len(), 5);

    let result = shard
        .backfill_enqueue_time_index(3, Duration::ZERO)
        .await
        .expect("backfill");

    assert!(result.complete, "{result:?}");
    assert!(!result.cancelled, "{result:?}");
    assert_eq!(result.rows_scanned, 10);
    assert_eq!(result.rows_written, 5);
    assert_eq!(sorted(indexed_ids(shard.db()).await), ids);
    assert!(shard.enqueue_time_index_complete());
}

#[silo::test]
async fn sweep_carries_the_terminal_ttl_of_the_job_it_indexes() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 2).await;
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    let done_id = tasks[0].attempt().job_id().to_string();
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");
    strip_entries(&shard, &ids).await;

    shard
        .backfill_enqueue_time_index(100, Duration::ZERO)
        .await
        .expect("backfill");

    let db = shard.db();
    for id in &ids {
        let job = shard.get_job(TENANT, id).await.unwrap().unwrap();
        let entry_ttl =
            expire_ts_of(db, &idx_enqueue_time_key(TENANT, job.enqueue_time_ms(), id)).await;
        let info_ttl = expire_ts_of(db, &job_info_key(TENANT, id)).await;
        assert_eq!(
            entry_ttl, info_ttl,
            "entry TTL for {id} must match JOB_INFO"
        );
        assert_eq!(
            entry_ttl.is_some(),
            *id == done_id,
            "only the terminal job {done_id} carries a TTL"
        );
    }
}

#[silo::test]
async fn completion_marker_gates_the_sweep_and_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 3).await;
    strip_entries(&shard, &ids).await;
    assert!(!shard.enqueue_time_index_complete());

    shard
        .set_enqueue_time_index_complete(true)
        .await
        .expect("set marker");
    assert!(shard.enqueue_time_index_complete());
    let result = shard
        .backfill_enqueue_time_index(100, Duration::ZERO)
        .await
        .expect("backfill");
    assert!(result.complete);
    assert_eq!(
        result.rows_scanned, 0,
        "a completed sweep must not run again"
    );
    assert!(indexed_ids(shard.db()).await.is_empty());

    shard.close().await.expect("close");
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    assert!(
        shard.enqueue_time_index_complete(),
        "marker must survive reopen"
    );

    shard
        .set_enqueue_time_index_complete(false)
        .await
        .expect("clear marker");
    assert!(!shard.enqueue_time_index_complete());
    let result = shard
        .backfill_enqueue_time_index(100, Duration::ZERO)
        .await
        .expect("backfill");
    assert_eq!(result.rows_written, 3);
    assert_eq!(sorted(indexed_ids(shard.db()).await), ids);
}

/// Every job in the tenant has exactly one entry with the job's enqueue time
/// and TTL, and no entry points at a missing job.
async fn assert_index_matches_jobs(shard: &JobStoreShard) {
    let db = shard.db();
    let start = idx_enqueue_time_tenant_prefix(TENANT);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut indexed: Vec<(String, i64, Option<i64>)> = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let parsed = parse_enqueue_time_index_key(&kv.key).expect("key");
        let enqueue_time_ms = parsed.enqueue_time_ms();
        indexed.push((parsed.job_id, enqueue_time_ms, kv.expire_ts));
    }
    indexed.sort();

    let start = silo::keys::job_info_prefix(TENANT);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut jobs: Vec<(String, i64, Option<i64>)> = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let parsed = silo::keys::parse_job_info_key(&kv.key).expect("key");
        let view = silo::job::JobView::new(kv.value).expect("decode");
        jobs.push((parsed.job_id, view.enqueue_time_ms(), kv.expire_ts));
    }
    jobs.sort();

    assert_eq!(
        indexed, jobs,
        "index entries must match JOB_INFO rows exactly"
    );
}

#[silo::test]
async fn job_deleted_while_the_sweep_runs_has_no_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 60).await;
    strip_entries(&shard, &ids).await;
    // Only a terminal job can be deleted.
    let victim = ids[40].clone();
    let tasks = shard
        .dequeue("worker", "default", 60)
        .await
        .expect("dequeue")
        .tasks;
    let victim_task = tasks
        .iter()
        .find(|t| t.attempt().job_id() == victim)
        .expect("victim dequeued");
    shard
        .report_attempt_outcome(
            victim_task.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");

    let sweeper = Arc::clone(&shard);
    let sweep = tokio::spawn(async move {
        sweeper
            .backfill_enqueue_time_index(1, Duration::from_millis(5))
            .await
            .expect("backfill")
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    shard.delete_job(TENANT, &victim).await.expect("delete_job");
    let result = sweep.await.expect("join");

    assert!(result.complete);
    assert!(!indexed_ids(shard.db()).await.contains(&victim));
    assert_index_matches_jobs(&shard).await;
}

#[silo::test]
async fn job_finishing_while_the_sweep_runs_gets_an_entry_with_its_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 60).await;
    strip_entries(&shard, &ids).await;
    let tasks = shard
        .dequeue("worker", "default", 60)
        .await
        .expect("dequeue")
        .tasks;
    let late = tasks
        .iter()
        .find(|t| t.attempt().job_id() == ids[45])
        .expect("dequeued");

    let sweeper = Arc::clone(&shard);
    let sweep = tokio::spawn(async move {
        sweeper
            .backfill_enqueue_time_index(1, Duration::from_millis(5))
            .await
            .expect("backfill")
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    shard
        .report_attempt_outcome(
            late.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");
    let result = sweep.await.expect("join");

    assert!(result.complete);
    let info_ttl = expire_ts_of(shard.db(), &job_info_key(TENANT, &ids[45])).await;
    assert!(info_ttl.is_some(), "finished job carries a TTL");
    assert_index_matches_jobs(&shard).await;
}

#[silo::test]
async fn sweep_completes_under_sustained_concurrent_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "seed", 200).await;
    strip_entries(&shard, &ids).await;

    let sweep_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sweeper = Arc::clone(&shard);
    let sweep = tokio::spawn(async move {
        sweeper
            .backfill_enqueue_time_index(8, Duration::from_millis(1))
            .await
            .expect("backfill")
    });
    // Finish seeded jobs (each re-puts JOB_INFO with a TTL, conflicting with
    // any batch that read it), delete some, and enqueue new ones, until the
    // sweep is done.
    let writer = Arc::clone(&shard);
    let stop = Arc::clone(&sweep_done);
    let churn = tokio::spawn(async move {
        let mut round = 0usize;
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            let tasks = writer
                .dequeue("worker", "default", 5)
                .await
                .expect("dequeue")
                .tasks;
            if tasks.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            for task in &tasks {
                writer
                    .report_attempt_outcome(
                        task.attempt().task_id(),
                        AttemptOutcome::Success { result: vec![] },
                    )
                    .await
                    .expect("report");
            }
            if round % 3 == 0 {
                let doomed = tasks[0].attempt().job_id().to_string();
                writer.delete_job(TENANT, &doomed).await.expect("delete");
            }
            enqueue_many(&writer, &format!("late-{round}"), 1).await;
            round += 1;
        }
    });
    let result = sweep.await.expect("join");
    sweep_done.store(true, std::sync::atomic::Ordering::Release);
    churn.await.expect("churn");

    assert!(result.complete, "{result:?}");
    assert!(!result.cancelled, "{result:?}");
    assert_index_matches_jobs(&shard).await;
}

fn enabled(batch_size: usize, pause_ms: u64) -> EnqueueTimeIndexBackfillConfig {
    EnqueueTimeIndexBackfillConfig {
        enabled: true,
        batch_size,
        pause_ms,
    }
}

async fn indexed_count(db: &InstrumentedDb) -> usize {
    indexed_ids(db).await.len()
}

#[silo::test]
async fn close_stops_the_sweep_and_it_resumes_from_its_checkpoint_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 200).await;
    strip_entries(&shard, &ids).await;
    shard.close().await.expect("close");

    // The background sweep starts on open and checkpoints after every row.
    let shard = open_shard_at(tmp.path(), None, enabled(1, 20)).await;
    assert!(!shard.enqueue_time_index_complete());
    let indexed_before_close = poll_until(|| indexed_count(shard.db()), |n| *n >= 5, 10_000).await;
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    assert!(
        !shard.enqueue_time_index_complete(),
        "sweep was interrupted"
    );
    let resumed = indexed_count(shard.db()).await;
    assert!(
        resumed >= indexed_before_close && resumed < 200,
        "{resumed} entries should survive the close and leave the sweep partial"
    );
    let result = shard
        .backfill_enqueue_time_index(50, Duration::ZERO)
        .await
        .expect("backfill");
    assert!(result.complete);
    assert_eq!(
        result.rows_scanned, 200,
        "counts accumulate across the resume"
    );
    assert_eq!(result.rows_written, 200);
    assert!(shard.enqueue_time_index_complete());
    assert_eq!(sorted(indexed_ids(shard.db()).await), ids);
}

#[silo::test]
async fn default_settings_spawn_no_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 3).await;
    strip_entries(&shard, &ids).await;
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    assert!(!shard.enqueue_time_index_complete());
    assert!(
        indexed_ids(shard.db()).await.is_empty(),
        "nothing backfilled"
    );
}

#[silo::test]
async fn enabled_sweep_on_an_empty_shard_sets_the_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, enabled(256, 10)).await;
    poll_until(
        || async { shard.enqueue_time_index_complete() },
        |done| *done,
        10_000,
    )
    .await;
    assert!(
        shard
            .db()
            .get(&silo::keys::enqueue_time_index_backfill_complete_key())
            .await
            .expect("get")
            .is_some(),
        "marker persisted"
    );
}
