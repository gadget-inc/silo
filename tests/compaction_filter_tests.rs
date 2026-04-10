//! Integration test for the slatedb `CompletedJobCompactionFilter`.
//!
//! Verifies end-to-end that, after a manual full compaction, rows for jobs
//! that have been driven to a terminal state and are older than the filter
//! retention window are removed across all handled prefixes (JOB_STATUS,
//! IDX_STATUS_TIME, IDX_METADATA, ATTEMPT, JOB_CANCELLED), while
//! non-terminal rows survive.

mod test_helpers;

use silo::gubernator::MockGubernatorClient;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::keys::{attempt_key, idx_metadata_key, job_info_key, job_status_key};
use silo::settings::Backend;
use silo::shard_range::ShardRange;
use silo::storage::resolve_object_store;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use test_helpers::{
    count_job_info_keys, count_job_status_keys, count_status_time_index_keys,
    count_with_binary_prefix, fast_flush_slatedb_settings, now_ms,
};

const TENANT: &str = "test-tenant";
const TASK_GROUP: &str = "";
const RETENTION: Duration = Duration::from_secs(1);

/// Metadata attached to completed jobs so IDX_METADATA rows are created.
const META_KEY: &str = "queue";
const META_VALUE: &str = "billing";

/// Open a temp shard with a 1-second compaction filter retention and the
/// size-tiered scheduler suppressed so manual compaction is the only
/// compaction that runs.
async fn open_shard_with_short_retention() -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let resolved = resolve_object_store(&Backend::Fs, tmp.path().to_string_lossy().as_ref())
        .expect("resolve fs object store");

    let mut scheduler_options: HashMap<String, String> = HashMap::new();
    scheduler_options.insert("min_compaction_sources".to_string(), "1000000".to_string());

    let mut settings = fast_flush_slatedb_settings();
    settings.compactor_options = Some(slatedb::config::CompactorOptions {
        poll_interval: Duration::from_millis(100),
        scheduler_options,
        ..slatedb::config::CompactorOptions::default()
    });

    let shard = JobStoreShard::open_with_resolved_store(
        "test".to_string(),
        &resolved.canonical_path,
        OpenShardOptions {
            store: resolved.store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(settings),
            memory_cache: None,
            rate_limiter: MockGubernatorClient::new_arc(),
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(5000),
            compaction_filter_retention: Some(RETENTION),
        },
        ShardRange::full(),
    )
    .await
    .expect("open shard");
    (tmp, shard)
}

/// Close the shard and reopen it so reads come from on-disk state.
///
/// After a compaction completes, the slatedb writer's in-memory state
/// doesn't immediately pick up the new sorted run — it polls the manifest
/// from object storage on its `manifest_poll_interval`. Closing and
/// reopening forces a fresh manifest read.
async fn reopen_shard(shard: Arc<JobStoreShard>) -> Arc<JobStoreShard> {
    let path = shard.db_path().to_string();
    let store = shard.store().clone();
    shard.close().await.expect("close mid-test");
    JobStoreShard::open_with_resolved_store(
        "test".to_string(),
        &path,
        OpenShardOptions {
            store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(fast_flush_slatedb_settings()),
            memory_cache: None,
            rate_limiter: MockGubernatorClient::new_arc(),
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(5000),
            compaction_filter_retention: Some(RETENTION),
        },
        ShardRange::full(),
    )
    .await
    .expect("reopen shard")
}

/// Drive `run_full_compaction()` and poll the manifest until settled.
async fn run_full_compaction_and_wait(shard: &JobStoreShard) {
    use slatedb::admin::AdminBuilder;

    shard
        .run_full_compaction()
        .await
        .expect("run_full_compaction");

    let admin = AdminBuilder::new(shard.db_path(), shard.store().clone()).build();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let state = admin
            .read_compactor_state_view()
            .await
            .expect("read compactor state view");
        let manifest = state.manifest();
        if manifest.l0.is_empty() && manifest.compacted.len() <= 1 {
            return;
        }
        if Instant::now() > deadline {
            panic!(
                "compaction did not finish within deadline: l0={} compacted_runs={}",
                manifest.l0.len(),
                manifest.compacted.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_filter_drops_all_related_rows_for_old_completed_jobs() {
    let (_tmp, shard) = open_shard_with_short_retention().await;

    let now = now_ms();
    let far_future = now + 60 * 60 * 1000;
    let metadata = Some(vec![(META_KEY.to_string(), META_VALUE.to_string())]);

    // ---- 1. Enqueue 3 completed candidates (with metadata) and 2
    //         scheduled candidates (also with metadata, for comparison). ----
    let completed_ids: Vec<String> = (0..3).map(|i| format!("completed-job-{i}")).collect();
    let scheduled_ids: Vec<String> = (0..2).map(|i| format!("scheduled-job-{i}")).collect();

    for id in &completed_ids {
        shard
            .enqueue(
                TENANT,
                Some(id.clone()),
                50,
                now,
                None,
                Vec::new(),
                vec![],
                metadata.clone(),
                TASK_GROUP,
            )
            .await
            .expect("enqueue completed candidate");
    }
    for id in &scheduled_ids {
        shard
            .enqueue(
                TENANT,
                Some(id.clone()),
                50,
                far_future,
                None,
                Vec::new(),
                vec![],
                metadata.clone(),
                TASK_GROUP,
            )
            .await
            .expect("enqueue scheduled candidate");
    }

    // ---- 2. Drive the 3 completed candidates to terminal Succeeded. ----
    let mut total_completed = 0usize;
    let dequeue_deadline = Instant::now() + Duration::from_secs(5);
    while total_completed < completed_ids.len() {
        let result = shard
            .dequeue("test-worker", TASK_GROUP, 16)
            .await
            .expect("dequeue");
        for task in &result.tasks {
            let task_id = task.attempt().task_id().to_string();
            shard
                .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
                .await
                .expect("report success");
            total_completed += 1;
        }
        if result.tasks.is_empty() {
            assert!(
                Instant::now() < dequeue_deadline,
                "timed out waiting to dequeue completed candidates",
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // ---- 3. Pre-compaction sanity checks. ----
    assert_eq!(count_job_info_keys(shard.db()).await, 5);
    assert_eq!(count_job_status_keys(shard.db()).await, 5);
    // 5 jobs × 1 metadata entry each = 5 IDX_METADATA rows
    assert_eq!(count_with_binary_prefix(shard.db(), &[0x04]).await, 5);
    // 3 completed jobs have 1 attempt each
    assert_eq!(count_with_binary_prefix(shard.db(), &[0x07]).await, 3);
    // IDX_STATUS_TIME: 3 Succeeded + 2 Scheduled = 5 (each job has
    // exactly one IDX_STATUS_TIME entry for its current status because
    // the lifecycle writes: Scheduled → delete old idx + write new Running idx
    // → delete old idx + write new Succeeded idx)
    assert_eq!(count_status_time_index_keys(shard.db()).await, 5);

    // ---- 4. Sleep past retention cutoff. ----
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;

    // ---- 5. Compact and reopen. ----
    run_full_compaction_and_wait(&shard).await;
    let shard = reopen_shard(shard).await;

    // ---- 6. JOB_INFO: 3 completed gone, 2 scheduled remain. ----
    for id in &completed_ids {
        let key = job_info_key(TENANT, id);
        let raw = shard.db().get(key.as_slice()).await.expect("db.get");
        assert!(
            raw.is_none(),
            "JOB_INFO for completed job {id} should be tombstoned"
        );
    }
    for id in &scheduled_ids {
        let raw = shard
            .db()
            .get(job_info_key(TENANT, id).as_slice())
            .await
            .expect("db.get");
        assert!(
            raw.is_some(),
            "JOB_INFO for scheduled job {id} should survive"
        );
    }
    assert_eq!(count_job_info_keys(shard.db()).await, 2);

    // ---- 7. JOB_STATUS: 3 completed gone, 2 scheduled remain. ----
    for id in &completed_ids {
        let key = job_status_key(TENANT, id);
        let raw = shard.db().get(key.as_slice()).await.expect("db.get");
        assert!(
            raw.is_none(),
            "JOB_STATUS for completed job {id} should be tombstoned"
        );
    }
    for id in &scheduled_ids {
        let raw = shard
            .db()
            .get(job_status_key(TENANT, id).as_slice())
            .await
            .expect("db.get");
        assert!(
            raw.is_some(),
            "JOB_STATUS for scheduled job {id} should survive"
        );
    }
    assert_eq!(count_job_status_keys(shard.db()).await, 2);

    // ---- 8. IDX_STATUS_TIME: only the 2 Scheduled entries remain. ----
    assert_eq!(
        count_status_time_index_keys(shard.db()).await,
        2,
        "expected 2 IDX_STATUS_TIME rows (the 2 scheduled jobs)"
    );

    // ---- 9. ATTEMPT: all 3 attempt rows for completed jobs are gone. ----
    for id in &completed_ids {
        let key = attempt_key(TENANT, id, 1);
        let raw = shard.db().get(key.as_slice()).await.expect("db.get");
        assert!(
            raw.is_none(),
            "ATTEMPT for completed job {id} should be tombstoned"
        );
    }
    assert_eq!(
        count_with_binary_prefix(shard.db(), &[0x07]).await,
        0,
        "no attempt rows should survive (scheduled jobs have no attempts)"
    );

    // ---- 10. IDX_METADATA: completed jobs' metadata gone, scheduled
    //         jobs' metadata remains. ----
    for id in &completed_ids {
        let key = idx_metadata_key(TENANT, META_KEY, META_VALUE, id);
        let raw = shard.db().get(key.as_slice()).await.expect("db.get");
        assert!(
            raw.is_none(),
            "IDX_METADATA for completed job {id} should be tombstoned"
        );
    }
    for id in &scheduled_ids {
        let key = idx_metadata_key(TENANT, META_KEY, META_VALUE, id);
        let raw = shard.db().get(key.as_slice()).await.expect("db.get");
        assert!(
            raw.is_some(),
            "IDX_METADATA for scheduled job {id} should survive"
        );
    }
    assert_eq!(
        count_with_binary_prefix(shard.db(), &[0x04]).await,
        2,
        "only the 2 scheduled jobs' metadata should survive"
    );

    shard.close().await.expect("close");
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_filter_keeps_all_rows_for_recently_completed_jobs() {
    let (_tmp, shard) = open_shard_with_short_retention().await;
    let now = now_ms();
    let metadata = Some(vec![(META_KEY.to_string(), META_VALUE.to_string())]);

    let job_id = "fresh-job".to_string();
    shard
        .enqueue(
            TENANT,
            Some(job_id.clone()),
            50,
            now,
            None,
            Vec::new(),
            vec![],
            metadata,
            TASK_GROUP,
        )
        .await
        .expect("enqueue");

    // Dequeue + complete.
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !completed {
        let result = shard
            .dequeue("test-worker", TASK_GROUP, 4)
            .await
            .expect("dequeue");
        for task in &result.tasks {
            let task_id = task.attempt().task_id().to_string();
            shard
                .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
                .await
                .expect("report success");
            completed = true;
        }
        if !completed {
            assert!(Instant::now() < deadline, "timed out dequeuing fresh job");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Compact immediately (no sleep) — job is within retention.
    run_full_compaction_and_wait(&shard).await;
    let shard = reopen_shard(shard).await;

    // JOB_INFO survives.
    let raw = shard
        .db()
        .get(job_info_key(TENANT, &job_id).as_slice())
        .await
        .expect("db.get");
    assert!(
        raw.is_some(),
        "JOB_INFO should survive for recently completed job"
    );

    // JOB_STATUS survives.
    let raw = shard
        .db()
        .get(job_status_key(TENANT, &job_id).as_slice())
        .await
        .expect("db.get")
        .expect("JOB_STATUS should survive");
    let status = silo::codec::decode_job_status_owned(&raw).expect("decode");
    assert!(status.is_terminal());

    // IDX_STATUS_TIME survives.
    assert_eq!(
        count_status_time_index_keys(shard.db()).await,
        1,
        "IDX_STATUS_TIME should survive for recently completed job"
    );

    // ATTEMPT survives.
    let raw = shard
        .db()
        .get(attempt_key(TENANT, &job_id, 1).as_slice())
        .await
        .expect("db.get");
    assert!(
        raw.is_some(),
        "ATTEMPT should survive for recently completed job"
    );

    // IDX_METADATA survives.
    let raw = shard
        .db()
        .get(idx_metadata_key(TENANT, META_KEY, META_VALUE, &job_id).as_slice())
        .await
        .expect("db.get");
    assert!(
        raw.is_some(),
        "IDX_METADATA should survive for recently completed job"
    );

    shard.close().await.expect("close");
}
