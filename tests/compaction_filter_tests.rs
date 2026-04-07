//! Integration test for the slatedb `CompletedJobCompactionFilter`.
//!
//! Verifies end-to-end that, after a manual full compaction, JOB_STATUS rows
//! for jobs that have been driven to a terminal state and are older than the
//! filter retention window are removed from the LSM tree, while non-terminal
//! rows survive.

mod test_helpers;

use silo::gubernator::MockGubernatorClient;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::compaction_filter::CompletedJobCompactionFilterSupplier;
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::keys::job_status_key;
use silo::settings::Backend;
use silo::shard_range::ShardRange;
use silo::storage::resolve_object_store;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use test_helpers::{count_job_status_keys, fast_flush_slatedb_settings, now_ms};

const TENANT: &str = "test-tenant";
const TASK_GROUP: &str = "";
const RETENTION: Duration = Duration::from_secs(1);

/// Open a temp shard with a 1-second compaction filter retention and the
/// size-tiered scheduler suppressed so manual compaction is the only
/// compaction that runs. This makes the test deterministic — the filter
/// only fires when we explicitly trigger compaction.
async fn open_shard_with_short_retention() -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let resolved = resolve_object_store(&Backend::Fs, tmp.path().to_string_lossy().as_ref())
        .expect("resolve fs object store");

    // `min_compaction_sources = 1_000_000` is unreachable so the size-tiered
    // scheduler never proposes a compaction on its own. The compactor still
    // polls the .compactions file for manually-submitted work, so our
    // explicit `run_full_compaction()` is what drives the filter.
    let mut scheduler_options: HashMap<String, String> = HashMap::new();
    scheduler_options.insert("min_compaction_sources".to_string(), "1000000".to_string());

    let mut settings = fast_flush_slatedb_settings();
    settings.compactor_options = Some(slatedb::config::CompactorOptions {
        // Short poll so the manually-submitted compaction is picked up
        // promptly without making the test wait seconds.
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
            compaction_filter_supplier: Some(Arc::new(CompletedJobCompactionFilterSupplier::new(
                RETENTION,
            ))),
        },
        ShardRange::full(),
    )
    .await
    .expect("open shard");
    (tmp, shard)
}

/// Drive `run_full_compaction()` and poll the manifest until the LSM has
/// been merged into a single sorted run with no L0 SSTs (or until the
/// deadline expires, in which case we panic with the manifest state).
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
async fn compaction_filter_drops_old_completed_job_status_rows() {
    let (_tmp, shard) = open_shard_with_short_retention().await;

    let now = now_ms();
    // Far enough in the future that these jobs aren't dequeueable during
    // the test, so they stay in Scheduled state.
    let far_future = now + 60 * 60 * 1000;

    // ---- 1. Enqueue 3 jobs we'll drive to Succeeded, and 2 we'll leave
    //         in Scheduled. Use deterministic IDs so we can rebuild the
    //         JOB_STATUS keys for direct db.get assertions later. ----
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
                None,
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
                None,
                TASK_GROUP,
            )
            .await
            .expect("enqueue scheduled candidate");
    }

    // ---- 2. Drive the completed candidates to terminal Succeeded.
    //         The far-future scheduled jobs are not dequeueable yet, so
    //         only the 3 completed candidates come back. ----
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
                "timed out waiting to dequeue {} completed candidates",
                completed_ids.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // ---- 3. Pre-compaction sanity: all 5 JOB_STATUS rows are present and
    //         decode to the expected terminality. ----
    let count_before = count_job_status_keys(shard.db()).await;
    assert_eq!(
        count_before, 5,
        "expected 5 JOB_STATUS rows before compaction (3 succeeded + 2 scheduled)"
    );

    for id in &completed_ids {
        let key = job_status_key(TENANT, id);
        let raw = shard
            .db()
            .get(key.as_slice())
            .await
            .expect("db.get")
            .unwrap_or_else(|| panic!("missing JOB_STATUS for completed job {id} pre-compaction"));
        let status = silo::codec::decode_job_status_owned(&raw).expect("decode job status");
        assert!(
            status.is_terminal(),
            "expected job {id} to be terminal pre-compaction, got {:?}",
            status.kind
        );
    }
    for id in &scheduled_ids {
        let key = job_status_key(TENANT, id);
        let raw = shard
            .db()
            .get(key.as_slice())
            .await
            .expect("db.get")
            .unwrap_or_else(|| panic!("missing JOB_STATUS for scheduled job {id} pre-compaction"));
        let status = silo::codec::decode_job_status_owned(&raw).expect("decode job status");
        assert!(
            !status.is_terminal(),
            "expected job {id} to be non-terminal pre-compaction, got {:?}",
            status.kind
        );
    }

    // ---- 4. Sleep past the retention cutoff so the completed rows'
    //         `changed_at_ms` is older than `RETENTION` when the filter
    //         computes its cutoff at compaction time. ----
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;

    // ---- 5. Run a full manual compaction and wait for it to settle. ----
    run_full_compaction_and_wait(&shard).await;

    // ---- 6. Close and reopen the shard before verifying.
    //
    // After a compaction completes, the slatedb writer's in-memory state
    // doesn't immediately pick up the new sorted run — it polls the
    // manifest from object storage on its `manifest_poll_interval`. Until
    // that next poll, reads on the live shard still go through the
    // pre-compaction state. Closing and reopening forces a fresh manifest
    // read so the assertions exercise the post-compaction state. ----
    let resolved_path = shard.db_path().to_string();
    let resolved_store = shard.store().clone();
    shard.close().await.expect("close mid-test");
    let shard = JobStoreShard::open_with_resolved_store(
        "test".to_string(),
        &resolved_path,
        OpenShardOptions {
            store: resolved_store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(fast_flush_slatedb_settings()),
            memory_cache: None,
            rate_limiter: MockGubernatorClient::new_arc(),
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(5000),
            compaction_filter_supplier: Some(Arc::new(CompletedJobCompactionFilterSupplier::new(
                RETENTION,
            ))),
        },
        ShardRange::full(),
    )
    .await
    .expect("reopen shard after compaction");

    // ---- 7. The 3 completed JOB_STATUS rows should be gone. ----
    for id in &completed_ids {
        let key = job_status_key(TENANT, id);
        let raw = shard.db().get(key.as_slice()).await.expect("db.get");
        assert!(
            raw.is_none(),
            "expected JOB_STATUS for completed job {id} to be tombstoned by the compaction filter, got {:?}",
            raw
        );
    }

    // ---- 7. The 2 scheduled JOB_STATUS rows should still be present. ----
    for id in &scheduled_ids {
        let key = job_status_key(TENANT, id);
        let raw = shard
            .db()
            .get(key.as_slice())
            .await
            .expect("db.get")
            .unwrap_or_else(|| {
                panic!("expected JOB_STATUS for scheduled job {id} to survive compaction")
            });
        let status = silo::codec::decode_job_status_owned(&raw).expect("decode job status");
        assert!(
            !status.is_terminal(),
            "scheduled job {id} should still be non-terminal after compaction, got {:?}",
            status.kind
        );
    }

    let count_after = count_job_status_keys(shard.db()).await;
    assert_eq!(
        count_after, 2,
        "expected 2 JOB_STATUS rows to survive compaction (the 2 scheduled jobs)"
    );

    shard.close().await.expect("close");
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_filter_keeps_recently_completed_jobs() {
    // Sanity check the inverse: if a job is completed but the retention
    // hasn't elapsed yet at compaction time, the filter must keep it.
    // Otherwise we'd be silently dropping fresh completions.
    let (_tmp, shard) = open_shard_with_short_retention().await;
    let now = now_ms();

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
            None,
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

    // Compact immediately — no sleep — so the completed row's
    // `changed_at_ms` is well within the 1-second retention window.
    run_full_compaction_and_wait(&shard).await;

    // Close and reopen (see comment in the other test for why).
    let resolved_path = shard.db_path().to_string();
    let resolved_store = shard.store().clone();
    shard.close().await.expect("close mid-test");
    let shard = JobStoreShard::open_with_resolved_store(
        "test".to_string(),
        &resolved_path,
        OpenShardOptions {
            store: resolved_store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(fast_flush_slatedb_settings()),
            memory_cache: None,
            rate_limiter: MockGubernatorClient::new_arc(),
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(5000),
            compaction_filter_supplier: Some(Arc::new(CompletedJobCompactionFilterSupplier::new(
                RETENTION,
            ))),
        },
        ShardRange::full(),
    )
    .await
    .expect("reopen shard after compaction");

    let key = job_status_key(TENANT, &job_id);
    let raw = shard
        .db()
        .get(key.as_slice())
        .await
        .expect("db.get")
        .expect("recently-completed JOB_STATUS row should survive immediate compaction");
    let status = silo::codec::decode_job_status_owned(&raw).expect("decode");
    assert!(
        status.is_terminal(),
        "expected fresh job to still be terminal after compaction, got {:?}",
        status.kind
    );

    shard.close().await.expect("close");
}
