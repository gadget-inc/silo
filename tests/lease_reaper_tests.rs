//! Tests for the lease reaper: between its periodic scans of the whole lease
//! range it reads only the lease rows its in-memory index reports due, so a
//! pass costs nothing while no lease has expired and does not grow with the
//! leases a shard has already released.

mod test_helpers;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use silo::codec::{decode_lease, encode_lease};
use silo::job::JobStatusKind;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::{JobStoreShard, LeaseReapPass};
use silo::lease_reaper::{KeyedTaskPool, LeaseReaper};
use silo::settings::DEFAULT_LEASE_REAPER_FULL_SCAN_INTERVAL_MS;
use silo::shard_range::{ShardId, ShardRange};
use silo::task::LeaseRecord;
use test_helpers::*;

const FULL_SCAN_INTERVAL_MS: i64 = DEFAULT_LEASE_REAPER_FULL_SCAN_INTERVAL_MS as i64;

async fn enqueue_job(shard: &JobStoreShard, job_id: &str) {
    shard
        .enqueue(
            "-",
            Some(job_id.to_string()),
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"j": job_id})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

/// Enqueue `count` jobs and lease them all, returning their task ids.
async fn lease_jobs(shard: &JobStoreShard, prefix: &str, count: usize) -> Vec<String> {
    for i in 0..count {
        enqueue_job(shard, &format!("{prefix}-{i}")).await;
    }
    dequeue_task_ids_until(shard, "worker", "default", count).await
}

/// Run the pass that seeds the index, which scans the whole lease range.
async fn seed(shard: &JobStoreShard) -> i64 {
    let now = now_ms();
    let pass = shard.reap_due_leases_at(now).await.expect("seeding pass");
    assert!(pass.full_scan, "the first pass scans the whole lease range");
    now
}

async fn lease_expiry_ms(shard: &JobStoreShard, task_id: &str) -> i64 {
    let raw = shard
        .db()
        .get(silo::keys::leased_task_key(task_id))
        .await
        .expect("get lease")
        .expect("lease exists");
    decode_lease(raw).expect("decode lease").expiry_ms()
}

async fn lease_exists(shard: &JobStoreShard, task_id: &str) -> bool {
    shard
        .db()
        .get(silo::keys::leased_task_key(task_id))
        .await
        .expect("get lease")
        .is_some()
}

/// Rewrite a lease row's expiry without telling the index, the state a
/// renewal leaves when its index update is lost.
async fn rewrite_lease_expiry(shard: &JobStoreShard, task_id: &str, expiry_ms: i64) {
    let key = silo::keys::leased_task_key(task_id);
    let raw = shard
        .db()
        .get(&key)
        .await
        .expect("get lease")
        .expect("lease exists");
    let decoded = decode_lease(raw).expect("decode lease");
    let record = LeaseRecord {
        worker_id: decoded.worker_id().to_string(),
        task: decoded.to_task().expect("lease task"),
        expiry_ms,
        started_at_ms: decoded.started_at_ms(),
    };
    shard
        .db()
        .put(&key, encode_lease(&record))
        .await
        .expect("put lease");
    shard.db().flush().await.expect("flush lease");
}

fn slatedb_scan_requests(shard: &JobStoreShard, metrics: &silo::metrics::Metrics) -> f64 {
    metrics.update_slatedb_stats("test", shard.slatedb_metrics_recorder());
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_slatedb_scan_requests_total", "shard=\"test\""],
    )
}

/// Wait until SlateDB's scan request count for the shard holds still across
/// a short pause, so a scan started by an earlier write has finished.
async fn wait_for_scans_to_settle(shard: &JobStoreShard, metrics: &silo::metrics::Metrics) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let before = slatedb_scan_requests(shard, metrics);
        tokio::time::sleep(Duration::from_millis(25)).await;
        if slatedb_scan_requests(shard, metrics) == before {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background scans never paused"
        );
    }
}

/// The fewest SlateDB scans opened across one reaper pass, sampled
/// `samples` times at `now_ms`. The count covers the whole shard, so a
/// background scan can add to a sample; a pass that scans shows in all of
/// them.
async fn fewest_scans_per_pass(
    shard: &JobStoreShard,
    metrics: &silo::metrics::Metrics,
    now_ms: i64,
    samples: usize,
) -> (f64, Vec<LeaseReapPass>) {
    let mut fewest = f64::MAX;
    let mut passes = Vec::new();
    for _ in 0..samples {
        wait_for_scans_to_settle(shard, metrics).await;
        let before = slatedb_scan_requests(shard, metrics);
        passes.push(shard.reap_due_leases_at(now_ms).await.expect("pass"));
        fewest = fewest.min(slatedb_scan_requests(shard, metrics) - before);
    }
    (fewest, passes)
}

/// With no leases, a pass after the seeding scan opens no scan.
#[silo::test]
async fn a_shard_with_no_leases_is_not_scanned() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let now = seed(&shard).await;

    let (fewest, passes) = fewest_scans_per_pass(&shard, &metrics, now + 1_000, 5).await;

    assert_eq!(fewest, 0.0, "a pass with nothing due opened a scan");
    for pass in passes {
        assert_eq!(pass, LeaseReapPass::default());
    }
}

/// Live leases that have not expired cost a pass neither a scan nor a read.
#[silo::test]
async fn unexpired_leases_are_neither_scanned_nor_read() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let now = seed(&shard).await;
    let task_ids = lease_jobs(&shard, "live", 3).await;
    assert_eq!(shard.tracked_lease_count(), 3);

    let (fewest, passes) = fewest_scans_per_pass(&shard, &metrics, now + 1_000, 5).await;

    assert_eq!(fewest, 0.0, "a pass with nothing due opened a scan");
    for pass in passes {
        assert_eq!(pass, LeaseReapPass::default());
    }
    for task_id in &task_ids {
        assert!(lease_exists(&shard, task_id).await);
    }
}

/// A lease is reaped by the first pass at or after its expiry, by reading
/// its row, and its attempt fails as a crashed worker.
#[silo::test]
async fn an_expired_lease_is_reaped_by_key_without_a_scan() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard).await;
    let task_ids = lease_jobs(&shard, "crash", 1).await;
    let expiry_ms = lease_expiry_ms(&shard, &task_ids[0]).await;

    let early = shard
        .reap_due_leases_at(expiry_ms - 1)
        .await
        .expect("early pass");
    assert_eq!(early, LeaseReapPass::default(), "nothing is due yet");

    wait_for_scans_to_settle(&shard, &metrics).await;
    let scans_before = slatedb_scan_requests(&shard, &metrics);
    let pass = shard
        .reap_due_leases_at(expiry_ms)
        .await
        .expect("pass at expiry");
    let scans = slatedb_scan_requests(&shard, &metrics) - scans_before;

    assert_eq!(
        pass,
        LeaseReapPass {
            reaped: 1,
            checked: 1,
            ..Default::default()
        }
    );
    assert_eq!(scans, 0.0, "reaping a due lease opened a scan");
    assert!(!lease_exists(&shard, &task_ids[0]).await);
    assert_eq!(shard.tracked_lease_count(), 0);

    let status = shard
        .get_job_status("-", "crash-0")
        .await
        .expect("get status")
        .expect("status exists");
    assert_eq!(status.kind, JobStatusKind::Failed);
    let attempt = shard
        .get_job_attempt("-", "crash-0", 1)
        .await
        .expect("get attempt")
        .expect("attempt exists");
    match attempt.state() {
        AttemptStatus::Failed { error_code, .. } => assert_eq!(error_code, "WORKER_CRASHED"),
        other => panic!("expected a failed attempt, got {other:?}"),
    }
}

/// A heartbeat moves the lease's index entry to the renewed expiry: the pass
/// at the original expiry does not read the row, and the pass at the renewed
/// one reaps it.
#[silo::test]
async fn a_heartbeat_moves_the_reap_to_the_renewed_expiry() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard).await;
    let task_ids = lease_jobs(&shard, "beat", 1).await;
    let original_expiry_ms = lease_expiry_ms(&shard, &task_ids[0]).await;

    tokio::time::sleep(Duration::from_millis(20)).await;
    shard
        .heartbeat_task("worker", &task_ids[0])
        .await
        .expect("heartbeat");
    let renewed_expiry_ms = lease_expiry_ms(&shard, &task_ids[0]).await;
    assert!(renewed_expiry_ms > original_expiry_ms);

    let at_original = shard
        .reap_due_leases_at(original_expiry_ms)
        .await
        .expect("pass at original expiry");
    assert_eq!(at_original, LeaseReapPass::default());
    assert!(lease_exists(&shard, &task_ids[0]).await);

    let at_renewed = shard
        .reap_due_leases_at(renewed_expiry_ms)
        .await
        .expect("pass at renewed expiry");
    assert_eq!(at_renewed.reaped, 1);
    assert!(!lease_exists(&shard, &task_ids[0]).await);
}

/// A row renewed without the index hearing of it is read once at the stale
/// expiry, kept, and reaped when the row's own expiry passes.
#[silo::test]
async fn a_lease_renewed_behind_the_index_is_kept_until_its_row_expires() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard).await;
    let task_ids = lease_jobs(&shard, "behind", 1).await;
    let indexed_expiry_ms = lease_expiry_ms(&shard, &task_ids[0]).await;
    let row_expiry_ms = indexed_expiry_ms + 5_000;
    rewrite_lease_expiry(&shard, &task_ids[0], row_expiry_ms).await;

    let stale = shard
        .reap_due_leases_at(indexed_expiry_ms)
        .await
        .expect("pass at indexed expiry");
    assert_eq!(
        stale,
        LeaseReapPass {
            checked: 1,
            ..Default::default()
        }
    );
    assert!(lease_exists(&shard, &task_ids[0]).await);

    let between = shard
        .reap_due_leases_at(row_expiry_ms - 1)
        .await
        .expect("pass before row expiry");
    assert_eq!(
        between,
        LeaseReapPass::default(),
        "the entry follows the row"
    );

    let at_row_expiry = shard
        .reap_due_leases_at(row_expiry_ms)
        .await
        .expect("pass at row expiry");
    assert_eq!(at_row_expiry.reaped, 1);
}

/// Released leases leave the index, so a pass reads only the leases still
/// held however many the shard has released.
#[silo::test]
async fn a_pass_reads_live_leases_not_released_ones() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard).await;
    let finished = lease_jobs(&shard, "done", 200).await;
    for task_id in &finished {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");
    }
    assert_eq!(count_lease_keys(shard.db()).await, 0);
    assert_eq!(shard.tracked_lease_count(), 0);

    let held = lease_jobs(&shard, "held", 2).await;
    let mut latest_expiry_ms = 0;
    for task_id in &held {
        latest_expiry_ms = latest_expiry_ms.max(lease_expiry_ms(&shard, task_id).await);
    }

    wait_for_scans_to_settle(&shard, &metrics).await;
    let scans_before = slatedb_scan_requests(&shard, &metrics);
    let pass = shard
        .reap_due_leases_at(latest_expiry_ms)
        .await
        .expect("pass");
    let scans = slatedb_scan_requests(&shard, &metrics) - scans_before;

    assert_eq!(
        pass,
        LeaseReapPass {
            reaped: 2,
            checked: 2,
            ..Default::default()
        },
        "the pass read the two held leases and none of the 200 released"
    );
    assert_eq!(scans, 0.0, "the pass opened a scan");
}

/// An expired lease on a cancelled job ends the job Cancelled.
#[silo::test]
async fn an_expired_lease_on_a_cancelled_job_reports_cancelled() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard).await;
    let task_ids = lease_jobs(&shard, "cancel", 1).await;
    shard.cancel_job("-", "cancel-0").await.expect("cancel");
    let expiry_ms = lease_expiry_ms(&shard, &task_ids[0]).await;

    let pass = shard.reap_due_leases_at(expiry_ms).await.expect("pass");

    assert_eq!(pass.reaped, 1);
    let status = shard
        .get_job_status("-", "cancel-0")
        .await
        .expect("get status")
        .expect("status exists");
    assert_eq!(status.kind, JobStatusKind::Cancelled);
    let attempt = shard
        .get_job_attempt("-", "cancel-0", 1)
        .await
        .expect("get attempt")
        .expect("attempt exists");
    assert!(
        matches!(attempt.state(), AttemptStatus::Cancelled { .. }),
        "expected a cancelled attempt, got {:?}",
        attempt.state()
    );
}

/// A lease taken through `lease_task` is indexed and reaped at its expiry.
#[silo::test]
async fn a_lease_taken_for_a_named_job_is_reaped_at_its_expiry() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard).await;
    enqueue_job(&shard, "named").await;
    let leased = shard
        .lease_task("-", "named", "worker")
        .await
        .expect("lease task");
    let task_id = leased.attempt().task_id().to_string();
    assert_eq!(shard.tracked_lease_count(), 1);
    let expiry_ms = lease_expiry_ms(&shard, &task_id).await;

    let pass = shard.reap_due_leases_at(expiry_ms).await.expect("pass");

    assert_eq!(pass.reaped, 1);
    assert!(!lease_exists(&shard, &task_id).await);
}

/// An expired floating limit refresh lease is reaped by key and its limit is
/// free to schedule another refresh.
#[silo::test]
async fn an_expired_refresh_lease_is_reaped_by_key() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard).await;
    let queue = "reaper-refresh-q".to_string();
    let limit = || {
        vec![silo::job::Limit::FloatingConcurrency(
            silo::job::FloatingConcurrencyLimit {
                key: queue.clone(),
                default_max_concurrency: 1,
                refresh_interval_ms: 50,
                metadata: vec![],
            },
        )]
    };
    for job in ["first", "second"] {
        shard
            .enqueue(
                "-",
                Some(job.to_string()),
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"j": job})),
                limit(),
                None,
                "default",
            )
            .await
            .expect("enqueue");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let leased =
        dequeue_refresh_tasks_until(&shard, "worker", "default", 1, Duration::from_secs(5)).await;
    let task_id = leased.refresh_tasks[0].task_id.clone();
    let expiry_ms = lease_expiry_ms(&shard, &task_id).await;

    let pass = shard.reap_due_leases_at(expiry_ms).await.expect("pass");

    assert!(pass.reaped >= 1, "the refresh lease was due: {pass:?}");
    assert!(!pass.full_scan);
    assert!(!lease_exists(&shard, &task_id).await);
    let state_raw = shard
        .db()
        .get(silo::keys::floating_limit_state_key("-", &queue))
        .await
        .expect("get state")
        .expect("state exists");
    let state = silo::codec::decode_floating_limit_state(state_raw).expect("decode state");
    assert!(!state.refresh_task_scheduled());
}

/// The seeding scan indexes leases already in the database, so a lease taken
/// before this process opened the shard is reaped at its expiry by key.
#[silo::test]
async fn the_seeding_scan_indexes_leases_already_held() {
    let (_tmp, shard) = open_temp_shard().await;
    let expiry_ms = now_ms() + 5_000;
    write_run_attempt_lease(
        &shard,
        "worker",
        "pre-existing",
        "-",
        "job",
        "default",
        expiry_ms,
        now_ms(),
    )
    .await;
    assert_eq!(shard.tracked_lease_count(), 0);

    let seeding = shard.reap_due_leases_at(now_ms()).await.expect("seed");

    assert_eq!(
        seeding,
        LeaseReapPass {
            full_scan: true,
            ..Default::default()
        },
        "leases found by the seeding scan are not counted as untracked"
    );
    assert_eq!(shard.tracked_lease_count(), 1);

    let pass = shard.reap_due_leases_at(expiry_ms).await.expect("pass");
    assert_eq!(
        pass,
        LeaseReapPass {
            reaped: 1,
            checked: 1,
            ..Default::default()
        }
    );
    assert!(!lease_exists(&shard, "pre-existing").await);
}

/// A lease the index never heard of waits for the periodic full scan, which
/// reaps it and counts it as untracked.
#[silo::test]
async fn the_periodic_full_scan_reaps_a_lease_missing_from_the_index() {
    let (_tmp, shard) = open_temp_shard().await;
    let seeded_at = seed(&shard).await;
    write_run_attempt_lease(
        &shard,
        "worker",
        "missed",
        "-",
        "job",
        "default",
        seeded_at - 1,
        seeded_at - 10_000,
    )
    .await;

    let before_interval = shard
        .reap_due_leases_at(seeded_at + FULL_SCAN_INTERVAL_MS - 1)
        .await
        .expect("pass before the interval");
    assert_eq!(before_interval, LeaseReapPass::default());
    assert!(lease_exists(&shard, "missed").await);

    let at_interval = shard
        .reap_due_leases_at(seeded_at + FULL_SCAN_INTERVAL_MS)
        .await
        .expect("pass at the interval");
    assert_eq!(
        at_interval,
        LeaseReapPass {
            reaped: 1,
            full_scan: true,
            untracked: 1,
            ..Default::default()
        }
    );
    assert!(!lease_exists(&shard, "missed").await);

    let after = shard
        .reap_due_leases_at(seeded_at + FULL_SCAN_INTERVAL_MS + 1)
        .await
        .expect("pass after the full scan");
    assert_eq!(
        after,
        LeaseReapPass::default(),
        "the interval restarts at the full scan"
    );
}

/// A periodic full scan over leases the index already holds counts none of
/// them as untracked.
#[silo::test]
async fn the_periodic_full_scan_finds_nothing_untracked_among_indexed_leases() {
    let (_tmp, shard) = open_temp_shard().await;
    let seeded_at = seed(&shard).await;
    lease_jobs(&shard, "tracked", 5).await;
    for task_id in lease_jobs(&shard, "released", 5).await {
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");
    }
    // Renew the held leases far enough that the full scan finds them live.
    for (key, raw) in all_leases(&shard).await {
        let decoded = decode_lease(raw).expect("decode lease");
        let task_id = decoded.task_id().expect("run attempt lease").to_string();
        assert_eq!(key, silo::keys::leased_task_key(&task_id));
        rewrite_lease_expiry(&shard, &task_id, seeded_at + 10 * FULL_SCAN_INTERVAL_MS).await;
    }

    // The index still holds each lease's original expiry, so the pass at the
    // interval reads the whole range instead of the due entries.
    let pass = shard
        .reap_due_leases_at(seeded_at + FULL_SCAN_INTERVAL_MS)
        .await
        .expect("full scan pass");

    assert_eq!(
        pass,
        LeaseReapPass {
            full_scan: true,
            ..Default::default()
        }
    );
    assert_eq!(shard.tracked_lease_count(), 5);
}

async fn all_leases(shard: &JobStoreShard) -> Vec<(Vec<u8>, bytes::Bytes)> {
    let start = silo::keys::leases_prefix();
    let end = silo::keys::end_bound(&start);
    let mut iter = shard
        .db()
        .scan::<Vec<u8>, _>(start..end)
        .await
        .expect("scan leases");
    let mut rows = Vec::new();
    while let Some(kv) = iter.next().await.expect("next lease") {
        rows.push((kv.key.to_vec(), kv.value));
    }
    rows
}

/// A lease for a tenant outside the shard's range is deleted by the seeding
/// scan without being reaped.
#[silo::test]
async fn the_seeding_scan_deletes_leases_outside_the_shard_range() {
    // aaa hashes to ae01, outside [, 8000); bbb hashes to 3a4f, inside it.
    let (_tmp, shard) = open_temp_shard_with_range(ShardRange::new("", "8000")).await;
    let expiry_ms = now_ms() + 5_000;
    write_run_attempt_lease(
        &shard,
        "worker",
        "outside",
        "aaa",
        "job",
        "default",
        expiry_ms,
        now_ms(),
    )
    .await;
    write_run_attempt_lease(
        &shard,
        "worker",
        "inside",
        "bbb",
        "job",
        "default",
        expiry_ms,
        now_ms(),
    )
    .await;

    let seeding = shard.reap_due_leases_at(now_ms()).await.expect("seed");

    assert_eq!(seeding.reaped, 0);
    assert!(!lease_exists(&shard, "outside").await);
    assert!(lease_exists(&shard, "inside").await);
    assert_eq!(shard.tracked_lease_count(), 1);
}

/// `reap_expired_leases` scans the whole range on every call, reaping a
/// lease whose row expired without the index hearing of it. The index entry
/// left at the lease's original expiry is dropped when it comes due.
#[silo::test]
async fn a_forced_full_reap_finds_a_lease_expired_behind_the_index() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard).await;
    let task_ids = lease_jobs(&shard, "forced", 1).await;
    let indexed_expiry_ms = lease_expiry_ms(&shard, &task_ids[0]).await;
    rewrite_lease_expiry(&shard, &task_ids[0], now_ms() - 1).await;

    let reaped = shard.reap_expired_leases().await.expect("full reap");

    assert_eq!(reaped, 1);
    assert!(!lease_exists(&shard, &task_ids[0]).await);

    let pass = shard
        .reap_due_leases_at(indexed_expiry_ms)
        .await
        .expect("pass at the indexed expiry");
    assert_eq!(
        pass,
        LeaseReapPass {
            checked: 1,
            ..Default::default()
        }
    );
    assert_eq!(shard.tracked_lease_count(), 0);
}

fn reaper_metric(metrics: &silo::metrics::Metrics, name: &str) -> f64 {
    metric_value_or_zero(&gather_metrics_text(metrics), &[name, "shard=\""])
}

/// Work for a key that still has work queued or running is not started
/// again until that work finishes.
#[silo::test]
async fn the_pool_runs_one_piece_of_work_per_key() {
    let mut pool = KeyedTaskPool::new(4);
    let gate = Arc::new(tokio::sync::Notify::new());
    let held = Arc::clone(&gate);

    assert!(pool.spawn("a", async move { held.notified().await }));
    assert!(!pool.spawn("a", async {}), "work for the key is in flight");

    gate.notify_one();
    assert_eq!(pool.join_next().await, Some("a"));
    assert!(pool.spawn("a", async {}), "the key is free again");
    assert_eq!(pool.join_next().await, Some("a"));
    assert_eq!(pool.join_next().await, None);
}

#[silo::test]
async fn the_pool_bounds_how_much_work_runs_at_once() {
    let mut pool = KeyedTaskPool::new(2);
    let running = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    for key in 0..6 {
        let (running, most) = (Arc::clone(&running), Arc::clone(&most));
        pool.spawn(key, async move {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            most.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            running.fetch_sub(1, Ordering::SeqCst);
        });
    }

    pool.drain().await;

    assert_eq!(most.load(Ordering::SeqCst), 2);
}

#[silo::test]
async fn slow_work_for_one_key_does_not_hold_back_another() {
    let mut pool = KeyedTaskPool::new(2);
    let gate = Arc::new(tokio::sync::Notify::new());
    let held = Arc::clone(&gate);
    pool.spawn("slow", async move { held.notified().await });
    pool.spawn("fast", async {});

    let finished = tokio::time::timeout(Duration::from_secs(5), pool.join_next())
        .await
        .expect("the fast key finishes while the slow one is held");
    assert_eq!(finished, Some("fast"));

    gate.notify_one();
    assert_eq!(pool.join_next().await, Some("slow"));
}

#[silo::test]
async fn work_that_panics_frees_its_key() {
    let mut pool = KeyedTaskPool::new(1);
    pool.spawn("a", async { panic!("work failed") });

    assert_eq!(pool.join_next().await, Some("a"));
    assert!(pool.spawn("a", async {}));
    assert_eq!(pool.join_next().await, Some("a"));
}

/// After the seeding scan, ticks on a shard with nothing due read nothing:
/// the scan counter stays where the seeding scan left it.
#[silo::test]
async fn ticks_with_nothing_due_record_no_scans() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let shards = HashMap::from([(ShardId::new(), Arc::clone(&shard))]);
    let mut reaper = LeaseReaper::new(Some(metrics.clone()));
    let now = now_ms();

    reaper.tick(&shards, now);
    reaper.drain().await;
    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_scans_total"),
        1.0
    );

    lease_jobs(&shard, "idle", 3).await;
    for second in 1..=5 {
        reaper.tick(&shards, now + second * 1_000);
        reaper.drain().await;
    }

    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_scans_total"),
        1.0
    );
    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_due_checks_total"),
        0.0
    );
    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_tracked_leases"),
        3.0
    );
}

/// A tick past a lease's expiry reaps it by key and records the pass.
#[silo::test]
async fn a_tick_past_an_expiry_reaps_and_records_the_pass() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let shards = HashMap::from([(ShardId::new(), Arc::clone(&shard))]);
    let mut reaper = LeaseReaper::new(Some(metrics.clone()));
    reaper.tick(&shards, now_ms());
    reaper.drain().await;
    let task_ids = lease_jobs(&shard, "crashed", 2).await;
    let mut latest_expiry_ms = 0;
    for task_id in &task_ids {
        latest_expiry_ms = latest_expiry_ms.max(lease_expiry_ms(&shard, task_id).await);
    }

    reaper.tick(&shards, latest_expiry_ms);
    reaper.drain().await;

    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_leases_reaped_total"),
        2.0
    );
    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_due_checks_total"),
        2.0
    );
    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_scans_total"),
        1.0
    );
    assert_eq!(count_lease_keys(shard.db()).await, 0);
}

/// A periodic full scan that finds a lease the index did not hold counts it.
#[silo::test]
async fn a_full_scan_tick_counts_untracked_leases() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let shards = HashMap::from([(ShardId::new(), Arc::clone(&shard))]);
    let mut reaper = LeaseReaper::new(Some(metrics.clone()));
    let now = now_ms();
    reaper.tick(&shards, now);
    reaper.drain().await;
    write_run_attempt_lease(
        &shard,
        "worker",
        "missed",
        "-",
        "job",
        "default",
        now + 10 * FULL_SCAN_INTERVAL_MS,
        now,
    )
    .await;

    reaper.tick(&shards, now + FULL_SCAN_INTERVAL_MS);
    reaper.drain().await;

    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_scans_total"),
        2.0
    );
    assert_eq!(
        reaper_metric(&metrics, "silo_lease_reaper_untracked_leases_total"),
        1.0
    );
}

/// A shard that is no longer open stops exporting a tracked-lease count.
#[silo::test]
async fn a_closed_shard_drops_its_tracked_lease_gauge() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let shards = HashMap::from([(ShardId::new(), Arc::clone(&shard))]);
    let mut reaper = LeaseReaper::new(Some(metrics.clone()));
    reaper.tick(&shards, now_ms());
    reaper.drain().await;
    assert!(gather_metrics_text(&metrics).contains("silo_lease_reaper_tracked_leases{"));

    reaper.tick(&HashMap::new(), now_ms());

    assert!(!gather_metrics_text(&metrics).contains("silo_lease_reaper_tracked_leases{"));
}
