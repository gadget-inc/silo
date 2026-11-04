//! Tests that the enqueue path releases its in-memory concurrency
//! reservations on every exit: a chain error after an immediate grant rolls
//! the reservation back, a dropped enqueue future hands it to the holder
//! reconciler, and a completed enqueue leaves nothing for the reconciler.

mod test_helpers;

use std::sync::Arc;

use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use silo::job_store_shard::{FloatingWaiterScanHook, JobStoreShard, JobStoreShardError};

use test_helpers::*;

const TENANT: &str = "-";
const TASK_GROUP: &str = "default";

/// A floating limit whose state row does not exist yet reads as never
/// refreshed, so the first enqueue against it finds a refresh due and runs
/// the waiter scan after its immediate grant.
fn fc_limit(queue: &str, default_max: u32) -> Limit {
    Limit::FloatingConcurrency(FloatingConcurrencyLimit {
        key: queue.to_string(),
        default_max_concurrency: default_max,
        refresh_interval_ms: 60_000,
        metadata: vec![],
    })
}

fn conc_limit(queue: &str, max: u32) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: max,
    })
}

/// A hook whose scan fails with a storage-shaped error.
fn failing_scan_hook() -> FloatingWaiterScanHook {
    Arc::new(|| {
        Box::pin(async {
            Err(JobStoreShardError::Codec(
                "injected waiter scan failure".to_string(),
            ))
        })
    })
}

/// A hook that reports when the chain reaches the waiter scan and then
/// never resolves, parking the enqueue future with its reservation held and
/// nothing durable written.
fn parking_scan_hook(reached: Arc<tokio::sync::Notify>) -> FloatingWaiterScanHook {
    Arc::new(move || {
        let reached = Arc::clone(&reached);
        Box::pin(async move {
            reached.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        })
    })
}

/// A hook that reports when the chain reaches the waiter scan and parks
/// there until `release` is notified, then lets the chain continue.
fn gated_scan_hook(
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
) -> FloatingWaiterScanHook {
    Arc::new(move || {
        let reached = Arc::clone(&reached);
        let release = Arc::clone(&release);
        Box::pin(async move {
            reached.notify_one();
            release.notified().await;
            Ok(())
        })
    })
}

async fn enqueue_with_limit(
    shard: &JobStoreShard,
    id: Option<String>,
    limit: Limit,
) -> Result<String, JobStoreShardError> {
    shard
        .enqueue(
            TENANT,
            id,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"j": 1})),
            vec![limit],
            None,
            TASK_GROUP,
        )
        .await
}

async fn assert_waiter_scan_error_rolls_back_grant(id: Option<String>) {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-scan-error-q";
    assert_eq!(shard.concurrency_holder_count(TENANT, queue), 0);

    shard.set_floating_waiter_scan_hook_for_test(Some(failing_scan_hook()));
    let err = enqueue_with_limit(&shard, id, fc_limit(queue, 1))
        .await
        .expect_err("the injected scan failure must fail the enqueue");
    assert!(
        matches!(err, JobStoreShardError::Codec(ref m) if m == "injected waiter scan failure"),
        "unexpected error: {err:?}"
    );
    shard.set_floating_waiter_scan_hook_for_test(None);

    assert_eq!(
        shard.concurrency_holder_count(TENANT, queue),
        0,
        "the reservation made before the failed waiter scan must be rolled back"
    );
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "nothing durable is written by a failed enqueue"
    );
    assert_eq!(
        shard.pending_holder_reconciliation_count_for_test(),
        0,
        "a rolled-back reservation needs no reconciliation"
    );

    // The slot is free again: a second enqueue on the same queue is granted
    // immediately and leasable.
    let job_id = enqueue_with_limit(&shard, None, fc_limit(queue, 1))
        .await
        .expect("enqueue after the failed one");
    assert_eq!(shard.concurrency_holder_count(TENANT, queue), 1);
    let leased = shard
        .dequeue("w1", TASK_GROUP, 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job().id(), job_id);
}

#[silo::test]
async fn waiter_scan_error_rolls_back_grant_on_batch_path() {
    assert_waiter_scan_error_rolls_back_grant(None).await;
}

#[silo::test]
async fn waiter_scan_error_rolls_back_grant_on_txn_path() {
    assert_waiter_scan_error_rolls_back_grant(Some("explicit-scan-error".to_string())).await;
}

async fn assert_dropped_enqueue_reservation_is_reconciled(id: Option<String>) {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-dropped-enqueue-q";

    let reached = Arc::new(tokio::sync::Notify::new());
    shard.set_floating_waiter_scan_hook_for_test(Some(parking_scan_hook(Arc::clone(&reached))));

    // Run the enqueue on its own task so it can be cancelled from here the
    // way a dropped RPC cancels the handler future.
    let enqueue = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move { enqueue_with_limit(&shard, id, fc_limit(queue, 1)).await })
    };
    reached.notified().await;
    assert_eq!(
        shard.concurrency_holder_count(TENANT, queue),
        1,
        "the chain holds an in-memory reservation while parked in the waiter scan"
    );
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "no durable holder exists before the batch commits"
    );

    enqueue.abort();
    let join = enqueue.await;
    assert!(
        join.as_ref().is_err_and(|e| e.is_cancelled()),
        "the enqueue task should have been cancelled, got {join:?}"
    );
    shard.set_floating_waiter_scan_hook_for_test(None);

    // The guard's Drop queued the reservation for the reconciler. The
    // reactive reconciler task may already have drained it; driving a pass
    // here and then waiting covers both orderings.
    shard.reconcile_pending_holders_for_test().await;
    let in_memory = poll_until(
        || async { shard.concurrency_holder_count(TENANT, queue) },
        |n| *n == 0,
        5_000,
    )
    .await;
    assert_eq!(
        in_memory, 0,
        "the reconciler must release the reservation the dropped enqueue left behind"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
    assert_eq!(shard.pending_holder_reconciliation_count_for_test(), 0);

    // The slot is usable again: a fresh job on the same max=1 queue is
    // granted immediately and leasable.
    let job_id = enqueue_with_limit(&shard, None, fc_limit(queue, 1))
        .await
        .expect("enqueue after the cancelled one");
    assert_eq!(shard.concurrency_holder_count(TENANT, queue), 1);
    let leased = shard
        .dequeue("w1", TASK_GROUP, 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job().id(), job_id);
}

#[silo::test]
async fn dropped_enqueue_reservation_is_reconciled_on_batch_path() {
    assert_dropped_enqueue_reservation_is_reconciled(None).await;
}

#[silo::test]
async fn dropped_enqueue_reservation_is_reconciled_on_txn_path() {
    assert_dropped_enqueue_reservation_is_reconciled(Some("explicit-dropped".to_string())).await;
}

async fn assert_completed_enqueue_leaves_nothing_pending(id: Option<String>, limit: Limit) {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = match &limit {
        Limit::Concurrency(cl) => cl.key.clone(),
        Limit::FloatingConcurrency(fl) => fl.key.clone(),
        Limit::RateLimit(_) => unreachable!("tests use concurrency limits"),
    };

    enqueue_with_limit(&shard, id, limit)
        .await
        .expect("enqueue");

    assert_eq!(
        shard.concurrency_holder_count(TENANT, &queue),
        1,
        "the immediate grant stays reserved after the commit"
    );
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "the committed batch wrote the holder row"
    );
    assert_eq!(
        shard.pending_holder_reconciliation_count_for_test(),
        0,
        "a completed enqueue queues nothing for the reconciler"
    );
}

#[silo::test]
async fn completed_enqueue_leaves_nothing_pending_on_batch_path() {
    assert_completed_enqueue_leaves_nothing_pending(None, conc_limit("c-complete-q", 1)).await;
    assert_completed_enqueue_leaves_nothing_pending(None, fc_limit("fl-complete-q", 1)).await;
}

#[silo::test]
async fn completed_enqueue_leaves_nothing_pending_on_txn_path() {
    assert_completed_enqueue_leaves_nothing_pending(
        Some("explicit-complete-c".to_string()),
        conc_limit("c-complete-q", 1),
    )
    .await;
    assert_completed_enqueue_leaves_nothing_pending(
        Some("explicit-complete-fl".to_string()),
        fc_limit("fl-complete-q", 1),
    )
    .await;
}

/// Park an enqueue inside its waiter scan with its reservation held so the
/// drift pass can run against it. Returns the parked task and the gate that
/// lets it continue to its commit.
async fn park_enqueue_with_reservation(
    shard: &Arc<JobStoreShard>,
    queue: &'static str,
) -> (
    tokio::task::JoinHandle<Result<String, JobStoreShardError>>,
    Arc<tokio::sync::Notify>,
) {
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    shard.set_floating_waiter_scan_hook_for_test(Some(gated_scan_hook(
        Arc::clone(&reached),
        Arc::clone(&release),
    )));
    let enqueue = {
        let shard = Arc::clone(shard);
        tokio::spawn(async move { enqueue_with_limit(&shard, None, fc_limit(queue, 1)).await })
    };
    reached.notified().await;
    assert_eq!(shard.concurrency_holder_count(TENANT, queue), 1);
    (enqueue, release)
}

async fn run_two_drift_passes(shard: &JobStoreShard) {
    for _ in 0..2 {
        shard.report_holder_drift_for_test().await;
        shard.reconcile_pending_holders_for_test().await;
    }
}

/// A reservation with no durable row that is seen by two consecutive drift
/// passes is released as a ghost. If its chain was merely slow and commits
/// afterwards, the commit hands the holder to the reconciler, which puts it
/// back, so the in-memory set matches the durable row and the queue admits
/// exactly its capacity.
#[silo::test]
async fn drift_release_of_slow_reservation_is_undone_by_its_commit() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-slow-reservation-q";
    let (enqueue, release) = park_enqueue_with_reservation(&shard, queue).await;

    run_two_drift_passes(&shard).await;
    assert_eq!(
        shard.concurrency_holder_count(TENANT, queue),
        0,
        "a reservation with no durable row is released after two drift passes"
    );

    release.notify_one();
    let job_id = enqueue
        .await
        .expect("enqueue task joins")
        .expect("the slow enqueue commits");
    shard.set_floating_waiter_scan_hook_for_test(None);
    // The commit found its holder gone and handed it to the reconciler,
    // which puts it back because the row now exists. The reactive task may
    // already have run; driving a pass here and waiting covers both.
    shard.reconcile_pending_holders_for_test().await;
    let in_memory = poll_until(
        || async { shard.concurrency_holder_count(TENANT, queue) },
        |n| *n == 1,
        5_000,
    )
    .await;
    assert_eq!(
        in_memory, 1,
        "the commit restores the in-memory holder the drift pass released"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, 1);

    // Capacity is honoured again: a second job on the max=1 queue waits.
    enqueue_with_limit(&shard, None, fc_limit(queue, 1))
        .await
        .expect("enqueue second job");
    assert_eq!(shard.concurrency_holder_count(TENANT, queue), 1);
    let leased = shard
        .dequeue("w1", TASK_GROUP, 2)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job().id(), job_id);
}

/// Seed a durable holder row and hydrate the queue around it, so later
/// reconciles compare against a hydrated in-memory set.
async fn hydrate_queue_with_seed(shard: &JobStoreShard, queue: &str) {
    let mut batch = slatedb::WriteBatch::new();
    batch.put(
        silo::keys::concurrency_holder_key(TENANT, queue, "seed"),
        silo::codec::encode_holder(&silo::task::HolderRecord {
            granted_at_ms: now_ms(),
            job_id: None,
            attempt_number: None,
        }),
    );
    shard.db().write(batch).await.expect("seed write");
    shard.db().flush().await.expect("flush seed");
    shard.request_concurrency_reconciliation(TENANT.to_string(), queue.to_string(), "seed".into());
    shard.reconcile_pending_holders_for_test().await;
    assert!(shard.concurrency_contains_holder(TENANT, queue, "seed"));
}

/// A grant whose holder is still in memory at commit needs nothing from the
/// reconciler.
#[silo::test]
async fn confirm_leaves_present_holder_alone() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "confirm-present-q";
    hydrate_queue_with_seed(&shard, queue).await;
    shard.concurrency_insert_holder(TENANT, queue, "live");

    shard.concurrency_confirm_grant_for_test(TENANT, queue, "live");
    assert_eq!(shard.pending_holder_reconciliation_count_for_test(), 0);
    assert!(shard.concurrency_contains_holder(TENANT, queue, "live"));
}

/// A holder released by a cancel or outcome between the batch landing and
/// its confirmation has no durable row any more, so confirming it must not
/// bring it back.
#[silo::test]
async fn confirm_does_not_revive_holder_released_with_its_row() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "confirm-released-q";
    hydrate_queue_with_seed(&shard, queue).await;

    shard.concurrency_confirm_grant_for_test(TENANT, queue, "gone");
    shard.reconcile_pending_holders_for_test().await;
    let in_memory = poll_until(
        || async { shard.concurrency_contains_holder(TENANT, queue, "gone") },
        |present| !*present,
        1_000,
    )
    .await;
    assert!(!in_memory, "a holder with no durable row stays released");
    assert_eq!(shard.pending_holder_reconciliation_count_for_test(), 0);
}

/// A holder the drift pass released while its batch was in flight still
/// has its durable row once the batch lands, so confirming it brings it
/// back through the reconciler.
#[silo::test]
async fn confirm_restores_holder_whose_row_landed() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "confirm-landed-q";
    hydrate_queue_with_seed(&shard, queue).await;
    let mut batch = slatedb::WriteBatch::new();
    batch.put(
        silo::keys::concurrency_holder_key(TENANT, queue, "landed"),
        silo::codec::encode_holder(&silo::task::HolderRecord {
            granted_at_ms: now_ms(),
            job_id: None,
            attempt_number: None,
        }),
    );
    shard.db().write(batch).await.expect("holder write");

    shard.concurrency_confirm_grant_for_test(TENANT, queue, "landed");
    shard.reconcile_pending_holders_for_test().await;
    let in_memory = poll_until(
        || async { shard.concurrency_contains_holder(TENANT, queue, "landed") },
        |present| *present,
        5_000,
    )
    .await;
    assert!(in_memory, "a holder whose row exists is put back");
}
