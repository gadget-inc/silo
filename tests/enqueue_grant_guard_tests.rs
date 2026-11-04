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
