//! A job that arrives at a concurrency queue with a free slot leaves the
//! slot to the jobs already waiting on the queue.

mod test_helpers;

use std::sync::Arc;
use std::time::Duration;

use silo::codec::encode_concurrency_action;
use silo::concurrency::GrantScannerConfig;
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::{concurrency_request_key, concurrency_requester_counter_key};
use silo::task::ConcurrencyAction;
use slatedb::WriteBatch;

use test_helpers::*;

const TENANT: &str = "-";
const TASK_GROUP: &str = "default";

fn limit(queue: &str, max_concurrency: u32) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency,
    })
}

async fn enqueue(shard: &JobStoreShard, limits: Vec<Limit>, start_at_ms: i64) -> String {
    shard
        .enqueue(
            TENANT,
            None,
            10,
            start_at_ms,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
            limits,
            None,
            TASK_GROUP,
        )
        .await
        .expect("enqueue")
}

/// Lease up to `count` ready tasks. Returns `(job_id, task_id)` pairs.
async fn lease(shard: &JobStoreShard, count: usize) -> Vec<(String, String)> {
    shard
        .dequeue("w", TASK_GROUP, count)
        .await
        .expect("dequeue")
        .tasks
        .iter()
        .map(|task| {
            (
                task.job().id().to_string(),
                task.attempt().task_id().to_string(),
            )
        })
        .collect()
}

async fn succeed(shard: &JobStoreShard, task_id: &str) {
    shard
        .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
}

async fn requester_count(shard: &JobStoreShard, queue: &str) -> i64 {
    shard
        .get_concurrency_requester_count(TENANT, queue)
        .await
        .expect("requester count")
}

/// Write a request row, and its requester counter increment, for a job that
/// starts an hour from now: a waiter no scanner pass grants.
async fn write_future_waiter(shard: &JobStoreShard, queue: &str) {
    let start_time_ms = now_ms() + 60 * 60 * 1000;
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority: 10,
        job_id: "future-job".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: TASK_GROUP.to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: "future-task".to_string(),
        limits: vec![limit(queue, 1)],
    };
    let mut batch = WriteBatch::new();
    batch.put(
        concurrency_request_key(TENANT, queue, start_time_ms, 10, "future-job", 1, "a"),
        encode_concurrency_action(&action),
    );
    batch.merge(
        concurrency_requester_counter_key(TENANT, queue),
        1i64.to_le_bytes(),
    );
    shard.db().write(batch).await.expect("write future waiter");
}

/// A shard whose scanner only runs when the test calls
/// `process_concurrency_grants`.
async fn open_shard_with_scanner_stopped() -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let (tmp, shard) = open_temp_shard_with_reconcile_interval_ms(60 * 60 * 1000).await;
    shard.stop_grant_scanner();
    (tmp, shard)
}

#[silo::test]
async fn arriving_job_leaves_a_free_slot_to_the_waiting_job() {
    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let queue = "yield-q";
    let now = now_ms();

    enqueue(&shard, vec![limit(queue, 1)], now).await;
    let holder = lease(&shard, 1).await;
    let waiter = enqueue(&shard, vec![limit(queue, 1)], now).await;

    // The slot frees and, before any scanner pass, another job arrives.
    succeed(&shard, &holder[0].1).await;
    let arrival = enqueue(&shard, vec![limit(queue, 1)], now_ms()).await;

    assert!(
        lease(&shard, 1).await.is_empty(),
        "the arrival must not take the slot ahead of the waiter"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
    assert_eq!(requester_count(&shard, queue).await, 2);

    shard.process_concurrency_grants(TENANT, queue, 1).await;
    let first = lease(&shard, 1).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, waiter);

    succeed(&shard, &first[0].1).await;
    shard.process_concurrency_grants(TENANT, queue, 1).await;
    let second = lease(&shard, 1).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, arrival);
}

#[silo::test]
async fn arriving_job_takes_a_free_slot_when_nothing_is_waiting() {
    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let queue = "no-waiters-q";

    enqueue(&shard, vec![limit(queue, 1)], now_ms()).await;
    let holder = lease(&shard, 1).await;
    succeed(&shard, &holder[0].1).await;

    let arrival = enqueue(&shard, vec![limit(queue, 1)], now_ms()).await;
    let leased = lease(&shard, 1).await;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].0, arrival);
    assert_eq!(requester_count(&shard, queue).await, 0);
}

#[silo::test]
async fn due_ticket_leaves_a_free_slot_to_the_waiting_job() {
    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let queue = "ticket-yield-q";
    let now = now_ms();

    enqueue(&shard, vec![limit(queue, 1)], now).await;
    let holder = lease(&shard, 1).await;
    let waiter = enqueue(&shard, vec![limit(queue, 1)], now).await;
    let scheduled = enqueue(&shard, vec![limit(queue, 1)], now + 300).await;

    succeed(&shard, &holder[0].1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The ticket is due and the slot is free, but a job is waiting.
    assert!(
        lease(&shard, 1).await.is_empty(),
        "the due ticket must not take the slot ahead of the waiter"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
    assert_eq!(requester_count(&shard, queue).await, 2);

    shard.process_concurrency_grants(TENANT, queue, 1).await;
    let first = lease(&shard, 1).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, waiter);

    succeed(&shard, &first[0].1).await;
    shard.process_concurrency_grants(TENANT, queue, 1).await;
    let second = lease(&shard, 1).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, scheduled);
}

#[silo::test]
async fn job_granted_one_queue_yields_at_the_next_queue_in_its_chain() {
    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let (first_queue, second_queue) = ("chain-first-q", "chain-second-q");
    let now = now_ms();

    // The second queue has a holder and a waiter; the first is untouched.
    enqueue(&shard, vec![limit(second_queue, 1)], now).await;
    let holder = lease(&shard, 1).await;
    let waiter = enqueue(&shard, vec![limit(second_queue, 1)], now).await;
    succeed(&shard, &holder[0].1).await;

    // The arrival is granted the first queue at once and reaches the second
    // while its slot is free.
    let arrival = enqueue(
        &shard,
        vec![limit(first_queue, 1), limit(second_queue, 1)],
        now_ms(),
    )
    .await;
    assert!(lease(&shard, 1).await.is_empty());
    assert_eq!(requester_count(&shard, second_queue).await, 2);

    shard
        .process_concurrency_grants(TENANT, second_queue, 1)
        .await;
    let first = lease(&shard, 1).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, waiter);

    succeed(&shard, &first[0].1).await;
    shard
        .process_concurrency_grants(TENANT, second_queue, 1)
        .await;
    let second = lease(&shard, 1).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, arrival);
}

#[silo::test]
async fn yielding_job_is_granted_by_the_running_scanner_without_another_release() {
    // A long reconcile interval leaves the post-commit nudge as the only
    // thing that can bring the scanner to the queue.
    let (_tmp, shard) = open_temp_shard_with_reconcile_interval_ms(60 * 60 * 1000).await;
    let queue = "nudge-q";

    // The only waiter is one the scanner cannot grant, and the slot is free.
    write_future_waiter(&shard, queue).await;

    let arrival = enqueue(&shard, vec![limit(queue, 1)], now_ms()).await;

    let mut leased = Vec::new();
    for _ in 0..100 {
        leased = lease(&shard, 1).await;
        if !leased.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(leased.len(), 1, "the scanner should grant the arrival");
    assert_eq!(leased[0].0, arrival);
    assert_eq!(requester_count(&shard, queue).await, 1);
}

#[silo::test]
async fn arriving_job_takes_a_slot_in_the_live_headroom_share() {
    // With half of each queue reserved for arrivals, the scanner fills one
    // of two slots and never the other.
    let (_tmp, shard, _metrics) = open_temp_shard_with_grant_scanner_config(GrantScannerConfig {
        live_headroom_fraction: 0.5,
        ..GrantScannerConfig::default()
    })
    .await;
    shard.stop_grant_scanner();
    let queue = "headroom-q";
    let now = now_ms();

    enqueue(&shard, vec![limit(queue, 2)], now).await;
    enqueue(&shard, vec![limit(queue, 2)], now).await;
    let holders = lease(&shard, 2).await;
    assert_eq!(holders.len(), 2);
    let waiter = enqueue(&shard, vec![limit(queue, 2)], now).await;

    // One slot frees. One holder remains, which is all the scanner may
    // fill, so the free slot is the arrivals' share.
    succeed(&shard, &holders[0].1).await;
    let arrival = enqueue(&shard, vec![limit(queue, 2)], now_ms()).await;

    let leased = lease(&shard, 1).await;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].0, arrival);
    assert_eq!(requester_count(&shard, queue).await, 1);
    let _ = waiter;
}
