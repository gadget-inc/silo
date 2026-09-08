//! Tests for bounded ack-tombstone suppression in the task broker.
//!
//! After a dequeue durably deletes a task row, the broker keeps an in-memory
//! tombstone so a stale scan cannot re-buffer the row. A row that is still
//! durable in the DB must not stay hidden behind that tombstone forever: once
//! the key has been re-observed past the revive bound, the broker point-reads
//! it and, if present, buffers it again. A key whose row is truly gone stays
//! suppressed.

mod test_helpers;

use std::time::Duration;

use silo::codec::decode_task_validated;
use silo::job_store_shard::JobStoreShard;
use test_helpers::*;

/// Scan the durable task rows and return the first RefreshFloatingLimit row.
async fn find_refresh_task_row(shard: &JobStoreShard) -> Option<(Vec<u8>, bytes::Bytes)> {
    let start = silo::keys::tasks_prefix();
    let end = silo::keys::end_bound(&start);
    let mut iter = shard
        .db()
        .scan::<Vec<u8>, _>(start..end)
        .await
        .expect("scan tasks");
    while let Some(kv) = iter.next().await.expect("iterate tasks") {
        let decoded = decode_task_validated(kv.value.clone()).expect("decode task");
        if decoded.as_refresh_floating_limit().is_some() {
            return Some((kv.key.to_vec(), kv.value));
        }
    }
    None
}

async fn enqueue_floating(shard: &JobStoreShard, queue: &str, tag: u32) {
    shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"j": tag})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.to_string(),
                    default_max_concurrency: 1,
                    refresh_interval_ms: 100,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue floating job");
}

/// Each dequeue against an empty buffer wakes the broker scanner, so repeated
/// dequeues advance scan generations without waiting out the scanner's backoff.
async fn dequeue_until_refresh_task_leased(
    shard: &JobStoreShard,
    attempts: usize,
) -> Option<String> {
    for _ in 0..attempts {
        let result = shard
            .dequeue("worker-2", "default", 10)
            .await
            .expect("dequeue");
        if let Some(task) = result.refresh_tasks.first() {
            return Some(task.task_id.clone());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

fn read_scan_outcome_total(metrics: &silo::metrics::Metrics, outcome: &str) -> f64 {
    metrics
        .registry()
        .gather()
        .into_iter()
        .find(|f| f.get_name() == "silo_broker_scan_tasks_read_total")
        .map(|f| {
            f.get_metric()
                .iter()
                .filter(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.get_name() == "outcome" && l.get_value() == outcome)
                })
                .map(|m| m.get_counter().get_value())
                .sum()
        })
        .unwrap_or(0.0)
}

/// The env-621258 shape: a refresh task row the broker acked and tombstoned
/// is still durable under the same bytes. With a revive bound of one scan
/// generation, driving scans through dequeue wakeups must lease it again
/// without a process restart.
#[silo::test]
async fn tombstoned_task_row_still_durable_is_revived_and_leased() {
    let (_tmp, shard, metrics) = open_temp_shard_with_tombstone_revive_after_generations(1).await;
    let queue = "tombstone-revive-q";

    // A holder plus a waiter schedules a refresh task row.
    enqueue_floating(&shard, queue, 1).await;
    enqueue_floating(&shard, queue, 2).await;
    let (row_key, row_value) = find_refresh_task_row(&shard)
        .await
        .expect("refresh task row is durable before dequeue");

    // Dequeue acks the row: the key is deleted and tombstoned in the broker.
    let first = dequeue_until_refresh_task_leased(&shard, 50)
        .await
        .expect("refresh task leased once");
    shard
        .report_refresh_success(&first, 1)
        .await
        .expect("report refresh success");
    assert!(
        find_refresh_task_row(&shard).await.is_none(),
        "dequeue deletes the refresh task row"
    );

    // The row comes back under the exact same bytes.
    shard
        .db()
        .put(&row_key, &row_value)
        .await
        .expect("put row back");
    shard.db().flush().await.expect("flush");

    let second = dequeue_until_refresh_task_leased(&shard, 200).await;
    assert!(
        second.is_some(),
        "a durable row re-observed past the revive bound must be leased again"
    );
    assert_eq!(second.as_deref(), Some(first.as_str()));
    assert_eq!(read_scan_outcome_total(&metrics, "revived"), 1.0);
    assert!(
        find_refresh_task_row(&shard).await.is_none(),
        "the revived row is deleted by its second dequeue"
    );
}

/// A key that was acked and whose row is truly gone stays suppressed: no
/// phantom lease, no phantom buffer entry, and no revival.
#[silo::test]
async fn tombstoned_task_row_that_is_gone_is_never_revived() {
    let (_tmp, shard, metrics) = open_temp_shard_with_tombstone_revive_after_generations(1).await;

    shard
        .enqueue(
            "-",
            Some("gone-job".to_string()),
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"j": 1})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let leased = poll_until(
        || async {
            shard
                .dequeue("worker-1", "default", 10)
                .await
                .expect("dequeue")
                .tasks
                .len()
        },
        |&n| n == 1,
        5000,
    )
    .await;
    assert_eq!(leased, 1);
    assert_eq!(count_task_keys(shard.db()).await, 0);

    // Drive scan generations well past the bound.
    for _ in 0..20 {
        let result = shard
            .dequeue("worker-2", "default", 10)
            .await
            .expect("dequeue");
        assert!(
            result.tasks.is_empty(),
            "a deleted row must never be leased"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(shard.broker_buffer_len(), 0);
    assert_eq!(read_scan_outcome_total(&metrics, "revived"), 0.0);
    assert_eq!(
        count_lease_keys(shard.db()).await,
        1,
        "only the real lease exists"
    );
}
