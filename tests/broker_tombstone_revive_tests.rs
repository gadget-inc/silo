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

use silo::job_store_shard::JobStoreShard;
use test_helpers::*;

/// The first durable task row in the task line.
async fn find_task_row(shard: &JobStoreShard) -> Option<(Vec<u8>, bytes::Bytes)> {
    first_task_kv(shard.db()).await
}

fn read_revived_total(metrics: &silo::metrics::Metrics) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_broker_scan_tasks_read_total", "outcome=\"revived\""],
    )
}

/// A RunAttempt row the broker acked and tombstoned is still durable under
/// the same bytes. With a revive bound of one scan generation, driving scans
/// through dequeue wakeups must lease it again without a process restart.
/// The same worker leases both times: a still-live lease held by the same
/// worker is re-delivered by overwrite, the requeue-after-ambiguous-commit
/// recovery.
#[silo::test]
async fn tombstoned_task_row_still_durable_is_revived_and_leased() {
    let (_tmp, shard, metrics) = open_temp_shard_with_tombstone_revive_after_generations(1).await;

    shard
        .enqueue(
            "-",
            Some("revive-job".to_string()),
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
    let (row_key, row_value) = find_task_row(&shard)
        .await
        .expect("task row is durable before dequeue");

    // Dequeue acks the row: the key is deleted and tombstoned in the broker.
    let first = dequeue_task_ids_until(&shard, "worker-1", "default", 1).await;
    assert_eq!(first.len(), 1);
    assert!(
        find_task_row(&shard).await.is_none(),
        "dequeue deletes the task row"
    );

    // The row comes back under the exact same bytes.
    shard
        .db()
        .put(&row_key, &row_value)
        .await
        .expect("put row back");
    shard.db().flush().await.expect("flush");

    let second = dequeue_task_ids_until(&shard, "worker-1", "default", 1).await;
    assert_eq!(second, first, "the revived row is the same task");
    assert_eq!(read_revived_total(&metrics), 1.0);
    assert!(
        find_task_row(&shard).await.is_none(),
        "the revived row is deleted by its second dequeue"
    );
}

/// A durably deleted task row is never re-leased and leaves no buffer entry
/// while its tombstone ages past the revive bound.
#[silo::test]
async fn durably_deleted_row_is_not_re_leased_and_leaves_no_buffer_entry() {
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

    let leased = dequeue_task_ids_until(&shard, "worker-1", "default", 1).await;
    assert_eq!(leased.len(), 1);
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
    assert_eq!(read_revived_total(&metrics), 0.0);
    assert_eq!(
        count_lease_keys(shard.db()).await,
        1,
        "only the real lease exists"
    );
}
