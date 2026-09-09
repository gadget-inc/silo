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

fn read_revived_total(metrics: &silo::metrics::Metrics) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_broker_scan_tasks_read_total", "outcome=\"revived\""],
    )
}

/// A refresh task row the broker acked and tombstoned is still durable under
/// the same bytes. With a revive bound of one scan generation, driving scans
/// through dequeue wakeups must lease it again without a process restart.
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
    let first =
        dequeue_refresh_tasks_until(&shard, "worker-1", "default", 1, Duration::from_secs(30))
            .await
            .refresh_tasks[0]
            .task_id
            .clone();
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

    let second =
        dequeue_refresh_tasks_until(&shard, "worker-2", "default", 1, Duration::from_secs(30))
            .await
            .refresh_tasks[0]
            .task_id
            .clone();
    assert_eq!(second, first, "the revived row is the same task");
    assert_eq!(read_revived_total(&metrics), 1.0);
    assert!(
        find_refresh_task_row(&shard).await.is_none(),
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
