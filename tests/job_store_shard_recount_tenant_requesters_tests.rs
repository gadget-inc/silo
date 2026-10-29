//! Tests for `JobStoreShard::recount_tenant_requesters`, the shard operation
//! behind the console's "Recount requesters" action.

mod test_helpers;

use std::sync::Arc;

use silo::job::{ConcurrencyLimit, Limit};
use silo::keys::concurrency_requester_counter_key;
use silo::query::ShardQueryEngine;
use test_helpers::*;

/// Fill a single-slot queue with one holder and park `waiters` requests
/// behind it, so the queue's requester counter should read `waiters`.
async fn park_requests(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    waiters: usize,
) {
    let limit = || {
        vec![Limit::Concurrency(ConcurrencyLimit {
            key: queue.to_string(),
            max_concurrency: 1,
        })]
    };
    shard
        .enqueue(
            tenant,
            Some(format!("{queue}-holder")),
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({})),
            limit(),
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1, "holder should dequeue");
    for i in 0..waiters {
        shard
            .enqueue(
                tenant,
                Some(format!("{queue}-waiter-{i}")),
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({})),
                limit(),
                None,
                "default",
            )
            .await
            .expect("enqueue waiter");
    }
}

async fn drift_requester_counter(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    delta: i64,
) {
    shard
        .db()
        .merge(
            concurrency_requester_counter_key(tenant, queue),
            delta.to_le_bytes(),
        )
        .await
        .expect("drift counter");
}

async fn requester_count(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
) -> i64 {
    shard
        .get_concurrency_requester_count(tenant, queue)
        .await
        .expect("get requester counter")
}

/// Recounts every queue under the tenant, reports how many it visited and how
/// many it changed, and leaves other tenants alone.
#[silo::test]
async fn recount_tenant_requesters_corrects_every_queue_of_the_tenant() {
    let (_tmp, shard) = open_temp_shard().await;

    park_requests(&shard, "tenant-a", "qa", 2).await;
    park_requests(&shard, "tenant-a", "qb", 3).await;
    park_requests(&shard, "tenant-a", "qc", 1).await;
    park_requests(&shard, "tenant-b", "qd", 2).await;

    drift_requester_counter(&shard, "tenant-a", "qa", -5).await;
    drift_requester_counter(&shard, "tenant-a", "qb", 4).await;
    drift_requester_counter(&shard, "tenant-b", "qd", -3).await;

    let stats = shard
        .recount_tenant_requesters("tenant-a")
        .await
        .expect("recount tenant");
    assert_eq!(stats.queues_scanned, 3);
    assert_eq!(stats.queues_corrected, 2);

    assert_eq!(requester_count(&shard, "tenant-a", "qa").await, 2);
    assert_eq!(requester_count(&shard, "tenant-a", "qb").await, 3);
    assert_eq!(requester_count(&shard, "tenant-a", "qc").await, 1);
    assert_eq!(
        requester_count(&shard, "tenant-b", "qd").await,
        -1,
        "another tenant's counter is untouched"
    );

    let again = shard
        .recount_tenant_requesters("tenant-a")
        .await
        .expect("recount tenant again");
    assert_eq!(again.queues_scanned, 3);
    assert_eq!(again.queues_corrected, 0);
}

/// A tenant with no requester counter rows scans and corrects nothing.
#[silo::test]
async fn recount_tenant_requesters_with_no_queues_is_a_noop() {
    let (_tmp, shard) = open_temp_shard().await;
    let stats = shard
        .recount_tenant_requesters("nobody")
        .await
        .expect("recount tenant");
    assert_eq!(stats.queues_scanned, 0);
    assert_eq!(stats.queues_corrected, 0);
}

/// `queue_counts` hides a counter at or below zero. The action restores the
/// true requester count for the tenant's queue.
#[silo::test]
async fn queue_counts_reports_corrected_requesters_after_tenant_recount() {
    let (_tmp, shard) = open_temp_shard().await;
    park_requests(&shard, "-", "recount-q", 4).await;
    drift_requester_counter(&shard, "-", "recount-q", -9).await;
    assert_eq!(requester_count(&shard, "-", "recount-q").await, -5);

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    assert_eq!(
        queue_counts_requesters(&sql, "recount-q").await,
        0,
        "a negative counter reads as zero before the recount"
    );

    let stats = shard
        .recount_tenant_requesters("-")
        .await
        .expect("recount tenant");
    assert_eq!(stats.queues_corrected, 1);

    assert_eq!(queue_counts_requesters(&sql, "recount-q").await, 4);
}

/// The `requesters` column of `queue_counts` for one queue of tenant `-`,
/// or 0 when the table has no row for it.
async fn queue_counts_requesters(sql: &ShardQueryEngine, queue: &str) -> i64 {
    let batches = sql
        .sql(&format!(
            "SELECT requesters FROM queue_counts WHERE tenant = '-' AND queue_name = '{queue}'"
        ))
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("requesters column")
                .value(0)
        })
        .next()
        .unwrap_or(0)
}
