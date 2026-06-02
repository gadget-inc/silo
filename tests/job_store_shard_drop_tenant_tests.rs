mod test_helpers;

use silo::job::{ConcurrencyLimit, Limit};
use test_helpers::*;

/// Enqueue a job for `tenant` with a concurrency limit on `queue` (max 1). The
/// immediate grant writes a concurrency holder and a queued `RunAttempt` task.
async fn enqueue_with_holder(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    now: i64,
) -> String {
    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            payload,
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

/// Drops both a queued and a running (leased) run attempt plus their holders for
/// one tenant, performs a clean in-memory release, and leaves another tenant's
/// state untouched.
#[silo::test]
async fn drop_tenant_clears_holders_runattempts_and_leases() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // tenant-a: two queues, each gets a holder + a queued RunAttempt task.
    enqueue_with_holder(&shard, "tenant-a", "qA", now).await;
    enqueue_with_holder(&shard, "tenant-a", "qB", now).await;

    // tenant-b: one holder + queued RunAttempt — must survive the drop.
    enqueue_with_holder(&shard, "tenant-b", "qC", now).await;

    // Lease one of tenant-a's tasks so it becomes a *running* (leased) attempt:
    // its TASK entry is removed and a lease (embedding the RunAttempt) is written.
    let leased = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1, "exactly one task leased");

    // Pre-conditions: tenant-a has 2 holders; tenant-b has 1.
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-a").await,
        2
    );
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-b").await,
        1
    );
    assert!(count_lease_keys(shard.db()).await >= 1, "a lease exists");

    // Act.
    let stats = shard
        .drop_tenant_holders_and_runattempts("tenant-a")
        .await
        .expect("drop tenant state");

    // Both holders and both run attempts (one queued, one leased) were dropped.
    assert_eq!(stats.holders_dropped, 2);
    assert_eq!(stats.run_attempts_dropped, 2);

    // tenant-a is fully cleared, in durable storage and in-memory counts.
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-a").await,
        0
    );
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qB"), 0);

    // tenant-b is untouched.
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-b").await,
        1
    );
    assert_eq!(shard.concurrency_holder_count("tenant-b", "qC"), 1);
}

/// Dropping a tenant with no holders/run attempts is a no-op that returns zeros.
#[silo::test]
async fn drop_tenant_with_no_state_is_a_noop() {
    let (_tmp, shard) = open_temp_shard().await;

    let stats = shard
        .drop_tenant_holders_and_runattempts("ghost")
        .await
        .expect("drop tenant state");

    assert_eq!(stats.holders_dropped, 0);
    assert_eq!(stats.run_attempts_dropped, 0);
}
