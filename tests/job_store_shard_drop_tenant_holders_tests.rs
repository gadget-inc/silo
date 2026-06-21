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

/// Drops every concurrency holder for one tenant (both a queued and a running
/// holder), performs a clean in-memory release, and leaves another tenant's
/// holders untouched. The run attempts themselves are NOT dropped.
#[silo::test]
async fn drop_tenant_clears_holders_and_preserves_runattempts() {
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

    // Pre-conditions: tenant-a has 2 holders; tenant-b has 1. One task is leased,
    // so one queued TASK entry and one lease exist for tenant-a.
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-a").await,
        2
    );
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-b").await,
        1
    );
    let leases_before = count_lease_keys(shard.db()).await;
    let tasks_before = count_task_keys(shard.db()).await;
    assert!(leases_before >= 1, "a lease exists");

    // Act.
    let stats = shard
        .drop_tenant_holders("tenant-a")
        .await
        .expect("drop tenant holders");

    // Both of tenant-a's holders were dropped.
    assert_eq!(stats.holders_dropped, 2);

    // tenant-a's holders are fully cleared, in durable storage and in-memory.
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-a").await,
        0
    );
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qB"), 0);

    // The run attempts themselves survive: leases and queued TASK entries are
    // untouched (only holders were freed).
    assert_eq!(
        count_lease_keys(shard.db()).await,
        leases_before,
        "leases must survive a holder drop"
    );
    assert_eq!(
        count_task_keys(shard.db()).await,
        tasks_before,
        "queued tasks must survive a holder drop"
    );

    // tenant-b is untouched.
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "tenant-b").await,
        1
    );
    assert_eq!(shard.concurrency_holder_count("tenant-b", "qC"), 1);
}

/// Dropping a tenant's holders must leave a *running* (leased) job exactly where
/// it is: still `Running`, with its lease intact. The attempt keeps running to
/// completion; only the held slot is freed.
#[silo::test]
async fn drop_tenant_leaves_running_job_running() {
    use silo::job::JobStatusKind;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = enqueue_with_holder(&shard, "tenant-a", "qA", now).await;

    // Lease it so the job is Running, backed by a durable lease.
    let leased = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1, "exactly one task leased");
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Running,
        "job is Running pre-drop"
    );
    assert!(count_lease_keys(shard.db()).await >= 1, "a lease exists");

    // Act.
    let stats = shard
        .drop_tenant_holders("tenant-a")
        .await
        .expect("drop tenant holders");
    assert_eq!(stats.holders_dropped, 1);

    // The job stays Running with its lease intact — the attempt is not finalized.
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Running,
        "running job must stay Running after a holder drop, got {:?}",
        status.kind
    );
    assert!(
        count_lease_keys(shard.db()).await >= 1,
        "the lease must survive"
    );
    // The held slot is freed.
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
}

/// Dropping a tenant's holders must leave a *queued* (Scheduled, not yet leased)
/// job exactly where it is: still `Scheduled`, with its queued task intact. Only
/// the held slot is freed.
#[silo::test]
async fn drop_tenant_leaves_queued_job_scheduled() {
    use silo::job::JobStatusKind;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue but do NOT lease — the job stays Scheduled with a queued RunAttempt.
    let job_id = enqueue_with_holder(&shard, "tenant-a", "qA", now).await;
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Scheduled,
        "job is Scheduled pre-drop"
    );
    let tasks_before = count_task_keys(shard.db()).await;
    assert!(tasks_before >= 1, "a queued task exists");

    // Act.
    let stats = shard
        .drop_tenant_holders("tenant-a")
        .await
        .expect("drop tenant holders");
    assert_eq!(stats.holders_dropped, 1);

    // The queued job stays Scheduled with its task intact — not cancelled.
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Scheduled,
        "queued job must stay Scheduled after a holder drop, got {:?}",
        status.kind
    );
    assert_eq!(
        count_task_keys(shard.db()).await,
        tasks_before,
        "the queued task must survive"
    );
    // The held slot is freed.
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
}

/// Dropping a tenant with no holders is a no-op that returns zero.
#[silo::test]
async fn drop_tenant_with_no_state_is_a_noop() {
    let (_tmp, shard) = open_temp_shard().await;

    let stats = shard
        .drop_tenant_holders("ghost")
        .await
        .expect("drop tenant holders");

    assert_eq!(stats.holders_dropped, 0);
}
