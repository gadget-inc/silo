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

/// Regression: dropping a tenant's *running* (leased) attempt must finalize the
/// job to a terminal status — it must never be left orphaned in `Running` with
/// no lease. The lease reaper is lease-driven, so a Running job with no lease
/// would never be cleaned up (this is exactly the bug raw lease-deletion caused).
#[silo::test]
async fn drop_tenant_finalizes_running_job_instead_of_orphaning() {
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
        .drop_tenant_holders_and_runattempts("tenant-a")
        .await
        .expect("drop tenant state");
    assert_eq!(stats.run_attempts_dropped, 1);

    // The job must be finalized to a terminal status — NOT left in Running —
    // and its lease must be gone, so nothing is orphaned beyond the reaper's reach.
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert!(
        status.is_terminal(),
        "job must be terminal after drop, got {:?}",
        status.kind
    );
    assert_ne!(
        status.kind,
        JobStatusKind::Running,
        "job must not be orphaned in Running"
    );
    assert_eq!(
        count_lease_keys(shard.db()).await,
        0,
        "no lease may be left behind"
    );
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
}

/// Regression: dropping a tenant's *queued* (Scheduled, not yet leased) attempt
/// must finalize the job to `Cancelled` — it must never be left orphaned in
/// `Scheduled` with no task to run it. (Raw task-deletion left it stuck in
/// `Scheduled` with nothing in the queue, invisible to every recovery path.)
#[silo::test]
async fn drop_tenant_cancels_queued_job_instead_of_orphaning() {
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

    // Act.
    let stats = shard
        .drop_tenant_holders_and_runattempts("tenant-a")
        .await
        .expect("drop tenant state");
    assert_eq!(stats.run_attempts_dropped, 1);

    // The queued job must be transitioned to Cancelled — not left orphaned in
    // Scheduled — and its holder released.
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Cancelled,
        "queued job must be Cancelled after drop, got {:?}",
        status.kind
    );
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
}

/// Dropping a *running* attempt whose job was already cancelled must finalize it
/// as `Cancelled` (the `was_cancelled` branch), not as a `WORKER_CRASHED`
/// failure. This mirrors what the lease reaper does for a cancelled job and
/// preserves the user's intent through the forced drop.
#[silo::test]
async fn drop_tenant_finalizes_cancelled_running_job_as_cancelled() {
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

    // Cancel the running job: this only records the cancellation flag (the
    // worker would normally ack it on heartbeat), leaving the job in Running
    // with a live lease — exactly the state the drop must finalize correctly.
    shard
        .cancel_job("tenant-a", &job_id)
        .await
        .expect("cancel running job");
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Running,
        "cancel of a Running job only sets the flag; status stays Running"
    );
    assert!(
        shard
            .is_job_cancelled("tenant-a", &job_id)
            .await
            .expect("cancellation lookup"),
        "cancellation flag is set pre-drop"
    );

    // Act.
    let stats = shard
        .drop_tenant_holders_and_runattempts("tenant-a")
        .await
        .expect("drop tenant state");
    assert_eq!(stats.run_attempts_dropped, 1);

    // Because the job was cancelled, the leased attempt must be finalized as
    // Cancelled — NOT Failed (which is what the WORKER_CRASHED path produces).
    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("status")
        .expect("job exists");
    assert_eq!(
        status.kind,
        JobStatusKind::Cancelled,
        "cancelled running job must finalize as Cancelled, not {:?}",
        status.kind
    );
    assert_eq!(
        count_lease_keys(shard.db()).await,
        0,
        "no lease may be left behind"
    );
    assert_eq!(shard.concurrency_holder_count("tenant-a", "qA"), 0);
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
