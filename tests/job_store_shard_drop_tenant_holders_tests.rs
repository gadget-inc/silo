mod test_helpers;

use silo::job::{ConcurrencyLimit, JobStatusKind, Limit};
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

/// A `Running` job whose lease was raw-deleted (the old admin-action bug) is an
/// orphan: invisible to the lease reaper. `reconcile_orphaned_jobs`
/// force-transitions it to terminal `Failed`.
#[silo::test]
async fn reconcile_fails_orphaned_running_job() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = enqueue_with_holder(&shard, "tenant-a", "qA", now).await;

    // Dequeue → the job is Running with a lease.
    let leased = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1, "exactly one task leased");
    let task_id = leased[0].attempt().task_id().to_string();

    // Simulate the old bug: raw-delete the lease without a status transition,
    // leaving JOB_STATUS = Running with no lease.
    shard
        .db()
        .delete(&silo::keys::leased_task_key(&task_id))
        .await
        .expect("delete lease");
    assert_eq!(count_lease_keys(shard.db()).await, 0, "lease removed");
    assert_eq!(
        shard
            .get_job_status("tenant-a", &job_id)
            .await
            .expect("status")
            .expect("job exists")
            .kind,
        JobStatusKind::Running,
        "job is orphaned Running"
    );

    // Reconcile finalizes the orphan.
    let stats = shard
        .reconcile_orphaned_jobs("tenant-a")
        .await
        .expect("reconcile");
    assert_eq!(stats.orphaned_running_failed, 1);

    assert_eq!(
        shard
            .get_job_status("tenant-a", &job_id)
            .await
            .expect("status")
            .expect("job exists")
            .kind,
        JobStatusKind::Failed,
        "orphan transitioned to terminal Failed"
    );
    assert_eq!(count_lease_keys(shard.db()).await, 0);
}

/// A `Running` job with its lease intact may have a worker actively running it,
/// so reconcile must never touch it.
#[silo::test]
async fn reconcile_leaves_live_leased_running_job_untouched() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = enqueue_with_holder(&shard, "tenant-a", "qA", now).await;

    // Dequeue → Running WITH lease intact (no bug simulated).
    let leased = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1);
    assert!(count_lease_keys(shard.db()).await >= 1, "lease present");

    let stats = shard
        .reconcile_orphaned_jobs("tenant-a")
        .await
        .expect("reconcile");
    assert_eq!(stats.orphaned_running_failed, 0, "live lease untouched");

    assert_eq!(
        shard
            .get_job_status("tenant-a", &job_id)
            .await
            .expect("status")
            .expect("job exists")
            .kind,
        JobStatusKind::Running,
        "still Running — a worker may be running it",
    );
}

/// Reconciling a tenant with no orphaned jobs is a no-op returning zeros.
#[silo::test]
async fn reconcile_with_no_orphans_is_noop() {
    let (_tmp, shard) = open_temp_shard().await;

    let stats = shard
        .reconcile_orphaned_jobs("ghost")
        .await
        .expect("reconcile");

    assert_eq!(stats.orphaned_running_failed, 0);
    assert_eq!(stats.orphaned_scheduled_redriven, 0);
}

/// Enqueue a job with no limits → status `Scheduled` + a single `RunAttempt`
/// task in the TASK queue. Returns the job id.
async fn enqueue_no_limits(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
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
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

/// A `Scheduled` job whose queued `RunAttempt` task was raw-deleted (the old
/// admin-action bug) — with no concurrency request either — is an orphan that
/// nothing will dispatch. `reconcile_orphaned_jobs` redrives it back into the
/// limit chain so it runs again.
#[silo::test]
async fn reconcile_redrives_task_less_scheduled_orphan() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = enqueue_no_limits(&shard, "tenant-a", now).await;
    assert_eq!(count_task_keys(shard.db()).await, 1, "one task enqueued");

    // Simulate the old bug: raw-delete the queued task, leaving JOB_STATUS =
    // Scheduled with nothing in the pipeline.
    let (task_key, _) = first_task_kv(shard.db()).await.expect("task exists");
    shard.db().delete(&task_key).await.expect("delete task");
    assert_eq!(count_task_keys(shard.db()).await, 0, "task removed");
    assert_eq!(
        shard
            .get_job_status("tenant-a", &job_id)
            .await
            .expect("status")
            .expect("job exists")
            .kind,
        JobStatusKind::Scheduled,
        "job is orphaned Scheduled"
    );

    // Reconcile redrives the orphan.
    let stats = shard
        .reconcile_orphaned_jobs("tenant-a")
        .await
        .expect("reconcile");
    assert_eq!(stats.orphaned_scheduled_redriven, 1);
    assert_eq!(stats.orphaned_running_failed, 0);

    // A fresh RunAttempt task exists and the job dequeues + runs again.
    assert_eq!(count_task_keys(shard.db()).await, 1, "task re-created");
    let leased = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1, "redriven job dequeues");
    assert_eq!(leased[0].job().id(), job_id);
    assert_eq!(
        shard
            .get_job_status("tenant-a", &job_id)
            .await
            .expect("status")
            .expect("job exists")
            .kind,
        JobStatusKind::Running,
    );
}

/// A `Scheduled` job that still has its queued task is healthy — reconcile must
/// not touch it or write a duplicate task.
#[silo::test]
async fn reconcile_leaves_scheduled_with_task_untouched() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = enqueue_no_limits(&shard, "tenant-a", now).await;
    assert_eq!(count_task_keys(shard.db()).await, 1);

    let stats = shard
        .reconcile_orphaned_jobs("tenant-a")
        .await
        .expect("reconcile");
    assert_eq!(
        stats.orphaned_scheduled_redriven, 0,
        "healthy job untouched"
    );

    // No duplicate task, status unchanged.
    assert_eq!(count_task_keys(shard.db()).await, 1, "no duplicate task");
    assert_eq!(
        shard
            .get_job_status("tenant-a", &job_id)
            .await
            .expect("status")
            .expect("job exists")
            .kind,
        JobStatusKind::Scheduled,
    );
}

/// A `Scheduled` job parked on a full concurrency queue has a pending request
/// (not a task) and is legitimately waiting — reconcile must never redrive it.
#[silo::test]
async fn reconcile_leaves_scheduled_waiting_on_concurrency_untouched() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // First job grabs the only slot (holder + RunAttempt task). Second job is
    // parked as a concurrency request (Scheduled, no task).
    enqueue_with_holder(&shard, "tenant-a", "qA", now).await;
    enqueue_with_holder(&shard, "tenant-a", "qA", now).await;
    assert_eq!(
        count_task_keys(shard.db()).await,
        1,
        "only first has a task"
    );
    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        1,
        "second parked as a request"
    );

    let stats = shard
        .reconcile_orphaned_jobs("tenant-a")
        .await
        .expect("reconcile");
    assert_eq!(
        stats.orphaned_scheduled_redriven, 0,
        "neither the task-holding nor the request-waiting job is an orphan"
    );

    // Nothing duplicated.
    assert_eq!(count_task_keys(shard.db()).await, 1);
    assert_eq!(count_concurrency_requests(shard.db()).await, 1);
}

/// Running and Scheduled orphans for the same tenant are both recovered in one
/// reconcile pass.
#[silo::test]
async fn reconcile_handles_running_and_scheduled_orphans_together() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Running orphan: enqueue → dequeue (Running + lease) → delete the lease.
    let running_id = enqueue_no_limits(&shard, "tenant-a", now).await;
    let leased = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job().id(), running_id);
    let task_id = leased[0].attempt().task_id().to_string();
    shard
        .db()
        .delete(&silo::keys::leased_task_key(&task_id))
        .await
        .expect("delete lease");

    // Scheduled orphan: enqueue (now in TASK queue) → delete its task.
    let scheduled_id = enqueue_no_limits(&shard, "tenant-a", now).await;
    let (task_key, _) = first_task_kv(shard.db()).await.expect("scheduled task");
    shard.db().delete(&task_key).await.expect("delete task");

    let stats = shard
        .reconcile_orphaned_jobs("tenant-a")
        .await
        .expect("reconcile");
    assert_eq!(stats.orphaned_running_failed, 1);
    assert_eq!(stats.orphaned_scheduled_redriven, 1);

    assert_eq!(
        shard
            .get_job_status("tenant-a", &running_id)
            .await
            .expect("status")
            .expect("job")
            .kind,
        JobStatusKind::Failed,
    );
    assert_eq!(
        shard
            .get_job_status("tenant-a", &scheduled_id)
            .await
            .expect("status")
            .expect("job")
            .kind,
        JobStatusKind::Scheduled,
        "redriven job stays Scheduled until a worker leases its new task"
    );
}
