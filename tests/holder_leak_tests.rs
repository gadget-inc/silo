//! Reproduction tests for `floatingConcurrency` holder leaks.
//!
//! These cover code paths where holders ought to be released atomically with the
//! task transition that ends a lease, but currently may not be.

mod test_helpers;

use silo::codec::{decode_lease, encode_holder, encode_lease, encode_task};
use silo::job::{FloatingConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::keys::{concurrency_holder_key, job_info_key, task_key};
use silo::task::{GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

use test_helpers::*;

fn fc_limit(queue: &str, default_max: u32) -> Limit {
    Limit::FloatingConcurrency(FloatingConcurrencyLimit {
        key: queue.to_string(),
        default_max_concurrency: default_max,
        refresh_interval_ms: 60_000,
        metadata: vec![],
    })
}

/// Force the worker's lease for `task_id` to be already expired by rewriting the
/// stored lease record. Mirrors the pattern in
/// `tests/concurrency_tests.rs::concurrency_reap_expired_lease_releases_holder`.
async fn expire_first_lease(shard: &silo::job_store_shard::JobStoreShard) {
    let (lease_key, lease_value) = first_lease_kv(shard.db()).await.expect("lease present");
    let decoded = decode_lease(lease_value).expect("decode lease");
    let expired = LeaseRecord {
        worker_id: decoded.worker_id().to_string(),
        task: decoded.to_task().unwrap(),
        expiry_ms: now_ms() - 1,
        started_at_ms: decoded.started_at_ms(),
    };
    shard
        .db()
        .put(&lease_key, &encode_lease(&expired))
        .await
        .expect("put expired lease");
    shard.db().flush().await.expect("flush");
}

/// Headline scenario: a job with a `FloatingConcurrency` limit gets its lease
/// reaped (worker SIGTERM). The holder must be released so the next requester
/// can be admitted, and `count_concurrency_holders` must be zero in steady state.
#[silo::test]
async fn floating_concurrency_holder_released_on_lease_expiry() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "fl-leak-q".to_string();

    let _job1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![fc_limit(&queue, 1)],
            None,
            "default",
        )
        .await
        .expect("enqueue1");

    let leased = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
    assert_eq!(leased.len(), 1, "first job should lease immediately");
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "holder should exist while task is leased"
    );

    // Worker disappears (SIGTERM) — lease expires, server reaps.
    expire_first_lease(&shard).await;
    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 1);

    // Steady-state invariant: holder must be gone.
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "holder leaked: reaper failed to release it for FloatingConcurrency"
    );
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "-").await,
        0,
        "tenant-scoped holder count should also be zero"
    );
}

/// After a lease has been reaped server-side, the worker may still call
/// `report_attempt_outcome` with the same task_id (race against reap). That call
/// must return `LeaseNotFound` AND must not leave any orphaned holder behind.
#[silo::test]
async fn floating_concurrency_holder_released_when_report_outcome_after_reap() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "fl-late-ack-q".to_string();

    shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![fc_limit(&queue, 1)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let leased = shard.dequeue("w1", "default", 1).await.expect("deq").tasks;
    assert_eq!(leased.len(), 1);
    let task_id = leased[0].attempt().task_id().to_string();

    expire_first_lease(&shard).await;
    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 1);

    // Late ack from the worker that just came back: server should signal not-found,
    // and the steady state must still have zero holders.
    let result = shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await;
    assert!(
        matches!(result, Err(JobStoreShardError::LeaseNotFound(_))),
        "report_attempt_outcome on a reaped task should return LeaseNotFound, got {:?}",
        result
    );

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "holder must remain released after a late report_attempt_outcome"
    );
}

/// Direct reproduction of a real holder-leak path: a `RunAttempt` task is
/// dequeued (the concurrency grant already wrote a holder at enqueue time), but
/// `handle_run_attempt` finds no `job_info` for the task. Today
/// (`dequeue.rs:657-662`) it deletes the task and returns Ok without releasing
/// the held queues — the holder is stranded.
///
/// Job-info-missing-while-task-exists shows up at least via:
///   - mid-flight tenant migration (new shard has the row, old shard's task
///     still references the now-missing job_info)
///   - background cleanup that deletes job_info ahead of derived data
///   - an import/restore that wrote a partial state
#[silo::test]
async fn holder_leaked_when_run_attempt_finds_missing_job_info() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue = "fl-missing-job-q".to_string();

    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![fc_limit(&queue, 1)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // FloatingConcurrency granted at enqueue time → holder + RunAttempt task.
    assert_eq!(count_concurrency_holders(shard.db()).await, 1);

    // Simulate the job_info disappearing before the worker dequeues.
    let mut delete_batch = slatedb::WriteBatch::new();
    delete_batch.delete(&job_info_key(tenant, &job_id));
    shard
        .db()
        .write(delete_batch)
        .await
        .expect("delete job_info");
    shard.db().flush().await.expect("flush");

    // Worker dequeues. handle_run_attempt at dequeue.rs:657-662 deletes the
    // task and returns Ok without releasing held_queues.
    let result = shard.dequeue("w1", "default", 1).await.expect("dequeue");
    assert_eq!(
        result.tasks.len(),
        0,
        "task is dropped because job_info is missing"
    );

    // BUG: the holder is now stranded — no task, no lease, no job, but the
    // FloatingConcurrency cap is still consumed by this dead grant.
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "holder must be released when handle_run_attempt drops a task with held_queues",
    );
}

// NOTE: `handle_check_rate_limit`'s max-retries early return now releases
// `held_queues` symmetrically with `handle_run_attempt`. The path remains
// dormant in production today because `record_grant_outcome` at
// `src/job_store_shard/enqueue.rs:67-80` swaps the None/Some arms relative to
// what `concurrency::handle_enqueue` returns, so chained limits never advance
// past the first grant and `held_queues` never accumulate inside a
// CheckRateLimit task. The release is in place defensively; a follow-up that
// fixes `record_grant_outcome` should add an end-to-end repro test.

/// Regression test for the `handle_check_rate_limit` max-retries early return.
/// `record_grant_outcome` cannot drive a CheckRateLimit task with non-empty
/// `held_queues` end-to-end today, so we plant the task and a matching holder
/// row directly. With the rate-limit mock returning `under_limit:false` and
/// `retry_count == max_retries`, the dequeue must hit the early-return branch
/// and release the holder. Without the fix at `dequeue.rs`, the holder leaks
/// and `count_concurrency_holders` stays at 1.
#[silo::test]
async fn check_rate_limit_max_retries_releases_held_queues() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue = "rl-max-retries-q";
    let task_group = "default";
    let priority: u8 = 10;

    // Baseline enqueue gives us a real `job_info` row so the dequeue handler's
    // job lookup succeeds and we exercise the rate-limit branch (not the
    // separate missing-job_info early return).
    let job_id = shard
        .enqueue(
            tenant,
            None,
            priority,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"baseline": true})),
            vec![],
            None,
            task_group,
        )
        .await
        .expect("enqueue baseline");

    // Plant the orphan-to-be holder under a synthetic task_id.
    let task_id = format!("crl-orphan-{}", uuid::Uuid::new_v4());
    let holder = encode_holder(&HolderRecord {
        granted_at_ms: now_ms(),
    });
    shard
        .db()
        .put(&concurrency_holder_key(tenant, queue, &task_id), &holder)
        .await
        .expect("plant holder");

    // Plant a CheckRateLimit task already at retry_count == max_retries with
    // a non-empty held_queues. The mock gubernator with limit=0 will report
    // under_limit:false → the over-limit branch fires → max-retries arm hit.
    let max_retries = 3u32;
    let rl = GubernatorRateLimitData {
        name: "api".to_string(),
        unique_key: format!("u-{}", uuid::Uuid::new_v4()),
        limit: 0,
        duration_ms: 60_000,
        hits: 1,
        algorithm: 0,
        behavior: 0,
        retry_initial_backoff_ms: 10,
        retry_max_backoff_ms: 1000,
        retry_backoff_multiplier: 2.0,
        retry_max_retries: max_retries,
    };
    let attempt_number = 7u32;
    let crl = Task::CheckRateLimit {
        task_id: task_id.clone(),
        tenant: tenant.to_string(),
        job_id: job_id.clone(),
        attempt_number,
        relative_attempt_number: 1,
        limit_index: 0,
        rate_limit: rl,
        retry_count: max_retries,
        started_at_ms: now_ms(),
        priority,
        held_queues: vec![queue.to_string()],
        task_group: task_group.to_string(),
    };
    let crl_key = task_key(task_group, now_ms(), priority, &job_id, attempt_number);
    shard
        .db()
        .put(&crl_key, &encode_task(&crl))
        .await
        .expect("plant CheckRateLimit task");
    shard.db().flush().await.expect("flush");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "holder must exist before dequeue"
    );

    // The broker may already have buffered the baseline RunAttempt before we
    // planted the CheckRateLimit, so loop until either the holder is released
    // or we run out of attempts.
    for _ in 0..20 {
        let _ = shard.dequeue("w1", task_group, 4).await.expect("dequeue");
        if count_concurrency_holders(shard.db()).await == 0 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "max-retries early return must release held concurrency holders \
         (regression for dequeue.rs handle_check_rate_limit)"
    );
}

/// Regression test for `handle_check_rate_limit`'s missing-job_info early
/// return. Same shape as `holder_leaked_when_run_attempt_finds_missing_job_info`
/// but in the CheckRateLimit handler: a CRL task carries `held_queues` from a
/// prior chained limit, the job_info row is gone by the time the broker scans
/// it (cleanup race / split / partial restore), and the dequeue handler's
/// `None => return Ok(())` arm drops the task without releasing the holders.
#[silo::test]
async fn check_rate_limit_missing_job_info_releases_held_queues() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue = "crl-missing-job-q";
    let task_group = "default";
    let priority: u8 = 10;

    // Use a synthetic job_id with no corresponding job_info row. The handler
    // will load job_info, hit None, and take the early-return arm.
    let job_id = format!("ghost-job-{}", uuid::Uuid::new_v4());
    let task_id = format!("crl-orphan-{}", uuid::Uuid::new_v4());

    // Plant the held concurrency holder we'll prove gets released.
    let holder = encode_holder(&HolderRecord {
        granted_at_ms: now_ms(),
    });
    shard
        .db()
        .put(&concurrency_holder_key(tenant, queue, &task_id), &holder)
        .await
        .expect("plant holder");

    // Plant a CheckRateLimit task whose job_info doesn't exist. With
    // retry_count well below max_retries the gubernator branch would normally
    // proceed, but we never get there because job_info is missing first.
    let rl = GubernatorRateLimitData {
        name: "api".to_string(),
        unique_key: format!("u-{}", uuid::Uuid::new_v4()),
        limit: 100,
        duration_ms: 60_000,
        hits: 1,
        algorithm: 0,
        behavior: 0,
        retry_initial_backoff_ms: 10,
        retry_max_backoff_ms: 1000,
        retry_backoff_multiplier: 2.0,
        retry_max_retries: 3,
    };
    let attempt_number = 1u32;
    let crl = Task::CheckRateLimit {
        task_id: task_id.clone(),
        tenant: tenant.to_string(),
        job_id: job_id.clone(),
        attempt_number,
        relative_attempt_number: 1,
        limit_index: 0,
        rate_limit: rl,
        retry_count: 0,
        started_at_ms: now_ms(),
        priority,
        held_queues: vec![queue.to_string()],
        task_group: task_group.to_string(),
    };
    let crl_key = task_key(task_group, now_ms(), priority, &job_id, attempt_number);
    shard
        .db()
        .put(&crl_key, &encode_task(&crl))
        .await
        .expect("plant CheckRateLimit task");
    shard.db().flush().await.expect("flush");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "holder must exist before dequeue"
    );

    for _ in 0..20 {
        let _ = shard.dequeue("w1", task_group, 4).await.expect("dequeue");
        if count_concurrency_holders(shard.db()).await == 0 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "missing-job_info early return must release held concurrency holders \
         (regression for dequeue.rs handle_check_rate_limit)"
    );
}

/// Defensive idempotency: a stranded holder (no corresponding lease and no
/// task) must be removable by an explicit purge call. This is the recovery hook
/// the gRPC handler invokes when a worker reports outcome for a task whose
/// lease has already been reaped, so workers can self-heal a leaked holder by
/// retrying their ack.
#[silo::test]
async fn purge_orphaned_holders_for_task_removes_stranded_holders() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_a = "purge-q-a";
    let queue_b = "purge-q-b";
    let task_id = "orphan-task-id";
    let other_task_id = "other-task-id";

    // Manually plant orphaned holders (no lease, no task).
    let holder_val = encode_holder(&HolderRecord {
        granted_at_ms: now_ms(),
    });
    shard
        .db()
        .put(
            &concurrency_holder_key(tenant, queue_a, task_id),
            &holder_val,
        )
        .await
        .expect("put holder a");
    shard
        .db()
        .put(
            &concurrency_holder_key(tenant, queue_b, task_id),
            &holder_val,
        )
        .await
        .expect("put holder b");
    // A holder for an unrelated task — must be left alone.
    shard
        .db()
        .put(
            &concurrency_holder_key(tenant, queue_a, other_task_id),
            &holder_val,
        )
        .await
        .expect("put unrelated holder");
    shard.db().flush().await.expect("flush");
    assert_eq!(count_concurrency_holders(shard.db()).await, 3);

    let purged = shard
        .purge_orphaned_holders_for_task(tenant, task_id)
        .await
        .expect("purge");
    assert_eq!(purged, 2, "should report two holders deleted");

    // Only the unrelated holder remains.
    assert_eq!(count_concurrency_holders(shard.db()).await, 1);

    // Idempotent: calling again is a no-op.
    let purged_again = shard
        .purge_orphaned_holders_for_task(tenant, task_id)
        .await
        .expect("purge again");
    assert_eq!(purged_again, 0);
    assert_eq!(count_concurrency_holders(shard.db()).await, 1);
}

/// End-to-end variant: a worker calls `report_attempt_outcome` for a task_id
/// whose lease was already reaped. The server returns LeaseNotFound. The worker
/// (or the gRPC layer) then invokes `purge_orphaned_holders_for_task` and the
/// stranded holder is gone.
#[silo::test]
async fn late_report_outcome_followed_by_purge_clears_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue = "fl-late-purge-q".to_string();

    shard
        .enqueue(
            tenant,
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![fc_limit(&queue, 1)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let leased = shard.dequeue("w1", "default", 1).await.expect("deq").tasks;
    let task_id = leased[0].attempt().task_id().to_string();
    assert_eq!(count_concurrency_holders(shard.db()).await, 1);

    // Plant an extra stale holder under the same task_id to simulate an
    // orphan that escaped a prior release path. (We can't reproduce the
    // production failure mode here, but we can prove purge cleans it up.)
    let stale_queue = "stale-extra-q";
    shard
        .db()
        .put(
            &concurrency_holder_key(tenant, stale_queue, &task_id),
            &encode_holder(&HolderRecord {
                granted_at_ms: now_ms(),
            }),
        )
        .await
        .expect("plant stale");
    shard.db().flush().await.expect("flush");
    assert_eq!(count_concurrency_holders(shard.db()).await, 2);

    expire_first_lease(&shard).await;
    shard.reap_expired_leases(tenant).await.expect("reap");
    // Reaper handled the legitimate held queue; the planted orphan remains.
    assert_eq!(count_concurrency_holders(shard.db()).await, 1);

    // Worker comes back with a late ack — server signals not-found.
    let res = shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await;
    assert!(matches!(res, Err(JobStoreShardError::LeaseNotFound(_))));

    // gRPC handler should call purge in this case → stale holder is gone.
    shard
        .purge_orphaned_holders_for_task(tenant, &task_id)
        .await
        .expect("purge");
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "purge after late ack should remove the planted orphan"
    );
}

/// Drive several jobs through enqueue → lease → mixture of (success, reap) and
/// assert that the per-tenant holder count is zero in steady state. Guards
/// against double-release and accounting drift across concurrent grants.
#[silo::test]
async fn floating_concurrency_steady_state_zero_holders_after_full_cycle() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "fl-cycle-q".to_string();

    // Cap = 3 with 3 jobs, all leased concurrently.
    for i in 0..3 {
        shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"i": i})),
                vec![fc_limit(&queue, 3)],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let leased = shard.dequeue("w1", "default", 3).await.expect("deq").tasks;
    assert_eq!(leased.len(), 3);
    assert_eq!(count_concurrency_holders(shard.db()).await, 3);

    // Succeed one explicitly.
    shard
        .report_attempt_outcome(
            leased[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report success");

    // Reap the rest by expiring all remaining leases.
    let now_minus_1 = now_ms() - 1;
    let leases_prefix = silo::keys::leases_prefix();
    let leases_end = silo::keys::end_bound(&leases_prefix);
    let mut iter = shard
        .db()
        .scan::<Vec<u8>, _>(leases_prefix..leases_end)
        .await
        .expect("scan leases");
    let mut to_expire: Vec<(Vec<u8>, bytes::Bytes)> = Vec::new();
    while let Some(kv) = iter.next().await.expect("iter") {
        to_expire.push((kv.key.to_vec(), kv.value));
    }
    for (lease_key, lease_value) in to_expire {
        let decoded = decode_lease(lease_value).expect("decode lease");
        let expired = LeaseRecord {
            worker_id: decoded.worker_id().to_string(),
            task: decoded.to_task().unwrap(),
            expiry_ms: now_minus_1,
            started_at_ms: decoded.started_at_ms(),
        };
        shard
            .db()
            .put(&lease_key, &encode_lease(&expired))
            .await
            .expect("put expired");
    }
    shard.db().flush().await.expect("flush");
    shard.reap_expired_leases("-").await.expect("reap");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "holders must be zero after every job has reached a terminal state"
    );
    assert_eq!(
        count_concurrency_holders_for_tenant(shard.db(), "-").await,
        0,
        "per-tenant holder count must be zero too"
    );
}
