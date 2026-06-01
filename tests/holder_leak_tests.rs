//! Reproduction tests for `floatingConcurrency` holder leaks.
//!
//! These cover code paths where holders ought to be released atomically with the
//! task transition that ends a lease, but currently may not be.

mod test_helpers;

use silo::codec::{decode_lease, encode_holder, encode_lease, encode_task};
use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::keys::{concurrency_holder_key, job_info_key, task_key};
use silo::task::{GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

use test_helpers::*;

fn conc_limit(queue: &str, max: u32) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: max,
    })
}

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

/// Regression test for the broker-tombstone collision in process_grants' chain
/// resumer.
///
/// Before the fix, `ShardChainResumer::resume_chain` re-emitted the resumed
/// chain's `RunAttempt` task at `task_key(params.start_at_ms, …)` — the same
/// task_key the original enqueue's chain had written (and the same batch had
/// then deleted) when the job first queued. If the broker's scan happened to
/// observe the interim RunAttempt before the delete won in the LSM, a worker
/// could lease and ack-delete it, installing a tombstone for that task_key.
/// The resumed chain's subsequent write at the same key was then silently
/// suppressed, stranding the holders the resumer had just granted.
///
/// The fix lands the resumed task at `task_key(now_ms, …)` so the broker
/// tombstone for the original key cannot suppress it — mirroring the
/// long-standing precedent in `handle_request_ticket`. See
/// `project_broker_tombstone_chain_continuation`.
///
/// This test pins down the simpler invariant the fix introduces: after a
/// queued job is granted by `process_grants`, its terminal `RunAttempt` lives
/// at a `task_key` whose `start_time_ms` is strictly greater than the job's
/// original enqueue `start_at_ms`, provided enough wall-clock has passed.
#[silo::test]
async fn resume_chain_writes_run_attempt_at_now_not_original_start_at_ms() {
    use silo::job::ConcurrencyLimit;
    use silo::keys::{parse_task_key, tasks_prefix};

    let (_tmp, shard) = open_temp_shard().await;
    let queue = "resume-chain-q".to_string();
    let tenant = "-";

    // Job A occupies the only Concurrency slot. Its RunAttempt is leased and
    // acknowledged so any later write at the same task_key would be eligible
    // for broker-tombstone suppression — that's exactly the trap the resumer
    // needs to dodge.
    let original_enqueue_ms = now_ms();
    let _job_a = shard
        .enqueue(
            tenant,
            None,
            10u8,
            original_enqueue_ms,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": "A"})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue A");

    // Job B has no slot to take — it queues as a concurrency request and the
    // chain walker drops its interim RunAttempt at `task_key(original_enqueue_ms, …)`
    // in the same batch as the request write.
    let _job_b = shard
        .enqueue(
            tenant,
            None,
            10u8,
            original_enqueue_ms,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": "B"})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue B");

    // Lease A.
    let leased_a = shard.dequeue("w", "default", 1).await.expect("deq A").tasks;
    assert_eq!(leased_a.len(), 1, "A should lease immediately");
    let task_a_id = leased_a[0].attempt().task_id().to_string();

    // Force a measurable wall-clock gap so that any later "now_ms" the resumer
    // captures is strictly greater than `original_enqueue_ms`. Without this
    // gap the assertion below would be vacuously satisfied by same-millisecond
    // resolution rather than by the fix.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let before_release_ms = now_ms();
    assert!(
        before_release_ms > original_enqueue_ms,
        "test setup must advance the wall clock past the original enqueue"
    );

    // Releasing A frees the slot; the grant scanner picks up B's request and
    // calls `ShardChainResumer::resume_chain`, which now uses `now_ms` for the
    // terminal RunAttempt's task_key.
    shard
        .report_attempt_outcome(&task_a_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report A");

    // Poll for B's RunAttempt task to appear; the grant scanner runs
    // asynchronously after the release.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut b_task_start_time_ms: Option<u64> = None;
    while std::time::Instant::now() < deadline {
        let start = tasks_prefix();
        let end = silo::keys::end_bound(&start);
        let mut iter = shard
            .db()
            .scan::<Vec<u8>, _>(start..end)
            .await
            .expect("scan tasks");
        let mut found = None;
        while let Some(kv) = iter.next().await.expect("iter") {
            let Some(parsed) = parse_task_key(&kv.key) else {
                continue;
            };
            // The leased Job A's task_key has already been ack-deleted, so any
            // surviving task at this point is Job B's resumed RunAttempt.
            found = Some(parsed.start_time_ms);
            break;
        }
        if let Some(t) = found {
            b_task_start_time_ms = Some(t);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let b_start = b_task_start_time_ms
        .expect("grant scanner failed to write Job B's RunAttempt within the deadline");

    assert!(
        b_start >= before_release_ms as u64,
        "resumed chain must write at task_key(now_ms, …), not at the original \
         enqueue's start_at_ms — got start_time_ms={} which is not >= now-at-release={}. \
         A regression here re-introduces the broker-tombstone collision tracked in \
         project_broker_tombstone_chain_continuation.",
        b_start,
        before_release_ms,
    );
    assert!(
        b_start > original_enqueue_ms as u64,
        "resumed RunAttempt's task_key still encodes the original enqueue time \
         ({}), so any tombstone planted at that key would silently suppress this \
         task — exactly the leak the fix is meant to prevent.",
        original_enqueue_ms,
    );
}

/// Regression for the "stranded inflight task → orphaned holder" defect that
/// `e6660cd` ("Clear up stranded inflight tasks with drop guard") targets.
///
/// Failure shape this reproduces:
///   1. A concurrency-limited job is enqueued. `handle_enqueue` grants the slot
///      immediately: `append_grant_edits` writes the durable holder in the SAME
///      batch as the terminal `RunAttempt` task. Holder committed, task durable.
///   2. A worker dequeues the `RunAttempt`. `claim_ready` moves the task into the
///      broker's in-memory `inflight` set. Then, under worker overload, the
///      `LeaseTasks` RPC future is *cancelled* (deadline / disconnect / drop)
///      before the lease is durably written.
///   3. Without the drop guard the task is stranded in `inflight` forever (no
///      TTL): the scanner skips inflight keys, so the `RunAttempt` is never
///      re-leased — yet the holder from step 1 keeps consuming the slot. That's
///      the leak: holder committed, task consumed, no lease ever appears.
///
/// The guard closes this by releasing the claimed key from `inflight` on drop
/// (without re-buffering), so the DB scanner — which still sees the un-deleted
/// task key — re-adds it and a later dequeue leases it exactly once. This test
/// drives the *real* `dequeue` future, cancels it mid-flight right after the
/// claim, and asserts the task recovers and the holder never leaks.
///
/// If the guard regresses, the recovery loop below never re-leases the task and
/// the `expect` fires with "never re-leased — holder orphaned".
#[silo::test]
async fn dropped_dequeue_future_does_not_orphan_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue = "drop-guard-q";

    // (1) Enqueue a Concurrency(max=1) job. The slot is free, so the grant is
    // immediate: holder + RunAttempt land together in the enqueue batch.
    let _job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![conc_limit(queue, 1)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "concurrency grant should write a holder at enqueue time"
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, queue),
        1,
        "in-memory holder reservation should exist after the immediate grant"
    );

    // Wait for the background scanner to buffer the RunAttempt so the dequeue's
    // first `claim_ready` is synchronous — guaranteeing the single poll below
    // claims the task (moves it into `inflight`) before parking on an await.
    let buffered = poll_until(|| async { shard.broker_buffer_len() }, |n| *n >= 1, 2_000).await;
    assert!(buffered >= 1, "scanner should buffer the RunAttempt task");

    // (2) Drive the real dequeue future, poll it exactly once, then drop it.
    // The first poll claims the task synchronously — `claim_ready` moves the key
    // into the broker's `inflight` set — then parks awaiting durability. The
    // future is then dropped (cancelled RPC), which must run
    // `ClaimedInflightGuard::drop`.
    {
        let mut fut = Box::pin(shard.dequeue("w-overloaded", "default", 1));
        let polled = futures::poll!(fut.as_mut());
        assert!(
            polled.is_pending(),
            "single poll must leave the dequeue mid-flight (claimed, not yet acked); \
             got a ready result instead, so the cancellation window was not exercised"
        );
        // While the future is alive and parked, the claimed key is in-flight.
        assert_eq!(
            shard.broker_inflight_len("default"),
            1,
            "the claimed RunAttempt must be in-flight while the dequeue is mid-flight"
        );
        drop(fut); // ClaimedInflightGuard::drop → release_inflight(key)
    }

    // PRIMARY REGRESSION ASSERTION (deterministic): dropping the dequeue future
    // must have released the claimed key from the in-flight set. Without the
    // drop guard (e6660cd) the key is stranded here forever — the scanner skips
    // in-flight keys, so the RunAttempt is never re-leased and its holder leaks.
    assert_eq!(
        shard.broker_inflight_len("default"),
        0,
        "cancelled dequeue left the claimed task stranded in-flight — drop guard regression; \
         the holder it backs can never be re-leased and is orphaned"
    );

    // Holder is still committed from enqueue and must NOT have been doubled or
    // dropped by the cancellation.
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "holder count must stay at exactly 1 across the cancellation"
    );

    // (3) Recovery: a subsequent dequeue must re-lease the stranded task. With a
    // broken/absent guard the task is stuck in `inflight`, the scanner never
    // re-adds it, and this loop never finds a lease.
    let mut recovered_task_id: Option<String> = None;
    for _ in 0..50 {
        let res = shard
            .dequeue("w-healthy", "default", 1)
            .await
            .expect("recovery dequeue");
        if let Some(t) = res.tasks.first() {
            recovered_task_id = Some(t.attempt().task_id().to_string());
            break;
        }
        // Tolerate the ambiguous case where the cancelled write actually landed:
        // a lease may already exist even though the future was dropped.
        if let Some((_, lease_val)) = first_lease_kv(shard.db()).await {
            if let Ok(Task::RunAttempt { id, .. }) =
                decode_lease(lease_val).expect("decode lease").to_task()
            {
                recovered_task_id = Some(id);
                break;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }

    let task_id = recovered_task_id.expect(
        "dropped RunAttempt was never re-leased — the holder is orphaned (no task, no lease, \
         yet the concurrency slot stays consumed). This is the stranded-inflight holder leak \
         the e6660cd drop guard must prevent.",
    );

    // Exactly one holder backs the (now re-leased) task — no phantom duplicate.
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "re-lease must reuse the original holder, not mint a second one"
    );

    // (4) Completing the job releases the holder — proving it was never orphaned
    // and the slot is reclaimable.
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "durable holder must be released once the recovered job completes"
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, queue),
        0,
        "in-memory holder reservation must also drain to zero — no slot leak"
    );
}

/// PRODUCTION WEDGE REPRODUCTION — an in-memory concurrency holder that the
/// dequeue drop guard does NOT release, permanently consuming a queue's cap.
///
/// A concurrency holder has two halves: a durable KV row (prefix 0x09) AND an
/// in-memory reservation (`ConcurrencyCounts`, a per-(tenant,queue) set of
/// task_ids). `try_reserve`/`process_grants` gate admission on the IN-MEMORY
/// count, so an in-memory reservation with no durable backing silently steals a
/// slot forever (nothing durable for cancel/reap/expiry to key a release off,
/// and hydration only ever *inserts*). Accumulate enough — prod hit 300/300 for
/// env-10000215255 — and the queue wedges while ~1M requesters starve.
///
/// The drop guard `ClaimedInflightGuard` (e6660cd) only rolls back the broker's
/// `inflight` set; it has no reference to `self.concurrency`. So any in-memory
/// holder bookkeeping that lives in `dequeue`'s post-commit tail is lost when
/// the future is cancelled. This test pins the cleanest, deterministic instance:
///
///   `handle_run_attempt` for a RunAttempt whose `job_info` is gone deletes the
///   held concurrency holder *in the committed batch* but performs the matching
///   in-memory `atomic_release` only afterwards, in `dequeue`'s post-commit loop
///   (dequeue.rs ~334-339). `write_with_options` applies the batch to the WAL on
///   its first poll, so the durable delete lands even if the future is then
///   dropped — but the post-commit `atomic_release` never runs. Net: durable
///   holder gone, in-memory reservation retained = a ghost that wedges the queue.
///
/// The sibling on the *grant* side (handle_request_ticket's `try_reserve` not
/// rolled back on drop) produces the same ghost when the commit doesn't land;
/// it needs a genuinely-pending pre-commit await (a cold hydration scan over a
/// large queue in prod) to hit, so it isn't deterministic in the warm test DB.
/// Both share one root cause and one fix: roll back in-memory concurrency state
/// on the dequeue future-drop path, exactly as the error/commit-failure paths do.
///
/// This test cancels the real `dequeue` future at the commit and asserts the
/// queue is NOT wedged. It FAILS on current code (durable=0 but in-memory=1),
/// reproducing the leak; it passes once the drop path releases in-memory holders.
///
/// Currently `#[ignore]`d because it reproduces an UNFIXED bug (a failing test
/// would redden CI). Run it explicitly with:
///   cargo test --test holder_leak_tests \
///     dropped_dequeue_future_skips_inmemory_holder_release_and_wedges_queue \
///     -- --ignored --nocapture
/// Remove the `#[ignore]` once the drop path releases in-memory holders; it then
/// becomes the regression guard. (Uses `#[tokio::test]` rather than
/// `#[silo::test]` because the latter's proc-macro drops outer attributes like
/// `#[ignore]`.)
#[ignore = "reproduces an unfixed in-memory holder leak / queue wedge; un-ignore after the fix"]
#[tokio::test]
async fn dropped_dequeue_future_skips_inmemory_holder_release_and_wedges_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue = "wedge-q";
    let task_group = "default";

    // Enqueue a Concurrency(max=1) job: holder granted at enqueue (in-memory +
    // durable), and a RunAttempt carrying held_queues=[queue] is written.
    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![conc_limit(queue, 1)],
            None,
            task_group,
        )
        .await
        .expect("enqueue");

    assert_eq!(count_concurrency_holders(shard.db()).await, 1, "durable holder at enqueue");
    assert_eq!(shard.concurrency_holder_count(tenant, queue), 1, "in-memory holder at enqueue");

    // The job_info row disappears before the worker dequeues (migration / cleanup
    // race / partial restore). `handle_run_attempt` will take the missing-job
    // branch: delete the task AND the held concurrency holder in the batch, then
    // (post-commit) `atomic_release` the in-memory slot.
    {
        let mut batch = slatedb::WriteBatch::new();
        batch.delete(&job_info_key(tenant, &job_id));
        shard.db().write(batch).await.expect("delete job_info");
        shard.db().flush().await.expect("flush");
    }

    // Wait for the scanner to buffer the RunAttempt.
    let buffered = poll_until(|| async { shard.broker_buffer_len() }, |n| *n >= 1, 5_000).await;
    assert!(buffered >= 1, "scanner should buffer the RunAttempt");

    // Drive the real dequeue future to the commit, then cancel it there.
    //
    // We poll with `yield_now` (which does NOT advance the paused clock), so the
    // 10ms flush timer never fires and the `write_with_options` await stays
    // pending — the future parks exactly at the commit with the batch already
    // applied to the WAL. Dropping there is the overloaded-worker RPC cancel.
    {
        let mut fut = Box::pin(shard.dequeue("w-overloaded", task_group, 1));
        for _ in 0..500 {
            let polled = futures::poll!(fut.as_mut());
            assert!(
                polled.is_pending(),
                "dequeue completed before we could cancel it at the commit"
            );
            tokio::task::yield_now().await;
        }
        drop(fut); // cancel at the commit await; the post-commit atomic_release never runs
    }

    // Let the flush land the buffered batch (durable holder delete).
    let durable = poll_until(
        || async { count_concurrency_holders(shard.db()).await },
        |n| *n == 0,
        5_000,
    )
    .await;

    // Smoking gun: the durable holder was deleted by the committed batch, but the
    // in-memory reservation was NOT released (post-commit loop skipped on drop).
    let ghost = shard.concurrency_holder_count(tenant, queue);
    assert_eq!(durable, 0, "durable holder should be deleted by the committed batch");
    assert_eq!(
        ghost, 1,
        "LEAK: in-memory holder reservation survives (durable={durable}, in_mem={ghost}). \
         `atomic_release` only runs in dequeue's post-commit loop, which the cancelled \
         future skipped — the drop guard does not touch concurrency state."
    );

    // The slot is now wedged in memory: a fresh Concurrency(max=1) job on the
    // same queue can never be granted, because `try_reserve` sees the ghost.
    let _job2 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![conc_limit(queue, 1)],
            None,
            task_group,
        )
        .await
        .expect("enqueue job2");

    let mut job2_leased = false;
    for _ in 0..50 {
        let res = shard.dequeue("w2", task_group, 1).await.expect("dequeue job2");
        if !res.tasks.is_empty() {
            job2_leased = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }

    assert!(
        job2_leased,
        "QUEUE WEDGED: with the ghost in-memory holder (durable={durable}, in_mem={ghost}) \
         occupying the only slot, a brand-new job can never acquire concurrency and never \
         leases — the permanent orphaned-holder wedge seen in production. Nothing durable \
         exists for reap/cancel/expiry to release, so it survives until process restart."
    );
}
