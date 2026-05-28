//! Reproduction tests for `floatingConcurrency` holder leaks.
//!
//! These cover code paths where holders ought to be released atomically with the
//! task transition that ends a lease, but currently may not be.

mod test_helpers;

use silo::codec::{decode_lease, encode_holder, encode_lease, encode_task};
use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, JobStatusKind, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::keys::{concurrency_holder_key, job_info_key, task_key};
use silo::retry::RetryPolicy;
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
/// Force every lease currently stored on the shard to be already expired.
/// Useful for batch lease-expiry simulation where multiple holders are
/// drained at once.
async fn expire_all_leases(shard: &silo::job_store_shard::JobStoreShard) {
    let start = silo::keys::leases_prefix();
    let end = silo::keys::end_bound(&start);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .expect("scan leases");
    let mut updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while let Ok(Some(kv)) = iter.next().await {
        let decoded = decode_lease(kv.value.clone()).expect("decode lease");
        let expired = LeaseRecord {
            worker_id: decoded.worker_id().to_string(),
            task: decoded.to_task().unwrap(),
            expiry_ms: now_ms() - 1,
            started_at_ms: decoded.started_at_ms(),
        };
        updates.push((kv.key.to_vec(), encode_lease(&expired)));
    }
    drop(iter);
    for (k, v) in updates {
        shard.db().put(&k, &v).await.expect("put expired lease");
    }
    shard.db().flush().await.expect("flush");
}

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

/// Catch-all steady-state assertion: a single shard, single tenant drives a
/// mixed bag of job life cycles to terminal — success, exhausted retries,
/// cancel-before-grant, cancel-after-grant, lease expiry + reap, rate-limit
/// retry → success, future-scheduled then cancel, and reimport-to-terminal —
/// then confirms no concurrency holders, no concurrency requests, and no
/// in-memory holder remain. This is the catch-all the bench-only
/// `HolderAudit` provides for live workloads, moved into `cargo test` so
/// any new leak path (one we forgot to cover with a targeted test) trips
/// CI rather than waiting for the bench.
#[silo::test]
async fn steady_state_zero_holders_under_mixed_workload() {
    use silo::gubernator::{GubernatorError, MockGubernatorClient, RateLimitClient, RateLimitResult};
    use silo::job::{GubernatorAlgorithm, GubernatorRateLimit, RateLimitRetryPolicy};
    use silo::pb::gubernator::Algorithm;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Single-call-success Gubernator stub for the rate-limit-then-pass leg.
    struct FailFirstThenPass {
        seen: AtomicU32,
    }
    #[async_trait::async_trait]
    impl RateLimitClient for FailFirstThenPass {
        async fn check_rate_limit(
            &self,
            _name: &str,
            _unique_key: &str,
            _hits: i64,
            limit: i64,
            duration_ms: i64,
            _algorithm: Algorithm,
            _behavior: i32,
        ) -> Result<RateLimitResult, GubernatorError> {
            let i = self.seen.fetch_add(1, Ordering::SeqCst);
            let under_limit = i >= 1;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(RateLimitResult {
                under_limit,
                limit,
                remaining: if under_limit { limit } else { 0 },
                reset_time_ms: now_ms + duration_ms,
                error: None,
            })
        }
        async fn health_check(&self) -> Result<(String, i32), GubernatorError> {
            Ok(("ok".to_string(), 1))
        }
    }

    // The rate-limit leg needs its own shard so its rate-limit client doesn't
    // affect the other workloads.
    let rl_client = Arc::new(FailFirstThenPass { seen: AtomicU32::new(0) });
    let (_rl_tmp, rl_shard) = open_temp_shard_with_rate_limiter(rl_client.clone()).await;

    // Everything else: one shard with the default mock rate limiter.
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";

    // Track distinct queue names so the final per-queue in-memory assertion
    // visits every queue we touched (a typo'd queue would otherwise be
    // silently ignored).
    let mut queues_touched: Vec<String> = Vec::new();

    // ----- 1) Success: 8 jobs on a big-cap queue, all dequeued + reported success.
    let q_success = "mix-success".to_string();
    queues_touched.push(q_success.clone());
    let n_success = 8;
    for i in 0..n_success {
        shard
            .enqueue(
                tenant,
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"i": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: q_success.clone(),
                    max_concurrency: 10,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue success");
    }
    let success_tasks = shard
        .dequeue("w-success", "default", n_success as usize)
        .await
        .expect("dequeue success")
        .tasks;
    assert_eq!(success_tasks.len(), n_success);
    for t in &success_tasks {
        shard
            .report_attempt_outcome(t.attempt().task_id(), AttemptOutcome::Success { result: vec![] })
            .await
            .expect("complete success");
    }

    // ----- 2) Exhausted retries: 3 jobs, retry_count=1, both attempts Error → Failed.
    let q_fail = "mix-fail".to_string();
    queues_touched.push(q_fail.clone());
    let n_fail = 3;
    for i in 0..n_fail {
        shard
            .enqueue(
                tenant,
                None,
                10u8,
                now,
                Some(RetryPolicy {
                    retry_count: 1,
                    initial_interval_ms: 1,
                    max_interval_ms: 10,
                    randomize_interval: false,
                    backoff_factor: 1.0,
                }),
                test_helpers::msgpack_payload(&serde_json::json!({"f": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: q_fail.clone(),
                    max_concurrency: 5,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue fail");
    }
    // Drain through both attempts (initial + 1 retry).
    for _ in 0..3 {
        let tasks = shard.dequeue("w-fail", "default", 16).await.expect("dq fail").tasks;
        if tasks.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            continue;
        }
        for t in &tasks {
            shard
                .report_attempt_outcome(
                    t.attempt().task_id(),
                    AttemptOutcome::Error {
                        error_code: "ERR".to_string(),
                        error: b"boom".to_vec(),
                    },
                )
                .await
                .expect("error outcome");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // ----- 3) Cancel-before-grant: enqueue 5 jobs on a cap=1 queue. j0 takes
    //         the slot; j1..j4 sit as deferred concurrency requests. We let
    //         j0 succeed naturally (so its release happens through the
    //         normal completion path) and cancel only the 4 deferred jobs.
    //         Cancelling j0 too would race the background grant scanner —
    //         each release of j0's slot wakes the scanner, which grants a
    //         deferred request whose status check passed before the next
    //         cancel commits. The race is a real TOCTOU but unrelated to
    //         the steady-state assertion this test is meant to defend; we
    //         scope around it by exercising the cleaner shape.
    let q_cancel_pre = "mix-cancel-pre".to_string();
    queues_touched.push(q_cancel_pre.clone());
    let mut pre_ids = Vec::new();
    for i in 0..5 {
        let id = shard
            .enqueue(
                tenant,
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"c": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: q_cancel_pre.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue cancel-pre");
        pre_ids.push(id);
    }
    // Cancel j1..j4 first (TicketRequested → request-record cleanup, no
    // scanner wake-up race) then complete j0 via the normal worker path.
    for id in pre_ids.iter().skip(1) {
        shard.cancel_job(tenant, id).await.expect("cancel-pre deferred");
    }
    let pre_tasks = shard
        .dequeue("w-pre", "default", 5)
        .await
        .expect("dq cancel-pre");
    let pre_run = pre_tasks
        .tasks
        .into_iter()
        .find(|t| t.job().id() == pre_ids[0])
        .expect("j0 should be leasable after deferred siblings are cancelled");
    shard
        .report_attempt_outcome(
            pre_run.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("complete j0");

    // ----- 4) Cancel after grant: 3 jobs leased then cancelled mid-flight.
    //         Worker acks via Cancelled outcome.
    let q_cancel_post = "mix-cancel-post".to_string();
    queues_touched.push(q_cancel_post.clone());
    let mut post_ids = Vec::new();
    for i in 0..3 {
        let id = shard
            .enqueue(
                tenant,
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"p": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: q_cancel_post.clone(),
                    max_concurrency: 10,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue cancel-post");
        post_ids.push(id);
    }
    let post_tasks = shard
        .dequeue("w-post", "default", 3)
        .await
        .expect("dq cancel-post")
        .tasks;
    assert_eq!(post_tasks.len(), 3);
    for (id, t) in post_ids.iter().zip(post_tasks.iter()) {
        shard.cancel_job(tenant, id).await.expect("cancel-post");
        // Worker acks the cancellation outcome (Running → Cancelled).
        shard
            .report_attempt_outcome(t.attempt().task_id(), AttemptOutcome::Cancelled)
            .await
            .expect("worker ack cancel");
    }

    // ----- 5) Lease expiry: 4 jobs leased, leases force-expired, reap drains them.
    let q_reap = "mix-reap".to_string();
    queues_touched.push(q_reap.clone());
    for i in 0..4 {
        shard
            .enqueue(
                tenant,
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"r": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: q_reap.clone(),
                    max_concurrency: 10,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue reap");
    }
    let reap_tasks = shard
        .dequeue("w-reap", "default", 4)
        .await
        .expect("dq reap")
        .tasks;
    assert_eq!(reap_tasks.len(), 4);
    expire_all_leases(&shard).await;
    let reaped = shard.reap_expired_leases(tenant).await.expect("reap");
    assert!(reaped >= 4, "expected at least 4 leases reaped, got {reaped}");

    // ----- 6) Future-scheduled then cancel: 3 future jobs cancelled before their start time.
    let q_future = "mix-future".to_string();
    queues_touched.push(q_future.clone());
    let mut future_ids = Vec::new();
    for i in 0..3 {
        let id = shard
            .enqueue(
                tenant,
                None,
                10u8,
                now + 600_000,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"fu": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: q_future.clone(),
                    max_concurrency: 5,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue future");
        future_ids.push(id);
    }
    for id in &future_ids {
        shard.cancel_job(tenant, id).await.expect("cancel future");
    }

    // ----- 7) Reimport terminal: 3 jobs imported as Scheduled, then re-imported as Succeeded.
    let q_reimport = "mix-reimport".to_string();
    queues_touched.push(q_reimport.clone());
    for i in 0..3 {
        let job_id = format!("mix-reimp-{i}");
        let initial = ImportJobParams {
            id: job_id.clone(),
            priority: 50,
            enqueue_time_ms: now,
            start_at_ms: 0,
            retry_policy: None,
            payload: test_helpers::msgpack_payload(&serde_json::json!({"re": i})),
            limits: vec![Limit::Concurrency(ConcurrencyLimit {
                key: q_reimport.clone(),
                max_concurrency: 5,
            })],
            metadata: None,
            task_group: "default".to_string(),
            attempts: vec![],
        };
        let r = shard.import_jobs(tenant, vec![initial]).await.unwrap();
        assert!(r[0].success);
        assert_eq!(r[0].status, JobStatusKind::Scheduled);
        let reimport = ImportJobParams {
            id: job_id.clone(),
            priority: 50,
            enqueue_time_ms: now,
            start_at_ms: 0,
            retry_policy: None,
            payload: test_helpers::msgpack_payload(&serde_json::json!({"re": i})),
            limits: vec![Limit::Concurrency(ConcurrencyLimit {
                key: q_reimport.clone(),
                max_concurrency: 5,
            })],
            metadata: None,
            task_group: "default".to_string(),
            attempts: vec![ImportedAttempt {
                status: ImportedAttemptStatus::Succeeded { result: vec![4, 5, 6] },
                started_at_ms: now,
                finished_at_ms: now + 1_000,
            }],
        };
        let r = shard.import_jobs(tenant, vec![reimport]).await.unwrap();
        assert!(r[0].success, "reimport failed: {:?}", r[0].error);
        assert_eq!(r[0].status, JobStatusKind::Succeeded);
    }

    // ----- 8) Rate-limit retry → success on its own shard.
    let q_rl = "mix-rl".to_string();
    let rl_limit = GubernatorRateLimit {
        name: "mix-rl".to_string(),
        unique_key: "mix-rl-key".to_string(),
        limit: 1,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 5,
            max_backoff_ms: 50,
            backoff_multiplier: 2.0,
            max_retries: 5,
        },
    };
    let _rl_job = rl_shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"rl": 0})),
            vec![
                Limit::Concurrency(ConcurrencyLimit {
                    key: q_rl.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(rl_limit),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue rl");
    // Drive dequeues until a RunAttempt is leased, then complete it.
    let mut rl_leased_id: Option<String> = None;
    for _ in 0..40 {
        let tasks = rl_shard
            .dequeue("w-rl", "default", 1)
            .await
            .expect("dq rl")
            .tasks;
        if let Some(t) = tasks.into_iter().next() {
            rl_leased_id = Some(t.attempt().task_id().to_string());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let rl_leased_id = rl_leased_id.expect("rate-limit chain should produce a RunAttempt");
    rl_shard
        .report_attempt_outcome(&rl_leased_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("complete rl");

    // ===== Final assertions =====
    // The grant scanner runs asynchronously after each release; poll briefly
    // so the catch-all assertion isn't racing the background pass.
    let holders = poll_until(
        || count_concurrency_holders(shard.db()),
        |n| *n == 0,
        3_000,
    )
    .await;
    if holders != 0 {
        // Per-queue diagnostic for the failure mode.
        for q in &queues_touched {
            let on_disk = silo::keys::concurrency_holders_queue_prefix(tenant, q);
            let end = silo::keys::end_bound(&on_disk);
            let mut iter = shard
                .db()
                .scan_with_options::<Vec<u8>, _>(on_disk..end, &silo::scan_options())
                .await
                .expect("scan holders");
            let mut keys = Vec::new();
            while let Ok(Some(kv)) = iter.next().await {
                keys.push(kv.key.to_vec());
            }
            if !keys.is_empty() {
                eprintln!(
                    "queue {q} still has {} holder(s) post-drain: {:?}",
                    keys.len(),
                    keys.iter()
                        .map(|k| silo::keys::parse_concurrency_holder_key(k))
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
    assert_eq!(
        holders, 0,
        "steady state: no concurrency holders should remain on the main shard"
    );
    let requests = poll_until(
        || count_concurrency_requests(shard.db()),
        |n| *n == 0,
        2_000,
    )
    .await;
    assert_eq!(
        requests, 0,
        "steady state: no concurrency requests should remain on the main shard"
    );
    for q in &queues_touched {
        assert_eq!(
            shard.concurrency_holder_count(tenant, q),
            0,
            "steady state: in-memory holder count for queue {q} should be 0",
        );
    }

    // Rate-limit shard's own audit.
    let rl_holders = poll_until(
        || count_concurrency_holders(rl_shard.db()),
        |n| *n == 0,
        2_000,
    )
    .await;
    assert_eq!(rl_holders, 0, "rate-limit shard must drain its holder");
    assert_eq!(rl_shard.concurrency_holder_count(tenant, &q_rl), 0);

    // Sanity: silence unused-import warnings — the gubernator mock isn't
    // referenced after construction; explicitly drop it here for clarity.
    let _ = MockGubernatorClient::new_arc();
}
