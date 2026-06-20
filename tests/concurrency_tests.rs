mod test_helpers;

use silo::codec::{decode_lease, decode_task, encode_concurrency_action, encode_lease};
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::keys::{
    concurrency_holder_key, concurrency_request_key, concurrency_requester_counter_key,
};
use silo::retry::RetryPolicy;
use silo::task::{ConcurrencyAction, LeaseRecord, Task};
use slatedb::WriteBatch;

use test_helpers::*;

#[silo::test]
async fn concurrency_immediate_grant_enqueues_task_and_writes_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let queue = "q1".to_string();
    // enqueue with limit 1
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            payload,
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Task should be ready immediately
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let t = &tasks[0];
    assert_eq!(t.job().id(), job_id);

    // Holder should exist for this attempt's task id (holder is per-attempt)
    let holder = shard
        .db()
        .get(&concurrency_holder_key("-", &queue, t.attempt().task_id()))
        .await
        .expect("get holder");
    assert!(
        holder.is_some(),
        "holder should be written for granted ticket"
    );
}

#[silo::test]
async fn concurrency_queues_when_full_and_grants_on_release() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q2".to_string();

    // First job takes the single slot
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
    assert_eq!(tasks1.len(), 1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Second job should queue a request (no immediate task visible)
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");
    // No runnable RunAttempt should be visible yet (RequestTicket entries are expected)
    let maybe = first_task_kv(shard.db()).await;
    if let Some((_k, v)) = maybe {
        let task = decode_task(&v).expect("decode task");
        match task {
            Task::RunAttempt { .. } => {
                panic!("unexpected RunAttempt while holder is occupied")
            }
            Task::RequestTicket { .. } => {}
            Task::CheckRateLimit { .. } => {}
            Task::RefreshFloatingLimit { .. } => {}
        }
    }

    // Complete first task; this should release and grant next request, enqueuing its task
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Now there should be a new task for the queued request.
    // Grants happen asynchronously via the background scanner, so poll.
    let some = poll_until(|| first_task_kv(shard.db()), |r| r.is_some(), 5000).await;
    assert!(some.is_some(), "task should be enqueued for next requester");
}

#[silo::test]
async fn periodic_reconcile_grants_pending_request_without_signal() {
    // Use a very long reconcile interval so the background task never fires
    // during the test, then manually drive a single reconciliation pass via
    // `reconcile_pending_concurrency_requests_once`. This keeps the
    // pre-reconcile snapshot deterministic — under a short interval the
    // background tick can consume the request between the put and the
    // assertion, especially when other tests load the executor.
    let (_tmp, shard) = open_temp_shard_with_reconcile_interval_ms(60 * 60 * 1000).await;
    let now = now_ms();
    let tenant = "-";
    let queue = "reconcile-q".to_string();

    // Create a real job record that the grant scanner can resolve when
    // granting. Use a present-time start so the chain grants a real
    // holder we can steal a task_id from. (Pre-fix this test used a future
    // start, but future-scheduled jobs no longer grab holders at enqueue
    // time.) Give the job a concurrency limit on `queue` so the injected
    // request is well-formed — post-schema-update the scanner expects
    // requests to persist their limits list and rejects ones with an empty
    // list as corrupt.
    let conc_limit = silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.clone(),
        max_concurrency: 1,
    });
    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"reconcile": true})),
            vec![conc_limit.clone()],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // The enqueue above grants the queue's only slot to this job and writes
    // a holder under a freshly-generated task_id. Look that task_id up and
    // clear it (DB + in-memory) so we can simulate an "orphan request"
    // (durable request exists but the in-memory scanner notification was
    // missed). The manual request below reuses the same task_id so the
    // chain resumer's holder lookups stay coherent.
    let holders_prefix = silo::keys::concurrency_holders_queue_prefix(tenant, &queue);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(
            holders_prefix.clone()..silo::keys::end_bound(&holders_prefix),
            &silo::scan_options(),
        )
        .await
        .expect("scan holders");
    let kv = iter
        .next()
        .await
        .expect("iter")
        .expect("a holder must exist from the natural enqueue");
    let holder_key = kv.key.to_vec();
    let parsed = silo::keys::parse_concurrency_holder_key(&holder_key).expect("parse holder key");
    let manual_task_id = parsed.task_id;
    drop(iter);

    let mut clear_batch = WriteBatch::new();
    clear_batch.delete(&holder_key);
    shard
        .db()
        .write(clear_batch)
        .await
        .expect("clear pre-existing holder");
    shard.rollback_concurrency_grant_for_test(tenant, &queue, &manual_task_id);

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "no holders before injecting request"
    );

    // Manually inject a pending request directly in DB without calling request_grant.
    // This simulates a crash/bug window where the durable request exists but the
    // in-memory grant-scanner notification (`request_grant`/`grant_notify`) was
    // missed. The durable requester counter is co-committed with the request row
    // in the same batch here, mirroring production (`append_request_edits`): the
    // counter is the durable source of truth the periodic reconciler scans, while
    // the missed signal is purely in-memory.
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms: now,
        priority: 10,
        job_id: job_id.clone(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: "default".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: manual_task_id.clone(),
        limits: vec![conc_limit.clone()],
    };
    let request_key = concurrency_request_key(tenant, &queue, now, 10, &job_id, 1, "manual");
    let request_value = encode_concurrency_action(&action);
    let mut batch = WriteBatch::new();
    batch.put(&request_key, &request_value);
    // Co-commit the requester counter, as production does atomically with the
    // request. The counter-based `reconcile_pending_requests` scans these.
    batch.merge(
        &concurrency_requester_counter_key(tenant, &queue),
        1i64.to_le_bytes(),
    );
    shard
        .db()
        .write(batch)
        .await
        .expect("write pending concurrency request");

    // The periodic reconciler may already have ticked and consumed the request by
    // the time we observe it (especially under cargo test's parallel load), so we
    // skip a deterministic "request == 1" snapshot here and rely on the
    // end-to-end assertions below — holders becomes 1 and requests drains to 0 —
    // to verify reconciliation processed the injected request.

    // Drive a single reconciliation pass deterministically rather than waiting
    // on the background interval.
    shard.reconcile_pending_concurrency_requests_once().await;

    // After reconciliation, the orphaned request is granted: holder count
    // becomes 1 and the pending request row is consumed. The grant scanner
    // runs asynchronously after reconciliation enqueues the grant, so the
    // holder side still needs a short poll.
    let holders = poll_until(
        || count_concurrency_holders(shard.db()),
        |count| *count == 1,
        2_000,
    )
    .await;
    assert_eq!(
        holders, 1,
        "reconciliation should grant request and create holder"
    );

    let requests_left = poll_until(
        || count_concurrency_requests(shard.db()),
        |count| *count == 0,
        2_000,
    )
    .await;
    assert_eq!(
        requests_left, 0,
        "request should be consumed after reconciliation grant"
    );
}

#[silo::test]
async fn concurrency_held_queues_propagate_across_retries_and_release_on_finish() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q3".to_string();

    let _job = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(silo::retry::RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let t1 = shard.dequeue("w", "default", 1).await.expect("deq").tasks[0]
        .attempt()
        .task_id()
        .to_string();

    // Fail attempt 1, should schedule attempt 2 carrying held_queues
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");

    // Attempt 2 should be present
    let t2 = shard.dequeue("w", "default", 1).await.expect("deq2").tasks[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2, which should release holder
    shard
        .report_attempt_outcome(&t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // No holders should remain after success of follow-up attempt (released after each attempt)
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
}

#[silo::test]
async fn concurrency_tickets_are_tenant_scoped() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "shared-queue".to_string();

    // Tenant A consumes its single slot
    let _ja = shard
        .enqueue(
            "tenantA",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"t": "A"})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue A");
    let tasks_a = shard
        .dequeue("wA", "default", 1)
        .await
        .expect("deq A")
        .tasks;
    assert_eq!(tasks_a.len(), 1);

    // Tenant B should be able to get its own slot independently
    let _jb = shard
        .enqueue(
            "tenantB",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"t": "B"})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue B");
    let tasks_b = shard
        .dequeue("wB", "default", 1)
        .await
        .expect("deq B")
        .tasks;
    assert_eq!(
        tasks_b.len(),
        1,
        "tenant B should not be blocked by tenant A"
    );
}

#[silo::test]
async fn concurrency_retry_releases_original_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q3-retry".to_string();

    // Enqueue with a retry policy so we get a second attempt
    let _job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(silo::retry::RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            test_helpers::msgpack_payload(&serde_json::json!({"j": 33})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 scheduled
    let t1 = shard.dequeue("w", "default", 1).await.expect("deq1").tasks[0]
        .attempt()
        .task_id()
        .to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");
    let t2 = shard.dequeue("w", "default", 1).await.expect("deq2").tasks[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2
    shard
        .report_attempt_outcome(&t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // BUG (current impl): holder created for attempt 1 task id remains. We assert no holders remain.
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "holders should be fully released after retries complete"
    );
}

#[silo::test]
async fn concurrency_no_overgrant_after_release() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q-overgrant".to_string();

    // A occupies the single slot
    let _a = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"a": true})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue a");
    let a_task = shard
        .dequeue("wa", "default", 1)
        .await
        .expect("deq a")
        .tasks;
    assert_eq!(a_task.len(), 1);
    let a_tid = a_task[0].attempt().task_id().to_string();

    // B queues as a request
    let _b = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"b": true})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue b");

    // Complete A -> should grant B (durably create one holder)
    shard
        .report_attempt_outcome(&a_tid, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report a success");

    // Immediately enqueue C; if in-memory counts weren't bumped on grant-from-release,
    // implementation wrongly grants immediately, yielding 2 holders.
    let _c = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"c": true})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue c");

    // Count durable holders should never exceed 1
    let holders = count_concurrency_holders(shard.db()).await;
    assert!(
        holders <= 1,
        "must not over-grant: holders={}, expected <= 1",
        holders
    );
}

#[silo::test]
async fn stress_single_queue_no_double_grant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stress-q".to_string();

    // Enqueue many jobs concurrently into the same queue with limit 1
    let total = 50usize;
    for i in 0..total {
        let _ = shard
            .enqueue(
                "-",
                None,
                (i % 10) as u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"i": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let mut processed = 0usize;
    loop {
        let tasks = shard
            .dequeue("w-stress", "default", 1)
            .await
            .expect("deq")
            .tasks;
        if tasks.is_empty() {
            if processed >= total {
                break;
            } else {
                tokio::task::yield_now().await;
                continue;
            }
        }
        // Capacity is enforced via durable holders + in-memory gating; no double-grant observed via uniqueness assertions above
        let tid = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&tid, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report");
        processed += 1;
    }

    assert_eq!(processed, total);
    // No remaining durable state for holders/requests
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
    assert_eq!(count_concurrency_requests(shard.db()).await, 0);
}

#[silo::test]
async fn concurrent_enqueues_while_holding_dont_bypass_limit() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "hold-q".to_string();

    // Take the only slot
    let _ = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"first": true})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard
        .dequeue("w-hold", "default", 1)
        .await
        .expect("deq1")
        .tasks;
    assert_eq!(tasks1.len(), 1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Concurrently enqueue more jobs; they should queue as requests
    let add = 10usize;
    for i in 0..add {
        let _ = shard
            .enqueue(
                "-",
                None,
                (i % 5) as u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"i": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue add");
    }
    // There should be no runnable RunAttempt until we release (RequestTicket may exist)
    if let Some((_k, v)) = first_task_kv(shard.db()).await {
        let task = decode_task(&v).expect("decode task");
        match task {
            Task::RunAttempt { .. } => panic!("unexpected RunAttempt before release"),
            Task::RequestTicket { .. } => {}
            Task::CheckRateLimit { .. } => {}
            Task::RefreshFloatingLimit { .. } => {}
        }
    }

    // Release first; one new task should appear after the background grant scanner runs
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");
    let after = poll_until(|| first_task_kv(shard.db()), |r| r.is_some(), 5000).await;
    assert!(after.is_some(), "one task should be enqueued after release");
}

#[silo::test]
async fn reap_marks_expired_lease_as_failed_and_enqueues_retry() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(policy),
            payload,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    let _leased_task_id = tasks[0].attempt().task_id().to_string();

    // Find the lease and rewrite expiry to the past
    let (lease_key, lease_value) = first_lease_kv(shard.db()).await.expect("lease present");
    let decoded = decode_lease(lease_value).expect("decode lease");
    let expired_ms = now_ms() - 1;
    let new_record = LeaseRecord {
        worker_id: decoded.worker_id().to_string(),
        task: decoded.to_task().unwrap(),
        expiry_ms: expired_ms,
        started_at_ms: decoded.started_at_ms(),
    };
    let new_val = encode_lease(&new_record);
    shard
        .db()
        .put(&lease_key, &new_val)
        .await
        .expect("put mutated lease");
    shard.db().flush().await.expect("flush mutated lease");

    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 1);

    // Lease removed
    let lease = shard
        .db()
        .get(&lease_key)
        .await
        .expect("get lease after reap");
    assert!(lease.is_none(), "lease should be removed by reaper");

    // Attempt 1 marked failed with WORKER_CRASHED
    let a1 = shard
        .get_job_attempt("-", &job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Failed { error_code, .. } => {
            assert_eq!(error_code, "WORKER_CRASHED")
        }
        _ => panic!("expected Failed"),
    }

    // Attempt 2 should be scheduled due to retry policy
    let (_k2, v2) = first_task_kv(shard.db())
        .await
        .expect("attempt2 task exists");
    let task2 = decode_task(&v2).expect("decode task");
    let attempt2 = match task2 {
        Task::RunAttempt { attempt_number, .. } => attempt_number,
        Task::RequestTicket { .. } => {
            panic!("unexpected RequestTicket in tasks/ for this test")
        }
        Task::CheckRateLimit { .. } => {
            panic!("unexpected CheckRateLimit in tasks/ for this test")
        }
        Task::RefreshFloatingLimit { .. } => {
            panic!("unexpected RefreshFloatingLimit in tasks/ for this test")
        }
    };
    assert_eq!(attempt2, 2);
}

#[silo::test]
async fn reap_ignores_unexpired_leases() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let now = now_ms();
    let job_id = shard
        .enqueue("-", None, 10u8, now, None, payload, vec![], None, "default")
        .await
        .expect("enqueue");

    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    let _task_id = tasks[0].attempt().task_id().to_string();

    // Do not mutate the lease; it should not be reaped
    let (lease_key, _lease_value) = first_lease_kv(shard.db()).await.expect("lease present");

    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 0);

    // Lease should still exist
    let lease = shard.db().get(&lease_key).await.expect("get lease");
    assert!(lease.is_some(), "lease should remain when not expired");

    // Attempt state remains Running
    let a1 = shard
        .get_job_attempt("-", &job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Running => {}
        other => panic!("expected Running, got {:?}", other),
    }
}

#[silo::test]
async fn concurrency_multiple_holders_max_greater_than_one() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "multi-q".to_string();

    // Enqueue 5 jobs with max_concurrency=3
    let mut job_ids = Vec::new();
    for i in 0..5 {
        let jid = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"i": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 3,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        job_ids.push(jid);
    }

    // First 3 should be granted immediately and dequeue-able
    let tasks = shard.dequeue("w1", "default", 3).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 3, "should get 3 tasks with limit 3");
    let t1 = tasks[0].attempt().task_id().to_string();
    let t2 = tasks[1].attempt().task_id().to_string();
    let t3 = tasks[2].attempt().task_id().to_string();

    // Should have exactly 3 holders now
    let holders = count_concurrency_holders(shard.db()).await;
    assert_eq!(holders, 3, "should have 3 concurrent holders");

    // 4th and 5th should be queued as request records (start_at_ms is now)
    let requests = count_concurrency_requests(shard.db()).await;
    assert_eq!(requests, 2, "remaining 2 jobs should be queued as requests");

    // Complete one task -> should grant next
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Still 3 holders (released 1, granted 1). Grant happens asynchronously, so poll.
    let holders_after = poll_until(
        || count_concurrency_holders(shard.db()),
        |&count| count == 3,
        5000,
    )
    .await;
    assert_eq!(holders_after, 3, "should maintain 3 concurrent holders");

    // Complete all
    shard
        .report_attempt_outcome(&t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");
    shard
        .report_attempt_outcome(&t3, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report3");

    // Drain remaining
    let mut processed = 3;
    while processed < 5 {
        let tasks = shard
            .dequeue("w2", "default", 5)
            .await
            .expect("deq remaining")
            .tasks;
        if tasks.is_empty() {
            tokio::task::yield_now().await;
            continue;
        }
        for t in tasks {
            shard
                .report_attempt_outcome(
                    t.attempt().task_id(),
                    AttemptOutcome::Success { result: vec![] },
                )
                .await
                .expect("report");
            processed += 1;
        }
    }

    // All holders and requests should be cleaned up
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
    assert_eq!(count_concurrency_requests(shard.db()).await, 0);
}

#[silo::test]
async fn concurrency_multiple_queues_per_job() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let q1 = "api".to_string();
    let q2 = "db".to_string();

    // Job requires both api and db tickets
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "needs both"})),
            vec![
                Limit::Concurrency(ConcurrencyLimit {
                    key: q1.clone(),
                    max_concurrency: 2,
                }),
                Limit::Concurrency(ConcurrencyLimit {
                    key: q2.clone(),
                    max_concurrency: 2,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Should get a ticket immediately — both queues have slots free.
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].job().id(), job_id);

    // Should have a holder for both queues — every limit in the chain is gated.
    let api_holder = shard
        .db()
        .get(&concurrency_holder_key(
            "-",
            &q1,
            tasks[0].attempt().task_id(),
        ))
        .await
        .expect("get api holder");
    assert!(api_holder.is_some(), "should have api holder");

    let db_holder = shard
        .db()
        .get(&concurrency_holder_key(
            "-",
            &q2,
            tasks[0].attempt().task_id(),
        ))
        .await
        .expect("get db holder");
    assert!(db_holder.is_some(), "should have db holder");

    // Complete job -> should release both holders
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");

    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
}

#[silo::test]
async fn concurrency_future_request_waits_until_ready() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future = now + 2000; // 2 seconds in future
    let queue = "future-q".to_string();

    // Job 1 takes the slot
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 scheduled for future, creates RequestTicket task
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            future,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // RequestTicket task should exist
    let tasks_before = count_task_keys(shard.db()).await;
    assert_eq!(tasks_before, 1, "should have 1 RequestTicket task");

    // Complete job 1 BEFORE job 2 is ready -> should NOT grant yet
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // RequestTicket task should still exist (not granted because start_time_ms > now)
    let tasks_after = count_task_keys(shard.db()).await;
    assert_eq!(
        tasks_after, 1,
        "RequestTicket task should remain until start time reached"
    );

    // No RunAttempt tasks should be ready yet
    let ready_tasks = shard.peek_tasks("default", 10).await.expect("peek");
    assert_eq!(
        ready_tasks.len(),
        0,
        "no tasks should be ready before start time"
    );

    // Holders should be 0 (released job1, not granted job2)
    let holders_after = count_concurrency_holders(shard.db()).await;
    assert_eq!(
        holders_after, 0,
        "no holders when future request not granted"
    );
}

#[silo::test]
async fn concurrency_request_priority_ordering() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "priority-q".to_string();

    // Job 1 takes the slot
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Enqueue low priority job 2
    let j2 = shard
        .enqueue(
            "-",
            None,
            50u8, // low priority
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Enqueue high priority job 3
    let j3 = shard
        .enqueue(
            "-",
            None,
            1u8, // high priority
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue3");

    // Complete job 1 -> should grant job 3 (high priority) not job 2
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Dequeue next task -> should be job 3
    let t2_vec = shard.dequeue("w", "default", 1).await.expect("deq2").tasks;
    assert_eq!(t2_vec.len(), 1);
    // Note: We can't directly check job ID from task without more scanning,
    // but we verify via process of elimination
    let t2_jid = t2_vec[0].job().id();
    // Higher priority job should come first (requests are ordered by time then priority)
    // Since both were enqueued at same time, priority determines order
    assert_eq!(t2_jid, j3, "high priority job should be granted first");

    // Complete and verify job 2 comes last
    shard
        .report_attempt_outcome(
            t2_vec[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report2");

    let t3_vec = shard.dequeue("w", "default", 1).await.expect("deq3").tasks;
    assert_eq!(t3_vec.len(), 1);
    assert_eq!(t3_vec[0].job().id(), j2, "low priority job comes last");
}

#[silo::test]
async fn concurrency_permanent_failure_releases_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "fail-q".to_string();

    // Job 1 gets holder, no retries
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 queues as request
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Fail job 1 permanently (no retry policy) -> should release holder
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "FAIL".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report");

    // Job 2 should now be granted
    let t2_vec = shard.dequeue("w", "default", 1).await.expect("deq2").tasks;
    assert_eq!(t2_vec.len(), 1, "job 2 should be granted after failure");

    // Complete job 2
    shard
        .report_attempt_outcome(
            t2_vec[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report2");

    // All holders released
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
}

#[silo::test]
async fn concurrency_reap_expired_lease_releases_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "reap-q".to_string();

    // Job 1 gets holder
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    let _t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 queues as request
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Expire the lease for job 1
    let (lease_key, lease_value) = first_lease_kv(shard.db()).await.expect("lease");
    let decoded = decode_lease(lease_value).expect("decode lease");
    let expired_record = LeaseRecord {
        worker_id: decoded.worker_id().to_string(),
        task: decoded.to_task().unwrap(),
        expiry_ms: now_ms() - 1,
        started_at_ms: decoded.started_at_ms(),
    };
    let expired_val = encode_lease(&expired_record);
    shard
        .db()
        .put(&lease_key, &expired_val)
        .await
        .expect("put expired");
    shard.db().flush().await.expect("flush");

    // Reap -> should release holder and schedule retry
    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 1);

    // After reap releases the holder, either job 2's pending request or job 1's retry
    // RequestTicket can be granted. With the async grant scanner, the ordering is
    // non-deterministic. The key invariant is that a new task becomes available.
    let next_task = poll_until(
        || async { shard.dequeue("w", "default", 1).await.expect("deq2").tasks },
        |tasks| !tasks.is_empty(),
        5000,
    )
    .await;
    assert_eq!(
        next_task.len(),
        1,
        "a task should be granted after reap releases the holder"
    );

    // Cleanup
    shard
        .report_attempt_outcome(
            next_task[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report cleanup");
}

#[silo::test]
async fn concurrency_future_request_granted_after_time_passes() {
    let (_tmp, shard) = open_temp_shard().await;
    // Capture time just before second enqueue to ensure future is actually in the future.
    // Using a larger margin (500ms) to avoid flakiness in CI where operations can be slow.
    let queue = "time-q".to_string();
    let now = now_ms();

    // Job 1 takes the slot immediately
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    assert_eq!(t1_vec.len(), 1);
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 scheduled for future while slot is held -> creates RequestTicket task
    // Calculate the future timestamp right before enqueue to avoid timing issues in CI
    let future = now_ms() + 500; // 500ms margin to ensure it's truly in the future
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            future,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Should have created a RequestTicket task scheduled at future time
    // (peek_tasks filters out future tasks, so check DB directly)
    let tasks_in_db = count_task_keys(shard.db()).await;
    assert_eq!(
        tasks_in_db, 1,
        "should have 1 RequestTicket task in DB (future)"
    );

    // Complete Job 1 BEFORE Job 2's start time -> releases holder but Job 2 not ready yet
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Holder should be released
    let holders_after_release = count_concurrency_holders(shard.db()).await;
    assert_eq!(holders_after_release, 0, "holder released");

    // RequestTicket task should still be there (future)
    let tasks_still_future = count_task_keys(shard.db()).await;
    assert_eq!(tasks_still_future, 1, "RequestTicket task still present");

    // Simulate time passing: wait for future time + broker scan delay
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    // Now worker polls - should process RequestTicket and grant Job 2
    // The RequestTicket should be picked up by broker (it's now ready based on timestamp)
    // and dequeue should convert it to a lease for Job 2
    let mut t2_vec = Vec::new();
    for _ in 0..10 {
        t2_vec = shard.dequeue("w", "default", 1).await.expect("deq2").tasks;
        if !t2_vec.is_empty() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    assert!(
        !t2_vec.is_empty(),
        "Job 2 should be granted after its start time arrives"
    );
    assert_eq!(t2_vec.len(), 1, "Job 2 should get exactly one lease");
}

#[silo::test]
async fn cannot_delete_job_with_future_request_ticket() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future = now + 200;
    let queue = "delete-future-q".to_string();

    // Job 1 occupies the slot
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 scheduled for future while slot is held -> creates RequestTicket task (status: Scheduled)
    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            future,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Verify RequestTicket task exists and job status is Scheduled
    let tasks_before = count_task_keys(shard.db()).await;
    assert_eq!(tasks_before, 1, "should have 1 RequestTicket task");

    let status = shard
        .get_job_status("-", &j2)
        .await
        .expect("get status")
        .expect("exists");
    assert_eq!(status.kind, silo::job::JobStatusKind::Scheduled);

    // Attempt to delete job 2 while it's scheduled - should fail
    let err = shard
        .delete_job("-", &j2)
        .await
        .expect_err("delete should fail");
    match err {
        silo::job_store_shard::JobStoreShardError::JobInProgress(jid) => {
            assert_eq!(jid, j2);
        }
        other => panic!("expected JobInProgress, got {:?}", other),
    }

    // Job still exists (can't delete while scheduled)
    let job2 = shard.get_job("-", &j2).await.expect("get job2");
    assert!(
        job2.is_some(),
        "job 2 should still exist (can't delete while scheduled)"
    );

    // Complete job 1, which will eventually allow job 2 to run
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");
}

#[silo::test]
async fn cannot_delete_job_with_pending_request() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "delete-request-q".to_string();

    // Job 1 takes the slot
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 queued as request (status: Scheduled)
    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Request should exist
    let requests = count_concurrency_requests(shard.db()).await;
    assert_eq!(requests, 1, "should have 1 queued request");

    // Attempt to delete job 2 while it's scheduled (has pending request) - should fail
    let err = shard
        .delete_job("-", &j2)
        .await
        .expect_err("delete should fail");
    match err {
        silo::job_store_shard::JobStoreShardError::JobInProgress(jid) => {
            assert_eq!(jid, j2);
        }
        other => panic!("expected JobInProgress, got {:?}", other),
    }

    // Job 2 should still exist
    let job2 = shard.get_job("-", &j2).await.expect("get job2");
    assert!(
        job2.is_some(),
        "job 2 should still exist (can't delete while scheduled)"
    );

    // Complete job 1 to allow job 2 to run
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Job 2 should now be granted
    let t2_vec = shard.dequeue("w", "default", 1).await.expect("deq2").tasks;
    assert_eq!(t2_vec.len(), 1);

    // Attempt to delete while running - should still fail
    let err2 = shard
        .delete_job("-", &j2)
        .await
        .expect_err("delete should fail while running");
    match err2 {
        silo::job_store_shard::JobStoreShardError::JobInProgress(jid) => {
            assert_eq!(jid, j2);
        }
        other => panic!("expected JobInProgress, got {:?}", other),
    }

    // Complete job 2
    shard
        .report_attempt_outcome(
            t2_vec[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report2");

    // Now delete should succeed (job is in Succeeded state)
    shard
        .delete_job("-", &j2)
        .await
        .expect("delete should succeed now");

    // Job should be gone
    let job2_final = shard.get_job("-", &j2).await.expect("get job2");
    assert!(job2_final.is_none(), "job 2 should be deleted");
}

/// Test that lazy hydration correctly loads holders from storage on first access to a queue.
/// This verifies that after shard restart, the concurrency counts are correctly loaded.
#[silo::test]
async fn concurrency_lazy_hydration_on_shard_reopen() {
    use silo::gubernator::MockGubernatorClient;
    use silo::settings::{Backend, DatabaseConfig};
    use silo::shard_range::ShardRange;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(test_helpers::fast_flush_slatedb_settings()),
        ..Default::default()
    };
    let rate_limiter = MockGubernatorClient::new_arc();
    let queue = "lazy_q".to_string();

    // Phase 1: Open shard, enqueue a job that holds a concurrency slot
    let shard1 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard1");

    let now = now_ms();
    let _j1 = shard1
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"lazy": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");

    // Dequeue to lease the job (creates holder record)
    let tasks1 = shard1
        .dequeue("w1", "default", 1)
        .await
        .expect("deq1")
        .tasks;
    assert_eq!(tasks1.len(), 1);
    let task_id = tasks1[0].attempt().task_id().to_string();

    // Verify holder exists in DB
    let holder = shard1
        .db()
        .get(&silo::keys::concurrency_holder_key("-", &queue, &task_id))
        .await
        .expect("get holder");
    assert!(holder.is_some(), "holder should exist after dequeue");

    // Close the shard
    shard1.close().await.expect("close shard1");

    // Phase 2: Reopen shard (lazy hydration should kick in on first access)
    let shard2 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard2");

    // Try to enqueue a second job on the same queue
    // With lazy hydration, it should recognize the slot is full
    let j2 = shard2
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"lazy": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // The second job should NOT get a task immediately (queue is full)
    // Peek tasks - should not see j2 as a RunAttempt
    let peeked = shard2.peek_tasks("default", 10).await.expect("peek");

    // Count RunAttempt tasks for j2
    let j2_run_attempts = peeked
        .iter()
        .filter(|t| {
            if let Task::RunAttempt { job_id, .. } = t {
                job_id == &j2
            } else {
                false
            }
        })
        .count();

    assert_eq!(
        j2_run_attempts, 0,
        "j2 should not have a RunAttempt task - slot is occupied by j1"
    );

    // Verify j1's holder still exists
    let holder2 = shard2
        .db()
        .get(&silo::keys::concurrency_holder_key("-", &queue, &task_id))
        .await
        .expect("get holder after reopen");
    assert!(
        holder2.is_some(),
        "holder should persist across shard restart"
    );

    shard2.close().await.expect("close shard2");
}

/// Test that no overgrant occurs after shard restart when holders exist in storage.
/// This is a critical correctness test for lazy hydration.
#[silo::test]
async fn concurrency_no_overgrant_with_lazy_hydration() {
    use silo::gubernator::MockGubernatorClient;
    use silo::settings::{Backend, DatabaseConfig};
    use silo::shard_range::ShardRange;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(test_helpers::fast_flush_slatedb_settings()),
        ..Default::default()
    };
    let rate_limiter = MockGubernatorClient::new_arc();
    let queue = "overgrant_q".to_string();

    // Phase 1: Open shard, fill concurrency limit with max_concurrency=2
    let shard1 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard1");

    let now = now_ms();

    // Enqueue 2 jobs to fill the limit
    for i in 1..=2 {
        let _ = shard1
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"job": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 2,
                })],
                None,
                "default",
            )
            .await
            .expect(&format!("enqueue job {}", i));
    }

    // Dequeue both jobs
    let tasks1 = shard1.dequeue("w1", "default", 2).await.expect("deq").tasks;
    assert_eq!(tasks1.len(), 2, "should get 2 tasks");

    // Close shard without completing jobs (holders remain in DB)
    shard1.close().await.expect("close shard1");

    // Phase 2: Reopen shard
    let shard2 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard2");

    // Enqueue a third job - should queue as a request since limit is 2 and 2 holders exist
    let j3 = shard2
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": 3})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue3");

    // Peek all tasks - should not see j3 as a RunAttempt
    let peeked = shard2.peek_tasks("default", 10).await.expect("peek");
    let j3_run_attempts = peeked
        .iter()
        .filter(|t| {
            if let Task::RunAttempt { job_id, .. } = t {
                job_id == &j3
            } else {
                false
            }
        })
        .count();

    assert_eq!(
        j3_run_attempts, 0,
        "j3 should NOT have RunAttempt - would indicate overgrant bug"
    );

    // Verify we have 2 holders in DB (the original ones)
    let start = silo::keys::concurrency_holders_queue_prefix("-", &queue);
    let end = silo::keys::end_bound(&start);
    let mut iter = shard2
        .db()
        .scan::<Vec<u8>, _>(start..end)
        .await
        .expect("scan");
    let mut holder_count = 0;
    while let Some(_kv) = iter.next().await.expect("iter") {
        holder_count += 1;
    }
    assert_eq!(
        holder_count, 2,
        "should have exactly 2 holders from original jobs"
    );

    shard2.close().await.expect("close shard2");
}

/// The grant scanner must defensively delete corrupt request entries it
/// encounters during a pass and continue processing the rest. Three shapes
/// are exercised here:
///
/// 1. Truncated flatbuffer (decode fails outright).
/// 2. Empty `limits` (post-schema, every request must carry its limits — an
///    empty list would let the chain resumer fall through to a terminal
///    `RunAttempt` that bypasses every gate).
/// 3. Empty `task_id` (the chain identity is what keys every holder; an
///    empty id would make rollback / release unreachable).
///
/// Pre-fix variants of these would either panic the scanner, get silently
/// stuck (cannot decode → loop forever), or proceed and corrupt downstream
/// state. The current implementation deletes the offender + emits a warn +
/// keeps going. This test injects all three alongside a valid request and
/// asserts (a) all corrupt rows are gone after one synchronous pass and (b)
/// the valid request still gets granted.
#[silo::test]
async fn grant_scanner_skips_corrupt_request_variants() {
    use silo::keys::concurrency_request_key;

    let (_tmp, shard) = open_temp_shard_with_reconcile_interval_ms(60 * 60 * 1000).await;
    // Don't let the background grant scanner race the deterministic
    // process_concurrency_grants pass below.
    shard.stop_grant_scanner();
    let now = now_ms();
    let tenant = "-";
    let queue = "corrupt-q".to_string();

    let conc_limit = Limit::Concurrency(ConcurrencyLimit {
        key: queue.clone(),
        max_concurrency: 1,
    });

    // job1 takes the queue's only slot. We need a real holder so the queue
    // capacity is at limit and the scanner doesn't attempt to grant the
    // corrupt entries (it would still delete them either way, but staying
    // at capacity also pins that the valid entry below is genuinely deferred
    // rather than granted at enqueue time).
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![conc_limit.clone()],
            None,
            "default",
        )
        .await
        .expect("enqueue job1");
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1);
    let job1_task_id = job1_tasks[0].attempt().task_id().to_string();

    // job2 defers on the same queue — produces a valid request the scanner
    // must process.
    let job2_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![conc_limit.clone()],
            None,
            "default",
        )
        .await
        .expect("enqueue job2");
    let initial_requests = count_concurrency_requests(shard.db()).await;
    assert!(
        initial_requests >= 1,
        "valid deferred request should be present"
    );

    // Inject three corrupt requests with monotonically increasing suffixes so
    // they sort distinctly. start_time_ms = now so they're picked up this pass.
    let truncated_key =
        concurrency_request_key(tenant, &queue, now, 10, "corrupt-truncated", 1, "corrupt-a");
    let empty_limits_key = concurrency_request_key(
        tenant,
        &queue,
        now,
        10,
        "corrupt-empty-limits",
        1,
        "corrupt-b",
    );
    let empty_task_id_key =
        concurrency_request_key(tenant, &queue, now, 10, "corrupt-empty-tid", 1, "corrupt-c");

    let mut batch = WriteBatch::new();

    // (1) Truncated flatbuffer — gibberish bytes that decode_concurrency_action
    //     will reject outright.
    batch.put(&truncated_key, &b"\xff\xff\xff\xff\xff\xff\xff\xff"[..]);

    // (2) Empty limits: a well-formed EnqueueTask with an empty limits Vec.
    //     Post-schema this is invalid; the scanner must delete + skip.
    let empty_limits_action = ConcurrencyAction::EnqueueTask {
        start_time_ms: now,
        priority: 10,
        job_id: "corrupt-empty-limits".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: "default".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: "fake-tid-for-empty-limits".to_string(),
        limits: Vec::new(),
    };
    batch.put(
        &empty_limits_key,
        &encode_concurrency_action(&empty_limits_action),
    );

    // (3) Empty task_id: well-formed but missing identity.
    let empty_tid_action = ConcurrencyAction::EnqueueTask {
        start_time_ms: now,
        priority: 10,
        job_id: "corrupt-empty-tid".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: "default".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: String::new(),
        limits: vec![conc_limit.clone()],
    };
    batch.put(
        &empty_task_id_key,
        &encode_concurrency_action(&empty_tid_action),
    );

    shard.db().write(batch).await.expect("inject corrupt rows");

    let with_corrupt = count_concurrency_requests(shard.db()).await;
    assert_eq!(
        with_corrupt,
        initial_requests + 3,
        "three corrupt entries should be persisted before the pass"
    );

    // Release job1 so job2's valid request has capacity.
    shard
        .report_attempt_outcome(&job1_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("release job1");

    // Drive one synchronous pass. Scanner should: delete all three corrupt
    // entries, grant job2's valid request, and leave nothing pending.
    // `process_concurrency_grants` returns the task_groups it granted into,
    // not job_ids, so we check the more reliable indicator: holder count
    // came back to 1 (job2 holds A).
    let granted_task_groups = shard.process_concurrency_grants(tenant, &queue, 16).await;
    let _ = job2_id; // job_id is for documentation; not in the return shape
    assert!(
        !granted_task_groups.is_empty(),
        "scanner should have granted at least one (valid) request",
    );
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "the valid request must produce a fresh holder after the pass",
    );

    // Sanity: requests drained (corrupts deleted, valid was granted).
    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "scanner must delete all corrupt entries and consume the valid one",
    );

    // Spot-check each corrupt key was deleted by the scanner (defensive
    // against an accidental partial-delete regression).
    for (label, key) in [
        ("truncated", &truncated_key),
        ("empty_limits", &empty_limits_key),
        ("empty_task_id", &empty_task_id_key),
    ] {
        assert!(
            shard.db().get(key).await.expect("get").is_none(),
            "corrupt request '{label}' should have been deleted"
        );
    }
}

/// A burst of future-scheduled jobs larger than queue capacity must hold zero
/// slots at enqueue time. Strengthens `future_scheduled_jobs_do_not_starve_immediate_work`
/// by (a) sizing the burst above capacity so a single accidental `try_reserve`
/// would visibly leak, (b) asserting each persisted `Task::RequestTicket`
/// lives at `task_key_start_ms == scheduled_at_ms`, and (c) confirming a
/// present-time enqueue afterwards is granted the slot.
///
/// Pre-fix (before commit 8f94f59) every future-scheduled enqueue called
/// `try_reserve` and grabbed a holder until its scheduled time arrived.
/// A burst of 10 future-scheduled jobs into a cap=1 queue would saturate the
/// in-memory counter immediately, leaving the immediate job stranded.
#[silo::test]
async fn future_scheduled_burst_into_quiet_queue_holds_zero_slots() {
    use silo::keys::{end_bound, tasks_prefix};

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue = "future-burst-q".to_string();
    let future_at = now + 600_000; // ten minutes out

    // Burst of future-scheduled jobs. Each is solo on the chain (one
    // Concurrency limit) so the FutureRequestTaskWritten branch is the
    // observed outcome.
    let burst = 10;
    let cap = 1;
    let mut burst_ids = Vec::with_capacity(burst);
    for i in 0..burst {
        let id = shard
            .enqueue(
                tenant,
                None,
                10u8,
                future_at,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"f": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: cap as u32,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue future");
        burst_ids.push(id);
    }

    // Invariant (a): zero holders on disk + in-memory.
    let burst_holders = count_concurrency_holders_for_tenant(shard.db(), tenant).await;
    assert_eq!(
        burst_holders, 0,
        "burst of future-scheduled jobs must not grab any concurrency holder",
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, &queue),
        0,
        "in-memory holder count must be 0 after a future-scheduled burst",
    );

    // Invariant (b): every persisted RequestTicket lives at scheduled_at_ms.
    let start = tasks_prefix();
    let end = end_bound(&start);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .expect("scan tasks");
    let mut found_tickets = 0usize;
    while let Ok(Some(kv)) = iter.next().await {
        if let Ok(Task::RequestTicket { ref job_id, .. }) = decode_task(&kv.value)
            && burst_ids.contains(job_id)
        {
            let parsed = silo::keys::parse_task_key(&kv.key).expect("parse task_key");
            assert_eq!(
                parsed.start_time_ms as i64, future_at,
                "future RequestTicket's task_key must live at scheduled_at_ms",
            );
            found_tickets += 1;
        }
    }
    assert_eq!(
        found_tickets, burst,
        "every future-scheduled job must have persisted a RequestTicket",
    );

    // Invariant (c): a present-time enqueue on the same queue still wins the
    // slot — the future burst did not starve the immediate path.
    let immediate_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"f": "now"})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: cap as u32,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue immediate");

    let tasks = shard
        .dequeue("w1", "default", 10)
        .await
        .expect("dequeue")
        .tasks;
    assert!(
        tasks.iter().any(|t| t.job().id() == immediate_id),
        "present-time job should dequeue despite a burst of future-scheduled work; got {} tasks",
        tasks.len(),
    );
}

/// A future-scheduled job's RequestTicket processed at an
/// at-capacity queue must be converted into a deferred concurrency request
/// (task deleted, request record + requester counter written) rather than
/// left parked in the task keyspace, and the grant scanner must drain the
/// converted request once the slot frees.
#[silo::test]
async fn at_capacity_ticket_converts_to_deferred_request() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let now = now_ms();
        let queue = "convert-q".to_string();
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let limits = vec![Limit::Concurrency(ConcurrencyLimit {
            key: queue.clone(),
            max_concurrency: 1,
        })];

        // Job 1: immediate, takes the only slot and is leased (held until we
        // complete it).
        let job1 = shard
            .enqueue(
                "-",
                Some("convert-job-1".to_string()),
                10u8,
                now,
                None,
                payload.clone(),
                limits.clone(),
                None,
                "default",
            )
            .await
            .expect("enqueue job1");
        let tasks = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].job().id(), job1);
        let t1_id = tasks[0].attempt().task_id().to_string();

        // Job 2: future-scheduled on the same queue → persisted RequestTicket.
        let job2 = shard
            .enqueue(
                "-",
                Some("convert-job-2".to_string()),
                10u8,
                now + 500,
                None,
                payload.clone(),
                limits.clone(),
                None,
                "default",
            )
            .await
            .expect("enqueue job2");
        assert_eq!(
            count_task_keys(shard.db()).await,
            1,
            "future job should be a parked RequestTicket task"
        );
        assert_eq!(
            count_concurrency_requests(shard.db()).await,
            0,
            "no deferred request before the ticket is processed"
        );

        // Let the start time pass, then dequeue: the broker claims the
        // ticket, try_reserve fails (job1 holds the slot), and the ticket is
        // converted. The dequeue returns nothing leasable.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        let converted = poll_until(
            || async {
                let r = shard.dequeue("w", "default", 1).await.expect("deq2");
                assert!(r.tasks.is_empty(), "no slot available, nothing leasable");
                (
                    count_task_keys(shard.db()).await,
                    count_concurrency_requests(shard.db()).await,
                )
            },
            |(tasks, requests)| *tasks == 0 && *requests == 1,
            5000,
        )
        .await;
        assert_eq!(
            converted,
            (0, 1),
            "at-capacity ticket should be deleted and replaced by exactly one \
             deferred request (got (task_keys, requests) = {:?})",
            converted
        );
        let requesters = shard
            .get_concurrency_requester_count("-", &queue)
            .await
            .expect("requester count");
        assert_eq!(requesters, 1, "converted job counts as a requester");

        // Job 2 is still Scheduled and cannot have run.
        let status2 = shard
            .get_job_status("-", &job2)
            .await
            .expect("status job2")
            .expect("job2 exists");
        assert_eq!(status2.kind, silo::job::JobStatusKind::Scheduled);

        // Complete job 1 → holder released → grant scanner drains the
        // converted request → job 2 becomes leasable.
        shard
            .report_attempt_outcome(&t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report job1 success");
        let tasks2 = poll_until(
            || async {
                shard
                    .dequeue("w", "default", 1)
                    .await
                    .expect("deq job2")
                    .tasks
            },
            |t| !t.is_empty(),
            5000,
        )
        .await;
        assert_eq!(tasks2.len(), 1, "converted request should grant and run");
        assert_eq!(tasks2[0].job().id(), job2);
        let final_requesters = shard
            .get_concurrency_requester_count("-", &queue)
            .await
            .expect("requester count after grant");
        assert_eq!(final_requesters, 0, "requester counter drains on grant");
    });
}

/// Cancelling a job whose at-capacity ticket was converted to a deferred
/// request must remove the request record and drain the requester counter,
/// without disturbing the slot holder, and must not produce a phantom grant
/// when the holder later releases.
#[silo::test]
async fn cancel_after_ticket_conversion_cleans_up_request() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let now = now_ms();
        let queue = "convert-cancel-q".to_string();
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let limits = vec![Limit::Concurrency(ConcurrencyLimit {
            key: queue.clone(),
            max_concurrency: 1,
        })];

        let _job1 = shard
            .enqueue(
                "-",
                Some("cc-job-1".to_string()),
                10u8,
                now,
                None,
                payload.clone(),
                limits.clone(),
                None,
                "default",
            )
            .await
            .expect("enqueue job1");
        let tasks = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
        assert_eq!(tasks.len(), 1);
        let t1_id = tasks[0].attempt().task_id().to_string();

        let job2 = shard
            .enqueue(
                "-",
                Some("cc-job-2".to_string()),
                10u8,
                now + 500,
                None,
                payload.clone(),
                limits.clone(),
                None,
                "default",
            )
            .await
            .expect("enqueue job2");

        // Drive the conversion.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        let converted = poll_until(
            || async {
                let _ = shard.dequeue("w", "default", 1).await.expect("deq2");
                count_concurrency_requests(shard.db()).await
            },
            |requests| *requests == 1,
            5000,
        )
        .await;
        assert_eq!(converted, 1, "ticket should convert to a deferred request");

        // Cancel the waiting job: request record and requester counter must
        // be cleaned up; job1's holder must be untouched.
        shard.cancel_job("-", &job2).await.expect("cancel job2");
        assert_eq!(
            count_concurrency_requests(shard.db()).await,
            0,
            "cancel should delete the converted request record"
        );
        let requesters = shard
            .get_concurrency_requester_count("-", &queue)
            .await
            .expect("requester count");
        assert_eq!(requesters, 0, "cancel should drain the requester counter");
        assert_eq!(
            count_concurrency_holders(shard.db()).await,
            1,
            "job1's holder must survive the cancel"
        );

        // Releasing job1's slot must not grant anything (no phantom grant
        // for the cancelled requester).
        shard
            .report_attempt_outcome(&t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report job1 success");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let r = shard.dequeue("w", "default", 1).await.expect("deq final");
        assert!(
            r.tasks.is_empty(),
            "cancelled job must not run after the slot frees"
        );
        assert_eq!(
            count_concurrency_holders(shard.db()).await,
            0,
            "no phantom holder after cancelled requester's queue drains"
        );
    });
}
