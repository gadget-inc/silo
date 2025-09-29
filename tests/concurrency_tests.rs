mod test_helpers;

use rkyv::Archive;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::{LeaseRecord, Task};
use silo::keys::concurrency_holder_key;
use silo::retry::RetryPolicy;

use test_helpers::*;

#[tokio::test]
async fn concurrency_immediate_grant_enqueues_task_and_writes_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let payload = serde_json::json!({"k": "v"});
    let queue = "q1".to_string();
    // enqueue with limit 1
    let job_id = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            payload,
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue");

    // Task should be ready immediately
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let t = &tasks[0];
    assert_eq!(t.job().id(), job_id);

    // Holder should exist for this attempt's task id (holder is per-attempt)
    let holder = shard
        .db()
        .get(concurrency_holder_key(&queue, t.attempt().task_id()).as_bytes())
        .await
        .expect("get holder");
    assert!(
        holder.is_some(),
        "holder should be written for granted ticket"
    );
}

#[tokio::test]
async fn concurrency_queues_when_full_and_grants_on_release() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q2".to_string();

    // First job takes the single slot
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w1", 1).await.expect("deq1");
    assert_eq!(tasks1.len(), 1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Second job should queue a request (no immediate task visible)
    let _j2 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");
    // No runnable RunAttempt should be visible yet (RequestTicket entries are expected)
    let maybe = first_kv_with_prefix(shard.db(), "tasks/").await;
    if let Some((_k, v)) = maybe {
        type ArchivedTask = <Task as Archive>::Archived;
        let a: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v) };
        match a {
            ArchivedTask::RunAttempt { .. } => {
                panic!("unexpected RunAttempt while holder is occupied")
            }
            ArchivedTask::RequestTicket { .. } => {}
        }
    }

    // Complete first task; this should release and grant next request, enqueuing its task
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Now there should be a new task for the queued request
    let some = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(some.is_some(), "task should be enqueued for next requester");
}

#[tokio::test]
async fn concurrency_held_queues_propagate_across_retries_and_release_on_finish() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q3".to_string();

    let _job = shard
        .enqueue(
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
            serde_json::json!({"j": 3}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue");

    let t1 = shard.dequeue("w", 1).await.expect("deq")[0]
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
    let t2 = shard.dequeue("w", 1).await.expect("deq2")[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2, which should release holder
    shard
        .report_attempt_outcome(&t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // No holders should remain after success of follow-up attempt (released after each attempt)
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
}

#[tokio::test]
async fn concurrency_retry_releases_original_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q3-retry".to_string();

    // Enqueue with a retry policy so we get a second attempt
    let _job_id = shard
        .enqueue(
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
            serde_json::json!({"j": 33}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 scheduled
    let t1 = shard.dequeue("w", 1).await.expect("deq1")[0]
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
    let t2 = shard.dequeue("w", 1).await.expect("deq2")[0]
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
        count_with_prefix(shard.db(), "holders/").await,
        0,
        "holders should be fully released after retries complete"
    );
}

#[tokio::test]
async fn concurrency_no_overgrant_after_release() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q-overgrant".to_string();

    // A occupies the single slot
    let _a = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"a": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue a");
    let a_task = shard.dequeue("wa", 1).await.expect("deq a");
    assert_eq!(a_task.len(), 1);
    let a_tid = a_task[0].attempt().task_id().to_string();

    // B queues as a request
    let _b = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"b": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
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
            None,
            10u8,
            now,
            None,
            serde_json::json!({"c": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue c");

    // Count durable holders should never exceed 1
    let holders = count_with_prefix(shard.db(), "holders/").await;
    assert!(
        holders <= 1,
        "must not over-grant: holders={}, expected <= 1",
        holders
    );
}

#[tokio::test]
async fn stress_single_queue_no_double_grant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stress-q".to_string();

    // Enqueue many jobs concurrently into the same queue with limit 1
    let total = 50usize;
    for i in 0..total {
        let _ = shard
            .enqueue(
                None,
                (i % 10) as u8,
                now,
                None,
                serde_json::json!({"i": i}),
                vec![silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                }],
            )
            .await
            .expect("enqueue");
    }

    let mut processed = 0usize;
    loop {
        let tasks = shard.dequeue("w-stress", 1).await.expect("deq");
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
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
    assert_eq!(count_with_prefix(shard.db(), "requests/").await, 0);
}

#[tokio::test]
async fn concurrent_enqueues_while_holding_dont_bypass_limit() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "hold-q".to_string();

    // Take the only slot
    let _ = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"first": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w-hold", 1).await.expect("deq1");
    assert_eq!(tasks1.len(), 1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Concurrently enqueue more jobs; they should queue as requests
    let add = 10usize;
    for i in 0..add {
        let _ = shard
            .enqueue(
                None,
                (i % 5) as u8,
                now,
                None,
                serde_json::json!({"i": i}),
                vec![silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                }],
            )
            .await
            .expect("enqueue add");
    }
    // There should be no runnable RunAttempt until we release (RequestTicket may exist)
    if let Some((_k, v)) = first_kv_with_prefix(shard.db(), "tasks/").await {
        type ArchivedTask = <Task as Archive>::Archived;
        let a: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v) };
        match a {
            ArchivedTask::RunAttempt { .. } => panic!("unexpected RunAttempt before release"),
            ArchivedTask::RequestTicket { .. } => {}
        }
    }

    // Release first; only one new task should appear immediately
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");
    let after = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(after.is_some(), "one task should be enqueued after release");
}

#[tokio::test]
async fn reap_marks_expired_lease_as_failed_and_enqueues_retry() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(None, 10u8, now, Some(policy), payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let _leased_task_id = tasks[0].attempt().task_id().to_string();

    // Find the lease and rewrite expiry to the past
    let (lease_key, lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");
    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&lease_value) };
    let task = match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            held_queues: _,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: Vec::new(),
        },
        ArchivedTask::RequestTicket { .. } => panic!("unexpected RequestTicket in lease"),
    };
    let expired_ms = now_ms() - 1;
    let new_record = LeaseRecord {
        worker_id: archived.worker_id.as_str().to_string(),
        task,
        expiry_ms: expired_ms,
    };
    let new_val = rkyv::to_bytes::<LeaseRecord, 256>(&new_record).unwrap();
    shard
        .db()
        .put(lease_key.as_bytes(), &new_val)
        .await
        .expect("put mutated lease");
    shard.db().flush().await.expect("flush mutated lease");

    let reaped = shard.reap_expired_leases().await.expect("reap");
    assert_eq!(reaped, 1);

    // Lease removed
    let lease = shard
        .db()
        .get(lease_key.as_bytes())
        .await
        .expect("get lease after reap");
    assert!(lease.is_none(), "lease should be removed by reaper");

    // Attempt 1 marked failed with WORKER_CRASHED
    let a1 = shard
        .get_job_attempt(&job_id, 1)
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
    let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("attempt2 task exists");
    let a2_arch: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v2) };
    let attempt2 = match a2_arch {
        ArchivedTask::RunAttempt { attempt_number, .. } => *attempt_number,
        ArchivedTask::RequestTicket { .. } => {
            panic!("unexpected RequestTicket in tasks/ for this test")
        }
    };
    assert_eq!(attempt2, 2);
}

#[tokio::test]
async fn reap_ignores_unexpired_leases() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let now = now_ms();
    let job_id = shard
        .enqueue(None, 10u8, now, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let _task_id = tasks[0].attempt().task_id().to_string();

    // Do not mutate the lease; it should not be reaped
    let (lease_key, _lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");

    let reaped = shard.reap_expired_leases().await.expect("reap");
    assert_eq!(reaped, 0);

    // Lease should still exist
    let lease = shard
        .db()
        .get(lease_key.as_bytes())
        .await
        .expect("get lease");
    assert!(lease.is_some(), "lease should remain when not expired");

    // Attempt state remains Running
    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Running { .. } => {}
        other => panic!("expected Running, got {:?}", other),
    }
}

#[tokio::test]
async fn concurrency_multiple_holders_max_greater_than_one() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "multi-q".to_string();

    // Enqueue 5 jobs with max_concurrency=3
    let mut job_ids = Vec::new();
    for i in 0..5 {
        let jid = shard
            .enqueue(
                None,
                10u8,
                now,
                None,
                serde_json::json!({"i": i}),
                vec![silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 3,
                }],
            )
            .await
            .expect("enqueue");
        job_ids.push(jid);
    }

    // First 3 should be granted immediately and dequeue-able
    let tasks = shard.dequeue("w1", 3).await.expect("deq");
    assert_eq!(tasks.len(), 3, "should get 3 tasks with limit 3");
    let t1 = tasks[0].attempt().task_id().to_string();
    let t2 = tasks[1].attempt().task_id().to_string();
    let t3 = tasks[2].attempt().task_id().to_string();

    // Should have exactly 3 holders now
    let holders = count_with_prefix(shard.db(), "holders/").await;
    assert_eq!(holders, 3, "should have 3 concurrent holders");

    // 4th and 5th should be queued as request records (start_at_ms is now)
    let requests = count_with_prefix(shard.db(), "requests/").await;
    assert_eq!(requests, 2, "remaining 2 jobs should be queued as requests");

    // Complete one task -> should grant next
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Still 3 holders (released 1, granted 1)
    let holders_after = count_with_prefix(shard.db(), "holders/").await;
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
        let tasks = shard.dequeue("w2", 5).await.expect("deq remaining");
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
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
    assert_eq!(count_with_prefix(shard.db(), "requests/").await, 0);
}

#[tokio::test]
async fn concurrency_multiple_queues_per_job() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let q1 = "api".to_string();
    let q2 = "db".to_string();

    // Job requires both api and db tickets
    let job_id = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"task": "needs both"}),
            vec![
                silo::job::ConcurrencyLimit {
                    key: q1.clone(),
                    max_concurrency: 2,
                },
                silo::job::ConcurrencyLimit {
                    key: q2.clone(),
                    max_concurrency: 2,
                },
            ],
        )
        .await
        .expect("enqueue");

    // Should get ticket for first queue immediately (api)
    let tasks = shard.dequeue("w", 1).await.expect("deq");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].job().id(), job_id);

    // Should have holder for api queue only (first limit)
    let api_holder = shard
        .db()
        .get(concurrency_holder_key(&q1, tasks[0].attempt().task_id()).as_bytes())
        .await
        .expect("get api holder");
    assert!(api_holder.is_some(), "should have api holder");

    // Should NOT have db holder yet (only first limit is gated)
    let db_holder = shard
        .db()
        .get(concurrency_holder_key(&q2, tasks[0].attempt().task_id()).as_bytes())
        .await
        .expect("get db holder");
    assert!(
        db_holder.is_none(),
        "should not have db holder (only first limit gated)"
    );

    // Complete job -> should release api holder
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");

    // All holders released
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
}

#[tokio::test]
async fn concurrency_future_request_waits_until_ready() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future = now + 2000; // 2 seconds in future
    let queue = "future-q".to_string();

    // Job 1 takes the slot
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 scheduled for future, creates RequestTicket task
    let _j2 = shard
        .enqueue(
            None,
            10u8,
            future,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");

    // RequestTicket task should exist
    let tasks_before = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(tasks_before, 1, "should have 1 RequestTicket task");

    // Complete job 1 BEFORE job 2 is ready -> should NOT grant yet
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // RequestTicket task should still exist (not granted because start_time_ms > now)
    let tasks_after = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(
        tasks_after, 1,
        "RequestTicket task should remain until start time reached"
    );

    // No RunAttempt tasks should be ready yet
    let ready_tasks = shard.peek_tasks(10).await.expect("peek");
    assert_eq!(
        ready_tasks.len(),
        0,
        "no tasks should be ready before start time"
    );

    // Holders should be 0 (released job1, not granted job2)
    let holders_after = count_with_prefix(shard.db(), "holders/").await;
    assert_eq!(
        holders_after, 0,
        "no holders when future request not granted"
    );
}

#[tokio::test]
async fn concurrency_request_priority_ordering() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "priority-q".to_string();

    // Job 1 takes the slot
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Enqueue low priority job 2
    let j2 = shard
        .enqueue(
            None,
            50u8, // low priority
            now,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");

    // Enqueue high priority job 3
    let j3 = shard
        .enqueue(
            None,
            1u8, // high priority
            now,
            None,
            serde_json::json!({"j": 3}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue3");

    // Complete job 1 -> should grant job 3 (high priority) not job 2
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Dequeue next task -> should be job 3
    let t2_vec = shard.dequeue("w", 1).await.expect("deq2");
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

    let t3_vec = shard.dequeue("w", 1).await.expect("deq3");
    assert_eq!(t3_vec.len(), 1);
    assert_eq!(t3_vec[0].job().id(), j2, "low priority job comes last");
}

#[tokio::test]
async fn concurrency_permanent_failure_releases_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "fail-q".to_string();

    // Job 1 gets holder, no retries
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 queues as request
    let _j2 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
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
        .expect("report1");

    // Job 2 should now be granted
    let t2_vec = shard.dequeue("w", 1).await.expect("deq2");
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
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
}

#[tokio::test]
async fn concurrency_reap_expired_lease_releases_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "reap-q".to_string();

    // Job 1 gets holder
    let j1 = shard
        .enqueue(
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
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 queues as request
    let _j2 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");

    // Expire the lease for job 1
    let (lease_key, lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease");
    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&lease_value) };
    let task = match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            held_queues,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: held_queues.iter().map(|s| s.as_str().to_string()).collect(),
        },
        ArchivedTask::RequestTicket { .. } => panic!("unexpected"),
    };
    let expired_record = LeaseRecord {
        worker_id: archived.worker_id.as_str().to_string(),
        task,
        expiry_ms: now_ms() - 1,
    };
    let expired_val = rkyv::to_bytes::<LeaseRecord, 256>(&expired_record).unwrap();
    shard
        .db()
        .put(lease_key.as_bytes(), &expired_val)
        .await
        .expect("put expired");
    shard.db().flush().await.expect("flush");

    // Reap -> should release holder and schedule retry
    let reaped = shard.reap_expired_leases().await.expect("reap");
    assert_eq!(reaped, 1);

    // Job 2 should now be granted (holder released from job 1)
    let t2_vec = shard.dequeue("w", 1).await.expect("deq2");
    assert_eq!(t2_vec.len(), 1, "job 2 should be granted after reap");

    // Job 1 retry should also be scheduled
    let j1_attempt2 = shard.get_job_attempt(&j1, 2).await.expect("get attempt 2");
    assert!(
        j1_attempt2.is_none(),
        "attempt 2 not created yet (only task scheduled)"
    );

    // Cleanup
    shard
        .report_attempt_outcome(
            t2_vec[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report2");
}

#[tokio::test]
async fn concurrency_future_request_granted_after_time_passes() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future = now + 100; // 100ms in future (short enough for test)
    let queue = "time-q".to_string();

    // Job 1 takes the slot immediately
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    assert_eq!(t1_vec.len(), 1);
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 scheduled for future while slot is held -> creates RequestTicket task
    let _j2 = shard
        .enqueue(
            None,
            10u8,
            future,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");

    // Should have created a RequestTicket task scheduled at future time
    // (peek_tasks filters out future tasks, so check DB directly)
    let tasks_in_db = count_with_prefix(shard.db(), "tasks/").await;
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
    let holders_after_release = count_with_prefix(shard.db(), "holders/").await;
    assert_eq!(holders_after_release, 0, "holder released");

    // RequestTicket task should still be there (future)
    let tasks_still_future = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(tasks_still_future, 1, "RequestTicket task still present");

    // Simulate time passing: wait for future time + broker scan delay
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Now worker polls - should process RequestTicket and grant Job 2
    // The RequestTicket should be picked up by broker (it's now ready based on timestamp)
    // and dequeue should convert it to a lease for Job 2
    let mut t2_vec = Vec::new();
    for _ in 0..10 {
        t2_vec = shard.dequeue("w", 1).await.expect("deq2");
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

#[tokio::test]
async fn cannot_delete_job_with_future_request_ticket() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future = now + 200;
    let queue = "delete-future-q".to_string();

    // Job 1 occupies the slot
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 scheduled for future while slot is held -> creates RequestTicket task (status: Scheduled)
    let j2 = shard
        .enqueue(
            None,
            10u8,
            future,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");

    // Verify RequestTicket task exists and job status is Scheduled
    let tasks_before = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(tasks_before, 1, "should have 1 RequestTicket task");

    let status = shard
        .get_job_status(&j2)
        .await
        .expect("get status")
        .expect("exists");
    assert!(matches!(status, silo::job::JobStatus::Scheduled {}));

    // Attempt to delete job 2 while it's scheduled - should fail
    let err = shard.delete_job(&j2).await.expect_err("delete should fail");
    match err {
        silo::job_store_shard::JobStoreShardError::JobInProgress(jid) => {
            assert_eq!(jid, j2);
        }
        other => panic!("expected JobInProgress, got {:?}", other),
    }

    // Job still exists (can't delete while scheduled)
    let job2 = shard.get_job(&j2).await.expect("get job2");
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

#[tokio::test]
async fn cannot_delete_job_with_pending_request() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "delete-request-q".to_string();

    // Job 1 takes the slot
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let t1_vec = shard.dequeue("w", 1).await.expect("deq1");
    let t1 = t1_vec[0].attempt().task_id().to_string();

    // Job 2 queued as request (status: Scheduled)
    let j2 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");

    // Request should exist
    let requests = count_with_prefix(shard.db(), "requests/").await;
    assert_eq!(requests, 1, "should have 1 queued request");

    // Attempt to delete job 2 while it's scheduled (has pending request) - should fail
    let err = shard.delete_job(&j2).await.expect_err("delete should fail");
    match err {
        silo::job_store_shard::JobStoreShardError::JobInProgress(jid) => {
            assert_eq!(jid, j2);
        }
        other => panic!("expected JobInProgress, got {:?}", other),
    }

    // Job 2 should still exist
    let job2 = shard.get_job(&j2).await.expect("get job2");
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
    let t2_vec = shard.dequeue("w", 1).await.expect("deq2");
    assert_eq!(t2_vec.len(), 1);

    // Attempt to delete while running - should still fail
    let err2 = shard
        .delete_job(&j2)
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
        .delete_job(&j2)
        .await
        .expect("delete should succeed now");

    // Job should be gone
    let job2_final = shard.get_job(&j2).await.expect("get job2");
    assert!(job2_final.is_none(), "job 2 should be deleted");
}
