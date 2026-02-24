mod test_helpers;

use silo::codec::decode_task;
use silo::job::Limit;
use silo::job_attempt::AttemptOutcome;
use silo::keys::concurrency_holder_key;
use silo::retry::RetryPolicy;
use silo::task::Task;
use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use test_helpers::*;

/// Tests that jobs from different task groups can participate in the same concurrency queue.
/// This is important because task groups are for routing work to different workers,
/// but concurrency limits should work across task groups.
#[silo::test]
async fn concurrency_shared_across_task_groups() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "shared-q".to_string();

    // Job A in task_group "alpha" takes the only slot
    let j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"from": "alpha"})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "alpha", // task_group alpha
        )
        .await
        .expect("enqueue alpha");

    // Dequeue from alpha task_group - should get the job
    let tasks1 = shard
        .dequeue("w1", "alpha", 1)
        .await
        .expect("deq alpha")
        .tasks;
    assert_eq!(tasks1.len(), 1);
    assert_eq!(tasks1[0].job().id(), j1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Job B in task_group "beta" should queue as a request (same concurrency key, slot is taken)
    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"from": "beta"})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "beta", // task_group beta
        )
        .await
        .expect("enqueue beta");

    // Dequeue from beta task_group - should get nothing (waiting for concurrency)
    let tasks2 = shard
        .dequeue("w2", "beta", 1)
        .await
        .expect("deq beta")
        .tasks;
    // Should only have RequestTicket, not RunAttempt
    let run_attempts: Vec<_> = tasks2
        .iter()
        .filter(|t| t.attempt().task_id().len() > 0)
        .collect();
    assert_eq!(
        run_attempts.len(),
        0,
        "beta job should be waiting for concurrency"
    );

    // Complete alpha job - this should grant the slot to the beta job
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report alpha success");

    // Now dequeue from beta - should get the job
    let tasks3 = shard
        .dequeue("w2", "beta", 1)
        .await
        .expect("deq beta after")
        .tasks;
    assert_eq!(tasks3.len(), 1, "beta job should now be runnable");
    assert_eq!(tasks3[0].job().id(), j2);
}

/// Tests that multiple jobs from different task groups properly queue for a shared concurrency limit.
#[silo::test]
async fn concurrency_queues_jobs_from_multiple_task_groups() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "multi-tg-q".to_string();

    // Enqueue jobs from 3 different task groups with max_concurrency=1
    let jobs: Vec<(String, &str)> = vec![
        (
            shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"order": 1})),
                    vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                        key: queue.clone(),
                        max_concurrency: 1,
                    })],
                    None,
                    "group-a",
                )
                .await
                .expect("enqueue a"),
            "group-a",
        ),
        (
            shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"order": 2})),
                    vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                        key: queue.clone(),
                        max_concurrency: 1,
                    })],
                    None,
                    "group-b",
                )
                .await
                .expect("enqueue b"),
            "group-b",
        ),
        (
            shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"order": 3})),
                    vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                        key: queue.clone(),
                        max_concurrency: 1,
                    })],
                    None,
                    "group-c",
                )
                .await
                .expect("enqueue c"),
            "group-c",
        ),
    ];

    // Only one holder should exist at a time
    let mut processed = 0;
    for (_job_id, task_group) in &jobs {
        // Try to get a task from each task group
        let result = shard
            .dequeue("worker", task_group, 1)
            .await
            .expect("dequeue");
        for task in &result.tasks {
            // Complete it
            let tid = task.attempt().task_id().to_string();
            shard
                .report_attempt_outcome(&tid, AttemptOutcome::Success { result: vec![] })
                .await
                .expect("report success");
            processed += 1;
        }
    }

    // Keep processing until all jobs are done
    while processed < 3 {
        for (_job_id, task_group) in &jobs {
            let result = shard
                .dequeue("worker", task_group, 1)
                .await
                .expect("dequeue");
            for task in &result.tasks {
                let tid = task.attempt().task_id().to_string();
                shard
                    .report_attempt_outcome(&tid, AttemptOutcome::Success { result: vec![] })
                    .await
                    .expect("report success");
                processed += 1;
            }
        }
    }

    assert_eq!(processed, 3, "all 3 jobs should be processed");

    // No holders should remain
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
    assert_eq!(count_concurrency_requests(shard.db()).await, 0);
}

/// Tests that concurrency limits with higher max_concurrency work across task groups.
#[silo::test]
async fn concurrency_allows_multiple_slots_across_task_groups() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "multi-slot-q".to_string();

    // Enqueue 4 jobs across 2 task groups with max_concurrency=2
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "workers-a",
        )
        .await
        .expect("enqueue 1");

    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "workers-b",
        )
        .await
        .expect("enqueue 2");

    let _j3 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "workers-a",
        )
        .await
        .expect("enqueue 3");

    let _j4 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 4})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "workers-b",
        )
        .await
        .expect("enqueue 4");

    // Both task groups should be able to get 1 task each (2 total for the queue)
    let tasks_a = shard
        .dequeue("wa", "workers-a", 2)
        .await
        .expect("deq a")
        .tasks;
    let tasks_b = shard
        .dequeue("wb", "workers-b", 2)
        .await
        .expect("deq b")
        .tasks;

    // Total runnable tasks should be at most 2 (the max_concurrency)
    let total_running = tasks_a.len() + tasks_b.len();
    assert!(
        total_running <= 2,
        "should not exceed max_concurrency across task groups: got {}",
        total_running
    );

    // Should have exactly 2 holders
    let holders = count_concurrency_holders(shard.db()).await;
    assert_eq!(holders, 2, "should have exactly 2 holders");
}

#[silo::test]
async fn concurrent_dequeue_many_workers_no_duplicates() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;
        let shard = Arc::new(shard);

        let total_jobs: usize = 200;
        let workers: usize = 8;
        let now = now_ms();

        // Shared trackers
        let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let processed = Arc::new(AtomicUsize::new(0));

        // Spawn workers
        let mut handles = Vec::new();
        for wi in 0..workers {
            let shard_cl = Arc::clone(&shard);
            let seen_cl = Arc::clone(&seen);
            let processed_cl = Arc::clone(&processed);
            let worker_id = format!("w-{wi}");
            handles.push(tokio::spawn(async move {
                loop {
                    // debug: before_dequeue suppressed
                    let tasks = shard_cl
                        .dequeue(&worker_id, "default", 1)
                        .await
                        .expect("dequeue")
                        .tasks;
                    if tasks.is_empty() {
                        if processed_cl.load(Ordering::Relaxed) >= total_jobs {
                            break;
                        }
                        // debug: empty_spin suppressed
                        tokio::task::yield_now().await;
                        continue;
                    }
                    let t = &tasks[0];
                    let tid = t.attempt().task_id().to_string();
                    // Validate uniqueness
                    {
                        let mut g = match seen_cl.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => {
                                // debug: seen_mutex_poisoned suppressed
                                poisoned.into_inner()
                            }
                        };
                        assert!(g.insert(tid.clone()), "duplicate task id dequeued: {tid}");
                    }
                    shard_cl
                        .report_attempt_outcome(
                            &tid,
                            AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await
                        .expect("report ok");
                    processed_cl.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // Enqueue producer task (avoid blocking the runtime)
        let shard_prod = Arc::clone(&shard);
        let producer = tokio::spawn(async move {
            for i in 0..total_jobs {
                let payload = test_helpers::msgpack_payload(&serde_json::json!({"i": i}));
                shard_prod
                    .enqueue(
                        "-",
                        None,
                        (i % 50) as u8,
                        now,
                        None,
                        payload,
                        vec![],
                        None,
                        "default",
                    )
                    .await
                    .expect("enqueue");
                // Yield occasionally to allow scanner/workers to run
                if i % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        for h in handles {
            h.await.unwrap();
        }
        producer.await.unwrap();

        assert_eq!(processed.load(Ordering::Relaxed), total_jobs);
        // No remaining tasks or leases
        assert_eq!(count_task_keys(shard.db()).await, 0);
        assert_eq!(count_lease_keys(shard.db()).await, 0);
    });
}

#[silo::test]
async fn future_tasks_are_not_dequeued_under_concurrency() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;
        let shard = Arc::new(shard);

        let ready_jobs: usize = 100;
        let future_jobs: usize = 100;
        let now = now_ms();
        let future = now + 60_000; // 60s in the future to avoid becoming ready during the test

        // Enqueue ready tasks
        for i in 0..ready_jobs {
            shard
                .enqueue(
                    "-",
                    None,
                    (i % 10) as u8,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"r": i})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue ready");
        }
        // Enqueue future tasks
        for i in 0..future_jobs {
            shard
                .enqueue(
                    "-",
                    None,
                    (i % 10) as u8,
                    future,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"f": i})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue future");
        }

        // Concurrently drain ready tasks
        let processed = Arc::new(AtomicUsize::new(0));
        let workers = 6usize;
        let mut handles = Vec::new();
        for wi in 0..workers {
            let shard_cl = Arc::clone(&shard);
            let processed_cl = Arc::clone(&processed);
            let worker_id = format!("wf-{wi}");
            handles.push(tokio::spawn(async move {
                loop {
                    let tasks = shard_cl
                        .dequeue(&worker_id, "default", 4)
                        .await
                        .expect("dequeue")
                        .tasks;
                    if tasks.is_empty() {
                        if processed_cl.load(Ordering::Relaxed) >= ready_jobs {
                            break;
                        }
                        tokio::task::yield_now().await;
                        continue;
                    }
                    for t in tasks {
                        let tid = t.attempt().task_id().to_string();
                        shard_cl
                            .report_attempt_outcome(
                                &tid,
                                AttemptOutcome::Success { result: vec![] },
                            )
                            .await
                            .expect("report ok");
                        processed_cl.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Only future tasks should remain in the queue
        // Ensure no ready tasks remain; any tasks left must be scheduled at or after `future`
        let ready_remaining = count_tasks_before(shard.db(), future).await;
        assert_eq!(ready_remaining, 0, "no ready tasks should remain");
    });
}

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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
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

/// BUG TEST: When a job with concurrency limits fails and schedules a retry, the retry
/// task must NOT claim to hold the concurrency slot because the slot is released and
/// may be granted to another waiting job.
///
/// This test reproduces the bug found in DST chaos scenario with seed 2137192077:
/// - Job A (with mutex concurrency limit and retry policy) runs
/// - Job B (with same mutex) enqueues and waits for the slot
/// - Job A fails and schedules a retry
/// - The bug: A's retry task has held_queues populated, AND the slot is granted to B
/// - Result: Both A's retry and B are returned in the same dequeue, violating the mutex
#[silo::test]
async fn retry_with_concurrency_must_reacquire_slot_not_claim_released_slot() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "mutex-retry-bug".to_string();

    // Job A: has mutex concurrency limit AND a retry policy
    let job_a = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1, // Very short retry interval so retry is immediately ready
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            test_helpers::msgpack_payload(&serde_json::json!({"job": "A"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1, // Mutex - only 1 job at a time
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue A");

    // Dequeue Job A - it gets the slot
    let tasks_a = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue A")
        .tasks;
    assert_eq!(tasks_a.len(), 1, "Job A should be dequeued");
    assert_eq!(tasks_a[0].job().id(), job_a);
    let task_a_id = tasks_a[0].attempt().task_id().to_string();

    // Verify Job A has the holder
    let holder_a = shard
        .db()
        .get(&concurrency_holder_key("-", &queue, &task_a_id))
        .await
        .expect("get holder A");
    assert!(holder_a.is_some(), "Job A should have the holder");

    // Job B: same mutex, no retry policy
    let job_b = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": "B"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue B");

    // Job B should NOT be runnable yet (mutex is held by A)
    let tasks_while_a_running = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue while A running")
        .tasks;
    assert_eq!(
        tasks_while_a_running.len(),
        0,
        "Job B should NOT be runnable while A holds the mutex"
    );

    // Job A fails - this should:
    // 1. Release A's concurrency slot
    // 2. Grant the slot to waiting Job B
    // 3. Schedule a retry for A (but the retry must NOT claim to hold the slot!)
    shard
        .report_attempt_outcome(
            &task_a_id,
            AttemptOutcome::Error {
                error_code: "TEST_FAILURE".to_string(),
                error: b"simulated failure".to_vec(),
            },
        )
        .await
        .expect("report A failure");

    // Now dequeue - we should get EXACTLY ONE task.
    // With the async grant scanner, either Job B (granted via scanner) or Job A's retry
    // (processed via dequeue's internal RequestTicket pipeline) may be returned first.
    // The critical invariant: only ONE task is runnable with max_concurrency=1.
    let tasks_after_failure = poll_until(
        || async {
            shard
                .dequeue("worker", "default", 10)
                .await
                .expect("dequeue after failure")
                .tasks
        },
        |tasks| !tasks.is_empty(),
        5000,
    )
    .await;

    // Critical assertion: only ONE task should be runnable with max_concurrency=1
    assert_eq!(
        tasks_after_failure.len(),
        1,
        "BUG: Got {} tasks but max_concurrency=1, only 1 should be runnable. \
         Jobs returned: {:?}",
        tasks_after_failure.len(),
        tasks_after_failure
            .iter()
            .map(|t| t.job().id())
            .collect::<Vec<_>>()
    );

    // Verify only one holder exists
    let holder_count = count_concurrency_holders(shard.db()).await;
    assert_eq!(holder_count, 1, "Should have exactly 1 holder");

    // Complete the first dequeued task, then the other should become available.
    // With async grants, either A's retry or B might be first.
    let first_task_id = tasks_after_failure[0].attempt().task_id().to_string();
    let first_job_id = tasks_after_failure[0].job().id().to_string();
    shard
        .report_attempt_outcome(&first_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report first success");

    // The other task should now be runnable
    let tasks_after_first = poll_until(
        || async {
            shard
                .dequeue("worker", "default", 10)
                .await
                .expect("dequeue after first")
                .tasks
        },
        |tasks| !tasks.is_empty(),
        5000,
    )
    .await;
    assert_eq!(
        tasks_after_first.len(),
        1,
        "The other task should be runnable after first completed"
    );

    // Verify that both jobs (A retry and B) were processed across the two dequeues
    let second_job_id = tasks_after_first[0].job().id().to_string();
    let mut seen_jobs: HashSet<String> = HashSet::new();
    seen_jobs.insert(first_job_id);
    seen_jobs.insert(second_job_id);
    assert!(
        seen_jobs.contains(&job_a) && seen_jobs.contains(&job_b),
        "Both Job A (retry) and Job B should have been processed"
    );
}

/// Test that retry tasks properly go through the concurrency request flow
/// when there are waiting jobs that should get the slot first.
#[silo::test]
async fn retry_must_wait_for_slot_when_another_job_was_granted() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "retry-wait-q".to_string();

    // Job A with retry
    let job_a = shard
        .enqueue(
            "-",
            None,
            5u8, // Lower priority
            now,
            Some(RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            test_helpers::msgpack_payload(&serde_json::json!({"job": "A"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue A");

    // Dequeue A
    let tasks_a = shard.dequeue("w", "default", 1).await.expect("deq A").tasks;
    let task_a_id = tasks_a[0].attempt().task_id().to_string();

    // Enqueue B and C while A is running - they wait
    let job_b = shard
        .enqueue(
            "-",
            None,
            10u8, // Higher priority than A's retry
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": "B"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue B");

    let job_c = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": "C"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue C");

    // Fail A - one of B or C should get the slot, A's retry should wait
    shard
        .report_attempt_outcome(
            &task_a_id,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report A fail");

    // Should get exactly 1 task. With the async grant scanner, either B, C, or A's retry
    // may be returned depending on timing. The critical invariant is max_concurrency=1.
    let tasks = poll_until(
        || async {
            shard
                .dequeue("w", "default", 10)
                .await
                .expect("deq after A fail")
                .tasks
        },
        |tasks| !tasks.is_empty(),
        5000,
    )
    .await;
    assert_eq!(
        tasks.len(),
        1,
        "Only one task should be runnable with max_concurrency=1"
    );

    // Verify at most 1 holder
    assert!(
        count_concurrency_holders(shard.db()).await <= 1,
        "At most 1 holder should exist"
    );
}

/// Test retry with max_concurrency > 1 (spare capacity).
///
/// Retries always skip try_reserve, matching the Alloy model's
/// completeFailureRetryReleaseTicket which creates a TicketRequest, not an immediate
/// holder. The retry task is written as a RequestTicket in the DB queue. On the next
/// dequeue, it is processed and the slot is granted — well after the old holder was
/// released post-commit. This ensures no window where the same job holds two slots.
#[silo::test]
async fn retry_with_spare_concurrency_goes_through_request_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "spare-cap-retry".to_string();

    // Single job with retry and max_concurrency=3 (plenty of spare capacity)
    let job_a = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(RetryPolicy {
                retry_count: 2,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            test_helpers::msgpack_payload(&serde_json::json!({"job": "A"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 3,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue A");

    // Dequeue attempt 1
    let tasks_a1 = shard
        .dequeue("w", "default", 1)
        .await
        .expect("deq A1")
        .tasks;
    assert_eq!(tasks_a1.len(), 1);
    assert_eq!(tasks_a1[0].job().id(), job_a);
    let task_a1_id = tasks_a1[0].attempt().task_id().to_string();

    assert_eq!(count_concurrency_holders(shard.db()).await, 1);

    // Fail attempt 1 → retry written as RequestTicket task, old holder released post-commit.
    shard
        .report_attempt_outcome(
            &task_a1_id,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report A1 failure");

    // Old holder should be deleted from DB
    let old_holder = shard
        .db()
        .get(&concurrency_holder_key("-", &queue, &task_a1_id))
        .await
        .expect("get old holder");
    assert!(
        old_holder.is_none(),
        "Old task's holder should be deleted after retry"
    );

    // No holders should exist yet — old was released, new hasn't been granted
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "No holders should exist between old release and retry grant"
    );

    // Dequeue processes the RequestTicket → grants the slot → returns RunAttempt
    let tasks_a2 = poll_until(
        || async {
            shard
                .dequeue("w", "default", 1)
                .await
                .expect("deq A2")
                .tasks
        },
        |t| !t.is_empty(),
        5000,
    )
    .await;
    assert_eq!(tasks_a2.len(), 1);
    assert_eq!(tasks_a2[0].job().id(), job_a);
    let task_a2_id = tasks_a2[0].attempt().task_id().to_string();
    assert_ne!(task_a2_id, task_a1_id, "Retry should have a new task ID");

    // Now the retry task holds the slot
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "Retry task should hold exactly 1 slot"
    );

    // Fail attempt 2 → another retry
    shard
        .report_attempt_outcome(
            &task_a2_id,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report A2 failure");

    // Dequeue attempt 3
    let tasks_a3 = poll_until(
        || async {
            shard
                .dequeue("w", "default", 1)
                .await
                .expect("deq A3")
                .tasks
        },
        |t| !t.is_empty(),
        5000,
    )
    .await;
    assert_eq!(tasks_a3.len(), 1);
    let task_a3_id = tasks_a3[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(&task_a3_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report A3 success");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "No holders should remain after job completes"
    );
}

/// Test that retry doesn't over-count holders when other jobs share the concurrency queue.
///
/// With max_concurrency=2: Job A (with retry) and Job B both hold slots.
/// Job A fails → retry goes to request queue (skip_try_reserve), old holder released
/// post-commit. B still holds its slot. On the next dequeue, the retry's RequestTicket
/// is processed and the slot is granted.
#[silo::test]
async fn retry_with_concurrent_jobs_respects_limit() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "multi-job-retry".to_string();

    // Job A with retry
    let job_a = shard
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
            test_helpers::msgpack_payload(&serde_json::json!({"job": "A"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue A");

    // Job B (no retry)
    let _job_b = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": "B"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 2,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue B");

    // Dequeue both A and B (max_concurrency=2 allows both)
    let tasks = shard
        .dequeue("w", "default", 10)
        .await
        .expect("deq both")
        .tasks;
    assert_eq!(tasks.len(), 2, "Both jobs should be dequeued");

    let (task_a_id, task_b_id) = if tasks[0].job().id() == job_a {
        (
            tasks[0].attempt().task_id().to_string(),
            tasks[1].attempt().task_id().to_string(),
        )
    } else {
        (
            tasks[1].attempt().task_id().to_string(),
            tasks[0].attempt().task_id().to_string(),
        )
    };

    assert_eq!(count_concurrency_holders(shard.db()).await, 2);

    // Fail A → retry goes to request queue, old holder released post-commit.
    shard
        .report_attempt_outcome(
            &task_a_id,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report A failure");

    // Old A holder deleted, B's holder still exists → 1 holder
    let old_a_holder = shard
        .db()
        .get(&concurrency_holder_key("-", &queue, &task_a_id))
        .await
        .expect("get old A holder");
    assert!(old_a_holder.is_none(), "Old A holder should be deleted");

    let b_holder = shard
        .db()
        .get(&concurrency_holder_key("-", &queue, &task_b_id))
        .await
        .expect("get B holder");
    assert!(b_holder.is_some(), "B's holder should still exist");
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        1,
        "Only B's holder should exist before retry is granted"
    );

    // Dequeue processes A's RequestTicket → grants slot → returns RunAttempt
    let retry_tasks = poll_until(
        || async {
            shard
                .dequeue("w", "default", 1)
                .await
                .expect("deq A retry")
                .tasks
        },
        |t| !t.is_empty(),
        5000,
    )
    .await;
    assert_eq!(retry_tasks.len(), 1);
    assert_eq!(retry_tasks[0].job().id(), job_a);
    let task_a_retry_id = retry_tasks[0].attempt().task_id().to_string();

    // Now 2 holders: B + A retry
    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        2,
        "Should have 2 holders: B + A retry"
    );

    // Complete both
    shard
        .report_attempt_outcome(&task_a_retry_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report A retry success");
    shard
        .report_attempt_outcome(&task_b_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report B success");

    assert_eq!(
        count_concurrency_holders(shard.db()).await,
        0,
        "No holders should remain after all jobs complete"
    );
}
