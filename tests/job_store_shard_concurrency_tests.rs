mod test_helpers;

use silo::codec::{decode_task, encode_concurrency_action};
use silo::job::Limit;
use silo::job_attempt::AttemptOutcome;
use silo::keys::concurrency_holder_key;
use silo::retry::RetryPolicy;
use silo::task::ConcurrencyAction;
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
        .filter(|t| !t.attempt().task_id().is_empty())
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
            Task::ResumeAfterConcurrencyGrant { .. } => {}
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
            Task::ResumeAfterConcurrencyGrant { .. } => {}
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
    let _job_a = shard
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
    let _job_b = shard
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

    let _job_c = shard
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

    // Give the task broker scanner time to pick up both tasks from the DB
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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

/// Tests that process_grants skips stale requests and continues scanning to
/// fulfill the requested count from valid requests behind them.
#[silo::test]
async fn grant_scanner_skips_stale_requests_and_grants_valid_ones() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stale-skip-q";
    let tenant = "stale-skip-tenant";
    let limit = Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 3,
    });

    shard.stop_grant_scanner();

    // Fill the 3 concurrency slots with holders, then enqueue 3 more as waiters
    let mut holder_tasks = Vec::new();
    for i in 0..3 {
        shard
            .enqueue(
                tenant,
                Some(format!("holder-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }
    for _ in 0..3 {
        let tasks = shard.dequeue("w", "tg", 1).await.unwrap().tasks;
        holder_tasks.push(tasks[0].attempt().task_id().to_string());
    }

    // Enqueue 3 legitimate waiters (queue at capacity 3/3, so these become requests)
    for i in 0..3 {
        shard
            .enqueue(
                tenant,
                Some(format!("valid-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }

    // Inject 5 stale requests with earlier start_time so they sort BEFORE the valid ones.
    // These reference job IDs that don't exist, so they'll be detected as stale.
    for i in 0..5 {
        let stale_action = ConcurrencyAction::EnqueueTask {
            start_time_ms: 0,
            priority: 50,
            job_id: format!("nonexistent-job-{}", i),
            attempt_number: 1,
            relative_attempt_number: 1,
            task_group: "tg".to_string(),
            held_queues: vec![],
            limit_index: 0,
            total_limits: 1,
        };
        let key = silo::keys::concurrency_request_key(
            tenant,
            queue,
            0,
            50,
            &format!("nonexistent-job-{}", i),
            1,
            &format!("stale{:04}", i),
        );
        let val = encode_concurrency_action(&stale_action);
        let mut batch = slatedb::WriteBatch::new();
        batch.put(&key, &val);
        shard.db().write(batch).await.unwrap();
    }

    assert_eq!(count_concurrency_requests(shard.db()).await, 8); // 5 stale + 3 valid

    // Release all holders to free capacity
    for task_id in &holder_tasks {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .unwrap();
    }

    // Ask for 3 grants — should skip all 5 stale, then grant all 3 valid
    let granted = shard.process_concurrency_grants(tenant, queue, 3).await;
    assert_eq!(
        granted.len(),
        3,
        "should grant all 3 valid requests despite 5 stale ones ahead of them"
    );

    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "all requests (stale + granted) should be removed"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, 3);
}

/// Tests that when all scanned requests are stale and no valid requests exist,
/// process_grants returns zero grants and cleans up the stale entries.
#[silo::test]
async fn grant_scanner_handles_all_stale_requests() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "all-stale-q";
    let tenant = "all-stale-tenant";

    shard.stop_grant_scanner();

    // Inject 5 stale requests (no real jobs behind them)
    for i in 0..5 {
        let stale_action = ConcurrencyAction::EnqueueTask {
            start_time_ms: 0,
            priority: 50,
            job_id: format!("ghost-{}", i),
            attempt_number: 1,
            relative_attempt_number: 1,
            task_group: "tg".to_string(),
            held_queues: vec![],
            limit_index: 0,
            total_limits: 1,
        };
        let key = silo::keys::concurrency_request_key(
            tenant,
            queue,
            0,
            50,
            &format!("ghost-{}", i),
            1,
            &format!("s{:04}", i),
        );
        let val = encode_concurrency_action(&stale_action);
        let mut batch = slatedb::WriteBatch::new();
        batch.put(&key, &val);
        shard.db().write(batch).await.unwrap();
    }

    assert_eq!(count_concurrency_requests(shard.db()).await, 5);

    let granted = shard.process_concurrency_grants(tenant, queue, 3).await;
    assert_eq!(granted.len(), 0, "no valid requests to grant");

    // All stale requests should still be cleaned up
    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "stale requests should be deleted even when no grants are made"
    );
}

/// Tests that the grant scanner increments the `silo_concurrency_tickets_granted_total`
/// counter once per ticket it grants. Without this, the metric only reflects the
/// synchronous dequeue path (handle_request_ticket) and goes silent the moment a
/// queue first hits its concurrency limit, since all subsequent grants flow through
/// the async grant scanner as holders are released.
#[silo::test]
async fn grant_scanner_records_concurrency_ticket_granted_metric() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let now = now_ms();
    let queue = "metric-q";
    let tenant = "metric-tenant";
    let limit = Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 2,
    });

    shard.stop_grant_scanner();

    // Fill the 2 concurrency slots
    let mut holder_tasks = Vec::new();
    for i in 0..2 {
        shard
            .enqueue(
                tenant,
                Some(format!("holder-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }
    for _ in 0..2 {
        let tasks = shard.dequeue("w", "tg", 1).await.unwrap().tasks;
        holder_tasks.push(tasks[0].attempt().task_id().to_string());
    }

    let baseline_scanned =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Scanned));
    let baseline_immediate =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Immediate));

    // Enqueue 3 more jobs that will queue as concurrency requests (queue is full).
    for i in 0..3 {
        shard
            .enqueue(
                tenant,
                Some(format!("waiter-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }

    // Free both slots so the grant scanner has capacity to grant the waiters.
    for task_id in &holder_tasks {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .unwrap();
    }

    // Drive the async grant-scanner path directly.
    let granted = shard.process_concurrency_grants(tenant, queue, 2).await;
    assert_eq!(granted.len(), 2, "should grant 2 of the 3 waiters");

    let after_scanned =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Scanned));
    let after_immediate =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Immediate));
    assert_eq!(
        after_scanned - baseline_scanned,
        2.0,
        "process_grants must increment the scanned-path counter once per granted ticket"
    );
    assert_eq!(
        after_immediate - baseline_immediate,
        0.0,
        "process_grants must not touch the immediate-path counter"
    );
}

/// Tests that the immediate-grant enqueue path (where `try_reserve` finds capacity
/// and `handle_enqueue` skips the request queue) increments
/// `silo_concurrency_tickets_granted_total{path="immediate"}` once per granted
/// enqueue, and does not bleed into the `path="scanned"` series.
#[silo::test]
async fn immediate_enqueue_records_concurrency_ticket_granted_metric() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let now = now_ms();
    let queue = "immediate-metric-q";
    let tenant = "immediate-metric-tenant";
    let limit = Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 3,
    });

    let baseline_immediate =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Immediate));
    let baseline_scanned =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Scanned));

    // 3 enqueues into a queue with capacity=3 should all hit the immediate-grant
    // path (try_reserve succeeds for each).
    for i in 0..3 {
        shard
            .enqueue(
                tenant,
                Some(format!("immediate-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }

    let after_immediate =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Immediate));
    let after_scanned =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Scanned));

    assert_eq!(
        after_immediate - baseline_immediate,
        3.0,
        "immediate-grant enqueue path must increment the immediate-path counter once per grant"
    );
    assert_eq!(
        after_scanned - baseline_scanned,
        0.0,
        "immediate-grant enqueue path must not touch the scanned-path counter"
    );

    // A 4th enqueue exceeds capacity and goes through the request queue, so the
    // immediate-path counter should not advance.
    shard
        .enqueue(
            tenant,
            Some("waiter".to_string()),
            50,
            now,
            None,
            vec![1],
            vec![limit.clone()],
            None,
            "tg",
        )
        .await
        .unwrap();

    let final_immediate =
        read_concurrency_tickets_granted_for(&metrics, Some(silo::metrics::GrantPath::Immediate));
    assert_eq!(
        final_immediate - after_immediate,
        0.0,
        "an at-capacity enqueue must not increment the immediate-path counter"
    );
}

/// Read the current value of the `silo_concurrency_tickets_granted_total` counter
/// for a specific `path` label value, or summed across all values when `path`
/// is `None`.
fn read_concurrency_tickets_granted_for(
    metrics: &silo::metrics::Metrics,
    path: Option<silo::metrics::GrantPath>,
) -> f64 {
    metrics
        .registry()
        .gather()
        .into_iter()
        .find(|f| f.get_name() == "silo_concurrency_tickets_granted_total")
        .map(|f| {
            f.get_metric()
                .iter()
                .filter(|m| match path {
                    None => true,
                    Some(want) => m
                        .get_label()
                        .iter()
                        .any(|l| l.get_name() == "path" && l.get_value() == want.as_str()),
                })
                .map(|m| m.get_counter().get_value())
                .sum()
        })
        .unwrap_or(0.0)
}

/// Tests that stale requests interleaved with valid ones are handled correctly:
/// the grant scanner should skip stale ones and keep scanning to find valid ones.
#[silo::test]
async fn grant_scanner_interleaved_stale_and_valid_requests() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "interleave-q";
    let tenant = "interleave-tenant";
    let limit = Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 5,
    });

    shard.stop_grant_scanner();

    // Fill all 5 slots with holders
    let mut holder_tasks = Vec::new();
    for i in 0..5 {
        shard
            .enqueue(
                tenant,
                Some(format!("holder-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }
    for _ in 0..5 {
        let tasks = shard.dequeue("w", "tg", 1).await.unwrap().tasks;
        holder_tasks.push(tasks[0].attempt().task_id().to_string());
    }

    // Enqueue 5 valid waiters (capacity 5/5, these become requests)
    for i in 0..5 {
        shard
            .enqueue(
                tenant,
                Some(format!("real-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }

    // Inject 10 stale requests with earlier start_time so they sort before valid ones
    for i in 0..10 {
        let stale_action = ConcurrencyAction::EnqueueTask {
            start_time_ms: now - 1000 + i,
            priority: 50,
            job_id: format!("phantom-{}", i),
            attempt_number: 1,
            relative_attempt_number: 1,
            task_group: "tg".to_string(),
            held_queues: vec![],
            limit_index: 0,
            total_limits: 1,
        };
        let key = silo::keys::concurrency_request_key(
            tenant,
            queue,
            now - 1000 + i,
            50,
            &format!("phantom-{}", i),
            1,
            &format!("x{:04}", i),
        );
        let val = encode_concurrency_action(&stale_action);
        let mut batch = slatedb::WriteBatch::new();
        batch.put(&key, &val);
        shard.db().write(batch).await.unwrap();
    }

    assert_eq!(count_concurrency_requests(shard.db()).await, 15); // 10 stale + 5 valid

    // Release all holders to free capacity
    for task_id in &holder_tasks {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .unwrap();
    }

    // Ask for 5 grants — should skip 10 stale, grant 5 valid
    let granted = shard.process_concurrency_grants(tenant, queue, 5).await;
    assert_eq!(
        granted.len(),
        5,
        "should grant 5 valid requests despite 10 stale ones interleaved"
    );

    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "all stale and granted requests should be removed"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, 5);
}

/// Regression test for the unbounded `join_all` fan-out in `process_grants`
/// that drove production OOMs (heap profile showed ~5.5 GB pinned in
/// `CachedObjectStore::read_part` from concurrent slatedb reads).
///
/// Exercises the bounded `buffered(STATUS_LOOKUP_CONCURRENCY)` path with a
/// pending backlog larger than the in-flight cap (currently 64). Verifies
/// correctness — order-preserving validation must still pair each result
/// with the right request — across more than one buffered batch.
#[silo::test]
async fn grant_scanner_handles_backlog_larger_than_status_lookup_concurrency() {
    // Sized to be a multiple of STATUS_LOOKUP_CONCURRENCY (64) plus extras
    // so we cross at least two buffered windows. Kept modest to keep the
    // test fast while still beating the in-flight cap meaningfully.
    const N: usize = 200;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "backlog-q";
    let tenant = "backlog-tenant";
    let limit = Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: N as u32,
    });

    shard.stop_grant_scanner();

    // Fill all N concurrency slots with holders.
    for i in 0..N {
        shard
            .enqueue(
                tenant,
                Some(format!("holder-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }
    let mut holder_tasks = Vec::with_capacity(N);
    while holder_tasks.len() < N {
        let tasks = shard.dequeue("w", "tg", N).await.unwrap().tasks;
        assert!(!tasks.is_empty(), "dequeue should return holders");
        for t in tasks {
            holder_tasks.push(t.attempt().task_id().to_string());
        }
    }

    // Enqueue N more — capacity is full, so each becomes a request record.
    for i in 0..N {
        shard
            .enqueue(
                tenant,
                Some(format!("waiter-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }

    assert_eq!(count_concurrency_requests(shard.db()).await, N);

    // Release all holders to free capacity for the waiters.
    for task_id in &holder_tasks {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .unwrap();
    }

    // One process_grants call should drain all N waiters. Internally this
    // runs the buffered status-lookup pipeline across multiple batches of
    // up to STATUS_LOOKUP_CONCURRENCY (64) in-flight db.gets.
    let granted = shard
        .process_concurrency_grants(tenant, queue, N as u32)
        .await;
    assert_eq!(
        granted.len(),
        N,
        "all {} valid waiters should be granted via the bounded buffered pipeline",
        N
    );

    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "all request records should be consumed"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, N);
}

/// Regression test for `MAX_GRANTS_PER_PASS` bounding in `process_grants`.
///
/// A single `request_grant_count` accumulation can be arbitrarily large (every
/// release between scanner wakeups adds to it). Without a per-pass cap, the
/// scanner would materialize `count` `ScannedRequest`s, issue `count` buffered
/// status gets, and accumulate `count` edits in one `WriteBatch` before
/// committing. The cap forces the scanner to drain a large `count` over multiple
/// bounded passes — this test verifies that an end-to-end drain of a backlog
/// larger than `MAX_GRANTS_PER_PASS` (256) still grants every waiter.
#[silo::test]
async fn grant_scanner_drains_backlog_larger_than_max_per_pass() {
    // Sized to cross the per-pass cap (256) by enough to require at least three
    // passes, exercising the multi-pass commit path.
    const N: usize = 600;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "max-per-pass-q";
    let tenant = "max-per-pass-tenant";
    let limit = Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: N as u32,
    });

    shard.stop_grant_scanner();

    // Fill all N slots with holders.
    for i in 0..N {
        shard
            .enqueue(
                tenant,
                Some(format!("holder-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }
    let mut holder_tasks = Vec::with_capacity(N);
    while holder_tasks.len() < N {
        let tasks = shard.dequeue("w", "tg", N).await.unwrap().tasks;
        assert!(!tasks.is_empty(), "dequeue should return holders");
        for t in tasks {
            holder_tasks.push(t.attempt().task_id().to_string());
        }
    }

    // Enqueue N waiters — each becomes a request (capacity is full).
    for i in 0..N {
        shard
            .enqueue(
                tenant,
                Some(format!("waiter-{}", i)),
                50,
                now,
                None,
                vec![1],
                vec![limit.clone()],
                None,
                "tg",
            )
            .await
            .unwrap();
    }
    assert_eq!(count_concurrency_requests(shard.db()).await, N);

    // Release all holders to free capacity.
    for task_id in &holder_tasks {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .unwrap();
    }

    // One process_grants call asks for N grants. Internally this must run as
    // multiple bounded passes (each ≤ MAX_GRANTS_PER_PASS = 256) and still
    // return all N grants.
    let granted = shard
        .process_concurrency_grants(tenant, queue, N as u32)
        .await;
    assert_eq!(
        granted.len(),
        N,
        "all {} waiters should be granted via multiple bounded passes",
        N
    );

    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "all request records should be consumed"
    );
    assert_eq!(count_concurrency_holders(shard.db()).await, N);
}

/// Reproduces a Gadget production report: jobs enqueued with TWO concurrency limits
/// (a floating "platform" limit + a user-named fixed concurrency limit, e.g.
/// `exampleshopname.myshopify.com`) advance through the first (platform) limit
/// but the user-named queue never gets a holder or request — workers therefore see
/// no leases for that queue.
///
/// Mirrors `SiloActionStore.createAction` which constructs:
///   limits[0] = FloatingConcurrency(platform-tenant-env-…)
///   limits[1] = Concurrency(action.queue)
#[silo::test]
async fn enqueue_with_two_concurrency_limits_grants_both() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";

    let platform_queue = "platform-tenant-env-123".to_string();
    let user_queue = "exampleshopname.myshopify.com".to_string();

    let _job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![
                Limit::FloatingConcurrency(silo::job::FloatingConcurrencyLimit {
                    key: platform_queue.clone(),
                    default_max_concurrency: 100,
                    refresh_interval_ms: 60_000,
                    metadata: vec![],
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: user_queue.clone(),
                    max_concurrency: 5,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue with two limits");

    let total_holders = count_concurrency_holders(shard.db()).await;
    let platform_holders = count_holders_for_queue(shard.db(), tenant, &platform_queue).await;
    let user_holders = count_holders_for_queue(shard.db(), tenant, &user_queue).await;

    println!(
        "total_holders={}, platform_holders={}, user_holders={}",
        total_holders, platform_holders, user_holders
    );

    assert_eq!(
        platform_holders, 1,
        "platform queue should have a holder for the granted floating limit"
    );
    assert_eq!(
        user_holders, 1,
        "user queue should have a holder for the granted fixed concurrency limit \
         — if this is 0, the second limit was silently skipped at enqueue"
    );
}

/// Verifies the held_queues subtlety in append_grant_edits: when multiple limits
/// each grant immediately, append_grant_edits writes interim RunAttempts with
/// only the current queue in held_queues. The loop's terminal branch must
/// overwrite the same task_key with the full accumulated held_queues — and
/// reporting the worker outcome must release BOTH holders.
///
/// If only one queue is released on outcome, the other holder leaks and that
/// queue's capacity is permanently consumed.
#[silo::test]
async fn two_concurrency_limits_release_both_holders_on_success() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";

    let platform_queue = "platform-tenant-env-456".to_string();
    let user_queue = "exampleshopname.myshopify.com".to_string();

    let _job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![
                Limit::FloatingConcurrency(silo::job::FloatingConcurrencyLimit {
                    key: platform_queue.clone(),
                    default_max_concurrency: 100,
                    refresh_interval_ms: 60_000,
                    metadata: vec![],
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: user_queue.clone(),
                    max_concurrency: 5,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue with two limits");

    // Sanity: a holder exists on each queue.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &platform_queue).await,
        1
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &user_queue).await,
        1
    );

    // Dequeue the RunAttempt and verify the lease record's held_queues
    // contains BOTH queues so ReportOutcome can release them all.
    let tasks = shard
        .dequeue("w1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1, "the RunAttempt should be leaseable");
    let task_id = tasks[0].attempt().task_id().to_string();

    // Read the lease record to confirm held_queues was preserved end-to-end.
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&task_id))
        .await
        .expect("read lease")
        .expect("lease present after dequeue");
    let decoded_lease = silo::codec::decode_lease(lease_bytes).expect("decode lease");
    let held = decoded_lease.held_queues();
    assert!(
        held.iter().any(|hq| hq.queue == platform_queue),
        "lease held_queues should include platform queue, got {:?}",
        held
    );
    assert!(
        held.iter().any(|hq| hq.queue == user_queue),
        "lease held_queues should include user queue — if missing, the final \
         RunAttempt overwrite dropped the earlier granted queue. Got {:?}",
        held
    );
    assert_eq!(
        held.len(),
        2,
        "lease held_queues should be exactly the two granted queues, got {:?}",
        held
    );

    // Report success — silo must release holders for BOTH queues.
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &platform_queue).await,
        0,
        "platform queue holder should be released after outcome"
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &user_queue).await,
        0,
        "user queue holder should be released after outcome — leak indicates \
         held_queues wasn't fully populated on the final RunAttempt"
    );
}

/// When a job has limits = [A, B] and A grants but B is at capacity, the
/// `append_grant_edits` call for A writes an interim RunAttempt at task_key.
/// `append_request_edits` for B only writes a concurrency request record — it
/// does NOT delete/overwrite the task at task_key. If this bug is real, a
/// worker can dequeue and run the RunAttempt for a job that should be blocked
/// on B, violating the B limit.
#[silo::test]
async fn second_limit_request_must_not_leave_runnable_task() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "queue-a".to_string();
    let queue_b = "queue-b".to_string();

    // Fill queue B to capacity: enqueue job1 with only limit B, then dequeue
    // it so the holder persists.
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_b.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue job1");
    let job1_tasks = shard
        .dequeue("w1", "default", 1)
        .await
        .expect("deq job1")
        .tasks;
    assert_eq!(job1_tasks.len(), 1, "job1 should be runnable");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        1,
        "queue B should be at capacity (1 holder)"
    );

    // Enqueue job2 with [A, B]. A grants immediately; B is full so returns a
    // TicketRequested.
    let _job2 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 5,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_b.clone(),
                    max_concurrency: 1,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue job2");

    // A concurrency request for queue B should exist for job2.
    assert!(
        count_concurrency_requests(shard.db()).await >= 1,
        "expected a concurrency request to be written for job2 on queue B"
    );

    // The critical assertion: dequeue must not return a RunAttempt for job2.
    // If the interim RunAttempt from A's append_grant_edits was left at
    // task_key, a worker will pick it up and run the job while B is at
    // capacity — violating the B limit.
    let job2_tasks = shard
        .dequeue("w2", "default", 1)
        .await
        .expect("deq job2")
        .tasks;
    assert_eq!(
        job2_tasks.len(),
        0,
        "job2 must not be runnable while queue B is at capacity — a stale \
         RunAttempt at task_key from limit[0]'s append_grant_edits was not \
         removed when limit[1] returned TicketRequested. \
         Got task: {:?}",
        job2_tasks
            .first()
            .map(|t| t.attempt().task_id().to_string()),
    );
}

// ---------------------------------------------------------------------------
// Chain-of-limits regression tests for held_queues + bypass enforcement.
//
// PR #315 fixed multi-limit holder leaks at enqueue. PR #317 reordered
// Concurrency before FloatingConcurrency so the common Gadget shape no
// longer hit the latent grant-time leak. These tests cover the remaining
// chain-time gaps that the resume-after-grant + per-queue task_id machinery
// closes: holder preservation on grant-scanner promotion and enforcement
// of limits-after-the-queued-one.
// ---------------------------------------------------------------------------

/// `[C1, C2]` where C1 grants immediately and C2 queues (other job holds C2).
/// When the holder on C2 is freed, the promotion path must preserve the
/// already-granted C1 holder AND enforce no bypass of any limit that follows
/// C2 in the chain. Before the fix, `process_grants` wrote a RunAttempt with
/// `held_queues = vec![C2]` only, orphaning the C1 holder; on report success
/// the C1 slot leaked permanently.
#[silo::test]
async fn chain_c1_c2_promotion_preserves_prior_holder_and_releases_both() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_c1 = "chain-c1-free".to_string();
    let queue_c2 = "chain-c2-mutex".to_string();
    let now = now_ms();

    // Holder job for C2 — single-limit Concurrency with max=1, holds the slot.
    let _holder_job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "holder"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_c2.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    let holder_tasks = shard
        .dequeue("h", "default", 1)
        .await
        .expect("dequeue holder")
        .tasks;
    assert_eq!(holder_tasks.len(), 1);
    let holder_task_id = holder_tasks[0].attempt().task_id().to_string();

    // Target job — `[C1(free, max=5), C2(full, max=1)]`. C1 grants
    // immediately, C2 queues. The interim RunAttempt is then dropped by
    // PR #317's cleanup; the request record carries `held_queues = [{C1, T}]`.
    let target_job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "target"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c1.clone(),
                    max_concurrency: 5,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c2.clone(),
                    max_concurrency: 1,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue target");

    // C1 has the target's holder; C2 has the holder job's holder.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        1
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c2).await,
        1
    );

    // Free C2 by completing the holder job.
    shard
        .report_attempt_outcome(&holder_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report holder success");

    // Poll for the target's RunAttempt to become dequeueable.
    let target_tasks = poll_until(
        || async {
            shard
                .dequeue("t", "default", 1)
                .await
                .expect("dq target")
                .tasks
        },
        |t| !t.is_empty(),
        5000,
    )
    .await;
    assert_eq!(target_tasks.len(), 1, "target should become runnable");
    assert_eq!(target_tasks[0].job().id(), target_job);
    let target_task_id = target_tasks[0].attempt().task_id().to_string();

    // Inspect the lease: held_queues must include BOTH C1 and C2.
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&target_task_id))
        .await
        .expect("read lease")
        .expect("lease present");
    let decoded_lease = silo::codec::decode_lease(lease_bytes).expect("decode lease");
    let held = decoded_lease.held_queues();
    assert!(
        held.iter().any(|hq| hq.queue == queue_c1),
        "lease must carry C1 holder; pre-fix the promotion path dropped it. \
         Got {:?}",
        held
    );
    assert!(
        held.iter().any(|hq| hq.queue == queue_c2),
        "lease must carry C2 holder (just-granted queue). Got {:?}",
        held
    );

    // Report success — both holders must drop to zero.
    shard
        .report_attempt_outcome(&target_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report target success");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        0,
        "C1 holder leaked: report_outcome only released the chained task_id, \
         but the pre-fix held_queues dropped C1 entirely so its holder \
         (under the original task_id) was never deleted"
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c2).await,
        0,
        "C2 holder should be released"
    );
}

/// `[C, FC]` mixed-kind chain (the canonical Gadget shape post-PR #317
/// reorder). C grants immediately; FC queues because another job is
/// holding it. When the FC holder is released, the grant scanner must
/// promote the target while preserving the C holder it already had.
///
/// `process_grants` doesn't differentiate fixed vs floating concurrency
/// (it operates on the queue name + `held_queues` from the request, not
/// on the limit kind), so the held_queues plumbing should work
/// identically. This test locks that in and guards against future
/// divergence.
#[silo::test]
async fn chain_c_then_fc_promotion_preserves_concurrency_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_c = "chain-c-fc-fixed".to_string();
    let queue_fc = "chain-c-fc-floating".to_string();
    let now = now_ms();

    // Holder job for the FloatingConcurrency limit — single FC limit, max=1.
    let _fc_holder = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "fc-holder"})),
            vec![Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue_fc.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms: 60_000,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue fc holder");
    let fc_holder_tasks = shard
        .dequeue("h", "default", 1)
        .await
        .expect("dq fc holder")
        .tasks;
    assert_eq!(fc_holder_tasks.len(), 1);
    let fc_holder_task_id = fc_holder_tasks[0].attempt().task_id().to_string();

    // Target with `[C(free, max=5), FC(full, max=1)]`. With PR #317's
    // reorder, C is processed first and grants immediately; FC then queues
    // because the fc_holder occupies the FC slot.
    let target_job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "target"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c.clone(),
                    max_concurrency: 5,
                }),
                Limit::FloatingConcurrency(silo::job::FloatingConcurrencyLimit {
                    key: queue_fc.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms: 60_000,
                    metadata: vec![],
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue target");

    // Sanity: C has target's holder; FC has the fc_holder's holder.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c).await,
        1,
        "target should hold C immediately"
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_fc).await,
        1,
        "fc_holder should hold FC"
    );

    // Free FC by completing the fc_holder.
    shard
        .report_attempt_outcome(
            &fc_holder_task_id,
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report fc_holder success");

    // Target should now become runnable. The grant scanner promotes the
    // queued FC request, and because total_limits == 2 and limit_index == 1
    // (the FC position), next_index >= total_limits so a terminal RunAttempt
    // is written directly — NOT a ResumeAfterConcurrencyGrant.
    let target_tasks = poll_until(
        || async {
            shard
                .dequeue("t", "default", 1)
                .await
                .expect("dq target")
                .tasks
        },
        |t| !t.is_empty(),
        5000,
    )
    .await;
    assert_eq!(target_tasks.len(), 1, "target should become runnable");
    assert_eq!(target_tasks[0].job().id(), target_job);
    let target_task_id = target_tasks[0].attempt().task_id().to_string();

    // Inspect the lease: held_queues must carry BOTH C and FC.
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&target_task_id))
        .await
        .expect("read lease")
        .expect("lease present");
    let decoded_lease = silo::codec::decode_lease(lease_bytes).expect("decode lease");
    let held = decoded_lease.held_queues();
    assert!(
        held.iter().any(|hq| hq.queue == queue_c),
        "lease must carry the C holder (preserved across FC promotion); got {:?}",
        held
    );
    assert!(
        held.iter().any(|hq| hq.queue == queue_fc),
        "lease must carry the FC holder (just-granted); got {:?}",
        held
    );
    assert_eq!(
        held.len(),
        2,
        "exactly the two granted queues; got {:?}",
        held
    );

    // Report success — both holders must release.
    shard
        .report_attempt_outcome(&target_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report target success");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c).await,
        0,
        "C holder leaked: the [C, FC] promotion path dropped the pre-granted \
         C holder, or release_outcome failed to find the C holder under the \
         original target task_id"
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_fc).await,
        0,
        "FC holder should be released"
    );
}

/// Three-limit chain `[C1, C2, C3]`. C1 grants, C2 queues (other job holds it).
/// On C2's promotion the chain must continue at C3 — not bypass C3 by leasing
/// a RunAttempt directly. Tests that the new `ResumeAfterConcurrencyGrant`
/// task fires and chains through `enqueue_limit_task_at_index` to the next
/// limit, which sees C3 is full and writes a request.
#[silo::test]
async fn chain_3_limits_middle_queue_does_not_bypass_later_limits() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_c1 = "chain3-c1".to_string();
    let queue_c2 = "chain3-c2".to_string();
    let queue_c3 = "chain3-c3".to_string();
    let now = now_ms();

    // Block C2 with a holder job.
    let _c2_holder = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "c2-holder"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_c2.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue c2 holder");
    let c2_holder_task = shard.dequeue("w", "default", 1).await.expect("dq").tasks;
    let c2_holder_id = c2_holder_task[0].attempt().task_id().to_string();

    // Block C3 with a holder job too.
    let _c3_holder = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "c3-holder"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_c3.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue c3 holder");
    let c3_holder_task = shard.dequeue("w", "default", 1).await.expect("dq").tasks;
    let c3_holder_id = c3_holder_task[0].attempt().task_id().to_string();

    // Target: `[C1 free, C2 full, C3 full]`. C1 grants, C2 queues.
    let target_job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "target"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c1.clone(),
                    max_concurrency: 5,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c2.clone(),
                    max_concurrency: 1,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c3.clone(),
                    max_concurrency: 1,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue target");
    let _ = target_job;

    // Free C2 — target's chain should advance to C3 and queue there, NOT
    // produce a leasable RunAttempt.
    shard
        .report_attempt_outcome(&c2_holder_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("release c2");

    // Give the grant scanner and resume task a chance to settle.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // No runnable target task: it must still be blocked on C3.
    let target_attempt = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert!(
        target_attempt.is_empty(),
        "target must remain blocked on C3 — bypass would yield {:?}",
        target_attempt
            .first()
            .map(|t| t.attempt().task_id().to_string())
    );

    // Free C3 — chain completes, target becomes runnable.
    shard
        .report_attempt_outcome(&c3_holder_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("release c3");
    let target_runs = poll_until(
        || async {
            shard
                .dequeue("worker", "default", 1)
                .await
                .expect("dq")
                .tasks
        },
        |t| !t.is_empty(),
        5000,
    )
    .await;
    assert_eq!(target_runs.len(), 1, "target should run after C3 frees");
    let target_task_id = target_runs[0].attempt().task_id().to_string();

    // Lease should carry all three holders, each under the right task_id.
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&target_task_id))
        .await
        .expect("get lease")
        .expect("lease present");
    let decoded_lease = silo::codec::decode_lease(lease_bytes).expect("decode lease");
    let held = decoded_lease.held_queues();
    assert!(held.iter().any(|hq| hq.queue == queue_c1));
    assert!(held.iter().any(|hq| hq.queue == queue_c2));
    assert!(held.iter().any(|hq| hq.queue == queue_c3));
    assert_eq!(held.len(), 3, "all three queues expected; got {:?}", held);

    // Report success — every holder must release.
    shard
        .report_attempt_outcome(&target_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        0
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c2).await,
        0
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c3).await,
        0
    );
}

/// `[C(full), RateLimit]`. C queues. When C is freed, the resume path must
/// hand off to the rate-limit branch — writing a `CheckRateLimit` task that
/// carries the C holder with the request_id as its task_id. Tests the
/// resume-after-grant → rate-limit handoff: this is the common Gadget shape
/// when a tenant queue + an API rate limit are both configured.
#[silo::test]
async fn chain_concurrency_to_ratelimit_resumes_with_held_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_c = "chain-c-rl".to_string();
    let now = now_ms();

    // Holder job for the Concurrency limit.
    let _c_holder = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "c-holder"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_c.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue c-holder");
    let c_holder_task = shard.dequeue("h", "default", 1).await.expect("dq").tasks;
    let c_holder_id = c_holder_task[0].attempt().task_id().to_string();

    // Target with `[C, RateLimit]`. Use a rate limit pointing at a name we
    // won't actually call Gubernator for in this test (we tear down before).
    // We're only interested in the structure: after C is freed, the chain
    // must write a CheckRateLimit task — not a leasable RunAttempt.
    let _target = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "target"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(silo::job::GubernatorRateLimit {
                    name: "test-rl".to_string(),
                    unique_key: "chain-rl-key".to_string(),
                    limit: 100,
                    duration_ms: 60_000,
                    hits: 1,
                    algorithm: silo::job::GubernatorAlgorithm::TokenBucket,
                    behavior: 0,
                    retry_policy: silo::job::RateLimitRetryPolicy {
                        initial_backoff_ms: 10,
                        max_backoff_ms: 1000,
                        backoff_multiplier: 2.0,
                        max_retries: 3,
                    },
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue target");

    // Free C — the grant scanner promotes target's request. Because limits
    // remain (the RateLimit at index 1), `process_grants` writes a
    // `ResumeAfterConcurrencyGrant` at target's task_key carrying the
    // just-granted C holder paired with the request_id. The resume task is
    // the contract we care about here — the subsequent RateLimit walk goes
    // through standard `enqueue_limit_task_at_index` machinery already
    // covered by other tests.
    shard
        .report_attempt_outcome(&c_holder_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("release c");

    let resume = poll_until(
        || async {
            let kv = first_task_kv(shard.db()).await;
            kv.and_then(|(_, v)| decode_task(&v).ok())
        },
        |t| matches!(t, Some(Task::ResumeAfterConcurrencyGrant { .. })),
        5000,
    )
    .await;
    let task = resume.expect("expected resume task to be written");
    let (held, req_id, lim_idx) = match task {
        Task::ResumeAfterConcurrencyGrant {
            held_queues,
            request_id,
            limit_index,
            ..
        } => (held_queues, request_id, limit_index),
        other => panic!("expected ResumeAfterConcurrencyGrant, got {:?}", other),
    };
    assert_eq!(
        lim_idx, 1,
        "resume must continue at the RateLimit (index 1)"
    );
    assert_eq!(
        held.len(),
        1,
        "resume must carry exactly the just-granted C holder; got {:?}",
        held
    );
    assert_eq!(held[0].queue, queue_c);
    assert_eq!(
        held[0].task_id, req_id,
        "the holder's task_id must match the resume's request_id — that's the \
         task_id `process_ticket_request_task` / `process_grants` used when \
         creating the holder, and the release path looks holders up by it."
    );

    // Sanity: the actual holder row in storage uses (queue, request_id) too.
    let holder = shard
        .db()
        .get(&concurrency_holder_key(tenant, &queue_c, &req_id))
        .await
        .expect("read holder");
    assert!(
        holder.is_some(),
        "holder for granted C ticket must be keyed by request_id"
    );
}

/// `[C1, C2]` chain cancelled mid-flight: C1 granted, C2 queued as a request.
/// `cancel_job` must clean up the request AND release the C1 holder.
#[silo::test]
async fn cancel_mid_chain_releases_prior_holders() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_c1 = "cancel-mid-c1".to_string();
    let queue_c2 = "cancel-mid-c2".to_string();
    let now = now_ms();

    let _c2_holder = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "c2-holder"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_c2.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue c2 holder");
    let _ = shard.dequeue("h", "default", 1).await.expect("dq").tasks;

    let target_job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"role": "target"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c1.clone(),
                    max_concurrency: 5,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c2.clone(),
                    max_concurrency: 1,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue target");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        1
    );

    shard
        .cancel_job(tenant, &target_job)
        .await
        .expect("cancel target");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        0,
        "C1 holder leaked on cancel-mid-chain"
    );
}

/// JobMissing arrival in `handle_request_ticket`: synthesize a
/// `Task::RequestTicket` with prior `held_queues`, plant the corresponding
/// holder rows, then dequeue. The handler must take the JobMissing branch
/// (the job_id doesn't exist) and clean up both the task and the prior
/// holders. Currently, the future-scheduled RequestTicket only writes from
/// the first limit (before any held_queues accumulate), but the field is
/// typed as a possibility — this exercises the contract.
#[silo::test]
async fn handle_request_ticket_job_missing_releases_prior_held_queues() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    let queue_c1 = "rt-jm-c1".to_string();
    let queue_c2 = "rt-jm-c2".to_string();
    let now = now_ms();

    let fake_job_id = "rt-jm-ghost-job".to_string();
    let request_id = "rt-jm-req-1".to_string();
    let prior_task_id = "rt-jm-prior-task".to_string();

    let holder = silo::task::HolderRecord { granted_at_ms: now };
    shard
        .db()
        .put(
            &silo::keys::concurrency_holder_key(tenant, &queue_c1, &prior_task_id),
            &silo::codec::encode_holder(&holder),
        )
        .await
        .expect("plant holder");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        1
    );

    let task = silo::task::Task::RequestTicket {
        queue: queue_c2.clone(),
        start_time_ms: now,
        priority: 10,
        tenant: tenant.to_string(),
        job_id: fake_job_id.clone(),
        attempt_number: 1,
        relative_attempt_number: 1,
        request_id: request_id.clone(),
        task_group: "default".to_string(),
        held_queues: vec![silo::task::HeldQueue {
            queue: queue_c1.clone(),
            task_id: prior_task_id.clone(),
        }],
        limit_index: 0,
        total_limits: 2,
    };
    let planted_task_key = silo::keys::task_key("default", now, 10, &fake_job_id, 1);
    shard
        .db()
        .put(&planted_task_key, &silo::codec::encode_task(&task))
        .await
        .expect("plant request ticket");
    shard.db().flush().await.expect("flush");

    // Drive the broker until the JobMissing branch processes the planted
    // task. Returns no leasable tasks (job is gone). The prior C1 holder
    // and the task_key must both be cleaned up.
    let _ = poll_until(
        || async {
            let _ = shard.dequeue("w", "default", 5).await.expect("dq").tasks;
            shard
                .db()
                .get(&planted_task_key)
                .await
                .expect("read task_key")
        },
        |r| r.is_none(),
        5000,
    )
    .await;
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_c1).await,
        0,
        "prior chain holder leaked when handle_request_ticket hit JobMissing"
    );
}

/// Helper: count concurrency holder keys for a specific (tenant, queue) pair.
async fn count_holders_for_queue(
    db: &silo::instrumented_db::InstrumentedDb,
    tenant: &str,
    queue: &str,
) -> usize {
    let start = silo::keys::concurrency_holders_queue_prefix(tenant, queue);
    let end = silo::keys::end_bound(&start);
    let mut iter = db
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .expect("scan holders");
    let mut count = 0;
    while let Ok(Some(_)) = iter.next().await {
        count += 1;
    }
    count
}
