mod test_helpers;

use silo::codec::{decode_task, encode_concurrency_action};
use silo::job::{
    GubernatorAlgorithm, GubernatorRateLimit, JobStatusKind, Limit, RateLimitRetryPolicy,
};
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
            limit_index: 0,
            held_queues: Vec::new(),
            task_id: format!("nonexistent-job-{}:1:stale", i),
            limits: Vec::new(),
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
            limit_index: 0,
            held_queues: Vec::new(),
            task_id: format!("ghost-{}:1:stale", i),
            limits: Vec::new(),
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
            limit_index: 0,
            held_queues: Vec::new(),
            task_id: format!("phantom-{}:1:stale", i),
            limits: Vec::new(),
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
        held.iter().any(|q| q == &platform_queue),
        "lease held_queues should include platform queue, got {:?}",
        held
    );
    assert!(
        held.iter().any(|q| q == &user_queue),
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

/// Multi-limit chain resume: a job blocked on the FIRST concurrency limit
/// must, when granted later by the scanner, run through the REMAINING
/// concurrency limits in order — accumulating their holders — before becoming
/// runnable.
///
/// Bug pre-fix: the grant scanner created a `RunAttempt` directly with
/// `held_queues = vec![just_granted_queue]`, skipping every subsequent limit.
/// Any second concurrency limit was silently bypassed and never gated, and any
/// second holder would have been orphaned. This test pins the post-fix
/// invariant.
#[silo::test]
async fn two_concurrency_limits_chain_resumes_correctly() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "chain-a".to_string();
    let queue_b = "chain-b".to_string();

    // job1: holds A and B. Both limits set to 1 so subsequent jobs block on A.
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
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
        .expect("enqueue job1");
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1);
    let job1_task_id = job1_tasks[0].attempt().task_id().to_string();

    // job2: same limits. Blocks on A (and would block on B too once A frees up).
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
                    max_concurrency: 1,
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

    // Release job1 — frees both A and B. The grant scanner picks up job2's
    // request on A, then the chain resumer continues to B (also free now) and
    // finally writes a terminal RunAttempt with held_queues = [A, B].
    shard
        .report_attempt_outcome(&job1_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report job1 success");

    // Poll until job2 is runnable (grant scanner is async).
    let mut job2_tasks = vec![];
    for _ in 0..50 {
        let t = shard.dequeue("w2", "default", 1).await.unwrap().tasks;
        if !t.is_empty() {
            job2_tasks = t;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        job2_tasks.len(),
        1,
        "job2 should become runnable after job1 completes (grant scanner + chain resume)"
    );
    let job2_task_id = job2_tasks[0].attempt().task_id().to_string();

    // Inspect the lease: held_queues must include BOTH A and B. Pre-fix this
    // would have been only [A] (the queue just granted by the scanner), and B
    // would be silently bypassed.
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&job2_task_id))
        .await
        .expect("read lease")
        .expect("lease present");
    let decoded_lease = silo::codec::decode_lease(lease_bytes).expect("decode lease");
    let held = decoded_lease.held_queues();
    assert!(
        held.iter().any(|q| q == &queue_a),
        "held_queues should include A, got {:?}",
        held
    );
    assert!(
        held.iter().any(|q| q == &queue_b),
        "held_queues should include B — if missing, the grant scanner bypassed \
         the second limit instead of resuming the chain. Got {:?}",
        held
    );
    assert_eq!(held.len(), 2, "held_queues must be [A, B]; got {:?}", held);

    // Holders exist on both queues until job2 completes.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        1
    );

    // Completing job2 must release both holders.
    shard
        .report_attempt_outcome(&job2_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report job2 success");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        0
    );
}

/// Scan a task group's keyspace for the (single) pending RunAttempt task of
/// `job_id` and return its parsed task key. Returns `None` if no such task is
/// currently in the DB queue.
async fn find_run_attempt_task_key(
    shard: &std::sync::Arc<silo::job_store_shard::JobStoreShard>,
    task_group: &str,
    job_id: &str,
) -> Option<silo::keys::ParsedTaskKey> {
    let prefix = silo::keys::task_group_prefix(task_group);
    let end = silo::keys::end_bound(&prefix);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(prefix..end, &silo::scan_options())
        .await
        .ok()?;
    while let Ok(Some(kv)) = iter.next().await {
        if let Ok(Task::RunAttempt { job_id: jid, .. }) = decode_task(&kv.value)
            && jid == job_id
        {
            return silo::keys::parse_task_key(&kv.key);
        }
    }
    None
}

/// Regression for the granted-RunAttempt back-of-queue leak (the production
/// back-of-queue wedge incident).
///
/// When the grant scanner resumes a waiting job's limit-chain, the terminal
/// `RunAttempt` must be written at a task_key whose `start_time_ms` is the job's
/// ORIGINAL scheduled time — not the grant time. The chain resumer historically
/// bumped `task_key_start_ms` to `now_ms` to dodge the broker's ack tombstone,
/// which shoved freshly-granted RunAttempts to the back of the broker's
/// start-time-ordered scan. Behind a wall of old, ungrantable RequestTickets the
/// capped scan never reached them, so they never ran, never released their
/// holders, and wedged the whole tenant.
#[silo::test]
async fn granted_run_attempt_keeps_original_start_time() {
    let (_tmp, shard) = open_temp_shard().await;
    let tenant = "-";
    // Original schedule is well in the past, so a grant-time (now_ms) stamp is
    // unmistakably distinguishable from the scheduled-time stamp.
    let old_start = now_ms() - 3_600_000; // 1h ago
    let queue_a = "oldstart-a".to_string();
    let queue_b = "oldstart-b".to_string();
    let limits = |n: i64| {
        (
            n,
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_b.clone(),
                    max_concurrency: 1,
                }),
            ],
        )
    };

    // job1 holds both A and B (both limits capped at 1).
    let (_, l1) = limits(1);
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            old_start,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            l1,
            None,
            "default",
        )
        .await
        .expect("enqueue job1");
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1);
    let job1_task_id = job1_tasks[0].attempt().task_id().to_string();

    // job2 waits on A, scheduled at the SAME old start time.
    let (_, l2) = limits(2);
    let job2 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            old_start,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            l2,
            None,
            "default",
        )
        .await
        .expect("enqueue job2");

    // Release job1 — the grant scanner resumes job2's chain across A and B and
    // writes job2's terminal RunAttempt into the task keyspace.
    shard
        .report_attempt_outcome(&job1_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report job1 success");

    // Poll until job2's terminal RunAttempt appears in the DB (grant scanner is
    // async). Inspect it in place — do NOT dequeue it, or it leaves the keyspace.
    let mut parsed = None;
    for _ in 0..50 {
        if let Some(p) = find_run_attempt_task_key(&shard, "default", &job2).await {
            parsed = Some(p);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let parsed = parsed.expect("job2's terminal RunAttempt should be written after job1 completes");

    // THE INVARIANT: the granted RunAttempt keeps its original scheduled start
    // time. Pre-fix this is ~now_ms (≈1h after old_start), pushing it to the
    // back of the broker scan.
    assert_eq!(
        parsed.start_time_ms, old_start as u64,
        "granted RunAttempt must keep the job's original scheduled start_time \
         ({old_start}); got {} (≈grant time = back of the broker queue)",
        parsed.start_time_ms
    );

    // Sanity: it must also actually be runnable and hold both queues.
    let mut job2_tasks = vec![];
    for _ in 0..50 {
        let t = shard.dequeue("w2", "default", 1).await.unwrap().tasks;
        if !t.is_empty() {
            job2_tasks = t;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(job2_tasks.len(), 1, "job2 should be dequeuable");
    let job2_task_id = job2_tasks[0].attempt().task_id().to_string();
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&job2_task_id))
        .await
        .expect("read lease")
        .expect("lease present");
    let held = silo::codec::decode_lease(lease_bytes)
        .expect("decode lease")
        .held_queues();
    assert!(
        held.iter().any(|q| q == &queue_a) && held.iter().any(|q| q == &queue_b),
        "job2 must hold both A and B; got {:?}",
        held
    );
}

/// When the chain resumer hits a STILL-FULL downstream concurrency limit, it
/// must write a fresh deferred request rather than fabricating a RunAttempt.
/// The fresh request must carry the partial chain state (limit_index pointing
/// past the just-won limit, held_queues containing the queues won so far) so
/// when *it* is later granted, the chain picks up at the right spot.
#[silo::test]
async fn two_concurrency_limits_chain_writes_new_request_when_b_full() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "chain-bfull-a".to_string();
    let queue_b = "chain-bfull-b".to_string();

    // A has capacity 3 so all three jobs can take an A slot; B is the
    // bottleneck. With A capacity 3 we guarantee job2 and job3 both reach
    // limit_index=1 with held_queues=[A] and defer on B — that's the
    // resumable-chain state this test pins.
    let limits = || {
        vec![
            Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_a.clone(),
                max_concurrency: 3,
            }),
            Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue_b.clone(),
                max_concurrency: 1,
            }),
        ]
    };

    // job1 grabs both A and B.
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            limits(),
            None,
            "default",
        )
        .await
        .expect("enqueue job1");
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1, "job1 should run");

    // job2 grabs A, defers on B with limit_index=1, held_queues=[A].
    let _job2 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            limits(),
            None,
            "default",
        )
        .await
        .expect("enqueue job2");

    // job3 also grabs A, also defers on B with limit_index=1, held_queues=[A].
    let _job3 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
            limits(),
            None,
            "default",
        )
        .await
        .expect("enqueue job3");

    // All three should hold A; only job1 holds B.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        3,
        "all three jobs should hold A"
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        1,
        "only job1 should hold B"
    );

    let job1_task_id = job1_tasks[0].attempt().task_id().to_string();

    // Release job1 — frees one A slot and the only B slot. The grant
    // scanner gives B to one of {job2, job3}; that job's chain advances and
    // it becomes runnable. The other still has its deferred B-request with
    // limit_index=1 and held_queues=[A] persisted.
    shard
        .report_attempt_outcome(&job1_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report job1 success");

    // Poll until a job becomes runnable.
    let mut runnable = vec![];
    for _ in 0..50 {
        let t = shard.dequeue("w2", "default", 1).await.unwrap().tasks;
        if !t.is_empty() {
            runnable = t;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        runnable.len(),
        1,
        "exactly one of job2/job3 should run after job1"
    );
    let runnable_task_id = runnable[0].attempt().task_id().to_string();
    let lease_bytes = shard
        .db()
        .get(&silo::keys::leased_task_key(&runnable_task_id))
        .await
        .expect("read lease")
        .expect("lease present");
    let decoded_lease = silo::codec::decode_lease(lease_bytes).expect("decode lease");
    let held = decoded_lease.held_queues();
    // The runnable job must hold BOTH A and B for the chain to be correct.
    assert!(
        held.iter().any(|q| q == &queue_a) && held.iter().any(|q| q == &queue_b),
        "runnable job's held_queues must include A and B; got {:?}",
        held
    );

    // The other job is still queued. Its concurrency request value must
    // carry the persisted chain state.
    let pending = count_concurrency_requests(shard.db()).await;
    assert!(
        pending >= 1,
        "the un-granted job should still have a concurrency request"
    );

    // Find and decode that request to confirm limit_index/held_queues/task_id
    // are persisted. Scan the request key prefix.
    let req_prefix = silo::keys::concurrency_requests_prefix();
    let end = silo::keys::end_bound(&req_prefix);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(req_prefix..end, &silo::scan_options())
        .await
        .expect("scan requests");
    let mut saw_resumable_state = false;
    while let Ok(Some(kv)) = iter.next().await {
        let decoded =
            silo::codec::decode_concurrency_action(kv.value.clone()).expect("decode action");
        let fb = decoded.fb();
        let et = fb.variant_as_enqueue_task().expect("EnqueueTask variant");
        assert!(
            !et.task_id().unwrap_or_default().is_empty(),
            "deferred request must persist a task_id"
        );
        let li = et.limit_index();
        let hq: Vec<String> = et
            .held_queues()
            .map(|v| v.iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();
        // A chain resumed past index 0 must persist both limit_index AND a
        // matching held_queues. Both must be non-empty and consistent.
        if li > 0 || !hq.is_empty() {
            saw_resumable_state = true;
            assert_eq!(
                li as usize,
                hq.len(),
                "chain state consistency: limit_index should equal the count of held_queues for a non-initial deferred request; got li={li} hq={:?}",
                hq
            );
            assert!(
                hq.iter().any(|q| q == &queue_a),
                "deferred B-request must carry A in held_queues; got {:?}",
                hq
            );
        }
    }
    // With A capacity=3 the un-granted job is guaranteed to be deferred on
    // B at limit_index=1 with held_queues=[A], so we MUST observe the
    // resumable state.
    assert!(
        saw_resumable_state,
        "expected at least one deferred concurrency request to carry resumed-chain state (limit_index>0, held_queues=[A])"
    );
}

/// Future-scheduled jobs must not eat concurrency holders before their
/// scheduled run time.
///
/// Pre-fix: `handle_enqueue` calls `try_reserve` unconditionally, so a
/// future-scheduled job whose queue has free capacity is granted a holder
/// immediately. With enough future-scheduled jobs the queue fills up and
/// blocks present-time work that should be running now.
///
/// Setup: capacity-3 queue. Enqueue 3 future-scheduled jobs (start_at far
/// in the future). Then enqueue 1 present-time job. Pre-fix the 3 future
/// jobs grab the 3 holders and the present-time job is parked in the
/// request queue — `dequeue` returns nothing. Post-fix the future jobs
/// take no holders, and the present-time job is granted and dequeueable.
#[silo::test]
async fn future_scheduled_jobs_do_not_starve_immediate_work() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue = "future-starve-q".to_string();
    let future_at = now + 600_000; // ten minutes out

    // Fill the queue with future-scheduled jobs equal to its capacity.
    for i in 0..3 {
        shard
            .enqueue(
                tenant,
                None,
                10u8,
                future_at,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"f": i})),
                vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 3,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue future job");
    }

    // No holders should be in place for jobs that won't run for 10 minutes.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue).await,
        0,
        "future-scheduled jobs must not grab concurrency holders at enqueue time"
    );

    // A present-time job on the same queue should be free to run.
    let immediate_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"f": "now"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 3,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue immediate job");

    let tasks = shard
        .dequeue("w1", "default", 10)
        .await
        .expect("dequeue")
        .tasks;
    let ran_immediate = tasks.iter().any(|t| t.job().id() == immediate_id);
    assert!(
        ran_immediate,
        "present-time job should be granted and dequeueable; \
         future-scheduled jobs starved its slot. got {} tasks",
        tasks.len()
    );
}

/// Cancelling a job parked on a rate-limit step must release any
/// concurrency holders the chain accumulated upstream.
///
/// Setup: enqueue `[Conc-A, RateLimit]`. The chain grants A immediately and
/// the walker writes a `CheckRateLimit` task carrying `held_queues=[A]`.
/// Pre-fix the cancel path's catch-all match arm only deleted the task;
/// A's holder leaked. Post-fix, the new `Task::CheckRateLimit` arm
/// releases the held queue.
#[silo::test]
async fn cancel_check_rate_limit_releases_held_queues() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "cancel-crl-a".to_string();

    let rate_limit = GubernatorRateLimit {
        name: "api".to_string(),
        unique_key: "cancel-crl-key".to_string(),
        limit: 100,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 10,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_retries: 10,
        },
    };

    // Present-time enqueue so the chain immediately grants A and writes a
    // CheckRateLimit task carrying held_queues=[A]. (We don't dequeue, so
    // the task sits in the queue until we cancel.) Pre-fix this test used
    // a future start time, but future-scheduled jobs no longer grab holders
    // at enqueue time, so the multi-limit chain wouldn't accumulate A.
    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "crl"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(rate_limit),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue job");

    // Chain granted A immediately and wrote a CheckRateLimit carrying [A].
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1,
        "chain should have granted A at enqueue"
    );

    let crl_present = {
        let start = silo::keys::tasks_prefix();
        let end = silo::keys::end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        let mut found = false;
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(task) = decode_task(&kv.value)
                && let Task::CheckRateLimit {
                    job_id: jid,
                    ref held_queues,
                    ..
                } = task
                && jid == job_id
            {
                assert_eq!(
                    held_queues.as_slice(),
                    std::slice::from_ref(&queue_a),
                    "CheckRateLimit should carry [A] in held_queues"
                );
                found = true;
                break;
            }
        }
        found
    };
    assert!(crl_present, "expected a CheckRateLimit task for the job");

    shard.cancel_job(tenant, &job_id).await.expect("cancel");

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0,
        "cancelling a CheckRateLimit must release prior chain holders on A"
    );
}

/// Cancelling a job whose chain is parked on a deferred (immediate)
/// concurrency request must release any holders the chain accumulated
/// before the gating limit. Mirror of
/// `cancel_future_request_ticket_releases_held_queues` for the
/// `start_at_ms <= now_ms` case (TicketRequested, not
/// FutureRequestTaskWritten).
#[silo::test]
async fn cancel_deferred_concurrency_request_releases_held_queues() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "cancel-def-a".to_string();
    let queue_b = "cancel-def-b".to_string();

    // job1 holds B at capacity 1 so job2 must defer on B.
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
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1, "job1 should run and hold B");

    // job2: [A free, B full] at now_ms — chain grants A and writes a
    // concurrency_request_key for B with held_queues=[A].
    let job2_id = shard
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
                    max_concurrency: 1,
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

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1,
        "job2 should have grabbed A at enqueue"
    );
    assert!(
        count_concurrency_requests(shard.db()).await >= 1,
        "expected a deferred concurrency request for B"
    );

    shard
        .cancel_job(tenant, &job2_id)
        .await
        .expect("cancel job2");

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0,
        "cancelling a deferred concurrency request must release prior chain holders on A"
    );
    // The deferred request itself is also gone.
    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "cancel should delete the deferred concurrency request"
    );
}

/// Rate-limit retry must carry the chain's `task_id` forward. The chain's
/// holders are keyed by that id; if the retry's CheckRateLimit gets a
/// fresh UUID, the terminal RunAttempt's id diverges from the holders
/// previously written by the chain and worker completion releases under
/// the wrong key — leaking one slot per rate-limit retry.
///
/// This test exercises a job with `[Concurrency-A, RateLimit]` where
/// Gubernator says "over" on the first check and "under" on the second.
/// After the worker completes the RunAttempt, A must have zero holders.
#[silo::test]
async fn rate_limit_retry_preserves_chain_task_id() {
    use silo::gubernator::{GubernatorError, RateLimitClient, RateLimitResult};
    use silo::pb::gubernator::Algorithm;
    use std::sync::atomic::AtomicU32;

    /// Returns under_limit=false on the first N calls, then under_limit=true.
    struct FailFirstThenPassClient {
        fail_n: u32,
        seen: AtomicU32,
    }
    #[async_trait::async_trait]
    impl RateLimitClient for FailFirstThenPassClient {
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
            let under_limit = i >= self.fail_n;
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

    let client = Arc::new(FailFirstThenPassClient {
        fail_n: 1,
        seen: AtomicU32::new(0),
    });
    let (_tmp, shard) = open_temp_shard_with_rate_limiter(client.clone()).await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "rl-retry-a".to_string();

    let rate_limit = GubernatorRateLimit {
        name: "rl-retry".to_string(),
        unique_key: "rl-retry-key".to_string(),
        limit: 1,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 5,
            max_backoff_ms: 50,
            backoff_multiplier: 2.0,
            max_retries: 10,
        },
    };

    let _job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "rl-retry"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(rate_limit),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Chain grants A and writes a CheckRateLimit task; A's holder is in
    // place under the chain's `task_id`.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1,
        "chain should have granted A at enqueue"
    );

    // Drive dequeues: first dequeue picks up the CheckRateLimit, which
    // returns over-limit and schedules a retry. Second dequeue (after the
    // retry's backoff) picks up the retry, which now passes and writes a
    // RunAttempt under the *same* task_id.
    let mut leased_task_id: Option<String> = None;
    for _ in 0..50 {
        let tasks = shard
            .dequeue("w1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        if let Some(t) = tasks.into_iter().next() {
            leased_task_id = Some(t.attempt().task_id().to_string());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let leased_task_id = leased_task_id.expect("a RunAttempt should eventually be leased");
    assert!(
        client.seen.load(Ordering::SeqCst) >= 2,
        "the rate limiter should have been called at least twice (one fail, one pass)"
    );

    // Complete the RunAttempt. The release path keys by the leased
    // task_id; if the retry mints a fresh UUID, the holder key doesn't
    // match and A leaks.
    shard
        .report_attempt_outcome(&leased_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("complete RunAttempt");

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0,
        "A's holder must be released after the chain completes — rate-limit retry must reuse the chain's task_id"
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, &queue_a),
        0,
        "in-memory holder count for A must also drop to zero"
    );
}

/// Rate-limit retry must dodge the broker tombstone installed by the
/// parent CheckRateLimit's ack-delete, even with zero (or near-zero)
/// backoff. Pre-fix, `schedule_rate_limit_retry` wrote the retry at
/// `task_key(retry_at_ms, …)` with no bump — if `retry_at_ms` collided
/// with the parent's `start_time_ms` (zero backoff, or
/// `reset_time_ms == parent.start_time_ms`), the retry write landed on
/// the just-tombstoned key and was silently suppressed. The chain
/// stalled and every entry in the CheckRateLimit's `held_queues`
/// leaked.
///
/// This test scans the broker queue immediately after the first retry
/// is scheduled and asserts the retry's persisted `start_time_ms` is
/// strictly greater than the parent's — i.e. the dodge fires
/// regardless of clock skew between enqueue and dequeue.
#[silo::test]
async fn rate_limit_retry_dodges_parent_tombstone_with_zero_backoff() {
    use silo::gubernator::{GubernatorError, RateLimitClient, RateLimitResult};
    use silo::pb::gubernator::Algorithm;

    // Always-over-limit client so we can observe the retry that gets
    // scheduled without it being consumed by a successful dequeue first.
    struct AlwaysOverLimit;
    #[async_trait::async_trait]
    impl RateLimitClient for AlwaysOverLimit {
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
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(RateLimitResult {
                under_limit: false,
                limit,
                remaining: 0,
                reset_time_ms: now_ms + duration_ms,
                error: None,
            })
        }
        async fn health_check(&self) -> Result<(String, i32), GubernatorError> {
            Ok(("ok".to_string(), 1))
        }
    }

    let (_tmp, shard) = open_temp_shard_with_rate_limiter(Arc::new(AlwaysOverLimit)).await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "rl-tomb-a".to_string();

    // Zero-backoff rate-limit policy: this is the worst case for
    // `retry_at_ms == parent_start_time_ms` collisions.
    let rate_limit = GubernatorRateLimit {
        name: "rl-tomb".to_string(),
        unique_key: "rl-tomb-key".to_string(),
        limit: 1,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_multiplier: 1.0,
            max_retries: 10,
        },
    };

    let _job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "rl-tomb"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(rate_limit),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Snapshot the parent CheckRateLimit's task_key start_time_ms before
    // we let dequeue run.
    let parent_start_time_ms = {
        let start = silo::keys::tasks_prefix();
        let end = silo::keys::end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        let mut found = None;
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(task) = decode_task(&kv.value)
                && matches!(task, Task::CheckRateLimit { .. })
            {
                let parsed = silo::keys::parse_task_key(&kv.key).expect("parse task_key");
                found = Some(parsed.start_time_ms as i64);
                break;
            }
        }
        found.expect("a CheckRateLimit task should be in the queue")
    };

    // Drive one dequeue — Gubernator returns over-limit, so the handler
    // schedules a retry. With backoff=0 and `reset_time_ms` typically
    // landing in the same wall-clock millisecond as `parent_start_time_ms`,
    // pre-fix this would have produced a task_key that collides with the
    // tombstone and gets suppressed; the retry would never re-appear in
    // the queue.
    let _ = shard.dequeue("w1", "default", 1).await.expect("dequeue");

    // Scan again. The retry CheckRateLimit must be present AND its
    // task_key's start_time_ms must be strictly greater than the
    // parent's, demonstrating the dodge fired.
    let retry_start_time_ms = {
        let start = silo::keys::tasks_prefix();
        let end = silo::keys::end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        let mut found = None;
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(task) = decode_task(&kv.value)
                && matches!(task, Task::CheckRateLimit { .. })
            {
                let parsed = silo::keys::parse_task_key(&kv.key).expect("parse task_key");
                found = Some(parsed.start_time_ms as i64);
                break;
            }
        }
        found
    };
    let retry_start_time_ms = retry_start_time_ms
        .expect("retry CheckRateLimit should be in the queue after the over-limit response");

    assert!(
        retry_start_time_ms > parent_start_time_ms,
        "retry's task_key start_time_ms ({retry_start_time_ms}) must be > parent's ({parent_start_time_ms}) to dodge the tombstone"
    );
}

/// If the chain resumer is unavailable when the grant scanner runs (shard
/// shutting down, or never installed), the scanner must release every
/// in-memory reservation it made this pass and return without committing.
/// Otherwise it would leak phantom holders that no future request could
/// touch.
///
/// Setup: job1 holds A (cap=1). job2 enqueues with `[A]` and defers on A.
/// We drop the chain resumer, then release job1 to expose A's slot and
/// drive `process_concurrency_grants` synchronously. The scanner enters the
/// `chain_resumer = None` branch after `try_reserve` succeeds.
/// Post-call invariants:
///   - DB holder for A is absent (batch was not committed).
///   - The deferred request is still in the DB (batch was not committed).
///   - In-memory holder count for A is 0 (the just-made reservation was
///     released by the bail-out — no phantom holder).
#[silo::test]
async fn grant_scanner_releases_reservations_when_chain_resumer_missing() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue = "no-resumer-a".to_string();

    // job1 takes A.
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue job1");
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1, "job1 should run and hold A");
    let job1_task_id = job1_tasks[0].attempt().task_id().to_string();

    // job2 defers on A (capacity 1 is full).
    let _job2 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue job2");
    assert!(
        count_concurrency_requests(shard.db()).await >= 1,
        "expected a deferred concurrency request"
    );

    // Drop the chain resumer to force the bailout path. Keep the returned
    // Arc alive so the inner `ShardChainResumer`'s `Weak<JobStoreShard>` is
    // not the trigger — we want the chain_resumer field itself to be None.
    let _stashed = shard
        .take_chain_resumer_for_test()
        .expect("resumer should have been installed");

    // Release job1 so A has capacity. Use complete_attempt directly so the
    // background grant scanner doesn't race ahead — we want to drive
    // process_concurrency_grants explicitly.
    shard
        .report_attempt_outcome(&job1_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report job1 success");

    // Drive a synchronous grant pass. With the resumer missing, the scanner
    // tries `try_reserve` for job2, succeeds, then enters the resumer-None
    // branch and releases the reservation before returning. No grant is
    // produced.
    let granted = shard.process_concurrency_grants(tenant, &queue, 10).await;
    assert!(
        granted.is_empty(),
        "no grants should be produced when resumer is unavailable"
    );

    // The batch wasn't committed, so the deferred request is still there
    // and no DB holder was written for job2 on A.
    assert!(
        count_concurrency_requests(shard.db()).await >= 1,
        "deferred request must survive the aborted pass"
    );
    // job1's release dropped its holder; job2's reservation was released
    // by the bail-out, so the DB shows 0 holders on A.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue).await,
        0,
        "no DB holder should be written when the pass aborts before commit"
    );
    // Crucially, the in-memory count is also 0 — the just-made reservation
    // was released. If this leaks, the next request can never run because
    // try_reserve will see `holders == limit` forever.
    assert_eq!(
        shard.concurrency_holder_count(tenant, &queue),
        0,
        "in-memory reservation must be released when the resumer is missing"
    );
}

/// Multi-limit chain resume — the case the old code was silently
/// shortcutting on: a job with `[Concurrency-A, Concurrency-B, RateLimit]`.
/// Conc-A grants immediately at enqueue, Conc-B defers (capacity full), and
/// when Conc-B later grants via the grant scanner the chain MUST resume into
/// the RateLimit step (writing a `CheckRateLimit` task), not fabricate a
/// terminal `RunAttempt` that bypasses the rate limit.
///
/// Pre-fix: the grant scanner wrote a `RunAttempt` directly with
/// `held_queues = vec![conc_b]`, dropping Conc-A's holder and bypassing the
/// rate limit entirely. This test pins both invariants:
///   1. the follow-up task is a `CheckRateLimit`, not a `RunAttempt`;
///   2. it carries `held_queues = [Conc-A, Conc-B]` so the rate-limit step
///      releases both queues when it completes.
#[silo::test]
async fn deferred_concurrency_resumes_into_rate_limit() {
    use silo::keys::{end_bound, tasks_prefix};

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "chain-rl-a".to_string();
    let queue_b = "chain-rl-b".to_string();

    let rate_limit = GubernatorRateLimit {
        name: "api".to_string(),
        unique_key: "chain-rl-key".to_string(),
        limit: 100,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 10,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_retries: 10,
        },
    };

    // Job 1: [A(cap=2), B(cap=1)] — chain grants both. Lease it and hold
    // its slots so job 2 will defer on B.
    let _job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 2,
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
        .expect("enqueue job1");
    let job1_tasks = shard.dequeue("w1", "default", 1).await.unwrap().tasks;
    assert_eq!(job1_tasks.len(), 1);
    let job1_task_id = job1_tasks[0].attempt().task_id().to_string();

    // Job 2: [A(cap=2), B(cap=1), RateLimit]. Conc-A still has a slot, so the
    // chain grants A immediately; B is full so it defers via a
    // concurrency_request_key. The chain does NOT reach RateLimit yet.
    let job2 = shard
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
                    max_concurrency: 2,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_b.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(rate_limit.clone()),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue job2");

    // Sanity: job 2 holds Conc-A but not Conc-B yet.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        2
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        1
    );

    // No CheckRateLimit task should exist yet — the chain is parked on B.
    assert_eq!(
        count_check_rate_limit_tasks_for_tenant(shard.db(), tenant).await,
        0,
        "chain blocked on Conc-B should not have written a CheckRateLimit"
    );

    // Complete job 1 — releases its Conc-A and Conc-B holders. The grant
    // scanner then promotes job 2's deferred Conc-B request, the chain
    // resumes at limit_index = RateLimit, and writes a CheckRateLimit
    // task carrying held_queues = [A, B].
    shard
        .report_attempt_outcome(&job1_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report job1 success");

    // Poll for the CheckRateLimit task. Pre-fix the scanner would have
    // written a RunAttempt and bypassed the rate limit — that assertion
    // below would fail with `RunAttempt` instead.
    let mut check_rl: Option<(silo::keys::ParsedTaskKey, Task)> = None;
    for _ in 0..50 {
        if let Some((key, t)) = find_task_key_and_task_for_tenant(shard.db(), tenant).await
            && matches!(t, Task::CheckRateLimit { .. })
        {
            check_rl = Some((key, t));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let (check_rl_key, check_rl) = check_rl.expect(
        "expected a CheckRateLimit task for job 2 after Conc-B was promoted by the grant scanner; \
         if a RunAttempt was written instead, the chain shortcut bypassed RateLimit",
    );
    let job2_status = shard
        .get_job_status(tenant, &job2)
        .await
        .expect("load job2 status")
        .expect("job2 status exists");
    assert_eq!(job2_status.kind, JobStatusKind::Scheduled);
    assert_eq!(
        job2_status.next_attempt_starts_after_ms,
        Some(check_rl_key.start_time_ms as i64),
        "Scheduled status must point at the retargeted CheckRateLimit key"
    );

    // Verify held_queues = [A, B] — Conc-A's prior grant survived into the
    // resumed chain.
    let Task::CheckRateLimit { held_queues, .. } = &check_rl else {
        unreachable!("matched above");
    };
    assert!(
        held_queues.iter().any(|q| q == &queue_a),
        "CheckRateLimit must carry Conc-A's holder; held_queues={:?}",
        held_queues
    );
    assert!(
        held_queues.iter().any(|q| q == &queue_b),
        "CheckRateLimit must carry Conc-B's holder; held_queues={:?}",
        held_queues
    );
    assert_eq!(
        held_queues.len(),
        2,
        "CheckRateLimit held_queues should be exactly [A, B]; got {:?}",
        held_queues
    );

    // Holders for both queues should be present (the rate limit hasn't been
    // checked yet, so neither slot has been released).
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        1
    );

    shard
        .cancel_job(tenant, &job2)
        .await
        .expect("cancel job2 after grant scanner wrote CheckRateLimit");
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0,
        "cancel must find retargeted CheckRateLimit and release Conc-A"
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_b).await,
        0,
        "cancel must find retargeted CheckRateLimit and release Conc-B"
    );
    assert_eq!(
        count_check_rate_limit_tasks_for_tenant(shard.db(), tenant).await,
        0,
        "cancel must delete the retargeted CheckRateLimit task"
    );
    let _ = end_bound(&tasks_prefix());
}

#[silo::test]
async fn grant_scanner_hydrates_legacy_request_without_task_id_or_limits() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue = "legacy-request-q".to_string();

    let job1 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder job");
    let job2 = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue queued job");

    let (request_key, _) = first_concurrency_request_kv(shard.db())
        .await
        .expect("queued job should have a request");
    let parsed =
        silo::keys::parse_concurrency_request_key(&request_key).expect("request key should parse");
    assert_eq!(parsed.job_id, job2);

    // Simulate a request value written by the pre-upgrade schema: the durable
    // key still carries the request id, but the value has no task_id, limits,
    // held_queues, or limit_index fields.
    let legacy_value = encode_legacy_enqueue_task_value(&parsed, "default");
    shard
        .db()
        .put(&request_key, &legacy_value)
        .await
        .expect("overwrite request with legacy value");

    let leased_a = shard
        .dequeue("w1", "default", 1)
        .await
        .expect("dequeue job1")
        .tasks;
    assert_eq!(leased_a.len(), 1);
    assert_eq!(leased_a[0].job().id(), job1);
    let task1 = leased_a[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(&task1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("complete job1");

    let mut leased_b = Vec::new();
    for _ in 0..50 {
        leased_b = shard
            .dequeue("w2", "default", 1)
            .await
            .expect("dequeue job2")
            .tasks;
        if !leased_b.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_eq!(
        leased_b.len(),
        1,
        "legacy request should be granted, not deleted as corrupt"
    );
    assert_eq!(leased_b[0].job().id(), job2);
    assert_eq!(
        count_concurrency_requests(shard.db()).await,
        0,
        "granted legacy request should be consumed"
    );
}

async fn find_task_key_and_task_for_tenant(
    db: &silo::instrumented_db::InstrumentedDb,
    tenant: &str,
) -> Option<(silo::keys::ParsedTaskKey, Task)> {
    let start = silo::keys::tasks_prefix();
    let end = silo::keys::end_bound(&start);
    let mut iter = db
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .ok()?;
    while let Ok(Some(kv)) = iter.next().await {
        let Some(parsed) = silo::keys::parse_task_key(&kv.key) else {
            continue;
        };
        if let Ok(task) = decode_task(&kv.value)
            && task.tenant() == tenant
        {
            return Some((parsed, task));
        }
    }
    None
}

fn encode_legacy_enqueue_task_value(
    parsed: &silo::keys::ParsedConcurrencyRequestKey,
    task_group: &str,
) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(128);
    let job_id = builder.create_string(&parsed.job_id);
    let task_group = builder.create_string(task_group);
    let et = silo::fb::silo::fb::EnqueueTask::create(
        &mut builder,
        &silo::fb::silo::fb::EnqueueTaskArgs {
            start_time_ms: parsed.start_time_ms as i64,
            priority: parsed.priority,
            job_id: Some(job_id),
            attempt_number: parsed.attempt_number,
            relative_attempt_number: parsed.attempt_number,
            task_group: Some(task_group),
            limit_index: 0,
            held_queues: None,
            task_id: None,
            limits: None,
        },
    );
    let root = silo::fb::silo::fb::ConcurrencyAction::create(
        &mut builder,
        &silo::fb::silo::fb::ConcurrencyActionArgs {
            variant_type: silo::fb::silo::fb::ConcurrencyActionVariant::EnqueueTask,
            variant: Some(et.as_union_value()),
        },
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

/// Helper: count `CheckRateLimit` tasks for a tenant.
async fn count_check_rate_limit_tasks_for_tenant(
    db: &silo::instrumented_db::InstrumentedDb,
    tenant: &str,
) -> usize {
    let start = silo::keys::tasks_prefix();
    let end = silo::keys::end_bound(&start);
    let mut iter = db
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .expect("scan tasks");
    let mut count = 0;
    while let Ok(Some(kv)) = iter.next().await {
        if let Ok(task) = decode_task(&kv.value)
            && task.tenant() == tenant
            && matches!(task, Task::CheckRateLimit { .. })
        {
            count += 1;
        }
    }
    count
}

/// `handle_request_ticket`'s terminal-status short-circuit must drop a future
/// `Task::RequestTicket` (ack-delete the task) and release any `held_queues`
/// it carries. Today's enqueue path always writes a future RequestTicket with
/// `held_queues=[]` — but the cleanup is defensive against a hypothetical
/// future writer producing non-empty held_queues, and the *core* terminal
/// short-circuit (drop the ticket cleanly rather than process it for a
/// cancelled job) is exercised on every cancel-races-broker-buffer scenario.
///
/// Setup races the broker-eviction step of `cancel_job`: we enqueue a
/// far-future RequestTicket, manually overwrite job status to `Cancelled`
/// (bypassing cancel's task-delete + broker-evict), and inject the still-
/// persisted ticket into the broker buffer via the test hook. Dequeue should
/// observe the Cancelled status and drop the ticket without producing a
/// RunAttempt or leaking any holder.
#[silo::test]
async fn request_ticket_terminal_short_circuit_drops_ticket() {
    use silo::codec::encode_job_status;
    use silo::job::{JobStatus, JobStatusKind};
    use silo::keys::{end_bound, job_status_key, tasks_prefix};

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue = "rt-terminal-q".to_string();
    let future_at = now + 600_000;

    // Far-future enqueue ⇒ writes a `Task::RequestTicket` at scheduled_at_ms.
    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            future_at,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "rt-term"})),
            vec![Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue future");

    // The future-scheduled path holds no slot at enqueue.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue).await,
        0,
        "future-scheduled job must not grab a concurrency slot at enqueue",
    );

    // Locate the persisted RequestTicket task_key. The broker scanner will
    // skip it (start_time_ms > now_ms), so we'll inject it directly.
    let ticket_key = {
        let start = tasks_prefix();
        let end = end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        let mut found: Option<Vec<u8>> = None;
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(Task::RequestTicket { job_id: jid, .. }) = decode_task(&kv.value)
                && jid == job_id
            {
                found = Some(kv.key.to_vec());
                break;
            }
        }
        found.expect("expected a RequestTicket task in the DB")
    };

    // Overwrite job_status to Cancelled directly. We deliberately do NOT call
    // `cancel_job` here — the production path deletes the task and evicts it
    // from the broker buffer, which would short-circuit this test. The race
    // we want to exercise is: status flipped terminal, but the ticket survived
    // (e.g. cancel raced the broker scan and we missed the evict window).
    let cancelled = JobStatus::new(JobStatusKind::Cancelled, now, None, None);
    shard
        .db()
        .put(
            &job_status_key(tenant, &job_id),
            &encode_job_status(&cancelled),
        )
        .await
        .expect("write cancelled status");

    // Inject the future RequestTicket into the broker buffer so dequeue can
    // claim it without waiting for the scanner (which would skip it as future).
    let injected = shard
        .force_buffer_tasks_for_test(vec![ticket_key.clone()])
        .await
        .expect("inject ticket");
    assert_eq!(injected, 1, "ticket should have been injected");

    // Dequeue: handler decodes the RequestTicket, reads cancelled status,
    // and takes the terminal short-circuit (ack-delete + release held_queues).
    let result = shard.dequeue("w1", "default", 1).await.expect("dequeue");
    assert!(
        result.tasks.is_empty(),
        "no RunAttempt should be produced for a cancelled job",
    );

    // Post-conditions: ticket is gone from DB, no orphan holders anywhere.
    assert!(
        shard
            .db()
            .get(&ticket_key)
            .await
            .expect("get ticket key")
            .is_none(),
        "terminal short-circuit must ack-delete the ticket",
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue).await,
        0,
        "no DB holder should exist post-short-circuit",
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, &queue),
        0,
        "no in-memory holder should exist post-short-circuit",
    );
}

/// Commit 17d94a8 removed the interim `RunAttempt` write inside
/// `append_grant_edits`. Before that fix, a multi-limit chain transiently
/// wrote a `RunAttempt(held=[just_this_queue])` for each granted limit,
/// then overwrote it at the terminal step with the full `held_queues`. If
/// slatedb's batch-vs-scan visibility ever exposed an in-flight batch state,
/// a worker could lease the interim and under-release on completion.
///
/// This test pins the post-fix invariant: at any time after a multi-limit
/// enqueue commits, the ONLY `Task::RunAttempt` persisted for that job is
/// the terminal one with the full `held_queues`. (A regression that
/// re-introduces the interim would persist two RunAttempts at different
/// task_keys — caught by the "exactly one" assertion below.)
#[silo::test]
async fn multi_limit_enqueue_persists_only_terminal_run_attempt() {
    use silo::keys::{end_bound, tasks_prefix};

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "interim-run-attempt-a".to_string();
    let queue_b = "interim-run-attempt-b".to_string();
    let queue_c = "interim-run-attempt-c".to_string();

    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "no-interim"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 10,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_b.clone(),
                    max_concurrency: 10,
                }),
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_c.clone(),
                    max_concurrency: 10,
                }),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Scan all tasks for this job_id.
    let start = tasks_prefix();
    let end = end_bound(&start);
    let mut iter = shard
        .db()
        .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
        .await
        .expect("scan tasks");
    let mut run_attempts: Vec<(Vec<u8>, Vec<String>)> = Vec::new();
    while let Ok(Some(kv)) = iter.next().await {
        if let Ok(Task::RunAttempt {
            job_id: ref jid,
            ref held_queues,
            ..
        }) = decode_task(&kv.value)
            && *jid == job_id
        {
            run_attempts.push((kv.key.to_vec(), held_queues.clone()));
        }
    }

    assert_eq!(
        run_attempts.len(),
        1,
        "exactly one RunAttempt should be persisted; an interim leak would write multiple at different task_keys. got: {run_attempts:?}",
    );
    let (_key, held) = &run_attempts[0];
    let held_set: HashSet<&String> = held.iter().collect();
    let expected: HashSet<String> = [queue_a.clone(), queue_b.clone(), queue_c.clone()]
        .into_iter()
        .collect();
    let expected_refs: HashSet<&String> = expected.iter().collect();
    assert_eq!(
        held_set, expected_refs,
        "the single RunAttempt must carry the full accumulated held_queues",
    );
}

/// Rate-limit retry chain must preserve the chain's `task_id` across EVERY
/// retry — not just the first — and on exhaustion must release every
/// `held_queues` entry exactly once. The existing
/// `rate_limit_retry_preserves_chain_task_id` covers one retry → success;
/// this exercises the multi-retry → exhaustion path that
/// `handle_check_rate_limit`'s `retry_count >= max_retries` branch handles.
///
/// Pre-fix on the task_id path: every retry minted a fresh UUID, so the
/// terminal exhaustion's holder-key reconstruction (which keys by
/// `check_task_id`) referred to a different id than the chain's original
/// holder, leaving A leaked. With N retries that's N leaks per failed
/// chain — worse than the one-retry case the original regression test
/// caught.
#[silo::test]
async fn rate_limit_retry_chain_preserves_task_id_through_exhaustion() {
    use silo::codec::decode_task;
    use silo::gubernator::{GubernatorError, RateLimitClient, RateLimitResult};
    use silo::pb::gubernator::Algorithm;

    struct AlwaysOverLimit;
    #[async_trait::async_trait]
    impl RateLimitClient for AlwaysOverLimit {
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
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(RateLimitResult {
                under_limit: false,
                limit,
                remaining: 0,
                reset_time_ms: now_ms + duration_ms,
                error: None,
            })
        }
        async fn health_check(&self) -> Result<(String, i32), GubernatorError> {
            Ok(("ok".to_string(), 1))
        }
    }

    let (_tmp, shard) = open_temp_shard_with_rate_limiter(Arc::new(AlwaysOverLimit)).await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "rl-exhaust-a".to_string();

    // Zero backoff so retries fire on the next broker scan with `task_key_start_ms`
    // bumped only by the +1ms tombstone dodge.
    let max_retries: u32 = 3;
    let rate_limit = GubernatorRateLimit {
        name: "rl-exhaust".to_string(),
        unique_key: "rl-exhaust-key".to_string(),
        limit: 1,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_multiplier: 1.0,
            max_retries,
        },
    };

    let _job = shard
        .enqueue(
            tenant,
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "rl-exhaust"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
                }),
                Limit::RateLimit(rate_limit),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Chain grants A and writes the initial CheckRateLimit. Snapshot its
    // task_id — every retry must preserve it.
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1,
        "chain should have granted A at enqueue"
    );
    let initial_task_id = {
        let start = silo::keys::tasks_prefix();
        let end = silo::keys::end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        let mut found = None;
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(Task::CheckRateLimit { task_id, .. }) = decode_task(&kv.value) {
                found = Some(task_id);
                break;
            }
        }
        found.expect("initial CheckRateLimit must exist")
    };

    // Drive dequeue repeatedly. Each iteration consumes the current
    // CheckRateLimit, calls Gubernator (returns over-limit), and either
    // schedules a retry or exhausts. We bound at 4*max_retries to avoid an
    // infinite loop if the chain stalls (which itself would be a bug —
    // the broker tombstone tests would have caught it first).
    for _ in 0..(max_retries as usize * 4 + 4) {
        let _ = shard.dequeue("w1", "default", 10).await.expect("dequeue");
        // Every CheckRateLimit currently in the DB must carry the original
        // chain's task_id. If the retry path mints a fresh id, this fires.
        let start = silo::keys::tasks_prefix();
        let end = silo::keys::end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(Task::CheckRateLimit { task_id, .. }) = decode_task(&kv.value) {
                assert_eq!(
                    task_id, initial_task_id,
                    "every CheckRateLimit retry must preserve the chain's task_id",
                );
            }
        }

        // Exhaustion: no more CheckRateLimit tasks in the DB.
        let remaining = count_check_rate_limit_tasks_for_tenant(shard.db(), tenant).await;
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Post-exhaustion: holders released both on disk and in-memory.
    assert_eq!(
        count_check_rate_limit_tasks_for_tenant(shard.db(), tenant).await,
        0,
        "all CheckRateLimit retries must be drained at exhaustion",
    );
    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0,
        "exhaustion must release the chain's held_queues from the DB",
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, &queue_a),
        0,
        "exhaustion must release the chain's held_queues from in-memory",
    );
}

/// Symmetric with `cancel_check_rate_limit_releases_held_queues` and
/// `cancel_deferred_concurrency_request_releases_held_queues`: cancelling a
/// job parked on a future `Task::RequestTicket` carrying upstream
/// `held_queues` must release those holders. The natural enqueue path
/// always writes future tickets with `held_queues=[]` today, so this test
/// fabricates the multi-limit-future state directly and pins the cancel.rs
/// `Task::RequestTicket` arm against a future regression that drops the
/// `for queue in held_queues { ... }` loop.
#[silo::test]
async fn cancel_request_ticket_releases_upstream_holders() {
    use silo::codec::{decode_task, encode_holder, encode_task};
    use silo::keys::{concurrency_holder_key, end_bound, tasks_prefix};
    use silo::task::HolderRecord;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let tenant = "-";
    let queue_a = "cancel-rt-a".to_string();
    let queue_b = "cancel-rt-b".to_string();
    let future_at = now + 600_000;

    // Enqueue a far-future job. Real JobInfo + JobStatus + RequestTicket
    // land in the DB. The chain walker can't naturally produce a future
    // RequestTicket with `held_queues=[A]` today (future-scheduling stops
    // the chain on its first limit before any holder is accumulated), so
    // we'll rewrite the persisted ticket and seed an A holder below.
    let job_id = shard
        .enqueue(
            tenant,
            None,
            10u8,
            future_at,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "cancel-rt"})),
            vec![
                Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue_a.clone(),
                    max_concurrency: 1,
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
        .expect("enqueue future");

    // Locate the persisted RequestTicket.
    let (ticket_key, ticket_task) = {
        let start = tasks_prefix();
        let end = end_bound(&start);
        let mut iter = shard
            .db()
            .scan_with_options::<Vec<u8>, _>(start..end, &silo::scan_options())
            .await
            .expect("scan tasks");
        let mut found = None;
        while let Ok(Some(kv)) = iter.next().await {
            if let Ok(task) = decode_task(&kv.value)
                && let Task::RequestTicket {
                    job_id: ref jid, ..
                } = task
                && *jid == job_id
            {
                found = Some((kv.key.to_vec(), task));
                break;
            }
        }
        found.expect("expected a RequestTicket task")
    };

    let task_id = match &ticket_task {
        Task::RequestTicket { task_id, .. } => task_id.clone(),
        _ => unreachable!(),
    };

    // Rewrite the ticket to carry `held_queues=[A]` as if an earlier chain
    // step had granted A. (Today's walker can't produce this, but cancel.rs's
    // RequestTicket arm has a `for queue in held_queues` loop to handle it.)
    let morphed = match ticket_task {
        Task::RequestTicket {
            queue,
            start_time_ms,
            priority,
            tenant: t,
            job_id: jid,
            attempt_number,
            relative_attempt_number,
            task_id: tid,
            task_group,
            limit_index,
            held_queues: _,
            limits,
        } => Task::RequestTicket {
            queue,
            start_time_ms,
            priority,
            tenant: t,
            job_id: jid,
            attempt_number,
            relative_attempt_number,
            task_id: tid,
            task_group,
            limit_index,
            held_queues: vec![queue_a.clone()],
            limits,
        },
        _ => unreachable!(),
    };
    shard
        .db()
        .put(&ticket_key, &encode_task(&morphed))
        .await
        .expect("rewrite ticket with held_queues=[A]");

    // Seed the matching fake A holder (DB + in-memory) under the chain's
    // task_id. Cancel's post-commit atomic_release keys by task_id; if either
    // copy isn't seeded, that assertion stays at 0 trivially and would mask a
    // missed release.
    shard
        .db()
        .put(
            &concurrency_holder_key(tenant, &queue_a, &task_id),
            &encode_holder(&HolderRecord { granted_at_ms: now }),
        )
        .await
        .expect("seed DB holder for A");
    let reserved = shard
        .force_reserve_concurrency_for_test(tenant, &queue_a, &task_id, 1)
        .await;
    assert!(reserved, "in-memory reservation should succeed");

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        1,
        "fake A holder should be live before cancel",
    );
    assert_eq!(shard.concurrency_holder_count(tenant, &queue_a), 1);

    shard.cancel_job(tenant, &job_id).await.expect("cancel");

    assert_eq!(
        count_holders_for_queue(shard.db(), tenant, &queue_a).await,
        0,
        "cancelling a future RequestTicket must release its held_queues from the DB",
    );
    assert_eq!(
        shard.concurrency_holder_count(tenant, &queue_a),
        0,
        "cancelling a future RequestTicket must release its held_queues from the in-memory cache",
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
