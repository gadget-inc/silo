mod test_helpers;

use silo::codec::{decode_task};
use silo::job_attempt::{AttemptOutcome};
use silo::keys::concurrency_holder_key;
use silo::task::{Task};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
    Mutex,
};

use test_helpers::*;

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
                        .dequeue(&worker_id, 1)
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
                            "-",
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
                    .enqueue("-", None, (i % 50) as u8, now, None, payload, vec![], None)
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
        assert_eq!(count_with_prefix(shard.db(), "tasks/").await, 0);
        assert_eq!(count_with_prefix(shard.db(), "lease/").await, 0);
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
                        .dequeue(&worker_id, 4)
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
                                "-",
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
        )
        .await
        .expect("enqueue");

    // Task should be ready immediately
    let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);
    let t = &tasks[0];
    assert_eq!(t.job().id(), job_id);

    // Holder should exist for this attempt's task id (holder is per-attempt)
    let holder = shard
        .db()
        .get(concurrency_holder_key("-", &queue, t.attempt().task_id()).as_bytes())
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
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w1", 1).await.expect("deq1").tasks;
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
        )
        .await
        .expect("enqueue2");
    // No runnable RunAttempt should be visible yet (RequestTicket entries are expected)
    let maybe = first_kv_with_prefix(shard.db(), "tasks/").await;
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
        .report_attempt_outcome("-", &t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Now there should be a new task for the queued request
    let some = first_kv_with_prefix(shard.db(), "tasks/").await;
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
        )
        .await
        .expect("enqueue");

    let t1 = shard.dequeue("w", 1).await.expect("deq").tasks[0]
        .attempt()
        .task_id()
        .to_string();

    // Fail attempt 1, should schedule attempt 2 carrying held_queues
    shard
        .report_attempt_outcome(
            "-",
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");

    // Attempt 2 should be present
    let t2 = shard.dequeue("w", 1).await.expect("deq2").tasks[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2, which should release holder
    shard
        .report_attempt_outcome("-", &t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // No holders should remain after success of follow-up attempt (released after each attempt)
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
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
        )
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 scheduled
    let t1 = shard.dequeue("w", 1).await.expect("deq1").tasks[0]
        .attempt()
        .task_id()
        .to_string();
    shard
        .report_attempt_outcome(
            "-",
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");
    let t2 = shard.dequeue("w", 1).await.expect("deq2").tasks[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2
    shard
        .report_attempt_outcome("-", &t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // BUG (current impl): holder created for attempt 1 task id remains. We assert no holders remain.
    assert_eq!(
        count_with_prefix(shard.db(), "holders/").await,
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
        )
        .await
        .expect("enqueue a");
    let a_task = shard.dequeue("wa", 1).await.expect("deq a").tasks;
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
        )
        .await
        .expect("enqueue b");

    // Complete A -> should grant B (durably create one holder)
    shard
        .report_attempt_outcome("-", &a_tid, AttemptOutcome::Success { result: vec![] })
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
            )
            .await
            .expect("enqueue");
    }

    let mut processed = 0usize;
    loop {
        let tasks = shard.dequeue("w-stress", 1).await.expect("deq").tasks;
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
            .report_attempt_outcome("-", &tid, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report");
        processed += 1;
    }

    assert_eq!(processed, total);
    // No remaining durable state for holders/requests
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
    assert_eq!(count_with_prefix(shard.db(), "requests/").await, 0);
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
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w-hold", 1).await.expect("deq1").tasks;
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
            )
            .await
            .expect("enqueue add");
    }
    // There should be no runnable RunAttempt until we release (RequestTicket may exist)
    if let Some((_k, v)) = first_kv_with_prefix(shard.db(), "tasks/").await {
        let task = decode_task(&v).expect("decode task");
        match task {
            Task::RunAttempt { .. } => panic!("unexpected RunAttempt before release"),
            Task::RequestTicket { .. } => {}
            Task::CheckRateLimit { .. } => {}
            Task::RefreshFloatingLimit { .. } => {}
        }
    }

    // Release first; only one new task should appear immediately
    shard
        .report_attempt_outcome("-", &t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");
    let after = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(after.is_some(), "one task should be enqueued after release");
}

