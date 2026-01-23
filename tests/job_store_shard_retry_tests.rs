mod test_helpers;

use silo::codec::{decode_lease, decode_task, encode_lease};
use rkyv::Archive;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::JobStoreShardError;
use silo::retry::{next_retry_time_ms, RetryPolicy};
use silo::task::{LeaseRecord, Task};

use test_helpers::*;

#[silo::test]
async fn reporting_attempt_outcome_updates_attempt_and_deletes_lease() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", "default", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Outcome: success
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");

        // Lease should be gone
        let lease_key = format!("lease/{}", task_id);
        let lease = shard
            .db()
            .get(lease_key.as_bytes())
            .await
            .expect("get lease");
        assert!(lease.is_none(), "lease should be deleted");

        // Attempt state should be updated
        let attempt_view = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt view")
            .expect("attempt exists");
        match attempt_view.state() {
            AttemptStatus::Succeeded { .. } => {}
            _ => panic!("expected Succeeded"),
        }
    });
}

#[silo::test]
async fn error_with_no_retries_does_not_enqueue_next_attempt() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        // Enqueue with retry_count = 0 (only first attempt, no retries)
        let policy = RetryPolicy {
            retry_count: 0,
            initial_interval_ms: 1000,
            max_interval_ms: i64::MAX,
            randomize_interval: false,
            backoff_factor: 2.0,
        };
        let _job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                Some(policy),
                payload,
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"x".to_vec(),
                },
            )
            .await
            .expect("report");

        // No new tasks should be present
        let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(none_left.is_none(), "no follow-up task should be enqueued");
    });
}

#[silo::test]
async fn error_with_retries_enqueues_next_attempt_until_limit() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        // Allow 2 retries (so attempts 1 -> error → attempt 2 scheduled; error again → attempt 3 scheduled; then stop)
        let policy = RetryPolicy {
            retry_count: 2,
            initial_interval_ms: 1,
            max_interval_ms: i64::MAX,
            randomize_interval: false,
            backoff_factor: 1.0,
        };
        let _job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                Some(policy),
                payload,
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue");

        // Run attempt 1 and error
        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e1".to_vec(),
                },
            )
            .await
            .expect("report");

        // One new task should exist (attempt 2)
        let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
            .await
            .expect("task2");
        let task2 = decode_task(&v2).expect("decode task");
        let attempt = match task2 {
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
        assert_eq!(attempt, 2);

        // Dequeue attempt 2 and error again
        let tasks2 = shard.dequeue("w", "default", 1).await.expect("dequeue2").tasks;
        let t2 = tasks2[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t2,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e2".to_vec(),
                },
            )
            .await
            .expect("report2");

        // One new task should exist (attempt 3), but no more beyond that after another error
        let (_k3, v3) = first_kv_with_prefix(shard.db(), "tasks/")
            .await
            .expect("task3");
        let task3 = decode_task(&v3).expect("decode task");
        let attempt3 = match task3 {
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
        assert_eq!(attempt3, 3);

        // Dequeue attempt 3 and error again — but no further tasks since retries exhausted
        let tasks3 = shard.dequeue("w", "default", 1).await.expect("dequeue3").tasks;
        let t3 = tasks3[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t3,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e3".to_vec(),
                },
            )
            .await
            .expect("report3");
        let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(none_left.is_none(), "no attempt 4 should be enqueued");
    });
}

#[silo::test]
async fn double_reporting_same_attempt_is_idempotent_success_then_success() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", "default", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // First report: success
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");

        // Second report: expect LeaseNotFound since lease was removed on first report
        let err = shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect_err("second report should error with LeaseNotFound");
        match err {
            JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, task_id),
            other => panic!("unexpected error: {other:?}"),
        }

        // Lease should remain gone
        let lease_key = format!("lease/{}", task_id);
        let lease = shard
            .db()
            .get(lease_key.as_bytes())
            .await
            .expect("get lease");
        assert!(lease.is_none(), "lease should be deleted");

        // Attempt state should be succeeded
        let attempt_view = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt view")
            .expect("attempt exists");
        match attempt_view.state() {
            AttemptStatus::Succeeded { .. } => {}
            _ => panic!("expected Succeeded"),
        }

        // No additional tasks should be enqueued
        let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(none_left.is_none(), "no follow-up task should be enqueued");
    });
}

#[silo::test]
async fn double_reporting_same_attempt_is_idempotent_success_then_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v2"}));
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", "default", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // First report: success
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");

        // Second report: error, expect LeaseNotFound
        let err = shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect_err("second report should error with LeaseNotFound");
        match err {
            JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, task_id),
            other => panic!("unexpected error: {other:?}"),
        }

        // Attempt state should still be succeeded
        let attempt_view = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt view")
            .expect("attempt exists");
        match attempt_view.state() {
            AttemptStatus::Succeeded { .. } => {}
            _ => panic!("expected Succeeded"),
        }

        // No tasks should be enqueued
        let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(none_left.is_none(), "no follow-up task should be enqueued");
    });
}

#[silo::test]
async fn retry_count_one_boundary_enqueues_attempt2_then_stops_on_second_error() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let priority = 10u8;
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
            priority,
            now,
            Some(policy.clone()),
            payload,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 should be enqueued
    let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue1").tasks;
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e1".to_vec(),
            },
        )
        .await
        .expect("report1");

    // Verify attempt 2 task exists
    let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("task2");
    let task2 = decode_task(&v2).expect("decode task");
    let attempt2 = match task2 {
        Task::RunAttempt { attempt_number, .. } => attempt_number,
        Task::RequestTicket { .. } => {
            panic!("unexpected RequestTicket in tasks/ for this test")
        }
        Task::CheckRateLimit { .. } => panic!("unexpected CheckRateLimit in tasks/ for this test"),
        Task::RefreshFloatingLimit { .. } => {
            panic!("unexpected RefreshFloatingLimit in tasks/ for this test")
        }
    };
    assert_eq!(attempt2, 2);

    // Run attempt 2 and fail -> no attempt 3
    let tasks2 = shard.dequeue("w", "default", 1).await.expect("dequeue2").tasks;
    let t2 = tasks2[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t2,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e2".to_vec(),
            },
        )
        .await
        .expect("report2");

    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no attempt 3 should be enqueued");

    // Attempt 2 state should be Failed
    let a2_view = shard
        .get_job_attempt("-", &job_id, 2)
        .await
        .expect("get attempt2 view")
        .expect("attempt2 exists");
    match a2_view.state() {
        AttemptStatus::Failed { .. } => {}
        _ => panic!("expected Failed"),
    }
}

#[silo::test]
async fn next_retry_time_matches_scheduled_time_smoke() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let policy = RetryPolicy {
            retry_count: 5,
            initial_interval_ms: 25,
            max_interval_ms: 1_000_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        };
        let job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                Some(policy.clone()),
                payload.clone(),
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e".to_vec(),
                },
            )
            .await
            .expect("report1");

        // Read attempt1 finished_at and next task key time
        let attempt1 = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get a1")
            .expect("a1 exists");
        let finished_at = match attempt1.state() {
            AttemptStatus::Failed { finished_at_ms, .. } => finished_at_ms,
            _ => panic!("expected Failed"),
        };
        let (k2, _v2) = first_kv_with_prefix(shard.db(), "tasks/")
            .await
            .expect("task2");
        let scheduled_ms = parse_time_from_task_key(&k2).expect("parse time");
        let expected = next_retry_time_ms(finished_at, 1, &policy).unwrap() as u64;
        assert_eq!(
            scheduled_ms, expected,
            "scheduled time should equal next_retry_time_ms"
        );
    });
}

#[silo::test]
async fn duplicate_reporting_error_then_error_is_rejected_and_no_extra_tasks() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let policy = RetryPolicy {
            retry_count: 2,
            initial_interval_ms: 1,
            max_interval_ms: i64::MAX,
            randomize_interval: false,
            backoff_factor: 1.0,
        };
        let _job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                Some(policy),
                payload,
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report1");

        // Duplicate error report should fail with LeaseNotFound
        let err = shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"boom2".to_vec(),
                },
            )
            .await
            .expect_err("dup should fail");
        match err {
            JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, t1),
            other => panic!("unexpected error: {other:?}"),
        }

        // Only one follow-up task exists
        let count = count_with_prefix(shard.db(), "tasks/").await;
        assert_eq!(count, 1);
    });
}

#[silo::test]
async fn duplicate_reporting_error_then_success_is_rejected_and_state_persists() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", "default", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e".to_vec(),
                },
            )
            .await
            .expect("report1");
        let err = shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect_err("dup");
        match err {
            JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, t1),
            other => panic!("unexpected error: {other:?}"),
        }

        let a1 = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get a1")
            .expect("exists");
        match a1.state() {
            AttemptStatus::Failed { .. } => {}
            _ => panic!("a1 should remain Failed"),
        }
    });
}

#[silo::test]
async fn attempt_records_exist_across_retries_and_task_ids_distinct() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let policy = RetryPolicy {
            retry_count: 2,
            initial_interval_ms: 1,
            max_interval_ms: i64::MAX,
            randomize_interval: false,
            backoff_factor: 1.0,
        };
        let job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                Some(policy),
                payload,
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue");

        let tasks1 = shard.dequeue("w", "default", 1).await.expect("dequeue1").tasks;
        let t1 = tasks1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &t1,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e1".to_vec(),
                },
            )
            .await
            .expect("report1");
        let (_k2, _v2) = first_kv_with_prefix(shard.db(), "tasks/")
            .await
            .expect("task2");
        let tasks2 = shard.dequeue("w", "default", 1).await.expect("dequeue2").tasks;
        let t2 = tasks2[0].attempt().task_id().to_string();
        assert_ne!(t1, t2, "task ids should be distinct across attempts");
        shard
            .report_attempt_outcome(
                &t2,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"e2".to_vec(),
                },
            )
            .await
            .expect("report2");

        let a1 = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get a1")
            .expect("a1 exists");
        let a2 = shard
            .get_job_attempt("-", &job_id, 2)
            .await
            .expect("get a2")
            .expect("a2 exists");
        match a1.state() {
            AttemptStatus::Failed { .. } => {}
            _ => panic!("a1 should be Failed"),
        }
        match a2.state() {
            AttemptStatus::Failed { .. } => {}
            _ => panic!("a2 should be Failed"),
        }
    });
}

#[silo::test]
async fn outcome_payload_edge_cases_empty_vectors_round_trip() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success { result: Vec::new() },
            )
            .await
            .expect("report ok");
        let a1 = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get")
            .expect("exists");
        match a1.state() {
            AttemptStatus::Succeeded { result, .. } => {
                assert_eq!(result.len(), 0)
            }
            _ => panic!("expected Succeeded"),
        }

        // Re-enqueue a new job for error case
        let job_id2 = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"k": "v2"})),
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue2");
        let tasks2 = shard.dequeue("w", "default", 1).await.expect("dequeue2").tasks;
        let task2 = tasks2[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task2,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: Vec::new(),
                },
            )
            .await
            .expect("report err");
        let a_err = shard
            .get_job_attempt("-", &job_id2, 1)
            .await
            .expect("get2")
            .expect("exists2");
        match a_err.state() {
            AttemptStatus::Failed { error, .. } => {
                assert_eq!(error.len(), 0)
            }
            _ => panic!("expected Failed"),
        }
    });
}

#[silo::test]
async fn large_outcome_payloads_round_trip() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        let big_ok = vec![1u8; 2_000_000];
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: big_ok.clone(),
                },
            )
            .await
            .expect("report ok");
        let a1 = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get")
            .expect("exists");
        match a1.state() {
            AttemptStatus::Succeeded { result, .. } => {
                assert_eq!(result.len(), big_ok.len())
            }
            _ => panic!("expected Succeeded"),
        }

        let job_id2 = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"k": "v2"})),
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue2");
        let tasks2 = shard.dequeue("w", "default", 1).await.expect("dequeue2").tasks;
        let task2 = tasks2[0].attempt().task_id().to_string();
        let big_err = vec![2u8; 2_000_000];
        shard
            .report_attempt_outcome(
                &task2,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: big_err.clone(),
                },
            )
            .await
            .expect("report err");
        let a2 = shard
            .get_job_attempt("-", &job_id2, 1)
            .await
            .expect("get2")
            .expect("exists2");
        match a2.state() {
            AttemptStatus::Failed { error, .. } => {
                assert_eq!(error.len(), big_err.len())
            }
            _ => panic!("expected Failed"),
        }
    });
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
        .enqueue("-", None, 10u8, now, Some(policy), payload, vec![], None, "default")
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
    let _leased_task_id = tasks[0].attempt().task_id().to_string();

    // Find the lease and rewrite expiry to the past
    let (lease_key, lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");
    type ArchivedTask = <Task as Archive>::Archived;
    let decoded = decode_lease(&lease_value).expect("decode lease");
    let archived = decoded.archived();
    let task = match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            tenant,
            job_id,
            attempt_number,
            held_queues: _, ..
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            tenant: tenant.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: Vec::new(),
            task_group: "default".to_string(),
        },
        ArchivedTask::RequestTicket { .. } => panic!("unexpected RequestTicket in lease"),
        ArchivedTask::CheckRateLimit { .. } => panic!("unexpected CheckRateLimit in lease"),
        ArchivedTask::RefreshFloatingLimit { .. } => {
            panic!("unexpected RefreshFloatingLimit in lease")
        }
    };
    let expired_ms = now_ms() - 1;
    let new_record = LeaseRecord {
        worker_id: archived.worker_id.as_str().to_string(),
        task,
        expiry_ms: expired_ms,
    };
    let new_val = encode_lease(&new_record).unwrap();
    shard
        .db()
        .put(lease_key.as_bytes(), &new_val)
        .await
        .expect("put mutated lease");
    shard.db().flush().await.expect("flush mutated lease");

    let reaped = shard.reap_expired_leases("-").await.expect("reap");
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
    let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("attempt2 task exists");
    let task2 = decode_task(&v2).expect("decode task");
    let attempt2 = match task2 {
        Task::RunAttempt { attempt_number, .. } => attempt_number,
        Task::RequestTicket { .. } => {
            panic!("unexpected RequestTicket in tasks/ for this test")
        }
        Task::CheckRateLimit { .. } => panic!("unexpected CheckRateLimit in tasks/ for this test"),
        Task::RefreshFloatingLimit { .. } => {
            panic!("unexpected RefreshFloatingLimit in tasks/ for this test")
        }
    };
    assert_eq!(attempt2, 2);
}

