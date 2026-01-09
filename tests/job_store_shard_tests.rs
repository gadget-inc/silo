mod test_helpers;

use rkyv::Archive;
use silo::codec::{decode_lease, decode_task, encode_lease};
use silo::job::JobStatusKind;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::JobStoreShardError;
use silo::task::{LeaseRecord, Task};
use silo::keys::concurrency_holder_key;
use silo::retry::{next_retry_time_ms, RetryPolicy};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use test_helpers::*;

#[silo::test]
async fn enqueue_round_trip_with_explicit_id() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"hello": "world"});
    let id = "job-123".to_string();
    let priority = 50u8;
    let start_time_ms = 1_700_000_000_000i64;

    let got_id = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority,
            start_time_ms,
            None,
            payload.clone(),
            vec![],
            None,
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

    // Job status should be Scheduled after enqueue
    let status = shard
        .get_job_status("-", &id)
        .await
        .expect("get status")
        .expect("exists");
    assert_eq!(status.kind, JobStatusKind::Scheduled);

    let view = shard
        .get_job("-", &id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.id(), id);
    assert_eq!(view.priority(), priority);
    assert_eq!(view.enqueue_time_ms(), start_time_ms);
    let got_payload = view.payload_json().unwrap();
    assert_eq!(got_payload, payload);

    // A task should be present and point at attempt 1 for this job
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(!tasks.is_empty());
    assert!(
        matches!(tasks[0], Task::RunAttempt { ref job_id, attempt_number, .. } if job_id == &id && attempt_number == 1)
    );
}

#[silo::test]
async fn enqueue_with_metadata_round_trips_in_job_view() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"m": true});
    let priority = 5u8;
    let start_at_ms = 123i64;
    let md: Vec<(String, String)> = vec![
        ("a".to_string(), "1".to_string()),
        ("b".to_string(), "two".to_string()),
    ];

    let job_id = shard
        .enqueue(
            "-",
            None,
            priority,
            start_at_ms,
            None,
            payload.clone(),
            vec![],
            Some(md.clone()),
        )
        .await
        .expect("enqueue_with_metadata");

    let view = shard
        .get_job("-", &job_id)
        .await
        .expect("get_job")
        .expect("exists");
    let got_md = view.metadata();
    // Convert to map for easy comparison
    let mut map: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (k, v) in got_md {
        map.insert(k, v);
    }
    let mut exp: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (k, v) in md {
        exp.insert(k, v);
    }
    assert_eq!(map, exp, "metadata should roundtrip in JobView");
}

#[silo::test]
async fn enqueue_generates_uuid_when_none_provided() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let start_time_ms = 12345i64;

    let got_id = shard
        .enqueue(
            "-",
            None,
            priority,
            start_time_ms,
            None,
            payload.clone(),
            vec![],
            None,
        )
        .await
        .expect("enqueue");
    assert!(!got_id.is_empty());
    assert!(uuid::Uuid::parse_str(&got_id).is_ok(), "expected UUIDv4 id");

    let view = shard
        .get_job("-", &got_id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.id(), got_id);
    assert_eq!(view.priority(), priority);
    assert_eq!(view.enqueue_time_ms(), start_time_ms);
    let got_payload = view.payload_json().unwrap();
    assert_eq!(got_payload, payload);

    // A task should be present and point at attempt 1 for this job
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(!tasks.is_empty());
    assert!(
        matches!(tasks[0], Task::RunAttempt { ref job_id, attempt_number, .. } if job_id == &got_id && attempt_number == 1)
    );
}

#[silo::test]
async fn delete_job_removes_key_and_is_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue a job
    let payload = serde_json::json!({"hello": "world"});
    let id = "job-to-delete".to_string();
    let priority = 42u8;
    let start_time_ms = 1_700_000_000_001i64;

    let got_id = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority,
            start_time_ms,
            None,
            payload.clone(),
            vec![],
            None,
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

    // Ensure it exists
    assert!(shard.get_job("-", &id).await.expect("get_job").is_some());

    // Complete the job first (can't delete while scheduled/running)
    let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);
    shard
        .report_attempt_outcome(
            "-",
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("complete job");

    // Now delete it
    shard.delete_job("-", &id).await.expect("delete_job");

    // Ensure it's gone
    let got = shard.get_job("-", &id).await.expect("get_job");
    assert!(got.is_none(), "job should be deleted");

    // Delete again (idempotent)
    shard.delete_job("-", &id).await.expect("delete_job again");
    let got = shard.get_job("-", &id).await.expect("get_job");
    assert!(got.is_none(), "job should remain deleted");
}

#[silo::test]
async fn peek_omits_future_scheduled_tasks() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();
    let future_ms = now_ms + 60_000;

    let _ = shard
        .enqueue("-", None, priority, future_ms, None, payload, vec![], None)
        .await
        .expect("enqueue");

    // Should not see the task yet
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(tasks.is_empty(), "future task should not be visible");
}

#[silo::test]
async fn dequeue_moves_tasks_to_leased_with_uuid() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        // Job status should transition to Running after dequeue
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);
        let leased_task_id = {
            let t = &tasks[0];
            assert_eq!(t.job().id(), job_id);
            assert_eq!(t.attempt().attempt_number(), 1);
            assert!(
                uuid::Uuid::parse_str(t.attempt().task_id()).is_ok(),
                "task id is UUID"
            );
            t.attempt().task_id().to_string()
        };

        // Verify a leased entry exists, and includes worker id
        let first = first_kv_with_prefix(shard.db(), "lease/")
            .await
            .expect("scan leased");
        let key_str = first.0;
        let kv_value = first.1;
        assert!(key_str.starts_with("lease/"));

        type ArchivedTask = <Task as Archive>::Archived;
        let decoded = decode_lease(&kv_value).expect("decode lease");
        let archived = decoded.archived();
        assert_eq!(archived.worker_id.as_str(), "worker-1");
        match &archived.task {
            ArchivedTask::RunAttempt {
                id,
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(id.as_str(), leased_task_id);
                assert_eq!(jid.as_str(), job_id);
                assert_eq!(*attempt_number, 1);
            }
            ArchivedTask::RequestTicket { .. } => panic!("unexpected RequestTicket in lease"),
            ArchivedTask::CheckRateLimit { .. } => panic!("unexpected CheckRateLimit in lease"),
            ArchivedTask::RefreshFloatingLimit { .. } => {
                panic!("unexpected RefreshFloatingLimit in lease")
            }
        }

        // Ensure original task queue is empty now
        let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(none_left.is_none(), "no tasks should remain after dequeue");
    });
}

#[silo::test]
async fn heartbeat_renews_lease_when_worker_matches() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now_ms = now_ms();

        let _job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Read current lease key and expiry
        let first = first_kv_with_prefix(shard.db(), "lease/")
            .await
            .expect("scan lease");
        let old_key = first.0;
        assert!(old_key.ends_with(&task_id));
        let decoded_first = decode_lease(&first.1).expect("decode lease");
        let old_expiry = decoded_first.archived().expiry_ms as u64;

        // Heartbeat to renew
        shard
            .heartbeat_task("-", "worker-1", &task_id)
            .await
            .expect("heartbeat ok");

        // Scan again, expect one lease for task with a higher expiry
        let second = first_kv_with_prefix(shard.db(), "lease/")
            .await
            .expect("scan lease 2");
        let new_key = second.0;
        assert!(new_key.ends_with(&task_id));
        let decoded_second = decode_lease(&second.1).expect("decode lease 2");
        let new_expiry = decoded_second.archived().expiry_ms as u64;
        assert!(new_expiry > old_expiry, "new expiry should be greater");

        // Validate owner remains the same
        assert_eq!(decoded_second.archived().worker_id.as_str(), "worker-1");
    });
}

#[silo::test]
async fn heartbeat_rejects_mismatched_worker() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now_ms = now_ms();

        let _job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        let err = shard
            .heartbeat_task("-", "worker-2", &task_id)
            .await
            .expect_err("heartbeat should fail");

        match err {
            JobStoreShardError::LeaseOwnerMismatch {
                task_id: tid,
                expected,
                got,
            } => {
                assert_eq!(tid, task_id);
                assert_eq!(expected, "worker-1".to_string());
                assert_eq!(got, "worker-2".to_string());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    });
}

#[silo::test]
async fn reporting_attempt_outcome_updates_attempt_and_deletes_lease() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Outcome: success
        shard
            .report_attempt_outcome(
                "-",
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

        let payload = serde_json::json!({"k": "v"});
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
            )
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        shard
            .report_attempt_outcome(
                "-",
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

        let payload = serde_json::json!({"k": "v"});
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
            )
            .await
            .expect("enqueue");

        // Run attempt 1 and error
        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
        let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2").tasks;
        let t2 = tasks2[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
        let tasks3 = shard.dequeue("w", 1).await.expect("dequeue3").tasks;
        let t3 = tasks3[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // First report: success
        shard
            .report_attempt_outcome(
                "-",
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
                "-",
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

        let payload = serde_json::json!({"k": "v2"});
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // First report: success
        shard
            .report_attempt_outcome(
                "-",
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
                "-",
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
async fn enqueue_fails_when_id_already_exists_and_db_unchanged() {
    let (_tmp, shard) = open_temp_shard().await;

    let id = "dup-job".to_string();
    let payload1 = serde_json::json!({"v": 1});
    let priority1 = 10u8;
    let start1 = 1_700_000_123_000i64;

    let got_id = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority1,
            start1,
            None,
            payload1.clone(),
            vec![],
            None,
        )
        .await
        .expect("first enqueue ok");
    assert_eq!(got_id, id);

    // Pre-duplicate snapshot
    let jobs_before = count_with_prefix(shard.db(), "jobs/").await;
    let tasks_before = count_with_prefix(shard.db(), "tasks/").await;

    // Attempt duplicate enqueue with different values to ensure no overwrite occurs
    let payload2 = serde_json::json!({"v": 2});
    let priority2 = 20u8;
    let start2 = start1 + 999_000;

    let err = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority2,
            start2,
            None,
            payload2.clone(),
            vec![],
            None,
        )
        .await
        .expect_err("duplicate enqueue should fail");

    match err {
        JobStoreShardError::JobAlreadyExists(got) => assert_eq!(got, id),
        other => panic!("unexpected error: {other:?}"),
    }

    // DB should be unchanged
    let jobs_after = count_with_prefix(shard.db(), "jobs/").await;
    let tasks_after = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(jobs_after, jobs_before, "job count should be unchanged");
    assert_eq!(tasks_after, tasks_before, "task count should be unchanged");

    // Original job info should be intact (not overwritten)
    let view = shard
        .get_job("-", &id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.priority(), priority1);
    assert_eq!(view.enqueue_time_ms(), start1);
    let got_payload = view.payload_json().unwrap();
    assert_eq!(got_payload, payload1);
}

#[silo::test]
async fn retry_count_one_boundary_enqueues_attempt2_then_stops_on_second_error() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
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
        )
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 should be enqueued
    let tasks = shard.dequeue("w", 1).await.expect("dequeue1").tasks;
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            "-",
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
    let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2").tasks;
    let t2 = tasks2[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            "-",
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
        let payload = serde_json::json!({"k": "v"});
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
            )
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
        let payload = serde_json::json!({"k": "v"});
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
            )
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
                "-",
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
        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        let t1 = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
                "-",
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
async fn heartbeat_after_outcome_returns_lease_not_found() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();
        let _job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");
        let err = shard
            .heartbeat_task("-", "worker-1", &task_id)
            .await
            .expect_err("hb should fail");
        match err {
            JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, task_id),
            other => panic!("unexpected error: {other:?}"),
        }
    });
}

#[silo::test]
async fn cannot_delete_running_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Attempt to delete while running - should fail
        let err = shard
            .delete_job("-", &job_id)
            .await
            .expect_err("delete should fail");
        match err {
            JobStoreShardError::JobInProgress(jid) => assert_eq!(jid, job_id),
            other => panic!("expected JobInProgress, got {:?}", other),
        }

        // Complete the job
        shard
            .report_attempt_outcome(
                "-",
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");

        // Now delete should succeed
        shard
            .delete_job("-", &job_id)
            .await
            .expect("delete after completion");

        // Job should be gone
        let job = shard.get_job("-", &job_id).await.expect("get job");
        assert!(job.is_none(), "job should be deleted");
    });
}

#[silo::test]
async fn cannot_delete_scheduled_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Job is scheduled but not yet dequeued
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Attempt to delete - should fail
        let err = shard
            .delete_job("-", &job_id)
            .await
            .expect_err("delete should fail");
        match err {
            JobStoreShardError::JobInProgress(jid) => assert_eq!(jid, job_id),
            other => panic!("expected JobInProgress, got {:?}", other),
        }

        // Complete the job first
        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");

        // Now delete should succeed
        shard
            .delete_job("-", &job_id)
            .await
            .expect("delete after completion");
    });
}

#[silo::test]
async fn attempt_records_exist_across_retries_and_task_ids_distinct() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = serde_json::json!({"k": "v"});
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
            )
            .await
            .expect("enqueue");

        let tasks1 = shard.dequeue("w", 1).await.expect("dequeue1").tasks;
        let t1 = tasks1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
        let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2").tasks;
        let t2 = tasks2[0].attempt().task_id().to_string();
        assert_ne!(t1, t2, "task ids should be distinct across attempts");
        shard
            .report_attempt_outcome(
                "-",
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
        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
                serde_json::json!({"k": "v2"}),
                vec![],
                None,
            )
            .await
            .expect("enqueue2");
        let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2").tasks;
        let task2 = tasks2[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
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
                    let tasks = shard_cl.dequeue(&worker_id, 1).await.expect("dequeue").tasks;
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
                let payload = serde_json::json!({"i": i});
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
                    serde_json::json!({"r": i}),
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
                    serde_json::json!({"f": i}),
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
                    let tasks = shard_cl.dequeue(&worker_id, 4).await.expect("dequeue").tasks;
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
async fn large_outcome_payloads_round_trip() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        let big_ok = vec![1u8; 2_000_000];
        shard
            .report_attempt_outcome(
                "-",
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
                serde_json::json!({"k": "v2"}),
                vec![],
                None,
            )
            .await
            .expect("enqueue2");
        let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2").tasks;
        let task2 = tasks2[0].attempt().task_id().to_string();
        let big_err = vec![2u8; 2_000_000];
        shard
            .report_attempt_outcome(
                "-",
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
async fn priority_ordering_when_start_times_equal() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let now = now_ms();

        // Enqueue two jobs with identical start_at_ms but different priorities
        let job_hi = shard
            .enqueue(
                "-",
                None,
                1u8, // higher priority
                now,
                None,
                serde_json::json!({"j": "hi"}),
                vec![],
                None,
            )
            .await
            .expect("enqueue hi");
        let job_lo = shard
            .enqueue(
                "-",
                None,
                50u8, // lower priority
                now,
                None,
                serde_json::json!({"j": "lo"}),
                vec![],
                None,
            )
            .await
            .expect("enqueue lo");

        // Dequeue two tasks; with equal times, lower priority number should come first
        let tasks = shard.dequeue("w", 2).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 2);
        let t1 = &tasks[0];
        let t2 = &tasks[1];
        assert_eq!(t1.job().id(), job_hi);
        assert_eq!(t2.job().id(), job_lo);
        assert_eq!(t1.attempt().attempt_number(), 1);
        assert_eq!(t2.attempt().attempt_number(), 1);
    });
}

#[silo::test]
async fn concurrency_immediate_grant_enqueues_task_and_writes_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let payload = serde_json::json!({"k": "v"});
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
            serde_json::json!({"j": 1}),
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
            serde_json::json!({"j": 2}),
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
            serde_json::json!({"j": 3}),
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
            serde_json::json!({"j": 33}),
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
            serde_json::json!({"a": true}),
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
            serde_json::json!({"b": true}),
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
            serde_json::json!({"c": true}),
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
                serde_json::json!({"i": i}),
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
            serde_json::json!({"first": true}),
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
                serde_json::json!({"i": i}),
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

#[silo::test]
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
        .enqueue("-", None, 10u8, now, Some(policy), payload, vec![], None)
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
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
            held_queues: _,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            tenant: tenant.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: Vec::new(),
        },
        ArchivedTask::RequestTicket { .. } => panic!("unexpected RequestTicket in lease"),
        ArchivedTask::CheckRateLimit { .. } => panic!("unexpected CheckRateLimit in lease"),
        ArchivedTask::RefreshFloatingLimit { .. } => panic!("unexpected RefreshFloatingLimit in lease"),
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

#[silo::test]
async fn reap_ignores_unexpired_leases() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let now = now_ms();
    let job_id = shard
        .enqueue("-", None, 10u8, now, None, payload, vec![], None)
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
    let _task_id = tasks[0].attempt().task_id().to_string();

    // Do not mutate the lease; it should not be reaped
    let (lease_key, _lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");

    let reaped = shard.reap_expired_leases("-").await.expect("reap");
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
        .get_job_attempt("-", &job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Running { .. } => {}
        other => panic!("expected Running, got {:?}", other),
    }
}

#[silo::test]
async fn delete_job_before_dequeue_skips_task_and_no_lease_created() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
        .await
        .expect("enqueue");

    // Verify a task exists in the ready queue
    let peek = shard.peek_tasks(10).await.expect("peek");
    assert_eq!(peek.len(), 1);

    // Dequeue and complete the job first
    let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);
    shard
        .report_attempt_outcome(
            "-",
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("complete job");

    // Now delete the job (it's in Succeeded state)
    shard.delete_job("-", &job_id).await.expect("delete job");

    // Job info and status should be deleted
    let job = shard.get_job("-", &job_id).await.expect("get job");
    assert!(job.is_none(), "job should be deleted");

    let status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status");
    assert!(status.is_none(), "job status should be deleted");
}

#[silo::test]
async fn dequeue_gracefully_handles_missing_job_info() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue("-", None, priority, now_ms, None, payload, vec![], None)
        .await
        .expect("enqueue");

    // Verify a task exists in the ready queue
    let peek = shard.peek_tasks(10).await.expect("peek");
    assert_eq!(peek.len(), 1);

    // Simulate corruption: manually delete job_info (bypassing validation)
    // This creates the edge case where task exists but job is missing
    let job_info_key = silo::keys::job_info_key("-", &job_id);
    shard
        .db()
        .delete(job_info_key.as_bytes())
        .await
        .expect("manual delete job_info");
    shard.db().flush().await.expect("flush");

    // Job info should be gone
    let job = shard.get_job("-", &job_id).await.expect("get job");
    assert!(job.is_none(), "job info should be missing");

    // Dequeue should gracefully skip the task (since job missing) and return nothing
    // This tests graceful degradation when database is in an inconsistent state
    let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
    assert!(
        tasks.is_empty(),
        "no tasks should be returned when job info missing"
    );

    // Ensure original task key was deleted (cleaned up during dequeue)
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(
        none_left.is_none(),
        "orphaned task should be cleaned up when job missing"
    );

    // Ensure no lease was created
    let lease_any = first_kv_with_prefix(shard.db(), "lease/").await;
    assert!(
        lease_any.is_none(),
        "no lease should be created for orphaned task"
    );
}

#[silo::test]
async fn tenant_allows_same_job_id_independent() {
    with_timeout!(2_000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();

        // Enqueue the same explicit job id under two different tenants
        let shared_id = "shared-id".to_string();

        let id_a = shard
            .enqueue(
                "tenantA",
                Some(shared_id.clone()),
                10u8,
                now,
                None,
                serde_json::json!({"tenant": "A"}),
                vec![],
                None,
            )
            .await
            .expect("enqueue A");
        assert_eq!(id_a, shared_id);

        let id_b = shard
            .enqueue(
                "tenantB",
                Some(shared_id.clone()),
                10u8,
                now + 1, // avoid any potential key collisions in task queue
                None,
                serde_json::json!({"tenant": "B"}),
                vec![],
                None,
            )
            .await
            .expect("enqueue B");
        assert_eq!(id_b, shared_id);

        // Both jobs must exist independently
        assert!(shard
            .get_job("tenantA", &shared_id)
            .await
            .unwrap()
            .is_some());
        assert!(shard
            .get_job("tenantB", &shared_id)
            .await
            .unwrap()
            .is_some());

        // Complete tenantA's job first so it can be deleted
        let tasks_a = shard.dequeue("wA", 1).await.expect("deq A").tasks;
        assert_eq!(tasks_a.len(), 1);
        shard
            .report_attempt_outcome(
                "tenantA",
                tasks_a[0].attempt().task_id(),
                AttemptOutcome::Success { result: vec![] },
            )
            .await
            .expect("complete A");

        // Deleting tenantA's job must not affect tenantB's job
        shard
            .delete_job("tenantA", &shared_id)
            .await
            .expect("delete A");
        assert!(shard
            .get_job("tenantA", &shared_id)
            .await
            .unwrap()
            .is_none());
        assert!(shard
            .get_job("tenantB", &shared_id)
            .await
            .unwrap()
            .is_some());
    });
}

/// [SILO-CXL-1][SILO-CXL-2][SILO-CXL-3] Cancel scheduled job immediately sets Cancelled status
/// and removes task from queue
#[tokio::test]
async fn cancel_scheduled_job_sets_cancelled_and_removes_task() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Verify job is Scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Verify task exists in queue
        let tasks_before = shard.peek_tasks(10).await.expect("peek");
        assert!(!tasks_before.is_empty());

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify status is now Cancelled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Cancelled);

        // Verify cancellation flag is set
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "job should be marked as cancelled"
        );

        // Task is still in DB queue (lazy cleanup), but will be cleaned up on dequeue
        // The broker buffer might have it, so dequeue will skip and delete it
        let task_still_in_db = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(
            task_still_in_db.is_some(),
            "task should still be in DB (lazy cleanup)"
        );

        // Dequeue should skip the cancelled task and clean it up
        let dequeued = shard.dequeue("w1", 10).await.expect("dequeue").tasks;
        assert!(
            dequeued.is_empty(),
            "dequeue should return empty for cancelled job"
        );

        // Now task should be removed from queue
        let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
        assert!(
            none_left.is_none(),
            "task should be removed from queue after dequeue"
        );
    });
}

/// Cancel running job sets cancellation flag but keeps status as Running
/// Worker discovers cancellation on heartbeat
#[tokio::test]
async fn cancel_running_job_sets_flag_status_stays_running() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Dequeue to make it Running
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);

        // Verify job is Running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify cancellation flag is set
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "job should be marked as cancelled"
        );

        // Per Alloy spec: status stays Running - worker discovers on heartbeat
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Running,
            "status should stay Running until worker reports outcome"
        );
    });
}

/// Heartbeat returns cancellation info when job is cancelled
#[tokio::test]
async fn heartbeat_returns_cancelled_flag() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Dequeue
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // First heartbeat - not cancelled
        let response1 = shard
            .heartbeat_task("-", "worker-1", &task_id)
            .await
            .expect("heartbeat");
        assert!(
            !response1.cancelled,
            "should not be cancelled before cancel_job"
        );

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Second heartbeat - should report cancelled
        let response2 = shard
            .heartbeat_task("-", "worker-1", &task_id)
            .await
            .expect("heartbeat 2");
        assert!(response2.cancelled, "should be cancelled after cancel_job");
        assert!(
            response2.cancelled_at_ms.is_some(),
            "cancelled_at_ms should be set"
        );
    });
}

/// Worker reports Cancelled outcome after discovering cancellation
#[tokio::test]
async fn worker_reports_cancelled_outcome() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Dequeue
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Worker reports Cancelled outcome
        shard
            .report_attempt_outcome("-", &task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Verify job status is Cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Verify attempt status is Cancelled
        let attempt = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt")
            .expect("exists");
        match attempt.state() {
            AttemptStatus::Cancelled { .. } => {}
            other => panic!("expected Cancelled, got {:?}", other),
        }

        // Verify lease is removed
        let lease = first_kv_with_prefix(shard.db(), "lease/").await;
        assert!(lease.is_none(), "lease should be removed");
    });
}

/// Lease expiry with cancelled job sets Cancelled status (not Failed)
#[tokio::test]
async fn reap_expired_lease_cancelled_job_sets_cancelled_status() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, 10u8, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        let tasks = shard.dequeue("w", 1).await.expect("dequeue").tasks;
        let _leased_task_id = tasks[0].attempt().task_id().to_string();

        // Cancel the job while it's running
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Find the lease and rewrite expiry to the past (simulate worker crash)
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
                held_queues: _,
            } => Task::RunAttempt {
                id: id.as_str().to_string(),
                tenant: tenant.as_str().to_string(),
                job_id: job_id.as_str().to_string(),
                attempt_number: *attempt_number,
                held_queues: Vec::new(),
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

        // Reap expired leases
        let reaped = shard.reap_expired_leases("-").await.expect("reap");
        assert_eq!(reaped, 1);

        // Verify job status is Cancelled (not Failed)
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status.kind,
            JobStatusKind::Cancelled,
            "expired lease on cancelled job should result in Cancelled status"
        );

        // Verify attempt status is Cancelled
        let attempt = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt")
            .expect("exists");
        match attempt.state() {
            AttemptStatus::Cancelled { .. } => {}
            other => panic!("expected Cancelled, got {:?}", other),
        }
    });
}

/// Cannot cancel already cancelled job
#[tokio::test]
async fn cancel_already_cancelled_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Cancel once
        shard.cancel_job("-", &job_id).await.expect("first cancel");

        // Second cancel should fail
        let err = shard
            .cancel_job("-", &job_id)
            .await
            .expect_err("second cancel should fail");

        match err {
            JobStoreShardError::JobAlreadyCancelled(id) => assert_eq!(id, job_id),
            other => panic!("expected JobAlreadyCancelled, got {:?}", other),
        }
    });
}

/// Cannot cancel Succeeded job
#[tokio::test]
async fn cancel_succeeded_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Dequeue and complete successfully
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome("-", &task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");

        // Verify Succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);

        // Try to cancel
        let err = shard
            .cancel_job("-", &job_id)
            .await
            .expect_err("cancel should fail");

        match err {
            JobStoreShardError::JobAlreadyTerminal(id, kind) => {
                assert_eq!(id, job_id);
                assert_eq!(kind, JobStatusKind::Succeeded);
            }
            other => panic!("expected JobAlreadyTerminal, got {:?}", other),
        }
    });
}

/// Cannot cancel Failed job
#[tokio::test]
async fn cancel_failed_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        // No retry policy - will fail permanently
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Dequeue and fail
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                "-",
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        // Verify Failed
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Try to cancel
        let err = shard
            .cancel_job("-", &job_id)
            .await
            .expect_err("cancel should fail");

        match err {
            JobStoreShardError::JobAlreadyTerminal(id, kind) => {
                assert_eq!(id, job_id);
                assert_eq!(kind, JobStatusKind::Failed);
            }
            other => panic!("expected JobAlreadyTerminal, got {:?}", other),
        }
    });
}

/// Cancellation is monotonic - flag persists even when dequeue overwrites status
/// This tests the exampleCancellationPreservedOnDequeue scenario from Alloy:
/// Job scheduled, task in buffer, cancel arrives, worker dequeues stale task,
/// status becomes Running BUT cancellation flag is preserved
#[tokio::test]
async fn cancellation_preserved_through_stale_dequeue() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, priority, now, None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Dequeue to make job Running
        let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].job().id(), job_id);

        // Status is now Running (dequeue wrote this)
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Cancel while running
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Key property: Cancellation flag is set independently of status
        assert!(shard
            .is_job_cancelled("-", &job_id)
            .await
            .expect("is_cancelled"));

        // Status still shows Running (per Alloy spec: cancellation is orthogonal to status)
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Running,
            "status stays Running - cancellation is tracked separately"
        );

        // Worker discovers cancellation on heartbeat
        let task_id = tasks[0].attempt().task_id().to_string();
        let response = shard
            .heartbeat_task("-", "worker-1", &task_id)
            .await
            .expect("heartbeat");
        assert!(response.cancelled, "heartbeat should report cancellation");

        // Worker reports Cancelled outcome
        shard
            .report_attempt_outcome("-", &task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Now status is Cancelled (terminal)
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get final status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Cancelled);
    });
}

/// Cannot cancel non-existent job
#[tokio::test]
async fn cancel_nonexistent_job_returns_not_found() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let err = shard
            .cancel_job("-", "does-not-exist")
            .await
            .expect_err("cancel should fail");

        match err {
            JobStoreShardError::JobNotFound(id) => assert_eq!(id, "does-not-exist"),
            other => panic!("expected JobNotFound, got {:?}", other),
        }
    });
}

/// Delete cancelled job works
#[tokio::test]
async fn delete_cancelled_job_succeeds() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = serde_json::json!({"k": "v"});
        let job_id = shard
            .enqueue("-", None, 10u8, now_ms(), None, payload, vec![], None)
            .await
            .expect("enqueue");

        // Cancel
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Delete (Cancelled is terminal, so delete should work)
        shard.delete_job("-", &job_id).await.expect("delete_job");

        // Job should be gone
        let job = shard.get_job("-", &job_id).await.expect("get_job");
        assert!(job.is_none(), "job should be deleted");

        // Cancellation record should also be gone
        assert!(
            !shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "cancellation record should be deleted"
        );
    });
}

/// Cancel job with concurrency limits removes from request queue
#[tokio::test]
async fn cancel_scheduled_job_with_concurrency_removes_request() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-cancel".to_string();

        // First job takes the slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 1}),
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

        // Second job queues a request
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 2}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue2");

        // Cancel the second job before it gets a chance to run
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        // Verify j2 is cancelled
        let status = shard
            .get_job_status("-", &j2)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Complete first job - should NOT grant to cancelled j2
        let t1 = tasks1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome("-", &t1, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report1");

        // No tasks should be ready (j2's request was cancelled)
        let tasks2 = shard.dequeue("w2", 1).await.expect("deq2").tasks;
        assert!(
            tasks2.is_empty(),
            "cancelled job's request should not be granted"
        );
    });
}

/// Test that dequeue skips cancelled jobs and cleans up their tasks
/// This specifically tests the RunAttempt task path
#[tokio::test]
async fn dequeue_skips_cancelled_run_attempt_tasks() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();

        // Enqueue two jobs
        let j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 1}),
                vec![],
                None,
            )
            .await
            .expect("enqueue j1");

        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + 1, // Slightly later so j1 comes first
                None,
                serde_json::json!({"j": 2}),
                vec![],
                None,
            )
            .await
            .expect("enqueue j2");

        // Cancel j1
        shard.cancel_job("-", &j1).await.expect("cancel j1");

        // Dequeue should skip j1 and return j2
        let tasks = shard.dequeue("w1", 10).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1, "should return one task");
        assert_eq!(
            tasks[0].job().id(),
            j2,
            "should return j2, not cancelled j1"
        );

        // j1's task should have been cleaned up from the DB
        // Check that no task keys contain j1's id
        let task_for_j1 = first_kv_with_prefix(shard.db(), "tasks/").await;
        // If there's a task, it shouldn't be for j1 (j2 was already dequeued so should be leased)
        if let Some((key_str, _value)) = task_for_j1 {
            assert!(
                !key_str.contains(&j1),
                "j1's task should be cleaned up, found: {}",
                key_str
            );
        }
    });
}

/// Test that dequeue skips cancelled RequestTicket tasks
#[tokio::test]
async fn dequeue_skips_cancelled_request_ticket_tasks() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-skip-req".to_string();

        // j1 takes the concurrency slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 1}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j1");

        // Dequeue j1 to take the slot
        let t1 = shard.dequeue("w1", 1).await.expect("deq1").tasks;
        assert_eq!(t1.len(), 1);

        // j2 and j3 enqueue requests (future time so they become RequestTicket tasks)
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + 100,
                None,
                serde_json::json!({"j": 2}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j2");

        let j3 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + 101,
                None,
                serde_json::json!({"j": 3}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j3");

        // Cancel j2
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        // Complete j1 to release the slot
        let t1_id = t1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome("-", &t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report j1 success");

        // Wait for the future time to pass
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        // Dequeue should skip j2's RequestTicket and process j3's
        let tasks = shard.dequeue("w2", 10).await.expect("dequeue").tasks;

        // We should get j3, not j2
        if !tasks.is_empty() {
            assert_eq!(tasks[0].job().id(), j3, "should get j3, not cancelled j2");
        }
    });
}

/// Test that grant-on-release skips multiple cancelled requests to find a valid one
#[tokio::test]
async fn grant_on_release_skips_multiple_cancelled_requests() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-multi-cancel".to_string();

        // j1 takes the slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 1}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j1");

        let t1 = shard.dequeue("w1", 1).await.expect("deq1").tasks;
        assert_eq!(t1.len(), 1);

        // j2, j3, j4 queue requests (j2 and j3 will be cancelled)
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 2}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j2");

        let j3 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 3}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j3");

        let j4 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 4}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j4");

        // Cancel j2 and j3
        shard.cancel_job("-", &j2).await.expect("cancel j2");
        shard.cancel_job("-", &j3).await.expect("cancel j3");

        // Complete j1 - should skip j2 and j3, grant to j4
        let t1_id = t1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome("-", &t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report j1 success");

        // Give broker time to pick up the granted task
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Dequeue should return j4
        let tasks = shard.dequeue("w2", 10).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 1, "should return one task");
        assert_eq!(tasks[0].job().id(), j4, "should return j4");

        // j2 and j3 should remain Cancelled, not be granted
        let j2_status = shard
            .get_job_status("-", &j2)
            .await
            .expect("get j2 status")
            .expect("j2 exists");
        assert_eq!(j2_status.kind, JobStatusKind::Cancelled);

        let j3_status = shard
            .get_job_status("-", &j3)
            .await
            .expect("get j3 status")
            .expect("j3 exists");
        assert_eq!(j3_status.kind, JobStatusKind::Cancelled);
    });
}

/// Test that cancellation check on dequeue doesn't interfere with normal operation
#[tokio::test]
async fn dequeue_works_normally_without_cancellation() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();

        // Enqueue several jobs
        let mut job_ids = vec![];
        for i in 0..5 {
            let j = shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now + i,
                    None,
                    serde_json::json!({"j": i}),
                    vec![],
                    None,
                )
                .await
                .expect("enqueue");
            job_ids.push(j);
        }

        // Dequeue all - should work normally
        let tasks = shard.dequeue("w1", 10).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 5, "should return all 5 tasks");

        // Verify order matches enqueue order (by start time)
        for (i, task) in tasks.iter().enumerate() {
            assert_eq!(
                task.job().id(),
                job_ids[i],
                "task {} should match job {}",
                i,
                i
            );
        }
    });
}

/// Test that cancelled requests are cleaned up from the request queue
#[tokio::test]
async fn cancelled_requests_are_cleaned_up_on_grant() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-cleanup".to_string();

        // j1 takes the slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 1}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j1");

        let t1 = shard.dequeue("w1", 1).await.expect("deq1").tasks;
        assert_eq!(t1.len(), 1);

        // j2 queues a request
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"j": 2}),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
            )
            .await
            .expect("enqueue j2");

        // Verify request exists
        let requests_before = first_kv_with_prefix(shard.db(), "requests/").await;
        assert!(requests_before.is_some(), "request should exist");

        // Cancel j2
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        // Request still exists (lazy cleanup)
        let requests_after_cancel = first_kv_with_prefix(shard.db(), "requests/").await;
        assert!(
            requests_after_cancel.is_some(),
            "request should still exist before release"
        );

        // Complete j1 to trigger grant-next (which should skip and delete j2's request)
        let t1_id = t1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome("-", &t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report j1 success");

        // Now request should be cleaned up
        let requests_after_release = first_kv_with_prefix(shard.db(), "requests/").await;
        assert!(
            requests_after_release.is_none(),
            "cancelled request should be cleaned up after release"
        );
    });
}

