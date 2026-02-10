mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;

use test_helpers::*;

/// Enqueue a scheduled job and lease it directly - verify it becomes Running with an attempt created.
#[silo::test]
async fn lease_task_scheduled_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Verify job is scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Lease the specific task
        let leased = shard
            .lease_task("-", &job_id, "test-worker")
            .await
            .expect("lease_task");

        assert_eq!(leased.job().id(), job_id);
        assert_eq!(leased.attempt().attempt_number(), 1);
        assert_eq!(leased.tenant_id(), "-");

        // Verify job is now Running
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Running);
        assert_eq!(
            status_after.current_attempt, None,
            "Running job should have no current_attempt"
        );

        // Verify attempt was created
        let attempt = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt")
            .expect("attempt exists");
        assert_eq!(attempt.attempt_number(), 1);
    });
}

/// Enqueue a future-scheduled job and lease it directly - should work (expedites atomically).
#[silo::test]
async fn lease_task_future_scheduled_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let future_time_ms = now_ms() + 3_600_000; // 1 hour in the future
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                future_time_ms,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Verify the task is NOT dequeue-able (future scheduled)
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert!(tasks.is_empty(), "future task should not be dequeued yet");

        // Lease it directly - should work even for future-scheduled jobs
        let leased = shard
            .lease_task("-", &job_id, "test-worker")
            .await
            .expect("lease_task");

        assert_eq!(leased.job().id(), job_id);

        // Verify job is now Running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);
    });
}

/// Lease a running job returns FAILED_PRECONDITION.
#[silo::test]
async fn lease_task_running_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue to make it running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        // Verify running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Try to lease - should fail
        let err = shard
            .lease_task("-", &job_id, "test-worker")
            .await
            .expect_err("lease_task should fail for running job");

        match err {
            JobStoreShardError::JobNotLeaseable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("running"),
                    "should mention running: {}",
                    e.reason
                );
            }
            other => panic!("expected JobNotLeaseable error, got {:?}", other),
        }

        // Clean up
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");
    });
}

/// Lease a completed (terminal) job returns FAILED_PRECONDITION.
#[silo::test]
async fn lease_task_terminal_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue and complete
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");

        // Verify succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);

        // Try to lease - should fail
        let err = shard
            .lease_task("-", &job_id, "test-worker")
            .await
            .expect_err("lease_task should fail for terminal job");

        match err {
            JobStoreShardError::JobNotLeaseable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("terminal"),
                    "should mention terminal: {}",
                    e.reason
                );
            }
            other => panic!("expected JobNotLeaseable error, got {:?}", other),
        }
    });
}

/// Lease a cancelled job returns error.
#[silo::test]
async fn lease_task_cancelled_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let future_time_ms = now_ms() + 3_600_000;
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                future_time_ms,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Try to lease - should fail
        let err = shard
            .lease_task("-", &job_id, "test-worker")
            .await
            .expect_err("lease_task should fail for cancelled job");

        match err {
            JobStoreShardError::JobNotLeaseable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("cancelled"),
                    "should mention cancelled: {}",
                    e.reason
                );
            }
            other => panic!("expected JobNotLeaseable error, got {:?}", other),
        }
    });
}

/// Lease a nonexistent job returns NOT_FOUND.
#[silo::test]
async fn lease_task_nonexistent_job_returns_error() {
    with_timeout!(10000, {
        let (_tmp, shard) = open_temp_shard().await;

        let err = shard
            .lease_task("-", "does-not-exist", "test-worker")
            .await
            .expect_err("lease_task should fail for nonexistent job");

        match err {
            JobStoreShardError::JobNotFound(id) => {
                assert_eq!(id, "does-not-exist");
            }
            other => panic!("expected JobNotFound error, got {:?}", other),
        }
    });
}

/// Lease a task then report a successful outcome - verify the job reaches Succeeded.
#[silo::test]
async fn lease_task_then_report_outcome() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"action": "test"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Lease the task directly
        let leased = shard
            .lease_task("-", &job_id, "test-worker")
            .await
            .expect("lease_task");

        let task_id = leased.attempt().task_id().to_string();

        // Report success
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"{\"done\": true}".to_vec(),
                },
            )
            .await
            .expect("report success");

        // Verify job is succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);

        // Verify the attempt is succeeded
        let attempt = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt")
            .expect("attempt exists");
        assert!(
            matches!(
                attempt.state(),
                silo::job_attempt::AttemptStatus::Succeeded { .. }
            ),
            "attempt should be succeeded"
        );
    });
}
