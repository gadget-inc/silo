mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::retry::RetryPolicy;
use silo::task::Task;

use test_helpers::*;

#[silo::test]
async fn restart_cancelled_scheduled_job() {
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

        // Verify counters after enqueue
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 0);

        // Cancel the scheduled job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify counters after cancel - completed_jobs should be 1 (terminal state)
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 1);

        // Verify job is cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled")
        );

        // [SILO-RESTART-*] Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Verify counters after restart - completed_jobs should be 0 (back to scheduled)
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(
            counters.completed_jobs, 0,
            "completed_jobs should be 0 after restart"
        );

        // [SILO-RESTART-6] Verify status is now Scheduled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Scheduled,
            "job should be Scheduled after restart"
        );

        // [SILO-RESTART-4] Verify cancellation flag is cleared
        assert!(
            !shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "cancellation flag should be cleared after restart"
        );

        // [SILO-RESTART-5] Verify a new task exists in queue
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        assert!(!tasks.is_empty(), "new task should exist after restart");
        let new_task = &tasks[0];
        match new_task {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 1,
                    "restart should reset attempt number to 1"
                );
            }
            _ => panic!("expected RunAttempt task, got {:?}", new_task),
        }
    });
}

/// [SILO-RESTART-1][SILO-RESTART-5][SILO-RESTART-6]
/// Restart a failed job (no retry policy) - creates new task with fresh retries

#[silo::test]
async fn restart_failed_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // No retry policy - job will fail permanently on first error
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

        // Dequeue and fail the job
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: b"test failure".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Verify job is Failed
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // [SILO-RESTART-*] Restart the failed job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // [SILO-RESTART-6] Verify status is now Scheduled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Scheduled,
            "failed job should be Scheduled after restart"
        );

        // [SILO-RESTART-5] Verify a new task exists with attempt number 1
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        assert!(!tasks.is_empty(), "new task should exist after restart");
        match &tasks[0] {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 1,
                    "restart should reset attempt number to 1 for fresh retries"
                );
            }
            _ => panic!("expected RunAttempt task"),
        }

        // Dequeue and complete successfully this time
        let tasks2 = shard
            .dequeue("worker-2", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks2.len(), 1);
        let task_id2 = tasks2[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id2,
                AttemptOutcome::Success {
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");

        // Verify job succeeded
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// [SILO-RESTART-2] Cannot restart a succeeded job

#[silo::test]
async fn restart_succeeded_job_returns_error() {
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

        // Complete the job successfully
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

        // Verify job is Succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);

        // Try to restart - should fail
        let err = shard
            .restart_job("-", &job_id)
            .await
            .expect_err("restart should fail for succeeded job");

        match err {
            JobStoreShardError::JobNotRestartable(e) => {
                assert_eq!(e.job_id, job_id);
                assert_eq!(e.status, JobStatusKind::Succeeded);
                assert!(e.reason.contains("succeeded"));
            }
            other => panic!("expected JobNotRestartable, got {:?}", other),
        }
    });
}

/// [SILO-RESTART-3] Cannot restart a running job

#[silo::test]
async fn restart_running_job_returns_error() {
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

        // Dequeue to make it Running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        // Verify job is Running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Try to restart - should fail
        let err = shard
            .restart_job("-", &job_id)
            .await
            .expect_err("restart should fail for running job");

        match err {
            JobStoreShardError::JobNotRestartable(e) => {
                assert_eq!(e.job_id, job_id);
                assert_eq!(e.status, JobStatusKind::Running);
                assert!(e.reason.contains("in progress"));
            }
            other => panic!("expected JobNotRestartable, got {:?}", other),
        }

        // Clean up - complete the job
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

/// [SILO-RESTART-3] Cannot restart a scheduled job (not cancelled or failed)

#[silo::test]
async fn restart_scheduled_job_returns_error() {
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

        // Verify job is Scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Try to restart - should fail
        let err = shard
            .restart_job("-", &job_id)
            .await
            .expect_err("restart should fail for scheduled job");

        match err {
            JobStoreShardError::JobNotRestartable(e) => {
                assert_eq!(e.job_id, job_id);
                assert_eq!(e.status, JobStatusKind::Scheduled);
                assert!(e.reason.contains("in progress"));
            }
            other => panic!("expected JobNotRestartable, got {:?}", other),
        }
    });
}

/// Cannot restart a non-existent job

#[silo::test]
async fn restart_nonexistent_job_returns_not_found() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let err = shard
            .restart_job("-", "does-not-exist")
            .await
            .expect_err("restart should fail");

        match err {
            JobStoreShardError::JobNotFound(id) => assert_eq!(id, "does-not-exist"),
            other => panic!("expected JobNotFound, got {:?}", other),
        }
    });
}

/// Restart a job with retry policy that failed after exhausting retries

#[silo::test]
async fn restart_failed_job_with_retry_policy_resets_retries() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let retry_policy = RetryPolicy {
            retry_count: 2,
            initial_interval_ms: 1,
            max_interval_ms: 10,
            randomize_interval: false,
            backoff_factor: 1.0,
        };

        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                Some(retry_policy),
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Fail the job 3 times (1 initial + 2 retries = 3 attempts)
        for attempt in 1..=3 {
            let tasks = shard
                .dequeue("worker-1", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            assert_eq!(tasks.len(), 1, "should have task for attempt {}", attempt);
            let task_id = tasks[0].attempt().task_id().to_string();
            shard
                .report_attempt_outcome(
                    &task_id,
                    AttemptOutcome::Error {
                        error_code: "TEST_ERROR".to_string(),
                        error: format!("attempt {} failed", attempt).into_bytes(),
                    },
                )
                .await
                .expect("report error");

            // Wait a bit for scheduled retry task to become available (if not last attempt)
            if attempt < 3 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }

        // Verify job is Failed after exhausting retries
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Verify we had 3 attempts
        let attempts = shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("get attempts");
        assert_eq!(attempts.len(), 3, "should have 3 failed attempts");

        // Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Verify status is Scheduled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Scheduled);

        // Verify new task has attempt number 1 (fresh start)
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        match &tasks[0] {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 1,
                    "restart should reset to attempt 1 for fresh retries"
                );
            }
            _ => panic!("expected RunAttempt task"),
        }

        // Successfully complete the restarted job
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
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report success");

        // Verify job succeeded
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// Restart a cancelled job that was running - after worker acknowledges cancellation

#[silo::test]
async fn restart_cancelled_running_job_after_acknowledgement() {
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

        // Dequeue to make job Running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Cancel while running
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Worker discovers cancellation via heartbeat
        let hb_result = shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat");
        assert!(
            hb_result.cancelled,
            "heartbeat should indicate cancellation"
        );

        // Worker acknowledges cancellation
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Verify job is Cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Restart the cancelled job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Verify status is Scheduled and cancellation cleared
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Scheduled);
        assert!(
            !shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "cancellation should be cleared"
        );

        // Complete the restarted job
        let tasks = shard
            .dequeue("worker-2", "default", 1)
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

        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// Multiple restarts of the same job

#[silo::test]
async fn multiple_restarts_of_same_job() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"iteration": 0}));
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

        for iteration in 1..=3 {
            // Dequeue and fail
            let tasks = shard
                .dequeue("worker-1", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            let task_id = tasks[0].attempt().task_id().to_string();
            shard
                .report_attempt_outcome(
                    &task_id,
                    AttemptOutcome::Error {
                        error_code: "TEMP_ERROR".to_string(),
                        error: vec![],
                    },
                )
                .await
                .expect("report error");

            // Verify failed
            let status = shard
                .get_job_status("-", &job_id)
                .await
                .expect("status")
                .expect("exists");
            assert_eq!(status.kind, JobStatusKind::Failed);

            // Restart
            shard.restart_job("-", &job_id).await.expect("restart_job");

            // Verify scheduled
            let status_after = shard
                .get_job_status("-", &job_id)
                .await
                .expect("status")
                .expect("exists");
            assert_eq!(
                status_after.kind,
                JobStatusKind::Scheduled,
                "iteration {} should be Scheduled after restart",
                iteration
            );
        }

        // Finally succeed
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
                    result: b"done".to_vec(),
                },
            )
            .await
            .expect("report success");

        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}
