mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::retry::RetryPolicy;

use test_helpers::*;

#[silo::test]
async fn expedite_future_scheduled_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // Schedule job for 1 hour in the future
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

        // Verify job is scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Verify task is future-scheduled (not dequeue-able yet)
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert!(tasks.is_empty(), "future task should not be dequeued yet");

        // Expedite the job
        shard
            .expedite_job("-", &job_id)
            .await
            .expect("expedite_job");

        // Verify status is still Scheduled (expedite doesn't change status)
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Scheduled,
            "status should remain Scheduled after expedite"
        );

        // Now the task should be dequeue-able
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1, "expedited task should be dequeue-able");
        assert_eq!(tasks[0].job().id(), job_id);

        // Complete the job
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

/// [SILO-EXP-1][SILO-EXP-4] Expedite a mid-retry job to skip backoff delay

#[silo::test]
async fn expedite_mid_retry_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // Job with retry policy - will get backoff delay on failure
        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 60_000, // 1 minute backoff
            max_interval_ms: 300_000,
            randomize_interval: false,
            backoff_factor: 2.0,
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

        // Dequeue and fail (triggers retry with backoff)
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
                    error_code: "TEMP_ERROR".to_string(),
                    error: b"temporary failure".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Job should be scheduled for retry (future timestamp due to backoff)
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Task should NOT be dequeue-able yet (waiting for backoff)
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert!(tasks.is_empty(), "retry task should be waiting for backoff");

        // Expedite the job to skip backoff
        shard
            .expedite_job("-", &job_id)
            .await
            .expect("expedite_job");

        // Now the retry task should be dequeue-able
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(
            tasks.len(),
            1,
            "expedited retry task should be dequeue-able"
        );
        assert_eq!(
            tasks[0].attempt().attempt_number(),
            2,
            "should be attempt 2"
        );

        // Complete successfully
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

/// [SILO-EXP-1] Expedite non-existent job returns error

#[silo::test]
async fn expedite_nonexistent_job_returns_error() {
    with_timeout!(10000, {
        let (_tmp, shard) = open_temp_shard().await;

        let err = shard
            .expedite_job("-", "does-not-exist")
            .await
            .expect_err("expedite should fail for non-existent job");

        match err {
            JobStoreShardError::JobNotFound(id) => {
                assert_eq!(id, "does-not-exist");
            }
            other => panic!("expected JobNotFound error, got {:?}", other),
        }
    });
}

/// [SILO-EXP-2] Expedite a succeeded job returns error

#[silo::test]
async fn expedite_succeeded_job_returns_error() {
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

        // Try to expedite - should fail
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("expedite should fail for succeeded job");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("terminal"),
                    "should mention terminal state"
                );
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }
    });
}

/// [SILO-EXP-2] Expedite a failed job returns error

#[silo::test]
async fn expedite_failed_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // No retry policy - job will fail permanently
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

        // Fail the job
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
                    error_code: "PERM_ERROR".to_string(),
                    error: b"permanent failure".to_vec(),
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

        // Try to expedite - should fail
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("expedite should fail for failed job");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("terminal"),
                    "should mention terminal state"
                );
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }
    });
}

/// [SILO-EXP-3] Expedite a cancelled job returns error

#[silo::test]
async fn expedite_cancelled_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // Schedule job for the future
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

        // Verify job is cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Try to expedite - should fail
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("expedite should fail for cancelled job");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(e.reason.contains("cancelled"), "should mention cancelled");
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }
    });
}

/// [SILO-EXP-5] Expedite a job that's already ready to run returns error

#[silo::test]
async fn expedite_already_ready_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // Schedule job for NOW (immediately ready)
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

        // Job task should already be ready to run (timestamp <= now)
        // Try to expedite - should fail because task is already ready
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("expedite should fail for already-ready job");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("already ready") || e.reason.contains("not future"),
                    "should mention task is already ready: {}",
                    e.reason
                );
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }
    });
}

/// [SILO-EXP-6] Expedite a running job returns error

#[silo::test]
async fn expedite_running_job_returns_error() {
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

        // Dequeue to start running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        // Verify job is running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Try to expedite - should fail
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("expedite should fail for running job");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert_eq!(e.job_id, job_id);
                assert!(
                    e.reason.contains("running"),
                    "should mention running: {}",
                    e.reason
                );
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }

        // Complete the job to clean up
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

/// [SILO-EXP-4] Expedite a job with no pending task returns error

#[silo::test]
async fn expedite_job_no_pending_task_returns_error() {
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

        // Dequeue and complete the job
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

        // Job is now succeeded - no pending task
        // This should fail with terminal state error, not no pending task
        // (because we check terminal first)
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("expedite should fail");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert_eq!(e.job_id, job_id);
                // Could be either terminal or no task - both are valid
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }
    });
}

/// Second expedite should fail because task is already ready

#[silo::test]
async fn double_expedite_fails_second_time() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // Schedule job for 1 hour in the future
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

        // First expedite succeeds
        shard
            .expedite_job("-", &job_id)
            .await
            .expect("first expedite_job");

        // Second expedite should fail because task is already ready
        let err = shard
            .expedite_job("-", &job_id)
            .await
            .expect_err("second expedite should fail");

        match err {
            JobStoreShardError::JobNotExpediteable(e) => {
                assert!(
                    e.reason.contains("already ready") || e.reason.contains("not future"),
                    "should mention task is already ready: {}",
                    e.reason
                );
            }
            other => panic!("expected JobNotExpediteable error, got {:?}", other),
        }
    });
}

/// Verify current_attempt is set correctly after enqueue

#[silo::test]
async fn enqueue_sets_current_attempt_in_status() {
    with_timeout!(10000, {
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

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");

        assert_eq!(status.kind, JobStatusKind::Scheduled);
        assert_eq!(
            status.current_attempt,
            Some(1),
            "enqueue should set current_attempt to 1"
        );
        assert_eq!(
            status.next_attempt_starts_after_ms,
            Some(future_time_ms),
            "enqueue should set next_attempt_starts_after_ms"
        );
    });
}

/// Verify current_attempt is cleared when job transitions to Running

#[silo::test]
async fn dequeue_clears_current_attempt_in_status() {
    with_timeout!(10000, {
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

        // Verify current_attempt is set after enqueue
        let status_before = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_before.current_attempt, Some(1));

        // Dequeue the task
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        // Verify current_attempt is cleared after dequeue (job is now Running)
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");

        assert_eq!(status_after.kind, JobStatusKind::Running);
        assert_eq!(
            status_after.current_attempt, None,
            "dequeue should clear current_attempt when job transitions to Running"
        );
        assert_eq!(
            status_after.next_attempt_starts_after_ms, None,
            "dequeue should clear next_attempt_starts_after_ms when job transitions to Running"
        );

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

/// Verify current_attempt is updated on retry scheduling

#[silo::test]
async fn retry_updates_current_attempt_in_status() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 60_000,
            max_interval_ms: 300_000,
            randomize_interval: false,
            backoff_factor: 2.0,
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

        // Verify initial attempt number
        let status1 = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status1.current_attempt, Some(1));

        // Dequeue and fail to trigger retry
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
                    error: b"temporary failure".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Verify attempt number is updated to 2
        let status2 = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");

        assert_eq!(status2.kind, JobStatusKind::Scheduled);
        assert_eq!(
            status2.current_attempt,
            Some(2),
            "retry should update current_attempt to 2"
        );
        assert!(
            status2.next_attempt_starts_after_ms.is_some(),
            "retry should set next_attempt_starts_after_ms"
        );
    });
}

/// Verify expedite updates next_attempt_starts_after_ms to now

#[silo::test]
async fn expedite_updates_status_start_time() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // Schedule job for 1 hour in the future
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

        // Verify initial state
        let status_before = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_before.next_attempt_starts_after_ms,
            Some(future_time_ms)
        );
        assert_eq!(status_before.current_attempt, Some(1));

        let time_before_expedite = now_ms();

        // Expedite the job
        shard
            .expedite_job("-", &job_id)
            .await
            .expect("expedite_job");

        let time_after_expedite = now_ms();

        // Verify status is updated
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");

        assert_eq!(status_after.kind, JobStatusKind::Scheduled);
        assert_eq!(
            status_after.current_attempt,
            Some(1),
            "expedite should preserve current_attempt"
        );

        // next_attempt_starts_after_ms should now be close to current time
        let new_start_time = status_after
            .next_attempt_starts_after_ms
            .expect("should have start time");
        assert!(
            new_start_time >= time_before_expedite && new_start_time <= time_after_expedite,
            "expedite should update next_attempt_starts_after_ms to current time, got {} (expected between {} and {})",
            new_start_time,
            time_before_expedite,
            time_after_expedite
        );
    });
}

/// Verify O(1) expedite works for mid-retry jobs

#[silo::test]
async fn expedite_retry_job_uses_o1_lookup() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 3_600_000, // 1 hour backoff
            max_interval_ms: 7_200_000,
            randomize_interval: false,
            backoff_factor: 2.0,
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

        // Dequeue and fail to trigger retry
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
                    error: b"temporary failure".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Verify retry job has attempt 2
        let status_before = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_before.current_attempt, Some(2));
        assert!(
            status_before
                .next_attempt_starts_after_ms
                .expect("should have start time")
                > now_ms(),
            "retry should be scheduled for future"
        );

        // Expedite should work using O(1) lookup (verifies the key reconstruction works)
        shard
            .expedite_job("-", &job_id)
            .await
            .expect("expedite_job");

        // Verify the job can now be dequeued
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].attempt().attempt_number(),
            2,
            "should be attempt 2"
        );

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
