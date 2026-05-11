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

        // [SILO-RESTART-5] Verify a new task exists with monotonically increasing attempt number
        // After 1 failed attempt, restart creates attempt 2 (not reset to 1)
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
                    *attempt_number, 2,
                    "restart should continue attempt numbers monotonically (was 1, now 2)"
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

/// When `terminal_job_expire_ms` is set, the records written by the terminal
/// outcome path carry a SlateDB row TTL with `expire_ts ≈ now + ttl_ms`.
/// SlateDB applies the TTL at compaction time, so this test verifies the
/// per-row metadata directly rather than waiting for compaction to drop the
/// row.
///
/// This is the safety hook for the spec's `expireTerminalJob` transition:
/// once compaction passes the TTL, the rows disappear from the shard's view
/// and `restartFailedJob`'s `j in jobExistsAt[t]` precondition stops holding.
#[silo::test]
async fn terminal_records_are_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_info_key, job_status_key};
        let ttl_ms: u64 = 7 * 24 * 60 * 60 * 1000; // 7 days
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(ttl_ms).await;

        let before_ms = now_ms();
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

        // Drive the job to Failed (no retry policy → first error is permanent).
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
                    error_code: "TEST".to_string(),
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report failure");
        let after_ms = now_ms();

        // JOB_STATUS and JOB_INFO must both carry an `expire_ts` set to roughly
        // `now + ttl_ms`. Allow a generous window around the wall-clock bounds
        // (clocks can skew under load and the implementation samples `now_ms`
        // once per termination call).
        let raw_db = shard.db();
        for key in [job_status_key("-", &job_id), job_info_key("-", &job_id)] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            let expire_ts = kv
                .expire_ts
                .expect("terminal record should carry expire_ts");
            let lower = before_ms + ttl_ms as i64 - 5_000;
            let upper = after_ms + ttl_ms as i64 + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} outside expected window [{lower}, {upper}] for key {key:?}"
            );
        }
    });
}

/// Regression: when terminal_job_expire_ms is set, the ATTEMPT row for the
/// job's current attempt must reflect the **terminal** outcome status (e.g.
/// Succeeded), not the prior Running status that was on disk before
/// `report_attempt_outcome` ran. An earlier implementation re-scanned
/// ATTEMPT rows after writing the new terminal attempt and clobbered the
/// new value with the old Running value because WriteBatch is last-write-wins
/// per key. This test reads the ATTEMPT row directly and asserts both the
/// terminal status and the TTL are present.
#[silo::test]
async fn terminal_attempt_row_keeps_terminal_status_under_ttl() {
    with_timeout!(20000, {
        use silo::job_attempt::{AttemptStatus, JobAttemptView};
        use silo::keys::attempt_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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

        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
            .await
            .expect("report success");

        // The committed attempt row for attempt 1 must be Succeeded with a TTL.
        let kv = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row present");
        assert!(
            kv.expire_ts.is_some(),
            "terminal attempt row should carry expire_ts"
        );
        let view = JobAttemptView::new(kv.value).expect("decode attempt");
        match view.state() {
            AttemptStatus::Succeeded { .. } => {}
            other => panic!("expected AttemptStatus::Succeeded, got {:?}", other),
        }
    });
}

/// Even with TTL configured, a Failed job is restartable **before** the TTL
/// elapses. Guards against accidentally writing the TTL with `expire_ts` in
/// the past, or applying the row TTL on the wrong write path.
#[silo::test]
async fn ttl_unexpired_failed_job_is_still_restartable() {
    with_timeout!(30000, {
        // Long enough that the records can't expire during the test.
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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
                    error_code: "TEST".to_string(),
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report failure");

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Restart well before the TTL — should succeed.
        shard
            .restart_job("-", &job_id)
            .await
            .expect("restart of not-yet-expired job should succeed");

        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Scheduled);
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

        // Verify new task has monotonically increasing attempt number
        // After 3 failed attempts, restart creates attempt 4 (not reset to 1)
        // The retry SCHEDULE resets, but attempt numbers continue monotonically
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        match &tasks[0] {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 4,
                    "restart should continue attempt numbers monotonically (was 3, now 4)"
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
