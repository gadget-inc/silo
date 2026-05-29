mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
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
#[silo::test]
async fn cancel_scheduled_job_tags_records_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_cancelled_key, job_info_key, job_status_key};

        let ttl_s: u64 = 7 * 24 * 60 * 60; // 7 days
        let ttl_ms: i64 = ttl_s as i64 * 1000;
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(ttl_s).await;

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

        shard.cancel_job("-", &job_id).await.expect("cancel_job");
        let after_ms = now_ms();

        // JOB_STATUS, JOB_INFO, and JOB_CANCELLED must all carry an `expire_ts`
        // set to roughly `now + ttl_s*1000`. Allow a generous window around the
        // wall-clock bounds (the implementation samples `now_ms` once per
        // cancellation call).
        let raw_db = shard.db();
        for key in [
            job_status_key("-", &job_id),
            job_info_key("-", &job_id),
            job_cancelled_key("-", &job_id),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            let expire_ts = kv
                .expire_ts
                .expect("cancellation-terminal record should carry expire_ts");
            let lower = before_ms + ttl_ms - 5_000;
            let upper = after_ms + ttl_ms + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} outside expected window [{lower}, {upper}] for key {key:?}"
            );
        }
    });
}

/// The new IDX_STATUS_TIME row written for the Cancelled status by
/// `set_job_status_with_index_opts` must carry the row TTL when the cancel
/// path transitions a Scheduled job to terminal.
#[silo::test]
async fn cancel_scheduled_job_tags_idx_status_time_with_expire_ts() {
    with_timeout!(20000, {
        use silo::job::JobStatusKind;
        use silo::keys::{idx_status_time_key, status_index_timestamp};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

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

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        let ts = status_index_timestamp(&status);
        let kv = shard
            .db()
            .get_key_value(&idx_status_time_key(
                "-",
                JobStatusKind::Cancelled.as_str(),
                ts,
                &job_id,
            ))
            .await
            .expect("get_key_value")
            .expect("IDX_STATUS_TIME row present");
        assert!(
            kv.expire_ts.is_some(),
            "IDX_STATUS_TIME row for terminal Cancelled status must carry expire_ts"
        );
    });
}

/// IDX_METADATA rows must also be re-put with the row TTL on the cancel
/// path. Mirrors `terminal_idx_metadata_rows_are_tagged_with_expire_ts` for
/// the cancel-driven terminal transition.
#[silo::test]
async fn cancel_scheduled_job_tags_idx_metadata_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::idx_metadata_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        let metadata = vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ];
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
                Some(metadata.clone()),
                "default",
            )
            .await
            .expect("enqueue");

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        for (k, v) in &metadata {
            let kv = shard
                .db()
                .get_key_value(&idx_metadata_key("-", k, v, &job_id))
                .await
                .expect("get_key_value")
                .unwrap_or_else(|| panic!("IDX_METADATA row missing for {k}={v}"));
            assert!(
                kv.expire_ts.is_some(),
                "IDX_METADATA row for {k}={v} must carry expire_ts after cancel"
            );
        }
    });
}

/// A prior ATTEMPT row written by a retry-scheduling outcome was left
/// without a TTL on purpose (job wasn't yet terminal). When the job is
/// later cancelled via the cancel RPC, the cancel path must re-put that
/// attempt row with the row TTL — that's what
/// `expire_terminal_job_records` does, and we want to pin it down for the
/// cancel call site.
#[silo::test]
async fn cancel_scheduled_job_tags_prior_attempt_rows_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::attempt_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        };

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
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

        // Run-and-fail attempt 1 → retry-scheduling branch (job back to
        // Scheduled, attempt 1 row has NO TTL).
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

        // Sanity: attempt 1 row currently has no TTL.
        let attempt_kv = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row present");
        assert!(
            attempt_kv.expire_ts.is_none(),
            "pre-cancel attempt row should not yet carry expire_ts"
        );

        // Cancel — the job is back to Scheduled, so cancel transitions it
        // straight to terminal Cancelled and the prior attempt row must
        // pick up the TTL.
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let attempt_kv_after = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row still present");
        assert!(
            attempt_kv_after.expire_ts.is_some(),
            "prior attempt row must carry expire_ts after terminal cancel"
        );
    });
}

/// Running-job cancellation does NOT immediately tag records with the row
/// TTL. The job is still Running until the worker acknowledges via
/// `report_attempt_outcome(Cancelled)` — only then is it terminal. Tagging
/// records on the cancel call would mean records expire before the worker
/// even hears about the cancellation. The worker-ack path
/// (`cancelled_terminal_job_cancellation_row_is_tagged_with_expire_ts`)
/// covers TTL for that case.
#[silo::test]
async fn cancel_running_job_does_not_tag_records_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_cancelled_key, job_info_key, job_status_key};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

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

        // Dequeue first so the job is Running.
        let _ = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("status")
            .expect("exists");
        assert_eq!(
            status.kind,
            JobStatusKind::Running,
            "status should still be Running until worker acknowledges"
        );

        let raw_db = shard.db();
        for key in [
            job_status_key("-", &job_id),
            job_info_key("-", &job_id),
            job_cancelled_key("-", &job_id),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_none(),
                "running-cancel must not pre-tag record with expire_ts: key={key:?}"
            );
        }
    });
}

/// Sanity: when the terminal-expire feature is *not* configured, cancellation
/// of a Scheduled job must NOT tag any record with `expire_ts`. Guards
/// against an accidental hard-coded default that would enable the TTL
/// feature for operators that haven't opted in.
#[silo::test]
async fn cancel_without_terminal_expire_config_writes_no_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_cancelled_key, job_info_key, job_status_key};

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

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let raw_db = shard.db();
        for key in [
            job_status_key("-", &job_id),
            job_info_key("-", &job_id),
            job_cancelled_key("-", &job_id),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_none(),
                "cancel without TTL config must not tag record: key={key:?}"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Import / reimport TTL coverage
//
// When an import or reimport lands the job directly in a terminal status, the
// records it writes (JOB_INFO, JOB_STATUS, IDX_STATUS_TIME, IDX_METADATA,
// ATTEMPT) must carry a SlateDB row TTL so they age out alongside the
// `report_attempt_outcome` and `cancel_job` paths — otherwise import-driven
// terminal records would stick around indefinitely.
// ---------------------------------------------------------------------------

fn import_base(id: &str) -> ImportJobParams {
    ImportJobParams {
        id: id.to_string(),
        priority: 50,
        enqueue_time_ms: 1_700_000_000_000,
        start_at_ms: 0,
        retry_policy: None,
        payload: test_helpers::msgpack_payload(&serde_json::json!({"imported": true})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts: vec![],
    }
}

fn import_succeeded_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Succeeded {
            result: vec![1, 2, 3],
        },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

fn import_failed_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Failed {
            error_code: "ERR".to_string(),
            error: vec![4, 5, 6],
        },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

/// Importing a job with a terminal final attempt must tag JOB_STATUS,
/// JOB_INFO, and every ATTEMPT row with a row TTL. Mirrors
/// `terminal_records_are_tagged_with_expire_ts` for the import path.
#[silo::test]
async fn terminal_import_tags_records_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{attempt_key, job_info_key, job_status_key};

        let ttl_s: u64 = 7 * 24 * 60 * 60;
        let ttl_ms: i64 = ttl_s as i64 * 1000;
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(ttl_s).await;

        let before_ms = now_ms();
        let mut params = import_base("import-terminal");
        params.attempts = vec![
            import_failed_attempt(before_ms - 5_000),
            import_succeeded_attempt(before_ms - 1_000),
        ];

        let results = shard
            .import_jobs("-", vec![params])
            .await
            .expect("import_jobs");
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].status, JobStatusKind::Succeeded);
        let after_ms = now_ms();

        let raw_db = shard.db();
        for key in [
            job_status_key("-", "import-terminal"),
            job_info_key("-", "import-terminal"),
            attempt_key("-", "import-terminal", 1),
            attempt_key("-", "import-terminal", 2),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            let expire_ts = kv
                .expire_ts
                .expect("import-terminal record should carry expire_ts");
            let lower = before_ms + ttl_ms - 5_000;
            let upper = after_ms + ttl_ms + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} outside [{lower}, {upper}] for key {key:?}"
            );
        }
    });
}

/// IDX_STATUS_TIME and IDX_METADATA secondary-index rows written by a
/// terminal import must also carry the row TTL.
#[silo::test]
async fn terminal_import_tags_secondary_indexes_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{idx_metadata_key, idx_status_time_key, status_index_timestamp};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        let metadata = vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ];
        let mut params = import_base("import-idx");
        params.metadata = Some(metadata.clone());
        params.attempts = vec![import_succeeded_attempt(now_ms() - 1_000)];

        shard
            .import_jobs("-", vec![params])
            .await
            .expect("import_jobs");

        let status = shard
            .get_job_status("-", "import-idx")
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);
        let ts = status_index_timestamp(&status);
        let idx_key = idx_status_time_key("-", status.kind.as_str(), ts, "import-idx");
        let kv = shard
            .db()
            .get_key_value(&idx_key)
            .await
            .expect("get_key_value")
            .expect("IDX_STATUS_TIME row present");
        assert!(
            kv.expire_ts.is_some(),
            "IDX_STATUS_TIME row for terminal import must carry expire_ts"
        );

        for (k, v) in &metadata {
            let kv = shard
                .db()
                .get_key_value(&idx_metadata_key("-", k, v, "import-idx"))
                .await
                .expect("get_key_value")
                .unwrap_or_else(|| panic!("IDX_METADATA row missing for {k}={v}"));
            assert!(
                kv.expire_ts.is_some(),
                "IDX_METADATA row for {k}={v} must carry expire_ts on terminal import"
            );
        }
    });
}

/// Importing a job that lands non-terminal (Scheduled) must NOT carry a row
/// TTL — the TTL is only applied when the resulting status is terminal.
#[silo::test]
async fn non_terminal_import_does_not_tag_records() {
    with_timeout!(20000, {
        use silo::keys::{job_info_key, job_status_key};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        let params = import_base("import-scheduled");
        // No attempts → Scheduled (per determine_import_status).
        shard
            .import_jobs("-", vec![params])
            .await
            .expect("import_jobs");

        let raw_db = shard.db();
        for key in [
            job_status_key("-", "import-scheduled"),
            job_info_key("-", "import-scheduled"),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_none(),
                "non-terminal import must not tag record: key={key:?}"
            );
        }
    });
}

/// Reimporting a Scheduled job to a terminal status must tag both the
/// pre-existing attempt rows (rewritten via `expire_terminal_job_records`)
/// and the new in-txn attempt rows, plus the updated JOB_INFO. Exercises the
/// reimport branch end-to-end.
#[silo::test]
async fn terminal_reimport_tags_existing_and_new_records_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{attempt_key, job_info_key, job_status_key};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        // First import: Scheduled (no attempts, no retry policy needed because
        // zero attempts always lands Scheduled).
        let initial = import_base("reimport-terminal");
        shard
            .import_jobs("-", vec![initial])
            .await
            .expect("initial import");

        // Manually write one prior ATTEMPT row by reimporting with a single
        // Failed attempt + retry policy that still allows more retries.
        // The fail timestamp must be stable across the two reimport calls so
        // the reimport's attempt-equality check passes on attempt #1.
        let failed_at_ms = now_ms() - 3_000;
        let mut step = import_base("reimport-terminal");
        step.retry_policy = Some(RetryPolicy {
            retry_count: 5,
            initial_interval_ms: 100,
            max_interval_ms: 10_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        });
        step.attempts = vec![import_failed_attempt(failed_at_ms)];
        let step_results = shard
            .import_jobs("-", vec![step])
            .await
            .expect("reimport step");
        assert!(step_results[0].success);
        assert_eq!(step_results[0].status, JobStatusKind::Scheduled);

        // First ATTEMPT row was just written without a TTL because the job is
        // still Scheduled — sanity-check that pre-condition.
        let raw_db = shard.db();
        let pre_attempt = raw_db
            .get_key_value(&attempt_key("-", "reimport-terminal", 1))
            .await
            .expect("get_key_value")
            .expect("attempt 1 present");
        assert!(
            pre_attempt.expire_ts.is_none(),
            "non-terminal reimport must leave attempt row untagged"
        );

        // Now reimport with attempt 2 = Succeeded. This lands the job
        // terminal, and the helper should retroactively tag the existing
        // attempt 1 alongside the new in-txn attempt 2.
        let mut terminal = import_base("reimport-terminal");
        terminal.retry_policy = Some(RetryPolicy {
            retry_count: 5,
            initial_interval_ms: 100,
            max_interval_ms: 10_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        });
        terminal.attempts = vec![
            import_failed_attempt(failed_at_ms),
            import_succeeded_attempt(now_ms() - 500),
        ];
        let terminal_results = shard
            .import_jobs("-", vec![terminal])
            .await
            .expect("terminal reimport");
        assert!(
            terminal_results[0].success,
            "terminal reimport failed: {:?}",
            terminal_results[0].error
        );
        assert_eq!(terminal_results[0].status, JobStatusKind::Succeeded);

        for key in [
            job_status_key("-", "reimport-terminal"),
            job_info_key("-", "reimport-terminal"),
            attempt_key("-", "reimport-terminal", 1),
            attempt_key("-", "reimport-terminal", 2),
        ] {
            let kv = shard
                .db()
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_some(),
                "reimport-terminal record must carry expire_ts: key={key:?}"
            );
        }
    });
}

/// A terminal import must source its row TTL from the status-specific setting:
/// Succeeded → `completed_job_expire_s`, Failed → `terminal_job_expire_s`.
/// Configures the two settings to widely disjoint values so a swap of the two
/// sources (which `open_temp_shard_with_terminal_expire_s` can't catch, since it
/// sets both equal) lands the `expire_ts` in the wrong window and fails.
#[silo::test]
async fn terminal_import_uses_status_specific_ttl_source() {
    with_timeout!(20000, {
        use silo::keys::job_status_key;

        let completed_s: u64 = 100 * 24 * 60 * 60; // 100 days
        let terminal_s: u64 = 24 * 60 * 60; // 1 day
        let completed_ms: i64 = completed_s as i64 * 1000;
        let terminal_ms: i64 = terminal_s as i64 * 1000;
        let (_tmp, shard) = open_temp_shard_with_split_expire_s(completed_s, terminal_s).await;

        let before_ms = now_ms();

        // Succeeded import → completed_job_expire_s.
        let mut ok = import_base("import-succeeded");
        ok.attempts = vec![import_succeeded_attempt(before_ms - 1_000)];

        // Failed import (no retry policy → retries exhausted → terminal Failed)
        // → terminal_job_expire_s.
        let mut failed = import_base("import-failed");
        failed.attempts = vec![import_failed_attempt(before_ms - 1_000)];

        let results = shard
            .import_jobs("-", vec![ok, failed])
            .await
            .expect("import_jobs");
        assert_eq!(results[0].status, JobStatusKind::Succeeded);
        assert_eq!(results[1].status, JobStatusKind::Failed);
        let after_ms = now_ms();

        let raw_db = shard.db();
        for (id, ttl_ms) in [
            ("import-succeeded", completed_ms),
            ("import-failed", terminal_ms),
        ] {
            let kv = raw_db
                .get_key_value(&job_status_key("-", id))
                .await
                .expect("get_key_value")
                .expect("row present");
            let expire_ts = kv.expire_ts.expect("terminal import must carry expire_ts");
            let lower = before_ms + ttl_ms - 5_000;
            let upper = after_ms + ttl_ms + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} for {id} outside expected window [{lower}, {upper}] \
                 — wrong TTL source?"
            );
        }
    });
}

/// Reimporting a Scheduled job to another non-terminal (Scheduled) status must
/// NOT tag any record with `expire_ts`. Complements
/// `non_terminal_import_does_not_tag_records` (new import) and the terminal
/// reimport coverage by exercising the reimport branch when it stays
/// non-terminal.
#[silo::test]
async fn non_terminal_reimport_does_not_tag_records() {
    with_timeout!(20000, {
        use silo::keys::{attempt_key, job_info_key, job_status_key};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        // First import: Scheduled (no attempts).
        let initial = import_base("reimport-scheduled");
        shard
            .import_jobs("-", vec![initial])
            .await
            .expect("initial import");

        // Reimport with one Failed attempt but a retry policy that still allows
        // retries → job stays Scheduled (non-terminal).
        let mut step = import_base("reimport-scheduled");
        step.retry_policy = Some(RetryPolicy {
            retry_count: 5,
            initial_interval_ms: 100,
            max_interval_ms: 10_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        });
        step.attempts = vec![import_failed_attempt(now_ms() - 1_000)];
        let results = shard
            .import_jobs("-", vec![step])
            .await
            .expect("reimport step");
        assert!(results[0].success);
        assert_eq!(results[0].status, JobStatusKind::Scheduled);

        let raw_db = shard.db();
        for key in [
            job_status_key("-", "reimport-scheduled"),
            job_info_key("-", "reimport-scheduled"),
            attempt_key("-", "reimport-scheduled", 1),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_none(),
                "non-terminal reimport must not tag record: key={key:?}"
            );
        }
    });
}

#[silo::test]
async fn terminal_records_are_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_info_key, job_status_key};
        let ttl_s: u64 = 7 * 24 * 60 * 60; // 7 days
        let ttl_ms: i64 = ttl_s as i64 * 1000;
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(ttl_s).await;

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
            let lower = before_ms + ttl_ms - 5_000;
            let upper = after_ms + ttl_ms + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} outside expected window [{lower}, {upper}] for key {key:?}"
            );
        }
    });
}

/// Regression: when terminal_job_expire_s is set, the ATTEMPT row for the
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

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

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

/// IDX_METADATA rows are the reason `expire_terminal_job_records` reads
/// JOB_INFO back: we extract the metadata pairs from JOB_INFO so we know
/// which IDX_METADATA keys to TTL. This test pins down that every
/// IDX_METADATA row for a terminal job carries the row TTL.
#[silo::test]
async fn terminal_idx_metadata_rows_are_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::idx_metadata_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        let metadata = vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
            ("kind".to_string(), "ingest".to_string()),
        ];
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
                Some(metadata.clone()),
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

        for (k, v) in &metadata {
            let kv = shard
                .db()
                .get_key_value(&idx_metadata_key("-", k, v, &job_id))
                .await
                .expect("get_key_value")
                .unwrap_or_else(|| panic!("IDX_METADATA row missing for {k}={v}"));
            assert!(
                kv.expire_ts.is_some(),
                "IDX_METADATA row for {k}={v} must carry expire_ts"
            );
        }
    });
}

/// IDX_STATUS_TIME row written for the new terminal status must carry the
/// row TTL. This is written inside `write_job_status_with_index_opts` rather
/// than the helper, so it's a separate code path from the JOB_INFO/ATTEMPT
/// re-puts and worth its own assertion.
#[silo::test]
async fn terminal_idx_status_time_row_is_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::job::JobStatusKind;
        use silo::keys::{idx_status_time_key, status_index_timestamp};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

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

        // Re-derive the IDX_STATUS_TIME key for the new (Succeeded) status from
        // the recorded JobStatus so this test stays in lockstep with however
        // `set_job_status_with_index_opts` constructs the key.
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get_job_status")
            .expect("status present");
        assert_eq!(status.kind, JobStatusKind::Succeeded);
        let ts = status_index_timestamp(&status);
        let idx_key = idx_status_time_key("-", status.kind.as_str(), ts, &job_id);
        let kv = shard
            .db()
            .get_key_value(&idx_key)
            .await
            .expect("get_key_value")
            .expect("IDX_STATUS_TIME row present");
        assert!(
            kv.expire_ts.is_some(),
            "IDX_STATUS_TIME row for terminal status must carry expire_ts"
        );
    });
}

/// JOB_CANCELLED rows are written on the cancellation path and must also
/// be TTL'd when the cancellation results in a terminal outcome. Exercises
/// the Cancelled-acknowledgement branch which neither of the other new
/// tests touches.
#[silo::test]
async fn cancelled_terminal_job_cancellation_row_is_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::job_cancelled_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

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

        // Dequeue so the job is Running; only then does worker-acknowledged
        // cancellation flow through report_attempt_outcome(Cancelled).
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Cancel from the API side first (writes JOB_CANCELLED), then have the
        // worker acknowledge with AttemptOutcome::Cancelled.
        shard.cancel_job("-", &job_id).await.expect("cancel_job");
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        let kv = shard
            .db()
            .get_key_value(&job_cancelled_key("-", &job_id))
            .await
            .expect("get_key_value")
            .expect("JOB_CANCELLED row present");
        assert!(
            kv.expire_ts.is_some(),
            "JOB_CANCELLED row for terminal Cancelled job must carry expire_ts"
        );
    });
}

/// When `report_attempt_outcome(Error)` schedules a retry (job goes back to
/// Scheduled, not terminal), the just-finalized attempt row must NOT carry
/// a TTL. Otherwise retried jobs that never reach terminal status would
/// silently lose their attempt history once the next compaction runs.
/// This is the inverse of `terminal_attempt_row_keeps_terminal_status_under_ttl`
/// and pins down the doc-comment claim in lease.rs.
#[silo::test]
async fn retry_branch_attempt_row_has_no_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::attempt_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;

        // Retry policy so the first error leads to retry-scheduling, not Failed.
        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        };

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
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
            .expect("report error");

        // Job should be back to Scheduled (retry scheduled), not terminal.
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("status")
            .expect("present");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        let kv = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row present");
        assert!(
            kv.expire_ts.is_none(),
            "attempt row written under the retry-scheduling branch must NOT carry expire_ts \
             (job is still alive and could be retried for hours); got {:?}",
            kv.expire_ts
        );
    });
}
