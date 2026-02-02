mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::retry::RetryPolicy;

use test_helpers::*;

/// Test that attempt numbers are monotonically increasing across job restarts.
/// After a restart, attempt numbers should continue from where they left off,
/// not reset to 1.
#[silo::test]
async fn attempt_numbers_are_monotonic_across_restarts() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"test": "data"}));
        let retry_policy = RetryPolicy {
            retry_count: 2, // 3 total attempts per run
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

        // Fail all 3 attempts in the first run
        let mut seen_attempt_numbers: Vec<u32> = Vec::new();

        for expected_attempt in 1..=3 {
            let tasks = shard
                .dequeue("worker-1", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            assert_eq!(
                tasks.len(),
                1,
                "should have task for attempt {}",
                expected_attempt
            );

            let attempt_number = tasks[0].attempt().attempt_number();

            assert_eq!(
                attempt_number, expected_attempt,
                "attempt number should be {} in first run",
                expected_attempt
            );
            seen_attempt_numbers.push(attempt_number);

            let task_id = tasks[0].attempt().task_id().to_string();
            shard
                .report_attempt_outcome(
                    &task_id,
                    AttemptOutcome::Error {
                        error_code: "TEST_ERROR".to_string(),
                        error: format!("attempt {} failed", expected_attempt).into_bytes(),
                    },
                )
                .await
                .expect("report error");

            // Wait for retry to be scheduled (if not last attempt)
            if expected_attempt < 3 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }

        // Job should be failed now
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Now fail 2 more attempts after restart
        // Attempt numbers should be 4, 5, 6 (not 1, 2, 3)
        for expected_attempt in 4..=6 {
            let tasks = shard
                .dequeue("worker-1", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            assert_eq!(
                tasks.len(),
                1,
                "should have task for attempt {}",
                expected_attempt
            );

            let attempt_number = tasks[0].attempt().attempt_number();

            assert_eq!(
                attempt_number, expected_attempt,
                "attempt number should be {} after restart (not reset to 1)",
                expected_attempt
            );

            // Verify this attempt number hasn't been seen before (monotonically increasing)
            assert!(
                !seen_attempt_numbers.contains(&attempt_number),
                "attempt number {} was already used before restart",
                attempt_number
            );
            seen_attempt_numbers.push(attempt_number);

            let task_id = tasks[0].attempt().task_id().to_string();

            if expected_attempt < 6 {
                shard
                    .report_attempt_outcome(
                        &task_id,
                        AttemptOutcome::Error {
                            error_code: "TEST_ERROR".to_string(),
                            error: format!("attempt {} failed", expected_attempt).into_bytes(),
                        },
                    )
                    .await
                    .expect("report error");
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            } else {
                // Succeed on the last attempt
                shard
                    .report_attempt_outcome(
                        &task_id,
                        AttemptOutcome::Success {
                            result: b"success".to_vec(),
                        },
                    )
                    .await
                    .expect("report success");
            }
        }

        // Verify job succeeded
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);

        // Verify all attempts are stored with unique, monotonically increasing numbers
        let attempts = shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("get attempts");
        assert_eq!(attempts.len(), 6, "should have 6 total attempts");

        let stored_attempt_numbers: Vec<u32> =
            attempts.iter().map(|a| a.attempt_number()).collect();
        assert_eq!(
            stored_attempt_numbers,
            vec![1, 2, 3, 4, 5, 6],
            "stored attempt numbers should be monotonically increasing"
        );
    });
}

/// Test that the retry schedule resets after a job restart.
/// The first retry after restart should use a short delay based on relative attempt 1,
/// not the accumulated delay from before the restart.
#[silo::test]
async fn retry_schedule_resets_after_restart() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"test": "data"}));
        // Use exponential backoff with factor 2 to make delay differences obvious
        let retry_policy = RetryPolicy {
            retry_count: 2, // 3 total attempts per run
            initial_interval_ms: 100,
            max_interval_ms: 10000,
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

        // Fail all 3 attempts in the first run
        for attempt in 1..=3 {
            // Wait for task to become available (accounting for retry delay)
            // Use longer waits to ensure retry tasks are scheduled
            for _ in 0..20 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let tasks = shard
                    .dequeue("worker-1", "default", 1)
                    .await
                    .expect("dequeue")
                    .tasks;
                if !tasks.is_empty() {
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
                    break;
                }
            }
        }

        // Wait for job to fail
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Get the first task after restart and fail it
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Record time before failing
        let before_fail = now_ms();

        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: b"post-restart failure".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Check the next_attempt_starts_after_ms - it should be based on relative attempt 1
        // With initial=100ms and factor=2, delay for relative attempt 1 failing = 100 * 2^1 = 200ms
        // NOT the accumulated delay from before restart
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        let next_attempt_time = status
            .next_attempt_starts_after_ms
            .expect("should have next attempt time");

        // The retry delay should be short (~200ms based on relative_attempt=1)
        // Not a large delay like 800ms (100 * 2^3) from total attempt 4
        let delay = next_attempt_time - before_fail;
        assert!(
            delay < 500,
            "retry delay after restart should be based on relative attempt (~200ms), not accumulated delay. Got {}ms",
            delay
        );
        assert!(
            delay >= 100,
            "retry delay should be at least the initial interval. Got {}ms",
            delay
        );
    });
}

/// Test that relative_attempt_number resets to 1 after restart while attempt_number
/// continues monotonically increasing.
#[silo::test]
async fn relative_attempt_number_resets_after_restart() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"test": "data"}));
        let retry_policy = RetryPolicy {
            retry_count: 1, // 2 attempts per run
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

        // First run: attempts 1 and 2, relative 1 and 2
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].attempt().attempt_number(), 1);
        assert_eq!(tasks[0].attempt().relative_attempt_number(), 1);

        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: b"failed".to_vec(),
                },
            )
            .await
            .expect("report error");

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].attempt().attempt_number(), 2);
        assert_eq!(tasks[0].attempt().relative_attempt_number(), 2);

        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: b"failed".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Wait for job to fail
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // After restart: attempt 3, but relative attempt 1
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].attempt().attempt_number(),
            3,
            "total attempt number should continue from 3"
        );
        assert_eq!(
            tasks[0].attempt().relative_attempt_number(),
            1,
            "relative attempt should reset to 1 after restart"
        );

        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: b"failed".to_vec(),
                },
            )
            .await
            .expect("report error");

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Second retry after restart: attempt 4, relative attempt 2
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].attempt().attempt_number(),
            4,
            "total attempt number should continue from 4"
        );
        assert_eq!(
            tasks[0].attempt().relative_attempt_number(),
            2,
            "relative attempt should be 2 (second attempt in this run)"
        );

        // Complete the job
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
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// Test that all stored attempts have unique attempt numbers (no overwrites).
#[silo::test]
async fn no_attempt_key_collisions_after_restart() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"test": "data"}));

        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None, // No retry policy - single attempt per run
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Run and fail, restart, run and fail, restart, run and succeed
        for run in 1..=3 {
            let tasks = shard
                .dequeue("worker-1", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            assert_eq!(tasks.len(), 1);
            let task_id = tasks[0].attempt().task_id().to_string();

            if run < 3 {
                shard
                    .report_attempt_outcome(
                        &task_id,
                        AttemptOutcome::Error {
                            error_code: "TEST_ERROR".to_string(),
                            error: format!("run {} failed", run).into_bytes(),
                        },
                    )
                    .await
                    .expect("report error");

                let status = shard
                    .get_job_status("-", &job_id)
                    .await
                    .expect("status")
                    .expect("exists");
                assert_eq!(status.kind, JobStatusKind::Failed);

                shard.restart_job("-", &job_id).await.expect("restart");
            } else {
                shard
                    .report_attempt_outcome(
                        &task_id,
                        AttemptOutcome::Success {
                            result: b"success".to_vec(),
                        },
                    )
                    .await
                    .expect("report success");
            }
        }

        // All 3 attempts should be stored with unique numbers
        let attempts = shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("get attempts");
        assert_eq!(attempts.len(), 3, "should have 3 attempts stored");

        let attempt_numbers: Vec<u32> = attempts.iter().map(|a| a.attempt_number()).collect();
        assert_eq!(
            attempt_numbers,
            vec![1, 2, 3],
            "attempt numbers should be 1, 2, 3 (no collisions)"
        );

        // Verify each attempt has distinct data
        for (i, attempt) in attempts.iter().enumerate() {
            let expected_num = (i + 1) as u32;
            assert_eq!(
                attempt.attempt_number(),
                expected_num,
                "attempt {} should have number {}",
                i,
                expected_num
            );
        }
    });
}
