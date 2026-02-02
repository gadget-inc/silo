mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::ShardCounters;
use silo::retry::RetryPolicy;

use test_helpers::*;

#[silo::test]
async fn counters_start_at_zero() {
    let (_tmp, shard) = open_temp_shard().await;

    let counters = shard.get_counters().await.expect("get_counters");
    assert_eq!(
        counters,
        ShardCounters {
            total_jobs: 0,
            completed_jobs: 0,
        }
    );
    assert_eq!(counters.open_jobs(), 0);
}

#[silo::test]
async fn enqueue_increments_total_jobs() {
    let (_tmp, shard) = open_temp_shard().await;

    // Initial state
    let counters = shard.get_counters().await.expect("get_counters");
    assert_eq!(counters.total_jobs, 0);
    assert_eq!(counters.completed_jobs, 0);

    // Enqueue a job
    let _job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"test": "enqueue"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Check counters after enqueue
    let counters = shard.get_counters().await.expect("get_counters");
    assert_eq!(
        counters.total_jobs, 1,
        "total_jobs should be 1 after enqueue"
    );
    assert_eq!(
        counters.completed_jobs, 0,
        "completed_jobs should still be 0"
    );
    assert_eq!(counters.open_jobs(), 1, "open_jobs should be 1");

    // Enqueue another job
    let _job_id2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"test": "enqueue2"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    // Check counters after second enqueue
    let counters = shard.get_counters().await.expect("get_counters");
    assert_eq!(
        counters.total_jobs, 2,
        "total_jobs should be 2 after second enqueue"
    );
    assert_eq!(
        counters.completed_jobs, 0,
        "completed_jobs should still be 0"
    );
    assert_eq!(counters.open_jobs(), 2, "open_jobs should be 2");
}

#[silo::test]
async fn success_outcome_increments_completed_jobs() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue a job
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "success"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue and succeed
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 0, "not completed yet");

        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");

        // Check counters after success
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1, "total_jobs unchanged");
        assert_eq!(
            counters.completed_jobs, 1,
            "completed_jobs should be 1 after success"
        );
        assert_eq!(counters.open_jobs(), 0, "no open jobs after completion");

        // Verify job status is Succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);
    });
}

#[silo::test]
async fn failed_outcome_without_retry_increments_completed_jobs() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue a job with no retry policy (will fail permanently)
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "fail"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Verify open_jobs before failure
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.open_jobs(), 1, "one open job before failure");

        // Dequeue and fail
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        // Check counters after permanent failure
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1, "total_jobs unchanged");
        assert_eq!(
            counters.completed_jobs, 1,
            "completed_jobs should be 1 after permanent failure"
        );
        assert_eq!(
            counters.open_jobs(),
            0,
            "open_jobs should be 0 after permanent failure"
        );

        // Verify job status is Failed
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);
    });
}

#[silo::test]
async fn failed_outcome_with_retry_does_not_increment_completed_jobs() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue a job with retry policy
        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 100,
            max_interval_ms: 1000,
            backoff_factor: 2.0,
            randomize_interval: false,
        };

        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                Some(retry_policy),
                msgpack_payload(&serde_json::json!({"test": "retry"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue and fail (first attempt)
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        // Check counters after failure with retry pending
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1, "total_jobs unchanged");
        assert_eq!(
            counters.completed_jobs, 0,
            "completed_jobs should still be 0 - retry is pending"
        );
        assert_eq!(counters.open_jobs(), 1, "job is still open (scheduled)");

        // Verify job status is Scheduled (not Failed)
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);
    });
}

#[silo::test]
async fn cancel_scheduled_job_increments_completed_jobs() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue a job
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms() + 60_000, // Future time so it stays Scheduled
                None,
                msgpack_payload(&serde_json::json!({"test": "cancel"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Verify initial counters
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 0);

        // Cancel the scheduled job
        shard.cancel_job("-", &job_id).await.expect("cancel");

        // Check counters after cancellation
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1, "total_jobs unchanged");
        assert_eq!(
            counters.completed_jobs, 1,
            "completed_jobs should be 1 after cancel"
        );

        // Verify job status is Cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
    });
}

#[silo::test]
async fn cancel_running_job_increments_completed_on_worker_report() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue a job
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "cancel_running"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue to make it Running
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Verify counters while running
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 0, "still running");

        // Cancel while running - status stays Running, completed_jobs unchanged
        shard.cancel_job("-", &job_id).await.expect("cancel");

        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(
            counters.completed_jobs, 0,
            "completed_jobs unchanged - worker hasn't reported yet"
        );

        // Worker reports Cancelled outcome
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Now completed_jobs should be incremented
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(
            counters.completed_jobs, 1,
            "completed_jobs should be 1 after worker reports cancelled"
        );

        // Verify job status is Cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
    });
}

#[silo::test]
async fn restart_decrements_completed_jobs() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue a job and fail it permanently
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "restart"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue and fail
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        // Verify counters after failure
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 1, "job is now Failed (terminal)");

        // Restart the job
        shard.restart_job("-", &job_id).await.expect("restart");

        // Check counters after restart
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1, "total_jobs unchanged");
        assert_eq!(
            counters.completed_jobs, 0,
            "completed_jobs should be 0 after restart"
        );
        assert_eq!(counters.open_jobs(), 1, "job is open again");

        // Verify job status is Scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);
    });
}

#[silo::test]
async fn delete_decrements_total_and_completed() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue and complete a job
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "delete"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue and succeed
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");

        // Verify counters before delete
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 1);

        // Delete the job
        shard.delete_job("-", &job_id).await.expect("delete");

        // Check counters after delete
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(
            counters.total_jobs, 0,
            "total_jobs should be 0 after delete"
        );
        assert_eq!(
            counters.completed_jobs, 0,
            "completed_jobs should be 0 after delete"
        );
    });
}

#[silo::test]
async fn multiple_jobs_counter_tracking() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue 5 jobs
        let mut job_ids = vec![];
        for i in 0..5 {
            let job_id = shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now_ms(),
                    None,
                    msgpack_payload(&serde_json::json!({"job": i})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
            job_ids.push(job_id);
        }

        // Verify counters
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 5);
        assert_eq!(counters.completed_jobs, 0);
        assert_eq!(counters.open_jobs(), 5);

        // Complete 3 jobs (success for 2, fail for 1)
        for i in 0..3 {
            let tasks = shard
                .dequeue("w", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            if tasks.is_empty() {
                continue;
            }
            let task_id = tasks[0].attempt().task_id().to_string();
            let outcome = if i < 2 {
                AttemptOutcome::Success { result: vec![] }
            } else {
                AttemptOutcome::Error {
                    error_code: "FAIL".to_string(),
                    error: vec![],
                }
            };
            shard
                .report_attempt_outcome(&task_id, outcome)
                .await
                .expect("report");
        }

        // Verify counters
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 5, "all jobs still exist");
        assert_eq!(counters.completed_jobs, 3, "3 jobs completed");
        assert_eq!(counters.open_jobs(), 2, "2 jobs still open");

        // Delete one completed job
        shard.delete_job("-", &job_ids[0]).await.expect("delete");

        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 4);
        assert_eq!(counters.completed_jobs, 2);
        assert_eq!(counters.open_jobs(), 2);
    });
}

#[silo::test]
async fn counters_survive_close_and_reopen() {
    with_timeout!(20000, {
        // Create a shard with local WAL for durability testing
        let (config, shard) = open_temp_shard_with_local_wal(true).await;

        // Enqueue some jobs and complete one
        let _job1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"j": 1})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue1");

        let _job2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"j": 2})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue2");

        // Complete first job
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report");

        // Verify counters before close
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 2);
        assert_eq!(counters.completed_jobs, 1);

        // Close the shard
        shard.close().await.expect("close");

        // Reopen the shard
        use silo::gubernator::MockGubernatorClient;
        use silo::settings::{Backend, DatabaseConfig, WalConfig};

        let rate_limiter = MockGubernatorClient::new_arc();
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: config.data_dir.path().to_string_lossy().to_string(),
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: config.wal_dir.path().to_string_lossy().to_string(),
            }),
            apply_wal_on_close: true,
            slatedb: Some(test_helpers::fast_flush_slatedb_settings()),
        };

        let shard2 = silo::job_store_shard::JobStoreShard::open(
            &cfg,
            rate_limiter,
            None,
            silo::shard_range::ShardRange::full(),
        )
        .await
        .expect("reopen shard");

        // Verify counters persist
        let counters = shard2.get_counters().await.expect("get_counters");
        assert_eq!(
            counters.total_jobs, 2,
            "total_jobs should persist after reopen"
        );
        assert_eq!(
            counters.completed_jobs, 1,
            "completed_jobs should persist after reopen"
        );
    });
}
