mod test_helpers;

use silo::job::{
    GubernatorAlgorithm, GubernatorRateLimit, JobStatusKind, Limit, RateLimitRetryPolicy,
};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::retry::RetryPolicy;

fn default_retry_policy() -> RetryPolicy {
    RetryPolicy {
        retry_count: 3,
        initial_interval_ms: 100,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 2.0,
    }
}

fn failed_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Failed {
            error_code: "ERR".to_string(),
            error: vec![1, 2, 3],
        },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

fn succeeded_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Succeeded {
            result: vec![4, 5, 6],
        },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

fn cancelled_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Cancelled,
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

fn base_import_params(id: &str) -> ImportJobParams {
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

// =========================================================================
// Basic import cases
// =========================================================================

#[silo::test]
async fn import_job_with_zero_attempts_creates_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let params = base_import_params("job-zero-attempts");
    let results = shard.import_jobs("-", vec![params]).await.unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Verify job info was written
    let view = shard
        .get_job("-", "job-zero-attempts")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.id(), "job-zero-attempts");
    assert_eq!(view.priority(), 50);
    assert_eq!(view.enqueue_time_ms(), 1_700_000_000_000);

    // Verify status
    let status = shard
        .get_job_status("-", "job-zero-attempts")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Scheduled);
    assert_eq!(status.current_attempt, Some(1));

    // Verify task was created (dequeue should work)
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task = &dequeued.tasks[0];
    assert_eq!(task.job().id(), "job-zero-attempts");
    assert_eq!(task.attempt().attempt_number(), 1);
}

#[silo::test]
async fn import_job_with_succeeded_attempt() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-succeeded");
    params.attempts = vec![succeeded_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Succeeded);

    let status = shard
        .get_job_status("-", "job-succeeded")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Succeeded);
    assert!(status.is_terminal());

    // Verify no task created (should not be dequeue-able)
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 0);

    // Verify counters
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

#[silo::test]
async fn import_job_with_failed_retries_exhausted() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-failed-exhausted");
    params.retry_policy = Some(RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 100,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 2.0,
    });
    // 3 failed attempts: retry_count=2 means 3 total attempts allowed (1 initial + 2 retries)
    // After 3 failures, failures_so_far=3 > retry_count=2, so exhausted
    params.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        failed_attempt(1_700_000_003_000),
    ];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Failed);

    let status = shard
        .get_job_status("-", "job-failed-exhausted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Failed);

    // No task created
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 0);

    // Counters: terminal
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

#[silo::test]
async fn import_job_with_cancelled_last_attempt() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-cancelled");
    params.attempts = vec![
        failed_attempt(1_700_000_001_000),
        cancelled_attempt(1_700_000_002_000),
    ];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Cancelled);

    let status = shard
        .get_job_status("-", "job-cancelled")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Cancelled);

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

#[silo::test]
async fn import_job_with_failed_retries_remaining() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-retries-left");
    params.retry_policy = Some(default_retry_policy()); // retry_count=3
    // 2 failed attempts: failures_so_far=2 <= retry_count=3, so retries remain
    params.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    let status = shard
        .get_job_status("-", "job-retries-left")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Scheduled);
    assert_eq!(status.current_attempt, Some(3)); // next attempt is 3

    // Task should be dequeue-able
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task = &dequeued.tasks[0];
    assert_eq!(task.attempt().attempt_number(), 3);
    assert_eq!(task.attempt().relative_attempt_number(), 3);

    // Counter: not terminal
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 0);
}

#[silo::test]
async fn import_job_with_no_retry_policy_and_failed_attempt() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-no-retry");
    params.retry_policy = None;
    params.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Failed);
}

// =========================================================================
// Retry schedule validation
// =========================================================================

#[silo::test]
async fn import_non_terminal_dequeue_fail_until_exhausted() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-exhaust");
    params.retry_policy = Some(RetryPolicy {
        retry_count: 1, // 1 retry after initial = 2 total allowed from here
        initial_interval_ms: 10,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 1.0,
    });
    // 1 prior failure, retry_count=1: failures_so_far=1 <= retry_count=1, so 1 retry remains
    params.attempts = vec![failed_attempt(1_700_000_001_000)];

    shard.import_jobs("-", vec![params]).await.unwrap();

    // Dequeue attempt 2
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();

    // Fail attempt 2
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![],
            },
        )
        .await
        .unwrap();

    // Wait for retry scheduling
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Should now be Failed (2 total failures > retry_count=1)
    let status = shard
        .get_job_status("-", "job-exhaust")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Failed);
}

#[silo::test]
async fn import_near_final_retry_one_left() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-one-retry-left");
    params.retry_policy = Some(RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 10,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 1.0,
    });
    // 2 failures, retry_count=2: failures_so_far=2 <= retry_count=2, exactly 1 more allowed
    params.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    shard.import_jobs("-", vec![params]).await.unwrap();

    // Dequeue attempt 3
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();

    // Fail attempt 3
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![],
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 3 failures > retry_count=2 -> Failed
    let status = shard
        .get_job_status("-", "job-one-retry-left")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Failed);
}

// =========================================================================
// Lifecycle operations on imported jobs
// =========================================================================

#[silo::test]
async fn cancel_imported_scheduled_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let params = base_import_params("job-cancel-sched");
    shard.import_jobs("-", vec![params]).await.unwrap();

    // Cancel the scheduled job
    shard.cancel_job("-", "job-cancel-sched").await.unwrap();

    let status = shard
        .get_job_status("-", "job-cancel-sched")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Cancelled);
}

#[silo::test]
async fn cancel_imported_running_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let params = base_import_params("job-cancel-running");
    shard.import_jobs("-", vec![params]).await.unwrap();

    // Dequeue to make it running
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();

    // Cancel the running job
    shard.cancel_job("-", "job-cancel-running").await.unwrap();

    // Heartbeat should discover cancellation
    let hb = shard.heartbeat_task("worker-1", &task_id).await.unwrap();
    assert!(hb.cancelled);

    // Report cancelled outcome
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
        .await
        .unwrap();

    let status = shard
        .get_job_status("-", "job-cancel-running")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Cancelled);
}

#[silo::test]
async fn restart_imported_failed_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-restart-failed");
    params.retry_policy = None;
    params.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert_eq!(results[0].status, JobStatusKind::Failed);

    // Restart the failed job
    shard.restart_job("-", "job-restart-failed").await.unwrap();

    let status = shard
        .get_job_status("-", "job-restart-failed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Scheduled);
    // After restart, next attempt is 2 (1 imported + 1 new)
    assert_eq!(status.current_attempt, Some(2));

    // Verify dequeue works and relative_attempt_number resets to 1
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task = &dequeued.tasks[0];
    assert_eq!(task.attempt().attempt_number(), 2);
    assert_eq!(task.attempt().relative_attempt_number(), 1);
}

#[silo::test]
async fn restart_imported_cancelled_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-restart-cancelled");
    params.attempts = vec![cancelled_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert_eq!(results[0].status, JobStatusKind::Cancelled);

    // Restart
    shard
        .restart_job("-", "job-restart-cancelled")
        .await
        .unwrap();

    let status = shard
        .get_job_status("-", "job-restart-cancelled")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Scheduled);
}

#[silo::test]
async fn expedite_imported_future_scheduled_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-expedite");
    params.retry_policy = Some(default_retry_policy());
    // Schedule far in the future
    params.start_at_ms = test_helpers::now_ms() + 600_000;
    params.attempts = vec![failed_attempt(1_700_000_001_000)];

    shard.import_jobs("-", vec![params]).await.unwrap();

    // Should not be dequeue-able yet
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 0);

    // Expedite
    shard.expedite_job("-", "job-expedite").await.unwrap();

    // Now should be dequeue-able
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
}

// =========================================================================
// Concurrency limits
// =========================================================================

#[silo::test]
async fn import_non_terminal_with_concurrency_limits() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-conc-limits");
    params.limits = vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: "test-queue".to_string(),
        max_concurrency: 2,
    })];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Dequeue should work and grant concurrency
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);

    // Verify concurrency holder was created
    let holders = test_helpers::count_concurrency_holders(shard.db()).await;
    assert!(holders > 0);
}

#[silo::test]
async fn import_non_terminal_with_concurrency_at_capacity() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // First, fill up concurrency with a regular enqueue
    let payload = test_helpers::msgpack_payload(&serde_json::json!({"type": "filler"}));
    shard
        .enqueue(
            "-",
            Some("filler-job".to_string()),
            50,
            0,
            None,
            payload,
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: "limited-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .unwrap();

    // Dequeue the filler job to hold the concurrency slot
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);

    // Now import a job with the same concurrency limit at capacity
    let mut params = base_import_params("job-conc-blocked");
    params.limits = vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
        key: "limited-queue".to_string(),
        max_concurrency: 1,
    })];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // The imported job should have a concurrency request (not immediately dequeue-able)
    let requests = test_helpers::count_concurrency_requests(shard.db()).await;
    assert!(requests > 0, "should have a concurrency request ticket");
}

// =========================================================================
// Validation errors
// =========================================================================

#[silo::test]
async fn import_with_running_attempt_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-running-attempt");
    // We can't directly create a Running ImportedAttempt since our enum only has terminal states,
    // but we can test the validation of non-Failed intermediate attempts
    params.attempts = vec![
        succeeded_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("must be Failed")
    );
}

#[silo::test]
async fn import_with_non_failed_intermediate_attempt_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-bad-intermediate");
    params.attempts = vec![
        cancelled_attempt(1_700_000_001_000), // intermediate must be Failed
        failed_attempt(1_700_000_002_000),
    ];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("must be Failed")
    );
}

#[silo::test]
async fn import_duplicate_id_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let params = base_import_params("job-dup");
    shard.import_jobs("-", vec![params]).await.unwrap();

    // Try to import again with same ID
    let params2 = base_import_params("job-dup");
    let results = shard.import_jobs("-", vec![params2]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("already exists")
    );
}

// =========================================================================
// Data integrity
// =========================================================================

#[silo::test]
async fn import_attempts_readable_via_get_job_attempts() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-read-attempts");
    params.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        succeeded_attempt(1_700_000_003_000),
    ];

    shard.import_jobs("-", vec![params]).await.unwrap();

    let attempts = shard
        .get_job_attempts("-", "job-read-attempts")
        .await
        .unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].attempt_number(), 1);
    assert_eq!(attempts[1].attempt_number(), 2);
    assert_eq!(attempts[2].attempt_number(), 3);

    // Verify attempt 1 is Failed
    match attempts[0].state() {
        silo::job_attempt::AttemptStatus::Failed {
            error_code, error, ..
        } => {
            assert_eq!(error_code, "ERR");
            assert_eq!(error, vec![1, 2, 3]);
        }
        _ => panic!("expected Failed"),
    }

    // Verify attempt 3 is Succeeded
    match attempts[2].state() {
        silo::job_attempt::AttemptStatus::Succeeded { result, .. } => {
            assert_eq!(result, vec![4, 5, 6]);
        }
        _ => panic!("expected Succeeded"),
    }
}

#[silo::test]
async fn import_get_job_returns_correct_data() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-get-data");
    params.priority = 10;
    params.enqueue_time_ms = 1_600_000_000_000;
    params.metadata = Some(vec![
        ("key1".to_string(), "val1".to_string()),
        ("key2".to_string(), "val2".to_string()),
    ]);

    shard.import_jobs("-", vec![params]).await.unwrap();

    let view = shard.get_job("-", "job-get-data").await.unwrap().unwrap();
    assert_eq!(view.id(), "job-get-data");
    assert_eq!(view.priority(), 10);
    assert_eq!(view.enqueue_time_ms(), 1_600_000_000_000);
    assert_eq!(view.task_group(), "default");

    let metadata = view.metadata();
    assert_eq!(metadata.len(), 2);
    assert!(metadata.contains(&("key1".to_string(), "val1".to_string())));
    assert!(metadata.contains(&("key2".to_string(), "val2".to_string())));
}

#[silo::test]
async fn import_counters_for_all_states() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import a scheduled job (0 attempts)
    let params1 = base_import_params("job-cnt-sched");
    shard.import_jobs("-", vec![params1]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 0);

    // Import a succeeded job
    let mut params2 = base_import_params("job-cnt-succ");
    params2.attempts = vec![succeeded_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![params2]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 2);
    assert_eq!(counters.completed_jobs, 1);

    // Import a failed job (no retry policy)
    let mut params3 = base_import_params("job-cnt-fail");
    params3.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![params3]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 3);
    assert_eq!(counters.completed_jobs, 2);

    // Import a cancelled job
    let mut params4 = base_import_params("job-cnt-canc");
    params4.attempts = vec![cancelled_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![params4]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 4);
    assert_eq!(counters.completed_jobs, 3);
}

#[silo::test]
async fn import_metadata_index_works() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-meta-idx");
    params.metadata = Some(vec![("env".to_string(), "production".to_string())]);

    shard.import_jobs("-", vec![params]).await.unwrap();

    // Verify metadata index was created by querying for it
    let prefix = silo::keys::idx_metadata_prefix("-", "env", "production");
    let count = test_helpers::count_with_binary_prefix(shard.db(), &prefix).await;
    assert!(count > 0, "metadata index entry should exist");
}

// =========================================================================
// Batch import
// =========================================================================

#[silo::test]
async fn import_batch_mixed_results() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let params1 = base_import_params("batch-job-1");
    let mut params2 = base_import_params("batch-job-2");
    params2.attempts = vec![succeeded_attempt(1_700_000_001_000)];

    // Import both
    let results = shard
        .import_jobs("-", vec![params1, params2])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);
    assert!(results[1].success);
    assert_eq!(results[1].status, JobStatusKind::Succeeded);

    // Both jobs should be queryable
    assert!(shard.get_job("-", "batch-job-1").await.unwrap().is_some());
    assert!(shard.get_job("-", "batch-job-2").await.unwrap().is_some());
}

#[silo::test]
async fn import_batch_partial_failure() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // First import a job to create a duplicate
    let params0 = base_import_params("batch-dup");
    shard.import_jobs("-", vec![params0]).await.unwrap();

    // Batch with one good and one duplicate
    let params1 = base_import_params("batch-new");
    let params2 = base_import_params("batch-dup"); // will fail

    let results = shard
        .import_jobs("-", vec![params1, params2])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].success);
    assert!(!results[1].success);
}

// =========================================================================
// Edge cases
// =========================================================================

#[silo::test]
async fn import_with_enqueue_time_zero_uses_now() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let now = test_helpers::now_ms();

    let mut params = base_import_params("job-time-zero");
    params.enqueue_time_ms = 0;

    shard.import_jobs("-", vec![params]).await.unwrap();

    let view = shard.get_job("-", "job-time-zero").await.unwrap().unwrap();
    // Should be close to now (within a few seconds)
    assert!((view.enqueue_time_ms() - now).abs() < 5000);
}

#[silo::test]
async fn import_empty_id_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("");
    params.id = "".to_string();

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("id is required")
    );
}

// =========================================================================
// Successful execution after import with retries remaining
// =========================================================================

#[silo::test]
async fn import_non_terminal_dequeue_succeed() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-succeed-after-import");
    params.retry_policy = Some(default_retry_policy()); // retry_count=3
    params.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Dequeue attempt 2
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task = &dequeued.tasks[0];
    assert_eq!(task.attempt().attempt_number(), 2);
    let task_id = task.attempt().task_id().to_string();

    // Report success
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: vec![42, 43],
            },
        )
        .await
        .unwrap();

    // Verify job is now succeeded
    let status = shard
        .get_job_status("-", "job-succeed-after-import")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.kind, JobStatusKind::Succeeded);

    // Verify counters
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);

    // Verify both attempts are readable
    let attempts = shard
        .get_job_attempts("-", "job-succeed-after-import")
        .await
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(matches!(
        attempts[0].state(),
        silo::job_attempt::AttemptStatus::Failed { .. }
    ));
    assert!(matches!(
        attempts[1].state(),
        silo::job_attempt::AttemptStatus::Succeeded { .. }
    ));
}

// =========================================================================
// Import with rate limits
// =========================================================================

#[silo::test]
async fn import_non_terminal_with_rate_limits() {
    use silo::gubernator::MockGubernatorClient;

    let rate_limiter = MockGubernatorClient::new_arc();
    let (_tmp, shard) = test_helpers::open_temp_shard_with_rate_limiter(rate_limiter.clone()).await;

    let mut params = base_import_params("job-rate-limited");
    params.limits = vec![Limit::RateLimit(GubernatorRateLimit {
        name: "api-limit".to_string(),
        unique_key: format!("import-test-{}", uuid::Uuid::new_v4()),
        limit: 100,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 10,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_retries: 10,
        },
    })];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Dequeue should process the rate limit check internally and return the task
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task = &dequeued.tasks[0];
    assert_eq!(task.attempt().attempt_number(), 1);
}

// =========================================================================
// Delete imported terminal job
// =========================================================================

#[silo::test]
async fn delete_imported_terminal_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-delete-me");
    params.attempts = vec![succeeded_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Succeeded);

    // Verify counters before delete
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);

    // Delete the job
    shard.delete_job("-", "job-delete-me").await.unwrap();

    // Verify job is gone
    assert!(shard.get_job("-", "job-delete-me").await.unwrap().is_none());
    assert!(
        shard
            .get_job_status("-", "job-delete-me")
            .await
            .unwrap()
            .is_none()
    );

    // Verify counters decremented
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 0);
    assert_eq!(counters.completed_jobs, 0);
}

// =========================================================================
// Multi-tenant import
// =========================================================================

#[silo::test]
async fn import_multi_tenant_isolation() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import a job into tenant-a
    let mut params_a = base_import_params("shared-id");
    params_a.attempts = vec![succeeded_attempt(1_700_000_001_000)];
    let results = shard.import_jobs("tenant-a", vec![params_a]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Succeeded);

    // Import a job with the same ID into tenant-b (should succeed - different tenant)
    let params_b = base_import_params("shared-id");
    let results = shard.import_jobs("tenant-b", vec![params_b]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Verify tenant-a sees succeeded job
    let status_a = shard
        .get_job_status("tenant-a", "shared-id")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status_a.kind, JobStatusKind::Succeeded);

    // Verify tenant-b sees scheduled job
    let status_b = shard
        .get_job_status("tenant-b", "shared-id")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status_b.kind, JobStatusKind::Scheduled);

    // Dequeue from tenant-b's task group should return the scheduled job
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    assert_eq!(dequeued.tasks[0].tenant_id(), "tenant-b");
}

// =========================================================================
// started_at_ms preservation
// =========================================================================

#[silo::test]
async fn import_preserves_started_at_ms() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut params = base_import_params("job-started-at");
    params.attempts = vec![
        ImportedAttempt {
            status: ImportedAttemptStatus::Failed {
                error_code: "ERR".to_string(),
                error: vec![],
            },
            started_at_ms: 1_700_000_000_000,
            finished_at_ms: 1_700_000_001_000,
        },
        ImportedAttempt {
            status: ImportedAttemptStatus::Succeeded {
                result: vec![1, 2, 3],
            },
            started_at_ms: 1_700_000_002_000,
            finished_at_ms: 1_700_000_003_000,
        },
    ];

    shard.import_jobs("-", vec![params]).await.unwrap();

    let attempts = shard.get_job_attempts("-", "job-started-at").await.unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].started_at_ms(), 1_700_000_000_000);
    assert_eq!(attempts[1].started_at_ms(), 1_700_000_002_000);
}
