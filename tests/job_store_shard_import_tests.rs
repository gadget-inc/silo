mod test_helpers;

use silo::codec::{decode_task, encode_task};
use silo::job::{
    ConcurrencyLimit, GubernatorAlgorithm, GubernatorRateLimit, JobStatusKind, Limit,
    RateLimitRetryPolicy,
};
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::JobStoreShard;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::retry::RetryPolicy;
use silo::task::{GubernatorRateLimitData, Task};
use slatedb::WriteBatch;
use std::sync::Arc;

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

    // Try to import again with same ID and same data (no new attempts)
    let params2 = base_import_params("job-dup");
    let results = shard.import_jobs("-", vec![params2]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("at least one new attempt"),
        "expected reimport error about new attempts, got: {}",
        results[0].error.as_ref().unwrap()
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

// =========================================================================
// Reimport tests
// =========================================================================

/// Helper: import a job and then reimport with additional attempts.
/// Returns the reimport result status.
async fn do_reimport(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    _id: &str,
    initial: ImportJobParams,
    reimport: ImportJobParams,
) -> JobStatusKind {
    let results = shard.import_jobs(tenant, vec![initial]).await.unwrap();
    assert!(
        results[0].success,
        "initial import failed: {:?}",
        results[0].error
    );

    let initial_status = results[0].status;
    let tasks_before = test_helpers::count_task_keys(shard.db()).await;
    if initial_status == JobStatusKind::Scheduled {
        assert!(
            tasks_before >= 1,
            "expected at least 1 task after initial import of Scheduled job, got {tasks_before}"
        );
    }

    let results = shard.import_jobs(tenant, vec![reimport]).await.unwrap();
    assert!(
        results[0].success,
        "reimport failed: {:?}",
        results[0].error
    );

    let reimport_status = results[0].status;
    let tasks_after = test_helpers::count_task_keys(shard.db()).await;
    if reimport_status == JobStatusKind::Scheduled {
        // Non-terminal reimport: old task removed, new task created
        assert!(
            tasks_after >= 1,
            "expected at least 1 task after non-terminal reimport, got {tasks_after}"
        );
    } else {
        // Terminal reimport: old task removed, no new task created
        assert_eq!(
            tasks_after, 0,
            "expected 0 tasks after terminal reimport, got {tasks_after}"
        );
    }

    reimport_status
}

// =========================================================================
// State transition matrix
// =========================================================================

#[silo::test]
async fn reimport_scheduled_zero_attempts_add_failed_exhausted() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let initial = base_import_params("reimp-s0-fail");
    let mut reimport = base_import_params("reimp-s0-fail");
    reimport.retry_policy = None; // no retries -> exhausted
    reimport.attempts = vec![failed_attempt(1_700_000_001_000)];

    let status = do_reimport(&shard, "-", "reimp-s0-fail", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Failed);

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 0);
}

#[silo::test]
async fn reimport_scheduled_with_attempts_add_failed_retries_remain() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Initial: Scheduled with 2 failed attempts (retries remain with retry_count=3)
    let mut initial = base_import_params("reimp-s2-sched");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    // Reimport: same 2 + 1 more failed, still retries remain (3 fails <= retry_count=3)
    let mut reimport = base_import_params("reimp-s2-sched");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        failed_attempt(1_700_000_003_000),
    ];

    let status = do_reimport(&shard, "-", "reimp-s2-sched", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Scheduled);

    // New task should be created
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    assert_eq!(dequeued.tasks[0].attempt().attempt_number(), 4);
}

#[silo::test]
async fn reimport_scheduled_with_attempts_add_succeeded() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-s2-succ");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let mut reimport = base_import_params("reimp-s2-succ");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        succeeded_attempt(1_700_000_003_000),
    ];

    let status = do_reimport(&shard, "-", "reimp-s2-succ", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Succeeded);

    // Old task should be gone
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 0);
}

#[silo::test]
async fn reimport_scheduled_with_attempts_add_failed_exhausted() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-s2-exh");
    initial.retry_policy = Some(RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 100,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 2.0,
    });
    initial.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    // 3 total failures > retry_count=2 -> exhausted
    let mut reimport = base_import_params("reimp-s2-exh");
    reimport.retry_policy = Some(RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 100,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 2.0,
    });
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        failed_attempt(1_700_000_003_000),
    ];

    let status = do_reimport(&shard, "-", "reimp-s2-exh", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Failed);

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 0);
}

#[silo::test]
async fn reimport_failed_exhausted_add_failed_new_retry_policy() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Initial: Failed (exhausted with retry_count=0)
    let mut initial = base_import_params("reimp-f-sched");
    initial.retry_policy = None;
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];

    // Reimport: same 1 + 1 more failed, but with retry_count=3 so retries remain
    let mut reimport = base_import_params("reimp-f-sched");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let status = do_reimport(&shard, "-", "reimp-f-sched", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Scheduled);

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
}

#[silo::test]
async fn reimport_failed_exhausted_still_exhausted() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-f-f");
    initial.retry_policy = None;
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];

    let mut reimport = base_import_params("reimp-f-f");
    reimport.retry_policy = None;
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let status = do_reimport(&shard, "-", "reimp-f-f", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Failed);
}

#[silo::test]
async fn reimport_cancelled_add_failed_exhausted() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-c-fail");
    initial.attempts = vec![cancelled_attempt(1_700_000_001_000)];

    let mut reimport = base_import_params("reimp-c-fail");
    reimport.retry_policy = None;
    reimport.attempts = vec![
        cancelled_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let status = do_reimport(&shard, "-", "reimp-c-fail", initial, reimport).await;
    assert_eq!(status, JobStatusKind::Failed);
}

// =========================================================================
// Reimport precondition rejection
// =========================================================================

#[silo::test]
async fn reimport_succeeded_job_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-rej-succ");
    initial.attempts = vec![succeeded_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    let mut reimport = base_import_params("reimp-rej-succ");
    reimport.attempts = vec![
        succeeded_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];

    let results = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("already succeeded")
    );
}

#[silo::test]
async fn reimport_running_job_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import a scheduled job and dequeue it to make it Running
    let initial = base_import_params("reimp-rej-run");
    shard.import_jobs("-", vec![initial]).await.unwrap();

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);

    // Try reimport while Running
    let mut reimport = base_import_params("reimp-rej-run");
    reimport.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("currently running")
    );
}

#[silo::test]
async fn reimport_fewer_attempts_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-rej-fewer");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Reimport with only 1 attempt (fewer than existing 2)
    let mut reimport = base_import_params("reimp-rej-fewer");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("existing attempts")
    );
}

#[silo::test]
async fn reimport_mismatched_attempt_data_fails() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-rej-mismatch");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Reimport with different error_code for existing attempt
    let mut reimport = base_import_params("reimp-rej-mismatch");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![
        ImportedAttempt {
            status: ImportedAttemptStatus::Failed {
                error_code: "DIFFERENT_ERR".to_string(),
                error: vec![1, 2, 3],
            },
            started_at_ms: 1_700_000_000_000,
            finished_at_ms: 1_700_000_001_000,
        },
        failed_attempt(1_700_000_002_000),
    ];

    let results = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("does not match")
    );
}

// =========================================================================
// Scheduling state cleanup
// =========================================================================

#[silo::test]
async fn reimport_scheduled_to_scheduled_replaces_task() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-clean-replace");
    initial.retry_policy = Some(default_retry_policy());
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Verify task exists
    let tasks_before = test_helpers::count_task_keys(shard.db()).await;
    assert_eq!(tasks_before, 1);

    // Reimport: still scheduled with new attempt
    let mut reimport = base_import_params("reimp-clean-replace");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Should still have exactly 1 task (old replaced by new)
    let tasks_after = test_helpers::count_task_keys(shard.db()).await;
    assert_eq!(tasks_after, 1);

    // Dequeue should give attempt 3 (2 imported + next)
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    assert_eq!(dequeued.tasks[0].attempt().attempt_number(), 2);
}

// =========================================================================
// Counter tests
// =========================================================================

#[silo::test]
async fn reimport_scheduled_to_terminal_increments_completed() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let initial = base_import_params("reimp-cnt-inc");
    shard.import_jobs("-", vec![initial]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 0);

    let mut reimport = base_import_params("reimp-cnt-inc");
    reimport.attempts = vec![succeeded_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![reimport]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

#[silo::test]
async fn reimport_failed_to_scheduled_decrements_completed() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-cnt-dec");
    initial.retry_policy = None;
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);

    let mut reimport = base_import_params("reimp-cnt-dec");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    shard.import_jobs("-", vec![reimport]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 0);
}

#[silo::test]
async fn reimport_failed_to_failed_no_counter_change() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let mut initial = base_import_params("reimp-cnt-same");
    initial.retry_policy = None;
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.completed_jobs, 1);

    let mut reimport = base_import_params("reimp-cnt-same");
    reimport.retry_policy = None;
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    shard.import_jobs("-", vec![reimport]).await.unwrap();

    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

#[silo::test]
async fn reimport_multiple_counter_consistency() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import as Scheduled
    let initial = base_import_params("reimp-cnt-multi");
    shard.import_jobs("-", vec![initial]).await.unwrap();
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 0);

    // Reimport 1: Scheduled -> Failed
    let mut reimport1 = base_import_params("reimp-cnt-multi");
    reimport1.retry_policy = None;
    reimport1.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![reimport1]).await.unwrap();
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);

    // Reimport 2: Failed -> Scheduled (new retry policy)
    let mut reimport2 = base_import_params("reimp-cnt-multi");
    reimport2.retry_policy = Some(default_retry_policy());
    reimport2.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    shard.import_jobs("-", vec![reimport2]).await.unwrap();
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 0);

    // Reimport 3: Scheduled -> Succeeded
    let mut reimport3 = base_import_params("reimp-cnt-multi");
    reimport3.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        succeeded_attempt(1_700_000_003_000),
    ];
    shard.import_jobs("-", vec![reimport3]).await.unwrap();
    let counters = shard.get_counters().await.unwrap();
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

// =========================================================================
// Multi-reimport tests
// =========================================================================

#[silo::test]
async fn reimport_full_migration_cycle() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import with 0 attempts (Scheduled)
    let initial = base_import_params("reimp-cycle");
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Reimport 1: +1 fail (still Scheduled with retries)
    let mut reimport1 = base_import_params("reimp-cycle");
    reimport1.retry_policy = Some(default_retry_policy());
    reimport1.attempts = vec![failed_attempt(1_700_000_001_000)];
    let results = shard.import_jobs("-", vec![reimport1]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Scheduled);

    // Reimport 2: +1 succeed (Succeeded)
    let mut reimport2 = base_import_params("reimp-cycle");
    reimport2.attempts = vec![
        failed_attempt(1_700_000_001_000),
        succeeded_attempt(1_700_000_002_000),
    ];
    let results = shard.import_jobs("-", vec![reimport2]).await.unwrap();
    assert!(results[0].success);
    assert_eq!(results[0].status, JobStatusKind::Succeeded);

    // Verify all attempts readable
    let attempts = shard.get_job_attempts("-", "reimp-cycle").await.unwrap();
    assert_eq!(attempts.len(), 2);
}

#[silo::test]
async fn reimport_three_sequential_accumulating_attempts() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import with 1 failure
    let mut initial = base_import_params("reimp-accum");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Reimport 1: +1 failure (2 total)
    let mut reimport1 = base_import_params("reimp-accum");
    reimport1.retry_policy = Some(default_retry_policy());
    reimport1.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    let results = shard.import_jobs("-", vec![reimport1]).await.unwrap();
    assert!(results[0].success);

    // Reimport 2: +1 failure (3 total)
    let mut reimport2 = base_import_params("reimp-accum");
    reimport2.retry_policy = Some(default_retry_policy());
    reimport2.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        failed_attempt(1_700_000_003_000),
    ];
    let results = shard.import_jobs("-", vec![reimport2]).await.unwrap();
    assert!(results[0].success);

    // All 3 attempts should be readable
    let attempts = shard.get_job_attempts("-", "reimp-accum").await.unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].attempt_number(), 1);
    assert_eq!(attempts[1].attempt_number(), 2);
    assert_eq!(attempts[2].attempt_number(), 3);
}

// =========================================================================
// Interaction tests
// =========================================================================

#[silo::test]
async fn reimport_all_attempts_readable() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import with 2 failures
    let mut initial = base_import_params("reimp-readall");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Reimport with +1 succeeded
    let mut reimport = base_import_params("reimp-readall");
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
        succeeded_attempt(1_700_000_003_000),
    ];
    shard.import_jobs("-", vec![reimport]).await.unwrap();

    // All 3 attempts should be readable with correct data
    let attempts = shard.get_job_attempts("-", "reimp-readall").await.unwrap();
    assert_eq!(attempts.len(), 3);

    assert!(matches!(
        attempts[0].state(),
        silo::job_attempt::AttemptStatus::Failed { .. }
    ));
    assert!(matches!(
        attempts[1].state(),
        silo::job_attempt::AttemptStatus::Failed { .. }
    ));
    assert!(matches!(
        attempts[2].state(),
        silo::job_attempt::AttemptStatus::Succeeded { .. }
    ));
}

// =========================================================================
// Data integrity: status index
// =========================================================================

#[silo::test]
async fn reimport_status_index_correct_after_transition() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import as Scheduled
    let initial = base_import_params("reimp-idx");
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Verify Scheduled status index entry
    let scheduled_count = test_helpers::count_with_binary_prefix(
        shard.db(),
        &silo::keys::idx_status_time_prefix("-", "Scheduled"),
    )
    .await;
    assert_eq!(scheduled_count, 1);

    // Reimport to Succeeded
    let mut reimport = base_import_params("reimp-idx");
    reimport.attempts = vec![succeeded_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![reimport]).await.unwrap();

    // Old Scheduled index should be gone, Succeeded should exist
    let scheduled_count = test_helpers::count_with_binary_prefix(
        shard.db(),
        &silo::keys::idx_status_time_prefix("-", "Scheduled"),
    )
    .await;
    assert_eq!(scheduled_count, 0);

    let succeeded_count = test_helpers::count_with_binary_prefix(
        shard.db(),
        &silo::keys::idx_status_time_prefix("-", "Succeeded"),
    )
    .await;
    assert_eq!(succeeded_count, 1);
}

// =========================================================================
// Reimport transition tests
// =========================================================================
//
// These tests systematically explore all reachable pre-reimport states that
// can occur from silo lifecycle operations happening between imports.
// For each state we reimport to both a non-terminal (Scheduled) and terminal
// (Succeeded) target, then verify ALL side-effect state is correct:
//   - job status
//   - cancelled key cleaned up
//   - task keys (1 if Scheduled, 0 if terminal)
//   - concurrency holders cleaned up
//   - concurrency requests cleaned up
//   - dequeue works if Scheduled
//   - counters are consistent

/// Convert existing silo attempt records into ImportedAttempt form for reimport matching.
async fn existing_attempts_as_imported(
    shard: &JobStoreShard,
    tenant: &str,
    job_id: &str,
) -> Vec<ImportedAttempt> {
    let attempts = shard.get_job_attempts(tenant, job_id).await.unwrap();
    attempts
        .iter()
        .map(|a| {
            let (status, finished_at_ms) = match a.state() {
                AttemptStatus::Succeeded {
                    finished_at_ms,
                    result,
                } => (ImportedAttemptStatus::Succeeded { result }, finished_at_ms),
                AttemptStatus::Failed {
                    finished_at_ms,
                    error_code,
                    error,
                } => (
                    ImportedAttemptStatus::Failed { error_code, error },
                    finished_at_ms,
                ),
                AttemptStatus::Cancelled { finished_at_ms } => {
                    (ImportedAttemptStatus::Cancelled, finished_at_ms)
                }
                AttemptStatus::Running => panic!("attempt should be terminal for reimport"),
            };
            ImportedAttempt {
                status,
                started_at_ms: a.started_at_ms(),
                finished_at_ms,
            }
        })
        .collect()
}

/// Verify all reimport invariants hold after a reimport.
/// If `isolated` is true, we also check global counts (task keys, lease keys) which
/// require no other jobs in the shard. If false, we skip checks that could be affected
/// by other jobs sharing the shard.
async fn verify_reimport_invariants(
    shard: &JobStoreShard,
    tenant: &str,
    job_id: &str,
    expected_status: JobStatusKind,
    expected_attempt_count: usize,
    scenario_name: &str,
) {
    verify_reimport_invariants_inner(
        shard,
        tenant,
        job_id,
        expected_status,
        expected_attempt_count,
        scenario_name,
        true,
    )
    .await;
}

async fn verify_reimport_invariants_inner(
    shard: &JobStoreShard,
    tenant: &str,
    job_id: &str,
    expected_status: JobStatusKind,
    expected_attempt_count: usize,
    scenario_name: &str,
    isolated: bool,
) {
    let ctx = format!("[{}]", scenario_name);

    // 1. Status is correct
    let status = shard.get_job_status(tenant, job_id).await.unwrap().unwrap();
    assert_eq!(
        status.kind, expected_status,
        "{ctx} expected status {:?}",
        expected_status
    );

    // 2. Cancelled key must not exist after reimport (regardless of terminal/non-terminal)
    assert!(
        !shard.is_job_cancelled(tenant, job_id).await.unwrap(),
        "{ctx} cancelled key should not exist after reimport"
    );

    // 3. Task keys: (only meaningful when no other jobs are present)
    if isolated {
        let task_count = test_helpers::count_task_keys(shard.db()).await;
        if expected_status == JobStatusKind::Scheduled {
            assert_eq!(
                task_count, 1,
                "{ctx} should have exactly 1 task key when Scheduled"
            );
        } else {
            assert_eq!(task_count, 0, "{ctx} should have 0 task keys when terminal");
        }
    }

    // 4. Attempt count is correct
    let attempts = shard.get_job_attempts(tenant, job_id).await.unwrap();
    assert_eq!(
        attempts.len(),
        expected_attempt_count,
        "{ctx} expected {} attempts",
        expected_attempt_count
    );

    // 5. No lease keys lingering (only meaningful when no other jobs are present)
    if isolated {
        let lease_count = test_helpers::count_lease_keys(shard.db()).await;
        assert_eq!(
            lease_count, 0,
            "{ctx} should have 0 lease keys after reimport"
        );
    }

    // 7. Status index has entry for the current status
    // (must check before dequeue which would change the status)
    let idx_count = test_helpers::count_with_binary_prefix(
        shard.db(),
        &silo::keys::idx_status_time_prefix(tenant, expected_status.as_str()),
    )
    .await;
    assert!(
        idx_count >= 1,
        "{ctx} status index should have entry for {:?}",
        expected_status
    );

    // 8. Dequeue works if Scheduled
    if expected_status == JobStatusKind::Scheduled {
        let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
        assert_eq!(
            dequeued.tasks.len(),
            1,
            "{ctx} should be able to dequeue when Scheduled"
        );
        assert_eq!(
            dequeued.tasks[0].job().id(),
            job_id,
            "{ctx} dequeued wrong job"
        );
        // Report success to clean up
        let task_id = dequeued.tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![99] })
            .await
            .unwrap();
    }
}

/// Build reimport params that include existing attempts plus new ones targeting the given status.
fn build_reimport_params(
    job_id: &str,
    existing_attempts: Vec<ImportedAttempt>,
    target_terminal: bool,
) -> ImportJobParams {
    let mut attempts = existing_attempts;
    if target_terminal {
        // Add a succeeded attempt to make it terminal
        attempts.push(succeeded_attempt(1_700_000_100_000));
    } else {
        // Add a failed attempt; with retry_count=3 it stays Scheduled
        attempts.push(failed_attempt(1_700_000_100_000));
    }

    ImportJobParams {
        id: job_id.to_string(),
        priority: 50,
        enqueue_time_ms: 1_700_000_000_000,
        start_at_ms: 0,
        retry_policy: Some(RetryPolicy {
            retry_count: 10, // generous to always allow retries when non-terminal
            initial_interval_ms: 10,
            max_interval_ms: 10_000,
            randomize_interval: false,
            backoff_factor: 1.0,
        }),
        payload: test_helpers::msgpack_payload(&serde_json::json!({"reimport": true})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts,
    }
}

// ── Lifecycle path: fresh Scheduled (import with 0 attempts) ────────────

async fn setup_fresh_scheduled(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(default_retry_policy());
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);
}

#[silo::test]
async fn reimport_transitions_fresh_scheduled_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-fresh-sched-to-sched";
    setup_fresh_scheduled(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        1,
        "fresh→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_fresh_scheduled_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-fresh-sched-to-term";
    setup_fresh_scheduled(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        1,
        "fresh→term",
    )
    .await;
}

// ── Lifecycle path: Cancelled via cancel_job ────────────────────────────

async fn setup_cancelled_via_cancel_job(shard: &Arc<JobStoreShard>, job_id: &str) {
    setup_fresh_scheduled(shard, job_id).await;
    shard.cancel_job("-", job_id).await.unwrap();
    let s = shard.get_job_status("-", job_id).await.unwrap().unwrap();
    assert_eq!(s.kind, JobStatusKind::Cancelled);
    // Verify cancelled key exists
    assert!(shard.is_job_cancelled("-", job_id).await.unwrap());
}

#[silo::test]
async fn reimport_transitions_cancel_job_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cancel-to-sched";
    setup_cancelled_via_cancel_job(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        1,
        "cancel→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_cancel_job_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cancel-to-term";
    setup_cancelled_via_cancel_job(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        1,
        "cancel→term",
    )
    .await;
}

// ── Lifecycle path: Failed via dequeue + error (exhausted) ──────────────

async fn setup_failed_via_dequeue(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = None; // exhausted after first failure
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);

    // Dequeue and fail
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();

    let s = shard.get_job_status("-", job_id).await.unwrap().unwrap();
    assert_eq!(s.kind, JobStatusKind::Failed);
}

#[silo::test]
async fn reimport_transitions_failed_via_dequeue_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-fail-deq-to-sched";
    setup_failed_via_dequeue(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        2,
        "fail-deq→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_failed_via_dequeue_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-fail-deq-to-term";
    setup_failed_via_dequeue(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        2,
        "fail-deq→term",
    )
    .await;
}

// ── Lifecycle path: Scheduled via dequeue + error + retry ───────────────

async fn setup_scheduled_via_retry(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 10,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 1.0,
    });
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);

    // Dequeue, fail (retries remain → Scheduled)
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "RETRY".to_string(),
                error: vec![],
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let s = shard.get_job_status("-", job_id).await.unwrap().unwrap();
    assert_eq!(s.kind, JobStatusKind::Scheduled);
}

#[silo::test]
async fn reimport_transitions_retry_scheduled_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-retry-to-sched";
    setup_scheduled_via_retry(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        2,
        "retry→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_retry_scheduled_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-retry-to-term";
    setup_scheduled_via_retry(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        2,
        "retry→term",
    )
    .await;
}

// ── Lifecycle path: Cancelled via dequeue + report(Cancelled) ───────────

async fn setup_cancelled_via_dequeue(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(default_retry_policy());
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
        .await
        .unwrap();

    let s = shard.get_job_status("-", job_id).await.unwrap().unwrap();
    assert_eq!(s.kind, JobStatusKind::Cancelled);
}

#[silo::test]
async fn reimport_transitions_cancelled_via_dequeue_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-deq-to-sched";
    setup_cancelled_via_dequeue(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        2,
        "cxl-deq→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_cancelled_via_dequeue_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-deq-to-term";
    setup_cancelled_via_dequeue(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        2,
        "cxl-deq→term",
    )
    .await;
}

// ── Lifecycle path: Cancelled via cancel_job on Running ─────────────────
// Import → dequeue → Running → cancel_job → heartbeat → report(Cancelled)

async fn setup_cancelled_while_running(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(default_retry_policy());
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();

    // Cancel while running
    shard.cancel_job("-", job_id).await.unwrap();
    assert!(shard.is_job_cancelled("-", job_id).await.unwrap());

    // Worker discovers cancellation and reports it
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
        .await
        .unwrap();

    let s = shard.get_job_status("-", job_id).await.unwrap().unwrap();
    assert_eq!(s.kind, JobStatusKind::Cancelled);
    // Cancelled key still exists (report_outcome doesn't remove it)
    assert!(shard.is_job_cancelled("-", job_id).await.unwrap());
}

#[silo::test]
async fn reimport_transitions_cancel_while_running_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-run-to-sched";
    setup_cancelled_while_running(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        2,
        "cxl-run→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_cancel_while_running_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-run-to-term";
    setup_cancelled_while_running(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        2,
        "cxl-run→term",
    )
    .await;
}

// ── Lifecycle path: Scheduled with concurrency holder ───────────────────

async fn setup_scheduled_with_concurrency(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(default_retry_policy());
    params.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-test-queue".to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);

    // Verify holder exists
    assert!(
        test_helpers::count_concurrency_holders(shard.db()).await > 0,
        "should have concurrency holder after import with limits"
    );
}

#[silo::test]
async fn reimport_transitions_concurrency_holder_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-conc-hold-to-sched";
    setup_scheduled_with_concurrency(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    // Must include the same limits for reimport
    let mut reimport = build_reimport_params(job_id, existing, false);
    reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-test-queue".to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    // Should still have exactly 1 holder (old released, new acquired)
    let holders = test_helpers::count_concurrency_holders(shard.db()).await;
    assert_eq!(
        holders, 1,
        "[conc-hold→sched] should have 1 holder after reimport"
    );

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        1,
        "conc-hold→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_concurrency_holder_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-conc-hold-to-term";
    setup_scheduled_with_concurrency(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let mut reimport = build_reimport_params(job_id, existing, true);
    reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-test-queue".to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        1,
        "conc-hold→term",
    )
    .await;
}

// ── Lifecycle path: Scheduled with concurrency request (at capacity) ────

async fn setup_scheduled_with_concurrency_request(shard: &Arc<JobStoreShard>, job_id: &str) {
    // Fill the concurrency slot with another job
    let payload = test_helpers::msgpack_payload(&serde_json::json!({"filler": true}));
    shard
        .enqueue(
            "-",
            Some("sys-filler".to_string()),
            50,
            0,
            None,
            payload,
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "sys-limited-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .unwrap();
    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);

    // Now import - will get a concurrency request (not a holder)
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(default_retry_policy());
    params.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-limited-queue".to_string(),
        max_concurrency: 1,
    })];
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);

    assert!(
        test_helpers::count_concurrency_requests(shard.db()).await > 0,
        "should have concurrency request when at capacity"
    );
}

#[silo::test]
async fn reimport_transitions_concurrency_request_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-conc-req-to-term";
    setup_scheduled_with_concurrency_request(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let mut reimport = build_reimport_params(job_id, existing, true);
    reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-limited-queue".to_string(),
        max_concurrency: 1,
    })];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    // Use non-isolated check since filler job has active lease + holder
    verify_reimport_invariants_inner(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        1,
        "conc-req→term",
        false,
    )
    .await;

    // Verify our job's concurrency request was cleaned up specifically
    // (the filler job's holder is unrelated)
    let requests = test_helpers::count_concurrency_requests(shard.db()).await;
    assert_eq!(
        requests, 0,
        "concurrency request should be removed after terminal reimport"
    );
}

// ── Lifecycle path: Failed import (no lifecycle ops) ────────────────────

async fn setup_failed_import(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = None;
    params.attempts = vec![failed_attempt(1_700_000_001_000)];
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Failed);
}

#[silo::test]
async fn reimport_transitions_failed_import_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-fail-imp-to-sched";
    setup_failed_import(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        2,
        "fail-imp→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_failed_import_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-fail-imp-to-term";
    setup_failed_import(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        2,
        "fail-imp→term",
    )
    .await;
}

// ── Lifecycle path: Cancelled import (no cancelled key) ─────────────────

async fn setup_cancelled_import(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.attempts = vec![cancelled_attempt(1_700_000_001_000)];
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Cancelled);
    // No cancelled key — that's only from cancel_job
    assert!(!shard.is_job_cancelled("-", job_id).await.unwrap());
}

#[silo::test]
async fn reimport_transitions_cancelled_import_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-imp-to-sched";
    setup_cancelled_import(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, false);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        2,
        "cxl-imp→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_cancelled_import_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-imp-to-term";
    setup_cancelled_import(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let reimport = build_reimport_params(job_id, existing, true);
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        2,
        "cxl-imp→term",
    )
    .await;
}

// ── Lifecycle path: Cancelled via cancel_job + concurrency holder ───────
// Import with concurrency → Scheduled(holder) → cancel_job → Cancelled

async fn setup_cancelled_with_concurrency(shard: &Arc<JobStoreShard>, job_id: &str) {
    let mut params = base_import_params(job_id);
    params.retry_policy = Some(default_retry_policy());
    params.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-cancel-conc-q".to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![params]).await.unwrap();
    assert!(r[0].success);

    assert!(test_helpers::count_concurrency_holders(shard.db()).await > 0);

    shard.cancel_job("-", job_id).await.unwrap();
    assert!(shard.is_job_cancelled("-", job_id).await.unwrap());
}

#[silo::test]
async fn reimport_transitions_cancel_with_concurrency_to_scheduled() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-conc-to-sched";
    setup_cancelled_with_concurrency(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let mut reimport = build_reimport_params(job_id, existing, false);
    reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-cancel-conc-q".to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Scheduled,
        1,
        "cxl-conc→sched",
    )
    .await;
}

#[silo::test]
async fn reimport_transitions_cancel_with_concurrency_to_terminal() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "sys-cxl-conc-to-term";
    setup_cancelled_with_concurrency(&shard, job_id).await;

    let existing = existing_attempts_as_imported(&shard, "-", job_id).await;
    let mut reimport = build_reimport_params(job_id, existing, true);
    reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: "sys-cancel-conc-q".to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);

    verify_reimport_invariants(
        &shard,
        "-",
        job_id,
        JobStatusKind::Succeeded,
        1,
        "cxl-conc→term",
    )
    .await;
}

#[silo::test]
async fn reimport_no_new_attempts_rejected() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    // Import with 1 failure
    let mut initial = base_import_params("reimp-no-new");
    initial.retry_policy = Some(default_retry_policy());
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];
    shard.import_jobs("-", vec![initial]).await.unwrap();

    // Reimport with same attempt (no new ones)
    let mut reimport = base_import_params("reimp-no-new");
    reimport.retry_policy = Some(default_retry_policy());
    reimport.attempts = vec![failed_attempt(1_700_000_001_000)];

    let results = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(!results[0].success);
    assert!(
        results[0]
            .error
            .as_ref()
            .unwrap()
            .contains("at least one new attempt")
    );
}

// =========================================================================
// Reimport regressions
// =========================================================================

#[silo::test]
async fn reimport_terminal_releases_holders_for_check_rate_limit_task() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "reimp-check-rate-limit-holder";
    let queue = "reimp-check-rate-limit-q";

    let mut initial = base_import_params(job_id);
    initial.retry_policy = Some(default_retry_policy());
    initial.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 10,
    })];
    let r = shard.import_jobs("-", vec![initial]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);

    let (task_key, task_raw) = test_helpers::first_task_kv(shard.db())
        .await
        .expect("expected scheduled task");
    let task = decode_task(&task_raw).expect("decode task");
    let held_queues = match task {
        Task::RunAttempt {
            id,
            tenant,
            job_id: task_job_id,
            attempt_number,
            relative_attempt_number,
            held_queues,
            task_group,
        } => {
            let synthetic = Task::CheckRateLimit {
                task_id: id,
                tenant,
                job_id: task_job_id,
                attempt_number,
                relative_attempt_number,
                limit_index: 1,
                rate_limit: GubernatorRateLimitData {
                    name: "api-limit".to_string(),
                    unique_key: format!("reimport-{}", uuid::Uuid::new_v4()),
                    limit: 100,
                    duration_ms: 60_000,
                    hits: 1,
                    algorithm: GubernatorAlgorithm::TokenBucket.as_u8(),
                    behavior: 0,
                    retry_initial_backoff_ms: 10,
                    retry_max_backoff_ms: 1000,
                    retry_backoff_multiplier: 2.0,
                    retry_max_retries: 10,
                },
                retry_count: 0,
                started_at_ms: test_helpers::now_ms(),
                priority: 50,
                held_queues: held_queues.clone(),
                task_group,
            };
            let mut batch = WriteBatch::new();
            batch.put(&task_key, &encode_task(&synthetic));
            shard
                .db()
                .write(batch)
                .await
                .expect("replace RunAttempt with CheckRateLimit");
            held_queues
        }
        other => panic!("expected RunAttempt task before replacement, got {other:?}"),
    };
    assert!(
        !held_queues.is_empty(),
        "synthetic CheckRateLimit should carry held queues"
    );
    assert_eq!(
        test_helpers::count_concurrency_holders(shard.db()).await,
        1,
        "import should hold one queue before reimport cleanup"
    );

    let mut reimport = base_import_params(job_id);
    reimport.retry_policy = Some(default_retry_policy());
    reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 10,
    })];
    reimport.attempts = vec![succeeded_attempt(1_700_000_001_000)];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);
    assert_eq!(r[0].status, JobStatusKind::Succeeded);

    assert_eq!(
        test_helpers::count_concurrency_holders(shard.db()).await,
        0,
        "terminal reimport should release holders from CheckRateLimit task"
    );
}

#[silo::test]
async fn reimport_updates_retry_policy_used_for_future_failures() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let job_id = "reimp-retry-policy-persisted";

    let mut initial = base_import_params(job_id);
    initial.retry_policy = None;
    initial.attempts = vec![failed_attempt(1_700_000_001_000)];
    let r = shard.import_jobs("-", vec![initial]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Failed);

    let updated_policy = RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 10,
        max_interval_ms: 100,
        randomize_interval: false,
        backoff_factor: 2.0,
    };
    let mut reimport = base_import_params(job_id);
    reimport.retry_policy = Some(updated_policy.clone());
    reimport.attempts = vec![
        failed_attempt(1_700_000_001_000),
        failed_attempt(1_700_000_002_000),
    ];
    let r = shard.import_jobs("-", vec![reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);

    let view = shard.get_job("-", job_id).await.unwrap().unwrap();
    let persisted = view
        .retry_policy()
        .expect("reimport should persist new retry policy");
    assert_eq!(persisted.retry_count, updated_policy.retry_count);
    assert_eq!(
        persisted.initial_interval_ms,
        updated_policy.initial_interval_ms
    );
    assert_eq!(persisted.max_interval_ms, updated_policy.max_interval_ms);
    assert_eq!(persisted.backoff_factor, updated_policy.backoff_factor);

    let dequeued = shard.dequeue("worker-1", "default", 1).await.unwrap();
    assert_eq!(dequeued.tasks.len(), 1);
    let task_id = dequeued.tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "RETRY_ME".to_string(),
                error: vec![7, 7, 7],
            },
        )
        .await
        .unwrap();

    let status = shard.get_job_status("-", job_id).await.unwrap().unwrap();
    assert_eq!(
        status.kind,
        JobStatusKind::Scheduled,
        "next failure after reimport should still schedule retries from new policy"
    );
}

#[silo::test]
async fn reimport_releasing_holder_triggers_immediate_grant_scan() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;
    let queue = "reimp-release-triggers-grant-q";

    let mut holder_job = base_import_params("reimp-grant-holder");
    holder_job.retry_policy = Some(default_retry_policy());
    holder_job.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 1,
    })];
    let r = shard.import_jobs("-", vec![holder_job]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);
    assert_eq!(
        test_helpers::count_concurrency_holders(shard.db()).await,
        1,
        "first job should hold the only queue slot"
    );

    let mut waiting_job = base_import_params("reimp-grant-waiting");
    waiting_job.retry_policy = Some(default_retry_policy());
    waiting_job.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 1,
    })];
    let r = shard.import_jobs("-", vec![waiting_job]).await.unwrap();
    assert!(r[0].success);
    assert_eq!(r[0].status, JobStatusKind::Scheduled);
    assert_eq!(
        test_helpers::count_concurrency_requests(shard.db()).await,
        1,
        "second job should queue a pending request"
    );

    let mut holder_reimport = base_import_params("reimp-grant-holder");
    holder_reimport.retry_policy = Some(default_retry_policy());
    holder_reimport.limits = vec![Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 1,
    })];
    holder_reimport.attempts = vec![succeeded_attempt(1_700_000_003_000)];
    let r = shard.import_jobs("-", vec![holder_reimport]).await.unwrap();
    assert!(r[0].success, "reimport failed: {:?}", r[0].error);
    assert_eq!(r[0].status, JobStatusKind::Succeeded);

    let mut holders = 0usize;
    for _ in 0..20 {
        holders = test_helpers::count_concurrency_holders(shard.db()).await;
        if holders == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        holders, 1,
        "released holder should trigger immediate grant to pending request"
    );

    let mut requests_left = usize::MAX;
    for _ in 0..20 {
        requests_left = test_helpers::count_concurrency_requests(shard.db()).await;
        if requests_left == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        requests_left, 0,
        "pending request should be consumed after immediate grant"
    );
}
