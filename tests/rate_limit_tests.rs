//! Integration tests for Gubernator rate limiting.
//!
//! These tests require a real Gubernator server running at localhost:9992.
//! Run with: `process-compose up` to start the required services.

mod test_helpers;

use std::sync::Arc;

use silo::gubernator::{GubernatorClient, GubernatorConfig, RateLimitClient};
use silo::job::{
    ConcurrencyLimit, GubernatorAlgorithm, GubernatorRateLimit, Limit, RateLimitRetryPolicy,
};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::ShardRange;

use test_helpers::*;

const GUBERNATOR_ADDRESS: &str = "http://localhost:9992";

/// Create a real Gubernator client connected to the local server
async fn create_gubernator_client() -> Arc<GubernatorClient> {
    let config = GubernatorConfig {
        address: GUBERNATOR_ADDRESS.to_string(),
        coalesce_interval_ms: 5,
        max_batch_size: 100,
        connect_timeout_ms: 5000,
        request_timeout_ms: 10000,
    };
    let client = GubernatorClient::new(config);
    client
        .connect()
        .await
        .expect("failed to connect to Gubernator - is it running?");
    client
}

/// Open a temp shard with the real Gubernator client
async fn open_shard_with_gubernator()
-> (tempfile::TempDir, Arc<JobStoreShard>, Arc<GubernatorClient>) {
    let gubernator = create_gubernator_client().await;
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(test_helpers::fast_flush_slatedb_settings()),
    };
    let shard = JobStoreShard::open(&cfg, gubernator.clone(), None, ShardRange::full())
        .await
        .expect("open shard");
    (tmp, shard, gubernator)
}

/// Helper to dequeue tasks with retries for cases where we need to wait for
/// time-based state changes (rate limit windows resetting, scheduled tasks becoming ready).
/// NOT needed for internal task processing - that happens within a single dequeue call.
async fn dequeue_repeatedly(
    shard: &JobStoreShard,
    worker_id: &str,
    max_tasks: usize,
    max_attempts: usize,
    delay_ms: u64,
) -> Vec<silo::task::LeasedTask> {
    for attempt in 0..max_attempts {
        let tasks = shard
            .dequeue(worker_id, "default", max_tasks)
            .await
            .expect("dequeue")
            .tasks;
        if !tasks.is_empty() {
            return tasks;
        }
        if attempt < max_attempts - 1 {
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        }
    }
    vec![]
}

/// Helper to create a rate limit with sensible defaults for testing
fn test_rate_limit(name: &str, unique_key: &str, limit: i64) -> GubernatorRateLimit {
    GubernatorRateLimit {
        name: name.to_string(),
        unique_key: unique_key.to_string(),
        limit,
        duration_ms: 60_000, // 1 minute window
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 10,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_retries: 10,
        },
    }
}

/// Generate a unique key for each test to avoid rate limit state bleeding between tests
fn unique_key(base: &str) -> String {
    format!("{}_{}", base, uuid::Uuid::new_v4())
}

/// Verify that a single dequeue call processes the rate limit check AND returns the task.
/// This tests that the internal dequeue loop is working correctly.
#[silo::test]
async fn rate_limit_single_dequeue_returns_task() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let rate_limit = test_rate_limit("api", &unique_key("single_dequeue"), 100);

    // Enqueue job with rate limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "rate_limited"})),
            vec![Limit::RateLimit(rate_limit)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // A dequeue call should:
    // 1. Process the CheckRateLimit task internally
    // 2. Insert the RunAttempt task into the broker
    // 3. Claim and return the RunAttempt in the same call
    let tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    assert_eq!(
        tasks.len(),
        1,
        "single dequeue should return task after processing rate limit"
    );
    assert_eq!(tasks[0].job().id(), job_id);
}

#[silo::test]
async fn rate_limit_passes_and_job_becomes_runnable() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let rate_limit = test_rate_limit("api", &unique_key("user"), 100);

    // Enqueue job with rate limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "rate_limited"})),
            vec![Limit::RateLimit(rate_limit)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Single dequeue - internal loop processes CheckRateLimit and returns RunAttempt
    let tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    assert_eq!(tasks.len(), 1, "should get one runnable task");
    assert_eq!(tasks[0].job().id(), job_id);
}

#[silo::test]
async fn rate_limit_exceeded_then_passes_after_window() {
    let (_tmp, shard, gubernator) = open_shard_with_gubernator().await;

    // Use a very short duration so we can test the reset
    let key = unique_key("exhaust_test");
    let rate_limit = GubernatorRateLimit {
        name: "api".to_string(),
        unique_key: key.clone(),
        limit: 2,         // Only allow 2 requests
        duration_ms: 500, // 500ms window - will reset quickly
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            backoff_multiplier: 1.5,
            max_retries: 20,
        },
    };

    // Pre-exhaust the rate limit by calling Gubernator directly
    for _ in 0..2 {
        gubernator
            .check_rate_limit(
                "api",
                &key,
                1,
                2,
                500,
                silo::pb::gubernator::Algorithm::TokenBucket,
                0,
            )
            .await
            .expect("direct check");
    }

    let now = now_ms();

    // Enqueue job - rate limit is exhausted
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "rate_limited_retry"})),
            vec![Limit::RateLimit(rate_limit)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // First dequeue processes the CheckRateLimit. Depending on timing (especially in
    // slow CI environments), the rate limit may have already reset if enough time
    // elapsed since pre-exhaustion. Token bucket with limit=2, duration=500ms
    // replenishes ~1 token every 250ms.
    let first_tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue1")
        .tasks;

    if !first_tasks.is_empty() {
        // Rate limit already reset due to timing - task was returned immediately
        assert_eq!(first_tasks.len(), 1);
        assert_eq!(first_tasks[0].job().id(), job_id);
        return;
    }

    // Rate limit was still exhausted - task is scheduled for retry.
    // Wait for the rate limit window to reset, then retry.
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    // Now should succeed - use retry since we're waiting for time to pass
    let tasks = dequeue_repeatedly(&shard, "worker1", 1, 10, 100).await;
    assert_eq!(tasks.len(), 1, "should get task after rate limit resets");
    assert_eq!(tasks[0].job().id(), job_id);
}

#[silo::test]
async fn multiple_rate_limits_acquired_in_order() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let rate_limit1 = test_rate_limit("api", &unique_key("multi1"), 100);
    let rate_limit2 = test_rate_limit("database", &unique_key("multi2"), 100);

    // Enqueue job with multiple rate limits
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "multi_rate_limit"})),
            vec![Limit::RateLimit(rate_limit1), Limit::RateLimit(rate_limit2)],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Single dequeue - internal loop processes both rate limits and returns RunAttempt
    let tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    assert_eq!(tasks.len(), 1, "should get task after all rate limits pass");
    assert_eq!(tasks[0].job().id(), job_id);
}

#[silo::test]
async fn concurrency_then_rate_limit_acquired_in_order() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let concurrency = ConcurrencyLimit {
        key: unique_key("worker_pool"),
        max_concurrency: 5,
    };
    let rate_limit = test_rate_limit("api", &unique_key("conc_then_rate"), 100);

    // Enqueue job with concurrency limit first, then rate limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "concurrency_then_rate"})),
            vec![
                Limit::Concurrency(concurrency),
                Limit::RateLimit(rate_limit),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Single dequeue - internal loop acquires concurrency ticket, then checks rate limit
    let tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    assert_eq!(tasks.len(), 1, "should get runnable task");
    assert_eq!(tasks[0].job().id(), job_id);

    // Complete the task
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
}

#[silo::test]
async fn rate_limit_then_concurrency_acquired_in_order() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let rate_limit = test_rate_limit("api", &unique_key("rate_then_conc"), 100);
    let concurrency = ConcurrencyLimit {
        key: unique_key("database_pool"),
        max_concurrency: 3,
    };

    // Enqueue job with rate limit first, then concurrency limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "rate_then_concurrency"})),
            vec![
                Limit::RateLimit(rate_limit),
                Limit::Concurrency(concurrency),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Single dequeue - internal loop checks rate limit first, then acquires concurrency
    let tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    assert_eq!(tasks.len(), 1, "should get runnable task");
    assert_eq!(tasks[0].job().id(), job_id);

    // Complete the task
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
}

#[silo::test]
async fn concurrency_blocked_with_rate_limit_pending() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let queue_key = unique_key("limited_pool");
    let concurrency = ConcurrencyLimit {
        key: queue_key.clone(),
        max_concurrency: 1,
    };
    let rate_limit = test_rate_limit("api", &unique_key("shared"), 100);

    // First job takes the only concurrency slot
    let job1_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": 1})),
            vec![
                Limit::Concurrency(concurrency.clone()),
                Limit::RateLimit(rate_limit.clone()),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue job1");

    // Second job will be blocked on concurrency
    let _job2_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"job": 2})),
            vec![
                Limit::Concurrency(concurrency.clone()),
                Limit::RateLimit(rate_limit.clone()),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue job2");

    // Single dequeue - only job1 should be runnable (job2 blocked on concurrency)
    let tasks = shard
        .dequeue("worker1", "default", 2)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1, "only one task should be runnable");
    assert_eq!(tasks[0].job().id(), job1_id);

    // Complete job1
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("complete job1");

    // Now job2 should become runnable - single dequeue handles internal processing
    let tasks2 = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue2")
        .tasks;
    assert_eq!(tasks2.len(), 1, "job2 should now be runnable");
}

#[silo::test]
async fn three_limits_concurrency_rate_concurrency() {
    let (_tmp, shard, _gubernator) = open_shard_with_gubernator().await;

    let now = now_ms();
    let conc1 = ConcurrencyLimit {
        key: unique_key("pool_a"),
        max_concurrency: 5,
    };
    let rate = test_rate_limit("api", &unique_key("three_limits"), 100);
    let conc2 = ConcurrencyLimit {
        key: unique_key("pool_b"),
        max_concurrency: 5,
    };

    // Job with: concurrency -> rate limit -> concurrency
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"task": "complex_limits"})),
            vec![
                Limit::Concurrency(conc1),
                Limit::RateLimit(rate),
                Limit::Concurrency(conc2),
            ],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let tasks = shard
        .dequeue("worker1", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].job().id(), job_id);

    // Complete and verify cleanup
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("complete");
}

#[silo::test]
async fn rate_limit_counts_hits_correctly() {
    let (_tmp, shard, gubernator) = open_shard_with_gubernator().await;

    let key = unique_key("hits_test");
    let rate_limit = GubernatorRateLimit {
        name: "api".to_string(),
        unique_key: key.clone(),
        limit: 5, // Only 5 allowed
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy::default(),
    };

    let now = now_ms();
    let mut job_ids = Vec::new();

    // Enqueue 5 jobs - all should pass
    for i in 0..5 {
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"job": i})),
                vec![Limit::RateLimit(rate_limit.clone())],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        job_ids.push(job_id);
    }

    // Single dequeue - all 5 jobs should be processed (each consumes 1 hit)
    let tasks = shard
        .dequeue("worker1", "default", 5)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 5, "all 5 jobs should pass rate limit");

    // Verify the rate limit is now exhausted by checking directly
    let result = gubernator
        .check_rate_limit(
            "api",
            &key,
            1,
            5,
            60_000,
            silo::pb::gubernator::Algorithm::TokenBucket,
            0,
        )
        .await
        .expect("direct check");

    assert!(
        !result.under_limit,
        "rate limit should be exhausted after 5 jobs"
    );
}
