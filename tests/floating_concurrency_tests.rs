//! Tests for floating concurrency limits.
//!
//! Floating concurrency limits are dynamic concurrency queues where the max concurrency
//! is refreshed periodically by workers. This allows for adaptive rate limiting based on
//! external factors like API quotas or system load.

mod test_helpers;

use silo::codec::{decode_lease, encode_lease};
use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::task::{LeaseRecord, Task};

use test_helpers::*;

/// Tests that jobs from different task groups can share the same floating concurrency queue.
/// Task groups are for routing work to different workers, but floating limits should work
/// across all task groups using the same queue key.
#[silo::test]
async fn floating_limit_shared_across_task_groups() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-shared-tg-q".to_string();
    let refresh_interval_ms = 60_000i64; // long interval to avoid refresh during test

    // Job A in task_group "alpha" takes the only slot (max_concurrency=1)
    let j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"from": "alpha"})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "alpha",
        )
        .await
        .expect("enqueue alpha");

    // Dequeue from alpha - should get the job
    let result1 = shard.dequeue("w1", "alpha", 1).await.expect("deq alpha");
    assert_eq!(result1.tasks.len(), 1);
    assert_eq!(result1.tasks[0].job().id(), j1);
    let t1 = result1.tasks[0].attempt().task_id().to_string();

    // Job B in task_group "beta" should queue (same floating limit key, slot is taken)
    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"from": "beta"})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "beta",
        )
        .await
        .expect("enqueue beta");

    // Verify j2 is scheduled but waiting
    let j2_status = shard
        .get_job_status("-", &j2)
        .await
        .expect("get j2 status")
        .expect("j2 exists");
    assert_eq!(j2_status.kind, JobStatusKind::Scheduled);

    // Dequeue from beta - should get nothing (waiting for concurrency)
    let result2 = shard.dequeue("w2", "beta", 1).await.expect("deq beta");
    assert_eq!(
        result2.tasks.len(),
        0,
        "beta job should be waiting for concurrency"
    );

    // Complete alpha job
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report alpha success");

    // Now dequeue from beta - should get the job
    let result3 = shard
        .dequeue("w2", "beta", 1)
        .await
        .expect("deq beta after");
    assert_eq!(result3.tasks.len(), 1, "beta job should now be runnable");
    assert_eq!(result3.tasks[0].job().id(), j2);
}

/// Tests that floating concurrency state is shared correctly when jobs from different
/// task groups use the same queue key.
#[silo::test]
async fn floating_limit_state_shared_across_task_groups() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-state-shared-q".to_string();
    let refresh_interval_ms = 60_000i64;

    // Enqueue from task_group "alpha"
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"from": "alpha"})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 5,
                    refresh_interval_ms,
                    metadata: vec![("source".to_string(), "alpha".to_string())],
                },
            )],
            None,
            "alpha",
        )
        .await
        .expect("enqueue alpha");

    // Check floating limit state was created
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    assert_eq!(decoded.archived().current_max_concurrency, 5);

    // Enqueue from task_group "beta" with the same queue key
    // The floating limit state should already exist and be reused
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"from": "beta"})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 5,
                    refresh_interval_ms,
                    metadata: vec![("source".to_string(), "beta".to_string())],
                },
            )],
            None,
            "beta",
        )
        .await
        .expect("enqueue beta");

    // State should still have the same max_concurrency
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    assert_eq!(
        decoded.archived().current_max_concurrency,
        5,
        "state should be reused, not recreated"
    );
}

/// Tests that floating limit refresh tasks can be triggered by jobs from any task group
/// and the updated limit applies to all task groups.
#[silo::test]
async fn floating_limit_refresh_applies_to_all_task_groups() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-refresh-all-tg-q".to_string();
    let refresh_interval_ms = 100i64;

    // Create the floating limit with max_concurrency=1 via alpha task_group
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "alpha",
        )
        .await
        .expect("enqueue alpha");

    // Advance time to make the limit stale
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue from beta task_group - should trigger refresh scheduling
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "beta",
        )
        .await
        .expect("enqueue beta");

    // Dequeue from alpha - should get job and refresh task
    let result1 = shard
        .dequeue("w1", "alpha", 10)
        .await
        .expect("dequeue alpha");
    // Note: refresh tasks are returned regardless of task_group since they're system tasks

    // Dequeue from beta
    let result2 = shard.dequeue("w2", "beta", 10).await.expect("dequeue beta");

    // One of them should have the refresh task
    let has_refresh = !result1.refresh_tasks.is_empty() || !result2.refresh_tasks.is_empty();
    assert!(has_refresh, "refresh task should be available");

    // Get the refresh task and complete it with a higher limit
    let refresh_task_id = if !result1.refresh_tasks.is_empty() {
        result1.refresh_tasks[0].task_id.clone()
    } else {
        result2.refresh_tasks[0].task_id.clone()
    };

    // Report successful refresh with increased concurrency
    shard
        .report_refresh_success(&refresh_task_id, 3) // Increase from 1 to 3
        .await
        .expect("report refresh success");

    // Verify the state was updated
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    assert_eq!(
        decoded.archived().current_max_concurrency,
        3,
        "max_concurrency should be updated to 3"
    );
}

/// Tests that multiple jobs from different task groups properly queue for a shared
/// floating concurrency limit.
#[silo::test]
async fn floating_limit_queues_jobs_from_multiple_task_groups() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-multi-tg-q".to_string();
    let refresh_interval_ms = 60_000i64;

    // Enqueue 3 jobs from different task groups with max_concurrency=1
    let j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"order": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "group-x",
        )
        .await
        .expect("enqueue x");

    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"order": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "group-y",
        )
        .await
        .expect("enqueue y");

    let j3 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"order": 3})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "group-z",
        )
        .await
        .expect("enqueue z");

    // Process all jobs by cycling through task groups
    let mut processed_jobs: Vec<String> = Vec::new();
    let task_groups = ["group-x", "group-y", "group-z"];

    while processed_jobs.len() < 3 {
        for tg in &task_groups {
            let result = shard.dequeue("worker", tg, 1).await.expect("dequeue");
            for task in &result.tasks {
                let tid = task.attempt().task_id().to_string();
                let job_id = task.job().id().to_string();
                shard
                    .report_attempt_outcome(&tid, AttemptOutcome::Success { result: vec![] })
                    .await
                    .expect("report success");
                processed_jobs.push(job_id);
            }
        }
    }

    assert_eq!(processed_jobs.len(), 3, "all 3 jobs should be processed");
    assert!(processed_jobs.contains(&j1));
    assert!(processed_jobs.contains(&j2));
    assert!(processed_jobs.contains(&j3));

    // No holders should remain
    assert_eq!(count_concurrency_holders(shard.db()).await, 0);
}

#[silo::test]
async fn floating_concurrency_limit_creates_state_on_enqueue() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-test-q".to_string();
    let refresh_interval_ms = 60_000i64; // 60 seconds

    // Enqueue a job with a floating concurrency limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"test": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 5,
                    refresh_interval_ms,
                    metadata: vec![("env".to_string(), "test".to_string())],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue with floating limit");

    // Check that the floating limit state was created
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");

    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    let archived = decoded.archived();

    assert_eq!(archived.current_max_concurrency, 5);
    assert_eq!(archived.default_max_concurrency, 5);
    assert_eq!(archived.refresh_interval_ms, refresh_interval_ms);
    assert!(archived.last_refreshed_at_ms <= now);
    assert_eq!(archived.retry_count, 0);

    // Verify job is scheduled
    let status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status")
        .expect("exists");
    assert_eq!(status.kind, JobStatusKind::Scheduled);
}

#[silo::test]
async fn floating_concurrency_limit_schedules_refresh_when_stale() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-refresh-q".to_string();
    let refresh_interval_ms = 1000i64; // 1 second

    // Enqueue first job - should not need refresh yet (just created)
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Advance time past the refresh interval
    tokio::time::advance(std::time::Duration::from_millis(2000)).await;
    let now_advanced = now_ms();

    // Enqueue second job - should trigger refresh task scheduling
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_advanced,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Check that a refresh task was scheduled
    let tasks = shard.peek_tasks("default", 50).await.expect("peek tasks");
    let has_refresh_task = tasks.iter().any(|t| {
        matches!(
            t,
            Task::RefreshFloatingLimit { queue_key, .. } if queue_key == &queue
        )
    });
    assert!(has_refresh_task, "refresh task should be scheduled");
}

#[silo::test]
async fn floating_concurrency_limit_schedules_refresh_when_stale_with_waiters_above_one() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-refresh-waiters-gt1-q".to_string();
    let refresh_interval_ms = 100i64;
    let default_max = 3u32;

    // Fill the available slots (no waiters yet)
    for i in 1..=default_max {
        let _ = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": i})),
                vec![silo::job::Limit::FloatingConcurrency(
                    silo::job::FloatingConcurrencyLimit {
                        key: queue.clone(),
                        default_max_concurrency: default_max,
                        refresh_interval_ms,
                        metadata: vec![],
                    },
                )],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue one more to create a waiter and trigger refresh scheduling
    let _j4 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": "waiter"})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: default_max,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue waiter");

    assert!(
        first_concurrency_request_kv(shard.db()).await.is_some(),
        "expected a waiting request after exceeding max"
    );

    let tasks = shard.peek_tasks("default", 50).await.expect("peek tasks");
    let has_refresh_task = tasks.iter().any(|t| {
        matches!(
            t,
            Task::RefreshFloatingLimit { queue_key, .. } if queue_key == &queue
        )
    });
    assert!(
        has_refresh_task,
        "refresh task should be scheduled when waiters exist"
    );
}

#[silo::test]
async fn floating_concurrency_limit_skips_refresh_without_waiters() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-no-waiters-refresh-q".to_string();
    let refresh_interval_ms = 100i64; // 100ms

    // Enqueue first job - should be granted immediately
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 5,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Advance time past the refresh interval
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue another job - still no waiters (max=5)
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 5,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    assert!(
        first_concurrency_request_kv(shard.db()).await.is_none(),
        "no waiting requests expected"
    );

    let tasks = shard.peek_tasks("default", 50).await.expect("peek tasks");
    let has_refresh_task = tasks.iter().any(|t| {
        matches!(
            t,
            Task::RefreshFloatingLimit { queue_key, .. } if queue_key == &queue
        )
    });
    assert!(
        !has_refresh_task,
        "refresh task should not be scheduled without waiters"
    );
}

#[silo::test]
async fn floating_limit_allows_multiple_concurrent_grants() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-multi-grant-q".to_string();
    let refresh_interval_ms = 60_000i64;

    let j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 2,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 2,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    assert!(
        first_concurrency_request_kv(shard.db()).await.is_none(),
        "no waiters expected at max=2"
    );

    let tasks = shard.peek_tasks("default", 10).await.expect("peek tasks");
    let run_count = tasks
        .iter()
        .filter(|t| matches!(t, Task::RunAttempt { job_id, .. } if job_id == &j1 || job_id == &j2))
        .count();
    assert_eq!(run_count, 2, "both jobs should be runnable");
}

#[silo::test]
async fn floating_concurrency_limit_dequeue_returns_refresh_tasks() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-deq-refresh-q".to_string();
    let refresh_interval_ms = 100i64;

    // Enqueue a job to create the floating limit
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![("key".to_string(), "value".to_string())],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Advance time past refresh interval
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue another job to trigger refresh scheduling
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![("key".to_string(), "value".to_string())],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Dequeue should return the refresh task
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");

    // Should have job tasks and refresh tasks
    assert!(
        !result.refresh_tasks.is_empty(),
        "should have refresh tasks"
    );

    let refresh = &result.refresh_tasks[0];
    assert_eq!(refresh.queue_key, queue);
    assert_eq!(refresh.current_max_concurrency, 1);
    assert_eq!(refresh.metadata.len(), 1);
    assert_eq!(
        refresh.metadata[0],
        ("key".to_string(), "value".to_string())
    );
}

#[silo::test]
async fn floating_limit_refresh_success_updates_state() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-success-q".to_string();
    let refresh_interval_ms = 100i64;

    // Enqueue a job to create the floating limit
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Advance time past refresh interval
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue another to schedule refresh
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Dequeue to get the refresh task
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    assert!(!result.refresh_tasks.is_empty());
    let task_id = result.refresh_tasks[0].task_id.clone();
    let new_max = 10u32;

    // Report successful refresh
    shard
        .report_refresh_success(&task_id, new_max)
        .await
        .expect("report refresh success");

    // Check that the state was updated
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");

    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    let archived = decoded.archived();

    assert_eq!(
        archived.current_max_concurrency, new_max,
        "max concurrency should be updated"
    );
    // last_refreshed_at_ms should be updated to a recent time
    assert!(
        archived.last_refreshed_at_ms > now,
        "last_refreshed_at_ms should be updated"
    );
    assert!(!archived.refresh_task_scheduled);
    assert_eq!(archived.retry_count, 0);
}

#[silo::test]
async fn floating_limit_refresh_failure_triggers_backoff() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-failure-q".to_string();
    let refresh_interval_ms = 100i64;

    // Enqueue a job to create the floating limit
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Advance time past refresh interval
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue another to schedule refresh
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Dequeue to get the refresh task
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    assert!(!result.refresh_tasks.is_empty());
    let task_id = result.refresh_tasks[0].task_id.clone();

    // Report failed refresh
    shard
        .report_refresh_failure(&task_id, "test_error", "simulated failure")
        .await
        .expect("report refresh failure");

    // Check that the state has backoff info
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");

    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    let archived = decoded.archived();

    // Should still have old concurrency value
    assert_eq!(archived.current_max_concurrency, 1);
    // Retry count should be incremented
    assert_eq!(archived.retry_count, 1);
    // Next retry time should be set (with backoff)
    let next_retry_at = archived
        .next_retry_at_ms
        .as_ref()
        .copied()
        .expect("next_retry_at_ms should be set");
    assert!(
        next_retry_at > now_ms(),
        "retry should be in the future (backoff)"
    );
    // A new refresh task should be scheduled
    assert!(archived.refresh_task_scheduled);

    // Verify a task key exists with the floating_refresh prefix in the DB
    // (checking peek_tasks would require advancing time past backoff)
    let has_retry_task = first_task_kv(shard.db()).await.is_some();
    assert!(has_retry_task, "retry task should exist in db");
}

#[silo::test]
async fn floating_limit_refresh_failure_skips_retry_without_waiters() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-failure-no-waiters-q".to_string();
    let refresh_interval_ms = 100i64;

    // Enqueue first job to take the only slot
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Make limit stale, enqueue another to create a waiter and schedule refresh
    tokio::time::advance(std::time::Duration::from_millis(200)).await;
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    assert!(
        !result.refresh_tasks.is_empty(),
        "refresh task should be leased"
    );
    assert!(!result.tasks.is_empty(), "run task should be leased");

    let refresh_task_id = result.refresh_tasks[0].task_id.clone();
    let run_task_id = result.tasks[0].attempt().task_id().to_string();

    // Complete the run task to clear waiting requests
    shard
        .report_attempt_outcome(&run_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
    assert!(
        first_concurrency_request_kv(shard.db()).await.is_none(),
        "waiting requests should be cleared"
    );

    // Report failed refresh - no retry should be scheduled without waiters
    shard
        .report_refresh_failure(&refresh_task_id, "test_error", "simulated failure")
        .await
        .expect("report refresh failure");

    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode state");
    let archived = decoded.archived();

    assert!(
        !archived.refresh_task_scheduled,
        "retry should not be scheduled without waiters"
    );
    assert_eq!(archived.retry_count, 1);
    assert!(archived.next_retry_at_ms.is_some());
}

#[silo::test]
async fn floating_limit_concurrent_enqueues_no_duplicate_refresh() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-concurrent-q".to_string();
    let refresh_interval_ms = 100i64;

    // First enqueue creates the limit state
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Advance time to make the limit stale
    tokio::time::advance(std::time::Duration::from_millis(200)).await;
    let now_stale = now_ms();

    // Enqueue multiple jobs with the same floating limit at once
    // Each should see the limit is stale, but only one should schedule refresh
    for i in 2..=5 {
        let _ = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_stale,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": i})),
                vec![silo::job::Limit::FloatingConcurrency(
                    silo::job::FloatingConcurrencyLimit {
                        key: queue.clone(),
                        default_max_concurrency: 1,
                        refresh_interval_ms,
                        metadata: vec![],
                    },
                )],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    // Count refresh tasks
    let tasks = shard.peek_tasks("default", 100).await.expect("peek tasks");
    let refresh_count = tasks
        .iter()
        .filter(|t| {
            matches!(
                t,
                Task::RefreshFloatingLimit { queue_key, .. } if queue_key == &queue
            )
        })
        .count();

    assert_eq!(
        refresh_count, 1,
        "should have exactly one refresh task, not duplicates"
    );
}

#[silo::test]
async fn floating_limit_uses_dynamic_max_concurrency() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-dynamic-q".to_string();
    let refresh_interval_ms = 60_000i64; // long interval to avoid refresh

    // First enqueue creates limit with max_concurrency=1
    let j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Second job should queue (max=1, 1 is holding)
    let j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Both should be scheduled, but j2 waiting for concurrency
    let j1_status = shard
        .get_job_status("-", &j1)
        .await
        .expect("get j1 status")
        .expect("j1 exists");
    assert_eq!(j1_status.kind, JobStatusKind::Scheduled);

    let j2_status = shard
        .get_job_status("-", &j2)
        .await
        .expect("get j2 status")
        .expect("j2 exists");
    assert_eq!(j2_status.kind, JobStatusKind::Scheduled);

    // Dequeue j1
    let result = shard
        .dequeue("worker-1", "default", 1)
        .await
        .expect("dequeue");
    assert_eq!(result.tasks.len(), 1);
    let t1_id = result.tasks[0].attempt().task_id().to_string();

    // Complete j1 - j2 should get a ticket
    shard
        .report_attempt_outcome(&t1_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report j1 success");

    // j2 should now have a RunAttempt task
    let tasks = shard.peek_tasks("default", 10).await.expect("peek tasks");
    let has_j2_run = tasks.iter().any(|t| {
        matches!(
            t,
            Task::RunAttempt { job_id, .. } if job_id == &j2
        )
    });
    assert!(
        has_j2_run,
        "j2 should have RunAttempt task after j1 completes"
    );
}

#[silo::test]
async fn floating_limit_job_persists_limit_type() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-persist-q".to_string();

    // Enqueue with floating limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"test": true})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 10,
                    refresh_interval_ms: 5000,
                    metadata: vec![
                        ("org_id".to_string(), "123".to_string()),
                        ("env".to_string(), "prod".to_string()),
                    ],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Retrieve job and check limits
    let view = shard
        .get_job("-", &job_id)
        .await
        .expect("get_job")
        .expect("job exists");

    let limits = view.limits();
    assert_eq!(limits.len(), 1);

    match &limits[0] {
        silo::job::Limit::FloatingConcurrency(fl) => {
            assert_eq!(fl.key, queue);
            assert_eq!(fl.default_max_concurrency, 10);
            assert_eq!(fl.refresh_interval_ms, 5000);
            assert_eq!(fl.metadata.len(), 2);
            assert!(
                fl.metadata
                    .contains(&("org_id".to_string(), "123".to_string()))
            );
            assert!(
                fl.metadata
                    .contains(&("env".to_string(), "prod".to_string()))
            );
        }
        _ => panic!("expected FloatingConcurrency limit"),
    }
}

#[silo::test]
async fn floating_limit_multiple_retries_increase_backoff() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-backoff-q".to_string();
    let refresh_interval_ms = 100i64;

    // Create the limit
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Make limit stale
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Trigger refresh scheduling
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // First failure
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    assert!(!result.refresh_tasks.is_empty());
    let task_id = result.refresh_tasks[0].task_id.clone();

    shard
        .report_refresh_failure(&task_id, "test_error", "simulated failure 1")
        .await
        .expect("report failure 1");

    // Check retry count is 1
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("get")
        .expect("state");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
    let first_retry = decoded.archived().next_retry_at_ms.unwrap_or(0);
    assert_eq!(decoded.archived().retry_count, 1);

    // Advance time to make retry task available
    tokio::time::advance(std::time::Duration::from_millis(10_000)).await;

    // Second failure
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue 2");
    if !result.refresh_tasks.is_empty() {
        let task_id = result.refresh_tasks[0].task_id.clone();
        shard
            .report_refresh_failure(&task_id, "test_error", "simulated failure 2")
            .await
            .expect("report failure 2");

        // Check retry count is 2 and backoff increased
        let state_raw = shard
            .db()
            .get(&state_key)
            .await
            .expect("get")
            .expect("state");
        let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
        let second_retry = decoded.archived().next_retry_at_ms.unwrap_or(0);
        assert_eq!(decoded.archived().retry_count, 2);
        // Backoff should be longer for the second retry
        assert!(
            second_retry > first_retry,
            "second retry delay should be longer"
        );
    }
}

#[silo::test]
async fn floating_limit_successful_refresh_resets_backoff() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-reset-q".to_string();
    let refresh_interval_ms = 100i64;

    // Create the limit
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Make limit stale
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Trigger refresh
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // First failure to set retry_count
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    let task_id = result.refresh_tasks[0].task_id.clone();
    shard
        .report_refresh_failure(&task_id, "test_error", "simulated failure")
        .await
        .expect("report failure");

    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("get")
        .expect("state");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
    assert_eq!(decoded.archived().retry_count, 1);

    // Advance time and succeed
    tokio::time::advance(std::time::Duration::from_millis(10_000)).await;

    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue 2");
    if !result.refresh_tasks.is_empty() {
        let task_id = result.refresh_tasks[0].task_id.clone();
        shard
            .report_refresh_success(&task_id, 8)
            .await
            .expect("report success");

        // Check retry_count is reset and new value is stored
        let state_raw = shard
            .db()
            .get(&state_key)
            .await
            .expect("get")
            .expect("state");
        let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
        assert_eq!(decoded.archived().retry_count, 0);
        assert_eq!(decoded.archived().current_max_concurrency, 8);
        assert!(decoded.archived().next_retry_at_ms.is_none());
    }
}

#[silo::test]
async fn floating_limit_refresh_task_lease_expiry_allows_rescheduling() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-lease-expiry-q".to_string();
    let refresh_interval_ms = 100i64;

    // Create the floating limit
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Make limit stale to trigger refresh scheduling
    tokio::time::advance(std::time::Duration::from_millis(200)).await;

    // Enqueue another job to schedule refresh
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Dequeue to get the refresh task - this creates a lease
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    assert!(
        !result.refresh_tasks.is_empty(),
        "should have refresh task after dequeue"
    );
    let task_id = result.refresh_tasks[0].task_id.clone();

    // Verify the lease was created
    let lease_key = silo::keys::leased_task_key(&task_id);
    let lease_raw = shard
        .db()
        .get(&lease_key)
        .await
        .expect("db get")
        .expect("lease should exist after dequeue");

    // Verify state shows refresh_task_scheduled = true
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should exist");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
    assert!(
        decoded.archived().refresh_task_scheduled,
        "refresh_task_scheduled should be true after dequeue"
    );

    // Manually mutate the lease to make it expired (this is how other tests handle lease expiry)
    let decoded_lease = decode_lease(&lease_raw).expect("decode lease");
    let expired_ms = now_ms() - 1; // Set expiry to the past
    let new_record = LeaseRecord {
        worker_id: decoded_lease.worker_id().to_string(),
        task: decoded_lease.to_task(),
        expiry_ms: expired_ms,
    };
    let new_val = encode_lease(&new_record).expect("encode lease");
    shard
        .db()
        .put(&lease_key, &new_val)
        .await
        .expect("put mutated lease");
    shard.db().flush().await.expect("flush mutated lease");

    // Reap expired leases - this should handle the expired refresh task
    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 1, "should have reaped exactly one lease");

    // Verify the lease was deleted
    let lease_exists_after = shard.db().get(&lease_key).await.expect("db get").is_some();
    assert!(!lease_exists_after, "lease should be deleted after reaping");

    // Verify state was reset to allow new refresh scheduling
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("db get")
        .expect("state should still exist");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
    assert!(
        !decoded.archived().refresh_task_scheduled,
        "refresh_task_scheduled should be false after reaping"
    );

    // Verify that a new refresh can now be scheduled on the next enqueue
    // (limit should still be stale since we never completed the refresh)
    let _j3 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j3");

    // Check that a new refresh task was scheduled
    let tasks = shard.peek_tasks("default", 50).await.expect("peek tasks");
    let refresh_count = tasks
        .iter()
        .filter(|t| {
            matches!(
                t,
                Task::RefreshFloatingLimit { queue_key, .. } if queue_key == &queue
            )
        })
        .count();
    assert_eq!(
        refresh_count, 1,
        "new refresh task should be scheduled after lease expiry cleanup"
    );
}

#[silo::test]
async fn floating_limit_refresh_task_lease_expiry_preserves_state() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "fl-lease-preserve-q".to_string();
    let refresh_interval_ms = 12345i64; // specific value to verify preservation

    // Create the floating limit with specific values to verify preservation
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1, // specific value
                    refresh_interval_ms,
                    metadata: vec![
                        ("org".to_string(), "test-org".to_string()),
                        ("env".to_string(), "production".to_string()),
                    ],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j1");

    // Make limit stale
    tokio::time::advance(std::time::Duration::from_millis(20000)).await;

    // Trigger refresh
    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: 1,
                    refresh_interval_ms,
                    metadata: vec![
                        ("org".to_string(), "test-org".to_string()),
                        ("env".to_string(), "production".to_string()),
                    ],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue j2");

    // Dequeue to get the refresh task and create a lease
    let result = shard
        .dequeue("worker-1", "default", 10)
        .await
        .expect("dequeue");
    assert!(!result.refresh_tasks.is_empty(), "should have refresh task");
    let task_id = result.refresh_tasks[0].task_id.clone();

    // Verify state shows refresh_task_scheduled = true
    let state_key = silo::keys::floating_limit_state_key("-", &queue);
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("get")
        .expect("state");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");
    assert!(decoded.archived().refresh_task_scheduled);
    assert_eq!(decoded.archived().current_max_concurrency, 1);
    assert_eq!(decoded.archived().default_max_concurrency, 1);
    assert_eq!(decoded.archived().refresh_interval_ms, refresh_interval_ms);
    assert_eq!(decoded.archived().metadata.len(), 2);

    // Mutate the lease to be expired (simulating worker crash)
    let lease_key = silo::keys::leased_task_key(&task_id);
    let lease_raw = shard
        .db()
        .get(&lease_key)
        .await
        .expect("db get")
        .expect("lease should exist");
    let decoded_lease = decode_lease(&lease_raw).expect("decode lease");
    let expired_ms = now_ms() - 1; // Set expiry to the past
    let new_record = LeaseRecord {
        worker_id: decoded_lease.worker_id().to_string(),
        task: decoded_lease.to_task(),
        expiry_ms: expired_ms,
    };
    let new_val = encode_lease(&new_record).expect("encode lease");
    shard
        .db()
        .put(&lease_key, &new_val)
        .await
        .expect("put mutated lease");
    shard.db().flush().await.expect("flush mutated lease");

    // Reap the expired lease
    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 1);

    // Verify all state fields were preserved except refresh_task_scheduled
    let state_raw = shard
        .db()
        .get(&state_key)
        .await
        .expect("get")
        .expect("state");
    let decoded = silo::codec::decode_floating_limit_state(&state_raw).expect("decode");

    assert!(
        !decoded.archived().refresh_task_scheduled,
        "refresh_task_scheduled should be reset"
    );
    assert_eq!(
        decoded.archived().current_max_concurrency,
        1,
        "current_max_concurrency should be preserved"
    );
    assert_eq!(
        decoded.archived().default_max_concurrency,
        1,
        "default_max_concurrency should be preserved"
    );
    assert_eq!(
        decoded.archived().refresh_interval_ms,
        refresh_interval_ms,
        "refresh_interval_ms should be preserved"
    );
    assert_eq!(
        decoded.archived().retry_count,
        0,
        "retry_count should be preserved (was 0)"
    );
    assert!(
        decoded.archived().next_retry_at_ms.is_none(),
        "next_retry_at_ms should be preserved (was None)"
    );
    // Verify metadata preserved
    assert_eq!(decoded.archived().metadata.len(), 2);
    let metadata: Vec<_> = decoded
        .archived()
        .metadata
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert!(metadata.contains(&("org", "test-org")));
    assert!(metadata.contains(&("env", "production")));

    // Verify the lease was deleted
    let lease_exists = shard.db().get(&lease_key).await.expect("db get").is_some();
    assert!(!lease_exists, "lease should be deleted after reaping");
}
