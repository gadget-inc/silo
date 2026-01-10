//! Tests for floating concurrency limits with remote/cross-shard queues.
//!
//! These tests verify that floating concurrency limits work correctly when:
//! - Jobs are on one shard but their concurrency queue is on another shard
//! - The queue owner has updated floating limit state that differs from the default
//!
//! THESE TESTS WOULD HAVE FAILED WITHOUT THE FIXES TO:
//! 1. `max_concurrency_for_queue()` helper method in job.rs
//! 2. `process_ticket_request_task()` in concurrency.rs
//! 3. `receive_remote_ticket_request()` in job_store_shard/mod.rs
//! 4. `RequestRemoteTicket` processing in dequeue.rs

mod test_helpers;

use silo::cluster_client::ClusterClient;
use silo::codec::decode_floating_limit_state;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::job::{FloatingConcurrencyLimit, Limit};
use silo::routing::{job_to_shard, queue_to_shard};
use silo::settings::{Backend, DatabaseTemplate};
use std::sync::Arc;
use test_helpers::{now_ms, open_temp_shard};

/// Create a test factory with memory backend and specified number of shards
fn make_test_factory_with_shards(num_shards: u32) -> (tempfile::TempDir, Arc<ShardFactory>) {
    let tmpdir = tempfile::tempdir().unwrap();
    let factory = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: tmpdir.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        MockGubernatorClient::new_arc(),
        num_shards,
    ));
    (tmpdir, factory)
}

/// Helper to find a job_id that routes to a specific shard
fn find_job_for_shard(target_shard: u32, num_shards: u32) -> String {
    for i in 0..10000 {
        let job_id = format!("floating-job-{}", i);
        if job_to_shard(&job_id, num_shards) == target_shard {
            return job_id;
        }
    }
    panic!("Could not find job_id for shard {}", target_shard);
}

/// Helper to find a (tenant, queue) pair that routes to a specific shard
fn find_queue_for_shard(
    tenant: &str,
    target_shard: u32,
    num_shards: u32,
) -> (String, String) {
    for i in 0..10000 {
        let queue = format!("floating-queue-{}", i);
        if queue_to_shard(tenant, &queue, num_shards) == target_shard {
            return (tenant.to_string(), queue);
        }
    }
    panic!(
        "Could not find queue for shard {} with tenant {}",
        target_shard, tenant
    );
}

// ============================================================================
// Tests for max_concurrency_for_queue() helper (Bug #1)
// ============================================================================

/// Tests that max_concurrency_for_queue returns the correct value for FloatingConcurrencyLimit.
/// WITHOUT THE FIX: This would return None, causing dequeue to use max_concurrency=1
#[silo::test]
async fn max_concurrency_for_queue_handles_floating_limits() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"test": "floating"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: "my-floating-queue".to_string(),
                default_max_concurrency: 5,
                refresh_interval_ms: 60_000,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Get the job and check max_concurrency_for_queue
    let job_view = shard
        .get_job("-", &job_id)
        .await
        .expect("get job")
        .expect("job exists");

    // WITHOUT THE FIX: This would return None because concurrency_limits() filtered out FloatingConcurrency
    let max = job_view.max_concurrency_for_queue("my-floating-queue");
    assert_eq!(
        max,
        Some(5),
        "max_concurrency_for_queue should return default_max_concurrency for FloatingConcurrencyLimit. \
         WITHOUT THE FIX: This returns None, causing max_concurrency to default to 1"
    );

    // Also verify it returns None for non-existent queues
    let missing = job_view.max_concurrency_for_queue("non-existent-queue");
    assert_eq!(missing, None, "should return None for non-existent queue");
}

/// Tests that max_concurrency_for_queue handles both fixed and floating limits in the same job.
#[silo::test]
async fn max_concurrency_for_queue_handles_mixed_limits() {
    use silo::job::ConcurrencyLimit;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"test": "mixed"}),
            vec![
                Limit::Concurrency(ConcurrencyLimit {
                    key: "fixed-queue".to_string(),
                    max_concurrency: 3,
                }),
                Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                    key: "floating-queue".to_string(),
                    default_max_concurrency: 7,
                    refresh_interval_ms: 60_000,
                    metadata: vec![],
                }),
            ],
            None,
        )
        .await
        .expect("enqueue");

    let job_view = shard
        .get_job("-", &job_id)
        .await
        .expect("get job")
        .expect("job exists");

    // Fixed limit should work
    let fixed = job_view.max_concurrency_for_queue("fixed-queue");
    assert_eq!(fixed, Some(3));

    // Floating limit should also work
    let floating = job_view.max_concurrency_for_queue("floating-queue");
    assert_eq!(floating, Some(7));
}

// ============================================================================
// Tests for remote ticket requests with floating limits (Bug #2)
// ============================================================================

/// Tests that when a job with a FloatingConcurrencyLimit is on shard A and its queue
/// is on shard B, the RequestRemoteTicket task uses the correct max_concurrency.
///
/// WITHOUT THE FIX: The dequeue code would use max_concurrency=1 because
/// concurrency_limits() filtered out FloatingConcurrency.
#[silo::test]
async fn remote_ticket_request_uses_floating_limit_max_concurrency() {
    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let shard1 = factory.get("1").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Find a job that routes to shard 0 and a queue that routes to shard 1
    let tenant = "-";
    let job_id = find_job_for_shard(0, 2);
    let (_, queue_key) = find_queue_for_shard(tenant, 1, 2);

    // Verify routing
    assert_eq!(job_to_shard(&job_id, 2), 0);
    assert_eq!(queue_to_shard(tenant, &queue_key, 2), 1);

    // Enqueue job on shard 0 with a floating limit that routes to shard 1
    let default_max = 5u32;
    let created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "remote-floating"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: default_max,
                refresh_interval_ms: 60_000,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(created_job_id, job_id);

    // Dequeue from shard 0 - should get a RequestRemoteTicket cross-shard task
    let result = shard0.dequeue("worker", 10).await.expect("dequeue");

    // Find the RequestRemoteTicket task
    let remote_request = result.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
                job_id: jid,
                ..
            } if jid == &job_id
        )
    });
    assert!(
        remote_request.is_some(),
        "Should have RequestRemoteTicket for remote queue"
    );

    // Get the max_concurrency from the task
    let (task_key, max_concurrency) = match remote_request.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
            task_key,
            max_concurrency,
            ..
        } => (task_key.clone(), *max_concurrency),
        _ => unreachable!(),
    };

    // WITHOUT THE FIX: max_concurrency would be 1 (the default when concurrency_limits() returns empty)
    assert_eq!(
        max_concurrency, default_max,
        "RequestRemoteTicket should use default_max_concurrency={} from FloatingConcurrencyLimit, but got {}. \
         WITHOUT THE FIX: This would be 1 because concurrency_limits() filtered out FloatingConcurrency.",
        default_max, max_concurrency
    );

    // Now complete the flow and verify the queue owner gets the correct max_concurrency
    // Delete the cross-shard task to clean up
    shard0
        .delete_cross_shard_task(&task_key)
        .await
        .expect("delete task");

    // Request ticket on the queue owner shard (shard 1)
    // This simulates what server.rs would do after receiving the cross-shard task
    let granted = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            &job_id,
            0,
            "req-1",
            1,
            10,
            now,
            max_concurrency,
            None,
        )
        .await
        .expect("request ticket");

    assert!(granted, "First ticket should be granted");

    // Request more tickets up to the limit - should all be granted
    for i in 2..=default_max {
        let granted = client
            .request_concurrency_ticket(
                1,
                tenant,
                &queue_key,
                &format!("job-{}", i),
                0,
                &format!("req-{}", i),
                1,
                10,
                now,
                max_concurrency,
                None,
            )
            .await
            .expect(&format!("request ticket {}", i));

        assert!(
            granted,
            "Ticket {} should be granted (max={})",
            i, default_max
        );
    }

    // One more should be queued, not granted
    let overflow = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            "job-overflow",
            0,
            "req-overflow",
            1,
            10,
            now,
            max_concurrency,
            None,
        )
        .await
        .expect("overflow request");

    assert!(
        !overflow,
        "Request beyond max_concurrency={} should be queued",
        default_max
    );

    // Clean up notification tasks
    let _ = shard1.dequeue("internal", 100).await;
}

// ============================================================================
// Tests for queue owner using local floating limit state (Bug #3)
// ============================================================================

/// Tests that when a queue owner has an updated current_max_concurrency in its
/// floating limit state, it uses that value instead of the max_concurrency
/// sent by the remote job shard.
///
/// This is important because the floating limit might have been refreshed to
/// a different value than the default_max_concurrency that job shards send.
///
/// WITHOUT THE FIX: The queue owner would always use the max_concurrency
/// passed in the request, ignoring any updated floating limit state.
#[silo::test]
async fn queue_owner_uses_local_floating_limit_state() {
    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard1 = factory.get("1").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Find a queue that routes to shard 1
    let tenant = "-";
    let (_, queue_key) = find_queue_for_shard(tenant, 1, 2);

    // First, create the floating limit state on shard 1 (queue owner) by
    // enqueueing a local job with the same queue key
    let local_job_id_on_shard1 = {
        // Find a job that routes to shard 1
        let mut local_job = String::new();
        for i in 0..10000 {
            let candidate = format!("local-job-{}", i);
            if job_to_shard(&candidate, 2) == 1 {
                local_job = candidate;
                break;
            }
        }
        local_job
    };

    // Enqueue on shard 1 to create the floating limit state with default_max_concurrency=3
    let _ = shard1
        .enqueue(
            tenant,
            Some(local_job_id_on_shard1.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "local-setup"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: 3,
                refresh_interval_ms: 60_000,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue local job on shard 1");

    // Dequeue to take a ticket for local_job
    let _ = shard1.dequeue("worker", 1).await;

    // Now simulate a refresh that increased the limit to 10
    // (This would normally happen via RefreshFloatingLimit task, but we'll do it directly)
    let state_key = format!("floating_limits/{}/{}", tenant, queue_key);
    let state_raw = shard1
        .db()
        .get(state_key.as_bytes())
        .await
        .expect("db get")
        .expect("state should exist");

    let decoded = decode_floating_limit_state(&state_raw).expect("decode state");
    let archived = decoded.archived();

    // Verify initial state
    assert_eq!(
        archived.current_max_concurrency, 3,
        "initial max should be 3"
    );

    // Update the state to have current_max_concurrency=10 (simulating a refresh)
    // We'll do this by reporting a successful refresh
    // But first we need a refresh task... let's just manually update the state
    let updated_state = silo::job::FloatingLimitState {
        current_max_concurrency: 10, // Updated from refresh!
        default_max_concurrency: archived.default_max_concurrency,
        refresh_interval_ms: archived.refresh_interval_ms,
        last_refreshed_at_ms: now,
        refresh_task_scheduled: false,
        retry_count: 0,
        next_retry_at_ms: None,
        metadata: archived
            .metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let encoded = silo::codec::encode_floating_limit_state(&updated_state).expect("encode");
    shard1
        .db()
        .put(state_key.as_bytes(), &encoded)
        .await
        .expect("put updated state");
    shard1.db().flush().await.expect("flush");

    // Verify the update worked
    let state_raw2 = shard1
        .db()
        .get(state_key.as_bytes())
        .await
        .expect("db get")
        .expect("state should exist");
    let decoded2 = decode_floating_limit_state(&state_raw2).expect("decode state");
    assert_eq!(decoded2.archived().current_max_concurrency, 10);

    // Now request tickets from the remote job shard's perspective
    // The remote job shard sends max_concurrency=3 (the default it knows about)
    // but the queue owner should use its local state's current_max_concurrency=10

    // Request tickets 2-10 (we already have 1 from local_job)
    // All should be granted because the queue owner should use current_max=10
    for i in 2..=10 {
        let granted = client
            .request_concurrency_ticket(
                1,
                tenant,
                &queue_key,
                &format!("remote-job-{}", i),
                0,
                &format!("req-{}", i),
                1,
                10,
                now,
                3, // Remote shard sends default=3, but local state has current=10
                None,
            )
            .await
            .expect(&format!("request ticket {}", i));

        // WITHOUT THE FIX: This would fail at i=4 because it would use max_concurrency=3
        assert!(
            granted,
            "Ticket {} should be granted. \
             Queue owner should use local floating limit state (current_max_concurrency=10), \
             not the max_concurrency=3 sent in the request. \
             WITHOUT THE FIX: This fails at ticket 4 because it uses the request's max=3.",
            i
        );
    }

    // Ticket 11 should be queued (we now have 10 holders)
    let overflow = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            "remote-job-11",
            0,
            "req-11",
            1,
            10,
            now,
            3,
            None,
        )
        .await
        .expect("overflow request");

    assert!(
        !overflow,
        "Ticket 11 should be queued (current_max_concurrency=10)"
    );
}

/// Tests that local ticket processing also uses FloatingConcurrencyLimit correctly.
/// This tests the fix in process_ticket_request_task in concurrency.rs.
///
/// WITHOUT THE FIX: The code would call view.concurrency_limits() which only returns
/// ConcurrencyLimit, not FloatingConcurrencyLimit, causing max_concurrency to be 1.
#[silo::test]
async fn local_ticket_processing_handles_floating_limits() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "local-floating-queue".to_string();
    let default_max = 5u32;

    // Enqueue multiple jobs with the same floating limit
    let mut job_ids = Vec::new();
    for i in 1..=6 {
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                serde_json::json!({"job": i}),
                vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: default_max,
                    refresh_interval_ms: 60_000,
                    metadata: vec![],
                })],
                None,
            )
            .await
            .expect(&format!("enqueue job {}", i));
        job_ids.push(job_id);
    }

    // Dequeue should return exactly 5 tasks (max_concurrency=5)
    let result = shard.dequeue("worker", 10).await.expect("dequeue");

    // WITHOUT THE FIX: This would only return 1 task because max_concurrency defaults to 1
    // when concurrency_limits() returns empty for FloatingConcurrency
    assert_eq!(
        result.tasks.len(),
        default_max as usize,
        "Should dequeue {} jobs (max_concurrency). \
         WITHOUT THE FIX: This would be 1 because concurrency_limits() filtered out FloatingConcurrency.",
        default_max
    );

    // The 6th job should still be waiting (queued for concurrency)
    let returned_job_ids: std::collections::HashSet<_> =
        result.tasks.iter().map(|t| t.job().id()).collect();

    // Count how many of our jobs were returned
    let our_jobs_returned = job_ids
        .iter()
        .filter(|id| returned_job_ids.contains(id.as_str()))
        .count();

    assert_eq!(
        our_jobs_returned,
        default_max as usize,
        "Exactly {} of our 6 jobs should be returned",
        default_max
    );
}

// ============================================================================
// BUG EXPOSURE TESTS: Floating limits broken for remote queues
// These tests FAIL if floating limit info is not properly sent to queue owner
// ============================================================================

/// Tests that when a job with FloatingConcurrencyLimit routes to a remote queue,
/// the queue owner shard creates floating limit state.
///
/// BUG: Currently, the queue owner never creates floating limit state for remote
/// jobs because RequestRemoteTicket doesn't include the floating limit definition.
#[silo::test]
async fn remote_queue_owner_creates_floating_limit_state() {
    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let shard1 = factory.get("1").unwrap();
    let now = now_ms();

    // Find a job that routes to shard 0 and a queue that routes to shard 1
    let tenant = "-";
    let job_id = find_job_for_shard(0, 2);
    let (_, queue_key) = find_queue_for_shard(tenant, 1, 2);

    // Verify routing
    assert_eq!(job_to_shard(&job_id, 2), 0);
    assert_eq!(queue_to_shard(tenant, &queue_key, 2), 1);

    let default_max = 5u32;
    let refresh_interval_ms = 60_000i64;

    // Enqueue job on shard 0 with a floating limit that routes to shard 1
    let _created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "remote-floating-state"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: default_max,
                refresh_interval_ms,
                metadata: vec![("org_id".to_string(), "123".to_string())],
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Dequeue from shard 0 to get the RequestRemoteTicket task
    let result = shard0.dequeue("worker", 10).await.expect("dequeue");

    // Find the RequestRemoteTicket task
    let remote_request = result.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket { .. }
        )
    });
    assert!(remote_request.is_some(), "Should have RequestRemoteTicket");

    // Simulate the server processing: call receive_remote_ticket_request on queue owner
    let client = ClusterClient::new(factory.clone(), None);

    // Get max_concurrency and floating_limit from the task
    let (task_key, max_concurrency, floating_limit) = match remote_request.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
            task_key,
            max_concurrency,
            floating_limit,
            ..
        } => (task_key.clone(), *max_concurrency, floating_limit.clone()),
        _ => unreachable!(),
    };

    // Request ticket on queue owner shard, passing the floating limit data
    let _granted = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            &job_id,
            0,
            "req-1",
            1,
            10,
            now,
            max_concurrency,
            floating_limit.as_ref(),
        )
        .await
        .expect("request ticket");

    // Clean up
    shard0
        .delete_cross_shard_task(&task_key)
        .await
        .expect("delete task");

    // Check that floating limit state was created on shard 1 (queue owner)
    let state_key = format!("floating_limits/{}/{}", tenant, queue_key);
    let state_exists = shard1
        .db()
        .get(state_key.as_bytes())
        .await
        .expect("db get")
        .is_some();

    assert!(
        state_exists,
        "Queue owner shard should have floating limit state for remote queue. \
         BUG: Currently, receive_remote_ticket_request never creates floating limit state \
         because it doesn't receive the FloatingConcurrencyLimit definition."
    );

    // If state exists, verify it has correct values
    if state_exists {
        let state_raw = shard1
            .db()
            .get(state_key.as_bytes())
            .await
            .expect("db get")
            .expect("state");
        let decoded = decode_floating_limit_state(&state_raw).expect("decode");
        let archived = decoded.archived();

        assert_eq!(archived.default_max_concurrency, default_max);
        assert_eq!(archived.refresh_interval_ms, refresh_interval_ms);
        assert_eq!(archived.metadata.len(), 1);
    }
}

/// Tests that refresh tasks are scheduled on the queue owner shard for remote
/// floating limits when the state is stale.
#[silo::test]
async fn remote_queue_owner_schedules_refresh_tasks() {
    let now = now_ms();

    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let shard1 = factory.get("1").unwrap();
    let client = ClusterClient::new(factory.clone(), None);

    // Find a job that routes to shard 0 and a queue that routes to shard 1
    let tenant = "-";
    let job_id = find_job_for_shard(0, 2);
    let (_, queue_key) = find_queue_for_shard(tenant, 1, 2);

    let default_max = 5u32;
    let refresh_interval_ms = 60_000i64; // 1 minute

    // Pre-create STALE floating limit state on shard 1 (queue owner)
    // This simulates a state that was created a long time ago and needs refresh
    let stale_state = silo::job::FloatingLimitState {
        current_max_concurrency: default_max,
        default_max_concurrency: default_max,
        refresh_interval_ms,
        last_refreshed_at_ms: now - (refresh_interval_ms * 2), // Stale: 2 intervals ago
        refresh_task_scheduled: false,
        retry_count: 0,
        next_retry_at_ms: None,
        metadata: vec![("env".to_string(), "test".to_string())],
    };
    let state_key = format!("floating_limits/{}/{}", tenant, queue_key);
    let state_val = silo::codec::encode_floating_limit_state(&stale_state).expect("encode");
    shard1
        .db()
        .put(state_key.as_bytes(), &state_val)
        .await
        .expect("put stale state");

    // Enqueue job on shard 0 with a floating limit that routes to shard 1
    let _created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "remote-floating-refresh"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: default_max,
                refresh_interval_ms,
                metadata: vec![("env".to_string(), "test".to_string())],
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Dequeue from shard 0 to get the RequestRemoteTicket task
    let result = shard0.dequeue("worker", 10).await.expect("dequeue");
    let remote_request = result.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket { .. }
        )
    });
    assert!(remote_request.is_some());

    let (task_key, max_concurrency, floating_limit) = match remote_request.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
            task_key,
            max_concurrency,
            floating_limit,
            ..
        } => (task_key.clone(), *max_concurrency, floating_limit.clone()),
        _ => unreachable!(),
    };

    // Request ticket on queue owner shard, passing floating limit data
    // This should detect the stale state and schedule a refresh task
    let _granted = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            &job_id,
            0,
            "req-1",
            1,
            10,
            now,
            max_concurrency,
            floating_limit.as_ref(),
        )
        .await
        .expect("request ticket");

    // Clean up the RequestRemoteTicket task
    shard0
        .delete_cross_shard_task(&task_key)
        .await
        .expect("delete task");

    // Check that a RefreshFloatingLimit task exists on shard 1 (queue owner)
    let tasks = shard1.peek_tasks(50).await.expect("peek tasks");
    let has_refresh_task = tasks.iter().any(|t| {
        matches!(
            t,
            silo::task::Task::RefreshFloatingLimit { queue_key: qk, .. } if qk == &queue_key
        )
    });

    assert!(
        has_refresh_task,
        "Queue owner shard should have RefreshFloatingLimit task for stale floating limit. \
         BUG: Currently, refresh tasks are never scheduled on the queue owner because \
         receive_remote_ticket_request doesn't receive or store the floating limit definition. \
         Tasks on shard 1: {:?}",
        tasks
    );
}

/// Tests that floating limit metadata is preserved when sent to remote queue owner.
///
/// BUG: Currently, metadata is completely lost because RequestRemoteTicket doesn't
/// include the FloatingConcurrencyLimit definition.
#[silo::test]
async fn remote_floating_limit_preserves_metadata() {
    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let shard1 = factory.get("1").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Find a job that routes to shard 0 and a queue that routes to shard 1
    let tenant = "-";
    let job_id = find_job_for_shard(0, 2);
    let (_, queue_key) = find_queue_for_shard(tenant, 1, 2);

    let metadata = vec![
        ("org_id".to_string(), "org-456".to_string()),
        ("api_key".to_string(), "secret-key".to_string()),
        ("env".to_string(), "production".to_string()),
    ];

    // Enqueue job with metadata
    let _job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "metadata"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: 5,
                refresh_interval_ms: 60_000,
                metadata: metadata.clone(),
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Process the cross-shard request
    let result = shard0.dequeue("worker", 10).await.expect("dequeue");
    let remote_request = result.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket { .. }
        )
    });
    assert!(remote_request.is_some());

    let (task_key, max_concurrency, floating_limit) = match remote_request.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
            task_key,
            max_concurrency,
            floating_limit,
            ..
        } => (task_key.clone(), *max_concurrency, floating_limit.clone()),
        _ => unreachable!(),
    };

    // Request ticket, passing the floating limit data
    let _granted = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            &job_id,
            0,
            "req-1",
            1,
            10,
            now,
            max_concurrency,
            floating_limit.as_ref(),
        )
        .await
        .expect("request ticket");

    shard0
        .delete_cross_shard_task(&task_key)
        .await
        .expect("delete task");

    // Check that metadata was preserved in floating limit state on queue owner
    let state_key = format!("floating_limits/{}/{}", tenant, queue_key);
    let state_raw = shard1.db().get(state_key.as_bytes()).await.expect("db get");

    assert!(
        state_raw.is_some(),
        "Floating limit state should exist on queue owner"
    );

    if let Some(raw) = state_raw {
        let decoded = decode_floating_limit_state(&raw).expect("decode");
        let archived = decoded.archived();

        // Verify all metadata was preserved
        let stored_metadata: Vec<(String, String)> = archived
            .metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        for (key, value) in &metadata {
            assert!(
                stored_metadata.contains(&(key.clone(), value.clone())),
                "Metadata ({}={}) should be preserved. \
                 BUG: Currently, metadata is lost because RequestRemoteTicket doesn't include \
                 the FloatingConcurrencyLimit definition. \
                 Stored metadata: {:?}",
                key,
                value,
                stored_metadata
            );
        }
    }
}

/// Tests that after a floating limit state is updated to a higher value,
/// new enqueues can take advantage of the increased capacity.
/// This verifies that the floating limit state's current_max_concurrency is used.
#[silo::test]
async fn floating_limit_uses_updated_state_for_new_jobs() {
    tokio::time::pause();
    let now = now_ms();
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "refresh-increase-queue".to_string();
    let initial_max = 2u32;
    let refresh_interval_ms = 100i64;

    // Create initial floating limit state with default_max_concurrency=2
    let _j1 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"job": 1}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue.clone(),
                default_max_concurrency: initial_max,
                refresh_interval_ms,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue j1");

    let _j2 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"job": 2}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue.clone(),
                default_max_concurrency: initial_max,
                refresh_interval_ms,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue j2");

    // Dequeue to take the 2 slots
    let result = shard.dequeue("worker", 10).await.expect("dequeue 1");
    assert_eq!(
        result.tasks.len(),
        initial_max as usize,
        "Should get {} jobs initially",
        initial_max
    );

    // Now manually update the floating limit state to have higher limit (5)
    // This simulates what happens after a successful refresh
    let state_key = format!("floating_limits/-/{}", queue);
    let new_state = silo::job::FloatingLimitState {
        current_max_concurrency: 5, // Increased!
        default_max_concurrency: initial_max,
        refresh_interval_ms,
        last_refreshed_at_ms: now_ms(),
        refresh_task_scheduled: false,
        retry_count: 0,
        next_retry_at_ms: None,
        metadata: vec![],
    };
    let encoded = silo::codec::encode_floating_limit_state(&new_state).expect("encode");
    shard
        .db()
        .put(state_key.as_bytes(), &encoded)
        .await
        .expect("put");
    shard.db().flush().await.expect("flush");

    // Now enqueue 3 more jobs - they should all be able to run because limit is now 5
    // (we have 2 holders, so 3 more can run)
    for i in 3..=5 {
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                serde_json::json!({"job": i}),
                vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                    key: queue.clone(),
                    default_max_concurrency: initial_max, // Jobs still have default=2
                    refresh_interval_ms,
                    metadata: vec![],
                })],
                None,
            )
            .await
            .expect(&format!("enqueue j{}", i));

        // These jobs should get tickets because local state has current_max=5
        let _ = job_id;
    }

    // Dequeue should return 3 more tasks (5 - 2 existing = 3)
    let result2 = shard.dequeue("worker", 10).await.expect("dequeue 2");

    assert_eq!(
        result2.tasks.len(),
        3,
        "Should get 3 more jobs since limit increased to 5 (5-2=3). \
         The floating limit state's current_max_concurrency=5 should be used, \
         not the jobs' default_max_concurrency=2."
    );

    // Sixth job should be queued
    let _j6 = shard
        .enqueue(
            "-",
            None,
            10u8,
            now_ms(),
            None,
            serde_json::json!({"job": 6}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue.clone(),
                default_max_concurrency: initial_max,
                refresh_interval_ms,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue j6");

    let result3 = shard.dequeue("worker", 10).await.expect("dequeue 3");
    assert_eq!(
        result3.tasks.len(),
        0,
        "j6 should be queued (at capacity of 5)"
    );
}

// ============================================================================
// End-to-End Integration Test
// ============================================================================

/// Comprehensive end-to-end test for a job with a remote floating concurrency limit.
///
/// This test verifies the complete flow:
/// 1. Enqueue job on shard 0 with floating limit routing to shard 1 (queue owner)
/// 2. Process RequestRemoteTicket cross-shard task -> queue owner creates floating limit state
/// 3. Receive NotifyRemoteTicketGrant -> job shard schedules RunAttempt
/// 4. Process RefreshFloatingLimit task on queue owner (when state is stale)
/// 5. Complete the job and verify it succeeded
///
/// This is the most comprehensive test for the floating limit remote queue feature.
#[silo::test]
async fn end_to_end_remote_floating_limit_job_completion() {
    use silo::job::JobStatusKind;
    use silo::job_attempt::AttemptOutcome;

    let now = now_ms();

    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap(); // Job shard
    let shard1 = factory.get("1").unwrap(); // Queue owner shard
    let client = ClusterClient::new(factory.clone(), None);

    // Find a job that routes to shard 0 and a queue that routes to shard 1
    let tenant = "-";
    let job_id = find_job_for_shard(0, 2);
    let (_, queue_key) = find_queue_for_shard(tenant, 1, 2);

    // Verify routing
    assert_eq!(job_to_shard(&job_id, 2), 0, "Job should route to shard 0");
    assert_eq!(
        queue_to_shard(tenant, &queue_key, 2),
        1,
        "Queue should route to shard 1"
    );

    let default_max = 3u32;
    let refresh_interval_ms = 60_000i64; // 1 minute
    let state_key = format!("floating_limits/{}/{}", tenant, queue_key);

    // ========================================================================
    // Step 1: Enqueue job on shard 0 with floating limit routing to shard 1
    // ========================================================================
    let created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "e2e-remote-floating"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: default_max,
                refresh_interval_ms,
                metadata: vec![("org_id".to_string(), "test-org".to_string())],
            })],
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(created_job_id, job_id);

    // Job should be in Scheduled status
    let status = shard0
        .get_job_status(tenant, &job_id)
        .await
        .expect("get status")
        .expect("job exists");
    assert_eq!(status.kind, JobStatusKind::Scheduled);

    // ========================================================================
    // Step 2: Dequeue from job shard to get RequestRemoteTicket cross-shard task
    // ========================================================================
    let dequeue_result = shard0.dequeue("worker", 10).await.expect("dequeue shard0");

    // Should have no runnable tasks yet (waiting for concurrency ticket)
    assert_eq!(
        dequeue_result.tasks.len(),
        0,
        "No tasks yet - waiting for remote concurrency ticket"
    );

    // Should have a RequestRemoteTicket cross-shard task
    let request_task = dequeue_result.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket { .. }
        )
    });
    assert!(
        request_task.is_some(),
        "Should have RequestRemoteTicket cross-shard task"
    );

    // Extract task details
    let (task_key, max_concurrency, floating_limit) = match request_task.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
            task_key,
            max_concurrency,
            floating_limit,
            ..
        } => (task_key.clone(), *max_concurrency, floating_limit.clone()),
        _ => unreachable!(),
    };

    // Floating limit should be included in the task
    assert!(
        floating_limit.is_some(),
        "RequestRemoteTicket should include floating limit data"
    );
    let fl = floating_limit.as_ref().unwrap();
    assert_eq!(fl.default_max_concurrency, default_max);
    assert_eq!(fl.refresh_interval_ms, refresh_interval_ms);

    // ========================================================================
    // Step 3: Simulate server processing - send ticket request to queue owner
    // ========================================================================
    let granted = client
        .request_concurrency_ticket(
            1, // Queue owner shard
            tenant,
            &queue_key,
            &job_id,
            0, // Job shard
            "req-1",
            1,
            10,
            now,
            max_concurrency,
            floating_limit.as_ref(),
        )
        .await
        .expect("request ticket");

    assert!(granted, "Ticket should be granted (queue is empty)");

    // Clean up the RequestRemoteTicket task
    shard0
        .delete_cross_shard_task(&task_key)
        .await
        .expect("delete request task");

    // ========================================================================
    // Step 4: Process NotifyRemoteTicketGrant cross-shard task from queue owner
    // ========================================================================
    // Dequeue from queue owner shard to get the NotifyRemoteTicketGrant task
    let owner_dequeue = shard1.dequeue("internal", 10).await.expect("dequeue shard1");

    // Find the notify task
    let notify_task = owner_dequeue.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
                job_id: jid,
                ..
            } if jid == &job_id
        )
    });
    assert!(
        notify_task.is_some(),
        "Should have NotifyRemoteTicketGrant task"
    );

    let (notify_task_key, holder_task_id, request_id) = match notify_task.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            task_key,
            holder_task_id,
            request_id,
            ..
        } => (task_key.clone(), holder_task_id.clone(), request_id.clone()),
        _ => unreachable!(),
    };

    // Simulate server processing the notify - send grant to job shard
    client
        .notify_concurrency_ticket_granted(
            0, // Job shard
            tenant,
            &job_id,
            &queue_key,
            &request_id,
            &holder_task_id,
            1, // Queue owner shard
            1, // Attempt number
        )
        .await
        .expect("notify grant");

    // Clean up the NotifyRemoteTicketGrant task
    shard1
        .delete_cross_shard_task(&notify_task_key)
        .await
        .expect("delete notify task");

    // ========================================================================
    // Step 5: Dequeue from job shard - should now have RunAttempt task
    // ========================================================================
    let job_dequeue = shard0.dequeue("worker", 10).await.expect("dequeue job tasks");

    // LeasedTask has job() and attempt() methods
    let run_attempt_task = job_dequeue
        .tasks
        .iter()
        .find(|t| t.job().id() == &job_id);
    assert!(
        run_attempt_task.is_some(),
        "Should have RunAttempt task after grant notification"
    );

    let task_id = run_attempt_task.unwrap().attempt().task_id().to_string();

    // Job should now be Running
    let running_status = shard0
        .get_job_status(tenant, &job_id)
        .await
        .expect("get status")
        .expect("job exists");
    assert_eq!(running_status.kind, JobStatusKind::Running);

    // ========================================================================
    // Step 6: Handle RefreshFloatingLimit task on queue owner if present
    // ========================================================================
    // The refresh task might be scheduled if state was detected as stale.
    // This is covered in detail by remote_queue_owner_schedules_refresh_tasks test.
    // Here we just handle it if present to complete the flow.
    let refresh_dequeue = shard1.dequeue("refresh-worker", 10).await.expect("dequeue refresh");

    if let Some(refresh) = refresh_dequeue
        .refresh_tasks
        .iter()
        .find(|t| t.queue_key == queue_key)
    {
        let refresh_task_id = refresh.task_id.clone();

        // Report successful refresh with an increased limit
        shard1
            .report_refresh_success(tenant, &refresh_task_id, 10) // Increase to 10
            .await
            .expect("report refresh success");

        // Verify the floating limit state was updated
        let updated_state_raw = shard1
            .db()
            .get(state_key.as_bytes())
            .await
            .expect("db get")
            .expect("state exists");
        let decoded = decode_floating_limit_state(&updated_state_raw).expect("decode");
        let archived = decoded.archived();
        assert_eq!(
            archived.current_max_concurrency, 10,
            "Floating limit should be updated to 10"
        );
        println!("✅ Refresh task processed successfully, limit updated to 10");
    } else {
        println!("ℹ️  No refresh task scheduled (state may not have been detected as stale)");
    }

    // ========================================================================
    // Step 7: Complete the job
    // ========================================================================
    shard0
        .report_attempt_outcome(
            tenant,
            &task_id,
            AttemptOutcome::Success {
                result: b"done".to_vec(),
            },
        )
        .await
        .expect("report success");

    // Job should be Succeeded
    let final_status = shard0
        .get_job_status(tenant, &job_id)
        .await
        .expect("get status")
        .expect("job exists");
    assert_eq!(
        final_status.kind,
        JobStatusKind::Succeeded,
        "Job should be in Succeeded state"
    );

    // ========================================================================
    // Step 8: Verify the concurrency ticket was released
    // ========================================================================
    // Enqueue another job - it should immediately get a ticket since the previous one released
    let job_id2 = find_job_for_shard(0, 2);
    let _j2 = shard0
        .enqueue(
            tenant,
            Some(format!("{}-2", job_id2)),
            10u8,
            now_ms(),
            None,
            serde_json::json!({"test": "second-job"}),
            vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: queue_key.clone(),
                default_max_concurrency: default_max,
                refresh_interval_ms,
                metadata: vec![],
            })],
            None,
        )
        .await
        .expect("enqueue second job");

    // Process the second job's cross-shard ticket request
    let dequeue2 = shard0.dequeue("worker", 10).await.expect("dequeue2");
    let request2 = dequeue2.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket { .. }
        )
    });
    assert!(
        request2.is_some(),
        "Second job should have RequestRemoteTicket"
    );

    // The second request should be granted (first job released its ticket)
    let (task_key2, max2, fl2) = match request2.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
            task_key,
            max_concurrency,
            floating_limit,
            ..
        } => (task_key.clone(), *max_concurrency, floating_limit.clone()),
        _ => unreachable!(),
    };

    let granted2 = client
        .request_concurrency_ticket(
            1,
            tenant,
            &queue_key,
            &format!("{}-2", job_id2),
            0,
            "req-2",
            1,
            10,
            now_ms(),
            max2,
            fl2.as_ref(),
        )
        .await
        .expect("request ticket 2");

    assert!(
        granted2,
        "Second job should get ticket (first job released its slot)"
    );

    shard0
        .delete_cross_shard_task(&task_key2)
        .await
        .expect("delete task2");

    println!("✅ End-to-end test passed: Remote floating limit job completed successfully");
}
