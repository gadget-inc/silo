//! Integration tests for cross-shard concurrency ticket flow.
//!
//! These tests verify the distributed concurrency protocol where:
//! - Jobs are sharded by job_id
//! - Concurrency queues are sharded by (tenant, queue_key)
//! - Cross-shard RPCs coordinate ticket requests, grants, and releases

mod test_helpers;

use silo::cluster_client::ClusterClient;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::routing::{job_to_shard, queue_to_shard};
use silo::settings::{Backend, DatabaseTemplate};
use std::sync::Arc;
use test_helpers::{count_with_prefix, now_ms, open_temp_shard};

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
        let job_id = format!("job-{}", i);
        if job_to_shard(&job_id, num_shards) == target_shard {
            return job_id;
        }
    }
    panic!("Could not find job_id for shard {}", target_shard);
}

/// Helper to find a (tenant, queue) pair that routes to a specific shard
fn find_queue_for_shard(target_shard: u32, num_shards: u32) -> (String, String) {
    for i in 0..10000 {
        let tenant = format!("tenant-{}", i);
        let queue = "test-queue".to_string();
        if queue_to_shard(&tenant, &queue, num_shards) == target_shard {
            return (tenant, queue);
        }
    }
    panic!("Could not find queue for shard {}", target_shard);
}

// ============================================================================
// Routing Tests
// ============================================================================

#[silo::test]
async fn routing_job_to_shard_is_deterministic() {
    let num_shards = 4;
    let job_id = "test-job-123";

    let shard1 = job_to_shard(job_id, num_shards);
    let shard2 = job_to_shard(job_id, num_shards);

    assert_eq!(shard1, shard2, "job_to_shard should be deterministic");
    assert!(shard1 < num_shards, "shard should be in range");
}

#[silo::test]
async fn routing_queue_to_shard_is_deterministic() {
    let num_shards = 4;
    let tenant = "test-tenant";
    let queue = "test-queue";

    let shard1 = queue_to_shard(tenant, queue, num_shards);
    let shard2 = queue_to_shard(tenant, queue, num_shards);

    assert_eq!(shard1, shard2, "queue_to_shard should be deterministic");
    assert!(shard1 < num_shards, "shard should be in range");
}

#[silo::test]
async fn routing_can_find_job_for_each_shard() {
    let num_shards = 4;

    for target_shard in 0..num_shards {
        let job_id = find_job_for_shard(target_shard, num_shards);
        let actual_shard = job_to_shard(&job_id, num_shards);
        assert_eq!(
            actual_shard, target_shard,
            "job {} should route to shard {}",
            job_id, target_shard
        );
    }
}

#[silo::test]
async fn routing_can_find_queue_for_each_shard() {
    let num_shards = 4;

    for target_shard in 0..num_shards {
        let (tenant, queue) = find_queue_for_shard(target_shard, num_shards);
        let actual_shard = queue_to_shard(&tenant, &queue, num_shards);
        assert_eq!(
            actual_shard, target_shard,
            "queue ({}, {}) should route to shard {}",
            tenant, queue, target_shard
        );
    }
}

// ============================================================================
// Ticket Request Tests (queue owner receives request)
// ============================================================================

#[silo::test]
async fn request_ticket_granted_when_queue_empty() {
    // When queue has capacity, ticket should be granted immediately
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    let granted = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-1", 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("request should succeed");

    // request_concurrency_ticket returns true if granted immediately
    assert!(granted, "ticket should be granted when queue is empty");
}

#[silo::test]
async fn request_ticket_queued_when_at_capacity() {
    // When queue is at capacity, second request should be queued (not granted)
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // First request - granted
    let granted1 = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-1", 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("first request");
    assert!(granted1, "first request should be granted");

    // Second request - queued (max_concurrency=1)
    let granted2 = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-2", 0, "req-2", 1, 10, now, 1, None)
        .await
        .expect("second request");
    assert!(!granted2, "second request should be queued, not granted");
}

#[silo::test]
async fn request_ticket_multiple_allowed_when_limit_higher() {
    // With max_concurrency=2, two requests should both be granted
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    let granted1 = client
        .request_concurrency_ticket(
            0, "tenant", "queue", "job-1", 0, "req-1", 1, 10, now, 2, None, // max=2
        )
        .await
        .expect("first request");
    assert!(granted1, "first should be granted");

    let granted2 = client
        .request_concurrency_ticket(
            0, "tenant", "queue", "job-2", 0, "req-2", 1, 10, now, 2, None, // max=2
        )
        .await
        .expect("second request");
    assert!(granted2, "second should also be granted (max=2)");

    // Third request - queued
    let granted3 = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-3", 0, "req-3", 1, 10, now, 2, None)
        .await
        .expect("third request");
    assert!(!granted3, "third should be queued");
}

// ============================================================================
// Ticket Grant Notification Tests (job shard receives grant)
// ============================================================================

#[silo::test]
async fn receive_grant_creates_runattempt_task() {
    // When job shard receives grant notification, it should create a RunAttempt task
    // Use 2 shards so we can test cross-shard grant notification
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Find a job_id that routes to shard 0 and a queue that routes to shard 1
    let tenant = "tenant";
    let job_id = find_job_for_shard(0, 2);
    let (_, queue_key) = find_queue_for_shard(1, 2);

    // First create a job on shard 0
    shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "data"}),
            vec![],
            None,
        )
        .await
        .expect("enqueue");

    // Simulate receiving grant notification from shard 1 (queue owner)
    client
        .notify_concurrency_ticket_granted(
            0, // job_shard
            tenant, &job_id, &queue_key, "req-1", "holder-1", 1, // queue_owner_shard
            1, // attempt_number
        )
        .await
        .expect("notify grant");

    // Dequeue should return the job
    let result = shard0.dequeue("worker", 10).await.expect("dequeue");
    let task = result.tasks.iter().find(|t| t.job().id() == job_id);
    assert!(task.is_some(), "should have RunAttempt task for the job");
}

// ============================================================================
// Ticket Release Tests (queue owner receives release)
// ============================================================================

#[silo::test]
async fn release_ticket_allows_next_request() {
    // After releasing a ticket, the next queued request should be granted
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let shard = factory.get("0").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // First request - granted
    let granted1 = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-1", 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("first request");
    assert!(granted1);

    // Dequeue once to process the notification and get holder_task_id
    let result1 = shard.dequeue("internal", 1).await.expect("dequeue");
    assert!(!result1.cross_shard_tasks.is_empty());

    let holder_task_id = match &result1.cross_shard_tasks[0] {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            holder_task_id,
            ..
        } => holder_task_id.clone(),
        _ => panic!("expected NotifyRemoteTicketGrant"),
    };

    // Second request - queued
    let granted2 = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-2", 0, "req-2", 1, 10, now, 1, None)
        .await
        .expect("second request");
    assert!(!granted2, "second should be queued");

    // Release first ticket
    client
        .release_concurrency_ticket(0, "tenant", "queue", "job-1", &holder_task_id)
        .await
        .expect("release");

    // Now manually trigger grant processing by calling dequeue
    // The release_remote_ticket should have triggered grant_next
    let result2 = shard.dequeue("internal", 10).await.expect("dequeue 2");

    // Should have a grant notification for job-2
    let job2_grant = result2.cross_shard_tasks.iter().any(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
                job_id,
                ..
            } if job_id == "job-2"
        )
    });
    assert!(job2_grant, "job-2 should be granted after release");
}

// ============================================================================
// Two-Shard Integration Tests
// ============================================================================

#[silo::test]
async fn two_shards_cross_shard_grant_flow() {
    // Job on shard 0, queue owner on shard 1
    // Use 2-shard factory so routing is respected
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let shard1 = factory.get("1").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Find a job_id that routes to shard 0 and a queue that routes to shard 1
    let job_id = find_job_for_shard(0, 2);
    let (tenant, queue_key) = find_queue_for_shard(1, 2);

    // Create job on shard 0
    shard0
        .enqueue(
            &tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "cross-shard"}),
            vec![],
            None,
        )
        .await
        .expect("enqueue");

    // Request ticket from shard 1 (queue owner)
    let granted = client
        .request_concurrency_ticket(1, &tenant, &queue_key, &job_id, 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("request");
    assert!(granted, "should be granted (queue empty)");

    // Dequeue from shard 1 to get notify task
    let result1 = shard1.dequeue("internal", 1).await.expect("dequeue shard1");
    let notify = result1.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant { .. }
        )
    });
    assert!(notify.is_some(), "should have notify task on queue owner");

    let (holder_task_id, req_id) = match notify.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            holder_task_id,
            request_id,
            ..
        } => (holder_task_id.clone(), request_id.clone()),
        _ => unreachable!(),
    };

    // Notify shard 0 (job shard)
    client
        .notify_concurrency_ticket_granted(
            0,
            &tenant,
            &job_id,
            &queue_key,
            &req_id,
            &holder_task_id,
            1,
            1,
        )
        .await
        .expect("notify");

    // Dequeue from shard 0 - should get RunAttempt
    let result0 = shard0.dequeue("worker", 10).await.expect("dequeue shard0");
    let task = result0.tasks.iter().find(|t| t.job().id() == job_id);
    assert!(task.is_some(), "job shard should have RunAttempt task");
}

// ============================================================================
// Local Concurrency Tests (same shard, no cross-shard)
// ============================================================================

#[silo::test]
async fn same_shard_concurrency_uses_local_path() {
    // When job and queue are on the same shard, use local concurrency
    use silo::job::{ConcurrencyLimit, Limit};

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue with local concurrency limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"test": "local"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "local-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Dequeue should return RunAttempt directly (no cross-shard tasks)
    let result = shard.dequeue("worker", 10).await.expect("dequeue");
    assert_eq!(result.tasks.len(), 1, "should get RunAttempt task");
    assert_eq!(result.cross_shard_tasks.len(), 0, "no cross-shard tasks");
    assert_eq!(result.tasks[0].job().id(), job_id);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[silo::test]
async fn request_ticket_future_start_time_not_granted() {
    // Ticket request with future start_time should be queued, not granted
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();
    let future_time = now + 60_000; // 1 minute in the future

    let granted = client
        .request_concurrency_ticket(
            0,
            "tenant",
            "queue",
            "job-future",
            0,
            "req-future",
            1,
            10,
            future_time, // Future start time
            1,
            None,
        )
        .await
        .expect("request should succeed");

    // Should be queued, not granted immediately
    assert!(
        !granted,
        "future start time should not be granted immediately"
    );
}

#[silo::test]
async fn request_ticket_tenant_isolation() {
    // Different tenants should have independent concurrency limits
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Tenant A fills their queue
    let granted_a1 = client
        .request_concurrency_ticket(0, "tenant-a", "queue", "job-a1", 0, "req-a1", 1, 10, now, 1, None)
        .await
        .expect("tenant A first request");
    assert!(granted_a1);

    let granted_a2 = client
        .request_concurrency_ticket(0, "tenant-a", "queue", "job-a2", 0, "req-a2", 1, 10, now, 1, None)
        .await
        .expect("tenant A second request");
    assert!(!granted_a2, "tenant A at capacity");

    // Tenant B should still be able to get a ticket (independent queue)
    let granted_b1 = client
        .request_concurrency_ticket(0, "tenant-b", "queue", "job-b1", 0, "req-b1", 1, 10, now, 1, None)
        .await
        .expect("tenant B request");
    assert!(granted_b1, "tenant B should have independent capacity");
}

#[silo::test]
async fn request_ticket_different_queues_independent() {
    // Different queue keys should have independent limits
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Fill queue-1
    let granted_q1 = client
        .request_concurrency_ticket(0, "tenant", "queue-1", "job-1", 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("queue-1 request");
    assert!(granted_q1);

    // queue-2 should be independent
    let granted_q2 = client
        .request_concurrency_ticket(0, "tenant", "queue-2", "job-2", 0, "req-2", 1, 10, now, 1, None)
        .await
        .expect("queue-2 request");
    assert!(granted_q2, "queue-2 should have independent capacity");
}

#[silo::test]
async fn receive_grant_for_nonexistent_job_fails() {
    // Grant notification for a job that doesn't exist should fail
    // Use 2 shards so we can test cross-shard grant notification
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let client = ClusterClient::new(factory.clone(), None);

    // Find a job_id that would route to shard 0 and a queue that routes to shard 1
    let job_id = find_job_for_shard(0, 2);
    let (tenant, queue_key) = find_queue_for_shard(1, 2);

    // Try to notify for a job that doesn't exist (but would route to shard 0)
    let result = client
        .notify_concurrency_ticket_granted(
            0, &tenant, &job_id, &queue_key, "req-1", "holder-1", 1, // queue_owner_shard
            1, // attempt_number
        )
        .await;

    assert!(result.is_err(), "should fail for nonexistent job");
}

#[silo::test]
async fn release_ticket_idempotent() {
    // Releasing the same ticket twice should not cause errors
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let shard = factory.get("0").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Request and get grant
    let granted = client
        .request_concurrency_ticket(0, "tenant", "queue", "job-1", 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("request");
    assert!(granted);

    // Get holder_task_id
    let result = shard.dequeue("internal", 1).await.expect("dequeue");
    let holder_task_id = match &result.cross_shard_tasks[0] {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            holder_task_id,
            ..
        } => holder_task_id.clone(),
        _ => panic!("expected NotifyRemoteTicketGrant"),
    };

    // Release once
    client
        .release_concurrency_ticket(0, "tenant", "queue", "job-1", &holder_task_id)
        .await
        .expect("first release");

    // Release again - should not error (idempotent)
    let result2 = client
        .release_concurrency_ticket(0, "tenant", "queue", "job-1", &holder_task_id)
        .await;
    // Should succeed (or at least not panic)
    assert!(result2.is_ok(), "second release should be idempotent");
}

#[silo::test]
async fn high_concurrency_limit_allows_many_grants() {
    // Test with a higher concurrency limit
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();
    let max_concurrency = 10;

    // Request max_concurrency tickets - all should be granted
    for i in 0..max_concurrency {
        let granted = client
            .request_concurrency_ticket(
                0,
                "tenant",
                "queue",
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
            .expect(&format!("request {}", i));
        assert!(
            granted,
            "request {} should be granted (limit={})",
            i, max_concurrency
        );
    }

    // Next request should be queued
    let queued = client
        .request_concurrency_ticket(
            0,
            "tenant",
            "queue",
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
    assert!(!queued, "request beyond limit should be queued");
}

#[silo::test]
async fn priority_ordering_preserved_for_queued_requests() {
    // Higher priority requests should be granted first
    let (_tmp, factory) = make_test_factory_with_shards(1);
    factory.open(0).await.expect("open shard 0");

    let shard = factory.get("0").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Fill the queue
    let granted = client
        .request_concurrency_ticket(
            0,
            "tenant",
            "queue",
            "job-holder",
            0,
            "req-holder",
            1,
            10,
            now,
            1,
            None,
        )
        .await
        .expect("holder request");
    assert!(granted);

    // Get holder_task_id and delete the notify task
    let result = shard.dequeue("internal", 1).await.expect("dequeue");
    let (holder_task_id, task_key) = match &result.cross_shard_tasks[0] {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            holder_task_id,
            task_key,
            ..
        } => (holder_task_id.clone(), task_key.clone()),
        _ => panic!("expected NotifyRemoteTicketGrant"),
    };
    // Delete the first notify task so it doesn't interfere
    shard
        .delete_cross_shard_task(&task_key)
        .await
        .expect("delete task");

    // Queue requests with different priorities (lower number = higher priority)
    // Request with priority 20 (lower priority)
    client
        .request_concurrency_ticket(0, "tenant", "queue", "job-low", 0, "req-low", 1, 20, now, 1, None)
        .await
        .expect("low priority request");

    // Request with priority 5 (higher priority)
    client
        .request_concurrency_ticket(
            0, "tenant", "queue", "job-high", 0, "req-high", 1, 5, now, 1, None,
        )
        .await
        .expect("high priority request");

    // Release the holder
    client
        .release_concurrency_ticket(0, "tenant", "queue", "job-holder", &holder_task_id)
        .await
        .expect("release");

    // Next grant should be for high priority job
    let result2 = shard.dequeue("internal", 10).await.expect("dequeue 2");
    let next_grant = result2.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant { .. }
        )
    });

    assert!(next_grant.is_some(), "should have next grant");
    match next_grant.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            job_id, ..
        } => {
            assert_eq!(
                job_id, "job-high",
                "higher priority job should be granted first"
            );
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// Routing Edge Cases
// ============================================================================

#[silo::test]
async fn routing_single_shard_always_zero() {
    // With 1 shard, all routing should return 0
    let num_shards = 1;

    for i in 0..100 {
        let job_id = format!("job-{}", i);
        assert_eq!(
            job_to_shard(&job_id, num_shards),
            0,
            "single shard cluster always routes to 0"
        );
    }

    for i in 0..100 {
        let tenant = format!("tenant-{}", i);
        assert_eq!(
            queue_to_shard(&tenant, "queue", num_shards),
            0,
            "single shard cluster always routes to 0"
        );
    }
}

#[silo::test]
async fn routing_distribution_reasonably_even() {
    // Test that routing distributes jobs roughly evenly across shards
    let num_shards = 8;
    let num_jobs = 1000;
    let mut counts = vec![0u32; num_shards as usize];

    for i in 0..num_jobs {
        let job_id = format!("test-job-{}", i);
        let shard = job_to_shard(&job_id, num_shards);
        counts[shard as usize] += 1;
    }

    // Each shard should have roughly num_jobs/num_shards = 125 jobs
    // Allow 50% variance for statistical noise
    let expected = num_jobs / num_shards;
    let min_acceptable = expected / 2;
    let max_acceptable = expected * 2;

    for (shard, &count) in counts.iter().enumerate() {
        assert!(
            count >= min_acceptable && count <= max_acceptable,
            "shard {} has {} jobs, expected roughly {} (range {}-{})",
            shard,
            count,
            expected,
            min_acceptable,
            max_acceptable
        );
    }
}

#[silo::test]
async fn remote_grant_preserves_attempt_number() {
    // This test verifies that when a retry attempt (attempt_number > 1) requests
    // a remote ticket, the grant notification correctly passes the attempt_number
    // so that the RunAttempt task is created with the correct attempt number.
    //
    // Use 2-shard factory so routing is respected
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let shard1 = factory.get("1").unwrap();
    let client = ClusterClient::new(factory.clone(), None);
    let now = now_ms();

    // Find a job_id that routes to shard 0 and a queue that routes to shard 1
    let job_id = find_job_for_shard(0, 2);
    let (tenant, queue_key) = find_queue_for_shard(1, 2);

    // Create a job on shard 0 that we'll pretend is on its second attempt
    shard0
        .enqueue(
            &tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "data"}),
            vec![],
            None,
        )
        .await
        .expect("enqueue");

    // First dequeue to get the job's initial RunAttempt task (clears it out)
    let initial_result = shard0.dequeue("worker", 10).await.expect("initial dequeue");
    assert_eq!(
        initial_result.tasks.len(),
        1,
        "should have initial RunAttempt"
    );

    // Simulate: request ticket for attempt 2 (this is a retry) on shard 1 (queue owner)
    let attempt_number = 2u32;
    let granted = client
        .request_concurrency_ticket(
            1, // queue_owner_shard (shard 1 owns the queue)
            &tenant,
            &queue_key,
            &job_id,
            0, // job_shard
            "req-retry",
            attempt_number, // This is the retry attempt number
            10,
            now,
            1,
            None,
        )
        .await
        .expect("request");
    assert!(granted, "should be granted when queue empty");

    // Dequeue from shard 1 to get the NotifyRemoteTicketGrant task
    let result = shard1.dequeue("internal", 10).await.expect("dequeue");
    let notify = result.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant { .. }
        )
    });
    assert!(notify.is_some(), "should have notify task");

    let (holder_task_id, req_id) = match notify.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            holder_task_id,
            request_id,
            ..
        } => (holder_task_id.clone(), request_id.clone()),
        _ => unreachable!(),
    };

    // Now simulate receiving the grant on shard 0 (job shard)
    // This should create a RunAttempt task with attempt_number = 2
    client
        .notify_concurrency_ticket_granted(
            0, // job_shard
            &tenant,
            &job_id,
            &queue_key,
            &req_id,
            &holder_task_id,
            1,              // queue_owner_shard
            attempt_number, // This is the key fix - pass the retry attempt number
        )
        .await
        .expect("notify grant");

    // Dequeue the RunAttempt task from shard 0 and check attempt_number
    let result2 = shard0.dequeue("worker", 10).await.expect("dequeue");
    let task = result2.tasks.iter().find(|t| t.job().id() == job_id);
    assert!(task.is_some(), "should have RunAttempt task");

    let attempt = task.unwrap().attempt();
    assert_eq!(
        attempt.attempt_number(),
        attempt_number,
        "RunAttempt should have attempt_number={} for retry, but got {}",
        attempt_number,
        attempt.attempt_number()
    );
}

/// When restarting a job with concurrency limits, it should go through
/// the ticket acquisition flow, not create a RunAttempt directly.
#[silo::test]
async fn restart_job_with_concurrency_goes_through_ticket_flow() {
    use silo::job::{ConcurrencyLimit, Limit};
    use silo::job_attempt::AttemptOutcome;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue a job with concurrency limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"test": "restart-concurrency"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "restart-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Dequeue and fail the job (no retries)
    let result = shard.dequeue("worker", 1).await.expect("dequeue");
    assert_eq!(result.tasks.len(), 1);
    let task_id = result.tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            "-",
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST_FAILURE".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report failure");

    // Confirm job is in Failed status
    let status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status");
    assert!(status.is_some());
    assert!(
        status.unwrap().is_terminal(),
        "job should be in terminal state"
    );

    // Now fill the concurrency slot with another job
    let blocker_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"test": "blocker"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "restart-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue blocker");

    // Dequeue the blocker to take the concurrency slot
    let result2 = shard.dequeue("worker", 1).await.expect("dequeue blocker");
    assert_eq!(result2.tasks.len(), 1);
    assert_eq!(result2.tasks[0].job().id(), blocker_id);

    // Now restart the original job
    shard.restart_job("-", &job_id).await.expect("restart");

    // The restarted job should NOT have a RunAttempt task immediately
    // because the concurrency slot is taken by the blocker.
    // Instead, it should have a RequestTicket task waiting.
    let result3 = shard
        .dequeue("worker", 10)
        .await
        .expect("dequeue after restart");

    // Should NOT find a RunAttempt for the restarted job yet
    let restarted_task = result3.tasks.iter().find(|t| t.job().id() == job_id);
    assert!(
        restarted_task.is_none(),
        "Restarted job with concurrency limit should NOT have RunAttempt while slot is taken. \
         The restart_job function should create a RequestTicket task instead."
    );

    // There should be a pending request for this job (RequestTicket was processed internally)
    // Check that a concurrency request exists for the job
    let requests_count = count_with_prefix(shard.db(), "requests/-/restart-queue/").await;
    assert!(
        requests_count > 0,
        "Should have a concurrency request for the restarted job"
    );
}

/// When a job has concurrency limits, its pending task may be a RequestTicket.
/// start_next_attempt_now should handle this case.
#[silo::test]
async fn start_now_works_with_request_ticket_tasks() {
    use silo::job::{ConcurrencyLimit, Limit};

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future_time = now + 60_000; // 1 minute in the future

    // Enqueue a job scheduled for the future with a concurrency limit
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            future_time, // Scheduled for future
            None,
            serde_json::json!({"test": "start-now"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "start-now-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Verify job is scheduled and has a pending task
    let status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status");
    assert!(status.is_some());
    assert_eq!(status.unwrap().kind, silo::job::JobStatusKind::Scheduled);

    // Try to start the job immediately
    let result = shard.start_next_attempt_now("-", &job_id).await;

    // This should succeed even though the pending task is a RequestTicket
    assert!(
        result.is_ok(),
        "start_next_attempt_now should handle RequestTicket tasks, but got error: {:?}",
        result.err()
    );

    // After start_now, we should be able to dequeue the job
    let dequeue_result = shard.dequeue("worker", 10).await.expect("dequeue");
    let task = dequeue_result.tasks.iter().find(|t| t.job().id() == job_id);
    assert!(
        task.is_some(),
        "Job should be available for dequeue after start_next_attempt_now"
    );
}

/// When a job has a REMOTE concurrency queue and is scheduled for the future,
/// start_next_attempt_now should handle the RequestRemoteTicket task.
/// This tests that start_now works with all task types, not just RunAttempt.
#[silo::test]
async fn start_now_works_with_request_remote_ticket_tasks() {
    use silo::job::{ConcurrencyLimit, Limit};
    use silo::routing::{job_to_shard, queue_to_shard};

    // We need a (job_id, tenant, queue) combination where:
    // - job routes to shard 0
    // - queue routes to shard 1
    let num_shards = 2;
    let tenant = "-";

    // Find a job_id that routes to shard 0
    let mut job_id = String::new();
    for i in 0..10000 {
        let candidate = format!("start-now-remote-job-{}", i);
        if job_to_shard(&candidate, num_shards) == 0 {
            job_id = candidate;
            break;
        }
    }
    assert!(!job_id.is_empty(), "Could not find job_id for shard 0");

    // Find a queue key that routes to shard 1 (different from job's shard)
    let mut queue_key = String::new();
    for i in 0..10000 {
        let candidate = format!("start-now-remote-queue-{}", i);
        if queue_to_shard(tenant, &candidate, num_shards) == 1 {
            queue_key = candidate;
            break;
        }
    }
    assert!(!queue_key.is_empty(), "Could not find queue for shard 1");

    // Verify our routing assumptions
    assert_eq!(job_to_shard(&job_id, num_shards), 0);
    assert_eq!(queue_to_shard(tenant, &queue_key, num_shards), 1);

    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let now = now_ms();
    let future_time = now + 60_000; // 1 minute in the future

    // Enqueue a job on shard 0 scheduled for the FUTURE with a concurrency limit that routes to shard 1
    let created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            future_time, // KEY: scheduled for future, so task won't be immediately processed
            None,
            serde_json::json!({"test": "start-now-remote"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue_key.clone(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(created_job_id, job_id);

    // Verify the job has a RequestRemoteTicket task (not RunAttempt) in the DB
    // This task is scheduled for future_time, so it's not yet ready
    let start: Vec<u8> = b"tasks/".to_vec();
    let mut end: Vec<u8> = b"tasks/".to_vec();
    end.push(0xFF);
    let mut iter: slatedb::DbIterator = shard0.db().scan::<Vec<u8>, _>(start..=end).await.unwrap();

    let mut has_remote_ticket_task = false;
    while let Some(kv) = iter.next().await.unwrap() {
        let decoded = silo::codec::decode_task(&kv.value).expect("decode task");
        if let silo::task::Task::RequestRemoteTicket { job_id: jid, .. } = &decoded {
            if jid == &job_id {
                has_remote_ticket_task = true;
                break;
            }
        }
    }

    assert!(
        has_remote_ticket_task,
        "Job with future start time and remote queue should have RequestRemoteTicket task"
    );

    // Verify job is scheduled
    let status = shard0
        .get_job_status(tenant, &job_id)
        .await
        .expect("get status");
    assert!(status.is_some());
    assert_eq!(status.unwrap().kind, silo::job::JobStatusKind::Scheduled);

    // Try to start the job immediately - this should work even with RequestRemoteTicket task
    // BUG: Currently start_next_attempt_now only looks for RunAttempt tasks!
    let result = shard0.start_next_attempt_now(tenant, &job_id).await;

    // This assertion will FAIL with the current buggy implementation
    // because find_task_for_job only returns RunAttempt tasks
    assert!(
        result.is_ok(),
        "start_next_attempt_now should handle RequestRemoteTicket tasks, but got error: {:?}. \
         The find_task_for_job function only looks for RunAttempt tasks, \
         but jobs with remote concurrency queues have RequestRemoteTicket tasks.",
        result.err()
    );
}

/// When a job is enqueued on shard A with a concurrency limit that routes to shard B,
/// the enqueue should create a RequestRemoteTicket task (not a local RequestTicket).
/// This test verifies that cross-shard concurrency routing works correctly at enqueue time.
#[silo::test]
async fn enqueue_with_remote_concurrency_queue_creates_remote_request() {
    use silo::job::{ConcurrencyLimit, Limit};
    use silo::routing::{job_to_shard, queue_to_shard};

    // We need to find a (job_id, tenant, queue) combination where:
    // - job routes to shard 0
    // - queue routes to shard 1
    let num_shards = 2;

    // Find a job_id that routes to shard 0
    let mut job_id = String::new();
    let tenant = "-";
    for i in 0..10000 {
        let candidate = format!("job-{}", i);
        if job_to_shard(&candidate, num_shards) == 0 {
            job_id = candidate;
            break;
        }
    }
    assert!(!job_id.is_empty(), "Could not find job_id for shard 0");

    // Find a queue key that routes to shard 1 (different from job's shard)
    let mut queue_key = String::new();
    for i in 0..10000 {
        let candidate = format!("queue-{}", i);
        if queue_to_shard(tenant, &candidate, num_shards) == 1 {
            queue_key = candidate;
            break;
        }
    }
    assert!(!queue_key.is_empty(), "Could not find queue for shard 1");

    // Verify our routing assumptions
    assert_eq!(job_to_shard(&job_id, num_shards), 0);
    assert_eq!(queue_to_shard(tenant, &queue_key, num_shards), 1);

    // Set up a 2-shard cluster - each shard knows it's in a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let now = now_ms();

    // Enqueue a job on shard 0 with a concurrency limit that routes to shard 1
    let created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            None,
            serde_json::json!({"test": "remote-queue"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue_key.clone(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(created_job_id, job_id);

    // Dequeue from shard 0 - should get a cross-shard task, not a local RunAttempt
    let result = shard0.dequeue("worker", 10).await.expect("dequeue");

    // The bug: with the current implementation, we might get a local RunAttempt
    // (because enqueue doesn't check if the queue is remote and creates RequestTicket locally)
    //
    // The fix should make this work correctly:
    // - We should get a RequestRemoteTicket cross-shard task
    // - We should NOT get a RunAttempt (that would bypass the queue owner shard)
    let got_run_attempt = result.tasks.iter().any(|t| t.job().id() == job_id);
    let got_remote_ticket_request = result.cross_shard_tasks.iter().any(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket {
                job_id: jid,
                queue_key: qk,
                ..
            } if jid == &job_id && qk == &queue_key
        )
    });

    // This assertion will FAIL with the current buggy implementation
    // because enqueue() doesn't check queue routing and always uses local concurrency
    assert!(
        !got_run_attempt || got_remote_ticket_request,
        "Job with remote concurrency queue should NOT have local RunAttempt. \
         Expected RequestRemoteTicket cross-shard task. \
         Got {} tasks, {} cross-shard tasks. \
         Job routes to shard {}, queue routes to shard {}.",
        result.tasks.len(),
        result.cross_shard_tasks.len(),
        job_to_shard(&job_id, num_shards),
        queue_to_shard(tenant, &queue_key, num_shards)
    );

    // If the fix is implemented correctly:
    // - We should have a RequestRemoteTicket cross-shard task
    assert!(
        got_remote_ticket_request,
        "Expected RequestRemoteTicket cross-shard task for remote queue. \
         The enqueue should detect that queue '{}' routes to shard 1 (not shard 0) \
         and create a RequestRemoteTicket task.",
        queue_key
    );
}

/// When a job with a REMOTE concurrency queue fails and retries, the retry logic
/// should create a RequestRemoteTicket task (not RequestTicket).
/// This test verifies that report_attempt_outcome correctly routes retry tasks
/// for remote queues.
#[silo::test]
async fn retry_with_remote_queue_creates_remote_request_ticket() {
    use silo::codec::decode_task;
    use silo::job::{ConcurrencyLimit, Limit};
    use silo::job_attempt::AttemptOutcome;
    use silo::routing::{job_to_shard, queue_to_shard};
    use silo::task::Task;

    // We need a (job_id, tenant, queue) combination where:
    // - job routes to shard 0
    // - queue routes to shard 1
    let num_shards = 2;

    // Find a job_id that routes to shard 0
    let mut job_id = String::new();
    let tenant = "-";
    for i in 0..10000 {
        let candidate = format!("retry-remote-job-{}", i);
        if job_to_shard(&candidate, num_shards) == 0 {
            job_id = candidate;
            break;
        }
    }
    assert!(!job_id.is_empty(), "Could not find job_id for shard 0");

    // Find a queue key that routes to shard 1 (different from job's shard)
    let mut queue_key = String::new();
    for i in 0..10000 {
        let candidate = format!("retry-remote-queue-{}", i);
        if queue_to_shard(tenant, &candidate, num_shards) == 1 {
            queue_key = candidate;
            break;
        }
    }
    assert!(!queue_key.is_empty(), "Could not find queue for shard 1");

    // Verify our routing assumptions
    assert_eq!(
        job_to_shard(&job_id, num_shards),
        0,
        "job should route to shard 0"
    );
    assert_eq!(
        queue_to_shard(tenant, &queue_key, num_shards),
        1,
        "queue should route to shard 1"
    );

    // Set up a 2-shard cluster
    let (_tmp, factory) = make_test_factory_with_shards(2);
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let shard0 = factory.get("0").unwrap();
    let now = now_ms();

    // Enqueue a job on shard 0 with a concurrency limit that routes to shard 1
    // Also include a retry policy so that when it fails, it will create a retry task
    let created_job_id = shard0
        .enqueue(
            tenant,
            Some(job_id.clone()),
            10u8,
            now,
            Some(silo::retry::RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1, // Very short retry delay
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            serde_json::json!({"test": "retry-remote-queue"}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue_key.clone(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(created_job_id, job_id);

    // First, dequeue to get the RequestRemoteTicket cross-shard task (from enqueue)
    let result1 = shard0.dequeue("worker", 10).await.expect("first dequeue");

    // Find the RequestRemoteTicket task and get its task_key so we can delete it
    let remote_request = result1.cross_shard_tasks.iter().find(|t| {
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
        "enqueue should create RequestRemoteTicket for remote queue"
    );

    // Get the task_key to delete after we simulate the RPC
    let original_task_key = match remote_request.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::RequestRemoteTicket { task_key, .. } => {
            task_key.clone()
        }
        _ => unreachable!(),
    };

    // Now simulate the full flow: request ticket, get grant, dequeue RunAttempt
    let client = ClusterClient::new(factory.clone(), None);

    // The request was already made via the cross-shard task; now simulate it directly
    let granted = client
        .request_concurrency_ticket(1, tenant, &queue_key, &job_id, 0, "req-1", 1, 10, now, 1, None)
        .await
        .expect("request ticket");
    assert!(granted, "ticket should be granted");

    // Delete the original RequestRemoteTicket task (simulating what server.rs does after successful RPC)
    shard0
        .delete_cross_shard_task(&original_task_key)
        .await
        .expect("delete original task");

    // Get the notification task from shard 1 and process it
    let shard1 = factory.get("1").unwrap();
    let result_shard1 = shard1
        .dequeue("internal", 10)
        .await
        .expect("dequeue shard 1");

    let notify = result_shard1.cross_shard_tasks.iter().find(|t| {
        matches!(
            t,
            silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant { .. }
        )
    });
    assert!(notify.is_some(), "should have notify task");

    let (holder_task_id, req_id, notify_task_key) = match notify.unwrap() {
        silo::job_store_shard::PendingCrossShardTask::NotifyRemoteTicketGrant {
            holder_task_id,
            request_id,
            task_key,
            ..
        } => (holder_task_id.clone(), request_id.clone(), task_key.clone()),
        _ => unreachable!(),
    };

    // Delete the notify task after processing
    shard1
        .delete_cross_shard_task(&notify_task_key)
        .await
        .expect("delete notify task");

    // Notify job shard of grant
    client
        .notify_concurrency_ticket_granted(
            0,
            tenant,
            &job_id,
            &queue_key,
            &req_id,
            &holder_task_id,
            1,
            1,
        )
        .await
        .expect("notify grant");

    // Dequeue the RunAttempt from shard 0
    let result2 = shard0
        .dequeue("worker", 10)
        .await
        .expect("dequeue RunAttempt");
    let task = result2.tasks.iter().find(|t| t.job().id() == job_id);
    assert!(task.is_some(), "should have RunAttempt");
    let task_id = task.unwrap().attempt().task_id().to_string();

    // Now FAIL the job - this should create a retry task
    shard0
        .report_attempt_outcome(
            tenant,
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST_FAILURE".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report failure");

    // Check what task was created for the retry
    // It SHOULD be a RequestRemoteTicket task (since queue routes to shard 1)
    // BUG: Currently it creates a RequestTicket task instead!

    // Scan for the retry task
    let start: Vec<u8> = b"tasks/".to_vec();
    let mut end: Vec<u8> = b"tasks/".to_vec();
    end.push(0xFF);
    let mut iter: slatedb::DbIterator = shard0.db().scan::<Vec<u8>, _>(start..=end).await.unwrap();

    let mut found_retry_task = false;
    let mut task_type = String::new();

    while let Some(kv) = iter.next().await.unwrap() {
        let decoded = decode_task(&kv.value).expect("decode task");
        match &decoded {
            Task::RequestRemoteTicket {
                job_id: jid,
                queue_key: qk,
                attempt_number,
                ..
            } if jid == &job_id => {
                assert_eq!(*attempt_number, 2, "retry should be attempt 2");
                assert_eq!(qk, &queue_key, "should be for the same queue");
                found_retry_task = true;
                task_type = "RequestRemoteTicket".to_string();
            }
            Task::RequestTicket {
                job_id: jid,
                queue,
                attempt_number,
                ..
            } if jid == &job_id => {
                // This is the BUG - it creates RequestTicket instead of RequestRemoteTicket
                assert_eq!(*attempt_number, 2, "retry should be attempt 2");
                assert_eq!(queue, &queue_key, "should be for the same queue");
                found_retry_task = true;
                task_type = "RequestTicket".to_string();
            }
            _ => {}
        }
    }

    assert!(
        found_retry_task,
        "Should have found a retry task for the job"
    );

    // THE ACTUAL ASSERTION - this will FAIL with the current buggy implementation
    assert_eq!(
        task_type,
        "RequestRemoteTicket",
        "Retry task for job with remote queue should be RequestRemoteTicket, but got {}. \
         Job routes to shard {}, queue routes to shard {}. \
         The retry logic in lease.rs always creates RequestTicket regardless of queue routing.",
        task_type,
        job_to_shard(&job_id, num_shards),
        queue_to_shard(tenant, &queue_key, num_shards)
    );
}

/// When a job has concurrency limits fails and retries, the tickets are released.
/// The retry task should NOT claim to hold those tickets - it needs to re-acquire them.
#[silo::test]
async fn retry_reacquires_concurrency_tickets() {
    use silo::job::{ConcurrencyLimit, Limit};
    use silo::job_attempt::AttemptOutcome;

    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "retry-queue".to_string();

    // Enqueue first job with concurrency limit and retry policy
    let job1_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(silo::retry::RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1, // Very short retry delay
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            serde_json::json!({"job": 1}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue job1");

    // Dequeue and fail job1
    let result1 = shard.dequeue("worker", 1).await.expect("dequeue job1");
    assert_eq!(result1.tasks.len(), 1);
    let task1_id = result1.tasks[0].attempt().task_id().to_string();

    // Fail the job - this should release the ticket AND schedule a retry
    shard
        .report_attempt_outcome(
            "-",
            &task1_id,
            AttemptOutcome::Error {
                error_code: "TEST_FAILURE".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report failure");

    // Now enqueue a second job competing for the same concurrency slot
    let job2_id = shard
        .enqueue(
            "-",
            None,
            5u8, // Higher priority (lower number) than job1's retry
            now,
            None,
            serde_json::json!({"job": 2}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue job2");

    // The ticket was released by job1 on failure, so job2 should be able to get it
    // (grant_next should have given it to job2 since it queued a request)
    let result2 = shard
        .dequeue("worker", 10)
        .await
        .expect("dequeue after retry");

    // Count how many tasks we got for each job
    let job1_tasks: Vec<_> = result2
        .tasks
        .iter()
        .filter(|t| t.job().id() == job1_id)
        .collect();
    let job2_tasks: Vec<_> = result2
        .tasks
        .iter()
        .filter(|t| t.job().id() == job2_id)
        .collect();

    // With max_concurrency=1, at most ONE of the two jobs should have a RunAttempt task
    // If the bug exists (retry task has stale held_queues), we might see both tasks dequeued
    // which would violate the concurrency limit.
    let total_runnable = job1_tasks.len() + job2_tasks.len();
    assert!(
        total_runnable <= 1,
        "With max_concurrency=1, at most 1 job should be runnable. \
         Got {} runnable tasks (job1: {}, job2: {}). \
         If job1's retry has stale held_queues, it may run without actually holding the ticket.",
        total_runnable,
        job1_tasks.len(),
        job2_tasks.len()
    );

    // Verify that concurrency is actually enforced:
    // The holder count should be exactly 1 (whoever got the ticket)
    let holder_count = count_with_prefix(shard.db(), &format!("holders/-/{}/", queue)).await;
    assert_eq!(
        holder_count, 1,
        "Should have exactly 1 holder for max_concurrency=1 queue"
    );
}
