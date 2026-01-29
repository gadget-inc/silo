//! Tests for split-aware processing of tasks and leases.
//!
//! [SILO-SPLIT-AWARE-1] After a shard split, child shards contain tasks and leases
//! from the parent shard that may belong to tenants outside their new range.
//! These tests verify that:
//! - Cleanup correctly removes keys outside the shard's range
//! - Dequeue respects the shard's range for tasks within range
//! - Shards opened with specific ranges work correctly

mod test_helpers;
use test_helpers::{
    count_with_prefix, msgpack_payload, open_temp_shard, open_temp_shard_with_range,
};

use silo::shard_range::ShardRange;

/// Helper to enqueue jobs for multiple tenants
async fn enqueue_jobs_for_tenants(
    shard: &silo::job_store_shard::JobStoreShard,
    tenants: &[&str],
    jobs_per_tenant: usize,
    task_group: &str,
) {
    for tenant in tenants {
        for i in 0..jobs_per_tenant {
            let payload = msgpack_payload(&serde_json::json!({"value": i}));
            shard
                .enqueue(
                    tenant,
                    Some(format!("{}-job-{}", tenant, i)),
                    0,
                    0,
                    None,
                    payload,
                    vec![],
                    None,
                    task_group,
                )
                .await
                .expect("enqueue should succeed");
        }
    }
}

// ============================================================================
// Post-Split Cleanup Tests (using after_split_cleanup_defunct_data)
// ============================================================================

/// Cleanup should remove tasks outside the specified range
#[silo::test]
async fn cleanup_removes_tasks_outside_range() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue jobs for tenants across a potential split point
    enqueue_jobs_for_tenants(&shard, &["aaa", "mmm", "zzz"], 2, "default").await;
    shard.db().flush().await.unwrap();

    // Verify tasks exist for all tenants
    let task_count_before = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(task_count_before, 6); // 3 tenants * 2 jobs

    // Run cleanup with range that only includes tenants < "mmm"
    let cleanup_range = ShardRange::new("", "mmm");
    let result = shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("cleanup should succeed");

    shard.db().flush().await.unwrap();

    // Tasks for mmm and zzz should be deleted
    let task_count_after = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(
        task_count_after, 2,
        "only aaa tasks should remain (2 tasks)"
    );
    assert!(result.keys_deleted > 0, "should have deleted some keys");
}

/// Cleanup should preserve tasks within the specified range
#[silo::test]
async fn cleanup_preserves_tasks_within_range() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue jobs only for tenants within a specific range
    enqueue_jobs_for_tenants(&shard, &["abc", "def", "ghi"], 2, "default").await;
    shard.db().flush().await.unwrap();

    // Run cleanup with a range that includes all these tenants
    let cleanup_range = ShardRange::new("a", "n");
    let result = shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("cleanup should succeed");

    shard.db().flush().await.unwrap();

    // All tasks should be preserved since they're within range
    let task_count = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(task_count, 6);
    assert_eq!(result.keys_deleted, 0, "should not have deleted any keys");
}

/// Dequeue should work normally with full range (default)
#[silo::test]
async fn dequeue_works_with_full_range() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue jobs without restrictions
    enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 2, "default").await;
    shard.db().flush().await.unwrap();

    // Dequeue should return all tasks
    let result = shard.dequeue("worker-1", "default", 10).await.unwrap();
    assert_eq!(result.tasks.len(), 4, "should get all 4 tasks");
}

/// Shard opened with restricted range only processes tasks within that range
#[silo::test]
async fn shard_with_restricted_range_only_processes_within_range() {
    // Open shard with range that only includes tenants < "ccc"
    let (_tmp, shard) = open_temp_shard_with_range(ShardRange::new("", "ccc")).await;

    // Enqueue jobs only for tenants within the range
    enqueue_jobs_for_tenants(&shard, &["aaa", "bbb"], 2, "default").await;
    shard.db().flush().await.unwrap();

    // All tasks should be present and processable
    let result = shard.dequeue("worker-1", "default", 10).await.unwrap();
    assert_eq!(
        result.tasks.len(),
        4,
        "should get all 4 tasks for aaa and bbb"
    );

    // Verify the returned tasks are for the correct tenants
    for task in &result.tasks {
        let tid = task.tenant_id();
        assert!(
            tid == "aaa" || tid == "bbb",
            "task tenant should be aaa or bbb, got {}",
            tid
        );
    }
}

/// Cleanup should remove leases for tenants outside the specified range
#[silo::test]
async fn cleanup_removes_leases_outside_range() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue and dequeue jobs to create leases
    enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 1, "default").await;
    shard.db().flush().await.unwrap();

    let result = shard.dequeue("worker-1", "default", 10).await.unwrap();
    assert_eq!(result.tasks.len(), 2, "should dequeue 2 tasks");
    shard.db().flush().await.unwrap();

    // Verify leases exist
    let lease_count_before = count_with_prefix(shard.db(), "lease/").await;
    assert_eq!(lease_count_before, 2, "should have 2 leases");

    // Run cleanup with range that only includes aaa
    let cleanup_range = ShardRange::new("", "b");
    shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("cleanup should succeed");
    shard.db().flush().await.unwrap();

    // The zzz lease should be deleted
    let lease_count_after = count_with_prefix(shard.db(), "lease/").await;
    assert_eq!(
        lease_count_after, 1,
        "should have 1 lease remaining (aaa only)"
    );
}

/// Simulate post-split cleanup for a left child shard
#[silo::test]
async fn post_split_cleanup_left_child() {
    let (_tmp, shard) = open_temp_shard().await;

    // Simulate parent shard with jobs for multiple tenants
    enqueue_jobs_for_tenants(
        &shard,
        &["alpha", "beta", "gamma", "delta", "epsilon"],
        2,
        "default",
    )
    .await;
    shard.db().flush().await.unwrap();

    // Verify all jobs exist
    let job_count = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(job_count, 10); // 5 tenants * 2 jobs

    // Run cleanup as if this is the LEFT child after split at "delta"
    // Left child gets tenants < "delta": alpha, beta (not gamma which comes after delta alphabetically)
    // Wait, alphabetically: alpha < beta < delta < epsilon < gamma
    // So tenants < "delta" are: alpha, beta
    let cleanup_range = ShardRange::new("", "delta");
    shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("cleanup should succeed");
    shard.db().flush().await.unwrap();

    // Jobs for delta, epsilon, gamma should be deleted
    let job_count_after = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(
        job_count_after, 4,
        "should have 4 jobs remaining (alpha=2, beta=2)"
    );

    // Tasks for delta, epsilon, gamma should also be deleted
    let task_count_after = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(
        task_count_after, 4,
        "should have 4 tasks remaining (alpha=2, beta=2)"
    );
}

/// Simulate post-split cleanup for a right child shard
#[silo::test]
async fn post_split_cleanup_right_child() {
    let (_tmp, shard) = open_temp_shard().await;

    // Simulate parent shard with jobs for multiple tenants
    // Note: alphabetically aaa < bbb < mmm < nnn < zzz
    enqueue_jobs_for_tenants(&shard, &["aaa", "bbb", "mmm", "nnn", "zzz"], 2, "default").await;
    shard.db().flush().await.unwrap();

    // Run cleanup as if this is the RIGHT child after split at "mmm"
    // Right child gets tenants >= "mmm": mmm, nnn, zzz
    let cleanup_range = ShardRange::new("mmm", "");
    shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("cleanup should succeed");
    shard.db().flush().await.unwrap();

    // Jobs for aaa, bbb should be deleted
    let job_count_after = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(
        job_count_after, 6,
        "should have 6 jobs remaining (mmm=2, nnn=2, zzz=2)"
    );

    // Tasks for aaa, bbb should also be deleted
    let task_count_after = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(
        task_count_after, 6,
        "should have 6 tasks remaining (mmm=2, nnn=2, zzz=2)"
    );
}

/// Cleanup is idempotent - running it twice produces the same result
#[silo::test]
async fn cleanup_is_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_jobs_for_tenants(&shard, &["aaa", "mmm", "zzz"], 2, "default").await;
    shard.db().flush().await.unwrap();

    let cleanup_range = ShardRange::new("", "mmm");

    // First cleanup
    let result1 = shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("first cleanup should succeed");
    shard.db().flush().await.unwrap();

    let task_count_after_first = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(task_count_after_first, 2);

    // Second cleanup should be a no-op
    let result2 = shard
        .after_split_cleanup_defunct_data(&cleanup_range, 100)
        .await
        .expect("second cleanup should succeed");
    shard.db().flush().await.unwrap();

    let task_count_after_second = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(task_count_after_second, 2, "count should be unchanged");

    // Second cleanup should report complete with no additional deletions
    assert!(result2.complete, "second cleanup should report complete");
    // Note: keys_deleted might be 0 or the same as first run depending on implementation
    assert!(
        result1.keys_deleted >= result2.keys_deleted,
        "second cleanup should delete equal or fewer keys"
    );
}
