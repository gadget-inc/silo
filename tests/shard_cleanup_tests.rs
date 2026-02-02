mod test_helpers;
use test_helpers::{count_with_prefix, msgpack_payload, open_temp_shard};

use silo::shard_range::ShardRange;

/// Helper to enqueue jobs for multiple tenants
async fn enqueue_jobs_for_tenants(
    shard: &silo::job_store_shard::JobStoreShard,
    tenants: &[&str],
    jobs_per_tenant: usize,
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
                    "default",
                )
                .await
                .expect("enqueue should succeed");
        }
    }
}

#[silo::test]
async fn cleanup_removes_keys_outside_range() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue jobs for tenants a, m, and z
    // After split at "m", left shard gets [, m) and right shard gets [m, )
    enqueue_jobs_for_tenants(&shard, &["aaa", "bbb", "lll", "mmm", "nnn", "zzz"], 2).await;
    shard.db().flush().await.unwrap();

    // Verify jobs exist for all tenants before cleanup
    let count_before = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(count_before, 12); // 6 tenants * 2 jobs

    // Simulate this shard becoming the LEFT child after a split at "m"
    // Left child should have range [, m) - keeps tenants < "m"
    let left_range = ShardRange::new("", "mmm"); // Exclusive end

    // Run cleanup to remove keys outside range
    let result = shard
        .after_split_cleanup_defunct_data(&left_range, 10)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);
    assert!(result.keys_scanned > 0);
    assert!(result.keys_deleted > 0);

    // Flush to ensure deletes are visible
    shard.db().flush().await.unwrap();

    // Verify only left-range tenants remain
    // aaa, bbb, lll should remain (3 tenants * 2 jobs = 6)
    // mmm, nnn, zzz should be deleted
    let count_after = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(
        count_after, 6,
        "should have 6 job records (3 tenants * 2 jobs)"
    );

    // Verify specific tenants
    let aaa_jobs = count_with_prefix(shard.db(), "jobs/aaa/").await;
    let mmm_jobs = count_with_prefix(shard.db(), "jobs/mmm/").await;
    let zzz_jobs = count_with_prefix(shard.db(), "jobs/zzz/").await;

    assert_eq!(aaa_jobs, 2, "aaa jobs should remain");
    assert_eq!(mmm_jobs, 0, "mmm jobs should be deleted");
    assert_eq!(zzz_jobs, 0, "zzz jobs should be deleted");
}

#[silo::test]
async fn cleanup_removes_status_and_index_keys() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue jobs for tenants on both sides of a split point
    enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 3).await;
    shard.db().flush().await.unwrap();

    // Verify job_status keys exist
    let status_before = count_with_prefix(shard.db(), "job_status/").await;
    assert_eq!(status_before, 6);

    // Verify idx/status_ts keys exist
    let idx_before = count_with_prefix(shard.db(), "idx/status_ts/").await;
    assert_eq!(idx_before, 6);

    // Run cleanup for LEFT child range (tenants < "mmm")
    let left_range = ShardRange::new("", "mmm");
    let result = shard
        .after_split_cleanup_defunct_data(&left_range, 10)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);
    shard.db().flush().await.unwrap();

    // Verify only aaa tenant keys remain
    let status_after = count_with_prefix(shard.db(), "job_status/").await;
    assert_eq!(status_after, 3, "only aaa status records should remain");

    let idx_after = count_with_prefix(shard.db(), "idx/status_ts/aaa/").await;
    assert_eq!(idx_after, 3, "only aaa index records should remain");

    let idx_zzz = count_with_prefix(shard.db(), "idx/status_ts/zzz/").await;
    assert_eq!(idx_zzz, 0, "zzz index records should be deleted");
}

#[silo::test]
async fn cleanup_handles_right_child_range() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue jobs for tenants on both sides of a split point
    enqueue_jobs_for_tenants(&shard, &["aaa", "bbb", "mmm", "nnn", "zzz"], 2).await;
    shard.db().flush().await.unwrap();

    // Run cleanup for RIGHT child range (tenants >= "mmm")
    let right_range = ShardRange::new("mmm", ""); // Unbounded end
    let result = shard
        .after_split_cleanup_defunct_data(&right_range, 10)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);
    shard.db().flush().await.unwrap();

    // Verify only right-range tenants remain (mmm, nnn, zzz)
    let count_after = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(count_after, 6, "should have 6 job records (3 tenants * 2)");

    // Verify specific tenants
    let aaa_jobs = count_with_prefix(shard.db(), "jobs/aaa/").await;
    let bbb_jobs = count_with_prefix(shard.db(), "jobs/bbb/").await;
    let mmm_jobs = count_with_prefix(shard.db(), "jobs/mmm/").await;
    let zzz_jobs = count_with_prefix(shard.db(), "jobs/zzz/").await;

    assert_eq!(aaa_jobs, 0, "aaa jobs should be deleted");
    assert_eq!(bbb_jobs, 0, "bbb jobs should be deleted");
    assert_eq!(mmm_jobs, 2, "mmm jobs should remain");
    assert_eq!(zzz_jobs, 2, "zzz jobs should remain");
}

#[silo::test]
async fn cleanup_is_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 3).await;
    shard.db().flush().await.unwrap();

    let range = ShardRange::new("", "mmm");

    // First cleanup
    let result1 = shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("first cleanup should succeed");
    assert!(result1.complete);
    let deleted1 = result1.keys_deleted;

    // Second cleanup should be a no-op since it's already complete
    let result2 = shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("second cleanup should succeed");
    assert!(result2.complete);
    // Should report same totals but not delete anything new
    assert_eq!(
        result2.keys_deleted, deleted1,
        "second cleanup should report same count"
    );

    // Verify is_split_cleanup_complete returns true
    let is_complete = shard
        .is_split_cleanup_complete()
        .await
        .expect("should check cleanup status");
    assert!(is_complete);
}

#[silo::test]
async fn cleanup_progress_tracks_work() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue enough jobs to verify batching
    enqueue_jobs_for_tenants(&shard, &["aaa", "bbb", "mmm", "nnn"], 5).await;
    shard.db().flush().await.unwrap();

    let range = ShardRange::new("", "mmm");

    // Before cleanup, no pending work
    let pending_before = shard
        .has_pending_split_cleanup()
        .await
        .expect("check pending cleanup");
    assert!(!pending_before);

    // Run cleanup with small batch size
    let result = shard
        .after_split_cleanup_defunct_data(&range, 3)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);
    assert!(result.keys_scanned > 0);

    // After complete cleanup, no pending work
    let pending_after = shard
        .has_pending_split_cleanup()
        .await
        .expect("check pending cleanup");
    assert!(!pending_after);
}

#[silo::test]
async fn full_compaction_clears_cleanup_progress() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 2).await;
    shard.db().flush().await.unwrap();

    let range = ShardRange::new("", "mmm");

    // Run cleanup
    let result = shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("cleanup should succeed");
    assert!(result.complete);

    // Verify cleanup is marked complete
    let is_complete = shard
        .is_split_cleanup_complete()
        .await
        .expect("check cleanup status");
    assert!(is_complete);

    // Run compaction
    shard
        .run_full_compaction()
        .await
        .expect("compaction should succeed");

    // After compaction, cleanup progress should be cleared
    // (next split would start fresh)
    let is_complete_after = shard
        .is_split_cleanup_complete()
        .await
        .expect("check cleanup status");
    assert!(
        !is_complete_after,
        "cleanup progress should be cleared after compaction"
    );
}

#[silo::test]
async fn cleanup_empty_shard_succeeds() {
    let (_tmp, shard) = open_temp_shard().await;

    let range = ShardRange::new("", "mmm");

    let result = shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("cleanup should succeed on empty shard");

    assert!(result.complete);
    assert_eq!(result.keys_scanned, 0);
    assert_eq!(result.keys_deleted, 0);
}

#[silo::test]
async fn cleanup_full_range_deletes_nothing() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_jobs_for_tenants(&shard, &["aaa", "mmm", "zzz"], 2).await;
    shard.db().flush().await.unwrap();

    let count_before = count_with_prefix(shard.db(), "jobs/").await;

    // Full range means everything is in range
    let full_range = ShardRange::full();

    let result = shard
        .after_split_cleanup_defunct_data(&full_range, 10)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);
    assert!(result.keys_scanned > 0);
    assert_eq!(result.keys_deleted, 0, "nothing should be deleted");

    shard.db().flush().await.unwrap();
    let count_after = count_with_prefix(shard.db(), "jobs/").await;
    assert_eq!(
        count_after, count_before,
        "all jobs should remain after cleanup"
    );
}

#[silo::test]
async fn cleanup_handles_escaped_tenant_ids() {
    let (_tmp, shard) = open_temp_shard().await;

    // Tenant with special characters that need escaping
    let tenants = &[
        "tenant/with/slashes",
        "tenant%with%percent",
        "normal-tenant",
    ];

    for tenant in tenants {
        let payload = msgpack_payload(&serde_json::json!({"data": "test"}));
        shard
            .enqueue(
                tenant,
                Some(format!("{}-job", tenant)),
                0,
                0,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue should succeed");
    }
    shard.db().flush().await.unwrap();

    // Use a range that should include "normal-tenant" but exclude the others
    // The escaped versions sort differently than unescaped
    let range = ShardRange::new("normal", "normz");

    let result = shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);
    shard.db().flush().await.unwrap();

    // Verify normal-tenant jobs remain
    let normal_jobs = count_with_prefix(shard.db(), "jobs/normal-tenant/").await;
    assert_eq!(normal_jobs, 1, "normal-tenant job should remain");
}

#[test]
fn test_extract_tenant_from_key() {
    use silo::job_store_shard::extract_tenant_from_key;

    // jobs/{tenant}/{id}
    assert_eq!(
        extract_tenant_from_key("jobs/tenant123/job456", "jobs/"),
        Some("tenant123".to_string())
    );

    // job_status/{tenant}/{id}
    assert_eq!(
        extract_tenant_from_key("job_status/my-tenant/job789", "job_status/"),
        Some("my-tenant".to_string())
    );

    // idx/status_ts/{tenant}/...
    assert_eq!(
        extract_tenant_from_key(
            "idx/status_ts/tenant-abc/running/00000001/job1",
            "idx/status_ts/"
        ),
        Some("tenant-abc".to_string())
    );

    // Escaped tenant
    assert_eq!(
        extract_tenant_from_key("jobs/tenant%2Fwith%2Fslashes/job1", "jobs/"),
        Some("tenant%2Fwith%2Fslashes".to_string())
    );

    // No match
    assert_eq!(extract_tenant_from_key("other/key", "jobs/"), None);
}

/// Test that cleanup status is stored in and retrieved from the shard database
#[silo::test]
async fn cleanup_status_stored_in_shard_database() {
    use silo::coordination::SplitCleanupStatus;

    let (_tmp, shard) = open_temp_shard().await;

    // Initially, cleanup status should be CompactionDone (default)
    let initial_status = shard
        .get_cleanup_status()
        .await
        .expect("get initial cleanup status");
    assert_eq!(initial_status, SplitCleanupStatus::CompactionDone);

    // Set cleanup status to CleanupPending
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupPending)
        .await
        .expect("set cleanup pending");

    let status = shard
        .get_cleanup_status()
        .await
        .expect("get cleanup status");
    assert_eq!(status, SplitCleanupStatus::CleanupPending);

    // Set to CleanupRunning
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupRunning)
        .await
        .expect("set cleanup running");

    let status = shard
        .get_cleanup_status()
        .await
        .expect("get cleanup status");
    assert_eq!(status, SplitCleanupStatus::CleanupRunning);

    // Set to CleanupDone
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupDone)
        .await
        .expect("set cleanup done");

    let status = shard
        .get_cleanup_status()
        .await
        .expect("get cleanup status");
    assert_eq!(status, SplitCleanupStatus::CleanupDone);

    // Set to CompactionDone
    shard
        .set_cleanup_status(SplitCleanupStatus::CompactionDone)
        .await
        .expect("set compaction done");

    let status = shard
        .get_cleanup_status()
        .await
        .expect("get cleanup status");
    assert_eq!(status, SplitCleanupStatus::CompactionDone);
}

/// Test that after_split_cleanup_defunct_data sets cleanup status correctly
#[silo::test]
async fn cleanup_sets_status_to_running_then_done() {
    use silo::coordination::SplitCleanupStatus;
    use silo::shard_range::ShardRange;

    let (_tmp, shard) = open_temp_shard().await;

    // Set initial status to CleanupPending (as if just created from split)
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupPending)
        .await
        .expect("set cleanup pending");

    let range = ShardRange::new("", "mmm");

    // Run cleanup (this should set status to CleanupRunning, then CleanupDone)
    let result = shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("cleanup should succeed");

    assert!(result.complete);

    // After cleanup, status should be CleanupDone
    let status = shard
        .get_cleanup_status()
        .await
        .expect("get cleanup status");
    assert_eq!(status, SplitCleanupStatus::CleanupDone);
}

/// Test that run_full_compaction sets cleanup status to CompactionDone
#[silo::test]
async fn compaction_sets_status_to_compaction_done() {
    use silo::coordination::SplitCleanupStatus;

    let (_tmp, shard) = open_temp_shard().await;

    // Set status to CleanupDone (as if cleanup just finished)
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupDone)
        .await
        .expect("set cleanup done");

    // Run compaction
    shard
        .run_full_compaction()
        .await
        .expect("compaction should succeed");

    // After compaction, status should be CompactionDone
    let status = shard
        .get_cleanup_status()
        .await
        .expect("get cleanup status");
    assert_eq!(status, SplitCleanupStatus::CompactionDone);
}

/// Test that re-opening a shard after cleanup has completed doesn't restart cleanup
#[silo::test]
async fn reopening_shard_after_cleanup_preserves_status() {
    use silo::coordination::SplitCleanupStatus;
    use silo::gubernator::MockGubernatorClient;
    use silo::job_store_shard::JobStoreShard;
    use silo::settings::{Backend, DatabaseConfig};
    use silo::shard_range::ShardRange;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();

    // First, create the shard and complete cleanup
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            flush_interval_ms: Some(10),
            wal: None,
            apply_wal_on_close: true,
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, ShardRange::full())
            .await
            .expect("open shard first time");

        // Simulate a split child: set status to CleanupPending
        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupPending)
            .await
            .expect("set cleanup pending");

        // Run cleanup
        let range = ShardRange::new("", "mmm");
        let result = shard
            .after_split_cleanup_defunct_data(&range, 10)
            .await
            .expect("cleanup should succeed");
        assert!(result.complete);

        // Verify status is CleanupDone
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(status, SplitCleanupStatus::CleanupDone);

        // Verify cleanup_completed_at is set
        let cleanup_completed_at = shard
            .get_cleanup_completed_at_ms()
            .await
            .expect("get cleanup completed at");
        assert!(
            cleanup_completed_at.is_some(),
            "cleanup_completed_at should be set after cleanup"
        );

        // Close the shard
        shard.close().await.expect("close shard");
    }

    // Now re-open the shard
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            flush_interval_ms: Some(10),
            wal: None,
            apply_wal_on_close: true,
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, ShardRange::full())
            .await
            .expect("reopen shard");

        // Status should still be CleanupDone (not reset to CompactionDone or anything else)
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(
            status,
            SplitCleanupStatus::CleanupDone,
            "cleanup status should be preserved after reopen"
        );

        // cleanup_completed_at should still be set
        let cleanup_completed_at = shard
            .get_cleanup_completed_at_ms()
            .await
            .expect("get cleanup completed at");
        assert!(
            cleanup_completed_at.is_some(),
            "cleanup_completed_at should be preserved after reopen"
        );

        shard.close().await.expect("close shard");
    }
}

/// Test that shard creation timestamp is set on first open
#[silo::test]
async fn shard_created_at_is_set_on_first_open() {
    let (_tmp, shard) = open_temp_shard().await;

    let created_at = shard.get_created_at_ms().await.expect("get created_at");

    assert!(
        created_at.is_some(),
        "created_at should be set on first open"
    );

    let ts = created_at.unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Should be within the last few seconds
    assert!(
        (now_ms - ts).abs() < 5000,
        "created_at should be recent (within 5s)"
    );
}

/// Test that shard creation timestamp is preserved across reopens
#[silo::test]
async fn shard_created_at_preserved_across_reopen() {
    use silo::gubernator::MockGubernatorClient;
    use silo::job_store_shard::JobStoreShard;
    use silo::settings::{Backend, DatabaseConfig};
    use silo::shard_range::ShardRange;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();

    let original_created_at: i64;

    // First open
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            flush_interval_ms: Some(10),
            wal: None,
            apply_wal_on_close: true,
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, ShardRange::full())
            .await
            .expect("first open");

        original_created_at = shard
            .get_created_at_ms()
            .await
            .expect("get created_at")
            .expect("created_at should be set");

        shard.close().await.expect("close");
    }

    // Wait a bit to ensure any subsequent timestamp would be different
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Reopen
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            flush_interval_ms: Some(10),
            wal: None,
            apply_wal_on_close: true,
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, ShardRange::full())
            .await
            .expect("reopen");

        let reopened_created_at = shard
            .get_created_at_ms()
            .await
            .expect("get created_at")
            .expect("created_at should still be set");

        assert_eq!(
            original_created_at, reopened_created_at,
            "created_at should be preserved across reopens"
        );

        shard.close().await.expect("close");
    }
}

/// Test that cleanup_completed_at is only set when cleanup actually completes
#[silo::test]
async fn cleanup_completed_at_only_set_on_actual_completion() {
    use silo::coordination::SplitCleanupStatus;
    use silo::shard_range::ShardRange;

    let (_tmp, shard) = open_temp_shard().await;

    // Initially, cleanup_completed_at should not be set
    let initial = shard
        .get_cleanup_completed_at_ms()
        .await
        .expect("get cleanup_completed_at");
    assert!(
        initial.is_none(),
        "cleanup_completed_at should not be set initially"
    );

    // Set status to CleanupPending (simulating split child)
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupPending)
        .await
        .expect("set pending");

    // Check still not set
    let after_pending = shard
        .get_cleanup_completed_at_ms()
        .await
        .expect("get cleanup_completed_at");
    assert!(
        after_pending.is_none(),
        "cleanup_completed_at should not be set after just setting pending"
    );

    // Run cleanup
    let range = ShardRange::new("", "mmm");
    shard
        .after_split_cleanup_defunct_data(&range, 10)
        .await
        .expect("cleanup");

    // Now it should be set
    let after_cleanup = shard
        .get_cleanup_completed_at_ms()
        .await
        .expect("get cleanup_completed_at");
    assert!(
        after_cleanup.is_some(),
        "cleanup_completed_at should be set after cleanup completes"
    );
}
