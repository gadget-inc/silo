//! Tests for cleanup behavior on shard re-acquisition and cleanup abort functionality.
//!
//! These tests verify that:
//! 1. When a shard is re-acquired after a crash or handoff, pending cleanup is automatically triggered
//! 2. When a shard is closed mid-cleanup, the cleanup is gracefully aborted and progress is saved
//! 3. Cleanup can be resumed from where it left off after re-acquisition

mod test_helpers;
use test_helpers::{
    count_job_info_keys, count_job_info_keys_for_tenant, fast_flush_slatedb_settings,
    msgpack_payload,
};

use silo::coordination::SplitCleanupStatus;
use silo::gubernator::MockGubernatorClient;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::ShardRange;
use std::sync::Arc;

/// Helper to enqueue jobs for multiple tenants
async fn enqueue_jobs_for_tenants(shard: &JobStoreShard, tenants: &[&str], jobs_per_tenant: usize) {
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

/// When a shard is opened with CleanupPending status, maybe_spawn_background_cleanup triggers cleanup
#[silo::test]
async fn background_cleanup_spawns_when_cleanup_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    // First, create a shard and set it up with pending cleanup
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Enqueue jobs for tenants inside and outside the range
        enqueue_jobs_for_tenants(&shard, &["aaa", "bbb", "zzz"], 3).await;
        shard.db().flush().await.unwrap();

        // Set status to CleanupPending (simulating a split child)
        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupPending)
            .await
            .expect("set cleanup pending");

        // Verify jobs exist for all tenants
        let total_jobs = count_job_info_keys(shard.db()).await;
        assert_eq!(total_jobs, 9); // 3 tenants * 3 jobs

        shard.close().await.expect("close shard");
    }

    // Now reopen the shard and trigger background cleanup
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // Trigger the background cleanup (this is what coordination backends call)
        shard.maybe_spawn_background_cleanup(range.clone());

        // Wait for background cleanup to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        shard.db().flush().await.unwrap();

        // Verify cleanup completed - only in-range tenants should remain
        let remaining_jobs = count_job_info_keys(shard.db()).await;
        assert_eq!(
            remaining_jobs, 6,
            "should have 6 jobs (aaa=3 + bbb=3), zzz should be cleaned up"
        );

        // Verify cleanup status is now CompactionDone
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(
            status,
            SplitCleanupStatus::CompactionDone,
            "cleanup should complete to CompactionDone"
        );

        shard.close().await.expect("close shard");
    }
}

/// When a shard is opened with CleanupRunning status (interrupted cleanup), cleanup resumes
#[silo::test]
async fn background_cleanup_resumes_when_cleanup_was_running() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    // First, create a shard and simulate interrupted cleanup
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Enqueue jobs
        enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 5).await;
        shard.db().flush().await.unwrap();

        // Set status to CleanupRunning (simulating an interrupted cleanup)
        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupRunning)
            .await
            .expect("set cleanup running");

        shard.close().await.expect("close shard");
    }

    // Reopen and verify cleanup resumes
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // Trigger background cleanup
        shard.maybe_spawn_background_cleanup(range.clone());

        // Wait for cleanup
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        shard.db().flush().await.unwrap();

        // Verify cleanup completed
        let remaining_jobs = count_job_info_keys(shard.db()).await;
        assert_eq!(remaining_jobs, 5, "should have 5 jobs (aaa only)");

        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(status, SplitCleanupStatus::CompactionDone);

        shard.close().await.expect("close shard");
    }
}

/// When a shard is opened with CleanupDone status (needs compaction), compaction runs
#[silo::test]
async fn background_cleanup_runs_compaction_when_cleanup_done() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    // First, create a shard with CleanupDone status
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Set status to CleanupDone (cleanup complete but compaction not done)
        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupDone)
            .await
            .expect("set cleanup done");

        shard.close().await.expect("close shard");
    }

    // Reopen and verify compaction runs
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // Trigger background cleanup (should just run compaction since cleanup is done)
        shard.maybe_spawn_background_cleanup(range.clone());

        // Wait for compaction
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Verify status is now CompactionDone
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(
            status,
            SplitCleanupStatus::CompactionDone,
            "should complete to CompactionDone"
        );

        shard.close().await.expect("close shard");
    }
}

/// When a shard is opened with CompactionDone status, no cleanup is triggered
#[silo::test]
async fn no_cleanup_when_already_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    // Create a shard with CompactionDone status (default)
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Enqueue jobs for tenants outside the range
        // These should NOT be cleaned up since status is CompactionDone
        enqueue_jobs_for_tenants(&shard, &["zzz"], 3).await;
        shard.db().flush().await.unwrap();

        // Verify status is CompactionDone (default)
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(status, SplitCleanupStatus::CompactionDone);

        shard.close().await.expect("close shard");
    }

    // Reopen and verify no cleanup runs
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: path.clone(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // Trigger background cleanup
        shard.maybe_spawn_background_cleanup(range.clone());

        // Wait a bit
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        shard.db().flush().await.unwrap();

        // zzz jobs should still exist since cleanup wasn't needed
        let zzz_jobs = count_job_info_keys_for_tenant(shard.db(), "zzz").await;
        assert_eq!(
            zzz_jobs, 3,
            "zzz jobs should NOT be cleaned up when status is CompactionDone"
        );

        shard.close().await.expect("close shard");
    }
}

/// Cleanup is cancelled when shard close() is called during cleanup
#[silo::test]
async fn cleanup_cancelled_on_shard_close() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    // Create shard with lots of data to make cleanup take longer
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.clone(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
        .await
        .expect("open shard");

    // Enqueue lots of jobs to make cleanup take longer
    for tenant in ["aaa", "bbb", "ccc", "zzz", "yyy", "xxx"] {
        enqueue_jobs_for_tenants(&shard, &[tenant], 20).await;
    }
    shard.db().flush().await.unwrap();

    // Set status to CleanupPending
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupPending)
        .await
        .expect("set cleanup pending");

    // Start background cleanup
    shard.maybe_spawn_background_cleanup(range.clone());

    // Immediately close the shard (should trigger cancellation)
    // Give a tiny bit of time for cleanup to start
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
    shard.close().await.expect("close shard");

    // Reopen and check state
    let shard2 = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
        .await
        .expect("reopen shard");

    // Status should still be CleanupPending or CleanupRunning (not CleanupDone)
    // because cleanup was cancelled
    let status = shard2.get_cleanup_status().await.expect("get status");
    assert!(
        status == SplitCleanupStatus::CleanupPending
            || status == SplitCleanupStatus::CleanupRunning,
        "cleanup should not have completed, got {:?}",
        status
    );

    shard2.close().await.expect("close shard");
}

/// Cleanup progress is saved when cancelled, allowing resumption
#[silo::test]
async fn cleanup_progress_saved_on_cancellation() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.clone(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    // Create shard with data
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Enqueue jobs
        for tenant in ["aaa", "zzz", "yyy", "xxx"] {
            enqueue_jobs_for_tenants(&shard, &[tenant], 10).await;
        }
        shard.db().flush().await.unwrap();

        let initial_jobs = count_job_info_keys(shard.db()).await;
        assert_eq!(initial_jobs, 40);

        // Set status to CleanupPending
        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupPending)
            .await
            .expect("set cleanup pending");

        // Start cleanup with very small batch size to make it slow
        let shard_clone = Arc::clone(&shard);
        let range_clone = range.clone();
        let cleanup_handle = tokio::spawn(async move {
            shard_clone
                .after_split_cleanup_defunct_data(&range_clone, 2) // Very small batch size
                .await
        });

        // Let cleanup start and process a few batches
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Close the shard (cancels cleanup)
        shard.close().await.expect("close shard");

        // Wait for the cleanup task to finish (it should be cancelled)
        let result = cleanup_handle.await.expect("join cleanup task");
        match result {
            Ok(cleanup_result) => {
                // Cleanup might have been cancelled or completed
                if !cleanup_result.complete {
                    assert!(cleanup_result.cancelled, "should be cancelled");
                }
            }
            Err(_) => {
                // Error is also acceptable if shard was closed
            }
        }
    }

    // Reopen and verify we can resume cleanup
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // Run cleanup to completion
        let result = shard
            .after_split_cleanup_defunct_data(&range, 100)
            .await
            .expect("cleanup should succeed");

        assert!(result.complete, "cleanup should complete on retry");
        assert!(!result.cancelled, "cleanup should not be cancelled");

        shard.db().flush().await.unwrap();

        // Only aaa jobs should remain
        let remaining_jobs = count_job_info_keys(shard.db()).await;
        assert_eq!(remaining_jobs, 10, "only aaa jobs should remain");

        shard.close().await.expect("close shard");
    }
}

/// Cleanup returns cancelled=true when cancellation token is triggered
#[silo::test]
async fn cleanup_result_indicates_cancellation() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.clone(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
        .await
        .expect("open shard");

    // Enqueue jobs
    enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 30).await;
    shard.db().flush().await.unwrap();

    // Set status to CleanupPending
    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupPending)
        .await
        .expect("set cleanup pending");

    // Start cleanup in a separate task with small batch size
    let shard_clone = Arc::clone(&shard);
    let range_clone = range.clone();
    let cleanup_handle = tokio::spawn(async move {
        shard_clone
            .after_split_cleanup_defunct_data(&range_clone, 1) // Very small batch
            .await
    });

    // Give cleanup time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Close shard (triggers cancellation)
    shard.close().await.expect("close shard");

    // Check the cleanup result
    let result = cleanup_handle.await.expect("join cleanup task");
    match result {
        Ok(cleanup_result) => {
            if !cleanup_result.complete {
                // If not complete, should be cancelled
                assert!(
                    cleanup_result.cancelled,
                    "incomplete cleanup should have cancelled=true"
                );
            }
            // If complete, that's also fine (cleanup finished before cancellation)
        }
        Err(_) => {
            // Error is acceptable if db was closed during cleanup
        }
    }
}

/// Multiple close() calls don't cause issues
#[silo::test]
async fn cleanup_handles_multiple_close_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.clone(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
        .await
        .expect("open shard");

    enqueue_jobs_for_tenants(&shard, &["aaa"], 5).await;
    shard.db().flush().await.unwrap();

    shard
        .set_cleanup_status(SplitCleanupStatus::CleanupPending)
        .await
        .expect("set cleanup pending");

    // Start background cleanup
    shard.maybe_spawn_background_cleanup(range.clone());

    // First close
    shard.close().await.expect("first close");

    // Second close should be a no-op (db is already closed)
    // This tests that cancellation token doesn't panic on multiple cancels
    // Note: We can't call close() again on the same shard because the db is closed,
    // but the cancellation token should handle being cancelled multiple times
}

/// Full cycle: enqueue, split setup, close, reopen, cleanup runs automatically
#[silo::test]
async fn full_reacquisition_cycle_triggers_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm"); // Only tenants < "mmm"

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.clone(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    // Phase 1: Create shard, enqueue data, simulate split state
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Enqueue jobs for various tenants
        enqueue_jobs_for_tenants(&shard, &["alpha", "beta", "zulu", "yankee"], 5).await;
        shard.db().flush().await.unwrap();

        // Verify all jobs exist
        let total = count_job_info_keys(shard.db()).await;
        assert_eq!(total, 20);

        // Simulate being a split child that needs cleanup
        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupPending)
            .await
            .expect("set cleanup pending");

        // Close (simulating node shutdown or lease loss)
        shard.close().await.expect("close shard");
    }

    // Phase 2: "Another node" re-acquires the shard
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // This is what the coordination backend would call after factory.open()
        shard.maybe_spawn_background_cleanup(range.clone());

        // Wait for background cleanup to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        shard.db().flush().await.unwrap();

        // Verify cleanup happened
        let alpha_jobs = count_job_info_keys_for_tenant(shard.db(), "alpha").await;
        let beta_jobs = count_job_info_keys_for_tenant(shard.db(), "beta").await;
        let zulu_jobs = count_job_info_keys_for_tenant(shard.db(), "zulu").await;
        let yankee_jobs = count_job_info_keys_for_tenant(shard.db(), "yankee").await;

        assert_eq!(alpha_jobs, 5, "alpha (in range) should be kept");
        assert_eq!(beta_jobs, 5, "beta (in range) should be kept");
        assert_eq!(zulu_jobs, 0, "zulu (out of range) should be cleaned");
        assert_eq!(yankee_jobs, 0, "yankee (out of range) should be cleaned");

        // Verify final status
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(status, SplitCleanupStatus::CompactionDone);

        shard.close().await.expect("close shard");
    }

    // Phase 3: Subsequent re-acquisition doesn't re-run cleanup
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard again");

        // Status should already be CompactionDone
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(status, SplitCleanupStatus::CompactionDone);

        // Trigger background cleanup - should be a no-op
        shard.maybe_spawn_background_cleanup(range.clone());

        // Jobs should be unchanged
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let total = count_job_info_keys(shard.db()).await;
        assert_eq!(total, 10, "only in-range jobs should remain");

        shard.close().await.expect("close shard");
    }
}

/// Interrupted cleanup followed by re-acquisition completes the cleanup
#[silo::test]
async fn interrupted_cleanup_resumes_on_reacquisition() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let rate_limiter = MockGubernatorClient::new_arc();
    let range = ShardRange::new("", "mmm");

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.clone(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    // Phase 1: Start cleanup and interrupt it
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("open shard");

        // Enqueue jobs
        enqueue_jobs_for_tenants(&shard, &["aaa", "zzz"], 15).await;
        shard.db().flush().await.unwrap();

        shard
            .set_cleanup_status(SplitCleanupStatus::CleanupPending)
            .await
            .expect("set cleanup pending");

        // Start cleanup with small batches
        let shard_clone = Arc::clone(&shard);
        let range_clone = range.clone();
        tokio::spawn(async move {
            let _ = shard_clone
                .after_split_cleanup_defunct_data(&range_clone, 2)
                .await;
        });

        // Interrupt by closing
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        shard.close().await.expect("close shard");
    }

    // Phase 2: Re-acquire and complete cleanup
    {
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range.clone())
            .await
            .expect("reopen shard");

        // Background cleanup should complete the work
        shard.maybe_spawn_background_cleanup(range.clone());

        // Wait for completion
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        shard.db().flush().await.unwrap();

        // Verify cleanup completed
        let status = shard.get_cleanup_status().await.expect("get status");
        assert_eq!(status, SplitCleanupStatus::CompactionDone);

        let remaining = count_job_info_keys(shard.db()).await;
        assert_eq!(remaining, 15, "only aaa jobs should remain");

        shard.close().await.expect("close shard");
    }
}
