use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use silo::coordination::SplitPhase;
use silo::coordination::{Coordinator, EtcdCoordinator, ShardSplitter};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use silo::shard_range::ShardId;

// Global mutex to serialize coordination tests
// Note: If a test panics, this mutex becomes poisoned. Use lock().unwrap_or_else()
static COORDINATION_TEST_MUTEX: Mutex<()> = Mutex::new(());

/// Helper to acquire the test mutex, handling poisoned state
fn acquire_test_mutex() -> std::sync::MutexGuard<'static, ()> {
    COORDINATION_TEST_MUTEX
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("split-test-{}", nanos)
}

fn make_test_factory(prefix: &str, node_id: &str) -> Arc<ShardFactory> {
    let tmpdir = std::env::temp_dir().join(format!("silo-split-test-{}-{}", prefix, node_id));
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            // Use Fs backend for split tests because SlateDB cloning requires a real object store
            backend: Backend::Fs,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            concurrency_reconcile_interval_ms: 5000,
            slatedb: None,
            memory_cache: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Test that request_split creates a split record
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_split_creates_split_record() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    // Get the shard to split
    let shards = c1.owned_shards().await;
    assert_eq!(shards.len(), 1);
    let shard_id = shards[0];

    // Create splitter for split operations

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Request a split
    let split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split should succeed");

    assert_eq!(split.parent_shard_id, shard_id);
    assert_eq!(split.split_point, "m");
    assert_eq!(split.phase, SplitPhase::SplitRequested);
    assert_ne!(split.left_child_id, split.right_child_id);

    // Verify we can get the split status
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get split status");
    assert!(status.is_some());
    let status = status.unwrap();
    assert_eq!(status.parent_shard_id, shard_id);
    assert_eq!(status.split_point, "m");

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that request_split fails if node doesn't own the shard
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_split_fails_if_not_owner() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    // Use 32 shards to ensure statistically even distribution across 2 nodes via rendezvous hashing.
    // With random UUIDs and 2 nodes, 32 shards makes the probability of all landing on one node negligible.
    let num_shards: u32 = 32;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);

    // Wait a bit more for shard ownership to stabilize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Get shards owned by c2
    let c2_shards = c2.owned_shards().await;
    assert!(
        !c2_shards.is_empty(),
        "c2 should own at least one shard (c1 owns: {:?}, c2 owns: {:?})",
        c1.owned_shards().await,
        c2_shards
    );
    let c2_shard = c2_shards[0];

    // Create splitter for c1 and try to split a shard that c1 doesn't own

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    let result = splitter.request_split(c2_shard, "m".to_string()).await;
    assert!(result.is_err());

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2.abort();
}

/// Test that request_split fails if split is already in progress
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_split_fails_if_already_in_progress() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter for split operations

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // First split request should succeed
    let _split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("first split should succeed");

    // Second split request should fail
    let result = splitter.request_split(shard_id, "n".to_string()).await;
    assert!(result.is_err());

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that request_split fails for invalid split point
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_split_fails_for_invalid_split_point() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 2;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Get the shard's range
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    let shard_info = shard_map.get_shard(&shard_id).unwrap();

    // Try to split at a point outside the shard's range
    // This depends on where the shard's range is - find a point definitely outside
    let invalid_point = if shard_info.range.end.is_empty() {
        // If unbounded end, use start - 1 char (won't work, need to find bounded range)
        // Skip this test case for unbounded ranges
        c1.shutdown().await.unwrap();
        let _ = h1.abort();
        return;
    } else {
        // Use a point after the end
        format!("{}z", shard_info.range.end)
    };

    // Create splitter and try to split

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    let result = splitter.request_split(shard_id, invalid_point).await;
    assert!(result.is_err());

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that is_shard_paused returns correct values
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn is_shard_paused_returns_correct_values() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Before split, shard should not be paused
    assert!(!splitter.is_shard_paused(shard_id).await);

    // After requesting a split (in SplitRequested phase), shard should not be paused
    // (traffic is only paused in SplitPausing and SplitCloning phases)
    let _split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");

    // SplitRequested does NOT pause traffic
    assert!(!splitter.is_shard_paused(shard_id).await);

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that split state is persisted across coordinator restarts
/// Test that a persisted split record is picked up by a replacement
/// coordinator on restart. With auto-resume in the coord loop, the split is
/// driven to completion rather than left untouched.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_state_persists_and_resumes_across_restart() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    let splitter1 = ShardSplitter::new(Arc::new(c1.clone()));

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");

    // Simulate a crash: stop background tasks without releasing shard owner
    // keys in etcd, so the replacement coordinator can reclaim them.
    c1.crash().await;
    let _ = h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start a new coordinator with the same node_id + storage.
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    let splitter2 = ShardSplitter::new(Arc::new(c2.clone()));

    // Poll for the background resume to drive the split to completion.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut completed = false;
    while std::time::Instant::now() < deadline {
        if splitter2
            .get_split_status(shard_id)
            .await
            .ok()
            .flatten()
            .is_none()
        {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(completed, "auto-resume should complete the persisted split");

    let shard_map = c2.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());

    c2.shutdown().await.unwrap();
    let _ = h2.abort();
}

/// Test split state for nonexistent shard returns None
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_split_status_returns_none_for_nonexistent() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    // Create splitter and check split status

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Check split status for a random shard ID that doesn't have a split
    let random_shard_id = ShardId::new();
    let status = splitter
        .get_split_status(random_shard_id)
        .await
        .expect("get split status should succeed");
    assert!(status.is_none());

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that execute_split completes a full split cycle
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_split_completes_full_cycle() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Compute midpoint of the shard's range for a balanced split
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    let split_point = silo::shard_range::format_hash_boundary(
        shard_map
            .get_shard(&shard_id)
            .unwrap()
            .range
            .midpoint()
            .unwrap(),
    );

    // Request and execute the split
    let split = splitter
        .request_split(shard_id, split_point.clone())
        .await
        .expect("request split should succeed");

    // Execute the split to completion
    splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await
        .expect("execute split should succeed");

    // Verify split is complete
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(
        status.is_none(),
        "split record should be cleaned up after completion"
    );

    // Verify shard map has been updated with children
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2, "should have 2 shards after split");

    // Original shard should be gone
    assert!(
        shard_map.get_shard(&shard_id).is_none(),
        "parent shard should be removed"
    );

    // Children should exist
    assert!(
        shard_map.get_shard(&split.left_child_id).is_some(),
        "left child should exist"
    );
    assert!(
        shard_map.get_shard(&split.right_child_id).is_some(),
        "right child should exist"
    );

    // Children should have correct parent reference
    let left_info = shard_map.get_shard(&split.left_child_id).unwrap();
    let right_info = shard_map.get_shard(&split.right_child_id).unwrap();
    assert_eq!(left_info.parent_shard_id, Some(shard_id));
    assert_eq!(right_info.parent_shard_id, Some(shard_id));

    // Tenant routing should work correctly — all tenants route to one of the children
    for tenant in ["a", "m", "z", "env-123"] {
        let routed = shard_map.shard_for_tenant(tenant).unwrap();
        assert!(
            routed.id == split.left_child_id || routed.id == split.right_child_id,
            "tenant should route to one of the children"
        );
    }

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that is_shard_paused returns true during pausing phases
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_paused_during_split_execution() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Request split but don't execute yet
    let _split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");

    // In SplitRequested, not paused
    assert!(
        !splitter.is_shard_paused(shard_id).await,
        "SplitRequested should not pause traffic"
    );

    // Advance to SplitPausing
    splitter
        .advance_split_phase(shard_id)
        .await
        .expect("advance to pausing");

    // Now should be paused
    assert!(
        splitter.is_shard_paused(shard_id).await,
        "SplitPausing should pause traffic"
    );

    let status = splitter.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitPausing);

    // Advance to SplitCloning - still paused
    splitter
        .advance_split_phase(shard_id)
        .await
        .expect("advance to cloning");
    assert!(
        splitter.is_shard_paused(shard_id).await,
        "SplitCloning should pause traffic"
    );

    // Clean up by executing the rest
    splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await
        .expect("execute split");

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test execute_split fails when no split in progress
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_split_fails_without_request() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Try to execute without requesting first
    let result = splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await;
    assert!(result.is_err(), "execute_split should fail without request");

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that execute_split can resume from a partially completed split
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_split_resumes_from_partial_state() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Request split and advance to SplitPausing
    let split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");
    splitter
        .advance_split_phase(shard_id)
        .await
        .expect("advance to pausing");

    // Verify we're in SplitPausing
    let status = splitter.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitPausing);

    // Now execute should resume from SplitPausing and complete
    splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await
        .expect("execute split from partial state");

    // Verify completion
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(status.is_none(), "split should be complete");

    // Verify shard map updated
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test sequential splits (split, then split a child)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_splits_work_correctly() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // First split at midpoint
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    let split_point1 = silo::shard_range::format_hash_boundary(
        shard_map
            .get_shard(&shard_id)
            .unwrap()
            .range
            .midpoint()
            .unwrap(),
    );

    let split1 = splitter
        .request_split(shard_id, split_point1)
        .await
        .expect("first split request");
    splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await
        .expect("first split execute");

    // Verify first split
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);

    // Second split: split the left child at its midpoint
    let left_child_id = split1.left_child_id;
    let split_point2 = silo::shard_range::format_hash_boundary(
        shard_map
            .get_shard(&left_child_id)
            .unwrap()
            .range
            .midpoint()
            .unwrap(),
    );
    let split2 = splitter
        .request_split(left_child_id, split_point2)
        .await
        .expect("second split request");
    splitter
        .execute_split(left_child_id, || c1.get_shard_owner_map())
        .await
        .expect("second split execute");

    // Verify second split
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 3);

    // Verify tenant routing works — all tenants route to one of the three shards
    let valid_ids = [
        split2.left_child_id,
        split2.right_child_id,
        split1.right_child_id,
    ];
    for tenant in ["a", "h", "z", "env-123"] {
        let routed = shard_map.shard_for_tenant(tenant).unwrap();
        assert!(
            valid_ids.contains(&routed.id),
            "tenant '{}' should route to one of the three shards",
            tenant
        );
    }

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that split works correctly in a multi-node cluster
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_in_multi_node_cluster() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    // Use 32 shards to ensure statistically even distribution across 2 nodes via rendezvous hashing.
    // With random UUIDs and 2 nodes, 32 shards makes the probability of all landing on one node negligible.
    let num_shards: u32 = 32;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");

    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    // With 32 shards, convergence may take longer
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);

    // Wait for ownership to stabilize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Get a shard that c1 owns
    let c1_shards = c1.owned_shards().await;
    assert!(
        !c1_shards.is_empty(),
        "c1 should own at least one shard (c1 owns: {:?}, c2 owns: {:?})",
        c1_shards,
        c2.owned_shards().await
    );
    let shard_to_split = c1_shards[0];

    // Get the shard's range and compute a valid split point
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    let shard_info = shard_map
        .get_shard(&shard_to_split)
        .expect("find shard in map");
    let split_point = silo::shard_range::format_hash_boundary(
        shard_info
            .range
            .midpoint()
            .expect("shard range should have a midpoint"),
    );

    // Create splitter and split the shard

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    let split = splitter
        .request_split(shard_to_split, split_point)
        .await
        .expect("request split");
    splitter
        .execute_split(shard_to_split, || c1.get_shard_owner_map())
        .await
        .expect("execute split");

    // Both coordinators should eventually see the updated shard map
    // Wait for the shard map change to propagate via etcd watch
    // After splitting one shard, we should have 33 shards (32 original - 1 split + 2 children = 33)
    let expected_shards = (num_shards + 1) as usize;
    let mut retries = 20;
    loop {
        let map2 = c2.get_shard_map().await.expect("c2 shard map");
        if map2.len() == expected_shards || retries == 0 {
            break;
        }
        retries -= 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let map1 = c1.get_shard_map().await.expect("c1 shard map");
    let map2 = c2.get_shard_map().await.expect("c2 shard map");

    assert_eq!(
        map1.len(),
        expected_shards,
        "c1 should see {expected_shards} shards"
    );
    assert_eq!(
        map2.len(),
        expected_shards,
        "c2 should see {expected_shards} shards"
    );

    assert!(map2.get_shard(&split.left_child_id).is_some());
    assert!(map2.get_shard(&split.right_child_id).is_some());

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2.abort();
}

/// Test that execute_split abandons the split and restores the parent shard
/// when cloning fails (pre-commit error), using a real etcd coordinator.
///
/// This simulates the production scenario where SlateDB clone fails:
/// the split record is cleaned up and the parent shard resumes service.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_split_abandons_on_clone_failure() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Use a noop factory - reconciliation will open in-memory shards, but we
    // close them before splitting so the split fails when the parent is not found.
    let factory = Arc::new(ShardFactory::new_noop());

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        Arc::clone(&factory),
        Vec::new(),
    )
    .await
    .expect("start coordinator");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Close the shard so the split will fail when trying to find the parent
    factory.close(&shard_id).await.unwrap();

    let splitter = ShardSplitter::new(Arc::new(c1.clone()));

    // Request a split
    let _split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split should succeed");

    // Execute should fail because parent shard is not open in factory
    let result = splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await;
    assert!(
        result.is_err(),
        "execute_split should fail when cloning fails"
    );

    // The split record should have been cleaned up (abandoned)
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get split status");
    assert!(
        status.is_none(),
        "split record should be deleted after clone failure"
    );

    // The shard should no longer be paused
    assert!(
        !splitter.is_shard_paused(shard_id).await,
        "shard should not be paused after split is abandoned"
    );

    // The shard map should be unchanged (parent still exists)
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 1, "shard map should have 1 shard");
    assert!(
        shard_map.get_shard(&shard_id).is_some(),
        "parent shard should still exist"
    );

    // Should be able to request a new split on the same shard
    let split2 = splitter.request_split(shard_id, "m".to_string()).await;
    assert!(
        split2.is_ok(),
        "should be able to retry split after abandonment"
    );

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test crash recovery during early phase (split should be abandoned)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_early_phase_resumes_split() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Start first coordinator and initiate split
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    // Create splitter, request split and advance to SplitPausing (early phase)
    let splitter1 = ShardSplitter::new(Arc::new(c1.clone()));

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");
    splitter1
        .advance_split_phase(shard_id)
        .await
        .expect("advance to pausing");

    // Simulate a crash: stop background tasks without releasing shard owner
    // keys in etcd, so the replacement coordinator can reclaim them.
    c1.crash().await;
    let _ = h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator. Its coordination loop runs
    // resume_in_progress_splits after reclaim, so the paused split should
    // drive itself to SplitComplete automatically.
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    let splitter2 = ShardSplitter::new(Arc::new(c2.clone()));

    // Poll for background resume completion.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut completed = false;
    while std::time::Instant::now() < deadline {
        if splitter2
            .get_split_status(shard_id)
            .await
            .ok()
            .flatten()
            .is_none()
        {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(completed, "resumed split should clear its record");

    let shard_map = c2.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());
    assert!(
        shard_map.get_shard(&shard_id).is_none(),
        "parent should have been removed"
    );

    c2.shutdown().await.unwrap();
    let _ = h2.abort();
}

/// Test that a split crashed in SplitCloning (before shard-map commit) is
/// auto-resumed to completion on restart.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_cloning_phase_resumes_split() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Start first coordinator and advance split to cloning phase
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");

    assert!(c1.wait_converged(Duration::from_secs(10)).await);

    let shards = c1.owned_shards().await;
    let shard_id = shards[0];

    let splitter1 = ShardSplitter::new(Arc::new(c1.clone()));

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");

    // Advance to SplitCloning phase without actually cloning, then crash.
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitPausing
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitCloning

    let status = splitter1.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitCloning);

    // Simulate a crash: stop background tasks without releasing shard owner
    // keys in etcd, so the replacement coordinator can reclaim them.
    c1.crash().await;
    let _ = h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator - should auto-resume the split.
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    let splitter2 = ShardSplitter::new(Arc::new(c2.clone()));

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut completed = false;
    while std::time::Instant::now() < deadline {
        if splitter2
            .get_split_status(shard_id)
            .await
            .ok()
            .flatten()
            .is_none()
        {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        completed,
        "stuck SplitCloning split should resume and complete"
    );

    let shard_map = c2.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());

    // Re-issuing a split on one of the children should succeed — the prior
    // stuck split no longer blocks further operations on the parent.
    let followup = splitter2
        .request_split(split.left_child_id, "b".to_string())
        .await;
    assert!(
        followup.is_ok(),
        "follow-up split on child should succeed, got {:?}",
        followup
    );

    c2.shutdown().await.unwrap();
    let _ = h2.abort();
}

mod splitter_unit_tests {
    use async_trait::async_trait;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    use silo::coordination::CoordinationError;
    use silo::coordination::split::{ShardSplitter, SplitStorageBackend};
    use silo::coordination::{Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap, SplitPhase};
    use silo::factory::ShardFactory;
    use silo::shard_range::{ShardId, ShardMap, SplitInProgress};

    /// Mock coordinator for testing the splitter logic.
    /// Implements both Coordinator and SplitStorageBackend.
    struct MockSplitBackend {
        base: CoordinatorBase,
        splits: Mutex<HashMap<ShardId, SplitInProgress>>,
    }

    impl MockSplitBackend {
        fn new(
            shard_map: Arc<Mutex<ShardMap>>,
            owned: Arc<Mutex<HashSet<ShardId>>>,
            factory: Arc<ShardFactory>,
        ) -> Self {
            let base = CoordinatorBase::new_with_shared(
                "mock-node",
                "http://mock:7450",
                shard_map,
                owned,
                factory,
            );
            Self {
                base,
                splits: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl Coordinator for MockSplitBackend {
        fn base(&self) -> &CoordinatorBase {
            &self.base
        }

        async fn shutdown(&self) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn wait_converged(&self, _timeout: Duration) -> bool {
            true
        }

        async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
            Ok(vec![])
        }

        async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
            let shard_map = self.base.shard_map.lock().await.clone();
            Ok(ShardOwnerMap {
                shard_map,
                shard_to_node: HashMap::new(),
                shard_to_addr: HashMap::new(),
            })
        }

        async fn update_shard_placement_ring(
            &self,
            shard_id: &ShardId,
            ring: Option<&str>,
        ) -> Result<(Option<String>, Option<String>), CoordinationError> {
            let mut shard_map = self.base.shard_map.lock().await;
            let shard = shard_map
                .get_shard_mut(shard_id)
                .ok_or_else(|| CoordinationError::ShardNotFound(*shard_id))?;
            let previous = shard.placement_ring.clone();
            let current = ring.map(|s| s.to_string());
            shard.placement_ring = current.clone();
            Ok((previous, current))
        }

        async fn force_release_shard_lease(
            &self,
            _shard_id: &ShardId,
        ) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn reclaim_existing_leases(&self) -> Result<Vec<ShardId>, CoordinationError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl SplitStorageBackend for MockSplitBackend {
        async fn load_split(
            &self,
            parent_shard_id: &ShardId,
        ) -> Result<Option<SplitInProgress>, CoordinationError> {
            Ok(self.splits.lock().await.get(parent_shard_id).cloned())
        }

        async fn store_split(&self, split: &SplitInProgress) -> Result<(), CoordinationError> {
            self.splits
                .lock()
                .await
                .insert(split.parent_shard_id, split.clone());
            Ok(())
        }

        async fn delete_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
            self.splits.lock().await.remove(parent_shard_id);
            Ok(())
        }

        async fn update_shard_map_for_split(
            &self,
            split: &SplitInProgress,
        ) -> Result<(), CoordinationError> {
            let mut shard_map = self.base.shard_map.lock().await;
            shard_map.split_shard(
                &split.parent_shard_id,
                &split.split_point,
                split.left_child_id,
                split.right_child_id,
            )?;
            Ok(())
        }

        async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
            Ok(self.splits.lock().await.values().cloned().collect())
        }
    }

    /// Mock coordinator that accepts an external splits map, simulating
    /// persistent split storage (ConfigMaps) that survives process restarts.
    /// The CoordinatorBase is always fresh (empty paused_shards cache),
    /// just like a real coordinator after restart.
    struct MockSplitBackendWithSharedStorage {
        base: CoordinatorBase,
        splits: Arc<Mutex<HashMap<ShardId, SplitInProgress>>>,
    }

    impl MockSplitBackendWithSharedStorage {
        fn new(
            shard_map: Arc<Mutex<ShardMap>>,
            owned: Arc<Mutex<HashSet<ShardId>>>,
            factory: Arc<ShardFactory>,
            splits: Arc<Mutex<HashMap<ShardId, SplitInProgress>>>,
        ) -> Self {
            let base = CoordinatorBase::new_with_shared(
                "mock-node",
                "http://mock:7450",
                shard_map,
                owned,
                factory,
            );
            Self { base, splits }
        }
    }

    #[async_trait]
    impl Coordinator for MockSplitBackendWithSharedStorage {
        fn base(&self) -> &CoordinatorBase {
            &self.base
        }

        async fn shutdown(&self) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn wait_converged(&self, _timeout: Duration) -> bool {
            true
        }

        async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
            Ok(vec![])
        }

        async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
            let shard_map = self.base.shard_map.lock().await.clone();
            Ok(ShardOwnerMap {
                shard_map,
                shard_to_node: HashMap::new(),
                shard_to_addr: HashMap::new(),
            })
        }

        async fn update_shard_placement_ring(
            &self,
            _shard_id: &ShardId,
            _ring: Option<&str>,
        ) -> Result<(Option<String>, Option<String>), CoordinationError> {
            Ok((None, None))
        }

        async fn force_release_shard_lease(
            &self,
            _shard_id: &ShardId,
        ) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn reclaim_existing_leases(&self) -> Result<Vec<ShardId>, CoordinationError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl SplitStorageBackend for MockSplitBackendWithSharedStorage {
        async fn load_split(
            &self,
            parent_shard_id: &ShardId,
        ) -> Result<Option<SplitInProgress>, CoordinationError> {
            Ok(self.splits.lock().await.get(parent_shard_id).cloned())
        }

        async fn store_split(&self, split: &SplitInProgress) -> Result<(), CoordinationError> {
            self.splits
                .lock()
                .await
                .insert(split.parent_shard_id, split.clone());
            Ok(())
        }

        async fn delete_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
            self.splits.lock().await.remove(parent_shard_id);
            Ok(())
        }

        async fn update_shard_map_for_split(
            &self,
            split: &SplitInProgress,
        ) -> Result<(), CoordinationError> {
            let mut shard_map = self.base.shard_map.lock().await;
            shard_map.split_shard(
                &split.parent_shard_id,
                &split.split_point,
                split.left_child_id,
                split.right_child_id,
            )?;
            Ok(())
        }

        async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
            Ok(self.splits.lock().await.values().cloned().collect())
        }
    }

    /// After a process restart, the split ConfigMap still exists in storage
    /// but the local paused_shards cache is empty. This means is_shard_paused
    /// returns false, allowing traffic through to a shard that was mid-split.
    #[tokio::test]
    async fn test_restart_loses_paused_state_for_stuck_split() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = Arc::new(ShardFactory::new_noop());

        // Shared split storage — survives "restarts" like a real ConfigMap would.
        let splits: Arc<Mutex<HashMap<ShardId, SplitInProgress>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let shard_id = shard_map.lock().await.shard_ids()[0];
        owned.lock().await.insert(shard_id);

        // --- First incarnation: request a split and advance to SplitPausing ---
        let mock1 = Arc::new(MockSplitBackendWithSharedStorage::new(
            shard_map.clone(),
            owned.clone(),
            factory.clone(),
            splits.clone(),
        ));
        let splitter1 = ShardSplitter::new(Arc::clone(&mock1) as Arc<dyn Coordinator>);

        splitter1
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();
        splitter1.advance_split_phase(shard_id).await.unwrap();

        // In production, execute_split_inner populates the paused cache when
        // advancing to SplitPausing. advance_split_phase is a test helper that
        // only updates the record, so we replicate what execute_split does.
        mock1.base().mark_shard_paused(shard_id);

        // Split is in SplitPausing — traffic should be paused.
        let status = splitter1.get_split_status(shard_id).await.unwrap().unwrap();
        assert_eq!(status.phase, SplitPhase::SplitPausing);
        assert!(
            mock1.base().is_shard_paused(&shard_id),
            "shard should be paused before restart"
        );

        // The split record exists in storage.
        assert!(
            splits.lock().await.contains_key(&shard_id),
            "split ConfigMap should exist in storage"
        );

        // --- Simulate process restart: new coordinator, same storage ---
        // The CoordinatorBase is fresh (empty paused_shards cache),
        // but the split ConfigMap persists in the shared storage.
        let mock2 = Arc::new(MockSplitBackendWithSharedStorage::new(
            shard_map.clone(),
            owned.clone(),
            factory.clone(),
            splits.clone(),
        ));
        let splitter2 = ShardSplitter::new(Arc::clone(&mock2) as Arc<dyn Coordinator>);

        // The split record still exists in storage.
        let status = splitter2.get_split_status(shard_id).await.unwrap().unwrap();
        assert_eq!(
            status.phase,
            SplitPhase::SplitPausing,
            "split record should survive restart"
        );
        assert!(
            splits.lock().await.contains_key(&shard_id),
            "split ConfigMap should still exist after restart"
        );

        // Before re-hydration the local cache is empty.
        assert!(
            !mock2.base().is_shard_paused(&shard_id),
            "paused_shards cache should be empty before re-hydration"
        );

        // Re-hydrate the cache from persisted split records.
        splitter2.rehydrate_paused_shards().await.unwrap();

        // The shard should now be paused again.
        assert!(
            mock2.base().is_shard_paused(&shard_id),
            "shard should be paused after re-hydrating from stored split records"
        );

        // The split ConfigMap must still exist — rehydrate does not clean up.
        // An operator should decide whether to abandon or retry the split
        // (e.g. via siloctl or recover_stale_splits).
        assert!(
            splits.lock().await.contains_key(&shard_id),
            "split ConfigMap should not be deleted by rehydrate_paused_shards"
        );
        let status = splitter2.get_split_status(shard_id).await.unwrap().unwrap();
        assert_eq!(
            status.phase,
            SplitPhase::SplitPausing,
            "split record should be untouched"
        );
    }

    /// recover_stale_splits must clear the paused-shards cache entry after
    /// deleting the split record. Without this, a shard stays permanently
    /// paused with no way to unpause through normal operation.
    #[tokio::test]
    async fn test_recover_stale_splits_clears_paused_cache() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = Arc::new(ShardFactory::new_noop());
        let splits: Arc<Mutex<HashMap<ShardId, SplitInProgress>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let shard_id = shard_map.lock().await.shard_ids()[0];
        owned.lock().await.insert(shard_id);

        // Create a split and advance to SplitPausing.
        let mock = Arc::new(MockSplitBackendWithSharedStorage::new(
            shard_map.clone(),
            owned.clone(),
            factory.clone(),
            splits.clone(),
        ));
        let splitter = ShardSplitter::new(Arc::clone(&mock) as Arc<dyn Coordinator>);

        splitter
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();
        splitter.advance_split_phase(shard_id).await.unwrap();

        // Simulate startup: re-hydrate populates the cache.
        splitter.rehydrate_paused_shards().await.unwrap();
        assert!(
            mock.base().is_shard_paused(&shard_id),
            "shard should be paused after rehydrate"
        );

        // Operator runs recover_stale_splits to abandon the stuck split.
        splitter.recover_stale_splits().await.unwrap();

        // The split record should be gone.
        assert!(
            splits.lock().await.is_empty(),
            "split record should be deleted"
        );

        // The paused cache must also be cleared.
        assert!(
            !mock.base().is_shard_paused(&shard_id),
            "shard must not be paused after recover_stale_splits"
        );
    }

    #[tokio::test]
    async fn test_request_split_validates_ownership() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = Arc::new(ShardFactory::new_noop());

        let mock = MockSplitBackend::new(shard_map.clone(), owned.clone(), factory);
        let splitter = ShardSplitter::new(Arc::new(mock));

        // Get a shard ID from the map
        let shard_id = shard_map.lock().await.shard_ids()[0];

        // Should fail because we don't own the shard
        // Use "2" as the split point (valid within shard 0's hash-space range)
        let result = splitter.request_split(shard_id, "2".to_string()).await;
        assert!(matches!(result, Err(CoordinationError::NotShardOwner(_))));

        // Add ownership
        owned.lock().await.insert(shard_id);

        // Should succeed now
        let result = splitter.request_split(shard_id, "2".to_string()).await;
        assert!(result.is_ok());

        // Should fail if we try again (split already in progress)
        let result = splitter.request_split(shard_id, "3".to_string()).await;
        assert!(matches!(
            result,
            Err(CoordinationError::SplitAlreadyInProgress(_))
        ));
    }

    #[tokio::test]
    async fn test_is_shard_paused() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = Arc::new(ShardFactory::new_noop());

        let mock = MockSplitBackend::new(shard_map.clone(), owned.clone(), factory);
        let splitter = ShardSplitter::new(Arc::new(mock));

        let shard_id = shard_map.lock().await.shard_ids()[0];
        owned.lock().await.insert(shard_id);

        // Not paused initially
        assert!(!splitter.is_shard_paused(shard_id).await);

        // Request a split (use "2" since shard 0 covers "" to "4" with 4 shards)
        let split = splitter
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();
        assert_eq!(split.phase, SplitPhase::SplitRequested);

        // Still not paused (SplitRequested doesn't pause traffic)
        assert!(!splitter.is_shard_paused(shard_id).await);

        // Advance to pausing phase
        splitter.advance_split_phase(shard_id).await.unwrap();
        assert!(splitter.is_shard_paused(shard_id).await);
    }

    /// Test that execute_split abandons the split and cleans up the record
    /// when cloning fails (pre-commit error).
    #[tokio::test]
    async fn test_execute_split_abandons_on_clone_failure() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        // noop factory has no open shards, so clone_closed_shard will fail
        let factory = Arc::new(ShardFactory::new_noop());

        let mock = Arc::new(MockSplitBackend::new(
            shard_map.clone(),
            owned.clone(),
            factory,
        ));
        let splitter = ShardSplitter::new(Arc::clone(&mock) as Arc<dyn Coordinator>);

        let shard_id = shard_map.lock().await.shard_ids()[0];
        owned.lock().await.insert(shard_id);

        // Request a split
        let _split = splitter
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();

        // execute_split should fail because clone_closed_shard will fail (no open shards)
        let result = splitter
            .execute_split(shard_id, || async {
                Ok(ShardOwnerMap {
                    shard_map: shard_map.lock().await.clone(),
                    shard_to_node: HashMap::new(),
                    shard_to_addr: HashMap::new(),
                })
            })
            .await;
        assert!(result.is_err(), "execute_split should fail on clone error");

        // The split record should have been cleaned up (abandoned)
        let status = splitter.get_split_status(shard_id).await.unwrap();
        assert!(
            status.is_none(),
            "split record should be deleted after pre-commit failure"
        );

        // The shard should no longer be paused
        assert!(
            !splitter.is_shard_paused(shard_id).await,
            "shard should not be paused after split is abandoned"
        );

        // The shard map should be unchanged (parent still exists)
        let map = shard_map.lock().await;
        assert!(
            map.get_shard(&shard_id).is_some(),
            "parent shard should still exist in shard map"
        );
    }

    /// Test that after a failed split is abandoned, a new split can be requested
    /// on the same shard.
    #[tokio::test]
    async fn test_can_retry_split_after_abandonment() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = Arc::new(ShardFactory::new_noop());

        let mock = Arc::new(MockSplitBackend::new(
            shard_map.clone(),
            owned.clone(),
            factory,
        ));
        let splitter = ShardSplitter::new(Arc::clone(&mock) as Arc<dyn Coordinator>);

        let shard_id = shard_map.lock().await.shard_ids()[0];
        owned.lock().await.insert(shard_id);

        // First split attempt: request and execute (will fail on clone)
        let _split1 = splitter
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();
        let result = splitter
            .execute_split(shard_id, || async {
                Ok(ShardOwnerMap {
                    shard_map: shard_map.lock().await.clone(),
                    shard_to_node: HashMap::new(),
                    shard_to_addr: HashMap::new(),
                })
            })
            .await;
        assert!(result.is_err());

        // The split record was abandoned, so we should be able to request a new one
        let split2 = splitter.request_split(shard_id, "2".to_string()).await;
        assert!(
            split2.is_ok(),
            "should be able to request a new split after the previous one was abandoned"
        );
    }

    /// Test that the owned set is not polluted with child shard IDs after
    /// a pre-commit failure. If the clone fails, no children should be added
    /// to the owned set.
    #[tokio::test]
    async fn test_owned_set_clean_after_clone_failure() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = Arc::new(ShardFactory::new_noop());

        let mock = Arc::new(MockSplitBackend::new(
            shard_map.clone(),
            owned.clone(),
            factory,
        ));
        let splitter = ShardSplitter::new(Arc::clone(&mock) as Arc<dyn Coordinator>);

        let shard_id = shard_map.lock().await.shard_ids()[0];
        owned.lock().await.insert(shard_id);

        // Capture owned set before split
        let owned_before: HashSet<ShardId> = owned.lock().await.clone();

        // Request and attempt to execute (will fail on clone)
        let split = splitter
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();
        let _ = splitter
            .execute_split(shard_id, || async {
                Ok(ShardOwnerMap {
                    shard_map: shard_map.lock().await.clone(),
                    shard_to_node: HashMap::new(),
                    shard_to_addr: HashMap::new(),
                })
            })
            .await;

        // The owned set should only contain the original shards
        let owned_after: HashSet<ShardId> = owned.lock().await.clone();
        assert_eq!(
            owned_before, owned_after,
            "owned set should not be modified after pre-commit failure"
        );

        // Specifically, children should NOT be in the owned set
        assert!(
            !owned_after.contains(&split.left_child_id),
            "left child should not be in owned set"
        );
        assert!(
            !owned_after.contains(&split.right_child_id),
            "right child should not be in owned set"
        );
    }

    /// Mock coordinator that uses a real factory and can inject failures at the
    /// shard map update step. This allows testing the recovery path where the
    /// parent shard is closed during SplitCloning but the split fails before
    /// the commit point.
    struct FailingUpdateMockBackend {
        base: CoordinatorBase,
        splits: Mutex<HashMap<ShardId, SplitInProgress>>,
        /// When true, update_shard_map_for_split will return an error
        fail_shard_map_update: std::sync::atomic::AtomicBool,
    }

    impl FailingUpdateMockBackend {
        fn new(
            shard_map: Arc<Mutex<ShardMap>>,
            owned: Arc<Mutex<HashSet<ShardId>>>,
            factory: Arc<ShardFactory>,
        ) -> Self {
            let base = CoordinatorBase::new_with_shared(
                "mock-node",
                "http://mock:7450",
                shard_map,
                owned,
                factory,
            );
            Self {
                base,
                splits: Mutex::new(HashMap::new()),
                fail_shard_map_update: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl Coordinator for FailingUpdateMockBackend {
        fn base(&self) -> &CoordinatorBase {
            &self.base
        }

        async fn shutdown(&self) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn wait_converged(&self, _timeout: Duration) -> bool {
            true
        }

        async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
            Ok(vec![])
        }

        async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
            let shard_map = self.base.shard_map.lock().await.clone();
            Ok(ShardOwnerMap {
                shard_map,
                shard_to_node: HashMap::new(),
                shard_to_addr: HashMap::new(),
            })
        }

        async fn update_shard_placement_ring(
            &self,
            _shard_id: &ShardId,
            _ring: Option<&str>,
        ) -> Result<(Option<String>, Option<String>), CoordinationError> {
            Ok((None, None))
        }

        async fn force_release_shard_lease(
            &self,
            _shard_id: &ShardId,
        ) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn reclaim_existing_leases(&self) -> Result<Vec<ShardId>, CoordinationError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl SplitStorageBackend for FailingUpdateMockBackend {
        async fn load_split(
            &self,
            parent_shard_id: &ShardId,
        ) -> Result<Option<SplitInProgress>, CoordinationError> {
            Ok(self.splits.lock().await.get(parent_shard_id).cloned())
        }

        async fn store_split(&self, split: &SplitInProgress) -> Result<(), CoordinationError> {
            self.splits
                .lock()
                .await
                .insert(split.parent_shard_id, split.clone());
            Ok(())
        }

        async fn delete_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
            self.splits.lock().await.remove(parent_shard_id);
            Ok(())
        }

        async fn update_shard_map_for_split(
            &self,
            split: &SplitInProgress,
        ) -> Result<(), CoordinationError> {
            if self
                .fail_shard_map_update
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(CoordinationError::BackendError(
                    "injected shard map update failure".to_string(),
                ));
            }
            let mut shard_map = self.base.shard_map.lock().await;
            shard_map.split_shard(
                &split.parent_shard_id,
                &split.split_point,
                split.left_child_id,
                split.right_child_id,
            )?;
            Ok(())
        }

        async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
            Ok(())
        }

        async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
            Ok(self.splits.lock().await.values().cloned().collect())
        }
    }

    fn make_test_factory_for_unit_test(test_name: &str) -> Arc<ShardFactory> {
        use silo::gubernator::MockGubernatorClient;
        use silo::settings::{Backend, DatabaseTemplate};

        let tmpdir = std::env::temp_dir().join(format!("silo-splitter-unit-{}", test_name));
        // Clean up from previous test runs
        let _ = std::fs::remove_dir_all(&tmpdir);
        Arc::new(ShardFactory::new(
            DatabaseTemplate {
                backend: Backend::Fs,
                path: tmpdir.join("%shard%").to_string_lossy().to_string(),
                wal: None,
                apply_wal_on_close: true,
                concurrency_reconcile_interval_ms: 5000,
                slatedb: None,
                memory_cache: None,
            },
            MockGubernatorClient::new_arc(),
            None,
        ))
    }

    /// Test that when a split fails at the shard map update (after the parent
    /// shard's database was closed), the parent shard is recovered and can
    /// serve requests again.
    #[silo::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_parent_shard_recovered_after_shard_map_update_failure() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let owned = Arc::new(Mutex::new(HashSet::new()));
        let factory = make_test_factory_for_unit_test("recover-shard-map-fail");

        // Pick a shard and open it in the factory
        let shard_id = shard_map.lock().await.shard_ids()[0];
        let range = shard_map
            .lock()
            .await
            .get_shard(&shard_id)
            .unwrap()
            .range
            .clone();
        factory.open(&shard_id, &range).await.unwrap();
        owned.lock().await.insert(shard_id);

        // Verify the shard is usable before the split
        let shard_before = factory.get(&shard_id).unwrap();
        assert!(
            shard_before.get_job("test", "nonexistent").await.is_ok(),
            "shard should be usable before split"
        );

        let mock = Arc::new(FailingUpdateMockBackend::new(
            shard_map.clone(),
            owned.clone(),
            factory.clone(),
        ));
        // Set to fail at shard map update
        mock.fail_shard_map_update
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let splitter = ShardSplitter::new(Arc::clone(&mock) as Arc<dyn Coordinator>);

        // Request and execute a split — it will fail at the shard map update
        // after the parent shard's database has been closed
        splitter
            .request_split(shard_id, "2".to_string())
            .await
            .unwrap();
        let result = splitter
            .execute_split(shard_id, || async {
                Ok(ShardOwnerMap {
                    shard_map: shard_map.lock().await.clone(),
                    shard_to_node: HashMap::new(),
                    shard_to_addr: HashMap::new(),
                })
            })
            .await;
        assert!(result.is_err(), "split should fail at shard map update");

        // The split record should be cleaned up
        let status = splitter.get_split_status(shard_id).await.unwrap();
        assert!(
            status.is_none(),
            "split record should be deleted after failure"
        );

        // The parent shard should still be in the shard map
        assert!(
            shard_map.lock().await.get_shard(&shard_id).is_some(),
            "parent shard should still exist in shard map"
        );

        // The parent shard should be recovered and usable
        let shard_after = factory
            .get(&shard_id)
            .expect("parent shard should be in factory after recovery");
        assert!(
            shard_after.get_job("test", "nonexistent").await.is_ok(),
            "parent shard should be usable after recovery"
        );

        // The owned set should still contain the parent
        assert!(
            owned.lock().await.contains(&shard_id),
            "parent shard should still be in owned set"
        );
    }

    /// Test that without recovery, a failed split after parent close leaves
    /// the shard permanently unusable. This is the "before fix" scenario that
    /// validates the test setup would catch the bug.
    #[silo::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_parent_shard_unusable_without_recovery_after_close() {
        let shard_map = Arc::new(Mutex::new(ShardMap::create_initial(4).unwrap()));
        let _owned: Arc<Mutex<HashSet<ShardId>>> = Arc::new(Mutex::new(HashSet::new()));
        let factory = make_test_factory_for_unit_test("unusable-without-recovery");

        // Pick a shard and open it in the factory
        let shard_id = shard_map.lock().await.shard_ids()[0];
        let range = shard_map
            .lock()
            .await
            .get_shard(&shard_id)
            .unwrap()
            .range
            .clone();
        factory.open(&shard_id, &range).await.unwrap();

        // Verify the shard is usable
        let shard = factory.get(&shard_id).unwrap();
        assert!(shard.get_job("test", "nonexistent").await.is_ok());

        // Close the shard directly (simulating what happens during SplitCloning)
        shard.close().await.unwrap();

        // The shard is still in the factory but its DB is closed.
        // Operations on it should fail.
        let shard_closed = factory.get(&shard_id).unwrap();
        assert!(
            shard_closed.get_job("test", "nonexistent").await.is_err(),
            "shard should be unusable after close without factory removal"
        );

        // Re-opening via factory.open won't help because OnceCell already has
        // the closed entry — it returns the same closed instance.
        let reopen_result = factory.open(&shard_id, &range).await;
        assert!(reopen_result.is_ok(), "factory.open returns existing entry");
        let shard_still_closed = factory.get(&shard_id).unwrap();
        assert!(
            shard_still_closed
                .get_job("test", "nonexistent")
                .await
                .is_err(),
            "shard should still be unusable — factory.open returned the stale entry"
        );

        // Only factory.close() + factory.open() creates a fresh instance
        factory.close(&shard_id).await.unwrap();
        let shard_recovered = factory.open(&shard_id, &range).await.unwrap();
        assert!(
            shard_recovered.get_job("test", "nonexistent").await.is_ok(),
            "shard should be usable after factory.close + factory.open"
        );
    }
}
