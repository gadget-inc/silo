use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use silo::coordination::SplitPhase;
use silo::coordination::{Coordinator, EtcdCoordinator, ShardSplitter};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use silo::shard_range::ShardId;

/// Wait until a condition becomes true or timeout is reached.
async fn wait_until<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

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
            slatedb: None,
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
        "http://127.0.0.1:50051",
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
    // Use 8 shards to ensure even distribution across 2 nodes via rendezvous hashing
    let num_shards: u32 = 8;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    // Wait for convergence - with 8 shards, this may take longer
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
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50051",
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
    // (traffic is only paused in SplitPausing, SplitCloning, SplitUpdatingMap phases)
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
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_state_persists_across_restart() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Start first coordinator and request a split
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
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

    // Create splitter and request split

    let splitter1 = ShardSplitter::new(Arc::new(c1.clone()));

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");

    let left_child_id = split.left_child_id;
    let right_child_id = split.right_child_id;

    // Shutdown first coordinator
    c1.shutdown().await.unwrap();
    let _ = h1.abort();

    // Give etcd time to process the shutdown
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start a new coordinator
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1-restart"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    // Create splitter for c2 and check split status

    let splitter2 = ShardSplitter::new(Arc::new(c2.clone()));

    // Split state should still be there
    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get split status");
    assert!(status.is_some());
    let status = status.unwrap();
    assert_eq!(status.parent_shard_id, shard_id);
    assert_eq!(status.left_child_id, left_child_id);
    assert_eq!(status.right_child_id, right_child_id);

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
        "http://127.0.0.1:50051",
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

// ============================================================================
// End-to-end split execution tests
// ============================================================================

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
        "http://127.0.0.1:50051",
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

    // Request and execute the split
    let split = splitter
        .request_split(shard_id, "m".to_string())
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

    // Tenant routing should work correctly
    let tenant_a = shard_map.shard_for_tenant("a").unwrap();
    let tenant_z = shard_map.shard_for_tenant("z").unwrap();
    assert_eq!(tenant_a.id, split.left_child_id);
    assert_eq!(tenant_z.id, split.right_child_id);

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
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50051",
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

    // First split at "m"
    let split1 = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("first split request");
    splitter
        .execute_split(shard_id, || c1.get_shard_owner_map())
        .await
        .expect("first split execute");

    // Verify first split
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);

    // Second split: split the left child at "g"
    let left_child_id = split1.left_child_id;
    let split2 = splitter
        .request_split(left_child_id, "g".to_string())
        .await
        .expect("second split request");
    splitter
        .execute_split(left_child_id, || c1.get_shard_owner_map())
        .await
        .expect("second split execute");

    // Verify second split
    let shard_map = c1.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 3);

    // Verify tenant routing after both splits
    let tenant_a = shard_map.shard_for_tenant("a").unwrap();
    let tenant_h = shard_map.shard_for_tenant("h").unwrap();
    let tenant_z = shard_map.shard_for_tenant("z").unwrap();

    assert_eq!(tenant_a.id, split2.left_child_id);
    assert_eq!(tenant_h.id, split2.right_child_id);
    assert_eq!(tenant_z.id, split1.right_child_id);

    c1.shutdown().await.unwrap();
    let _ = h1.abort();
}

/// Test that split works correctly in a multi-node cluster
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_in_multi_node_cluster() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    // Use 8 shards to ensure even distribution across 2 nodes via rendezvous hashing
    let num_shards: u32 = 8;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
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
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    // With 8 shards, convergence may take longer
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
    let split_point = shard_info
        .range
        .midpoint()
        .expect("shard range should have a midpoint");

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
    // After splitting one shard, we should have 9 shards (8 original - 1 split + 2 children = 9)
    let expected_shards = 9;
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

    assert_eq!(map1.len(), expected_shards, "c1 should see 9 shards");
    assert_eq!(map2.len(), expected_shards, "c2 should see 9 shards");

    assert!(map2.get_shard(&split.left_child_id).is_some());
    assert!(map2.get_shard(&split.right_child_id).is_some());

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2.abort();
}

/// Test crash recovery during early phase (split should be abandoned)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_early_phase_abandons_split() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Start first coordinator and initiate split
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
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

    let _split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");
    splitter1
        .advance_split_phase(shard_id)
        .await
        .expect("advance to pausing");

    // Simulate crash by shutting down abruptly
    c1.shutdown().await.unwrap();
    let _ = h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1-restart"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    // Create splitter for c2

    let splitter2 = ShardSplitter::new(Arc::new(c2.clone()));

    // Early phase crash should abandon the split
    // The coordinator should detect the stale split and clean it up
    splitter2
        .recover_stale_splits()
        .await
        .expect("recover stale splits");

    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(
        status.is_none(),
        "early phase split should be abandoned after crash"
    );

    // Shard map should still have original shard
    let shard_map = c2.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 1);
    assert!(shard_map.get_shard(&shard_id).is_some());

    c2.shutdown().await.unwrap();
    let _ = h2.abort();
}

/// Test crash recovery during late phase (split should be resumed)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_late_phase_resumes_split() {
    let _guard = acquire_test_mutex();

    let prefix = unique_prefix();
    let num_shards: u32 = 1;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Start first coordinator and advance split to late phase
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
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

    // Create splitter, request split and advance through early phases

    let splitter1 = ShardSplitter::new(Arc::new(c1.clone()));

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request split");

    // Advance to SplitUpdatingMap (late phase)
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitPausing
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitCloning
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitUpdatingMap

    let status = splitter1.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitUpdatingMap);

    // Simulate crash
    c1.shutdown().await.unwrap();
    let _ = h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1-restart"),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");

    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    // Create splitter for c2

    let splitter2 = ShardSplitter::new(Arc::new(c2.clone()));

    // Late phase crash should preserve split for resumption
    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(status.is_some(), "late phase split should be preserved");
    assert_eq!(status.unwrap().phase, SplitPhase::SplitUpdatingMap);

    // Resume and complete the split
    splitter2
        .execute_split(shard_id, || c2.get_shard_owner_map())
        .await
        .expect("resume split should succeed");

    // Verify completion
    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(status.is_none(), "split should complete");

    let shard_map = c2.get_shard_map().await.expect("get shard map");
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());

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
            // Create a CoordinatorBase with the provided Arcs
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let base = CoordinatorBase {
                node_id: "mock-node".to_string(),
                grpc_addr: "http://mock:50051".to_string(),
                shard_map,
                owned,
                shutdown_tx,
                shutdown_rx,
                factory,
                startup_time_ms: None,
                placement_rings: Vec::new(),
            };
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
        // Use "2" as the split point since shard 0 covers "" to "4" with 4 shards
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
}
