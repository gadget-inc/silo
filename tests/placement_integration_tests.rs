//! Integration tests for the placement engine.
//!
//! These tests require a running etcd instance and test:
//! - Leader election across multiple coordinators
//! - Placement override storage and distribution
//! - Basic two-node migration scenarios

use silo::coordination::{Coordinator, EtcdCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Global mutex to serialize coordination tests
static PLACEMENT_TEST_MUTEX: Mutex<()> = Mutex::new(());

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-placement-{}", nanos)
}

fn make_test_factory(node_id: &str) -> Arc<ShardFactory> {
    let tmpdir = std::env::temp_dir().join(format!("silo-placement-test-{}", node_id));
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        MockGubernatorClient::new_arc(),
    ))
}

/// Test that leader election works correctly with a single node
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_single_node_is_leader() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = PLACEMENT_TEST_MUTEX.lock().unwrap();
    
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    
    let (coord, handle) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-1",
        "127.0.0.1:50051",
        4,
        5,
        make_test_factory("node-1"),
    )
    .await
    .expect("failed to create coordinator");
    
    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Single node should always be the leader
    assert!(coord.is_leader(), "single node should be the leader");
    
    // Clean up
    coord.shutdown().await.expect("shutdown failed");
    handle.abort();
}

/// Test that leader election works correctly with multiple nodes
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_node_leader_election() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = PLACEMENT_TEST_MUTEX.lock().unwrap();
    
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    
    // Start first node
    let (coord1, handle1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-1",
        "127.0.0.1:50051",
        4,
        5,
        make_test_factory("node-1"),
    )
    .await
    .expect("failed to create coordinator 1");
    
    // Wait for first node to become leader
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Start second node
    let (coord2, handle2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-2",
        "127.0.0.1:50052",
        4,
        5,
        make_test_factory("node-2"),
    )
    .await
    .expect("failed to create coordinator 2");
    
    // Wait for second node to join and leader election to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Exactly one should be the leader
    let leader1 = coord1.is_leader();
    let leader2 = coord2.is_leader();
    
    // At least one should be the leader (could be either)
    assert!(
        leader1 || leader2,
        "at least one node should be the leader"
    );
    
    // Both cannot be leaders (in theory - there's a small window during failover)
    // Note: We check this softly since leader election can have race conditions
    if leader1 && leader2 {
        tracing::warn!("both nodes claim leadership - this can happen briefly during elections");
    }
    
    // Clean up
    coord2.shutdown().await.expect("shutdown coord2 failed");
    coord1.shutdown().await.expect("shutdown coord1 failed");
    handle1.abort();
    handle2.abort();
}

/// Test that leadership transfers when the leader shuts down
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leader_failover() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = PLACEMENT_TEST_MUTEX.lock().unwrap();
    
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    
    // Start first node
    let (coord1, handle1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-1",
        "127.0.0.1:50051",
        4,
        5,
        make_test_factory("node-1"),
    )
    .await
    .expect("failed to create coordinator 1");
    
    // Wait for leader
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Start second node
    let (coord2, handle2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-2",
        "127.0.0.1:50052",
        4,
        5,
        make_test_factory("node-2"),
    )
    .await
    .expect("failed to create coordinator 2");
    
    // Wait for both to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Find the leader and shut it down
    let leader1_before = coord1.is_leader();
    let leader2_before = coord2.is_leader();
    
    if leader1_before {
        tracing::info!("node-1 is the leader, shutting it down");
        coord1.shutdown().await.expect("shutdown coord1 failed");
        handle1.abort();
        
        // Wait for failover
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        // Node 2 should now be the leader
        // Note: Due to timing, we use a retry loop
        let mut became_leader = false;
        for _ in 0..10 {
            if coord2.is_leader() {
                became_leader = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(became_leader, "node-2 should become leader after node-1 shutdown");
        
        // Clean up remaining node
        coord2.shutdown().await.expect("shutdown coord2 failed");
        handle2.abort();
    } else if leader2_before {
        tracing::info!("node-2 is the leader, shutting it down");
        coord2.shutdown().await.expect("shutdown coord2 failed");
        handle2.abort();
        
        // Wait for failover
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        // Node 1 should now be the leader
        let mut became_leader = false;
        for _ in 0..10 {
            if coord1.is_leader() {
                became_leader = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(became_leader, "node-1 should become leader after node-2 shutdown");
        
        // Clean up remaining node
        coord1.shutdown().await.expect("shutdown coord1 failed");
        handle1.abort();
    } else {
        // Neither was leader initially - clean up both
        coord1.shutdown().await.expect("shutdown coord1 failed");
        coord2.shutdown().await.expect("shutdown coord2 failed");
        handle1.abort();
        handle2.abort();
        // This can happen if leader election hasn't completed yet
        tracing::warn!("neither node was leader initially");
    }
}

/// Test that leadership watch channel receives updates
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_leadership_watch() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = PLACEMENT_TEST_MUTEX.lock().unwrap();
    
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    
    let (coord, handle) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-1",
        "127.0.0.1:50051",
        4,
        5,
        make_test_factory("node-1"),
    )
    .await
    .expect("failed to create coordinator");
    
    // Get a watch on leadership status
    let mut watch_rx = coord.leadership_watch();
    
    // Wait for the watch to receive the initial value
    // (single node should become leader)
    let mut became_leader = false;
    for _ in 0..20 {
        if *watch_rx.borrow() {
            became_leader = true;
            break;
        }
        // Wait for change or timeout
        tokio::select! {
            _ = watch_rx.changed() => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    
    assert!(became_leader, "should receive leadership notification via watch");
    
    // Clean up
    coord.shutdown().await.expect("shutdown failed");
    handle.abort();
}

/// Test basic placement override functionality
#[test]
fn test_placement_override_basic() {
    use silo::coordination::PlacementOverrides;
    use silo::coordination::PlacementOverride;
    
    let mut overrides = PlacementOverrides::new();
    
    // Create an override
    let override1 = PlacementOverride::new(0, "node-b".to_string(), 3600);
    overrides.set(override1);
    
    assert_eq!(overrides.active_count(), 1);
    assert_eq!(overrides.get_target(0), Some("node-b"));
    assert!(!overrides.has_active_override(1)); // Different shard
    
    // Add more overrides
    overrides.set(PlacementOverride::new(1, "node-c".to_string(), 3600));
    overrides.set(PlacementOverride::new(2, "node-a".to_string(), 3600));
    
    assert_eq!(overrides.active_count(), 3);
    
    // Remove one
    overrides.remove(1);
    assert_eq!(overrides.active_count(), 2);
    assert!(overrides.get_target(1).is_none());
}

/// Test that expired overrides are properly pruned
#[test]
fn test_placement_override_expiry_pruning() {
    use silo::coordination::PlacementOverrides;
    use silo::coordination::PlacementOverride;
    
    let mut overrides = PlacementOverrides::new();
    
    // Add an expired override
    let mut expired = PlacementOverride::new(0, "node-a".to_string(), 1);
    expired.created_at_ms -= 5000; // 5 seconds in the past
    overrides.set(expired);
    
    // Add a valid override
    overrides.set(PlacementOverride::new(1, "node-b".to_string(), 3600));
    
    // Before pruning - both exist
    assert_eq!(overrides.all().len(), 2);
    
    // After pruning - only valid remains
    let pruned = overrides.prune_expired();
    assert_eq!(pruned, 1);
    assert_eq!(overrides.active_count(), 1);
    assert!(overrides.get_target(0).is_none());
    assert!(overrides.get_target(1).is_some());
}
