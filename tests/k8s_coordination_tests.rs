//! Integration tests for Kubernetes Lease-based coordination.
//!
//! These tests require the `k8s` feature and a running Kubernetes cluster.
//!
//! **IMPORTANT**: These tests must be run single-threaded to prevent interference:
//!   cargo test --features k8s k8s_ -- --test-threads=1
//!
//! Local development:
//!   Uses orbstack's built-in K8S (or any cluster where `kubectl cluster-info` works)
//!
//! CI:
//!   Uses kind (Kubernetes-in-Docker) - see .github/workflows/ci.yml

#![cfg(feature = "k8s")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use silo::coordination::k8s::K8sShardGuard;
use silo::coordination::{Coordinator, K8sCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use silo::shard_range::ShardId;
use tokio::sync::Mutex;

// Atomic counter for truly unique prefixes even within the same nanosecond
static PREFIX_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = PREFIX_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    // Use shortened format to keep lease names DNS-compatible (max 63 chars)
    format!("t{}-{}", nanos % 1_000_000_000_000, counter)
}

fn get_namespace() -> String {
    std::env::var("TEST_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string())
}

fn make_test_factory(node_id: &str) -> Arc<ShardFactory> {
    let tmpdir = std::env::temp_dir().join(format!("silo-k8s-test-{}", node_id));
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Helper to wait for a condition with timeout
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Start a K8S coordinator, skipping the test if K8S is not available
macro_rules! start_coordinator {
    ($namespace:expr, $prefix:expr, $node_id:expr, $grpc_addr:expr, $num_shards:expr) => {{
        let factory = make_test_factory($node_id);
        match K8sCoordinator::start(
            $namespace,
            $prefix,
            $node_id,
            $grpc_addr,
            $num_shards,
            10,
            factory,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Skipping K8S test - could not connect to cluster: {}", e);
                return;
            }
        }
    }};
}

// =============================================================================
// COORDINATOR-LEVEL TESTS
// =============================================================================

/// Test that a single K8S coordinator owns all shards when alone.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_single_node_owns_all_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;
    let namespace = get_namespace();

    let (coord, handle) = start_coordinator!(
        &namespace,
        &prefix,
        "test-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );

    // Wait for convergence
    assert!(
        coord.wait_converged(Duration::from_secs(10)).await,
        "coordinator should converge"
    );

    // Single node should own all shards
    let owned = coord.owned_shards().await;
    let expected: HashSet<ShardId> = coord
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    let owned_set: HashSet<ShardId> = owned.into_iter().collect();
    assert_eq!(owned_set, expected, "single node should own all shards");

    // Cleanup
    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that multiple K8S coordinators partition shards correctly.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_multiple_nodes_partition_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 16;
    let namespace = get_namespace();

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "test-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );

    // Small delay to let c1 start acquiring shards
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "test-node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("test-node-2"),
    )
    .await
    .expect("start c2");

    // Wait for both to converge with longer timeout
    assert!(
        c1.wait_converged(Duration::from_secs(30)).await,
        "c1 should converge"
    );
    assert!(
        c2.wait_converged(Duration::from_secs(30)).await,
        "c2 should converge"
    );

    // Check that all shards are covered
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    let all: HashSet<ShardId> = s1.union(&s2).copied().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(all, expected, "all shards should be owned");

    // Check that shards are disjoint
    assert!(s1.is_disjoint(&s2), "shard ownership should be disjoint");

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test that membership changes trigger rebalancing.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_rebalances_on_membership_change() {
    let prefix = unique_prefix();
    let num_shards: u32 = 12;
    let namespace = get_namespace();

    // Start with one node
    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "test-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );

    assert!(c1.wait_converged(Duration::from_secs(10)).await);
    let initial_owned: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(
        initial_owned, expected,
        "single node should own all initially"
    );

    // Add second node
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "test-node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("test-node-2"),
    )
    .await
    .expect("start c2");

    // Wait for both to converge after membership change
    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Check redistribution
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();

    // Node 1 should have fewer shards now
    assert!(
        s1.len() < num_shards as usize,
        "c1 should give up some shards"
    );
    assert!(!s2.is_empty(), "c2 should own some shards");
    assert!(s1.is_disjoint(&s2), "ownership should be disjoint");

    let all: HashSet<ShardId> = s1.union(&s2).copied().collect();
    assert_eq!(all, expected, "all shards should still be covered");

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test three nodes with larger shard count - checks distribution evenness
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_three_nodes_even_distribution() {
    let prefix = unique_prefix();
    let num_shards: u32 = 128;
    let namespace = get_namespace();

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "node-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("node-2"),
    )
    .await
    .expect("start c2");
    let (c3, h3) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("node-3"),
    )
    .await
    .expect("start c3");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);
    assert!(c3.wait_converged(Duration::from_secs(30)).await);

    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();

    // All shards covered
    let all: HashSet<ShardId> = s1
        .iter()
        .chain(s2.iter())
        .chain(s3.iter())
        .copied()
        .collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(all, expected, "all shards should be owned exactly once");

    // Disjoint ownership
    assert!(s1.is_disjoint(&s2), "s1 and s2 should be disjoint");
    assert!(s1.is_disjoint(&s3), "s1 and s3 should be disjoint");
    assert!(s2.is_disjoint(&s3), "s2 and s3 should be disjoint");

    // Distribution evenness (within 15% tolerance)
    let sizes = [s1.len(), s2.len(), s3.len()];
    let max = *sizes.iter().max().unwrap();
    let min = *sizes.iter().min().unwrap();
    let tolerance = ((num_shards as f32) * 0.15).ceil() as usize;
    assert!(
        max - min <= tolerance,
        "distribution should be roughly even: {:?} (tolerance: {})",
        sizes,
        tolerance
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
    h3.abort();
}

/// Test removing a node causes rebalancing
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_removing_node_rebalances() {
    let prefix = unique_prefix();
    let num_shards: u32 = 64;
    let namespace = get_namespace();

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "node-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("node-2"),
    )
    .await
    .expect("start c2");
    let (c3, h3) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("node-3"),
    )
    .await
    .expect("start c3");

    // Wait for initial convergence with 3 nodes
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);
    assert!(c3.wait_converged(Duration::from_secs(30)).await);

    // Remove node 3
    c3.shutdown().await.unwrap();
    h3.abort();

    // Wait for remaining nodes to reconverge
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "c1 should reconverge after node removal"
    );
    assert!(
        c2.wait_converged(Duration::from_secs(15)).await,
        "c2 should reconverge after node removal"
    );

    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();

    // All shards still covered
    let all: HashSet<ShardId> = s1.union(&s2).copied().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(
        all, expected,
        "all shards should be covered after node removal"
    );
    assert!(s1.is_disjoint(&s2), "ownership should remain disjoint");

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test rapid membership churn converges correctly
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_rapid_membership_churn_converges() {
    let prefix = unique_prefix();
    let num_shards: u32 = 32;
    let namespace = get_namespace();

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "node-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("node-2"),
    )
    .await
    .expect("start c2");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (c3, h3) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("node-3"),
    )
    .await
    .expect("start c3");

    // Brief churn: stop and restart c2 quickly
    tokio::time::sleep(Duration::from_millis(200)).await;
    c2.shutdown().await.unwrap();
    h2.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Restart c2
    let (c2b, h2b) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("node-2-restart"),
    )
    .await
    .expect("restart c2");

    // Wait for all to converge post-churn
    let deadline = Duration::from_secs(30);
    assert!(
        c1.wait_converged(deadline).await,
        "c1 should converge post-churn"
    );
    assert!(
        c2b.wait_converged(deadline).await,
        "c2b should converge post-churn"
    );
    assert!(
        c3.wait_converged(deadline).await,
        "c3 should converge post-churn"
    );

    // Validate ownership
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2b.owned_shards().await.into_iter().collect();
    let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();

    let all: HashSet<ShardId> = s1
        .iter()
        .chain(s2.iter())
        .chain(s3.iter())
        .copied()
        .collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(all, expected, "all shards covered after churn");
    assert!(s1.is_disjoint(&s2), "s1 and s2 disjoint");
    assert!(s1.is_disjoint(&s3), "s1 and s3 disjoint");
    assert!(s2.is_disjoint(&s3), "s2 and s3 disjoint");

    // Cleanup
    c1.shutdown().await.unwrap();
    c2b.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    h1.abort();
    h2b.abort();
    h3.abort();
}

/// Test get_members returns correct member info
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_get_members_returns_correct_info() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;
    let namespace = get_namespace();

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "member-node-1",
        "http://10.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "member-node-2",
        "http://10.0.0.2:50052",
        num_shards,
        10,
        make_test_factory("member-node-2"),
    )
    .await
    .expect("start c2");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Get members from both coordinators
    let members_from_c1 = c1.get_members().await.expect("get_members from c1");
    let members_from_c2 = c2.get_members().await.expect("get_members from c2");

    // Both should see 2 members
    assert_eq!(members_from_c1.len(), 2, "c1 should see 2 members");
    assert_eq!(members_from_c2.len(), 2, "c2 should see 2 members");

    // Check member info
    let member_ids: HashSet<String> = members_from_c1.iter().map(|m| m.node_id.clone()).collect();
    assert!(
        member_ids.contains("member-node-1"),
        "should contain node-1"
    );
    assert!(
        member_ids.contains("member-node-2"),
        "should contain node-2"
    );

    // Verify addresses
    for m in &members_from_c1 {
        match m.node_id.as_str() {
            "member-node-1" => assert_eq!(m.grpc_addr, "http://10.0.0.1:50051"),
            "member-node-2" => assert_eq!(m.grpc_addr, "http://10.0.0.2:50052"),
            other => panic!("unexpected member: {}", other),
        }
    }

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test get_shard_owner_map returns accurate mapping
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_get_shard_owner_map_accurate() {
    let prefix = unique_prefix();
    let num_shards: u32 = 16;
    let namespace = get_namespace();

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "map-node-1",
        "http://10.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "map-node-2",
        "http://10.0.0.2:50052",
        num_shards,
        10,
        make_test_factory("map-node-2"),
    )
    .await
    .expect("start c2");

    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Get shard owner map
    let map = c1.get_shard_owner_map().await.expect("get_shard_owner_map");

    assert_eq!(
        map.num_shards(),
        num_shards as usize,
        "num_shards should match"
    );

    // All shards should have owners
    let shards_with_owners: HashSet<ShardId> = map.shard_to_addr.keys().copied().collect();
    let expected: HashSet<ShardId> = map.shard_ids().into_iter().collect();
    assert_eq!(
        shards_with_owners, expected,
        "all shards should have owners in map"
    );

    // Owners should be one of our nodes
    for (shard_id, addr) in &map.shard_to_addr {
        assert!(
            addr == "http://10.0.0.1:50051" || addr == "http://10.0.0.2:50052",
            "shard {} has unexpected owner addr: {}",
            shard_id,
            addr
        );
    }

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test coordinator metadata accessors
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_coordinator_metadata() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;
    let namespace = get_namespace();

    let (coord, handle) = start_coordinator!(
        &namespace,
        &prefix,
        "meta-test-node",
        "http://192.168.1.1:9999",
        num_shards
    );

    assert_eq!(coord.num_shards().await, num_shards as usize);
    assert_eq!(coord.node_id(), "meta-test-node");
    assert_eq!(coord.grpc_addr(), "http://192.168.1.1:9999");

    coord.shutdown().await.unwrap();
    handle.abort();
}

// =============================================================================
// SHARD GUARD TESTS
// These test the lower-level K8sShardGuard directly
// =============================================================================

/// Helper to create a K8sShardGuard for testing
async fn make_k8s_guard(
    namespace: &str,
    cluster_prefix: &str,
    node_id: &str,
    shard_id: ShardId,
) -> Result<
    (
        Arc<K8sShardGuard>,
        Arc<Mutex<HashSet<ShardId>>>,
        tokio::sync::watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ),
    String,
> {
    let client = kube::Client::try_default()
        .await
        .map_err(|e| format!("K8S not available: {}", e))?;

    let owned = Arc::new(Mutex::new(HashSet::new()));
    let (tx, rx) = tokio::sync::watch::channel(false);

    let guard = K8sShardGuard::new(
        shard_id,
        client,
        namespace.to_string(),
        cluster_prefix.to_string(),
        node_id.to_string(),
        10, // lease duration seconds
        rx,
    );

    let runner = guard.clone();
    let owned_arc = owned.clone();
    let factory = make_test_factory(node_id);
    let handle = tokio::spawn(async move {
        runner.run(owned_arc, factory).await;
    });

    Ok((guard, owned, tx, handle))
}

/// Test basic shard guard acquire and release
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_acquire_and_release() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, _tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "guard-node-1", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S shard guard test: {}", e);
                return;
            }
        };

    // Request acquisition
    guard.set_desired(true).await;

    // Wait until held
    let acquired = wait_until(Duration::from_secs(5), || async {
        guard.is_held().await && owned.lock().await.contains(&shard_id)
    })
    .await;
    assert!(acquired, "guard should acquire lease and mark owned");

    // Release
    guard.set_desired(false).await;

    let released = wait_until(Duration::from_secs(5), || async {
        !guard.is_held().await && !owned.lock().await.contains(&shard_id)
    })
    .await;
    assert!(released, "guard should release lease and clear owned");

    handle.abort();
}

/// Test two guards competing for the same shard - exactly one should win
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_shard_guard_contention() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (g1, owned1, _tx1, h1) =
        match make_k8s_guard(&namespace, &prefix, "contend-node-1", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S contention test: {}", e);
                return;
            }
        };
    let (g2, owned2, _tx2, h2) = make_k8s_guard(&namespace, &prefix, "contend-node-2", shard_id)
        .await
        .unwrap();

    // Both try to acquire
    g1.set_desired(true).await;
    g2.set_desired(true).await;

    // Wait until exactly one holds
    let one_holds = wait_until(Duration::from_secs(8), || async {
        let h1 = g1.is_held().await;
        let h2 = g2.is_held().await;
        h1 ^ h2 // XOR: exactly one should hold
    })
    .await;
    assert!(
        one_holds,
        "exactly one guard should acquire under contention"
    );

    // Verify owned sets are also exclusive
    let o1 = owned1.lock().await.contains(&shard_id);
    let o2 = owned2.lock().await.contains(&shard_id);
    assert!(o1 ^ o2, "exactly one owned set should contain the shard");

    // Cleanup
    g1.set_desired(false).await;
    g2.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    h1.abort();
    h2.abort();
}

/// Test shard handoff when the owner releases
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_shard_guard_handoff() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (g1, owned1, _tx1, h1) =
        match make_k8s_guard(&namespace, &prefix, "handoff-node-1", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S handoff test: {}", e);
                return;
            }
        };
    let (g2, owned2, _tx2, h2) = make_k8s_guard(&namespace, &prefix, "handoff-node-2", shard_id)
        .await
        .unwrap();

    // g1 acquires first
    g1.set_desired(true).await;
    let g1_acquired = wait_until(Duration::from_secs(5), || async { g1.is_held().await }).await;
    assert!(g1_acquired, "g1 should acquire");

    // g2 also wants it (will be waiting)
    g2.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // g1 releases
    g1.set_desired(false).await;

    // g2 should acquire after handoff
    let g2_acquired = wait_until(Duration::from_secs(10), || async {
        g2.is_held().await && owned2.lock().await.contains(&shard_id)
    })
    .await;
    assert!(g2_acquired, "g2 should acquire after g1 releases");

    // g1 should no longer hold
    assert!(!g1.is_held().await, "g1 should not hold after release");
    assert!(
        !owned1.lock().await.contains(&shard_id),
        "g1 owned should be cleared"
    );

    // Cleanup
    g2.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    h1.abort();
    h2.abort();
}

/// Test that calling set_desired(true) twice is idempotent
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_idempotent_set_desired() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, _tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "idemp-node", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S idempotent test: {}", e);
                return;
            }
        };

    guard.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(5), || async { guard.is_held().await }).await;
    assert!(acquired, "should acquire initially");

    // Call set_desired(true) again - should not cause release/reacquire
    guard.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Still held
    assert!(
        guard.is_held().await,
        "should still be held after idempotent call"
    );
    assert!(
        owned.lock().await.contains(&shard_id),
        "should still be in owned set"
    );

    // Cleanup
    guard.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async { !guard.is_held().await }).await;
    handle.abort();
}

/// Test quick flip from false to true during release
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_quick_flip_reacquires() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, _tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "flip-node", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S quick flip test: {}", e);
                return;
            }
        };

    guard.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(5), || async { guard.is_held().await }).await;
    assert!(acquired, "initial acquire");

    // Flip to false then quickly back to true
    guard.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    guard.set_desired(true).await;

    // Should end up held
    let reacquired = wait_until(Duration::from_secs(8), || async { guard.is_held().await }).await;
    assert!(reacquired, "should reacquire after flip");
    assert!(
        owned.lock().await.contains(&shard_id),
        "owned set should include shard"
    );

    // Cleanup
    guard.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async { !guard.is_held().await }).await;
    handle.abort();
}

/// Test that acquisition aborts when desired changes to false
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_shard_guard_acquire_aborts_on_desired_change() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    // g1 holds the lock to block g2's acquisition
    let (g1, _o1, _tx1, h1) =
        match make_k8s_guard(&namespace, &prefix, "abort-node-1", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S abort test: {}", e);
                return;
            }
        };
    g1.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(5), || async { g1.is_held().await }).await,
        "g1 should hold"
    );

    // g2 starts acquiring then we cancel
    let (g2, o2, _tx2, h2) = make_k8s_guard(&namespace, &prefix, "abort-node-2", shard_id)
        .await
        .unwrap();
    g2.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    g2.set_desired(false).await;

    // g2 should not end up holding
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !g2.is_held().await,
        "g2 should not hold after aborting acquire"
    );
    assert!(
        !o2.lock().await.contains(&shard_id),
        "g2 owned should not contain shard"
    );

    // Cleanup
    g1.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async { !g1.is_held().await }).await;
    h1.abort();
    h2.abort();
}

/// Test shutdown while idle
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_shutdown_while_idle() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "shutdown-idle", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S shutdown idle test: {}", e);
                return;
            }
        };

    // Don't acquire, just stay idle
    assert!(!guard.is_held().await, "should start idle");

    // Signal shutdown
    let _ = tx.send(true);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // No ownership
    assert!(!owned.lock().await.contains(&shard_id));
    handle.abort();
}

/// Test shutdown while holding - should release cleanly
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_shutdown_while_held() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "shutdown-held", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S shutdown held test: {}", e);
                return;
            }
        };

    guard.set_desired(true).await;
    assert!(wait_until(Duration::from_secs(5), || async { guard.is_held().await }).await);
    assert!(owned.lock().await.contains(&shard_id));

    // Shutdown - signal and then notify to wake up the renewal loop
    let _ = tx.send(true);
    guard.notify.notify_one();

    // Wait for owned to be cleared (the shutdown path should clear it)
    let released = wait_until(Duration::from_secs(5), || async {
        !owned.lock().await.contains(&shard_id)
    })
    .await;
    assert!(released, "owned should be cleared on shutdown");
    handle.abort();
}

// =============================================================================
// SAFETY & CORRECTNESS TESTS
// =============================================================================

/// Critical test: Verify no split-brain during transitions
/// At no point should two nodes simultaneously believe they own the same shard
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_no_split_brain_during_transitions() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 16;

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "brain-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "brain-node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("brain-node-2"),
    )
    .await
    .expect("start c2");

    // Initial convergence
    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Add a third node to trigger rebalancing
    let (c3, h3) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "brain-node-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("brain-node-3"),
    )
    .await
    .expect("start c3");

    // During rebalancing, repeatedly check that no shard has multiple owners
    let mut split_brain_detected = false;
    let check_deadline = Instant::now() + Duration::from_secs(20);

    while Instant::now() < check_deadline {
        let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
        let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
        let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();

        // Check for overlaps
        let s1_s2_overlap: HashSet<_> = s1.intersection(&s2).collect();
        let s1_s3_overlap: HashSet<_> = s1.intersection(&s3).collect();
        let s2_s3_overlap: HashSet<_> = s2.intersection(&s3).collect();

        if !s1_s2_overlap.is_empty() || !s1_s3_overlap.is_empty() || !s2_s3_overlap.is_empty() {
            eprintln!("SPLIT BRAIN DETECTED!");
            eprintln!("  c1 owns: {:?}", s1);
            eprintln!("  c2 owns: {:?}", s2);
            eprintln!("  c3 owns: {:?}", s3);
            split_brain_detected = true;
            break;
        }

        // Check if converged
        if c1.wait_converged(Duration::from_millis(10)).await
            && c2.wait_converged(Duration::from_millis(10)).await
            && c3.wait_converged(Duration::from_millis(10)).await
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        !split_brain_detected,
        "split-brain should never occur during transitions"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
    h3.abort();
}

/// Test that shards are acquired promptly after another node leaves
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_prompt_acquisition_after_node_departure() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 8;

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "prompt-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "prompt-node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("prompt-node-2"),
    )
    .await
    .expect("start c2");

    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Record what c2 owned
    let c2_shards: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert!(!c2_shards.is_empty(), "c2 should own some shards");

    // Shutdown c2
    let shutdown_start = Instant::now();
    c2.shutdown().await.unwrap();
    h2.abort();

    // Give some time for membership change to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // c1 should converge and own all shards reasonably quickly
    // Use a longer timeout (30s) since K8S API can have latency
    let converged = c1.wait_converged(Duration::from_secs(30)).await;
    let convergence_time = shutdown_start.elapsed();

    assert!(converged, "c1 should converge after c2 departure");
    // Allow up to 25 seconds for convergence (K8S leases may take time to expire)
    assert!(
        convergence_time < Duration::from_secs(25),
        "convergence should be reasonably prompt (was {:?})",
        convergence_time
    );

    let c1_final: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(
        c1_final, expected,
        "c1 should own all shards after c2 leaves"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    h1.abort();
}

/// Test multiple consecutive add/remove cycles
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_multiple_add_remove_cycles() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 16; // Reduced for faster test

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "cycle-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    assert!(
        c1.wait_converged(Duration::from_secs(20)).await,
        "c1 initial converge"
    );

    for i in 0..2 {
        // Reduced iterations
        // Add node
        let (c2, h2) = K8sCoordinator::start(
            &namespace,
            &prefix,
            &format!("cycle-node-2-iter-{}", i),
            "http://127.0.0.1:50052",
            num_shards,
            10,
            make_test_factory(&format!("cycle-node-2-iter-{}", i)),
        )
        .await
        .expect("start c2");

        assert!(
            c1.wait_converged(Duration::from_secs(30)).await,
            "c1 converge after add iter {}",
            i
        );
        assert!(
            c2.wait_converged(Duration::from_secs(30)).await,
            "c2 converge iter {}",
            i
        );

        // Verify partition
        let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
        let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
        assert!(s1.is_disjoint(&s2), "disjoint at iter {}", i);
        let all: HashSet<ShardId> = s1.union(&s2).copied().collect();
        let expected: HashSet<ShardId> = c1
            .get_shard_map()
            .await
            .unwrap()
            .shard_ids()
            .into_iter()
            .collect();
        assert_eq!(all, expected, "all covered at iter {}", i);

        // Remove node
        c2.shutdown().await.unwrap();
        h2.abort();

        // Wait for membership change to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            c1.wait_converged(Duration::from_secs(30)).await,
            "c1 converge after remove iter {}",
            i
        );
        let s1_after: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
        assert_eq!(s1_after, expected, "c1 owns all after remove iter {}", i);
    }

    c1.shutdown().await.unwrap();
    h1.abort();
}

/// Test that the system handles 4+ nodes correctly
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_four_node_cluster() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 32; // Reduced from 64 to speed up test

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "four-node-1",
        "http://127.0.0.1:50051",
        num_shards
    );

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "four-node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("four-node-2"),
    )
    .await
    .expect("start c2");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c3, h3) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "four-node-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("four-node-3"),
    )
    .await
    .expect("start c3");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c4, h4) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "four-node-4",
        "http://127.0.0.1:50054",
        num_shards,
        10,
        make_test_factory("four-node-4"),
    )
    .await
    .expect("start c4");

    // Convergence with longer timeout for 4 nodes
    let timeout = Duration::from_secs(60);
    assert!(c1.wait_converged(timeout).await, "c1 converge");
    assert!(c2.wait_converged(timeout).await, "c2 converge");
    assert!(c3.wait_converged(timeout).await, "c3 converge");
    assert!(c4.wait_converged(timeout).await, "c4 converge");

    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();
    let s4: HashSet<ShardId> = c4.owned_shards().await.into_iter().collect();

    // All covered
    let all: HashSet<ShardId> = s1
        .iter()
        .chain(s2.iter())
        .chain(s3.iter())
        .chain(s4.iter())
        .copied()
        .collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(all, expected, "all shards covered");

    // All disjoint
    assert!(s1.is_disjoint(&s2) && s1.is_disjoint(&s3) && s1.is_disjoint(&s4));
    assert!(s2.is_disjoint(&s3) && s2.is_disjoint(&s4));
    assert!(s3.is_disjoint(&s4));

    // Distribution check (within 25% for 4 nodes with smaller shard count)
    let sizes = [s1.len(), s2.len(), s3.len(), s4.len()];
    let max = *sizes.iter().max().unwrap();
    let min = *sizes.iter().min().unwrap();
    let tolerance = ((num_shards as f32) * 0.25).ceil() as usize;
    assert!(
        max - min <= tolerance,
        "distribution should be roughly even: {:?} (tolerance: {})",
        sizes,
        tolerance
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    c4.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
    h3.abort();
    h4.abort();
}

/// Test shutdown during the acquiring phase (guard blocked waiting for lease)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_shard_guard_shutdown_while_acquiring() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    // g1 holds the lock so g2 will be stuck acquiring
    let (g1, _o1, _tx1, h1) =
        match make_k8s_guard(&namespace, &prefix, "acq-shutdown-1", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S shutdown-while-acquiring test: {}", e);
                return;
            }
        };
    g1.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(5), || async { g1.is_held().await }).await,
        "g1 should acquire"
    );

    // g2 starts acquiring (will block)
    let (g2, o2, tx2, h2) = make_k8s_guard(&namespace, &prefix, "acq-shutdown-2", shard_id)
        .await
        .unwrap();
    g2.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Signal shutdown to g2 while it's blocked acquiring
    let _ = tx2.send(true);
    g2.notify.notify_one();

    // g2 should NOT end up holding (it should abort acquisition)
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !g2.is_held().await,
        "g2 should not hold after shutdown during acquire"
    );
    assert!(
        !o2.lock().await.contains(&shard_id),
        "g2 owned should be empty"
    );

    // g1 should still hold
    assert!(g1.is_held().await, "g1 should still hold");

    // Cleanup
    g1.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async { !g1.is_held().await }).await;
    h1.abort();
    h2.abort();
}

/// Test shutdown during the releasing phase (100ms delay window)
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_shutdown_while_releasing() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "rel-shutdown", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S shutdown-while-releasing test: {}", e);
                return;
            }
        };

    // Acquire first
    guard.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(5), || async { guard.is_held().await }).await,
        "should acquire"
    );
    assert!(
        owned.lock().await.contains(&shard_id),
        "should be in owned set"
    );

    // Start release
    guard.set_desired(false).await;
    // Immediately send shutdown during the release delay
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = tx.send(true);
    guard.notify.notify_one();

    // Should complete shutdown cleanly
    let released = wait_until(Duration::from_secs(5), || async {
        !owned.lock().await.contains(&shard_id)
    })
    .await;
    assert!(
        released,
        "owned should be cleared after shutdown during release"
    );

    handle.abort();
}

/// Test three guards competing for the same shard - exactly one should win
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_shard_guard_three_way_contention() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (g1, o1, _tx1, h1) =
        match make_k8s_guard(&namespace, &prefix, "three-way-1", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S three-way contention test: {}", e);
                return;
            }
        };
    let (g2, o2, _tx2, h2) = make_k8s_guard(&namespace, &prefix, "three-way-2", shard_id)
        .await
        .unwrap();
    let (g3, o3, _tx3, h3) = make_k8s_guard(&namespace, &prefix, "three-way-3", shard_id)
        .await
        .unwrap();

    // All three try to acquire simultaneously
    g1.set_desired(true).await;
    g2.set_desired(true).await;
    g3.set_desired(true).await;

    // Wait until exactly one holds
    let one_holds = wait_until(Duration::from_secs(10), || async {
        let h1 = g1.is_held().await;
        let h2 = g2.is_held().await;
        let h3 = g3.is_held().await;
        let count = [h1, h2, h3].iter().filter(|&&x| x).count();
        count == 1
    })
    .await;
    assert!(
        one_holds,
        "exactly one guard should acquire under three-way contention"
    );

    // Verify owned sets are also exclusive
    let o1_has = o1.lock().await.contains(&shard_id);
    let o2_has = o2.lock().await.contains(&shard_id);
    let o3_has = o3.lock().await.contains(&shard_id);
    let owned_count = [o1_has, o2_has, o3_has].iter().filter(|&&x| x).count();
    assert_eq!(
        owned_count, 1,
        "exactly one owned set should contain the shard"
    );

    // Cleanup
    g1.set_desired(false).await;
    g2.set_desired(false).await;
    g3.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    h1.abort();
    h2.abort();
    h3.abort();
}

/// Test rapid toggling of desired state (stress test the state machine)
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_rapid_desired_toggling() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, _tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "rapid-toggle", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S rapid toggling test: {}", e);
                return;
            }
        };

    // Rapidly toggle desired state multiple times
    for _ in 0..5 {
        guard.set_desired(true).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        guard.set_desired(false).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // End with desired=true
    guard.set_desired(true).await;

    // Should eventually stabilize to Held
    let held = wait_until(Duration::from_secs(10), || async {
        guard.is_held().await && owned.lock().await.contains(&shard_id)
    })
    .await;
    assert!(held, "should stabilize to Held after rapid toggling");

    // Cleanup
    guard.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async { !guard.is_held().await }).await;
    handle.abort();
}

/// Test node restart with same ID (simulates pod restart)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_node_restart_same_id() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 8;

    // Start first instance
    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "restart-node",
        "http://127.0.0.1:50051",
        num_shards
    );
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "c1 initial converge"
    );

    let initial_owned: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(initial_owned, expected, "single node should own all");

    // Shutdown first instance
    c1.shutdown().await.unwrap();
    h1.abort();

    // Small delay to simulate restart
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start second instance with SAME node ID (simulates pod restart)
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "restart-node",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory("restart-node-2"),
    )
    .await
    .expect("restart node");

    // Should converge and own all shards
    assert!(
        c2.wait_converged(Duration::from_secs(20)).await,
        "restarted node should converge"
    );
    let restarted_owned: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert_eq!(
        restarted_owned, expected,
        "restarted node should own all shards"
    );

    // Cleanup
    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test immediate shutdown after coordinator start (before initial reconciliation completes)
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_immediate_shutdown_after_start() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 4;

    let (coord, handle) = start_coordinator!(
        &namespace,
        &prefix,
        "immediate-shutdown",
        "http://127.0.0.1:50051",
        num_shards
    );

    // Immediately shutdown without waiting for convergence
    coord.shutdown().await.unwrap();
    handle.abort();

    // Should not panic or hang - test passes if we get here
}

/// Test simultaneous node additions (race condition for membership)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_simultaneous_node_additions() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 16;

    // Start all three nodes as simultaneously as possible
    let f1 = K8sCoordinator::start(
        &namespace,
        &prefix,
        "simul-1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory("simul-1"),
    );
    let f2 = K8sCoordinator::start(
        &namespace,
        &prefix,
        "simul-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("simul-2"),
    );
    let f3 = K8sCoordinator::start(
        &namespace,
        &prefix,
        "simul-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("simul-3"),
    );

    let (r1, r2, r3) = tokio::join!(f1, f2, f3);
    let (c1, h1) = match r1 {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping simultaneous additions test: {}", e);
            return;
        }
    };
    let (c2, h2) = r2.expect("c2");
    let (c3, h3) = r3.expect("c3");

    // All should eventually converge
    let timeout = Duration::from_secs(30);
    assert!(c1.wait_converged(timeout).await, "c1 should converge");
    assert!(c2.wait_converged(timeout).await, "c2 should converge");
    assert!(c3.wait_converged(timeout).await, "c3 should converge");

    // All shards should be covered with no overlaps
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();

    let all: HashSet<ShardId> = s1
        .iter()
        .chain(s2.iter())
        .chain(s3.iter())
        .copied()
        .collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(
        all, expected,
        "all shards covered after simultaneous additions"
    );

    assert!(
        s1.is_disjoint(&s2) && s1.is_disjoint(&s3) && s2.is_disjoint(&s3),
        "no overlapping ownership after simultaneous additions"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
    h3.abort();
}

/// Test concurrent shutdown of multiple nodes
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_concurrent_shutdown_multiple_nodes() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 16;

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "conc-shut-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "conc-shut-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("conc-shut-2"),
    )
    .await
    .expect("c2");
    let (c3, h3) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "conc-shut-3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory("conc-shut-3"),
    )
    .await
    .expect("c3");

    // Wait for initial convergence
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);
    assert!(c3.wait_converged(Duration::from_secs(30)).await);

    // Shutdown c2 and c3 concurrently
    let (r2, r3) = tokio::join!(c2.shutdown(), c3.shutdown());
    r2.unwrap();
    r3.unwrap();
    h2.abort();
    h3.abort();

    // c1 should eventually own all shards
    assert!(
        c1.wait_converged(Duration::from_secs(30)).await,
        "c1 should converge after concurrent shutdowns"
    );

    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(
        s1, expected,
        "c1 should own all shards after concurrent peer shutdowns"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    h1.abort();
}

/// Test very short lease TTL (aggressive expiry)
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_short_lease_ttl() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 4;
    let short_ttl: i64 = 5; // 5 seconds (minimum practical)

    let (c1, h1) = match K8sCoordinator::start(
        &namespace,
        &prefix,
        "short-ttl-1",
        "http://127.0.0.1:50051",
        num_shards,
        short_ttl,
        make_test_factory("short-ttl-1"),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping short TTL test: {}", e);
            return;
        }
    };

    // Should still converge with short TTL
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "should converge with short TTL"
    );

    // Should maintain ownership over time (renewals working)
    tokio::time::sleep(Duration::from_secs(8)).await; // Longer than TTL

    let owned = c1.owned_shards().await;
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    let owned_set: HashSet<ShardId> = owned.into_iter().collect();
    assert_eq!(
        owned_set, expected,
        "should still own all shards after TTL period (renewals working)"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    h1.abort();
}

/// Test handoff chain: g1 -> g2 -> g3 in sequence
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_shard_guard_handoff_chain() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (g1, o1, _tx1, h1) = match make_k8s_guard(&namespace, &prefix, "chain-1", shard_id).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Skipping K8S handoff chain test: {}", e);
            return;
        }
    };
    let (g2, o2, _tx2, h2) = make_k8s_guard(&namespace, &prefix, "chain-2", shard_id)
        .await
        .unwrap();
    let (g3, o3, _tx3, h3) = make_k8s_guard(&namespace, &prefix, "chain-3", shard_id)
        .await
        .unwrap();

    // g1 acquires first
    g1.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(5), || async { g1.is_held().await }).await,
        "g1 initial acquire"
    );

    // g2 and g3 want it
    g2.set_desired(true).await;
    g3.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // g1 releases -> g2 or g3 should acquire
    g1.set_desired(false).await;
    let second_acquired = wait_until(Duration::from_secs(8), || async {
        (g2.is_held().await || g3.is_held().await) && !g1.is_held().await
    })
    .await;
    assert!(
        second_acquired,
        "second guard should acquire after g1 releases"
    );

    // Determine who got it
    let g2_held = g2.is_held().await;
    let g3_held = g3.is_held().await;
    assert!(g2_held ^ g3_held, "exactly one of g2/g3 should hold");

    // Release the current holder -> third should get it
    if g2_held {
        g2.set_desired(false).await;
        let third = wait_until(Duration::from_secs(8), || async {
            g3.is_held().await && !g2.is_held().await
        })
        .await;
        assert!(third, "g3 should acquire after g2 releases");
    } else {
        g3.set_desired(false).await;
        let third = wait_until(Duration::from_secs(8), || async {
            g2.is_held().await && !g3.is_held().await
        })
        .await;
        assert!(third, "g2 should acquire after g3 releases");
    }

    // Verify ownership tracking is correct
    let o1_has = o1.lock().await.contains(&shard_id);
    let o2_has = o2.lock().await.contains(&shard_id);
    let o3_has = o3.lock().await.contains(&shard_id);
    let total = [o1_has, o2_has, o3_has].iter().filter(|&&x| x).count();
    assert_eq!(total, 1, "exactly one owned set should have the shard");

    // Cleanup
    g1.set_desired(false).await;
    g2.set_desired(false).await;
    g3.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    h1.abort();
    h2.abort();
    h3.abort();
}

/// Test that owned shards list remains stable during steady state
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_ownership_stability_during_steady_state() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 16;

    let (c1, h1) = start_coordinator!(
        &namespace,
        &prefix,
        "stable-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "stable-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("stable-2"),
    )
    .await
    .expect("c2");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);

    // Record initial ownership
    let s1_initial: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2_initial: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();

    // Wait for a while with no changes
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check ownership hasn't changed
    let s1_after: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2_after: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();

    assert_eq!(s1_initial, s1_after, "c1 ownership should remain stable");
    assert_eq!(s2_initial, s2_after, "c2 ownership should remain stable");

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test shard guard set_desired(false) while already idle is a no-op
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_shard_guard_release_while_idle() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let shard_id = ShardId::new();

    let (guard, owned, _tx, handle) =
        match make_k8s_guard(&namespace, &prefix, "release-idle", shard_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("Skipping K8S release-while-idle test: {}", e);
                return;
            }
        };

    // Should start idle and not owned
    assert!(!guard.is_held().await, "should start not held");
    assert!(
        !owned.lock().await.contains(&shard_id),
        "should start not owned"
    );

    // Calling set_desired(false) while idle should be a no-op
    guard.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Still idle and not owned
    assert!(!guard.is_held().await, "should remain not held");
    assert!(
        !owned.lock().await.contains(&shard_id),
        "should remain not owned"
    );

    handle.abort();
}

/// Test coordinator with zero shards (edge case)
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_coordinator_zero_shards() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 0;

    let (coord, handle) = start_coordinator!(
        &namespace,
        &prefix,
        "zero-shards",
        "http://127.0.0.1:50051",
        num_shards
    );

    // Should converge immediately (nothing to own)
    assert!(
        coord.wait_converged(Duration::from_secs(5)).await,
        "should converge with zero shards"
    );

    // Should have no owned shards
    let owned = coord.owned_shards().await;
    assert!(owned.is_empty(), "should own no shards");

    // Cleanup
    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that a clean shutdown properly releases shards for other nodes to acquire
/// Note: Testing true crash behavior (task abort) is complex because spawned shard guard
/// tasks continue running independently. This test verifies the graceful shutdown path works.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn k8s_graceful_shutdown_releases_shards_promptly() {
    let prefix = unique_prefix();
    let namespace = get_namespace();
    let num_shards: u32 = 8;
    let short_ttl: i64 = 5; // 5 seconds for faster test

    // Start two nodes with short TTL
    let (c1, h1) = match K8sCoordinator::start(
        &namespace,
        &prefix,
        "graceful-1",
        "http://127.0.0.1:50051",
        num_shards,
        short_ttl,
        make_test_factory("graceful-1"),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping graceful shutdown test: {}", e);
            return;
        }
    };
    let (c2, h2) = K8sCoordinator::start(
        &namespace,
        &prefix,
        "graceful-2",
        "http://127.0.0.1:50052",
        num_shards,
        short_ttl,
        make_test_factory("graceful-2"),
    )
    .await
    .expect("c2");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);

    // Record what c2 owns
    let c2_initial: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert!(!c2_initial.is_empty(), "c2 should own some shards");

    // Graceful shutdown of c2 (releases shards and deletes membership lease)
    c2.shutdown().await.unwrap();
    h2.abort();

    // c1 should quickly acquire c2's shards
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "c1 should converge after c2 graceful shutdown"
    );

    // c1 should now own all shards
    let c1_final: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(
        c1_final, expected,
        "c1 should own all shards after c2 graceful shutdown"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    h1.abort();
}
