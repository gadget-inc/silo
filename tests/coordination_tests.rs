use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use silo::coordination::{Coordinator, EtcdCoordinator, etcd::EtcdConnection};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};

// Global mutex to serialize coordination tests
static COORDINATION_TEST_MUTEX: Mutex<()> = Mutex::new(());

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-{}", nanos)
}

fn make_test_factory(prefix: &str, node_id: &str) -> Arc<ShardFactory> {
    let tmpdir = std::env::temp_dir().join(format!("silo-coord-test-{}-{}", prefix, node_id));
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

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_nodes_own_unique_shards() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();

    // Assumes etcd is running locally (e.g., `just etcd` or via dev shell)
    let prefix = unique_prefix();
    let num_shards: u32 = 128;

    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    // Start three coordinators (nodes)
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .expect("start c1");
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
    )
    .await
    .expect("start c2");
    let (c3, h3) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory(&prefix, "n3"),
    )
    .await
    .expect("start c3");

    // Rely on convergence and explicit membership checks instead of per-node readiness

    // Ensure all 3 members are visible in membership before convergence
    let mut kv = coord.client().kv_client();
    let members_prefix = silo::coordination::keys::members_prefix(&prefix);
    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("members did not reach 3 within timeout");
        }
        let resp = kv
            .get(
                members_prefix.clone(),
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await
            .expect("read members");
        if resp.kvs().len() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Ensure coverage: wait for convergence on all nodes and union must equal 0..num_shards
    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);
    assert!(c3.wait_converged(Duration::from_secs(20)).await);
    let s1 = c1.owned_shards().await;
    let s2 = c2.owned_shards().await;
    let s3 = c3.owned_shards().await;
    let all: HashSet<u32> = s1
        .iter()
        .copied()
        .chain(s2.iter().copied())
        .chain(s3.iter().copied())
        .collect();
    let expected: HashSet<u32> = (0..num_shards).collect();
    assert_eq!(all, expected, "all shards should be owned exactly once");

    // Ensure uniqueness: no overlaps between nodes
    let set1: HashSet<u32> = s1.iter().copied().collect();
    let set2: HashSet<u32> = s2.iter().copied().collect();
    let set3: HashSet<u32> = s3.iter().copied().collect();
    assert!(set1.is_disjoint(&set2));
    assert!(set1.is_disjoint(&set3));
    assert!(set2.is_disjoint(&set3));

    // validate distribution sanity with a reasonable tolerance (10% of shards)
    let sizes = [set1.len(), set2.len(), set3.len()];
    let max = *sizes.iter().max().unwrap();
    let min = *sizes.iter().min().unwrap();
    let tolerance = ((num_shards as f32) * 0.10).ceil() as usize; // 10%
    assert!(
        max - min <= tolerance,
        "distribution should be roughly even: {:?}",
        sizes
    );

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2.abort();
    let _ = h3.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn adding_a_node_rebalances_shards() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();

    let prefix = unique_prefix();
    let num_shards: u32 = 128;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .unwrap();
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
    )
    .await
    .unwrap();
    assert!(c1.wait_converged(std::time::Duration::from_secs(20)).await);
    assert!(c2.wait_converged(std::time::Duration::from_secs(20)).await);

    let before_union: HashSet<u32> = c1
        .owned_shards()
        .await
        .into_iter()
        .chain(c2.owned_shards().await.into_iter())
        .collect();
    let expected: HashSet<u32> = (0..num_shards).collect();
    assert_eq!(before_union, expected);

    // Add new node
    let (c3, h3) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory(&prefix, "n3"),
    )
    .await
    .unwrap();
    // Converge after adding the new member
    assert!(c1.wait_converged(std::time::Duration::from_secs(5)).await);
    assert!(c2.wait_converged(std::time::Duration::from_secs(5)).await);
    assert!(c3.wait_converged(std::time::Duration::from_secs(5)).await);

    let s1 = c1.owned_shards().await;
    let s2 = c2.owned_shards().await;
    let s3 = c3.owned_shards().await;
    let all: HashSet<u32> = s1
        .iter()
        .copied()
        .chain(s2.iter().copied())
        .chain(s3.iter().copied())
        .collect();
    let expected: HashSet<u32> = (0..num_shards).collect();
    assert_eq!(all, expected);
    assert!(
        HashSet::<u32>::from_iter(s1.iter().copied())
            .is_disjoint(&HashSet::from_iter(s2.iter().copied()))
    );
    assert!(
        HashSet::<u32>::from_iter(s1.iter().copied())
            .is_disjoint(&HashSet::from_iter(s3.iter().copied()))
    );
    assert!(
        HashSet::<u32>::from_iter(s2.iter().copied())
            .is_disjoint(&HashSet::from_iter(s3.iter().copied()))
    );

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2.abort();
    let _ = h3.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_a_node_rebalances_shards() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();

    let prefix = unique_prefix();
    let num_shards: u32 = 128;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .unwrap();
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
    )
    .await
    .unwrap();
    let (c3, h3) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory(&prefix, "n3"),
    )
    .await
    .unwrap();
    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);
    assert!(c3.wait_converged(Duration::from_secs(20)).await);

    // Remove node 3
    c3.shutdown().await.unwrap();
    let _ = h3.abort();

    // Wait for convergence again
    assert!(c1.wait_converged(std::time::Duration::from_secs(10)).await);
    assert!(c2.wait_converged(std::time::Duration::from_secs(10)).await);
    let s1 = c1.owned_shards().await;
    let s2 = c2.owned_shards().await;
    let all: HashSet<u32> = s1.iter().copied().chain(s2.iter().copied()).collect();
    let expected: HashSet<u32> = (0..num_shards).collect();
    assert_eq!(all, expected);
    assert!(
        HashSet::<u32>::from_iter(s1.iter().copied())
            .is_disjoint(&HashSet::from_iter(s2.iter().copied()))
    );

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn rapid_membership_churn_converges() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();

    let prefix = unique_prefix();
    let num_shards: u32 = 128;
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");

    // Start first node, then quickly add/remove others to simulate churn
    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2"),
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (c3, h3) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n3",
        "http://127.0.0.1:50053",
        num_shards,
        10,
        make_test_factory(&prefix, "n3"),
    )
    .await
    .unwrap();

    // Brief churn: stop and restart n2 quickly
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    c2.shutdown().await.unwrap();
    let _ = h2.abort();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (c2b, h2b) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory(&prefix, "n2b"),
    )
    .await
    .unwrap();

    // Wait for all to converge post-churn
    let deadline = std::time::Duration::from_secs(20);
    assert!(c1.wait_converged(deadline).await);
    assert!(c2b.wait_converged(deadline).await);
    assert!(c3.wait_converged(deadline).await);

    // Validate ownership covers all shards and is disjoint
    let s1 = c1.owned_shards().await;
    let s2 = c2b.owned_shards().await;
    let s3 = c3.owned_shards().await;
    let all: std::collections::HashSet<u32> = s1
        .iter()
        .copied()
        .chain(s2.iter().copied())
        .chain(s3.iter().copied())
        .collect();
    let expected: std::collections::HashSet<u32> = (0..num_shards).collect();
    assert_eq!(all, expected);
    assert!(
        std::collections::HashSet::<u32>::from_iter(s1.iter().copied())
            .is_disjoint(&std::collections::HashSet::from_iter(s2.iter().copied()))
    );
    assert!(
        std::collections::HashSet::<u32>::from_iter(s1.iter().copied())
            .is_disjoint(&std::collections::HashSet::from_iter(s3.iter().copied()))
    );
    assert!(
        std::collections::HashSet::<u32>::from_iter(s2.iter().copied())
            .is_disjoint(&std::collections::HashSet::from_iter(s3.iter().copied()))
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    c2b.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    let _ = h1.abort();
    let _ = h2b.abort();
    let _ = h3.abort();
}

/// Verifies that membership persists beyond the lease TTL.
/// This catches bugs where keepalive requests aren't being sent.
/// Uses a short TTL (2s) to keep test fast while still validating keepalives.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_persists_beyond_lease_ttl() {
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();
    let prefix = unique_prefix();
    let num_shards: u32 = 8; // Small for fast convergence
    let lease_ttl_secs: i64 = 2; // Short TTL to speed up test

    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = silo::coordination::etcd::EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        lease_ttl_secs,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .expect("start coordinator");

    assert!(
        c1.wait_converged(Duration::from_secs(10)).await,
        "should converge"
    );
    let initial_shards = c1.owned_shards().await;
    assert_eq!(
        initial_shards.len(),
        num_shards as usize,
        "single node should own all shards"
    );

    // Verify membership key exists in etcd
    let mut kv = coord.client().kv_client();
    let members_prefix = silo::coordination::keys::members_prefix(&prefix);
    let resp = kv
        .get(
            members_prefix.clone(),
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .expect("get members");
    assert_eq!(resp.kvs().len(), 1, "should have 1 member initially");

    // Wait for 2.5x the lease TTL - if keepalives aren't working, lease will expire
    let wait_duration = Duration::from_millis((lease_ttl_secs as u64) * 2500);
    tokio::time::sleep(wait_duration).await;

    // Verify membership still exists (would FAIL if keepalives are broken!)
    let resp = kv
        .get(
            members_prefix.clone(),
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .expect("get members after wait");
    assert_eq!(
        resp.kvs().len(),
        1,
        "member should still exist after 2.5x TTL - keepalives must be working"
    );

    // Verify owned shards didn't change (no spurious rebalancing)
    let final_shards = c1.owned_shards().await;
    assert_eq!(
        initial_shards, final_shards,
        "owned shards should be stable over time"
    );

    c1.shutdown().await.unwrap();
    h1.abort();
}

/// Verifies that multiple nodes maintain stable shard ownership over time.
/// This catches race conditions or keepalive issues that cause unexpected rebalancing.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_node_ownership_stable_over_time() {
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();
    let prefix = unique_prefix();
    let num_shards: u32 = 16;
    let lease_ttl_secs: i64 = 2;

    let cfg = silo::settings::AppConfig::load(None).expect("load");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50051",
        num_shards,
        lease_ttl_secs,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .unwrap();

    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50052",
        num_shards,
        lease_ttl_secs,
        make_test_factory(&prefix, "n2"),
    )
    .await
    .unwrap();

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(10)).await);
    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    let initial_s1: HashSet<u32> = c1.owned_shards().await.into_iter().collect();
    let initial_s2: HashSet<u32> = c2.owned_shards().await.into_iter().collect();

    // Verify initial state
    assert!(initial_s1.is_disjoint(&initial_s2), "no overlap initially");
    let all: HashSet<u32> = initial_s1
        .iter()
        .copied()
        .chain(initial_s2.iter().copied())
        .collect();
    let expected: HashSet<u32> = (0..num_shards).collect();
    assert_eq!(all, expected, "all shards covered initially");

    // Sample ownership multiple times over 3x TTL with NO membership changes
    // Each sample should show stable ownership
    for i in 0..3 {
        tokio::time::sleep(Duration::from_secs(lease_ttl_secs as u64)).await;

        let s1: HashSet<u32> = c1.owned_shards().await.into_iter().collect();
        let s2: HashSet<u32> = c2.owned_shards().await.into_iter().collect();

        assert_eq!(
            s1, initial_s1,
            "c1 ownership should be stable at sample {i}"
        );
        assert_eq!(
            s2, initial_s2,
            "c2 ownership should be stable at sample {i}"
        );
    }

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Verifies that get_shard_owner_map reflects actual ownership accurately.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_owner_map_matches_actual_ownership() {
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();
    let prefix = unique_prefix();
    let num_shards: u32 = 8;
    let lease_ttl_secs: i64 = 3;

    let cfg = silo::settings::AppConfig::load(None).expect("load");

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:50061",
        num_shards,
        lease_ttl_secs,
        make_test_factory(&prefix, "n1"),
    )
    .await
    .unwrap();

    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:50062",
        num_shards,
        lease_ttl_secs,
        make_test_factory(&prefix, "n2"),
    )
    .await
    .unwrap();

    assert!(c1.wait_converged(Duration::from_secs(10)).await);
    assert!(c2.wait_converged(Duration::from_secs(10)).await);

    // Get the shard owner map from c1's perspective
    let owner_map = c1.get_shard_owner_map().await.expect("get owner map");

    // Verify all shards have owners
    assert_eq!(
        owner_map.shard_to_addr.len(),
        num_shards as usize,
        "all shards should have owners in map"
    );

    // Verify owner map is consistent with actual ownership
    let s1: HashSet<u32> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<u32> = c2.owned_shards().await.into_iter().collect();

    for shard_id in 0..num_shards {
        let addr = owner_map
            .shard_to_addr
            .get(&shard_id)
            .expect("shard should have addr");

        if s1.contains(&shard_id) {
            assert_eq!(
                addr, "http://127.0.0.1:50061",
                "shard {shard_id} owned by c1 should map to c1's addr"
            );
        } else if s2.contains(&shard_id) {
            assert_eq!(
                addr, "http://127.0.0.1:50062",
                "shard {shard_id} owned by c2 should map to c2's addr"
            );
        } else {
            panic!("shard {shard_id} not owned by any node");
        }
    }

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}
