use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod etcd_test_helpers;

use etcd_test_helpers::EtcdConnection;
use silo::coordination::etcd::{EtcdCoordinator, EtcdShardGuard, ShardPhase};
use silo::coordination::{CoordinationError, Coordinator, ShardSplitter, SplitPhase};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use silo::shard_range::{ShardId, ShardMap};

// Atomic counter for truly unique prefixes even within the same nanosecond
static PREFIX_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = PREFIX_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    format!("test-sg-{}-{}", nanos % 1_000_000_000_000, counter)
}

fn make_test_factory(node_id: &str) -> Arc<ShardFactory> {
    // Use unique directory per test run to avoid SlateDB conflicts with stale state
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = PREFIX_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let tmpdir =
        std::env::temp_dir().join(format!("silo-etcd-test-{}-{}-{}", node_id, nanos, counter));
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            // Use Fs backend for split tests because SlateDB cloning requires a real object store
            backend: Backend::Fs,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        MockGubernatorClient::new_arc(),
        None,
    ))
}

fn get_etcd_endpoints() -> Vec<String> {
    std::env::var("ETCD_ENDPOINTS")
        .map(|s| s.split(',').map(|s| s.to_string()).collect())
        .unwrap_or_else(|_| vec!["http://127.0.0.1:2379".to_string()])
}

/// Start an etcd coordinator, skipping the test if etcd is not available
macro_rules! start_etcd_coordinator {
    ($prefix:expr, $node_id:expr, $grpc_addr:expr, $num_shards:expr) => {{
        let factory = make_test_factory($node_id);
        let endpoints = get_etcd_endpoints();
        EtcdCoordinator::start(
            &endpoints,
            $prefix,
            $node_id,
            $grpc_addr,
            $num_shards,
            10, // lease TTL
            factory,
        )
        .await
        .expect("Failed to connect to etcd - ensure etcd is running")
    }};
}

async fn make_guard(
    coord: &EtcdConnection,
    cluster_prefix: &str,
    shard_id: ShardId,
) -> (
    std::sync::Arc<EtcdShardGuard>,
    std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<ShardId>>>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let mut client = coord.client();
    let liveness_lease_id = client.lease_grant(5, None).await.unwrap().id();
    let owned = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let (tx, rx) = tokio::sync::watch::channel(false);
    let guard = EtcdShardGuard::new(
        shard_id,
        client.clone(),
        cluster_prefix.to_string(),
        liveness_lease_id,
        rx,
    );
    let runner = guard.clone();
    let owned_arc = owned.clone();
    let factory = make_test_factory(cluster_prefix);
    // Create a shard map containing the specific shard_id being tested
    let mut shard_map = ShardMap::new();
    shard_map.add_shard(silo::shard_range::ShardInfo::new(
        shard_id,
        silo::shard_range::ShardRange::full(),
    ));
    let shard_map = Arc::new(tokio::sync::Mutex::new(shard_map));
    // Create a NoneCoordinator as a placeholder for split recovery (these tests don't involve splits)
    let none_coord = silo::coordination::NoneCoordinator::new(
        cluster_prefix,
        "http://127.0.0.1:0",
        1,
        factory.clone(),
    )
    .await;
    let coordinator: Arc<dyn Coordinator> = Arc::new(none_coord);
    let handle = tokio::spawn(async move {
        runner.run(owned_arc, factory, shard_map, coordinator).await;
    });
    (guard, owned, tx, handle)
}

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

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_acquire_and_release() {
    // Requires etcd to be running locally (e.g., `just etcd`).
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (guard, owned, _tx, handle) = make_guard(&coord, &prefix, shard_id).await;

    guard.set_desired(true).await;

    // Wait until Held and owned contains shard
    let ok = wait_until(Duration::from_secs(3), || async {
        let st = guard.state.lock().await;
        let ow = owned.lock().await;
        st.phase == ShardPhase::Held && ow.contains(&shard_id)
    })
    .await;
    assert!(ok, "guard should acquire lock and mark owned");

    // Release
    guard.set_desired(false).await;
    let released = wait_until(Duration::from_secs(3), || async {
        let st = guard.state.lock().await;
        let ow = owned.lock().await;
        st.phase == ShardPhase::Idle && st.held_key.is_none() && !ow.contains(&shard_id)
    })
    .await;
    assert!(released, "guard should release lock and clear owned");

    handle.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_contention_and_handoff() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord1 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #1");
    let coord2 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #2");

    let shard_id = ShardId::new();
    let (g1, owned1, _tx1, h1) = make_guard(&coord1, &prefix, shard_id).await;
    let (g2, owned2, _tx2, h2) = make_guard(&coord2, &prefix, shard_id).await;

    g1.set_desired(true).await;
    g2.set_desired(true).await;

    // Eventually exactly one should hold
    let one_holds = wait_until(Duration::from_secs(4), || async {
        let st1 = g1.state.lock().await;
        let st2 = g2.state.lock().await;
        (st1.phase == ShardPhase::Held) ^ (st2.phase == ShardPhase::Held)
    })
    .await;
    assert!(
        one_holds,
        "exactly one guard should acquire under contention"
    );

    // Determine current owner
    let owner_is_1 = { g1.state.lock().await.phase == ShardPhase::Held };

    if owner_is_1 {
        // Release from g1 and verify handoff to g2
        g1.set_desired(false).await;
        let handed = wait_until(Duration::from_secs(5), || async {
            let st1 = g1.state.lock().await;
            let st2 = g2.state.lock().await;
            let ow1 = owned1.lock().await;
            let ow2 = owned2.lock().await;
            st1.phase != ShardPhase::Held
                && st2.phase == ShardPhase::Held
                && !ow1.contains(&shard_id)
                && ow2.contains(&shard_id)
        })
        .await;
        assert!(handed, "g2 should acquire after g1 releases");
    } else {
        // Release from g2 and verify handoff to g1
        g2.set_desired(false).await;
        let handed = wait_until(Duration::from_secs(5), || async {
            let st1 = g1.state.lock().await;
            let st2 = g2.state.lock().await;
            let ow1 = owned1.lock().await;
            let ow2 = owned2.lock().await;
            st2.phase != ShardPhase::Held
                && st1.phase == ShardPhase::Held
                && !ow2.contains(&shard_id)
                && ow1.contains(&shard_id)
        })
        .await;
        assert!(handed, "g1 should acquire after g2 releases");
    }

    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_idempotent_set_desired_true() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (guard, owned, _tx, handle) = make_guard(&coord, &prefix, shard_id).await;
    guard.set_desired(true).await;

    let acquired = wait_until(Duration::from_secs(3), || async {
        guard.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(acquired, "should acquire initially");
    let key1 = { guard.state.lock().await.held_key.clone() };

    // Calling set_desired(true) again should not cause release/reacquire
    guard.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (phase, key2, in_owned) = {
        let st = guard.state.lock().await;
        let ow = owned.lock().await;
        (st.phase, st.held_key.clone(), ow.contains(&shard_id))
    };
    assert_eq!(phase, ShardPhase::Held);
    assert_eq!(key1, key2);
    assert!(in_owned);

    // Cleanup: release
    guard.set_desired(false).await;
    let released = wait_until(Duration::from_secs(3), || async {
        let st = guard.state.lock().await;
        let ow = owned.lock().await;
        st.phase == ShardPhase::Idle && st.held_key.is_none() && !ow.contains(&shard_id)
    })
    .await;
    assert!(released);
    handle.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_quick_flip_reacquires() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (guard, owned, _tx, handle) = make_guard(&coord, &prefix, shard_id).await;
    guard.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(3), || async {
        guard.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(acquired);

    // Flip to false then quickly back to true before delayed release completes
    guard.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    guard.set_desired(true).await;

    // Expect we end up Held again
    let reacquired = wait_until(Duration::from_secs(5), || async {
        guard.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(reacquired);
    let in_owned = { owned.lock().await.contains(&shard_id) };
    assert!(in_owned, "owned set should include shard after reacquire");

    // Cleanup
    guard.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async {
        guard.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    handle.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_acquire_aborts_when_desired_changes() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");
    let coord2 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #2");

    let shard_id = ShardId::new();
    // First guard holds the lock to block second guard's acquisition
    let (g1, _o1, _tx1, h1) = make_guard(&coord, &prefix, shard_id).await;
    g1.set_desired(true).await;
    let held = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(held);

    // Second guard starts acquiring then we cancel its desire
    let (g2, o2, _tx2, h2) = make_guard(&coord2, &prefix, shard_id).await;
    g2.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    g2.set_desired(false).await;

    // It should not end up holding after some time
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (phase2, owned_contains) = {
        let st = g2.state.lock().await;
        let ow = o2.lock().await;
        (st.phase, ow.contains(&shard_id))
    };
    assert_ne!(phase2, ShardPhase::Held);
    assert!(!owned_contains);

    // Cleanup
    g1.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_acquire_abort_sets_idle() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord1 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #1");
    let coord2 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #2");

    let shard_id = ShardId::new();
    // Block the lock with g1 so g2 enters Acquiring
    let (g1, _o1, _tx1, h1) = make_guard(&coord1, &prefix, shard_id).await;
    g1.set_desired(true).await;
    let held = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(held);

    // Start g2 acquiring then withdraw desire
    let (g2, _o2, _tx2, h2) = make_guard(&coord2, &prefix, shard_id).await;
    g2.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    g2.set_desired(false).await;

    // Expect g2 transitions back to Idle (not stuck in Acquiring)
    let idle = wait_until(Duration::from_secs(2), || async {
        g2.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    assert!(idle, "g2 should return to Idle after aborting acquire");

    // Cleanup
    g1.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_release_cancelled_on_desired_true() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (g, owned, _tx, h) = make_guard(&coord, &prefix, shard_id).await;
    g.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(3), || async {
        g.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(acquired);
    let key_before = { g.state.lock().await.held_key.clone() };
    assert!(key_before.is_some());

    // Begin release then flip desired back to true during the delay window
    g.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(50)).await; // < 100ms delay
    g.set_desired(true).await;

    // Expect we remain Held and the held_key is unchanged (no unlock/reacquire)
    let still_held = wait_until(Duration::from_secs(3), || async {
        g.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(still_held);
    let key_after = { g.state.lock().await.held_key.clone() };
    assert_eq!(
        key_before, key_after,
        "held key should be preserved if release is cancelled"
    );
    assert!(owned.lock().await.contains(&shard_id));

    // Cleanup
    g.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async {
        g.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    h.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_shutdown_in_idle() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (guard, owned, tx, handle) = make_guard(&coord, &prefix, shard_id).await;
    // Ensure Idle
    {
        let st = guard.state.lock().await;
        assert_eq!(st.phase, ShardPhase::Idle);
        assert!(st.held_key.is_none());
    }
    // Signal shutdown
    let _ = tx.send(true);
    // Give some time for loop to observe and exit
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Task should finish quickly after shutdown
    handle.abort();
    // No ownership
    assert!(!owned.lock().await.contains(&shard_id));
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_shutdown_while_acquiring() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord1 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #1");
    let coord2 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #2");

    let shard_id = ShardId::new();
    // g1 holds the lock so g2 is acquiring
    let (g1, _o1, _tx1, h1) = make_guard(&coord1, &prefix, shard_id).await;
    g1.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(3), || async {
            g1.state.lock().await.phase == ShardPhase::Held
        })
        .await
    );

    let (g2, o2, tx2, h2) = make_guard(&coord2, &prefix, shard_id).await;
    g2.set_desired(true).await;
    // Wait until g2 enters Acquiring
    assert!(
        wait_until(Duration::from_secs(2), || async {
            let st = g2.state.lock().await;
            st.phase == ShardPhase::Acquiring
        })
        .await
    );

    // Shutdown g2 while acquiring
    let _ = tx2.send(true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // It should not end up holding
    let st2 = g2.state.lock().await;
    assert_ne!(st2.phase, ShardPhase::Held);
    assert!(!o2.lock().await.contains(&shard_id));

    // Cleanup
    g1.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_shutdown_while_held() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (g, owned, tx, h) = make_guard(&coord, &prefix, shard_id).await;
    g.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(3), || async {
            g.state.lock().await.phase == ShardPhase::Held
        })
        .await
    );
    assert!(owned.lock().await.contains(&shard_id));

    // Shutdown should release and clear owned
    let _ = tx.send(true);
    // Wait for shutdown to complete
    assert!(
        wait_until(Duration::from_secs(5), || async {
            g.state.lock().await.phase == ShardPhase::ShutDown
        })
        .await,
        "should reach ShutDown phase"
    );
    let st = g.state.lock().await;
    assert_eq!(st.held_key, None);
    assert!(!owned.lock().await.contains(&shard_id));
    h.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_guard_shutdown_while_releasing() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let (g, owned, tx, h) = make_guard(&coord, &prefix, shard_id).await;
    g.set_desired(true).await;
    assert!(
        wait_until(Duration::from_secs(3), || async {
            g.state.lock().await.phase == ShardPhase::Held
        })
        .await
    );
    // Begin release
    g.set_desired(false).await;
    assert!(
        wait_until(Duration::from_secs(2), || async {
            g.state.lock().await.phase == ShardPhase::Releasing
        })
        .await
    );
    // Now shutdown during releasing delay
    let _ = tx.send(true);
    // Wait for shutdown to complete
    assert!(
        wait_until(Duration::from_secs(5), || async {
            g.state.lock().await.phase == ShardPhase::ShutDown
        })
        .await,
        "should reach ShutDown phase"
    );
    let st = g.state.lock().await;
    assert_eq!(st.held_key, None);
    assert!(!owned.lock().await.contains(&shard_id));
    h.abort();
}

// =============================================================================
// Coordinator-level tests (full EtcdCoordinator tests)
// =============================================================================

/// Test that a single etcd coordinator owns all shards when alone.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_single_node_owns_all_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "test-node-1", "http://127.0.0.1:50051", num_shards);

    // Wait for convergence
    assert!(
        coord.wait_converged(Duration::from_secs(15)).await,
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

/// Test that multiple etcd coordinators partition shards correctly.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_multiple_nodes_partition_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 16;

    let (c1, h1) =
        start_etcd_coordinator!(&prefix, "test-node-1", "http://127.0.0.1:50051", num_shards);

    // Small delay to let c1 start acquiring shards
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
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
async fn etcd_rebalances_on_membership_change() {
    let prefix = unique_prefix();
    let num_shards: u32 = 12;

    // Start with one node
    let (c1, h1) =
        start_etcd_coordinator!(&prefix, "test-node-1", "http://127.0.0.1:50051", num_shards);

    assert!(c1.wait_converged(Duration::from_secs(15)).await);
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
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
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
    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);

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

/// Test three nodes with larger shard count - checks all shards are owned
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_three_nodes_even_distribution() {
    let prefix = unique_prefix();
    // Use 12 shards (divisible by 3) - enough to test distribution without being slow
    let num_shards: u32 = 12;

    let (c1, h1) = start_etcd_coordinator!(&prefix, "node-1", "http://127.0.0.1:50051", num_shards);

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("node-2"),
    )
    .await
    .expect("start c2");
    let (c3, h3) = EtcdCoordinator::start(
        &endpoints,
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

    // Wait for all shards to be covered by the three coordinators
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();

    // Wait for all shards to be covered with disjoint ownership
    let all_shards_covered = wait_until(Duration::from_secs(30), || async {
        let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
        let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
        let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();
        let all: HashSet<ShardId> = s1
            .iter()
            .chain(s2.iter())
            .chain(s3.iter())
            .copied()
            .collect();
        all == expected && s1.is_disjoint(&s2) && s1.is_disjoint(&s3) && s2.is_disjoint(&s3)
    })
    .await;
    assert!(
        all_shards_covered,
        "all shards should be covered with disjoint ownership"
    );

    // Final verification
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
    assert_eq!(all, expected, "all shards should be owned exactly once");

    // Disjoint ownership
    assert!(s1.is_disjoint(&s2), "s1 and s2 should be disjoint");
    assert!(s1.is_disjoint(&s3), "s1 and s3 should be disjoint");
    assert!(s2.is_disjoint(&s3), "s2 and s3 should be disjoint");

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
async fn etcd_removing_node_rebalances() {
    let prefix = unique_prefix();
    // Use 12 shards - enough to test rebalancing without being slow
    let num_shards: u32 = 12;

    let (c1, h1) = start_etcd_coordinator!(&prefix, "node-1", "http://127.0.0.1:50051", num_shards);

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("node-2"),
    )
    .await
    .expect("start c2");
    let (c3, h3) = EtcdCoordinator::start(
        &endpoints,
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
        c1.wait_converged(Duration::from_secs(20)).await,
        "c1 should reconverge after node removal"
    );
    assert!(
        c2.wait_converged(Duration::from_secs(20)).await,
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

/// Test that owned shards list remains stable during steady state
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_ownership_stability_during_steady_state() {
    let prefix = unique_prefix();
    let num_shards: u32 = 16;

    let (c1, h1) =
        start_etcd_coordinator!(&prefix, "stable-1", "http://127.0.0.1:50051", num_shards);

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
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

/// Test that there is no split-brain during transitions (no overlapping ownership).
/// This is a critical safety test.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_no_split_brain_during_transitions() {
    let prefix = unique_prefix();
    let num_shards: u32 = 32;

    let (c1, h1) =
        start_etcd_coordinator!(&prefix, "brain-1", "http://127.0.0.1:50051", num_shards);

    // Wait for c1 to own all shards
    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "brain-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("brain-2"),
    )
    .await
    .expect("c2");

    // Monitor for split-brain during the rebalancing period
    let mut violations = 0;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
        let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();

        // Check for overlapping ownership (split-brain)
        let overlap: HashSet<ShardId> = s1.intersection(&s2).copied().collect();
        if !overlap.is_empty() {
            violations += 1;
            eprintln!(
                "SPLIT-BRAIN DETECTED at {:?}: {} shards owned by both nodes: {:?}",
                start.elapsed(),
                overlap.len(),
                overlap
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Stop if both have converged
        if c1.wait_converged(Duration::from_millis(0)).await
            && c2.wait_converged(Duration::from_millis(0)).await
        {
            // Verify convergence holds for a moment
            tokio::time::sleep(Duration::from_millis(200)).await;
            break;
        }
    }

    assert_eq!(
        violations, 0,
        "no split-brain violations should occur during rebalancing"
    );

    // Final verification
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert!(s1.is_disjoint(&s2), "final ownership must be disjoint");

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test that a clean shutdown properly releases shards for other nodes to acquire
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_graceful_shutdown_releases_shards_promptly() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;
    let short_ttl: i64 = 5; // 5 seconds for faster test

    // Start two nodes with short TTL
    let endpoints = get_etcd_endpoints();
    let (c1, h1) = match EtcdCoordinator::start(
        &endpoints,
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
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
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

    // Graceful shutdown of c2 (releases shards and deletes membership)
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

// =============================================================================
// Shard Split Tests
// =============================================================================

/// Test that request_split creates a split record in etcd.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_request_split_creates_split_record() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1; // Use 1 shard so it covers the entire keyspace

    let (coord, handle) = start_etcd_coordinator!(
        &prefix,
        "split-test-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(
        coord.wait_converged(Duration::from_secs(15)).await,
        "coordinator should converge"
    );

    // Get the first shard we own
    let owned = coord.owned_shards().await;
    assert!(!owned.is_empty(), "should own at least one shard");
    let shard_id = owned[0];

    // Use a simple split point - with 1 shard the range is unbounded so "m" is valid
    let split_point = "m".to_string();

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // Request a split
    let split = splitter
        .request_split(shard_id, split_point.clone())
        .await
        .expect("request_split should succeed");

    assert_eq!(split.parent_shard_id, shard_id);
    assert_eq!(split.split_point, split_point);
    assert_eq!(split.phase, SplitPhase::SplitRequested);

    // Verify we can retrieve the split status
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get_split_status should succeed");
    assert!(status.is_some(), "split status should exist");
    let status = status.unwrap();
    assert_eq!(status.parent_shard_id, shard_id);
    assert_eq!(status.split_point, split_point);
    assert_eq!(status.left_child_id, split.left_child_id);
    assert_eq!(status.right_child_id, split.right_child_id);

    // Cleanup
    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that request_split fails if not the shard owner.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_request_split_fails_if_not_owner() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;

    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "split-owner-1",
        "http://127.0.0.1:50051",
        num_shards
    );

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "split-owner-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("split-owner-2"),
    )
    .await
    .expect("start c2");
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);

    // Find a shard owned by c1
    let c1_owned: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let c2_owned: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert!(
        c1_owned.is_disjoint(&c2_owned),
        "ownership should be disjoint"
    );

    let c1_shard = *c1_owned
        .iter()
        .next()
        .expect("c1 should own at least one shard");

    // Create splitter for c2 and try to split a shard owned by c1
    let splitter2 = ShardSplitter::new(c2.clone());

    let result = splitter2.request_split(c1_shard, "mmmmm".to_string()).await;
    assert!(
        matches!(result, Err(CoordinationError::NotShardOwner(_))),
        "should fail with NotShardOwner, got: {:?}",
        result
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

/// Test that request_split fails if a split is already in progress.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_request_split_fails_if_already_in_progress() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1; // Use 1 shard so it covers the entire keyspace

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "split-dup-1", "http://127.0.0.1:50051", num_shards);
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // First split request should succeed
    let _split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("first request_split should succeed");

    // Second split request should fail
    let result = splitter.request_split(shard_id, "n".to_string()).await;
    assert!(
        matches!(result, Err(CoordinationError::SplitAlreadyInProgress(_))),
        "should fail with SplitAlreadyInProgress, got: {:?}",
        result
    );

    // Cleanup
    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that is_shard_paused returns correct values based on split phase.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_is_shard_paused_returns_correct_values() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1; // Use 1 shard so it covers the entire keyspace

    let (coord, handle) = start_etcd_coordinator!(
        &prefix,
        "paused-test-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // Initially not paused
    let paused = splitter.is_shard_paused(shard_id).await;
    assert!(!paused, "shard should not be paused initially");

    // Request a split - still not paused in SplitRequested phase
    let _split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split should succeed");

    // In SplitRequested phase, traffic is NOT paused yet
    let paused = splitter.is_shard_paused(shard_id).await;
    assert!(
        !paused,
        "shard should not be paused in SplitRequested phase"
    );

    // Cleanup
    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that split state persists across coordinator restart.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_split_state_persists_across_restart() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1; // Use 1 shard so it covers the entire keyspace

    // Start first coordinator
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "persist-test-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let c1: Arc<dyn Coordinator> = Arc::new(c1);

    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    let owned = c1.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter and request a split
    let splitter1 = ShardSplitter::new(c1.clone());

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split should succeed");

    // Shutdown the coordinator
    c1.shutdown().await.unwrap();
    h1.abort();

    // Small delay
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start new coordinator with same prefix (simulates restart)
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "persist-test-1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory("persist-test-1-restart"),
    )
    .await
    .expect("restart coordinator");
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Create splitter for c2 and verify the split state persisted
    let splitter2 = ShardSplitter::new(c2.clone());

    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get_split_status should succeed");
    assert!(
        status.is_some(),
        "split status should persist after restart"
    );
    let status = status.unwrap();
    assert_eq!(status.parent_shard_id, shard_id);
    assert_eq!(status.split_point, "m");
    assert_eq!(status.left_child_id, split.left_child_id);
    assert_eq!(status.right_child_id, split.right_child_id);

    // Cleanup
    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test that execute_split completes a full split cycle in etcd.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_execute_split_completes_full_cycle() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    let (coord, handle) = start_etcd_coordinator!(
        &prefix,
        "split-exec-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // Request and execute the split
    let split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split should succeed");

    splitter
        .execute_split(shard_id, || coord.get_shard_owner_map())
        .await
        .expect("execute_split should succeed");

    // Verify split is complete
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get_split_status");
    assert!(
        status.is_none(),
        "split record should be cleaned up after completion"
    );

    // Verify shard map has been updated
    let shard_map = coord.get_shard_map().await.expect("get_shard_map");
    assert_eq!(shard_map.len(), 2, "should have 2 shards after split");

    assert!(
        shard_map.get_shard(&shard_id).is_none(),
        "parent should be removed"
    );
    assert!(
        shard_map.get_shard(&split.left_child_id).is_some(),
        "left child should exist"
    );
    assert!(
        shard_map.get_shard(&split.right_child_id).is_some(),
        "right child should exist"
    );

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test execute_split fails without a prior request.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_execute_split_fails_without_request() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    let (coord, handle) = start_etcd_coordinator!(
        &prefix,
        "split-no-req-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // Try to execute without requesting first
    let result = splitter
        .execute_split(shard_id, || coord.get_shard_owner_map())
        .await;
    assert!(result.is_err(), "execute_split should fail without request");

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that execute_split resumes from partial state.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_execute_split_resumes_from_partial_state() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    let (coord, handle) = start_etcd_coordinator!(
        &prefix,
        "split-resume-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // Request and advance to SplitPausing
    let split = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split");
    splitter
        .advance_split_phase(shard_id)
        .await
        .expect("advance to pausing");

    // Verify partial state
    let status = splitter.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitPausing);

    // Execute should resume and complete
    splitter
        .execute_split(shard_id, || coord.get_shard_owner_map())
        .await
        .expect("execute_split from partial");

    // Verify completion
    let status = splitter
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(status.is_none(), "split should be complete");

    let shard_map = coord.get_shard_map().await.unwrap();
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test sequential splits work correctly.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_sequential_splits_work_correctly() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "split-seq-1", "http://127.0.0.1:50051", num_shards);
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter
    let splitter = ShardSplitter::new(coord.clone());

    // First split at "m"
    let split1 = splitter
        .request_split(shard_id, "m".to_string())
        .await
        .expect("first request_split");
    splitter
        .execute_split(shard_id, || coord.get_shard_owner_map())
        .await
        .expect("first execute_split");

    let shard_map = coord.get_shard_map().await.unwrap();
    assert_eq!(shard_map.len(), 2);

    // Second split: split left child at "g"
    let left_child_id = split1.left_child_id;
    let split2 = splitter
        .request_split(left_child_id, "g".to_string())
        .await
        .expect("second request_split");
    splitter
        .execute_split(left_child_id, || coord.get_shard_owner_map())
        .await
        .expect("second execute_split");

    let shard_map = coord.get_shard_map().await.unwrap();
    assert_eq!(shard_map.len(), 3);

    // Verify tenant routing
    let tenant_a = shard_map.shard_for_tenant("a").unwrap();
    let tenant_h = shard_map.shard_for_tenant("h").unwrap();
    let tenant_z = shard_map.shard_for_tenant("z").unwrap();

    assert_eq!(tenant_a.id, split2.left_child_id);
    assert_eq!(tenant_h.id, split2.right_child_id);
    assert_eq!(tenant_z.id, split1.right_child_id);

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test split works in multi-node etcd cluster.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_split_in_multi_node_cluster() {
    let prefix = unique_prefix();
    // Use 4 shards to ensure both nodes get some (rendezvous hashing)
    let num_shards: u32 = 4;

    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "split-multi-1",
        "http://127.0.0.1:50051",
        num_shards
    );

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "split-multi-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("split-multi-2"),
    )
    .await
    .expect("start c2");

    assert!(c1.wait_converged(Duration::from_secs(20)).await);
    assert!(c2.wait_converged(Duration::from_secs(20)).await);

    // Wait for all shards to be acquired (doesn't matter which node owns them)
    let all_shards_acquired = wait_until(Duration::from_secs(10), || async {
        let total = c1.owned_shards().await.len() + c2.owned_shards().await.len();
        total == num_shards as usize
    })
    .await;
    assert!(all_shards_acquired, "all shards should be owned");

    // Wrap coordinators in Arc for splitter
    let c1: Arc<dyn Coordinator> = Arc::new(c1);
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    // Pick whichever coordinator has shards (rendezvous hashing might give all to one)
    let c1_shards = c1.owned_shards().await;
    let (splitter_coord, shard_to_split) = if !c1_shards.is_empty() {
        (c1.clone(), c1_shards[0])
    } else {
        let c2_shards = c2.owned_shards().await;
        assert!(!c2_shards.is_empty(), "at least one node must own shards");
        (c2.clone(), c2_shards[0])
    };

    // Get the shard's range and compute a valid split point
    let shard_map = splitter_coord.get_shard_map().await.unwrap();
    let shard_info = shard_map
        .get_shard(&shard_to_split)
        .expect("find shard in map");
    let split_point = shard_info
        .range
        .midpoint()
        .expect("shard range should have a midpoint");

    // Create splitter and split the shard
    let splitter = ShardSplitter::new(splitter_coord.clone());

    let split = splitter
        .request_split(shard_to_split, split_point)
        .await
        .expect("request_split");
    splitter
        .execute_split(shard_to_split, || splitter_coord.get_shard_owner_map())
        .await
        .expect("execute_split");

    // Verify the splitting coordinator sees the updated shard map immediately
    // After splitting one shard, we have num_shards + 1 total shards
    let expected_shard_count = (num_shards + 1) as usize;
    let map1 = splitter_coord.get_shard_map().await.unwrap();
    assert_eq!(
        map1.len(),
        expected_shard_count,
        "splitter should see {} shards after split",
        expected_shard_count
    );
    assert!(map1.get_shard(&split.left_child_id).is_some());
    assert!(map1.get_shard(&split.right_child_id).is_some());

    // etcd has a watch on the shard map, so both coordinators should see the changes
    let both_see_split = wait_until(Duration::from_secs(5), || async {
        let map1 = c1.get_shard_map().await.unwrap();
        let map2 = c2.get_shard_map().await.unwrap();
        map1.len() == expected_shard_count && map2.len() == expected_shard_count
    })
    .await;
    assert!(
        both_see_split,
        "both coordinators should see updated shard map via etcd watch"
    );

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    h1.abort();
    h2.abort();
}

// =============================================================================
// Crash Recovery Tests
// =============================================================================

/// Test crash recovery during early phase abandons the split.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_crash_recovery_early_phase_abandons_split() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    // Start first coordinator
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "crash-early-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let c1: Arc<dyn Coordinator> = Arc::new(c1);

    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    let owned = c1.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter and request split, advance to SplitPausing (early phase)
    let splitter1 = ShardSplitter::new(c1.clone());

    let _split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split");
    splitter1
        .advance_split_phase(shard_id)
        .await
        .expect("advance to pausing");

    // Simulate crash
    c1.shutdown().await.unwrap();
    h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "crash-early-1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory("crash-early-1-restart"),
    )
    .await
    .expect("restart coordinator");
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Create splitter for c2
    let splitter2 = ShardSplitter::new(c2.clone());

    // Early phase crash should abandon the split
    splitter2
        .recover_stale_splits()
        .await
        .expect("recover_stale_splits");

    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(
        status.is_none(),
        "early phase split should be abandoned after crash"
    );

    // Shard map should still have original shard
    let shard_map = c2.get_shard_map().await.unwrap();
    assert_eq!(shard_map.len(), 1);
    assert!(shard_map.get_shard(&shard_id).is_some());

    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test crash recovery during late phase resumes the split.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_crash_recovery_late_phase_resumes_split() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    // Start first coordinator
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "crash-late-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    let c1: Arc<dyn Coordinator> = Arc::new(c1);

    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    let owned = c1.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter and request split, advance to SplitUpdatingMap (late phase)
    let splitter1 = ShardSplitter::new(c1.clone());

    let split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split");

    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitPausing
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitCloning
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitUpdatingMap

    let status = splitter1.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitUpdatingMap);

    // Simulate crash
    c1.shutdown().await.unwrap();
    h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "crash-late-1",
        "http://127.0.0.1:50051",
        num_shards,
        10,
        make_test_factory("crash-late-1-restart"),
    )
    .await
    .expect("restart coordinator");
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Create splitter for c2
    let splitter2 = ShardSplitter::new(c2.clone());

    // Late phase split should be preserved
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

    let shard_map = c2.get_shard_map().await.unwrap();
    assert_eq!(shard_map.len(), 2);
    assert!(shard_map.get_shard(&split.left_child_id).is_some());
    assert!(shard_map.get_shard(&split.right_child_id).is_some());

    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test that the gRPC RequestSplit endpoint actually executes the split to completion.
/// This test verifies the fix for the bug where request_split only stored the split
/// record but didn't call execute_split to drive it through all phases.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_grpc_request_split_executes_to_completion() {
    use silo::pb::silo_client::SiloClient;
    use silo::server::run_server;
    use silo::settings::AppConfig;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    // Start etcd coordinator
    let (coord, handle) = start_etcd_coordinator!(
        &prefix,
        "grpc-split-1",
        "http://127.0.0.1:50099",
        num_shards
    );
    let coord = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    // Get the shard to split
    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Start a gRPC server using the etcd coordinator
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("get addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let factory = coord.base().factory.clone();
    let cfg = AppConfig::load(None).unwrap();

    let server_coord = coord.clone();
    let server = tokio::spawn(async move {
        run_server(listener, factory, server_coord, cfg, None, shutdown_rx).await
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create gRPC client
    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = SiloClient::new(channel);

    // Make gRPC request to split the shard
    let response = client
        .request_split(silo::pb::RequestSplitRequest {
            shard_id: shard_id.to_string(),
            split_point: "m".to_string(),
        })
        .await
        .expect("request_split gRPC call should succeed");

    let split_response = response.into_inner();
    assert_eq!(
        split_response.phase, "SplitRequested",
        "initial phase should be SplitRequested"
    );

    // Wait for the split to complete (background task should execute it)
    let start = Instant::now();
    let timeout = Duration::from_secs(30);
    loop {
        if start.elapsed() > timeout {
            panic!("Split did not complete within timeout");
        }

        let status = client
            .get_split_status(silo::pb::GetSplitStatusRequest {
                shard_id: shard_id.to_string(),
            })
            .await
            .expect("get_split_status")
            .into_inner();

        if !status.in_progress {
            // Split completed - record was cleaned up
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify the shard map was updated
    let shard_map = coord.get_shard_map().await.expect("get_shard_map");
    assert_eq!(shard_map.len(), 2, "should have 2 shards after split");

    // Parent should be removed
    assert!(
        shard_map.get_shard(&shard_id).is_none(),
        "parent shard should be removed from shard map"
    );

    // Children should exist
    let left_child_id = silo::shard_range::ShardId::parse(&split_response.left_child_id)
        .expect("parse left child id");
    let right_child_id = silo::shard_range::ShardId::parse(&split_response.right_child_id)
        .expect("parse right child id");
    assert!(
        shard_map.get_shard(&left_child_id).is_some(),
        "left child should exist"
    );
    assert!(
        shard_map.get_shard(&right_child_id).is_some(),
        "right child should exist"
    );

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server.await;
    coord.shutdown().await.unwrap();
    handle.abort();
}
