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
            slatedb: None,
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
            Vec::new(),
        )
        .await
        .expect("Failed to connect to etcd - ensure etcd is running")
    }};
}

fn next_node_id() -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "test-node-{}",
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    )
}

async fn make_guard_with_node_id(
    coord: &EtcdConnection,
    cluster_prefix: &str,
    shard_id: ShardId,
    node_id: String,
) -> (
    std::sync::Arc<EtcdShardGuard>,
    std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<ShardId>>>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let client = coord.client();
    let owned = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let (tx, rx) = tokio::sync::watch::channel(false);
    let guard = EtcdShardGuard::new(
        shard_id,
        client.clone(),
        cluster_prefix.to_string(),
        node_id,
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
        Vec::new(),
    )
    .await
    .unwrap();
    let coordinator: Arc<dyn Coordinator> = Arc::new(none_coord);
    let handle = tokio::spawn(async move {
        runner.run(owned_arc, factory, shard_map, coordinator).await;
    });
    (guard, owned, tx, handle)
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
    make_guard_with_node_id(coord, cluster_prefix, shard_id, next_node_id()).await
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
        st.phase == ShardPhase::Idle && !st.is_held && !ow.contains(&shard_id)
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

    // Calling set_desired(true) again should not cause release/reacquire
    guard.set_desired(true).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (phase, is_held, in_owned) = {
        let st = guard.state.lock().await;
        let ow = owned.lock().await;
        (st.phase, st.is_held, ow.contains(&shard_id))
    };
    assert_eq!(phase, ShardPhase::Held);
    assert!(is_held);
    assert!(in_owned);

    // Cleanup: release
    guard.set_desired(false).await;
    let released = wait_until(Duration::from_secs(3), || async {
        let st = guard.state.lock().await;
        let ow = owned.lock().await;
        st.phase == ShardPhase::Idle && !st.is_held && !ow.contains(&shard_id)
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
    assert!(g.state.lock().await.is_held);

    // Begin release then flip desired back to true during the delay window
    g.set_desired(false).await;
    tokio::time::sleep(Duration::from_millis(50)).await; // < 100ms delay
    g.set_desired(true).await;

    // Expect we remain Held and ownership is unchanged (no release/reacquire)
    let still_held = wait_until(Duration::from_secs(3), || async {
        g.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(still_held);
    assert!(
        g.state.lock().await.is_held,
        "ownership should be preserved if release is cancelled"
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
        assert!(!st.is_held);
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
    assert_eq!(st.is_held, false);
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
    assert_eq!(st.is_held, false);
    assert!(!owned.lock().await.contains(&shard_id));
    h.abort();
}

/// Test that a single etcd coordinator owns all shards when alone.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_single_node_owns_all_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "test-node-1", "http://127.0.0.1:7450", num_shards);

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
        start_etcd_coordinator!(&prefix, "test-node-1", "http://127.0.0.1:7450", num_shards);

    // Small delay to let c1 start acquiring shards
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "test-node-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("test-node-2"),
        Vec::new(),
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
        start_etcd_coordinator!(&prefix, "test-node-1", "http://127.0.0.1:7450", num_shards);

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
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("test-node-2"),
        Vec::new(),
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

    let (c1, h1) = start_etcd_coordinator!(&prefix, "node-1", "http://127.0.0.1:7450", num_shards);

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("node-2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c3, h3) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-3",
        "http://127.0.0.1:7452",
        num_shards,
        10,
        make_test_factory("node-3"),
        Vec::new(),
    )
    .await
    .expect("start c3");

    // Wait for convergence (longer timeout for 3 nodes)
    let timeout = Duration::from_secs(45);
    assert!(c1.wait_converged(timeout).await);
    assert!(c2.wait_converged(timeout).await);
    assert!(c3.wait_converged(timeout).await);

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

    let (c1, h1) = start_etcd_coordinator!(&prefix, "node-1", "http://127.0.0.1:7450", num_shards);

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("node-2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c3, h3) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-3",
        "http://127.0.0.1:7452",
        num_shards,
        10,
        make_test_factory("node-3"),
        Vec::new(),
    )
    .await
    .expect("start c3");

    // Wait for initial convergence with 3 nodes (longer timeout)
    let timeout = Duration::from_secs(45);
    assert!(c1.wait_converged(timeout).await);
    assert!(c2.wait_converged(timeout).await);
    assert!(c3.wait_converged(timeout).await);

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
        start_etcd_coordinator!(&prefix, "stable-1", "http://127.0.0.1:7450", num_shards);

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "stable-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("stable-2"),
        Vec::new(),
    )
    .await
    .expect("c2");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);

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

    let (c1, h1) = start_etcd_coordinator!(&prefix, "brain-1", "http://127.0.0.1:7450", num_shards);

    // Wait for c1 to own all shards
    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "brain-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("brain-2"),
        Vec::new(),
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
        "http://127.0.0.1:7450",
        num_shards,
        short_ttl,
        make_test_factory("graceful-1"),
        Vec::new(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping graceful shutdown test: {}", e);
            return;
        }
    };

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "graceful-2",
        "http://127.0.0.1:7451",
        num_shards,
        short_ttl,
        make_test_factory("graceful-2"),
        Vec::new(),
    )
    .await
    .expect("c2");

    // Wait for convergence
    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);

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

/// Test that request_split creates a split record in etcd.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_request_split_creates_split_record() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1; // Use 1 shard so it covers the entire keyspace

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "split-test-1", "http://127.0.0.1:7450", num_shards);
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
        "http://127.0.0.1:7450",
        num_shards
    );

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "split-owner-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("split-owner-2"),
        Vec::new(),
    )
    .await
    .expect("start c2");
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);

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
        start_etcd_coordinator!(&prefix, "split-dup-1", "http://127.0.0.1:7450", num_shards);
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
        "http://127.0.0.1:7450",
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
        "http://127.0.0.1:7450",
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
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory("persist-test-1-restart"),
        Vec::new(),
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

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "split-exec-1", "http://127.0.0.1:7450", num_shards);
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
        "http://127.0.0.1:7450",
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
        "http://127.0.0.1:7450",
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
        start_etcd_coordinator!(&prefix, "split-seq-1", "http://127.0.0.1:7450", num_shards);
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
        "http://127.0.0.1:7450",
        num_shards
    );

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "split-multi-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("split-multi-2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    assert!(c1.wait_converged(Duration::from_secs(30)).await);
    assert!(c2.wait_converged(Duration::from_secs(30)).await);

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

/// Test crash recovery during early phase abandons the split.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_crash_recovery_early_phase_abandons_split() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    // Start first coordinator
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "crash-early-1",
        "http://127.0.0.1:7450",
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
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory("crash-early-1-restart"),
        Vec::new(),
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

/// Test crash recovery during cloning phase abandons the split.
///
/// With the simplified split model, ALL incomplete splits are abandoned on crash.
/// The shard map update is the commit point - if children don't exist in the
/// shard map, the split never completed and is safely abandoned.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_crash_recovery_cloning_phase_abandons_split() {
    let prefix = unique_prefix();
    let num_shards: u32 = 1;

    // Start first coordinator
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "crash-cloning-1",
        "http://127.0.0.1:7450",
        num_shards
    );
    let c1: Arc<dyn Coordinator> = Arc::new(c1);

    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    let owned = c1.owned_shards().await;
    let shard_id = owned[0];

    // Create splitter and request split, advance to SplitCloning phase
    let splitter1 = ShardSplitter::new(c1.clone());

    let _split = splitter1
        .request_split(shard_id, "m".to_string())
        .await
        .expect("request_split");

    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitPausing
    splitter1.advance_split_phase(shard_id).await.unwrap(); // -> SplitCloning

    let status = splitter1.get_split_status(shard_id).await.unwrap().unwrap();
    assert_eq!(status.phase, SplitPhase::SplitCloning);

    // Simulate crash
    c1.shutdown().await.unwrap();
    h1.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start new coordinator
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "crash-cloning-1",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        make_test_factory("crash-cloning-1-restart"),
        Vec::new(),
    )
    .await
    .expect("restart coordinator");
    let c2: Arc<dyn Coordinator> = Arc::new(c2);

    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Create splitter for c2
    let splitter2 = ShardSplitter::new(c2.clone());

    // Recover stale splits - should abandon the incomplete split
    splitter2
        .recover_stale_splits()
        .await
        .expect("recover stale splits");

    // Split should be abandoned (deleted)
    let status = splitter2
        .get_split_status(shard_id)
        .await
        .expect("get status");
    assert!(status.is_none(), "incomplete split should be abandoned");

    // Shard map should still have original shard (split was not committed)
    let shard_map = c2.get_shard_map().await.unwrap();
    assert_eq!(shard_map.len(), 1, "shard map should have original shard");
    assert!(
        shard_map.get_shard(&shard_id).is_some(),
        "original shard should still exist"
    );

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
    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "grpc-split-1", "http://127.0.0.1:7499", num_shards);
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

/// Test that a single node with default ring owns all default shards.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_single_node_default_ring_owns_all_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Node with empty placement_rings participates in default ring
    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "default-node", "http://127.0.0.1:7450", num_shards);

    assert!(
        coord.wait_converged(Duration::from_secs(10)).await,
        "coordinator should converge"
    );

    // All shards default to no ring, so default node should own all
    let owned = coord.owned_shards().await;
    assert_eq!(
        owned.len(),
        num_shards as usize,
        "default node should own all shards"
    );

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that nodes with different rings get different shards based on ring assignment.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_multi_ring_shard_assignment() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;

    // Start a default node (empty placement_rings = default ring)
    let (c_default, h_default) =
        start_etcd_coordinator!(&prefix, "default-node", "http://127.0.0.1:7450", num_shards);
    let c_default: Arc<dyn Coordinator> = Arc::new(c_default);

    assert!(
        c_default.wait_converged(Duration::from_secs(15)).await,
        "default node should converge"
    );

    // All shards start in default ring, so default node owns all
    let owned_default_before = c_default.owned_shards().await;
    assert_eq!(
        owned_default_before.len(),
        num_shards as usize,
        "default node should own all shards initially"
    );

    // Start a GPU node that only participates in the "gpu" ring
    let endpoints = get_etcd_endpoints();
    let (c_gpu, h_gpu) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "gpu-node",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("gpu-node"),
        vec!["gpu".to_string()],
    )
    .await
    .expect("start gpu node");
    let c_gpu: Arc<dyn Coordinator> = Arc::new(c_gpu);

    // Wait for both to converge
    assert!(
        c_default.wait_converged(Duration::from_secs(15)).await,
        "default node should converge"
    );
    assert!(
        c_gpu.wait_converged(Duration::from_secs(15)).await,
        "gpu node should converge"
    );

    // GPU node should own no shards (all shards are in default ring, not gpu ring)
    let owned_gpu = c_gpu.owned_shards().await;
    assert_eq!(
        owned_gpu.len(),
        0,
        "gpu node should own no shards (no shards in gpu ring)"
    );

    // Default node should still own all shards
    let owned_default_after = c_default.owned_shards().await;
    assert_eq!(
        owned_default_after.len(),
        num_shards as usize,
        "default node should still own all shards"
    );

    // Now move one shard to the gpu ring
    let shard_to_move = owned_default_after[0];
    let (prev, curr) = c_default
        .update_shard_placement_ring(&shard_to_move, Some("gpu"))
        .await
        .expect("update placement ring");
    assert!(prev.is_none(), "previous ring should be None (default)");
    assert_eq!(curr, Some("gpu".to_string()), "current ring should be gpu");

    // Wait for GPU node to acquire the shard - this involves:
    // 1. shard_map update propagating to gpu-node
    // 2. gpu-node reconciling and seeing shard_to_move as desired
    // 3. default-node releasing the shard lease
    // 4. gpu-node acquiring the shard lease
    let shard_to_move_clone = shard_to_move;
    let c_gpu_clone = c_gpu.clone();
    let acquired = wait_until(Duration::from_secs(30), || {
        let c = c_gpu_clone.clone();
        let s = shard_to_move_clone;
        async move { c.owned_shards().await.contains(&s) }
    })
    .await;
    assert!(acquired, "gpu node should acquire the moved shard");

    // Wait for default node to release the shard
    let c_default_clone = c_default.clone();
    let released = wait_until(Duration::from_secs(15), || {
        let c = c_default_clone.clone();
        let s = shard_to_move_clone;
        async move { !c.owned_shards().await.contains(&s) }
    })
    .await;
    assert!(released, "default node should release the moved shard");

    // GPU node should now own the moved shard
    let owned_gpu_after = c_gpu.owned_shards().await;
    assert_eq!(owned_gpu_after.len(), 1, "gpu node should own 1 shard");
    assert_eq!(
        owned_gpu_after[0], shard_to_move,
        "gpu node should own the moved shard"
    );

    // Default node should own one less shard
    let owned_default_final = c_default.owned_shards().await;
    assert_eq!(
        owned_default_final.len(),
        (num_shards - 1) as usize,
        "default node should own one less shard"
    );
    assert!(
        !owned_default_final.contains(&shard_to_move),
        "default node should not own the moved shard"
    );

    // Cleanup
    c_gpu.shutdown().await.unwrap();
    h_gpu.abort();
    c_default.shutdown().await.unwrap();
    h_default.abort();
}

/// Test ConfigureShard RPC moving shard between rings.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_configure_shard_ring_changes() {
    let prefix = unique_prefix();
    let num_shards: u32 = 2;

    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "test-node", "http://127.0.0.1:7450", num_shards);
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(
        coord.wait_converged(Duration::from_secs(10)).await,
        "coordinator should converge"
    );

    let owned = coord.owned_shards().await;
    let shard_id = owned[0];

    // Initially shard has no ring (default)
    let shard_map = coord.get_shard_map().await.unwrap();
    assert!(
        shard_map
            .get_shard(&shard_id)
            .unwrap()
            .placement_ring
            .is_none(),
        "shard should start with no ring"
    );

    // Configure shard to a specific ring
    let (prev, curr) = coord
        .update_shard_placement_ring(&shard_id, Some("tenant-a"))
        .await
        .expect("set ring");
    assert!(prev.is_none(), "previous should be None");
    assert_eq!(curr, Some("tenant-a".to_string()));

    // Verify in shard map
    let shard_map = coord.get_shard_map().await.unwrap();
    assert_eq!(
        shard_map.get_shard(&shard_id).unwrap().placement_ring,
        Some("tenant-a".to_string())
    );

    // Change to different ring
    let (prev, curr) = coord
        .update_shard_placement_ring(&shard_id, Some("tenant-b"))
        .await
        .expect("change ring");
    assert_eq!(prev, Some("tenant-a".to_string()));
    assert_eq!(curr, Some("tenant-b".to_string()));

    // Clear ring back to default
    let (prev, curr) = coord
        .update_shard_placement_ring(&shard_id, None)
        .await
        .expect("clear ring");
    assert_eq!(prev, Some("tenant-b".to_string()));
    assert!(curr.is_none(), "current should be None (default)");

    // Verify cleared
    let shard_map = coord.get_shard_map().await.unwrap();
    assert!(
        shard_map
            .get_shard(&shard_id)
            .unwrap()
            .placement_ring
            .is_none(),
        "shard ring should be cleared"
    );

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that MemberInfo correctly reports placement rings.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_member_info_reports_rings() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Start a default node
    let (c1, h1) =
        start_etcd_coordinator!(&prefix, "node-default", "http://127.0.0.1:7450", num_shards);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start a node with multiple rings
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-multi",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("node-multi"),
        vec!["gpu".to_string(), "high-memory".to_string()],
    )
    .await
    .expect("start node-multi");

    // Wait for both to be visible
    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Get members from c1's perspective
    let members = c1.get_members().await.expect("get members");
    assert_eq!(members.len(), 2, "should see 2 members");

    // Find each node
    let default_member = members.iter().find(|m| m.node_id == "node-default");
    let multi_member = members.iter().find(|m| m.node_id == "node-multi");

    assert!(default_member.is_some(), "should find default node");
    assert!(multi_member.is_some(), "should find multi node");

    // Check rings
    let default_rings = &default_member.unwrap().placement_rings;
    let multi_rings = &multi_member.unwrap().placement_rings;

    assert!(
        default_rings.is_empty(),
        "default node should have empty rings"
    );
    assert_eq!(multi_rings.len(), 2, "multi node should have 2 rings");
    assert!(
        multi_rings.contains(&"gpu".to_string()),
        "multi node should have gpu ring"
    );
    assert!(
        multi_rings.contains(&"high-memory".to_string()),
        "multi node should have high-memory ring"
    );

    // Cleanup
    c1.shutdown().await.unwrap();
    h1.abort();
    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test that ring change triggers shard handoff between nodes.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_ring_change_triggers_handoff() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Start default node
    let (c_default, h_default) =
        start_etcd_coordinator!(&prefix, "default-node", "http://127.0.0.1:7450", num_shards);
    let c_default: Arc<dyn Coordinator> = Arc::new(c_default);

    assert!(c_default.wait_converged(Duration::from_secs(15)).await);

    // Capture initial ownership
    let initial_owned = c_default.owned_shards().await;
    assert_eq!(initial_owned.len(), num_shards as usize);

    // Start a dedicated tenant node
    let endpoints = get_etcd_endpoints();
    let (c_tenant, h_tenant) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "tenant-node",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("tenant-node"),
        vec!["tenant-acme".to_string()],
    )
    .await
    .expect("start tenant node");
    let c_tenant: Arc<dyn Coordinator> = Arc::new(c_tenant);

    // Wait for tenant node to be ready
    assert!(c_tenant.wait_converged(Duration::from_secs(15)).await);

    // Tenant node should own no shards yet
    let tenant_owned_before = c_tenant.owned_shards().await;
    assert_eq!(
        tenant_owned_before.len(),
        0,
        "tenant node should own no shards initially"
    );

    // Move two shards to tenant ring
    let shard_1 = initial_owned[0];
    let shard_2 = initial_owned[1];

    c_default
        .update_shard_placement_ring(&shard_1, Some("tenant-acme"))
        .await
        .expect("move shard 1");
    c_default
        .update_shard_placement_ring(&shard_2, Some("tenant-acme"))
        .await
        .expect("move shard 2");

    // Wait for convergence
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        c_default.wait_converged(Duration::from_secs(15)).await,
        "default node should converge"
    );
    assert!(
        c_tenant.wait_converged(Duration::from_secs(15)).await,
        "tenant node should converge"
    );

    // Verify tenant node now owns the moved shards
    let tenant_owned_after: HashSet<ShardId> = c_tenant.owned_shards().await.into_iter().collect();
    assert_eq!(tenant_owned_after.len(), 2, "tenant should own 2 shards");
    assert!(tenant_owned_after.contains(&shard_1));
    assert!(tenant_owned_after.contains(&shard_2));

    // Verify default node no longer owns those shards
    let default_owned_after: HashSet<ShardId> =
        c_default.owned_shards().await.into_iter().collect();
    assert_eq!(
        default_owned_after.len(),
        (num_shards - 2) as usize,
        "default should own 2 fewer shards"
    );
    assert!(!default_owned_after.contains(&shard_1));
    assert!(!default_owned_after.contains(&shard_2));

    // Cleanup
    c_tenant.shutdown().await.unwrap();
    h_tenant.abort();
    c_default.shutdown().await.unwrap();
    h_default.abort();
}

/// Test rapid membership churn (node restart) converges correctly.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_rapid_membership_churn_converges() {
    let prefix = unique_prefix();
    let num_shards: u32 = 16;

    // Start first node, then quickly add/remove others to simulate churn
    let (c1, h1) = start_etcd_coordinator!(&prefix, "n1", "http://127.0.0.1:7450", num_shards);

    // Stagger node starts to reduce contention
    tokio::time::sleep(Duration::from_millis(100)).await;
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("n2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c3, h3) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "n3",
        "http://127.0.0.1:7452",
        num_shards,
        10,
        make_test_factory("n3"),
        Vec::new(),
    )
    .await
    .expect("start c3");

    // Brief churn: stop and restart n2 quickly
    tokio::time::sleep(Duration::from_millis(150)).await;
    c2.shutdown().await.unwrap();
    let _ = h2.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c2b, h2b) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("n2b"),
        Vec::new(),
    )
    .await
    .expect("restart c2");

    // Wait for all to converge post-churn (longer timeout for 3 nodes)
    let deadline = Duration::from_secs(45);
    assert!(c1.wait_converged(deadline).await);
    assert!(c2b.wait_converged(deadline).await);
    assert!(c3.wait_converged(deadline).await);

    // Validate ownership covers all shards and is disjoint
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2b.owned_shards().await.into_iter().collect();
    let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();
    let all: HashSet<ShardId> = s1
        .iter()
        .copied()
        .chain(s2.iter().copied())
        .chain(s3.iter().copied())
        .collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(all, expected, "all shards should be owned after churn");
    assert!(s1.is_disjoint(&s2), "s1 and s2 should be disjoint");
    assert!(s1.is_disjoint(&s3), "s1 and s3 should be disjoint");
    assert!(s2.is_disjoint(&s3), "s2 and s3 should be disjoint");

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
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_membership_persists_beyond_lease_ttl() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8; // Small for fast convergence
    let lease_ttl_secs: i64 = 2; // Short TTL to speed up test

    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let endpoints = get_etcd_endpoints();
    let (c1, h1) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7450",
        num_shards,
        lease_ttl_secs,
        make_test_factory("n1"),
        Vec::new(),
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

/// Verifies that get_shard_owner_map reflects actual ownership accurately.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_shard_owner_map_matches_actual_ownership() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;
    let lease_ttl_secs: i64 = 3;

    let endpoints = get_etcd_endpoints();
    let (c1, h1) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "n1",
        "http://127.0.0.1:7461",
        num_shards,
        lease_ttl_secs,
        make_test_factory("n1"),
        Vec::new(),
    )
    .await
    .expect("start c1");

    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "n2",
        "http://127.0.0.1:7462",
        num_shards,
        lease_ttl_secs,
        make_test_factory("n2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    assert!(c2.wait_converged(Duration::from_secs(15)).await);

    // Get the shard owner map from c1's perspective
    let owner_map = c1.get_shard_owner_map().await.expect("get owner map");

    // Verify all shards have owners
    assert_eq!(
        owner_map.shard_to_addr.len(),
        num_shards as usize,
        "all shards should have owners in map"
    );

    // Verify owner map is consistent with actual ownership
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s2: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();

    for (shard_id, addr) in &owner_map.shard_to_addr {
        if s1.contains(shard_id) {
            assert_eq!(
                addr, "http://127.0.0.1:7461",
                "shard {shard_id} owned by c1 should map to c1's addr"
            );
        } else if s2.contains(shard_id) {
            assert_eq!(
                addr, "http://127.0.0.1:7462",
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

/// Test that wait_converged refreshes the shard map before computing convergence.
/// This prevents flaky tests where the watch hasn't caught up with shard map changes.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_wait_converged_refreshes_shard_map() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Start a default node
    let (c_default, h_default) =
        start_etcd_coordinator!(&prefix, "default-node", "http://127.0.0.1:7450", num_shards);
    let c_default: Arc<dyn Coordinator> = Arc::new(c_default);

    assert!(
        c_default.wait_converged(Duration::from_secs(15)).await,
        "default node should converge"
    );

    // Verify default node owns all shards
    let owned_before = c_default.owned_shards().await;
    assert_eq!(owned_before.len(), num_shards as usize);

    // Start a GPU node that participates in "gpu" ring
    let endpoints = get_etcd_endpoints();
    let (c_gpu, h_gpu) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "gpu-node",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("gpu-node"),
        vec!["gpu".to_string()],
    )
    .await
    .expect("start gpu node");
    let c_gpu: Arc<dyn Coordinator> = Arc::new(c_gpu);

    // Wait for GPU node to converge (it should own nothing since no shards are in GPU ring)
    assert!(
        c_gpu.wait_converged(Duration::from_secs(15)).await,
        "gpu node should converge"
    );
    assert_eq!(
        c_gpu.owned_shards().await.len(),
        0,
        "gpu node should own no shards initially"
    );

    // Move a shard to the GPU ring using the default coordinator
    let shard_to_move = owned_before[0];
    c_default
        .update_shard_placement_ring(&shard_to_move, Some("gpu"))
        .await
        .expect("update placement ring");

    // Immediately call wait_converged on the GPU node.
    // Without the shard map refresh fix, this could see stale data where the shard
    // is still in the default ring, and incorrectly report convergence with 0 shards.
    // With the fix, wait_converged refreshes the shard map first and sees the shard
    // should now be owned by the GPU node.
    //
    // We use a longer timeout to allow for the actual shard acquisition to complete.
    assert!(
        c_gpu.wait_converged(Duration::from_secs(30)).await,
        "gpu node should converge after shard map change"
    );

    // Verify GPU node now owns the shard
    let gpu_owned = c_gpu.owned_shards().await;
    assert!(
        gpu_owned.contains(&shard_to_move),
        "gpu node should own the moved shard after wait_converged returns"
    );

    // Verify default node no longer owns it
    assert!(
        c_default.wait_converged(Duration::from_secs(15)).await,
        "default node should converge"
    );
    let default_owned = c_default.owned_shards().await;
    assert!(
        !default_owned.contains(&shard_to_move),
        "default node should not own the moved shard"
    );

    // Cleanup
    c_gpu.shutdown().await.unwrap();
    h_gpu.abort();
    c_default.shutdown().await.unwrap();
    h_default.abort();
}

/// Test that orphaned shards (no eligible node) remain unowned.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_orphaned_ring_shard_remains_unowned() {
    let prefix = unique_prefix();
    let num_shards: u32 = 2;

    // Start only a default node
    let (coord, handle) =
        start_etcd_coordinator!(&prefix, "default-node", "http://127.0.0.1:7450", num_shards);
    let coord: Arc<dyn Coordinator> = Arc::new(coord);

    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned_before = coord.owned_shards().await;
    assert_eq!(owned_before.len(), num_shards as usize);

    // Move a shard to a ring with no nodes
    let shard_to_orphan = owned_before[0];
    coord
        .update_shard_placement_ring(&shard_to_orphan, Some("nonexistent-ring"))
        .await
        .expect("move to nonexistent ring");

    // Wait for convergence
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    // The orphaned shard should no longer be owned
    let owned_after: HashSet<ShardId> = coord.owned_shards().await.into_iter().collect();
    assert_eq!(
        owned_after.len(),
        (num_shards - 1) as usize,
        "should own one less shard"
    );
    assert!(
        !owned_after.contains(&shard_to_orphan),
        "orphaned shard should not be owned"
    );

    // The shard owner map should reflect this
    let owner_map = coord.get_shard_owner_map().await.unwrap();
    assert!(
        owner_map.get_node(&shard_to_orphan).is_none(),
        "orphaned shard should have no owner in map"
    );

    // Move it back to default ring
    coord
        .update_shard_placement_ring(&shard_to_orphan, None)
        .await
        .expect("move back to default");

    // Wait and verify it's owned again
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(coord.wait_converged(Duration::from_secs(15)).await);

    let owned_final = coord.owned_shards().await;
    assert_eq!(owned_final.len(), num_shards as usize);
    assert!(
        owned_final.contains(&shard_to_orphan),
        "shard should be owned again after moving to default"
    );

    coord.shutdown().await.unwrap();
    handle.abort();
}

/// Test that in-flight shard acquisitions are cancelled during reconciliation.
///
/// Scenario: node-1 and node-2 converge with 12 shards split between them.
/// node-2 departs, so node-1 begins acquiring node-2's former shards. Before
/// node-1 finishes, node-3 joins. node-1 must cancel in-flight acquisitions
/// for shards that now belong to node-3 and converge quickly.
///
/// Without the fix (cancelling Acquiring-phase guards), node-1's guards would
/// keep competing for locks they no longer need, causing convergence to hang.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_reconciliation_cancels_in_flight_acquisitions() {
    let prefix = unique_prefix();
    let num_shards: u32 = 12;
    let endpoints = get_etcd_endpoints();

    // Start two nodes and converge
    let (c1, h1) = start_etcd_coordinator!(&prefix, "node-1", "http://127.0.0.1:7450", num_shards);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-2",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        make_test_factory("node-2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    assert!(
        c1.wait_converged(Duration::from_secs(30)).await,
        "initial c1 converge"
    );
    assert!(
        c2.wait_converged(Duration::from_secs(30)).await,
        "initial c2 converge"
    );

    let c1_initial = c1.owned_shards().await.len();
    let c2_initial = c2.owned_shards().await.len();
    assert_eq!(c1_initial + c2_initial, num_shards as usize);

    // Shut down node-2. node-1 will start acquiring node-2's former shards.
    c2.shutdown().await.unwrap();
    h2.abort();

    // Immediately start node-3 before node-1 finishes acquiring all of node-2's shards.
    // This forces node-1 to cancel in-flight acquisitions for shards that should go to node-3.
    let (c3, h3) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "node-3",
        "http://127.0.0.1:7452",
        num_shards,
        10,
        make_test_factory("node-3"),
        Vec::new(),
    )
    .await
    .expect("start c3");

    // Both nodes should converge within a reasonable time.
    // Without the in-flight cancellation fix, this would hang because node-1's
    // guards would be stuck in the Acquiring phase competing for locks that
    // node-3 now needs.
    let timeout = Duration::from_secs(30);
    assert!(
        c1.wait_converged(timeout).await,
        "c1 should converge after node-3 joins (in-flight acquisitions should be cancelled)"
    );
    assert!(
        c3.wait_converged(timeout).await,
        "c3 should converge after joining"
    );

    // Validate ownership: all shards covered, no overlap
    let s1: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    let s3: HashSet<ShardId> = c3.owned_shards().await.into_iter().collect();
    let all: HashSet<ShardId> = s1.union(&s3).copied().collect();
    let expected: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(all, expected, "all shards should be covered");
    assert!(s1.is_disjoint(&s3), "ownership should be disjoint");

    // Cleanup
    c1.shutdown().await.unwrap();
    c3.shutdown().await.unwrap();
    h1.abort();
    h3.abort();
}

// =============================================================================
// Permanent shard lease tests
// =============================================================================

/// Test that aborting a guard (simulating crash) does NOT release the shard owner key in etcd.
/// This is the core invariant of permanent leases: ownership survives crashes.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_lease_persists_after_abort() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let node_id = "crash-node-1".to_string();
    let (guard, _owned, _tx, handle) =
        make_guard_with_node_id(&coord, &prefix, shard_id, node_id.clone()).await;

    // Acquire the shard
    guard.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(3), || async {
        guard.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(acquired, "guard should acquire shard");

    // Abort the guard task (simulates crash -- no graceful release)
    handle.abort();
    let _ = handle.await; // wait for abort to complete

    // Verify the owner key still exists in etcd with the original node_id
    let owner_key = silo::coordination::keys::shard_owner_key(&prefix, &shard_id);
    let resp = coord
        .client()
        .kv_client()
        .get(owner_key, None)
        .await
        .expect("get owner key");
    assert_eq!(
        resp.kvs().len(),
        1,
        "owner key should persist after crash (abort)"
    );
    let value = std::str::from_utf8(resp.kvs()[0].value()).expect("valid utf8");
    assert_eq!(value, node_id, "owner should still be the crashed node");
}

/// Test that a permanent lease blocks another node from acquiring the shard.
/// When a node crashes, its shard stays unavailable until explicitly released.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_lease_blocks_other_nodes() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord1 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #1");
    let coord2 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #2");

    let shard_id = ShardId::new();

    // Node 1 acquires the shard
    let (g1, _owned1, _tx1, h1) =
        make_guard_with_node_id(&coord1, &prefix, shard_id, "blocker-node".to_string()).await;
    g1.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(acquired, "node 1 should acquire");

    // Crash node 1 (abort without release)
    h1.abort();
    let _ = h1.await;

    // Node 2 tries to acquire the same shard -- should be blocked
    let (g2, _owned2, _tx2, h2) =
        make_guard_with_node_id(&coord2, &prefix, shard_id, "blocked-node".to_string()).await;
    g2.set_desired(true).await;

    // Wait a bit -- node 2 should NOT be able to acquire
    tokio::time::sleep(Duration::from_secs(2)).await;
    let st2 = g2.state.lock().await;
    assert_ne!(
        st2.phase,
        ShardPhase::Held,
        "node 2 should NOT acquire shard owned by crashed node"
    );
    assert_eq!(
        st2.phase,
        ShardPhase::Acquiring,
        "node 2 should be stuck in Acquiring phase"
    );
    drop(st2);

    h2.abort();
}

/// Test that force_release_shard_lease clears the owner key and allows reacquisition.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_release_allows_reacquisition() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord1 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #1");
    let coord2 = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd #2");

    let shard_id = ShardId::new();

    // Node 1 acquires the shard
    let (g1, _owned1, _tx1, h1) =
        make_guard_with_node_id(&coord1, &prefix, shard_id, "force-node-1".to_string()).await;
    g1.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(3), || async {
        g1.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(acquired, "node 1 should acquire");

    // Crash node 1
    h1.abort();
    let _ = h1.await;

    // Force-release the shard via direct etcd KV delete
    let owner_key = silo::coordination::keys::shard_owner_key(&prefix, &shard_id);
    let resp = coord1
        .client()
        .kv_client()
        .delete(owner_key, None)
        .await
        .expect("force-release");
    assert_eq!(resp.deleted(), 1, "should delete one key");

    // Node 2 should now be able to acquire
    let (g2, owned2, _tx2, h2) =
        make_guard_with_node_id(&coord2, &prefix, shard_id, "force-node-2".to_string()).await;
    g2.set_desired(true).await;
    let acquired2 = wait_until(Duration::from_secs(5), || async {
        g2.state.lock().await.phase == ShardPhase::Held
    })
    .await;
    assert!(
        acquired2,
        "node 2 should acquire after force-release of crashed node's lease"
    );
    assert!(owned2.lock().await.contains(&shard_id));

    // Cleanup
    g2.set_desired(false).await;
    let _ = wait_until(Duration::from_secs(3), || async {
        g2.state.lock().await.phase == ShardPhase::Idle
    })
    .await;
    h2.abort();
}

/// Test that reclaim_existing_leases correctly finds leases owned by this node.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_existing_leases_finds_owned_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Start coordinator and let it acquire shards
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "reclaim-node",
        "http://127.0.0.1:50051",
        num_shards
    );
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "coordinator should converge"
    );
    let owned_before: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    assert_eq!(owned_before.len(), num_shards as usize);

    // Shutdown (graceful -- this releases leases) then verify reclaim returns empty
    c1.shutdown().await.unwrap();
    h1.abort();

    // Start a new coordinator with the same node_id and prefix -- since we gracefully
    // shut down, reclaim should find nothing (leases were released)
    let (c2, h2) = start_etcd_coordinator!(
        &prefix,
        "reclaim-node",
        "http://127.0.0.1:50051",
        num_shards
    );
    let reclaimed = c2.reclaim_existing_leases().await.expect("reclaim");
    assert!(
        reclaimed.is_empty(),
        "graceful shutdown should release all leases, so reclaim finds nothing"
    );
    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test that reclaim_existing_leases finds shards after a simulated crash.
/// Uses raw etcd KV to pre-populate owner keys, simulating a crash scenario
/// where the node's leases were not released.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_existing_leases_finds_shards_after_crash() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;
    let node_id = "reclaim-crash-node";

    // Start coordinator, let it acquire shards, then simulate crash by aborting
    let (c1, h1) = start_etcd_coordinator!(&prefix, node_id, "http://127.0.0.1:50051", num_shards);
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "coordinator should converge"
    );
    let owned_before: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    assert_eq!(owned_before.len(), num_shards as usize);

    // Simulate crash: abort the background task without graceful shutdown.
    // Drop the coordinator -- the owner keys should persist.
    h1.abort();
    drop(c1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start a new coordinator with the same node_id -- reclaim should find the old leases
    let (c2, h2) = start_etcd_coordinator!(&prefix, node_id, "http://127.0.0.1:50051", num_shards);
    let reclaimed: HashSet<ShardId> = c2
        .reclaim_existing_leases()
        .await
        .expect("reclaim")
        .into_iter()
        .collect();

    assert_eq!(
        reclaimed, owned_before,
        "reclaim should find all shards from the crashed coordinator"
    );

    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test that crash-restart automatically reclaims and opens shards from the previous run.
///
/// This tests the full startup reclamation integration: crash -> restart with same node_id ->
/// shards are automatically reclaimed, opened (WAL recovery), and the coordinator converges
/// owning all shards without requiring any manual intervention.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_crash_restart_reclaims_and_opens_shards() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;
    let node_id = "reclaim-restart-node";

    // Start coordinator, let it acquire all shards
    let (c1, h1) = start_etcd_coordinator!(&prefix, node_id, "http://127.0.0.1:50051", num_shards);
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "coordinator should converge"
    );
    let owned_before: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    assert_eq!(owned_before.len(), num_shards as usize);

    // Simulate crash: abort without graceful shutdown so leases persist
    h1.abort();
    drop(c1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start a new coordinator with the same node_id.
    // The startup reclamation should automatically discover, open, and own the shards.
    let (c2, h2) = start_etcd_coordinator!(&prefix, node_id, "http://127.0.0.1:50051", num_shards);

    // The coordinator should converge quickly -- reclaimed shards are opened immediately
    // during startup before the first reconcile, so owned set should match right away.
    assert!(
        c2.wait_converged(Duration::from_secs(15)).await,
        "restarted coordinator should converge with reclaimed shards"
    );

    let owned_after: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert_eq!(
        owned_after, owned_before,
        "restarted coordinator should own the same shards as before the crash"
    );

    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test hash ring divergence on restart: A and B share shards, A crashes, restarts
/// with the same node_id, reclaims all its old shards (WAL recovery), then releases
/// the ones that the hash ring says should belong to B via reconciliation.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_hash_ring_divergence_on_restart() {
    let prefix = unique_prefix();
    let num_shards: u32 = 8;

    // Start A and B, let them partition shards
    let (ca, ha) =
        start_etcd_coordinator!(&prefix, "diverge-a", "http://127.0.0.1:50051", num_shards);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoints = get_etcd_endpoints();
    let (cb, hb) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "diverge-b",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("diverge-b"),
        Vec::new(),
    )
    .await
    .expect("start cb");

    assert!(
        ca.wait_converged(Duration::from_secs(30)).await,
        "ca should converge"
    );
    assert!(
        cb.wait_converged(Duration::from_secs(30)).await,
        "cb should converge"
    );

    let all_shards: HashSet<ShardId> = ca
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    let a_owned_before: HashSet<ShardId> = ca.owned_shards().await.into_iter().collect();
    let b_owned_before: HashSet<ShardId> = cb.owned_shards().await.into_iter().collect();
    assert_eq!(
        a_owned_before
            .union(&b_owned_before)
            .copied()
            .collect::<HashSet<_>>(),
        all_shards,
        "all shards should be covered"
    );
    assert!(a_owned_before.is_disjoint(&b_owned_before), "no overlap");
    // With 8 shards and 2 nodes, each should have some
    assert!(!a_owned_before.is_empty() && !b_owned_before.is_empty());

    // Crash A (abort without graceful shutdown -- permanent leases persist)
    ha.abort();
    drop(ca);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // B still owns its shards, but can't take A's due to permanent leases
    let b_owned_during_crash: HashSet<ShardId> = cb.owned_shards().await.into_iter().collect();
    assert_eq!(b_owned_during_crash, b_owned_before);

    // Restart A with the same node_id -- reclaim opens old shards, then reconciliation
    // hands off shards that the hash ring assigns to B
    let (ca2, ha2) =
        start_etcd_coordinator!(&prefix, "diverge-a", "http://127.0.0.1:50051", num_shards);

    // Both should converge to the same distribution as before the crash
    assert!(
        ca2.wait_converged(Duration::from_secs(30)).await,
        "restarted A should converge"
    );
    assert!(
        cb.wait_converged(Duration::from_secs(30)).await,
        "B should converge after A restarts"
    );

    let a_owned_after: HashSet<ShardId> = ca2.owned_shards().await.into_iter().collect();
    let b_owned_after: HashSet<ShardId> = cb.owned_shards().await.into_iter().collect();

    assert_eq!(
        a_owned_after
            .union(&b_owned_after)
            .copied()
            .collect::<HashSet<_>>(),
        all_shards,
        "all shards should be covered after restart"
    );
    assert!(
        a_owned_after.is_disjoint(&b_owned_after),
        "no overlap after restart"
    );
    // The distribution should match the pre-crash state (same nodes, same hash ring)
    assert_eq!(
        a_owned_after, a_owned_before,
        "A should own the same shards after restart as before crash"
    );
    assert_eq!(
        b_owned_after, b_owned_before,
        "B should own the same shards after restart as before crash"
    );

    // Cleanup
    ca2.shutdown().await.unwrap();
    cb.shutdown().await.unwrap();
    ha2.abort();
    hb.abort();
}

/// Test the full coordinator-level flow: crash preserves leases, other nodes can't take them,
/// force-release unblocks.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_crash_blocks_reacquisition_until_force_release() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Start node 1, let it own all shards
    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "coord-crash-1",
        "http://127.0.0.1:50051",
        num_shards
    );
    assert!(c1.wait_converged(Duration::from_secs(15)).await);
    let all_shards: HashSet<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    let c1_owned: HashSet<ShardId> = c1.owned_shards().await.into_iter().collect();
    assert_eq!(c1_owned, all_shards);

    // Crash node 1 (abort without shutdown)
    h1.abort();
    drop(c1);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start node 2 -- it should NOT be able to acquire node 1's shards
    let endpoints = get_etcd_endpoints();
    let (c2, h2) = EtcdCoordinator::start(
        &endpoints,
        &prefix,
        "coord-crash-2",
        "http://127.0.0.1:50052",
        num_shards,
        10,
        make_test_factory("coord-crash-2"),
        Vec::new(),
    )
    .await
    .expect("start c2");

    // Give c2 time to try acquiring -- it should fail since leases are permanent
    tokio::time::sleep(Duration::from_secs(3)).await;
    let c2_owned: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    assert!(
        c2_owned.is_empty(),
        "node 2 should NOT acquire any shards while node 1's permanent leases exist"
    );

    // Force-release all of node 1's shards via raw etcd client
    let cfg = silo::settings::AppConfig::load(None).expect("load config");
    let raw_conn = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd for force-release");
    for shard_id in &all_shards {
        let owner_key = silo::coordination::keys::shard_owner_key(&prefix, shard_id);
        raw_conn
            .client()
            .kv_client()
            .delete(owner_key, None)
            .await
            .expect("force release");
    }

    // Now node 2 should be able to acquire -- may take a few reconciliation cycles
    let all_acquired = wait_until(Duration::from_secs(30), || async {
        let owned: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
        owned == all_shards
    })
    .await;
    assert!(all_acquired, "c2 should own all shards after force-release");

    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Test that force_release_shard_lease returns ShardNotFound for a non-existent shard.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_release_nonexistent_shard_returns_error() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    let (c1, h1) = start_etcd_coordinator!(
        &prefix,
        "force-err-node",
        "http://127.0.0.1:50051",
        num_shards
    );
    assert!(c1.wait_converged(Duration::from_secs(15)).await);

    // Force-release a shard that doesn't exist (random UUID)
    let fake_shard_id = ShardId::new();
    let result = c1.force_release_shard_lease(&fake_shard_id).await;
    assert!(
        matches!(result, Err(CoordinationError::ShardNotFound(id)) if id == fake_shard_id),
        "force-releasing a non-existent shard should return ShardNotFound, got: {:?}",
        result,
    );

    c1.shutdown().await.unwrap();
    h1.abort();
}

/// Test that reclaim_existing_leases does NOT return shards owned by a different node_id.
/// This validates the critical node_id stability assumption of permanent leases.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_with_different_node_id_returns_empty() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;

    // Start coordinator A, let it acquire all shards
    let (ca, ha) = start_etcd_coordinator!(
        &prefix,
        "original-node-a",
        "http://127.0.0.1:50051",
        num_shards
    );
    assert!(
        ca.wait_converged(Duration::from_secs(15)).await,
        "coordinator A should converge"
    );
    let owned: HashSet<ShardId> = ca.owned_shards().await.into_iter().collect();
    assert_eq!(owned.len(), num_shards as usize);

    // Crash coordinator A (abort without graceful shutdown -- permanent leases persist)
    ha.abort();
    drop(ca);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start coordinator B with a DIFFERENT node_id but same prefix.
    // reclaim_existing_leases should return empty because B doesn't own A's leases.
    let (cb, hb) = start_etcd_coordinator!(
        &prefix,
        "different-node-b",
        "http://127.0.0.1:50051",
        num_shards
    );
    let reclaimed = cb.reclaim_existing_leases().await.expect("reclaim");
    assert!(
        reclaimed.is_empty(),
        "different node_id should NOT reclaim another node's leases, but found: {:?}",
        reclaimed,
    );

    cb.shutdown().await.unwrap();
    hb.abort();
}

/// Test that graceful shutdown deletes the shard owner keys in etcd,
/// allowing other nodes to immediately acquire them without force-release.
/// This is the etcd parallel of k8s_graceful_shutdown_clears_lease_holder_identity.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_graceful_shutdown_clears_shard_owner_keys() {
    let prefix = unique_prefix();
    let num_shards: u32 = 4;
    let node_id = "shutdown-lease-etcd";

    // Start coordinator and let it acquire all shards
    let endpoints = get_etcd_endpoints();
    let factory = make_test_factory(node_id);
    let (c1, h1) = match EtcdCoordinator::start(
        &endpoints,
        &prefix,
        node_id,
        "http://127.0.0.1:50051",
        num_shards,
        10,
        factory,
        Vec::new(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };
    assert!(
        c1.wait_converged(Duration::from_secs(15)).await,
        "coordinator should converge"
    );

    // Collect the shard IDs
    let shard_ids: Vec<ShardId> = c1
        .get_shard_map()
        .await
        .unwrap()
        .shard_ids()
        .into_iter()
        .collect();
    assert_eq!(shard_ids.len(), num_shards as usize);

    // Verify owner keys exist before shutdown
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let conn = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");
    let mut client = conn.client();
    for shard_id in &shard_ids {
        let owner_key = silo::coordination::keys::shard_owner_key(&prefix, shard_id);
        let resp = client
            .get(owner_key.as_bytes(), None)
            .await
            .expect("get key");
        assert!(
            resp.count() > 0,
            "owner key for shard {} should exist before shutdown",
            shard_id,
        );
        let kv = resp.kvs().first().unwrap();
        assert_eq!(
            std::str::from_utf8(kv.value()).unwrap(),
            node_id,
            "owner key for shard {} should be held by {}",
            shard_id,
            node_id,
        );
    }

    // Graceful shutdown
    c1.shutdown().await.unwrap();
    h1.abort();

    // Verify owner keys are deleted after shutdown
    for shard_id in &shard_ids {
        let owner_key = silo::coordination::keys::shard_owner_key(&prefix, shard_id);
        let resp = client
            .get(owner_key.as_bytes(), None)
            .await
            .expect("get key");
        assert_eq!(
            resp.count(),
            0,
            "owner key for shard {} should be deleted after graceful shutdown, but found value: {:?}",
            shard_id,
            resp.kvs()
                .first()
                .map(|kv| String::from_utf8_lossy(kv.value()).to_string()),
        );
    }

    // Verify a new node can immediately acquire the shards (no force-release needed)
    let (c2, h2) = start_etcd_coordinator!(
        &prefix,
        "shutdown-lease-etcd-2",
        "http://127.0.0.1:50052",
        num_shards
    );
    assert!(
        c2.wait_converged(Duration::from_secs(15)).await,
        "new coordinator should converge and acquire all shards without force-release"
    );
    let c2_owned: HashSet<ShardId> = c2.owned_shards().await.into_iter().collect();
    let expected: HashSet<ShardId> = shard_ids.into_iter().collect();
    assert_eq!(
        c2_owned, expected,
        "new coordinator should own all shards after graceful shutdown of first"
    );

    // Cleanup
    c2.shutdown().await.unwrap();
    h2.abort();
}

/// Helper to create an etcd guard with an externally-provided factory.
/// This allows tests to know the factory's tmpdir for fault injection (e.g., chmod).
async fn make_guard_with_factory(
    coord: &EtcdConnection,
    cluster_prefix: &str,
    shard_id: ShardId,
    node_id: String,
    factory: Arc<ShardFactory>,
) -> (
    std::sync::Arc<EtcdShardGuard>,
    std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<ShardId>>>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let client = coord.client();
    let owned = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let (tx, rx) = tokio::sync::watch::channel(false);
    let guard = EtcdShardGuard::new(
        shard_id,
        client.clone(),
        cluster_prefix.to_string(),
        node_id.clone(),
        rx,
    );
    let runner = guard.clone();
    let owned_arc = owned.clone();
    let mut shard_map = silo::shard_range::ShardMap::new();
    shard_map.add_shard(silo::shard_range::ShardInfo::new(
        shard_id,
        silo::shard_range::ShardRange::full(),
    ));
    let shard_map = Arc::new(tokio::sync::Mutex::new(shard_map));
    let none_coord = silo::coordination::NoneCoordinator::new(
        &node_id,
        "http://127.0.0.1:0",
        1,
        factory.clone(),
        Vec::new(),
    )
    .await
    .unwrap();
    let coordinator: Arc<dyn Coordinator> = Arc::new(none_coord);
    let handle = tokio::spawn(async move {
        runner.run(owned_arc, factory, shard_map, coordinator).await;
    });
    (guard, owned, tx, handle)
}

/// Make a directory tree read-only so SlateDB writes fail, simulating a storage failure.
fn make_dir_tree_readonly(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                make_dir_tree_readonly(&entry_path);
            } else {
                let _ =
                    std::fs::set_permissions(&entry_path, std::fs::Permissions::from_mode(0o444));
            }
        }
    }
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555));
}

/// Restore a directory tree to writable after a test.
fn restore_dir_tree_writable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755));
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                restore_dir_tree_writable(&entry_path);
            } else {
                let _ =
                    std::fs::set_permissions(&entry_path, std::fs::Permissions::from_mode(0o644));
            }
        }
    }
}

/// If factory.close() fails during a normal release, the guard should keep retrying
/// and the ownership key should persist in etcd until close succeeds.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_shard_close_failure_keeps_ownership() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let node_id = next_node_id();
    let tmpdir = std::env::temp_dir().join(format!("silo-etcd-close-fail-{}", &node_id));
    let mut factory = ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    );
    factory.set_close_timeout(Duration::from_secs(3));
    let factory = Arc::new(factory);

    let (guard, owned, _tx, handle) =
        make_guard_with_factory(&coord, &prefix, shard_id, node_id, factory).await;

    // Acquire the shard
    guard.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(5), || async {
        let st = guard.state.lock().await;
        st.phase == ShardPhase::Held && st.is_held
    })
    .await;
    assert!(acquired, "guard should acquire ownership");

    // Make the shard data directory read-only so close() fails
    let shard_data_path = tmpdir.join(shard_id.to_string());
    assert!(
        shard_data_path.exists(),
        "shard data dir should exist after acquisition"
    );
    make_dir_tree_readonly(&shard_data_path);

    // Trigger release
    guard.set_desired(false).await;
    guard.notify.notify_one();

    // Wait for at least one close attempt to fail (timeout is 3s).
    // The etcd guard's state machine automatically retries (Held→Releasing cycle),
    // so we can't check for Held phase (it's transient). Instead, wait long enough
    // for at least one close attempt to have timed out, then verify ownership persists.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify ownership is still held despite the close retry loop
    assert!(
        owned.lock().await.contains(&shard_id),
        "shard should still be in owned set"
    );

    // Verify the owner key still exists in etcd
    let owner_key = silo::coordination::keys::shard_owner_key(&prefix, &shard_id);
    let resp = coord
        .client()
        .get(owner_key.as_bytes(), None)
        .await
        .expect("etcd get");
    assert!(
        !resp.kvs().is_empty(),
        "owner key should still exist in etcd"
    );

    // Restore permissions so close can succeed on the next retry cycle
    restore_dir_tree_writable(&shard_data_path);

    // The guard is already retrying release in its Held→Releasing loop,
    // so it will pick up the restored permissions and succeed.
    let released = wait_until(Duration::from_secs(10), || async {
        let st = guard.state.lock().await;
        st.phase == ShardPhase::Idle && !st.is_held
    })
    .await;
    assert!(released, "guard should release after permissions restored");
    assert!(
        !owned.lock().await.contains(&shard_id),
        "shard should be removed from owned set"
    );

    handle.abort();
    let _ = std::fs::remove_dir_all(&tmpdir);
}

/// If factory.close() fails during shutdown, the ownership key should persist in etcd,
/// mimicking crash behavior so no other node acquires unflushed data.
#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_shard_close_failure_during_shutdown_keeps_ownership() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = EtcdConnection::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let shard_id = ShardId::new();
    let node_id = next_node_id();
    let tmpdir = std::env::temp_dir().join(format!("silo-etcd-close-fail-sd-{}", &node_id));
    let mut factory = ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    );
    factory.set_close_timeout(Duration::from_secs(3));
    let factory = Arc::new(factory);

    let (guard, owned, tx, handle) =
        make_guard_with_factory(&coord, &prefix, shard_id, node_id, factory).await;

    // Acquire the shard
    guard.set_desired(true).await;
    let acquired = wait_until(Duration::from_secs(5), || async {
        let st = guard.state.lock().await;
        st.phase == ShardPhase::Held && st.is_held
    })
    .await;
    assert!(acquired, "guard should acquire ownership");

    // Make the shard data directory read-only so close() fails
    let shard_data_path = tmpdir.join(shard_id.to_string());
    assert!(
        shard_data_path.exists(),
        "shard data dir should exist after acquisition"
    );
    make_dir_tree_readonly(&shard_data_path);

    // Trigger shutdown
    let _ = tx.send(true);
    guard.notify.notify_one();

    // Wait for the guard to reach ShutDown phase.
    // The close timeout is 3s, plus processing time.
    let shut_down = wait_until(Duration::from_secs(10), || async {
        let st = guard.state.lock().await;
        st.phase == ShardPhase::ShutDown
    })
    .await;
    assert!(shut_down, "guard should reach ShutDown phase");

    // Verify the shard is still in owned set (ownership not released because close failed)
    assert!(
        owned.lock().await.contains(&shard_id),
        "shard should still be in owned set since ownership was not released"
    );

    // Verify the owner key still exists in etcd
    let owner_key = silo::coordination::keys::shard_owner_key(&prefix, &shard_id);
    let resp = coord
        .client()
        .get(owner_key.as_bytes(), None)
        .await
        .expect("etcd get");
    assert!(
        !resp.kvs().is_empty(),
        "owner key should still exist in etcd after shutdown with close failure"
    );

    // Cleanup
    restore_dir_tree_writable(&shard_data_path);
    handle.abort();
    let _ = std::fs::remove_dir_all(&tmpdir);
}
