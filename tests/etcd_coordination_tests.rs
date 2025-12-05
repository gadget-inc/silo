use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use silo::coordination::etcd::{EtcdConnection, EtcdShardGuard, ShardPhase};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-sg-{}", nanos)
}

fn make_test_factory(prefix: &str) -> Arc<ShardFactory> {
    let tmpdir = std::env::temp_dir().join(format!("silo-test-{}", prefix));
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

async fn make_guard(
    coord: &EtcdConnection,
    cluster_prefix: &str,
    shard_id: u32,
) -> (
    std::sync::Arc<EtcdShardGuard>,
    std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<u32>>>,
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
    let handle = tokio::spawn(async move {
        runner.run(owned_arc, factory).await;
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

    let shard_id = 7u32;
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

    let shard_id = 3u32;
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

    let shard_id = 11u32;
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

    let shard_id = 19u32;
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

    let shard_id = 23u32;
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

    let shard_id = 21u32;
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

    let shard_id = 17u32;
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

    let shard_id = 29u32;
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

    let shard_id = 31u32;
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

    let shard_id = 37u32;
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
    // Allow loop to unlock
    tokio::time::sleep(Duration::from_millis(200)).await;
    let st = g.state.lock().await;
    assert_eq!(st.held_key, None);
    assert_eq!(st.phase, ShardPhase::ShutDown);
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

    let shard_id = 41u32;
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
    tokio::time::sleep(Duration::from_millis(200)).await;
    let st = g.state.lock().await;
    assert_eq!(st.held_key, None);
    assert_eq!(st.phase, ShardPhase::ShutDown);
    assert!(!owned.lock().await.contains(&shard_id));
    h.abort();
}
