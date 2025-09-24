use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use silo::coordination::{Coordination, Coordinator};

// Global mutex to serialize coordination tests
static COORDINATION_TEST_MUTEX: Mutex<()> = Mutex::new(());

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-{}", nanos)
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_nodes_own_unique_shards() {
    // Serialize coordination tests to avoid etcd conflicts
    let _guard = COORDINATION_TEST_MUTEX.lock().unwrap();

    // Assumes etcd is running locally (e.g., `just etcd` or via dev shell)
    let prefix = unique_prefix();
    let num_shards: u32 = 128;

    let cfg = silo::settings::AppConfig::load(None).expect("load default config");
    let coord = Coordination::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    // Start three coordinators (nodes)
    let (c1, h1) = Coordinator::start(&coord, &prefix, "n1", num_shards, 10)
        .await
        .expect("start c1");
    let (c2, h2) = Coordinator::start(&coord, &prefix, "n2", num_shards, 10)
        .await
        .expect("start c2");
    let (c3, h3) = Coordinator::start(&coord, &prefix, "n3", num_shards, 10)
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
    let coord = Coordination::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let (c1, h1) = Coordinator::start(&coord, &prefix, "n1", num_shards, 10)
        .await
        .unwrap();
    let (c2, h2) = Coordinator::start(&coord, &prefix, "n2", num_shards, 10)
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
    let (c3, h3) = Coordinator::start(&coord, &prefix, "n3", num_shards, 10)
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
    assert!(HashSet::<u32>::from_iter(s1.iter().copied())
        .is_disjoint(&HashSet::from_iter(s2.iter().copied())));
    assert!(HashSet::<u32>::from_iter(s1.iter().copied())
        .is_disjoint(&HashSet::from_iter(s3.iter().copied())));
    assert!(HashSet::<u32>::from_iter(s2.iter().copied())
        .is_disjoint(&HashSet::from_iter(s3.iter().copied())));

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
    let coord = Coordination::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    let (c1, h1) = Coordinator::start(&coord, &prefix, "n1", num_shards, 10)
        .await
        .unwrap();
    let (c2, h2) = Coordinator::start(&coord, &prefix, "n2", num_shards, 10)
        .await
        .unwrap();
    let (c3, h3) = Coordinator::start(&coord, &prefix, "n3", num_shards, 10)
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
    assert!(HashSet::<u32>::from_iter(s1.iter().copied())
        .is_disjoint(&HashSet::from_iter(s2.iter().copied())));

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
    let coord = Coordination::connect(&cfg.coordination)
        .await
        .expect("connect etcd");

    // Start first node, then quickly add/remove others to simulate churn
    let (c1, h1) = Coordinator::start(&coord, &prefix, "n1", num_shards, 10)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (c2, h2) = Coordinator::start(&coord, &prefix, "n2", num_shards, 10)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (c3, h3) = Coordinator::start(&coord, &prefix, "n3", num_shards, 10)
        .await
        .unwrap();

    // Brief churn: stop and restart n2 quickly
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    c2.shutdown().await.unwrap();
    let _ = h2.abort();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (c2b, h2b) = Coordinator::start(&coord, &prefix, "n2", num_shards, 10)
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
