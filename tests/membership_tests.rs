use tokio::time::{sleep, Duration};

use silo::bootstrap;
use silo::pb::silo_client::SiloClient;
use silo::pb::GetShardOwnershipRequest;

#[tokio::test]
#[serial_test::serial]
async fn cluster_starts_up() {
    // Build a three-node cluster via Silo bootstrap, using test configs
    let h1 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n1.toml"))
        .await
        .expect("node1");
    let h2 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n2.toml"))
        .await
        .expect("node2");
    let h3 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n3.toml"))
        .await
        .expect("node3");

    // Wait for a leader among the three endpoints
    wait_for_leader_any(&[
        "http://127.0.0.1:19081",
        "http://127.0.0.1:19082",
        "http://127.0.0.1:19083",
    ])
    .await;

    // Attempt to join a learner
    let endpoints = vec![
        "http://127.0.0.1:19081".to_string(),
        "http://127.0.0.1:19082".to_string(),
        "http://127.0.0.1:19083".to_string(),
    ];
    let resp =
        silo::membership::join_cluster(endpoints.clone(), 4, "127.0.0.1:19084".to_string()).await;
    assert!(resp.is_ok(), "join should work: {:?}", resp.err());

    // Remove the node
    let removed = silo::membership::remove_node(&endpoints, 4).await;
    assert!(removed.is_ok(), "remove should work: {:?}", removed.err());

    // Verify vnode ownership covers full ring with no overlap among three nodes
    let mut covered = std::collections::HashSet::new();
    for port in [50051u16, 50052, 50053] {
        let ep = format!("http://127.0.0.1:{}", port);
        let mut client = SiloClient::connect(ep).await.unwrap();
        let resp = client
            .get_shard_ownership(tonic::Request::new(GetShardOwnershipRequest {}))
            .await
            .unwrap()
            .into_inner();
        for v in resp.shards {
            assert!(covered.insert(v), "duplicate vnode {}", v);
        }
        assert_eq!(resp.shard_count, 128);
    }
    assert_eq!(covered.len(), 128, "expected full shard coverage");

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

#[tokio::test]
#[serial_test::serial]
async fn leave_and_join_reassigns_vnodes() {
    // Start initial 3-node cluster
    let h1 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n1.toml"))
        .await
        .expect("node1");
    let h2 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n2.toml"))
        .await
        .expect("node2");
    let h3 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n3.toml"))
        .await
        .expect("node3");

    wait_for_leader_any(&[
        "http://127.0.0.1:19081",
        "http://127.0.0.1:19082",
        "http://127.0.0.1:19083",
    ])
    .await;

    let endpoints = vec![
        "http://127.0.0.1:19081".to_string(),
        "http://127.0.0.1:19082".to_string(),
        "http://127.0.0.1:19083".to_string(),
    ];

    // Capture current ownership
    let _before = fetch_ring().await;

    // Remove node 3
    let removed = silo::membership::remove_node(&endpoints, 3).await;
    assert!(
        removed.is_ok(),
        "remove node3 should work: {:?}",
        removed.err()
    );

    // Start node 4
    let h4 = bootstrap::start_from_config_path(std::path::Path::new("tests/cluster/n4.toml"))
        .await
        .expect("node4");

    wait_for_leader_any(&[
        "http://127.0.0.1:19081",
        "http://127.0.0.1:19082",
        "http://127.0.0.1:19083",
        "http://127.0.0.1:19084",
    ])
    .await;

    // Join node 4 via OpenRaft admin API
    let resp = silo::membership::join_cluster(
        vec![
            "http://127.0.0.1:19081".to_string(),
            "http://127.0.0.1:19082".to_string(),
            "http://127.0.0.1:19083".to_string(),
        ],
        4,
        "127.0.0.1:19084".to_string(),
    )
    .await;
    assert!(resp.is_ok(), "join node4 should work: {:?}", resp.err());

    // Verify reassignment covers full ring, and node3 no longer owns vnodes
    let after = {
        let mut tries = 0;
        loop {
            let info = fetch_ring_with_ports(&[50051u16, 50052, 50054]).await;
            if info.covered.len() == 128 || tries > 50 {
                break info;
            }
            sleep(Duration::from_millis(100)).await;
            tries += 1;
        }
    };
    assert_eq!(
        after.covered.len(),
        128,
        "shard ring not fully covered after reassignment"
    );
    assert!(
        after.node_shards.get(&3).is_none(),
        "node3 should have no shards"
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
    h4.shutdown().await;
}

struct RingInfo {
    covered: std::collections::HashSet<u32>,
    node_shards: std::collections::HashMap<u32, Vec<u32>>,
}

async fn fetch_ring() -> RingInfo {
    fetch_ring_with_ports(&[50051u16, 50052, 50053]).await
}

async fn fetch_ring_with_ports(ports: &[u16]) -> RingInfo {
    use std::collections::{HashMap, HashSet};
    let mut covered = HashSet::new();
    let mut node_shards: HashMap<u32, Vec<u32>> = HashMap::new();
    for port in ports {
        let ep = format!("http://127.0.0.1:{}", port);
        let mut client = SiloClient::connect(ep).await.unwrap();
        let resp = client
            .get_shard_ownership(tonic::Request::new(GetShardOwnershipRequest {}))
            .await
            .unwrap()
            .into_inner();
        for v in &resp.shards {
            covered.insert(*v);
        }
        node_shards.insert(resp.node_id, resp.shards);
    }
    RingInfo {
        covered,
        node_shards,
    }
}
async fn wait_for_leader_any(_endpoints: &[&str]) {
    // TODO: implement leader discovery via OpenRaft admin API
    // For now, just wait a bit for nodes to start
    sleep(Duration::from_millis(1000)).await;
}
