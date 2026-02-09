use std::sync::Arc;
use std::time::Duration;

use silo::coordination::{Coordinator, NoneCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};

fn make_test_factory() -> Arc<ShardFactory> {
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: "unused".to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    ))
}

#[silo::test]
async fn none_coordinator_owns_all_shards() {
    let coord = NoneCoordinator::new(
        "test-node",
        "http://localhost:7450",
        16,
        make_test_factory(),
        Vec::new(),
    )
    .await
    .unwrap();

    let owned = coord.owned_shards().await;
    assert_eq!(owned.len(), 16);
    // Just verify we have 16 unique ShardIds
    let unique_ids: std::collections::HashSet<_> = owned.iter().collect();
    assert_eq!(unique_ids.len(), 16);
}

#[silo::test]
async fn none_coordinator_always_converged() {
    let coord = NoneCoordinator::new(
        "test-node",
        "http://localhost:7450",
        16,
        make_test_factory(),
        Vec::new(),
    )
    .await
    .unwrap();

    assert!(coord.wait_converged(Duration::from_millis(1)).await);
}

#[silo::test]
async fn none_coordinator_single_member() {
    let coord = NoneCoordinator::new(
        "test-node",
        "http://localhost:7450",
        16,
        make_test_factory(),
        Vec::new(),
    )
    .await
    .unwrap();

    let members = coord.get_members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].node_id, "test-node");
}

#[silo::test]
async fn none_coordinator_shard_map() {
    let coord = NoneCoordinator::new(
        "test-node",
        "http://localhost:7450",
        4,
        make_test_factory(),
        Vec::new(),
    )
    .await
    .unwrap();

    let map = coord.get_shard_owner_map().await.unwrap();
    assert_eq!(map.num_shards(), 4);
    assert_eq!(map.shard_to_addr.len(), 4);

    // Verify all shards have the correct address and node mappings
    for shard_info in map.shard_map.shards() {
        assert_eq!(
            map.shard_to_addr.get(&shard_info.id),
            Some(&"http://localhost:7450".to_string())
        );
        assert_eq!(
            map.shard_to_node.get(&shard_info.id),
            Some(&"test-node".to_string())
        );
    }
}
