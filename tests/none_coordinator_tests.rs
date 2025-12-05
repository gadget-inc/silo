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
        },
        MockGubernatorClient::new_arc(),
    ))
}

#[silo::test]
async fn none_coordinator_owns_all_shards() {
    let coord = NoneCoordinator::new("test-node", "http://localhost:50051", 16, make_test_factory()).await;

    let owned = coord.owned_shards().await;
    assert_eq!(owned.len(), 16);
    assert_eq!(owned, (0..16).collect::<Vec<_>>());
}

#[silo::test]
async fn none_coordinator_always_converged() {
    let coord = NoneCoordinator::new("test-node", "http://localhost:50051", 16, make_test_factory()).await;

    assert!(coord.wait_converged(Duration::from_millis(1)).await);
}

#[silo::test]
async fn none_coordinator_single_member() {
    let coord = NoneCoordinator::new("test-node", "http://localhost:50051", 16, make_test_factory()).await;

    let members = coord.get_members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].node_id, "test-node");
}

#[silo::test]
async fn none_coordinator_shard_map() {
    let coord = NoneCoordinator::new("test-node", "http://localhost:50051", 4, make_test_factory()).await;

    let map = coord.get_shard_owner_map().await.unwrap();
    assert_eq!(map.num_shards, 4);
    assert_eq!(map.shard_to_addr.len(), 4);

    for shard_id in 0..4 {
        assert_eq!(
            map.shard_to_addr.get(&shard_id),
            Some(&"http://localhost:50051".to_string())
        );
        assert_eq!(
            map.shard_to_node.get(&shard_id),
            Some(&"test-node".to_string())
        );
    }
}

