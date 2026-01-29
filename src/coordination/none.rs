//! No-op coordination backend for single-node local development.
//!
//! This backend assumes a single node owns all shards. No actual coordination
//! is performed - useful for local development and testing without etcd or K8S.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::factory::ShardFactory;
use crate::shard_range::{ShardId, ShardMap};

use crate::coordination::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap, SplitCleanupStatus,
    SplitStorageBackend, get_hostname,
};
use crate::shard_range::SplitInProgress;

/// A no-op coordinator for single-node deployments.
///
/// In this mode, the single node is assumed to own all shards.
/// No distributed coordination is performed.
pub struct NoneCoordinator {
    base: CoordinatorBase,
}

impl NoneCoordinator {
    /// Create a new single-node coordinator.
    ///
    /// Creates a shard map with the given number of shards and opens all shards
    /// immediately since this node owns everything.
    pub async fn new(
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        initial_shard_count: u32,
        factory: Arc<ShardFactory>,
    ) -> Self {
        // Create the shard map for single-node mode
        let shard_map = ShardMap::create_initial(initial_shard_count)
            .expect("failed to create initial shard map");

        let base = CoordinatorBase::new(node_id, grpc_addr, shard_map, Arc::clone(&factory));

        // In single-node mode, we own all shards immediately - open them all
        {
            let shard_map = base.shard_map.lock().await;
            let mut owned = base.owned.lock().await;
            for shard_info in shard_map.shards() {
                if let Err(e) = factory.open(&shard_info.id, &shard_info.range).await {
                    tracing::error!(shard_id = %shard_info.id, error = %e, "failed to open shard");
                } else {
                    owned.insert(shard_info.id);
                }
            }
        }

        Self { base }
    }

    /// Create a single-node coordinator using shards that are already open in the factory.
    ///
    /// This is useful for tests where shards are pre-opened with predictable IDs.
    /// The shard map is built from the factory's existing instances.
    pub async fn from_factory(
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        factory: Arc<ShardFactory>,
    ) -> Self {
        use crate::shard_range::ShardInfo;

        // Build shard map from factory's existing shards
        let instances = factory.instances();
        let shard_infos: Vec<ShardInfo> = instances
            .iter()
            .map(|(id, shard)| ShardInfo::new(*id, shard.get_range()))
            .collect();
        let shard_map = ShardMap::from_shards(shard_infos);

        let base = CoordinatorBase::new(node_id, grpc_addr, shard_map, Arc::clone(&factory));

        // Mark all existing shards as owned
        {
            let mut owned = base.owned.lock().await;
            for shard_id in instances.keys() {
                owned.insert(*shard_id);
            }
        }

        Self { base }
    }
}

#[async_trait]
impl Coordinator for NoneCoordinator {
    fn base(&self) -> &CoordinatorBase {
        &self.base
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        // Nothing to clean up in single-node mode
        Ok(())
    }

    async fn wait_converged(&self, _timeout: Duration) -> bool {
        // Always converged in single-node mode
        true
    }

    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        // Only this node is a member
        Ok(vec![MemberInfo {
            node_id: self.base.node_id.clone(),
            grpc_addr: self.base.grpc_addr.clone(),
            startup_time_ms: self.base.startup_time_ms,
            hostname: get_hostname(),
        }])
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
        let shard_map = self.base.shard_map.lock().await;

        // All shards owned by this node
        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_info in shard_map.shards() {
            shard_to_addr.insert(shard_info.id, self.base.grpc_addr.clone());
            shard_to_node.insert(shard_info.id, self.base.node_id.clone());
        }

        Ok(ShardOwnerMap {
            shard_map: shard_map.clone(),
            shard_to_addr,
            shard_to_node,
        })
    }
}

/// NoneCoordinator does not support split operations.
/// All methods return `NotSupported` errors.
#[async_trait]
impl SplitStorageBackend for NoneCoordinator {
    async fn load_split(
        &self,
        _parent_shard_id: &ShardId,
    ) -> Result<Option<SplitInProgress>, CoordinationError> {
        // No splits in single-node mode
        Ok(None)
    }

    async fn store_split(&self, _split: &SplitInProgress) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn delete_split(&self, _parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn update_shard_map_for_split(
        &self,
        _split: &SplitInProgress,
    ) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        // Nothing to reload in single-node mode
        Ok(())
    }

    async fn update_cleanup_status_in_shard_map(
        &self,
        _shard_id: ShardId,
        _status: SplitCleanupStatus,
    ) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
        // No splits in single-node mode
        Ok(vec![])
    }
}
