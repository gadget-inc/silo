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

use super::{CoordinationError, Coordinator, MemberInfo, ShardOwnerMap, get_hostname};

/// A no-op coordinator for single-node deployments.
///
/// In this mode, the single node is assumed to own all shards.
/// No distributed coordination is performed.
pub struct NoneCoordinator {
    node_id: String,
    grpc_addr: String,
    shard_map: ShardMap,
    startup_time_ms: Option<i64>,
    hostname: Option<String>,
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

        // In single-node mode, we own all shards immediately - open them all
        for shard_info in shard_map.shards() {
            if let Err(e) = factory.open(&shard_info.id).await {
                tracing::error!(shard_id = %shard_info.id, error = %e, "failed to open shard");
            }
        }

        let startup_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .ok();

        Self {
            node_id: node_id.into(),
            grpc_addr: grpc_addr.into(),
            shard_map,
            startup_time_ms,
            hostname: get_hostname(),
        }
    }
}

#[async_trait]
impl Coordinator for NoneCoordinator {
    async fn owned_shards(&self) -> Vec<ShardId> {
        // Single node owns all shards
        self.shard_map.shard_ids()
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
            node_id: self.node_id.clone(),
            grpc_addr: self.grpc_addr.clone(),
            startup_time_ms: self.startup_time_ms,
            hostname: self.hostname.clone(),
        }])
    }

    async fn get_shard_map(&self) -> Result<ShardMap, CoordinationError> {
        Ok(self.shard_map.clone())
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
        // All shards owned by this node
        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_info in self.shard_map.shards() {
            shard_to_addr.insert(shard_info.id, self.grpc_addr.clone());
            shard_to_node.insert(shard_info.id, self.node_id.clone());
        }

        Ok(ShardOwnerMap {
            shard_map: self.shard_map.clone(),
            shard_to_addr,
            shard_to_node,
        })
    }

    async fn num_shards(&self) -> usize {
        self.shard_map.len()
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn grpc_addr(&self) -> &str {
        &self.grpc_addr
    }
}
