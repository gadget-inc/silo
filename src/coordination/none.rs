//! No-op coordination backend for single-node local development.
//!
//! This backend assumes a single node owns all shards. No actual coordination
//! is performed - useful for local development and testing without etcd or K8S.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::factory::ShardFactory;

use super::{CoordinationError, Coordinator, MemberInfo, ShardOwnerMap};

/// A no-op coordinator for single-node deployments.
///
/// In this mode, the single node is assumed to own all shards.
/// No distributed coordination is performed.
pub struct NoneCoordinator {
    node_id: String,
    grpc_addr: String,
    num_shards: u32,
}

impl NoneCoordinator {
    /// Create a new single-node coordinator.
    ///
    /// Opens all shards immediately since this node owns everything.
    pub async fn new(
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
        factory: Arc<ShardFactory>,
    ) -> Self {
        // In single-node mode, we own all shards immediately - open them all
        for shard_id in 0..num_shards {
            if let Err(e) = factory.open(shard_id as usize).await {
                tracing::error!(shard_id, error = %e, "failed to open shard");
            }
        }

        Self {
            node_id: node_id.into(),
            grpc_addr: grpc_addr.into(),
            num_shards,
        }
    }
}

#[async_trait]
impl Coordinator for NoneCoordinator {
    async fn owned_shards(&self) -> Vec<u32> {
        // Single node owns all shards
        (0..self.num_shards).collect()
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
        }])
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
        // All shards owned by this node
        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_id in 0..self.num_shards {
            shard_to_addr.insert(shard_id, self.grpc_addr.clone());
            shard_to_node.insert(shard_id, self.node_id.clone());
        }

        Ok(ShardOwnerMap {
            num_shards: self.num_shards,
            shard_to_addr,
            shard_to_node,
        })
    }

    fn num_shards(&self) -> u32 {
        self.num_shards
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn grpc_addr(&self) -> &str {
        &self.grpc_addr
    }
}
