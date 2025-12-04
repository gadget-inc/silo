//! Coordination backends for distributed shard ownership.
//!
//! This module provides pluggable backends for coordinating shard ownership
//! across multiple Silo nodes. Available backends:
//!
//! - `none`: Single-node mode, no coordination (for local development)
//! - `etcd`: Distributed coordination using etcd leases and locks
//! - `k8s`: Distributed coordination using Kubernetes Lease objects

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub mod etcd;
#[cfg(feature = "k8s")]
pub mod k8s;
pub mod none;

// Re-export the backends
pub use etcd::EtcdCoordinator;
#[cfg(feature = "k8s")]
pub use k8s::K8sCoordinator;
pub use none::NoneCoordinator;

/// Information about a cluster member
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemberInfo {
    pub node_id: String,
    pub grpc_addr: String,
}

/// Mapping of shard IDs to their owning node's gRPC address
#[derive(Debug, Clone)]
pub struct ShardOwnerMap {
    /// Total number of shards in the cluster
    pub num_shards: u32,
    /// Maps shard_id -> grpc_addr of the owning node
    pub shard_to_addr: HashMap<u32, String>,
    /// Maps shard_id -> node_id of the owning node
    pub shard_to_node: HashMap<u32, String>,
}

/// Error type for coordination operations
#[derive(Debug, thiserror::Error)]
pub enum CoordinationError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("backend error: {0}")]
    BackendError(String),
    #[error("shutdown in progress")]
    ShuttingDown,
    #[error("not supported by this backend")]
    NotSupported,
}

/// Trait for coordination backends.
///
/// This trait abstracts over different coordination mechanisms (etcd, K8S Leases, etc.)
/// to allow Silo to run in different environments.
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Get the list of shard IDs currently owned by this node.
    async fn owned_shards(&self) -> Vec<u32>;

    /// Gracefully shutdown the coordinator, releasing all owned shards.
    async fn shutdown(&self) -> Result<(), CoordinationError>;

    /// Wait until this node's owned shards match the expected set based on
    /// current cluster membership, or until timeout.
    /// Returns true if converged, false if timed out.
    async fn wait_converged(&self, timeout: Duration) -> bool;

    /// Get all member information from the cluster.
    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError>;

    /// Compute a mapping of shard IDs to their owning node's address.
    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError>;

    /// Get the number of shards in the cluster.
    fn num_shards(&self) -> u32;

    /// Get this node's ID.
    fn node_id(&self) -> &str;

    /// Get this node's gRPC address.
    fn grpc_addr(&self) -> &str;
}

/// Dynamically create a coordinator based on configuration.
pub async fn create_coordinator(
    config: &crate::settings::CoordinationConfig,
    node_id: impl Into<String>,
    grpc_addr: impl Into<String>,
    num_shards: u32,
) -> Result<(Arc<dyn Coordinator>, Option<tokio::task::JoinHandle<()>>), CoordinationError> {
    let node_id = node_id.into();
    let grpc_addr = grpc_addr.into();

    match &config.backend {
        crate::settings::CoordinationBackend::None => {
            let coord = NoneCoordinator::new(node_id, grpc_addr, num_shards);
            Ok((Arc::new(coord), None))
        }
        crate::settings::CoordinationBackend::Etcd => {
            let (coord, handle) = EtcdCoordinator::start(
                &config.etcd_endpoints,
                &config.cluster_prefix,
                node_id,
                grpc_addr,
                num_shards,
                config.lease_ttl_secs,
            )
            .await?;
            Ok((Arc::new(coord), Some(handle)))
        }
        #[cfg(feature = "k8s")]
        crate::settings::CoordinationBackend::K8s => {
            let (coord, handle) = K8sCoordinator::start(
                &config.k8s_namespace,
                &config.cluster_prefix,
                node_id,
                grpc_addr,
                num_shards,
                config.lease_ttl_secs,
            )
            .await?;
            Ok((Arc::new(coord), Some(handle)))
        }
        #[cfg(not(feature = "k8s"))]
        crate::settings::CoordinationBackend::K8s => Err(CoordinationError::NotSupported),
    }
}

// Helper functions for rendezvous hashing (shared across backends)

/// Deterministically select the owner node for a shard using rendezvous hashing.
pub fn select_owner_for_shard(shard_id: u32, member_ids: &[String]) -> Option<String> {
    if member_ids.is_empty() {
        return None;
    }
    let mut best: Option<(u64, &String)> = None;
    for m in member_ids {
        let member_hash = fnv1a64(m.as_bytes());
        let shard_hash = (shard_id as u64).wrapping_mul(0x9e3779b97f4a7c15);
        let score = mix64(member_hash ^ shard_hash);
        if let Some((cur, _)) = best {
            if score > cur {
                best = Some((score, m));
            }
        } else {
            best = Some((score, m));
        }
    }
    best.map(|(_, m)| m.clone())
}

/// Compute the desired set of shard ids for a node given current membership.
pub fn compute_desired_shards_for_node(
    num_shards: u32,
    node_id: &str,
    member_ids: &[String],
) -> HashSet<u32> {
    let mut desired: HashSet<u32> = HashSet::new();
    for s in 0..num_shards {
        if let Some(owner) = select_owner_for_shard(s, member_ids) {
            if owner == node_id {
                desired.insert(s);
            }
        }
    }
    desired
}

fn fnv1a64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for b in data {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Helpers to build key paths used for coordination (shared naming convention).
pub mod keys {
    pub fn members_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/members/", cluster_prefix)
    }
    pub fn member_key(cluster_prefix: &str, node_id: &str) -> String {
        format!("{}{}", members_prefix(cluster_prefix), node_id)
    }
    pub fn shards_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/shards/", cluster_prefix)
    }
    pub fn shard_owner_key(cluster_prefix: &str, shard_id: u32) -> String {
        format!("{}{}/owner", shards_prefix(cluster_prefix), shard_id)
    }

    // K8S-specific naming (Lease object names must be DNS-compatible)
    pub fn k8s_member_lease_name(cluster_prefix: &str, node_id: &str) -> String {
        format!("{}-member-{}", cluster_prefix, node_id)
    }
    pub fn k8s_shard_lease_name(cluster_prefix: &str, shard_id: u32) -> String {
        format!("{}-shard-{}", cluster_prefix, shard_id)
    }
}
