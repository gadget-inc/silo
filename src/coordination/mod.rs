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
use tokio::sync::{watch, Mutex, Notify};
use tracing::debug;

use crate::factory::ShardFactory;

pub mod etcd;
#[cfg(feature = "k8s")]
pub mod k8s;
pub mod none;
pub mod placement;

// Re-export the backends
pub use etcd::EtcdCoordinator;
#[cfg(feature = "k8s")]
pub use k8s::K8sCoordinator;
pub use none::NoneCoordinator;

// Re-export placement types
pub use placement::{
    compute_desired_shards_with_overrides, PlacementOverride, PlacementOverrides,
};

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

/// Phase of a shard guard's lifecycle.
///
/// Shared by all coordination backends (etcd, k8s).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardPhase {
    /// Not attempting to own this shard
    Idle,
    /// Actively trying to acquire ownership
    Acquiring,
    /// Successfully holding ownership
    Held,
    /// Releasing ownership (with delay for cancellation)
    Releasing,
    /// Shutdown initiated, releasing resources
    ShuttingDown,
    /// Fully shut down
    ShutDown,
}

impl std::fmt::Display for ShardPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardPhase::Idle => write!(f, "Idle"),
            ShardPhase::Acquiring => write!(f, "Acquiring"),
            ShardPhase::Held => write!(f, "Held"),
            ShardPhase::Releasing => write!(f, "Releasing"),
            ShardPhase::ShuttingDown => write!(f, "ShuttingDown"),
            ShardPhase::ShutDown => write!(f, "ShutDown"),
        }
    }
}

/// Generic state for a shard guard.
///
/// The ownership token type varies by backend:
/// - etcd: `Vec<u8>` (lock key)
/// - k8s: `String` (resourceVersion)
pub struct ShardGuardState<T> {
    pub desired: bool,
    pub phase: ShardPhase,
    pub ownership_token: Option<T>,
}

impl<T> ShardGuardState<T> {
    /// Create a new shard guard state in idle phase.
    pub fn new() -> Self {
        Self {
            desired: false,
            phase: ShardPhase::Idle,
            ownership_token: None,
        }
    }

    /// Check if we have an ownership token (i.e., we believe we own the shard).
    pub fn has_token(&self) -> bool {
        self.ownership_token.is_some()
    }

    /// Compute the next phase based on current state and desired ownership.
    ///
    /// Returns the new phase if a transition should occur, or None if no change.
    pub fn compute_transition(&self) -> Option<ShardPhase> {
        match (self.phase, self.desired, self.has_token()) {
            // Terminal states - no transitions
            (ShardPhase::ShutDown, _, _) => None,
            (ShardPhase::ShuttingDown, _, _) => None,
            // Want to own but don't - start acquiring
            (ShardPhase::Idle, true, false) => Some(ShardPhase::Acquiring),
            // Don't want to own but do - start releasing
            (ShardPhase::Held, false, true) => Some(ShardPhase::Releasing),
            // No transition needed
            _ => None,
        }
    }

    /// Apply a phase transition if one is needed.
    /// Returns true if a transition occurred.
    pub fn maybe_transition(&mut self) -> bool {
        if let Some(new_phase) = self.compute_transition() {
            self.phase = new_phase;
            true
        } else {
            false
        }
    }
}

impl<T> Default for ShardGuardState<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Common context for a shard guard that's shared across backends.
pub struct ShardGuardContext {
    pub shard_id: u32,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
}

impl ShardGuardContext {
    pub fn new(shard_id: u32, shutdown: watch::Receiver<bool>) -> Self {
        Self {
            shard_id,
            notify: Notify::new(),
            shutdown,
        }
    }

    /// Check if shutdown has been signaled.
    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    /// Wait for either a notification or shutdown signal.
    pub async fn wait_for_change(&self) {
        let mut shutdown_rx = self.shutdown.clone();
        tokio::select! {
            _ = self.notify.notified() => {}
            _ = shutdown_rx.changed() => {}
        }
    }
}

/// Shared state and helpers for coordinator implementations.
///
/// This struct contains the common state that both etcd and k8s coordinators
/// need, and provides shared implementations for common operations.
pub struct CoordinatorBase {
    pub node_id: String,
    pub grpc_addr: String,
    pub num_shards: u32,
    pub owned: Arc<Mutex<HashSet<u32>>>,
    pub shutdown_tx: watch::Sender<bool>,
    pub shutdown_rx: watch::Receiver<bool>,
    pub factory: Arc<ShardFactory>,
    /// Whether this node is currently the placement engine leader
    pub leader_tx: watch::Sender<bool>,
    pub leader_rx: watch::Receiver<bool>,
}

impl CoordinatorBase {
    /// Create a new coordinator base with the given configuration.
    pub fn new(
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
        factory: Arc<ShardFactory>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (leader_tx, leader_rx) = watch::channel(false);
        Self {
            node_id: node_id.into(),
            grpc_addr: grpc_addr.into(),
            num_shards,
            owned: Arc::new(Mutex::new(HashSet::new())),
            shutdown_tx,
            shutdown_rx,
            factory,
            leader_tx,
            leader_rx,
        }
    }

    /// Check if this node is currently the leader.
    pub fn is_leader(&self) -> bool {
        *self.leader_rx.borrow()
    }

    /// Get a watch receiver for leadership changes.
    pub fn leadership_watch(&self) -> watch::Receiver<bool> {
        self.leader_rx.clone()
    }

    /// Set this node's leadership status.
    pub fn set_leader(&self, is_leader: bool) {
        let _ = self.leader_tx.send(is_leader);
    }

    /// Get owned shards sorted (shared by both backends).
    pub async fn owned_shards(&self) -> Vec<u32> {
        let guard = self.owned.lock().await;
        let mut v: Vec<u32> = guard.iter().copied().collect();
        v.sort_unstable();
        v
    }

    /// Compute shard owner map from members (shared by both backends).
    pub fn compute_shard_owner_map(&self, members: &[MemberInfo]) -> ShardOwnerMap {
        let member_ids: Vec<String> = members.iter().map(|m| m.node_id.clone()).collect();

        let addr_map: HashMap<String, String> = members
            .iter()
            .map(|m| (m.node_id.clone(), m.grpc_addr.clone()))
            .collect();

        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_id in 0..self.num_shards {
            if let Some(owner_node) = select_owner_for_shard(shard_id, &member_ids) {
                if let Some(addr) = addr_map.get(&owner_node) {
                    shard_to_addr.insert(shard_id, addr.clone());
                    shard_to_node.insert(shard_id, owner_node);
                }
            }
        }

        ShardOwnerMap {
            num_shards: self.num_shards,
            shard_to_addr,
            shard_to_node,
        }
    }

    /// Wait for convergence (shared logic).
    ///
    /// The `get_member_ids` closure is called to fetch the current member list,
    /// allowing backends to use their own membership fetching mechanism.
    pub async fn wait_converged<F, Fut>(&self, timeout: Duration, get_member_ids: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<String>, CoordinationError>>,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let member_ids = match get_member_ids().await {
                Ok(m) => m,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if member_ids.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let desired: HashSet<u32> =
                compute_desired_shards_for_node(self.num_shards, &self.node_id, &member_ids);
            let guard = self.owned.lock().await;
            if *guard == desired {
                return true;
            }
            drop(guard);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log diff on timeout
        if let Ok(member_ids) = get_member_ids().await {
            let desired: HashSet<u32> =
                compute_desired_shards_for_node(self.num_shards, &self.node_id, &member_ids);
            let owned_now: HashSet<u32> = {
                let g = self.owned.lock().await;
                g.clone()
            };
            let missing: Vec<u32> = desired.difference(&owned_now).copied().collect();
            let extra: Vec<u32> = owned_now.difference(&desired).copied().collect();
            debug!(node_id = %self.node_id, missing = ?missing, extra = ?extra, "wait_converged: timed out");
        }
        false
    }

    /// Signal shutdown to all components.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
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

    // === Leader Election for Placement Engine ===

    /// Check if this node is currently the placement engine leader.
    ///
    /// The leader is responsible for making shard placement decisions and
    /// writing placement overrides. Only one node should be the leader at a time.
    fn is_leader(&self) -> bool;

    /// Subscribe to leadership changes.
    ///
    /// Returns a watch receiver that will be notified when leadership status changes.
    /// The boolean value is true when this node becomes the leader, false when it loses leadership.
    fn leadership_watch(&self) -> watch::Receiver<bool>;
}

/// Dynamically create a coordinator based on configuration.
///
/// The coordinator will manage shard ownership and automatically open/close
/// shards in the factory as ownership changes.
pub async fn create_coordinator(
    config: &crate::settings::CoordinationConfig,
    node_id: impl Into<String>,
    grpc_addr: impl Into<String>,
    num_shards: u32,
    factory: Arc<ShardFactory>,
) -> Result<(Arc<dyn Coordinator>, Option<tokio::task::JoinHandle<()>>), CoordinationError> {
    let node_id = node_id.into();
    let grpc_addr = grpc_addr.into();

    match &config.backend {
        crate::settings::CoordinationBackend::None => {
            let coord = NoneCoordinator::new(node_id, grpc_addr, num_shards, factory).await;
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
                factory,
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
                factory,
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

    /// Key for leader election (used by placement engine)
    pub fn leader_election_key(cluster_prefix: &str) -> String {
        format!("{}/coord/leader", cluster_prefix)
    }

    /// Key prefix for placement overrides
    pub fn placements_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/placements/", cluster_prefix)
    }

    /// Key for a specific placement override
    pub fn placement_key(cluster_prefix: &str, shard_id: u32) -> String {
        format!("{}{}", placements_prefix(cluster_prefix), shard_id)
    }

    // K8S-specific naming (Lease object names must be DNS-compatible)
    pub fn k8s_member_lease_name(cluster_prefix: &str, node_id: &str) -> String {
        format!("{}-member-{}", cluster_prefix, node_id)
    }
    pub fn k8s_shard_lease_name(cluster_prefix: &str, shard_id: u32) -> String {
        format!("{}-shard-{}", cluster_prefix, shard_id)
    }
    pub fn k8s_leader_lease_name(cluster_prefix: &str) -> String {
        format!("{}-placement-leader", cluster_prefix)
    }
}
