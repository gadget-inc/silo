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
use tokio::sync::{Mutex, Notify, watch};
use tracing::debug;

use crate::factory::ShardFactory;
use crate::shard_range::{ShardId, ShardMap, ShardMapError};

pub mod etcd;
#[cfg(feature = "k8s")]
pub mod k8s;
#[cfg(feature = "k8s")]
pub mod k8s_backend;
pub mod none;
pub mod split;

// Re-export the split splitter types and split enums
pub use split::{ShardSplitter, SplitCleanupStatus, SplitPhase, SplitStorageBackend};

// Re-export the backends
pub use etcd::EtcdCoordinator;
#[cfg(feature = "k8s")]
pub use k8s::{K8sCoordinator, K8sCoordinatorConfig};
#[cfg(feature = "k8s")]
pub use k8s_backend::{
    ConfigMapWatchEvent, ConfigMapWatchStream, K8sBackend, KubeBackend, LeaseWatchEvent,
    LeaseWatchStream,
};
pub use none::NoneCoordinator;

/// Information about a cluster member
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemberInfo {
    pub node_id: String,
    pub grpc_addr: String,
    /// Unix timestamp in milliseconds when this node started
    #[serde(default)]
    pub startup_time_ms: Option<i64>,
    /// Hostname of the machine running this node
    #[serde(default)]
    pub hostname: Option<String>,
    /// Placement rings this node participates in.
    /// Empty means the node participates in the default ring only.
    #[serde(default)]
    pub placement_rings: Vec<String>,
}

/// Get the hostname of the current machine
pub fn get_hostname() -> Option<String> {
    hostname::get().ok().and_then(|h| h.into_string().ok())
}

/// Mapping of shard IDs to their owning node's gRPC address.
///
/// This struct combines the static shard map (which defines shard identities and ranges)
/// with dynamic ownership information (which nodes own which shards).
#[derive(Debug, Clone)]
pub struct ShardOwnerMap {
    /// The shard map defining all shards and their ranges
    pub shard_map: ShardMap,
    /// Maps shard_id -> grpc_addr of the owning node
    pub shard_to_addr: HashMap<ShardId, String>,
    /// Maps shard_id -> node_id of the owning node
    pub shard_to_node: HashMap<ShardId, String>,
}

impl ShardOwnerMap {
    /// Get the total number of shards in the cluster.
    pub fn num_shards(&self) -> usize {
        self.shard_map.len()
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shard_map.shard_ids()
    }

    /// Find the shard that owns a given tenant ID.
    pub fn shard_for_tenant(&self, tenant_id: &str) -> Option<ShardId> {
        self.shard_map.shard_for_tenant(tenant_id).map(|s| s.id)
    }

    /// Get the gRPC address for a shard owner.
    pub fn get_addr(&self, shard_id: &ShardId) -> Option<&String> {
        self.shard_to_addr.get(shard_id)
    }

    /// Get the node ID for a shard owner.
    pub fn get_node(&self, shard_id: &ShardId) -> Option<&String> {
        self.shard_to_node.get(shard_id)
    }
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
    #[error("shard not found: {0}")]
    ShardNotFound(ShardId),
    #[error("shard map error: {0}")]
    ShardMapError(#[from] ShardMapError),
    #[error("split already in progress for shard: {0}")]
    SplitAlreadyInProgress(ShardId),
    #[error("no split in progress for shard: {0}")]
    NoSplitInProgress(ShardId),
    #[error("shard is paused for split")]
    ShardPausedForSplit(ShardId),
    #[error("node does not own shard: {0}")]
    NotShardOwner(ShardId),
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
    pub shard_id: ShardId,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
}

impl ShardGuardContext {
    pub fn new(shard_id: ShardId, shutdown: watch::Receiver<bool>) -> Self {
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
/// Contains all the common state that all coordinators need, and is presented at coordinator.base()
#[derive(Clone)]
pub struct CoordinatorBase {
    pub node_id: String,
    pub grpc_addr: String,
    /// The shard map defining all shards and their ranges.
    /// This is loaded from the coordination backend or created during cluster init.
    pub shard_map: Arc<Mutex<ShardMap>>,
    pub owned: Arc<Mutex<HashSet<ShardId>>>,
    pub shutdown_tx: watch::Sender<bool>,
    pub shutdown_rx: watch::Receiver<bool>,
    pub factory: Arc<ShardFactory>,
    /// Unix timestamp in milliseconds when this node started
    pub startup_time_ms: Option<i64>,
    /// Placement rings this node participates in.
    /// Empty means the node participates in the default ring only.
    pub placement_rings: Vec<String>,
}

impl CoordinatorBase {
    /// Create a new coordinator base with the given configuration.
    pub fn new(
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        shard_map: ShardMap,
        factory: Arc<ShardFactory>,
        placement_rings: Vec<String>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let startup_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .ok();
        Self {
            node_id: node_id.into(),
            grpc_addr: grpc_addr.into(),
            shard_map: Arc::new(Mutex::new(shard_map)),
            owned: Arc::new(Mutex::new(HashSet::new())),
            shutdown_tx,
            shutdown_rx,
            factory,
            startup_time_ms,
            placement_rings,
        }
    }

    /// Get the current shard map.
    pub async fn get_shard_map(&self) -> ShardMap {
        self.shard_map.lock().await.clone()
    }

    /// Get the number of shards.
    pub async fn num_shards(&self) -> usize {
        self.shard_map.lock().await.len()
    }

    /// Get all shard IDs from the shard map.
    pub async fn shard_ids(&self) -> Vec<ShardId> {
        self.shard_map.lock().await.shard_ids()
    }

    /// Get owned shards sorted (shared by both backends).
    pub async fn owned_shards(&self) -> Vec<ShardId> {
        let guard = self.owned.lock().await;
        let mut v: Vec<ShardId> = guard.iter().copied().collect();
        v.sort_by_key(|id| id.to_string());
        v
    }

    /// Compute shard owner map from members (shared by both backends).
    ///
    /// Uses ring-aware selection to determine shard ownership based on
    /// each shard's placement ring and member ring participation.
    pub async fn compute_shard_owner_map(&self, members: &[MemberInfo]) -> ShardOwnerMap {
        let addr_map: HashMap<String, String> = members
            .iter()
            .map(|m| (m.node_id.clone(), m.grpc_addr.clone()))
            .collect();

        let shard_map = self.shard_map.lock().await.clone();
        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_info in shard_map.shards() {
            // Use ring-aware selection
            if let Some(owner_node) =
                select_owner_for_shard(&shard_info.id, shard_info.placement_ring(), members)
                && let Some(addr) = addr_map.get(&owner_node)
            {
                shard_to_addr.insert(shard_info.id, addr.clone());
                shard_to_node.insert(shard_info.id, owner_node);
            }
        }

        ShardOwnerMap {
            shard_map,
            shard_to_addr,
            shard_to_node,
        }
    }

    /// Wait for convergence (shared logic).
    ///
    /// The `get_members` closure is called to fetch the current member list,
    /// allowing backends to use their own membership fetching mechanism.
    ///
    /// Uses `tokio::time::Instant` for the timeout to ensure compatibility with
    /// turmoil's simulated time in deterministic simulation tests.
    pub async fn wait_converged<F, Fut>(&self, timeout: Duration, get_members: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<MemberInfo>, CoordinationError>>,
    {
        let start = tokio::time::Instant::now();
        while start.elapsed() < timeout {
            let members = match get_members().await {
                Ok(m) => m,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if members.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let shard_map = self.shard_map.lock().await;
            let shards: Vec<&crate::shard_range::ShardInfo> = shard_map.shards().iter().collect();
            let desired: HashSet<ShardId> =
                compute_desired_shards_for_node(&shards, &self.node_id, &members);
            drop(shard_map);
            let guard = self.owned.lock().await;
            if *guard == desired {
                return true;
            }
            drop(guard);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log diff on timeout
        if let Ok(members) = get_members().await {
            let shard_map = self.shard_map.lock().await;
            let shards: Vec<&crate::shard_range::ShardInfo> = shard_map.shards().iter().collect();
            let desired: HashSet<ShardId> =
                compute_desired_shards_for_node(&shards, &self.node_id, &members);
            drop(shard_map);
            let owned_now: HashSet<ShardId> = {
                let g = self.owned.lock().await;
                g.clone()
            };
            let missing: Vec<ShardId> = desired.difference(&owned_now).copied().collect();
            let extra: Vec<ShardId> = owned_now.difference(&desired).copied().collect();
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
/// This trait abstracts over different coordination mechanisms (etcd, K8S Leases, etc.) to allow Silo to run in different environments.
///
/// Implementations only need to provide `base()` and a few backend-specific methods. Common functionality is provided via default implementations that delegate to the base.
#[async_trait]
pub trait Coordinator: SplitStorageBackend + Send + Sync {
    /// Get the coordinator base containing shared state.
    /// This is the only required method for accessing common state.
    fn base(&self) -> &CoordinatorBase;

    /// Gracefully shutdown the coordinator, releasing all owned shards.
    async fn shutdown(&self) -> Result<(), CoordinationError>;

    /// Wait until this node's owned shards match the expected set based on current cluster membership, or until timeout.
    /// Returns true if converged, false if timed out.
    async fn wait_converged(&self, timeout: Duration) -> bool;

    /// Get all member information from the cluster.
    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError>;

    /// Compute a mapping of shard IDs to their owning node's address.
    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError>;

    // === Default implementations derived from base() ===

    /// Get the list of shard IDs currently owned by this node.
    async fn owned_shards(&self) -> Vec<ShardId> {
        self.base().owned_shards().await
    }

    /// Get the current shard map defining all shards and their ranges.
    async fn get_shard_map(&self) -> Result<ShardMap, CoordinationError> {
        Ok(self.base().get_shard_map().await)
    }

    /// Get the number of shards in the cluster.
    async fn num_shards(&self) -> usize {
        self.base().num_shards().await
    }

    /// Get this node's ID.
    fn node_id(&self) -> &str {
        &self.base().node_id
    }

    /// Get this node's gRPC address.
    fn grpc_addr(&self) -> &str {
        &self.base().grpc_addr
    }

    /// Check if a shard is currently paused for split.
    ///
    /// Returns true if the shard has an in-progress split in a traffic-pausing phase.
    /// Callers should return retryable errors when this returns true.
    async fn is_shard_paused(&self, shard_id: ShardId) -> bool {
        match self.load_split(&shard_id).await {
            Ok(Some(split)) => split.phase.traffic_paused(),
            _ => false,
        }
    }

    /// Update a shard's placement ring.
    ///
    /// This changes which placement ring the shard belongs to, which affects which nodes are eligible to own it. After changing the ring, the shard will be handed off to a node that participates in the new ring.
    ///
    /// Returns the previous and current placement ring values.
    async fn update_shard_placement_ring(
        &self,
        shard_id: &ShardId,
        ring: Option<&str>,
    ) -> Result<(Option<String>, Option<String>), CoordinationError>;
}

/// Dynamically create a coordinator based on configuration.
///
/// The coordinator will manage shard ownership and automatically open/close shards in the factory as ownership changes.
///
/// The `initial_shard_count` is only used when bootstrapping a new cluster. For existing clusters, the shard map is loaded from the coordination backend.
pub async fn create_coordinator(
    config: &crate::settings::CoordinationConfig,
    node_id: impl Into<String>,
    grpc_addr: impl Into<String>,
    initial_shard_count: u32,
    factory: Arc<ShardFactory>,
) -> Result<(Arc<dyn Coordinator>, Option<tokio::task::JoinHandle<()>>), CoordinationError> {
    let node_id = node_id.into();
    let grpc_addr = grpc_addr.into();

    match &config.backend {
        crate::settings::CoordinationBackend::None => {
            let coord = NoneCoordinator::new(
                node_id,
                grpc_addr,
                initial_shard_count,
                factory,
                config.placement_rings.clone(),
            )
            .await;
            Ok((Arc::new(coord), None))
        }
        crate::settings::CoordinationBackend::Etcd => {
            let (coord, handle) = EtcdCoordinator::start(
                &config.etcd_endpoints,
                &config.cluster_prefix,
                node_id,
                grpc_addr,
                initial_shard_count,
                config.lease_ttl_secs,
                factory,
                config.placement_rings.clone(),
            )
            .await?;
            Ok((Arc::new(coord), Some(handle)))
        }
        #[cfg(feature = "k8s")]
        crate::settings::CoordinationBackend::K8s => {
            let k8s_config = K8sCoordinatorConfig {
                namespace: config.k8s_namespace.clone(),
                cluster_prefix: config.cluster_prefix.clone(),
                node_id,
                grpc_addr,
                initial_shard_count,
                lease_duration_secs: config.lease_ttl_secs,
                placement_rings: config.placement_rings.clone(),
            };
            let (coord, handle) = K8sCoordinator::start(k8s_config, factory).await?;
            Ok((Arc::new(coord), Some(handle)))
        }
        #[cfg(not(feature = "k8s"))]
        crate::settings::CoordinationBackend::K8s => Err(CoordinationError::NotSupported),
    }
}

// Helper functions for rendezvous hashing (shared across backends)

/// Check if a member participates in a given placement ring.
///
/// Rules:
/// - If shard_ring is None (default ring), member participates if:
///   - member.placement_rings is empty (default behavior), OR
///   - member.placement_rings contains "default"
/// - If shard_ring is Some(ring), member participates if:
///   - member.placement_rings contains that ring
pub fn member_in_ring(member: &MemberInfo, shard_ring: Option<&str>) -> bool {
    match shard_ring {
        None => {
            // Default ring: members with empty rings or explicit "default"
            member.placement_rings.is_empty()
                || member.placement_rings.iter().any(|r| r == "default")
        }
        Some(ring) => {
            // Named ring: must explicitly contain the ring
            member.placement_rings.iter().any(|r| r == ring)
        }
    }
}

/// Deterministically select the owner node for a shard using rendezvous hashing, filtering by placement ring.
///
/// Uses the shard's UUID as input to the hash function, ensuring consistent distribution regardless of when shards were created.
///
/// Only members that participate in the shard's placement ring are considered.
/// If no members participate in the ring, returns None.
pub fn select_owner_for_shard(
    shard_id: &ShardId,
    shard_ring: Option<&str>,
    members: &[MemberInfo],
) -> Option<String> {
    // Filter to members that participate in this ring
    let eligible: Vec<&String> = members
        .iter()
        .filter(|m| member_in_ring(m, shard_ring))
        .map(|m| &m.node_id)
        .collect();

    if eligible.is_empty() {
        return None;
    }

    let mut best: Option<(u64, &String)> = None;
    for m in &eligible {
        let member_hash = fnv1a64(m.as_bytes());
        // Use the UUID bytes for consistent hashing
        let shard_hash = fnv1a64(shard_id.as_uuid().as_bytes());
        let score = mix64(member_hash ^ shard_hash);
        if let Some((cur, _)) = best {
            if score > cur {
                best = Some((score, m));
            }
        } else {
            best = Some((score, m));
        }
    }
    best.map(|(_, m)| (*m).clone())
}

/// Compute the desired set of shard ids for a node given current membership and considering placement rings.
///
/// A shard is desired by a node if:
/// 1. The node participates in the shard's placement ring
/// 2. The node is selected as owner by rendezvous hashing among eligible members
pub fn compute_desired_shards_for_node(
    shards: &[&crate::shard_range::ShardInfo],
    node_id: &str,
    members: &[MemberInfo],
) -> HashSet<ShardId> {
    let mut desired: HashSet<ShardId> = HashSet::new();
    for shard in shards {
        if let Some(owner) = select_owner_for_shard(&shard.id, shard.placement_ring(), members)
            && owner == node_id
        {
            desired.insert(shard.id);
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
    use crate::shard_range::ShardId;

    pub fn members_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/members/", cluster_prefix)
    }
    pub fn member_key(cluster_prefix: &str, node_id: &str) -> String {
        format!("{}{}", members_prefix(cluster_prefix), node_id)
    }
    pub fn shards_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/shards/", cluster_prefix)
    }
    /// Key for the shard map that defines all shards and their ranges.
    pub fn shard_map_key(cluster_prefix: &str) -> String {
        format!("{}/coord/shard_map", cluster_prefix)
    }
    /// Key for shard ownership lock (uses UUID).
    pub fn shard_owner_key(cluster_prefix: &str, shard_id: &ShardId) -> String {
        format!("{}{}/owner", shards_prefix(cluster_prefix), shard_id)
    }

    /// Prefix for all split operations.
    pub fn splits_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/splits/", cluster_prefix)
    }

    /// Key for tracking an in-progress split operation.
    /// Stored as JSON-serialized SplitInProgress.
    pub fn split_key(cluster_prefix: &str, parent_shard_id: &ShardId) -> String {
        format!("{}{}", splits_prefix(cluster_prefix), parent_shard_id)
    }

    // K8S-specific naming (Lease object names must be DNS-compatible)
    pub fn k8s_member_lease_name(cluster_prefix: &str, node_id: &str) -> String {
        format!("{}-member-{}", cluster_prefix, node_id)
    }
    /// K8S lease name for shard ownership (uses UUID, which is DNS-compatible).
    pub fn k8s_shard_lease_name(cluster_prefix: &str, shard_id: &ShardId) -> String {
        format!("{}-shard-{}", cluster_prefix, shard_id)
    }
    /// K8S ConfigMap name for split state (uses parent shard UUID, which is DNS-compatible).
    pub fn k8s_split_configmap_name(cluster_prefix: &str, parent_shard_id: &ShardId) -> String {
        format!("{}-split-{}", cluster_prefix, parent_shard_id)
    }
}
