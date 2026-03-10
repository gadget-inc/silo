//! Cluster-aware gRPC client for connecting to a silo cluster.
//!
//! Handles topology discovery, shard routing, connection pooling, and automatic
//! retry on wrong-shard errors (NOT_FOUND with redirect metadata or
//! FailedPrecondition for stale tenant routing).
//!
//! This client is designed for external consumers of the silo cluster (benchmarks,
//! workers, other services) rather than internal node-to-node communication.
//! Uses the same [`ClientConfig`] and connection infrastructure as the internal
//! [`ClusterClient`](crate::cluster_client::ClusterClient).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::cluster_client::{
    ClientConfig, InterceptedSiloClient, create_silo_client_lazy, ensure_http_scheme,
};
use crate::pb::GetClusterInfoRequest;
use crate::server::SHARD_OWNER_ADDR_METADATA_KEY;

/// Information about a shard owner in the cluster.
#[derive(Debug, Clone)]
pub struct ShardOwnerInfo {
    pub shard_id: String,
    pub grpc_addr: String,
    pub node_id: String,
    pub range_start: String,
    pub range_end: String,
}

/// Cluster topology snapshot.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    pub num_shards: u32,
    pub shard_owners: Vec<ShardOwnerInfo>,
    pub this_node_id: String,
    pub this_grpc_addr: String,
    /// Per-shard address overrides from redirect metadata. These take precedence
    /// over the topology's address when a server tells us a shard moved.
    shard_addr_overrides: std::collections::HashMap<String, String>,
}

impl ClusterTopology {
    /// Find the shard that owns the given tenant ID based on shard ranges.
    ///
    /// Hashes the tenant ID with the same algorithm used by the server
    /// and matches against hash-space range boundaries.
    pub fn shard_for_tenant(&self, tenant_id: &str) -> Option<&ShardOwnerInfo> {
        let hashed = crate::shard_range::hash_tenant(tenant_id);
        self.shard_owners.iter().find(|owner| {
            let after_start =
                owner.range_start.is_empty() || hashed.as_str() >= owner.range_start.as_str();
            let before_end =
                owner.range_end.is_empty() || hashed.as_str() < owner.range_end.as_str();
            after_start && before_end
        })
    }

    /// Get the effective address for a shard, checking overrides first.
    pub fn address_for_shard(&self, shard_id: &str) -> Option<&str> {
        if let Some(addr) = self.shard_addr_overrides.get(shard_id) {
            return Some(addr.as_str());
        }
        self.shard_owners
            .iter()
            .find(|owner| owner.shard_id == shard_id)
            .map(|owner| owner.grpc_addr.as_str())
    }

    /// Get a random shard ID.
    pub fn random_shard(&self) -> Option<&str> {
        if self.shard_owners.is_empty() {
            return None;
        }
        let idx = rand::random_range(0..self.shard_owners.len());
        Some(&self.shard_owners[idx].shard_id)
    }

    /// Get all unique server addresses (including overrides).
    pub fn all_addresses(&self) -> Vec<String> {
        let mut addrs: Vec<String> = self
            .shard_owners
            .iter()
            .map(|owner| owner.grpc_addr.clone())
            .chain(self.shard_addr_overrides.values().cloned())
            .collect();
        addrs.sort();
        addrs.dedup();
        addrs
    }
}

/// Result of classifying a gRPC error for retry purposes.
#[derive(Debug)]
pub enum ErrorAction {
    /// Server returned NOT_FOUND with a redirect address for the shard.
    Redirect { shard_id: String, new_addr: String },
    /// Server returned FailedPrecondition — tenant not in shard range. Need full topology refresh.
    RefreshTopology,
    /// Server returned UNAVAILABLE — shard is temporarily unavailable (split in progress,
    /// acquisition pending). Caller should retry with backoff.
    RetryWithBackoff,
    /// Not a routing error — don't retry.
    NotRoutingError,
}

/// Classify a tonic error to determine what retry action to take.
pub fn classify_error(status: &tonic::Status, shard_id: &str) -> ErrorAction {
    match status.code() {
        tonic::Code::NotFound => {
            // Check for redirect metadata
            let metadata = status.metadata();
            if let Some(addr) = metadata.get(SHARD_OWNER_ADDR_METADATA_KEY)
                && let Ok(addr_str) = addr.to_str()
            {
                return ErrorAction::Redirect {
                    shard_id: shard_id.to_string(),
                    new_addr: addr_str.to_string(),
                };
            }
            // NOT_FOUND without redirect — could be a deleted shard, refresh topology
            ErrorAction::RefreshTopology
        }
        tonic::Code::FailedPrecondition => {
            // "tenant X is not within shard Y range Z; refresh topology and retry"
            ErrorAction::RefreshTopology
        }
        tonic::Code::Unavailable => {
            // Shard is temporarily unavailable: split in progress or acquisition pending.
            // Retry with backoff — the shard will be ready soon.
            ErrorAction::RetryWithBackoff
        }
        _ => ErrorAction::NotRoutingError,
    }
}

/// Cluster-aware client that handles topology changes, redirects, and connection pooling.
///
/// Uses the same [`ClientConfig`] and [`create_silo_client_lazy`] connection infrastructure
/// as the internal [`ClusterClient`](crate::cluster_client::ClusterClient), giving it
/// proper connect timeouts, keepalive, and auth token support.
///
/// Uses `std::sync::RwLock` for the topology since the critical section is tiny
/// (just reading/cloning small strings) and writes are extremely rare. This avoids
/// the overhead of `tokio::sync::RwLock` which is designed for holding locks across
/// await points.
///
/// Topology refreshes are coalesced: when multiple concurrent tasks hit routing errors
/// simultaneously, only one `GetClusterInfo` RPC is made and the others wait for its
/// result.
pub struct RoutingClient {
    topology: RwLock<ClusterTopology>,
    connections: DashMap<String, InterceptedSiloClient>,
    /// The original seed addresses used to discover the cluster. These are
    /// immutable after construction and may include load balancer addresses
    /// not present in topology responses. Combined with
    /// `topology.all_addresses()` to form the full set of refresh candidates.
    seed_addresses: Vec<String>,
    /// Client configuration for timeouts and connection behavior.
    config: ClientConfig,
    /// Mutex to coalesce concurrent topology refreshes. Only one refresh runs at a
    /// time; other callers wait for the in-flight refresh to complete.
    refresh_mutex: Mutex<()>,
    /// Monotonic counter incremented after every completed refresh attempt.
    refresh_generation: AtomicU64,
    /// Result of the most recent refresh attempt, used by waiters to reuse the
    /// in-flight refresh result instead of re-running discovery.
    last_refresh_succeeded: AtomicBool,
}

impl RoutingClient {
    /// Connect to the cluster and discover its topology using default configuration.
    pub async fn discover(address: &str) -> anyhow::Result<Arc<Self>> {
        Self::discover_with_config(address, ClientConfig::default()).await
    }

    /// Connect to the cluster and discover its topology with custom configuration.
    pub async fn discover_with_config(
        address: &str,
        config: ClientConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let topology = Self::fetch_topology(address, &config).await?;

        let seed_addresses = vec![ensure_http_scheme(address)];

        let client = Arc::new(Self {
            topology: RwLock::new(topology),
            connections: DashMap::new(),
            seed_addresses,
            config,
            refresh_mutex: Mutex::new(()),
            refresh_generation: AtomicU64::new(0),
            last_refresh_succeeded: AtomicBool::new(false),
        });

        // Pre-populate the connection pool for all known servers. Since connections
        // are lazy, this doesn't establish TCP — it just creates configured endpoints
        // so the first RPC to each server doesn't need to build them.
        let addrs = client.all_addresses();
        for addr in &addrs {
            if let Err(e) = client.get_or_connect(addr) {
                warn!(address = %addr, error = %e, "Failed to create endpoint");
            }
        }

        Ok(client)
    }

    /// Fetch topology from a specific server address.
    async fn fetch_topology(
        address: &str,
        config: &ClientConfig,
    ) -> anyhow::Result<ClusterTopology> {
        let mut client = create_silo_client_lazy(address, config)?;

        let response = client
            .get_cluster_info(GetClusterInfoRequest {})
            .await?
            .into_inner();

        let shard_owners: Vec<ShardOwnerInfo> = response
            .shard_owners
            .into_iter()
            .map(|owner| ShardOwnerInfo {
                shard_id: owner.shard_id,
                grpc_addr: owner.grpc_addr,
                node_id: owner.node_id,
                range_start: owner.range_start,
                range_end: owner.range_end,
            })
            .collect();

        Ok(ClusterTopology {
            num_shards: response.num_shards,
            shard_owners,
            this_node_id: response.this_node_id,
            this_grpc_addr: response.this_grpc_addr,
            shard_addr_overrides: std::collections::HashMap::new(),
        })
    }

    /// Get a client and shard ID for the given tenant.
    /// Returns (client, shard_id).
    pub fn client_for_tenant(
        &self,
        tenant: &str,
    ) -> anyhow::Result<(InterceptedSiloClient, String)> {
        let (shard_id, addr) = {
            let topo = self.topology.read().expect("topology lock poisoned");
            let owner = topo
                .shard_for_tenant(tenant)
                .ok_or_else(|| anyhow::anyhow!("no shard found for tenant '{}'", tenant))?;
            let shard_id = owner.shard_id.clone();
            let addr = topo
                .address_for_shard(&shard_id)
                .unwrap_or(&owner.grpc_addr)
                .to_string();
            (shard_id, addr)
        };

        let client = self.get_or_connect(&addr)?;
        Ok((client, shard_id))
    }

    /// Get a client for a random shard (used by workers).
    /// Returns (client, shard_id).
    pub fn client_for_random_shard(&self) -> anyhow::Result<(InterceptedSiloClient, String)> {
        let (shard_id, addr) = {
            let topo = self.topology.read().expect("topology lock poisoned");
            let shard_id = topo
                .random_shard()
                .ok_or_else(|| anyhow::anyhow!("no shards available in topology"))?
                .to_string();
            let addr = topo
                .address_for_shard(&shard_id)
                .ok_or_else(|| anyhow::anyhow!("no address for shard '{}'", shard_id))?
                .to_string();
            (shard_id, addr)
        };

        let client = self.get_or_connect(&addr)?;
        Ok((client, shard_id))
    }

    /// Get a client for a specific shard (used for report_outcome).
    pub fn client_for_shard(&self, shard_id: &str) -> anyhow::Result<InterceptedSiloClient> {
        let addr = {
            let topo = self.topology.read().expect("topology lock poisoned");
            topo.address_for_shard(shard_id)
                .ok_or_else(|| anyhow::anyhow!("no address for shard '{}'", shard_id))?
                .to_string()
        };

        self.get_or_connect(&addr)
    }

    /// Handle a routing error by updating internal state.
    ///
    /// Returns `Some(true)` if the caller should retry with backoff (e.g., shard
    /// temporarily unavailable), `Some(false)` if the caller should retry
    /// immediately (e.g., topology refreshed or redirect applied), or `None` if
    /// the error is not a routing error and should not be retried.
    pub async fn handle_routing_error(
        &self,
        status: &tonic::Status,
        shard_id: &str,
    ) -> Option<bool> {
        match classify_error(status, shard_id) {
            ErrorAction::Redirect { shard_id, new_addr } => {
                let new_addr = ensure_http_scheme(&new_addr);
                info!(
                    shard_id = %shard_id,
                    new_addr = %new_addr,
                    "Shard redirected to new address"
                );
                // Update the override in topology. This also makes the address
                // available to `refresh_topology_once` via `topology.all_addresses()`.
                {
                    let mut topo = self.topology.write().expect("topology lock poisoned");
                    topo.shard_addr_overrides.insert(shard_id, new_addr);
                }
                Some(false)
            }
            ErrorAction::RefreshTopology => {
                info!("Routing error detected, refreshing topology");
                if self.refresh_topology().await {
                    Some(false)
                } else {
                    None
                }
            }
            ErrorAction::RetryWithBackoff => {
                // UNAVAILABLE — shard temporarily down (split/acquisition). Just signal
                // the caller to retry with backoff; no topology change needed.
                debug!("Shard temporarily unavailable, retrying with backoff");
                Some(true)
            }
            ErrorAction::NotRoutingError => None,
        }
    }

    /// Invalidate the cached connection for the given address, forcing a fresh
    /// lazy channel to be created on the next request. Call this after transport-level
    /// errors to recover from broken connections.
    pub fn invalidate_connection(&self, addr: &str) {
        let addr = ensure_http_scheme(addr);
        if self.connections.remove(&addr).is_some() {
            debug!(addr = %addr, "invalidated cached connection");
        }
    }

    /// Refresh the cluster topology by contacting any available server.
    /// Returns true if the refresh succeeded.
    ///
    /// Coalesced: if multiple tasks call this concurrently, only one
    /// `GetClusterInfo` RPC is made. Others wait for it to finish.
    pub async fn refresh_topology(&self) -> bool {
        let generation_before_wait = self.refresh_generation.load(Ordering::Acquire);

        // Acquire the refresh mutex. If another task is already refreshing,
        // we wait for it to finish rather than issuing a duplicate RPC.
        let _guard = self.refresh_mutex.lock().await;

        let generation_after_wait = self.refresh_generation.load(Ordering::Acquire);
        if generation_after_wait != generation_before_wait {
            return self.last_refresh_succeeded.load(Ordering::Acquire);
        }

        let refreshed = self.refresh_topology_once().await;
        self.last_refresh_succeeded
            .store(refreshed, Ordering::Release);
        self.refresh_generation.fetch_add(1, Ordering::AcqRel);
        refreshed
    }

    /// Build the list of addresses to try when refreshing topology.
    /// Combines the immutable seed addresses with the current topology's addresses
    /// (which include any redirect overrides).
    fn refresh_candidate_addresses(&self) -> Vec<String> {
        let topo_addrs = self
            .topology
            .read()
            .expect("topology lock poisoned")
            .all_addresses();
        let mut addrs: Vec<String> = self
            .seed_addresses
            .iter()
            .cloned()
            .chain(topo_addrs)
            .collect();
        addrs.sort();
        addrs.dedup();
        addrs
    }

    async fn refresh_topology_once(&self) -> bool {
        let addresses = self.refresh_candidate_addresses();

        for addr in &addresses {
            match Self::fetch_topology(addr, &self.config).await {
                Ok(new_topo) => {
                    debug!(
                        address = %addr,
                        num_shards = new_topo.num_shards,
                        "Topology refreshed"
                    );

                    let new_addrs = new_topo.all_addresses();

                    // Replace topology (clears overrides since we have fresh data)
                    {
                        let mut topo = self.topology.write().expect("topology lock poisoned");
                        *topo = new_topo;
                    }

                    // Pre-populate endpoints for any new servers
                    for a in &new_addrs {
                        let _ = self.get_or_connect(a);
                    }

                    return true;
                }
                Err(e) => {
                    debug!(address = %addr, error = %e, "Failed to refresh topology from server");
                }
            }
        }

        warn!("Failed to refresh topology from any known server");
        false
    }

    /// Get all unique addresses in the current topology.
    pub fn all_addresses(&self) -> Vec<String> {
        self.topology
            .read()
            .expect("topology lock poisoned")
            .all_addresses()
    }

    /// Get a snapshot of the current topology.
    pub fn topology(&self) -> ClusterTopology {
        self.topology
            .read()
            .expect("topology lock poisoned")
            .clone()
    }

    /// Get or create a lazy connection to the given address.
    ///
    /// Returns immediately without blocking on TCP. The actual connection is
    /// established on the first RPC call. tonic handles reconnection automatically.
    fn get_or_connect(&self, addr: &str) -> anyhow::Result<InterceptedSiloClient> {
        let addr = ensure_http_scheme(addr);
        if let Some(client) = self.connections.get(&addr) {
            return Ok(client.clone());
        }

        let client = create_silo_client_lazy(&addr, &self.config)?;
        self.connections.insert(addr, client.clone());
        Ok(client)
    }
}
