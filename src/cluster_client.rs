//! Cluster client for routing queries to appropriate nodes in a distributed Silo cluster.
//!
//! This module provides a client that can query any shard in the cluster by:
//! - Querying local shards directly via the ShardFactory
//! - Making gRPC Query calls to remote nodes for shards they own
//!
//! ## Connection Resilience
//!
//! The client includes built-in timeout and reconnection logic to handle network failures gracefully. When a connection fails or times out, the client will  automatically invalidate the cached connection and attempt to reconnect on the next request.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::future::join_all;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, trace, warn};

use crate::coordination::{Coordinator, ShardOwnerMap, SplitCleanupStatus};
use crate::factory::ShardFactory;
use crate::pb::silo_client::SiloClient;
use crate::pb::{
    CancelJobRequest, ColumnInfo, GetJobRequest, GetJobResponse, GetNodeInfoRequest, JobStatus,
    QueryRequest, RequestSplitRequest, RequestSplitResponse, SerializedBytes, serialized_bytes,
};
use crate::shard_range::ShardId;

/// Configuration for gRPC client connections.
///
/// These settings control timeouts, retries, and connection behavior for both internal cluster communication and external client connections.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Timeout for establishing a connection to a remote node.
    /// Default: 5 seconds.
    pub connect_timeout: Duration,
    /// Timeout for individual RPC requests.
    /// Default: 30 seconds.
    pub request_timeout: Duration,
    /// Interval for HTTP/2 keepalive pings.
    /// Default: 10 seconds.
    pub keepalive_interval: Duration,
    /// Timeout for keepalive ping acknowledgment.
    /// Default: 5 seconds.
    pub keepalive_timeout: Duration,
    /// Maximum number of connection retry attempts.
    /// Default: 3.
    pub max_retries: u32,
    /// Base delay for exponential backoff between retries.
    /// Default: 50ms. Actual delays are base * 2^attempt, capped at 16x base.
    pub retry_backoff_base: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(10),
            keepalive_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(50),
        }
    }
}

impl ClientConfig {
    /// Create a config optimized for high-latency or unreliable networks.
    /// Uses shorter timeouts to fail fast and more retries.
    pub fn for_unreliable_network() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(5),
            keepalive_timeout: Duration::from_secs(3),
            max_retries: 10,
            retry_backoff_base: Duration::from_millis(50),
        }
    }

    /// Create a config optimized for deterministic simulation testing (DST).
    /// Uses very short timeouts to ensure simulations complete within their time budgets, while still being long enough to handle normal request processing.
    pub fn for_dst() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            keepalive_interval: Duration::from_secs(1),
            keepalive_timeout: Duration::from_secs(1),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(50),
        }
    }

    /// Create an Endpoint configured with these timeout settings.
    pub fn configure_endpoint(&self, uri: &str) -> Result<Endpoint, tonic::transport::Error> {
        Endpoint::from_shared(uri.to_string()).map(|e| {
            e.connect_timeout(self.connect_timeout)
                .timeout(self.request_timeout)
                .http2_keep_alive_interval(self.keepalive_interval)
                .keep_alive_timeout(self.keepalive_timeout)
                .keep_alive_while_idle(true)
        })
    }

    /// Calculate backoff duration for a given retry attempt.
    /// Uses exponential backoff: base * 2^attempt, capped at 16x base.
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        self.retry_backoff_base * (1 << attempt.min(4))
    }
}

/// Helper to create a SiloClient with proper timeout and retry configuration.
///
/// This function creates a gRPC client with configurable timeouts and automatic retry logic with exponential backoff. Retries are attempted when the initial connection fails.
///
/// Each connection attempt is wrapped with a hard timeout to ensure we don't hang indefinitely when TCP/HTTP2 gets into a bad state (e.g., due to packet loss or network partitions that leave the connection half-open).
///
/// # Arguments
/// * `uri` - The server URI (e.g., "http://localhost:9910")
/// * `config` - Client configuration with timeout and retry settings
///
/// # Example
/// ```ignore
/// let config = ClientConfig::default();
/// let client = create_silo_client("http://localhost:9910", &config).await?;
/// ```
pub async fn create_silo_client(
    uri: &str,
    config: &ClientConfig,
) -> Result<SiloClient<Channel>, ClusterClientError> {
    // Ensure address has http:// scheme
    let full_uri = if uri.starts_with("http://") || uri.starts_with("https://") {
        uri.to_string()
    } else {
        format!("http://{}", uri)
    };

    let endpoint = config
        .configure_endpoint(&full_uri)
        .map_err(|e| ClusterClientError::ConnectionFailed(full_uri.clone(), e.to_string()))?;

    // Hard timeout per connection attempt. The endpoint has a connect_timeout, but in practice TCP/HTTP2 can get into states where the timeout doesn't fire
    // (e.g., half-open connections, corrupted h2 streams). This ensures we don't hang indefinitely on any single attempt.
    let attempt_timeout = config.connect_timeout + Duration::from_secs(1);

    let mut last_error = None;
    for attempt in 0..config.max_retries {
        let connect_result = tokio::time::timeout(attempt_timeout, endpoint.connect()).await;

        match connect_result {
            Ok(Ok(channel)) => return Ok(SiloClient::new(channel)),
            Ok(Err(e)) => {
                trace!(
                    attempt = attempt,
                    max_retries = config.max_retries,
                    error = %e,
                    uri = %full_uri,
                    "connection attempt failed, retrying"
                );
                last_error = Some(e.to_string());
            }
            Err(_elapsed) => {
                trace!(
                    attempt = attempt,
                    max_retries = config.max_retries,
                    uri = %full_uri,
                    timeout_secs = ?attempt_timeout,
                    "connection attempt timed out, retrying"
                );
                last_error = Some(format!("connection timed out after {:?}", attempt_timeout));
            }
        }

        if attempt + 1 < config.max_retries {
            tokio::time::sleep(config.backoff_for_attempt(attempt)).await;
        }
    }

    Err(ClusterClientError::ConnectionFailed(
        full_uri,
        last_error.unwrap_or_else(|| "no connection attempts made".into()),
    ))
}

/// Error types for cluster client operations
#[derive(Debug, thiserror::Error)]
pub enum ClusterClientError {
    #[error("Shard {0} not found in cluster")]
    ShardNotFound(ShardId),
    #[error("Job not found")]
    JobNotFound,
    #[error("Failed to connect to node at {0}: {1}")]
    ConnectionFailed(String, String),
    #[error("Query failed: {0}")]
    QueryFailed(String),
    #[error("RPC failed: {0}")]
    RpcFailed(String),
    #[error("No coordinator available")]
    NoCoordinator,
}

/// Result of a query across shards
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column schema information
    pub columns: Vec<ColumnInfo>,
    /// Rows as serialized objects (MessagePack by default)
    pub rows: Vec<SerializedBytes>,
    /// Number of rows returned
    pub row_count: i32,
    /// Which shard this result came from
    pub shard_id: ShardId,
}

/// Information about a single shard from GetNodeInfo RPC
#[derive(Debug, Clone)]
pub struct ShardNodeInfo {
    /// The shard ID
    pub shard_id: ShardId,
    /// Total number of jobs in the shard
    pub total_jobs: i64,
    /// Number of jobs in terminal states
    pub completed_jobs: i64,
    /// The cleanup status (post-split cleanup state)
    pub cleanup_status: SplitCleanupStatus,
    /// Timestamp (ms) when this shard was first created
    pub created_at_ms: i64,
    /// Timestamp (ms) when cleanup completed (0 if not applicable)
    pub cleanup_completed_at_ms: i64,
}

/// Information about a node and its owned shards from GetNodeInfo RPC
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// The node ID
    pub node_id: String,
    /// Information about each shard owned by this node
    pub shards: Vec<ShardNodeInfo>,
}

/// Aggregated information from all nodes in the cluster
#[derive(Debug, Clone)]
pub struct ClusterNodeInfo {
    /// Information from each reachable node
    pub nodes: Vec<NodeInfo>,
}

impl ClusterNodeInfo {
    /// Get the info for a specific shard, if available
    pub fn get_shard_info(&self, shard_id: &ShardId) -> Option<&ShardNodeInfo> {
        for node in &self.nodes {
            for shard in &node.shards {
                if &shard.shard_id == shard_id {
                    return Some(shard);
                }
            }
        }
        None
    }

    /// Get all shard info as a map from shard_id to ShardNodeInfo
    pub fn shard_info_map(&self) -> HashMap<ShardId, ShardNodeInfo> {
        let mut map = HashMap::new();
        for node in &self.nodes {
            for shard in &node.shards {
                map.insert(shard.shard_id, shard.clone());
            }
        }
        map
    }
}

/// Client for querying shards across a distributed Silo cluster
pub struct ClusterClient {
    /// Local shard factory for querying local shards
    factory: Arc<ShardFactory>,
    /// Coordinator for getting shard ownership information
    coordinator: Option<Arc<dyn Coordinator>>,
    /// Cache of gRPC client connections to peer nodes
    connections: DashMap<String, SiloClient<Channel>>,
    /// Client configuration for timeouts and connection behavior
    config: ClientConfig,
}

impl ClusterClient {
    /// Create a new cluster client with default configuration
    pub fn new(factory: Arc<ShardFactory>, coordinator: Option<Arc<dyn Coordinator>>) -> Self {
        Self::with_config(factory, coordinator, ClientConfig::default())
    }

    /// Create a new cluster client with custom configuration
    pub fn with_config(
        factory: Arc<ShardFactory>,
        coordinator: Option<Arc<dyn Coordinator>>,
        config: ClientConfig,
    ) -> Self {
        Self {
            factory,
            coordinator,
            connections: DashMap::new(),
            config,
        }
    }

    /// Get or create a gRPC client connection to a remote node
    async fn get_client(&self, addr: &str) -> Result<SiloClient<Channel>, ClusterClientError> {
        // Ensure address has http:// scheme for gRPC connection
        let full_addr = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };

        // Check cache first
        if let Some(client) = self.connections.get(&full_addr) {
            return Ok(client.clone());
        }

        // Create new connection with configured timeouts
        debug!(addr = %full_addr, "connecting to remote node");
        let client = create_silo_client(&full_addr, &self.config).await?;

        // Cache the connection - if another task raced us, use their connection
        self.connections.insert(full_addr.clone(), client.clone());

        Ok(client)
    }

    /// Invalidate a cached connection (call after RPC failures to force reconnect)
    pub fn invalidate_connection(&self, addr: &str) {
        let full_addr = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };

        if self.connections.remove(&full_addr).is_some() {
            debug!(addr = %full_addr, "invalidated cached connection");
        }
    }

    /// Query a specific shard, routing to the appropriate node
    pub async fn query_shard(
        &self,
        shard_id: &ShardId,
        sql: &str,
    ) -> Result<QueryResult, ClusterClientError> {
        // Check if shard is local first
        if let Some(shard) = self.factory.get(shard_id) {
            debug!(shard_id = %shard_id, "querying local shard");
            return self.query_local_shard(shard_id, &shard, sql).await;
        }

        // Shard is not local, need to query remote node
        let addr = self.get_shard_addr(shard_id).await?;
        debug!(shard_id = %shard_id, addr = %addr, "querying remote shard");
        self.query_remote_shard(shard_id, &addr, sql).await
    }

    /// Query a local shard directly
    async fn query_local_shard(
        &self,
        shard_id: &ShardId,
        shard: &Arc<crate::job_store_shard::JobStoreShard>,
        sql: &str,
    ) -> Result<QueryResult, ClusterClientError> {
        let query_engine = shard.query_engine();

        let dataframe = query_engine
            .sql(sql)
            .await
            .map_err(|e| ClusterClientError::QueryFailed(format!("SQL error: {}", e)))?;

        let fallback_schema = Arc::new(dataframe.schema().as_arrow().clone());

        let batches = dataframe.collect().await.map_err(|e| {
            ClusterClientError::QueryFailed(format!("Query execution failed: {}", e))
        })?;

        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or(fallback_schema);

        let columns: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        // Convert batches directly to MessagePack rows
        let row_bytes = crate::query::record_batches_to_msgpack(&batches)
            .map_err(ClusterClientError::QueryFailed)?;
        let rows: Vec<SerializedBytes> = row_bytes
            .into_iter()
            .map(|data| SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(data)),
            })
            .collect();

        let row_count = rows.len() as i32;

        Ok(QueryResult {
            columns,
            rows,
            row_count,
            shard_id: *shard_id,
        })
    }

    /// Query a remote shard via gRPC
    async fn query_remote_shard(
        &self,
        shard_id: &ShardId,
        addr: &str,
        sql: &str,
    ) -> Result<QueryResult, ClusterClientError> {
        let mut client = self.get_client(addr).await?;

        let request = QueryRequest {
            shard: shard_id.to_string(),
            sql: sql.to_string(),
            tenant: None,
        };

        let response = match client.query(request).await {
            Ok(resp) => resp,
            Err(e) => {
                // Invalidate connection on failure to force reconnect on next attempt
                self.invalidate_connection(addr);
                return Err(ClusterClientError::QueryFailed(format!(
                    "gRPC error: {}",
                    e
                )));
            }
        };

        let resp = response.into_inner();

        Ok(QueryResult {
            columns: resp.columns,
            rows: resp.rows,
            row_count: resp.row_count,
            shard_id: *shard_id,
        })
    }

    /// Query all shards in the cluster and combine results
    pub async fn query_all_shards(
        &self,
        sql: &str,
    ) -> Result<Vec<QueryResult>, ClusterClientError> {
        let shard_ids = self.get_all_shard_ids().await?;

        // Query all shards in parallel
        let futures: Vec<_> = shard_ids
            .into_iter()
            .map(|shard_id| async move { (shard_id, self.query_shard(&shard_id, sql).await) })
            .collect();

        let results: Vec<_> = join_all(futures)
            .await
            .into_iter()
            .filter_map(|(shard_id, result)| match result {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!(shard_id = %shard_id, error = %e, "failed to query shard");
                    None
                }
            })
            .collect();

        Ok(results)
    }

    /// Get all shard IDs in the cluster
    async fn get_all_shard_ids(&self) -> Result<Vec<ShardId>, ClusterClientError> {
        if let Some(coordinator) = &self.coordinator {
            let owner_map = coordinator
                .get_shard_owner_map()
                .await
                .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;
            Ok(owner_map.shard_ids())
        } else {
            // No coordinator, just return local shard IDs
            let local_shards: Vec<ShardId> = self.factory.instances().keys().copied().collect();
            Ok(local_shards)
        }
    }

    /// Get the shard owner map from the coordinator
    pub async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, ClusterClientError> {
        let Some(coordinator) = &self.coordinator else {
            return Err(ClusterClientError::NoCoordinator);
        };

        coordinator
            .get_shard_owner_map()
            .await
            .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))
    }

    /// Check if this node owns a specific shard
    pub fn owns_shard(&self, shard_id: &ShardId) -> bool {
        self.factory.owns_shard(shard_id)
    }

    /// Get the address for a remote shard
    async fn get_shard_addr(&self, shard_id: &ShardId) -> Result<String, ClusterClientError> {
        let Some(coordinator) = &self.coordinator else {
            return Err(ClusterClientError::NoCoordinator);
        };

        let owner_map = coordinator
            .get_shard_owner_map()
            .await
            .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;

        owner_map
            .get_addr(shard_id)
            .cloned()
            .ok_or(ClusterClientError::ShardNotFound(*shard_id))
    }

    /// Get a job from any shard (local or remote)
    pub async fn get_job(
        &self,
        shard_id: &ShardId,
        tenant: &str,
        job_id: &str,
        include_attempts: bool,
    ) -> Result<GetJobResponse, ClusterClientError> {
        // Check if shard is local first
        if let Some(shard) = self.factory.get(shard_id) {
            debug!(
                shard_id = %shard_id,
                job_id = job_id,
                "getting job from local shard"
            );
            let job_view = shard
                .get_job(tenant, job_id)
                .await
                .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?
                .ok_or(ClusterClientError::JobNotFound)?;

            let retry_policy = job_view.retry_policy().map(|p| crate::pb::RetryPolicy {
                retry_count: p.retry_count,
                initial_interval_ms: p.initial_interval_ms,
                max_interval_ms: p.max_interval_ms,
                randomize_interval: p.randomize_interval,
                backoff_factor: p.backoff_factor,
            });

            // Get job status - should always exist if job exists
            let job_status = shard
                .get_job_status(tenant, job_id)
                .await
                .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?
                .ok_or_else(|| {
                    ClusterClientError::QueryFailed("job exists but has no status".to_string())
                })?;
            let status = match job_status.kind {
                crate::job::JobStatusKind::Scheduled => JobStatus::Scheduled,
                crate::job::JobStatusKind::Running => JobStatus::Running,
                crate::job::JobStatusKind::Succeeded => JobStatus::Succeeded,
                crate::job::JobStatusKind::Failed => JobStatus::Failed,
                crate::job::JobStatusKind::Cancelled => JobStatus::Cancelled,
            };
            let status_changed_at_ms = job_status.changed_at_ms;

            // Optionally fetch attempts if requested
            let attempts = if include_attempts {
                let attempt_views = shard
                    .get_job_attempts(tenant, job_id)
                    .await
                    .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;
                attempt_views
                    .into_iter()
                    .map(|a| crate::server::job_attempt_view_to_proto(&a))
                    .collect()
            } else {
                Vec::new()
            };

            return Ok(GetJobResponse {
                id: job_view.id().to_string(),
                priority: job_view.priority() as u32,
                enqueue_time_ms: job_view.enqueue_time_ms(),
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        job_view.payload_bytes().to_vec(),
                    )),
                }),
                retry_policy,
                limits: job_view
                    .limits()
                    .into_iter()
                    .map(crate::server::job_limit_to_proto_limit)
                    .collect(),
                metadata: job_view.metadata().into_iter().collect(),
                status: status.into(),
                status_changed_at_ms,
                attempts,
                next_attempt_starts_after_ms: job_status.next_attempt_starts_after_ms,
                task_group: job_view.task_group().to_string(),
            });
        }

        // Shard is not local, need to call remote node
        let addr = self.get_shard_addr(shard_id).await?;
        debug!(shard_id = %shard_id, addr = %addr, job_id = job_id, "getting job from remote shard");

        let mut client = self.get_client(&addr).await?;
        let request = GetJobRequest {
            shard: shard_id.to_string(),
            id: job_id.to_string(),
            tenant: Some(tenant.to_string()),
            include_attempts,
        };

        let response = match client.get_job(request).await {
            Ok(resp) => resp,
            Err(e) => {
                // Invalidate connection on failure to force reconnect on next attempt
                self.invalidate_connection(&addr);
                if e.code() == tonic::Code::NotFound {
                    return Err(ClusterClientError::JobNotFound);
                } else {
                    return Err(ClusterClientError::RpcFailed(e.to_string()));
                }
            }
        };

        Ok(response.into_inner())
    }

    /// Cancel a job on any shard (local or remote)
    pub async fn cancel_job(
        &self,
        shard_id: &ShardId,
        tenant: &str,
        job_id: &str,
    ) -> Result<(), ClusterClientError> {
        // Check if shard is local first
        if let Some(shard) = self.factory.get(shard_id) {
            debug!(
                shard_id = %shard_id,
                job_id = job_id,
                "cancelling job on local shard"
            );
            shard
                .cancel_job(tenant, job_id)
                .await
                .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;
            return Ok(());
        }

        // Shard is not local, need to call remote node
        let addr = self.get_shard_addr(shard_id).await?;
        debug!(shard_id = %shard_id, addr = %addr, job_id = job_id, "cancelling job on remote shard");

        let mut client = self.get_client(&addr).await?;
        let request = CancelJobRequest {
            shard: shard_id.to_string(),
            id: job_id.to_string(),
            tenant: Some(tenant.to_string()),
        };

        if let Err(e) = client.cancel_job(request).await {
            // Invalidate connection on failure to force reconnect on next attempt
            self.invalidate_connection(&addr);
            return Err(ClusterClientError::RpcFailed(e.to_string()));
        }

        Ok(())
    }

    /// Request a shard split operation
    ///
    /// Routes the request to the node that owns the shard. The split will be
    /// executed asynchronously by the owning node.
    pub async fn request_split(
        &self,
        shard_id: &ShardId,
        split_point: &str,
    ) -> Result<RequestSplitResponse, ClusterClientError> {
        // Always route to the shard owner for split operations
        let addr = if self.factory.owns_shard(shard_id) {
            // Local shard - need to get our own address from coordinator
            if let Some(coordinator) = &self.coordinator {
                coordinator.grpc_addr().to_string()
            } else {
                return Err(ClusterClientError::NoCoordinator);
            }
        } else {
            self.get_shard_addr(shard_id).await?
        };

        debug!(shard_id = %shard_id, addr = %addr, split_point = %split_point, "requesting shard split");

        let mut client = self.get_client(&addr).await?;
        let request = RequestSplitRequest {
            shard_id: shard_id.to_string(),
            split_point: split_point.to_string(),
        };

        match client.request_split(request).await {
            Ok(resp) => Ok(resp.into_inner()),
            Err(e) => {
                self.invalidate_connection(&addr);
                Err(ClusterClientError::RpcFailed(e.to_string()))
            }
        }
    }

    /// Get node information from all cluster members.
    /// Calls GetNodeInfo RPC on all cluster members and aggregates the results. This returns counters and cleanup status for all shards across the cluster. For the local node, gets information directly from the factory instead of gRPC.
    pub async fn get_all_node_info(&self) -> Result<ClusterNodeInfo, ClusterClientError> {
        let Some(coordinator) = &self.coordinator else {
            return Err(ClusterClientError::NoCoordinator);
        };

        // Get all members from the coordinator
        let members = coordinator
            .get_members()
            .await
            .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;

        // Identify the local node by comparing grpc addresses
        let local_addr = coordinator.grpc_addr();

        // Call GetNodeInfo on all members in parallel
        // For the local node, get info directly from factory instead of gRPC
        let futures: Vec<_> = members
            .iter()
            .map(|member| {
                let addr = member.grpc_addr.clone();
                let node_id = member.node_id.clone();
                let is_local = addr == local_addr;
                async move {
                    let result = if is_local {
                        self.get_local_node_info(&node_id).await
                    } else {
                        self.get_node_info(&addr).await
                    };
                    (addr, result)
                }
            })
            .collect();

        let results: Vec<NodeInfo> = join_all(futures)
            .await
            .into_iter()
            .filter_map(|(addr, result)| match result {
                Ok(info) => Some(info),
                Err(e) => {
                    warn!(addr = %addr, error = %e, "failed to get node info");
                    None
                }
            })
            .collect();

        Ok(ClusterNodeInfo { nodes: results })
    }

    /// Get node information for the local node directly from the factory.
    ///
    /// This avoids making a gRPC call when we can access the shards directly.
    async fn get_local_node_info(&self, node_id: &str) -> Result<NodeInfo, ClusterClientError> {
        let Some(coordinator) = &self.coordinator else {
            return Err(ClusterClientError::NoCoordinator);
        };

        let owned_shard_ids = coordinator.owned_shards().await;
        let mut shards = Vec::with_capacity(owned_shard_ids.len());

        for shard_id in owned_shard_ids {
            if let Some(shard) = self.factory.get(&shard_id) {
                // Get counters
                let (total_jobs, completed_jobs) = match shard.get_counters().await {
                    Ok(counters) => (counters.total_jobs, counters.completed_jobs),
                    Err(e) => {
                        warn!(shard_id = %shard_id, error = %e, "failed to get counters from local shard");
                        (0, 0)
                    }
                };

                // Get cleanup status
                let cleanup_status = match shard.get_cleanup_status().await {
                    Ok(status) => status,
                    Err(e) => {
                        warn!(shard_id = %shard_id, error = %e, "failed to get cleanup status from local shard");
                        SplitCleanupStatus::CompactionDone
                    }
                };

                // Get shard metadata timestamps
                let created_at_ms = shard.get_created_at_ms().await.ok().flatten().unwrap_or(0);
                let cleanup_completed_at_ms = shard
                    .get_cleanup_completed_at_ms()
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);

                shards.push(ShardNodeInfo {
                    shard_id,
                    total_jobs,
                    completed_jobs,
                    cleanup_status,
                    created_at_ms,
                    cleanup_completed_at_ms,
                });
            }
        }

        Ok(NodeInfo {
            node_id: node_id.to_string(),
            shards,
        })
    }

    /// Get node information from a specific node via gRPC
    async fn get_node_info(&self, addr: &str) -> Result<NodeInfo, ClusterClientError> {
        let mut client = self.get_client(addr).await?;

        let request = GetNodeInfoRequest {};
        let response = match client.get_node_info(request).await {
            Ok(resp) => resp,
            Err(e) => {
                self.invalidate_connection(addr);
                return Err(ClusterClientError::RpcFailed(format!(
                    "GetNodeInfo failed: {}",
                    e
                )));
            }
        };

        let resp = response.into_inner();

        // Parse the response into our internal types
        let shards: Vec<ShardNodeInfo> = resp
            .owned_shards
            .into_iter()
            .filter_map(|shard_info| {
                let shard_id = match ShardId::parse(&shard_info.shard_id) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(
                            shard_id = %shard_info.shard_id,
                            error = %e,
                            "failed to parse shard ID from GetNodeInfo response"
                        );
                        return None;
                    }
                };

                let cleanup_status = match shard_info.cleanup_status.parse::<SplitCleanupStatus>() {
                    Ok(status) => status,
                    Err(e) => {
                        warn!(
                            shard_id = %shard_id,
                            status = %shard_info.cleanup_status,
                            error = %e,
                            "failed to parse cleanup status, defaulting to CompactionDone"
                        );
                        SplitCleanupStatus::CompactionDone
                    }
                };

                Some(ShardNodeInfo {
                    shard_id,
                    total_jobs: shard_info.total_jobs,
                    completed_jobs: shard_info.completed_jobs,
                    cleanup_status,
                    created_at_ms: shard_info.created_at_ms,
                    cleanup_completed_at_ms: shard_info.cleanup_completed_at_ms,
                })
            })
            .collect();

        Ok(NodeInfo {
            node_id: resp.node_id,
            shards,
        })
    }
}
