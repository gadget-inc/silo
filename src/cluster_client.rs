//! Cluster client for routing queries to appropriate nodes in a distributed Silo cluster.
//!
//! This module provides a client that can query any shard in the cluster by:
//! - Querying local shards directly via the ShardFactory
//! - Making gRPC Query calls to remote nodes for shards they own

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::join_all;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::coordination::{Coordinator, ShardOwnerMap};
use crate::factory::ShardFactory;
use crate::pb::silo_client::SiloClient;
use crate::pb::{
    CancelJobRequest, ColumnInfo, GetJobRequest, GetJobResponse, JobStatus, MsgpackBytes,
    QueryRequest,
};

/// Error types for cluster client operations
#[derive(Debug, thiserror::Error)]
pub enum ClusterClientError {
    #[error("Shard {0} not found in cluster")]
    ShardNotFound(u32),
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
    /// Rows as MessagePack-encoded objects
    pub rows: Vec<MsgpackBytes>,
    /// Number of rows returned
    pub row_count: i32,
    /// Which shard this result came from
    pub shard_id: u32,
}

/// Client for querying shards across a distributed Silo cluster
pub struct ClusterClient {
    /// Local shard factory for querying local shards
    factory: Arc<ShardFactory>,
    /// Coordinator for getting shard ownership information
    coordinator: Option<Arc<dyn Coordinator>>,
    /// Cache of gRPC client connections to peer nodes
    connections: RwLock<HashMap<String, SiloClient<Channel>>>,
}

impl ClusterClient {
    /// Create a new cluster client
    pub fn new(factory: Arc<ShardFactory>, coordinator: Option<Arc<dyn Coordinator>>) -> Self {
        Self {
            factory,
            coordinator,
            connections: RwLock::new(HashMap::new()),
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

        // Check cache first (fast path with read lock)
        {
            let cache = self.connections.read().await;
            if let Some(client) = cache.get(&full_addr) {
                return Ok(client.clone());
            }
        }

        // Create new connection
        debug!(addr = %full_addr, "connecting to remote node");
        let channel = Channel::from_shared(full_addr.clone())
            .map_err(|e| ClusterClientError::ConnectionFailed(full_addr.clone(), e.to_string()))?
            .connect()
            .await
            .map_err(|e| ClusterClientError::ConnectionFailed(full_addr.clone(), e.to_string()))?;

        let client = SiloClient::new(channel);

        // Cache the connection (double-check after acquiring write lock to avoid race)
        let mut cache = self.connections.write().await;
        if let Some(existing) = cache.get(&full_addr) {
            // Another task created the connection while we were connecting
            return Ok(existing.clone());
        }
        cache.insert(full_addr, client.clone());

        Ok(client)
    }

    /// Query a specific shard, routing to the appropriate node
    pub async fn query_shard(
        &self,
        shard_id: u32,
        sql: &str,
    ) -> Result<QueryResult, ClusterClientError> {
        let shard_name = shard_id.to_string();

        // Check if shard is local first
        if let Some(shard) = self.factory.get(&shard_name) {
            debug!(shard_id = shard_id, "querying local shard");
            return self.query_local_shard(shard_id, &shard, sql).await;
        }

        // Shard is not local, need to query remote node
        let addr = self.get_shard_addr(shard_id).await?;
        debug!(shard_id = shard_id, addr = %addr, "querying remote shard");
        self.query_remote_shard(shard_id, &addr, sql).await
    }

    /// Query a local shard directly
    async fn query_local_shard(
        &self,
        shard_id: u32,
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
        let rows: Vec<MsgpackBytes> = row_bytes
            .into_iter()
            .map(|data| MsgpackBytes { data })
            .collect();

        let row_count = rows.len() as i32;

        Ok(QueryResult {
            columns,
            rows,
            row_count,
            shard_id,
        })
    }

    /// Query a remote shard via gRPC
    async fn query_remote_shard(
        &self,
        shard_id: u32,
        addr: &str,
        sql: &str,
    ) -> Result<QueryResult, ClusterClientError> {
        let mut client = self.get_client(addr).await?;

        let request = QueryRequest {
            shard: shard_id,
            sql: sql.to_string(),
            tenant: None,
        };

        let response = client
            .query(request)
            .await
            .map_err(|e| ClusterClientError::QueryFailed(format!("gRPC error: {}", e)))?;

        let resp = response.into_inner();

        Ok(QueryResult {
            columns: resp.columns,
            rows: resp.rows,
            row_count: resp.row_count,
            shard_id,
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
            .map(|shard_id| async move { (shard_id, self.query_shard(shard_id, sql).await) })
            .collect();

        let results: Vec<_> = join_all(futures)
            .await
            .into_iter()
            .filter_map(|(shard_id, result)| match result {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!(shard_id = shard_id, error = %e, "failed to query shard");
                    None
                }
            })
            .collect();

        Ok(results)
    }

    /// Get all shard IDs in the cluster
    async fn get_all_shard_ids(&self) -> Result<Vec<u32>, ClusterClientError> {
        if let Some(coordinator) = &self.coordinator {
            let owner_map = coordinator
                .get_shard_owner_map()
                .await
                .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;
            Ok((0..owner_map.num_shards).collect())
        } else {
            // No coordinator, just return local shard IDs
            let local_shards: Vec<u32> = self
                .factory
                .instances()
                .keys()
                .filter_map(|s| s.parse().ok())
                .collect();
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
    pub fn owns_shard(&self, shard_id: u32) -> bool {
        self.factory.get(&shard_id.to_string()).is_some()
    }

    /// Get the address for a remote shard
    async fn get_shard_addr(&self, shard_id: u32) -> Result<String, ClusterClientError> {
        let Some(coordinator) = &self.coordinator else {
            return Err(ClusterClientError::NoCoordinator);
        };

        let owner_map = coordinator
            .get_shard_owner_map()
            .await
            .map_err(|e| ClusterClientError::QueryFailed(e.to_string()))?;

        owner_map
            .shard_to_addr
            .get(&shard_id)
            .cloned()
            .ok_or(ClusterClientError::ShardNotFound(shard_id))
    }

    /// Get a job from any shard (local or remote)
    pub async fn get_job(
        &self,
        shard_id: u32,
        tenant: &str,
        job_id: &str,
        include_attempts: bool,
    ) -> Result<GetJobResponse, ClusterClientError> {
        let shard_name = shard_id.to_string();

        // Check if shard is local first
        if let Some(shard) = self.factory.get(&shard_name) {
            debug!(
                shard_id = shard_id,
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
                payload: Some(MsgpackBytes {
                    data: job_view.payload_bytes().to_vec(),
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
        debug!(shard_id = shard_id, addr = %addr, job_id = job_id, "getting job from remote shard");

        let mut client = self.get_client(&addr).await?;
        let request = GetJobRequest {
            shard: shard_id,
            id: job_id.to_string(),
            tenant: Some(tenant.to_string()),
            include_attempts,
        };

        let response = client.get_job(request).await.map_err(|e| {
            if e.code() == tonic::Code::NotFound {
                ClusterClientError::JobNotFound
            } else {
                ClusterClientError::RpcFailed(e.to_string())
            }
        })?;

        Ok(response.into_inner())
    }

    /// Cancel a job on any shard (local or remote)
    pub async fn cancel_job(
        &self,
        shard_id: u32,
        tenant: &str,
        job_id: &str,
    ) -> Result<(), ClusterClientError> {
        let shard_name = shard_id.to_string();

        // Check if shard is local first
        if let Some(shard) = self.factory.get(&shard_name) {
            debug!(
                shard_id = shard_id,
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
        debug!(shard_id = shard_id, addr = %addr, job_id = job_id, "cancelling job on remote shard");

        let mut client = self.get_client(&addr).await?;
        let request = CancelJobRequest {
            shard: shard_id,
            id: job_id.to_string(),
            tenant: Some(tenant.to_string()),
        };

        client
            .cancel_job(request)
            .await
            .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;

        Ok(())
    }
}
