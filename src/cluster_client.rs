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
    CancelJobRequest, ColumnInfo, GetJobRequest, GetJobResponse, JobStatus, JsonValueBytes,
    NotifyConcurrencyTicketGrantedRequest, QueryRequest, ReleaseConcurrencyTicketRequest,
    RequestConcurrencyTicketRequest,
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
    /// Rows as JSON objects
    pub rows: Vec<JsonValueBytes>,
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

        // Convert batches to JSON rows using RawValue to avoid double-serialization
        let mut rows = Vec::new();
        for batch in batches {
            let mut buf = Vec::new();
            let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
            writer.write(&batch).map_err(|e| {
                ClusterClientError::QueryFailed(format!("Serialization error: {}", e))
            })?;
            writer.finish().map_err(|e| {
                ClusterClientError::QueryFailed(format!("Serialization error: {}", e))
            })?;

            // Parse as raw JSON values to avoid deserialize + serialize round-trip
            let json_array: Vec<Box<serde_json::value::RawValue>> = serde_json::from_slice(&buf)
                .map_err(|e| ClusterClientError::QueryFailed(format!("JSON parse error: {}", e)))?;

            for row_raw in json_array {
                rows.push(JsonValueBytes {
                    data: row_raw.get().as_bytes().to_vec(),
                });
            }
        }

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

            return Ok(GetJobResponse {
                id: job_view.id().to_string(),
                priority: job_view.priority() as u32,
                enqueue_time_ms: job_view.enqueue_time_ms(),
                payload: Some(JsonValueBytes {
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

    // ========================================================================
    // Internal cross-shard concurrency coordination RPCs
    // ========================================================================

    /// Request a concurrency ticket from a queue owner shard.
    /// Called by job shard when a job needs a ticket from a remote queue.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_concurrency_ticket(
        &self,
        queue_owner_shard: u32,
        tenant: &str,
        queue_key: &str,
        job_id: &str,
        job_shard: u32,
        request_id: &str,
        attempt_number: u32,
        priority: u8,
        start_time_ms: i64,
        max_concurrency: u32,
        floating_limit: Option<&crate::task::FloatingConcurrencyLimitData>,
    ) -> Result<bool, ClusterClientError> {
        let shard_name = queue_owner_shard.to_string();

        // Check if shard is local first
        if let Some(shard) = self.factory.get(&shard_name) {
            debug!(
                queue_owner_shard = queue_owner_shard,
                queue_key = queue_key,
                job_id = job_id,
                has_floating_limit = floating_limit.is_some(),
                "requesting ticket from local queue owner"
            );
            let granted = shard
                .receive_remote_ticket_request(
                    tenant,
                    queue_key,
                    job_id,
                    job_shard,
                    request_id,
                    attempt_number,
                    priority,
                    start_time_ms,
                    max_concurrency,
                    floating_limit,
                )
                .await
                .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;
            return Ok(granted);
        }

        // Shard is not local, need to call remote node
        let addr = self.get_shard_addr(queue_owner_shard).await?;
        debug!(
            queue_owner_shard = queue_owner_shard,
            addr = %addr,
            queue_key = queue_key,
            job_id = job_id,
            has_floating_limit = floating_limit.is_some(),
            "requesting ticket from remote queue owner"
        );

        let mut client = self.get_client(&addr).await?;

        // Convert floating limit to protobuf format if present
        let floating_limit_pb = floating_limit.map(|fl| crate::pb::FloatingConcurrencyLimitInfo {
            default_max_concurrency: fl.default_max_concurrency,
            refresh_interval_ms: fl.refresh_interval_ms,
            metadata: fl
                .metadata
                .iter()
                .map(|(k, v)| crate::pb::FloatingLimitMetadata {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        });

        let request = RequestConcurrencyTicketRequest {
            queue_owner_shard,
            tenant: tenant.to_string(),
            queue_key: queue_key.to_string(),
            job_id: job_id.to_string(),
            job_shard,
            request_id: request_id.to_string(),
            attempt_number,
            priority: priority as u32,
            start_time_ms,
            max_concurrency,
            floating_limit: floating_limit_pb,
        };

        let response = client
            .request_concurrency_ticket(request)
            .await
            .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;

        // registered=true means queued (not granted), registered=false means granted immediately
        Ok(!response.into_inner().registered)
    }

    /// Notify a job shard that a ticket has been granted.
    /// Called by queue owner when a ticket becomes available for a waiting request.
    #[allow(clippy::too_many_arguments)]
    pub async fn notify_concurrency_ticket_granted(
        &self,
        job_shard: u32,
        tenant: &str,
        job_id: &str,
        queue_key: &str,
        request_id: &str,
        holder_task_id: &str,
        queue_owner_shard: u32,
        attempt_number: u32,
    ) -> Result<(), ClusterClientError> {
        let shard_name = job_shard.to_string();

        // Check if shard is local first
        if let Some(shard) = self.factory.get(&shard_name) {
            debug!(
                job_shard = job_shard,
                job_id = job_id,
                queue_key = queue_key,
                "notifying local job shard of ticket grant"
            );
            shard
                .receive_remote_ticket_grant(
                    tenant,
                    job_id,
                    queue_key,
                    request_id,
                    holder_task_id,
                    queue_owner_shard,
                    attempt_number,
                )
                .await
                .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;
            return Ok(());
        }

        // Shard is not local, need to call remote node
        let addr = self.get_shard_addr(job_shard).await?;
        debug!(
            job_shard = job_shard,
            addr = %addr,
            job_id = job_id,
            queue_key = queue_key,
            "notifying remote job shard of ticket grant"
        );

        let mut client = self.get_client(&addr).await?;
        let request = NotifyConcurrencyTicketGrantedRequest {
            job_shard,
            tenant: tenant.to_string(),
            job_id: job_id.to_string(),
            queue_key: queue_key.to_string(),
            request_id: request_id.to_string(),
            holder_task_id: holder_task_id.to_string(),
            queue_owner_shard,
            attempt_number,
        };

        client
            .notify_concurrency_ticket_granted(request)
            .await
            .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;

        Ok(())
    }

    /// Release a concurrency ticket back to the queue owner.
    /// Called by job shard when a job completes (success/failure/cancel).
    pub async fn release_concurrency_ticket(
        &self,
        queue_owner_shard: u32,
        tenant: &str,
        queue_key: &str,
        job_id: &str,
        holder_task_id: &str,
    ) -> Result<(), ClusterClientError> {
        let shard_name = queue_owner_shard.to_string();

        // Check if shard is local first
        if let Some(shard) = self.factory.get(&shard_name) {
            debug!(
                queue_owner_shard = queue_owner_shard,
                queue_key = queue_key,
                job_id = job_id,
                "releasing ticket to local queue owner"
            );
            shard
                .release_remote_ticket(tenant, queue_key, job_id, holder_task_id)
                .await
                .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;
            return Ok(());
        }

        // Shard is not local, need to call remote node
        let addr = self.get_shard_addr(queue_owner_shard).await?;
        debug!(
            queue_owner_shard = queue_owner_shard,
            addr = %addr,
            queue_key = queue_key,
            job_id = job_id,
            "releasing ticket to remote queue owner"
        );

        let mut client = self.get_client(&addr).await?;
        let request = ReleaseConcurrencyTicketRequest {
            queue_owner_shard,
            tenant: tenant.to_string(),
            queue_key: queue_key.to_string(),
            job_id: job_id.to_string(),
            holder_task_id: holder_task_id.to_string(),
        };

        client
            .release_concurrency_ticket(request)
            .await
            .map_err(|e| ClusterClientError::RpcFailed(e.to_string()))?;

        Ok(())
    }
}
