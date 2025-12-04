//! Cluster client for routing queries to appropriate nodes in a distributed Silo cluster.
//!
//! This module provides a client that can query any shard in the cluster by:
//! - Querying local shards directly via the ShardFactory
//! - Making gRPC Query calls to remote nodes for shards they own

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::coordination::{CoordinationError, Coordinator, ShardOwnerMap};
use crate::factory::ShardFactory;
use crate::pb::silo_client::SiloClient;
use crate::pb::{ColumnInfo, JsonValueBytes, QueryRequest};

/// Error types for cluster client operations
#[derive(Debug, thiserror::Error)]
pub enum ClusterClientError {
    #[error("Shard {0} not found in cluster")]
    ShardNotFound(u32),
    #[error("Failed to connect to node at {0}: {1}")]
    ConnectionFailed(String, String),
    #[error("Query failed: {0}")]
    QueryFailed(String),
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
        // Check cache first
        {
            let cache = self.connections.read().await;
            if let Some(client) = cache.get(addr) {
                return Ok(client.clone());
            }
        }

        // Create new connection
        debug!(addr = %addr, "connecting to remote node");
        let channel = Channel::from_shared(addr.to_string())
            .map_err(|e| ClusterClientError::ConnectionFailed(addr.to_string(), e.to_string()))?
            .connect()
            .await
            .map_err(|e| ClusterClientError::ConnectionFailed(addr.to_string(), e.to_string()))?;

        let client = SiloClient::new(channel);

        // Cache the connection
        {
            let mut cache = self.connections.write().await;
            cache.insert(addr.to_string(), client.clone());
        }

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
        let Some(coordinator) = &self.coordinator else {
            return Err(ClusterClientError::NoCoordinator);
        };

        let owner_map = coordinator
            .get_shard_owner_map()
            .await
            .map_err(|e: CoordinationError| ClusterClientError::QueryFailed(e.to_string()))?;

        let Some(addr) = owner_map.shard_to_addr.get(&shard_id) else {
            return Err(ClusterClientError::ShardNotFound(shard_id));
        };

        debug!(shard_id = shard_id, addr = %addr, "querying remote shard");
        self.query_remote_shard(shard_id, addr, sql).await
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

        let schema = Arc::new(dataframe.schema().as_arrow().clone());

        let batches = dataframe.collect().await.map_err(|e| {
            ClusterClientError::QueryFailed(format!("Query execution failed: {}", e))
        })?;

        // Use schema from dataframe or first batch
        let schema = if let Some(batch) = batches.first() {
            batch.schema()
        } else {
            schema
        };

        let columns: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        // Convert batches to JSON rows
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

            let json_array: Vec<serde_json::Value> = serde_json::from_slice(&buf)
                .map_err(|e| ClusterClientError::QueryFailed(format!("JSON parse error: {}", e)))?;

            for row_value in json_array {
                let row_bytes = serde_json::to_vec(&row_value).map_err(|e| {
                    ClusterClientError::QueryFailed(format!("JSON serialize error: {}", e))
                })?;
                rows.push(JsonValueBytes { data: row_bytes });
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
            shard: shard_id.to_string(),
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
        let mut results = Vec::new();

        for shard_id in shard_ids {
            match self.query_shard(shard_id, sql).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    warn!(shard_id = shard_id, error = %e, "failed to query shard");
                    // Continue with other shards even if one fails
                }
            }
        }

        Ok(results)
    }

    /// Get all shard IDs in the cluster
    async fn get_all_shard_ids(&self) -> Result<Vec<u32>, ClusterClientError> {
        if let Some(coordinator) = &self.coordinator {
            let owner_map = coordinator
                .get_shard_owner_map()
                .await
                .map_err(|e: CoordinationError| ClusterClientError::QueryFailed(e.to_string()))?;
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
            .map_err(|e: CoordinationError| ClusterClientError::QueryFailed(e.to_string()))
    }

    /// Check if this node owns a specific shard
    pub fn owns_shard(&self, shard_id: u32) -> bool {
        self.factory.get(&shard_id.to_string()).is_some()
    }
}
