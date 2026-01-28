//! Cluster-wide query engine using Apache DataFusion.
//!
//! This module provides a query engine that can execute SQL queries across all shards
//! in a Silo cluster. It creates a multi-partition execution plan where each partition
//! corresponds to a shard, enabling DataFusion to properly aggregate results across
//! the entire cluster.
//!
//! For local shards, data is read directly. For remote shards, the engine makes
//! gRPC calls using Arrow IPC for efficient data transfer.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session as CatalogSession;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::DataFrame;
use datafusion_sql::unparser::Unparser;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::arrow_ipc::ipc_to_batches_only;
use crate::coordination::Coordinator;
use crate::factory::ShardFactory;
use crate::job_store_shard::JobStoreShard;
use crate::pb::QueryArrowRequest;
use crate::pb::silo_client::SiloClient;
use crate::query::{JobsScanner, QueuesScanner, ScannerRef, explain_dataframe};
use crate::shard_range::ShardId;

/// Cluster-wide SQL query engine using DataFusion.
///
/// This engine registers tables that span all shards in the cluster,
/// allowing SQL queries like `SELECT COUNT(*) FROM jobs` to return
/// correct cluster-wide aggregations.
pub struct ClusterQueryEngine {
    ctx: SessionContext,
}

impl std::fmt::Debug for ClusterQueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterQueryEngine")
    }
}

/// Configuration for a shard - either local or remote
#[derive(Clone)]
enum ShardConfig {
    Local(ShardId, Arc<JobStoreShard>),
    Remote { shard_id: ShardId, addr: String },
}

impl ClusterQueryEngine {
    /// Create a new cluster-wide query engine.
    ///
    /// # Arguments
    /// * `factory` - The shard factory for accessing local shards
    /// * `coordinator` - Optional coordinator for discovering shards
    pub async fn new(
        factory: Arc<ShardFactory>,
        coordinator: Option<Arc<dyn Coordinator>>,
    ) -> DfResult<Self> {
        let ctx = SessionContext::new();

        // Register cluster-wide jobs table (shard_id is included in the scanner's schema)
        let jobs_schema = JobsScanner::base_schema();
        let jobs_provider = Arc::new(ClusterTableProvider::new(
            jobs_schema,
            factory.clone(),
            coordinator.clone(),
            TableKind::Jobs,
        ));
        ctx.register_table("jobs", jobs_provider)?;

        // Register cluster-wide queues table (shard_id is included in the scanner's schema)
        let queues_schema = QueuesScanner::base_schema();
        let queues_provider = Arc::new(ClusterTableProvider::new(
            queues_schema,
            factory,
            coordinator,
            TableKind::Queues,
        ));
        ctx.register_table("queues", queues_provider)?;

        Ok(Self { ctx })
    }

    /// Build shard configurations by checking which shards are local and which are remote.
    /// This is called at query time to get the current cluster state.
    async fn build_shard_configs(
        factory: &ShardFactory,
        coordinator: Option<&Arc<dyn Coordinator>>,
    ) -> Vec<ShardConfig> {
        // Get shard ownership info from coordinator
        let (shard_ids, remote_addrs): (Vec<ShardId>, HashMap<ShardId, String>) =
            if let Some(coord) = coordinator {
                match coord.get_shard_owner_map().await {
                    Ok(map) => (map.shard_ids(), map.shard_to_addr),
                    Err(e) => {
                        warn!(error = %e, "failed to get shard owner map, using only local shards");
                        // Fall back to local shards only
                        let local_ids: Vec<ShardId> = factory.instances().keys().copied().collect();
                        (local_ids, HashMap::new())
                    }
                }
            } else {
                // No coordinator - use local shards only
                let local_ids: Vec<ShardId> = factory.instances().keys().copied().collect();
                (local_ids, HashMap::new())
            };

        let mut configs = Vec::with_capacity(shard_ids.len());

        for shard_id in shard_ids {
            if let Some(local_shard) = factory.get(&shard_id) {
                configs.push(ShardConfig::Local(shard_id, local_shard));
            } else if let Some(addr) = remote_addrs.get(&shard_id) {
                configs.push(ShardConfig::Remote {
                    shard_id,
                    addr: addr.clone(),
                });
            } else {
                // Shard not found locally and no remote address known
                // This might happen during cluster startup - we'll skip it
                debug!(
                    shard_id = %shard_id,
                    "shard not available locally or remotely, skipping"
                );
            }
        }

        configs
    }

    /// Get the DataFusion session context
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Execute a SQL query and return a DataFrame
    pub async fn sql(&self, query: &str) -> DfResult<DataFrame> {
        self.ctx.sql(query).await
    }

    /// Get the EXPLAIN plan for a query
    pub async fn explain(&self, query: &str) -> DfResult<String> {
        explain_dataframe(&self.ctx, query).await
    }
}

/// Which table type this provider is for
#[derive(Debug, Clone, Copy)]
enum TableKind {
    Jobs,
    Queues,
}

/// A TableProvider that spans multiple shards in the cluster.
/// Each shard becomes a partition in the execution plan.
///
/// Shard configurations are computed dynamically at scan time to ensure
/// queries always use the current cluster state.
struct ClusterTableProvider {
    schema: SchemaRef,
    /// Factory for accessing local shards
    factory: Arc<ShardFactory>,
    /// Optional coordinator for discovering remote shards
    coordinator: Option<Arc<dyn Coordinator>>,
    table_kind: TableKind,
}

impl std::fmt::Debug for ClusterTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTableProvider")
            .field("table_kind", &self.table_kind)
            .finish()
    }
}

impl std::fmt::Debug for ShardConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardConfig::Local(shard_id, shard) => {
                write!(f, "Local({}, {})", shard_id, shard.name())
            }
            ShardConfig::Remote { shard_id, addr } => {
                write!(f, "Remote(shard={}, addr={})", shard_id, addr)
            }
        }
    }
}

impl ClusterTableProvider {
    fn new(
        schema: SchemaRef,
        factory: Arc<ShardFactory>,
        coordinator: Option<Arc<dyn Coordinator>>,
        table_kind: TableKind,
    ) -> Self {
        Self {
            schema,
            factory,
            coordinator,
            table_kind,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for ClusterTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn CatalogSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Build shard configs dynamically to pick up current cluster state
        let shard_configs =
            ClusterQueryEngine::build_shard_configs(&self.factory, self.coordinator.as_ref()).await;

        // Handle projection
        let (output_schema, projection_indices) = match projection {
            Some(p) => (SchemaRef::new(self.schema.project(p)?), Some(p.clone())),
            None => (Arc::clone(&self.schema), None),
        };

        Ok(Arc::new(ClusterExecutionPlan::new(
            output_schema,
            shard_configs,
            self.table_kind,
            filters.to_vec(),
            limit,
            projection_indices,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Pass all filters through - we'll push them down to each shard
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// Execution plan that queries multiple shards in parallel.
/// Each shard is a separate partition, allowing DataFusion to properly
/// aggregate results across all partitions.
#[derive(Debug)]
struct ClusterExecutionPlan {
    /// Output schema (projected if applicable)
    schema: SchemaRef,
    shard_configs: Vec<ShardConfig>,
    table_kind: TableKind,
    filters: Vec<Expr>,
    limit: Option<usize>,
    /// Projection indices to apply to remote results (None = no projection)
    projection: Option<Vec<usize>>,
    plan_properties: PlanProperties,
}

impl ClusterExecutionPlan {
    fn new(
        schema: SchemaRef,
        shard_configs: Vec<ShardConfig>,
        table_kind: TableKind,
        filters: Vec<Expr>,
        limit: Option<usize>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let num_partitions = shard_configs.len().max(1);
        let eq = EquivalenceProperties::new(schema.clone());
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            schema,
            shard_configs,
            table_kind,
            filters,
            limit,
            projection,
            plan_properties: props,
        }
    }
}

impl ExecutionPlan for ClusterExecutionPlan {
    fn name(&self) -> &str {
        "ClusterExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !new_children.is_empty() {
            return Err(DataFusionError::Execution(
                "ClusterExecutionPlan does not support children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition >= self.shard_configs.len() {
            // Return empty stream for invalid partition
            let schema = self.schema.clone();
            let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(1);
            drop(tx); // Close immediately to signal empty
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                ReceiverStream::new(rx),
            )));
        }

        let shard_config = self.shard_configs[partition].clone();
        let schema = self.schema.clone();
        let table_kind = self.table_kind;
        let filters = self.filters.clone();
        let limit = self.limit;
        let projection = self.projection.clone();
        let batch_size = context.session_config().batch_size();

        let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(4);

        tokio::spawn(async move {
            match shard_config {
                ShardConfig::Local(_shard_id, shard) => {
                    // Create the appropriate scanner for this table type
                    let scanner: ScannerRef = match table_kind {
                        TableKind::Jobs => Arc::new(JobsScanner::new(Arc::clone(&shard))),
                        TableKind::Queues => Arc::new(QueuesScanner::new(Arc::clone(&shard))),
                    };

                    // Execute the scan - scanner handles projection via schema
                    let mut stream = scanner.scan(schema, &filters, batch_size, limit);

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(batch) => {
                                if tx.send(Ok(batch)).await.is_err() {
                                    break; // Receiver dropped
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                }
                ShardConfig::Remote { shard_id, addr } => {
                    // Query remote shard via gRPC with Arrow IPC (returns full schema)
                    match query_remote_shard_batches(&shard_id, &addr, table_kind, &filters, limit)
                        .await
                    {
                        Ok(batches) => {
                            for batch in batches {
                                // Apply projection if needed (remote returns all columns)
                                let output_batch = if let Some(ref proj) = projection {
                                    match batch.project(proj) {
                                        Ok(b) => b,
                                        Err(e) => {
                                            let _ = tx
                                                .send(Err(DataFusionError::ArrowError(
                                                    Box::new(e),
                                                    None,
                                                )))
                                                .await;
                                            break;
                                        }
                                    }
                                } else {
                                    batch
                                };

                                if tx.send(Ok(output_batch)).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                        }
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            ReceiverStream::new(rx),
        )))
    }

    fn statistics(&self) -> DfResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

impl DisplayAs for ClusterExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ClusterExecutionPlan: partitions={}, table={:?}",
                    self.shard_configs.len(),
                    self.table_kind
                )
            }
            DisplayFormatType::TreeRender => write!(f, "ClusterExecutionPlan"),
        }
    }
}

/// Query a remote shard via gRPC and return all batches.
///
/// Includes retry logic for:
/// - UNAVAILABLE: The target node is acquiring the shard, retry with backoff
/// - NOT_FOUND with redirect: The shard moved, retry to the new address
async fn query_remote_shard_batches(
    shard_id: &ShardId,
    addr: &str,
    table_kind: TableKind,
    filters: &[Expr],
    limit: Option<usize>,
) -> DfResult<Vec<RecordBatch>> {
    // Build SQL query from filters
    let table_name = match table_kind {
        TableKind::Jobs => "jobs",
        TableKind::Queues => "queues",
    };

    let sql = build_sql_query(table_name, filters, limit);

    // Retry configuration
    const MAX_RETRIES: u32 = 5;
    const INITIAL_BACKOFF_MS: u64 = 50;
    const MAX_BACKOFF_MS: u64 = 2000;

    let mut current_addr = addr.to_string();
    let mut attempt = 0u32;

    loop {
        attempt += 1;

        // Ensure address has http:// scheme for gRPC connection
        let full_addr =
            if current_addr.starts_with("http://") || current_addr.starts_with("https://") {
                current_addr.clone()
            } else {
                format!("http://{}", current_addr)
            };
        debug!(shard_id = %shard_id, addr = %full_addr, sql = %sql, attempt, "querying remote shard");

        // Connect to remote node
        let channel = match Channel::from_shared(full_addr.clone()) {
            Ok(c) => c,
            Err(e) => {
                return Err(DataFusionError::External(
                    format!("invalid address '{}': {}", full_addr, e).into(),
                ));
            }
        };

        let channel = match channel.connect().await {
            Ok(c) => c,
            Err(e) => {
                if attempt >= MAX_RETRIES {
                    return Err(DataFusionError::External(
                        format!(
                            "failed to connect to shard {} at '{}' after {} attempts: {}",
                            shard_id, full_addr, attempt, e
                        )
                        .into(),
                    ));
                }
                // Retry with backoff for connection errors
                let backoff = std::cmp::min(
                    INITIAL_BACKOFF_MS * (1 << attempt.saturating_sub(1)),
                    MAX_BACKOFF_MS,
                );
                warn!(
                    shard_id = %shard_id,
                    addr = %full_addr,
                    attempt,
                    backoff_ms = backoff,
                    error = %e,
                    "connection failed, retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                continue;
            }
        };

        let mut client = SiloClient::new(channel);

        // Make streaming query
        let request = QueryArrowRequest {
            shard: shard_id.to_string(),
            sql: sql.clone(),
            tenant: None,
        };

        let response = match client.query_arrow(request).await {
            Ok(r) => r,
            Err(status) => {
                // Check if we should retry based on the error type
                let should_retry = match status.code() {
                    tonic::Code::Unavailable => {
                        // Target node is acquiring the shard - retry with backoff
                        debug!(
                            shard_id = %shard_id,
                            addr = %full_addr,
                            attempt,
                            "shard unavailable (acquisition in progress), will retry"
                        );
                        true
                    }
                    tonic::Code::NotFound => {
                        // Check for redirect metadata
                        let metadata = status.metadata();
                        if let Some(new_addr) =
                            metadata.get(crate::server::SHARD_OWNER_ADDR_METADATA_KEY)
                        {
                            if let Ok(new_addr_str) = new_addr.to_str() {
                                // Redirect to new owner
                                debug!(
                                    shard_id = %shard_id,
                                    old_addr = %full_addr,
                                    new_addr = %new_addr_str,
                                    attempt,
                                    "shard moved, redirecting"
                                );
                                current_addr = new_addr_str.to_string();
                                true
                            } else {
                                false
                            }
                        } else {
                            // No redirect info - shard truly not found
                            false
                        }
                    }
                    _ => false,
                };

                if should_retry && attempt < MAX_RETRIES {
                    let backoff = std::cmp::min(
                        INITIAL_BACKOFF_MS * (1 << attempt.saturating_sub(1)),
                        MAX_BACKOFF_MS,
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    continue;
                }

                return Err(DataFusionError::External(Box::new(status)));
            }
        };

        let mut stream = response.into_inner();
        let mut all_batches = Vec::new();

        // Process Arrow IPC messages
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            // Deserialize Arrow IPC to RecordBatches
            let batches = ipc_to_batches_only(&msg.ipc_data)?;
            all_batches.extend(batches);
        }

        return Ok(all_batches);
    }
}

/// Build a SQL query string from filters and limit.
/// This reconstructs the query to send to remote shards.
///
/// Uses DataFusion's SQL unparser to properly convert expressions to standard SQL,
/// avoiding issues with internal representations like `utf8('value')`.
fn build_sql_query(table_name: &str, filters: &[Expr], limit: Option<usize>) -> String {
    let mut sql = format!("SELECT * FROM {}", table_name);

    if !filters.is_empty() {
        let unparser = Unparser::default().with_pretty(true);
        let filter_strs: Vec<String> = filters
            .iter()
            .filter_map(|f| {
                unparser
                    .expr_to_sql(f)
                    .ok()
                    .map(|sql_expr| sql_expr.to_string())
            })
            .collect();

        if !filter_strs.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&filter_strs.join(" AND "));
        }
    }

    if let Some(lim) = limit {
        sql.push_str(&format!(" LIMIT {}", lim));
    }

    sql
}
