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

use datafusion::arrow::array::{Array, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session as CatalogSession;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::DataFrame;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::arrow_ipc::ipc_to_batches_only;
use crate::coordination::Coordinator;
use crate::factory::ShardFactory;
use crate::job_store_shard::JobStoreShard;
use crate::pb::silo_client::SiloClient;
use crate::pb::QueryArrowRequest;
use crate::query::{explain_dataframe, JobsScanner, QueuesScanner, ScannerRef};

/// Extend a base schema with a shard_id column at the beginning
fn schema_with_shard_id(base_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> =
        vec![Arc::new(Field::new("shard_id", DataType::UInt32, false))];
    fields.extend(base_schema.fields().iter().cloned());
    Arc::new(Schema::new(fields))
}

/// Add a shard_id column to a record batch
fn add_shard_id_to_batch(
    batch: &RecordBatch,
    shard_id: u32,
    output_schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let num_rows = batch.num_rows();
    let shard_id_array = Arc::new(UInt32Array::from(vec![shard_id; num_rows]));

    let mut columns: Vec<Arc<dyn Array>> = vec![shard_id_array];
    columns.extend(batch.columns().iter().cloned());

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

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
    Local(Arc<JobStoreShard>),
    Remote { shard_id: u32, addr: String },
}

impl ClusterQueryEngine {
    /// Create a new cluster-wide query engine.
    ///
    /// # Arguments
    /// * `factory` - The shard factory for accessing local shards
    /// * `coordinator` - Optional coordinator for discovering remote shards
    /// * `num_shards` - Total number of shards in the cluster
    pub async fn new(
        factory: Arc<ShardFactory>,
        coordinator: Option<Arc<dyn Coordinator>>,
        num_shards: u32,
    ) -> DfResult<Self> {
        let ctx = SessionContext::new();

        // Build shard configuration - determine which shards are local vs remote
        let shard_configs =
            Self::build_shard_configs(&factory, coordinator.as_ref(), num_shards).await;

        // Register cluster-wide jobs table with shard_id column
        let jobs_base_schema = JobsScanner::base_schema();
        let jobs_schema = schema_with_shard_id(&jobs_base_schema);
        let jobs_provider = Arc::new(ClusterTableProvider::new(
            jobs_schema,
            jobs_base_schema,
            shard_configs.clone(),
            TableKind::Jobs,
        ));
        ctx.register_table("jobs", jobs_provider)?;

        // Register cluster-wide queues table with shard_id column
        let queues_base_schema = QueuesScanner::base_schema();
        let queues_schema = schema_with_shard_id(&queues_base_schema);
        let queues_provider = Arc::new(ClusterTableProvider::new(
            queues_schema,
            queues_base_schema,
            shard_configs,
            TableKind::Queues,
        ));
        ctx.register_table("queues", queues_provider)?;

        Ok(Self { ctx })
    }

    /// Build shard configurations by checking which shards are local and which are remote
    async fn build_shard_configs(
        factory: &ShardFactory,
        coordinator: Option<&Arc<dyn Coordinator>>,
        num_shards: u32,
    ) -> Vec<ShardConfig> {
        let mut configs = Vec::with_capacity(num_shards as usize);

        // Get remote shard addresses if we have a coordinator
        let remote_addrs: HashMap<u32, String> = if let Some(coord) = coordinator {
            match coord.get_shard_owner_map().await {
                Ok(map) => map.shard_to_addr,
                Err(e) => {
                    warn!(error = %e, "failed to get shard owner map, assuming all shards are local");
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };

        for shard_id in 0..num_shards {
            let shard_name = shard_id.to_string();
            if let Some(local_shard) = factory.get(&shard_name) {
                configs.push(ShardConfig::Local(local_shard));
            } else if let Some(addr) = remote_addrs.get(&shard_id) {
                configs.push(ShardConfig::Remote {
                    shard_id,
                    addr: addr.clone(),
                });
            } else {
                // Shard not found locally and no remote address known
                // This might happen during cluster startup - we'll skip it
                debug!(
                    shard_id,
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
#[derive(Debug)]
struct ClusterTableProvider {
    /// Schema including shard_id column
    schema: SchemaRef,
    /// Base schema without shard_id (used for underlying scanners)
    base_schema: SchemaRef,
    shard_configs: Vec<ShardConfig>,
    table_kind: TableKind,
}

impl std::fmt::Debug for ShardConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardConfig::Local(shard) => write!(f, "Local({})", shard.name()),
            ShardConfig::Remote { shard_id, addr } => {
                write!(f, "Remote(shard={}, addr={})", shard_id, addr)
            }
        }
    }
}

impl ClusterTableProvider {
    fn new(
        schema: SchemaRef,
        base_schema: SchemaRef,
        shard_configs: Vec<ShardConfig>,
        table_kind: TableKind,
    ) -> Self {
        Self {
            schema,
            base_schema,
            shard_configs,
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
        // Handle projection: we need to track if shard_id is requested and which
        // base columns are requested
        let (output_schema, include_shard_id, base_projection) = match projection {
            Some(p) => {
                let output_schema = SchemaRef::new(self.schema.project(p)?);
                // Check if shard_id (column 0) is in the projection
                let include_shard_id = p.contains(&0);
                // Convert projection indices: subtract 1 for columns after shard_id
                let base_proj: Vec<usize> = p.iter().filter(|&&i| i > 0).map(|&i| i - 1).collect();
                let base_projection = if base_proj.is_empty() {
                    None
                } else {
                    Some(base_proj)
                };
                (output_schema, include_shard_id, base_projection)
            }
            None => (Arc::clone(&self.schema), true, None),
        };

        Ok(Arc::new(ClusterExecutionPlan::new(
            output_schema,
            Arc::clone(&self.base_schema),
            self.shard_configs.clone(),
            self.table_kind,
            filters.to_vec(),
            limit,
            include_shard_id,
            base_projection,
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
    /// Output schema (may include shard_id)
    output_schema: SchemaRef,
    /// Base schema without shard_id (for underlying scanners)
    base_schema: SchemaRef,
    shard_configs: Vec<ShardConfig>,
    table_kind: TableKind,
    filters: Vec<Expr>,
    limit: Option<usize>,
    /// Whether to include shard_id in output
    include_shard_id: bool,
    /// Projection for base schema columns (None = all columns)
    base_projection: Option<Vec<usize>>,
    plan_properties: PlanProperties,
}

impl ClusterExecutionPlan {
    fn new(
        output_schema: SchemaRef,
        base_schema: SchemaRef,
        shard_configs: Vec<ShardConfig>,
        table_kind: TableKind,
        filters: Vec<Expr>,
        limit: Option<usize>,
        include_shard_id: bool,
        base_projection: Option<Vec<usize>>,
    ) -> Self {
        let num_partitions = shard_configs.len().max(1);
        let eq = EquivalenceProperties::new(output_schema.clone());
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            output_schema,
            base_schema,
            shard_configs,
            table_kind,
            filters,
            limit,
            include_shard_id,
            base_projection,
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
        self.output_schema.clone()
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
            let schema = self.output_schema.clone();
            let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(1);
            drop(tx); // Close immediately to signal empty
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                ReceiverStream::new(rx),
            )));
        }

        let shard_config = self.shard_configs[partition].clone();
        let output_schema = self.output_schema.clone();
        let output_schema_for_stream = output_schema.clone();
        let base_schema = self.base_schema.clone();
        let table_kind = self.table_kind;
        let filters = self.filters.clone();
        let limit = self.limit;
        let include_shard_id = self.include_shard_id;
        let base_projection = self.base_projection.clone();
        let batch_size = context.session_config().batch_size();

        // Get the shard_id for this partition
        let shard_id: u32 = match &shard_config {
            ShardConfig::Local(shard) => shard.name().parse().unwrap_or(partition as u32),
            ShardConfig::Remote { shard_id, .. } => *shard_id,
        };

        let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(4);

        tokio::spawn(async move {
            match shard_config {
                ShardConfig::Local(shard) => {
                    // Create the appropriate scanner for this table type
                    let scanner: ScannerRef = match table_kind {
                        TableKind::Jobs => Arc::new(JobsScanner::new(Arc::clone(&shard))),
                        TableKind::Queues => Arc::new(QueuesScanner::new(Arc::clone(&shard))),
                    };

                    // Execute the scan with base schema (scanner doesn't know about shard_id)
                    let mut stream = scanner.scan(base_schema.clone(), &filters, batch_size, limit);

                    while let Some(result) = stream.next().await {
                        let batch = match result {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        };

                        // Apply base projection if needed
                        let batch = if let Some(ref proj) = base_projection {
                            match batch.project(proj) {
                                Ok(b) => b,
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(DataFusionError::ArrowError(Box::new(e), None)))
                                        .await;
                                    break;
                                }
                            }
                        } else {
                            batch
                        };

                        // Add shard_id column if requested
                        let output_batch = if include_shard_id {
                            match add_shard_id_to_batch(&batch, shard_id, &output_schema) {
                                Ok(b) => b,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    break;
                                }
                            }
                        } else {
                            batch
                        };

                        if tx.send(Ok(output_batch)).await.is_err() {
                            break; // Receiver dropped
                        }
                    }
                }
                ShardConfig::Remote { shard_id, addr } => {
                    // Query remote shard via gRPC with Arrow IPC
                    match query_remote_shard_batches(shard_id, &addr, table_kind, &filters, limit)
                        .await
                    {
                        Ok(batches) => {
                            for batch in batches {
                                // Apply base projection if needed
                                let batch = if let Some(ref proj) = base_projection {
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

                                // Add shard_id column if requested
                                let output_batch = if include_shard_id {
                                    match add_shard_id_to_batch(&batch, shard_id, &output_schema) {
                                        Ok(b) => b,
                                        Err(e) => {
                                            let _ = tx.send(Err(e)).await;
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
            output_schema_for_stream,
            ReceiverStream::new(rx),
        )))
    }

    fn statistics(&self) -> DfResult<Statistics> {
        Ok(Statistics::new_unknown(&self.output_schema))
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

/// Query a remote shard via gRPC and return all batches
async fn query_remote_shard_batches(
    shard_id: u32,
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
    debug!(shard_id, addr, sql = %sql, "querying remote shard");

    // Connect to remote node
    let channel = Channel::from_shared(addr.to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .connect()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut client = SiloClient::new(channel);

    // Make streaming query
    let request = QueryArrowRequest {
        shard: shard_id,
        sql,
        tenant: None,
    };

    let response = client
        .query_arrow(request)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

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

    Ok(all_batches)
}

/// Build a SQL query string from filters and limit.
/// This reconstructs the query to send to remote shards.
fn build_sql_query(table_name: &str, filters: &[Expr], limit: Option<usize>) -> String {
    let mut sql = format!("SELECT * FROM {}", table_name);

    if !filters.is_empty() {
        let filter_strs: Vec<String> = filters.iter().map(|f| format!("{}", f)).collect();
        sql.push_str(" WHERE ");
        sql.push_str(&filter_strs.join(" AND "));
    }

    if let Some(lim) = limit {
        sql.push_str(&format!(" LIMIT {}", lim));
    }

    sql
}
