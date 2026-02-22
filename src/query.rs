use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, UInt8Array, UInt32Array, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session as CatalogSession;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::DataFrame;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::job::{JobStatus, JobView};
use crate::job_store_shard::JobStoreShard;

/// Shared utility to get the EXPLAIN plan for a query.
/// Used by both ShardQueryEngine and ClusterQueryEngine.
pub async fn explain_dataframe(ctx: &SessionContext, query: &str) -> DfResult<String> {
    let df = ctx.sql(query).await?;
    let explain_df = df.explain(false, false)?;
    let batches = explain_df.collect().await?;

    let mut output = String::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            for col in 0..batch.num_columns() {
                if let Some(arr) = batch.column(col).as_any().downcast_ref::<StringArray>()
                    && !arr.is_null(row)
                {
                    output.push_str(arr.value(row));
                    output.push('\n');
                }
            }
        }
    }
    Ok(output)
}

/// Represents a query engine over a single `JobStoreShard` using Apache DataFusion.
///
/// This is the low-level query engine used by gRPC handlers to query individual shards.
/// For cluster-wide queries, use `ClusterQueryEngine` instead.
pub struct ShardQueryEngine {
    ctx: SessionContext,
}

/// Information about filters pushed down to the scan
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushedFilters {
    pub filters: Vec<String>,
}

impl std::fmt::Debug for ShardQueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ShardQueryEngine")
    }
}

impl ShardQueryEngine {
    pub fn new(shard: Arc<JobStoreShard>, table_name: &str) -> DfResult<Self> {
        let ctx = SessionContext::new();

        // Register jobs table
        let jobs_schema = JobsScanner::base_schema();
        let jobs_scanner: ScannerRef = Arc::new(JobsScanner {
            shard: Arc::clone(&shard),
        });
        let jobs_provider = Arc::new(SiloTableProvider::new(jobs_schema, jobs_scanner));
        ctx.register_table(table_name, jobs_provider)?;

        // Register queues table for concurrency queue data
        let queues_schema = QueuesScanner::base_schema();
        let queues_scanner: ScannerRef = Arc::new(QueuesScanner {
            shard: Arc::clone(&shard),
        });
        let queues_provider = Arc::new(SiloTableProvider::new(queues_schema, queues_scanner));
        ctx.register_table("queues", queues_provider)?;

        Ok(Self { ctx })
    }

    pub async fn sql(&self, query: &str) -> DfResult<DataFrame> {
        self.ctx.sql(query).await
    }

    /// Get the EXPLAIN plan for a query to inspect optimization strategies
    pub async fn explain(&self, query: &str) -> DfResult<String> {
        explain_dataframe(&self.ctx, query).await
    }

    /// Get the physical execution plan for a query to inspect what filters were pushed down
    ///
    /// This is useful for testing to verify that predicate pushdown is working correctly.
    ///
    /// # Example
    /// ```ignore
    /// let plan = sql.get_physical_plan("SELECT * FROM jobs WHERE id = 'foo'").await?;
    /// let filters = ShardQueryEngine::extract_pushed_filters(&plan).expect("filters");
    /// assert!(filters.filters.iter().any(|f| f.contains("id")));
    /// ```
    pub async fn get_physical_plan(&self, query: &str) -> DfResult<Arc<dyn ExecutionPlan>> {
        let df = self.ctx.sql(query).await?;
        df.create_physical_plan().await
    }

    /// Extract pushed down filters from a physical plan (helper for testing)
    ///
    /// Returns the filters that were pushed down to our custom scan operator.
    /// This lets you verify that DataFusion is properly utilizing predicate pushdown
    /// instead of doing full table scans.
    pub fn extract_pushed_filters(plan: &Arc<dyn ExecutionPlan>) -> Option<PushedFilters> {
        // Try to downcast to our SiloExecutionPlan
        if let Some(silo_plan) = plan.as_any().downcast_ref::<SiloExecutionPlan>() {
            let filters: Vec<String> = silo_plan.filters.iter().map(|f| format!("{}", f)).collect();
            return Some(PushedFilters { filters });
        }

        // Recursively check children
        for child in plan.children() {
            if let Some(filters) = Self::extract_pushed_filters(child) {
                return Some(filters);
            }
        }

        None
    }

    /// Extract the actual pushed down filter expressions from a physical plan.
    ///
    /// Returns the `Expr` objects that DataFusion passed to our scan operator after
    /// query optimization. Used with `parse_jobs_scan_strategy` in tests to verify
    /// that the correct index-backed scan path is selected for a given query.
    pub fn extract_pushed_filter_exprs(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<Expr>> {
        if let Some(silo_plan) = plan.as_any().downcast_ref::<SiloExecutionPlan>() {
            return Some(silo_plan.filters.clone());
        }
        for child in plan.children() {
            if let Some(exprs) = Self::extract_pushed_filter_exprs(child) {
                return Some(exprs);
            }
        }
        None
    }
}

/// Scan trait for table scanners.
/// Implementors provide streaming access to table data with filter pushdown.
pub trait Scan: std::fmt::Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;

    /// Describe the scan strategy for EXPLAIN output. Returns a human-readable
    /// description of what index/scan path will be used for the given filters.
    fn describe(&self, _filters: &[Expr], _limit: Option<usize>) -> String {
        "CustomScan".to_string()
    }
}

/// Reference to a scanner implementing the Scan trait
pub type ScannerRef = Arc<dyn Scan>;

// Implementation of the DataFusion TableProvider trait for all our scanners.
#[derive(Debug)]
struct SiloTableProvider {
    schema: SchemaRef,
    scanner: ScannerRef,
}

impl SiloTableProvider {
    fn new(schema: SchemaRef, scanner: ScannerRef) -> Self {
        Self { schema, scanner }
    }
    fn schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[async_trait::async_trait]
impl TableProvider for SiloTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema_ref()
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
        let projected_schema = match projection {
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema_ref(),
        };
        Ok(Arc::new(SiloExecutionPlan::new(
            projected_schema,
            filters,
            limit,
            Arc::clone(&self.scanner),
        )))
    }

    // Tell data fusion we support all filter pushdowns, but that it still needs to filter on its side after we've scanned.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // All filters are Inexact - we use them for scan optimization (index lookups)
        // but DataFusion still applies them as post-filters for correctness.
        // This is important because our scan only handles one metadata filter at a time,
        // and DataFusion may call this method multiple times with different filter subsets.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

#[derive(Debug, Clone)]
struct SiloExecutionPlan {
    projected_schema: SchemaRef,
    scanner: ScannerRef,
    limit: Option<usize>,
    filters: Vec<Expr>,
    plan_properties: PlanProperties,
}

impl SiloExecutionPlan {
    fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        scanner: ScannerRef,
    ) -> Self {
        let eq = EquivalenceProperties::new(projected_schema.clone());
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            projected_schema,
            scanner,
            limit,
            filters: filters.to_vec(),
            plan_properties: props,
        }
    }
}

impl ExecutionPlan for SiloExecutionPlan {
    fn name(&self) -> &str {
        "SiloExecutionPlan"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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
                "SiloExecutionPlan does not support children".to_string(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        Ok(self.scanner.scan(
            self.projected_schema.clone(),
            &self.filters,
            batch_size,
            self.limit,
        ))
    }
    fn statistics(&self) -> DfResult<Statistics> {
        Ok(Statistics::default())
    }
}

impl DisplayAs for SiloExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let desc = self.scanner.describe(&self.filters, self.limit);
                write!(f, "SiloExecutionPlan: {}", desc)
            }
        }
    }
}

/// Represents the scan strategy chosen for a jobs query based on pushed-down filters.
/// This is the resolved dispatch decision: which index/scan method will be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobsScanStrategy {
    /// Lookup a single job by exact ID
    ExactId { tenant: Option<String>, id: String },
    /// Scan the metadata index for an exact key=value match
    MetadataExact {
        tenant: Option<String>,
        key: String,
        value: String,
    },
    /// Scan the metadata index for a key with a value prefix
    MetadataPrefix {
        tenant: Option<String>,
        key: String,
        prefix: String,
    },
    /// Scan the status/time index for a specific status
    Status {
        tenant: Option<String>,
        status: QueryStatusFilter,
    },
    /// Full scan (no index-backed filter)
    FullScan { tenant: Option<String> },
}

impl std::fmt::Display for JobsScanStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobsScanStrategy::ExactId { tenant, id } => {
                write!(f, "ExactId(tenant={:?}, id={:?})", tenant, id)
            }
            JobsScanStrategy::MetadataExact { tenant, key, value } => {
                write!(
                    f,
                    "MetadataExact(tenant={:?}, key={:?}, value={:?})",
                    tenant, key, value
                )
            }
            JobsScanStrategy::MetadataPrefix {
                tenant,
                key,
                prefix,
            } => {
                write!(
                    f,
                    "MetadataPrefix(tenant={:?}, key={:?}, prefix={:?})",
                    tenant, key, prefix
                )
            }
            JobsScanStrategy::Status { tenant, status } => {
                write!(f, "Status(tenant={:?}, status={})", tenant, status)
            }
            JobsScanStrategy::FullScan { tenant } => {
                write!(f, "FullScan(tenant={:?})", tenant)
            }
        }
    }
}

/// Parse DataFusion filter expressions into a scan strategy.
/// This determines which index-backed scan path will be used.
pub fn parse_jobs_scan_strategy(filters: &[Expr]) -> JobsScanStrategy {
    let mut tenant_filter: Option<String> = None;
    let mut status_filter: Option<QueryStatusFilter> = None;
    let mut id_filter: Option<String> = None;
    let mut metadata_filter: Option<(String, String)> = None;
    let mut metadata_prefix_filter: Option<(String, String)> = None;

    for f in filters {
        if let Some((col, val)) = parse_eq_filter(f) {
            match col.as_str() {
                "tenant" => tenant_filter = Some(val),
                "status_kind" => status_filter = parse_status_kind(&val),
                "id" => id_filter = Some(val),
                _ => {}
            }
        } else if metadata_filter.is_none() && metadata_prefix_filter.is_none() {
            if let Some((k, v)) = parse_metadata_eq_filter(f) {
                metadata_filter = Some((k, v));
            } else if let Some((k, v)) = parse_metadata_contains_filter(f) {
                metadata_filter = Some((k, v));
            } else if let Some((k, v)) = parse_metadata_prefix_filter(f) {
                metadata_prefix_filter = Some((k, v));
            }
        }
    }

    if let Some(id) = id_filter {
        JobsScanStrategy::ExactId {
            tenant: tenant_filter,
            id,
        }
    } else if let Some((key, value)) = metadata_filter {
        JobsScanStrategy::MetadataExact {
            tenant: tenant_filter,
            key,
            value,
        }
    } else if let Some((key, prefix)) = metadata_prefix_filter {
        JobsScanStrategy::MetadataPrefix {
            tenant: tenant_filter,
            key,
            prefix,
        }
    } else if let Some(status) = status_filter {
        JobsScanStrategy::Status {
            tenant: tenant_filter,
            status,
        }
    } else {
        JobsScanStrategy::FullScan {
            tenant: tenant_filter,
        }
    }
}

/// Scanner for the jobs table - reads job data from a single shard.
pub struct JobsScanner {
    pub(crate) shard: Arc<JobStoreShard>,
}

impl JobsScanner {
    /// Create a new JobsScanner for the given shard
    pub fn new(shard: Arc<JobStoreShard>) -> Self {
        Self { shard }
    }
}

impl std::fmt::Debug for JobsScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JobsScanner")
    }
}

impl JobsScanner {
    /// Get the base schema for the jobs table
    pub fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("shard_id", DataType::Utf8, false),
            Field::new("tenant", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
            Field::new("enqueue_time_ms", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("status_kind", DataType::Utf8, true),
            Field::new("status_changed_at_ms", DataType::Int64, true),
            Field::new("task_group", DataType::Utf8, false),
            Field::new("current_attempt", DataType::UInt32, true),
            Field::new("next_attempt_starts_after_ms", DataType::Int64, true),
            // Arbitrary key/value metadata as Arrow Map<Utf8, Utf8>
            Field::new(
                "metadata",
                DataType::Map(
                    // entries struct: { key: Utf8 (non-null), value: Utf8 (nullable) }
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]))
    }
}

/// Captures what columns the DataFusion projection requires, so we can pick
/// the most efficient scan path and skip fetches we don't need.
struct ProjectionNeeds {
    /// Needs fields only in job_info: priority, enqueue_time_ms, payload, task_group, metadata
    need_job_info: bool,
    /// Needs fields available from the status/time index key: status_kind, status_changed_at_ms
    need_status_index_fields: bool,
    /// Needs fields only in the status record: current_attempt, next_attempt_starts_after_ms
    need_status_point_lookup: bool,
    /// Fast path: scan status/time index directly, no point-lookups required
    use_status_index_path: bool,
    /// ExactId with a tenant: must verify existence even without job_info columns
    needs_existence_check: bool,
}

impl ProjectionNeeds {
    fn need_any_status(&self) -> bool {
        self.need_status_index_fields || self.need_status_point_lookup
    }
}

fn analyze_projection(projection: &SchemaRef, strategy: &JobsScanStrategy) -> ProjectionNeeds {
    let need_job_info = projection.fields().iter().any(|f| {
        matches!(
            f.name().as_str(),
            "priority" | "enqueue_time_ms" | "payload" | "task_group" | "metadata"
        )
    });
    // status_kind and status_changed_at_ms are encoded in the status/time index key, so
    // they can be read without an extra point-lookup into the status record.
    let need_status_index_fields = projection
        .fields()
        .iter()
        .any(|f| matches!(f.name().as_str(), "status_kind" | "status_changed_at_ms"));
    let need_status_point_lookup = projection.fields().iter().any(|f| {
        matches!(
            f.name().as_str(),
            "current_attempt" | "next_attempt_starts_after_ms"
        )
    });
    // When no job_info fields or status point-lookup fields are needed (including the
    // empty-projection case for COUNT(*)), scan the status/time index directly and skip
    // all get_jobs_batch / get_jobs_status_batch point-lookups.
    // Only valid for FullScan — other strategies already have a bounded pair list.
    let use_status_index_path = !need_job_info
        && !need_status_point_lookup
        && matches!(strategy, JobsScanStrategy::FullScan { .. });
    // ExactId with a tenant synthesises the pair without scanning the DB, so we must
    // verify existence via get_jobs_batch even when job_info columns aren't projected.
    let needs_existence_check = matches!(
        strategy,
        JobsScanStrategy::ExactId {
            tenant: Some(_),
            ..
        }
    );
    ProjectionNeeds {
        need_job_info,
        need_status_index_fields,
        need_status_point_lookup,
        use_status_index_path,
        needs_existence_check,
    }
}

impl Scan for JobsScanner {
    fn describe(&self, filters: &[Expr], limit: Option<usize>) -> String {
        let strategy = parse_jobs_scan_strategy(filters);
        format!("jobs[{}], limit={:?}", strategy, limit)
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let strategy = parse_jobs_scan_strategy(filters);
        let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(2);
        let shard = Arc::clone(&self.shard);
        let proj = Arc::clone(&projection);
        tokio::spawn(async move {
            if let Err(e) = stream_jobs(&shard, &proj, &strategy, batch_size, limit, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&projection),
            ReceiverStream::new(rx).map(|r| r),
        ))
    }
}

/// Top-level driver: picks the status-index fast path or the standard job-pairs path.
async fn stream_jobs(
    shard: &Arc<JobStoreShard>,
    projection: &SchemaRef,
    strategy: &JobsScanStrategy,
    batch_size: usize,
    limit: Option<usize>,
    tx: &mpsc::Sender<DfResult<RecordBatch>>,
) -> DfResult<()> {
    let needs = analyze_projection(projection, strategy);
    if needs.use_status_index_path {
        stream_via_status_index(shard, projection, strategy, batch_size, limit, tx).await
    } else {
        stream_via_job_pairs(shard, projection, strategy, &needs, batch_size, limit, tx).await
    }
}

/// Scans the status/time index and emits RecordBatches without any point-lookups.
/// Used when the projection only needs tenant, id, status_kind, or status_changed_at_ms.
async fn stream_via_status_index(
    shard: &Arc<JobStoreShard>,
    projection: &SchemaRef,
    strategy: &JobsScanStrategy,
    batch_size: usize,
    limit: Option<usize>,
    tx: &mpsc::Sender<DfResult<RecordBatch>>,
) -> DfResult<()> {
    let tenant = if let JobsScanStrategy::FullScan { tenant } = strategy {
        tenant.as_deref()
    } else {
        None
    };
    let indexed = collect_status_index_data(shard, tenant, limit).await?;
    let shard_id = shard.name().to_string();
    let mut sent = 0usize;
    let mut i = 0usize;
    while i < indexed.len() && limit.is_none_or(|l| sent < l) {
        let row_count = if let Some(l) = limit {
            batch_size
                .min(indexed.len() - i)
                .min(l.saturating_sub(sent))
        } else {
            batch_size.min(indexed.len() - i)
        };
        let chunk = &indexed[i..i + row_count];
        let batch = build_status_index_batch(projection, &shard_id, chunk)?;
        sent += batch.num_rows();
        if tx.send(Ok(batch)).await.is_err() {
            return Ok(());
        }
        i += row_count;
    }
    Ok(())
}

/// Gather all (tenant, job_id, status_kind, changed_at_ms) tuples from the status/time index.
async fn collect_status_index_data(
    shard: &JobStoreShard,
    tenant: Option<&str>,
    limit: Option<usize>,
) -> DfResult<Vec<(String, String, String, i64)>> {
    match tenant {
        Some(t) => shard
            .scan_jobs_with_status_kind(t, limit)
            .await
            .map(|v| {
                v.into_iter()
                    .map(|(id, sk, ts)| (t.to_string(), id, sk, ts))
                    .collect()
            })
            .map_err(|e| DataFusionError::Execution(e.to_string())),
        None => shard
            .scan_all_jobs_with_status_kind(limit)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string())),
    }
}

/// Build a RecordBatch for the status-index fast path from a chunk of index entries.
fn build_status_index_batch(
    projection: &SchemaRef,
    shard_id: &str,
    chunk: &[(String, String, String, i64)],
) -> DfResult<RecordBatch> {
    let n = chunk.len();
    if projection.fields().is_empty() {
        return make_empty_projection_batch(projection, n);
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(projection.fields().len());
    for f in projection.fields() {
        let col: ArrayRef = match f.name().as_str() {
            "shard_id" => Arc::new(StringArray::from(vec![shard_id; n])),
            "tenant" => Arc::new(StringArray::from(
                chunk
                    .iter()
                    .map(|(t, _, _, _)| t.as_str())
                    .collect::<Vec<_>>(),
            )),
            "id" => Arc::new(StringArray::from(
                chunk
                    .iter()
                    .map(|(_, id, _, _)| id.as_str())
                    .collect::<Vec<_>>(),
            )),
            "status_kind" => Arc::new(StringArray::from(
                chunk
                    .iter()
                    .map(|(_, _, sk, _)| Some(sk.as_str()))
                    .collect::<Vec<_>>(),
            )),
            "status_changed_at_ms" => Arc::new(Int64Array::from(
                chunk
                    .iter()
                    .map(|(_, _, _, ts)| Some(*ts))
                    .collect::<Vec<_>>(),
            )),
            _ => new_null_array(f.data_type(), n),
        };
        cols.push(col);
    }
    RecordBatch::try_new(Arc::clone(projection), cols)
        .map_err(|e| DataFusionError::Execution(e.to_string()))
}

/// Resolves (tenant, job_id) pairs from the appropriate index, then batch-fetches job data.
async fn stream_via_job_pairs(
    shard: &Arc<JobStoreShard>,
    projection: &SchemaRef,
    strategy: &JobsScanStrategy,
    needs: &ProjectionNeeds,
    batch_size: usize,
    limit: Option<usize>,
    tx: &mpsc::Sender<DfResult<RecordBatch>>,
) -> DfResult<()> {
    // Fast path: FullScan + need_job_info — use combined range scans to avoid the
    // double-read that would occur from scan_jobs (discards values) + get_jobs_batch
    // (re-reads same KVs). Status records are also fetched via a sequential range scan
    // instead of 79K+ random point-lookups.
    if needs.need_job_info && matches!(strategy, JobsScanStrategy::FullScan { .. }) {
        return stream_via_fullscan_range(
            shard, projection, strategy, needs, batch_size, limit, tx,
        )
        .await;
    }

    // Standard path: collect (tenant, job_id) pairs then batch-fetch.
    // Cap fetch batches so DataFusion can stop early for LIMIT queries.
    // Full scans pay nearly identical total I/O (same ops, more smaller batches).
    const POINT_LOOKUP_BATCH: usize = 256;
    let fetch_batch_size = batch_size.min(POINT_LOOKUP_BATCH);

    let job_pairs = collect_job_pairs(shard, strategy, limit).await?;
    let shard_id = shard.name().to_string();
    let mut sent = 0usize;
    let mut i = 0usize;
    while i < job_pairs.len() && limit.is_none_or(|l| sent < l) {
        let end = (i + fetch_batch_size).min(job_pairs.len());
        let batch_pairs = &job_pairs[i..end];
        let (jobs_map, status_map) = fetch_batch_data(shard, batch_pairs, needs).await?;
        // When we fetched job_info, use the map as the authoritative presence check.
        // Otherwise the index scan is the source of truth (LSM scans skip tombstones).
        let existing_pairs: Vec<&(String, String)> =
            if needs.need_job_info || needs.needs_existence_check {
                batch_pairs
                    .iter()
                    .filter(|(_, id)| jobs_map.contains_key(id))
                    .collect()
            } else {
                batch_pairs.iter().collect()
            };
        if !existing_pairs.is_empty() {
            let batch = build_job_pairs_batch(
                projection,
                &shard_id,
                &existing_pairs,
                &jobs_map,
                &status_map,
            )?;
            sent += batch.num_rows();
            if tx.send(Ok(batch)).await.is_err() {
                return Ok(());
            }
        }
        i = end;
    }
    Ok(())
}

/// Fast path for FullScan + need_job_info: runs combined range scans to read each KV
/// exactly once. Concurrently scans job_info and status ranges, builds in-memory maps,
/// then emits batches without any point-lookups.
async fn stream_via_fullscan_range(
    shard: &Arc<JobStoreShard>,
    projection: &SchemaRef,
    strategy: &JobsScanStrategy,
    needs: &ProjectionNeeds,
    batch_size: usize,
    limit: Option<usize>,
    tx: &mpsc::Sender<DfResult<RecordBatch>>,
) -> DfResult<()> {
    let tenant = if let JobsScanStrategy::FullScan { tenant } = strategy {
        tenant.as_deref()
    } else {
        return Ok(()); // unreachable: caller guards on FullScan
    };

    // Concurrently scan job_info values and (optionally) status records.
    let (jobs_result, status_result) = tokio::join!(
        async {
            match tenant {
                Some(t) => shard.scan_jobs_with_views(t, limit).await.map(|v| {
                    v.into_iter()
                        .map(|(id, view)| (t.to_string(), id, view))
                        .collect::<Vec<_>>()
                }),
                None => shard.scan_all_jobs_with_views(limit).await,
            }
            .map_err(|e: crate::job_store_shard::JobStoreShardError| {
                DataFusionError::Execution(e.to_string())
            })
        },
        async {
            if needs.need_any_status() {
                match tenant {
                    Some(t) => shard
                        .scan_jobs_status_records(t, limit)
                        .await
                        .map(|v| v.into_iter().collect::<HashMap<_, _>>()),
                    None => shard.scan_all_jobs_status_records(limit).await.map(|v| {
                        v.into_iter()
                            .map(|(_, id, s)| (id, s))
                            .collect::<HashMap<_, _>>()
                    }),
                }
                .map_err(|e: crate::job_store_shard::JobStoreShardError| {
                    DataFusionError::Execution(e.to_string())
                })
            } else {
                Ok(HashMap::new())
            }
        }
    );

    let jobs_with_tenant: Vec<(String, String, JobView)> = jobs_result?;
    let status_map: HashMap<String, JobStatus> = status_result?;

    // Build the (tenant, job_id) pairs and jobs_map from the range scan results.
    // Range scans skip tombstones, so all returned entries are live jobs.
    let job_pairs: Vec<(String, String)> = jobs_with_tenant
        .iter()
        .map(|(t, id, _)| (t.clone(), id.clone()))
        .collect();
    let jobs_map: HashMap<String, JobView> = jobs_with_tenant
        .into_iter()
        .map(|(_, id, view)| (id, view))
        .collect();

    let shard_id = shard.name().to_string();
    let mut sent = 0usize;
    let mut i = 0usize;
    while i < job_pairs.len() && limit.is_none_or(|l| sent < l) {
        let end = (i + batch_size).min(job_pairs.len());
        let batch_pairs: Vec<&(String, String)> = job_pairs[i..end].iter().collect();
        let batch =
            build_job_pairs_batch(projection, &shard_id, &batch_pairs, &jobs_map, &status_map)?;
        sent += batch.num_rows();
        if tx.send(Ok(batch)).await.is_err() {
            return Ok(());
        }
        i = end;
    }
    Ok(())
}

/// Dispatch to the appropriate index scan to resolve (tenant, job_id) pairs.
async fn collect_job_pairs(
    shard: &JobStoreShard,
    strategy: &JobsScanStrategy,
    limit: Option<usize>,
) -> DfResult<Vec<(String, String)>> {
    let result = match strategy {
        JobsScanStrategy::ExactId { tenant, id } => {
            if let Some(t) = tenant {
                Ok(vec![(t.clone(), id.clone())])
            } else {
                shard
                    .scan_all_jobs(limit)
                    .await
                    .map(|all| all.into_iter().filter(|(_, jid)| jid == id).collect())
            }
        }
        JobsScanStrategy::MetadataExact { tenant, key, value } => {
            if let Some(t) = tenant {
                shard
                    .scan_jobs_by_metadata(t, key, value, limit)
                    .await
                    .map(|v| v.into_iter().map(|id| (t.clone(), id)).collect())
            } else {
                shard.scan_all_jobs(limit).await
            }
        }
        JobsScanStrategy::MetadataPrefix {
            tenant,
            key,
            prefix,
        } => {
            if let Some(t) = tenant {
                shard
                    .scan_jobs_by_metadata_prefix(t, key, prefix, limit)
                    .await
                    .map(|v| v.into_iter().map(|id| (t.clone(), id)).collect())
            } else {
                shard.scan_all_jobs(limit).await
            }
        }
        JobsScanStrategy::Status { tenant, status } => {
            collect_job_pairs_by_status(shard, tenant.as_deref(), *status, limit).await
        }
        JobsScanStrategy::FullScan { tenant } => {
            if let Some(t) = tenant {
                shard
                    .scan_jobs(t, limit)
                    .await
                    .map(|v| v.into_iter().map(|id| (t.clone(), id)).collect())
            } else {
                shard.scan_all_jobs(limit).await
            }
        }
    };
    result.map_err(|e| DataFusionError::Execution(e.to_string()))
}

/// Resolve (tenant, job_id) pairs for status-filtered queries.
async fn collect_job_pairs_by_status(
    shard: &JobStoreShard,
    tenant: Option<&str>,
    status: QueryStatusFilter,
    limit: Option<usize>,
) -> Result<Vec<(String, String)>, crate::job_store_shard::JobStoreShardError> {
    let now_ms = crate::job_store_shard::helpers::now_epoch_ms();
    match (status, tenant) {
        (QueryStatusFilter::Waiting, Some(t)) => shard
            .scan_jobs_waiting(t, now_ms, limit)
            .await
            .map(|v| v.into_iter().map(|id| (t.to_string(), id)).collect()),
        (QueryStatusFilter::Waiting, None) => shard.scan_all_jobs_waiting(now_ms, limit).await,
        (QueryStatusFilter::FutureScheduled, Some(t)) => shard
            .scan_jobs_future_scheduled(t, now_ms, limit)
            .await
            .map(|v| v.into_iter().map(|id| (t.to_string(), id)).collect()),
        (QueryStatusFilter::FutureScheduled, None) => {
            shard.scan_all_jobs_future_scheduled(now_ms, limit).await
        }
        (QueryStatusFilter::Stored(kind), Some(t)) => shard
            .scan_jobs_by_status(t, kind, limit)
            .await
            .map(|v| v.into_iter().map(|id| (t.to_string(), id)).collect()),
        (QueryStatusFilter::Stored(kind), None) => shard.scan_all_jobs_by_status(kind, limit).await,
    }
}

/// Batch-fetch job_info and/or status records for the given pairs.
/// Only fetches what the projection actually needs.
async fn fetch_batch_data(
    shard: &JobStoreShard,
    pairs: &[(String, String)],
    needs: &ProjectionNeeds,
) -> DfResult<(HashMap<String, JobView>, HashMap<String, JobStatus>)> {
    let mut jobs_map: HashMap<String, JobView> = HashMap::new();
    let mut status_map: HashMap<String, JobStatus> = HashMap::new();
    if !needs.need_job_info && !needs.need_any_status() && !needs.needs_existence_check {
        return Ok((jobs_map, status_map));
    }
    // Group job IDs by tenant so we can issue one batch call per tenant.
    let mut by_tenant: HashMap<String, Vec<String>> = HashMap::new();
    for (tenant, job_id) in pairs {
        by_tenant
            .entry(tenant.clone())
            .or_default()
            .push(job_id.clone());
    }
    for (tenant, ids) in &by_tenant {
        let need_jobs = needs.need_job_info || needs.needs_existence_check;
        let need_status = needs.need_any_status();
        let (jobs_result, status_result) = tokio::join!(
            async {
                if need_jobs {
                    shard
                        .get_jobs_batch(tenant, ids)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                } else {
                    Ok(HashMap::new())
                }
            },
            async {
                if need_status {
                    shard
                        .get_jobs_status_batch(tenant, ids)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                } else {
                    Ok(HashMap::new())
                }
            }
        );
        jobs_map.extend(jobs_result?);
        status_map.extend(status_result?);
    }
    Ok((jobs_map, status_map))
}

/// Build a RecordBatch for the standard path, reading columns from jobs_map and status_map.
fn build_job_pairs_batch(
    projection: &SchemaRef,
    shard_id: &str,
    pairs: &[&(String, String)],
    jobs_map: &HashMap<String, JobView>,
    status_map: &HashMap<String, JobStatus>,
) -> DfResult<RecordBatch> {
    let n = pairs.len();
    if projection.fields().is_empty() {
        return make_empty_projection_batch(projection, n);
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(projection.fields().len());
    for f in projection.fields() {
        let col: ArrayRef = match f.name().as_str() {
            "shard_id" => Arc::new(StringArray::from(vec![shard_id; n])),
            "tenant" => Arc::new(StringArray::from(
                pairs.iter().map(|p| p.0.as_str()).collect::<Vec<_>>(),
            )),
            "id" => Arc::new(StringArray::from(
                pairs.iter().map(|p| p.1.as_str()).collect::<Vec<_>>(),
            )),
            "priority" => Arc::new(UInt8Array::from(
                pairs
                    .iter()
                    .map(|p| jobs_map.get(&p.1).map_or(0, |v| v.priority()))
                    .collect::<Vec<u8>>(),
            )),
            "enqueue_time_ms" => Arc::new(Int64Array::from(
                pairs
                    .iter()
                    .map(|p| jobs_map.get(&p.1).map_or(0, |v| v.enqueue_time_ms()))
                    .collect::<Vec<i64>>(),
            )),
            "payload" => Arc::new(StringArray::from(
                pairs
                    .iter()
                    .map(|p| {
                        jobs_map
                            .get(&p.1)
                            .and_then(|v| v.payload_as_json().ok().map(|j| j.to_string()))
                    })
                    .collect::<Vec<Option<String>>>(),
            )),
            "task_group" => Arc::new(StringArray::from(
                pairs
                    .iter()
                    .map(|p| {
                        jobs_map
                            .get(&p.1)
                            .map_or_else(String::new, |v| v.task_group().to_string())
                    })
                    .collect::<Vec<String>>(),
            )),
            "status_kind" => Arc::new(StringArray::from(
                pairs
                    .iter()
                    .map(|p| status_map.get(&p.1).map(display_status_kind))
                    .collect::<Vec<Option<String>>>(),
            )),
            "status_changed_at_ms" => Arc::new(Int64Array::from(
                pairs
                    .iter()
                    .map(|p| status_map.get(&p.1).map(|s| s.changed_at_ms))
                    .collect::<Vec<Option<i64>>>(),
            )),
            "current_attempt" => Arc::new(UInt32Array::from(
                pairs
                    .iter()
                    .map(|p| status_map.get(&p.1).and_then(|s| s.current_attempt))
                    .collect::<Vec<Option<u32>>>(),
            )),
            "next_attempt_starts_after_ms" => Arc::new(Int64Array::from(
                pairs
                    .iter()
                    .map(|p| {
                        status_map
                            .get(&p.1)
                            .and_then(|s| s.next_attempt_starts_after_ms)
                    })
                    .collect::<Vec<Option<i64>>>(),
            )),
            "metadata" => build_metadata_column(pairs, jobs_map)?,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unknown column {}",
                    other
                )));
            }
        };
        cols.push(col);
    }
    RecordBatch::try_new(Arc::clone(projection), cols)
        .map_err(|e| DataFusionError::Execution(e.to_string()))
}

/// Build the Arrow `Map<Utf8, Utf8>` column for job metadata key/value pairs.
fn build_metadata_column(
    pairs: &[&(String, String)],
    jobs_map: &HashMap<String, JobView>,
) -> DfResult<ArrayRef> {
    use datafusion::arrow::array::{MapArray, StructArray};
    let mut keys_builder = datafusion::arrow::array::StringBuilder::new();
    let mut values_builder = datafusion::arrow::array::StringBuilder::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(pairs.len() + 1);
    offsets.push(0);
    let mut total = 0i32;
    for p in pairs {
        let metadata = jobs_map.get(&p.1).map_or_else(Vec::new, |v| v.metadata());
        for (k, v) in &metadata {
            keys_builder.append_value(k);
            values_builder.append_value(v);
            total += 1;
        }
        offsets.push(total);
    }
    let struct_array = StructArray::try_new(
        Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]),
        vec![
            Arc::new(keys_builder.finish()) as ArrayRef,
            Arc::new(values_builder.finish()) as ArrayRef,
        ],
        None,
    )
    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(Arc::new(MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        )),
        datafusion::arrow::buffer::OffsetBuffer::new(offsets.into()),
        struct_array,
        None,
        false,
    )))
}

/// Build a zero-column RecordBatch with the given row count.
/// Used for COUNT(*) and similar queries where DataFusion only needs a row tally.
fn make_empty_projection_batch(projection: &SchemaRef, row_count: usize) -> DfResult<RecordBatch> {
    RecordBatch::try_new_with_options(
        Arc::clone(projection),
        vec![],
        &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| DataFusionError::Execution(e.to_string()))
}

/// Scanner for the queues table - reads concurrency queue data from a single shard.
pub struct QueuesScanner {
    pub(crate) shard: Arc<JobStoreShard>,
}

impl QueuesScanner {
    /// Create a new QueuesScanner for the given shard
    pub fn new(shard: Arc<JobStoreShard>) -> Self {
        Self { shard }
    }
}

impl std::fmt::Debug for QueuesScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueuesScanner")
    }
}

/// Row type for queue entries
#[derive(Debug, Clone)]
struct QueueEntry {
    tenant: String,
    queue_name: String,
    entry_type: String, // "holder" or "requester"
    task_id: String,
    job_id: Option<String>,
    priority: Option<u8>,
    timestamp_ms: i64,
}

impl QueuesScanner {
    /// Get the base schema for the queues table
    pub fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("shard_id", DataType::Utf8, false),
            Field::new("tenant", DataType::Utf8, false),
            Field::new("queue_name", DataType::Utf8, false),
            Field::new("entry_type", DataType::Utf8, false), // "holder" or "requester"
            Field::new("task_id", DataType::Utf8, false),
            Field::new("job_id", DataType::Utf8, true),
            Field::new("priority", DataType::UInt8, true),
            Field::new("timestamp_ms", DataType::Int64, false),
        ]))
    }
}

impl Scan for QueuesScanner {
    fn describe(&self, filters: &[Expr], limit: Option<usize>) -> String {
        let mut tenant = None;
        let mut queue = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f) {
                match col.as_str() {
                    "tenant" => tenant = Some(val),
                    "queue_name" => queue = Some(val),
                    _ => {}
                }
            }
        }
        format!(
            "queues[tenant={:?}, queue={:?}], limit={:?}",
            tenant, queue, limit
        )
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        // Parse filters for tenant and queue_name
        let mut tenant_filter: Option<String> = None;
        let mut queue_filter: Option<String> = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f) {
                match col.as_str() {
                    "tenant" => tenant_filter = Some(val),
                    "queue_name" => queue_filter = Some(val),
                    _ => {}
                }
            }
        }
        let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(2);
        let shard = Arc::clone(&self.shard);
        let proj_for_stream = Arc::clone(&projection);
        tokio::spawn(async move {
            let db = shard.db();
            let mut entries: Vec<QueueEntry> = Vec::new();

            // Scan holders using binary storekey prefix
            let holders_start = match (&tenant_filter, &queue_filter) {
                (Some(t), Some(q)) => crate::keys::concurrency_holders_queue_prefix(t, q),
                (Some(t), None) => crate::keys::concurrency_holders_tenant_prefix(t),
                (None, _) => crate::keys::concurrency_holders_prefix(),
            };
            let holders_end = crate::keys::end_bound(&holders_start);

            if let Ok(mut iter) = db.scan::<Vec<u8>, _>(holders_start..=holders_end).await {
                while let Ok(Some(kv)) = iter.next().await {
                    if limit.is_some_and(|l| entries.len() >= l) {
                        break;
                    }
                    if let Some(parsed) = crate::keys::parse_concurrency_holder_key(&kv.key) {
                        // Filter by queue if specified
                        if let Some(ref q) = queue_filter
                            && parsed.queue != *q
                        {
                            continue;
                        }
                        let timestamp_ms = crate::codec::decode_holder_granted_at_ms(&kv.value)
                            .unwrap_or_default();
                        entries.push(QueueEntry {
                            tenant: parsed.tenant,
                            queue_name: parsed.queue,
                            entry_type: "holder".to_string(),
                            task_id: parsed.task_id,
                            job_id: None,
                            priority: None,
                            timestamp_ms,
                        });
                    }
                }
            }

            // Scan requests using binary storekey prefix
            let requests_start = match (&tenant_filter, &queue_filter) {
                (Some(t), Some(q)) => crate::keys::concurrency_request_prefix(t, q),
                (Some(t), None) => crate::keys::concurrency_request_tenant_prefix(t),
                (None, _) => crate::keys::concurrency_requests_prefix(),
            };
            let requests_end = crate::keys::end_bound(&requests_start);

            if let Ok(mut iter) = db.scan::<Vec<u8>, _>(requests_start..=requests_end).await {
                while let Ok(Some(kv)) = iter.next().await {
                    if limit.is_some_and(|l| entries.len() >= l) {
                        break;
                    }
                    if let Some(parsed) = crate::keys::parse_concurrency_request_key(&kv.key) {
                        // Filter by queue if specified
                        if let Some(ref q) = queue_filter
                            && parsed.queue != *q
                        {
                            continue;
                        }
                        let job_id = if let Ok(action) =
                            crate::codec::decode_concurrency_action(kv.value.clone())
                        {
                            action
                                .fb()
                                .variant_as_enqueue_task()
                                .and_then(|et| et.job_id().map(|s| s.to_string()))
                        } else {
                            None
                        };
                        entries.push(QueueEntry {
                            tenant: parsed.tenant,
                            queue_name: parsed.queue,
                            entry_type: "requester".to_string(),
                            task_id: parsed.request_id,
                            job_id,
                            priority: Some(parsed.priority),
                            timestamp_ms: parsed.start_time_ms as i64,
                        });
                    }
                }
            }

            // Build record batches
            let mut i: usize = 0;
            while i < entries.len() {
                let start = i;
                let end = std::cmp::min(entries.len(), start + batch_size);
                let batch_entries = &entries[start..end];

                // Handle empty projection (when DataFusion just needs row count)
                if proj_for_stream.fields().is_empty() {
                    let batch = match RecordBatch::try_new_with_options(
                        Arc::clone(&proj_for_stream),
                        vec![],
                        &datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(batch_entries.len())),
                    ) {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = tx
                                .send(Err(DataFusionError::Execution(e.to_string())))
                                .await;
                            return;
                        }
                    };
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                    i = end;
                    continue;
                }

                // Get shard_id from shard name (now a UUID string)
                let shard_id = shard.name().to_string();

                let mut cols: Vec<ArrayRef> = Vec::with_capacity(proj_for_stream.fields().len());
                for f in proj_for_stream.fields() {
                    match f.name().as_str() {
                        "shard_id" => {
                            let vals: Vec<&str> = vec![&shard_id; batch_entries.len()];
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "tenant" => {
                            let vals: Vec<&str> =
                                batch_entries.iter().map(|e| e.tenant.as_str()).collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "queue_name" => {
                            let vals: Vec<&str> = batch_entries
                                .iter()
                                .map(|e| e.queue_name.as_str())
                                .collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "entry_type" => {
                            let vals: Vec<&str> = batch_entries
                                .iter()
                                .map(|e| e.entry_type.as_str())
                                .collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "task_id" => {
                            let vals: Vec<&str> =
                                batch_entries.iter().map(|e| e.task_id.as_str()).collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "job_id" => {
                            let vals: Vec<Option<&str>> =
                                batch_entries.iter().map(|e| e.job_id.as_deref()).collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "priority" => {
                            let vals: Vec<Option<u8>> =
                                batch_entries.iter().map(|e| e.priority).collect();
                            cols.push(Arc::new(UInt8Array::from(vals)));
                        }
                        "timestamp_ms" => {
                            let vals: Vec<i64> =
                                batch_entries.iter().map(|e| e.timestamp_ms).collect();
                            cols.push(Arc::new(Int64Array::from(vals)));
                        }
                        other => {
                            let _ = tx
                                .send(Err(DataFusionError::Execution(format!(
                                    "unknown column {}",
                                    other
                                ))))
                                .await;
                            return;
                        }
                    }
                }

                let batch = match RecordBatch::try_new(Arc::clone(&proj_for_stream), cols) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(e.to_string())))
                            .await;
                        return;
                    }
                };
                if tx.send(Ok(batch)).await.is_err() {
                    return;
                }
                i = end;
            }

            // If no entries, send an empty batch
            if entries.is_empty() {
                let empty_cols: Vec<ArrayRef> = proj_for_stream
                    .fields()
                    .iter()
                    .map(|f| -> ArrayRef {
                        match f.name().as_str() {
                            "tenant" | "queue_name" | "entry_type" | "task_id" => {
                                Arc::new(StringArray::from(Vec::<&str>::new()))
                            }
                            "job_id" => Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                            "priority" => Arc::new(UInt8Array::from(Vec::<Option<u8>>::new())),
                            "timestamp_ms" => Arc::new(Int64Array::from(Vec::<i64>::new())),
                            _ => Arc::new(StringArray::from(Vec::<&str>::new())),
                        }
                    })
                    .collect();
                if let Ok(batch) = RecordBatch::try_new(Arc::clone(&proj_for_stream), empty_cols) {
                    let _ = tx.send(Ok(batch)).await;
                }
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&projection),
            ReceiverStream::new(rx).map(|r| r),
        ))
    }
}

fn parse_eq_filter(expr: &Expr) -> Option<(String, String)> {
    // Match forms: col("x") = lit("v") or lit = col
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            match (&**left, &**right) {
                (Expr::Column(c), Expr::Literal(s, _)) => {
                    literal_to_string(s).map(|v| (c.flat_name().to_string(), v))
                }
                (Expr::Literal(s, _), Expr::Column(c)) => {
                    literal_to_string(s).map(|v| (c.flat_name().to_string(), v))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

// Parse metadata equality filter patterns and return (key, value)
fn parse_metadata_eq_filter(expr: &Expr) -> Option<(String, String)> {
    use datafusion::scalar::ScalarValue;
    // Match: element_at(metadata, 'k') = 'v'
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            // Helper to extract literal string
            let lit_str = |s: &ScalarValue| match s {
                ScalarValue::Utf8(Some(v)) => Some(v.clone()),
                ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
                _ => None,
            };

            // Try left is indexed metadata, right is literal
            if let Some(key) = extract_metadata_key_from_expr(left.as_ref())
                && let Expr::Literal(s, _) = right.as_ref()
                && let Some(val) = lit_str(s)
            {
                return Some((key, val));
            }
            // Or right is indexed metadata, left is literal
            if let Some(key) = extract_metadata_key_from_expr(right.as_ref())
                && let Expr::Literal(s, _) = left.as_ref()
                && let Some(val) = lit_str(s)
            {
                return Some((key, val));
            }
            None
        }
        _ => None,
    }
}

// Support array_contains(element_at(metadata, 'key'), 'value') which DataFusion uses for Map lookups
fn parse_metadata_contains_filter(expr: &Expr) -> Option<(String, String)> {
    use datafusion::scalar::ScalarValue;

    // Match: array_contains(element_at(metadata, 'key'), 'value')
    if let Expr::ScalarFunction(func) = expr {
        // Check if this is array_contains (DataFusion may rename to array_has)
        if (func.func.name() == "array_contains" || func.func.name() == "array_has")
            && func.args.len() == 2
        {
            // First arg should be element_at(metadata, 'key')
            if let Some(key) = extract_metadata_key_from_expr(&func.args[0]) {
                // Second arg should be the literal value
                if let Expr::Literal(
                    ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)),
                    _,
                ) = &func.args[1]
                {
                    return Some((key, v.clone()));
                }
            }
        }
    }
    None
}

/// Parse metadata prefix filter patterns and return (key, value_prefix).
/// Matches:
///   - `starts_with(array_any_value(element_at(metadata, 'key')), 'prefix')` — ScalarFunction
///   - `starts_with(element_at(metadata, 'key'), 'prefix')` — also accepted
///   - `array_any_value(element_at(metadata, 'key')) LIKE 'prefix%'` — Like expression with simple prefix
///   - `element_at(metadata, 'key') LIKE 'prefix%'` — also accepted
fn parse_metadata_prefix_filter(expr: &Expr) -> Option<(String, String)> {
    use datafusion::scalar::ScalarValue;

    // Match: starts_with(array_any_value(element_at(metadata, 'key')), 'prefix')
    if let Expr::ScalarFunction(func) = expr
        && func.func.name() == "starts_with"
        && func.args.len() == 2
        && let Some(key) = extract_metadata_key_from_expr(&func.args[0])
        && let Expr::Literal(ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)), _) =
            &func.args[1]
    {
        return Some((key, v.clone()));
    }

    // Match: array_any_value(element_at(metadata, 'key')) LIKE 'prefix%'
    // DataFusion rewrites starts_with(x, 'prefix') to x LIKE 'prefix%'
    if let Expr::Like(like) = expr
        && !like.negated
        && !like.case_insensitive
        && like.escape_char.is_none()
        && let Some(key) = extract_metadata_key_from_expr(&like.expr)
        && let Expr::Literal(
            ScalarValue::Utf8(Some(pattern)) | ScalarValue::LargeUtf8(Some(pattern)),
            _,
        ) = like.pattern.as_ref()
        && pattern.ends_with('%')
    {
        let prefix = &pattern[..pattern.len() - 1];
        if !prefix.contains('%') && !prefix.contains('_') {
            return Some((key.clone(), prefix.to_string()));
        }
    }

    None
}

// Extract metadata key from element_at(metadata, 'key') expressions using AST traversal.
// Also handles array_any_value(element_at(metadata, 'key')) which is needed when
// the caller requires a scalar Utf8 instead of the List<Utf8> that element_at returns on Maps.
fn extract_metadata_key_from_expr(expr: &Expr) -> Option<String> {
    use datafusion::scalar::ScalarValue;

    if let Expr::ScalarFunction(func) = expr {
        let name = func.func.name();
        // element_at(metadata, 'key') or map_extract(metadata, 'key')
        // DataFusion may rename element_at to map_extract during planning.
        if (name == "element_at" || name == "map_extract")
            && func.args.len() == 2
            && let Expr::Column(col) = &func.args[0]
            && col.name == "metadata"
            && let Expr::Literal(ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)), _) =
                &func.args[1]
        {
            return Some(v.clone());
        }
        // array_any_value(element_at(metadata, 'key')) - unwraps List<Utf8> to Utf8
        if name == "array_any_value" && func.args.len() == 1 {
            return extract_metadata_key_from_expr(&func.args[0]);
        }
    }
    None
}

fn literal_to_string(s: &datafusion::scalar::ScalarValue) -> Option<String> {
    use datafusion::scalar::ScalarValue;
    match s {
        ScalarValue::Utf8(Some(v)) => Some(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        _ => None,
    }
}

/// Represents the different status filters that can be used in WHERE clauses.
/// Waiting and Scheduled are virtual statuses derived from the stored Scheduled status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatusFilter {
    /// A stored status kind (Running, Failed, Cancelled, Succeeded)
    Stored(crate::job::JobStatusKind),
    /// Virtual: Scheduled + start_time <= now (ready to run)
    Waiting,
    /// Virtual: Scheduled + start_time > now (future only)
    FutureScheduled,
}

impl std::fmt::Display for QueryStatusFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStatusFilter::Stored(kind) => write!(f, "{:?}", kind),
            QueryStatusFilter::Waiting => write!(f, "Waiting"),
            QueryStatusFilter::FutureScheduled => write!(f, "FutureScheduled"),
        }
    }
}

fn parse_status_kind(s: &str) -> Option<QueryStatusFilter> {
    use crate::job::JobStatusKind;
    match s {
        "Waiting" | "waiting" => Some(QueryStatusFilter::Waiting),
        "Scheduled" | "scheduled" => Some(QueryStatusFilter::FutureScheduled),
        "Running" | "running" => Some(QueryStatusFilter::Stored(JobStatusKind::Running)),
        "Failed" | "failed" => Some(QueryStatusFilter::Stored(JobStatusKind::Failed)),
        "Cancelled" | "canceled" | "cancelled" => {
            Some(QueryStatusFilter::Stored(JobStatusKind::Cancelled))
        }
        "Succeeded" | "success" | "succeeded" => {
            Some(QueryStatusFilter::Stored(JobStatusKind::Succeeded))
        }
        _ => None,
    }
}

/// Compute the display status kind string for a job status.
/// Scheduled jobs with start_time <= now display as "Waiting".
fn display_status_kind(status: &crate::job::JobStatus) -> String {
    if status.kind == crate::job::JobStatusKind::Scheduled {
        let now_ms = crate::job_store_shard::helpers::now_epoch_ms();
        if status
            .next_attempt_starts_after_ms
            .is_none_or(|t| t <= now_ms)
        {
            return "Waiting".to_string();
        }
    }
    format!("{:?}", status.kind)
}

/// Get the schema from a set of record batches.
///
/// Convert Arrow RecordBatches directly to MessagePack-encoded rows.
/// Uses streaming serialization to avoid buffering intermediate structures.
pub fn record_batches_to_msgpack(batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>, String> {
    let mut rows = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns() as u32;

        for row_idx in 0..num_rows {
            let mut buf = Vec::with_capacity(128); // Pre-allocate reasonable size

            // Write map header with number of columns
            rmp::encode::write_map_len(&mut buf, num_cols)
                .map_err(|e| format!("Failed to write map header: {}", e))?;

            // Write each column as key-value pair directly to buffer
            for col_idx in 0..num_cols as usize {
                let field = schema.field(col_idx);
                let col_name = field.name();
                let array = batch.column(col_idx);

                // Write key (column name)
                rmp::encode::write_str(&mut buf, col_name)
                    .map_err(|e| format!("Failed to write key: {}", e))?;

                // Write value directly based on Arrow type
                write_arrow_value_to_msgpack(&mut buf, array.as_ref(), row_idx)?;
            }

            rows.push(buf);
        }
    }

    Ok(rows)
}

/// Write a single Arrow array value at the given row index directly to MessagePack buffer.
fn write_arrow_value_to_msgpack(
    buf: &mut Vec<u8>,
    array: &dyn Array,
    row_idx: usize,
) -> Result<(), String> {
    use datafusion::arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        LargeStringArray, TimestampMillisecondArray, UInt16Array, UInt64Array,
    };

    if array.is_null(row_idx) {
        rmp::encode::write_nil(buf).map_err(|e| format!("Failed to write nil: {}", e))?;
        return Ok(());
    }

    // Handle each Arrow type with direct MessagePack encoding
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        rmp::encode::write_str(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write string: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        rmp::encode::write_str(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write string: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        rmp::encode::write_sint(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write int64: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        rmp::encode::write_sint(buf, arr.value(row_idx) as i64)
            .map_err(|e| format!("Failed to write int32: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
        rmp::encode::write_sint(buf, arr.value(row_idx) as i64)
            .map_err(|e| format!("Failed to write int16: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
        rmp::encode::write_sint(buf, arr.value(row_idx) as i64)
            .map_err(|e| format!("Failed to write int8: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
        rmp::encode::write_uint(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write uint64: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
        rmp::encode::write_uint(buf, arr.value(row_idx) as u64)
            .map_err(|e| format!("Failed to write uint32: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() {
        rmp::encode::write_uint(buf, arr.value(row_idx) as u64)
            .map_err(|e| format!("Failed to write uint16: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt8Array>() {
        rmp::encode::write_uint(buf, arr.value(row_idx) as u64)
            .map_err(|e| format!("Failed to write uint8: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        rmp::encode::write_f64(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write f64: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        rmp::encode::write_f32(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write f32: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        rmp::encode::write_bool(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write bool: {}", e))?;
        return Ok(());
    }
    if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        rmp::encode::write_sint(buf, arr.value(row_idx))
            .map_err(|e| format!("Failed to write timestamp: {}", e))?;
        return Ok(());
    }

    // For complex types or unknown types, fall back to string representation
    let formatter = datafusion::arrow::util::display::ArrayFormatter::try_new(
        array,
        &datafusion::arrow::util::display::FormatOptions::default(),
    );
    match formatter {
        Ok(fmt) => {
            rmp::encode::write_str(buf, &fmt.value(row_idx).to_string())
                .map_err(|e| format!("Failed to write formatted value: {}", e))?;
        }
        Err(_) => {
            rmp::encode::write_str(buf, "<unable to format>")
                .map_err(|e| format!("Failed to write fallback: {}", e))?;
        }
    }

    Ok(())
}
