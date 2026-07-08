use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::ops::ControlFlow;
use std::pin::Pin;
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
use futures::{Stream, StreamExt};
use slatedb::KeyValue;

use crate::job::{JobStatus, JobView};
use crate::job_store_shard::{JobStoreShard, TenantStatusCounterScanRange};

/// Error surfaced when a statement exceeds its configured deadline. It travels
/// out of the scan stream as a `DataFusionError::External` so the gRPC layer can
/// downcast it and map to `DEADLINE_EXCEEDED` (rather than a generic internal error).
#[derive(Debug)]
pub struct StatementTimeout {
    pub timeout: std::time::Duration,
}

impl std::fmt::Display for StatementTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Query exceeded statement timeout of {} ms",
            self.timeout.as_millis()
        )
    }
}

impl std::error::Error for StatementTimeout {}

/// Wrap a record-batch stream so each poll is bounded by an absolute deadline.
/// When the deadline elapses the stream yields a single
/// `DataFusionError::External(StatementTimeout)` and ends. Dropping the wrapped
/// stream at that point drops the underlying scan cursor, so the scan actually
/// stops — this is the defense that turns a timed-out query into no work rather
/// than a background zombie.
pub fn stream_with_deadline(
    inner: SendableRecordBatchStream,
    deadline: tokio::time::Instant,
    timeout: std::time::Duration,
) -> SendableRecordBatchStream {
    let schema = inner.schema();
    let stream = async_stream::try_stream! {
        let mut inner = inner;
        loop {
            match tokio::time::timeout_at(deadline, inner.next()).await {
                Ok(Some(item)) => yield item?,
                Ok(None) => break,
                Err(_) => {
                    Err(DataFusionError::External(Box::new(StatementTimeout { timeout })))?;
                }
            }
        }
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, Box::pin(stream)))
}

/// Boxed per-key extractor mapping an index key to an optional `(tenant, id)`
/// pair, mirroring the `ControlFlow` contract of `scan_collect`'s `extract`
/// closure so streaming scans can early-exit (`Break`) and skip (`Continue(None)`).
type PairExtractor = Box<dyn Fn(&[u8]) -> ControlFlow<(), Option<(String, String)>> + Send>;

/// Map any displayable error into a DataFusion execution error.
fn exec_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

/// Default key-chunk size for streaming scans of bounded auxiliary keyspaces
/// (holder entries, task-queue entries) whose scanners collect the whole range.
const DEFAULT_SCAN_CHUNK: usize = 1024;

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

        // Register tenant_counts table for pre-computed per-tenant status counters
        let tenant_counts_schema = TenantCountsScanner::base_schema();
        let tenant_counts_scanner: ScannerRef = Arc::new(TenantCountsScanner {
            shard: Arc::clone(&shard),
        });
        let tenant_counts_provider = Arc::new(SiloTableProvider::new(
            tenant_counts_schema,
            tenant_counts_scanner,
        ));
        ctx.register_table("tenant_counts", tenant_counts_provider)?;

        // Register queue_counts table for pre-computed per-queue concurrency counters
        let queue_counts_schema = QueueCountsScanner::base_schema();
        let queue_counts_scanner: ScannerRef = Arc::new(QueueCountsScanner {
            shard: Arc::clone(&shard),
        });
        let queue_counts_provider = Arc::new(SiloTableProvider::new(
            queue_counts_schema,
            queue_counts_scanner,
        ));
        ctx.register_table("queue_counts", queue_counts_provider)?;

        // Register tasks table for debugging the internal task queue
        let tasks_schema = TasksScanner::base_schema();
        let tasks_scanner: ScannerRef = Arc::new(TasksScanner {
            shard: Arc::clone(&shard),
        });
        let tasks_provider = Arc::new(SiloTableProvider::new(tasks_schema, tasks_scanner));
        ctx.register_table("tasks", tasks_provider)?;

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

    /// Classify each filter as Exact (fully handled by the scan) or Inexact.
    /// Returning Exact prevents DataFusion from adding a post-filter FilterExec,
    /// which enables LIMIT pushdown into the scan.
    /// Default: all Inexact (safe fallback).
    fn classify_filters(&self, _filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        vec![TableProviderFilterPushDown::Inexact; _filters.len()]
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(self.scanner.classify_filters(filters))
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
/// Classify jobs table filters for use by both ShardQueryEngine and ClusterQueryEngine.
pub fn classify_jobs_filters(filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
    // Strip table qualifiers from filter expressions before parsing the strategy,
    // because supports_filters_pushdown receives qualified names (e.g. "jobs.tenant")
    // while parse_jobs_scan_strategy expects unqualified names ("tenant").
    let unqualified_filters: Vec<Expr> = filters.iter().map(|f| unqualify_expr(f)).collect();
    let strategy = parse_jobs_scan_strategy(&unqualified_filters);

    filters
        .iter()
        .map(|f| {
            if let Some((col, _)) = parse_eq_filter(f) {
                let col_name = col.rsplit('.').next().unwrap_or(&col);
                classify_filter_pushdown(col_name, &strategy)
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

/// Strip table qualifiers from column references in an expression.
/// Converts e.g. `jobs.tenant = 'X'` to `tenant = 'X'`.
fn unqualify_expr(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(unqualify_expr(left)),
            op: *op,
            right: Box::new(unqualify_expr(right)),
        }),
        Expr::Column(col) => Expr::Column(datafusion::common::Column::new_unqualified(&col.name)),
        other => other.clone(),
    }
}

/// Determine if a specific filter column is Exact (fully handled) for the given strategy.
/// A filter is Exact when the scan guarantees correct results without post-filtering.
fn classify_filter_pushdown(
    col_name: &str,
    strategy: &JobsScanStrategy,
) -> TableProviderFilterPushDown {
    match strategy {
        // ExactId with tenant: both filters are handled exactly (direct pair construction).
        // ExactId without tenant: the scan falls back to scan_all_jobs + in-memory filter,
        // so neither filter is exact (limit pushdown would truncate before filtering).
        JobsScanStrategy::ExactId { tenant, .. } => match col_name {
            "id" if tenant.is_some() => TableProviderFilterPushDown::Exact,
            "tenant" if tenant.is_some() => TableProviderFilterPushDown::Exact,
            _ => TableProviderFilterPushDown::Inexact,
        },
        // Status: tenant scopes the scan, status_kind selects the index range.
        JobsScanStrategy::Status { tenant, .. } => match col_name {
            "status_kind" => TableProviderFilterPushDown::Exact,
            "tenant" if tenant.is_some() => TableProviderFilterPushDown::Exact,
            _ => TableProviderFilterPushDown::Inexact,
        },
        // Metadata strategies: tenant is handled, but status_kind is NOT filtered
        // by the metadata index scan, so it must remain Inexact.
        JobsScanStrategy::MetadataExact { tenant, .. }
        | JobsScanStrategy::MetadataPrefix { tenant, .. } => match col_name {
            "tenant" if tenant.is_some() => TableProviderFilterPushDown::Exact,
            _ => TableProviderFilterPushDown::Inexact,
        },
        // FullScan: tenant scopes the scan range but no other filters are handled.
        JobsScanStrategy::FullScan { tenant } => match col_name {
            "tenant" if tenant.is_some() => TableProviderFilterPushDown::Exact,
            _ => TableProviderFilterPushDown::Inexact,
        },
    }
}

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
            // Job-level limits (concurrency/rate/floating) serialized as a JSON array
            // of {limit_type, key, value} objects for display purposes.
            Field::new("limits", DataType::Utf8, true),
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
            "priority" | "enqueue_time_ms" | "payload" | "task_group" | "metadata" | "limits"
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

    fn classify_filters(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        classify_jobs_filters(filters)
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let strategy = parse_jobs_scan_strategy(filters);
        let shard = Arc::clone(&self.shard);
        let needs = analyze_projection(&projection, &strategy);

        // Each branch produces its own lazy `async_stream` generator that owns
        // the underlying scan cursor(s). Because nothing is spawned, dropping the
        // returned stream (statement timeout, client disconnect, LIMIT satisfied)
        // drops the cursor and stops the scan at its next `.await` — no zombies.
        let inner: Pin<Box<dyn Stream<Item = DfResult<RecordBatch>> + Send>> =
            if projection.fields().is_empty()
                && shard.count_from_status_counters
                && matches!(&strategy, JobsScanStrategy::FullScan { tenant: Some(_) })
            {
                // COUNT(*) over a whole tenant: answer from per-status counters.
                Box::pin(count_from_counters_stream(
                    shard,
                    projection.clone(),
                    strategy,
                ))
            } else if needs.use_status_index_path {
                Box::pin(status_index_stream(
                    shard,
                    projection.clone(),
                    strategy,
                    batch_size,
                    limit,
                ))
            } else if needs.need_job_info && matches!(strategy, JobsScanStrategy::FullScan { .. }) {
                Box::pin(fullscan_join_stream(
                    shard,
                    projection.clone(),
                    needs,
                    strategy,
                    batch_size,
                    limit,
                ))
            } else {
                Box::pin(job_pairs_stream(
                    shard,
                    projection.clone(),
                    needs,
                    strategy,
                    batch_size,
                    limit,
                ))
            };

        Box::pin(RecordBatchStreamAdapter::new(projection, inner))
    }
}

/// COUNT(*)-over-a-tenant fast path: sum the transactionally-maintained
/// per-status counters instead of walking the status index. Emits a single
/// zero-column batch whose row count is the tenant's live job total.
fn count_from_counters_stream(
    shard: Arc<JobStoreShard>,
    projection: SchemaRef,
    strategy: JobsScanStrategy,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    async_stream::try_stream! {
        let JobsScanStrategy::FullScan { tenant: Some(tenant) } = strategy else {
            return; // guarded by the caller; nothing to emit otherwise
        };
        let total = shard
            .count_tenant_live_jobs(&tenant)
            .await
            .map_err(exec_err)?;
        yield make_empty_projection_batch(&projection, total.max(0) as usize)?;
    }
}

/// Streams the status/time index in bounded chunks, emitting RecordBatches with
/// no point-lookups. Used when the projection only needs tenant, id,
/// status_kind, or status_changed_at_ms (including the COUNT(*) fallback when
/// counter-based counting is disabled).
fn status_index_stream(
    shard: Arc<JobStoreShard>,
    projection: SchemaRef,
    strategy: JobsScanStrategy,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    async_stream::try_stream! {
        let tenant = match &strategy {
            JobsScanStrategy::FullScan { tenant } => tenant.clone(),
            _ => None,
        };
        let (start, end) = match &tenant {
            Some(t) => {
                let s = crate::keys::idx_status_time_tenant_prefix(t);
                let e = crate::keys::end_bound(&s);
                (s, e)
            }
            None => {
                let s = crate::keys::idx_status_time_all_prefix();
                let e = crate::keys::end_bound(&s);
                (s, e)
            }
        };
        let shard_id = shard.name().to_string();
        let mut cursor = shard
            .open_range_cursor(start, end, &crate::scan_options())
            .await
            .map_err(exec_err)?;
        let mut sent = 0usize;
        loop {
            if limit.is_some_and(|l| sent >= l) {
                break;
            }
            // Never pull more index keys than the LIMIT still needs.
            let want = limit.map_or(batch_size, |l| batch_size.min(l - sent));
            let chunk = cursor.next_kv_chunk(want).await.map_err(exec_err)?;
            if chunk.is_empty() {
                break;
            }
            let mut rows: Vec<(String, String, String, i64)> = Vec::with_capacity(chunk.len());
            for kv in &chunk {
                let Some(p) = crate::keys::parse_status_time_index_key(&kv.key)
                    .filter(|p| !p.job_id.is_empty())
                else {
                    continue;
                };
                let changed = p.changed_at_ms();
                rows.push((p.tenant, p.job_id, p.status, changed));
            }
            if rows.is_empty() {
                continue;
            }
            if let Some(l) = limit {
                let remaining = l - sent;
                if rows.len() > remaining {
                    rows.truncate(remaining);
                }
            }
            let batch = build_status_index_batch(&projection, &shard_id, &rows)?;
            sent += batch.num_rows();
            yield batch;
        }
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
            "status_kind" => {
                // Apply the same Waiting/Scheduled logic as display_status_kind.
                // For Scheduled jobs, the index timestamp is next_attempt_starts_after_ms
                // (see status_index_timestamp). If that time has passed, the job is Waiting.
                let now_ms = crate::job_store_shard::helpers::now_epoch_ms();
                Arc::new(StringArray::from(
                    chunk
                        .iter()
                        .map(|(_, _, sk, ts)| {
                            if sk == "Scheduled" && *ts <= now_ms {
                                Some("Waiting")
                            } else {
                                Some(sk.as_str())
                            }
                        })
                        .collect::<Vec<_>>(),
                ))
            }
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

/// FullScan + need_job_info path: streams `job_info` in bounded windows, joining
/// each window against `job_status` via a lock-step merge (both keyspaces are
/// ordered by `(tenant, job_id)`). Reads each KV exactly once and holds only one
/// window in memory, so a full-tenant scan stays O(batch) instead of buffering
/// every `JobView` — this is the path the editor's `ORDER BY enqueue_time_ms`
/// query lands on, and the one that used to OOM the node.
fn fullscan_join_stream(
    shard: Arc<JobStoreShard>,
    projection: SchemaRef,
    needs: ProjectionNeeds,
    strategy: JobsScanStrategy,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    async_stream::try_stream! {
        let JobsScanStrategy::FullScan { tenant } = strategy else {
            return; // guarded by the caller
        };
        let shard_id = shard.name().to_string();

        let (info_start, info_end) = match &tenant {
            Some(t) => {
                let s = crate::keys::job_info_prefix(t);
                let e = crate::keys::end_bound(&s);
                (s, e)
            }
            None => {
                let s = crate::keys::jobs_prefix();
                let e = crate::keys::end_bound(&s);
                (s, e)
            }
        };
        let mut info_cursor = shard
            .open_range_cursor(info_start, info_end, &crate::scan_options())
            .await
            .map_err(exec_err)?;

        // Only open the status join cursor when a status column is projected.
        let mut status_join = if needs.need_any_status() {
            let (status_start, status_end) = match &tenant {
                Some(t) => {
                    let s = crate::keys::job_status_prefix(t);
                    let e = crate::keys::end_bound(&s);
                    (s, e)
                }
                None => {
                    let s = crate::keys::jobs_status_prefix();
                    let e = crate::keys::end_bound(&s);
                    (s, e)
                }
            };
            let cursor = shard
                .open_range_cursor(status_start, status_end, &crate::scan_options())
                .await
                .map_err(exec_err)?;
            Some(StatusJoinCursor::new(cursor, batch_size))
        } else {
            None
        };

        let mut sent = 0usize;
        loop {
            if limit.is_some_and(|l| sent >= l) {
                break;
            }
            // Never pull more job_info keys than the LIMIT still needs.
            let want = limit.map_or(batch_size, |l| batch_size.min(l - sent));
            let info_chunk = info_cursor.next_kv_chunk(want).await.map_err(exec_err)?;
            if info_chunk.is_empty() {
                break;
            }
            // Decode the window's job_info values (range scans skip tombstones,
            // so every entry is a live job).
            let mut infos: Vec<(String, String, JobView)> = Vec::with_capacity(info_chunk.len());
            for kv in info_chunk {
                let Some(p) =
                    crate::keys::parse_job_info_key(&kv.key).filter(|p| !p.job_id.is_empty())
                else {
                    continue;
                };
                infos.push((p.tenant, p.job_id, JobView::new(kv.value).map_err(exec_err)?));
            }
            if infos.is_empty() {
                continue;
            }
            if let Some(l) = limit {
                let remaining = l - sent;
                if infos.len() > remaining {
                    infos.truncate(remaining);
                }
            }

            // Advance the status cursor up to this window's last key.
            let mut status_map: HashMap<String, JobStatus> = HashMap::new();
            if let Some(join) = status_join.as_mut() {
                let last = infos.last().expect("infos non-empty");
                let window_end = (last.0.clone(), last.1.clone());
                join.collect_window(&window_end, &mut status_map).await?;
            }

            let pairs: Vec<(String, String)> =
                infos.iter().map(|(t, id, _)| (t.clone(), id.clone())).collect();
            let jobs_map: HashMap<String, JobView> =
                infos.into_iter().map(|(_, id, view)| (id, view)).collect();
            let pair_refs: Vec<&(String, String)> = pairs.iter().collect();
            let batch =
                build_job_pairs_batch(&projection, &shard_id, &pair_refs, &jobs_map, &status_map)?;
            sent += batch.num_rows();
            yield batch;
        }
    }
}

/// A `job_status` range cursor that a merge-join drives forward one window at a
/// time. Buffers at most one pulled chunk of not-yet-consumed status rows, so a
/// full scan stays O(batch) even though `job_info` and `job_status` are walked
/// as two independent iterators.
struct StatusJoinCursor {
    cursor: crate::job_store_shard::scan::RangeScanCursor,
    buffer: VecDeque<KeyValue>,
    exhausted: bool,
    batch_size: usize,
}

impl StatusJoinCursor {
    fn new(cursor: crate::job_store_shard::scan::RangeScanCursor, batch_size: usize) -> Self {
        Self {
            cursor,
            buffer: VecDeque::new(),
            exhausted: false,
            batch_size,
        }
    }

    /// Insert into `out` (keyed by job_id) every status record whose
    /// `(tenant, job_id)` key is `<= window_end`. Status rows past the window are
    /// left buffered for the next call. Both keyspaces encode `(tenant, job_id)`
    /// with the same order-preserving tuple encoding, so Rust tuple comparison
    /// matches on-disk key order.
    async fn collect_window(
        &mut self,
        window_end: &(String, String),
        out: &mut HashMap<String, JobStatus>,
    ) -> DfResult<()> {
        loop {
            if self.buffer.is_empty() {
                if self.exhausted {
                    break;
                }
                let chunk = self
                    .cursor
                    .next_kv_chunk(self.batch_size)
                    .await
                    .map_err(exec_err)?;
                if chunk.is_empty() {
                    self.exhausted = true;
                    break;
                }
                self.buffer.extend(chunk);
            }
            let front_key = {
                let kv = self.buffer.front().expect("buffer non-empty");
                crate::keys::parse_job_status_key(&kv.key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| (p.tenant, p.job_id))
            };
            match front_key {
                None => {
                    // Malformed/empty key — drop and continue.
                    self.buffer.pop_front();
                }
                Some(key) if key <= *window_end => {
                    let kv = self.buffer.pop_front().expect("buffer non-empty");
                    let status =
                        crate::job_store_shard::helpers::decode_job_status_owned(&kv.value)
                            .map_err(exec_err)?;
                    out.insert(key.1, status);
                }
                Some(_) => break, // front is past this window; keep it buffered
            }
        }
        Ok(())
    }
}

/// Non-FullScan path: resolves `(tenant, job_id)` pairs from an index in bounded
/// chunks and hydrates each chunk with `fetch_batch_data`, yielding a batch per
/// chunk. Covers ExactId, Metadata, and Status strategies.
fn job_pairs_stream(
    shard: Arc<JobStoreShard>,
    projection: SchemaRef,
    needs: ProjectionNeeds,
    strategy: JobsScanStrategy,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    // Cap hydration batches so DataFusion can stop early for LIMIT queries; full
    // scans pay near-identical total I/O (same ops, more smaller batches).
    const POINT_LOOKUP_BATCH: usize = 256;
    async_stream::try_stream! {
        let shard_id = shard.name().to_string();
        let fetch_batch = batch_size.clamp(1, POINT_LOOKUP_BATCH);

        // ExactId with a tenant: synthesize the single pair, no scan required.
        if let JobsScanStrategy::ExactId { tenant: Some(tenant), id } = &strategy {
            let pairs = vec![(tenant.clone(), id.clone())];
            let (jobs_map, status_map) = fetch_batch_data(&shard, &pairs, &needs).await?;
            let existing: Vec<&(String, String)> = if needs.need_job_info || needs.needs_existence_check {
                pairs.iter().filter(|(_, id)| jobs_map.contains_key(id)).collect()
            } else {
                pairs.iter().collect()
            };
            if !existing.is_empty() {
                yield build_job_pairs_batch(&projection, &shard_id, &existing, &jobs_map, &status_map)?;
            }
            return;
        }

        let now_ms = crate::job_store_shard::helpers::now_epoch_ms();
        let (start, end, extractor) = pairs_scan_plan(&strategy, now_ms);
        let mut cursor = shard
            .open_range_cursor(start, end, &crate::scan_options())
            .await
            .map_err(exec_err)?;

        let mut sent = 0usize;
        let mut pending: Vec<(String, String)> = Vec::new();
        let mut done = false;
        loop {
            if limit.is_some_and(|l| sent >= l) {
                break;
            }
            // Fill `pending` up to a hydration batch, but never resolve more pairs
            // than the LIMIT still needs (so a small LIMIT scans few index keys).
            let want = limit.map_or(fetch_batch, |l| fetch_batch.min(l - sent));
            while pending.len() < want && !done {
                let chunk = cursor
                    .next_kv_chunk(want - pending.len())
                    .await
                    .map_err(exec_err)?;
                if chunk.is_empty() {
                    done = true;
                    break;
                }
                for kv in &chunk {
                    match extractor(&kv.key) {
                        ControlFlow::Continue(Some(pair)) => pending.push(pair),
                        ControlFlow::Continue(None) => {}
                        ControlFlow::Break(()) => {
                            done = true;
                            break;
                        }
                    }
                }
            }
            if pending.is_empty() {
                break;
            }
            if let Some(l) = limit {
                let remaining = l - sent;
                if pending.len() > remaining {
                    pending.truncate(remaining);
                }
            }
            let (jobs_map, status_map) = fetch_batch_data(&shard, &pending, &needs).await?;
            // With job_info fetched, the map is the authoritative presence check;
            // otherwise the index scan is the source of truth (scans skip tombstones).
            let existing: Vec<&(String, String)> = if needs.need_job_info || needs.needs_existence_check {
                pending.iter().filter(|(_, id)| jobs_map.contains_key(id)).collect()
            } else {
                pending.iter().collect()
            };
            if !existing.is_empty() {
                let batch =
                    build_job_pairs_batch(&projection, &shard_id, &existing, &jobs_map, &status_map)?;
                sent += batch.num_rows();
                yield batch;
            }
            pending.clear();
            if done {
                break;
            }
        }
    }
}

/// Compute the `(start, end, extractor)` scan plan for a non-FullScan pair
/// resolution. The extractor mirrors the `ControlFlow` contract of the
/// collect-oriented `scan_*` helpers so early-exit and skip semantics are preserved.
fn pairs_scan_plan(strategy: &JobsScanStrategy, now_ms: i64) -> (Vec<u8>, Vec<u8>, PairExtractor) {
    use crate::keys;
    match strategy {
        JobsScanStrategy::ExactId { tenant: None, id } => {
            let start = keys::jobs_prefix();
            let end = keys::end_bound(&start);
            let id = id.clone();
            let f: PairExtractor = Box::new(move |key: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_job_info_key(key)
                        .filter(|p| !p.job_id.is_empty() && p.job_id == id)
                        .map(|p| (p.tenant, p.job_id)),
                )
            });
            (start, end, f)
        }
        JobsScanStrategy::MetadataExact {
            tenant: Some(t),
            key,
            value,
        } => {
            let start = keys::idx_metadata_prefix(t, key, value);
            let end = keys::end_bound(&start);
            let tenant = t.clone();
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_metadata_index_key(k)
                        .filter(|p| !p.job_id.is_empty())
                        .map(|p| (tenant.clone(), p.job_id)),
                )
            });
            (start, end, f)
        }
        JobsScanStrategy::MetadataPrefix {
            tenant: Some(t),
            key,
            prefix,
        } => {
            let start = keys::idx_metadata_prefix(t, key, prefix);
            let end = keys::end_bound(&keys::idx_metadata_key_only_prefix(t, key));
            let tenant = t.clone();
            let prefix = prefix.clone();
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                let Some(parsed) = keys::parse_metadata_index_key(k) else {
                    return ControlFlow::Continue(None);
                };
                if parsed.value.starts_with(&prefix) && !parsed.job_id.is_empty() {
                    ControlFlow::Continue(Some((tenant.clone(), parsed.job_id)))
                } else if parsed.value.as_str() > prefix.as_str()
                    && !parsed.value.starts_with(&prefix)
                {
                    // Values sort lexicographically; once past the prefix range, stop.
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(None)
                }
            });
            (start, end, f)
        }
        JobsScanStrategy::Status { tenant, status } => {
            status_pairs_scan_plan(tenant.as_deref(), *status, now_ms)
        }
        // No-tenant metadata filters have no tenant-scoped index, so scan all
        // job_info and let DataFusion's FilterExec re-apply the (Inexact) filter.
        // ExactId{Some} is handled by the caller; FullScan by other stream paths.
        JobsScanStrategy::MetadataExact { tenant: None, .. }
        | JobsScanStrategy::MetadataPrefix { tenant: None, .. }
        | JobsScanStrategy::ExactId {
            tenant: Some(_), ..
        }
        | JobsScanStrategy::FullScan { .. } => all_jobs_pairs_plan(),
    }
}

/// Scan-plan that walks all `job_info` keys and yields every `(tenant, job_id)`.
fn all_jobs_pairs_plan() -> (Vec<u8>, Vec<u8>, PairExtractor) {
    let start = crate::keys::jobs_prefix();
    let end = crate::keys::end_bound(&start);
    let f: PairExtractor = Box::new(|key: &[u8]| {
        ControlFlow::Continue(
            crate::keys::parse_job_info_key(key)
                .filter(|p| !p.job_id.is_empty())
                .map(|p| (p.tenant, p.job_id)),
        )
    });
    (start, end, f)
}

/// Scan-plan for status-filtered pair resolution, covering the stored-status and
/// virtual Waiting / FutureScheduled variants across tenant-scoped and
/// cross-tenant scans. Mirrors `scan_jobs_by_status` / `scan_jobs_waiting` /
/// `scan_jobs_future_scheduled`.
fn status_pairs_scan_plan(
    tenant: Option<&str>,
    status: QueryStatusFilter,
    now_ms: i64,
) -> (Vec<u8>, Vec<u8>, PairExtractor) {
    use crate::keys;
    let inverted_now = u64::MAX - (now_ms.max(0) as u64);
    match (status, tenant) {
        (QueryStatusFilter::Stored(kind), Some(t)) => {
            let start = keys::idx_status_time_prefix(t, kind.as_str());
            let end = keys::end_bound(&start);
            let tenant = t.to_string();
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_status_time_index_key(k)
                        .filter(|p| !p.job_id.is_empty())
                        .map(|p| (tenant.clone(), p.job_id)),
                )
            });
            (start, end, f)
        }
        (QueryStatusFilter::Stored(kind), None) => {
            let start = keys::idx_status_time_all_prefix();
            let end = keys::end_bound(&start);
            let status_str = kind.as_str().to_string();
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_status_time_index_key(k)
                        .filter(|p| p.status == status_str && !p.job_id.is_empty())
                        .map(|p| (p.tenant, p.job_id)),
                )
            });
            (start, end, f)
        }
        (QueryStatusFilter::Waiting, Some(t)) => {
            let start = keys::idx_status_time_prefix_with_time(t, "Scheduled", inverted_now);
            let end = keys::end_bound(&keys::idx_status_time_prefix(t, "Scheduled"));
            let tenant = t.to_string();
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_status_time_index_key(k)
                        .filter(|p| !p.job_id.is_empty())
                        .map(|p| (tenant.clone(), p.job_id)),
                )
            });
            (start, end, f)
        }
        (QueryStatusFilter::Waiting, None) => {
            let start = keys::idx_status_time_all_prefix();
            let end = keys::end_bound(&start);
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_status_time_index_key(k)
                        .filter(|p| {
                            p.status == "Scheduled"
                                && p.inverted_timestamp >= inverted_now
                                && !p.job_id.is_empty()
                        })
                        .map(|p| (p.tenant, p.job_id)),
                )
            });
            (start, end, f)
        }
        (QueryStatusFilter::FutureScheduled, Some(t)) => {
            let start = keys::idx_status_time_prefix(t, "Scheduled");
            let end = keys::idx_status_time_prefix_with_time(t, "Scheduled", inverted_now);
            let tenant = t.to_string();
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_status_time_index_key(k)
                        .filter(|p| !p.job_id.is_empty())
                        .map(|p| (tenant.clone(), p.job_id)),
                )
            });
            (start, end, f)
        }
        (QueryStatusFilter::FutureScheduled, None) => {
            let start = keys::idx_status_time_all_prefix();
            let end = keys::end_bound(&start);
            let f: PairExtractor = Box::new(move |k: &[u8]| {
                ControlFlow::Continue(
                    keys::parse_status_time_index_key(k)
                        .filter(|p| {
                            p.status == "Scheduled"
                                && p.inverted_timestamp < inverted_now
                                && !p.job_id.is_empty()
                        })
                        .map(|p| (p.tenant, p.job_id)),
                )
            });
            (start, end, f)
        }
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
            "limits" => Arc::new(StringArray::from(
                pairs
                    .iter()
                    .map(|p| jobs_map.get(&p.1).map(|v| limits_to_json(&v.limits())))
                    .collect::<Vec<Option<String>>>(),
            )),
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

/// Serialize a job's limits into a JSON array of `{limit_type, key, value}` objects
/// for display in the web UI. Each limit variant is flattened into a single display row.
fn limits_to_json(limits: &[crate::job::Limit]) -> String {
    use crate::job::Limit;
    let rows: Vec<serde_json::Value> = limits
        .iter()
        .map(|limit| match limit {
            Limit::Concurrency(c) => serde_json::json!({
                "limit_type": "Concurrency",
                "key": c.key,
                "value": c.max_concurrency.to_string(),
            }),
            Limit::RateLimit(r) => serde_json::json!({
                "limit_type": "Rate",
                "key": r.unique_key,
                "value": format!("{} per {}ms", r.limit, r.duration_ms),
            }),
            Limit::FloatingConcurrency(f) => serde_json::json!({
                "limit_type": "Floating Concurrency",
                "key": f.key,
                "value": f.default_max_concurrency.to_string(),
            }),
        })
        .collect();
    serde_json::Value::Array(rows).to_string()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueRequesterFastPathProjection {
    Empty,
    ConstantColumns,
}

fn queue_requester_fast_path_projection(
    projection: &SchemaRef,
) -> Option<QueueRequesterFastPathProjection> {
    if projection.fields().is_empty() {
        return Some(QueueRequesterFastPathProjection::Empty);
    }

    projection
        .fields()
        .iter()
        .all(|field| {
            matches!(
                field.name().as_str(),
                "shard_id" | "tenant" | "queue_name" | "entry_type"
            )
        })
        .then_some(QueueRequesterFastPathProjection::ConstantColumns)
}

fn make_queue_constant_projection_batch(
    projection: &SchemaRef,
    shard_id: &str,
    tenant: &str,
    queue: &str,
    entry_type: &str,
    row_count: usize,
) -> DfResult<RecordBatch> {
    if projection.fields().is_empty() {
        return make_empty_projection_batch(projection, row_count);
    }

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(projection.fields().len());
    for field in projection.fields() {
        match field.name().as_str() {
            "shard_id" => cols.push(Arc::new(StringArray::from(vec![shard_id; row_count]))),
            "tenant" => cols.push(Arc::new(StringArray::from(vec![tenant; row_count]))),
            "queue_name" => cols.push(Arc::new(StringArray::from(vec![queue; row_count]))),
            "entry_type" => cols.push(Arc::new(StringArray::from(vec![entry_type; row_count]))),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "requester fast path does not support projection column {other}"
                )));
            }
        }
    }

    RecordBatch::try_new(Arc::clone(projection), cols)
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
        let mut entry_type = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f) {
                match col.as_str() {
                    "tenant" => tenant = Some(val),
                    "queue_name" => queue = Some(val),
                    "entry_type" => entry_type = Some(val),
                    _ => {}
                }
            }
        }
        format!(
            "queues[tenant={:?}, queue={:?}, entry_type={:?}], limit={:?}",
            tenant, queue, entry_type, limit
        )
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        // Parse filters for tenant, queue_name, and entry_type
        let mut tenant_filter: Option<String> = None;
        let mut queue_filter: Option<String> = None;
        let mut entry_type_filter: Option<String> = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f) {
                match col.as_str() {
                    "tenant" => tenant_filter = Some(val),
                    "queue_name" => queue_filter = Some(val),
                    "entry_type" => entry_type_filter = Some(val),
                    _ => {}
                }
            }
        }

        // Fast path: requester-only scans for a specific tenant + queue.
        // Filtered COUNT(*) keeps the filter columns projected because FilterExec stays above
        // our scan, so support both the empty projection and constant-column variants.
        if let Some(fast_path_projection) = queue_requester_fast_path_projection(&projection)
            && let Some(ref tenant) = tenant_filter
            && let Some(ref queue) = queue_filter
            && entry_type_filter.as_deref() == Some("requester")
        {
            let tenant = tenant.clone();
            let queue = queue.clone();
            let shard = Arc::clone(&self.shard);
            let proj_for_stream = Arc::clone(&projection);
            let stream = async_stream::try_stream! {
                let counter_key = crate::keys::concurrency_requester_counter_key(&tenant, &queue);
                let counter = shard
                    .db()
                    .get(&counter_key)
                    .await
                    .map_err(|e| exec_err(format!("failed to read requester counter: {e}")))?;
                let count = match counter {
                    Some(bytes) => {
                        crate::job_store_shard::counters::decode_counter(&bytes).max(0) as usize
                    }
                    None => {
                        // Counter missing: fall back to counting request rows directly.
                        let prefix = crate::keys::concurrency_request_prefix(&tenant, &queue);
                        let end = crate::keys::end_bound(&prefix);
                        let mut cursor = shard
                            .open_range_cursor(prefix, end, &crate::scan_options())
                            .await
                            .map_err(exec_err)?;
                        let mut scanned_count: usize = 0;
                        loop {
                            let chunk =
                                cursor.next_kv_chunk(DEFAULT_SCAN_CHUNK).await.map_err(exec_err)?;
                            if chunk.is_empty() {
                                break;
                            }
                            for kv in &chunk {
                                if crate::keys::parse_concurrency_request_key(&kv.key).is_some() {
                                    scanned_count += 1;
                                }
                            }
                        }
                        scanned_count
                    }
                };

                match fast_path_projection {
                    QueueRequesterFastPathProjection::Empty => {
                        yield make_empty_projection_batch(&proj_for_stream, count)?;
                    }
                    QueueRequesterFastPathProjection::ConstantColumns => {
                        let batch_rows = batch_size.max(1);
                        let shard_id = shard.name().to_string();
                        let mut remaining = count;
                        loop {
                            let rows = remaining.min(batch_rows);
                            if remaining == 0 && rows == 0 {
                                yield make_queue_constant_projection_batch(
                                    &proj_for_stream, &shard_id, &tenant, &queue, "requester", 0,
                                )?;
                                break;
                            }
                            yield make_queue_constant_projection_batch(
                                &proj_for_stream, &shard_id, &tenant, &queue, "requester", rows,
                            )?;
                            remaining -= rows;
                            if remaining == 0 {
                                break;
                            }
                        }
                    }
                }
            };
            return Box::pin(RecordBatchStreamAdapter::new(projection, Box::pin(stream)));
        }

        let shard = Arc::clone(&self.shard);
        let proj_for_stream = Arc::clone(&projection);
        let stream = async_stream::try_stream! {
            let mut entries: Vec<QueueEntry> = Vec::new();

            // Scan holders using binary storekey prefix
            let holders_start = match (&tenant_filter, &queue_filter) {
                (Some(t), Some(q)) => crate::keys::concurrency_holders_queue_prefix(t, q),
                (Some(t), None) => crate::keys::concurrency_holders_tenant_prefix(t),
                (None, _) => crate::keys::concurrency_holders_prefix(),
            };
            let holders_end = crate::keys::end_bound(&holders_start);
            let mut holder_cursor = shard
                .open_range_cursor(holders_start, holders_end, &crate::scan_options())
                .await
                .map_err(exec_err)?;
            'holders: loop {
                let chunk = holder_cursor.next_kv_chunk(DEFAULT_SCAN_CHUNK).await.map_err(exec_err)?;
                if chunk.is_empty() {
                    break;
                }
                for kv in &chunk {
                    if limit.is_some_and(|l| entries.len() >= l) {
                        break 'holders;
                    }
                    if let Some(parsed) = crate::keys::parse_concurrency_holder_key(&kv.key) {
                        if let Some(ref q) = queue_filter
                            && parsed.queue != *q
                        {
                            continue;
                        }
                        let timestamp_ms =
                            crate::codec::decode_holder_granted_at_ms(&kv.value).unwrap_or_default();
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
            let mut request_cursor = shard
                .open_range_cursor(requests_start, requests_end, &crate::scan_options())
                .await
                .map_err(exec_err)?;
            'requests: loop {
                let chunk = request_cursor.next_kv_chunk(DEFAULT_SCAN_CHUNK).await.map_err(exec_err)?;
                if chunk.is_empty() {
                    break;
                }
                for kv in &chunk {
                    if limit.is_some_and(|l| entries.len() >= l) {
                        break 'requests;
                    }
                    if let Some(parsed) = crate::keys::parse_concurrency_request_key(&kv.key) {
                        if let Some(ref q) = queue_filter
                            && parsed.queue != *q
                        {
                            continue;
                        }
                        let task_id = parsed.request_id();
                        let job_id = Some(parsed.job_id.clone());
                        entries.push(QueueEntry {
                            tenant: parsed.tenant,
                            queue_name: parsed.queue,
                            entry_type: "requester".to_string(),
                            task_id,
                            job_id,
                            priority: Some(parsed.priority),
                            timestamp_ms: parsed.start_time_ms as i64,
                        });
                    }
                }
            }

            // Build record batches
            let shard_id = shard.name().to_string();
            let mut i: usize = 0;
            while i < entries.len() {
                let start = i;
                let end = std::cmp::min(entries.len(), start + batch_size);
                let batch_entries = &entries[start..end];

                // Handle empty projection (when DataFusion just needs row count)
                if proj_for_stream.fields().is_empty() {
                    yield RecordBatch::try_new_with_options(
                        Arc::clone(&proj_for_stream),
                        vec![],
                        &datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(batch_entries.len())),
                    )
                    .map_err(exec_err)?;
                    i = end;
                    continue;
                }

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
                            let vals: Vec<&str> =
                                batch_entries.iter().map(|e| e.queue_name.as_str()).collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "entry_type" => {
                            let vals: Vec<&str> =
                                batch_entries.iter().map(|e| e.entry_type.as_str()).collect();
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
                            Err(DataFusionError::Execution(format!("unknown column {}", other)))?;
                        }
                    }
                }

                yield RecordBatch::try_new(Arc::clone(&proj_for_stream), cols).map_err(exec_err)?;
                i = end;
            }

            // If no entries, emit a single empty batch so the schema is observed.
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
                yield RecordBatch::try_new(Arc::clone(&proj_for_stream), empty_cols).map_err(exec_err)?;
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(projection, Box::pin(stream)))
    }
}

/// Scanner for the tenant_counts table - reads pre-computed per-tenant, per-status counters.
pub struct TenantCountsScanner {
    pub(crate) shard: Arc<JobStoreShard>,
}

impl TenantCountsScanner {
    /// Create a new TenantCountsScanner for the given shard
    pub fn new(shard: Arc<JobStoreShard>) -> Self {
        Self { shard }
    }

    /// Get the base schema for the tenant_counts table
    pub fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("shard_id", DataType::Utf8, false),
            Field::new("tenant", DataType::Utf8, false),
            Field::new("status_kind", DataType::Utf8, false),
            Field::new("cnt", DataType::Int64, false),
        ]))
    }
}

impl std::fmt::Debug for TenantCountsScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TenantCountsScanner")
    }
}

impl Scan for TenantCountsScanner {
    fn describe(&self, filters: &[Expr], limit: Option<usize>) -> String {
        let tenant_range = parse_tenant_counts_scan_range(filters)
            .map(|range| describe_tenant_status_counter_scan_range(&range))
            .unwrap_or_else(|| "all".to_string());
        format!("tenant_counts[{}], limit={:?}", tenant_range, limit)
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        _batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let shard = Arc::clone(&self.shard);
        let proj = Arc::clone(&projection);
        let tenant_range = parse_tenant_counts_scan_range(filters);

        let stream = async_stream::try_stream! {
            let entries = shard
                .scan_tenant_status_counters(tenant_range)
                .await
                .map_err(exec_err)?;
            let shard_id = shard.name().to_string();
            let n = entries.len();

            if proj.fields().is_empty() {
                yield make_empty_projection_batch(&proj, n)?;
                return;
            }

            let mut cols: Vec<ArrayRef> = Vec::with_capacity(proj.fields().len());
            for f in proj.fields() {
                let col: ArrayRef = match f.name().as_str() {
                    "shard_id" => Arc::new(StringArray::from(vec![shard_id.as_str(); n])),
                    "tenant" => Arc::new(StringArray::from(
                        entries.iter().map(|(t, _, _)| t.as_str()).collect::<Vec<_>>(),
                    )),
                    "status_kind" => Arc::new(StringArray::from(
                        entries.iter().map(|(_, s, _)| s.as_str()).collect::<Vec<_>>(),
                    )),
                    "cnt" => Arc::new(Int64Array::from(
                        entries.iter().map(|(_, _, c)| *c).collect::<Vec<_>>(),
                    )),
                    other => Err(DataFusionError::Execution(format!("unknown column {}", other)))?,
                };
                cols.push(col);
            }

            yield RecordBatch::try_new(proj, cols).map_err(exec_err)?;
        };

        Box::pin(RecordBatchStreamAdapter::new(projection, Box::pin(stream)))
    }
}

/// Scanner for the queue_counts table - reads pre-computed per-queue concurrency requester
/// counters and scans holder entries (which are bounded by concurrency limits and thus fast).
pub struct QueueCountsScanner {
    pub(crate) shard: Arc<JobStoreShard>,
}

impl QueueCountsScanner {
    pub fn new(shard: Arc<JobStoreShard>) -> Self {
        Self { shard }
    }

    pub fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("shard_id", DataType::Utf8, false),
            Field::new("tenant", DataType::Utf8, false),
            Field::new("queue_name", DataType::Utf8, false),
            Field::new("holders", DataType::Int64, false),
            Field::new("requesters", DataType::Int64, false),
            Field::new("max_concurrency", DataType::Int64, true),
            Field::new("limit_type", DataType::Utf8, true),
        ]))
    }
}

impl std::fmt::Debug for QueueCountsScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueueCountsScanner")
    }
}

impl Scan for QueueCountsScanner {
    fn describe(&self, filters: &[Expr], limit: Option<usize>) -> String {
        let mut tenant = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f)
                && col == "tenant"
            {
                tenant = Some(val);
            }
        }
        format!("queue_counts[tenant={:?}], limit={:?}", tenant, limit)
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        _batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let mut tenant_filter: Option<String> = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f)
                && col == "tenant"
            {
                tenant_filter = Some(val);
            }
        }

        let shard = Arc::clone(&self.shard);
        let proj = Arc::clone(&projection);

        let stream = async_stream::try_stream! {
            // Collect requester counts from pre-computed counters (fast)
            // and holder counts by scanning holder entries (bounded by concurrency limits).
            // queue_data: (tenant, queue) -> (holders, requesters)
            let mut queue_data: HashMap<(String, String), (i64, i64)> = HashMap::new();

            // Scan requester counters
            let requester_results = if let Some(ref tenant) = tenant_filter {
                shard
                    .scan_concurrency_requester_counters(tenant)
                    .await
                    .map(|entries| {
                        entries
                            .into_iter()
                            .map(|(queue, count)| (tenant.clone(), queue, count))
                            .collect::<Vec<_>>()
                    })
            } else {
                shard.scan_all_concurrency_requester_counters().await
            };
            for (tenant, queue, count) in requester_results.map_err(exec_err)? {
                queue_data.entry((tenant, queue)).or_insert((0, 0)).1 = count;
            }

            // Scan holder entries (bounded by concurrency limits, so fast)
            let holders_start = match &tenant_filter {
                Some(t) => crate::keys::concurrency_holders_tenant_prefix(t),
                None => crate::keys::concurrency_holders_prefix(),
            };
            let holders_end = crate::keys::end_bound(&holders_start);
            let mut holder_cursor = shard
                .open_range_cursor(holders_start, holders_end, &crate::scan_options())
                .await
                .map_err(exec_err)?;
            loop {
                let chunk = holder_cursor
                    .next_kv_chunk(DEFAULT_SCAN_CHUNK)
                    .await
                    .map_err(exec_err)?;
                if chunk.is_empty() {
                    break;
                }
                for kv in &chunk {
                    if let Some(parsed) = crate::keys::parse_concurrency_holder_key(&kv.key) {
                        queue_data
                            .entry((parsed.tenant, parsed.queue))
                            .or_insert((0, 0))
                            .0 += 1;
                    }
                }
            }

            // Read concurrency limits from the in-memory cache (populated during
            // enqueue, grant_next, and floating limit refresh — no DB scanning needed).
            let cached_limits = shard.snapshot_queue_limits();
            let mut limit_info: HashMap<(String, String), (i64, &str)> = HashMap::new();
            for cached in &cached_limits {
                if let Some(ref t) = tenant_filter
                    && cached.tenant != *t
                {
                    continue;
                }
                let lt = match cached.limit_type {
                    crate::concurrency::ConcurrencyLimitType::Fixed => "fixed",
                    crate::concurrency::ConcurrencyLimitType::Floating => "floating",
                };
                limit_info.insert(
                    (cached.tenant.clone(), cached.queue.clone()),
                    (cached.max_concurrency as i64, lt),
                );
            }

            let shard_id = shard.name().to_string();
            let entries: Vec<_> = queue_data.into_iter().collect();
            let n = entries.len();

            if proj.fields().is_empty() {
                yield make_empty_projection_batch(&proj, n)?;
                return;
            }

            let mut cols: Vec<ArrayRef> = Vec::with_capacity(proj.fields().len());
            for f in proj.fields() {
                let col: ArrayRef = match f.name().as_str() {
                    "shard_id" => Arc::new(StringArray::from(vec![shard_id.as_str(); n])),
                    "tenant" => Arc::new(StringArray::from(
                        entries.iter().map(|((t, _), _)| t.as_str()).collect::<Vec<_>>(),
                    )),
                    "queue_name" => Arc::new(StringArray::from(
                        entries.iter().map(|((_, q), _)| q.as_str()).collect::<Vec<_>>(),
                    )),
                    "holders" => Arc::new(Int64Array::from(
                        entries.iter().map(|(_, (h, _))| *h).collect::<Vec<_>>(),
                    )),
                    "requesters" => Arc::new(Int64Array::from(
                        entries.iter().map(|(_, (_, r))| *r).collect::<Vec<_>>(),
                    )),
                    "max_concurrency" => Arc::new(Int64Array::from(
                        entries
                            .iter()
                            .map(|(key, _)| limit_info.get(key).map(|(max, _)| *max))
                            .collect::<Vec<_>>(),
                    )),
                    "limit_type" => Arc::new(StringArray::from(
                        entries
                            .iter()
                            .map(|(key, _)| limit_info.get(key).map(|(_, lt)| *lt))
                            .collect::<Vec<_>>(),
                    )),
                    other => Err(DataFusionError::Execution(format!("unknown column {}", other)))?,
                };
                cols.push(col);
            }

            yield RecordBatch::try_new(proj, cols).map_err(exec_err)?;
        };

        Box::pin(RecordBatchStreamAdapter::new(projection, Box::pin(stream)))
    }
}

/// Scanner for the tasks table - reads task queue entries from a single shard.
///
/// This table exposes the internal task queue for debugging purposes. Performance may be
/// poor for large datasets unless queries are carefully constructed. For best performance,
/// filter by `task_group` and optionally a `start_time_ms` range, which mirrors the
/// access pattern used by the task broker.
///
/// Schema: shard_id, tenant, task_group, start_time_ms, priority, job_id, attempt, variant_type, task_id, held_queues
pub struct TasksScanner {
    pub(crate) shard: Arc<JobStoreShard>,
}

impl TasksScanner {
    pub fn new(shard: Arc<JobStoreShard>) -> Self {
        Self { shard }
    }

    pub fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("shard_id", DataType::Utf8, false),
            Field::new("tenant", DataType::Utf8, false),
            Field::new("task_group", DataType::Utf8, false),
            Field::new("start_time_ms", DataType::Int64, false),
            Field::new("priority", DataType::UInt8, false),
            Field::new("job_id", DataType::Utf8, false),
            Field::new("attempt", DataType::UInt32, false),
            Field::new("variant_type", DataType::Utf8, false),
            Field::new("task_id", DataType::Utf8, false),
            Field::new("held_queues", DataType::Utf8, false),
        ]))
    }
}

impl std::fmt::Debug for TasksScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TasksScanner")
    }
}

/// Scan strategy for the tasks table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TasksScanStrategy {
    /// Scan a specific task group, optionally bounded by a time range.
    /// This is the fast path matching the task broker's access pattern.
    TaskGroupScan {
        task_group: String,
        start_time_lower: Option<i64>,
        start_time_upper: Option<i64>,
    },
    /// Full scan across all task groups, optionally filtered by tenant (post-filter).
    FullScan,
}

impl std::fmt::Display for TasksScanStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TasksScanStrategy::TaskGroupScan {
                task_group,
                start_time_lower,
                start_time_upper,
            } => {
                write!(
                    f,
                    "TaskGroupScan(task_group={:?}, time_lower={:?}, time_upper={:?})",
                    task_group, start_time_lower, start_time_upper
                )
            }
            TasksScanStrategy::FullScan => write!(f, "FullScan"),
        }
    }
}

/// Parse filters into a TasksScanStrategy.
pub fn parse_tasks_scan_strategy(filters: &[Expr]) -> TasksScanStrategy {
    let mut task_group: Option<String> = None;
    let mut time_lower: Option<i64> = None;
    let mut time_upper: Option<i64> = None;

    for f in filters {
        if let Some((col, val)) = parse_eq_filter(f) {
            if col == "task_group" {
                task_group = Some(val);
            }
        } else if let Some((col, op, val)) = parse_i64_comparison_filter(f)
            && col == "start_time_ms"
        {
            match op {
                I64ComparisonOp::Eq => {
                    time_lower = Some(val);
                    time_upper = Some(val);
                }
                I64ComparisonOp::Gt => {
                    time_lower = Some(val.saturating_add(1));
                }
                I64ComparisonOp::GtEq => {
                    time_lower = Some(val);
                }
                I64ComparisonOp::Lt => {
                    time_upper = Some(val.saturating_sub(1));
                }
                I64ComparisonOp::LtEq => {
                    time_upper = Some(val);
                }
            }
        }
    }

    if let Some(task_group) = task_group {
        TasksScanStrategy::TaskGroupScan {
            task_group,
            start_time_lower: time_lower,
            start_time_upper: time_upper,
        }
    } else {
        TasksScanStrategy::FullScan
    }
}

/// Classify task filters for pushdown.
pub fn classify_tasks_filters(filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
    let unqualified_filters: Vec<Expr> = filters.iter().map(|f| unqualify_expr(f)).collect();
    let strategy = parse_tasks_scan_strategy(&unqualified_filters);

    filters
        .iter()
        .map(|f| {
            let unqualified = unqualify_expr(f);
            if let Some((col, _)) = parse_eq_filter(&unqualified) {
                match strategy {
                    TasksScanStrategy::TaskGroupScan { .. } if col == "task_group" => {
                        TableProviderFilterPushDown::Exact
                    }
                    _ => TableProviderFilterPushDown::Inexact,
                }
            } else if let Some((col, ..)) = parse_i64_comparison_filter(&unqualified) {
                match strategy {
                    TasksScanStrategy::TaskGroupScan { .. } if col == "start_time_ms" => {
                        TableProviderFilterPushDown::Exact
                    }
                    _ => TableProviderFilterPushDown::Inexact,
                }
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

fn variant_type_name(vt: crate::fb::silo::fb::TaskVariant) -> &'static str {
    use crate::fb::silo::fb::TaskVariant;
    match vt {
        TaskVariant::RunAttempt => "RunAttempt",
        TaskVariant::RequestTicket => "RequestTicket",
        TaskVariant::CheckRateLimit => "CheckRateLimit",
        TaskVariant::RefreshFloatingLimit => "RefreshFloatingLimit",
        _ => "Unknown",
    }
}

impl Scan for TasksScanner {
    fn describe(&self, filters: &[Expr], limit: Option<usize>) -> String {
        let strategy = parse_tasks_scan_strategy(filters);
        format!("tasks[{}], limit={:?}", strategy, limit)
    }

    fn classify_filters(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        classify_tasks_filters(filters)
    }

    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let strategy = parse_tasks_scan_strategy(filters);
        let shard = Arc::clone(&self.shard);
        let proj = Arc::clone(&projection);

        let stream = async_stream::try_stream! {
            // Determine scan range based on strategy
            let (start, end) = match &strategy {
                TasksScanStrategy::TaskGroupScan {
                    task_group,
                    start_time_lower,
                    ..
                } => {
                    let start = if let Some(lower) = start_time_lower {
                        // Encode a key with the lower time bound (priority=0, empty job_id, attempt=0, epoch=0)
                        crate::keys::task_key(task_group, *lower, 0, "", 0, 0)
                    } else {
                        crate::keys::task_group_prefix(task_group)
                    };
                    let end = crate::keys::end_bound(&crate::keys::task_group_prefix(task_group));
                    (start, end)
                }
                TasksScanStrategy::FullScan => {
                    let start = crate::keys::tasks_prefix();
                    let end = crate::keys::end_bound(&start);
                    (start, end)
                }
            };

            // Extract upper time bound for early termination in TaskGroupScan
            let time_upper = match &strategy {
                TasksScanStrategy::TaskGroupScan {
                    start_time_upper, ..
                } => *start_time_upper,
                _ => None,
            };

            let mut cursor = shard
                .open_range_cursor(start, end, &crate::scan_options())
                .await
                .map_err(exec_err)?;
            let shard_id = shard.name().to_string();
            let mut buf: VecDeque<KeyValue> = VecDeque::new();
            let mut exhausted = false;
            let mut stop = false; // set when the upper time bound is crossed
            let mut total_emitted: usize = 0;

            // Collect rows in batches
            loop {
                if stop {
                    break;
                }
                if limit.is_some_and(|l| total_emitted >= l) {
                    break;
                }

                let remaining = limit.map(|l| l - total_emitted).unwrap_or(batch_size);
                let target = remaining.min(batch_size).max(1);

                let mut shard_ids = Vec::with_capacity(target);
                let mut tenants = Vec::with_capacity(target);
                let mut task_groups = Vec::with_capacity(target);
                let mut start_times = Vec::with_capacity(target);
                let mut priorities = Vec::with_capacity(target);
                let mut job_ids = Vec::with_capacity(target);
                let mut attempts = Vec::with_capacity(target);
                let mut variant_types = Vec::with_capacity(target);
                let mut task_ids = Vec::with_capacity(target);
                let mut held_queues_strs = Vec::with_capacity(target);

                let mut batch_count: usize = 0;

                while batch_count < target {
                    if buf.is_empty() {
                        if exhausted {
                            break;
                        }
                        let chunk = cursor
                            .next_kv_chunk(batch_size.max(DEFAULT_SCAN_CHUNK))
                            .await
                            .map_err(exec_err)?;
                        if chunk.is_empty() {
                            exhausted = true;
                            break;
                        }
                        buf.extend(chunk);
                    }
                    let kv = buf.pop_front().expect("buffer non-empty");

                    let Some(parsed) = crate::keys::parse_task_key(&kv.key) else {
                        continue;
                    };

                    // Early termination: if we have an upper time bound and the key's
                    // time exceeds it, stop scanning (keys are ordered by time within a group)
                    if let Some(upper) = time_upper
                        && parsed.start_time_ms > upper as u64
                    {
                        stop = true;
                        break;
                    }

                    // Decode the task value to get tenant and variant_type
                    let decoded = match crate::codec::decode_task_validated(
                        bytes::Bytes::copy_from_slice(&kv.value),
                    ) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                    shard_ids.push(shard_id.clone());
                    tenants.push(decoded.tenant().to_string());
                    task_groups.push(parsed.task_group.clone());
                    start_times.push(parsed.start_time_ms as i64);
                    priorities.push(parsed.priority);
                    job_ids.push(parsed.job_id.clone());
                    attempts.push(parsed.attempt);
                    variant_types.push(variant_type_name(decoded.variant_type()).to_string());

                    // Per-variant: pull task_id and (for RunAttempt) the held_queues list.
                    let (task_id_str, held_str) = match decoded.variant_type() {
                        crate::fb::silo::fb::TaskVariant::RunAttempt => {
                            let ra = decoded.as_run_attempt();
                            let tid = ra.and_then(|r| r.id()).unwrap_or_default().to_string();
                            let held = ra
                                .and_then(|r| r.held_queues())
                                .map(|v| v.iter().collect::<Vec<&str>>().join(","))
                                .unwrap_or_default();
                            (tid, held)
                        }
                        crate::fb::silo::fb::TaskVariant::RequestTicket => {
                            let rt = decoded.as_request_ticket();
                            let tid = rt.and_then(|r| r.task_id()).unwrap_or_default().to_string();
                            let held = rt
                                .and_then(|r| r.held_queues())
                                .map(|v| v.iter().collect::<Vec<&str>>().join(","))
                                .unwrap_or_default();
                            (tid, held)
                        }
                        crate::fb::silo::fb::TaskVariant::CheckRateLimit => {
                            let cr = decoded.as_check_rate_limit();
                            let tid = cr.and_then(|r| r.task_id()).unwrap_or_default().to_string();
                            let held = cr
                                .and_then(|r| r.held_queues())
                                .map(|v| v.iter().collect::<Vec<&str>>().join(","))
                                .unwrap_or_default();
                            (tid, held)
                        }
                        _ => (String::new(), String::new()),
                    };
                    task_ids.push(task_id_str);
                    held_queues_strs.push(held_str);

                    batch_count += 1;
                }

                if batch_count == 0 {
                    break;
                }

                total_emitted += batch_count;

                if proj.fields().is_empty() {
                    yield make_empty_projection_batch(&proj, batch_count)?;
                    continue;
                }

                let mut cols: Vec<ArrayRef> = Vec::with_capacity(proj.fields().len());
                for f in proj.fields() {
                    let col: ArrayRef = match f.name().as_str() {
                        "shard_id" => Arc::new(StringArray::from(shard_ids.clone())),
                        "tenant" => Arc::new(StringArray::from(tenants.clone())),
                        "task_group" => Arc::new(StringArray::from(task_groups.clone())),
                        "start_time_ms" => Arc::new(Int64Array::from(start_times.clone())),
                        "priority" => Arc::new(UInt8Array::from(priorities.clone())),
                        "job_id" => Arc::new(StringArray::from(job_ids.clone())),
                        "attempt" => Arc::new(UInt32Array::from(attempts.clone())),
                        "variant_type" => Arc::new(StringArray::from(variant_types.clone())),
                        "task_id" => Arc::new(StringArray::from(task_ids.clone())),
                        "held_queues" => Arc::new(StringArray::from(held_queues_strs.clone())),
                        other => {
                            Err(DataFusionError::Execution(format!(
                                "unknown tasks column: {}",
                                other
                            )))?
                        }
                    };
                    cols.push(col);
                }

                yield RecordBatch::try_new(Arc::clone(&proj), cols).map_err(exec_err)?;
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(projection, Box::pin(stream)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum I64ComparisonOp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

fn parse_i64_comparison_filter(expr: &Expr) -> Option<(String, I64ComparisonOp, i64)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match (&**left, &**right) {
            (Expr::Column(c), Expr::Literal(s, _)) => i64_comparison_op(op)
                .zip(literal_to_i64(s))
                .map(|(cmp, value)| (c.flat_name().to_string(), cmp, value)),
            (Expr::Literal(s, _), Expr::Column(c)) => inverted_i64_comparison_op(op)
                .zip(literal_to_i64(s))
                .map(|(cmp, value)| (c.flat_name().to_string(), cmp, value)),
            _ => None,
        },
        _ => None,
    }
}

fn i64_comparison_op(op: &Operator) -> Option<I64ComparisonOp> {
    match op {
        Operator::Eq => Some(I64ComparisonOp::Eq),
        Operator::Lt => Some(I64ComparisonOp::Lt),
        Operator::LtEq => Some(I64ComparisonOp::LtEq),
        Operator::Gt => Some(I64ComparisonOp::Gt),
        Operator::GtEq => Some(I64ComparisonOp::GtEq),
        _ => None,
    }
}

fn inverted_i64_comparison_op(op: &Operator) -> Option<I64ComparisonOp> {
    match op {
        Operator::Eq => Some(I64ComparisonOp::Eq),
        Operator::Lt => Some(I64ComparisonOp::Gt),
        Operator::LtEq => Some(I64ComparisonOp::GtEq),
        Operator::Gt => Some(I64ComparisonOp::Lt),
        Operator::GtEq => Some(I64ComparisonOp::LtEq),
        _ => None,
    }
}

fn literal_to_i64(s: &datafusion::scalar::ScalarValue) -> Option<i64> {
    use datafusion::scalar::ScalarValue;
    match s {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringComparisonOp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
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

fn parse_string_comparison_filter(expr: &Expr) -> Option<(String, StringComparisonOp, String)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match (&**left, &**right) {
            (Expr::Column(c), Expr::Literal(s, _)) => string_comparison_op(op)
                .zip(literal_to_string(s))
                .map(|(cmp, value)| (c.flat_name().to_string(), cmp, value)),
            (Expr::Literal(s, _), Expr::Column(c)) => inverted_string_comparison_op(op)
                .zip(literal_to_string(s))
                .map(|(cmp, value)| (c.flat_name().to_string(), cmp, value)),
            _ => None,
        },
        _ => None,
    }
}

fn string_comparison_op(op: &Operator) -> Option<StringComparisonOp> {
    match op {
        Operator::Eq => Some(StringComparisonOp::Eq),
        Operator::Lt => Some(StringComparisonOp::Lt),
        Operator::LtEq => Some(StringComparisonOp::LtEq),
        Operator::Gt => Some(StringComparisonOp::Gt),
        Operator::GtEq => Some(StringComparisonOp::GtEq),
        _ => None,
    }
}

fn inverted_string_comparison_op(op: &Operator) -> Option<StringComparisonOp> {
    match op {
        Operator::Eq => Some(StringComparisonOp::Eq),
        Operator::Lt => Some(StringComparisonOp::Gt),
        Operator::LtEq => Some(StringComparisonOp::GtEq),
        Operator::Gt => Some(StringComparisonOp::Lt),
        Operator::GtEq => Some(StringComparisonOp::LtEq),
        _ => None,
    }
}

fn parse_tenant_counts_scan_range(filters: &[Expr]) -> Option<TenantStatusCounterScanRange> {
    let mut lower: Option<(String, bool)> = None;
    let mut upper: Option<(String, bool)> = None;
    let mut saw_tenant_filter = false;

    for filter in filters {
        let Some((col, op, value)) = parse_string_comparison_filter(filter) else {
            continue;
        };
        if col != "tenant" {
            continue;
        }

        saw_tenant_filter = true;
        match op {
            StringComparisonOp::Eq => {
                update_lower_string_bound(&mut lower, value.clone(), true);
                update_upper_string_bound(&mut upper, value, true);
            }
            StringComparisonOp::Gt => update_lower_string_bound(&mut lower, value, false),
            StringComparisonOp::GtEq => update_lower_string_bound(&mut lower, value, true),
            StringComparisonOp::Lt => update_upper_string_bound(&mut upper, value, false),
            StringComparisonOp::LtEq => update_upper_string_bound(&mut upper, value, true),
        }
    }

    saw_tenant_filter.then(|| TenantStatusCounterScanRange {
        start_tenant: lower.as_ref().map(|(value, _)| value.clone()),
        start_inclusive: lower
            .as_ref()
            .map(|(_, inclusive)| *inclusive)
            .unwrap_or(true),
        end_tenant: upper.as_ref().map(|(value, _)| value.clone()),
        end_inclusive: upper
            .as_ref()
            .map(|(_, inclusive)| *inclusive)
            .unwrap_or(true),
    })
}

fn update_lower_string_bound(bound: &mut Option<(String, bool)>, value: String, inclusive: bool) {
    match bound {
        Some((current_value, current_inclusive)) => match value.cmp(current_value) {
            std::cmp::Ordering::Greater => {
                *bound = Some((value, inclusive));
            }
            std::cmp::Ordering::Equal => {
                *current_inclusive &= inclusive;
            }
            std::cmp::Ordering::Less => {}
        },
        None => *bound = Some((value, inclusive)),
    }
}

fn update_upper_string_bound(bound: &mut Option<(String, bool)>, value: String, inclusive: bool) {
    match bound {
        Some((current_value, current_inclusive)) => match value.cmp(current_value) {
            std::cmp::Ordering::Less => {
                *bound = Some((value, inclusive));
            }
            std::cmp::Ordering::Equal => {
                *current_inclusive &= inclusive;
            }
            std::cmp::Ordering::Greater => {}
        },
        None => *bound = Some((value, inclusive)),
    }
}

fn describe_tenant_status_counter_scan_range(range: &TenantStatusCounterScanRange) -> String {
    if range.start_inclusive
        && range.end_inclusive
        && range.start_tenant.is_some()
        && range.start_tenant == range.end_tenant
        && let Some(tenant) = range.start_tenant.as_ref()
    {
        return format!("tenant={:?}", tenant);
    }

    let mut parts = Vec::new();

    if let Some(tenant) = &range.start_tenant {
        parts.push(if range.start_inclusive {
            format!("tenant>={:?}", tenant)
        } else {
            format!("tenant>{:?}", tenant)
        });
    }

    if let Some(tenant) = &range.end_tenant {
        parts.push(if range.end_inclusive {
            format!("tenant<={:?}", tenant)
        } else {
            format!("tenant<{:?}", tenant)
        });
    }

    if parts.is_empty() {
        "all".to_string()
    } else {
        parts.join(", ")
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
