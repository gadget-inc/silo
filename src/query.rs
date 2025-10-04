use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session as CatalogSession;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContext;
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
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

const DEFAULT_SCAN_LIMIT: usize = 10_000;

use crate::job_store_shard::JobStoreShard;

/// Represents a query engine over a single `JobStoreShard` using Apache DataFusion.
pub struct JobSql {
    ctx: SessionContext,
}

/// Information about filters pushed down to the scan
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushedFilters {
    pub filters: Vec<String>,
}

impl std::fmt::Debug for JobSql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JobSql")
    }
}

impl JobSql {
    pub fn new(shard: Arc<JobStoreShard>, table_name: &str) -> DfResult<Self> {
        let ctx = SessionContext::new();
        let schema = JobsScanner::base_schema();
        let scanner: ScannerRef = Arc::new(JobsScanner { shard });
        let provider = Arc::new(SiloTableProvider::new(schema, scanner));
        ctx.register_table(table_name, provider)?;
        Ok(Self { ctx })
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn sql(&self, query: &str) -> DfResult<DataFrame> {
        self.ctx.sql(query).await
    }

    /// Get the EXPLAIN plan for a query to inspect optimization strategies
    pub async fn explain(&self, query: &str) -> DfResult<String> {
        let df = self.ctx.sql(query).await?;
        let explain_df = df.explain(false, false)?;
        let batches = explain_df.collect().await?;

        let mut output = String::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                for col in 0..batch.num_columns() {
                    if let Some(arr) = batch.column(col).as_any().downcast_ref::<StringArray>() {
                        if !arr.is_null(row) {
                            output.push_str(arr.value(row));
                            output.push('\n');
                        }
                    }
                }
            }
        }
        Ok(output)
    }

    /// Get the physical execution plan for a query to inspect what filters were pushed down
    ///
    /// This is useful for testing to verify that predicate pushdown is working correctly.
    ///
    /// # Example
    /// ```ignore
    /// let plan = sql.get_physical_plan("SELECT * FROM jobs WHERE id = 'foo'").await?;
    /// let filters = JobSql::extract_pushed_filters(&plan).expect("filters");
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
}

/// Scan trait and silo provider/plan
pub(crate) trait Scan: std::fmt::Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;
}

pub(crate) type ScannerRef = Arc<dyn Scan>;

// Implementation of the DataFusion TableProvider trait for all our scanners.
#[derive(Debug)]
pub(crate) struct SiloTableProvider {
    schema: SchemaRef,
    scanner: ScannerRef,
}

impl SiloTableProvider {
    pub(crate) fn new(schema: SchemaRef, scanner: ScannerRef) -> Self {
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
        let mut out: Vec<TableProviderFilterPushDown> = Vec::with_capacity(filters.len());
        for f in filters {
            if parse_metadata_eq_filter(f).is_some() || parse_metadata_contains_filter(f).is_some()
            {
                out.push(TableProviderFilterPushDown::Exact);
            } else {
                out.push(TableProviderFilterPushDown::Inexact);
            }
        }
        Ok(out)
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
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SiloExecutionPlan")
            }
            DisplayFormatType::TreeRender => write!(f, "SiloExecutionPlan"),
        }
    }
}

struct JobsScanner {
    shard: Arc<JobStoreShard>,
}

impl std::fmt::Debug for JobsScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JobsScanner")
    }
}

impl JobsScanner {
    fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
            Field::new("enqueue_time_ms", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("status_kind", DataType::Utf8, true),
            Field::new("status_changed_at_ms", DataType::Int64, true),
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

impl Scan for JobsScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let mut tenant: Option<String> = None;
        let mut status_kind: Option<crate::job::JobStatusKind> = None;
        let mut id_filter: Option<String> = None;
        // Track optional metadata filter of the form metadata['key'] = 'value' or element_at(metadata, 'key') = 'value'
        let mut metadata_filter: Option<(String, String)> = None;
        for f in filters {
            if let Some((col, val)) = parse_eq_filter(f) {
                match col.as_str() {
                    "tenant" => tenant = Some(val),
                    "status_kind" => status_kind = parse_status_kind(&val),
                    "id" => id_filter = Some(val),
                    _ => {}
                }
            } else if let Some((k, v)) = parse_metadata_eq_filter(f) {
                metadata_filter = Some((k, v));
            } else if let Some((k, v)) = parse_metadata_contains_filter(f) {
                metadata_filter = Some((k, v));
            }
        }
        let tenant = tenant.unwrap_or_else(|| "-".to_string());

        let (tx, rx) = mpsc::channel::<DfResult<RecordBatch>>(2);
        let shard = Arc::clone(&self.shard);
        let proj_for_stream = Arc::clone(&projection);
        tokio::spawn(async move {
            let mut sent: usize = 0;
            let hard_limit = limit.unwrap_or(DEFAULT_SCAN_LIMIT);

            let ids: Vec<String> = if let Some(idv) = id_filter.clone() {
                vec![idv]
            } else if let Some((meta_key, meta_val)) = metadata_filter.clone() {
                match shard
                    .scan_jobs_by_metadata(tenant.as_str(), &meta_key, &meta_val, hard_limit)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(e.to_string())))
                            .await;
                        return;
                    }
                }
            } else if let Some(kind) = status_kind {
                match shard
                    .scan_jobs_by_status(tenant.as_str(), kind, hard_limit)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(e.to_string())))
                            .await;
                        return;
                    }
                }
            } else {
                match shard.scan_jobs(tenant.as_str(), hard_limit).await {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(e.to_string())))
                            .await;
                        return;
                    }
                }
            };

            let mut i: usize = 0;
            while i < ids.len() && sent < hard_limit {
                let start = i;
                let end = std::cmp::min(ids.len(), start + batch_size);

                // Batch fetch all jobs and statuses for this batch to avoid N+1 queries
                let batch_ids: Vec<String> = ids[start..end].to_vec();
                let jobs_map = match shard.get_jobs_batch(tenant.as_str(), &batch_ids).await {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(e.to_string())))
                            .await;
                        return;
                    }
                };
                let status_map = match shard
                    .get_jobs_status_batch(tenant.as_str(), &batch_ids)
                    .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(e.to_string())))
                            .await;
                        return;
                    }
                };

                // Filter to only include IDs that actually have jobs
                let existing_ids: Vec<&String> = ids[start..end]
                    .iter()
                    .filter(|id| jobs_map.contains_key(*id))
                    .collect();

                // Skip this batch if no jobs exist
                if existing_ids.is_empty() {
                    i = end;
                    continue;
                }

                // Handle empty projection (when DataFusion just needs row count)
                if proj_for_stream.fields().is_empty() {
                    // Create an empty RecordBatch with the correct number of rows
                    let batch = match RecordBatch::try_new_with_options(
                        Arc::clone(&proj_for_stream),
                        vec![],
                        &datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(existing_ids.len())),
                    ) {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = tx
                                .send(Err(DataFusionError::Execution(e.to_string())))
                                .await;
                            return;
                        }
                    };
                    sent += batch.num_rows();
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                    i = end;
                    continue;
                }

                let mut cols: Vec<ArrayRef> = Vec::with_capacity(proj_for_stream.fields().len());
                for f in proj_for_stream.fields() {
                    match f.name().as_str() {
                        "tenant" => {
                            let vals: Vec<String> =
                                existing_ids.iter().map(|_| tenant.clone()).collect();
                            cols.push(Arc::new(StringArray::from(vals)));
                        }
                        "id" => {
                            let mut out: Vec<String> = Vec::with_capacity(existing_ids.len());
                            for id in &existing_ids {
                                out.push((*id).clone());
                            }
                            cols.push(Arc::new(StringArray::from(out)));
                        }
                        "priority" | "enqueue_time_ms" | "payload" => {
                            let mut prio: Vec<u8> = Vec::new();
                            let mut enq: Vec<i64> = Vec::new();
                            let mut payloads: Vec<Option<String>> = Vec::new();
                            let need_prio = f.name() == "priority";
                            let need_enq = f.name() == "enqueue_time_ms";
                            let need_payload = f.name() == "payload";
                            for id in &existing_ids {
                                if let Some(view) = jobs_map.get(*id) {
                                    if need_prio {
                                        prio.push(view.priority());
                                    }
                                    if need_enq {
                                        enq.push(view.enqueue_time_ms());
                                    }
                                    if need_payload {
                                        match view.payload_json() {
                                            Ok(v) => payloads.push(Some(v.to_string())),
                                            Err(_) => payloads.push(None),
                                        }
                                    }
                                }
                            }
                            if need_prio {
                                cols.push(Arc::new(UInt8Array::from(prio)));
                            }
                            if need_enq {
                                cols.push(Arc::new(Int64Array::from(enq)));
                            }
                            if need_payload {
                                cols.push(Arc::new(StringArray::from(payloads)));
                            }
                        }
                        "status_kind" | "status_changed_at_ms" => {
                            let mut kinds: Vec<Option<String>> = Vec::new();
                            let mut changed: Vec<Option<i64>> = Vec::new();
                            let need_kind = f.name() == "status_kind";
                            let need_changed = f.name() == "status_changed_at_ms";
                            for id in &existing_ids {
                                if let Some(status) = status_map.get(*id) {
                                    if need_kind {
                                        kinds.push(Some(format!("{:?}", status.kind())));
                                    }
                                    if need_changed {
                                        changed.push(Some(status.changed_at_ms()));
                                    }
                                } else {
                                    if need_kind {
                                        kinds.push(None);
                                    }
                                    if need_changed {
                                        changed.push(None);
                                    }
                                }
                            }
                            if need_kind {
                                cols.push(Arc::new(StringArray::from(kinds)));
                            }
                            if need_changed {
                                cols.push(Arc::new(Int64Array::from(changed)));
                            }
                        }
                        "metadata" => {
                            use datafusion::arrow::array::{MapArray, StructArray};

                            // Build the metadata map arrays
                            let mut all_metadata: Vec<Vec<(String, String)>> = Vec::new();
                            for id in &existing_ids {
                                if let Some(view) = jobs_map.get(*id) {
                                    all_metadata.push(view.metadata());
                                } else {
                                    all_metadata.push(Vec::new());
                                }
                            }

                            // Build keys and values arrays
                            let mut keys_builder = datafusion::arrow::array::StringBuilder::new();
                            let mut values_builder = datafusion::arrow::array::StringBuilder::new();
                            let mut offsets: Vec<i32> = Vec::with_capacity(all_metadata.len() + 1);
                            offsets.push(0);

                            let mut total_entries = 0i32;
                            for metadata in &all_metadata {
                                for (k, v) in metadata {
                                    keys_builder.append_value(k);
                                    values_builder.append_value(v);
                                    total_entries += 1;
                                }
                                offsets.push(total_entries);
                            }

                            let keys_array = Arc::new(keys_builder.finish());
                            let values_array = Arc::new(values_builder.finish());

                            // Create struct array for the entries
                            let struct_array = StructArray::try_new(
                                Fields::from(vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new("value", DataType::Utf8, true),
                                ]),
                                vec![keys_array, values_array],
                                None,
                            )
                            .map_err(|e| DataFusionError::Execution(e.to_string()));

                            let struct_array = match struct_array {
                                Ok(a) => a,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            };

                            // Create map array
                            let map_array = MapArray::new(
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
                            );

                            cols.push(Arc::new(map_array));
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
                sent += batch.num_rows();
                if tx.send(Ok(batch)).await.is_err() {
                    return;
                }
                i = end;
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
            if let Some(key) = extract_metadata_key_from_expr(left.as_ref()) {
                if let Expr::Literal(s, _) = right.as_ref() {
                    if let Some(val) = lit_str(s) {
                        return Some((key, val));
                    }
                }
            }
            // Or right is indexed metadata, left is literal
            if let Some(key) = extract_metadata_key_from_expr(right.as_ref()) {
                if let Expr::Literal(s, _) = left.as_ref() {
                    if let Some(val) = lit_str(s) {
                        return Some((key, val));
                    }
                }
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
        // Check if this is array_contains
        if func.func.name() == "array_contains" && func.args.len() == 2 {
            // First arg should be element_at(metadata, 'key')
            if let Some(key) = extract_metadata_key_from_expr(&func.args[0]) {
                // Second arg should be the literal value
                if let Expr::Literal(s, _) = &func.args[1] {
                    match s {
                        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                            return Some((key, v.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    None
}

// Extract metadata key from element_at(metadata, 'key') expressions using AST traversal
fn extract_metadata_key_from_expr(expr: &Expr) -> Option<String> {
    use datafusion::scalar::ScalarValue;

    // Match: element_at(metadata, 'key') or GetIndexedField for metadata['key']
    match expr {
        Expr::ScalarFunction(func) => {
            // element_at(metadata, 'key')
            if func.func.name() == "element_at" && func.args.len() == 2 {
                // First arg should be Column("metadata")
                if let Expr::Column(col) = &func.args[0] {
                    if col.name == "metadata" {
                        // Second arg should be the literal key
                        if let Expr::Literal(s, _) = &func.args[1] {
                            match s {
                                ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                                    return Some(v.clone());
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        // Note: DataFusion may not use GetIndexedField in this version,
        // relying on ScalarFunction for element_at instead
        _ => {}
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

fn parse_status_kind(s: &str) -> Option<crate::job::JobStatusKind> {
    use crate::job::JobStatusKind;
    match s {
        "Scheduled" | "scheduled" => Some(JobStatusKind::Scheduled),
        "Running" | "running" => Some(JobStatusKind::Running),
        "Failed" | "failed" => Some(JobStatusKind::Failed),
        "Cancelled" | "canceled" | "cancelled" => Some(JobStatusKind::Cancelled),
        "Succeeded" | "success" | "succeeded" => Some(JobStatusKind::Succeeded),
        _ => None,
    }
}
