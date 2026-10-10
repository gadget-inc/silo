//! Prefix search and queue discovery using index keys, without reading unrelated payloads.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::Stream;

use crate::job::{JobStatus, JobStatusKind};
use crate::job_store_shard::JobStoreShard;
use crate::keys;
use crate::query::{
    JobsScanStrategy, OrderingColumn, QueryStatusFilter, Scan, ScanDecision, StringComparisonOp,
    analyze_projection, build_job_pairs_batch, exec_err, extract_metadata_key_from_expr,
    fetch_batch_data, parse_eq_filter, parse_string_comparison_filter,
};

/// Fixed-size candidate filter: broad prefixes cannot allocate one String per job.
/// False positives are harmless: SQL rechecks search predicates, and discovery
/// checks the current status before emitting a value. There are no false negatives.
struct CandidateIds {
    bits: Vec<u64>,
}

impl CandidateIds {
    fn new() -> Self {
        Self {
            bits: vec![0; 1 << 18],
        }
    }
    fn positions(id: &str) -> impl Iterator<Item = usize> {
        let first = xxhash_rust::xxh64::xxh64(id.as_bytes(), 0);
        let step = xxhash_rust::xxh64::xxh64(id.as_bytes(), 1) | 1;
        (0..4).map(move |i| first.wrapping_add(step.wrapping_mul(i)) as usize & ((1 << 24) - 1))
    }
    fn insert(&mut self, id: &str) {
        for bit in Self::positions(id) {
            self.bits[bit / 64] |= 1 << (bit % 64);
        }
    }
    fn contains(&self, id: &str) -> bool {
        Self::positions(id).all(|bit| self.bits[bit / 64] & (1 << (bit % 64)) != 0)
    }
}

#[derive(Clone, Debug)]
enum SearchField {
    Id,
    Metadata(String),
}

#[derive(Clone, Debug)]
pub(crate) struct PrefixSearch {
    field: SearchField,
    prefix: String,
}

fn literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => Some(s),
        _ => None,
    }
}

/// Only recognize an entire OR expression of case-insensitive prefixes.
/// All predicates remain SQL post-filters; these indexes only select candidates.
fn parse_search(expr: &Expr) -> Option<Vec<PrefixSearch>> {
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Or
    {
        let mut arms = parse_search(&binary.left)?;
        arms.extend(parse_search(&binary.right)?);
        return Some(arms);
    }
    let (value, prefix) = match expr {
        Expr::ScalarFunction(f) if f.func.name() == "starts_with" && f.args.len() == 2 => {
            (&f.args[0], literal(&f.args[1])?.to_string())
        }
        Expr::Like(like) if !like.negated && !like.case_insensitive => {
            let pattern = literal(&like.pattern)?;
            let mut chars = pattern.chars();
            let mut prefix = String::new();
            while let Some(ch) = chars.next() {
                // DataFusion can rewrite starts_with into LIKE. Stop at the first
                // wildcard, keeping a conservative candidate prefix; the SQL
                // post-filter still enforces the complete original predicate.
                if ch == like.escape_char.unwrap_or('\\') {
                    prefix.push(chars.next()?);
                } else if ch == '%' || ch == '_' {
                    break;
                } else {
                    prefix.push(ch);
                }
            }
            (like.expr.as_ref(), prefix)
        }
        _ => return None,
    };
    let Expr::ScalarFunction(lower) = value else {
        return None;
    };
    if lower.func.name() != "lower" || lower.args.len() != 1 {
        return None;
    }
    let field = match &lower.args[0] {
        Expr::Column(col) if col.name == "id" => SearchField::Id,
        expr => SearchField::Metadata(extract_metadata_key_from_expr(expr)?),
    };
    Some(vec![PrefixSearch { field, prefix }])
}

pub(crate) fn search_filter(filters: &[Expr]) -> Option<Vec<PrefixSearch>> {
    filters.iter().find_map(parse_search)
}

/// Read one distinct value, then seek past its entire job-ID range. A queue with
/// 100,000 jobs costs the same to discover as a queue with one job.
async fn next_value(
    shard: &JobStoreShard,
    start: Vec<u8>,
    end: Vec<u8>,
) -> DfResult<Option<String>> {
    if start >= end {
        return Ok(None);
    }
    let mut cursor = shard
        .open_range_cursor(start, end, &crate::scan_options())
        .await
        .map_err(exec_err)?;
    let rows = cursor.next_kv_chunk(1).await.map_err(exec_err)?;
    Ok(rows
        .first()
        .and_then(|kv| keys::parse_metadata_index_key(&kv.key))
        .map(|p| p.value))
}

/// Restrict discovery to the requested value range before reading any statuses.
/// In particular, a keyset page must not rescan every preceding queue's history.
fn metadata_value_range(tenant: &str, key: &str, filters: &[Expr]) -> (Vec<u8>, Vec<u8>) {
    let mut start = keys::idx_metadata_key_only_prefix(tenant, key);
    let mut end = keys::end_bound(&start);
    for filter in filters {
        let Some((column, op, value)) = parse_string_comparison_filter(filter) else {
            continue;
        };
        if column != "value" {
            continue;
        }
        let prefix = keys::idx_metadata_prefix(tenant, key, &value);
        match op {
            StringComparisonOp::Eq => {
                end = end.min(keys::end_bound(&prefix));
                start = start.max(prefix);
            }
            StringComparisonOp::Gt => start = start.max(keys::end_bound(&prefix)),
            StringComparisonOp::GtEq => start = start.max(prefix),
            StringComparisonOp::Lt => end = end.min(prefix),
            StringComparisonOp::LtEq => end = end.min(keys::end_bound(&prefix)),
        }
    }
    (start, end)
}

pub(crate) fn search_index_stream(
    shard: Arc<JobStoreShard>,
    projection: SchemaRef,
    tenant: String,
    strategy: JobsScanStrategy,
    searches: Vec<PrefixSearch>,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    async_stream::try_stream! {
        let mut candidates = CandidateIds::new();
        for search in &searches {
            let SearchField::Metadata(key) = &search.field else {
                continue;
            };
            let mut start = keys::idx_metadata_key_only_prefix(&tenant, key);
            let end = keys::end_bound(&start);
            while let Some(value) = next_value(&shard, start, end.clone()).await? {
                let value_start = keys::idx_metadata_prefix(&tenant, key, &value);
                start = keys::end_bound(&value_start);
                if !value.to_lowercase().starts_with(&search.prefix) {
                    continue;
                }
                let mut cursor = shard
                    .open_range_cursor(value_start, start.clone(), &crate::scan_options())
                    .await
                    .map_err(exec_err)?;
                loop {
                    let rows = cursor.next_kv_chunk(1024).await.map_err(exec_err)?;
                    if rows.is_empty() {
                        break;
                    }
                    for row in rows {
                        if let Some(p) = keys::parse_metadata_index_key(&row.key) {
                            candidates.insert(&p.job_id);
                        }
                    }
                }
            }
        }
        let start = keys::idx_enqueue_time_tenant_prefix(&tenant);
        let mut cursor = shard
            .open_range_cursor(
                start.clone(),
                keys::end_bound(&start),
                &crate::scan_options(),
            )
            .await
            .map_err(exec_err)?;
        let mut needs = analyze_projection(&projection, &strategy);
        needs.needs_existence_check = true;
        // Status predicates may be classified Exact and omitted by DataFusion.
        // Preserve their contract even when search changes the physical scan path.
        let statuses_to_match = match &strategy {
            JobsScanStrategy::Status { status, .. } => vec![*status],
            JobsScanStrategy::StatusUnion { statuses, .. } => statuses.clone(),
            _ => Vec::new(),
        };
        needs.need_status_point_lookup |= !statuses_to_match.is_empty();
        let now_ms = crate::job_store_shard::helpers::now_epoch_ms();
        let mut sent = 0;
        loop {
            if limit.is_some_and(|l| sent >= l) {
                break;
            }
            let rows = cursor
                .next_kv_chunk(batch_size.clamp(1, 256))
                .await
                .map_err(exec_err)?;
            if rows.is_empty() {
                break;
            }
            let mut pairs = Vec::new();
            for row in rows {
                let Some(p) = keys::parse_enqueue_time_index_key(&row.key) else {
                    continue;
                };
                if candidates.contains(&p.job_id)
                    || searches.iter().any(|s| {
                        matches!(s.field, SearchField::Id)
                            && p.job_id.to_lowercase().starts_with(&s.prefix)
                    })
                {
                    pairs.push((tenant.clone(), p.job_id));
                }
            }
            if pairs.is_empty() {
                continue;
            }
            let (jobs, statuses) = fetch_batch_data(&shard, &pairs, &needs).await?;
            let mut existing: Vec<_> = pairs
                .iter()
                .filter(|p| {
                    jobs.contains_key(*p)
                        && (statuses_to_match.is_empty()
                            || statuses.get(*p).is_some_and(|s| {
                                statuses_to_match
                                    .iter()
                                    .any(|filter| matches_status(*filter, s, now_ms))
                            }))
                })
                .collect();
            if let Some(l) = limit {
                existing.truncate(l - sent);
            }
            if existing.is_empty() {
                continue;
            }
            let batch = build_job_pairs_batch(&projection, shard.name(), &existing, &jobs, &statuses)?;
            sent += batch.num_rows();
            yield batch;
        }
    }
}

fn matches_status(filter: QueryStatusFilter, status: &JobStatus, now_ms: i64) -> bool {
    match filter {
        QueryStatusFilter::Stored(kind) => status.kind == kind,
        QueryStatusFilter::Waiting => {
            status.kind == JobStatusKind::Scheduled
                && status
                    .next_attempt_starts_after_ms
                    .is_none_or(|t| t <= now_ms)
        }
        QueryStatusFilter::FutureScheduled => {
            status.kind == JobStatusKind::Scheduled
                && status
                    .next_attempt_starts_after_ms
                    .is_some_and(|t| t > now_ms)
        }
    }
}

/// Distinct metadata values belonging to waiting/scheduled jobs (including retries).
/// Uses only the existing status and metadata indexes; no schema backfill required.
pub struct PendingMetadataScanner {
    pub(crate) shard: Arc<JobStoreShard>,
}

impl std::fmt::Debug for PendingMetadataScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PendingMetadataScanner")
    }
}

impl PendingMetadataScanner {
    pub fn new(shard: Arc<JobStoreShard>) -> Self {
        Self { shard }
    }

    pub fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }
}

impl Scan for PendingMetadataScanner {
    fn resolve(&self, _: &SchemaRef, _: &[Expr], _: Option<usize>) -> ScanDecision {
        ScanDecision::single_path().ordered_with_fetch(vec![OrderingColumn {
            name: "value".to_string(),
            descending: false,
        }])
    }

    fn describe(&self, _: &ScanDecision, _: &[Expr], _: Option<usize>) -> String {
        "pending_metadata[metadata index, status checks]".to_string()
    }

    fn scan(
        &self,
        _: &ScanDecision,
        projection: SchemaRef,
        filters: &[Expr],
        _: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let shard = Arc::clone(&self.shard);
        let proj = Arc::clone(&projection);
        let equality = |column: &str| {
            filters
                .iter()
                .filter_map(parse_eq_filter)
                .find(|(k, _)| k == column)
                .map(|(_, v)| v)
        };
        let tenant = equality("tenant");
        let key = equality("key");
        let filters = filters.to_vec();
        let stream = async_stream::try_stream! {
            let (Some(tenant), Some(key)) = (tenant, key) else {
                Err(DataFusionError::Plan(
                    "pending_metadata requires tenant and key equalities".to_string(),
                ))?;
                return;
            };
            if limit == Some(0) {
                return;
            }
            // Most live queues can be confirmed with their first status record.
            // Build a pending-ID filter only after encountering terminal history,
            // avoiding an O(pending jobs) scan before the first queue is returned.
            let mut pending: Option<CandidateIds> = None;
            let mut unsuccessful_probes = 0;
            let (mut start, end) = metadata_value_range(&tenant, &key, &filters);
            if start >= end {
                return;
            }
            let mut sent = 0;
            while let Some(value) = next_value(&shard, start, end.clone()).await? {
                let value_start = keys::idx_metadata_prefix(&tenant, &key, &value);
                start = keys::end_bound(&value_start);
                let mut cursor = shard
                    .open_range_cursor(value_start, start.clone(), &crate::scan_options())
                    .await
                    .map_err(exec_err)?;
                let mut found = false;
                loop {
                    let rows = cursor.next_kv_chunk(256).await.map_err(exec_err)?;
                    if rows.is_empty() {
                        break;
                    }
                    for row in rows {
                        let Some(p) = keys::parse_metadata_index_key(&row.key) else {
                            continue;
                        };
                        if pending.is_none() && unsuccessful_probes >= 32 {
                            let Some(filter) = pending_candidates(&shard, &tenant).await? else {
                                return;
                            };
                            pending = Some(filter);
                        }
                        if pending
                            .as_ref()
                            .is_some_and(|filter| !filter.contains(&p.job_id))
                        {
                            continue;
                        }
                        if let Some(metrics) = &shard.metrics {
                            metrics.record_query_point_lookups(shard.name(), 1);
                        }
                        if shard
                            .get_job_status(&tenant, &p.job_id)
                            .await
                            .map_err(exec_err)?
                            .is_some_and(|s| s.kind == JobStatusKind::Scheduled)
                        {
                            found = true;
                            break;
                        }
                        unsuccessful_probes += 1;
                    }
                    if found {
                        break;
                    }
                }
                if found {
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for field in proj.fields() {
                        let text = match field.name().as_str() {
                            "tenant" => &tenant,
                            "key" => &key,
                            "value" => &value,
                            other => Err(DataFusionError::Execution(format!(
                                "unknown column {other}"
                            )))?,
                        };
                        cols.push(Arc::new(StringArray::from(vec![text.as_str()])));
                    }
                    yield RecordBatch::try_new(Arc::clone(&proj), cols)?;
                    sent += 1;
                    if limit.is_some_and(|limit| sent >= limit) {
                        break;
                    }
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(projection, Box::pin(stream)))
    }
}

/// Avoid point-reading a long terminal history when a value has no pending jobs.
async fn pending_candidates(shard: &JobStoreShard, tenant: &str) -> DfResult<Option<CandidateIds>> {
    let start = keys::idx_status_time_prefix(tenant, "Scheduled");
    let mut cursor = shard
        .open_range_cursor(
            start.clone(),
            keys::end_bound(&start),
            &crate::scan_options(),
        )
        .await
        .map_err(exec_err)?;
    let mut pending = CandidateIds::new();
    let mut has_pending = false;
    loop {
        let rows = cursor.next_kv_chunk(1024).await.map_err(exec_err)?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            if let Some(p) = keys::parse_status_time_index_key(&row.key) {
                pending.insert(&p.job_id);
                has_pending = true;
            }
        }
    }
    Ok(has_pending.then_some(pending))
}
