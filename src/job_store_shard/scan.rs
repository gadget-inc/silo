//! Scanning and query operations for jobs.

use std::ops::ControlFlow;

use slatedb::KeyValue;
use slatedb::config::ScanOptions;

use crate::instrumented_db::{InstrumentedDb, InstrumentedDbIterator};
use crate::job::JobStatusKind;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    end_bound, idx_metadata_key_only_prefix, idx_metadata_prefix, idx_status_time_prefix,
    job_info_prefix, jobs_prefix, parse_job_info_key, parse_metadata_index_key,
    parse_status_time_index_key,
};
use crate::metrics::Metrics;

/// An owning, pull-based cursor over a SlateDB key range.
///
/// This is the primitive the streaming query scanners build on. It differs from
/// the collect-oriented [`scan_collect`] helper in two load-bearing ways:
///
/// 1. **Laziness / cancellation.** The cursor owns the underlying
///    [`InstrumentedDbIterator`] (and thus the SlateDB read snapshot). When the
///    query stream that holds it is dropped — e.g. because the statement
///    deadline fired or the client disconnected — the cursor is dropped and the
///    scan stops at the next `.await`. There is no detached task to leak.
/// 2. **Incremental yield.** Callers pull one bounded chunk at a time
///    ([`next_kv_chunk`]) and emit a `RecordBatch` per chunk, so peak memory is
///    O(chunk) rather than O(entire index).
///
/// Every key pulled is counted into the `silo_query_scanned_keys_total` metric
/// (per shard), which is how tests and dashboards observe that a scan actually
/// stopped early instead of running to completion as a zombie.
pub(crate) struct RangeScanCursor {
    iter: InstrumentedDbIterator,
    shard_name: String,
    metrics: Option<Metrics>,
}

impl RangeScanCursor {
    /// Pull up to `max` more key/value pairs from the range. Returns an empty
    /// `Vec` once the range is exhausted. Each returned key counts toward the
    /// per-shard scanned-keys metric.
    ///
    /// This is the sole `.await` point in a streaming scan loop, so it is also
    /// the cancellation point: dropping the cursor between calls stops the scan.
    pub(crate) async fn next_kv_chunk(
        &mut self,
        max: usize,
    ) -> Result<Vec<KeyValue>, JobStoreShardError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(max.min(1024));
        while out.len() < max {
            match self.iter.next().await? {
                Some(kv) => out.push(kv),
                None => break,
            }
        }
        if !out.is_empty()
            && let Some(metrics) = &self.metrics
        {
            metrics.record_query_scanned_keys(&self.shard_name, out.len() as u64);
        }
        Ok(out)
    }
}

/// Shared scan-and-collect loop: opens a range iterator, applies a per-key
/// extractor, and collects up to `limit` results.
///
/// `extract` receives each key and returns:
/// - `ControlFlow::Continue(Some(val))` to include the value
/// - `ControlFlow::Continue(None)` to skip the entry
/// - `ControlFlow::Break(())` to stop iteration early
async fn scan_collect<T>(
    db: &InstrumentedDb,
    start: Vec<u8>,
    end: Vec<u8>,
    limit: Option<usize>,
    mut extract: impl FnMut(&[u8]) -> ControlFlow<(), Option<T>>,
) -> Result<Vec<T>, JobStoreShardError> {
    if limit == Some(0) {
        return Ok(Vec::new());
    }

    let mut iter = db
        .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
        .await?;
    let capacity = limit.unwrap_or(1024).min(1024);
    let mut out = Vec::with_capacity(capacity);

    loop {
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
        let Some(kv) = iter.next().await? else {
            break;
        };
        match extract(&kv.key) {
            ControlFlow::Continue(Some(val)) => out.push(val),
            ControlFlow::Continue(None) => {}
            ControlFlow::Break(()) => break,
        }
    }

    Ok(out)
}

impl JobStoreShard {
    /// Open a [`RangeScanCursor`] over `start..end` for incremental, cancellable
    /// streaming scans. Prefer this over [`scan_collect`] on the query path,
    /// where results must be yielded in bounded chunks rather than materialized
    /// whole.
    pub(crate) async fn open_range_cursor(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        opts: &ScanOptions,
    ) -> Result<RangeScanCursor, JobStoreShardError> {
        let iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, opts)
            .await?;
        Ok(RangeScanCursor {
            iter,
            shard_name: self.name.clone(),
            metrics: self.metrics.clone(),
        })
    }

    /// Scan all jobs for a tenant ordered by job id (lexicographic), unfiltered.
    pub async fn scan_jobs(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let start = job_info_prefix(tenant);
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_job_info_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| p.job_id),
            )
        })
        .await
    }

    /// Scan all jobs across ALL tenants, returning (tenant, job_id) pairs.
    /// Used for admin queries that need cluster-wide visibility.
    pub async fn scan_all_jobs(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let start = jobs_prefix();
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_job_info_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| (p.tenant, p.job_id)),
            )
        })
        .await
    }

    /// Scan newest-first job IDs by status using the time-ordered index.
    pub async fn scan_jobs_by_status(
        &self,
        tenant: &str,
        status: JobStatusKind,
        limit: Option<usize>,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let start = idx_status_time_prefix(tenant, status.as_str());
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| p.job_id),
            )
        })
        .await
    }

    /// Scan jobs by metadata key/value. Order is not specified.
    pub async fn scan_jobs_by_metadata(
        &self,
        tenant: &str,
        key: &str,
        value: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let start = idx_metadata_prefix(tenant, key, value);
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |k| {
            ControlFlow::Continue(
                parse_metadata_index_key(k)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| p.job_id),
            )
        })
        .await
    }

    /// Scan jobs by metadata key with a value prefix. Returns jobs where the metadata
    /// value for the given key starts with `value_prefix`. Order is not specified.
    pub async fn scan_jobs_by_metadata_prefix(
        &self,
        tenant: &str,
        key: &str,
        value_prefix: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let start = idx_metadata_prefix(tenant, key, value_prefix);
        let end = end_bound(&idx_metadata_key_only_prefix(tenant, key));
        scan_collect(&self.db, start, end, limit, |k| {
            let Some(parsed) = parse_metadata_index_key(k) else {
                return ControlFlow::Continue(None);
            };
            if parsed.value.starts_with(value_prefix) && !parsed.job_id.is_empty() {
                ControlFlow::Continue(Some(parsed.job_id))
            } else if parsed.value.as_str() > value_prefix
                && !parsed.value.starts_with(value_prefix)
            {
                // Values are sorted lexicographically; once we've passed the prefix range, stop
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(None)
            }
        })
        .await
    }
}
