//! Scanning and query operations for jobs.

use std::ops::ControlFlow;

use slatedb::KeyValue;
use slatedb::config::ScanOptions;

use crate::instrumented_db::{InstrumentedDb, InstrumentedDbIterator};
use crate::job::JobStatusKind;
use crate::job::{JobStatus, JobView};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    end_bound, idx_metadata_key_only_prefix, idx_metadata_prefix, idx_status_time_all_prefix,
    idx_status_time_prefix, idx_status_time_prefix_with_time, idx_status_time_tenant_prefix,
    job_info_prefix, job_status_prefix, jobs_prefix, jobs_status_prefix, parse_job_info_key,
    parse_job_status_key, parse_metadata_index_key, parse_status_time_index_key,
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

    /// Scan newest-first job IDs by status across ALL tenants, returning (tenant, job_id) pairs.
    /// Used for admin queries that need cluster-wide visibility.
    pub async fn scan_all_jobs_by_status(
        &self,
        status: JobStatusKind,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let status_str = status.as_str();
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| p.status == status_str && !p.job_id.is_empty())
                    .map(|p| (p.tenant, p.job_id)),
            )
        })
        .await
    }

    /// Scan the status/time index for a single tenant, returning (job_id, status_kind, changed_at_ms)
    /// for every job without any point-lookup round-trips.  Used for index-only query scans
    /// when the projection only needs status fields (not job_info fields like payload or priority).
    pub async fn scan_jobs_with_status_kind(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String, i64)>, JobStoreShardError> {
        let start = idx_status_time_tenant_prefix(tenant);
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| (p.job_id.clone(), p.status.clone(), p.changed_at_ms())),
            )
        })
        .await
    }

    /// Scan the status/time index for ALL tenants, returning (tenant, job_id, status_kind, changed_at_ms)
    /// for every job without any point-lookup round-trips.  Used for cross-tenant index-only scans.
    pub async fn scan_all_jobs_with_status_kind(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String, String, i64)>, JobStoreShardError> {
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| {
                        (
                            p.tenant.clone(),
                            p.job_id.clone(),
                            p.status.clone(),
                            p.changed_at_ms(),
                        )
                    }),
            )
        })
        .await
    }

    /// Scan jobs that are waiting (Scheduled with start_time <= now) for a tenant.
    /// Uses range scan within the Scheduled prefix from inverted(now_ms) to end.
    pub async fn scan_jobs_waiting(
        &self,
        tenant: &str,
        now_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<String>, JobStoreShardError> {
        // Waiting jobs have start_time <= now, which means inverted_timestamp >= u64::MAX - now
        // These sort AFTER future jobs in the index, so scan from the boundary to end.
        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_prefix_with_time(tenant, "Scheduled", inverted_now);
        let end = end_bound(&idx_status_time_prefix(tenant, "Scheduled"));
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| p.job_id),
            )
        })
        .await
    }

    /// Scan waiting jobs across ALL tenants, returning (tenant, job_id) pairs.
    pub async fn scan_all_jobs_waiting(
        &self,
        now_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| {
                        p.status == "Scheduled"
                            && p.inverted_timestamp >= inverted_now
                            && !p.job_id.is_empty()
                    })
                    .map(|p| (p.tenant, p.job_id)),
            )
        })
        .await
    }

    /// Scan future-scheduled jobs (Scheduled with start_time > now) for a tenant.
    /// Uses range scan from Scheduled prefix start to inverted(now_ms).
    pub async fn scan_jobs_future_scheduled(
        &self,
        tenant: &str,
        now_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<String>, JobStoreShardError> {
        // Future jobs have start_time > now, which means inverted_timestamp < u64::MAX - now
        // These sort BEFORE waiting jobs in the index, so scan from prefix start to the boundary.
        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_prefix(tenant, "Scheduled");
        let end = idx_status_time_prefix_with_time(tenant, "Scheduled", inverted_now);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| !p.job_id.is_empty())
                    .map(|p| p.job_id),
            )
        })
        .await
    }

    /// Scan future-scheduled jobs across ALL tenants, returning (tenant, job_id) pairs.
    pub async fn scan_all_jobs_future_scheduled(
        &self,
        now_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        scan_collect(&self.db, start, end, limit, |key| {
            ControlFlow::Continue(
                parse_status_time_index_key(key)
                    .filter(|p| {
                        p.status == "Scheduled"
                            && p.inverted_timestamp < inverted_now
                            && !p.job_id.is_empty()
                    })
                    .map(|p| (p.tenant, p.job_id)),
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

    /// Range scan returning (job_id, JobView) pairs for a tenant.
    /// Reads each job_info KV exactly once — the value IS the job data.
    /// Avoids the double-read that would occur from scan_jobs + get_jobs_batch.
    pub async fn scan_jobs_with_views(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, JobView)>, JobStoreShardError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let start = job_info_prefix(tenant);
        let end = end_bound(&start);
        let mut iter = self
            .db
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
            let Some(p) = parse_job_info_key(&kv.key).filter(|p| !p.job_id.is_empty()) else {
                continue;
            };
            out.push((p.job_id, JobView::new(kv.value)?));
        }
        Ok(out)
    }

    /// Range scan returning (tenant, job_id, JobView) across ALL tenants.
    /// Replaces scan_all_jobs + get_jobs_batch for full-cluster scans.
    pub async fn scan_all_jobs_with_views(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String, JobView)>, JobStoreShardError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let start = jobs_prefix();
        let end = end_bound(&start);
        let mut iter = self
            .db
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
            let Some(p) = parse_job_info_key(&kv.key).filter(|p| !p.job_id.is_empty()) else {
                continue;
            };
            out.push((p.tenant, p.job_id, JobView::new(kv.value)?));
        }
        Ok(out)
    }

    /// Range scan returning (job_id, JobStatus) for a tenant's status records.
    /// Replaces batched get_jobs_status_batch point-lookups for full-tenant scans.
    pub async fn scan_jobs_status_records(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, JobStatus)>, JobStoreShardError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let start = job_status_prefix(tenant);
        let end = end_bound(&start);
        let mut iter = self
            .db
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
            let Some(p) = parse_job_status_key(&kv.key).filter(|p| !p.job_id.is_empty()) else {
                continue;
            };
            out.push((
                p.job_id,
                crate::job_store_shard::helpers::decode_job_status_owned(&kv.value)?,
            ));
        }
        Ok(out)
    }

    /// Range scan returning (tenant, job_id, JobStatus) across ALL tenants.
    /// Replaces batched get_jobs_status_batch point-lookups for full-cluster scans.
    pub async fn scan_all_jobs_status_records(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String, JobStatus)>, JobStoreShardError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let start = jobs_status_prefix();
        let end = end_bound(&start);
        let mut iter = self
            .db
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
            let Some(p) = parse_job_status_key(&kv.key).filter(|p| !p.job_id.is_empty()) else {
                continue;
            };
            out.push((
                p.tenant,
                p.job_id,
                crate::job_store_shard::helpers::decode_job_status_owned(&kv.value)?,
            ));
        }
        Ok(out)
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
