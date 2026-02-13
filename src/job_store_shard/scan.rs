//! Scanning and query operations for jobs.

use std::ops::ControlFlow;

use slatedb::Db;

use crate::job::JobStatusKind;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    end_bound, idx_metadata_key_only_prefix, idx_metadata_prefix, idx_status_time_all_prefix,
    idx_status_time_prefix, idx_status_time_prefix_with_time, job_info_prefix, jobs_prefix,
    parse_job_info_key, parse_metadata_index_key, parse_status_time_index_key,
};

/// Shared scan-and-collect loop: opens a range iterator, applies a per-key
/// extractor, and collects up to `limit` results.
///
/// `extract` receives each key and returns:
/// - `ControlFlow::Continue(Some(val))` to include the value
/// - `ControlFlow::Continue(None)` to skip the entry
/// - `ControlFlow::Break(())` to stop iteration early
async fn scan_collect<T>(
    db: &Db,
    start: Vec<u8>,
    end: Vec<u8>,
    limit: Option<usize>,
    mut extract: impl FnMut(&[u8]) -> ControlFlow<(), Option<T>>,
) -> Result<Vec<T>, JobStoreShardError> {
    if limit == Some(0) {
        return Ok(Vec::new());
    }

    let mut iter = db.scan::<Vec<u8>, _>(start..end).await?;
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
