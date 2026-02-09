//! Scanning and query operations for jobs.

use slatedb::DbIterator;

use crate::job::JobStatusKind;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    end_bound, idx_metadata_prefix, idx_status_time_all_prefix, idx_status_time_prefix,
    idx_status_time_prefix_with_time, job_info_prefix, jobs_prefix, parse_job_info_key,
    parse_metadata_index_key, parse_status_time_index_key,
};

impl JobStoreShard {
    /// Scan all jobs for a tenant ordered by job id (lexicographic), unfiltered.
    pub async fn scan_jobs(
        &self,
        tenant: &str,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start = job_info_prefix(tenant);
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_job_info_key(&kv.key)
                && !parsed.job_id.is_empty()
            {
                out.push(parsed.job_id);
            }
        }

        Ok(out)
    }

    /// Scan all jobs across ALL tenants, returning (tenant, job_id) pairs.
    /// Used for admin queries that need cluster-wide visibility.
    pub async fn scan_all_jobs(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start = jobs_prefix();
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_job_info_key(&kv.key)
                && !parsed.job_id.is_empty()
            {
                out.push((parsed.tenant, parsed.job_id));
            }
        }

        Ok(out)
    }

    /// Scan newest-first job IDs by status using the time-ordered index.
    pub async fn scan_jobs_by_status(
        &self,
        tenant: &str,
        status: JobStatusKind,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start = idx_status_time_prefix(tenant, status.as_str());
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_status_time_index_key(&kv.key)
                && !parsed.job_id.is_empty()
            {
                out.push(parsed.job_id);
            }
        }

        Ok(out)
    }

    /// Scan newest-first job IDs by status across ALL tenants, returning (tenant, job_id) pairs.
    /// Used for admin queries that need cluster-wide visibility.
    pub async fn scan_all_jobs_by_status(
        &self,
        status: JobStatusKind,
        limit: usize,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let status_str = status.as_str();
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_status_time_index_key(&kv.key) {
                // Filter by status since we're scanning all statuses
                if parsed.status == status_str && !parsed.job_id.is_empty() {
                    out.push((parsed.tenant, parsed.job_id));
                }
            }
        }

        Ok(out)
    }

    /// Scan jobs that are waiting (Scheduled with start_time <= now) for a tenant.
    /// Uses range scan within the Scheduled prefix from inverted(now_ms) to end.
    pub async fn scan_jobs_waiting(
        &self,
        tenant: &str,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        // Waiting jobs have start_time <= now, which means inverted_timestamp >= u64::MAX - now
        // These sort AFTER future jobs in the index, so scan from the boundary to end.
        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_prefix_with_time(tenant, "Scheduled", inverted_now);
        let prefix_end = end_bound(&idx_status_time_prefix(tenant, "Scheduled"));
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..prefix_end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_status_time_index_key(&kv.key)
                && !parsed.job_id.is_empty()
            {
                out.push(parsed.job_id);
            }
        }

        Ok(out)
    }

    /// Scan waiting jobs across ALL tenants, returning (tenant, job_id) pairs.
    pub async fn scan_all_jobs_waiting(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_status_time_index_key(&kv.key) {
                // Filter: must be Scheduled status AND inverted_timestamp >= inverted_now (start_time <= now)
                if parsed.status == "Scheduled"
                    && parsed.inverted_timestamp >= inverted_now
                    && !parsed.job_id.is_empty()
                {
                    out.push((parsed.tenant, parsed.job_id));
                }
            }
        }

        Ok(out)
    }

    /// Scan future-scheduled jobs (Scheduled with start_time > now) for a tenant.
    /// Uses range scan from Scheduled prefix start to inverted(now_ms).
    pub async fn scan_jobs_future_scheduled(
        &self,
        tenant: &str,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        // Future jobs have start_time > now, which means inverted_timestamp < u64::MAX - now
        // These sort BEFORE waiting jobs in the index, so scan from prefix start to the boundary.
        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_prefix(tenant, "Scheduled");
        let boundary = idx_status_time_prefix_with_time(tenant, "Scheduled", inverted_now);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..boundary).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_status_time_index_key(&kv.key)
                && !parsed.job_id.is_empty()
            {
                out.push(parsed.job_id);
            }
        }

        Ok(out)
    }

    /// Scan future-scheduled jobs across ALL tenants, returning (tenant, job_id) pairs.
    pub async fn scan_all_jobs_future_scheduled(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let inverted_now = u64::MAX - (now_ms.max(0) as u64);
        let start = idx_status_time_all_prefix();
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_status_time_index_key(&kv.key) {
                // Filter: must be Scheduled status AND inverted_timestamp < inverted_now (start_time > now)
                if parsed.status == "Scheduled"
                    && parsed.inverted_timestamp < inverted_now
                    && !parsed.job_id.is_empty()
                {
                    out.push((parsed.tenant, parsed.job_id));
                }
            }
        }

        Ok(out)
    }

    /// Scan jobs by metadata key/value. Order is not specified.
    pub async fn scan_jobs_by_metadata(
        &self,
        tenant: &str,
        key: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start = idx_metadata_prefix(tenant, key, value);
        let end = end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut out = Vec::with_capacity(limit);

        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            if let Some(parsed) = parse_metadata_index_key(&kv.key)
                && !parsed.job_id.is_empty()
            {
                out.push(parsed.job_id);
            }
        }

        Ok(out)
    }
}
