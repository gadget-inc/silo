//! Scanning and query operations for jobs.

use slatedb::DbIterator;

use crate::job::JobStatusKind;
use crate::job_store_shard::{JobStoreShardError, JobStoreShard};
use crate::keys::idx_metadata_prefix;

impl JobStoreShard {
    /// Generic helper for scanning keys with a given prefix and extracting items.
    pub(crate) async fn scan_prefix<T, F>(
        &self,
        prefix: &str,
        limit: usize,
        extract: F,
    ) -> Result<Vec<T>, JobStoreShardError>
    where
        F: Fn(&str) -> Option<T>,
    {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start = prefix.as_bytes().to_vec();
        let mut end = start.clone();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;
        let mut out = Vec::with_capacity(limit);
        while out.len() < limit {
            let Some(kv) = iter.next().await? else {
                break;
            };
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(item) = extract(&key_str) {
                out.push(item);
            }
        }
        Ok(out)
    }

    /// Scan all jobs for a tenant ordered by job id (lexicographic), unfiltered.
    pub async fn scan_jobs(
        &self,
        tenant: &str,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let prefix = crate::keys::job_info_key(tenant, "");
        self.scan_prefix(&prefix, limit, |key| {
            // key format: jobs/<tenant>/<job-id>
            let job_id = key.rsplit('/').next()?;
            (!job_id.is_empty()).then(|| job_id.to_string())
        })
        .await
    }

    /// Scan all jobs across ALL tenants, returning (tenant, job_id) pairs.
    /// Used for admin queries that need cluster-wide visibility.
    pub async fn scan_all_jobs(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        self.scan_prefix("jobs/", limit, |key| {
            // key format: jobs/<tenant>/<job-id>
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 3 && parts[0] == "jobs" && !parts[2].is_empty() {
                let tenant = crate::keys::decode_tenant(parts[1]);
                Some((tenant, parts[2].to_string()))
            } else {
                None
            }
        })
        .await
    }

    /// Scan newest-first job IDs by status using the time-ordered index.
    pub async fn scan_jobs_by_status(
        &self,
        tenant: &str,
        status: JobStatusKind,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let tenant_enc = crate::keys::job_info_key(tenant, "")
            .trim_start_matches("jobs/")
            .trim_end_matches('/')
            .to_string();
        let prefix = format!("idx/status_ts/{}/{}/", tenant_enc, status.as_str());
        self.scan_prefix(&prefix, limit, |key| {
            // key format: idx/status_ts/<tenant>/<status>/<inv_ts>/<job-id>
            let job_id = key.rsplit('/').next()?;
            (!job_id.is_empty()).then(|| job_id.to_string())
        })
        .await
    }

    /// Scan newest-first job IDs by status across ALL tenants, returning (tenant, job_id) pairs.
    /// Used for admin queries that need cluster-wide visibility.
    pub async fn scan_all_jobs_by_status(
        &self,
        status: JobStatusKind,
        limit: usize,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let status_str = status.as_str();
        self.scan_prefix("idx/status_ts/", limit, |key| {
            // key format: idx/status_ts/<tenant>/<status>/<inv_ts>/<job-id>
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 5
                && parts[0] == "idx"
                && parts[1] == "status_ts"
                && parts[3] == status_str
            {
                let job_id = parts.last()?;
                if !job_id.is_empty() {
                    let tenant = crate::keys::decode_tenant(parts[2]);
                    return Some((tenant, job_id.to_string()));
                }
            }
            None
        })
        .await
    }

    /// Scan jobs by metadata key/value. Order is not specified.
    pub async fn scan_jobs_by_metadata(
        &self,
        tenant: &str,
        key: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        let prefix = idx_metadata_prefix(tenant, key, value);
        self.scan_prefix(&prefix, limit, |k| {
            // key format: idx/meta/<tenant>/<key>/<value>/<job-id>
            let job_id = k.rsplit('/').next()?;
            (!job_id.is_empty()).then(|| job_id.to_string())
        })
        .await
    }
}
