//! Shard-level job counters for high-performance job counting.
//!
//! These counters track:
//! - `total_jobs`: Count of all jobs that exist in the shard (not deleted)
//! - `completed_jobs`: Count of jobs that have reached a terminal state
//!   (Succeeded, Failed, or Cancelled)
//!
//! Counters use SlateDB's MergeOperator to avoid read-modify-write cycles. Instead of reading
//! the current value, modifying it, and writing it back, we write a delta (+1 or -1) that gets
//! merged at read time.
//!
//! Counter updates go through the `WriteBatcher::merge` method, which handles both batch and
//! transaction paths. For transactions, `TxnWriter::merge` calls `unmark_write` to exclude
//! counter keys from conflict detection, since these global shard-level keys would otherwise
//! cause excessive transaction conflicts under concurrent load.

use std::collections::HashMap;
use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError};

use crate::codec::decode_job_status_owned;
use crate::job_store_shard::helpers::WriteBatcher;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    concurrency_requester_counter_key, concurrency_requester_counter_tenant_prefix, end_bound,
    parse_concurrency_requester_counter_key, parse_job_info_key, parse_job_status_key, prefix,
    shard_completed_jobs_counter_key, shard_total_jobs_counter_key, tenant_status_counter_key,
};
use crate::shard_range::ShardRange;

/// Shard job counters returned by get_counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardCounters {
    /// Total number of jobs in the shard (not deleted).
    pub total_jobs: i64,
    /// Number of jobs in terminal states (Succeeded, Failed, Cancelled).
    pub completed_jobs: i64,
}

/// Optional lexicographic tenant bounds for scanning tenant status counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantStatusCounterScanRange {
    pub start_tenant: Option<String>,
    pub start_inclusive: bool,
    pub end_tenant: Option<String>,
    pub end_inclusive: bool,
}

impl TenantStatusCounterScanRange {
    pub fn exact(tenant: impl Into<String>) -> Self {
        let tenant = tenant.into();
        Self {
            start_tenant: Some(tenant.clone()),
            start_inclusive: true,
            end_tenant: Some(tenant),
            end_inclusive: true,
        }
    }

    fn scan_bounds(&self) -> (Vec<u8>, Vec<u8>) {
        let start = match &self.start_tenant {
            Some(tenant) if self.start_inclusive => {
                crate::keys::tenant_status_counter_tenant_prefix(tenant)
            }
            Some(tenant) => {
                let prefix = crate::keys::tenant_status_counter_tenant_prefix(tenant);
                crate::keys::end_bound(&prefix)
            }
            None => crate::keys::tenant_status_counter_prefix(),
        };

        let end = match &self.end_tenant {
            Some(tenant) if self.end_inclusive => {
                let prefix = crate::keys::tenant_status_counter_tenant_prefix(tenant);
                crate::keys::end_bound(&prefix)
            }
            Some(tenant) => crate::keys::tenant_status_counter_tenant_prefix(tenant),
            None => {
                let prefix = crate::keys::tenant_status_counter_prefix();
                crate::keys::end_bound(&prefix)
            }
        };

        (start, end)
    }
}

impl ShardCounters {
    /// Calculate the number of open (non-terminal) jobs.
    pub fn open_jobs(&self) -> i64 {
        self.total_jobs.saturating_sub(self.completed_jobs)
    }
}

/// MergeOperator for counter keys that sums i64 deltas.
///
/// This operator treats both the existing value and operands as little-endian i64s
/// and sums them together. This allows counters to be incremented/decremented
/// without reading the current value first.
#[derive(Debug)]
pub struct CounterMergeOperator;

impl MergeOperator for CounterMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let existing = existing_value.map(|v| decode_counter(&v)).unwrap_or(0);
        let delta = decode_counter(&operand);
        let new_value = existing.saturating_add(delta);
        Ok(Bytes::copy_from_slice(&encode_counter(new_value)))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut total = existing_value.map(|v| decode_counter(&v)).unwrap_or(0);

        for operand in operands {
            let delta = decode_counter(operand);
            total = total.saturating_add(delta);
        }

        Ok(Bytes::copy_from_slice(&encode_counter(total)))
    }
}

/// Create an Arc'd CounterMergeOperator for use with DbBuilder.
pub fn counter_merge_operator() -> Arc<CounterMergeOperator> {
    Arc::new(CounterMergeOperator)
}

/// Helper to encode an i64 counter value as bytes.
pub(crate) fn encode_counter(value: i64) -> [u8; 8] {
    value.to_le_bytes()
}

/// Helper to decode an i64 counter value from bytes.
pub(crate) fn decode_counter(bytes: &[u8]) -> i64 {
    if bytes.len() >= 8 {
        let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0; 8]);
        i64::from_le_bytes(arr)
    } else {
        0
    }
}

impl JobStoreShard {
    /// Get the current job counters for this shard.
    pub async fn get_counters(&self) -> Result<ShardCounters, JobStoreShardError> {
        let total_key = shard_total_jobs_counter_key();
        let completed_key = shard_completed_jobs_counter_key();

        let total_jobs = match self.db.get(&total_key).await? {
            Some(bytes) => decode_counter(&bytes),
            None => 0,
        };

        let completed_jobs = match self.db.get(&completed_key).await? {
            Some(bytes) => decode_counter(&bytes),
            None => 0,
        };

        Ok(ShardCounters {
            total_jobs,
            completed_jobs,
        })
    }

    /// Increment the total jobs counter.
    pub(crate) fn increment_total_jobs_counter(
        &self,
        writer: &mut impl WriteBatcher,
    ) -> Result<(), JobStoreShardError> {
        let key = shard_total_jobs_counter_key();
        writer.merge(&key, encode_counter(1))?;
        Ok(())
    }

    /// Decrement the total jobs counter.
    pub(crate) fn decrement_total_jobs_counter(
        &self,
        writer: &mut impl WriteBatcher,
    ) -> Result<(), JobStoreShardError> {
        let key = shard_total_jobs_counter_key();
        writer.merge(&key, encode_counter(-1))?;
        Ok(())
    }

    /// Increment the completed jobs counter.
    pub(crate) fn increment_completed_jobs_counter(
        &self,
        writer: &mut impl WriteBatcher,
    ) -> Result<(), JobStoreShardError> {
        let key = shard_completed_jobs_counter_key();
        writer.merge(&key, encode_counter(1))?;
        Ok(())
    }

    /// Decrement the completed jobs counter.
    pub(crate) fn decrement_completed_jobs_counter(
        &self,
        writer: &mut impl WriteBatcher,
    ) -> Result<(), JobStoreShardError> {
        let key = shard_completed_jobs_counter_key();
        writer.merge(&key, encode_counter(-1))?;
        Ok(())
    }

    /// Scan all tenant status counters for this shard.
    ///
    /// Returns a vec of (tenant, status_kind, count) tuples, optionally limited
    /// to a lexicographic tenant range, filtered to only include tenants that
    /// belong to this shard's range. Entries with count <= 0 are excluded.
    pub async fn scan_tenant_status_counters(
        &self,
        tenant_range: Option<TenantStatusCounterScanRange>,
    ) -> Result<Vec<(String, String, i64)>, JobStoreShardError> {
        use crate::keys::{
            end_bound, parse_tenant_status_counter_key, tenant_status_counter_prefix,
        };
        use slatedb::DbIterator;

        let (start, end) = tenant_range
            .as_ref()
            .map(TenantStatusCounterScanRange::scan_bounds)
            .unwrap_or_else(|| {
                let prefix = tenant_status_counter_prefix();
                let end = end_bound(&prefix);
                (prefix, end)
            });

        if start >= end {
            return Ok(Vec::new());
        }

        let range = self.get_range();

        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut results = Vec::new();

        while let Some(kv) = iter.next().await? {
            if let Some(parsed) = parse_tenant_status_counter_key(&kv.key) {
                if !range.contains_tenant(&parsed.tenant) {
                    continue;
                }
                let count = decode_counter(&kv.value);
                if count > 0 {
                    results.push((parsed.tenant, parsed.status_kind, count));
                }
            }
        }

        Ok(results)
    }

    /// Get the current concurrency requester count for a specific tenant/queue.
    pub async fn get_concurrency_requester_count(
        &self,
        tenant: &str,
        queue: &str,
    ) -> Result<i64, JobStoreShardError> {
        let key = concurrency_requester_counter_key(tenant, queue);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(decode_counter(&bytes)),
            None => Ok(0),
        }
    }

    /// Scan all concurrency requester counters for a specific tenant.
    /// Returns a list of (queue_name, count) pairs.
    pub async fn scan_concurrency_requester_counters(
        &self,
        tenant: &str,
    ) -> Result<Vec<(String, i64)>, JobStoreShardError> {
        let prefix = concurrency_requester_counter_tenant_prefix(tenant);
        let end = end_bound(&prefix);
        let mut iter = self.db.scan::<Vec<u8>, _>(prefix..end).await?;
        let mut results = Vec::new();
        loop {
            match iter.next().await {
                Ok(Some(kv)) => {
                    if let Some(parsed) = parse_concurrency_requester_counter_key(&kv.key) {
                        let count = decode_counter(&kv.value);
                        if count > 0 {
                            results.push((parsed.queue, count));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(JobStoreShardError::Slate(e)),
            }
        }
        Ok(results)
    }

    /// Scan all concurrency requester counters across all tenants.
    /// Returns a list of (tenant, queue_name, count) tuples.
    pub async fn scan_all_concurrency_requester_counters(
        &self,
    ) -> Result<Vec<(String, String, i64)>, JobStoreShardError> {
        let prefix = vec![crate::keys::prefix::COUNTER_CONCURRENCY_REQUESTERS];
        let end = end_bound(&prefix);
        let mut iter = self.db.scan::<Vec<u8>, _>(prefix..end).await?;
        let mut results = Vec::new();
        loop {
            match iter.next().await {
                Ok(Some(kv)) => {
                    if let Some(parsed) = parse_concurrency_requester_counter_key(&kv.key) {
                        let count = decode_counter(&kv.value);
                        if count > 0 {
                            results.push((parsed.tenant, parsed.queue, count));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(JobStoreShardError::Slate(e)),
            }
        }
        Ok(results)
    }

    /// Re-derive shard-level and per-tenant counter truth by scanning the
    /// `JOB_INFO` and `JOB_STATUS` source-of-truth keys, then write a single
    /// signed merge operand per counter to correct any drift.
    ///
    /// Drift is expected because the standalone `silo-compactor` drops terminal
    /// job rows during compaction without a writable `Db` handle and therefore
    /// cannot decrement counters at drop time. This method is the sole owner of
    /// counter truth in steady state.
    ///
    /// The pass is best-effort: if any individual scan or merge fails, a
    /// `warn!` is logged and the rest of the pass continues. Concurrent writes
    /// during the scan can leave the corrected value slightly off; the next
    /// pass corrects that drift.
    pub async fn reconcile_counters(&self, range: &ShardRange) -> ReconcileSummary {
        let mut summary = ReconcileSummary::default();

        // Single shared walk of the JOB_STATUS prefix feeds both the
        // shard-level `completed_jobs` correction and every per-tenant
        // `tenant_status_counter` correction.
        let truth = match self.scan_job_status_truth(range).await {
            Ok(t) => {
                summary.scanned_jobs = t.scanned_jobs;
                Some(t)
            }
            Err(e) => {
                summary.failed += 1;
                tracing::warn!(
                    shard = %self.name,
                    error = %e,
                    "counter reconcile: JOB_STATUS scan failed; \
                     completed_jobs and tenant_status counters will remain stale until next pass"
                );
                None
            }
        };

        match self.reconcile_shard_counters(range, truth.as_ref()).await {
            Ok(corrected) => summary.corrected += corrected,
            Err(e) => {
                summary.failed += 1;
                tracing::warn!(
                    shard = %self.name,
                    error = %e,
                    "counter reconcile failed (shard counters); shard counters will remain stale until next pass"
                );
            }
        }

        if let Some(t) = truth.as_ref() {
            match self.reconcile_tenant_status_counters(range, t).await {
                Ok(corrected) => summary.corrected += corrected,
                Err(e) => {
                    summary.failed += 1;
                    tracing::warn!(
                        shard = %self.name,
                        error = %e,
                        "counter reconcile failed (tenant status counters); per-tenant counters will remain stale until next pass"
                    );
                }
            }
        }

        summary
    }

    /// Walk the `JOB_STATUS` prefix once and aggregate everything either
    /// downstream reconciler needs into a single [`JobStatusTruth`].
    ///
    /// Range-filtered. Decode failures are logged at warn (not silently
    /// skipped) and the row is dropped from both aggregations.
    pub async fn scan_job_status_truth(
        &self,
        range: &ShardRange,
    ) -> Result<JobStatusTruth, JobStoreShardError> {
        let mut truth = JobStatusTruth::default();
        let status_prefix = vec![prefix::JOB_STATUS];
        let status_end = end_bound(&status_prefix);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(status_prefix..status_end, &crate::scan_options())
            .await?;
        while let Some(kv) = iter.next().await? {
            let Some(parsed) = parse_job_status_key(&kv.key) else {
                continue;
            };
            if !range.contains_tenant(&parsed.tenant) {
                continue;
            }
            truth.scanned_jobs += 1;
            match decode_job_status_owned(&kv.value) {
                Ok(status) => {
                    if status.is_terminal() {
                        truth.completed_jobs += 1;
                    }
                    *truth
                        .per_tenant_status
                        .entry((parsed.tenant, status.kind.as_str().to_owned()))
                        .or_insert(0) += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        shard = %self.name,
                        tenant = %parsed.tenant,
                        job_id = %parsed.job_id,
                        error = %e,
                        "counter reconcile: failed to decode job status, skipping row"
                    );
                }
            }
        }
        Ok(truth)
    }

    /// Reconcile the two shard-level counters (`total_jobs`, `completed_jobs`).
    ///
    /// `total_jobs` is derived from a fresh `JOB_INFO` scan (canonical source
    /// of truth, different prefix). `completed_jobs` is read from the
    /// precomputed `truth`. When `truth` is `None` (the upstream JOB_STATUS
    /// scan failed) we still correct `total_jobs` and skip `completed_jobs`.
    async fn reconcile_shard_counters(
        &self,
        range: &ShardRange,
        truth: Option<&JobStatusTruth>,
    ) -> Result<u64, JobStoreShardError> {
        let mut total_jobs_truth: i64 = 0;
        let info_prefix = vec![prefix::JOB_INFO];
        let info_end = end_bound(&info_prefix);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(info_prefix..info_end, &crate::scan_options())
            .await?;
        while let Some(kv) = iter.next().await? {
            if let Some(parsed) = parse_job_info_key(&kv.key)
                && range.contains_tenant(&parsed.tenant)
            {
                total_jobs_truth += 1;
            }
        }

        let mut corrected: u64 = 0;
        corrected += self
            .apply_counter_correction(shard_total_jobs_counter_key(), total_jobs_truth)
            .await?;
        if let Some(t) = truth {
            corrected += self
                .apply_counter_correction(shard_completed_jobs_counter_key(), t.completed_jobs)
                .await?;
        }

        Ok(corrected)
    }

    /// Reconcile per-tenant per-status counters using a precomputed truth
    /// from [`scan_job_status_truth`]. Drives stale stored counters (including
    /// zombies that should be 0) to match `truth.per_tenant_status`.
    /// Returns the number of counters that received a non-zero correction.
    async fn reconcile_tenant_status_counters(
        &self,
        _range: &ShardRange,
        truth: &JobStatusTruth,
    ) -> Result<u64, JobStoreShardError> {
        // Enumerate existing stored counters in this shard's range so we can
        // explicitly drive zombies (counter > 0 but no matching JOB_STATUS) to
        // zero. `scan_tenant_status_counters` already filters by shard range
        // and only returns count > 0 rows — exactly the set we might need to
        // zero out.
        let existing = self.scan_tenant_status_counters(None).await?;

        let mut corrected: u64 = 0;
        let mut truth_remaining = truth.per_tenant_status.clone();

        for (tenant, kind, _current) in existing {
            let want = truth_remaining
                .remove(&(tenant.clone(), kind.clone()))
                .unwrap_or(0);
            let key = tenant_status_counter_key(&tenant, &kind);
            corrected += self.apply_counter_correction(key, want).await?;
        }

        // Anything remaining in `truth_remaining` has no existing stored
        // counter (or its existing value was filtered out as <= 0). Issue a
        // fresh delta from 0.
        for ((tenant, kind), want) in truth_remaining {
            if want != 0 {
                let key = tenant_status_counter_key(&tenant, &kind);
                corrected += self.apply_counter_correction(key, want).await?;
            }
        }

        Ok(corrected)
    }

    /// Read `key`, compute `delta = truth - current`, and merge the delta if
    /// non-zero. Returns 1 if a merge was issued, 0 otherwise.
    async fn apply_counter_correction(
        &self,
        key: Vec<u8>,
        truth: i64,
    ) -> Result<u64, JobStoreShardError> {
        let current = match self.db.get(&key).await? {
            Some(b) => decode_counter(&b),
            None => 0,
        };
        let delta = truth.saturating_sub(current);
        if delta == 0 {
            return Ok(0);
        }
        self.db.merge(&key, &encode_counter(delta)).await?;
        Ok(1)
    }
}

/// Aggregated truth from one walk of the `JOB_STATUS` prefix in a shard's
/// range. Built once per [`JobStoreShard::reconcile_counters`] pass and
/// consumed by both the shard-level and per-tenant reconcilers so we don't
/// scan `JOB_STATUS` twice.
///
/// Memory bound: `per_tenant_status` holds at most `N_tenants × N_status_kinds`
/// entries (status_kind is a small fixed-size enum). For a shard with 100k
/// tenants this is ~500k entries × ~100 bytes ≈ 50 MB — bounded by tenant
/// count, not by job count. Unchanged from the pre-fusion code, which already
/// built the same map inside `reconcile_tenant_status_counters`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct JobStatusTruth {
    /// Total terminal rows seen — feeds shard `completed_jobs`.
    pub completed_jobs: i64,
    /// Per-(tenant, status_kind) row counts — feeds `tenant_status_counter`.
    /// See struct-level note for the bound.
    pub per_tenant_status: HashMap<(String, String), i64>,
    /// Total in-range rows examined (terminal and non-terminal).
    /// Drives the [`ReconcileSummary::scanned_jobs`] field.
    pub scanned_jobs: u64,
}

/// Result of a single `reconcile_counters` pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileSummary {
    /// Number of `JOB_STATUS` rows scanned.
    pub scanned_jobs: u64,
    /// Number of counters that received a non-zero correction merge.
    pub corrected: u64,
    /// Number of sub-tasks that failed (each has been logged at warn).
    pub failed: u64,
}
