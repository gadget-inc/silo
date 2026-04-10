//! SlateDB compaction filter that drops completed jobs older than a retention window.
//!
//! During compaction, the filter inspects rows across multiple key prefixes:
//!
//! - **JOB_INFO (0x01):** The key encodes `(tenant, job_id)` but carries no
//!   status information. Since JOB_INFO sorts *before* JOB_STATUS (0x02), the
//!   filter can't use its in-memory set yet. Instead, it does an async point
//!   lookup (`db.get`) against the live DB to read the job's current
//!   `JOB_STATUS` row. If terminal + old → tombstone.
//!
//! - **JOB_STATUS (0x02):** If a status decodes to terminal (Succeeded, Failed,
//!   or Cancelled) and its `changed_at_ms` is older than the configured retention
//!   cutoff, the row is tombstoned and the `job_id` is recorded in an in-memory
//!   set for use by later prefixes.
//!
//! - **IDX_STATUS_TIME (0x03):** The key itself encodes the status string and
//!   timestamp. If the status is terminal and the timestamp is older than the
//!   cutoff, the row is tombstoned (no value decode or set lookup needed).
//!
//! - **IDX_METADATA (0x04), ATTEMPT (0x07), JOB_CANCELLED (0x0A):** The key
//!   encodes the `job_id`. If the `job_id` is in the purged set (populated from
//!   JOB_STATUS rows earlier in the same compaction pass), the row is tombstoned.
//!   This works because keys are sorted by prefix and 0x02 < 0x04 < 0x07 < 0x0A,
//!   so the set is fully populated before these prefixes are reached.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, Db, RowEntry, ValueDeletable,
};
use std::time::Duration;
use tracing::{debug, warn};

use crate::codec::decode_job_status_owned;
use crate::job_store_shard::helpers::now_epoch_ms;
use crate::keys::{
    parse_attempt_key, parse_job_cancelled_key, parse_job_info_key, parse_job_status_key,
    parse_metadata_index_key, parse_status_time_index_key, prefix,
};

/// Retain completed jobs for 7 days before allowing the compaction filter to drop them.
pub const COMPLETED_JOB_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Terminal status strings as stored in IDX_STATUS_TIME keys.
const TERMINAL_STATUSES: &[&str] = &["Succeeded", "Failed", "Cancelled"];

/// A compaction filter that drops completed (terminal) job data older than
/// `retention` across multiple key prefixes.
pub struct CompletedJobCompactionFilter {
    /// Wall-clock cutoff (epoch ms). Rows with `changed_at_ms < cutoff_ms` and a
    /// terminal status are dropped.
    cutoff_ms: i64,
    /// Live DB handle for point lookups (used by JOB_INFO rows that sort
    /// before JOB_STATUS). `None` if the DB handle wasn't available at
    /// filter creation time (e.g., in unit tests); JOB_INFO rows are
    /// kept in that case.
    db: Option<Arc<Db>>,
    /// Set of `job_id`s whose JOB_STATUS rows were tombstoned in this
    /// compaction pass. Used to decide on IDX_METADATA, ATTEMPT, and
    /// JOB_CANCELLED rows (which don't carry status information in their
    /// values, only in their keys via job_id).
    purged_job_ids: HashSet<String>,
    /// Per-prefix tombstone counts for diagnostics.
    dropped_job_info: u64,
    dropped_status: u64,
    dropped_idx_status_time: u64,
    dropped_idx_metadata: u64,
    dropped_attempt: u64,
    dropped_job_cancelled: u64,
}

impl CompletedJobCompactionFilter {
    fn new(cutoff_ms: i64, db: Option<Arc<Db>>) -> Self {
        Self {
            cutoff_ms,
            db,
            purged_job_ids: HashSet::new(),
            dropped_job_info: 0,
            dropped_status: 0,
            dropped_idx_status_time: 0,
            dropped_idx_metadata: 0,
            dropped_attempt: 0,
            dropped_job_cancelled: 0,
        }
    }

    fn total_dropped(&self) -> u64 {
        self.dropped_job_info
            + self.dropped_status
            + self.dropped_idx_status_time
            + self.dropped_idx_metadata
            + self.dropped_attempt
            + self.dropped_job_cancelled
    }

    /// Check whether a job is terminal + old by reading its JOB_STATUS
    /// from the live DB. Returns `true` if the job should be purged.
    async fn is_job_expired(&self, tenant: &str, job_id: &str) -> bool {
        let Some(db) = &self.db else {
            return false;
        };
        let key = crate::keys::job_status_key(tenant, job_id);
        let Ok(Some(raw)) = db.get(key.as_slice()).await else {
            return false;
        };
        let Ok(status) = decode_job_status_owned(&raw) else {
            return false;
        };
        status.is_terminal() && status.changed_at_ms < self.cutoff_ms
    }
}

#[async_trait]
impl CompactionFilter for CompletedJobCompactionFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        let Some(&pfx) = entry.key.first() else {
            return Ok(CompactionFilterDecision::Keep);
        };

        // Tombstones and merge operands carry no decodable payload; leave
        // them alone so the compactor can resolve them normally.
        if matches!(
            entry.value,
            ValueDeletable::Merge(_) | ValueDeletable::Tombstone
        ) {
            return Ok(CompactionFilterDecision::Keep);
        }

        match pfx {
            // ── JOB_INFO (0x01): point-lookup JOB_STATUS via live DB ──
            prefix::JOB_INFO => {
                if let Some(parsed) = parse_job_info_key(&entry.key) {
                    if self.is_job_expired(&parsed.tenant, &parsed.job_id).await {
                        self.dropped_job_info += 1;
                        return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
                    }
                }
            }

            // ── JOB_STATUS (0x02): decode value, check terminal + old ──
            prefix::JOB_STATUS => {
                let value_bytes = match &entry.value {
                    ValueDeletable::Value(bytes) => bytes,
                    _ => return Ok(CompactionFilterDecision::Keep),
                };

                let status = match decode_job_status_owned(value_bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "compaction filter: failed to decode job status, keeping row");
                        return Ok(CompactionFilterDecision::Keep);
                    }
                };

                if status.is_terminal() && status.changed_at_ms < self.cutoff_ms {
                    if let Some(parsed) = parse_job_status_key(&entry.key) {
                        self.purged_job_ids.insert(parsed.job_id);
                    }
                    self.dropped_status += 1;
                    return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
                }
            }

            // ── IDX_STATUS_TIME (0x03): self-contained, key has status + ts ──
            prefix::IDX_STATUS_TIME => {
                if let Some(parsed) = parse_status_time_index_key(&entry.key) {
                    if TERMINAL_STATUSES.contains(&parsed.status.as_str())
                        && parsed.changed_at_ms() < self.cutoff_ms
                    {
                        self.dropped_idx_status_time += 1;
                        return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
                    }
                }
            }

            // ── IDX_METADATA (0x04): check job_id against purged set ──
            prefix::IDX_METADATA => {
                if let Some(parsed) = parse_metadata_index_key(&entry.key) {
                    if self.purged_job_ids.contains(&parsed.job_id) {
                        self.dropped_idx_metadata += 1;
                        return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
                    }
                }
            }

            // ── ATTEMPT (0x07): check job_id against purged set ──
            prefix::ATTEMPT => {
                if let Some(parsed) = parse_attempt_key(&entry.key) {
                    if self.purged_job_ids.contains(&parsed.job_id) {
                        self.dropped_attempt += 1;
                        return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
                    }
                }
            }

            // ── JOB_CANCELLED (0x0A): check job_id against purged set ──
            prefix::JOB_CANCELLED => {
                if let Some(parsed) = parse_job_cancelled_key(&entry.key) {
                    if self.purged_job_ids.contains(&parsed.job_id) {
                        self.dropped_job_cancelled += 1;
                        return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
                    }
                }
            }

            _ => {}
        }

        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        let total = self.total_dropped();
        if total > 0 {
            debug!(
                total,
                job_info = self.dropped_job_info,
                status = self.dropped_status,
                idx_status_time = self.dropped_idx_status_time,
                idx_metadata = self.dropped_idx_metadata,
                attempt = self.dropped_attempt,
                job_cancelled = self.dropped_job_cancelled,
                purged_jobs = self.purged_job_ids.len(),
                cutoff_ms = self.cutoff_ms,
                "compaction filter: tombstoned rows for completed jobs"
            );
        }
        Ok(())
    }
}

/// Supplier that creates a fresh [`CompletedJobCompactionFilter`] per compaction job.
///
/// The supplier optionally holds a lazy `Db` handle (via `OnceLock<Weak<Db>>`)
/// so that the filter can do point lookups for JOB_INFO rows. The handle is
/// set after the DB is opened via [`CompletedJobCompactionFilterSupplier::set_db`].
/// If not set, JOB_INFO rows are kept (graceful degradation).
pub struct CompletedJobCompactionFilterSupplier {
    retention: Duration,
    /// Lazy-init DB handle. Set by `open_with_resolved_store` after
    /// `db_builder.build()` returns. `Weak` avoids preventing DB shutdown.
    db_handle: Arc<OnceLock<Weak<Db>>>,
}

impl CompletedJobCompactionFilterSupplier {
    /// Create a supplier that drops completed jobs older than `retention`.
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            db_handle: Arc::new(OnceLock::new()),
        }
    }

    /// Provide the live DB handle so the filter can do point lookups for
    /// JOB_INFO rows. Call this after `db_builder.build()`.
    pub fn set_db(&self, db: &Arc<Db>) {
        let _ = self.db_handle.set(Arc::downgrade(db));
    }

    /// Get a clone of the `db_handle` Arc so it can be shared with the
    /// caller (who will call `set_db` later).
    pub fn db_handle(&self) -> Arc<OnceLock<Weak<Db>>> {
        Arc::clone(&self.db_handle)
    }
}

impl Default for CompletedJobCompactionFilterSupplier {
    fn default() -> Self {
        Self::new(COMPLETED_JOB_RETENTION)
    }
}

#[async_trait]
impl CompactionFilterSupplier for CompletedJobCompactionFilterSupplier {
    async fn create_compaction_filter(
        &self,
        _context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        let retention_ms = self.retention.as_millis() as i64;
        let cutoff_ms = now_epoch_ms().saturating_sub(retention_ms);

        // Try to upgrade the Weak<Db> to a strong reference for this
        // compaction job's lifetime. If the DB has been closed, the
        // upgrade fails and JOB_INFO rows will be kept.
        let db = self.db_handle.get().and_then(|weak| weak.upgrade());

        Ok(Box::new(CompletedJobCompactionFilter::new(cutoff_ms, db)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_job_status;
    use crate::job::{JobStatus, JobStatusKind};
    use crate::keys::{attempt_key, idx_metadata_key, idx_status_time_key, job_status_key};
    use slatedb::bytes::Bytes;

    fn row(key: Vec<u8>, value: Bytes) -> RowEntry {
        RowEntry {
            key: Bytes::from(key),
            value: ValueDeletable::Value(value),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn status_row(kind: JobStatusKind, changed_at_ms: i64) -> RowEntry {
        let status = JobStatus::new(kind, changed_at_ms, None, None);
        let bytes = encode_job_status(&status);
        row(job_status_key("tenant-1", "job-1"), Bytes::from(bytes))
    }

    fn status_row_with_id(job_id: &str, kind: JobStatusKind, changed_at_ms: i64) -> RowEntry {
        let status = JobStatus::new(kind, changed_at_ms, None, None);
        let bytes = encode_job_status(&status);
        row(job_status_key("tenant-1", job_id), Bytes::from(bytes))
    }

    // Unit tests don't have a Db handle, so JOB_INFO lookups are skipped.
    // The JOB_INFO integration test covers the full path.

    // ── JOB_STATUS tests ──

    #[tokio::test]
    async fn drops_old_completed_status() {
        let now = now_epoch_ms();
        let cutoff = now - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let old_succeeded = status_row(JobStatusKind::Succeeded, cutoff - 1);
        let decision = filter.filter(&old_succeeded).await.unwrap();
        assert_eq!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.dropped_status, 1);
    }

    #[tokio::test]
    async fn keeps_recent_completed_status() {
        let now = now_epoch_ms();
        let cutoff = now - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let recent = status_row(JobStatusKind::Failed, cutoff + 1);
        let decision = filter.filter(&recent).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.dropped_status, 0);
    }

    #[tokio::test]
    async fn keeps_old_running_status() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let old_running = status_row(JobStatusKind::Running, cutoff - 10);
        let decision = filter.filter(&old_running).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn ignores_non_status_prefixes() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        // Without a Db handle, JOB_INFO rows are kept.
        let job_info = row(
            crate::keys::job_info_key("tenant-1", "job-1"),
            Bytes::from_static(b"opaque"),
        );
        let decision = filter.filter(&job_info).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    // ── IDX_STATUS_TIME tests ──

    #[tokio::test]
    async fn drops_old_terminal_idx_status_time() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let old_ts = cutoff - 500;
        let key = idx_status_time_key("tenant-1", "Succeeded", old_ts, "job-1");
        let entry = row(key, Bytes::from_static(b""));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.dropped_idx_status_time, 1);
    }

    #[tokio::test]
    async fn keeps_recent_terminal_idx_status_time() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let recent_ts = cutoff + 500;
        let key = idx_status_time_key("tenant-1", "Failed", recent_ts, "job-1");
        let entry = row(key, Bytes::from_static(b""));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn keeps_non_terminal_idx_status_time() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let old_ts = cutoff - 500;
        let key = idx_status_time_key("tenant-1", "Scheduled", old_ts, "job-1");
        let entry = row(key, Bytes::from_static(b""));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    // ── ATTEMPT tests (set-based) ──

    #[tokio::test]
    async fn drops_attempt_for_purged_job() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let status = status_row_with_id("purged-job", JobStatusKind::Succeeded, cutoff - 100);
        filter.filter(&status).await.unwrap();
        assert!(filter.purged_job_ids.contains("purged-job"));

        let attempt = row(
            attempt_key("tenant-1", "purged-job", 0),
            Bytes::from_static(b"attempt-data"),
        );
        let decision = filter.filter(&attempt).await.unwrap();
        assert_eq!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.dropped_attempt, 1);
    }

    #[tokio::test]
    async fn keeps_attempt_for_non_purged_job() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let attempt = row(
            attempt_key("tenant-1", "some-active-job", 0),
            Bytes::from_static(b"attempt-data"),
        );
        let decision = filter.filter(&attempt).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    // ── IDX_METADATA tests (set-based) ──

    #[tokio::test]
    async fn drops_metadata_for_purged_job() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let status = status_row_with_id("purged-job", JobStatusKind::Failed, cutoff - 100);
        filter.filter(&status).await.unwrap();

        let meta = row(
            idx_metadata_key("tenant-1", "queue", "billing", "purged-job"),
            Bytes::from_static(b""),
        );
        let decision = filter.filter(&meta).await.unwrap();
        assert_eq!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.dropped_idx_metadata, 1);
    }

    #[tokio::test]
    async fn keeps_metadata_for_non_purged_job() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff, None);

        let meta = row(
            idx_metadata_key("tenant-1", "queue", "billing", "active-job"),
            Bytes::from_static(b""),
        );
        let decision = filter.filter(&meta).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }
}
