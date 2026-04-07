//! SlateDB compaction filter that drops completed jobs older than a retention window.
//!
//! During compaction, every job-status row is inspected. If a status decodes to a
//! terminal state (Succeeded, Failed, or Cancelled) and its `changed_at_ms` is older
//! than the configured retention cutoff, the row is replaced with a tombstone so that
//! older versions in lower sorted runs are also shadowed and eventually reclaimed.
//!
//! Only `JOB_STATUS` rows are considered: that prefix is the canonical "is this job
//! completed?" record, and the value contains the timestamp directly so the decision
//! can be made from the row alone (no out-of-band lookups, which the compaction filter
//! API does not allow).

use async_trait::async_trait;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};
use std::time::Duration;
use tracing::{debug, warn};

use crate::codec::decode_job_status_owned;
use crate::job_store_shard::helpers::now_epoch_ms;
use crate::keys::prefix;

/// Retain completed jobs for 7 days before allowing the compaction filter to drop them.
pub const COMPLETED_JOB_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// A compaction filter that drops completed (terminal) job-status rows older than
/// `retention`.
pub struct CompletedJobCompactionFilter {
    /// Wall-clock cutoff (epoch ms). Rows with `changed_at_ms < cutoff_ms` and a
    /// terminal status are dropped.
    cutoff_ms: i64,
    /// Number of rows tombstoned in this compaction job (for diagnostics).
    dropped: u64,
}

impl CompletedJobCompactionFilter {
    fn new(cutoff_ms: i64) -> Self {
        Self {
            cutoff_ms,
            dropped: 0,
        }
    }
}

#[async_trait]
impl CompactionFilter for CompletedJobCompactionFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // Only consider JOB_STATUS rows. Every other prefix (job info, indexes,
        // attempts, counters, ...) is left untouched.
        if entry.key.first() != Some(&prefix::JOB_STATUS) {
            return Ok(CompactionFilterDecision::Keep);
        }

        // Tombstones and merge operands carry no decodable JobStatus payload; leave
        // them alone so the compactor can resolve them normally.
        let value_bytes = match &entry.value {
            ValueDeletable::Value(bytes) => bytes,
            ValueDeletable::Merge(_) | ValueDeletable::Tombstone => {
                return Ok(CompactionFilterDecision::Keep);
            }
        };

        let status = match decode_job_status_owned(value_bytes) {
            Ok(status) => status,
            Err(e) => {
                // A row we cannot decode is not safe to drop; log and keep it.
                warn!(
                    error = %e,
                    "compaction filter: failed to decode job status, keeping row"
                );
                return Ok(CompactionFilterDecision::Keep);
            }
        };

        if status.is_terminal() && status.changed_at_ms < self.cutoff_ms {
            self.dropped += 1;
            // Use Tombstone (rather than Drop) so older non-terminal versions of this
            // key in lower sorted runs are shadowed and cannot resurrect.
            return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
        }

        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        if self.dropped > 0 {
            debug!(
                dropped = self.dropped,
                cutoff_ms = self.cutoff_ms,
                "compaction filter: tombstoned completed job-status rows"
            );
        }
        Ok(())
    }
}

/// Supplier that creates a fresh [`CompletedJobCompactionFilter`] per compaction job.
pub struct CompletedJobCompactionFilterSupplier {
    retention: Duration,
}

impl CompletedJobCompactionFilterSupplier {
    /// Create a supplier that drops completed jobs older than `retention`.
    pub fn new(retention: Duration) -> Self {
        Self { retention }
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
        Ok(Box::new(CompletedJobCompactionFilter::new(cutoff_ms)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_job_status;
    use crate::job::{JobStatus, JobStatusKind};
    use crate::keys::job_status_key;
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

    #[tokio::test]
    async fn drops_old_completed_status() {
        let now = now_epoch_ms();
        let cutoff = now - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff);

        let old_succeeded = status_row(JobStatusKind::Succeeded, cutoff - 1);
        let decision = filter.filter(&old_succeeded).await.unwrap();
        assert_eq!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.dropped, 1);
    }

    #[tokio::test]
    async fn keeps_recent_completed_status() {
        let now = now_epoch_ms();
        let cutoff = now - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff);

        let recent = status_row(JobStatusKind::Failed, cutoff + 1);
        let decision = filter.filter(&recent).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.dropped, 0);
    }

    #[tokio::test]
    async fn keeps_old_running_status() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff);

        let old_running = status_row(JobStatusKind::Running, cutoff - 10);
        let decision = filter.filter(&old_running).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn ignores_non_status_prefixes() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = CompletedJobCompactionFilter::new(cutoff);

        let job_info = row(
            crate::keys::job_info_key("tenant-1", "job-1"),
            Bytes::from_static(b"opaque"),
        );
        let decision = filter.filter(&job_info).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }
}
