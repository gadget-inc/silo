//! Compaction filter that drops completed (terminal) job data older than a
//! configurable retention window.
//!
//! During compaction this filter inspects rows across six key prefixes. For
//! each prefix it determines whether the owning job is terminal (Succeeded,
//! Failed, Cancelled) and older than the retention cutoff, then either drops
//! the entry or converts it to a tombstone depending on whether the
//! destination sorted run is the last (oldest) one.
//!
//! ## Prefix decision table
//!
//! | Prefix | Name             | Decision method                      |
//! |--------|------------------|--------------------------------------|
//! | 0x01   | JOB_INFO         | Point-read JOB_STATUS via DbReader   |
//! | 0x02   | JOB_STATUS       | Decode value directly                |
//! | 0x03   | IDX_STATUS_TIME  | Self-contained in key                |
//! | 0x04   | IDX_METADATA     | Point-read JOB_STATUS via DbReader   |
//! | 0x07   | ATTEMPT          | Point-read JOB_STATUS via DbReader   |
//! | 0x0A   | JOB_CANCELLED    | Point-read JOB_STATUS via DbReader   |
//!
//! In the standalone `silo-compactor` process we do not have a writable `Db`
//! handle, so counter decrements are tracked but emitted only via the warn
//! log on `on_compaction_end`. The live silo server owns the counters; those
//! will drift slightly and are expected to be reconciled by silo's own
//! cleanup paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::path::Path;
use silo::codec::decode_job_status_owned;
use silo::job_store_shard::counter_merge_operator;
use silo::keys::{
    parse_attempt_key, parse_job_cancelled_key, parse_job_info_key, parse_job_status_key,
    parse_metadata_index_key, parse_status_time_index_key, prefix,
};
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, DbReader, DbReaderBuilder, RowEntry, ValueDeletable,
};
use tracing::{debug, info, warn};

/// Terminal status strings as stored in IDX_STATUS_TIME keys.
const TERMINAL_STATUSES: &[&str] = &["Succeeded", "Failed", "Cancelled"];

/// Default retention window for completed jobs: 7 days.
pub const DEFAULT_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

// ---------------------------------------------------------------------------
// Supplier
// ---------------------------------------------------------------------------

/// Creates a fresh [`CompletedJobCompactionFilter`] for each compaction job.
///
/// The supplier lazily opens a [`DbReader`] per compaction job for point-
/// reading JOB_STATUS rows. The reader is NOT kept alive across jobs — this
/// avoids a long-lived manifest poller / checkpoint that can interfere with
/// the compactor's state management.
pub struct CompletedJobCompactionFilterSupplier {
    retention: Duration,
    /// Object-store context for lazily opening a `DbReader` per compaction
    /// job. `None` disables point reads (unit tests only).
    reader_ctx: Option<ReaderContext>,
}

/// Everything needed to open a short-lived [`DbReader`].
struct ReaderContext {
    db_path: Path,
    store: Arc<dyn object_store::ObjectStore>,
}

impl CompletedJobCompactionFilterSupplier {
    pub fn new(
        retention: Duration,
        db_path: Path,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        Self {
            retention,
            reader_ctx: Some(ReaderContext { db_path, store }),
        }
    }

    #[cfg(test)]
    fn new_without_reader(retention: Duration) -> Self {
        Self {
            retention,
            reader_ctx: None,
        }
    }
}

#[async_trait]
impl CompactionFilterSupplier for CompletedJobCompactionFilterSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        let retention_ms = self.retention.as_millis() as i64;
        let cutoff_ms = now_epoch_ms().saturating_sub(retention_ms);

        // Open a short-lived DbReader for point reads during this compaction
        // job. The reader is dropped when the filter is dropped (after
        // on_compaction_end), which keeps its checkpoint / manifest poller
        // from interfering with the compactor across jobs.
        let reader = if let Some(ctx) = &self.reader_ctx {
            match DbReaderBuilder::new(ctx.db_path.clone(), Arc::clone(&ctx.store))
                .with_merge_operator(counter_merge_operator())
                .build()
                .await
            {
                Ok(r) => {
                    info!("completed_jobs filter: opened DbReader for point reads");
                    Some(Arc::new(r))
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "completed_jobs filter: failed to open DbReader, \
                         JOB_INFO/IDX_METADATA/ATTEMPT/JOB_CANCELLED rows will be kept"
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(Box::new(CompletedJobCompactionFilter {
            cutoff_ms,
            reader,
            is_dest_last_run: context.is_dest_last_run,
            dropped_job_info: 0,
            dropped_status: 0,
            dropped_idx_status_time: 0,
            dropped_idx_metadata: 0,
            dropped_attempt: 0,
            dropped_job_cancelled: 0,
            tenant_status_drops: HashMap::new(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

pub struct CompletedJobCompactionFilter {
    cutoff_ms: i64,
    reader: Option<Arc<DbReader>>,
    is_dest_last_run: bool,

    // Per-prefix drop counts (diagnostics).
    dropped_job_info: u64,
    dropped_status: u64,
    dropped_idx_status_time: u64,
    dropped_idx_metadata: u64,
    dropped_attempt: u64,
    dropped_job_cancelled: u64,

    // (tenant, status_kind) → count for counter decrements.
    tenant_status_drops: HashMap<(String, String), u64>,
}

impl CompletedJobCompactionFilter {
    fn total_dropped(&self) -> u64 {
        self.dropped_job_info
            + self.dropped_status
            + self.dropped_idx_status_time
            + self.dropped_idx_metadata
            + self.dropped_attempt
            + self.dropped_job_cancelled
    }

    /// Returns [`Drop`] when the destination is the oldest sorted run (no
    /// resurrection risk), [`Modify(Tombstone)`] otherwise.
    fn drop_decision(&self) -> CompactionFilterDecision {
        if self.is_dest_last_run {
            CompactionFilterDecision::Drop
        } else {
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        }
    }

    /// Point-read the job's `JOB_STATUS` from the live DB and check whether
    /// it is terminal and older than the retention cutoff.
    async fn is_job_expired(&self, tenant: &str, job_id: &str) -> bool {
        let Some(reader) = &self.reader else {
            return false;
        };
        let key = silo::keys::job_status_key(tenant, job_id);
        let Ok(Some(raw)) = reader.get(key.as_slice()).await else {
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
            // ── JOB_INFO (0x01): point-read JOB_STATUS via DbReader ──
            prefix::JOB_INFO => {
                if let Some(parsed) = parse_job_info_key(&entry.key)
                    && self.is_job_expired(&parsed.tenant, &parsed.job_id).await
                {
                    self.dropped_job_info += 1;
                    return Ok(self.drop_decision());
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
                        *self
                            .tenant_status_drops
                            .entry((parsed.tenant, status.kind.as_str().to_owned()))
                            .or_insert(0) += 1;
                    }
                    self.dropped_status += 1;
                    return Ok(self.drop_decision());
                }
            }

            // ── IDX_STATUS_TIME (0x03): self-contained, key has status + ts ──
            prefix::IDX_STATUS_TIME => {
                if let Some(parsed) = parse_status_time_index_key(&entry.key)
                    && TERMINAL_STATUSES.contains(&parsed.status.as_str())
                    && parsed.changed_at_ms() < self.cutoff_ms
                {
                    self.dropped_idx_status_time += 1;
                    return Ok(self.drop_decision());
                }
            }

            // ── IDX_METADATA (0x04): point-read JOB_STATUS ──
            prefix::IDX_METADATA => {
                if let Some(parsed) = parse_metadata_index_key(&entry.key)
                    && self.is_job_expired(&parsed.tenant, &parsed.job_id).await
                {
                    self.dropped_idx_metadata += 1;
                    return Ok(self.drop_decision());
                }
            }

            // ── ATTEMPT (0x07): point-read JOB_STATUS ──
            prefix::ATTEMPT => {
                if let Some(parsed) = parse_attempt_key(&entry.key)
                    && self.is_job_expired(&parsed.tenant, &parsed.job_id).await
                {
                    self.dropped_attempt += 1;
                    return Ok(self.drop_decision());
                }
            }

            // ── JOB_CANCELLED (0x0A): point-read JOB_STATUS ──
            prefix::JOB_CANCELLED => {
                if let Some(parsed) = parse_job_cancelled_key(&entry.key)
                    && self.is_job_expired(&parsed.tenant, &parsed.job_id).await
                {
                    self.dropped_job_cancelled += 1;
                    return Ok(self.drop_decision());
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
                is_dest_last_run = self.is_dest_last_run,
                cutoff_ms = self.cutoff_ms,
                "completed_jobs compaction filter: dropped rows"
            );
        }

        // In the standalone compactor we have no writable Db handle, so
        // counter decrements are only tracked for diagnostics and emitted
        // here. The live silo writer reconciles counters elsewhere.
        if self.dropped_status > 0 {
            let tenants: Vec<_> = self
                .tenant_status_drops
                .iter()
                .map(|((tenant, status), count)| format!("{tenant}/{status}={count}"))
                .collect();
            warn!(
                jobs_dropped = self.dropped_status,
                tenant_status = ?tenants,
                "completed_jobs compaction filter: counter decrements skipped \
                 (standalone compactor has no Db writer)"
            );
        }

        Ok(())
    }
}

fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use silo::codec::encode_job_status;
    use silo::job::{JobStatus, JobStatusKind};
    use silo::keys::{idx_status_time_key, job_info_key, job_status_key};
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

    fn make_filter(cutoff_ms: i64, is_dest_last_run: bool) -> CompletedJobCompactionFilter {
        CompletedJobCompactionFilter {
            cutoff_ms,
            reader: None,
            is_dest_last_run,
            dropped_job_info: 0,
            dropped_status: 0,
            dropped_idx_status_time: 0,
            dropped_idx_metadata: 0,
            dropped_attempt: 0,
            dropped_job_cancelled: 0,
            tenant_status_drops: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn drop_decision_uses_drop_when_last_run() {
        let filter = make_filter(0, true);
        assert_eq!(filter.drop_decision(), CompactionFilterDecision::Drop);
    }

    #[tokio::test]
    async fn drop_decision_uses_tombstone_when_not_last_run() {
        let filter = make_filter(0, false);
        assert_eq!(
            filter.drop_decision(),
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
    }

    #[tokio::test]
    async fn drops_old_completed_status() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let old_succeeded = status_row(JobStatusKind::Succeeded, cutoff - 1);
        let decision = filter.filter(&old_succeeded).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Drop);
        assert_eq!(filter.dropped_status, 1);
    }

    #[tokio::test]
    async fn tombstones_old_completed_status_when_not_last_run() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, false);
        let old_failed = status_row(JobStatusKind::Failed, cutoff - 1);
        let decision = filter.filter(&old_failed).await.unwrap();
        assert_eq!(
            decision,
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.dropped_status, 1);
    }

    #[tokio::test]
    async fn keeps_recent_completed_status() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let recent = status_row(JobStatusKind::Failed, cutoff + 1);
        let decision = filter.filter(&recent).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.dropped_status, 0);
    }

    #[tokio::test]
    async fn keeps_old_running_status() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let old_running = status_row(JobStatusKind::Running, cutoff - 10);
        let decision = filter.filter(&old_running).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn drops_old_terminal_idx_status_time() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let key = idx_status_time_key("tenant-1", "Succeeded", cutoff - 500, "job-1");
        let entry = row(key, Bytes::from_static(b""));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Drop);
        assert_eq!(filter.dropped_idx_status_time, 1);
    }

    #[tokio::test]
    async fn keeps_non_terminal_idx_status_time() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let key = idx_status_time_key("tenant-1", "Scheduled", cutoff - 500, "job-1");
        let entry = row(key, Bytes::from_static(b""));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn keeps_tombstone_entries() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let entry = RowEntry {
            key: Bytes::from(job_status_key("t", "j")),
            value: ValueDeletable::Tombstone,
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn keeps_job_info_without_reader() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let entry = row(job_info_key("tenant-1", "job-1"), Bytes::from_static(b"x"));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    #[tokio::test]
    async fn tracks_tenant_status_drops() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);
        let row_a = status_row(JobStatusKind::Succeeded, cutoff - 10);
        let _ = filter.filter(&row_a).await.unwrap();
        let row_b = status_row(JobStatusKind::Failed, cutoff - 5);
        let _ = filter.filter(&row_b).await.unwrap();
        let succ = filter
            .tenant_status_drops
            .get(&("tenant-1".to_string(), "Succeeded".to_string()));
        let fail = filter
            .tenant_status_drops
            .get(&("tenant-1".to_string(), "Failed".to_string()));
        assert_eq!(succ.copied(), Some(1));
        assert_eq!(fail.copied(), Some(1));
    }

    #[tokio::test]
    async fn supplier_without_reader_builds_filter() {
        let sup = CompletedJobCompactionFilterSupplier::new_without_reader(Duration::from_secs(5));
        let ctx = CompactionJobContext {
            destination: 0,
            is_dest_last_run: true,
            compaction_clock_tick: 0,
            retention_min_seq: None,
        };
        let filter = sup.create_compaction_filter(&ctx).await.unwrap();
        drop(filter);
    }
}
