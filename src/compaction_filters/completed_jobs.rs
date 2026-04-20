//! Compaction filter that drops completed (terminal) job data older than a
//! configurable retention window.
//!
//! During compaction the filter inspects rows across six key prefixes. For
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

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, Db, DbReader, DbReaderBuilder, RowEntry, ValueDeletable,
};
use tracing::{debug, info, warn};

use crate::codec::decode_job_status_owned;
use crate::job_store_shard::counter_merge_operator;
use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::helpers::now_epoch_ms;
use crate::keys::{
    parse_attempt_key, parse_job_cancelled_key, parse_job_info_key, parse_job_status_key,
    parse_metadata_index_key, parse_status_time_index_key, prefix,
    shard_completed_jobs_counter_key, shard_total_jobs_counter_key, tenant_status_counter_key,
};

/// Terminal status strings as stored in IDX_STATUS_TIME keys.
const TERMINAL_STATUSES: &[&str] = &["Succeeded", "Failed", "Cancelled"];

// ---------------------------------------------------------------------------
// Supplier
// ---------------------------------------------------------------------------

/// Creates a fresh [`CompletedJobCompactionFilter`] for each compaction job.
///
/// The supplier lazily opens a [`DbReader`] per compaction job for point-
/// reading JOB_STATUS rows. The reader is NOT kept alive across jobs — this
/// avoids a long-lived manifest poller / checkpoint that can interfere with
/// the compactor's state management.
///
/// An optional `Db` writer handle (via `OnceLock<Weak<Db>>`) is available
/// for counter decrements in `on_compaction_end`. In the standalone
/// `silo-compactor` binary there is no writable `Db`, so counter writes
/// are skipped.
pub struct CompletedJobCompactionFilterSupplier {
    retention: Duration,
    /// Object-store context for lazily opening a `DbReader` per compaction
    /// job. `None` when no reader is desired (unit tests).
    reader_ctx: Option<ReaderContext>,
    db_writer: Arc<OnceLock<Weak<Db>>>,
}

/// Everything needed to open a short-lived [`DbReader`].
struct ReaderContext {
    db_path: Path,
    store: Arc<dyn object_store::ObjectStore>,
}

impl CompletedJobCompactionFilterSupplier {
    pub fn new(
        retention: Duration,
        reader_ctx: Option<(Path, Arc<dyn object_store::ObjectStore>)>,
    ) -> Self {
        Self {
            retention,
            reader_ctx: reader_ctx.map(|(db_path, store)| ReaderContext { db_path, store }),
            db_writer: Arc::new(OnceLock::new()),
        }
    }

    /// Provide the live `Db` handle so `on_compaction_end` can write counter
    /// decrements. Call this after `DbBuilder::build()` in server-side usage.
    pub fn set_db(&self, db: &Arc<Db>) {
        let _ = self.db_writer.set(Arc::downgrade(db));
    }

    /// Clone of the writer-handle Arc for external callers that will call
    /// `set_db` later.
    pub fn db_writer_handle(&self) -> Arc<OnceLock<Weak<Db>>> {
        Arc::clone(&self.db_writer)
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

        let db_writer = self.db_writer.get().and_then(|weak| weak.upgrade());

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
            db_writer,
            is_dest_last_run: context.is_dest_last_run,
            dropped_job_info: 0,
            dropped_status: 0,
            dropped_idx_status_time: 0,
            dropped_idx_metadata: 0,
            dropped_attempt: 0,
            dropped_job_cancelled: 0,
            decision_drops: 0,
            decision_tombstones: 0,
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
    db_writer: Option<Arc<Db>>,
    is_dest_last_run: bool,

    // Per-prefix drop counts (diagnostics).
    dropped_job_info: u64,
    dropped_status: u64,
    dropped_idx_status_time: u64,
    dropped_idx_metadata: u64,
    dropped_attempt: u64,
    dropped_job_cancelled: u64,
    // Drop vs Tombstone counts — every entry in a compaction job gets the
    // same decision (determined by is_dest_last_run), but tracking both
    // makes it visible in the logs which path was taken.
    decision_drops: u64,
    decision_tombstones: u64,

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
    /// resurrection risk), [`Modify(Tombstone)`] otherwise. Tracks the
    /// count of each decision for diagnostics.
    fn drop_decision(&mut self) -> CompactionFilterDecision {
        if self.is_dest_last_run {
            self.decision_drops += 1;
            CompactionFilterDecision::Drop
        } else {
            self.decision_tombstones += 1;
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        }
    }

    /// Point-read the job's `JOB_STATUS` from the live DB and check whether
    /// it is terminal and older than the retention cutoff.
    async fn is_job_expired(&self, tenant: &str, job_id: &str) -> bool {
        let Some(reader) = &self.reader else {
            return false;
        };
        let key = crate::keys::job_status_key(tenant, job_id);
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
                if let Some(parsed) = parse_job_info_key(&entry.key) {
                    if self.is_job_expired(&parsed.tenant, &parsed.job_id).await {
                        self.dropped_job_info += 1;
                        return Ok(self.drop_decision());
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
                    // Track per-tenant status drops for counter decrements.
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
                if let Some(parsed) = parse_status_time_index_key(&entry.key) {
                    if TERMINAL_STATUSES.contains(&parsed.status.as_str())
                        && parsed.changed_at_ms() < self.cutoff_ms
                    {
                        self.dropped_idx_status_time += 1;
                        return Ok(self.drop_decision());
                    }
                }
            }

            // ── IDX_METADATA (0x04): point-read JOB_STATUS ──
            prefix::IDX_METADATA => {
                if let Some(parsed) = parse_metadata_index_key(&entry.key) {
                    if self.is_job_expired(&parsed.tenant, &parsed.job_id).await {
                        self.dropped_idx_metadata += 1;
                        return Ok(self.drop_decision());
                    }
                }
            }

            // ── ATTEMPT (0x07): point-read JOB_STATUS ──
            prefix::ATTEMPT => {
                if let Some(parsed) = parse_attempt_key(&entry.key) {
                    if self.is_job_expired(&parsed.tenant, &parsed.job_id).await {
                        self.dropped_attempt += 1;
                        return Ok(self.drop_decision());
                    }
                }
            }

            // ── JOB_CANCELLED (0x0A): point-read JOB_STATUS ──
            prefix::JOB_CANCELLED => {
                if let Some(parsed) = parse_job_cancelled_key(&entry.key) {
                    if self
                        .is_job_expired(&parsed.tenant, &parsed.job_id)
                        .await
                    {
                        self.dropped_job_cancelled += 1;
                        return Ok(self.drop_decision());
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
                drops = self.decision_drops,
                tombstones = self.decision_tombstones,
                is_dest_last_run = self.is_dest_last_run,
                cutoff_ms = self.cutoff_ms,
                "completed_jobs compaction filter: dropped rows"
            );
        }

        let jobs_dropped = self.dropped_status;
        if jobs_dropped == 0 {
            return Ok(());
        }

        let Some(db) = &self.db_writer else {
            warn!(
                jobs_dropped,
                drops = self.decision_drops,
                tombstones = self.decision_tombstones,
                "completed_jobs compaction filter: counter decrements skipped (no Db writer)"
            );
            return Ok(());
        };

        // Decrement shard-level counters.
        let delta = Bytes::copy_from_slice(&encode_counter(-(jobs_dropped as i64)));
        db.merge(shard_total_jobs_counter_key(), delta.clone())
            .await
            .map_err(|e| CompactionFilterError::CompactionEndError(e.into()))?;
        db.merge(shard_completed_jobs_counter_key(), delta)
            .await
            .map_err(|e| CompactionFilterError::CompactionEndError(e.into()))?;

        // Decrement per-tenant status counters.
        for ((tenant, status_kind), count) in &self.tenant_status_drops {
            let key = tenant_status_counter_key(tenant, status_kind);
            let delta = Bytes::copy_from_slice(&encode_counter(-(*count as i64)));
            db.merge(key, delta)
                .await
                .map_err(|e| CompactionFilterError::CompactionEndError(e.into()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_job_status;
    use crate::job::{JobStatus, JobStatusKind};
    use crate::keys::{idx_status_time_key, job_status_key};

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
            db_writer: None,
            is_dest_last_run,
            dropped_job_info: 0,
            dropped_status: 0,
            dropped_idx_status_time: 0,
            dropped_idx_metadata: 0,
            dropped_attempt: 0,
            dropped_job_cancelled: 0,
            decision_drops: 0,
            decision_tombstones: 0,
            tenant_status_drops: HashMap::new(),
        }
    }

    // ── drop_decision tests ──

    #[tokio::test]
    async fn drop_decision_uses_drop_when_last_run() {
        let mut filter = make_filter(0, true);
        assert_eq!(filter.drop_decision(), CompactionFilterDecision::Drop);
        assert_eq!(filter.decision_drops, 1);
        assert_eq!(filter.decision_tombstones, 0);
    }

    #[tokio::test]
    async fn drop_decision_uses_tombstone_when_not_last_run() {
        let mut filter = make_filter(0, false);
        assert_eq!(
            filter.drop_decision(),
            CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
        );
        assert_eq!(filter.decision_drops, 0);
        assert_eq!(filter.decision_tombstones, 1);
    }

    // ── JOB_STATUS tests ──

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
    async fn keeps_old_scheduled_status() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);

        let old_scheduled = status_row(JobStatusKind::Scheduled, cutoff - 10);
        let decision = filter.filter(&old_scheduled).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    // ── IDX_STATUS_TIME tests ──

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
    async fn keeps_recent_terminal_idx_status_time() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);

        let key = idx_status_time_key("tenant-1", "Failed", cutoff + 500, "job-1");
        let entry = row(key, Bytes::from_static(b""));
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
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

    // ── Tombstone / Merge skip tests ──

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
    async fn keeps_merge_entries() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);

        let entry = RowEntry {
            key: Bytes::from(shard_total_jobs_counter_key()),
            value: ValueDeletable::Merge(Bytes::copy_from_slice(&encode_counter(1))),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    // ── JOB_INFO tests (no reader → graceful degradation) ──

    #[tokio::test]
    async fn keeps_job_info_without_reader() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);

        let entry = row(
            crate::keys::job_info_key("tenant-1", "job-1"),
            Bytes::from_static(b"opaque"),
        );
        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
    }

    // ── Tenant status counter tracking ──

    #[tokio::test]
    async fn tracks_tenant_status_drops() {
        let cutoff = now_epoch_ms() - 1_000;
        let mut filter = make_filter(cutoff, true);

        // Drop a Succeeded and a Failed job
        let s1 = status_row(JobStatusKind::Succeeded, cutoff - 1);
        filter.filter(&s1).await.unwrap();

        let s2 = {
            let status = JobStatus::new(JobStatusKind::Failed, cutoff - 2, None, None);
            let bytes = encode_job_status(&status);
            row(job_status_key("tenant-1", "job-2"), Bytes::from(bytes))
        };
        filter.filter(&s2).await.unwrap();

        assert_eq!(filter.dropped_status, 2);
        assert_eq!(
            filter
                .tenant_status_drops
                .get(&("tenant-1".to_owned(), "Succeeded".to_owned())),
            Some(&1)
        );
        assert_eq!(
            filter
                .tenant_status_drops
                .get(&("tenant-1".to_owned(), "Failed".to_owned())),
            Some(&1)
        );
    }
}
