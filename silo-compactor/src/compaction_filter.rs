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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use object_store::path::Path;
use silo::codec::decode_job_status_owned;
use silo::job_store_shard::counter_merge_operator;
use silo::keys::{
    idx_status_time_all_prefix, parse_attempt_key, parse_job_cancelled_key, parse_job_info_key,
    parse_job_status_key, parse_metadata_index_key, parse_status_time_index_key, prefix,
};
use slatedb::config::ScanOptions;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, DbReader, DbReaderBuilder, RowEntry, ValueDeletable,
};
use tracing::{debug, info, warn};

/// Terminal status strings as stored in IDX_STATUS_TIME keys.
const TERMINAL_STATUSES: &[&str] = &["Succeeded", "Failed", "Cancelled"];

/// Default retention window for completed jobs: 7 days.
pub const DEFAULT_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// Default upper bound on the pre-scanned `ExpiredSet`. If the IDX_STATUS_TIME
/// pass would exceed this, the supplier falls back to per-row point reads
/// rather than holding an arbitrarily large set in memory.
pub const DEFAULT_EXPIRED_SET_MAX_ENTRIES: usize = 250_000;

// ---------------------------------------------------------------------------
// ExpiredSet
// ---------------------------------------------------------------------------

/// Cache of `(tenant, job_id)` pairs that the filter knows to be terminal +
/// older than `cutoff_ms`. Built once per compaction job by scanning
/// IDX_STATUS_TIME via a `DbReader`, then consulted by `is_job_expired`
/// instead of issuing one point-read per row.
///
/// Stored as a per-tenant map so that `contains(&str, &str)` doesn't have to
/// allocate a tuple key on every lookup — the `HashMap::get` accepts `&str`
/// directly via `Borrow<str>` on `String`.
struct ExpiredSet {
    by_tenant: HashMap<String, HashSet<String>>,
    total: usize,
}

impl ExpiredSet {
    fn new() -> Self {
        Self {
            by_tenant: HashMap::new(),
            total: 0,
        }
    }

    fn contains(&self, tenant: &str, job_id: &str) -> bool {
        self.by_tenant
            .get(tenant)
            .is_some_and(|jobs| jobs.contains(job_id))
    }

    fn insert(&mut self, tenant: String, job_id: String) {
        if self.by_tenant.entry(tenant).or_default().insert(job_id) {
            self.total += 1;
        }
    }

    fn len(&self) -> usize {
        self.total
    }
}

/// Scan IDX_STATUS_TIME via `reader` and collect every (tenant, job_id) pair
/// whose key parses to a TERMINAL status with `changed_at_ms < cutoff_ms`.
///
/// Returns `Ok(Some(set))` on a clean drain, `Ok(None)` when the cap was
/// exceeded mid-scan (caller falls back to per-row point reads), or `Err`
/// when the underlying scan errored.
///
/// Reads with `cache_blocks: false` so a one-shot pre-scan doesn't pollute
/// the compactor's block cache.
async fn build_expired_set(
    reader: &DbReader,
    cutoff_ms: i64,
    max_entries: usize,
) -> Result<Option<ExpiredSet>, slatedb::Error> {
    if max_entries == 0 {
        return Ok(None);
    }
    let opts = ScanOptions {
        cache_blocks: false,
        ..ScanOptions::default()
    };
    let mut iter = reader
        .scan_prefix_with_options(idx_status_time_all_prefix(), &opts)
        .await?;
    let mut set = ExpiredSet::new();
    while let Some(kv) = iter.next().await? {
        let Some(parsed) = parse_status_time_index_key(&kv.key) else {
            continue;
        };
        if !TERMINAL_STATUSES.contains(&parsed.status.as_str()) {
            continue;
        }
        if parsed.changed_at_ms() >= cutoff_ms {
            continue;
        }
        set.insert(parsed.tenant, parsed.job_id);
        if set.len() > max_entries {
            return Ok(None);
        }
    }
    Ok(Some(set))
}

// ---------------------------------------------------------------------------
// Supplier
// ---------------------------------------------------------------------------

/// Creates a fresh [`CompletedJobCompactionFilter`] for each compaction job.
///
/// The supplier lazily opens a [`DbReader`] per compaction job and uses it
/// to pre-scan the IDX_STATUS_TIME index into an [`ExpiredSet`]. Per-row
/// `is_job_expired` decisions are then served from the set instead of
/// issuing one point-read per row. The reader is NOT kept alive across
/// jobs — this avoids a long-lived manifest poller / checkpoint that can
/// interfere with the compactor's state management.
///
/// If the pre-scan fails (reader couldn't open, scan errored, or
/// `expired_set_max_entries` was exceeded) the filter falls back to
/// per-row point reads via the same `DbReader`.
pub struct CompletedJobCompactionFilterSupplier {
    retention: Duration,
    /// Object-store context for lazily opening a `DbReader` per compaction
    /// job. `None` disables point reads (unit tests only).
    reader_ctx: Option<ReaderContext>,
    /// Soft cap on the pre-scanned [`ExpiredSet`]. `0` disables the
    /// pre-scan entirely (every `is_job_expired` becomes a point read).
    expired_set_max_entries: usize,
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
            expired_set_max_entries: DEFAULT_EXPIRED_SET_MAX_ENTRIES,
        }
    }

    /// Override the per-filter pre-scan cap. `0` disables the pre-scan and
    /// forces every `is_job_expired` to issue a point read.
    pub fn with_max_expired_entries(mut self, cap: usize) -> Self {
        self.expired_set_max_entries = cap;
        self
    }

    #[cfg(test)]
    fn new_without_reader(retention: Duration) -> Self {
        Self {
            retention,
            reader_ctx: None,
            expired_set_max_entries: DEFAULT_EXPIRED_SET_MAX_ENTRIES,
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
                Ok(r) => Some(Arc::new(r)),
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

        // Pre-scan IDX_STATUS_TIME into an ExpiredSet. When this succeeds the
        // filter serves `is_job_expired` from memory; otherwise it falls back
        // to per-row point reads via the same DbReader.
        let expired = match reader.as_ref() {
            Some(r) => {
                let started = Instant::now();
                match build_expired_set(r, cutoff_ms, self.expired_set_max_entries).await {
                    Ok(Some(set)) => {
                        info!(
                            entries = set.len(),
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            cap = self.expired_set_max_entries,
                            "completed_jobs filter: pre-scan ready"
                        );
                        Some(Arc::new(set))
                    }
                    Ok(None) => {
                        warn!(
                            cap = self.expired_set_max_entries,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "completed_jobs filter: pre-scan exceeded cap or disabled, \
                             falling back to per-row point reads"
                        );
                        None
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "completed_jobs filter: pre-scan failed, falling back to \
                             per-row point reads"
                        );
                        None
                    }
                }
            }
            None => None,
        };

        Ok(Box::new(CompletedJobCompactionFilter {
            cutoff_ms,
            reader,
            expired,
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
    /// DbReader used by `is_job_expired` only when `expired` is `None` (the
    /// pre-scan failed or was disabled). Otherwise membership is served
    /// entirely from the pre-built [`ExpiredSet`].
    reader: Option<Arc<DbReader>>,
    /// Pre-scanned set of `(tenant, job_id)` that the filter knows to be
    /// terminal + expired. Built once at filter creation. `None` means the
    /// filter must fall back to per-row point reads.
    expired: Option<Arc<ExpiredSet>>,
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

    /// Decide whether `(tenant, job_id)` is terminal and older than the
    /// retention cutoff.
    ///
    /// Fast path: consult the pre-scanned [`ExpiredSet`] when present. The
    /// set is built from IDX_STATUS_TIME, which silo maintains as a
    /// single-entry-per-job index — a pair only appears in the set if the
    /// live status row says terminal+old, so no point read is needed.
    ///
    /// Fallback: when the pre-scan was skipped (cap exceeded, scan failed,
    /// or no reader), point-read the job's `JOB_STATUS` and decode.
    async fn is_job_expired(&self, tenant: &str, job_id: &str) -> bool {
        if let Some(set) = &self.expired {
            return set.contains(tenant, job_id);
        }
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
            expired: None,
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

    // ── ExpiredSet tests (no DB needed) ──

    #[test]
    fn expired_set_lookup_by_str_does_not_allocate() {
        let mut set = ExpiredSet::new();
        set.insert("tenant-a".into(), "job-1".into());
        set.insert("tenant-a".into(), "job-2".into());
        set.insert("tenant-b".into(), "job-3".into());
        assert_eq!(set.len(), 3);
        assert!(set.contains("tenant-a", "job-1"));
        assert!(set.contains("tenant-a", "job-2"));
        assert!(set.contains("tenant-b", "job-3"));
        assert!(!set.contains("tenant-a", "job-3"));
        assert!(!set.contains("tenant-c", "job-1"));
    }

    #[test]
    fn expired_set_dedupes_inserts() {
        let mut set = ExpiredSet::new();
        set.insert("t".into(), "j".into());
        set.insert("t".into(), "j".into());
        assert_eq!(set.len(), 1);
    }

    #[tokio::test]
    async fn is_job_expired_uses_set_when_present() {
        // No reader, but a populated set: filter must answer purely from the
        // set with no point-read attempt.
        let mut set = ExpiredSet::new();
        set.insert("tenant-a".into(), "expired-job".into());
        let mut filter = make_filter(now_epoch_ms() - 1_000, true);
        filter.expired = Some(Arc::new(set));
        assert!(filter.is_job_expired("tenant-a", "expired-job").await);
        assert!(!filter.is_job_expired("tenant-a", "other-job").await);
        assert!(!filter.is_job_expired("tenant-b", "expired-job").await);
    }

    // ── build_expired_set against a real in-memory slatedb ──

    use slatedb::Db;
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;

    /// Open an in-memory slatedb, write a batch of IDX_STATUS_TIME rows
    /// matching the given (tenant, status, ts, job_id) tuples, close it,
    /// then return a freshly-opened `DbReader` over the same store.
    async fn seed_idx_status_time(rows: &[(&str, &str, i64, &str)]) -> Arc<DbReader> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = slatedb::object_store::path::Path::from("/db");
        {
            let db = Db::builder(path.clone(), Arc::clone(&store))
                .build()
                .await
                .unwrap();
            for (tenant, status, ts, job_id) in rows {
                let key = idx_status_time_key(tenant, status, *ts, job_id);
                db.put(&key, []).await.unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
            db.close().await.unwrap();
        }
        Arc::new(DbReaderBuilder::new(path, store).build().await.unwrap())
    }

    #[tokio::test]
    async fn build_expired_set_picks_only_terminal_and_old() {
        let cutoff = 1_000_000_000;
        let reader = seed_idx_status_time(&[
            ("tenant-a", "Succeeded", cutoff - 1, "job-old-ok"),
            ("tenant-a", "Failed", cutoff - 100, "job-old-fail"),
            ("tenant-b", "Cancelled", cutoff - 50, "job-old-cancel"),
            ("tenant-a", "Succeeded", cutoff + 1, "job-recent-ok"),
            ("tenant-a", "Running", cutoff - 999_999, "job-old-run"),
            ("tenant-a", "Scheduled", cutoff - 999_999, "job-old-sched"),
        ])
        .await;
        let set = build_expired_set(&reader, cutoff, 100)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains("tenant-a", "job-old-ok"));
        assert!(set.contains("tenant-a", "job-old-fail"));
        assert!(set.contains("tenant-b", "job-old-cancel"));
        assert!(!set.contains("tenant-a", "job-recent-ok"));
        assert!(!set.contains("tenant-a", "job-old-run"));
        assert!(!set.contains("tenant-a", "job-old-sched"));
    }

    #[tokio::test]
    async fn build_expired_set_returns_none_when_cap_exceeded() {
        let cutoff = 1_000_000_000;
        let reader = seed_idx_status_time(&[
            ("tenant-a", "Succeeded", cutoff - 1, "j1"),
            ("tenant-a", "Succeeded", cutoff - 2, "j2"),
            ("tenant-a", "Succeeded", cutoff - 3, "j3"),
        ])
        .await;
        // Cap of 2 with three terminal+old rows → returns None.
        let result = build_expired_set(&reader, cutoff, 2).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn build_expired_set_returns_none_when_max_entries_zero() {
        // cap = 0 short-circuits without scanning at all.
        let reader = seed_idx_status_time(&[("t", "Succeeded", 0, "j")]).await;
        let result = build_expired_set(&reader, i64::MAX, 0).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn build_expired_set_handles_empty_index() {
        let reader = seed_idx_status_time(&[]).await;
        let set = build_expired_set(&reader, i64::MAX, 100)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set.len(), 0);
        assert!(!set.contains("anything", "anywhere"));
    }
}
