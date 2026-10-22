//! Backfill of the listing indexes for jobs created before they existed:
//! the enqueue-time index (`IDX_ENQUEUE_TIME`, version 1), the
//! status-segmented enqueue-time index (`IDX_STATUS_ENQUEUE_TIME`, version
//! 2), and the enqueue time carried in each metadata index entry's value
//! (`IDX_METADATA`, version 3).
//!
//! A completion marker, mirrored by an in-memory version, records the highest
//! version the sweep has completed; each query path requires its index's
//! version. A sweep drives the shard from its current version to a target.
//!
//! The version-1 sweep walks `JOB_INFO` inside the shard's tenant range and
//! writes each job's missing enqueue-time entry. The version-2 sweep makes two
//! passes: the first walks `JOB_INFO`, writes each job's missing enqueue-time
//! entry, and reconciles its status-segmented entry, placing it under the
//! job's stored status and removing any entry in another segment; the second
//! walks both index families and removes entries whose job is gone. The
//! version-3 sweep's jobs pass also rewrites each of the job's metadata
//! entries whose value lacks the job's enqueue time or whose TTL differs
//! from `JOB_INFO`'s, and a third pass walks the metadata index and removes
//! entries whose job is gone.
//!
//! Enumeration runs outside any transaction (a transactional scan would
//! register the whole range in the read set and abort on every busy tenant);
//! each batch's writes then run in a serializable-snapshot transaction that
//! re-reads the rows it depends on, so a delete or transition that lands
//! mid-batch wins. A conflicting batch is retried and then split into per-row
//! transactions, so a row that still has `JOB_INFO` is never skipped: a
//! transition with no retention TTL leaves an entry untouched, so a skipped
//! live job would be missing from the index for good.
//!
//! Progress is checkpointed after every batch with the target version, the
//! pass, and the last key walked, and the sweep resumes from there after a
//! restart. A checkpoint from a different target is discarded.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use slatedb::IsolationLevel;
use tracing::{debug, error, info, warn};

use crate::job::{JobView, ListingSegment};
use crate::job_store_shard::helpers::{
    decode_job_status_owned, is_txn_conflict, put_with_optional_expire, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, shard_name_jitter_ms};
use crate::keys::{
    MetadataIndexValue, StatusEnqueueTimeIndexValue, end_bound,
    enqueue_time_index_backfill_complete_key, enqueue_time_index_backfill_progress_key,
    idx_enqueue_time_key, idx_metadata_key, idx_status_enqueue_time_candidate_keys,
    idx_status_enqueue_time_key, job_info_key, job_status_key, jobs_prefix,
    parse_enqueue_time_index_key, parse_job_info_key, parse_metadata_index_key,
    parse_status_enqueue_time_index_key, prefix,
};

/// The listing-index version at which the enqueue-time index covers every
/// job on the shard.
pub const ENQUEUE_TIME_INDEX_VERSION: u8 = 1;

/// The listing-index version at which both the enqueue-time index and the
/// status-segmented enqueue-time index cover every job on the shard.
pub const STATUS_ENQUEUE_TIME_INDEX_VERSION: u8 = 2;

/// The listing-index version at which, in addition, every metadata index
/// entry on the shard carries its job's enqueue time and points at a job
/// that exists.
pub const METADATA_ENQUEUE_TIME_INDEX_VERSION: u8 = 3;

/// Upper bound on the deterministic delay before a shard's background sweep
/// starts, so shards opening together do not scan in lockstep.
const START_JITTER_MS: u64 = 1_000;

/// Longest pause between attempts when the background sweep fails with an
/// error other than a transaction conflict (an object-store hiccup, say).
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// Rows a batch may walk, as a multiple of its in-range batch size, before it
/// checkpoints even when most of them are outside the shard's range.
const WALK_MULTIPLIER: usize = 4;

/// The walk a checkpoint belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
enum BackfillPass {
    /// Every in-range `JOB_INFO` row: write the job's entries.
    #[default]
    Jobs,
    /// Every in-range entry in both listing-index families: remove entries
    /// whose `JOB_INFO` is gone.
    Orphans,
    /// Every in-range metadata index entry: remove entries whose `JOB_INFO`
    /// is gone.
    MetadataOrphans,
}

impl BackfillPass {
    /// The pass that follows this one in a sweep to `target_version`, or
    /// None when the sweep is done.
    fn next(self, target_version: u8) -> Option<Self> {
        match self {
            Self::Jobs => {
                (target_version >= STATUS_ENQUEUE_TIME_INDEX_VERSION).then_some(Self::Orphans)
            }
            Self::Orphans => (target_version >= METADATA_ENQUEUE_TIME_INDEX_VERSION)
                .then_some(Self::MetadataOrphans),
            Self::MetadataOrphans => None,
        }
    }
}

/// A checkpoint without a `version` field belongs to the version-1 sweep.
fn checkpoint_version_default() -> u8 {
    ENQUEUE_TIME_INDEX_VERSION
}

/// Progress checkpoint persisted after every batch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackfillProgress {
    /// The target version this checkpoint belongs to.
    #[serde(default = "checkpoint_version_default")]
    version: u8,
    #[serde(default)]
    pass: BackfillPass,
    /// Last key examined by the pass (in or out of range), hex-encoded; the
    /// next batch starts just after it. In the orphan pass the key's prefix
    /// identifies the family being walked.
    last_key: Option<String>,
    rows_scanned: u64,
    rows_written: u64,
}

impl BackfillProgress {
    fn fresh(version: u8) -> Self {
        Self {
            version,
            pass: BackfillPass::Jobs,
            last_key: None,
            rows_scanned: 0,
            rows_written: 0,
        }
    }
}

/// Outcome of a run of the sweep.
#[derive(Debug, Clone)]
pub struct ListingIndexBackfillResult {
    /// Rows inside the shard's range the sweep examined, across both passes.
    pub rows_scanned: u64,
    /// Rows the sweep wrote or deleted entries for.
    pub rows_written: u64,
    /// Whether the marker is at or above the target version.
    pub complete: bool,
    /// Whether the run stopped early because the shard is closing.
    pub cancelled: bool,
}

/// A row the enumerating scan found inside the shard's range: a `JOB_INFO`
/// key in the jobs pass, an index key in the orphan pass.
struct Candidate {
    key: Vec<u8>,
    tenant: String,
    job_id: String,
}

/// One batch of the enumerating scan.
struct BackfillBatch {
    candidates: Vec<Candidate>,
    /// The last key the scan examined, in or out of range; the next batch
    /// starts just after it.
    last_walked_key: Option<Vec<u8>>,
    /// Whether the scan reached the end of the pass's range.
    exhausted: bool,
}

fn decode_marker_version(value: &[u8]) -> u8 {
    value.first().copied().unwrap_or(ENQUEUE_TIME_INDEX_VERSION)
}

impl JobStoreShard {
    /// The highest listing-index version the backfill has completed on this
    /// shard: 0 before any sweep, [`ENQUEUE_TIME_INDEX_VERSION`] once the
    /// enqueue-time index covers every job,
    /// [`STATUS_ENQUEUE_TIME_INDEX_VERSION`] once the status-segmented index
    /// does too, and [`METADATA_ENQUEUE_TIME_INDEX_VERSION`] once every
    /// metadata index entry carries its job's enqueue time as well.
    pub fn listing_index_version(&self) -> u8 {
        self.listing_index_version.load(Ordering::Acquire)
    }

    /// Whether every metadata index entry on this shard carries its job's
    /// enqueue time and points at a job that exists, so the query engine may
    /// serve a metadata-filtered listing of ids and enqueue times from the
    /// index alone.
    pub fn metadata_enqueue_time_index_complete(&self) -> bool {
        self.listing_index_version() >= METADATA_ENQUEUE_TIME_INDEX_VERSION
    }

    /// Whether this shard's enqueue-time index covers every job, so the
    /// query engine may serve listings from it.
    pub fn enqueue_time_index_complete(&self) -> bool {
        self.listing_index_version() >= ENQUEUE_TIME_INDEX_VERSION
    }

    /// Whether this shard's status-segmented enqueue-time index covers every
    /// job, so the query engine may serve status listings from it.
    pub fn status_enqueue_time_index_complete(&self) -> bool {
        self.listing_index_version() >= STATUS_ENQUEUE_TIME_INDEX_VERSION
    }

    /// Set the completion marker to `version` directly, bypassing the sweep;
    /// version 0 clears it.
    pub async fn set_listing_index_version(&self, version: u8) -> Result<(), JobStoreShardError> {
        let key = enqueue_time_index_backfill_complete_key();
        if version == 0 {
            self.db.delete(&key).await?;
        } else {
            self.db.put(&key, [version]).await?;
        }
        self.listing_index_version.store(version, Ordering::Release);
        Ok(())
    }

    /// Load the persisted completion marker into the in-memory version. A
    /// marker with an empty value records version 1.
    pub(crate) async fn load_listing_index_version(&self) -> Result<(), JobStoreShardError> {
        let version = self
            .db
            .get(&enqueue_time_index_backfill_complete_key())
            .await?
            .map_or(0, |value| decode_marker_version(&value));
        self.listing_index_version.store(version, Ordering::Release);
        Ok(())
    }

    /// Run the sweep to `target_version` (or until the shard starts closing),
    /// resuming from the persisted checkpoint when it belongs to the same
    /// target. Returns immediately when the marker is already at or above
    /// the target.
    pub async fn backfill_listing_index(
        &self,
        target_version: u8,
        batch_size: usize,
        pause: Duration,
    ) -> Result<ListingIndexBackfillResult, JobStoreShardError> {
        let batch_size = batch_size.max(1);
        if self.listing_index_version() >= target_version {
            return Ok(Self::backfill_result(
                &BackfillProgress::fresh(target_version),
                true,
                false,
            ));
        }
        let mut progress = self.load_backfill_progress(target_version).await?;

        info!(
            shard = %self.name,
            target_version,
            pass = ?progress.pass,
            resuming_from = ?progress.last_key,
            "starting listing index backfill"
        );

        loop {
            if self.cancellation.is_cancelled() {
                info!(
                    shard = %self.name,
                    rows_scanned = progress.rows_scanned,
                    rows_written = progress.rows_written,
                    "listing index backfill cancelled (shard closing)"
                );
                return Ok(Self::backfill_result(&progress, false, true));
            }

            let batch = self
                .enumerate_backfill_candidates(&progress, batch_size)
                .await?;
            // A batch that walked nothing without exhausting the range was
            // cut short by cancellation; the loop head reports it next.
            if let Some(last_walked) = &batch.last_walked_key {
                let written = if batch.candidates.is_empty() {
                    0
                } else {
                    self.apply_backfill_batch(&progress, &batch.candidates)
                        .await?
                };

                progress.rows_scanned += batch.candidates.len() as u64;
                progress.rows_written += written;
                progress.last_key = Some(hex::encode(last_walked));
                self.save_backfill_progress(&progress).await?;
                debug!(
                    shard = %self.name,
                    pass = ?progress.pass,
                    rows_scanned = progress.rows_scanned,
                    rows_written = progress.rows_written,
                    "listing index backfill checkpoint"
                );
            }

            if batch.exhausted {
                match progress.pass.next(target_version) {
                    Some(pass) => {
                        progress.pass = pass;
                        progress.last_key = None;
                        self.save_backfill_progress(&progress).await?;
                    }
                    None => break,
                }
            }
            if !pause.is_zero() {
                tokio::select! {
                    biased;
                    _ = self.cancellation.cancelled() => {}
                    _ = tokio::time::sleep(pause) => {}
                }
            }
        }

        // The marker and the checkpoint change together, so a restart never
        // sees a completed version beside a checkpoint for it.
        let mut completion = slatedb::WriteBatch::new();
        completion.put(enqueue_time_index_backfill_complete_key(), [target_version]);
        completion.delete(enqueue_time_index_backfill_progress_key());
        self.db.write(completion).await?;
        self.listing_index_version
            .store(target_version, Ordering::Release);
        info!(
            shard = %self.name,
            target_version,
            rows_scanned = progress.rows_scanned,
            rows_written = progress.rows_written,
            "listing index backfill complete"
        );
        Ok(Self::backfill_result(&progress, true, false))
    }

    /// Spawn the background sweep at shard open when it is enabled and the
    /// marker is below the configured target. Starts after a
    /// shard-deterministic delay.
    pub(crate) fn spawn_listing_index_backfill(self: &Arc<Self>) {
        let cfg = self.enqueue_time_index_backfill.clone();
        let target_version = cfg.target_version();
        if !cfg.enabled || self.listing_index_version() >= target_version {
            return;
        }
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();

        let task = tokio::spawn(async move {
            let jitter_ms = shard_name_jitter_ms(&shard_name, START_JITTER_MS);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(jitter_ms)) => {}
            }

            // The sweep is idempotent and resumes from its checkpoint, so an
            // error is retried with capped backoff rather than leaving the
            // shard without an index until its next open.
            let mut backoff = Duration::from_secs(1);
            loop {
                match shard
                    .backfill_listing_index(
                        target_version,
                        cfg.batch_size,
                        Duration::from_millis(cfg.pause_ms),
                    )
                    .await
                {
                    Ok(_) => return,
                    Err(e) => error!(
                        shard = %shard_name,
                        error = %e,
                        retry_in_s = backoff.as_secs(),
                        "listing index backfill failed; retrying from its checkpoint"
                    ),
                }
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
            }
        });
        match self.enqueue_time_index_backfill_task.try_lock() {
            Ok(mut slot) => *slot = Some(task),
            Err(_) => {
                task.abort();
                error!(
                    shard = %self.name,
                    "listing index backfill task slot is held at open; sweep not started"
                );
            }
        }
    }

    fn backfill_result(
        progress: &BackfillProgress,
        complete: bool,
        cancelled: bool,
    ) -> ListingIndexBackfillResult {
        ListingIndexBackfillResult {
            rows_scanned: progress.rows_scanned,
            rows_written: progress.rows_written,
            complete,
            cancelled,
        }
    }

    /// Pull up to `batch_size` in-range rows of the current pass after the
    /// checkpoint, outside any transaction. The walk is also bounded by rows
    /// examined, so a long run of out-of-range rows (a split child before
    /// its cleanup) still checkpoints, pauses, and observes cancellation per
    /// batch.
    async fn enumerate_backfill_candidates(
        &self,
        progress: &BackfillProgress,
        batch_size: usize,
    ) -> Result<BackfillBatch, JobStoreShardError> {
        // The two listing-index families have adjacent prefixes, so one walk
        // covers the enqueue-time family and then the status-segmented family.
        let (range_start, range_end) = match progress.pass {
            BackfillPass::Jobs => (jobs_prefix(), end_bound(&jobs_prefix())),
            BackfillPass::Orphans => (
                vec![prefix::IDX_ENQUEUE_TIME],
                end_bound(&[prefix::IDX_STATUS_ENQUEUE_TIME]),
            ),
            BackfillPass::MetadataOrphans => (
                vec![prefix::IDX_METADATA],
                end_bound(&[prefix::IDX_METADATA]),
            ),
        };
        let start = match &progress.last_key {
            Some(last) => {
                let mut key = hex::decode(last).map_err(|e| {
                    JobStoreShardError::Codec(format!("listing index backfill checkpoint: {e}"))
                })?;
                // The smallest key strictly greater than the checkpoint.
                key.push(0x00);
                key
            }
            None => range_start,
        };

        let mut iter = self.db.scan::<Vec<u8>, _>(start..range_end).await?;
        let walk_limit = batch_size.saturating_mul(WALK_MULTIPLIER);
        let mut batch = BackfillBatch {
            candidates: Vec::with_capacity(batch_size),
            last_walked_key: None,
            exhausted: false,
        };
        let mut walked = 0usize;
        while batch.candidates.len() < batch_size
            && walked < walk_limit
            && !self.cancellation.is_cancelled()
        {
            let Some(kv) = iter.next().await? else {
                batch.exhausted = true;
                break;
            };
            walked += 1;
            batch.last_walked_key = Some(kv.key.to_vec());
            let Some((tenant, job_id)) = Self::backfill_candidate_identity(progress.pass, &kv.key)
            else {
                warn!(
                    shard = %self.name,
                    pass = ?progress.pass,
                    key = %hex::encode(&kv.key),
                    "listing index backfill: unparseable key"
                );
                continue;
            };
            if !self.range.contains_tenant(&tenant) {
                continue;
            }
            batch.candidates.push(Candidate {
                key: kv.key.to_vec(),
                tenant,
                job_id,
            });
        }
        Ok(batch)
    }

    /// The `(tenant, job_id)` a key in the pass's range identifies.
    fn backfill_candidate_identity(pass: BackfillPass, key: &[u8]) -> Option<(String, String)> {
        match pass {
            BackfillPass::Jobs => parse_job_info_key(key).map(|p| (p.tenant, p.job_id)),
            BackfillPass::Orphans => match key.first() {
                Some(&prefix::IDX_ENQUEUE_TIME) => {
                    parse_enqueue_time_index_key(key).map(|p| (p.tenant, p.job_id))
                }
                Some(&prefix::IDX_STATUS_ENQUEUE_TIME) => {
                    parse_status_enqueue_time_index_key(key).map(|p| (p.tenant, p.job_id))
                }
                _ => None,
            },
            BackfillPass::MetadataOrphans => {
                parse_metadata_index_key(key).map(|p| (p.tenant, p.job_id))
            }
        }
    }

    /// Apply the batch: the whole batch in one transaction, retried once on
    /// conflict, then one transaction per row with backoff.
    async fn apply_backfill_batch(
        &self,
        progress: &BackfillProgress,
        candidates: &[Candidate],
    ) -> Result<u64, JobStoreShardError> {
        for _ in 0..2 {
            match self.apply_backfill_txn(progress, candidates).await {
                Err(e) if is_txn_conflict(&e) => debug!(
                    shard = %self.name,
                    rows = candidates.len(),
                    "listing index backfill batch conflicted, retrying"
                ),
                result => return result,
            }
        }

        let mut written = 0;
        for candidate in candidates {
            written += retry_on_txn_conflict("listing_index_backfill_row", || {
                self.apply_backfill_txn(progress, std::slice::from_ref(candidate))
            })
            .await?;
        }
        Ok(written)
    }

    /// One serializable-snapshot transaction over `candidates`, returning the
    /// number of rows it changed.
    async fn apply_backfill_txn(
        &self,
        progress: &BackfillProgress,
        candidates: &[Candidate],
    ) -> Result<u64, JobStoreShardError> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mut written = 0;
        for candidate in candidates {
            let changed = match progress.pass {
                BackfillPass::Jobs => {
                    self.backfill_job_entries(&txn, progress.version, candidate)
                        .await?
                }
                BackfillPass::Orphans | BackfillPass::MetadataOrphans => {
                    Self::remove_orphan_entry(&txn, candidate).await?
                }
            };
            if changed {
                written += 1;
            }
        }
        if written > 0 {
            txn.commit().await?;
        }
        Ok(written)
    }

    /// Re-read the job's `JOB_INFO` (a row deleted since enumeration is
    /// skipped) and write its missing enqueue-time entry with the job's
    /// current `expire_ts`. For version 3, also rewrite each of the job's
    /// metadata entries whose value lacks the job's enqueue time or whose
    /// TTL differs from `JOB_INFO`'s. For version 2 and above, also read its
    /// `JOB_STATUS` and put the status-segmented entry under that status
    /// with the same TTL, deleting the job's entry in every other segment; a
    /// job with no status record gets no status-segmented entry in any
    /// segment.
    async fn backfill_job_entries(
        &self,
        txn: &crate::instrumented_db::InstrumentedDbTransaction,
        target_version: u8,
        candidate: &Candidate,
    ) -> Result<bool, JobStoreShardError> {
        let Some(info) = txn.get_key_value(&candidate.key).await? else {
            return Ok(false);
        };
        let view = match JobView::new(info.value) {
            Ok(view) => view,
            Err(e) => {
                warn!(
                    shard = %self.name,
                    tenant = %candidate.tenant,
                    job_id = %candidate.job_id,
                    error = %e,
                    "listing index backfill: undecodable JOB_INFO, skipping"
                );
                return Ok(false);
            }
        };
        let tenant = candidate.tenant.as_str();
        let job_id = candidate.job_id.as_str();
        let mut changed = false;

        let entry_key = idx_enqueue_time_key(tenant, view.enqueue_time_ms(), job_id);
        if txn.get(&entry_key).await?.is_none() {
            put_with_optional_expire(txn, &entry_key, [], info.expire_ts)?;
            changed = true;
        }
        if target_version >= METADATA_ENQUEUE_TIME_INDEX_VERSION {
            let value = MetadataIndexValue::new(view.enqueue_time_ms()).encode();
            for (mk, mv) in &view.metadata() {
                let key = idx_metadata_key(tenant, mk, mv, job_id);
                let current = txn.get_key_value(&key).await?;
                let up_to_date = current.is_some_and(|row| {
                    row.value.as_ref() == value.as_slice() && row.expire_ts == info.expire_ts
                });
                if !up_to_date {
                    put_with_optional_expire(txn, &key, &value, info.expire_ts)?;
                    changed = true;
                }
            }
        }
        if target_version < STATUS_ENQUEUE_TIME_INDEX_VERSION {
            return Ok(changed);
        }

        let status = match txn.get(&job_status_key(tenant, job_id)).await? {
            Some(raw) => match decode_job_status_owned(&raw) {
                Ok(status) => Some(status),
                Err(e) => {
                    warn!(
                        shard = %self.name,
                        tenant = %tenant,
                        job_id = %job_id,
                        error = %e,
                        "listing index backfill: undecodable JOB_STATUS, leaving its entries"
                    );
                    return Ok(changed);
                }
            },
            None => None,
        };
        let desired = status.as_ref().map(|status| {
            (
                idx_status_enqueue_time_key(
                    tenant,
                    ListingSegment::from_status(status),
                    view.priority(),
                    view.enqueue_time_ms(),
                    job_id,
                ),
                StatusEnqueueTimeIndexValue::for_status(status).encode(),
            )
        });
        for key in idx_status_enqueue_time_candidate_keys(
            tenant,
            view.priority(),
            view.enqueue_time_ms(),
            job_id,
        ) {
            match &desired {
                Some((desired_key, value)) if *desired_key == key => {
                    let current = txn.get_key_value(&key).await?;
                    let up_to_date = current.is_some_and(|row| {
                        row.value.as_ref() == value.as_slice() && row.expire_ts == info.expire_ts
                    });
                    if !up_to_date {
                        put_with_optional_expire(txn, &key, value, info.expire_ts)?;
                        changed = true;
                    }
                }
                _ => {
                    if txn.get(&key).await?.is_some() {
                        txn.delete(&key)?;
                        changed = true;
                    }
                }
            }
        }
        Ok(changed)
    }

    /// Delete the index entry when its job's `JOB_INFO` is gone.
    async fn remove_orphan_entry(
        txn: &crate::instrumented_db::InstrumentedDbTransaction,
        candidate: &Candidate,
    ) -> Result<bool, JobStoreShardError> {
        if txn
            .get(&job_info_key(&candidate.tenant, &candidate.job_id))
            .await?
            .is_some()
        {
            return Ok(false);
        }
        txn.delete(&candidate.key)?;
        Ok(true)
    }

    /// The checkpoint for `target_version`, or a fresh one when the stored
    /// checkpoint belongs to a different target.
    async fn load_backfill_progress(
        &self,
        target_version: u8,
    ) -> Result<BackfillProgress, JobStoreShardError> {
        let Some(data) = self
            .db
            .get(&enqueue_time_index_backfill_progress_key())
            .await?
        else {
            return Ok(BackfillProgress::fresh(target_version));
        };
        let stored: BackfillProgress = serde_json::from_slice(&data)?;
        if stored.version != target_version {
            info!(
                shard = %self.name,
                checkpoint_version = stored.version,
                target_version,
                "listing index backfill: discarding checkpoint from another target"
            );
            return Ok(BackfillProgress::fresh(target_version));
        }
        Ok(stored)
    }

    async fn save_backfill_progress(
        &self,
        progress: &BackfillProgress,
    ) -> Result<(), JobStoreShardError> {
        let data = serde_json::to_vec(progress)?;
        self.db
            .put(&enqueue_time_index_backfill_progress_key(), &data)
            .await?;
        Ok(())
    }
}
