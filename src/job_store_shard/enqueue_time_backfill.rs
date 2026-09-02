//! One-shot per-shard sweep that fills `enqueue_time_ms` into status records
//! and status/time index entries that lack it.
//!
//! The sweep walks the `JOB_STATUS` keyspace inside the shard's tenant range
//! in batches, reads `JOB_INFO` for each status record without the value, and
//! rewrites that record and its index entry in a conflict-detecting
//! transaction. It moves no counters: it never changes a status kind or adds
//! or removes rows, and every rewritten row keeps the `expire_ts` it already
//! carries. Progress is persisted after every batch so a restart resumes
//! where it left off; reaching the end of the keyspace sets the shard's
//! completion marker and flag, after which the sweep never runs again.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use slatedb::IsolationLevel;
use slatedb::bytes::Bytes;
use tracing::{debug, info, warn};

use crate::job::JobView;
use crate::job_store_shard::helpers::{TxnWriter, decode_job_status_owned};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    end_bound, enqueue_time_backfill_progress_key, job_info_key, parse_job_status_key, prefix,
};
use crate::shard_range::ShardRange;

/// Persisted sweep state: the resume point and running counts. Stays on the
/// shard after completion so the final counts remain readable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnqueueTimeBackfillProgress {
    /// Last `JOB_STATUS` key a completed batch ended on, hex encoded. The
    /// next batch starts just after it.
    pub last_key: Option<String>,
    /// Status records inside the shard range examined so far.
    pub scanned: u64,
    /// Rows rewritten with the value.
    pub repaired: u64,
    /// Rows skipped because a concurrent transition rewrote them first.
    pub skipped_conflict: u64,
    /// Rows skipped because their `JOB_INFO` no longer exists.
    pub skipped_missing_job_info: u64,
}

/// Outcome of one [`JobStoreShard::run_enqueue_time_backfill`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueTimeBackfillOutcome {
    pub progress: EnqueueTimeBackfillProgress,
    /// True when the walk reached the end of the keyspace and the completion
    /// marker is set; false when the shard's cancellation stopped it early.
    pub complete: bool,
}

/// FNV-1a hash of the shard name, for shard-deterministic jitter.
fn shard_name_hash(name: &str) -> u64 {
    let mut h: u64 = 1469598103934665603;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

impl JobStoreShard {
    /// Start the sweep in the background unless it is disabled or the shard
    /// already holds the completion marker.
    pub(crate) fn spawn_enqueue_time_backfill(self: &Arc<Self>, range: ShardRange) {
        let config = self.enqueue_time_backfill.clone();
        if !config.enabled || self.enqueue_time_backfill_complete() {
            return;
        }
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        tokio::spawn(async move {
            // Shard-deterministic jitter inside the inter-batch pause so shards
            // opening together do not start scanning at the same instant.
            let jitter_ms = shard_name_hash(&shard.name) % config.pause_ms.max(1);
            tokio::select! {
                biased;
                _ = tokio::time::sleep(Duration::from_millis(jitter_ms)) => {}
                _ = cancellation.cancelled() => return,
            }
            match shard.run_enqueue_time_backfill(&range).await {
                Ok(outcome) if outcome.complete => info!(
                    shard = %shard.name,
                    scanned = outcome.progress.scanned,
                    repaired = outcome.progress.repaired,
                    skipped_conflict = outcome.progress.skipped_conflict,
                    skipped_missing_job_info = outcome.progress.skipped_missing_job_info,
                    "enqueue-time backfill complete"
                ),
                Ok(outcome) => debug!(
                    shard = %shard.name,
                    scanned = outcome.progress.scanned,
                    repaired = outcome.progress.repaired,
                    "enqueue-time backfill paused (shard closing); resumes on next open"
                ),
                Err(e) => warn!(
                    shard = %shard.name,
                    error = %e,
                    "enqueue-time backfill failed; resumes from the persisted key on next open"
                ),
            }
        });
    }

    /// Run the sweep until the keyspace is exhausted or the shard's
    /// cancellation token fires, resuming from any persisted progress.
    /// Completion writes the marker and sets the in-memory flag.
    pub async fn run_enqueue_time_backfill(
        &self,
        range: &ShardRange,
    ) -> Result<EnqueueTimeBackfillOutcome, JobStoreShardError> {
        let batch_size = self.enqueue_time_backfill.batch_size.max(1);
        let pause = Duration::from_millis(self.enqueue_time_backfill.pause_ms);
        let keyspace_end = end_bound(&[prefix::JOB_STATUS]);
        let mut progress = self
            .enqueue_time_backfill_progress()
            .await?
            .unwrap_or_default();

        loop {
            let start = match &progress.last_key {
                Some(last_key) => {
                    let mut key = hex::decode(last_key).map_err(|e| {
                        JobStoreShardError::Codec(format!("backfill progress key: {e}"))
                    })?;
                    key.push(0);
                    key
                }
                None => vec![prefix::JOB_STATUS],
            };
            let batch_end = self
                .enqueue_time_backfill_batch(
                    range,
                    start,
                    keyspace_end.clone(),
                    batch_size,
                    &mut progress,
                )
                .await?;
            let Some(last_key) = batch_end else {
                self.save_enqueue_time_backfill_progress(&progress).await?;
                self.set_enqueue_time_backfill_complete(true).await?;
                return Ok(EnqueueTimeBackfillOutcome {
                    progress,
                    complete: true,
                });
            };
            progress.last_key = Some(hex::encode(&last_key));
            self.save_enqueue_time_backfill_progress(&progress).await?;
            debug!(
                shard = %self.name,
                scanned = progress.scanned,
                repaired = progress.repaired,
                "enqueue-time backfill batch checkpoint"
            );
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => {
                    return Ok(EnqueueTimeBackfillOutcome { progress, complete: false });
                }
                _ = tokio::time::sleep(pause) => {}
            }
        }
    }

    /// The persisted sweep progress, or `None` if the sweep has not run on
    /// this shard.
    pub async fn enqueue_time_backfill_progress(
        &self,
    ) -> Result<Option<EnqueueTimeBackfillProgress>, JobStoreShardError> {
        match self.db.get(&enqueue_time_backfill_progress_key()).await? {
            Some(raw) => Ok(Some(serde_json::from_slice(&raw)?)),
            None => Ok(None),
        }
    }

    async fn save_enqueue_time_backfill_progress(
        &self,
        progress: &EnqueueTimeBackfillProgress,
    ) -> Result<(), JobStoreShardError> {
        let data = serde_json::to_vec(progress)?;
        self.db
            .put(&enqueue_time_backfill_progress_key(), &data)
            .await?;
        Ok(())
    }

    /// Examine up to `batch_size` status records from `start`. Returns the
    /// last key examined, or `None` when the keyspace ended before the batch
    /// filled. Iterates the database directly with uncached scan options so
    /// the sweep's reads never register as query scanned keys.
    async fn enqueue_time_backfill_batch(
        &self,
        range: &ShardRange,
        start: Vec<u8>,
        end: Vec<u8>,
        batch_size: usize,
        progress: &mut EnqueueTimeBackfillProgress,
    ) -> Result<Option<Vec<u8>>, JobStoreShardError> {
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options_uncached())
            .await?;
        let mut last_key = None;
        for _ in 0..batch_size {
            let Some(kv) = iter.next().await? else {
                return Ok(None);
            };
            last_key = Some(kv.key.to_vec());
            let Some(parsed) = parse_job_status_key(&kv.key) else {
                continue;
            };
            if !range.contains_tenant(&parsed.tenant) {
                continue;
            }
            progress.scanned += 1;
            self.backfill_status_row(
                &parsed.tenant,
                &parsed.job_id,
                &kv.key,
                &kv.value,
                kv.expire_ts,
                progress,
            )
            .await?;
        }
        Ok(last_key)
    }

    /// Rewrite one status record and its index entry with `enqueue_time_ms`
    /// when the record lacks it. The row keeps `expire_ts` exactly. A row that
    /// changed since the scan, or whose rewrite conflicts with a concurrent
    /// transition, is skipped: that transition wrote the value itself.
    async fn backfill_status_row(
        &self,
        tenant: &str,
        job_id: &str,
        status_key: &Bytes,
        status_raw: &Bytes,
        expire_ts: Option<i64>,
        progress: &mut EnqueueTimeBackfillProgress,
    ) -> Result<(), JobStoreShardError> {
        let status = match decode_job_status_owned(status_raw) {
            Ok(status) => status,
            Err(e) => {
                warn!(
                    shard = %self.name,
                    tenant = %tenant,
                    job_id = %job_id,
                    error = %e,
                    "enqueue-time backfill: failed to decode job status, skipping row"
                );
                return Ok(());
            }
        };
        if status.enqueue_time_ms.is_some() {
            return Ok(());
        }

        if let Some(m) = &self.metrics {
            m.record_enqueue_time_repair_reads(&self.name, 1);
        }
        let Some(job_raw) = self.db.get(&job_info_key(tenant, job_id)).await? else {
            progress.skipped_missing_job_info += 1;
            return Ok(());
        };
        let enqueue_time_ms = JobView::new(job_raw)?.enqueue_time_ms();

        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        if txn.get(status_key).await?.as_ref() != Some(status_raw) {
            progress.skipped_conflict += 1;
            return Ok(());
        }
        Self::write_job_status_row(
            &mut TxnWriter(&txn),
            tenant,
            job_id,
            &status.with_enqueue_time_ms(enqueue_time_ms),
            expire_ts,
        )?;
        match txn.commit().await {
            Ok(_) => progress.repaired += 1,
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => {
                progress.skipped_conflict += 1;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}
