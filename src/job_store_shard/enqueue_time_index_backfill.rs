//! One-shot backfill of the enqueue-time index (`IDX_ENQUEUE_TIME`) for jobs
//! created before the index existed.
//!
//! The sweep walks `JOB_INFO` inside the shard's tenant range in small
//! batches. Enumeration runs outside any transaction (a transactional scan
//! would register the whole range in the read set and abort on every busy
//! tenant); each batch's writes then run in a serializable-snapshot
//! transaction that re-reads `JOB_INFO` and the entry per row, so a delete or
//! terminal transition that lands mid-batch wins. A conflicting batch is
//! retried and then split into per-row transactions, so a row that still has
//! `JOB_INFO` is never skipped: a transition with no retention TTL leaves the
//! entry untouched, so a skipped live job would be missing from the index for
//! good.
//!
//! Progress is checkpointed after every batch and the sweep resumes from that
//! key after a restart. A completion marker, mirrored by an in-memory flag,
//! records that the sweep never needs to run again and gates the query
//! engine's index-served listing path.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use slatedb::IsolationLevel;
use tracing::{debug, error, info, warn};

use crate::job::JobView;
use crate::job_store_shard::helpers::{
    is_txn_conflict, put_with_optional_expire, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, shard_name_jitter_ms};
use crate::keys::{
    end_bound, enqueue_time_index_backfill_complete_key, enqueue_time_index_backfill_progress_key,
    idx_enqueue_time_key, jobs_prefix, parse_job_info_key,
};

/// Upper bound on the deterministic delay before a shard's background sweep
/// starts, so shards opening together do not scan in lockstep.
const START_JITTER_MS: u64 = 1_000;

/// Longest pause between attempts when the background sweep fails with an
/// error other than a transaction conflict (an object-store hiccup, say).
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// Rows a batch may walk, as a multiple of its in-range batch size, before it
/// checkpoints even when most of them are outside the shard's range.
const WALK_MULTIPLIER: usize = 4;

/// Progress checkpoint persisted after every batch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct BackfillProgress {
    /// Last `JOB_INFO` key examined (in or out of range), hex-encoded; the
    /// next batch starts just after it.
    last_key: Option<String>,
    rows_scanned: u64,
    rows_written: u64,
}

/// Outcome of a run of the sweep.
#[derive(Debug, Clone)]
pub struct EnqueueTimeIndexBackfillResult {
    /// `JOB_INFO` rows inside the shard's range the sweep examined.
    pub rows_scanned: u64,
    /// Index entries the sweep wrote.
    pub rows_written: u64,
    /// Whether the completion marker is set.
    pub complete: bool,
    /// Whether the run stopped early because the shard is closing.
    pub cancelled: bool,
}

/// A `JOB_INFO` row the enumerating scan found inside the shard's range.
struct Candidate {
    key: Vec<u8>,
    tenant: String,
    job_id: String,
}

/// One batch of the enumerating scan.
struct BackfillBatch {
    candidates: Vec<Candidate>,
    /// The last `JOB_INFO` key the scan examined, in or out of range; the
    /// next batch starts just after it.
    last_walked_key: Option<Vec<u8>>,
    /// Whether the scan reached the end of the `JOB_INFO` range.
    exhausted: bool,
}

impl JobStoreShard {
    /// Whether this shard's enqueue-time index covers every job, so the
    /// query engine may serve listings from it.
    pub fn enqueue_time_index_complete(&self) -> bool {
        self.enqueue_time_index_complete.load(Ordering::Acquire)
    }

    /// Set or clear the completion marker directly, bypassing the sweep.
    pub async fn set_enqueue_time_index_complete(
        &self,
        complete: bool,
    ) -> Result<(), JobStoreShardError> {
        let key = enqueue_time_index_backfill_complete_key();
        if complete {
            self.db.put(&key, []).await?;
        } else {
            self.db.delete(&key).await?;
        }
        self.enqueue_time_index_complete
            .store(complete, Ordering::Release);
        Ok(())
    }

    /// Load the persisted completion marker into the in-memory flag.
    pub(crate) async fn load_enqueue_time_index_complete(&self) -> Result<(), JobStoreShardError> {
        let complete = self
            .db
            .get(&enqueue_time_index_backfill_complete_key())
            .await?
            .is_some();
        self.enqueue_time_index_complete
            .store(complete, Ordering::Release);
        Ok(())
    }

    /// Run the sweep to completion (or until the shard starts closing),
    /// resuming from the persisted checkpoint. Returns immediately when the
    /// completion marker is already set.
    pub async fn backfill_enqueue_time_index(
        &self,
        batch_size: usize,
        pause: Duration,
    ) -> Result<EnqueueTimeIndexBackfillResult, JobStoreShardError> {
        let batch_size = batch_size.max(1);
        if self.enqueue_time_index_complete() {
            return Ok(Self::backfill_result(
                &BackfillProgress::default(),
                true,
                false,
            ));
        }
        let mut progress = self.load_backfill_progress().await?;

        info!(
            shard = %self.name,
            resuming_from = ?progress.last_key,
            "starting enqueue-time index backfill"
        );

        loop {
            if self.cancellation.is_cancelled() {
                info!(
                    shard = %self.name,
                    rows_scanned = progress.rows_scanned,
                    rows_written = progress.rows_written,
                    "enqueue-time index backfill cancelled (shard closing)"
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
                    self.write_backfill_batch(&batch.candidates).await?
                };

                progress.rows_scanned += batch.candidates.len() as u64;
                progress.rows_written += written;
                progress.last_key = Some(hex::encode(last_walked));
                self.save_backfill_progress(&progress).await?;
                debug!(
                    shard = %self.name,
                    rows_scanned = progress.rows_scanned,
                    rows_written = progress.rows_written,
                    "enqueue-time index backfill checkpoint"
                );
            }

            if batch.exhausted {
                break;
            }
            if !pause.is_zero() {
                tokio::select! {
                    biased;
                    _ = self.cancellation.cancelled() => {}
                    _ = tokio::time::sleep(pause) => {}
                }
            }
        }

        self.set_enqueue_time_index_complete(true).await?;
        self.db
            .delete(&enqueue_time_index_backfill_progress_key())
            .await?;
        info!(
            shard = %self.name,
            rows_scanned = progress.rows_scanned,
            rows_written = progress.rows_written,
            "enqueue-time index backfill complete"
        );
        Ok(Self::backfill_result(&progress, true, false))
    }

    /// Spawn the background sweep at shard open when it is enabled and has
    /// not completed. Starts after a shard-deterministic delay.
    pub(crate) fn spawn_enqueue_time_index_backfill(self: &Arc<Self>) {
        let cfg = self.enqueue_time_index_backfill.clone();
        if !cfg.enabled || self.enqueue_time_index_complete() {
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
                    .backfill_enqueue_time_index(
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
                        "enqueue-time index backfill failed; retrying from its checkpoint"
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
        *self
            .enqueue_time_index_backfill_task
            .try_lock()
            .expect("backfill task slot is uncontended at open") = Some(task);
    }

    fn backfill_result(
        progress: &BackfillProgress,
        complete: bool,
        cancelled: bool,
    ) -> EnqueueTimeIndexBackfillResult {
        EnqueueTimeIndexBackfillResult {
            rows_scanned: progress.rows_scanned,
            rows_written: progress.rows_written,
            complete,
            cancelled,
        }
    }

    /// Pull up to `batch_size` in-range `JOB_INFO` rows after the checkpoint,
    /// outside any transaction. The walk is also bounded by rows examined, so
    /// a long run of out-of-range rows (a split child before its cleanup)
    /// still checkpoints, pauses, and observes cancellation per batch.
    async fn enumerate_backfill_candidates(
        &self,
        progress: &BackfillProgress,
        batch_size: usize,
    ) -> Result<BackfillBatch, JobStoreShardError> {
        let start = match &progress.last_key {
            Some(last) => {
                let mut key = hex::decode(last).map_err(|e| {
                    JobStoreShardError::Codec(format!(
                        "enqueue-time index backfill checkpoint: {e}"
                    ))
                })?;
                // The smallest key strictly greater than the checkpoint.
                key.push(0x00);
                key
            }
            None => jobs_prefix(),
        };
        let end = end_bound(&jobs_prefix());

        let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
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
            let Some(parsed) = parse_job_info_key(&kv.key) else {
                warn!(
                    shard = %self.name,
                    key = %hex::encode(&kv.key),
                    "enqueue-time index backfill: unparseable JOB_INFO key"
                );
                continue;
            };
            if !self.range.contains_tenant(&parsed.tenant) {
                continue;
            }
            batch.candidates.push(Candidate {
                key: kv.key.to_vec(),
                tenant: parsed.tenant,
                job_id: parsed.job_id,
            });
        }
        Ok(batch)
    }

    /// Write the batch's missing entries: the whole batch in one transaction,
    /// retried once on conflict, then one transaction per row with backoff.
    async fn write_backfill_batch(
        &self,
        candidates: &[Candidate],
    ) -> Result<u64, JobStoreShardError> {
        for _ in 0..2 {
            match self.write_backfill_txn(candidates).await {
                Err(e) if is_txn_conflict(&e) => debug!(
                    shard = %self.name,
                    rows = candidates.len(),
                    "enqueue-time index backfill batch conflicted, retrying"
                ),
                result => return result,
            }
        }

        let mut written = 0;
        for candidate in candidates {
            written += retry_on_txn_conflict("enqueue_time_index_backfill_row", || {
                self.write_backfill_txn(std::slice::from_ref(candidate))
            })
            .await?;
        }
        Ok(written)
    }

    /// One serializable-snapshot transaction over `candidates`: re-read each
    /// job's `JOB_INFO` (skipping rows deleted since enumeration) and its
    /// entry (skipping rows already indexed), then write the missing entries
    /// carrying the job's current `expire_ts`.
    async fn write_backfill_txn(
        &self,
        candidates: &[Candidate],
    ) -> Result<u64, JobStoreShardError> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mut written = 0;
        for candidate in candidates {
            let Some(info) = txn.get_key_value(&candidate.key).await? else {
                continue;
            };
            let view = match JobView::new(info.value) {
                Ok(view) => view,
                Err(e) => {
                    warn!(
                        shard = %self.name,
                        tenant = %candidate.tenant,
                        job_id = %candidate.job_id,
                        error = %e,
                        "enqueue-time index backfill: undecodable JOB_INFO, skipping"
                    );
                    continue;
                }
            };
            let entry_key =
                idx_enqueue_time_key(&candidate.tenant, view.enqueue_time_ms(), &candidate.job_id);
            if txn.get(&entry_key).await?.is_some() {
                continue;
            }
            put_with_optional_expire(&txn, &entry_key, [], info.expire_ts)?;
            written += 1;
        }
        if written > 0 {
            txn.commit().await?;
        }
        Ok(written)
    }

    async fn load_backfill_progress(&self) -> Result<BackfillProgress, JobStoreShardError> {
        match self
            .db
            .get(&enqueue_time_index_backfill_progress_key())
            .await?
        {
            Some(data) => Ok(serde_json::from_slice(&data)?),
            None => Ok(BackfillProgress::default()),
        }
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
