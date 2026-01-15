//! Job cancellation operations.

use slatedb::ErrorKind as SlateErrorKind;
use slatedb::IsolationLevel;

use crate::codec::{decode_job_cancellation, encode_job_cancellation, encode_job_status};
use crate::job::{JobCancellation, JobStatus, JobStatusKind};
use crate::job_store_shard::helpers::{decode_job_status_owned, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{idx_status_time_key, job_cancelled_key, job_status_key};
use tracing::debug;

impl JobStoreShard {
    /// Cancel a job by id. Prevents further execution and signals running workers to stop.
    ///
    /// Cancellation semantics:
    /// - Cancellation is tracked separately from status for performance
    /// - For scheduled jobs: immediately removes from queue and sets status to Cancelled
    /// - For running jobs: sets cancellation flag; worker discovers on heartbeat
    /// - For terminal jobs: returns error (cannot cancel completed jobs)
    /// - Cancellation is monotonic: once cancelled, always cancelled
    ///
    /// Uses a transaction with optimistic concurrency control to detect if the job state
    /// changes during the cancellation flow. Retries automatically on conflict.
    pub async fn cancel_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            match self.cancel_job_inner(tenant, id).await {
                Ok(()) => return Ok(()),
                Err(JobStoreShardError::Slate(ref e))
                    if e.kind() == SlateErrorKind::Transaction =>
                {
                    // Transaction conflict - retry with exponential backoff
                    if attempt + 1 < MAX_RETRIES {
                        let delay_ms = 10 * (1 << attempt); // 10ms, 20ms, 40ms, 80ms
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        debug!(
                            job_id = %id,
                            attempt = attempt + 1,
                            "cancel_job transaction conflict, retrying"
                        );
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(JobStoreShardError::TransactionConflict(
            "cancel_job".to_string(),
        ))
    }

    /// Inner implementation of cancel_job that runs within a single transaction attempt.
    /// All business logic checks are performed within the transaction so they are re-evaluated
    /// on retry if the transaction conflicts.
    ///
    /// Note: We do NOT scan/delete tasks or requests here. Instead:
    /// - Tasks are cleaned up lazily when dequeued (we check cancellation flag)
    /// - Requests are skipped when granting (we check cancellation flag)
    ///
    /// This avoids O(n) scans on the tasks/requests namespaces.
    async fn cancel_job_inner(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-CXL-1] Pre: job must exist and not already be cancelled
        // Read status within transaction to detect concurrent modifications
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(status_key.as_bytes()).await?;
        let Some(status_raw) = maybe_status_raw else {
            // No status means job doesn't exist
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let status = decode_job_status_owned(&status_raw)?;

        // Check if already cancelled within transaction
        let cancelled_key = job_cancelled_key(tenant, id);
        let maybe_cancelled = txn.get(cancelled_key.as_bytes()).await?;
        if maybe_cancelled.is_some() {
            return Err(JobStoreShardError::JobAlreadyCancelled(id.to_string()));
        }

        // Cannot cancel jobs in terminal states (Succeeded/Failed are truly terminal)
        if status.kind == JobStatusKind::Succeeded || status.kind == JobStatusKind::Failed {
            return Err(JobStoreShardError::JobAlreadyTerminal(
                id.to_string(),
                status.kind,
            ));
        }

        // [SILO-CXL-2] Post: Mark job as cancelled (write cancellation record)
        let cancellation = JobCancellation {
            cancelled_at_ms: now_ms,
        };
        let cancellation_value = encode_job_cancellation(&cancellation)?;
        txn.put(cancelled_key.as_bytes(), &cancellation_value)?;

        // [SILO-CXL-3] For Scheduled jobs, update status to Cancelled immediately
        // Tasks/requests are NOT deleted here - they will be cleaned up lazily:
        // - On dequeue: cancelled tasks are skipped and deleted
        // - On grant: cancelled requests are skipped and deleted
        // For Running jobs, status stays Running - worker discovers on heartbeat
        if status.kind == JobStatusKind::Scheduled {
            // Delete old status index entry
            let old_time =
                idx_status_time_key(tenant, status.kind.as_str(), status.changed_at_ms, id);
            txn.delete(old_time.as_bytes())?;

            // Set status to Cancelled immediately since job never started
            let cancelled_status = JobStatus::cancelled(now_ms);
            let status_value = encode_job_status(&cancelled_status)?;
            txn.put(status_key.as_bytes(), &status_value)?;

            // Insert new status index entry
            let new_time = idx_status_time_key(
                tenant,
                cancelled_status.kind.as_str(),
                cancelled_status.changed_at_ms,
                id,
            );
            txn.put(new_time.as_bytes(), [])?;
        }

        // Commit the transaction - this will detect conflicts with concurrent modifications
        txn.commit().await?;

        // Record stats: if job was scheduled (pending), it's now removed from pending
        if status.kind == JobStatusKind::Scheduled {
            self.stats.record_pending_job_removed();
        }
        // Note: For running jobs, the stats will be updated when the worker reports
        // the cancellation via report_attempt_outcome

        Ok(())
    }

    /// Check if a job has been cancelled.
    /// Returns true if the job has a cancellation record, false otherwise.
    pub async fn is_job_cancelled(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<bool, JobStoreShardError> {
        let key = job_cancelled_key(tenant, id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        Ok(maybe_raw.is_some())
    }

    /// Get the cancellation timestamp if job was cancelled.
    /// Returns None if job was not cancelled.
    pub async fn get_job_cancellation(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<i64>, JobStoreShardError> {
        let key = job_cancelled_key(tenant, id);
        let Some(raw) = self.db.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        let decoded = decode_job_cancellation(&raw)?;
        Ok(Some(decoded.archived().cancelled_at_ms))
    }
}
