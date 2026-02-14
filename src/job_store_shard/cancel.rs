//! Job cancellation operations.

use slatedb::IsolationLevel;

use crate::codec::{decode_cancellation_at_ms, encode_job_cancellation};
use crate::job::{JobCancellation, JobStatus, JobStatusKind};
use crate::job_store_shard::helpers::{
    TxnWriter, decode_job_status_owned, now_epoch_ms, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{job_cancelled_key, job_status_key};

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
        retry_on_txn_conflict("cancel_job", || self.cancel_job_inner(tenant, id)).await
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
        let maybe_status_raw = txn.get(&status_key).await?;
        let Some(status_raw) = maybe_status_raw else {
            // No status means job doesn't exist
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let status = decode_job_status_owned(&status_raw)?;

        // Check if already cancelled within transaction
        let cancelled_key = job_cancelled_key(tenant, id);
        let maybe_cancelled = txn.get(&cancelled_key).await?;
        if maybe_cancelled.is_some() {
            return Err(JobStoreShardError::JobAlreadyCancelled(id.to_string()));
        }

        // Cannot cancel jobs in final states (Succeeded/Failed are truly terminal)
        if status.kind.is_final() {
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
        txn.put(&cancelled_key, &cancellation_value)?;

        // Track whether we're transitioning a scheduled job to terminal state
        let was_scheduled = status.kind == JobStatusKind::Scheduled;

        // [SILO-CXL-3] For Scheduled jobs, update status to Cancelled immediately
        // Tasks/requests are NOT deleted here - they will be cleaned up lazily:
        // - On dequeue: cancelled tasks are skipped and deleted
        // - On grant: cancelled requests are skipped and deleted
        // For Running jobs, status stays Running - worker discovers on heartbeat
        if was_scheduled {
            let cancelled_status = JobStatus::cancelled(now_ms);
            self.set_job_status_with_index(&mut TxnWriter(&txn), tenant, id, cancelled_status)
                .await?;
        }

        // Include counter in the transaction (unmark_write excludes it from conflict detection)
        if was_scheduled {
            self.increment_completed_jobs_counter(&mut TxnWriter(&txn))?;
        }

        // Commit the transaction - this will detect conflicts with concurrent modifications
        txn.commit().await?;

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
        let maybe_raw = self.db.get(&key).await?;
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
        let Some(raw) = self.db.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(decode_cancellation_at_ms(&raw)?))
    }
}
