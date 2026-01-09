//! Job restart operations.
//!
//! Allows cancelled or failed jobs to be restarted, giving them a fresh set of retries.

use slatedb::ErrorKind as SlateErrorKind;
use slatedb::IsolationLevel;
use uuid::Uuid;

use crate::codec::encode_job_status;
use crate::job::{JobStatus, JobStatusKind};
use crate::job_store_shard::helpers::{decode_job_status_owned, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{idx_status_time_key, job_cancelled_key, job_status_key};
use crate::task::Task;
use tracing::debug;

/// Error returned when a job cannot be restarted because it's not in a restartable state.
#[derive(Debug, Clone)]
pub struct JobNotRestartableError {
    pub job_id: String,
    pub status: JobStatusKind,
    pub reason: String,
}

impl std::fmt::Display for JobNotRestartableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot restart job {}: {} (status: {:?})",
            self.job_id, self.reason, self.status
        )
    }
}

impl std::error::Error for JobNotRestartableError {}

impl JobStoreShard {
    /// Restart a cancelled or failed job, allowing it to be processed again.
    ///
    /// Restart semantics:
    /// - For cancelled jobs: clears cancellation flag and creates new task
    /// - For failed jobs: creates new task (job already has no cancellation flag)
    /// - Returns error for running, scheduled, or succeeded jobs
    /// - The job gets a fresh set of retries starting from attempt 1
    ///
    /// Per Alloy spec:
    /// - [SILO-RESTART-1] Pre: job exists and is in restartable state (Cancelled or Failed)
    /// - [SILO-RESTART-2] Pre: job is NOT Succeeded (truly terminal)
    /// - [SILO-RESTART-3] Pre: no active tasks for this job (checked via status)
    /// - [SILO-RESTART-4] Post: Clear cancellation record if present
    /// - [SILO-RESTART-5] Post: Create new task in DB queue with attempt 1
    /// - [SILO-RESTART-6] Post: Set status to Scheduled
    ///
    /// Uses a transaction with optimistic concurrency control to detect if the job state
    /// changes during the restart flow. Retries automatically on conflict.
    pub async fn restart_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            match self.restart_job_inner(tenant, id).await {
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
                            "restart_job transaction conflict, retrying"
                        );
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(JobStoreShardError::TransactionConflict(
            "restart_job".to_string(),
        ))
    }

    /// Inner implementation of restart_job that runs within a single transaction attempt.
    async fn restart_job_inner(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-RESTART-1] Pre: job must exist
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(status_key.as_bytes()).await?;
        let Some(status_raw) = maybe_status_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let status = decode_job_status_owned(&status_raw)?;

        // Check if job has a cancellation record
        let cancelled_key = job_cancelled_key(tenant, id);
        let has_cancellation = txn.get(cancelled_key.as_bytes()).await?.is_some();

        // [SILO-RESTART-1][SILO-RESTART-2] Pre: job must be in a restartable state
        // Only Cancelled or Failed jobs can be restarted
        match status.kind {
            JobStatusKind::Cancelled => {
                // Cancelled job - proceed with restart
            }
            JobStatusKind::Failed => {
                // Failed job - proceed with restart
            }
            JobStatusKind::Succeeded => {
                // [SILO-RESTART-2] Cannot restart succeeded jobs - they are truly terminal
                return Err(JobStoreShardError::JobNotRestartable(JobNotRestartableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job already succeeded".to_string(),
                }));
            }
            JobStatusKind::Running | JobStatusKind::Scheduled => {
                // [SILO-RESTART-3] Cannot restart jobs that are still in progress
                // These jobs may have active tasks/leases
                return Err(JobStoreShardError::JobNotRestartable(JobNotRestartableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job is still in progress".to_string(),
                }));
            }
        }

        // [SILO-RESTART-4] Post: Clear cancellation record if present
        if has_cancellation {
            txn.delete(cancelled_key.as_bytes())?;
        }

        // Delete old status index entry
        let old_time = idx_status_time_key(tenant, status.kind.as_str(), status.changed_at_ms, id);
        txn.delete(old_time.as_bytes())?;

        // [SILO-RESTART-6] Post: Set status to Scheduled
        let new_status = JobStatus::scheduled(now_ms);
        let status_value = encode_job_status(&new_status)?;
        txn.put(status_key.as_bytes(), &status_value)?;

        // Insert new status index entry
        let new_time = idx_status_time_key(
            tenant,
            new_status.kind.as_str(),
            new_status.changed_at_ms,
            id,
        );
        txn.put(new_time.as_bytes(), [])?;

        // [SILO-RESTART-5] Post: Create new task in DB queue with attempt 1 (fresh retries)
        // We need to get the job info to know the priority
        let job_info_key = crate::keys::job_info_key(tenant, id);
        let maybe_job_raw = txn.get(job_info_key.as_bytes()).await?;
        let Some(job_raw) = maybe_job_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let job_view = crate::job::JobView::new(job_raw)?;
        let priority = job_view.priority();
        let start_at_ms = job_view.enqueue_time_ms().max(now_ms);

        // Create a new task with attempt_number = 1 for fresh retry count
        let new_task_id = Uuid::new_v4().to_string();
        let new_task = Task::RunAttempt {
            id: new_task_id,
            tenant: tenant.to_string(),
            job_id: id.to_string(),
            attempt_number: 1, // Fresh start - retry counter reset
            held_queues: Vec::new(),
        };

        // Use a temporary WriteBatch to encode the task, then extract and put via txn
        let task_value = crate::codec::encode_task(&new_task)?;
        let task_key = crate::keys::task_key(start_at_ms, priority, id, 1);
        txn.put(task_key.as_bytes(), &task_value)?;

        // Commit the transaction
        txn.commit().await?;

        // Wake the broker to pick up the new task promptly
        if start_at_ms <= now_epoch_ms() {
            self.broker.wakeup();
        }

        Ok(())
    }
}
