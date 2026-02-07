//! Job expedite operations.
//!
//! Allows future-scheduled jobs to be expedited to run immediately,
//! skipping the scheduled start time or retry backoff delay.

use slatedb::IsolationLevel;

use crate::codec::{decode_task, encode_task};
use crate::job::JobStatusKind;
use crate::job_store_shard::helpers::{
    decode_job_status_owned, now_epoch_ms, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{job_cancelled_key, job_status_key, task_key};
use crate::task::Task;

/// Error returned when a job cannot be expedited because it's not in an expeditable state.
#[derive(Debug, Clone)]
pub struct JobNotExpediteableError {
    pub job_id: String,
    pub status: JobStatusKind,
    pub reason: String,
}

impl std::fmt::Display for JobNotExpediteableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot expedite job {}: {} (status: {:?})",
            self.job_id, self.reason, self.status
        )
    }
}

impl std::error::Error for JobNotExpediteableError {}

impl JobStoreShard {
    /// Expedite a future-scheduled job to run immediately.
    ///
    /// Expedite semantics:
    /// - Moves a future-scheduled task (start_at_ms > now) to run now
    /// - Also works for mid-retry jobs waiting for backoff delay
    /// - Returns error for running, cancelled, or terminal jobs
    /// - Returns error if the task is already ready to run (in buffer) or running (has lease)
    ///
    /// Uses a transaction with optimistic concurrency control to detect if the job state changes during the expedite flow. Retries automatically on conflict.
    pub async fn expedite_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        retry_on_txn_conflict("expedite_job", || self.expedite_job_inner(tenant, id)).await
    }

    /// Inner implementation of expedite_job that runs within a single transaction attempt.
    async fn expedite_job_inner(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-EXP-1] Pre: job must exist
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(&status_key).await?;
        let Some(status_raw) = maybe_status_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let status = decode_job_status_owned(&status_raw)?;

        // [SILO-EXP-2] Pre: job must NOT be terminal
        if status.kind == JobStatusKind::Succeeded || status.kind == JobStatusKind::Failed {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job is already in terminal state".to_string(),
                },
            ));
        }

        // [SILO-EXP-3] Pre: job must NOT be cancelled
        let cancelled_key = job_cancelled_key(tenant, id);
        if txn.get(&cancelled_key).await?.is_some() {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job is cancelled".to_string(),
                },
            ));
        }

        // [SILO-EXP-6] Pre: job has no active lease (not currently running)
        // If status is Running, there must be an active lease
        if status.kind == JobStatusKind::Running {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job is currently running".to_string(),
                },
            ));
        }

        // Load job info to get task_group and priority
        let job_info_key = crate::keys::job_info_key(tenant, id);
        let maybe_job_raw = txn.get(&job_info_key).await?;
        let Some(job_raw) = maybe_job_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };
        let job_view = crate::job::JobView::new(job_raw)?;
        let priority = job_view.priority();
        let task_group = job_view.task_group().to_string();

        // [SILO-EXP-4] Pre: task exists in DB queue for this job
        // O(1) direct key lookup using stored attempt info from JobStatus
        let attempt_number = status.current_attempt.ok_or_else(|| {
            JobStoreShardError::JobNotExpediteable(JobNotExpediteableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job has no pending task to expedite".to_string(),
            })
        })?;
        let start_time_ms = status.next_attempt_starts_after_ms.ok_or_else(|| {
            JobStoreShardError::JobNotExpediteable(JobNotExpediteableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job has no pending task to expedite".to_string(),
            })
        })?;

        let old_task_key = task_key(&task_group, start_time_ms, priority, id, attempt_number);
        let maybe_task_raw = txn.get(&old_task_key).await?;
        let Some(task_raw) = maybe_task_raw else {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job has no pending task to expedite".to_string(),
                },
            ));
        };
        let task = decode_task(&task_raw)?;

        // [SILO-EXP-5] Check if task is future-scheduled (timestamp > now)
        // If task timestamp <= now, it's already ready to run (may be in buffer)
        if start_time_ms <= now_ms {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "task is already ready to run (not future-scheduled)".to_string(),
                },
            ));
        }

        // RefreshFloatingLimit tasks cannot be expedited (they're internal system tasks)
        if matches!(task, Task::RefreshFloatingLimit { .. }) {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "cannot expedite internal refresh task".to_string(),
                },
            ));
        }

        // Delete the old task key
        txn.delete(&old_task_key)?;

        // Create new task key with current timestamp
        let new_task_key = task_key(&task_group, now_ms, priority, id, attempt_number);
        let task_value = encode_task(&task)?;
        txn.put(&new_task_key, &task_value)?;

        // Update job status with new start time (keeping same attempt number)
        let new_status = crate::job::JobStatus::scheduled(now_ms, now_ms, attempt_number);
        let new_status_value = crate::codec::encode_job_status(&new_status)?;
        txn.put(&status_key, &new_status_value)?;

        // Update status/time index
        let old_time_key = crate::keys::idx_status_time_key(
            tenant,
            status.kind.as_str(),
            status.changed_at_ms,
            id,
        );
        txn.delete(&old_time_key)?;
        let new_time_key = crate::keys::idx_status_time_key(
            tenant,
            new_status.kind.as_str(),
            new_status.changed_at_ms,
            id,
        );
        txn.put(&new_time_key, [])?;

        // Commit the transaction
        txn.commit().await?;

        // Wake the broker to pick up the expedited task promptly
        self.broker.wakeup();

        Ok(())
    }
}
