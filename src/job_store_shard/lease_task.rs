//! Lease a specific job's task directly, putting it into Running state.
//!
//! This is a test-oriented helper that bypasses concurrency/rate-limit
//! processing and creates a RunAttempt lease directly, similar to how
//! `expedite_job` bypasses the normal scheduling path.

use slatedb::IsolationLevel;
use slatedb::config::WriteOptions;
use uuid::Uuid;

use crate::codec::{decode_task, encode_attempt, encode_lease};
use crate::job::{JobStatusKind, JobView};
use crate::job_attempt::{AttemptStatus, JobAttempt, JobAttemptView};
use crate::job_store_shard::helpers::{
    TxnWriter, decode_job_status_owned, now_epoch_ms, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    attempt_key, job_cancelled_key, job_info_key, job_status_key, leased_task_key, task_key,
};
use crate::task::{DEFAULT_LEASE_MS, LeaseRecord, LeasedTask, Task};

/// Error returned when a job cannot be leased because it's not in a leaseable state.
#[derive(Debug, Clone)]
pub struct JobNotLeaseableError {
    pub job_id: String,
    pub status: JobStatusKind,
    pub reason: String,
}

impl std::fmt::Display for JobNotLeaseableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot lease job {}: {} (status: {:?})",
            self.job_id, self.reason, self.status
        )
    }
}

impl std::error::Error for JobNotLeaseableError {}

impl JobStoreShard {
    /// Lease a specific job's task directly, putting it into Running state.
    ///
    /// This is a test-oriented helper that bypasses concurrency/rate-limit
    /// processing. For normal task processing, use `dequeue` via the
    /// `LeaseTasks` RPC instead.
    ///
    /// Uses a transaction with optimistic concurrency control to atomically
    /// read the job state, delete the pending task, create a lease, and
    /// update the status.
    pub async fn lease_task(
        &self,
        tenant: &str,
        id: &str,
        worker_id: &str,
    ) -> Result<LeasedTask, JobStoreShardError> {
        retry_on_txn_conflict("lease_task", || {
            self.lease_task_inner(tenant, id, worker_id)
        })
        .await
    }

    /// Inner implementation of lease_task that runs within a single transaction attempt.
    async fn lease_task_inner(
        &self,
        tenant: &str,
        id: &str,
        worker_id: &str,
    ) -> Result<LeasedTask, JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // Read job status - must exist
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(&status_key).await?;
        let Some(status_raw) = maybe_status_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };
        let status = decode_job_status_owned(&status_raw)?;

        // Validate: job must not be running
        if status.kind == JobStatusKind::Running {
            return Err(JobStoreShardError::JobNotLeaseable(JobNotLeaseableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job is currently running".to_string(),
            }));
        }

        // Validate: job must not be terminal
        if status.kind == JobStatusKind::Succeeded || status.kind == JobStatusKind::Failed {
            return Err(JobStoreShardError::JobNotLeaseable(JobNotLeaseableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job is already in terminal state".to_string(),
            }));
        }

        // Validate: job must not be cancelled
        let cancelled_key = job_cancelled_key(tenant, id);
        if txn.get(&cancelled_key).await?.is_some() {
            return Err(JobStoreShardError::JobNotLeaseable(JobNotLeaseableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job is cancelled".to_string(),
            }));
        }

        // Read job info for task_group, priority, payload, etc.
        let info_key = job_info_key(tenant, id);
        let maybe_job_raw = txn.get(&info_key).await?;
        let Some(job_raw) = maybe_job_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };
        let job_view = JobView::new(job_raw)?;
        let priority = job_view.priority();
        let task_group = job_view.task_group().to_string();

        // Reconstruct the task key from status fields
        let attempt_number = status.current_attempt.ok_or_else(|| {
            JobStoreShardError::JobNotLeaseable(JobNotLeaseableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job has no pending task".to_string(),
            })
        })?;
        let start_time_ms = status.next_attempt_starts_after_ms.ok_or_else(|| {
            JobStoreShardError::JobNotLeaseable(JobNotLeaseableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job has no pending task".to_string(),
            })
        })?;

        // Read the pending task via O(1) key reconstruction.
        // When start_at_ms <= 0 at enqueue time, the task key uses 0 as its time
        // component but the status stores the effective time (now_ms). Try the
        // status time first, then fall back to 0 to handle immediate jobs.
        let old_task_key = task_key(&task_group, start_time_ms, priority, id, attempt_number);
        let (old_task_key, task_raw) = match txn.get(&old_task_key).await? {
            Some(raw) => (old_task_key, raw),
            None => {
                // Fallback: try with time=0 (immediate scheduling case)
                let zero_key = task_key(&task_group, 0, priority, id, attempt_number);
                match txn.get(&zero_key).await? {
                    Some(raw) => (zero_key, raw),
                    None => {
                        return Err(JobStoreShardError::JobNotLeaseable(JobNotLeaseableError {
                            job_id: id.to_string(),
                            status: status.kind,
                            reason: "job has no pending task in queue".to_string(),
                        }));
                    }
                }
            }
        };
        // Validate that the raw bytes decode to a valid task before replacing it
        decode_task(&task_raw)?;

        // Delete the pending task (regardless of type)
        txn.delete(&old_task_key)?;

        // Generate new task_id and create RunAttempt task
        let new_task_id = Uuid::new_v4().to_string();
        let run_task = Task::RunAttempt {
            id: new_task_id.clone(),
            tenant: tenant.to_string(),
            job_id: id.to_string(),
            attempt_number,
            relative_attempt_number: attempt_number, // Same for non-restarted jobs
            held_queues: vec![],
            task_group: task_group.clone(),
        };

        // Write lease record
        let expiry_ms = now_ms + DEFAULT_LEASE_MS;
        let lease_key = leased_task_key(&new_task_id);
        let record = LeaseRecord {
            worker_id: worker_id.to_string(),
            task: run_task.clone(),
            expiry_ms,
            started_at_ms: now_ms,
        };
        let leased_value = encode_lease(&record)?;
        txn.put(&lease_key, &leased_value)?;

        // Write attempt record
        let attempt = JobAttempt {
            job_id: id.to_string(),
            attempt_number,
            relative_attempt_number: attempt_number,
            task_id: new_task_id,
            started_at_ms: now_ms,
            status: AttemptStatus::Running,
        };
        let attempt_val = encode_attempt(&attempt)?;
        let akey = attempt_key(tenant, id, attempt_number);
        txn.put(&akey, &attempt_val)?;

        // Update job status to Running
        let mut writer = TxnWriter(&txn);
        self.set_job_status_with_index(
            &mut writer,
            tenant,
            id,
            crate::job::JobStatus::running(now_ms),
        )
        .await?;

        // Commit the transaction with durable writes
        txn.commit_with_options(&WriteOptions {
            await_durable: true,
        })
        .await?;

        // Post-commit: evict old task key from broker buffer and wake broker
        self.broker.evict_keys(&[old_task_key]);
        self.broker.wakeup();

        // Build and return the LeasedTask
        let attempt_view = JobAttemptView::new(&attempt_val)?;
        Ok(LeasedTask::new(tenant.to_string(), job_view, attempt_view))
    }
}
