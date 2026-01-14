//! Job expedite operations.
//!
//! Allows future-scheduled jobs to be expedited to run immediately,
//! skipping the scheduled start time or retry backoff delay.

use slatedb::DbIterator;
use slatedb::ErrorKind as SlateErrorKind;
use slatedb::IsolationLevel;

use crate::codec::{decode_task, encode_task};
use crate::job::JobStatusKind;
use crate::job_store_shard::helpers::{decode_job_status_owned, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{job_cancelled_key, job_status_key, task_key};
use crate::task::Task;
use tracing::debug;

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
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            match self.expedite_job_inner(tenant, id).await {
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
                            "expedite_job transaction conflict, retrying"
                        );
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(JobStoreShardError::TransactionConflict(
            "expedite_job".to_string(),
        ))
    }

    /// Inner implementation of expedite_job that runs within a single transaction attempt.
    async fn expedite_job_inner(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-EXP-1] Pre: job must exist
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(status_key.as_bytes()).await?;
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
        if txn.get(cancelled_key.as_bytes()).await?.is_some() {
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

        // [SILO-EXP-4] Pre: task exists in DB queue for this job
        // Find the task for this job by scanning tasks/ namespace
        let (task_key_str, task, original_time_ms) =
            self.find_task_for_job_id(id).await?.ok_or_else(|| {
                // No task found - job has no pending task
                JobStoreShardError::JobNotExpediteable(JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "job has no pending task to expedite".to_string(),
                })
            })?;

        // [SILO-EXP-5] Check if task is future-scheduled (timestamp > now)
        // If task timestamp <= now, it's already ready to run (may be in buffer)
        if original_time_ms <= now_ms {
            return Err(JobStoreShardError::JobNotExpediteable(
                JobNotExpediteableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "task is already ready to run (not future-scheduled)".to_string(),
                },
            ));
        }

        // Extract task details to create new key
        let (priority, attempt_number) = match &task {
            Task::RunAttempt { attempt_number, .. } => {
                // Get priority from job info
                let job_info_key = crate::keys::job_info_key(tenant, id);
                let maybe_job_raw = txn.get(job_info_key.as_bytes()).await?;
                let Some(job_raw) = maybe_job_raw else {
                    return Err(JobStoreShardError::JobNotFound(id.to_string()));
                };
                let job_view = crate::job::JobView::new(job_raw)?;
                (job_view.priority(), *attempt_number)
            }
            _ => {
                // Not a RunAttempt task - this shouldn't happen for normal jobs
                return Err(JobStoreShardError::JobNotExpediteable(
                    JobNotExpediteableError {
                        job_id: id.to_string(),
                        status: status.kind,
                        reason: "task is not a RunAttempt task".to_string(),
                    },
                ));
            }
        };

        // Delete the old task key
        txn.delete(task_key_str.as_bytes())?;

        // Create new task key with current timestamp
        let new_task_key = task_key(now_ms, priority, id, attempt_number);
        let task_value = encode_task(&task)?;
        txn.put(new_task_key.as_bytes(), &task_value)?;

        // Commit the transaction
        txn.commit().await?;

        // Wake the broker to pick up the expedited task promptly
        self.broker.wakeup();

        Ok(())
    }

    /// Find a task for the given job_id in the tasks/ namespace.
    /// Returns (task_key, task, timestamp_ms) if found, None otherwise.
    async fn find_task_for_job_id(
        &self,
        job_id: &str,
    ) -> Result<Option<(String, Task, i64)>, JobStoreShardError> {
        // Scan all tasks looking for one that belongs to this job
        // Task keys are: tasks/<timestamp>/<priority>/<job_id>/<attempt>
        let start: Vec<u8> = b"tasks/".to_vec();
        let mut end: Vec<u8> = b"tasks/".to_vec();
        end.push(0xFF);

        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        while let Some(kv) = iter.next().await? {
            let key_str = match std::str::from_utf8(&kv.key) {
                Ok(s) => s.to_string(),
                Err(_) => continue,
            };

            // Parse task key: tasks/<timestamp>/<priority>/<job_id>/<attempt>
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() < 5 || parts[0] != "tasks" {
                continue;
            }

            let ts_str = parts[1];
            let task_job_id = parts[3];

            if task_job_id != job_id {
                continue;
            }

            // Found a task for this job
            // Parse timestamp explicitly as u64 to avoid i32 overflow, then convert to i64
            let timestamp_ms: i64 = ts_str.parse::<u64>().unwrap_or(0) as i64;
            let task = decode_task(&kv.value)?;

            return Ok(Some((key_str, task, timestamp_ms)));
        }

        Ok(None)
    }
}
