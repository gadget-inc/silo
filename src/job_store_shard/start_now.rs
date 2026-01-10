//! Start next attempt now operations.
//!
//! Allows a scheduled job's next attempt to be started immediately,
//! bypassing any scheduled delay (e.g., for future-scheduled jobs or retry backoff).

use slatedb::DbIterator;
use slatedb::WriteBatch;

use crate::codec::{decode_task, encode_task};
use crate::job::JobStatusKind;
use crate::job_store_shard::helpers::{decode_job_status_owned, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{job_cancelled_key, job_status_key, task_key};
use crate::task::Task;
use tracing::debug;

/// Error returned when a job's attempt cannot be started now.
#[derive(Debug, Clone)]
pub struct JobNotStartableError {
    pub job_id: String,
    pub status: JobStatusKind,
    pub reason: String,
}

impl std::fmt::Display for JobNotStartableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot start job {} now: {} (status: {:?})",
            self.job_id, self.reason, self.status
        )
    }
}

impl std::error::Error for JobNotStartableError {}

impl JobStoreShard {
    /// Start the next attempt for a scheduled job immediately.
    ///
    /// This operation moves a scheduled task to run immediately by updating its
    /// start time in the task queue. Use this when:
    /// - A job is scheduled to run in the future and you want it to run now
    /// - A job has failed and is waiting for retry backoff, but you want to retry immediately
    ///
    /// Per Alloy spec:
    /// - [SILO-START-NOW-1] Pre: job exists
    /// - [SILO-START-NOW-2] Pre: job is Scheduled (either initial or after retry)
    /// - [SILO-START-NOW-3] Pre: job is NOT cancelled
    /// - [SILO-START-NOW-4] Pre: task exists in DB queue for this job (waiting to run)
    /// - [SILO-START-NOW-5] Pre: task is NOT already in buffer (not already ready)
    /// - [SILO-START-NOW-6] Post: task added to buffer (immediately available for dequeue)
    ///
    /// Note: [SILO-START-NOW-5] is not strictly enforced here - if the task is already
    /// ready (start_time <= now), the operation is idempotent and will succeed.
    /// The task will be moved to start_time = now either way.
    pub async fn start_next_attempt_now(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // [SILO-START-NOW-1] Pre: job must exist
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = self.db.get(status_key.as_bytes()).await?;
        let Some(status_raw) = maybe_status_raw else {
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let status = decode_job_status_owned(&status_raw)?;

        // [SILO-START-NOW-2] Pre: job must be Scheduled
        if status.kind != JobStatusKind::Scheduled {
            return Err(JobStoreShardError::JobNotStartable(JobNotStartableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: format!(
                    "job must be in Scheduled status, currently {:?}",
                    status.kind
                ),
            }));
        }

        // [SILO-START-NOW-3] Pre: job must NOT be cancelled
        let cancelled_key = job_cancelled_key(tenant, id);
        if self.db.get(cancelled_key.as_bytes()).await?.is_some() {
            return Err(JobStoreShardError::JobNotStartable(JobNotStartableError {
                job_id: id.to_string(),
                status: status.kind,
                reason: "job is cancelled".to_string(),
            }));
        }

        // [SILO-START-NOW-4] Pre: find the task in the DB queue for this job
        // Tasks are keyed as tasks/{start_time_ms}/{priority}/{job_id}/{attempt}
        // We need to scan to find any task for this job_id
        let (old_task_key, task, priority, attempt, old_start_time) =
            self.find_task_for_job(id).await?.ok_or_else(|| {
                JobStoreShardError::JobNotStartable(JobNotStartableError {
                    job_id: id.to_string(),
                    status: status.kind,
                    reason: "no pending task found in queue".to_string(),
                })
            })?;

        // [SILO-START-NOW-5] If task is already ready (start_time <= now), skip the move
        // The task is already available for the broker to pick up
        if old_start_time <= now_ms {
            debug!(
                job_id = %id,
                old_key = %old_task_key,
                old_start_time = %old_start_time,
                "start_next_attempt_now: task already ready, no move needed"
            );
            // Still wake the broker to ensure it picks up the task promptly
            self.broker.wakeup();
            return Ok(());
        }

        // [SILO-START-NOW-6] Post: move task to buffer by updating its start time to now
        // Delete old task key and insert new one with start_time = now
        let new_task_key = task_key(now_ms, priority, id, attempt);

        let mut batch = WriteBatch::new();
        batch.delete(old_task_key.as_bytes());
        let task_value = encode_task(&task)?;
        batch.put(new_task_key.as_bytes(), &task_value);
        self.db.write(batch).await?;

        debug!(
            job_id = %id,
            old_key = %old_task_key,
            new_key = %new_task_key,
            "start_next_attempt_now: moved task to run immediately"
        );

        // Wake the broker to pick up the task promptly
        self.broker.wakeup();

        Ok(())
    }

    /// Find a pending task in the DB queue for the given job_id.
    /// Returns (task_key, task, priority, attempt_number, start_time_ms) if found.
    ///
    /// Handles all task types that represent pending work for a job:
    /// - RunAttempt: Direct execution task
    /// - RequestTicket: Local concurrency ticket request
    /// - RequestRemoteTicket: Remote concurrency ticket request
    /// - CheckRateLimit: Rate limit check before execution
    async fn find_task_for_job(
        &self,
        job_id: &str,
    ) -> Result<Option<(String, Task, u8, u32, i64)>, JobStoreShardError> {
        // Scan all tasks - they're ordered by time, then priority
        let start: Vec<u8> = b"tasks/".to_vec();
        let mut end: Vec<u8> = b"tasks/".to_vec();
        end.push(0xFF);

        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        while let Some(kv) = iter.next().await? {
            let key_str = String::from_utf8_lossy(&kv.key).to_string();

            // Parse key: tasks/{ts}/{pri}/{job_id}/{attempt}
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() < 5 || parts[0] != "tasks" {
                continue;
            }

            let key_job_id = parts[3];
            if key_job_id != job_id {
                continue;
            }

            // Found a task for this job - decode it
            let task = decode_task(&kv.value)?;

            // Parse start time and priority from key (same for all task types)
            let start_time: i64 = parts[1].parse().unwrap_or(0);
            let priority: u8 = parts[2].parse().unwrap_or(50);

            // Handle all task types that represent pending work for a job
            let attempt_number = match &task {
                Task::RunAttempt { attempt_number, .. } => *attempt_number,
                Task::RequestTicket { attempt_number, .. } => *attempt_number,
                Task::RequestRemoteTicket { attempt_number, .. } => *attempt_number,
                Task::CheckRateLimit { attempt_number, .. } => *attempt_number,
                // Skip internal/system tasks that don't represent pending work
                Task::NotifyRemoteTicketGrant { .. }
                | Task::ReleaseRemoteTicket { .. }
                | Task::RefreshFloatingLimit { .. } => continue,
            };

            return Ok(Some((key_str, task, priority, attempt_number, start_time)));
        }

        Ok(None)
    }
}
