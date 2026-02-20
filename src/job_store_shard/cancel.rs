//! Job cancellation operations.

use slatedb::config::WriteOptions;
use slatedb::{DbIterator, IsolationLevel, WriteBatch};

use crate::codec::{decode_cancellation_at_ms, decode_task, encode_job_cancellation};
use crate::job::{JobCancellation, JobStatus, JobStatusKind, JobView, Limit};
use crate::job_store_shard::helpers::{
    TxnWriter, decode_job_status_owned, now_epoch_ms, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    concurrency_holder_key, concurrency_request_prefix, end_bound, job_cancelled_key, job_info_key,
    job_status_key, task_key,
};
use crate::task::Task;

impl JobStoreShard {
    /// Cancel a job by id. Prevents further execution and signals running workers to stop.
    ///
    /// Cancellation semantics:
    /// - Cancellation is tracked separately from status for performance
    /// - For scheduled jobs: immediately removes task from queue, releases concurrency,
    ///   and sets status to Cancelled
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
    /// [SILO-CXL-3] For scheduled jobs, eagerly deletes tasks and concurrency state:
    /// - Task key is deleted from the DB queue (O(1) key reconstruction from status fields)
    /// - RunAttempt tasks: concurrency holder keys are deleted
    /// - RequestTicket tasks: no additional cleanup needed (request is the task itself)
    /// - No-task case (TicketRequested): concurrency request records are scanned and deleted
    ///
    /// Post-commit, in-memory concurrency counts are updated and grant-next is triggered
    /// for any freed concurrency slots.
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
        let cancellation_value = encode_job_cancellation(&cancellation);
        txn.put(&cancelled_key, &cancellation_value)?;

        // Track whether we're transitioning a scheduled job to terminal state
        let was_scheduled = status.kind == JobStatusKind::Scheduled;

        // Track concurrency state for post-commit cleanup
        let mut held_queues_to_release: Vec<String> = Vec::new();
        let mut task_id_for_release: Option<String> = None;
        let mut deleted_task_key: Option<Vec<u8>> = None;

        // [SILO-CXL-3] For Scheduled jobs, eagerly delete task and concurrency state
        if was_scheduled {
            // Preserve scheduling fields in the cancelled status so task keys can be
            // reconstructed for reimport (O(1) key computation from status fields).
            let cancelled_status = JobStatus::new(
                JobStatusKind::Cancelled,
                now_ms,
                status.next_attempt_starts_after_ms,
                status.current_attempt,
            );
            self.set_job_status_with_index(&mut TxnWriter(&txn), tenant, id, cancelled_status)
                .await?;

            // Read job info to get task_group, priority, and limits for task key reconstruction
            let job_key = job_info_key(tenant, id);
            if let Some(job_bytes) = txn.get(&job_key).await? {
                let job_view = JobView::new(job_bytes)?;
                let task_group = job_view.task_group().to_string();
                let priority = job_view.priority();
                let limits = job_view.limits();

                // Try O(1) task key reconstruction from status fields
                let attempt_number = status.current_attempt.unwrap_or(1);
                let start_time_ms = status.next_attempt_starts_after_ms.unwrap_or(0);
                let computed_key =
                    task_key(&task_group, start_time_ms, priority, id, attempt_number);
                let task_found = match txn.get(&computed_key).await? {
                    Some(raw) => Some((computed_key, raw)),
                    None => {
                        // Fallback: try with time=0 (immediate scheduling case)
                        let zero_key = task_key(&task_group, 0, priority, id, attempt_number);
                        txn.get(&zero_key).await?.map(|raw| (zero_key, raw))
                    }
                };

                if let Some((found_key, task_raw)) = task_found {
                    let decoded = decode_task(&task_raw).map_err(|e| {
                        JobStoreShardError::Codec(format!("cancel task decode: {e}"))
                    })?;

                    // Delete the task from the DB queue
                    txn.delete(&found_key)?;
                    deleted_task_key = Some(found_key);

                    match decoded {
                        Task::RunAttempt {
                            id: tid,
                            held_queues,
                            ..
                        } => {
                            // Delete concurrency holders for each held queue
                            for queue in &held_queues {
                                txn.delete(concurrency_holder_key(tenant, queue, &tid))?;
                            }
                            if !held_queues.is_empty() {
                                task_id_for_release = Some(tid);
                                held_queues_to_release = held_queues;
                            }
                        }
                        Task::RequestTicket { .. } => {
                            // FutureRequestTaskWritten case: the RequestTicket task IS the
                            // pending request mechanism. Deleting the task is sufficient.
                        }
                        _ => {
                            // Other task types (CheckRateLimit, RefreshFloatingLimit, etc.)
                            // Just delete the task key above.
                        }
                    }
                } else {
                    // No task found - check for TicketRequested case (request record
                    // exists but no task in DB queue). Scan for requests for this job.
                    self.delete_concurrency_requests_for_job(&txn, tenant, id, &limits)
                        .await?;
                }
            }
        }
        // For Running jobs, status stays Running - worker discovers on heartbeat.
        // No task to delete (consumed by dequeue) and no concurrency cleanup needed
        // (holders released when worker completes).

        // Include counter in the transaction (unmark_write excludes it from conflict detection)
        if was_scheduled {
            self.increment_completed_jobs_counter(&mut TxnWriter(&txn))?;
        }

        // Commit the transaction - this will detect conflicts with concurrent modifications
        txn.commit().await?;

        // ---- Post-commit: update in-memory state ----

        // Evict the deleted task from the broker buffer
        if let Some(ref key) = deleted_task_key {
            self.broker.evict_keys(std::slice::from_ref(key));
        }

        // Release concurrency holders from in-memory counts and trigger grant-next
        if !held_queues_to_release.is_empty() {
            let finished_task_id = task_id_for_release.as_deref().unwrap_or_default();

            // Use a separate WriteBatch for grant-next (post-commit operation).
            // release_and_grant_next will: re-delete holder (idempotent), scan for next
            // request, grant if valid, and update in-memory counts atomically.
            let mut grant_batch = WriteBatch::new();
            let release_rollbacks = self
                .concurrency
                .release_and_grant_next(
                    &self.db,
                    &mut grant_batch,
                    tenant,
                    &held_queues_to_release,
                    finished_task_id,
                    now_ms,
                )
                .await
                .map_err(|e| JobStoreShardError::Codec(e.to_string()))?;

            if let Err(e) = self
                .db
                .write_with_options(
                    grant_batch,
                    &WriteOptions {
                        await_durable: true,
                    },
                )
                .await
            {
                // Rollback in-memory concurrency state on DB write failure
                self.concurrency.rollback_release_grants(&release_rollbacks);
                tracing::warn!(error = %e, "cancel: grant-next write failed, rolled back in-memory state");
                // Don't fail the cancel - the holder was already deleted in the txn.
                // The next release_and_grant_next call for this queue will grant correctly.
            } else {
                // Wake broker to pick up any newly granted tasks
                self.broker.wakeup();
            }
        }

        Ok(())
    }

    /// Delete concurrency request records for a specific job across all its concurrency queues.
    /// Used in the TicketRequested case where no task exists but request records do.
    async fn delete_concurrency_requests_for_job(
        &self,
        txn: &slatedb::DbTransaction,
        tenant: &str,
        job_id: &str,
        limits: &[Limit],
    ) -> Result<(), JobStoreShardError> {
        for limit in limits {
            let queue_key = match limit {
                Limit::Concurrency(cl) => &cl.key,
                Limit::FloatingConcurrency(fl) => &fl.key,
                Limit::RateLimit(_) => continue,
            };

            let prefix = concurrency_request_prefix(tenant, queue_key);
            let end = end_bound(&prefix);
            let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(prefix..end).await?;

            while let Some(kv) = iter.next().await? {
                // Decode the request value to check if it's for our job
                let decoded = crate::codec::decode_concurrency_action(kv.value.clone())
                    .map_err(|e| JobStoreShardError::Codec(e.to_string()))?;
                let action = decoded.fb();
                if let Some(et) = action.variant_as_enqueue_task()
                    && et.job_id().unwrap_or_default() == job_id
                {
                    txn.delete(&kv.key)?;
                    tracing::debug!(
                        job_id = %job_id,
                        queue = %queue_key,
                        "cancel: deleted concurrency request for cancelled job"
                    );
                }
            }
        }
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
