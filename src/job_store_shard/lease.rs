//! Lease management: heartbeat, outcome reporting, and expired lease reaping.

use slatedb::{DbIterator, WriteBatch};
use uuid::Uuid;

use crate::codec::{
    decode_floating_limit_state, decode_lease, encode_attempt, encode_floating_limit_state,
    encode_lease,
};
use crate::concurrency::MemoryEvent;
use crate::job::{FloatingLimitState, JobStatus, JobView};
use crate::job_attempt::{AttemptOutcome, AttemptStatus, JobAttempt};
use crate::job_store_shard::helpers::{now_epoch_ms, put_task};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{attempt_key, floating_limit_state_key, job_info_key, leased_task_key};
use crate::task::{HeartbeatResult, LeaseRecord, Task, DEFAULT_LEASE_MS};
use tracing::{debug, info_span};

impl JobStoreShard {
    /// Heartbeat a lease to renew it if the worker id matches. Bumps expiry by DEFAULT_LEASE_MS.
    ///
    /// [SILO-HB-3]: Heartbeat ALWAYS renews the lease, even for cancelled jobs.
    /// Worker discovers cancellation from the heartbeat response
    /// Worker can keep heartbeating during graceful shutdown until they report completion.
    pub async fn heartbeat_task(
        &self,
        tenant: &str,
        worker_id: &str,
        task_id: &str,
    ) -> Result<HeartbeatResult, JobStoreShardError> {
        // [SILO-HB-2] Directly read the lease for this task id
        let key = leased_task_key(task_id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes)?;

        // [SILO-HB-1] Check worker id matches
        let current_owner = decoded.worker_id();
        if current_owner != worker_id {
            return Err(JobStoreShardError::LeaseOwnerMismatch {
                task_id: task_id.to_string(),
                expected: current_owner.to_string(),
                got: worker_id.to_string(),
            });
        }

        // Extract job_id for cancellation checking (use helper method)
        let job_id = decoded.job_id().to_string();

        // [SILO-HB-3] Renew by creating new record with updated expiry
        // Note: to_task() allocates, but we need owned Task for LeaseRecord
        let record = LeaseRecord {
            worker_id: current_owner.to_string(),
            task: decoded.to_task(),
            expiry_ms: now_epoch_ms() + DEFAULT_LEASE_MS,
        };
        let value = encode_lease(&record)?;

        let mut batch = WriteBatch::new();
        batch.put(key.as_bytes(), &value);
        self.db.write(batch).await?;
        self.db.flush().await?;

        // Check cancellation status to return in response
        // Worker discovers cancellation via heartbeat response per Alloy spec
        let cancelled_at_ms = self.get_job_cancellation(tenant, &job_id).await?;

        Ok(HeartbeatResult {
            cancelled: cancelled_at_ms.is_some(),
            cancelled_at_ms,
        })
    }

    /// Report the outcome of a running attempt identified by task id.
    /// Removes the lease and finalizes the attempt state.
    pub async fn report_attempt_outcome(
        &self,
        tenant: &str,
        task_id: &str,
        outcome: AttemptOutcome,
    ) -> Result<(), JobStoreShardError> {
        // [SILO-SUCC-1][SILO-FAIL-1][SILO-RETRY-1] Load lease; must exist
        let leased_task_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(leased_task_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes)?;

        // Extract fields using zero-copy accessors
        let job_id = decoded.job_id().to_string();
        let attempt_number = decoded.attempt_number();
        let held_queues_local = decoded.held_queues(); // Only allocation needed

        let now_ms = now_epoch_ms();
        let attempt_status = match &outcome {
            AttemptOutcome::Success { result } => AttemptStatus::Succeeded {
                finished_at_ms: now_ms,
                result: result.clone(),
            },
            AttemptOutcome::Error { error_code, error } => AttemptStatus::Failed {
                finished_at_ms: now_ms,
                error_code: error_code.clone(),
                error: error.clone(),
            },
            AttemptOutcome::Cancelled => AttemptStatus::Cancelled {
                finished_at_ms: now_ms,
            },
        };
        let attempt = JobAttempt {
            job_id: job_id.clone(),
            attempt_number,
            task_id: task_id.to_string(),
            status: attempt_status,
        };
        let attempt_val = encode_attempt(&attempt)?;
        let attempt_key = attempt_key(tenant, &job_id, attempt_number);

        // Atomically update attempt and remove lease
        let mut batch = WriteBatch::new();
        // [SILO-SUCC-4][SILO-FAIL-4][SILO-RETRY-4] Update attempt status
        batch.put(attempt_key.as_bytes(), &attempt_val);
        // [SILO-SUCC-2][SILO-FAIL-2][SILO-RETRY-2] Release lease
        batch.delete(leased_task_key.as_bytes());

        let mut job_missing_error: Option<JobStoreShardError> = None;
        let mut followup_next_time: Option<i64> = None;

        match &outcome {
            // [SILO-SUCC-3] If success: mark job succeeded now (pure write)
            AttemptOutcome::Success { .. } => {
                let job_status = JobStatus::succeeded(now_ms);
                self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                    .await?;
            }
            // Worker acknowledges cancellation - set job status to Cancelled
            AttemptOutcome::Cancelled => {
                let job_status = JobStatus::cancelled(now_ms);
                self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                    .await?;
            }
            // Error: maybe enqueue next attempt; otherwise mark job failed
            AttemptOutcome::Error { .. } => {
                let mut scheduled_followup: bool = false;

                // Load job info to get priority and retry policy
                let job_info_key = job_info_key(tenant, &job_id);
                let maybe_job = self.db.get(job_info_key.as_bytes()).await?;
                if let Some(jbytes) = maybe_job {
                    let view = JobView::new(jbytes)?;
                    let priority = view.priority();
                    let failures_so_far = attempt_number;
                    if let Some(policy_rt) = view.retry_policy() {
                        if let Some(next_time) =
                            crate::retry::next_retry_time_ms(now_ms, failures_so_far, &policy_rt)
                        {
                            // [SILO-RETRY-5] Enqueue new task to DB queue for retry
                            let next_attempt_number = attempt_number + 1;
                            let next_task = Task::RunAttempt {
                                id: Uuid::new_v4().to_string(),
                                tenant: tenant.to_string(),
                                job_id: job_id.clone(),
                                attempt_number: next_attempt_number,
                                held_queues: held_queues_local.clone(),
                            };
                            put_task(
                                &mut batch,
                                next_time,
                                priority,
                                &job_id,
                                next_attempt_number,
                                &next_task,
                            )?;

                            // [SILO-RETRY-3] Set job status to Scheduled with next attempt time
                            let job_status = JobStatus::scheduled(now_ms, next_time);
                            self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                                .await?;
                            scheduled_followup = true;
                            followup_next_time = Some(next_time);
                        }
                    }
                    // [SILO-FAIL-3] If no follow-up scheduled, mark job as failed (pure write)
                    if !scheduled_followup {
                        let job_status = JobStatus::failed(now_ms);
                        self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                            .await?;
                    }
                } else {
                    job_missing_error = Some(JobStoreShardError::JobNotFound(job_id.clone()));
                }
            }
        }

        // [SILO-REL-1] Release any held concurrency tickets
        // This also handles [SILO-GRANT-*] granting to next waiting request
        let release_events: Vec<MemoryEvent> = self
            .concurrency
            .release_and_grant_next(
                &self.db,
                &mut batch,
                tenant,
                &held_queues_local,
                task_id,
                now_ms,
            )
            .await
            .map_err(JobStoreShardError::Rkyv)?;

        self.db.write(batch).await?;
        self.db.flush().await?;
        // Update in-memory broker counts after durable release and emit spans for release/grant
        for ev in release_events.into_iter() {
            match ev {
                MemoryEvent::Released {
                    queue,
                    task_id: tid,
                } => {
                    let span =
                        info_span!("concurrency.release", queue = %queue, finished_task_id = %tid);
                    let _g = span.enter();
                    debug!("released ticket for finished task");
                    self.concurrency
                        .counts()
                        .record_release(tenant, &queue, &tid);
                    // Wake broker; durable grant-from-release already enqueues run task if ready
                    self.broker.wakeup();
                }
                MemoryEvent::Granted { queue, task_id } => {
                    // We granted on release: bump in-memory counts now and wake the broker to scan promptly.
                    self.concurrency
                        .counts()
                        .record_grant(tenant, &queue, &task_id);
                    let span = info_span!("task.enqueue_from_grant", queue = %queue, task_id = %task_id, cause = "release");
                    let _g = span.enter();
                    debug!("enqueued task for next requester after release");
                    self.broker.wakeup();
                }
            }
        }
        // If we scheduled a follow-up that is ready now, wake the scanner
        if let Some(nt) = followup_next_time {
            if nt <= now_epoch_ms() {
                self.broker.wakeup();
            }
        }
        if let Some(err) = job_missing_error {
            return Err(err);
        }

        // Record stats based on outcome
        match &outcome {
            AttemptOutcome::Success { .. } | AttemptOutcome::Cancelled => {
                // Job completed (running -> terminal)
                self.stats.record_job_completed();
            }
            AttemptOutcome::Error { .. } => {
                if followup_next_time.is_some() {
                    // Job is being retried (running -> pending)
                    self.stats.record_job_requeued();
                } else {
                    // Job failed permanently (running -> terminal)
                    self.stats.record_job_completed();
                }
            }
        }

        tracing::debug!(task_id = %task_id, "report_attempt_outcome: completed");
        Ok(())
    }

    /// Scan all held leases and mark any expired ones as failed with a WORKER_CRASHED error code, or as Cancelled if the job was cancelled.
    /// For RefreshFloatingLimit tasks, resets the floating limit state so it can be retried on next periodic refresh.
    /// Returns the number of expired leases reaped.
    pub async fn reap_expired_leases(&self, tenant: &str) -> Result<usize, JobStoreShardError> {
        // Scan leases under lease/
        let start: Vec<u8> = b"lease/".to_vec();
        let mut end: Vec<u8> = b"lease/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        let now_ms = now_epoch_ms();
        let mut reaped: usize = 0;

        while let Some(kv) = iter.next().await? {
            let key_str = String::from_utf8_lossy(&kv.key);
            if !key_str.starts_with("lease/") {
                continue;
            }

            let decoded = match decode_lease(&kv.value) {
                Ok(l) => l,
                Err(_) => continue,
            };

            // [SILO-REAP-1] Pre: Lease exists (we found it)
            // [SILO-REAP-2] Pre: Check if lease has expired
            if decoded.expiry_ms() > now_ms {
                continue;
            }

            // Handle RefreshFloatingLimit tasks separately
            if let Some((task_id, queue_key)) = decoded.refresh_floating_limit_info() {
                let task_tenant = decoded.tenant();
                let _ = self
                    .reap_expired_refresh_task(task_tenant, task_id, queue_key, &decoded)
                    .await;
                reaped += 1;
                continue;
            }

            // Get task_id and job_id using helper methods (for RunAttempt tasks)
            let Some(task_id) = decoded.task_id() else {
                continue; // Not a RunAttempt lease
            };
            let job_id = decoded.job_id().to_string();

            // Check if job was cancelled - if so, report Cancelled instead of WORKER_CRASHED
            let was_cancelled = self
                .is_job_cancelled(tenant, &job_id)
                .await
                .unwrap_or(false);

            let outcome = if was_cancelled {
                // Job was cancelled - report as Cancelled (clean termination)
                AttemptOutcome::Cancelled
            } else {
                // [SILO-REAP-3][SILO-REAP-4] Report as worker crashed
                // SILO-REAP-3: Post: Set job status to Failed (via report_attempt_outcome)
                // SILO-REAP-4: Post: Set attempt status to AttemptFailed
                AttemptOutcome::Error {
                    error_code: "WORKER_CRASHED".to_string(),
                    error: format!(
                        "lease expired at {} (now {}), worker={}",
                        decoded.expiry_ms(),
                        now_ms,
                        decoded.worker_id()
                    )
                    .into_bytes(),
                }
            };

            // [SILO-REAP-REL] Release lease and update job/attempt status via report_attempt_outcome
            let _ = self.report_attempt_outcome(tenant, task_id, outcome).await;
            reaped += 1;
        }

        Ok(reaped)
    }

    /// Handle expiry of a RefreshFloatingLimit task lease.
    /// Resets the floating limit state so a new refresh can be scheduled on the next enqueue/dequeue.
    async fn reap_expired_refresh_task(
        &self,
        tenant: &str,
        task_id: &str,
        queue_key: &str,
        decoded: &crate::codec::DecodedLease,
    ) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();
        let lease_key = leased_task_key(task_id);
        let state_key = floating_limit_state_key(tenant, queue_key);

        // Load the floating limit state
        let maybe_state = self.db.get(state_key.as_bytes()).await?;
        let Some(raw) = maybe_state else {
            // State doesn't exist, just delete the orphaned lease
            tracing::warn!(
                tenant = %tenant,
                queue_key = %queue_key,
                task_id = %task_id,
                "refresh task lease expired but floating limit state not found, deleting orphaned lease"
            );
            let mut batch = WriteBatch::new();
            batch.delete(lease_key.as_bytes());
            self.db.write(batch).await?;
            self.db.flush().await?;
            return Ok(());
        };

        let decoded_state = decode_floating_limit_state(&raw)?;
        let archived = decoded_state.archived();

        // Reset the state to allow a new refresh to be scheduled
        // We don't increment retry_count here - we rely on the normal periodic refresh mechanism
        let new_state = FloatingLimitState {
            refresh_task_scheduled: false, // Allow new refresh to be scheduled
            // Preserve all other fields
            current_max_concurrency: archived.current_max_concurrency,
            last_refreshed_at_ms: archived.last_refreshed_at_ms,
            refresh_interval_ms: archived.refresh_interval_ms,
            default_max_concurrency: archived.default_max_concurrency,
            retry_count: archived.retry_count,
            next_retry_at_ms: archived.next_retry_at_ms.as_ref().copied(),
            metadata: archived
                .metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect(),
        };

        let mut batch = WriteBatch::new();
        let state_value = encode_floating_limit_state(&new_state)?;
        batch.put(state_key.as_bytes(), &state_value);
        batch.delete(lease_key.as_bytes());

        self.db.write(batch).await?;
        self.db.flush().await?;

        tracing::error!(
            tenant = %tenant,
            queue_key = %queue_key,
            task_id = %task_id,
            worker_id = %decoded.worker_id(),
            expiry_ms = decoded.expiry_ms(),
            now_ms = now_ms,
            "floating limit refresh task lease expired, reset state to allow re-scheduling"
        );

        Ok(())
    }
}
