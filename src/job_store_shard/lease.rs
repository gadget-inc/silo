//! Lease management: heartbeat, outcome reporting, and expired lease reaping.

use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use uuid::Uuid;

use crate::codec::{
    decode_floating_limit_state, decode_holder_granted_at_ms, decode_lease, encode_attempt,
    encode_floating_limit_state, encode_lease,
};
use crate::dst_events::{self, DstEvent};
use crate::job::{FloatingLimitState, JobStatus, JobView};
use crate::job_attempt::{AttemptOutcome, AttemptStatus, JobAttempt};
use crate::job_store_shard::helpers::{DbWriteBatcher, WriteBatcher, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, LimitTaskParams};
use crate::keys::{
    attempt_key, attempt_prefix, concurrency_holder_key, concurrency_holders_prefix,
    concurrency_holders_tenant_prefix, end_bound, floating_limit_state_key, idx_metadata_key,
    job_cancelled_key, job_info_key, leased_task_key, parse_concurrency_holder_key,
};
use crate::task::{DEFAULT_LEASE_MS, HeartbeatResult};
use tracing::{debug, info_span};

impl JobStoreShard {
    /// Heartbeat a lease to renew it if the worker id matches. Bumps expiry by DEFAULT_LEASE_MS.
    ///
    /// [SILO-HB-3]: Heartbeat ALWAYS renews the lease, even for cancelled jobs.
    /// Worker discovers cancellation from the heartbeat response
    /// Worker can keep heartbeating during graceful shutdown until they report completion.
    pub async fn heartbeat_task(
        &self,
        worker_id: &str,
        task_id: &str,
    ) -> Result<HeartbeatResult, JobStoreShardError> {
        // [SILO-HB-2] Directly read the lease for this task id
        let key = leased_task_key(task_id);
        let maybe_raw = self.db.get(&key).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(value_bytes)?;

        // [SILO-HB-1] Check worker id matches
        let current_owner = decoded.worker_id();
        if current_owner != worker_id {
            return Err(JobStoreShardError::LeaseOwnerMismatch {
                task_id: task_id.to_string(),
                expected: current_owner.to_string(),
                got: worker_id.to_string(),
            });
        }

        // Extract tenant and job_id from the lease (authoritative source)
        let tenant = decoded.tenant().to_string();
        let job_id = decoded.job_id().to_string();

        // [SILO-HB-3] Renew by creating new record with updated expiry
        let new_expiry_ms = now_epoch_ms() + DEFAULT_LEASE_MS;
        let task = decoded.to_task()?;
        let record = crate::task::LeaseRecord {
            worker_id: worker_id.to_string(),
            task,
            expiry_ms: new_expiry_ms,
            started_at_ms: decoded.started_at_ms(),
        };
        let value = encode_lease(&record);

        let mut batch = WriteBatch::new();
        batch.put(&key, &value);
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;

        // Check cancellation status to return in response
        // Worker discovers cancellation via heartbeat response per Alloy spec
        let cancelled_at_ms = self.get_job_cancellation(&tenant, &job_id).await?;

        Ok(HeartbeatResult {
            cancelled: cancelled_at_ms.is_some(),
            cancelled_at_ms,
        })
    }

    /// Report the outcome of a running attempt identified by task id.
    /// Removes the lease and finalizes the attempt state.
    pub async fn report_attempt_outcome(
        &self,
        task_id: &str,
        outcome: AttemptOutcome,
    ) -> Result<(), JobStoreShardError> {
        // [SILO-SUCC-1][SILO-FAIL-1][SILO-RETRY-1] Load lease; must exist
        let leased_task_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(&leased_task_key).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(value_bytes)?;

        // Extract fields using zero-copy accessors
        let tenant = decoded.tenant().to_string();
        let job_id = decoded.job_id().to_string();
        let attempt_number = decoded.attempt_number();
        let relative_attempt_number = decoded.relative_attempt_number();
        let held_queues_local = decoded.held_queues(); // Only allocation needed
        let task_group = decoded.task_group().to_string();

        // Decrement active lease counter
        if let Some(ref m) = self.metrics {
            m.dec_task_leases_active(&self.name, &task_group);
        }

        let now_ms = now_epoch_ms();
        // The row-TTL deadline for terminal records depends on which terminal
        // status the job reaches — `completed_job_expire_s` for Succeeded,
        // `terminal_job_expire_s` for Failed/Cancelled. We resolve it inside
        // each terminal branch below via `self.terminal_expire_ts(kind, now_ms)`.
        let mut terminal_expire_ts: Option<i64> = None;
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
        let attempt_key = attempt_key(&tenant, &job_id, attempt_number);

        // started_at_ms is stored on the lease record, so no extra DB read needed
        let started_at_ms = decoded.started_at_ms();

        let attempt = JobAttempt {
            job_id: job_id.clone(),
            attempt_number,
            relative_attempt_number,
            task_id: task_id.to_string(),
            started_at_ms,
            status: attempt_status,
        };
        let attempt_val = encode_attempt(&attempt);

        // Atomically update attempt and remove lease.
        // Note: the attempt put is deferred until after we determine whether the
        // job reached a terminal status, so that we can apply the row TTL only
        // to attempts of terminal jobs.
        let mut batch = WriteBatch::new();
        // [SILO-SUCC-2][SILO-FAIL-2][SILO-RETRY-2] Release lease
        batch.delete(&leased_task_key);

        let mut job_missing_error: Option<JobStoreShardError> = None;
        let mut followup_next_time: Option<i64> = None;
        // Track grants from retry scheduling for rollback if DB write fails
        let mut retry_grants: Vec<(String, String)> = Vec::new();
        // Track the new job status for DST event emission
        let mut new_job_status_for_dst: Option<String> = None;
        // Whether the job reached a terminal status during this call. Drives
        // both the `expire_terminal_job_records` invocation and the TTL on the
        // new attempt row written below.
        let mut reached_terminal = false;

        match &outcome {
            // [SILO-SUCC-3] If success: mark job succeeded now (pure write)
            AttemptOutcome::Success { .. } => {
                let job_status = JobStatus::succeeded(now_ms);
                terminal_expire_ts =
                    self.terminal_expire_ts(crate::job::JobStatusKind::Succeeded, now_ms);
                self.set_job_status_with_index_opts(
                    &mut DbWriteBatcher::new(&self.db, &mut batch),
                    &tenant,
                    &job_id,
                    job_status,
                    terminal_expire_ts,
                )
                .await?;
                // Job reached terminal state - include counter in batch
                self.increment_completed_jobs_counter(&mut DbWriteBatcher::new(
                    &self.db, &mut batch,
                ))?;
                new_job_status_for_dst = Some("Succeeded".to_string());
                reached_terminal = true;
            }
            // Worker acknowledges cancellation - set job status to Cancelled
            AttemptOutcome::Cancelled => {
                let job_status = JobStatus::cancelled(now_ms);
                terminal_expire_ts =
                    self.terminal_expire_ts(crate::job::JobStatusKind::Cancelled, now_ms);
                self.set_job_status_with_index_opts(
                    &mut DbWriteBatcher::new(&self.db, &mut batch),
                    &tenant,
                    &job_id,
                    job_status,
                    terminal_expire_ts,
                )
                .await?;
                // Job reached terminal state - include counter in batch
                self.increment_completed_jobs_counter(&mut DbWriteBatcher::new(
                    &self.db, &mut batch,
                ))?;
                new_job_status_for_dst = Some("Cancelled".to_string());
                reached_terminal = true;
            }
            // Error: maybe enqueue next attempt; otherwise mark job failed
            AttemptOutcome::Error { .. } => {
                let mut scheduled_followup: bool = false;

                // Load job info to get priority and retry policy
                let job_info_key = job_info_key(&tenant, &job_id);
                let maybe_job = self.db.get(&job_info_key).await?;
                if let Some(jbytes) = maybe_job {
                    let view = JobView::new(jbytes)?;
                    let priority = view.priority();
                    let task_group = view.task_group();
                    let limits = view.limits();
                    // Use relative_attempt_number for retry delay calculation
                    // This ensures retry schedule resets after a job restart
                    let failures_so_far = relative_attempt_number;
                    if let Some(policy_rt) = view.retry_policy()
                        && let Some(next_time) =
                            crate::retry::next_retry_time_ms(now_ms, failures_so_far, &policy_rt)
                    {
                        // [SILO-RETRY-5] Enqueue new task to DB queue for retry
                        // [SILO-RETRY-5-CONC] Retry task starts with no held queues, so it must
                        // re-acquire any concurrency tickets. Combined with [SILO-RETRY-REL] releasing
                        // the current task's tickets below, this allows other jobs to run during
                        // the retry backoff period.
                        let next_attempt_number = attempt_number + 1;
                        let next_relative_attempt_number = relative_attempt_number + 1;
                        let next_task_id = Uuid::new_v4().to_string();

                        // [SILO-RETRY-5-CONC] Enqueue retry with skip_try_reserve=true.
                        // The old holder is still in-memory (released post-commit), so the
                        // retry must go through the request queue, not get an immediate grant.
                        retry_grants = self
                            .enqueue_limit_task_at_index(
                                &mut DbWriteBatcher::new(&self.db, &mut batch),
                                LimitTaskParams {
                                    tenant: &tenant,
                                    task_id: &next_task_id,
                                    job_id: &job_id,
                                    attempt_number: next_attempt_number,
                                    relative_attempt_number: next_relative_attempt_number,
                                    limit_index: 0,
                                    limits: &limits,
                                    priority,
                                    // Retry: the user-requested next-run time
                                    // is `next_time`; the task_key uses the
                                    // same so the broker picks it up exactly
                                    // then. `next_attempt_number` already
                                    // differentiates this from the prior
                                    // attempt's task_key, so no tombstone
                                    // collision is possible here.
                                    scheduled_at_ms: next_time,
                                    task_key_start_ms: next_time,
                                    now_ms,
                                    held_queues: Vec::new(),
                                    task_group,
                                    skip_try_reserve: true,
                                },
                            )
                            .await?;

                        // [SILO-RETRY-3] Set job status to Scheduled with next attempt time
                        let job_status =
                            JobStatus::scheduled(now_ms, next_time, next_attempt_number);
                        self.set_job_status_with_index(
                            &mut DbWriteBatcher::new(&self.db, &mut batch),
                            &tenant,
                            &job_id,
                            job_status,
                        )
                        .await?;
                        scheduled_followup = true;
                        followup_next_time = Some(next_time);
                        new_job_status_for_dst = Some("Scheduled".to_string());
                    }
                    // [SILO-FAIL-3] If no follow-up scheduled, mark job as failed (pure write)
                    if !scheduled_followup {
                        let job_status = JobStatus::failed(now_ms);
                        terminal_expire_ts =
                            self.terminal_expire_ts(crate::job::JobStatusKind::Failed, now_ms);
                        self.set_job_status_with_index_opts(
                            &mut DbWriteBatcher::new(&self.db, &mut batch),
                            &tenant,
                            &job_id,
                            job_status,
                            terminal_expire_ts,
                        )
                        .await?;
                        // Job reached terminal state (failed permanently) - include counter in batch
                        self.increment_completed_jobs_counter(&mut DbWriteBatcher::new(
                            &self.db, &mut batch,
                        ))?;
                        new_job_status_for_dst = Some("Failed".to_string());
                        reached_terminal = true;
                    }
                } else {
                    job_missing_error = Some(JobStoreShardError::JobNotFound(job_id.clone()));
                }
            }
        }

        // Re-put all of the job's *prior* associated records (JOB_INFO,
        // IDX_METADATA, earlier ATTEMPT rows, JOB_CANCELLED) with the TTL.
        //
        // Ordering note: this MUST happen before we write the new ATTEMPT row
        // for the current attempt. The helper scans `attempt_prefix` from
        // `self.db` (which reflects committed state, pre-batch) — so the row
        // for the current attempt comes back with its old Running status, and
        // would clobber a freshly-batched terminal put on the same key
        // because WriteBatch is last-write-wins per key. Writing the new
        // attempt afterwards lets it win.
        //
        // Concurrency note: another writer cannot mutate this job's
        // attempt rows in parallel — the lease for the running attempt was
        // just deleted earlier in this batch, no other attempt for this
        // job_id can be Running, and the job's terminal status will block
        // dequeue / restart / reimport, so the scan sees a stable view.
        if reached_terminal && let Some(ts) = terminal_expire_ts {
            self.expire_terminal_job_records(
                &mut DbWriteBatcher::new(&self.db, &mut batch),
                &tenant,
                &job_id,
                ts,
            )
            .await?;
        }

        // [SILO-SUCC-4][SILO-FAIL-4][SILO-RETRY-4] Update attempt status.
        // Apply TTL to terminal-job attempts so they expire alongside the rest of
        // the job's records. The retry-scheduling branch leaves the attempt
        // without a TTL — it'll be picked up by the helper's scan when the job
        // ultimately hits a terminal status.
        match (reached_terminal, terminal_expire_ts) {
            (true, Some(ts)) => {
                use slatedb::config::{PutOptions, Ttl};
                batch.put_with_options(
                    &attempt_key,
                    &attempt_val,
                    &PutOptions {
                        ttl: Ttl::ExpireAt(ts),
                    },
                );
            }
            _ => batch.put(&attempt_key, &attempt_val),
        }

        // [SILO-REL-1][SILO-RETRY-REL] Delete concurrency holders in the batch.
        // In-memory release happens post-commit via atomic_release.
        for queue in &held_queues_local {
            batch.delete(concurrency_holder_key(&tenant, queue, task_id));
        }

        // Two-phase DST events: emit before write for correct causal ordering,
        // confirm after write succeeds, cancel if write fails.
        let write_op = dst_events::next_write_op();
        dst_events::emit_pending(
            DstEvent::TaskReleased {
                tenant: tenant.to_string(),
                job_id: job_id.clone(),
                task_id: task_id.to_string(),
            },
            write_op,
        );
        if let Some(new_status) = new_job_status_for_dst {
            dst_events::emit_pending(
                DstEvent::JobStatusChanged {
                    tenant: tenant.to_string(),
                    job_id: job_id.clone(),
                    new_status,
                },
                write_op,
            );
        }

        // Commit durable state — write_with_options with await_durable:true blocks
        // until the WAL is flushed to object storage, so no separate flush is needed.
        if let Err(e) = self
            .db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            dst_events::cancel_write(write_op);
            // Rollback any grants made during retry scheduling
            for (queue, grant_task_id) in &retry_grants {
                self.concurrency
                    .rollback_grant(&tenant, queue, grant_task_id);
            }
            return Err(e.into());
        }
        dst_events::confirm_write(write_op);

        // Post-commit: release in-memory concurrency counts and signal grant scanner.
        for queue in &held_queues_local {
            let span = info_span!(
                "concurrency.release",
                queue = %queue,
                finished_task_id = %task_id
            );
            let _g = span.enter();
            debug!("released ticket for finished task");

            self.concurrency
                .counts()
                .atomic_release(&tenant, queue, task_id);
            self.concurrency.request_grant(&tenant, queue);
        }
        // If we scheduled a follow-up that is ready now, wake the scanner
        if let Some(nt) = followup_next_time
            && nt <= now_epoch_ms()
        {
            self.brokers.wakeup(&task_group);
        }
        if let Some(err) = job_missing_error {
            return Err(err);
        }
        tracing::debug!(task_id = %task_id, "report_attempt_outcome: completed");
        Ok(())
    }

    /// Scan all held leases and mark any expired ones as failed with a WORKER_CRASHED error code, or as Cancelled if the job was cancelled.
    /// For RefreshFloatingLimit tasks, resets the floating limit state so it can be retried on next periodic refresh.
    /// Skips and deletes leases for tenants outside the shard's range.
    /// Returns the number of expired leases reaped.
    pub async fn reap_expired_leases(&self, tenant: &str) -> Result<usize, JobStoreShardError> {
        // Scan all lease keys using the binary prefix
        let start = crate::keys::leases_prefix();
        let end = crate::keys::end_bound(&start);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await?;

        let now_ms = now_epoch_ms();
        let mut reaped: usize = 0;

        // Get the shard range for split-aware filtering
        let shard_range = self.get_range();

        // Collect defunct lease keys to delete
        let mut defunct_keys: Vec<Vec<u8>> = Vec::new();

        while let Some(kv) = iter.next().await? {
            let decoded = match decode_lease(kv.value.clone()) {
                Ok(l) => l,
                Err(_) => continue,
            };

            // Check if lease's tenant is within shard range
            let lease_tenant = decoded.tenant();

            if !shard_range.contains_tenant(lease_tenant) {
                // Lease is for a tenant outside our range - mark for deletion
                debug!(
                    key = ?kv.key,
                    tenant = %lease_tenant,
                    range = %shard_range,
                    "deleting defunct lease (tenant outside shard range)"
                );
                defunct_keys.push(kv.key.to_vec());
                continue;
            }

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
            let _ = self.report_attempt_outcome(task_id, outcome).await;
            reaped += 1;
        }

        // Delete defunct leases from the database
        if !defunct_keys.is_empty() {
            let mut batch = WriteBatch::new();
            for key in &defunct_keys {
                batch.delete(key);
            }
            if let Err(e) = self.db.write(batch).await {
                debug!(error = %e, count = defunct_keys.len(), "failed to delete defunct leases");
            } else {
                debug!(
                    count = defunct_keys.len(),
                    "deleted defunct leases outside shard range"
                );
            }
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
        let maybe_state = self.db.get(&state_key).await?;
        let Some(raw) = maybe_state else {
            // State doesn't exist, just delete the orphaned lease
            tracing::warn!(
                tenant = %tenant,
                queue_key = %queue_key,
                task_id = %task_id,
                "refresh task lease expired but floating limit state not found, deleting orphaned lease"
            );
            let mut batch = WriteBatch::new();
            batch.delete(&lease_key);
            self.db.write(batch).await?;
            return Ok(());
        };

        let decoded_state = decode_floating_limit_state(raw)?;

        // Reset the state to allow a new refresh to be scheduled
        // We don't increment retry_count here - we rely on the normal periodic refresh mechanism
        let new_state = FloatingLimitState {
            refresh_task_scheduled: false,
            ..decoded_state.to_owned()
        };

        let mut batch = WriteBatch::new();
        let state_value = encode_floating_limit_state(&new_state);
        batch.put(&state_key, &state_value);
        batch.delete(&lease_key);

        self.db.write(batch).await?;

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

    /// Defensive cleanup: scan tenant-scoped concurrency holders and delete any
    /// rows attributed to the given `task_id`, releasing the in-memory slot for
    /// each. Used as a self-healing path when a worker reports outcome for a
    /// task whose lease has already been reaped — if the reaper somehow failed
    /// to release the holder atomically, this brings the system back to a
    /// consistent state.
    ///
    /// Returns the number of holder rows deleted. Idempotent: zero deletions on
    /// repeated calls.
    pub async fn purge_orphaned_holders_for_task(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> Result<usize, JobStoreShardError> {
        let start = concurrency_holders_tenant_prefix(tenant);
        let end = end_bound(&start);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await?;

        let mut to_delete: Vec<(Vec<u8>, String)> = Vec::new();
        while let Some(kv) = iter.next().await? {
            let Some(parsed) = parse_concurrency_holder_key(&kv.key) else {
                continue;
            };
            if parsed.task_id == task_id {
                to_delete.push((kv.key.to_vec(), parsed.queue));
            }
        }

        if to_delete.is_empty() {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        for (key, _) in &to_delete {
            batch.delete(key);
        }
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;

        // Post-commit: drop the in-memory reservation and wake the grant scanner
        // so a queued requester can be admitted in place of the orphan.
        for (_, queue) in &to_delete {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
            tracing::warn!(
                tenant = %tenant,
                queue = %queue,
                task_id = %task_id,
                "purged orphaned concurrency holder during late report_outcome cleanup"
            );
        }

        Ok(to_delete.len())
    }

    /// Scan all concurrency holders and release any orphaned ones: a holder
    /// whose `task_id` has no lease and whose grant is older than `grace_ms`.
    /// Releases via the full path (delete row + `atomic_release` +
    /// `request_grant`). Skips tenants outside the shard range (split-aware).
    /// Returns the number released.
    ///
    /// The invariant this repairs: every holder's `task_id` must have either a
    /// lease (`leased_task_key`) or a still-queued `RunAttempt`. When a worker
    /// pulls a `RunAttempt` into its in-flight buffer and its dequeue/lease
    /// future is dropped **before a lease is durably written**, the holder was
    /// already granted but the task is gone and no lease ever exists — so
    /// neither the lease reaper (iterates leases) nor a late `report_outcome`
    /// (`purge_orphaned_holders_for_task`, fires only for that exact `task_id`)
    /// will ever reclaim it. The in-memory count stays pegged and, once ghost
    /// holders fill the cap, the queue wedges. Nothing else iterates holders;
    /// this does.
    ///
    /// Scan-then-point-get ordering avoids false positives: if a worker leases
    /// a holder between the grace check and the point-get, the lease is
    /// observed and the holder is skipped. The grace window covers holders
    /// granted after the scan began. Holders are bounded (~Σ concurrency caps),
    /// so a full `0x09` scan is cheap — comparable to the lease scan.
    ///
    /// At most `max_per_tick` holders are released per call; the scan stops
    /// collecting once the cap is reached and any remainder is reclaimed on the
    /// next tick. This bounds the durable delete batch and the post-commit
    /// release loop so a shard with a large orphan backlog drains gradually
    /// instead of in one giant write.
    pub async fn reap_orphaned_holders(
        &self,
        grace_ms: i64,
        max_per_tick: usize,
    ) -> Result<usize, JobStoreShardError> {
        let start = concurrency_holders_prefix();
        let end = end_bound(&start);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await?;

        let now_ms = now_epoch_ms();
        let shard_range = self.get_range();

        // (key, tenant, queue, task_id) for each orphaned holder found.
        let mut to_delete: Vec<(Vec<u8>, String, String, String)> = Vec::new();

        while let Some(kv) = iter.next().await? {
            let Some(parsed) = parse_concurrency_holder_key(&kv.key) else {
                continue;
            };

            // Split-aware: skip holders for tenants outside this shard's range.
            if !shard_range.contains_tenant(&parsed.tenant) {
                continue;
            }

            // Skip holders younger than the grace window — guards the normal
            // grant→lease handoff and freshly granted holders whose RunAttempt
            // is still legitimately queued.
            let granted_at_ms = match decode_holder_granted_at_ms(&kv.value) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if now_ms - granted_at_ms < grace_ms {
                continue;
            }

            // Healthy if a lease exists for this task_id (the lease reaper owns
            // it). The point-get runs after the grace check, so a lease written
            // concurrently is observed here and the holder is skipped.
            if self
                .db
                .get(&leased_task_key(&parsed.task_id))
                .await?
                .is_some()
            {
                continue;
            }

            to_delete.push((kv.key.to_vec(), parsed.tenant, parsed.queue, parsed.task_id));

            // Cap the per-tick batch: stop collecting once we hit the limit and
            // let the next tick reclaim the rest. Bounds the durable write and
            // the post-commit release loop under a large orphan backlog.
            if to_delete.len() >= max_per_tick {
                tracing::warn!(
                    max_per_tick = max_per_tick,
                    "orphaned holder reaper hit per-tick cap; remaining orphans \
                     will be reclaimed on subsequent ticks"
                );
                break;
            }
        }
        drop(iter);

        if to_delete.is_empty() {
            return Ok(0);
        }

        // Durable batch-delete all orphaned holder rows before touching
        // in-memory state, mirroring report_attempt_outcome and
        // purge_orphaned_holders_for_task.
        let mut batch = WriteBatch::new();
        for (key, _, _, _) in &to_delete {
            batch.delete(key);
        }
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;

        // Post-commit: drop the in-memory reservation and wake the grant
        // scanner so queued requesters can be admitted in the orphans' place. A
        // raw delete that skipped this would leave the count pegged and the
        // queue wedged until the next rehydrate.
        //
        // `request_grant` is per-holder, but it coalesces by (tenant, queue)
        // into a single pending-grant entry whose accumulated count the scanner
        // drains in one `process_grants` pass — so even a queue with hundreds of
        // orphans triggers one grant scan, not one per holder. We collapse the
        // warn log the same way: one line per queue, not per holder.
        let mut reaped_per_queue: std::collections::HashMap<(&str, &str), usize> =
            std::collections::HashMap::new();
        for (_, tenant, queue, task_id) in &to_delete {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
            *reaped_per_queue
                .entry((tenant.as_str(), queue.as_str()))
                .or_default() += 1;
        }
        for ((tenant, queue), reaped) in reaped_per_queue {
            tracing::warn!(
                tenant = %tenant,
                queue = %queue,
                reaped = reaped,
                grace_ms = grace_ms,
                "reaped orphaned concurrency holders"
            );
        }

        Ok(to_delete.len())
    }

    /// Re-put all KV records associated with a job that has reached a terminal
    /// status, applying a SlateDB row TTL of `expire_ts` (epoch ms) so the
    /// rows are dropped during compaction once they age past `expire_ts`.
    ///
    /// Records covered:
    ///   - `JOB_INFO`            (read existing bytes, re-put with TTL)
    ///   - `IDX_METADATA`        (one entry per metadata pair on the job)
    ///   - `ATTEMPT`             (scan all attempt rows for the job, re-put each)
    ///   - `JOB_CANCELLED`       (re-put with TTL if present)
    ///
    /// `JOB_STATUS` and `IDX_STATUS_TIME` are written with TTL by the caller via
    /// `set_job_status_with_index_opts`, so they are intentionally not handled here.
    ///
    /// All reads go through `writer` so that when the caller uses a
    /// `TxnWriter`, the JOB_INFO / ATTEMPT range / JOB_CANCELLED reads are
    /// tracked in the transaction's SSI read set. A concurrent writer that
    /// mutates any of those keys between this helper and the caller's commit
    /// will trip a read-write (or phantom-read) conflict, so the re-put
    /// cannot silently clobber a concurrent write with stale bytes.
    ///
    /// The attempt scan returns the *committed* (pre-batch) attempt rows. For
    /// the current attempt this means the old Running value, so the caller
    /// MUST put the new terminal ATTEMPT row **after** invoking this helper.
    /// `WriteBatch` is last-write-wins per key, and putting the new row
    /// earlier would be silently clobbered when the helper re-puts the old
    /// Running value with TTL.
    pub(crate) async fn expire_terminal_job_records<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        expire_ts: i64,
    ) -> Result<(), JobStoreShardError> {
        let info_key = job_info_key(tenant, job_id);
        let metadata_pairs = if let Some(info_raw) = writer.get(&info_key).await? {
            let view = JobView::new(info_raw.clone())?;
            let pairs = view.metadata();
            writer.put_with_expire(&info_key, info_raw, expire_ts)?;
            pairs
        } else {
            Vec::new()
        };

        for (mk, mv) in &metadata_pairs {
            let mkey = idx_metadata_key(tenant, mk, mv, job_id);
            writer.put_with_expire(&mkey, [], expire_ts)?;
        }

        let attempt_start = attempt_prefix(tenant, job_id);
        let mut iter = writer.scan_prefix(&attempt_start).await?;
        while let Some(kv) = iter.next().await? {
            writer.put_with_expire(&kv.key, &kv.value, expire_ts)?;
        }

        let cancelled_key = job_cancelled_key(tenant, job_id);
        if let Some(cancelled_raw) = writer.get(&cancelled_key).await? {
            writer.put_with_expire(&cancelled_key, cancelled_raw, expire_ts)?;
        }

        Ok(())
    }
}
