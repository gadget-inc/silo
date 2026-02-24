//! Task dequeue and processing operations.

use slatedb::config::WriteOptions;
use slatedb::{DbIterator, WriteBatch};

use crate::codec::{DecodedTask, decode_task, encode_attempt, encode_lease};
use crate::concurrency::RequestTicketTaskOutcome;
use crate::dst_events::{self, DstEvent};
use crate::fb::silo::fb;
use crate::job::{JobStatus, JobView};
use crate::job_attempt::{AttemptStatus, JobAttempt, JobAttemptView};
use crate::job_store_shard::helpers::{DbWriteBatcher, now_epoch_ms};
use crate::job_store_shard::{DequeueResult, JobStoreShard, JobStoreShardError, LimitTaskParams};
use crate::keys::{attempt_key, job_info_key, leased_task_key};
use crate::shard_range::ShardRange;
use crate::task::{DEFAULT_LEASE_MS, LeaseRecord, LeasedRefreshTask, LeasedTask, Task};
use crate::task_broker::BrokerTask;

/// Mutable accumulators for a single dequeue iteration.
/// Bundles state that each task-type handler needs to read and write,
/// keeping handler signatures clean.
struct DequeueIterationState {
    batch: WriteBatch,
    release_keys: Vec<Vec<u8>>,
    tombstone_keys: Vec<Vec<u8>>,
    grants_to_rollback: Vec<(String, String, String)>,
    leased_tasks_for_dst: Vec<(String, String, String)>,
    pending_attempts: Vec<(String, JobView, Vec<u8>)>,
    processed_internal: bool,
}

impl DequeueIterationState {
    fn new(claimed_len: usize) -> Self {
        Self {
            batch: WriteBatch::new(),
            release_keys: Vec::with_capacity(claimed_len),
            tombstone_keys: Vec::with_capacity(claimed_len),
            grants_to_rollback: Vec::new(),
            leased_tasks_for_dst: Vec::new(),
            pending_attempts: Vec::new(),
            processed_internal: false,
        }
    }

    fn ack_release(&mut self, key: &[u8]) {
        self.release_keys.push(key.to_vec());
    }

    fn ack_deleted(&mut self, key: &[u8]) {
        self.release_keys.push(key.to_vec());
        self.tombstone_keys.push(key.to_vec());
    }
}

impl JobStoreShard {
    /// Dequeue up to `max_tasks` tasks available now, ordered by time then priority.
    ///
    /// This method processes internal tasks (CheckRateLimit, RequestTicket) transparently and returns
    /// RunAttempt tasks for job execution and RefreshFloatingLimit tasks for workers to refresh floating limits.
    ///
    /// The `task_group` parameter specifies which task group to poll for tasks.
    pub async fn dequeue(
        &self,
        worker_id: &str,
        task_group: &str,
        max_tasks: usize,
    ) -> Result<DequeueResult, JobStoreShardError> {
        if max_tasks == 0 {
            return Ok(DequeueResult {
                tasks: Vec::new(),
                refresh_tasks: Vec::new(),
            });
        }

        let mut out: Vec<LeasedTask> = Vec::new();
        let mut refresh_out: Vec<LeasedRefreshTask> = Vec::new();
        // Tuple: (tenant, job_view, encoded_attempt_bytes)
        // We keep the encoded bytes to construct JobAttemptView without a DB readback.
        let mut pending_attempts: Vec<(String, JobView, Vec<u8>)> = Vec::with_capacity(max_tasks);

        // Track grants made during this dequeue for rollback on failure
        // Format: (tenant, queue, task_id)
        let mut grants_to_rollback: Vec<(String, String, String)> = Vec::new();
        // Track leased tasks for DST event emission after commit
        // Format: (tenant, job_id, task_id)
        let mut leased_tasks_for_dst: Vec<(String, String, String)> = Vec::new();

        // Loop to process internal tasks until we have tasks that are destined for the worker, or no more ready tasks at all.
        const MAX_INTERNAL_ITERATIONS: usize = 10;
        for _iteration in 0..MAX_INTERNAL_ITERATIONS {
            let remaining = max_tasks.saturating_sub(out.len() + pending_attempts.len());
            if remaining == 0 {
                break;
            }

            // [SILO-DEQ-1] Claim from the broker buffer for the specified task_group
            let claimed: Vec<BrokerTask> = self
                .broker
                .claim_ready_or_nudge(task_group, remaining)
                .await;

            if claimed.is_empty() {
                break;
            }

            let now_ms = now_epoch_ms();
            let expiry_ms = now_ms + DEFAULT_LEASE_MS;
            let mut state = DequeueIterationState::new(claimed.len());

            // Get the shard range for split-aware filtering
            let shard_range = self.get_range();

            for entry in &claimed {
                let decoded = &entry.decoded;

                // Check if task's tenant is within shard range
                // Tasks for tenants outside the range are defunct (from before a split)
                let task_tenant = decoded.tenant();

                if !shard_range.contains(task_tenant) {
                    // Task is for a tenant outside our range - delete and skip
                    state.batch.delete(&entry.key);
                    state.ack_deleted(&entry.key);
                    tracing::debug!(
                        tenant = %task_tenant,
                        range = %shard_range,
                        "dequeue: skipping defunct task (tenant outside shard range)"
                    );
                    continue;
                }

                match decoded.variant_type() {
                    fb::TaskVariant::RequestTicket => {
                        self.handle_request_ticket(
                            &mut state,
                            &entry.key,
                            decoded,
                            &shard_range,
                            worker_id,
                            now_ms,
                            expiry_ms,
                        )
                        .await?;
                    }
                    fb::TaskVariant::CheckRateLimit => {
                        self.handle_check_rate_limit(&mut state, &entry.key, decoded, now_ms)
                            .await?;
                    }
                    fb::TaskVariant::RefreshFloatingLimit => {
                        self.handle_refresh_floating_limit(
                            &mut state,
                            &mut refresh_out,
                            &entry.key,
                            decoded,
                            worker_id,
                            expiry_ms,
                        )?;
                    }
                    fb::TaskVariant::RunAttempt => {
                        self.handle_run_attempt(
                            &mut state, &entry.key, decoded, worker_id, now_ms, expiry_ms,
                        )
                        .await?;
                    }
                    other => {
                        return Err(JobStoreShardError::Codec(format!(
                            "unexpected task variant {:?}",
                            other
                        )));
                    }
                }
            }

            // Merge iteration state into outer accumulators
            grants_to_rollback.append(&mut state.grants_to_rollback);

            // Two-phase DST events: emit before write for correct causal ordering,
            // confirm after write succeeds, cancel if write fails.
            let write_op = dst_events::next_write_op();
            for (tenant, job_id, task_id) in leased_tasks_for_dst
                .drain(..)
                .chain(state.leased_tasks_for_dst.drain(..))
            {
                dst_events::emit_pending(
                    DstEvent::TaskLeased {
                        tenant: tenant.clone(),
                        job_id: job_id.clone(),
                        task_id,
                        worker_id: worker_id.to_string(),
                    },
                    write_op,
                );
                dst_events::emit_pending(
                    DstEvent::JobStatusChanged {
                        tenant,
                        job_id,
                        new_status: "Running".to_string(),
                    },
                    write_op,
                );
            }

            // Commit durable state — write_with_options with await_durable:true blocks
            // until the WAL is flushed to object storage, so no separate flush is needed.
            if let Err(e) = self
                .db
                .write_with_options(
                    state.batch,
                    &WriteOptions {
                        await_durable: true,
                    },
                )
                .await
            {
                dst_events::cancel_write(write_op);
                // Rollback all grants made during this iteration
                for (tenant, queue, task_id) in &grants_to_rollback {
                    self.concurrency.rollback_grant(tenant, queue, task_id);
                }
                // Put back all claimed entries since we didn't lease them durably
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }
            dst_events::confirm_write(write_op);

            // DB write succeeded - clear rollback lists for next iteration
            grants_to_rollback.clear();

            // Collect pending attempts from this iteration
            pending_attempts.append(&mut state.pending_attempts);

            // [SILO-DEQ-3] Ack durable and evict from buffer.
            // TaskBroker tracks all release keys for inflight cleanup, but only
            // installs tombstones for keys that were durably deleted.
            self.broker
                .ack_durable(&state.release_keys, &state.tombstone_keys);
            self.broker.evict_keys(&state.release_keys);
            tracing::debug!(
                release_keys = state.release_keys.len(),
                tombstone_keys = state.tombstone_keys.len(),
                pending_attempts = pending_attempts.len(),
                buffer_size = self.broker.buffer_len(),
                inflight = self.broker.inflight_len(),
                processed_internal = state.processed_internal,
                "dequeue: acked and evicted keys"
            );

            // If we only processed internal tasks and haven't filled max_tasks, loop again
            // to pick up the follow-up tasks we just inserted into the broker
            if !state.processed_internal {
                // We processed real RunAttempt tasks, no need to loop
                break;
            }
            // Continue looping to process any follow-up tasks
        }

        // Build LeasedTask results from pending_attempts using pre-encoded bytes
        for (tenant, job_view, attempt_bytes) in pending_attempts.into_iter() {
            let attempt_view = JobAttemptView::new(attempt_bytes)?;
            out.push(LeasedTask::new(tenant, job_view, attempt_view));
        }
        tracing::debug!(
            worker_id = %worker_id,
            returned = out.len(),
            refresh_tasks = refresh_out.len(),
            "dequeue: completed"
        );
        Ok(DequeueResult {
            tasks: out,
            refresh_tasks: refresh_out,
        })
    }

    /// Write a lease record, set job status to Running, and create an attempt record.
    /// Returns the encoded attempt bytes for constructing `pending_attempts`.
    #[allow(clippy::too_many_arguments)]
    async fn write_lease_and_attempt(
        &self,
        batch: &mut WriteBatch,
        worker_id: &str,
        task: &Task,
        task_id: &str,
        tenant: &str,
        job_id: &str,
        attempt_number: u32,
        relative_attempt_number: u32,
        now_ms: i64,
        expiry_ms: i64,
    ) -> Result<Vec<u8>, JobStoreShardError> {
        // [SILO-DEQ-4] Create lease record
        let lease_key = leased_task_key(task_id);
        let record = LeaseRecord {
            worker_id: worker_id.to_string(),
            task: task.clone(),
            expiry_ms,
            started_at_ms: now_ms,
        };
        let leased_value = encode_lease(&record);
        batch.put(&lease_key, &leased_value);

        // [SILO-DEQ-6] Mark job as running
        let job_status = JobStatus::running(now_ms);
        self.set_job_status_with_index(
            &mut DbWriteBatcher::new(&self.db, batch),
            tenant,
            job_id,
            job_status,
        )
        .await?;

        // [SILO-DEQ-5] Create attempt record
        let attempt = JobAttempt {
            job_id: job_id.to_string(),
            attempt_number,
            relative_attempt_number,
            task_id: task_id.to_string(),
            started_at_ms: now_ms,
            status: AttemptStatus::Running,
        };
        let attempt_val = encode_attempt(&attempt);
        let akey = attempt_key(tenant, job_id, attempt_number);
        batch.put(&akey, &attempt_val);

        Ok(attempt_val)
    }

    /// Process a RequestTicket task: check cancellation, process concurrency ticket, maybe lease.
    #[allow(clippy::too_many_arguments)]
    async fn handle_request_ticket(
        &self,
        state: &mut DequeueIterationState,
        task_key: &[u8],
        decoded: &DecodedTask,
        shard_range: &ShardRange,
        worker_id: &str,
        now_ms: i64,
        expiry_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let rt = decoded.as_request_ticket().ok_or_else(|| {
            JobStoreShardError::Codec("expected RequestTicket variant".to_string())
        })?;
        let queue = rt.queue().unwrap_or_default();
        let tenant = rt.tenant().unwrap_or_default();
        let job_id = rt.job_id().unwrap_or_default();
        let attempt_number = rt.attempt_number();
        let relative_attempt_number = rt.relative_attempt_number();
        let request_id = rt.request_id().unwrap_or_default();
        let req_task_group = rt.task_group().unwrap_or_default();
        state.processed_internal = true;
        let tenant = tenant.to_string();

        // Note: No cancelled check here. Cancelled jobs' tasks are eagerly removed
        // by cancel_job. If a stale task appears (e.g., retry after cancel), it will
        // be processed normally and the worker discovers cancellation via heartbeat.

        // Load job info
        let job_key = job_info_key(&tenant, job_id);
        let maybe_job = self.db.get(&job_key).await?;
        let job_view = maybe_job
            .as_ref()
            .and_then(|bytes| JobView::new(bytes.clone()).ok());

        // Process ticket via concurrency manager
        let outcome = self
            .concurrency
            .process_ticket_request_task(
                &self.db,
                shard_range,
                &mut state.batch,
                task_key,
                &tenant,
                queue,
                request_id,
                job_id,
                attempt_number,
                now_ms,
                job_view.as_ref(),
            )
            .await?;

        match outcome {
            RequestTicketTaskOutcome::Granted { request_id, queue } => {
                // Track grant for rollback if DB write fails
                state
                    .grants_to_rollback
                    .push((tenant.clone(), queue.clone(), request_id.clone()));

                // Create RunAttempt task for the lease (new Task, not a copy)
                let run = Task::RunAttempt {
                    id: request_id.clone(),
                    tenant: tenant.clone(),
                    job_id: job_id.to_string(),
                    attempt_number,
                    relative_attempt_number,
                    held_queues: vec![queue],
                    task_group: req_task_group.to_string(),
                };

                let attempt_val = self
                    .write_lease_and_attempt(
                        &mut state.batch,
                        worker_id,
                        &run,
                        &request_id,
                        &tenant,
                        job_id,
                        attempt_number,
                        relative_attempt_number,
                        now_ms,
                        expiry_ms,
                    )
                    .await?;

                let view = job_view.ok_or_else(|| {
                    JobStoreShardError::Codec(format!(
                        "job view missing for granted ticket, job_id={}",
                        job_id
                    ))
                })?;
                state
                    .pending_attempts
                    .push((tenant.clone(), view, attempt_val));
                state.ack_deleted(task_key);

                // Track for DST event emission after commit
                state
                    .leased_tasks_for_dst
                    .push((tenant, job_id.to_string(), request_id));

                // Record concurrency ticket metric
                if let Some(ref m) = self.metrics {
                    m.record_concurrency_ticket_granted();
                }
            }
            RequestTicketTaskOutcome::Requested => {
                // Release inflight only; task key remains in DB and must be eligible
                // for future scans when capacity is available.
                state.ack_release(task_key);
            }
            RequestTicketTaskOutcome::JobMissing => {
                // process_ticket_request_task deleted the task key.
                state.ack_deleted(task_key);
            }
        }

        Ok(())
    }

    /// Process a CheckRateLimit task: check rate limit, enqueue follow-up or retry.
    async fn handle_check_rate_limit(
        &self,
        state: &mut DequeueIterationState,
        task_key: &[u8],
        decoded: &DecodedTask,
        now_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        // Materialize the full task since we need most fields for the Gubernator call
        let Task::CheckRateLimit {
            task_id: ref check_task_id,
            ref tenant,
            ref job_id,
            attempt_number,
            relative_attempt_number: check_relative_attempt_number,
            limit_index,
            ref rate_limit,
            retry_count,
            started_at_ms,
            priority,
            ref held_queues,
            task_group: ref check_task_group,
        } = decoded.to_task()?
        else {
            return Err(JobStoreShardError::Codec(
                "expected CheckRateLimit variant".to_string(),
            ));
        };

        state.processed_internal = true;
        state.batch.delete(task_key);
        state.ack_deleted(task_key);

        // Load job info to get the full limits list
        let job_key = job_info_key(tenant, job_id);
        let maybe_job = self.db.get(&job_key).await?;
        let job_view = match maybe_job {
            Some(bytes) => match JobView::new(bytes) {
                Ok(v) => v,
                Err(_) => return Ok(()), // Skip malformed job
            },
            None => return Ok(()), // Job deleted, skip
        };

        // Check the rate limit via Gubernator
        let rate_limit_result = self.check_gubernator_rate_limit(rate_limit).await;

        match rate_limit_result {
            Ok(result) if result.under_limit => {
                // Rate limit passed! Proceed to next limit or RunAttempt
                let grants = self
                    .enqueue_limit_task_at_index(
                        &mut DbWriteBatcher::new(&self.db, &mut state.batch),
                        LimitTaskParams {
                            tenant,
                            task_id: check_task_id,
                            job_id,
                            attempt_number,
                            relative_attempt_number: check_relative_attempt_number,
                            limit_index: (limit_index + 1) as usize,
                            limits: &job_view.limits(),
                            priority,
                            start_at_ms: now_ms,
                            now_ms,
                            held_queues: held_queues.clone(),
                            task_group: check_task_group,
                            skip_try_reserve: false,
                        },
                    )
                    .await?;
                // Track any immediate grants for rollback if DB write fails
                for (queue, task_id) in grants {
                    state
                        .grants_to_rollback
                        .push((tenant.clone(), queue, task_id));
                }
            }
            Ok(result) => {
                // Over limit - schedule retry
                let max_retries = rate_limit.retry_max_retries;
                if max_retries > 0 && retry_count >= max_retries {
                    tracing::warn!(
                        job_id = %job_id,
                        retry_count = retry_count,
                        max_retries = max_retries,
                        "rate limit check exceeded max retries"
                    );
                    return Ok(());
                }

                let retry_backoff = self.calculate_rate_limit_backoff(
                    rate_limit,
                    retry_count,
                    result.reset_time_ms,
                    now_ms,
                );
                self.schedule_rate_limit_retry(
                    &mut DbWriteBatcher::new(&self.db, &mut state.batch),
                    tenant,
                    job_id,
                    attempt_number,
                    check_relative_attempt_number,
                    limit_index,
                    rate_limit,
                    retry_count,
                    started_at_ms,
                    priority,
                    held_queues,
                    retry_backoff,
                    check_task_group,
                )?;
            }
            Err(e) => {
                tracing::warn!(job_id = %job_id, error = %e, "gubernator rate limit check failed, will retry");
                let retry_backoff = now_ms + rate_limit.retry_initial_backoff_ms;
                self.schedule_rate_limit_retry(
                    &mut DbWriteBatcher::new(&self.db, &mut state.batch),
                    tenant,
                    job_id,
                    attempt_number,
                    check_relative_attempt_number,
                    limit_index,
                    rate_limit,
                    retry_count,
                    started_at_ms,
                    priority,
                    held_queues,
                    retry_backoff,
                    check_task_group,
                )?;
            }
        }

        Ok(())
    }

    /// Process a RefreshFloatingLimit task: create lease and add to refresh output.
    fn handle_refresh_floating_limit(
        &self,
        state: &mut DequeueIterationState,
        refresh_out: &mut Vec<LeasedRefreshTask>,
        task_key: &[u8],
        decoded: &DecodedTask,
        worker_id: &str,
        expiry_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let rfl = decoded.as_refresh_floating_limit().ok_or_else(|| {
            JobStoreShardError::Codec("expected RefreshFloatingLimit variant".to_string())
        })?;
        let task_id = rfl.task_id().unwrap_or_default();

        // Build lease from the task
        let lease_key = leased_task_key(task_id);
        let task = decoded.to_task()?;
        let record = LeaseRecord {
            worker_id: worker_id.to_string(),
            task,
            expiry_ms,
            started_at_ms: 0, // not applicable for RefreshFloatingLimit tasks
        };
        let leased_value = encode_lease(&record);
        state.batch.put(&lease_key, &leased_value);
        state.batch.delete(task_key);

        // Materialize fields needed for the response
        let metadata = crate::codec::fb_kv_pairs_to_owned(rfl.metadata());

        refresh_out.push(LeasedRefreshTask {
            task_id: task_id.to_string(),
            tenant_id: rfl.tenant().unwrap_or_default().to_string(),
            queue_key: rfl.queue_key().unwrap_or_default().to_string(),
            current_max_concurrency: rfl.current_max_concurrency(),
            last_refreshed_at_ms: rfl.last_refreshed_at_ms(),
            metadata,
            task_group: rfl.task_group().unwrap_or_default().to_string(),
        });
        state.ack_deleted(task_key);

        Ok(())
    }

    /// Process a RunAttempt task: check cancellation, create lease, mark running.
    async fn handle_run_attempt(
        &self,
        state: &mut DequeueIterationState,
        task_key: &[u8],
        decoded: &DecodedTask,
        worker_id: &str,
        now_ms: i64,
        expiry_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let ra = decoded
            .as_run_attempt()
            .ok_or_else(|| JobStoreShardError::Codec("expected RunAttempt variant".to_string()))?;
        let task_id = ra.id().unwrap_or_default();
        let tenant = ra.tenant().unwrap_or_default();
        let job_id = ra.job_id().unwrap_or_default();
        let attempt_number = ra.attempt_number();
        let relative_attempt_number = ra.relative_attempt_number();

        // [SILO-DEQ-2] Look up job info; if missing, delete the task and skip
        let job_key = job_info_key(tenant, job_id);
        let maybe_job = self.db.get(&job_key).await?;
        let Some(job_bytes) = maybe_job else {
            // If job missing, delete task key to clean up
            state.batch.delete(task_key);
            state.ack_deleted(task_key);
            return Ok(());
        };

        // Note: No cancelled check here. Cancelled jobs' tasks are eagerly removed
        // by cancel_job. If a stale task appears (e.g., retry after cancel), it will
        // be leased normally and the worker discovers cancellation via heartbeat.

        let view = JobView::new(job_bytes)?;

        // [SILO-DEQ-CONC] Implicit: If job requires concurrency, task must hold all required queues.
        // This is guaranteed by construction: RunAttempt tasks are only created when concurrency
        // is granted (at enqueue or grant_next), with held_queues populated.

        // [SILO-DEQ-3] Delete task from task queue
        state.batch.delete(task_key);

        let task = decoded.to_task()?;
        let attempt_val = self
            .write_lease_and_attempt(
                &mut state.batch,
                worker_id,
                &task,
                task_id,
                tenant,
                job_id,
                attempt_number,
                relative_attempt_number,
                now_ms,
                expiry_ms,
            )
            .await?;

        // Construct AttemptView directly from encoded bytes (no DB readback needed)
        state
            .pending_attempts
            .push((tenant.to_string(), view, attempt_val));
        state.ack_deleted(task_key);

        // Track for DST event emission after commit
        state.leased_tasks_for_dst.push((
            tenant.to_string(),
            job_id.to_string(),
            task_id.to_string(),
        ));

        Ok(())
    }

    /// Peek up to `max_tasks` available tasks (time <= now), without deleting them.
    pub async fn peek_tasks(
        &self,
        task_group: &str,
        max_tasks: usize,
    ) -> Result<Vec<Task>, JobStoreShardError> {
        let (tasks, _keys) = self.scan_ready_tasks(task_group, max_tasks).await?;
        Ok(tasks)
    }

    /// Internal: scan up to `max_tasks` ready tasks and return them with their keys.
    pub(crate) async fn scan_ready_tasks(
        &self,
        task_group: &str,
        max_tasks: usize,
    ) -> Result<(Vec<Task>, Vec<Vec<u8>>), JobStoreShardError> {
        if max_tasks == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // Scan tasks under tasks/{task_group}/ using binary storekey encoding
        let start = crate::keys::task_group_prefix(task_group);
        let end = crate::keys::end_bound(&start);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;

        let mut tasks: Vec<Task> = Vec::with_capacity(max_tasks);
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(max_tasks);
        let now_ms = now_epoch_ms();
        while tasks.len() < max_tasks {
            let maybe_kv = iter.next().await?;
            let Some(kv) = maybe_kv else {
                break;
            };

            // Parse task key to extract timestamp for time cutoff
            let Some(parsed_key) = crate::keys::parse_task_key(&kv.key) else {
                continue;
            };

            // Enforce time cutoff: only keys with ts <= now_ms
            if parsed_key.start_time_ms > now_ms as u64 {
                continue;
            }

            let task = match decode_task(&kv.value) {
                Ok(t) => t,
                Err(_) => continue, // Skip malformed tasks
            };
            tasks.push(task);
            keys.push(kv.key.to_vec());
        }

        Ok((tasks, keys))
    }
}
