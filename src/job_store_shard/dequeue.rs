//! Task dequeue and processing operations.

use slatedb::config::WriteOptions;
use slatedb::{DbIterator, WriteBatch};

use crate::codec::{decode_task, encode_attempt, encode_lease};
use crate::concurrency::{ReleaseGrantRollback, RequestTicketTaskOutcome};
use crate::dst_events::{self, DstEvent};
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
    release_grants_to_rollback: Vec<ReleaseGrantRollback>,
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
            release_grants_to_rollback: Vec::new(),
            leased_tasks_for_dst: Vec::new(),
            pending_attempts: Vec::new(),
            processed_internal: false,
        }
    }

    fn ack_release(&mut self, key: &[u8]) {
        self.release_keys.push(key.to_owned());
    }

    fn ack_deleted(&mut self, key: &[u8]) {
        self.release_keys.push(key.to_owned());
        self.tombstone_keys.push(key.to_owned());
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
        // Track release-and-grant operations for rollback on failure
        let mut release_grants_to_rollback: Vec<ReleaseGrantRollback> = Vec::new();
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
                let task = &entry.task;

                // Check if task's tenant is within shard range
                // Tasks for tenants outside the range are defunct (from before a split)
                let task_tenant = task.tenant();

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

                match task {
                    Task::RequestTicket { .. } => {
                        self.handle_request_ticket(
                            &mut state,
                            entry,
                            &shard_range,
                            worker_id,
                            now_ms,
                            expiry_ms,
                        )
                        .await?;
                    }
                    Task::CheckRateLimit { .. } => {
                        self.handle_check_rate_limit(&mut state, entry, now_ms)
                            .await?;
                    }
                    Task::RefreshFloatingLimit { .. } => {
                        self.handle_refresh_floating_limit(
                            &mut state,
                            &mut refresh_out,
                            entry,
                            worker_id,
                            expiry_ms,
                        )?;
                    }
                    Task::RunAttempt { .. } => {
                        self.handle_run_attempt(&mut state, entry, worker_id, now_ms, expiry_ms)
                            .await?;
                    }
                }
            }

            // Merge iteration state into outer accumulators
            grants_to_rollback.append(&mut state.grants_to_rollback);
            release_grants_to_rollback.append(&mut state.release_grants_to_rollback);

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
                // Rollback all release-and-grant operations
                self.concurrency
                    .rollback_release_grants(&release_grants_to_rollback);
                // Put back all claimed entries since we didn't lease them durably
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }
            dst_events::confirm_write(write_op);

            // Wake broker for any release-grants that happened (before clearing)
            if !release_grants_to_rollback.is_empty() {
                self.broker.wakeup();
            }

            // DB write succeeded - clear rollback lists for next iteration
            grants_to_rollback.clear();
            release_grants_to_rollback.clear();

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
            let attempt_view = JobAttemptView::new(&attempt_bytes)?;
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
        let leased_value = encode_lease(&record)?;
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
        let attempt_val = encode_attempt(&attempt)?;
        let akey = attempt_key(tenant, job_id, attempt_number);
        batch.put(&akey, &attempt_val);

        Ok(attempt_val)
    }

    /// Process a RequestTicket task: check cancellation, process concurrency ticket, maybe lease.
    async fn handle_request_ticket(
        &self,
        state: &mut DequeueIterationState,
        entry: &BrokerTask,
        shard_range: &ShardRange,
        worker_id: &str,
        now_ms: i64,
        expiry_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let Task::RequestTicket {
            queue,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            request_id,
            task_group: req_task_group,
            ..
        } = &entry.task
        else {
            unreachable!()
        };
        let attempt_number = *attempt_number;
        let relative_attempt_number = *relative_attempt_number;
        state.processed_internal = true;
        let tenant = tenant.to_string();

        // [SILO-DEQ-CXL] Check if job is cancelled - if so, skip and clean up task
        if self.is_job_cancelled(&tenant, job_id).await? {
            state.batch.delete(&entry.key);
            state.ack_deleted(&entry.key);
            tracing::debug!(job_id = %job_id, "dequeue: skipping cancelled job RequestTicket");
            return Ok(());
        }

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
                &entry.key,
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

                // Create RunAttempt task for the lease
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

                let view = job_view.unwrap();
                state
                    .pending_attempts
                    .push((tenant.clone(), view, attempt_val));
                state.ack_deleted(&entry.key);

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
                state.ack_release(&entry.key);
            }
            RequestTicketTaskOutcome::JobMissing => {
                // process_ticket_request_task deleted the task key.
                state.ack_deleted(&entry.key);
            }
        }

        Ok(())
    }

    /// Process a CheckRateLimit task: check rate limit, enqueue follow-up or retry.
    async fn handle_check_rate_limit(
        &self,
        state: &mut DequeueIterationState,
        entry: &BrokerTask,
        now_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let Task::CheckRateLimit {
            task_id: check_task_id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number: check_relative_attempt_number,
            limit_index,
            rate_limit,
            retry_count,
            started_at_ms,
            priority,
            held_queues,
            task_group: check_task_group,
        } = &entry.task
        else {
            unreachable!()
        };
        let attempt_number = *attempt_number;
        let check_relative_attempt_number = *check_relative_attempt_number;
        let limit_index = *limit_index;
        let retry_count = *retry_count;
        let started_at_ms = *started_at_ms;
        let priority = *priority;
        state.processed_internal = true;
        let tenant = tenant.to_string();
        state.batch.delete(&entry.key);
        state.ack_deleted(&entry.key);

        // Load job info to get the full limits list
        let job_key = job_info_key(&tenant, job_id);
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
                            tenant: &tenant,
                            task_id: check_task_id,
                            job_id,
                            attempt_number,
                            relative_attempt_number: check_relative_attempt_number,
                            limit_index: (limit_index + 1) as usize,
                            limits: &job_view.limits(),
                            priority,
                            start_at_ms: now_ms,
                            now_ms,
                            held_queues: held_queues.to_vec(),
                            task_group: check_task_group,
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
                    &tenant,
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
                    &tenant,
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
        entry: &BrokerTask,
        worker_id: &str,
        expiry_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let Task::RefreshFloatingLimit {
            task_id,
            tenant: task_tenant,
            queue_key,
            current_max_concurrency,
            last_refreshed_at_ms,
            metadata,
            task_group: refresh_task_group,
        } = &entry.task
        else {
            unreachable!()
        };

        let lease_key = leased_task_key(task_id);
        let record = LeaseRecord {
            worker_id: worker_id.to_string(),
            task: entry.task.clone(),
            expiry_ms,
            started_at_ms: 0, // Not applicable for RefreshFloatingLimit tasks
        };
        let leased_value = encode_lease(&record)?;
        state.batch.put(&lease_key, &leased_value);
        state.batch.delete(&entry.key);

        refresh_out.push(LeasedRefreshTask {
            task_id: task_id.to_string(),
            tenant_id: task_tenant.to_string(),
            queue_key: queue_key.to_string(),
            current_max_concurrency: *current_max_concurrency,
            last_refreshed_at_ms: *last_refreshed_at_ms,
            metadata: metadata.to_vec(),
            task_group: refresh_task_group.to_string(),
        });
        state.ack_deleted(&entry.key);

        Ok(())
    }

    /// Process a RunAttempt task: check cancellation, create lease, mark running.
    async fn handle_run_attempt(
        &self,
        state: &mut DequeueIterationState,
        entry: &BrokerTask,
        worker_id: &str,
        now_ms: i64,
        expiry_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let Task::RunAttempt {
            id: task_id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            ..
        } = &entry.task
        else {
            unreachable!()
        };
        let attempt_number = *attempt_number;
        let relative_attempt_number = *relative_attempt_number;
        // [SILO-DEQ-2] Look up job info; if missing, delete the task and skip
        let job_key = job_info_key(tenant, job_id);
        let maybe_job = self.db.get(&job_key).await?;
        let Some(job_bytes) = maybe_job else {
            // If job missing, delete task key to clean up
            state.batch.delete(&entry.key);
            state.ack_deleted(&entry.key);
            return Ok(());
        };

        // [SILO-DEQ-CXL] Check if job is cancelled - if so, skip and clean up task
        if self.is_job_cancelled(tenant, job_id).await? {
            state.batch.delete(&entry.key);
            state.ack_deleted(&entry.key);

            // [SILO-DEQ-CXL-REL] Release any held concurrency tickets
            // This is required to maintain invariant: holders can only exist for active tasks
            let held_queues = match &entry.task {
                Task::RunAttempt { held_queues, .. } => held_queues.clone(),
                Task::CheckRateLimit { held_queues, .. } => held_queues.clone(),
                Task::RequestTicket { .. } | Task::RefreshFloatingLimit { .. } => Vec::new(),
            };
            if !held_queues.is_empty() {
                // release_and_grant_next updates in-memory atomically before returning
                // Track rollback info in case DB write fails
                let release_rollbacks = self
                    .concurrency
                    .release_and_grant_next(
                        &self.db,
                        &mut state.batch,
                        tenant,
                        &held_queues,
                        task_id,
                        now_ms,
                    )
                    .await?;
                state.release_grants_to_rollback.extend(release_rollbacks);
                tracing::debug!(job_id = %job_id, queues = ?held_queues, "dequeue: released tickets for cancelled job task");
            }

            tracing::debug!(job_id = %job_id, "dequeue: skipping cancelled job task");
            return Ok(());
        }

        let view = JobView::new(job_bytes)?;

        // [SILO-DEQ-CONC] Implicit: If job requires concurrency, task must hold all required queues.
        // This is guaranteed by construction: RunAttempt tasks are only created when concurrency
        // is granted (at enqueue or grant_next), with held_queues populated.

        // [SILO-DEQ-3] Delete task from task queue
        state.batch.delete(&entry.key);

        let attempt_val = self
            .write_lease_and_attempt(
                &mut state.batch,
                worker_id,
                &entry.task,
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
        state.ack_deleted(&entry.key);

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
