//! Task dequeue and processing operations.

use slatedb::{DbIterator, WriteBatch};

use crate::codec::{decode_task, encode_attempt, encode_lease};
use crate::concurrency::{MemoryEvent, RequestTicketTaskOutcome};
use crate::job::{JobStatus, JobView};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::job_store_shard::helpers::{now_epoch_ms, release_held_tickets};
use crate::job_store_shard::{DequeueResult, JobStoreShard, JobStoreShardError, PendingCrossShardTask};
use crate::keys::{attempt_key, job_info_key, leased_task_key};
use crate::task::{LeaseRecord, LeasedRefreshTask, LeasedTask, Task, DEFAULT_LEASE_MS};
use crate::job_store_shard::enqueue::NextLimitContext;
use crate::task_broker::BrokerTask;

impl JobStoreShard {
    /// Dequeue up to `max_tasks` tasks available now, ordered by time then priority.
    ///
    /// This method processes internal tasks (CheckRateLimit, RequestTicket) transparently and returns
    /// RunAttempt tasks for job execution and RefreshFloatingLimit tasks for workers to refresh floating limits.
    pub async fn dequeue(
        &self,
        worker_id: &str,
        max_tasks: usize,
    ) -> Result<DequeueResult, JobStoreShardError> {
        let _ts_enter = now_epoch_ms();
        if max_tasks == 0 {
            return Ok(DequeueResult {
                tasks: Vec::new(),
                refresh_tasks: Vec::new(),
                cross_shard_tasks: Vec::new(),
            });
        }

        let mut out: Vec<LeasedTask> = Vec::new();
        let mut refresh_out: Vec<LeasedRefreshTask> = Vec::new();
        let mut pending_attempts: Vec<(String, JobView, String, u32)> = Vec::new();
        let mut cross_shard_out: Vec<PendingCrossShardTask> = Vec::new();

        // Loop to process internal tasks until we have tasks that are destined for the worker, or no more ready tasks at all.
        const MAX_INTERNAL_ITERATIONS: usize = 10;
        for _iteration in 0..MAX_INTERNAL_ITERATIONS {
            let remaining = max_tasks.saturating_sub(out.len() + pending_attempts.len());
            if remaining == 0 {
                break;
            }

            // [SILO-DEQ-1] Claim from the broker buffer (tenant-agnostic)
            let claimed: Vec<BrokerTask> = self.broker.claim_ready_or_nudge(remaining).await;

            if claimed.is_empty() {
                break;
            }

            let now_ms = now_epoch_ms();
            let expiry_ms = now_ms + DEFAULT_LEASE_MS;
            let mut batch = WriteBatch::new();
            let mut ack_keys: Vec<String> = Vec::with_capacity(claimed.len());
            let mut processed_internal = false;

            for entry in &claimed {
                let task = &entry.task;
                // Internal tasks are processed inside the store and not leased
                match task {
                    Task::RequestTicket {
                        queue,
                        start_time_ms: _,
                        priority: _priority,
                        tenant,
                        job_id,
                        attempt_number,
                        request_id,
                        held_queues,
                    } => {
                        // Process ticket request internally
                        processed_internal = true;
                        let tenant = tenant.to_string();

                        // Debug assertion: RequestTicket tasks are for LOCAL queues only.
                        // If the queue routes to a different shard, a RequestRemoteTicket
                        // task should have been created instead.
                        self.debug_assert_owns_queue(&tenant, queue);

                        // [SILO-DEQ-CXL] Check if job is cancelled - if so, skip and clean up task
                        if self.is_job_cancelled(&tenant, job_id).await? {
                            batch.delete(entry.key.as_bytes());
                            ack_keys.push(entry.key.clone());
                            tracing::debug!(job_id = %job_id, "dequeue: skipping cancelled job RequestTicket");
                            continue;
                        }

                        // Load job info
                        let job_key = job_info_key(&tenant, job_id);
                        let maybe_job = self.db.get(job_key.as_bytes()).await?;
                        let job_view = maybe_job
                            .as_ref()
                            .and_then(|bytes| JobView::new(bytes.clone()).ok());

                        // Process ticket via concurrency manager
                        let outcome = self
                            .concurrency
                            .process_ticket_request_task(
                                &mut batch,
                                &entry.key,
                                &tenant,
                                queue,
                                request_id,
                                job_id,
                                *attempt_number,
                                now_ms,
                                job_view.as_ref(),
                            )
                            .map_err(JobStoreShardError::Rkyv)?;

                        match outcome {
                            RequestTicketTaskOutcome::Granted { request_id, queue } => {
                                // Accumulate previously held queues with the newly granted queue
                                let mut all_held_queues = held_queues.clone();
                                all_held_queues.push(queue.clone());

                                // Record the grant in memory
                                self.concurrency.counts().record_grant(
                                    &tenant,
                                    &queue,
                                    &request_id,
                                );

                                // Get the job's limits to check if there are more
                                let view = job_view.unwrap();
                                let limits = view.limits();

                                // Find the current limit index by matching queue
                                let current_limit_index = limits
                                    .iter()
                                    .position(|l| match l {
                                        crate::job::Limit::Concurrency(cl) => cl.key == queue,
                                        crate::job::Limit::FloatingConcurrency(fl) => fl.key == queue,
                                        crate::job::Limit::RateLimit(_) => false,
                                    })
                                    .unwrap_or(0) as u32;

                                // Check if there are more limits to process
                                if (current_limit_index + 1) < limits.len() as u32 {
                                    // More limits - enqueue the next limit task
                                    self.enqueue_next_limit_task(
                                        &mut batch,
                                        NextLimitContext {
                                            tenant: &tenant,
                                            task_id: &request_id, // Use same task_id for holder consistency
                                            job_id,
                                            attempt_number: *attempt_number,
                                            current_limit_index,
                                            limits: &limits,
                                            priority: view.priority(),
                                            start_at_ms: view.enqueue_time_ms(),
                                            now_ms,
                                            held_queues: all_held_queues,
                                        },
                                    )?;
                                    ack_keys.push(entry.key.clone());
                                    tracing::debug!(
                                        job_id = %job_id,
                                        queue = %queue,
                                        current_limit_index = current_limit_index,
                                        "dequeue RequestTicket: more limits to process"
                                    );
                                } else {
                                    // All limits satisfied - create lease and attempt records
                                    let run = Task::RunAttempt {
                                        id: request_id.clone(),
                                        tenant: tenant.clone(),
                                        job_id: job_id.clone(),
                                        attempt_number: *attempt_number,
                                        held_queues: all_held_queues,
                                    };
                                    let lease_key = leased_task_key(&request_id);
                                    let record = LeaseRecord {
                                        worker_id: worker_id.to_string(),
                                        task: run,
                                        expiry_ms,
                                    };
                                    let leased_value = encode_lease(&record)?;
                                    batch.put(lease_key.as_bytes(), &leased_value);

                                    // Mark job as running
                                    let job_status = JobStatus::running(now_ms);
                                    self.set_job_status_with_index(
                                        &mut batch, &tenant, job_id, job_status,
                                    )
                                    .await?;

                                    // Attempt record
                                    let attempt = JobAttempt {
                                        job_id: job_id.clone(),
                                        attempt_number: *attempt_number,
                                        task_id: request_id.clone(),
                                        status: AttemptStatus::Running {
                                            started_at_ms: now_ms,
                                        },
                                    };
                                    let attempt_val = encode_attempt(&attempt)?;
                                    let akey = attempt_key(&tenant, job_id, *attempt_number);
                                    batch.put(akey.as_bytes(), &attempt_val);

                                    // Track for response
                                    pending_attempts.push((
                                        tenant.clone(),
                                        view,
                                        job_id.clone(),
                                        *attempt_number,
                                    ));
                                    ack_keys.push(entry.key.clone());
                                }
                            }
                            RequestTicketTaskOutcome::Requested
                            | RequestTicketTaskOutcome::JobMissing => {
                                // Release inflight, task will be picked up later or cleaned up
                                ack_keys.push(entry.key.clone());
                            }
                        }
                        continue;
                    }
                    Task::CheckRateLimit {
                        task_id: check_task_id,
                        tenant,
                        job_id,
                        attempt_number,
                        limit_index,
                        rate_limit,
                        retry_count,
                        started_at_ms,
                        priority,
                        held_queues,
                    } => {
                        // Process rate limit check internally
                        processed_internal = true;
                        let tenant = tenant.to_string();
                        batch.delete(entry.key.as_bytes());
                        ack_keys.push(entry.key.clone());

                        // Load job info to get the full limits list
                        let job_key = job_info_key(&tenant, job_id);
                        let maybe_job = self.db.get(job_key.as_bytes()).await?;
                        let job_view = match maybe_job {
                            Some(bytes) => match JobView::new(bytes) {
                                Ok(v) => v,
                                Err(_) => continue, // Skip malformed job
                            },
                            None => continue, // Job deleted, skip
                        };

                        // Check the rate limit via Gubernator
                        let rate_limit_result = self.check_gubernator_rate_limit(rate_limit).await;

                        match rate_limit_result {
                            Ok(result) if result.under_limit => {
                                // Rate limit passed! Proceed to next limit or RunAttempt
                                self.enqueue_next_limit_task(
                                    &mut batch,
                                    NextLimitContext {
                                        tenant: &tenant,
                                        task_id: check_task_id,
                                        job_id,
                                        attempt_number: *attempt_number,
                                        current_limit_index: *limit_index,
                                        limits: &job_view.limits(),
                                        priority: *priority,
                                        start_at_ms: now_ms,
                                        now_ms,
                                        held_queues: held_queues.clone(),
                                    },
                                )?;
                            }
                            Ok(result) => {
                                // Over limit - schedule retry
                                let max_retries = rate_limit.retry_max_retries;
                                if max_retries > 0 && *retry_count >= max_retries {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        retry_count = retry_count,
                                        max_retries = max_retries,
                                        "rate limit check exceeded max retries"
                                    );
                                    continue;
                                }

                                let retry_backoff = self.calculate_rate_limit_backoff(
                                    rate_limit,
                                    *retry_count,
                                    result.reset_time_ms,
                                    now_ms,
                                );
                                self.schedule_rate_limit_retry(
                                    &mut batch,
                                    &tenant,
                                    job_id,
                                    *attempt_number,
                                    *limit_index,
                                    rate_limit,
                                    *retry_count,
                                    *started_at_ms,
                                    *priority,
                                    held_queues,
                                    retry_backoff,
                                )?;
                            }
                            Err(e) => {
                                tracing::warn!(job_id = %job_id, error = %e, "gubernator rate limit check failed, will retry");
                                let retry_backoff = now_ms + rate_limit.retry_initial_backoff_ms;
                                self.schedule_rate_limit_retry(
                                    &mut batch,
                                    &tenant,
                                    job_id,
                                    *attempt_number,
                                    *limit_index,
                                    rate_limit,
                                    *retry_count,
                                    *started_at_ms,
                                    *priority,
                                    held_queues,
                                    retry_backoff,
                                )?;
                            }
                        }
                        continue;
                    }
                    Task::RefreshFloatingLimit {
                        task_id,
                        tenant: _task_tenant,
                        queue_key,
                        current_max_concurrency,
                        last_refreshed_at_ms,
                        metadata,
                    } => {
                        // RefreshFloatingLimit tasks are sent to workers - create lease
                        let lease_key = leased_task_key(task_id);
                        let record = LeaseRecord {
                            worker_id: worker_id.to_string(),
                            task: task.clone(),
                            expiry_ms,
                        };
                        let leased_value = encode_lease(&record)?;
                        batch.put(lease_key.as_bytes(), &leased_value);
                        batch.delete(entry.key.as_bytes());

                        refresh_out.push(LeasedRefreshTask {
                            task_id: task_id.clone(),
                            queue_key: queue_key.clone(),
                            current_max_concurrency: *current_max_concurrency,
                            last_refreshed_at_ms: *last_refreshed_at_ms,
                            metadata: metadata.clone(),
                        });
                        ack_keys.push(entry.key.clone());
                        continue;
                    }
                    Task::RunAttempt { .. } => {}
                    // Cross-shard concurrency tasks - extract and return for caller to process via RPC
                    Task::RequestRemoteTicket {
                        task_id: _,
                        queue_owner_shard,
                        tenant,
                        queue_key,
                        job_id,
                        attempt_number,
                        priority,
                        start_time_ms,
                        request_id,
                        floating_limit,
                    } => {
                        // Need to get max_concurrency from the job
                        // This works for both ConcurrencyLimit and FloatingConcurrencyLimit
                        let job_key = job_info_key(tenant, job_id);
                        let maybe_job = self.db.get(job_key.as_bytes()).await?;
                        let max_concurrency = if let Some(bytes) = maybe_job {
                            if let Ok(view) = JobView::new(bytes) {
                                view.max_concurrency_for_queue(queue_key).unwrap_or(1)
                            } else {
                                1
                            }
                        } else {
                            // Job missing, clean up task
                            batch.delete(entry.key.as_bytes());
                            ack_keys.push(entry.key.clone());
                            continue;
                        };

                        cross_shard_out.push(PendingCrossShardTask::RequestRemoteTicket {
                            queue_owner_shard: *queue_owner_shard,
                            tenant: tenant.clone(),
                            queue_key: queue_key.clone(),
                            job_id: job_id.clone(),
                            request_id: request_id.clone(),
                            attempt_number: *attempt_number,
                            priority: *priority,
                            start_time_ms: *start_time_ms,
                            max_concurrency,
                            task_key: entry.key.clone(),
                            floating_limit: floating_limit.clone(),
                        });
                        // Don't delete task yet, and don't ack - task stays in inflight until
                        // delete_cross_shard_task is called after successful RPC
                        processed_internal = true;
                        continue;
                    }
                    Task::NotifyRemoteTicketGrant {
                        task_id: _,
                        job_shard,
                        tenant,
                        job_id,
                        queue_key,
                        request_id,
                        holder_task_id,
                        attempt_number,
                    } => {
                        cross_shard_out.push(PendingCrossShardTask::NotifyRemoteTicketGrant {
                            job_shard: *job_shard,
                            tenant: tenant.clone(),
                            job_id: job_id.clone(),
                            queue_key: queue_key.clone(),
                            request_id: request_id.clone(),
                            holder_task_id: holder_task_id.clone(),
                            attempt_number: *attempt_number,
                            task_key: entry.key.clone(),
                        });
                        // Don't delete task yet, and don't ack - task stays in inflight until
                        // delete_cross_shard_task is called after successful RPC
                        processed_internal = true;
                        continue;
                    }
                    Task::ReleaseRemoteTicket {
                        task_id: _,
                        queue_owner_shard,
                        tenant,
                        queue_key,
                        job_id,
                        holder_task_id,
                    } => {
                        cross_shard_out.push(PendingCrossShardTask::ReleaseRemoteTicket {
                            queue_owner_shard: *queue_owner_shard,
                            tenant: tenant.clone(),
                            queue_key: queue_key.clone(),
                            job_id: job_id.clone(),
                            holder_task_id: holder_task_id.clone(),
                            task_key: entry.key.clone(),
                        });
                        // Don't delete task yet, and don't ack - task stays in inflight until
                        // delete_cross_shard_task is called after successful RPC
                        processed_internal = true;
                        continue;
                    }
                }
                let (task_id, tenant, job_id, attempt_number) = match task {
                    Task::RunAttempt {
                        id,
                        tenant,
                        job_id,
                        attempt_number,
                        ..
                    } => (
                        id.clone(),
                        tenant.to_string(),
                        job_id.to_string(),
                        *attempt_number,
                    ),
                    Task::RequestTicket { .. }
                    | Task::CheckRateLimit { .. }
                    | Task::RefreshFloatingLimit { .. }
                    | Task::RequestRemoteTicket { .. }
                    | Task::NotifyRemoteTicketGrant { .. }
                    | Task::ReleaseRemoteTicket { .. } => unreachable!(),
                };

                // [SILO-DEQ-2] Look up job info; if missing, delete the task and skip
                let job_key = job_info_key(&tenant, &job_id);
                let maybe_job = self.db.get(job_key.as_bytes()).await?;
                if let Some(job_bytes) = maybe_job {
                    // [SILO-DEQ-CXL] Check if job is cancelled - if so, skip and clean up task
                    if self.is_job_cancelled(&tenant, &job_id).await? {
                        batch.delete(entry.key.as_bytes());
                        ack_keys.push(entry.key.clone());

                        // [SILO-DEQ-CXL-REL] Release any held concurrency tickets
                        // This is required to maintain invariant: holders can only exist for active tasks
                        let held_queues = match task {
                            Task::RunAttempt { held_queues, .. } => held_queues.clone(),
                            Task::CheckRateLimit { held_queues, .. } => held_queues.clone(),
                            Task::RequestTicket { .. }
                            | Task::RefreshFloatingLimit { .. }
                            | Task::RequestRemoteTicket { .. }
                            | Task::NotifyRemoteTicketGrant { .. }
                            | Task::ReleaseRemoteTicket { .. } => Vec::new(),
                        };
                        if !held_queues.is_empty() {
                            let release_events = release_held_tickets(
                                &self.db,
                                &mut batch,
                                &self.concurrency,
                                &tenant,
                                &job_id,
                                &task_id,
                                &held_queues,
                                now_ms,
                            )
                            .await?;
                            self.apply_concurrency_events(&tenant, release_events);
                        }

                        tracing::debug!(job_id = %job_id, "dequeue: skipping cancelled job task");
                        continue;
                    }

                    let view = JobView::new(job_bytes)?;

                    // [SILO-DEQ-CONC] Implicit: If job requires concurrency, task must hold all required queues.
                    // This is guaranteed by construction: RunAttempt tasks are only created when concurrency
                    // is granted (at enqueue or grant_next), with held_queues populated.

                    // [SILO-DEQ-4] Create lease record
                    let lease_key = leased_task_key(&task_id);
                    let record = LeaseRecord {
                        worker_id: worker_id.to_string(),
                        task: task.clone(),
                        expiry_ms,
                    };
                    let leased_value = encode_lease(&record)?;

                    batch.put(lease_key.as_bytes(), &leased_value);
                    // [SILO-DEQ-3] Delete task from task queue
                    batch.delete(entry.key.as_bytes());

                    // [SILO-DEQ-6] Mark job as running (pure write, no status read)
                    let job_status = JobStatus::running(now_ms);
                    self.set_job_status_with_index(&mut batch, &tenant, &job_id, job_status)
                        .await?;

                    // [SILO-DEQ-5] Also mark attempt as running
                    let attempt = JobAttempt {
                        job_id: job_id.clone(),
                        attempt_number,
                        task_id: task_id.clone(),
                        status: AttemptStatus::Running {
                            started_at_ms: now_ms,
                        },
                    };
                    let attempt_val = encode_attempt(&attempt)?;
                    let akey = attempt_key(&tenant, &job_id, attempt_number);
                    batch.put(akey.as_bytes(), &attempt_val);

                    // Defer constructing AttemptView; fetch from DB after batch is written
                    pending_attempts.push((tenant.clone(), view, job_id.clone(), attempt_number));
                    ack_keys.push(entry.key.clone());
                } else {
                    // If job missing, delete task key to clean up
                    batch.delete(entry.key.as_bytes());
                    ack_keys.push(entry.key.clone());
                }
            }

            // Try to commit durable state. On failure, requeue the tasks and return error.
            if let Err(e) = self.db.write(batch).await {
                // Put back all claimed entries since we didn't lease them durably
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }
            if let Err(e) = self.db.flush().await {
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }

            // [SILO-DEQ-3] Ack durable and evict from buffer; we no longer use TTL tombstones.
            self.broker.ack_durable(&ack_keys);
            self.broker.evict_keys(&ack_keys);
            tracing::debug!(
                ack_keys = ack_keys.len(),
                pending_attempts = pending_attempts.len(),
                buffer_size = self.broker.buffer_len(),
                inflight = self.broker.inflight_len(),
                processed_internal = processed_internal,
                "dequeue: acked and evicted keys"
            );

            // If we only processed internal tasks and haven't filled max_tasks, loop again
            // to pick up the follow-up tasks we just inserted into the broker
            if !processed_internal {
                // We processed real RunAttempt tasks, no need to loop
                break;
            }
            // Continue looping to process any follow-up tasks
        }

        // Build LeasedTask results from pending_attempts
        for (tenant, job_view, job_id, attempt_number) in pending_attempts.into_iter() {
            let attempt_view = self
                .get_job_attempt(tenant.as_str(), &job_id, attempt_number)
                .await?
                .ok_or_else(|| {
                    JobStoreShardError::Rkyv("attempt not found after dequeue".to_string())
                })?;
            out.push(LeasedTask::new(job_view, attempt_view));
        }
        tracing::debug!(
            worker_id = %worker_id,
            returned = out.len(),
            refresh_tasks = refresh_out.len(),
            cross_shard_tasks = cross_shard_out.len(),
            "dequeue: completed"
        );
        Ok(DequeueResult {
            tasks: out,
            refresh_tasks: refresh_out,
            cross_shard_tasks: cross_shard_out,
        })
    }

    /// Peek up to `max_tasks` available tasks (time <= now), without deleting them.
    pub async fn peek_tasks(&self, max_tasks: usize) -> Result<Vec<Task>, JobStoreShardError> {
        let (tasks, _keys) = self.scan_ready_tasks(max_tasks).await?;
        Ok(tasks)
    }

    /// Internal: scan up to `max_tasks` ready tasks and return them with their keys.
    pub(crate) async fn scan_ready_tasks(
        &self,
        max_tasks: usize,
    ) -> Result<(Vec<Task>, Vec<Vec<u8>>), JobStoreShardError> {
        if max_tasks == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // Scan tasks under tasks/
        let start: Vec<u8> = b"tasks/".to_vec();
        let mut end: Vec<u8> = b"tasks/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        let mut tasks: Vec<Task> = Vec::with_capacity(max_tasks);
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(max_tasks);
        let now_ms = now_epoch_ms();
        while tasks.len() < max_tasks {
            let maybe_kv = iter.next().await?;
            let Some(kv) = maybe_kv else {
                break;
            };

            // Only process task keys: tasks/...
            let key_str = String::from_utf8_lossy(&kv.key);

            // Enforce time cutoff: only keys with ts <= now_ms
            // Format: tasks/<ts>/...
            let mut parts = key_str.split('/');
            if parts.next() != Some("tasks") {
                continue;
            }
            let ts_part = match parts.next() {
                Some(v) => v,
                None => continue,
            };
            if let Ok(ts) = ts_part.parse::<u64>() {
                if ts > now_ms as u64 {
                    continue;
                }
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

    /// Apply concurrency release/grant events to in-memory counts and wake broker.
    /// Called after durably committing releases to update optimistic counts.
    pub(crate) fn apply_concurrency_events(&self, tenant: &str, events: Vec<MemoryEvent>) {
        for ev in events {
            match ev {
                MemoryEvent::Released { queue, task_id } => {
                    self.concurrency
                        .counts()
                        .record_release(tenant, &queue, &task_id);
                    self.broker.wakeup();
                }
                MemoryEvent::Granted { queue, task_id } => {
                    self.concurrency
                        .counts()
                        .record_grant(tenant, &queue, &task_id);
                    self.broker.wakeup();
                }
            }
        }
    }
}
