//! Task dequeue and processing operations.

use slatedb::{DbIterator, WriteBatch};

use crate::codec::{decode_task, encode_attempt, encode_lease};
use crate::concurrency::{ReleaseGrantRollback, RequestTicketTaskOutcome};
use crate::dst_events::{self, DstEvent};
use crate::job::{JobStatus, JobView};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::job_store_shard::helpers::now_epoch_ms;
use crate::job_store_shard::{DequeueResult, JobStoreShard, JobStoreShardError};
use crate::keys::{attempt_key, decode_tenant, job_info_key, leased_task_key};
use crate::task::{DEFAULT_LEASE_MS, LeaseRecord, LeasedRefreshTask, LeasedTask, Task};
use crate::task_broker::BrokerTask;

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
        let _ts_enter = now_epoch_ms();
        if max_tasks == 0 {
            return Ok(DequeueResult {
                tasks: Vec::new(),
                refresh_tasks: Vec::new(),
            });
        }

        let mut out: Vec<LeasedTask> = Vec::new();
        let mut refresh_out: Vec<LeasedRefreshTask> = Vec::new();
        let mut pending_attempts: Vec<(String, JobView, String, u32)> = Vec::new();

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
            let mut batch = WriteBatch::new();
            let mut ack_keys: Vec<String> = Vec::with_capacity(claimed.len());
            let mut processed_internal = false;

            // Get the shard range for split-aware filtering
            let shard_range = self.get_range();

            for entry in &claimed {
                let task = &entry.task;

                // [SILO-SPLIT-AWARE-1] Check if task's tenant is within shard range
                // Tasks for tenants outside the range are defunct (from before a split)
                let task_tenant = task.tenant();
                let decoded_tenant = decode_tenant(task_tenant);

                if !shard_range.contains(&decoded_tenant) {
                    // Task is for a tenant outside our range - delete and skip
                    batch.delete(entry.key.as_bytes());
                    ack_keys.push(entry.key.clone());
                    tracing::debug!(
                        key = %entry.key,
                        tenant = %decoded_tenant,
                        range = %shard_range,
                        "dequeue: skipping defunct task (tenant outside shard range)"
                    );
                    continue;
                }

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
                        task_group: req_task_group,
                    } => {
                        // Process ticket request internally
                        processed_internal = true;
                        let tenant = tenant.to_string();

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
                                // Track grant for rollback if DB write fails
                                // Grant already recorded by try_reserve in process_ticket_request_task
                                grants_to_rollback.push((
                                    tenant.clone(),
                                    queue.clone(),
                                    request_id.clone(),
                                ));

                                // Create lease and attempt records
                                let run = Task::RunAttempt {
                                    id: request_id.clone(),
                                    tenant: tenant.clone(),
                                    job_id: job_id.clone(),
                                    attempt_number: *attempt_number,
                                    held_queues: vec![queue.clone()],
                                    task_group: req_task_group.clone(),
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
                                let view = job_view.unwrap();
                                pending_attempts.push((
                                    tenant.clone(),
                                    view,
                                    job_id.clone(),
                                    *attempt_number,
                                ));
                                ack_keys.push(entry.key.clone());
                                // Grant already recorded by try_reserve - no need to call record_grant

                                // Track for DST event emission after commit
                                leased_tasks_for_dst.push((
                                    tenant.clone(),
                                    job_id.clone(),
                                    request_id.clone(),
                                ));

                                // Record concurrency ticket metric
                                if let Some(ref m) = self.metrics {
                                    m.record_concurrency_ticket_granted();
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
                        task_group: check_task_group,
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
                                let grants = self
                                    .enqueue_limit_task_at_index(
                                        &mut batch,
                                        &tenant,
                                        check_task_id,
                                        job_id,
                                        *attempt_number,
                                        (*limit_index + 1) as usize, // next limit after current
                                        &job_view.limits(),
                                        *priority,
                                        now_ms,
                                        now_ms,
                                        held_queues.clone(),
                                        check_task_group,
                                    )
                                    .await?;
                                // Track any immediate grants for rollback if DB write fails
                                for (queue, task_id) in grants {
                                    grants_to_rollback.push((tenant.clone(), queue, task_id));
                                }
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
                                    check_task_group,
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
                                    check_task_group,
                                )?;
                            }
                        }
                        continue;
                    }
                    Task::RefreshFloatingLimit {
                        task_id,
                        tenant: task_tenant,
                        queue_key,
                        current_max_concurrency,
                        last_refreshed_at_ms,
                        metadata,
                        task_group: refresh_task_group,
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
                            tenant_id: task_tenant.clone(),
                            queue_key: queue_key.clone(),
                            current_max_concurrency: *current_max_concurrency,
                            last_refreshed_at_ms: *last_refreshed_at_ms,
                            metadata: metadata.clone(),
                            task_group: refresh_task_group.clone(),
                        });
                        ack_keys.push(entry.key.clone());
                        continue;
                    }
                    Task::RunAttempt { .. } => {}
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
                    | Task::RefreshFloatingLimit { .. } => unreachable!(),
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
                            Task::RequestTicket { .. } | Task::RefreshFloatingLimit { .. } => {
                                Vec::new()
                            }
                        };
                        if !held_queues.is_empty() {
                            // release_and_grant_next updates in-memory atomically before returning
                            // Track rollback info in case DB write fails
                            let release_rollbacks = self
                                .concurrency
                                .release_and_grant_next(
                                    &self.db,
                                    &mut batch,
                                    &tenant,
                                    &held_queues,
                                    &task_id,
                                    now_ms,
                                )
                                .await
                                .map_err(JobStoreShardError::Rkyv)?;
                            release_grants_to_rollback.extend(release_rollbacks);
                            tracing::debug!(job_id = %job_id, queues = ?held_queues, "dequeue: released tickets for cancelled job task");
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

                    // Track for DST event emission after commit
                    leased_tasks_for_dst.push((tenant.clone(), job_id.clone(), task_id.clone()));
                } else {
                    // If job missing, delete task key to clean up
                    batch.delete(entry.key.as_bytes());
                    ack_keys.push(entry.key.clone());
                }
            }

            // Try to commit durable state. On failure, rollback grants and requeue tasks.
            if let Err(e) = self.db.write(batch).await {
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
            if let Err(e) = self.db.flush().await {
                // Write succeeded but flush failed - in-memory is correct, don't rollback
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }

            // Wake broker for any release-grants that happened (before clearing)
            if !release_grants_to_rollback.is_empty() {
                self.broker.wakeup();
            }

            // DB write succeeded - clear rollback lists for next iteration
            grants_to_rollback.clear();
            release_grants_to_rollback.clear();

            // Emit DST events for leased tasks after successful commit
            for (tenant, job_id, task_id) in leased_tasks_for_dst.drain(..) {
                dst_events::emit(DstEvent::TaskLeased {
                    tenant: tenant.clone(),
                    job_id: job_id.clone(),
                    task_id,
                    worker_id: worker_id.to_string(),
                });
                dst_events::emit(DstEvent::JobStatusChanged {
                    tenant,
                    job_id,
                    new_status: "Running".to_string(),
                });
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

        // Scan tasks under tasks/{task_group}/
        let prefix = crate::keys::task_group_prefix(task_group);
        let start: Vec<u8> = prefix.as_bytes().to_vec();
        let mut end: Vec<u8> = start.clone();
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

            // Only process task keys: tasks/{task_group}/...
            let key_str = String::from_utf8_lossy(&kv.key);

            // Enforce time cutoff: only keys with ts <= now_ms
            // Format: tasks/<task_group>/<ts>/...
            let mut parts = key_str.split('/');
            if parts.next() != Some("tasks") {
                continue;
            }
            // Skip task_group part
            if parts.next().is_none() {
                continue;
            }
            // Get timestamp part
            let ts_part = match parts.next() {
                Some(v) => v,
                None => continue,
            };
            if let Ok(ts) = ts_part.parse::<u64>()
                && ts > now_ms as u64
            {
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
