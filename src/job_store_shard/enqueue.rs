//! Job enqueue operations.

use serde_json::Value as JsonValue;
use slatedb::WriteBatch;
use uuid::Uuid;

use crate::codec::{encode_job_info, encode_job_status};
use crate::concurrency::{MemoryEvent, RequestTicketOutcome};
use crate::job::{JobInfo, JobStatus, Limit};
use crate::job_store_shard::helpers::{self, now_epoch_ms, put_task};
use crate::job_store_shard::{JobStoreShardError, JobStoreShard};
use crate::keys::{idx_metadata_key, idx_status_time_key, job_info_key, job_status_key};
use crate::retry::RetryPolicy;
use crate::routing::queue_to_shard;
use crate::task::{GubernatorRateLimitData, Task};
use tracing::info_span;

/// Context for creating the next limit task in a job's limit chain.
/// Groups related parameters to reduce argument count in `enqueue_next_limit_task`.
pub(crate) struct NextLimitContext<'a> {
    pub tenant: &'a str,
    pub task_id: &'a str,
    pub job_id: &'a str,
    pub attempt_number: u32,
    pub current_limit_index: u32,
    pub limits: &'a [Limit],
    pub priority: u8,
    pub start_at_ms: i64,
    pub now_ms: i64,
    pub held_queues: Vec<String>,
}

impl JobStoreShard {
    /// Enqueue a new job with optional limits (concurrency and/or rate limits).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        tenant: &str,
        id: Option<String>,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: JsonValue,
        limits: Vec<Limit>,
        metadata: Option<Vec<(String, String)>>,
    ) -> Result<String, JobStoreShardError> {
        let job_id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        // [SILO-ENQ-1] If caller provided an id, ensure it doesn't already exist
        if self
            .db
            .get(job_info_key(tenant, &job_id).as_bytes())
            .await?
            .is_some()
        {
            return Err(JobStoreShardError::JobAlreadyExists(job_id));
        }
        let payload_bytes = serde_json::to_vec(&payload)?;

        let job = JobInfo {
            id: job_id.clone(),
            priority,
            enqueue_time_ms: start_at_ms,
            payload: payload_bytes,
            retry_policy,
            metadata: metadata.unwrap_or_default(),
            limits: limits.clone(),
        };
        let job_value = encode_job_info(&job)?;

        let first_task_id = Uuid::new_v4().to_string();
        let now_ms = now_epoch_ms();
        // [SILO-ENQ-2] Create job with status Scheduled
        let job_status = JobStatus::scheduled(now_ms);

        // Atomically write job info, job status, and handle first limit
        let mut batch = WriteBatch::new();
        batch.put(job_info_key(tenant, &job_id).as_bytes(), &job_value);
        // Maintain metadata secondary index (metadata is immutable post-enqueue)
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, &job_id);
            batch.put(mkey.as_bytes(), []);
        }
        self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
            .await?;

        // Process the first limit in the list
        let first_limit = limits.first();
        let mut concurrency_outcome: Option<RequestTicketOutcome> = None;

        match first_limit {
            None => {
                // No limits - write RunAttempt task directly
                let first_task = Task::RunAttempt {
                    id: first_task_id.clone(),
                    tenant: tenant.to_string(),
                    job_id: job_id.clone(),
                    attempt_number: 1,
                    held_queues: Vec::new(),
                };
                put_task(&mut batch, start_at_ms, priority, &job_id, 1, &first_task)?;
            }
            Some(Limit::Concurrency(cl)) => {
                // Check if the queue is local or remote
                let queue_shard = queue_to_shard(tenant, &cl.key, self.num_shards);
                let is_local = queue_shard == self.shard_number;

                if is_local {
                    // Local queue - use existing concurrency logic for immediate grant attempt
                    concurrency_outcome = self
                        .concurrency
                        .handle_enqueue(
                            &mut batch,
                            tenant,
                            &first_task_id,
                            &job_id,
                            priority,
                            start_at_ms,
                            now_ms,
                            std::slice::from_ref(cl),
                        )
                        .map_err(JobStoreShardError::Rkyv)?;

                    // If concurrency was granted immediately, we need to proceed to next limit
                    // (if any). Note: handle_enqueue returns Some(GrantedImmediately{...}) when
                    // granted, not None. The GrantedImmediately case writes a RunAttempt task
                    // which we need to replace with the next limit task if there are more limits.
                    if let Some(RequestTicketOutcome::GrantedImmediately { .. }) = &concurrency_outcome
                    {
                        self.enqueue_next_limit_task(
                            &mut batch,
                            NextLimitContext {
                                tenant,
                                task_id: &first_task_id,
                                job_id: &job_id,
                                attempt_number: 1,
                                current_limit_index: 0,
                                limits: &limits,
                                priority,
                                start_at_ms,
                                now_ms,
                                held_queues: vec![cl.key.clone()],
                            },
                        )?;
                    }
                } else {
                    // Remote queue - use helper to create RequestRemoteTicket task
                    let request_task = helpers::create_ticket_request_task_with_batch(
                        &mut batch,
                        first_limit.unwrap(),
                        tenant,
                        &job_id,
                        1, // attempt_number
                        priority,
                        start_at_ms,
                        &first_task_id,
                        Vec::new(), // empty held_queues for first limit
                        self.shard_number,
                        self.num_shards,
                    )?;
                    put_task(&mut batch, start_at_ms, priority, &job_id, 1, &request_task)?;
                }
            }
            Some(Limit::RateLimit(rl)) => {
                // First limit is a rate limit - create CheckRateLimit task
                let check_task = Task::CheckRateLimit {
                    task_id: first_task_id.clone(),
                    tenant: tenant.to_string(),
                    job_id: job_id.clone(),
                    attempt_number: 1,
                    limit_index: 0,
                    rate_limit: GubernatorRateLimitData::from(rl),
                    retry_count: 0,
                    started_at_ms: now_ms,
                    priority,
                    held_queues: Vec::new(),
                };
                put_task(&mut batch, start_at_ms, priority, &job_id, 1, &check_task)?;
            }
            Some(Limit::FloatingConcurrency(fl)) => {
                // Check if the queue is local or remote
                let queue_shard = queue_to_shard(tenant, &fl.key, self.num_shards);
                let is_local = queue_shard == self.shard_number;

                if is_local {
                    // Local queue - use existing floating concurrency logic
                    // Get or create the floating limit state (zero-copy for existing)
                    let state = self
                        .get_or_create_floating_limit_state(&mut batch, tenant, fl)
                        .await?;

                    // Maybe schedule a refresh task if needed
                    self.maybe_schedule_floating_limit_refresh(&mut batch, tenant, fl, &state, now_ms)?;

                    // Create a temporary ConcurrencyLimit with the current max concurrency
                    let temp_cl = crate::job::ConcurrencyLimit {
                        key: fl.key.clone(),
                        max_concurrency: state.archived().current_max_concurrency,
                    };

                    // Use the concurrency system with the current floating limit value
                    concurrency_outcome = self
                        .concurrency
                        .handle_enqueue(
                            &mut batch,
                            tenant,
                            &first_task_id,
                            &job_id,
                            priority,
                            start_at_ms,
                            now_ms,
                            std::slice::from_ref(&temp_cl),
                        )
                        .map_err(JobStoreShardError::Rkyv)?;

                    // If no concurrency limits blocked, check if there are more limits
                    if concurrency_outcome.is_none() {
                        // Granted immediately, but we need to proceed to next limit
                        self.enqueue_next_limit_task(
                            &mut batch,
                            NextLimitContext {
                                tenant,
                                task_id: &first_task_id,
                                job_id: &job_id,
                                attempt_number: 1,
                                current_limit_index: 0,
                                limits: &limits,
                                priority,
                                start_at_ms,
                                now_ms,
                                held_queues: vec![fl.key.clone()],
                            },
                        )?;
                    }
                } else {
                    // Remote queue - use helper to create RequestRemoteTicket task
                    let request_task = helpers::create_ticket_request_task_with_batch(
                        &mut batch,
                        first_limit.unwrap(),
                        tenant,
                        &job_id,
                        1, // attempt_number
                        priority,
                        start_at_ms,
                        &first_task_id,
                        Vec::new(), // empty held_queues for first limit
                        self.shard_number,
                        self.num_shards,
                    )?;
                    put_task(&mut batch, start_at_ms, priority, &job_id, 1, &request_task)?;
                }
            }
        }

        self.db.write(batch).await?;
        self.db.flush().await?;

        // Apply memory events and log after durable commit
        if let Some(outcome) = concurrency_outcome {
            match outcome {
                RequestTicketOutcome::GrantedImmediately { events, .. } => {
                    for ev in events {
                        if let MemoryEvent::Granted { queue, task_id } = ev {
                            let span = info_span!("concurrency.grant", queue = %queue, task_id = %task_id, job_id = %job_id, attempt = 1u32, source = "immediate");
                            let _g = span.enter();
                            self.concurrency
                                .counts()
                                .record_grant(tenant, &queue, &task_id);
                        }
                    }
                }
                RequestTicketOutcome::TicketRequested { queue } => {
                    let span = info_span!("concurrency.request", queue = %queue, job_id = %job_id, attempt = 1u32, start_at_ms = start_at_ms, priority = priority);
                    let _g = span.enter();
                }
                RequestTicketOutcome::FutureRequestTaskWritten { queue, .. } => {
                    let span = info_span!("concurrency.ticket", queue = %queue, job_id = %job_id, attempt = 1u32, start_at_ms = start_at_ms, priority = priority);
                    let _g = span.enter();
                }
            }
        }

        // If ready now, wake the scanner to refill promptly
        if start_at_ms <= now_epoch_ms() {
            self.broker.wakeup();
        }

        Ok(job_id)
    }

    /// Helper to enqueue the next limit check task after passing a limit.
    /// Uses `NextLimitContext` to group related parameters.
    pub(crate) fn enqueue_next_limit_task(
        &self,
        batch: &mut WriteBatch,
        ctx: NextLimitContext<'_>,
    ) -> Result<(), JobStoreShardError> {
        let next_index = ctx.current_limit_index + 1;

        if next_index as usize >= ctx.limits.len() {
            // No more limits - enqueue RunAttempt
            let run_task = Task::RunAttempt {
                id: ctx.task_id.to_string(),
                tenant: ctx.tenant.to_string(),
                job_id: ctx.job_id.to_string(),
                attempt_number: ctx.attempt_number,
                held_queues: ctx.held_queues,
            };
            return put_task(
                batch,
                ctx.start_at_ms,
                ctx.priority,
                ctx.job_id,
                ctx.attempt_number,
                &run_task,
            );
        }

        // Enqueue task for the next limit
        // IMPORTANT: We use the same task_id for all holders in this attempt chain.
        // This ensures that when the job completes and releases held_queues, we can
        // find all the holders by the same task_id.
        let next_limit = &ctx.limits[next_index as usize];
        let next_task = match next_limit {
            // Concurrency limits (fixed and floating) use the same routing logic
            Limit::Concurrency(_) | Limit::FloatingConcurrency(_) => {
                helpers::create_ticket_request_task_with_batch(
                    batch,
                    next_limit,
                    ctx.tenant,
                    ctx.job_id,
                    ctx.attempt_number,
                    ctx.priority,
                    ctx.start_at_ms,
                    ctx.task_id,
                    ctx.held_queues,
                    self.shard_number,
                    self.num_shards,
                )?
            }
            Limit::RateLimit(rl) => Task::CheckRateLimit {
                task_id: ctx.task_id.to_string(),
                tenant: ctx.tenant.to_string(),
                job_id: ctx.job_id.to_string(),
                attempt_number: ctx.attempt_number,
                limit_index: next_index,
                rate_limit: GubernatorRateLimitData::from(rl),
                retry_count: 0,
                started_at_ms: ctx.now_ms,
                priority: ctx.priority,
                held_queues: ctx.held_queues,
            },
        };
        put_task(
            batch,
            ctx.start_at_ms,
            ctx.priority,
            ctx.job_id,
            ctx.attempt_number,
            &next_task,
        )
    }

    /// Update job status and maintain secondary indexes in the same write batch.
    pub(crate) async fn set_job_status_with_index(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), JobStoreShardError> {
        // Delete old index entries if present
        if let Some(old) = self.get_job_status(tenant, job_id).await? {
            let old_kind = old.kind;
            let old_changed = old.changed_at_ms;
            let old_time = idx_status_time_key(tenant, old_kind.as_str(), old_changed, job_id);
            batch.delete(old_time.as_bytes());
        }

        // Write new status value
        let job_status_value = encode_job_status(&new_status)?;
        batch.put(job_status_key(tenant, job_id).as_bytes(), &job_status_value);

        // Insert new index entries
        let new_kind = new_status.kind;
        let changed = new_status.changed_at_ms;
        let timek = idx_status_time_key(tenant, new_kind.as_str(), changed, job_id);
        batch.put(timek.as_bytes(), []);
        Ok(())
    }
}
