//! Job enqueue operations.

use serde_json::Value as JsonValue;
use slatedb::WriteBatch;
use uuid::Uuid;

use crate::codec::{encode_job_info, encode_job_status};
use crate::concurrency::{MemoryEvent, RequestTicketOutcome};
use crate::job::{JobInfo, JobStatus, Limit};
use crate::job_store_shard::helpers::{now_epoch_ms, put_task};
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::keys::{idx_metadata_key, idx_status_time_key, job_info_key, job_status_key};
use crate::retry::RetryPolicy;
use crate::task::{GubernatorRateLimitData, Task};
use tracing::info_span;

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
        // If start_at_ms is 0 (the default, which means start now) or in the past, use now_ms as the effective start time
        let effective_start_at_ms = if start_at_ms <= 0 {
            now_ms
        } else {
            start_at_ms
        };

        // [SILO-ENQ-2] Create job with status Scheduled, with next attempt time
        let job_status = JobStatus::scheduled(now_ms, effective_start_at_ms);

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
                // First limit is a concurrency limit - use existing logic
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

                // If no concurrency limits blocked, check if there are more limits
                if concurrency_outcome.is_none() {
                    // Granted immediately, but we need to proceed to next limit
                    self.enqueue_next_limit_task(
                        &mut batch,
                        tenant,
                        &first_task_id,
                        &job_id,
                        1, // attempt number
                        0, // current limit index
                        &limits,
                        priority,
                        start_at_ms,
                        now_ms,
                        vec![cl.key.clone()], // held queues
                    )?;
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
                // First limit is a floating concurrency limit
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
                        tenant,
                        &first_task_id,
                        &job_id,
                        1, // attempt number
                        0, // current limit index
                        &limits,
                        priority,
                        start_at_ms,
                        now_ms,
                        vec![fl.key.clone()], // held queues
                    )?;
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

    /// Helper to enqueue the next limit check task after passing a limit
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn enqueue_next_limit_task(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        attempt_number: u32,
        current_limit_index: u32,
        limits: &[Limit],
        priority: u8,
        start_at_ms: i64,
        now_ms: i64,
        held_queues: Vec<String>,
    ) -> Result<(), JobStoreShardError> {
        let next_index = current_limit_index + 1;

        if next_index as usize >= limits.len() {
            // No more limits - enqueue RunAttempt
            let run_task = Task::RunAttempt {
                id: task_id.to_string(),
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                held_queues,
            };
            return put_task(
                batch,
                start_at_ms,
                priority,
                job_id,
                attempt_number,
                &run_task,
            );
        }

        // Enqueue task for the next limit
        let next_task = match &limits[next_index as usize] {
            Limit::Concurrency(cl) => Task::RequestTicket {
                queue: cl.key.clone(),
                start_time_ms: start_at_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                request_id: Uuid::new_v4().to_string(),
            },
            Limit::RateLimit(rl) => Task::CheckRateLimit {
                task_id: Uuid::new_v4().to_string(),
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                limit_index: next_index,
                rate_limit: GubernatorRateLimitData::from(rl),
                retry_count: 0,
                started_at_ms: now_ms,
                priority,
                held_queues,
            },
            // Floating concurrency limits use the same RequestTicket mechanism as regular concurrency limits
            Limit::FloatingConcurrency(fl) => Task::RequestTicket {
                queue: fl.key.clone(),
                start_time_ms: start_at_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                request_id: Uuid::new_v4().to_string(),
            },
        };
        put_task(
            batch,
            start_at_ms,
            priority,
            job_id,
            attempt_number,
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
