//! Job enqueue operations.

use slatedb::ErrorKind as SlateErrorKind;
use slatedb::IsolationLevel;
use slatedb::config::WriteOptions;
use tracing::{debug, info_span};
use uuid::Uuid;

use crate::codec::{encode_job_info, encode_job_status};
use crate::concurrency::RequestTicketOutcome;
use crate::dst_events::{self, DstEvent};
use crate::job::{JobInfo, JobStatus, Limit};
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::job_store_shard::helpers::{
    TxnWriter, WriteBatcher, decode_job_status_owned, now_epoch_ms, put_task,
};
use crate::keys::{idx_metadata_key, idx_status_time_key, job_info_key, job_status_key};
use crate::retry::RetryPolicy;
use crate::task::{GubernatorRateLimitData, Task};

impl JobStoreShard {
    /// Enqueue a new job with optional limits (concurrency and/or rate limits).
    /// The payload is stored as raw bytes (MessagePack-encoded by the client).
    ///
    /// Uses a transaction with optimistic concurrency control to atomically check
    /// for duplicate job IDs and write the job. Retries automatically on conflict.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        tenant: &str,
        id: Option<String>,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: Vec<u8>,
        limits: Vec<Limit>,
        metadata: Option<Vec<(String, String)>>,
        task_group: &str,
    ) -> Result<String, JobStoreShardError> {
        const MAX_RETRIES: usize = 5;
        let job_id = id.unwrap_or_else(|| Uuid::new_v4().to_string());

        for attempt in 0..MAX_RETRIES {
            match self
                .enqueue_inner(
                    tenant,
                    &job_id,
                    priority,
                    start_at_ms,
                    retry_policy.clone(),
                    payload.clone(),
                    limits.clone(),
                    metadata.clone(),
                    task_group,
                )
                .await
            {
                Ok(()) => return Ok(job_id),
                Err(JobStoreShardError::JobAlreadyExists(_)) => {
                    // This is a real duplicate, not a transaction conflict, return the error to the caller
                    return Err(JobStoreShardError::JobAlreadyExists(job_id));
                }
                Err(JobStoreShardError::Slate(ref e))
                    if e.kind() == SlateErrorKind::Transaction =>
                {
                    // Transaction conflict - retry with exponential backoff
                    if attempt + 1 < MAX_RETRIES {
                        let delay_ms = 10 * (1 << attempt); // 10ms, 20ms, 40ms, 80ms
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        debug!(
                            job_id = %job_id,
                            attempt = attempt + 1,
                            "enqueue transaction conflict, retrying"
                        );
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(JobStoreShardError::TransactionConflict(
            "enqueue".to_string(),
        ))
    }

    /// Inner implementation of enqueue that runs within a single transaction attempt.
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_inner(
        &self,
        tenant: &str,
        job_id: &str,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: Vec<u8>,
        limits: Vec<Limit>,
        metadata: Option<Vec<(String, String)>>,
        task_group: &str,
    ) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-ENQ-1] If caller provided an id, ensure it doesn't already exist
        // This check is now protected by the transaction - concurrent enqueues with
        // the same ID will result in a transaction conflict.
        let job_info_key = job_info_key(tenant, job_id);
        if txn.get(&job_info_key).await?.is_some() {
            return Err(JobStoreShardError::JobAlreadyExists(job_id.to_string()));
        }

        let job = JobInfo {
            id: job_id.to_string(),
            priority,
            enqueue_time_ms: start_at_ms,
            payload,
            retry_policy,
            metadata: metadata.unwrap_or_default(),
            limits: limits.clone(),
            task_group: task_group.to_string(),
        };
        let job_value = encode_job_info(&job)?;

        let first_task_id = Uuid::new_v4().to_string();
        // If start_at_ms is 0 (the default, which means start now) or in the past, use now_ms as the effective start time
        let effective_start_at_ms = if start_at_ms <= 0 {
            now_ms
        } else {
            start_at_ms
        };

        // [SILO-ENQ-2] Create job with status Scheduled, with next attempt time
        // First attempt is always 1
        let job_status = JobStatus::scheduled(now_ms, effective_start_at_ms, 1);

        // Write job info
        txn.put(&job_info_key, &job_value)?;

        // Maintain metadata secondary index (metadata is immutable post-enqueue)
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, job_id);
            txn.put(&mkey, [])?;
        }

        Self::write_new_job_status_with_index(&mut TxnWriter(&txn), tenant, job_id, job_status)?;

        // Process limits starting from index 0. For concurrency limits, we try immediate
        // grant as an optimization. Returns all grants made for potential rollback.
        let grants = self
            .enqueue_limit_task_at_index(
                &mut TxnWriter(&txn),
                tenant,
                &first_task_id,
                job_id,
                1, // attempt number (total)
                1, // relative attempt number (within run)
                0, // start from first limit
                &limits,
                priority,
                start_at_ms,
                now_ms,
                Vec::new(), // no held queues yet
                task_group,
            )
            .await?;

        // Two-phase DST event: emit before commit for correct causal ordering,
        // confirm after commit succeeds, cancel if commit fails.
        let write_op = dst_events::next_write_op();
        dst_events::emit_pending(
            DstEvent::JobEnqueued {
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
            },
            write_op,
        );

        // Commit durable state — commit_with_options with await_durable:true blocks
        // until the WAL is flushed to object storage, so no separate flush is needed.
        if let Err(e) = txn
            .commit_with_options(&WriteOptions {
                await_durable: true,
            })
            .await
        {
            dst_events::cancel_write(write_op);
            for (queue, task_id) in &grants {
                self.concurrency.rollback_grant(tenant, queue, task_id);
            }
            return Err(e.into());
        }
        dst_events::confirm_write(write_op);

        // Increment total jobs counter for this shard.
        // This is done outside the transaction to avoid conflicts - see counters.rs for details.
        // TODO(slatedb#1254): Move back inside transaction once SlateDB supports key exclusion.
        self.increment_total_jobs_counter().await?;

        // Log grants after durable commit
        for (queue, task_id) in &grants {
            let span = info_span!("concurrency.grant", queue = %queue, task_id = %task_id, job_id = %job_id, attempt = 1u32, source = "immediate");
            let _g = span.enter();
            debug!("granted concurrency slot immediately");
        }

        // If ready now, wake the scanner to refill promptly
        if start_at_ms <= now_epoch_ms() {
            self.broker.wakeup();
        }

        Ok(())
    }

    /// Enqueue a task to begin processing limits starting at `limit_index`.
    ///
    /// This is the unified function for creating limit-processing tasks, used by:
    /// - Initial enqueue (starts at index 0)
    /// - Retry scheduling (starts at index 0 with empty held_queues)
    /// - Subsequent limit processing after passing a rate limit (in dequeue)
    ///
    /// For concurrency limits (regular and floating), this tries immediate grant as an
    /// optimization. If granted, it proceeds to the next limit (iteratively).
    ///
    /// Returns a Vec of all grants made (queue, task_id) for potential rollback if DB write fails.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue_limit_task_at_index<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        attempt_number: u32,
        relative_attempt_number: u32,
        limit_index: usize,
        limits: &[Limit],
        priority: u8,
        start_at_ms: i64,
        now_ms: i64,
        held_queues: Vec<String>,
        task_group: &str,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let mut grants = Vec::new();
        let mut current_index = limit_index;
        let mut current_held_queues = held_queues;
        let mut current_task_id = task_id.to_string();

        loop {
            if current_index >= limits.len() {
                // No more limits - enqueue RunAttempt
                let run_task = Task::RunAttempt {
                    id: current_task_id,
                    tenant: tenant.to_string(),
                    job_id: job_id.to_string(),
                    attempt_number,
                    relative_attempt_number,
                    held_queues: current_held_queues,
                    task_group: task_group.to_string(),
                };
                put_task(
                    writer,
                    task_group,
                    start_at_ms,
                    priority,
                    job_id,
                    attempt_number,
                    &run_task,
                )?;
                return Ok(grants);
            }

            match &limits[current_index] {
                Limit::Concurrency(cl) => {
                    // Try immediate grant for concurrency limits
                    let outcome = self
                        .concurrency
                        .handle_enqueue(
                            &self.db,
                            &self.broker.get_range(),
                            writer,
                            tenant,
                            &current_task_id,
                            job_id,
                            priority,
                            start_at_ms,
                            now_ms,
                            std::slice::from_ref(cl),
                            task_group,
                            attempt_number,
                            relative_attempt_number,
                        )
                        .await?;

                    match outcome {
                        None => {
                            // Granted immediately - record grant and continue to next limit
                            grants.push((cl.key.clone(), current_task_id.clone()));
                            current_held_queues.push(cl.key.clone());
                            current_index += 1;
                            current_task_id = Uuid::new_v4().to_string();
                            // Continue loop to process next limit
                        }
                        Some(_) => {
                            // Not granted - RequestTicket was created by handle_enqueue
                            return Ok(grants);
                        }
                    }
                }

                Limit::FloatingConcurrency(fl) => {
                    // Get/create floating limit state and maybe schedule refresh
                    let state = self
                        .get_or_create_floating_limit_state(writer, tenant, fl)
                        .await?;
                    let refresh_ready = JobStoreShard::floating_limit_refresh_ready(&state, now_ms);

                    // Try immediate grant using current max concurrency
                    let temp_cl = crate::job::ConcurrencyLimit {
                        key: fl.key.clone(),
                        max_concurrency: state.archived().current_max_concurrency,
                    };

                    let outcome = self
                        .concurrency
                        .handle_enqueue(
                            &self.db,
                            &self.broker.get_range(),
                            writer,
                            tenant,
                            &current_task_id,
                            job_id,
                            priority,
                            start_at_ms,
                            now_ms,
                            std::slice::from_ref(&temp_cl),
                            task_group,
                            attempt_number,
                            relative_attempt_number,
                        )
                        .await?;

                    let mut has_waiters =
                        matches!(outcome, Some(RequestTicketOutcome::TicketRequested { .. }));
                    if refresh_ready && !has_waiters {
                        has_waiters = self
                            .has_waiting_concurrency_requests(tenant, &fl.key)
                            .await?;
                    }
                    if refresh_ready {
                        self.maybe_schedule_floating_limit_refresh(
                            writer,
                            tenant,
                            fl,
                            &state,
                            now_ms,
                            task_group,
                            has_waiters,
                        )?;
                    }

                    match outcome {
                        None => {
                            // Granted immediately - record grant and continue to next limit
                            grants.push((fl.key.clone(), current_task_id.clone()));
                            current_held_queues.push(fl.key.clone());
                            current_index += 1;
                            current_task_id = Uuid::new_v4().to_string();
                            // Continue loop to process next limit
                        }
                        Some(_) => {
                            // Not granted - RequestTicket was created by handle_enqueue
                            return Ok(grants);
                        }
                    }
                }

                Limit::RateLimit(rl) => {
                    // Rate limits don't have immediate grant - create CheckRateLimit task
                    let task = Task::CheckRateLimit {
                        task_id: current_task_id,
                        tenant: tenant.to_string(),
                        job_id: job_id.to_string(),
                        attempt_number,
                        relative_attempt_number,
                        limit_index: current_index as u32,
                        rate_limit: GubernatorRateLimitData::from(rl),
                        retry_count: 0,
                        started_at_ms: now_ms,
                        priority,
                        held_queues: current_held_queues,
                        task_group: task_group.to_string(),
                    };
                    put_task(
                        writer,
                        task_group,
                        start_at_ms,
                        priority,
                        job_id,
                        attempt_number,
                        &task,
                    )?;
                    return Ok(grants);
                }
            }
        }
    }

    /// Update job status and maintain secondary indexes.
    pub(crate) async fn set_job_status_with_index<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), JobStoreShardError> {
        // Delete old index entries if present
        if let Some(old_raw) = writer.get(&job_status_key(tenant, job_id)).await? {
            let old = decode_job_status_owned(&old_raw)?;
            let old_kind = old.kind;
            let old_changed = old.changed_at_ms;
            let old_time = idx_status_time_key(tenant, old_kind.as_str(), old_changed, job_id);
            writer.delete(&old_time)?;
        }

        Self::write_job_status_with_index(writer, tenant, job_id, new_status)
    }

    /// Write a new job status + index entry (no old status to clean up).
    /// Use for brand-new jobs where there is no previous status.
    pub(crate) fn write_new_job_status_with_index<W: WriteBatcher>(
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), JobStoreShardError> {
        Self::write_job_status_with_index(writer, tenant, job_id, new_status)
    }

    /// Shared helper: write status value and index entry.
    fn write_job_status_with_index<W: WriteBatcher>(
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), JobStoreShardError> {
        // Write new status value
        let job_status_value = encode_job_status(&new_status)?;
        writer.put(job_status_key(tenant, job_id), &job_status_value)?;

        // Insert new index entries
        let new_kind = new_status.kind;
        let changed = new_status.changed_at_ms;
        let timek = idx_status_time_key(tenant, new_kind.as_str(), changed, job_id);
        writer.put(&timek, [])?;
        Ok(())
    }
}
