//! Job enqueue operations.

use slatedb::ErrorKind as SlateErrorKind;
use slatedb::config::WriteOptions;
use slatedb::{IsolationLevel, WriteBatch};
use tracing::{debug, info_span};
use uuid::Uuid;

use crate::codec::{encode_job_info, encode_job_status};
use crate::concurrency::RequestTicketOutcome;
use crate::dst_events::{self, DstEvent};
use crate::job::{JobInfo, JobStatus, Limit};
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::job_store_shard::helpers::{
    DbWriteBatcher, TxnWriter, WriteBatcher, decode_job_status_owned, now_epoch_ms, put_task,
};
use crate::keys::{idx_metadata_key, idx_status_time_key, job_info_key, job_status_key};
use crate::retry::RetryPolicy;
use crate::task::{GubernatorRateLimitData, Task};

/// Whether a concurrency grant was obtained or the job was queued for later.
enum GrantResult {
    /// Slot granted immediately; caller should advance to the next limit.
    Granted,
    /// Not granted; a RequestTicket/request was written. Caller should return.
    Queued,
}

/// Process the outcome of a `handle_enqueue` call, updating shared loop state.
/// Returns `Granted` if the caller should continue to the next limit, or `Queued`
/// if the caller should return early.
fn record_grant_outcome(
    outcome: Option<RequestTicketOutcome>,
    queue_key: &str,
    grants: &mut Vec<(String, String)>,
    current_task_id: &mut String,
    current_held_queues: &mut Vec<String>,
    current_index: &mut usize,
) -> GrantResult {
    match outcome {
        None => {
            // Granted immediately - record grant and continue to next limit
            grants.push((queue_key.to_string(), current_task_id.clone()));
            current_held_queues.push(queue_key.to_string());
            *current_index += 1;
            *current_task_id = Uuid::new_v4().to_string();
            GrantResult::Granted
        }
        Some(_) => {
            // Not granted - RequestTicket was created by handle_enqueue
            GrantResult::Queued
        }
    }
}

impl JobStoreShard {
    /// Enqueue a new job with optional limits (concurrency and/or rate limits).
    /// The payload is stored as raw bytes (MessagePack-encoded by the client).
    ///
    /// When the caller provides an explicit job ID, uses a transaction with optimistic
    /// concurrency control to atomically check for duplicates and write the job.
    /// When no ID is provided (auto-generated UUID), uses a faster WriteBatch path
    /// since UUID collisions are not a concern.
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
        if let Some(user_id) = id {
            self.enqueue_with_dedup(
                tenant,
                user_id,
                priority,
                start_at_ms,
                retry_policy,
                payload,
                limits,
                metadata,
                task_group,
            )
            .await
        } else {
            let job_id = Uuid::new_v4().to_string();
            self.enqueue_batch(
                tenant,
                &job_id,
                priority,
                start_at_ms,
                retry_policy,
                payload,
                limits,
                metadata,
                task_group,
            )
            .await?;
            Ok(job_id)
        }
    }

    /// Fast-path enqueue using WriteBatch for auto-generated job IDs.
    /// No transaction overhead since UUID collisions are not a concern.
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_batch(
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
        let mut batch = WriteBatch::new();
        let grants = self
            .write_enqueue_data(
                &mut DbWriteBatcher {
                    db: &self.db,
                    batch: &mut batch,
                },
                tenant,
                job_id,
                priority,
                start_at_ms,
                retry_policy,
                payload,
                limits,
                metadata,
                task_group,
            )
            .await?;

        // Include counter in the same batch for efficiency
        self.increment_total_jobs_counter_batch(&mut batch);

        // Two-phase DST event: emit before write for correct causal ordering,
        // confirm after write succeeds, cancel if write fails.
        let write_op = dst_events::next_write_op();
        dst_events::emit_pending(
            DstEvent::JobEnqueued {
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
            },
            write_op,
        );

        if let Err(e) = self
            .db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                },
            )
            .await
        {
            dst_events::cancel_write(write_op);
            self.rollback_grants(tenant, &grants);
            return Err(e.into());
        }
        dst_events::confirm_write(write_op);

        self.finish_enqueue(job_id, start_at_ms, &grants).await
    }

    /// Transaction-path enqueue for user-provided job IDs that need deduplication.
    /// Uses SerializableSnapshot to atomically check for duplicates and write.
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_with_dedup(
        &self,
        tenant: &str,
        job_id: String,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: Vec<u8>,
        limits: Vec<Limit>,
        metadata: Option<Vec<(String, String)>>,
        task_group: &str,
    ) -> Result<String, JobStoreShardError> {
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            match self
                .enqueue_txn(
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
                    return Err(JobStoreShardError::JobAlreadyExists(job_id));
                }
                Err(JobStoreShardError::Slate(ref e))
                    if e.kind() == SlateErrorKind::Transaction =>
                {
                    if attempt + 1 < MAX_RETRIES {
                        let delay_ms = 10 * (1 << attempt);
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

    /// Inner transaction-based enqueue for a single attempt.
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_txn(
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
        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-ENQ-1] If caller provided an id, ensure it doesn't already exist
        let info_key = job_info_key(tenant, job_id);
        if txn.get(&info_key).await?.is_some() {
            return Err(JobStoreShardError::JobAlreadyExists(job_id.to_string()));
        }

        let grants = self
            .write_enqueue_data(
                &mut TxnWriter(&txn),
                tenant,
                job_id,
                priority,
                start_at_ms,
                retry_policy,
                payload,
                limits,
                metadata,
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
            self.rollback_grants(tenant, &grants);
            return Err(e.into());
        }
        dst_events::confirm_write(write_op);

        // Counter must be outside transaction to avoid conflicts on shared counter key
        self.increment_total_jobs_counter().await?;

        self.finish_enqueue(job_id, start_at_ms, &grants).await
    }

    /// Write all enqueue data (job info, metadata, status, limit tasks) to the writer.
    /// Shared between the WriteBatch and transaction enqueue paths.
    #[allow(clippy::too_many_arguments)]
    async fn write_enqueue_data<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: Vec<u8>,
        limits: Vec<Limit>,
        metadata: Option<Vec<(String, String)>>,
        task_group: &str,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let now_ms = now_epoch_ms();

        let job = JobInfo {
            id: job_id.to_string(),
            priority,
            enqueue_time_ms: start_at_ms,
            payload,
            retry_policy,
            metadata: metadata.unwrap_or_default(),
            limits,
            task_group: task_group.to_string(),
        };
        let job_value = encode_job_info(&job)?;

        let first_task_id = Uuid::new_v4().to_string();
        let effective_start_at_ms = if start_at_ms <= 0 {
            now_ms
        } else {
            start_at_ms
        };

        // [SILO-ENQ-2] Create job with status Scheduled, with next attempt time
        let job_status = JobStatus::scheduled(now_ms, effective_start_at_ms, 1);

        writer.put(job_info_key(tenant, job_id), &job_value)?;

        // Maintain metadata secondary index
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, job_id);
            writer.put(&mkey, [])?;
        }

        Self::write_new_job_status_with_index(writer, tenant, job_id, job_status)?;

        self.enqueue_limit_task_at_index(
            writer,
            tenant,
            &first_task_id,
            job_id,
            1,
            1,
            0,
            &job.limits,
            priority,
            start_at_ms,
            now_ms,
            Vec::new(),
            task_group,
        )
        .await
    }

    /// Complete an enqueue after successful write/commit and DST event emission.
    /// Flushes to disk, logs grants, and wakes up the broker.
    async fn finish_enqueue(
        &self,
        job_id: &str,
        start_at_ms: i64,
        grants: &[(String, String)],
    ) -> Result<(), JobStoreShardError> {
        self.db.flush().await?;

        for (queue, task_id) in grants {
            let span = info_span!("concurrency.grant", queue = %queue, task_id = %task_id, job_id = %job_id, attempt = 1u32, source = "immediate");
            let _g = span.enter();
            debug!("granted concurrency slot immediately");
        }

        if start_at_ms <= now_epoch_ms() {
            self.broker.wakeup();
        }

        Ok(())
    }

    /// Rollback in-memory concurrency grants after a failed write/commit.
    fn rollback_grants(&self, tenant: &str, grants: &[(String, String)]) {
        for (queue, task_id) in grants {
            self.concurrency.rollback_grant(tenant, queue, task_id);
        }
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

                    if matches!(
                        record_grant_outcome(
                            outcome,
                            &cl.key,
                            &mut grants,
                            &mut current_task_id,
                            &mut current_held_queues,
                            &mut current_index,
                        ),
                        GrantResult::Queued
                    ) {
                        return Ok(grants);
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

                    if matches!(
                        record_grant_outcome(
                            outcome,
                            &fl.key,
                            &mut grants,
                            &mut current_task_id,
                            &mut current_held_queues,
                            &mut current_index,
                        ),
                        GrantResult::Queued
                    ) {
                        return Ok(grants);
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
