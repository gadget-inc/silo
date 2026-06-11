//! Job enqueue operations.

use slatedb::config::WriteOptions;
use slatedb::{IsolationLevel, WriteBatch};
use tracing::{debug, info_span};
use uuid::Uuid;

use crate::codec::{encode_job_info, encode_job_status};
use crate::concurrency::RequestTicketOutcome;
use crate::dst_events::{self, DstEvent};
use crate::job::{JobInfo, JobStatus, JobStatusKind, JobView, Limit};
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::job_store_shard::counters::{
    BackgroundActionMetricTransition, background_action_queue_counter_transition_is_relevant,
    encode_counter,
};
use crate::job_store_shard::helpers::{
    DbWriteBatcher, TxnWriter, WriteBatcher, decode_job_status_owned, now_epoch_ms, put_task,
    retry_on_txn_conflict,
};
use crate::keys::{
    idx_metadata_key, idx_status_time_key, job_info_key, job_status_key, status_index_timestamp,
    tenant_status_counter_key,
};
use crate::retry::RetryPolicy;
use crate::task::{GubernatorRateLimitData, Task};

/// Parameters for creating limit-processing tasks.
/// Bundles the many fields needed by `enqueue_limit_task_at_index` into a single struct.
pub(crate) struct LimitTaskParams<'a> {
    pub tenant: &'a str,
    pub task_id: &'a str,
    pub job_id: &'a str,
    pub attempt_number: u32,
    pub relative_attempt_number: u32,
    pub limit_index: usize,
    pub limits: &'a [Limit],
    pub priority: u8,
    /// The job's user-requested start time. This is the `start_time_ms`
    /// component baked into every `task_key` / `concurrency_request_key` this
    /// chain step writes (so the broker orders the task by its logical schedule)
    /// AND the predicate for "is this future-scheduled?" (`scheduled_at_ms >
    /// now_ms` ⇒ write a `Task::RequestTicket` at that future time). It is
    /// persisted as `start_time_ms` on `EnqueueTask` so `process_grants` can
    /// skip future-dated requests on resume. A chain *resume* keeps the job's
    /// original schedule here — the tombstone dodge lives in `task_key_epoch_ms`.
    pub scheduled_at_ms: i64,
    /// Write-time disambiguator for the trailing `epoch_ms` of every `task_key`
    /// this chain step writes. It does NOT affect broker ordering; it only makes
    /// a rewritten task_key unique versus the broker ack-tombstone left by an
    /// earlier ack-delete at the same `(start_time_ms, …)`. For a fresh chain
    /// head use `now_ms`; for a rewrite of a just-deleted predecessor use
    /// `now_ms.max(parent_epoch_ms + 1)`. See
    /// `project_broker_tombstone_chain_continuation` and the precedent in
    /// `handle_request_ticket` (dequeue.rs).
    pub task_key_epoch_ms: i64,
    pub now_ms: i64,
    pub held_queues: Vec<String>,
    pub task_group: &'a str,
    /// When true, skip try_reserve and always go through the request queue.
    /// Used for retry scheduling where the old holder is still in-memory (released post-commit).
    /// This matches the Alloy model's completeFailureRetryReleaseTicket which creates a
    /// TicketRequest, not an immediate holder.
    pub skip_try_reserve: bool,
}

/// Result of walking a job's remaining limits.
pub(crate) struct LimitTaskWriteResult {
    /// In-memory grants made while walking the chain. Callers must roll these
    /// back if their surrounding durable write fails.
    pub grants: Vec<(String, String)>,
    /// The task-key timestamp when this walk wrote a concrete DB task
    /// (`RunAttempt`, `CheckRateLimit`, or future `RequestTicket`). `None`
    /// means the walk parked on a durable concurrency request instead.
    pub pending_task_key_start_ms: Option<i64>,
}

/// Whether a concurrency grant was obtained or the job was queued for later.
enum GrantResult {
    /// Slot granted immediately; caller should advance to the next limit.
    Granted,
    /// Not granted; a RequestTicket/request was written. Caller should return.
    Queued {
        pending_task_key_start_ms: Option<i64>,
    },
}

/// Process the outcome of a `handle_enqueue` call, updating shared loop state.
/// Returns `Granted` if the caller should continue to the next limit, or `Queued`
/// if the caller should return early.
fn record_grant_outcome(
    outcome: Option<RequestTicketOutcome>,
    queue_key: &str,
    scheduled_at_ms: i64,
    grants: &mut Vec<(String, String)>,
    current_task_id: &str,
    current_held_queues: &mut Vec<String>,
    current_index: &mut usize,
) -> GrantResult {
    match outcome {
        None | Some(RequestTicketOutcome::GrantedImmediately { .. }) => {
            // Slot granted (or no limit existed) — record the grant and
            // continue. `handle_enqueue`'s `GrantedImmediately` path only
            // writes the holder DB key; the `RunAttempt` `task_key` is
            // written exactly once by the outer loop's terminal "no more
            // limits" branch, carrying the full accumulated `held_queues`.
            // This guarantees the broker can never observe a `RunAttempt`
            // with a partial `held_queues`, which would otherwise let a
            // worker complete a task and under-release holders.
            //
            // Crucially, `current_task_id` is NOT cycled here. The chained
            // cleanup paths (handle_check_rate_limit, handle_run_attempt) all
            // release holders by `concurrency_holder_key(tenant, queue,
            // <chained_task_id>)`, so every holder we accumulate across the
            // limit chain must share one task_id — the one carried forward to
            // the final RunAttempt / CheckRateLimit task. Cycling here left
            // earlier-granted holders orphaned and produced leaked slots on
            // outcome release.
            grants.push((queue_key.to_string(), current_task_id.to_string()));
            current_held_queues.push(queue_key.to_string());
            *current_index += 1;
            GrantResult::Granted
        }
        Some(RequestTicketOutcome::TicketRequested { .. }) => {
            // Not granted - a request/RequestTicket was already written by
            // handle_enqueue. Stop processing further limits; remaining ones
            // will be applied when the queued request is granted later.
            GrantResult::Queued {
                pending_task_key_start_ms: None,
            }
        }
        Some(RequestTicketOutcome::FutureRequestTaskWritten { .. }) => GrantResult::Queued {
            pending_task_key_start_ms: Some(scheduled_at_ms),
        },
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
    #[tracing::instrument(skip_all, fields(shard = %self.name))]
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
        let background_action_metadata = metadata.clone().unwrap_or_default();
        let grants = self
            .write_enqueue_data(
                &mut DbWriteBatcher::new(&self.db, &mut batch),
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
        if let Err(e) =
            self.increment_total_jobs_counter(&mut DbWriteBatcher::new(&self.db, &mut batch))
        {
            self.rollback_background_action_queue_counter_transition(
                tenant,
                task_group,
                None,
                JobStatusKind::Scheduled,
                &background_action_metadata,
            );
            return Err(e);
        }

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
                    ..Default::default()
                },
            )
            .await
        {
            dst_events::cancel_write(write_op);
            self.rollback_grants(tenant, &grants);
            self.rollback_background_action_queue_counter_transition(
                tenant,
                task_group,
                None,
                JobStatusKind::Scheduled,
                &background_action_metadata,
            );
            return Err(e.into());
        }
        dst_events::confirm_write(write_op);

        self.finish_enqueue(job_id, task_group, start_at_ms, &grants)
            .await
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
        retry_on_txn_conflict("enqueue", || {
            self.enqueue_txn(
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
        })
        .await?;
        Ok(job_id)
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
        let background_action_metadata = metadata.clone().unwrap_or_default();

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

        // Include counter in the transaction (unmark_write excludes it from conflict detection)
        if let Err(e) = self.increment_total_jobs_counter(&mut TxnWriter(&txn)) {
            self.rollback_background_action_queue_counter_transition(
                tenant,
                task_group,
                None,
                JobStatusKind::Scheduled,
                &background_action_metadata,
            );
            return Err(e);
        }

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
                ..Default::default()
            })
            .await
        {
            dst_events::cancel_write(write_op);
            self.rollback_grants(tenant, &grants);
            self.rollback_background_action_queue_counter_transition(
                tenant,
                task_group,
                None,
                JobStatusKind::Scheduled,
                &background_action_metadata,
            );
            return Err(e.into());
        }
        dst_events::confirm_write(write_op);

        self.finish_enqueue(job_id, task_group, start_at_ms, &grants)
            .await
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
        let job_value = encode_job_info(&job);

        let first_task_id = Uuid::new_v4().to_string();
        let effective_start_at_ms = if start_at_ms <= 0 {
            now_ms
        } else {
            start_at_ms
        };

        // [SILO-ENQ-2] Create job with status Scheduled, with next attempt time
        let job_status = JobStatus::scheduled(now_ms, effective_start_at_ms, 1);
        let job_status_kind = job_status.kind;

        writer.put(job_info_key(tenant, job_id), &job_value)?;

        // Maintain metadata secondary index
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, job_id);
            writer.put(&mkey, [])?;
        }

        Self::write_job_status_with_index_and_metadata(writer, tenant, job_id, job_status)?;

        let result = self
            .enqueue_limit_task_at_index(
                writer,
                LimitTaskParams {
                    tenant,
                    task_id: &first_task_id,
                    job_id,
                    attempt_number: 1,
                    relative_attempt_number: 1,
                    limit_index: 0,
                    limits: &job.limits,
                    priority,
                    // Fresh enqueue: the task_key starts at the job's schedule. If
                    // the job is future-scheduled (`start_at_ms > now_ms`) the
                    // resulting `RequestTicket` lands at the future time so the
                    // broker picks it up exactly then. Use `effective_start_at_ms`
                    // (not the raw `start_at_ms`) so an immediate enqueue
                    // (`start_at_ms <= 0`) keys the task at `now_ms` — matching the
                    // `effective_start_at_ms` the JobStatus stores and the import
                    // path, so identity lookups never have to fall back to time=0.
                    // Fresh chain head → epoch is just the write time; no
                    // predecessor tombstone to dodge.
                    scheduled_at_ms: effective_start_at_ms,
                    task_key_epoch_ms: now_ms,
                    now_ms,
                    held_queues: Vec::new(),
                    task_group,
                    skip_try_reserve: false,
                },
            )
            .await
            .map(|result| result.grants)?;
        self.apply_background_action_queue_counter_transition(
            tenant,
            task_group,
            None,
            job_status_kind,
            &job.metadata,
        );
        Ok(result)
    }

    /// Complete an enqueue after successful write/commit and DST event emission.
    /// Logs grants and wakes up the broker for the given task group.
    pub(crate) async fn finish_enqueue(
        &self,
        job_id: &str,
        task_group: &str,
        start_at_ms: i64,
        grants: &[(String, String)],
    ) -> Result<(), JobStoreShardError> {
        for (queue, task_id) in grants {
            let span = info_span!("concurrency.grant", queue = %queue, task_id = %task_id, job_id = %job_id, attempt = 1u32, source = "immediate");
            let _g = span.enter();
            debug!("granted concurrency slot immediately");
        }

        if start_at_ms <= now_epoch_ms() {
            self.brokers.wakeup(task_group);
        }

        Ok(())
    }

    /// Rollback in-memory concurrency grants after a failed write/commit.
    pub(crate) fn rollback_grants(&self, tenant: &str, grants: &[(String, String)]) {
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
    /// Returns all grants made for potential rollback if DB write fails, plus
    /// the concrete task-key timestamp when this walk wrote a DB task.
    ///
    /// **Error semantics:** if the walk errors mid-chain (e.g. a db.get for
    /// floating-limit state fails between an immediate grant and the next
    /// limit), any in-memory reservations made up to that point are released
    /// here before the error is returned. Callers therefore see either
    /// `Ok(grants)` — every entry is a live in-memory reservation paired with
    /// a pending batch write — or `Err(_)` with no leftover in-memory state to
    /// clean up. This avoids leaking holders when an interior `?` fires after
    /// a successful `try_reserve` but before the batch commits.
    pub(crate) async fn enqueue_limit_task_at_index<W: WriteBatcher>(
        &self,
        writer: &mut W,
        params: LimitTaskParams<'_>,
    ) -> Result<LimitTaskWriteResult, JobStoreShardError> {
        // Save the tenant ref so we can roll back grants if walk_limit_chain errors.
        // (references are Copy, so this doesn't conflict with moving `params`.)
        let tenant_for_rollback: &str = params.tenant;
        let mut grants: Vec<(String, String)> = Vec::new();
        match self.walk_limit_chain(writer, params, &mut grants).await {
            Ok(pending_task_key_start_ms) => Ok(LimitTaskWriteResult {
                grants,
                pending_task_key_start_ms,
            }),
            Err(e) => {
                for (queue, task_id) in &grants {
                    self.concurrency
                        .rollback_grant(tenant_for_rollback, queue, task_id);
                }
                Err(e)
            }
        }
    }

    /// Inner loop body for `enqueue_limit_task_at_index`. Pushes successful
    /// in-memory grants to `grants` as it goes. Returns `Ok(())` on normal
    /// completion (the caller hands `grants` back to its caller), returning
    /// the concrete task-key timestamp when the walk wrote a DB task, or
    /// `Err(_)` with `grants` containing whatever was accumulated up to the
    /// failure point — the outer wrapper rolls those back before returning the
    /// error.
    async fn walk_limit_chain<W: WriteBatcher>(
        &self,
        writer: &mut W,
        params: LimitTaskParams<'_>,
        grants: &mut Vec<(String, String)>,
    ) -> Result<Option<i64>, JobStoreShardError> {
        let LimitTaskParams {
            tenant,
            task_id,
            job_id,
            attempt_number,
            relative_attempt_number,
            limit_index,
            limits,
            priority,
            scheduled_at_ms,
            task_key_epoch_ms,
            now_ms,
            held_queues,
            task_group,
            skip_try_reserve,
        } = params;
        // Walk limits in the order the client provided them — silo no longer
        // reorders. `current_index` and the `limit_index` stored in
        // `CheckRateLimit` tasks are direct indices into `limits`, so
        // dequeue's `limit_index + 1` continuation lands on the next entry.
        // Clients are responsible for ordering their limits such that a
        // tighter `Concurrency` limit precedes any `FloatingConcurrency` that
        // would otherwise grant a leasable slot bypassing it.
        let mut current_index = limit_index;
        let mut current_held_queues = held_queues;
        let current_task_id = task_id.to_string();
        // `skip_try_reserve` is a retry-only knob (see the lease.rs retry
        // path): the old holder is still in-memory and the retry must go
        // through the request queue. It applies to the *first* limit the
        // walker touches, not every limit in the chain. Today the first
        // limit's `handle_enqueue` with `skip_try_reserve = true` is
        // guaranteed to write a TicketRequested and exit the loop, so any
        // value we leave in here for subsequent iterations is dead. But if
        // a future ordering ever lets the walker step past that first
        // limit while `skip_try_reserve` is still true, every downstream
        // concurrency limit would silently bypass its reservation —
        // exactly the bug class this PR is trying to close. Consume the
        // flag after every `handle_enqueue` so the dead-code property
        // becomes a compile-checked invariant.
        let mut skip_try_reserve = skip_try_reserve;

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
                    scheduled_at_ms,
                    priority,
                    job_id,
                    attempt_number,
                    task_key_epoch_ms,
                    &run_task,
                )?;
                return Ok(Some(scheduled_at_ms));
            }

            match &limits[current_index] {
                Limit::Concurrency(cl) => {
                    // Try immediate grant for concurrency limits
                    let outcome = self
                        .concurrency
                        .handle_enqueue(
                            &self.db,
                            &self.get_range(),
                            writer,
                            tenant,
                            &current_task_id,
                            job_id,
                            priority,
                            scheduled_at_ms,
                            task_key_epoch_ms,
                            now_ms,
                            std::slice::from_ref(cl),
                            task_group,
                            attempt_number,
                            relative_attempt_number,
                            std::mem::replace(&mut skip_try_reserve, false),
                            current_index as u32,
                            &current_held_queues,
                            limits,
                        )
                        .await?;

                    // Cache the resolved limit for the query system
                    self.concurrency.cache_queue_limit(
                        tenant,
                        &cl.key,
                        cl.max_concurrency,
                        crate::concurrency::ConcurrencyLimitType::Fixed,
                    );

                    match record_grant_outcome(
                        outcome,
                        &cl.key,
                        scheduled_at_ms,
                        grants,
                        &current_task_id,
                        &mut current_held_queues,
                        &mut current_index,
                    ) {
                        GrantResult::Granted => {}
                        GrantResult::Queued {
                            pending_task_key_start_ms,
                        } => {
                            // No partial RunAttempt task is written before a
                            // queued concurrency gate; if this was a future
                            // RequestTicket, report its concrete task key.
                            return Ok(pending_task_key_start_ms);
                        }
                    }
                }

                Limit::FloatingConcurrency(fl) => {
                    if fl.bypasses_limit_check() {
                        // `kind: shopifyPid` floating limits grant unconditionally:
                        // no holder is reserved (so there's nothing to release on
                        // completion and the queue is never entered), no state is
                        // created, and no refresh is scheduled. `skip_try_reserve`
                        // is deliberately NOT consumed — it must stay armed for
                        // the first limit that actually reserves a slot.
                        current_index += 1;
                        continue;
                    }

                    // Get/create floating limit state and maybe schedule refresh
                    let state = self
                        .get_or_create_floating_limit_state(writer, tenant, fl)
                        .await?;
                    let refresh_ready = JobStoreShard::floating_limit_refresh_ready(&state, now_ms);

                    // Try immediate grant using current max concurrency
                    let current_max = state.current_max_concurrency();
                    let temp_cl = crate::job::ConcurrencyLimit {
                        key: fl.key.clone(),
                        max_concurrency: current_max,
                    };

                    let outcome = self
                        .concurrency
                        .handle_enqueue(
                            &self.db,
                            &self.get_range(),
                            writer,
                            tenant,
                            &current_task_id,
                            job_id,
                            priority,
                            scheduled_at_ms,
                            task_key_epoch_ms,
                            now_ms,
                            std::slice::from_ref(&temp_cl),
                            task_group,
                            attempt_number,
                            relative_attempt_number,
                            std::mem::replace(&mut skip_try_reserve, false),
                            current_index as u32,
                            &current_held_queues,
                            limits,
                        )
                        .await?;

                    // Cache the resolved floating limit for the query system
                    self.concurrency.cache_queue_limit(
                        tenant,
                        &fl.key,
                        current_max,
                        crate::concurrency::ConcurrencyLimitType::Floating,
                    );

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

                    match record_grant_outcome(
                        outcome,
                        &fl.key,
                        scheduled_at_ms,
                        grants,
                        &current_task_id,
                        &mut current_held_queues,
                        &mut current_index,
                    ) {
                        GrantResult::Granted => {}
                        GrantResult::Queued {
                            pending_task_key_start_ms,
                        } => {
                            // See the Concurrency branch comment: holder-only
                            // grants continue, queued gates stop the walk.
                            return Ok(pending_task_key_start_ms);
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
                        scheduled_at_ms,
                        priority,
                        job_id,
                        attempt_number,
                        task_key_epoch_ms,
                        &task,
                    )?;
                    return Ok(Some(scheduled_at_ms));
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
        job_metric_info: Option<&JobView>,
    ) -> Result<Option<BackgroundActionMetricTransition>, JobStoreShardError> {
        self.set_job_status_with_index_opts(
            writer,
            tenant,
            job_id,
            new_status,
            None,
            job_metric_info,
        )
        .await
    }

    /// Update job status with optional row TTL on the new status/index records.
    ///
    /// When `expire_ts` is `Some`, the new `JOB_STATUS` and `IDX_STATUS_TIME`
    /// rows are written with a SlateDB TTL expiring at `expire_ts` (epoch ms).
    /// Used by the terminal-job expiration path; pass `None` everywhere else.
    ///
    /// `job_metric_info` is an optional `JobView` reference supplied by the
    /// caller when one is already in scope. When provided, it short-circuits
    /// the `JOB_INFO` lookup that this helper would otherwise issue to derive
    /// the task_group and metadata needed for background-action queue gauges —
    /// the lookup is on the hot path for callers like dequeue and
    /// `report_attempt_outcome`. Callers without a `JobView` in scope pass
    /// `None` and the helper falls back to reading `JOB_INFO` itself.
    pub(crate) async fn set_job_status_with_index_opts<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
        expire_ts: Option<i64>,
        job_metric_info: Option<&JobView>,
    ) -> Result<Option<BackgroundActionMetricTransition>, JobStoreShardError> {
        // Delete old index entries if present
        let old_status = if let Some(old_raw) = writer.get(&job_status_key(tenant, job_id)).await? {
            let old = decode_job_status_owned(&old_raw)?;
            let old_ts = status_index_timestamp(&old);
            let old_time = idx_status_time_key(tenant, old.kind.as_str(), old_ts, job_id);
            writer.delete(&old_time)?;
            // Decrement old status counter
            writer.merge(
                tenant_status_counter_key(tenant, old.kind.as_str()),
                encode_counter(-1),
            )?;
            Some(old)
        } else {
            None
        };

        let old_kind = old_status.as_ref().map(|old| old.kind);
        let new_kind = new_status.kind;
        let needs_background_action_counters =
            background_action_queue_counter_transition_is_relevant(old_kind, new_kind);
        let derived_metric_info: Option<(String, Vec<(String, String)>)> =
            if needs_background_action_counters {
                match job_metric_info {
                    Some(view) => Some((view.task_group().to_string(), view.metadata())),
                    None => writer
                        .get(&job_info_key(tenant, job_id))
                        .await?
                        .map(|raw| {
                            JobView::new(raw)
                                .map(|view| (view.task_group().to_string(), view.metadata()))
                                .map_err(|e| JobStoreShardError::Codec(e.to_string()))
                        })
                        .transpose()?,
                }
            } else {
                None
            };
        let background_action_transition =
            derived_metric_info.as_ref().map(|(task_group, metadata)| {
                BackgroundActionMetricTransition {
                    tenant: tenant.to_string(),
                    task_group: task_group.clone(),
                    old_status: old_kind,
                    new_status: new_kind,
                    metadata: metadata.clone(),
                }
            });
        if let Some((task_group, metadata)) = derived_metric_info.as_ref() {
            self.apply_background_action_queue_counter_transition(
                tenant, task_group, old_kind, new_kind, metadata,
            );
        }

        if let Err(e) = Self::write_job_status_with_index_opts_and_metadata(
            writer, tenant, job_id, new_status, expire_ts,
        ) {
            if let Some((task_group, metadata)) = derived_metric_info.as_ref() {
                self.rollback_background_action_queue_counter_transition(
                    tenant, task_group, old_kind, new_kind, metadata,
                );
            }
            return Err(e);
        }
        Ok(background_action_transition)
    }

    /// Shared helper: write status value and index entry.
    pub(crate) fn write_job_status_with_index_and_metadata<W: WriteBatcher>(
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), JobStoreShardError> {
        Self::write_job_status_with_index_opts_and_metadata(
            writer, tenant, job_id, new_status, None,
        )
    }

    pub(crate) fn write_job_status_with_index_opts_and_metadata<W: WriteBatcher>(
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
        expire_ts: Option<i64>,
    ) -> Result<(), JobStoreShardError> {
        // Write new status value
        let job_status_value = encode_job_status(&new_status);
        let status_key = job_status_key(tenant, job_id);
        match expire_ts {
            Some(ts) => writer.put_with_expire(&status_key, &job_status_value, ts)?,
            None => writer.put(&status_key, &job_status_value)?,
        }

        // Insert new index entries
        // For Scheduled statuses, use next_attempt_starts_after_ms as the timestamp
        // to enable efficient waiting/future-scheduled range scans.
        let new_kind = new_status.kind;
        let ts = status_index_timestamp(&new_status);
        let timek = idx_status_time_key(tenant, new_kind.as_str(), ts, job_id);
        match expire_ts {
            Some(ets) => writer.put_with_expire(&timek, [], ets)?,
            None => writer.put(&timek, [])?,
        }

        // Increment tenant status counter for the new status
        writer.merge(
            tenant_status_counter_key(tenant, new_kind.as_str()),
            encode_counter(1),
        )?;

        Ok(())
    }

    /// Retarget a Scheduled job's pending-task timestamp to the concrete DB
    /// task key written by a resumed limit chain.
    ///
    /// Status-based operations such as cancel, reimport, and direct lease
    /// reconstruct the pending task key from `next_attempt_starts_after_ms`.
    /// When a resumed chain deliberately writes at a fresh task-key timestamp
    /// to dodge broker tombstones, this keeps those operations able to find
    /// the task without falling back to a broad task scan.
    pub(crate) async fn retarget_scheduled_task_key<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        job_id: &str,
        attempt_number: u32,
        task_key_start_ms: i64,
    ) -> Result<Option<BackgroundActionMetricTransition>, JobStoreShardError> {
        let Some(old_raw) = writer.get(&job_status_key(tenant, job_id)).await? else {
            return Ok(None);
        };
        let old = decode_job_status_owned(&old_raw)?;
        if old.kind != JobStatusKind::Scheduled
            || old.current_attempt != Some(attempt_number)
            || old.next_attempt_starts_after_ms == Some(task_key_start_ms)
        {
            return Ok(None);
        }

        let new_status = JobStatus::new(
            JobStatusKind::Scheduled,
            old.changed_at_ms,
            Some(task_key_start_ms),
            old.current_attempt,
        );
        // No JobView is loaded on the retarget path (Scheduled→Scheduled is
        // not a gauge-relevant transition either), so let the helper's
        // fallback handle the rare case where the predicate ever does fire.
        self.set_job_status_with_index(writer, tenant, job_id, new_status, None)
            .await
    }
}
