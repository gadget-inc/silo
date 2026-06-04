//! Job cancellation operations.

use slatedb::IsolationLevel;

use crate::codec::{decode_cancellation_at_ms, decode_task, encode_job_cancellation};
use crate::job::{JobCancellation, JobStatus, JobStatusKind, JobView, Limit};
use crate::job_store_shard::counters::{BackgroundActionMetricTransition, encode_counter};
use crate::job_store_shard::helpers::{
    TxnWriter, decode_job_status_owned, find_task_by_identity, now_epoch_ms,
    put_with_optional_expire, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    concurrency_holder_key, concurrency_request_job_prefix, concurrency_requester_counter_key,
    end_bound, job_cancelled_key, job_info_key, job_status_key,
};
use crate::task::Task;

impl JobStoreShard {
    /// Cancel a job by id. Prevents further execution and signals running workers to stop.
    ///
    /// Cancellation semantics:
    /// - Cancellation is tracked separately from status for performance
    /// - For scheduled jobs: immediately removes task from queue, releases concurrency,
    ///   and sets status to Cancelled
    /// - For running jobs: sets cancellation flag; worker discovers on heartbeat
    /// - For terminal jobs: returns error (cannot cancel completed jobs)
    /// - Cancellation is monotonic: once cancelled, always cancelled
    ///
    /// Uses a transaction with optimistic concurrency control to detect if the job state
    /// changes during the cancellation flow. Retries automatically on conflict.
    #[tracing::instrument(skip_all, fields(shard = %self.name))]
    pub async fn cancel_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        retry_on_txn_conflict("cancel_job", || self.cancel_job_inner(tenant, id)).await
    }

    /// Inner implementation of cancel_job that runs within a single transaction attempt.
    /// All business logic checks are performed within the transaction so they are re-evaluated
    /// on retry if the transaction conflicts.
    ///
    /// [SILO-CXL-3] For scheduled jobs, eagerly deletes tasks and concurrency state:
    /// - Task key is deleted from the DB queue (O(1) key reconstruction from status fields)
    /// - RunAttempt tasks: concurrency holder keys are deleted
    /// - RequestTicket tasks: no additional cleanup needed (request is the task itself)
    /// - No-task case (TicketRequested): concurrency request records are scanned and deleted
    ///
    /// Post-commit, in-memory concurrency counts are updated and grant-next is triggered
    /// for any freed concurrency slots.
    async fn cancel_job_inner(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-CXL-1] Pre: job must exist and not already be cancelled
        // Read status within transaction to detect concurrent modifications
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(&status_key).await?;
        let Some(status_raw) = maybe_status_raw else {
            // No status means job doesn't exist
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let status = decode_job_status_owned(&status_raw)?;

        // Check if already cancelled within transaction
        let cancelled_key = job_cancelled_key(tenant, id);
        let maybe_cancelled = txn.get(&cancelled_key).await?;
        if maybe_cancelled.is_some() {
            return Err(JobStoreShardError::JobAlreadyCancelled(id.to_string()));
        }

        // Cannot cancel jobs in final states (Succeeded/Failed are truly terminal)
        if status.kind.is_final() {
            return Err(JobStoreShardError::JobAlreadyTerminal(
                id.to_string(),
                status.kind,
            ));
        }

        // Track whether we're transitioning a scheduled job to terminal state.
        // For Running cancellations the worker discovers the cancellation on
        // heartbeat and transitions to Cancelled via `report_attempt_outcome`,
        // which applies its own TTL — so we only tag the records here when the
        // cancel itself produces the terminal transition.
        let was_scheduled = status.kind == JobStatusKind::Scheduled;

        // Compute the row-TTL deadline once, up front. Scheduled → Cancelled
        // uses `terminal_job_expire_s` (Cancelled is a non-success terminal).
        let terminal_expire_ts: Option<i64> = if was_scheduled {
            self.terminal_expire_ts(JobStatusKind::Cancelled, now_ms)
        } else {
            None
        };

        // [SILO-CXL-2] Post: Mark job as cancelled (write cancellation record).
        // If this cancellation transitions the job to terminal, tag the new
        // JOB_CANCELLED row with the row TTL directly. `expire_terminal_job_records`
        // reads through the `TxnWriter` (so reads see this txn's own writes and
        // join the SSI read set), so it would also re-put this row with the same
        // TTL — tagging it here keeps the TTL co-located with the only put site
        // and makes the JOB_CANCELLED row's TTL correct independent of helper
        // ordering.
        let cancellation = JobCancellation {
            cancelled_at_ms: now_ms,
        };
        let cancellation_value = encode_job_cancellation(&cancellation);
        put_with_optional_expire(
            &txn,
            &cancelled_key,
            &cancellation_value,
            terminal_expire_ts,
        )?;

        // Track concurrency holders to release post-commit. Each entry is a
        // (task_id, queue) pair — `atomic_release` keys the in-memory slot
        // by task_id, and the grant scanner is woken per queue.
        let mut holders_to_release: Vec<(String, String)> = Vec::new();
        let mut deleted_task_key: Option<Vec<u8>> = None;
        let mut background_action_transition: Option<BackgroundActionMetricTransition> = None;
        macro_rules! try_after_status {
            ($expr:expr) => {
                match $expr {
                    Ok(value) => value,
                    Err(e) => {
                        if let Some(transition) = &background_action_transition {
                            self.rollback_background_action_metric_gauge_transition(transition);
                        }
                        return Err(e.into());
                    }
                }
            };
        }

        // [SILO-CXL-3] For Scheduled jobs, eagerly delete task and concurrency state
        if was_scheduled {
            // Preserve scheduling fields in the cancelled status so task keys can be
            // reconstructed for reimport (O(1) key computation from status fields).
            let cancelled_status = JobStatus::new(
                JobStatusKind::Cancelled,
                now_ms,
                status.next_attempt_starts_after_ms,
                status.current_attempt,
            );
            // JobView is loaded just below for task-key reconstruction; that
            // ordering predates the metric helper. Pass None for now and let
            // the helper fall back to its own JOB_INFO lookup — cancel is not
            // a hot path. Re-ordering the load is the deferred refactor.
            background_action_transition = self
                .set_job_status_with_index_opts(
                    &mut TxnWriter(&txn),
                    tenant,
                    id,
                    cancelled_status,
                    terminal_expire_ts,
                    None,
                )
                .await?;

            // Read job info to get task_group, priority, and limits for task key reconstruction
            let job_key = job_info_key(tenant, id);
            if let Some(job_bytes) = try_after_status!(txn.get(&job_key).await) {
                let job_view = try_after_status!(JobView::new(job_bytes));
                let task_group = job_view.task_group().to_string();
                let priority = job_view.priority();
                let limits = job_view.limits();

                // Locate the pending task by identity (the trailing epoch_ms is
                // write-only, so we prefix-scan instead of reconstructing the
                // exact key).
                let attempt_number = status.current_attempt.unwrap_or(1);
                let start_time_ms = status.next_attempt_starts_after_ms.unwrap_or(0);
                let task_found = match try_after_status!(
                    find_task_by_identity(
                        &txn,
                        &task_group,
                        start_time_ms,
                        priority,
                        id,
                        attempt_number
                    )
                    .await
                ) {
                    Some(found) => Some(found),
                    None => {
                        // Fallback: try with time=0 (immediate scheduling case)
                        try_after_status!(
                            find_task_by_identity(
                                &txn,
                                &task_group,
                                0,
                                priority,
                                id,
                                attempt_number
                            )
                            .await
                        )
                    }
                };

                if let Some((found_key, task_raw)) = task_found {
                    let decoded = try_after_status!(decode_task(&task_raw).map_err(|e| {
                        JobStoreShardError::Codec(format!("cancel task decode: {e}"))
                    }));

                    // Delete the task from the DB queue
                    try_after_status!(txn.delete(&found_key));
                    deleted_task_key = Some(found_key);

                    match decoded {
                        Task::RunAttempt {
                            id: tid,
                            held_queues,
                            ..
                        } => {
                            // Delete concurrency holders for each held queue
                            for queue in held_queues {
                                try_after_status!(
                                    txn.delete(concurrency_holder_key(tenant, &queue, &tid))
                                );
                                holders_to_release.push((tid.clone(), queue));
                            }
                        }
                        Task::RequestTicket {
                            task_id: tid,
                            held_queues,
                            ..
                        } => {
                            // FutureRequestTaskWritten case: the RequestTicket
                            // task is the pending request mechanism. Deleting
                            // it stops the request. But a multi-limit chain
                            // can reach this state with prior holders already
                            // granted (carried on the ticket's `held_queues`);
                            // those must be released or the slots leak.
                            for queue in held_queues {
                                try_after_status!(
                                    txn.delete(concurrency_holder_key(tenant, &queue, &tid))
                                );
                                holders_to_release.push((tid.clone(), queue));
                            }
                        }
                        Task::CheckRateLimit {
                            task_id: tid,
                            held_queues,
                            ..
                        } => {
                            // A chain parked on a rate-limit step can carry
                            // upstream concurrency grants on its
                            // `held_queues`. Deleting the task stops the
                            // chain, but the holders must be released too or
                            // those slots leak.
                            for queue in held_queues {
                                try_after_status!(
                                    txn.delete(concurrency_holder_key(tenant, &queue, &tid))
                                );
                                holders_to_release.push((tid.clone(), queue));
                            }
                        }
                        _ => {
                            // Other task types (RefreshFloatingLimit, etc.)
                            // carry no chain-accumulated holders; deleting
                            // the task key above is sufficient.
                        }
                    }
                } else {
                    // No task found - check for TicketRequested case (request record
                    // exists but no task in DB queue). Scan for requests for this job.
                    // A deferred concurrency request can carry `held_queues` from
                    // upstream chain steps; those holders must be released too,
                    // mirroring the future-RequestTicket path above.
                    let released = self
                        .delete_concurrency_requests_for_job(
                            &txn,
                            tenant,
                            id,
                            &limits,
                            attempt_number,
                            start_time_ms,
                            priority,
                        )
                        .await;
                    let released = try_after_status!(released);
                    holders_to_release.extend(released);
                }
            }
        }
        // For Running jobs, status stays Running - worker discovers on heartbeat.
        // No task to delete (consumed by dequeue) and no concurrency cleanup needed
        // (holders released when worker completes).

        // Include counter in the transaction (unmark_write excludes it from conflict detection)
        if was_scheduled {
            try_after_status!(self.increment_completed_jobs_counter(&mut TxnWriter(&txn)));
        }

        // Re-put the job's prior associated records (JOB_INFO, IDX_METADATA,
        // any pre-existing ATTEMPT rows) with the row TTL so they age out of
        // slatedb alongside JOB_STATUS / JOB_CANCELLED. JOB_STATUS was tagged
        // above via set_job_status_with_index_opts, and JOB_CANCELLED was
        // tagged at its put site, so the helper only needs to handle the
        // remaining records.
        //
        // The helper reads and re-puts through the `TxnWriter`: reads go via
        // `txn.get` / `txn.scan`, which see this transaction's own buffered
        // writes and join the SSI read set so a concurrent mutation of any of
        // these keys trips a conflict rather than being silently clobbered with
        // stale bytes (see `expire_terminal_job_records`). JOB_INFO and any
        // prior ATTEMPT rows were written by earlier transactions; re-putting
        // them here tags them with the row TTL.
        if let Some(ts) = terminal_expire_ts {
            let expire_result = self
                .expire_terminal_job_records(&mut TxnWriter(&txn), tenant, id, ts)
                .await;
            try_after_status!(expire_result);
        }

        // Commit the transaction - this will detect conflicts with concurrent modifications
        if let Err(e) = txn.commit().await {
            if let Some(transition) = &background_action_transition {
                self.rollback_background_action_metric_gauge_transition(transition);
            }
            return Err(e.into());
        }

        // ---- Post-commit: update in-memory state ----

        // Evict the deleted task from the broker buffer
        if let Some(ref key) = deleted_task_key {
            self.brokers.evict_keys(std::slice::from_ref(key));
        }

        // Release concurrency holders from in-memory counts and signal grant scanner
        for (task_id, queue) in &holders_to_release {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
        }

        Ok(())
    }

    /// Delete concurrency request records for a specific job across all its concurrency queues.
    /// Used in the TicketRequested case where no task exists but request records do.
    ///
    /// Uses a narrow prefix scan targeting exactly this job's requests, rather than scanning
    /// all requests for the queue. Falls back to start_time_ms=0 if no requests found
    /// (same pattern as task key reconstruction fallback).
    ///
    /// Returns the `(task_id, queue)` pairs of concurrency holders that the
    /// caller must release post-commit — a deferred request can carry
    /// `held_queues` from upstream chain steps whose holders are still
    /// reserved.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn delete_concurrency_requests_for_job(
        &self,
        txn: &crate::instrumented_db::InstrumentedDbTransaction,
        tenant: &str,
        job_id: &str,
        limits: &[Limit],
        attempt_number: u32,
        start_time_ms: i64,
        priority: u8,
    ) -> Result<Vec<(String, String)>, JobStoreShardError> {
        let mut holders_to_release: Vec<(String, String)> = Vec::new();
        for limit in limits {
            let queue_key = match limit {
                Limit::Concurrency(cl) => &cl.key,
                Limit::FloatingConcurrency(fl) => &fl.key,
                Limit::RateLimit(_) => continue,
            };

            let deleted = self
                .delete_concurrency_requests_with_prefix(
                    txn,
                    tenant,
                    job_id,
                    queue_key,
                    attempt_number,
                    start_time_ms,
                    priority,
                    &mut holders_to_release,
                )
                .await?;

            if !deleted {
                // Fallback: try with start_time_ms=0 (immediate scheduling case)
                self.delete_concurrency_requests_with_prefix(
                    txn,
                    tenant,
                    job_id,
                    queue_key,
                    attempt_number,
                    0,
                    priority,
                    &mut holders_to_release,
                )
                .await?;
            }
        }
        Ok(holders_to_release)
    }

    /// Scan a narrow prefix for this job's concurrency requests and delete them.
    /// Returns true if any requests were deleted.
    ///
    /// For each request value found, decodes the persisted `task_id` and
    /// `held_queues` and:
    /// - batches a delete of every `concurrency_holder_key(tenant, q, task_id)`
    ///   so upstream chain holders are dropped together with the request;
    /// - appends `(task_id, queue)` pairs to `holders_to_release` so the caller
    ///   can route them through the post-commit `atomic_release` path.
    ///
    /// A malformed value is logged and its key is still deleted (the entry is
    /// dead either way); the holders for that entry simply can't be recovered.
    #[expect(clippy::too_many_arguments)]
    async fn delete_concurrency_requests_with_prefix(
        &self,
        txn: &crate::instrumented_db::InstrumentedDbTransaction,
        tenant: &str,
        job_id: &str,
        queue_key: &str,
        attempt_number: u32,
        start_time_ms: i64,
        priority: u8,
        holders_to_release: &mut Vec<(String, String)>,
    ) -> Result<bool, JobStoreShardError> {
        let prefix = concurrency_request_job_prefix(
            tenant,
            queue_key,
            start_time_ms,
            priority,
            job_id,
            attempt_number,
        );
        let end = end_bound(&prefix);
        let mut iter = self.db.scan::<Vec<u8>, _>(prefix..end).await?;

        let mut deleted_count: i64 = 0;
        while let Some(kv) = iter.next().await? {
            // Decode the request to recover any upstream `held_queues` the
            // chain accumulated before this gate. Missing/corrupt values are
            // logged — we still delete the request key but cannot recover the
            // holders (they will leak in-memory until process restart).
            match crate::codec::decode_concurrency_action(kv.value.clone()) {
                Ok(decoded) => {
                    let a = decoded.fb();
                    if let Some(et) = a.variant_as_enqueue_task() {
                        let task_id = et.task_id().unwrap_or_default();
                        if !task_id.is_empty()
                            && let Some(held) = et.held_queues()
                        {
                            for q in held.iter() {
                                txn.delete(concurrency_holder_key(tenant, q, task_id))?;
                                holders_to_release.push((task_id.to_string(), q.to_string()));
                            }
                        }
                    } else {
                        tracing::warn!(
                            job_id = %job_id,
                            queue = %queue_key,
                            "cancel: concurrency request has unknown variant; holders (if any) cannot be recovered"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        queue = %queue_key,
                        error = %e,
                        "cancel: failed to decode concurrency request; holders (if any) cannot be recovered"
                    );
                }
            }
            txn.delete(&kv.key)?;
            deleted_count += 1;
            tracing::debug!(
                job_id = %job_id,
                queue = %queue_key,
                "cancel: deleted concurrency request for cancelled job"
            );
        }

        if deleted_count > 0 {
            // Decrement the per-queue requester counter
            let counter_key = concurrency_requester_counter_key(tenant, queue_key);
            txn.merge(&counter_key, encode_counter(-deleted_count))?;
            txn.unmark_write([counter_key.as_slice()])?;
        }

        Ok(deleted_count > 0)
    }

    /// Check if a job has been cancelled.
    /// Returns true if the job has a cancellation record, false otherwise.
    pub async fn is_job_cancelled(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<bool, JobStoreShardError> {
        let key = job_cancelled_key(tenant, id);
        let maybe_raw = self.db.get(&key).await?;
        Ok(maybe_raw.is_some())
    }

    /// Get the cancellation timestamp if job was cancelled.
    /// Returns None if job was not cancelled.
    pub async fn get_job_cancellation(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<i64>, JobStoreShardError> {
        let key = job_cancelled_key(tenant, id);
        let Some(raw) = self.db.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(decode_cancellation_at_ms(&raw)?))
    }
}
