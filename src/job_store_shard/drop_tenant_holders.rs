//! Tenant-wide forced cleanup of concurrency holders and in-flight run attempts.
//!
//! This backs the console's "Drop holders & run attempts" admin action: it
//! forcibly clears a tenant's stuck concurrency state on a single shard so the
//! tenant returns to a consistent, runnable state.

use std::collections::HashSet;

use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use tracing::warn;
use uuid::Uuid;

use crate::codec::{decode_lease, decode_task};
use crate::job::{JobStatus, JobStatusKind};
use crate::job_store_shard::helpers::{DbWriteBatcher, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, LimitTaskParams};
use crate::keys::{
    concurrency_holders_tenant_prefix, concurrency_request_tenant_prefix, end_bound, leases_prefix,
    parse_concurrency_holder_key, parse_concurrency_request_key, task_key_lookup_prefix,
    tasks_prefix,
};
use crate::task::Task;

/// Max deletes flushed per write batch. Bounds batch size/memory on a tenant
/// with a very large amount of stuck state; the operation is idempotent so
/// chunking across multiple batches is safe.
const DROP_BATCH_SIZE: usize = 1000;

/// Counts of what `drop_tenant_holders_and_runattempts` removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropTenantStats {
    /// Number of concurrency holder records deleted.
    pub holders_dropped: usize,
    /// Number of in-flight `RunAttempt`s deleted — both queued task-queue
    /// entries and running (leased) attempts.
    pub run_attempts_dropped: usize,
}

/// Counts of what `reconcile_orphaned_jobs` finalized.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileTenantStats {
    /// `Running` jobs that had no lease and were force-failed.
    pub orphaned_running_failed: usize,
    /// `Scheduled` jobs that had no task and no concurrency request, and were
    /// re-injected into the limit chain so they dispatch again.
    pub orphaned_scheduled_redriven: usize,
}

impl JobStoreShard {
    /// Forcibly drop **all** concurrency holders and in-flight `RunAttempt`
    /// tasks (plus their leases) for `tenant` on this shard.
    ///
    /// This is intentionally **destructive and non-transactional**, mirroring
    /// the bulk-delete pattern in [`cleanup`](super::cleanup): it scans
    /// read-only, then deletes the collected keys via plain write batches. It
    /// does *not* use a serializable transaction, because the run-attempt /
    /// lease scans aren't tenant-scoped (those keys aren't tenant-keyed) and
    /// would pull the whole shard's task/lease activity into an SSI read set —
    /// on a live, busy shard that conflicts on every other tenant's work and
    /// never commits. The batched-delete approach always commits regardless of
    /// concurrent activity, so it can fix a stuck tenant on a live env with no
    /// downtime.
    ///
    /// Semantics:
    /// - **Point-in-time / best-effort.** Holders or run attempts created for
    ///   the tenant *after* the scans are not caught; re-run if needed (the
    ///   operation is idempotent — re-deleting a missing key is a no-op).
    /// - **Partial progress is safe.** Each holder / task / lease delete is
    ///   independent; a holder without its lease (or vice versa) self-heals on
    ///   the worker's next heartbeat.
    /// - **Clean release.** After the deletes land, the in-memory concurrency
    ///   counts are updated and the grant scanner is woken so pending requests
    ///   can be re-granted.
    /// - Historical `ATTEMPT` records and concurrency *requests* are
    ///   deliberately left untouched.
    #[tracing::instrument(skip_all, fields(shard = %self.name, tenant))]
    pub async fn drop_tenant_holders_and_runattempts(
        &self,
        tenant: &str,
    ) -> Result<DropTenantStats, JobStoreShardError> {
        // ---- Collect keys to delete (read-only scans; no conflict set) ----

        // Holders are tenant-prefixed, so a single prefix scan captures every
        // holder for the tenant. Track (task_id, queue) for the post-delete
        // in-memory release. Holder counts have no persisted counter (in-memory
        // only), so deleting the rows is all that's needed durably.
        let mut holders_to_release: Vec<(String, String)> = Vec::new();
        let mut holder_keys: Vec<Vec<u8>> = Vec::new();
        {
            let start = concurrency_holders_tenant_prefix(tenant);
            let end = end_bound(&start);
            let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
            while let Some(kv) = iter.next().await? {
                let Some(parsed) = parse_concurrency_holder_key(&kv.key) else {
                    continue;
                };
                holder_keys.push(kv.key.to_vec());
                holders_to_release.push((parsed.task_id, parsed.queue));
            }
        }

        // Queued (granted, not yet leased) RunAttempts live in the TASK queue,
        // keyed by task_group (not tenant) — full-shard scan + filter by the
        // tenant carried inside the RunAttempt.
        let mut deleted_task_keys: Vec<Vec<u8>> = Vec::new();
        {
            let start = tasks_prefix();
            let end = end_bound(&start);
            let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
            while let Some(kv) = iter.next().await? {
                let task = match decode_task(&kv.value) {
                    Ok(task) => task,
                    Err(e) => {
                        warn!(error = %e, "skipping undecodable task during tenant drop");
                        continue;
                    }
                };
                if let Task::RunAttempt { tenant: t, .. } = task
                    && t == tenant
                {
                    deleted_task_keys.push(kv.key.to_vec());
                }
            }
        }

        // Running (leased) RunAttempts: the TASK entry was removed at lease time
        // and a LeaseRecord (which embeds the Task) was written under the LEASE
        // prefix, keyed by task_id — full-shard scan + filter on the embedded
        // task. Deleting the lease orphans the worker's attempt (it discovers
        // the missing lease on heartbeat), mirroring cancel semantics. The two
        // RunAttempt sets are disjoint (a leased task is no longer in TASK).
        let mut deleted_lease_keys: Vec<Vec<u8>> = Vec::new();
        {
            let start = leases_prefix();
            let end = end_bound(&start);
            let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
            while let Some(kv) = iter.next().await? {
                let task = match decode_lease(kv.value.clone()).and_then(|l| l.to_task()) {
                    Ok(task) => task,
                    Err(e) => {
                        warn!(error = %e, "skipping undecodable lease during tenant drop");
                        continue;
                    }
                };
                if let Task::RunAttempt { tenant: t, .. } = task
                    && t == tenant
                {
                    deleted_lease_keys.push(kv.key.to_vec());
                }
            }
        }

        // ---- Delete via chunked, non-transactional write batches ----
        // Each batch is atomic on its own; chunking across batches is safe
        // because the operation is idempotent. `db.write` has no read set, so
        // it commits regardless of concurrent activity on the shard.
        let mut batch = WriteBatch::new();
        let mut batch_count = 0usize;
        for key in holder_keys
            .iter()
            .chain(deleted_task_keys.iter())
            .chain(deleted_lease_keys.iter())
        {
            batch.delete(key);
            batch_count += 1;
            if batch_count >= DROP_BATCH_SIZE {
                self.db.write(batch).await?;
                batch = WriteBatch::new();
                batch_count = 0;
            }
        }
        if batch_count > 0 {
            self.db.write(batch).await?;
        }

        // ---- Post-delete: reconcile in-memory state ----

        // Evict deleted queued tasks from the broker buffers.
        if !deleted_task_keys.is_empty() {
            self.brokers.evict_keys(&deleted_task_keys);
        }

        // Release concurrency holders from in-memory counts and wake the grant
        // scanner per freed queue so pending requests get re-granted.
        for (task_id, queue) in &holders_to_release {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
        }

        Ok(DropTenantStats {
            holders_dropped: holder_keys.len(),
            run_attempts_dropped: deleted_task_keys.len() + deleted_lease_keys.len(),
        })
    }

    /// Recover a tenant's orphaned jobs — jobs that no longer have anything in
    /// the pipeline to advance them — on this shard.
    ///
    /// Two orphan classes are handled, both produced by the old "Drop holders &
    /// run attempts" admin action, which raw-deleted leases and queued
    /// `RunAttempt` tasks without touching job status (see
    /// [`drop_tenant_holders_and_runattempts`]):
    ///
    /// 1. **`Running` with no lease → force-`Failed`.** The lease reaper is
    ///    lease-driven, so a `Running` job whose lease was deleted is invisible
    ///    to it and never cleaned up. No other tool recovers it either: `delete`
    ///    rejects non-terminal jobs, `cancel` on `Running` only writes a marker
    ///    an absent worker must ack, and `restart`/`expedite` reject `Running`.
    ///    Its attempt is lost (the worker is gone), so we finalize it terminal.
    /// 2. **`Scheduled` with no task and no concurrency request → redrive.** A
    ///    `Scheduled` job always gets a pipeline entry at enqueue — a queued
    ///    task, or (at capacity) a deferred concurrency request. If *both* are
    ///    gone nothing will ever dispatch it. It never started, so rather than
    ///    fail it we re-inject it into the limit chain exactly as a fresh
    ///    enqueue would (via [`enqueue_limit_task_at_index`]), re-applying its
    ///    concurrency/rate limits — at capacity it re-parks as a request, never
    ///    bypassing the tenant's concurrency cap.
    ///
    /// This is the recovery path for already-orphaned jobs and a permanent
    /// backstop against any future orphan source.
    ///
    /// ## Semantics
    ///
    /// - **Lease/request sets collected up front.** We scan the whole LEASE
    ///   keyspace (any lease, regardless of expiry — if one exists the reaper
    ///   owns the job and we must not race it) and the tenant's concurrency
    ///   REQUEST keyspace once each, so a `Running` job with any lease and a
    ///   `Scheduled` job with any pending request are left untouched.
    /// - **No synthetic ATTEMPT row** on the `Failed` path: we have no
    ///   lease/attempt context, so `expire_terminal_job_records` just re-puts
    ///   the existing (stale `Running`) attempt with the terminal TTL.
    /// - **Redrive preserves the attempt and schedule.** The new chain head
    ///   keeps the job's `current_attempt` and `next_attempt_starts_after_ms`,
    ///   so a future-scheduled orphan re-lands at its original time and the
    ///   broker picks it up then. We do **not** re-apply the background-action
    ///   queue counter: the job has been continuously `Scheduled` (the drop
    ///   deleted only the task, never decremented the status gauge), so
    ///   re-applying would double-count.
    /// - **Idempotent / partial-progress-safe.** Each job is handled in its own
    ///   durable batch and re-reads status first; a pre-write point-check skips
    ///   any `Scheduled` job that already has a task (so a re-run never writes a
    ///   duplicate `RunAttempt`).
    /// - **Best-effort, like the reaper.** A per-job failure is logged, its
    ///   in-memory side effects rolled back, and the scan continues.
    #[tracing::instrument(skip_all, fields(shard = %self.name, tenant))]
    pub async fn reconcile_orphaned_jobs(
        &self,
        tenant: &str,
    ) -> Result<ReconcileTenantStats, JobStoreShardError> {
        // ---- Build the leased-job set (any lease, regardless of expiry) ----
        let mut leased_job_ids: HashSet<String> = HashSet::new();
        {
            let start = leases_prefix();
            let end = end_bound(&start);
            let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
            while let Some(kv) = iter.next().await? {
                let decoded = match decode_lease(kv.value.clone()) {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        warn!(error = %e, "skipping undecodable lease during reconcile");
                        continue;
                    }
                };
                if decoded.tenant() == tenant {
                    leased_job_ids.insert(decoded.job_id().to_string());
                }
            }
        }

        // ---- Build the pending-concurrency-request set for the tenant ----
        // A Scheduled job with a request is legitimately waiting on a slot and
        // must never be redriven. Requests are keyed (tenant, queue, ...), so a
        // single tenant-prefix scan captures all of them.
        let mut requested_job_ids: HashSet<String> = HashSet::new();
        {
            let start = concurrency_request_tenant_prefix(tenant);
            let end = end_bound(&start);
            let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
            while let Some(kv) = iter.next().await? {
                if let Some(parsed) = parse_concurrency_request_key(&kv.key) {
                    requested_job_ids.insert(parsed.job_id);
                }
            }
        }

        let orphaned_running_failed = self
            .reconcile_running_orphans(tenant, &leased_job_ids)
            .await?;
        let orphaned_scheduled_redriven = self
            .reconcile_scheduled_orphans(tenant, &requested_job_ids)
            .await?;

        Ok(ReconcileTenantStats {
            orphaned_running_failed,
            orphaned_scheduled_redriven,
        })
    }

    /// Force-`Failed` every `Running` job of `tenant` whose id is not in
    /// `leased_job_ids`. See [`reconcile_orphaned_jobs`].
    async fn reconcile_running_orphans(
        &self,
        tenant: &str,
        leased_job_ids: &HashSet<String>,
    ) -> Result<usize, JobStoreShardError> {
        let running_job_ids = self
            .scan_jobs_by_status(tenant, JobStatusKind::Running, None)
            .await?;

        let mut orphaned_running_failed = 0usize;
        for job_id in running_job_ids {
            if leased_job_ids.contains(&job_id) {
                continue;
            }

            let now_ms = now_epoch_ms();

            // Re-read status: skip unless still Running (guards against a
            // concurrent transition and makes re-runs idempotent).
            match self.get_job_status(tenant, &job_id).await? {
                Some(status) if status.kind == JobStatusKind::Running => {}
                _ => continue,
            }

            let expire_ts = self.terminal_expire_ts(JobStatusKind::Failed, now_ms);
            let mut batch = WriteBatch::new();

            // JOB_STATUS + IDX_STATUS_TIME + Running->Failed counter swap +
            // background-action queue counters.
            let transition = self
                .set_job_status_with_index_opts(
                    &mut DbWriteBatcher::new(&self.db, &mut batch),
                    tenant,
                    &job_id,
                    JobStatus::failed(now_ms),
                    expire_ts,
                    None,
                )
                .await?;

            // Terminal completion counter.
            self.increment_completed_jobs_counter(&mut DbWriteBatcher::new(&self.db, &mut batch))?;

            // TTL the job's records (JOB_INFO / IDX_METADATA / ATTEMPT /
            // JOB_CANCELLED), including the stale Running attempt row.
            if let Some(ts) = expire_ts {
                self.expire_terminal_job_records(
                    &mut DbWriteBatcher::new(&self.db, &mut batch),
                    tenant,
                    &job_id,
                    ts,
                )
                .await?;
            }

            // Commit durably; on failure roll back the in-memory queue-counter
            // side effect and continue (best-effort, like the reaper).
            match self
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
                Ok(_) => orphaned_running_failed += 1,
                Err(e) => {
                    if let Some(transition) = &transition {
                        self.rollback_background_action_metric_gauge_transition(transition);
                    }
                    warn!(error = %e, %job_id, "failed to finalize orphaned Running job; continuing");
                }
            }
        }

        Ok(orphaned_running_failed)
    }

    /// Redrive every `Scheduled` job of `tenant` that has neither a queued task
    /// nor a pending concurrency request (id not in `requested_job_ids`). See
    /// [`reconcile_orphaned_jobs`].
    async fn reconcile_scheduled_orphans(
        &self,
        tenant: &str,
        requested_job_ids: &HashSet<String>,
    ) -> Result<usize, JobStoreShardError> {
        let scheduled_job_ids = self
            .scan_jobs_by_status(tenant, JobStatusKind::Scheduled, None)
            .await?;

        let mut orphaned_scheduled_redriven = 0usize;
        for job_id in scheduled_job_ids {
            // A pending concurrency request means the job is legitimately
            // waiting on a slot — not an orphan.
            if requested_job_ids.contains(&job_id) {
                continue;
            }

            let now_ms = now_epoch_ms();

            // Re-read status: skip unless still Scheduled, and capture the
            // attempt number + scheduled start the redriven task must preserve.
            let (attempt, start_ms) = match self.get_job_status(tenant, &job_id).await? {
                Some(status) if status.kind == JobStatusKind::Scheduled => {
                    match (status.current_attempt, status.next_attempt_starts_after_ms) {
                        (Some(attempt), Some(start_ms)) => (attempt, start_ms),
                        // A Scheduled status always carries both fields; if not,
                        // skip rather than guess.
                        _ => continue,
                    }
                }
                _ => continue,
            };

            // Recover the job's task_group / priority / limits to rebuild the
            // chain head identically to a fresh enqueue.
            let Some(info_raw) = self
                .db
                .get(&crate::keys::job_info_key(tenant, &job_id))
                .await?
            else {
                continue;
            };
            let view = crate::job::JobView::new(info_raw)?;
            let task_group = view.task_group().to_string();
            let priority = view.priority();
            let limits = view.limits();

            // Point-check: skip if a task already exists for this identity, so a
            // re-run can never write a duplicate RunAttempt (double-execution).
            if self
                .job_has_queued_task(&task_group, start_ms, priority, &job_id, attempt)
                .await?
            {
                continue;
            }

            let mut batch = WriteBatch::new();
            let task_id = Uuid::new_v4().to_string();

            // Re-inject into the limit chain from index 0 with no held queues,
            // mirroring a fresh enqueue (and the retry path in lease.rs). At
            // capacity this writes a deferred request instead of a RunAttempt.
            let res = self
                .enqueue_limit_task_at_index(
                    &mut DbWriteBatcher::new(&self.db, &mut batch),
                    LimitTaskParams {
                        tenant,
                        task_id: &task_id,
                        job_id: &job_id,
                        attempt_number: attempt,
                        relative_attempt_number: 1,
                        limit_index: 0,
                        limits: &limits,
                        priority,
                        scheduled_at_ms: start_ms,
                        task_key_epoch_ms: now_ms,
                        now_ms,
                        held_queues: Vec::new(),
                        task_group: &task_group,
                        skip_try_reserve: false,
                    },
                )
                .await?;

            // Commit durably; mirror enqueue's grant lifecycle: confirm + wake
            // the broker on success, roll back the in-memory grants on failure.
            match self
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
                Ok(_) => {
                    self.finish_enqueue(&job_id, &task_group, start_ms, &res.grants)
                        .await?;
                    orphaned_scheduled_redriven += 1;
                }
                Err(e) => {
                    self.rollback_grants(tenant, &res.grants);
                    warn!(error = %e, %job_id, "failed to redrive orphaned Scheduled job; continuing");
                }
            }
        }

        Ok(orphaned_scheduled_redriven)
    }

    /// True if a task for `(task_group, start_ms, priority, job_id, attempt)`
    /// exists in the TASK keyspace. Used to avoid redriving a Scheduled job that
    /// already has a queued task.
    async fn job_has_queued_task(
        &self,
        task_group: &str,
        start_ms: i64,
        priority: u8,
        job_id: &str,
        attempt: u32,
    ) -> Result<bool, JobStoreShardError> {
        let prefix = task_key_lookup_prefix(task_group, start_ms, priority, job_id, attempt);
        let end = end_bound(&prefix);
        let mut iter = self.db.scan::<Vec<u8>, _>(prefix..end).await?;
        Ok(iter.next().await?.is_some())
    }
}
