//! Tenant-wide forced cleanup of concurrency holders and in-flight run attempts.
//!
//! This backs the console's "Drop holders & run attempts" admin action: it
//! forcibly clears a tenant's stuck concurrency state on a single shard so the
//! tenant returns to a consistent, runnable state.

use std::collections::HashSet;

use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use tracing::warn;

use crate::codec::{decode_lease, decode_task};
use crate::job::{JobStatus, JobStatusKind};
use crate::job_store_shard::helpers::{DbWriteBatcher, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    concurrency_holders_tenant_prefix, end_bound, leases_prefix, parse_concurrency_holder_key,
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

/// Counts of what `reconcile_orphaned_running_jobs` finalized.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileTenantStats {
    /// `Running` jobs that had no lease and were force-failed.
    pub orphaned_running_failed: usize,
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

    /// Finalize a tenant's orphaned `Running` jobs — jobs stuck in `Running`
    /// status with **no lease record** — by force-transitioning them to
    /// terminal `Failed`.
    ///
    /// ## Why this exists
    ///
    /// The lease reaper is lease-driven: it finalizes a `Running` job by acting
    /// on its (expired) lease. A `Running` job whose lease was raw-deleted
    /// without a status transition — e.g. by the old "Drop holders & run
    /// attempts" admin action before PR #365 — is therefore invisible to the
    /// reaper and never cleaned up. No other tool recovers it either: `delete`
    /// rejects non-terminal jobs, `cancel` on `Running` only writes a marker an
    /// absent worker must ack, and `restart`/`expedite` reject `Running`. This
    /// reconcile is the recovery path, and a permanent backstop against any
    /// future orphan source.
    ///
    /// ## Semantics
    ///
    /// - **Orphan = `Running` with no lease.** We scan the whole LEASE keyspace
    ///   and collect *every* lease for the tenant regardless of expiry: if a
    ///   lease exists at all, the reaper owns that job and we must not race it.
    ///   Only `Running` jobs with no lease are finalized.
    /// - **No synthetic ATTEMPT row.** We have no lease/attempt context, so we
    ///   write no new attempt. The job becomes `Failed` terminal;
    ///   `expire_terminal_job_records` re-puts the existing (stale `Running`)
    ///   attempt row with the terminal TTL so all records age out together.
    /// - **Idempotent / partial-progress-safe.** Each job is finalized in its
    ///   own durable batch and re-reads status first, so re-running skips
    ///   already-`Failed` jobs and a crash mid-run loses no correctness.
    /// - **Best-effort, like the reaper.** A per-job failure is logged, its
    ///   in-memory queue-counter side effect rolled back, and the scan
    ///   continues.
    ///
    /// ## Non-goal
    ///
    /// Orphaned `Scheduled` jobs are intentionally out of scope: a `Scheduled`
    /// job legitimately has either a queued task or a pending concurrency
    /// request, so classifying it as orphaned is ambiguous. A future extension
    /// could handle that with a more careful definition.
    #[tracing::instrument(skip_all, fields(shard = %self.name, tenant))]
    pub async fn reconcile_orphaned_running_jobs(
        &self,
        tenant: &str,
    ) -> Result<ReconcileTenantStats, JobStoreShardError> {
        // ---- 1. Build the leased-job set (any lease, regardless of expiry) ----
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

        // ---- 2. List the tenant's Running jobs ----
        let running_job_ids = self
            .scan_jobs_by_status(tenant, JobStatusKind::Running, None)
            .await?;

        // ---- 3. Finalize each orphan in its own durable, atomic batch ----
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

        Ok(ReconcileTenantStats {
            orphaned_running_failed,
        })
    }
}
