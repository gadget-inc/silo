//! Tenant-wide forced cleanup of concurrency holders and in-flight run attempts.
//!
//! This backs the console's "Drop holders & run attempts" admin action: it
//! forcibly clears a tenant's stuck concurrency state on a single shard so the
//! tenant returns to a consistent, runnable state.

use slatedb::WriteBatch;
use tracing::warn;

use crate::codec::{decode_lease, decode_task};
use crate::job_attempt::AttemptOutcome;
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
    /// Number of in-flight `RunAttempt`s finalized — both queued task-queue
    /// entries (cancelled) and running (leased) attempts (failed as if the
    /// worker crashed).
    pub run_attempts_dropped: usize,
}

impl JobStoreShard {
    /// Forcibly clear **all** concurrency holders and finalize every in-flight
    /// `RunAttempt` (queued or running) for `tenant` on this shard, returning
    /// every affected job to a consistent terminal/runnable state.
    ///
    /// This is intentionally **non-transactional at the tenant level**,
    /// mirroring the bulk-scan pattern in [`cleanup`](super::cleanup): it scans
    /// the (non-tenant-keyed) task/lease keyspaces read-only, then finalizes
    /// each collected attempt via a **per-attempt** call to the same code paths
    /// the system already uses live. It does *not* wrap the whole operation in
    /// one serializable transaction, because the run-attempt / lease scans
    /// aren't tenant-scoped and would pull the whole shard's task/lease activity
    /// into an SSI read set — on a live, busy shard that conflicts on every
    /// other tenant's work and never commits. Each per-attempt finalization is
    /// itself atomic and tenant/job-scoped (exactly like a normal worker
    /// outcome report or user-initiated cancel), so it commits regardless of
    /// concurrent activity and can fix a stuck tenant on a live env with no
    /// downtime.
    ///
    /// Finalization (NOT raw deletion — raw-deleting a lease while leaving
    /// `JOB_STATUS = Running` orphans the job, because the lease reaper only
    /// acts on leases and would never see it again):
    /// - **Running (leased) attempts** are finalized through
    ///   [`report_attempt_outcome`](Self::report_attempt_outcome) — the same
    ///   call the lease reaper makes — as `Cancelled` if the job was cancelled,
    ///   otherwise as a `WORKER_CRASHED` error (which fails the job or schedules
    ///   a retry per its policy). This atomically deletes the lease, transitions
    ///   `JOB_STATUS` off `Running`, releases the attempt's concurrency holders,
    ///   and updates the status/time index and counters.
    /// - **Queued (granted, not yet leased) attempts** are finalized through
    ///   [`cancel_job`](Self::cancel_job), which deletes the pending task,
    ///   transitions the `Scheduled` job to `Cancelled`, evicts the broker
    ///   buffer, and releases any chain-accumulated holders. (A cancelled or
    ///   failed job is restartable, so dropped work is recoverable.)
    ///
    /// Semantics:
    /// - **Point-in-time / best-effort.** Holders or attempts created for the
    ///   tenant *after* the scans (including retries scheduled by the
    ///   finalizations above) are not caught; re-run if needed (the operation is
    ///   idempotent — finalizing an already-finalized attempt is a no-op, and
    ///   re-deleting a missing holder is a no-op).
    /// - **Partial progress is safe.** Each finalization and holder delete is
    ///   independent; a failure on one attempt is logged and the sweep
    ///   continues with the rest.
    /// - **Clean release.** After finalizing attempts, any *remaining*
    ///   orphaned holder rows (rows with no corresponding live attempt) are
    ///   deleted and their in-memory counts released, and the grant scanner is
    ///   woken so pending requests can be re-granted.
    /// - Historical `ATTEMPT` records and concurrency *requests* are
    ///   deliberately left untouched.
    #[tracing::instrument(skip_all, fields(shard = %self.name, tenant))]
    pub async fn drop_tenant_holders_and_runattempts(
        &self,
        tenant: &str,
    ) -> Result<DropTenantStats, JobStoreShardError> {
        // ---- Collect work via read-only scans (a point-in-time snapshot; no
        // SSI read set, so the scans never conflict with concurrent activity) ----

        // Holders are tenant-prefixed, so a single prefix scan captures every
        // holder for the tenant. Track (task_id, queue) for the post-finalize
        // in-memory release of any rows not already released by an attempt
        // finalization below. Holder counts have no persisted counter
        // (in-memory only), so deleting the rows is all that's needed durably.
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
        // tenant carried inside the RunAttempt. Collect the job ids so we can
        // cancel each (which deletes the task and transitions the job).
        let mut queued_job_ids: Vec<String> = Vec::new();
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
                if let Task::RunAttempt {
                    tenant: t, job_id, ..
                } = task
                    && t == tenant
                {
                    queued_job_ids.push(job_id);
                }
            }
        }

        // Running (leased) RunAttempts: the TASK entry was removed at lease time
        // and a LeaseRecord (which embeds the Task) was written under the LEASE
        // prefix, keyed by task_id — full-shard scan + filter on the embedded
        // task. Collect (task_id, job_id) so we can finalize each via the lease
        // reaper's path. The two RunAttempt sets are disjoint (a leased task is
        // no longer in TASK).
        let mut leased_attempts: Vec<(String, String)> = Vec::new();
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
                if let Task::RunAttempt {
                    id: task_id,
                    tenant: t,
                    job_id,
                    ..
                } = task
                    && t == tenant
                {
                    leased_attempts.push((task_id, job_id));
                }
            }
        }

        let mut run_attempts_dropped = 0usize;

        // ---- Finalize running (leased) attempts via the reaper's path ----
        // Mirrors `reap_expired_leases`: a cancelled job reports Cancelled,
        // otherwise WORKER_CRASHED (fail or retry per policy). This deletes the
        // lease and transitions JOB_STATUS off Running atomically, so the job is
        // never left orphaned in Running with no lease.
        for (task_id, job_id) in &leased_attempts {
            let was_cancelled = self.is_job_cancelled(tenant, job_id).await.unwrap_or(false);
            let outcome = if was_cancelled {
                AttemptOutcome::Cancelled
            } else {
                AttemptOutcome::Error {
                    error_code: "WORKER_CRASHED".to_string(),
                    error: b"run attempt force-dropped via drop_tenant_holders admin action"
                        .to_vec(),
                }
            };
            match self.report_attempt_outcome(task_id, outcome).await {
                Ok(()) => run_attempts_dropped += 1,
                Err(e) => {
                    // Best-effort: a concurrent reaper/worker may have already
                    // finalized this lease. Log and continue.
                    warn!(
                        task_id = %task_id,
                        job_id = %job_id,
                        error = %e,
                        "failed to finalize leased run attempt during tenant drop"
                    );
                }
            }
        }

        // ---- Finalize queued (Scheduled) attempts via cancel_job ----
        // Deletes the pending task and transitions the job to Cancelled, so the
        // job is never left orphaned in Scheduled with no task to run it.
        for job_id in &queued_job_ids {
            match self.cancel_job(tenant, job_id).await {
                Ok(()) => run_attempts_dropped += 1,
                Err(e) => {
                    warn!(
                        job_id = %job_id,
                        error = %e,
                        "failed to cancel queued run attempt during tenant drop"
                    );
                }
            }
        }

        // ---- Delete any remaining orphaned holder rows ----
        // Attempt finalizations above already released the holders for the
        // attempts they handled; what remains are holder rows with no
        // corresponding live attempt (genuine leaks). Deleting the snapshot is
        // idempotent — already-released rows are no-ops — and only touches rows
        // observed before finalization, so holders granted by retries scheduled
        // above (new task ids) are left intact.
        let mut batch = WriteBatch::new();
        let mut batch_count = 0usize;
        for key in &holder_keys {
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

        // Release holders from in-memory counts and wake the grant scanner per
        // freed queue so pending requests get re-granted. `atomic_release` is a
        // set removal (idempotent), so re-releasing a holder already released by
        // a finalization above is harmless.
        for (task_id, queue) in &holders_to_release {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
        }

        Ok(DropTenantStats {
            holders_dropped: holder_keys.len(),
            run_attempts_dropped,
        })
    }
}
