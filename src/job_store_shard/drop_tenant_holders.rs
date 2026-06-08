//! Tenant-wide forced cleanup of concurrency holders and in-flight run attempts.
//!
//! This backs the console's "Drop holders & run attempts" admin action: it
//! forcibly clears a tenant's stuck concurrency state on a single shard so the
//! tenant returns to a consistent, runnable state.

use slatedb::WriteBatch;
use tracing::warn;

use crate::codec::{decode_lease, decode_task};
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
}
