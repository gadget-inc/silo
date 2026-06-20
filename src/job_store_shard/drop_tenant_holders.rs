//! Tenant-wide forced release of concurrency holders.
//!
//! This backs the console's "Drop holders" emergency admin action: when a
//! tenant's concurrency holders are *leaked* (held by attempts that will never
//! release them), no slots are free and the tenant's jobs stall. Force-deleting
//! the holders frees the slots so the grant scanner re-grants and jobs start
//! flowing again.

use slatedb::WriteBatch;

use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{concurrency_holders_tenant_prefix, end_bound, parse_concurrency_holder_key};

/// Max deletes flushed per write batch. Bounds batch size/memory on a tenant
/// with a very large amount of stuck state; the operation is idempotent so
/// chunking across multiple batches is safe.
const DROP_BATCH_SIZE: usize = 1000;

/// Counts of what `drop_tenant_holders` removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropTenantStats {
    /// Number of concurrency holder records deleted.
    pub holders_dropped: usize,
}

impl JobStoreShard {
    /// Forcibly delete **all** concurrency holders for `tenant` on this shard and
    /// release their in-memory counts, freeing the held slots so the grant
    /// scanner can re-grant pending requests.
    ///
    /// This is an **emergency** action for recovering a tenant whose holders have
    /// leaked. It deliberately touches **only** the concurrency holder rows: it
    /// does NOT cancel or fail in-flight run attempts and does NOT touch leases,
    /// pending tasks, or `JOB_STATUS`. In-flight attempts keep running to
    /// completion exactly as normal — there is no orphaning risk, because no
    /// lease or task is deleted out from under a job.
    ///
    /// Tradeoff: freeing a held slot while its attempt is still running lets the
    /// scanner grant another attempt for the same queue, so a job may briefly
    /// **over-admit / double-run**. That is the accepted cost of an emergency
    /// unblock — double-running is safe and far preferable to a wedged tenant.
    ///
    /// Semantics:
    /// - **Point-in-time / best-effort.** Holders created for the tenant *after*
    ///   the scan are not caught; re-run if needed.
    /// - **Idempotent / re-runnable.** Re-deleting a missing holder row is a
    ///   no-op, and `atomic_release` is a set removal, so releasing an
    ///   already-released holder is harmless.
    /// - Historical `ATTEMPT` records and concurrency *requests* are left
    ///   untouched.
    #[tracing::instrument(skip_all, fields(shard = %self.name, tenant))]
    pub async fn drop_tenant_holders(
        &self,
        tenant: &str,
    ) -> Result<DropTenantStats, JobStoreShardError> {
        // Holders are tenant-prefixed, so a single prefix scan captures every
        // holder for the tenant. Track (task_id, queue) for the in-memory release
        // below; holder counts have no persisted counter (in-memory only), so
        // deleting the rows is all that's needed durably.
        //
        // The scan is read-only (a point-in-time snapshot; no SSI read set), so
        // it never conflicts with concurrent activity on a live, busy shard.
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

        // ---- Delete the holder rows durably (chunked, idempotent) ----
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
        // set removal (idempotent), so re-releasing an already-released holder is
        // harmless.
        for (task_id, queue) in &holders_to_release {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
        }

        Ok(DropTenantStats {
            holders_dropped: holder_keys.len(),
        })
    }
}
