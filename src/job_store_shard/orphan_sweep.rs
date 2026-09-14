//! Periodic sweep that releases durable concurrency holders whose task is
//! gone.
//!
//! A holder row can outlive the task it was granted for: no lease, no pending
//! task, and no late outcome report ever deletes it, so it occupies one of the
//! queue's slots forever. The sweep walks the shard's durable holder prefix in
//! bounded slices from the periodic concurrency-reconcile tick, classifies
//! every holder past the grace window with `classify_orphan_holder`, and
//! purges a holder once it has been classified orphan on two consecutive
//! completed passes. The two-pass confirmation means a point-in-time snapshot
//! racing an in-flight grant or lease write never purges a live holder.
//!
//! The sweep never touches leases, tasks, requests, or job status; it only
//! deletes holder rows and releases their in-memory reservations.

use std::collections::{HashMap, HashSet};

use crate::codec::{decode_holder, decode_job_status_owned};
use crate::concurrency::{OrphanReason, OrphanVerdict, classify_orphan_holder};
use crate::job::JobStatus;
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::helpers::now_epoch_ms;
use crate::keys::{
    ParsedConcurrencyHolderKey, concurrency_holders_prefix, end_bound, job_status_key,
    parse_concurrency_holder_key,
};
use crate::task::HolderRecord;

/// Holder identity used for candidate tracking across passes.
type HolderId = (String, String, String);

/// Sweep state carried between reconcile ticks. Never persisted: a shard
/// restart begins a fresh pass with no candidates.
#[derive(Debug, Default)]
pub(crate) struct OrphanSweepPass {
    /// Resume point in the durable holder keyspace. `None` starts a new pass
    /// at the prefix start.
    cursor: Option<Vec<u8>>,
    /// Holders classified orphan during the last completed pass.
    prev_candidates: HashSet<HolderId>,
    /// Holders classified orphan so far during the current pass.
    next_candidates: HashSet<HolderId>,
}

struct OrphanCandidate {
    key: ParsedConcurrencyHolderKey,
    holder: HolderRecord,
    reason: OrphanReason,
}

fn holder_id(key: &ParsedConcurrencyHolderKey) -> HolderId {
    (key.tenant.clone(), key.queue.clone(), key.task_id.clone())
}

impl JobStoreShard {
    /// Advance the orphan holder sweep by at most `slice` durable holder rows,
    /// resuming from the cursor saved by the previous call. Returns the number
    /// of holders purged by this call and whether the call completed a full
    /// pass (reached the end of the holder keyspace).
    ///
    /// Each holder past the grace window costs one lease point read and, when
    /// no unexpired lease exists and the record names an owner, one job
    /// status point read. Holders for tenants outside the shard range and
    /// holders whose value does not decode are skipped. A holder is purged
    /// only when the previous completed pass also classified it orphan; a
    /// candidate that classifies live, or is not revisited, drops out at the
    /// next pass boundary.
    pub(crate) async fn sweep_orphan_holders(&self, slice: usize) -> (usize, bool) {
        // Clamp to >= 1 so the cursor always advances; a slice of zero would
        // never walk a row and never complete a pass.
        let slice = slice.max(1);
        let now_ms = now_epoch_ms();
        let range = self.get_range();
        let prefix_start = concurrency_holders_prefix();
        let end = end_bound(&prefix_start);
        let saved_cursor = self.orphan_sweep.lock().unwrap().cursor.clone();
        let start = saved_cursor.unwrap_or(prefix_start);
        // The cold full-prefix walk must not evict hot blocks used by the
        // short-range scans on the grant path.
        let mut iter = match self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options_uncached())
            .await
        {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(error = %e, "orphan sweep: failed to scan holders");
                return (0, false);
            }
        };

        let mut walked: usize = 0;
        let mut last_key: Option<Vec<u8>> = None;
        let mut stopped_early = false;
        let mut candidates: Vec<OrphanCandidate> = Vec::new();
        loop {
            if walked >= slice {
                stopped_early = true;
                break;
            }
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "orphan sweep: holder scan iteration error");
                    stopped_early = true;
                    break;
                }
            };
            walked += 1;
            last_key = Some(kv.key.to_vec());

            let Some(key) = parse_concurrency_holder_key(&kv.key) else {
                continue;
            };
            if !range.contains_tenant(&key.tenant) {
                continue;
            }
            let holder = match decode_holder(&kv.value) {
                Ok(h) => h,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        tenant = %key.tenant,
                        queue = %key.queue,
                        task_id = %key.task_id,
                        "orphan sweep: undecodable holder record; skipping"
                    );
                    continue;
                }
            };
            // Younger than the grace window: skip before spending any reads.
            if now_ms.saturating_sub(holder.granted_at_ms) < self.orphan_holder_grace_ms {
                continue;
            }
            if let Some(reason) = self.orphan_reason_for(&key, &holder, now_ms).await {
                candidates.push(OrphanCandidate {
                    key,
                    holder,
                    reason,
                });
            }
        }
        let pass_completed = !stopped_early;

        // Fold this slice into the pass state under the lock (never held
        // across an await) and pick the candidates the previous completed
        // pass already flagged. Stopping early resumes at the successor of
        // the last walked key (append 0x00, the next key in bytewise order);
        // a clean end resets to the front and swaps the candidate sets.
        let to_purge: Vec<OrphanCandidate> = {
            let mut pass = self.orphan_sweep.lock().unwrap();
            pass.cursor = match (stopped_early, last_key) {
                (true, Some(mut key)) => {
                    key.push(0x00);
                    Some(key)
                }
                _ => None,
            };
            let mut to_purge = Vec::new();
            for candidate in candidates {
                let id = holder_id(&candidate.key);
                let confirmed = pass.prev_candidates.contains(&id);
                pass.next_candidates.insert(id);
                if confirmed {
                    to_purge.push(candidate);
                }
            }
            if pass_completed {
                pass.prev_candidates = std::mem::take(&mut pass.next_candidates);
            }
            to_purge
        };
        if to_purge.is_empty() {
            return (0, pass_completed);
        }

        let details: HashMap<HolderId, (HolderRecord, OrphanReason)> = to_purge
            .iter()
            .map(|c| (holder_id(&c.key), (c.holder.clone(), c.reason)))
            .collect();
        let keys: Vec<ParsedConcurrencyHolderKey> = to_purge.into_iter().map(|c| c.key).collect();
        // A failed purge leaves the candidates in `next_candidates` (or
        // `prev_candidates` at a pass boundary), so they are retried on a
        // later pass.
        let purged = match self.purge_orphan_holders(keys).await {
            Ok(purged) => purged,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "orphan sweep: purge failed; candidates retried on a later pass"
                );
                return (0, pass_completed);
            }
        };
        for key in &purged {
            let Some((holder, reason)) = details.get(&holder_id(key)) else {
                continue;
            };
            tracing::warn!(
                tenant = %key.tenant,
                queue = %key.queue,
                task_id = %key.task_id,
                job_id = holder.job_id.as_deref().unwrap_or(""),
                attempt = ?holder.attempt_number,
                granted_at_ms = holder.granted_at_ms,
                reason = reason.as_str(),
                "orphan sweep: purged concurrency holder"
            );
            if let Some(ref m) = self.metrics {
                m.record_concurrency_orphan_holders_purged(&self.name, reason.as_str(), 1);
            }
        }
        (purged.len(), pass_completed)
    }

    /// Point-read the lease and, when no unexpired lease exists and the record
    /// names an owner, the job status for one holder, then run the
    /// classifier. Returns the orphan
    /// reason, or `None` when the holder is live or a read failed (a read
    /// failure skips the holder for this pass rather than guessing).
    async fn orphan_reason_for(
        &self,
        key: &ParsedConcurrencyHolderKey,
        holder: &HolderRecord,
        now_ms: i64,
    ) -> Option<OrphanReason> {
        let lease_present = match self.has_unexpired_lease(&key.task_id, now_ms).await {
            Ok(present) => present,
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    task_id = %key.task_id,
                    "orphan sweep: lease read failed; skipping holder this pass"
                );
                return None;
            }
        };
        // An unexpired lease settles the verdict; skip the status read.
        if lease_present {
            return None;
        }
        let status: Option<JobStatus> = match &holder.job_id {
            None => None,
            Some(job_id) => match self.db.get(&job_status_key(&key.tenant, job_id)).await {
                Ok(Some(raw)) => match decode_job_status_owned(&raw) {
                    Ok(status) => Some(status),
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            tenant = %key.tenant,
                            job_id = %job_id,
                            "orphan sweep: undecodable job status; skipping holder this pass"
                        );
                        return None;
                    }
                },
                Ok(None) => None,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        tenant = %key.tenant,
                        job_id = %job_id,
                        "orphan sweep: job status read failed; skipping holder this pass"
                    );
                    return None;
                }
            },
        };
        match classify_orphan_holder(
            holder,
            lease_present,
            status.as_ref(),
            self.orphan_holder_grace_ms,
            self.orphan_holder_stale_ms,
            now_ms,
        ) {
            OrphanVerdict::Live => None,
            OrphanVerdict::Orphan { reason } => Some(reason),
        }
    }
}
