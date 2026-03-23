//! In-memory lease tracker to avoid expensive DB scans during lease reaping.
//!
//! Leases are still persisted to SlateDB for durability. The tracker maintains
//! a parallel in-memory view that is populated on shard startup by scanning the
//! DB once, then kept in sync via insert/remove/update calls from the dequeue,
//! heartbeat, and report_outcome paths.
//!
//! Hydration (the initial DB scan) runs asynchronously on shard open. Methods
//! that need a complete view of pre-existing leases (like `expired_leases`)
//! internally await hydration before returning, so callers don't need to know
//! about it.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::debug;

/// Minimal lease info needed for reaping decisions.
#[derive(Debug, Clone)]
pub struct TrackedLease {
    pub task_id: String,
    pub expiry_ms: i64,
}

/// Thread-safe in-memory tracker for active leases.
pub struct LeaseManager {
    /// task_id → expiry_ms
    leases: DashMap<String, i64>,
    /// Notified once when the initial DB hydration scan completes.
    hydration_complete: Notify,
    /// Whether hydration has finished (for fast-path skip of await).
    hydration_done: std::sync::atomic::AtomicBool,
}

impl LeaseManager {
    pub fn new() -> Self {
        Self {
            leases: DashMap::new(),
            hydration_complete: Notify::new(),
            hydration_done: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Record a new lease (called when a task is leased during dequeue).
    /// Safe to call before hydration — new leases are always tracked immediately.
    pub fn insert(&self, task_id: String, expiry_ms: i64) {
        self.leases.insert(task_id, expiry_ms);
    }

    /// Remove a lease (called when a lease is released via report_outcome or reaping).
    pub fn remove(&self, task_id: &str) {
        self.leases.remove(task_id);
    }

    /// Return all leases that have expired as of `now_ms`.
    /// Awaits hydration if it hasn't completed yet, so the result includes
    /// pre-existing leases from the DB.
    pub async fn expired_leases(&self, now_ms: i64) -> Vec<TrackedLease> {
        self.await_hydration().await;
        self.leases
            .iter()
            .filter(|entry| *entry.value() <= now_ms)
            .map(|entry| TrackedLease {
                task_id: entry.key().clone(),
                expiry_ms: *entry.value(),
            })
            .collect()
    }

    /// Return the set of all tracked task IDs.
    /// Awaits hydration if it hasn't completed yet.
    pub async fn all_task_ids(&self) -> std::collections::HashSet<String> {
        self.await_hydration().await;
        self.leases.iter().map(|e| e.key().clone()).collect()
    }

    /// Number of tracked leases.
    pub fn len(&self) -> usize {
        self.leases.len()
    }

    /// Kick off hydration from a DB scan. Must be called exactly once during shard startup.
    /// Retries on failure with backoff to ensure pre-existing leases are not lost.
    pub fn start_hydration(
        self: &Arc<Self>,
        db: Arc<slatedb::Db>,
        range: crate::shard_range::ShardRange,
    ) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            const MAX_RETRIES: usize = 5;
            let mut attempt = 0;

            loop {
                match mgr.try_hydrate(&db, &range).await {
                    Ok(count) => {
                        debug!(count, "lease manager: hydration complete");
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        if attempt >= MAX_RETRIES {
                            tracing::error!(
                                error = %e,
                                attempts = attempt,
                                "lease manager: hydration failed after max retries, \
                                 pre-existing leases may not be tracked"
                            );
                            break;
                        }
                        let backoff_ms = 100 * (1 << attempt.min(4));
                        tracing::warn!(
                            error = %e,
                            attempt,
                            backoff_ms,
                            "lease manager: hydration scan failed, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }

            mgr.hydration_done
                .store(true, std::sync::atomic::Ordering::Release);
            mgr.hydration_complete.notify_waiters();
        });
    }

    /// Wait for hydration to complete. Fast no-op if already done.
    async fn await_hydration(&self) {
        // Fast path: already hydrated
        if self
            .hydration_done
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        // Slow path: wait for notification. Create the Notified future
        // BEFORE re-checking to avoid a TOCTOU race.
        loop {
            let notified = self.hydration_complete.notified();
            if self
                .hydration_done
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return;
            }
            notified.await;
        }
    }

    /// Attempt a single hydration scan. Returns the number of leases loaded.
    async fn try_hydrate(
        &self,
        db: &slatedb::Db,
        range: &crate::shard_range::ShardRange,
    ) -> Result<usize, slatedb::Error> {
        let start = crate::keys::leases_prefix();
        let end = crate::keys::end_bound(&start);
        let mut iter = db.scan::<Vec<u8>, _>(start..end).await?;

        let mut count = 0usize;
        loop {
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => break,
                Err(e) => return Err(e),
            };

            let decoded = match crate::codec::decode_lease(kv.value.clone()) {
                Ok(l) => l,
                Err(_) => continue,
            };

            // Only track leases for tenants in our shard range
            if !range.contains_tenant(decoded.tenant()) {
                continue;
            }

            if let Some(task_id) = decoded.task_id() {
                self.leases.insert(task_id.to_string(), decoded.expiry_ms());
                count += 1;
            } else if decoded.refresh_floating_limit_info().is_some() {
                let (task_id, _) = decoded.refresh_floating_limit_info().unwrap();
                self.leases.insert(task_id.to_string(), decoded.expiry_ms());
                count += 1;
            }
        }

        Ok(count)
    }
}
