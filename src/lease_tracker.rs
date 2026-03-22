//! In-memory lease tracker to avoid expensive DB scans during lease reaping.
//!
//! Leases are still persisted to SlateDB for durability. The tracker maintains
//! a parallel in-memory view that is populated on shard startup by scanning the
//! DB once, then kept in sync via insert/remove/update calls from the dequeue,
//! heartbeat, and report_outcome paths.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use tracing::debug;

/// Minimal lease info needed for reaping decisions.
#[derive(Debug, Clone)]
pub struct TrackedLease {
    pub task_id: String,
    pub expiry_ms: i64,
}

/// Thread-safe in-memory tracker for active leases.
pub struct LeaseTracker {
    /// task_id → expiry_ms
    leases: DashMap<String, i64>,
    /// Whether the initial DB scan has completed.
    hydrated: AtomicBool,
}

impl LeaseTracker {
    pub fn new() -> Self {
        Self {
            leases: DashMap::new(),
            hydrated: AtomicBool::new(false),
        }
    }

    /// Record a new lease (called when a task is leased during dequeue).
    pub fn insert(&self, task_id: String, expiry_ms: i64) {
        self.leases.insert(task_id, expiry_ms);
    }

    /// Remove a lease (called when a lease is released via report_outcome or reaping).
    pub fn remove(&self, task_id: &str) {
        self.leases.remove(task_id);
    }

    /// Update the expiry of an existing lease (called during heartbeat).
    pub fn update_expiry(&self, task_id: &str, new_expiry_ms: i64) {
        if let Some(mut entry) = self.leases.get_mut(task_id) {
            *entry = new_expiry_ms;
        }
    }

    /// Returns true if the initial DB hydration scan has completed.
    pub fn is_hydrated(&self) -> bool {
        self.hydrated.load(Ordering::Acquire)
    }

    /// Return all leases that have expired as of `now_ms`.
    pub fn expired_leases(&self, now_ms: i64) -> Vec<TrackedLease> {
        self.leases
            .iter()
            .filter(|entry| *entry.value() <= now_ms)
            .map(|entry| TrackedLease {
                task_id: entry.key().clone(),
                expiry_ms: *entry.value(),
            })
            .collect()
    }

    /// Number of tracked leases.
    pub fn len(&self) -> usize {
        self.leases.len()
    }

    /// Populate the tracker from a DB scan. Called once during shard startup.
    pub async fn hydrate_from_db(
        self: &Arc<Self>,
        db: &slatedb::Db,
        range: &crate::shard_range::ShardRange,
    ) {
        let start = crate::keys::leases_prefix();
        let end = crate::keys::end_bound(&start);
        let mut iter = match db.scan::<Vec<u8>, _>(start..end).await {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(error = %e, "lease tracker: failed to scan leases for hydration");
                // Mark as hydrated anyway so the reaper doesn't wait forever
                self.hydrated.store(true, Ordering::Release);
                return;
            }
        };

        let mut count = 0usize;
        loop {
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "lease tracker: hydration scan iteration error");
                    break;
                }
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
                // Track floating limit refresh leases too
                let (task_id, _) = decoded.refresh_floating_limit_info().unwrap();
                self.leases.insert(task_id.to_string(), decoded.expiry_ms());
                count += 1;
            }
        }

        self.hydrated.store(true, Ordering::Release);
        debug!(count, "lease tracker: hydration complete");
    }
}
