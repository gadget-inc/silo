//! Shard ownership invariants for the simulation scenarios.
//!
//! This file has no simulation dependencies, so `tests/shard_ownership_tracker_tests.rs`
//! includes it to run its unit tests in a binary that does not link
//! mad-turmoil. mad-turmoil replaces the process clock, and outside a
//! simulation that clock never advances, which the multi-threaded test
//! harness does not survive on Linux.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// Tracks shard ownership for split-brain detection and close-before-release ordering.
///
/// Verifies the invariants:
/// - noSplitBrain: A shard is owned by at most one node at any time
/// - closeBeforeRelease: Every ShardReleased was preceded by ShardClosed from the same node/shard
///
/// This tracker receives ShardAcquired/ShardClosed/ShardReleased events from server-side
/// instrumentation, allowing continuous verification without polling.
#[derive(Debug, Default)]
pub struct ShardOwnershipTracker {
    /// Maps shard_id -> current owner node_id (if any)
    current_owners: Mutex<HashMap<String, String>>,
    /// Records any split-brain violations detected: (shard_id, node1, node2, timestamp)
    violations: Mutex<Vec<(String, String, String, u64)>>,
    /// Monotonic counter for ordering events
    event_counter: std::sync::atomic::AtomicU64,
    /// Tracks which (node_id, shard_id) pairs have been closed but not yet released.
    /// Used to verify close-before-release ordering.
    pending_closes: Mutex<HashSet<(String, String)>>,
    /// Records close-before-release violations: (shard_id, node_id, timestamp)
    close_order_violations: Mutex<Vec<(String, String, u64)>>,
    /// Number of reopens observed per shard. Lets a scenario prove it exercised
    /// the reopen path rather than passing vacuously.
    reopens: Mutex<HashMap<String, u32>>,
    /// Nodes that have been marked as crashed. When a crashed node's shards are
    /// acquired by another node, it is not considered split-brain since the crashed
    /// node is no longer running (even though it never emitted ShardReleased events).
    crashed_nodes: Mutex<HashSet<String>>,
}

impl ShardOwnershipTracker {
    /// Create a new tracker.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a node acquired ownership of a shard.
    /// If another node already owns this shard, records a split-brain violation
    /// (unless the previous owner was marked as crashed via `node_crashed`).
    pub fn shard_acquired(&self, node_id: &str, shard_id: &str) {
        let timestamp = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut owners = self.current_owners.lock().unwrap();

        if let Some(existing_owner) = owners.get(shard_id) {
            if existing_owner != node_id {
                // Check if the existing owner was marked as crashed — if so, this is
                // expected takeover behavior, not split-brain.
                let crashed = self.crashed_nodes.lock().unwrap();
                if crashed.contains(existing_owner) {
                    tracing::debug!(
                        shard_id = %shard_id,
                        crashed_owner = %existing_owner,
                        new_owner = %node_id,
                        timestamp = timestamp,
                        "shard takeover from crashed node (not split-brain)"
                    );
                } else {
                    // SPLIT-BRAIN DETECTED!
                    tracing::error!(
                        shard_id = %shard_id,
                        existing_owner = %existing_owner,
                        new_owner = %node_id,
                        timestamp = timestamp,
                        "SPLIT-BRAIN DETECTED: shard acquired by new node while still owned by another"
                    );
                    let mut violations = self.violations.lock().unwrap();
                    violations.push((
                        shard_id.to_string(),
                        existing_owner.clone(),
                        node_id.to_string(),
                        timestamp,
                    ));
                }
            }
            // Even if same node, update is fine (idempotent)
        }

        owners.insert(shard_id.to_string(), node_id.to_string());
        tracing::trace!(
            shard_id = %shard_id,
            node_id = %node_id,
            timestamp = timestamp,
            "shard_ownership_acquired"
        );
    }

    /// Record that a node successfully closed a shard's storage.
    pub fn shard_closed(&self, node_id: &str, shard_id: &str) {
        let timestamp = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut pending = self.pending_closes.lock().unwrap();
        pending.insert((node_id.to_string(), shard_id.to_string()));
        tracing::trace!(
            shard_id = %shard_id,
            node_id = %node_id,
            timestamp = timestamp,
            "shard_closed"
        );
    }

    /// Record that a node released ownership of a shard.
    /// Also verifies that ShardClosed was emitted before ShardReleased.
    pub fn shard_released(&self, node_id: &str, shard_id: &str) {
        let timestamp = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Check close-before-release ordering
        {
            let mut pending = self.pending_closes.lock().unwrap();
            let key = (node_id.to_string(), shard_id.to_string());
            if !pending.remove(&key) {
                // ShardReleased without a preceding ShardClosed
                tracing::error!(
                    shard_id = %shard_id,
                    node_id = %node_id,
                    timestamp = timestamp,
                    "CLOSE-BEFORE-RELEASE VIOLATION: ShardReleased without preceding ShardClosed"
                );
                let mut violations = self.close_order_violations.lock().unwrap();
                violations.push((shard_id.to_string(), node_id.to_string(), timestamp));
            }
        }

        let mut owners = self.current_owners.lock().unwrap();

        if let Some(existing_owner) = owners.get(shard_id) {
            if existing_owner == node_id {
                owners.remove(shard_id);
                tracing::trace!(
                    shard_id = %shard_id,
                    node_id = %node_id,
                    timestamp = timestamp,
                    "shard_ownership_released"
                );
            } else {
                // Node releasing a shard it doesn't own - this might indicate a race
                // but isn't necessarily a split-brain (the release might be stale)
                tracing::warn!(
                    shard_id = %shard_id,
                    releasing_node = %node_id,
                    actual_owner = %existing_owner,
                    timestamp = timestamp,
                    "shard_release_by_non_owner"
                );
            }
        } else {
            // Releasing an unowned shard - might happen during cleanup
            tracing::trace!(
                shard_id = %shard_id,
                node_id = %node_id,
                timestamp = timestamp,
                "shard_release_of_unowned"
            );
        }
    }

    /// Record that a node crashed without emitting ShardClosed/ShardReleased events.
    /// Marks the node as crashed so that when DST events are replayed during
    /// `process_and_validate`, acquisitions by other nodes for shards previously
    /// owned by this crashed node are not flagged as split-brain.
    pub fn node_crashed(&self, node_id: &str) {
        let mut crashed = self.crashed_nodes.lock().unwrap();
        crashed.insert(node_id.to_string());
    }

    /// Verify no split-brain occurred. Panics if any violations were detected.
    pub fn verify_no_split_brain(&self) {
        let violations = self.violations.lock().unwrap();
        if !violations.is_empty() {
            let details: Vec<String> = violations
                .iter()
                .map(|(shard, n1, n2, ts)| {
                    format!(
                        "shard {} owned by both {} and {} at event {}",
                        shard, n1, n2, ts
                    )
                })
                .collect();
            panic!(
                "INVARIANT VIOLATION (noSplitBrain): {} split-brain events detected:\n  {}",
                violations.len(),
                details.join("\n  ")
            );
        }
    }

    /// Verify close-before-release ordering. Panics if any violations were detected.
    pub fn verify_close_before_release(&self) {
        let violations = self.close_order_violations.lock().unwrap();
        if !violations.is_empty() {
            let details: Vec<String> = violations
                .iter()
                .map(|(shard, node, ts)| {
                    format!(
                        "shard {} released by {} without preceding close at event {}",
                        shard, node, ts
                    )
                })
                .collect();
            panic!(
                "INVARIANT VIOLATION (closeBeforeRelease): {} violations detected:\n  {}",
                violations.len(),
                details.join("\n  ")
            );
        }
    }

    /// Record that a node reopened a shard it kept ownership of after closing it.
    /// The reopen consumes the pending close, so a later release needs a close
    /// of its own; the current owner is unchanged.
    pub fn shard_reopened(&self, node_id: &str, shard_id: &str) {
        let timestamp = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut pending = self.pending_closes.lock().unwrap();
        pending.remove(&(node_id.to_string(), shard_id.to_string()));
        *self
            .reopens
            .lock()
            .unwrap()
            .entry(shard_id.to_string())
            .or_insert(0) += 1;
        tracing::trace!(
            shard_id = %shard_id,
            node_id = %node_id,
            timestamp = timestamp,
            "shard_reopened"
        );
    }

    /// Number of reopens observed for a shard.
    #[allow(dead_code)]
    pub fn reopen_count(&self, shard_id: &str) -> u32 {
        self.reopens
            .lock()
            .unwrap()
            .get(shard_id)
            .copied()
            .unwrap_or(0)
    }

    /// Check if any close-before-release violations have been detected (non-panicking).
    pub fn has_close_order_violations(&self) -> bool {
        let violations = self.close_order_violations.lock().unwrap();
        !violations.is_empty()
    }

    /// Check if any split-brain violations have been detected (non-panicking).
    pub fn has_violations(&self) -> bool {
        let violations = self.violations.lock().unwrap();
        !violations.is_empty()
    }

    /// Get the current owner of a shard (for debugging).
    #[allow(dead_code)]
    pub fn get_owner(&self, shard_id: &str) -> Option<String> {
        let owners = self.current_owners.lock().unwrap();
        owners.get(shard_id).cloned()
    }

    /// Get the count of currently owned shards (for debugging).
    #[allow(dead_code)]
    pub fn owned_shard_count(&self) -> usize {
        let owners = self.current_owners.lock().unwrap();
        owners.len()
    }
}

#[cfg(test)]
mod tests {
    use super::ShardOwnershipTracker;

    const NODE: &str = "node-a";
    const SHARD: &str = "shard-1";

    /// A reopen consumes the close that preceded it, so a later release with
    /// no close of its own is a close-before-release violation.
    #[test]
    fn reopen_clears_the_pending_close() {
        let tracker = ShardOwnershipTracker::new();
        tracker.shard_acquired(NODE, SHARD);

        tracker.shard_closed(NODE, SHARD);
        tracker.shard_reopened(NODE, SHARD);
        tracker.shard_released(NODE, SHARD);

        assert!(
            tracker.has_close_order_violations(),
            "a release after close + reopen has no close of its own"
        );
    }

    #[test]
    fn close_then_release_is_not_a_violation() {
        let tracker = ShardOwnershipTracker::new();
        tracker.shard_acquired(NODE, SHARD);

        tracker.shard_closed(NODE, SHARD);
        tracker.shard_released(NODE, SHARD);

        assert!(!tracker.has_close_order_violations());
    }

    #[test]
    fn reopen_keeps_the_current_owner() {
        let tracker = ShardOwnershipTracker::new();
        tracker.shard_acquired(NODE, SHARD);

        tracker.shard_closed(NODE, SHARD);
        tracker.shard_reopened(NODE, SHARD);

        assert_eq!(tracker.get_owner(SHARD).as_deref(), Some(NODE));
    }

    #[test]
    fn reopens_are_counted_per_shard() {
        let tracker = ShardOwnershipTracker::new();
        assert_eq!(tracker.reopen_count(SHARD), 0);

        tracker.shard_reopened(NODE, SHARD);
        tracker.shard_reopened(NODE, SHARD);

        assert_eq!(tracker.reopen_count(SHARD), 2);
        assert_eq!(tracker.reopen_count("other-shard"), 0);
    }
}
