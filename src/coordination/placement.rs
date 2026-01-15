//! Placement overrides for load-aware shard placement.
//!
//! This module provides the ability to override the default rendezvous hashing
//! shard placement with explicit node assignments. The placement engine uses
//! these overrides to balance load across the cluster.
//!
//! Key properties:
//! - Overrides take precedence over rendezvous hashing
//! - Overrides have a TTL and expire if not refreshed
//! - The system falls back to rendezvous hashing if overrides expire

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A placement override assigns a shard to a specific node, bypassing rendezvous hashing.
///
/// Overrides are created by the placement engine leader when it decides to move
/// a shard from one node to another for load balancing purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementOverride {
    /// The shard ID being overridden
    pub shard_id: u32,
    /// The target node that should own this shard
    pub target_node: String,
    /// When the override was created (epoch millis)
    pub created_at_ms: i64,
    /// How long the override is valid for (seconds)
    /// The override expires if not refreshed within this time
    pub ttl_secs: u32,
    /// Optional reason for the override (for debugging/observability)
    pub reason: Option<String>,
}

impl PlacementOverride {
    /// Create a new placement override.
    pub fn new(shard_id: u32, target_node: String, ttl_secs: u32) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            shard_id,
            target_node,
            created_at_ms: now_ms,
            ttl_secs,
            reason: None,
        }
    }

    /// Create a new placement override with a reason.
    pub fn with_reason(shard_id: u32, target_node: String, ttl_secs: u32, reason: String) -> Self {
        let mut override_ = Self::new(shard_id, target_node, ttl_secs);
        override_.reason = Some(reason);
        override_
    }

    /// Check if the override has expired.
    pub fn is_expired(&self) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let expiry_ms = self.created_at_ms + (self.ttl_secs as i64 * 1000);
        now_ms > expiry_ms
    }

    /// Get the expiry time in epoch millis.
    pub fn expiry_ms(&self) -> i64 {
        self.created_at_ms + (self.ttl_secs as i64 * 1000)
    }

    /// Refresh the override by resetting the creation time.
    pub fn refresh(&mut self) {
        self.created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
    }
}

/// Compute the desired set of shard IDs for a node, taking placement overrides into account.
///
/// Overrides take precedence over the default rendezvous hashing assignment.
/// If an override assigns a shard to this node, it will be included.
/// If an override assigns a shard to another node, it will be excluded (even if
/// rendezvous hashing would assign it to this node).
pub fn compute_desired_shards_with_overrides(
    num_shards: u32,
    node_id: &str,
    member_ids: &[String],
    overrides: &HashMap<u32, PlacementOverride>,
) -> std::collections::HashSet<u32> {
    // Start with the default rendezvous hashing assignment
    let mut desired =
        crate::coordination::compute_desired_shards_for_node(num_shards, node_id, member_ids);

    // Apply overrides
    for (shard_id, override_) in overrides {
        // Skip expired overrides
        if override_.is_expired() {
            continue;
        }

        if override_.target_node == node_id {
            // This override assigns the shard to us
            desired.insert(*shard_id);
        } else {
            // This override assigns the shard to another node
            desired.remove(shard_id);
        }
    }

    desired
}

/// A collection of placement overrides with helper methods.
#[derive(Debug, Default, Clone)]
pub struct PlacementOverrides {
    overrides: HashMap<u32, PlacementOverride>,
}

impl PlacementOverrides {
    /// Create a new empty set of overrides.
    pub fn new() -> Self {
        Self {
            overrides: HashMap::new(),
        }
    }

    /// Add or update an override.
    pub fn set(&mut self, override_: PlacementOverride) {
        self.overrides.insert(override_.shard_id, override_);
    }

    /// Remove an override.
    pub fn remove(&mut self, shard_id: u32) -> Option<PlacementOverride> {
        self.overrides.remove(&shard_id)
    }

    /// Get an override by shard ID.
    pub fn get(&self, shard_id: u32) -> Option<&PlacementOverride> {
        self.overrides.get(&shard_id)
    }

    /// Get all overrides.
    pub fn all(&self) -> &HashMap<u32, PlacementOverride> {
        &self.overrides
    }

    /// Remove all expired overrides and return the count of removed entries.
    pub fn prune_expired(&mut self) -> usize {
        let before = self.overrides.len();
        self.overrides.retain(|_, v| !v.is_expired());
        before - self.overrides.len()
    }

    /// Check if there's an active (non-expired) override for a shard.
    pub fn has_active_override(&self, shard_id: u32) -> bool {
        self.overrides
            .get(&shard_id)
            .map(|o| !o.is_expired())
            .unwrap_or(false)
    }

    /// Get the target node for a shard if there's an active override.
    pub fn get_target(&self, shard_id: u32) -> Option<&str> {
        self.overrides
            .get(&shard_id)
            .filter(|o| !o.is_expired())
            .map(|o| o.target_node.as_str())
    }

    /// Count of active (non-expired) overrides.
    pub fn active_count(&self) -> usize {
        self.overrides.values().filter(|o| !o.is_expired()).count()
    }

    /// Merge another set of overrides into this one.
    /// Newer overrides (by created_at_ms) win.
    pub fn merge(&mut self, other: &PlacementOverrides) {
        for (shard_id, new_override) in &other.overrides {
            match self.overrides.get(shard_id) {
                Some(existing) if existing.created_at_ms >= new_override.created_at_ms => {
                    // Keep existing (it's newer or same age)
                }
                _ => {
                    // New override is newer or no existing override
                    self.overrides.insert(*shard_id, new_override.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_override_expiry() {
        let mut override_ = PlacementOverride::new(1, "node-a".to_string(), 1); // 1 second TTL

        // Should not be expired immediately
        assert!(!override_.is_expired());

        // Manually set created_at_ms to the past
        override_.created_at_ms = override_.created_at_ms - 2000; // 2 seconds ago
        assert!(override_.is_expired());

        // Refresh should reset the timer
        override_.refresh();
        assert!(!override_.is_expired());
    }

    #[test]
    fn test_compute_desired_with_overrides() {
        let members = vec!["node-a".to_string(), "node-b".to_string()];

        // Without overrides
        let base_desired =
            crate::coordination::compute_desired_shards_for_node(8, "node-a", &members);

        // With an override moving shard 0 to node-b
        let mut overrides = HashMap::new();
        overrides.insert(
            0,
            PlacementOverride::new(0, "node-b".to_string(), 3600),
        );

        let with_override = compute_desired_shards_with_overrides(8, "node-a", &members, &overrides);

        // node-a should not have shard 0 (even if base would have assigned it)
        assert!(!with_override.contains(&0));
    }

    #[test]
    fn test_placement_overrides_collection() {
        let mut overrides = PlacementOverrides::new();

        // Add some overrides
        overrides.set(PlacementOverride::new(1, "node-a".to_string(), 3600));
        overrides.set(PlacementOverride::new(2, "node-b".to_string(), 3600));

        assert_eq!(overrides.active_count(), 2);
        assert_eq!(overrides.get_target(1), Some("node-a"));
        assert_eq!(overrides.get_target(2), Some("node-b"));
        assert_eq!(overrides.get_target(3), None);
    }
}
