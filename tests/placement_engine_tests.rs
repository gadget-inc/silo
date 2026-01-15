//! Unit and integration tests for the placement engine.
//!
//! These tests verify:
//! - Load calculation and aggregation
//! - Migration decision algorithm
//! - Override expiration and TTL handling
//! - Rate limiting and cooldown behavior

use silo::coordination::placement::{compute_desired_shards_with_overrides, PlacementOverride, PlacementOverrides};
use silo::job_store_shard::stats::{ShardStats, ShardStatsSnapshot, ShardLoadReport};
use silo::placement_engine::{ClusterLoad, NodeLoadSnapshot, ShardLoadSnapshot, Migration};
use silo::settings::PlacementConfig;
use std::collections::HashMap;
use std::time::Instant;

// =============================================================================
// ShardStats Tests
// =============================================================================

#[test]
fn test_shard_stats_basic_operations() {
    let stats = ShardStats::new();
    
    // Initial state
    assert_eq!(stats.pending_jobs(), 0);
    assert_eq!(stats.running_jobs(), 0);
    
    // Enqueue increases pending
    stats.record_enqueue();
    stats.record_enqueue();
    stats.record_enqueue();
    assert_eq!(stats.pending_jobs(), 3);
    assert_eq!(stats.running_jobs(), 0);
    
    // Dequeue moves from pending to running
    stats.record_dequeue();
    assert_eq!(stats.pending_jobs(), 2);
    assert_eq!(stats.running_jobs(), 1);
    
    // Complete decreases running
    stats.record_job_completed();
    assert_eq!(stats.pending_jobs(), 2);
    assert_eq!(stats.running_jobs(), 0);
    
    // Requeue moves from running to pending
    stats.record_dequeue();
    stats.record_job_requeued();
    assert_eq!(stats.pending_jobs(), 2);
    assert_eq!(stats.running_jobs(), 0);
}

#[test]
fn test_shard_stats_snapshot() {
    let stats = ShardStats::new();
    
    // Generate some activity
    for _ in 0..100 {
        stats.record_enqueue();
    }
    for _ in 0..50 {
        stats.record_dequeue();
    }
    for _ in 0..20 {
        stats.record_job_completed();
    }
    
    let snapshot = stats.snapshot();
    
    // Verify counts
    assert_eq!(snapshot.pending_jobs, 50);
    assert_eq!(snapshot.running_jobs, 30);
    assert_eq!(snapshot.total_enqueued, 100);
    assert_eq!(snapshot.total_dequeued, 50);
    
    // Rates should be non-negative
    assert!(snapshot.enqueue_rate_per_sec >= 0.0);
    assert!(snapshot.dequeue_rate_per_sec >= 0.0);
}

#[test]
fn test_load_score_calculation() {
    // High throughput, low queue depth
    let snapshot1 = ShardStatsSnapshot {
        enqueue_rate_per_sec: 100.0,
        dequeue_rate_per_sec: 100.0,
        pending_jobs: 10,
        running_jobs: 5,
        total_enqueued: 1000,
        total_dequeued: 1000,
    };
    
    // Low throughput, high queue depth
    let snapshot2 = ShardStatsSnapshot {
        enqueue_rate_per_sec: 10.0,
        dequeue_rate_per_sec: 10.0,
        pending_jobs: 1000,
        running_jobs: 50,
        total_enqueued: 1000,
        total_dequeued: 1000,
    };
    
    let score1 = snapshot1.load_score();
    let score2 = snapshot2.load_score();
    
    // Both should be positive
    assert!(score1 > 0.0);
    assert!(score2 > 0.0);
    
    // High queue depth should result in higher score
    // (demonstrates that pending jobs contribute significantly)
    assert!(score2 > score1);
}

// =============================================================================
// PlacementOverride Tests
// =============================================================================

#[test]
fn test_placement_override_expiry() {
    // Create an override with 1 second TTL
    let mut override_ = PlacementOverride::new(1, "node-a".to_string(), 1);
    
    // Should not be expired immediately
    assert!(!override_.is_expired());
    
    // Manually set to past
    override_.created_at_ms = override_.created_at_ms - 2000;
    assert!(override_.is_expired());
    
    // Refresh should reset
    override_.refresh();
    assert!(!override_.is_expired());
}

#[test]
fn test_placement_override_expiry_time() {
    let override_ = PlacementOverride::new(1, "node-a".to_string(), 300);
    
    let expiry = override_.expiry_ms();
    let expected = override_.created_at_ms + 300_000;
    
    assert_eq!(expiry, expected);
}

#[test]
fn test_placement_overrides_collection() {
    let mut overrides = PlacementOverrides::new();
    
    // Empty collection
    assert_eq!(overrides.active_count(), 0);
    assert!(overrides.get_target(1).is_none());
    
    // Add overrides
    overrides.set(PlacementOverride::new(1, "node-a".to_string(), 3600));
    overrides.set(PlacementOverride::new(2, "node-b".to_string(), 3600));
    overrides.set(PlacementOverride::new(3, "node-a".to_string(), 3600));
    
    assert_eq!(overrides.active_count(), 3);
    assert_eq!(overrides.get_target(1), Some("node-a"));
    assert_eq!(overrides.get_target(2), Some("node-b"));
    assert_eq!(overrides.get_target(3), Some("node-a"));
    assert_eq!(overrides.get_target(4), None);
    
    // Remove one
    let removed = overrides.remove(2);
    assert!(removed.is_some());
    assert_eq!(overrides.active_count(), 2);
    assert!(overrides.get_target(2).is_none());
}

#[test]
fn test_placement_overrides_prune_expired() {
    let mut overrides = PlacementOverrides::new();
    
    // Add a mix of expired and non-expired overrides
    let mut expired = PlacementOverride::new(1, "node-a".to_string(), 1);
    expired.created_at_ms = expired.created_at_ms - 5000; // Expired
    overrides.set(expired);
    
    overrides.set(PlacementOverride::new(2, "node-b".to_string(), 3600)); // Not expired
    
    assert_eq!(overrides.all().len(), 2);
    
    // Prune should remove the expired one
    let pruned = overrides.prune_expired();
    assert_eq!(pruned, 1);
    assert_eq!(overrides.active_count(), 1);
    assert!(overrides.get_target(1).is_none());
    assert!(overrides.get_target(2).is_some());
}

#[test]
fn test_placement_overrides_merge() {
    let mut base = PlacementOverrides::new();
    let mut old_override = PlacementOverride::new(1, "node-a".to_string(), 3600);
    old_override.created_at_ms -= 1000; // Make it older
    base.set(old_override);
    base.set(PlacementOverride::new(2, "node-a".to_string(), 3600));
    
    let mut other = PlacementOverrides::new();
    // Newer override for shard 1 (explicitly newer timestamp)
    let new_override = PlacementOverride::new(1, "node-b".to_string(), 3600);
    other.set(new_override);
    // New override for shard 3
    other.set(PlacementOverride::new(3, "node-c".to_string(), 3600));
    
    base.merge(&other);
    
    // Should have 3 overrides now
    assert_eq!(base.active_count(), 3);
    // Shard 1 should be updated to node-b (newer)
    assert_eq!(base.get_target(1), Some("node-b"));
    // Shard 2 unchanged
    assert_eq!(base.get_target(2), Some("node-a"));
    // Shard 3 added
    assert_eq!(base.get_target(3), Some("node-c"));
}

// =============================================================================
// compute_desired_shards_with_overrides Tests
// =============================================================================

#[test]
fn test_compute_desired_with_no_overrides() {
    let members = vec!["node-a".to_string(), "node-b".to_string()];
    let overrides: HashMap<u32, PlacementOverride> = HashMap::new();
    
    let desired_a = compute_desired_shards_with_overrides(8, "node-a", &members, &overrides);
    let desired_b = compute_desired_shards_with_overrides(8, "node-b", &members, &overrides);
    
    // All shards should be assigned to exactly one node
    for shard_id in 0..8 {
        let in_a = desired_a.contains(&shard_id);
        let in_b = desired_b.contains(&shard_id);
        assert!(in_a != in_b, "shard {} should be in exactly one node", shard_id);
    }
    
    // Total shards should equal num_shards
    assert_eq!(desired_a.len() + desired_b.len(), 8);
}

#[test]
fn test_compute_desired_with_override_to_self() {
    let members = vec!["node-a".to_string(), "node-b".to_string()];
    
    // Override shard 0 to node-a
    let mut overrides: HashMap<u32, PlacementOverride> = HashMap::new();
    overrides.insert(0, PlacementOverride::new(0, "node-a".to_string(), 3600));
    
    let desired_a = compute_desired_shards_with_overrides(8, "node-a", &members, &overrides);
    
    // node-a should definitely have shard 0
    assert!(desired_a.contains(&0));
}

#[test]
fn test_compute_desired_with_override_away() {
    let members = vec!["node-a".to_string(), "node-b".to_string()];
    
    // Override shard 0 to node-b (regardless of what rendezvous hashing says)
    let mut overrides: HashMap<u32, PlacementOverride> = HashMap::new();
    overrides.insert(0, PlacementOverride::new(0, "node-b".to_string(), 3600));
    
    let desired_a = compute_desired_shards_with_overrides(8, "node-a", &members, &overrides);
    let desired_b = compute_desired_shards_with_overrides(8, "node-b", &members, &overrides);
    
    // node-a should NOT have shard 0
    assert!(!desired_a.contains(&0));
    // node-b should have shard 0
    assert!(desired_b.contains(&0));
}

#[test]
fn test_compute_desired_ignores_expired_overrides() {
    let members = vec!["node-a".to_string(), "node-b".to_string()];
    
    // Create an expired override
    let mut expired = PlacementOverride::new(0, "node-b".to_string(), 1);
    expired.created_at_ms = expired.created_at_ms - 5000; // Expired
    
    let mut overrides: HashMap<u32, PlacementOverride> = HashMap::new();
    overrides.insert(0, expired);
    
    // The result should be the same as with no overrides
    let with_override = compute_desired_shards_with_overrides(8, "node-a", &members, &overrides);
    let without_override = compute_desired_shards_with_overrides(8, "node-a", &members, &HashMap::new());
    
    assert_eq!(with_override, without_override);
}

// =============================================================================
// ClusterLoad Tests
// =============================================================================

#[test]
fn test_cluster_load_average() {
    let cluster = ClusterLoad {
        nodes: vec![
            NodeLoadSnapshot {
                node_id: "node-a".to_string(),
                grpc_addr: "node-a:50051".to_string(),
                shards: vec![],
                total_load_score: 100.0,
            },
            NodeLoadSnapshot {
                node_id: "node-b".to_string(),
                grpc_addr: "node-b:50051".to_string(),
                shards: vec![],
                total_load_score: 50.0,
            },
        ],
        timestamp: Instant::now(),
    };
    
    let avg = cluster.average_load_per_node();
    assert!((avg - 75.0).abs() < 0.001);
}

#[test]
fn test_cluster_load_max_min() {
    let cluster = ClusterLoad {
        nodes: vec![
            NodeLoadSnapshot {
                node_id: "node-a".to_string(),
                grpc_addr: "node-a:50051".to_string(),
                shards: vec![],
                total_load_score: 100.0,
            },
            NodeLoadSnapshot {
                node_id: "node-b".to_string(),
                grpc_addr: "node-b:50051".to_string(),
                shards: vec![],
                total_load_score: 50.0,
            },
            NodeLoadSnapshot {
                node_id: "node-c".to_string(),
                grpc_addr: "node-c:50051".to_string(),
                shards: vec![],
                total_load_score: 150.0,
            },
        ],
        timestamp: Instant::now(),
    };
    
    let max = cluster.max_loaded_node().unwrap();
    let min = cluster.min_loaded_node().unwrap();
    
    assert_eq!(max.node_id, "node-c");
    assert_eq!(min.node_id, "node-b");
}

#[test]
fn test_cluster_load_empty() {
    let cluster = ClusterLoad {
        nodes: vec![],
        timestamp: Instant::now(),
    };
    
    assert_eq!(cluster.average_load_per_node(), 0.0);
    assert!(cluster.max_loaded_node().is_none());
    assert!(cluster.min_loaded_node().is_none());
}

// =============================================================================
// PlacementConfig Tests
// =============================================================================

#[test]
fn test_placement_config_defaults() {
    let config = PlacementConfig::default();
    
    // Verify conservative defaults
    assert!(!config.enabled);
    assert_eq!(config.load_imbalance_threshold, 2.0);
    assert_eq!(config.stability_intervals, 3);
    assert_eq!(config.migration_cooldown_secs, 60);
    assert_eq!(config.max_migrations_per_hour, 10);
    assert_eq!(config.load_collection_interval_secs, 5);
    assert_eq!(config.override_ttl_secs, 300);
}

// =============================================================================
// Load Score Edge Cases
// =============================================================================

#[test]
fn test_load_score_zero_activity() {
    let snapshot = ShardStatsSnapshot {
        enqueue_rate_per_sec: 0.0,
        dequeue_rate_per_sec: 0.0,
        pending_jobs: 0,
        running_jobs: 0,
        total_enqueued: 0,
        total_dequeued: 0,
    };
    
    assert_eq!(snapshot.load_score(), 0.0);
}

#[test]
fn test_load_score_large_values() {
    let snapshot = ShardStatsSnapshot {
        enqueue_rate_per_sec: 100_000.0,
        dequeue_rate_per_sec: 100_000.0,
        pending_jobs: 1_000_000,
        running_jobs: 100_000,
        total_enqueued: u64::MAX / 2,
        total_dequeued: u64::MAX / 2,
    };
    
    let score = snapshot.load_score();
    assert!(score.is_finite());
    assert!(score > 0.0);
}

// =============================================================================
// ShardLoadReport Tests
// =============================================================================

#[test]
fn test_shard_load_report_from_snapshot() {
    let snapshot = ShardStatsSnapshot {
        enqueue_rate_per_sec: 50.0,
        dequeue_rate_per_sec: 40.0,
        pending_jobs: 100,
        running_jobs: 20,
        total_enqueued: 1000,
        total_dequeued: 800,
    };
    
    let report = ShardLoadReport::from_snapshot(42, snapshot.clone());
    
    assert_eq!(report.shard_id, 42);
    assert_eq!(report.enqueue_rate_per_sec, 50.0);
    assert_eq!(report.dequeue_rate_per_sec, 40.0);
    assert_eq!(report.pending_jobs, 100);
    assert_eq!(report.running_jobs, 20);
    assert_eq!(report.load_score, snapshot.load_score());
}
