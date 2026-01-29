use silo::coordination::{SplitCleanupStatus, SplitPhase};
use silo::shard_range::{ShardId, ShardInfo, ShardMap, ShardMapError, ShardRange, SplitInProgress};

#[test]
fn cleanup_status_default_is_compaction_done() {
    let status: SplitCleanupStatus = Default::default();
    assert_eq!(status, SplitCleanupStatus::CompactionDone);
}

#[test]
fn cleanup_status_needs_work() {
    assert!(!SplitCleanupStatus::CompactionDone.needs_work());
    assert!(SplitCleanupStatus::CleanupPending.needs_work());
    assert!(SplitCleanupStatus::CleanupRunning.needs_work());
    assert!(SplitCleanupStatus::CleanupDone.needs_work());
}

#[test]
fn split_phase_traffic_paused() {
    assert!(!SplitPhase::SplitRequested.traffic_paused());
    assert!(SplitPhase::SplitPausing.traffic_paused());
    assert!(SplitPhase::SplitCloning.traffic_paused());
    assert!(SplitPhase::SplitUpdatingMap.traffic_paused());
    assert!(!SplitPhase::SplitComplete.traffic_paused());
}

#[test]
fn split_phase_is_early_phase() {
    // [SILO-SPLIT-CRASH-1] Early phases are before children exist
    assert!(SplitPhase::SplitRequested.is_early_phase());
    assert!(SplitPhase::SplitPausing.is_early_phase());
    assert!(SplitPhase::SplitCloning.is_early_phase());
    assert!(!SplitPhase::SplitUpdatingMap.is_early_phase());
    assert!(!SplitPhase::SplitComplete.is_early_phase());
}

#[test]
fn split_phase_is_late_phase() {
    // [SILO-SPLIT-CRASH-2] Late phases are after children exist
    assert!(!SplitPhase::SplitRequested.is_late_phase());
    assert!(!SplitPhase::SplitPausing.is_late_phase());
    assert!(!SplitPhase::SplitCloning.is_late_phase());
    assert!(SplitPhase::SplitUpdatingMap.is_late_phase());
    assert!(SplitPhase::SplitComplete.is_late_phase());
}

#[test]
fn split_phase_display() {
    assert_eq!(format!("{}", SplitPhase::SplitRequested), "SplitRequested");
    assert_eq!(format!("{}", SplitPhase::SplitPausing), "SplitPausing");
    assert_eq!(format!("{}", SplitPhase::SplitCloning), "SplitCloning");
    assert_eq!(
        format!("{}", SplitPhase::SplitUpdatingMap),
        "SplitUpdatingMap"
    );
    assert_eq!(format!("{}", SplitPhase::SplitComplete), "SplitComplete");
}

#[test]
fn split_in_progress_new() {
    let parent_id = ShardId::new();
    let split = SplitInProgress::new(parent_id, "tenant-500".to_string(), "node-1".to_string());

    assert_eq!(split.parent_shard_id, parent_id);
    assert_eq!(split.split_point, "tenant-500");
    assert_eq!(split.initiator_node_id, "node-1");
    assert_eq!(split.phase, SplitPhase::SplitRequested);
    assert_ne!(split.left_child_id, split.right_child_id);
    assert_ne!(split.left_child_id, parent_id);
    assert_ne!(split.right_child_id, parent_id);
    assert!(!split.is_complete());
}

#[test]
fn split_in_progress_advance_phase() {
    let mut split = SplitInProgress::new(ShardId::new(), "m".to_string(), "node".to_string());

    assert_eq!(split.phase, SplitPhase::SplitRequested);

    split.advance_phase();
    assert_eq!(split.phase, SplitPhase::SplitPausing);

    split.advance_phase();
    assert_eq!(split.phase, SplitPhase::SplitCloning);

    split.advance_phase();
    assert_eq!(split.phase, SplitPhase::SplitUpdatingMap);

    split.advance_phase();
    assert_eq!(split.phase, SplitPhase::SplitComplete);
    assert!(split.is_complete());

    // SplitComplete is terminal
    split.advance_phase();
    assert_eq!(split.phase, SplitPhase::SplitComplete);
}

#[test]
fn shard_range_split_basic() {
    let range = ShardRange::new("a", "z");
    let (left, right) = range.split("m").expect("split should succeed");

    // [SILO-COORD-INV-2] Verify children cover parent's full range
    assert_eq!(left.start, "a");
    assert_eq!(left.end, "m");
    assert_eq!(right.start, "m");
    assert_eq!(right.end, "z");

    // [SILO-COORD-INV-3] Verify children are contiguous
    assert_eq!(left.end, right.start);
}

#[test]
fn shard_range_split_full_range() {
    let range = ShardRange::full();
    let (left, right) = range.split("middle").expect("split should succeed");

    // Unbounded start should be preserved on left child
    assert!(left.is_start_unbounded());
    assert_eq!(left.end, "middle");

    // Unbounded end should be preserved on right child
    assert_eq!(right.start, "middle");
    assert!(right.is_end_unbounded());
}

#[test]
fn shard_range_split_unbounded_start() {
    let range = ShardRange::new("", "z"); // Unbounded start
    let (left, right) = range.split("m").expect("split should succeed");

    assert!(left.is_start_unbounded());
    assert_eq!(left.end, "m");
    assert_eq!(right.start, "m");
    assert_eq!(right.end, "z");
}

#[test]
fn shard_range_split_unbounded_end() {
    let range = ShardRange::new("a", ""); // Unbounded end
    let (left, right) = range.split("m").expect("split should succeed");

    assert_eq!(left.start, "a");
    assert_eq!(left.end, "m");
    assert_eq!(right.start, "m");
    assert!(right.is_end_unbounded());
}

#[test]
fn shard_range_split_point_not_in_range() {
    let range = ShardRange::new("d", "h");

    // Split point before range
    let result = range.split("a");
    assert!(matches!(result, Err(ShardMapError::InvalidSplitPoint(_))));

    // Split point after range
    let result = range.split("z");
    assert!(matches!(result, Err(ShardMapError::InvalidSplitPoint(_))));
}

#[test]
fn shard_range_split_at_boundary() {
    let range = ShardRange::new("a", "z");

    // Can't split at the start (would create empty left child)
    let result = range.split("a");
    assert!(matches!(result, Err(ShardMapError::InvalidSplitPoint(_))));
}

#[test]
fn shard_range_split_children_contain_correct_tenants() {
    let range = ShardRange::new("aaa", "zzz");
    let (left, right) = range.split("mmm").expect("split should succeed");

    // Left child should contain tenants < split_point
    assert!(left.contains("aaa"));
    assert!(left.contains("abc"));
    assert!(left.contains("lll"));
    assert!(!left.contains("mmm")); // Exclusive end
    assert!(!left.contains("nnn"));

    // Right child should contain tenants >= split_point
    assert!(!right.contains("aaa"));
    assert!(!right.contains("lll"));
    assert!(right.contains("mmm")); // Inclusive start
    assert!(right.contains("nnn"));
    assert!(right.contains("yyy"));
}

#[test]
fn shard_range_midpoint_basic() {
    let range = ShardRange::new("a", "z");
    let midpoint = range.midpoint();

    // Midpoint should exist
    assert!(midpoint.is_some());
    let mid = midpoint.unwrap();

    // Midpoint should be within range
    assert!(range.contains(&mid));

    // Midpoint should be strictly between start and end
    assert!(mid.as_str() > "a");
    assert!(mid.as_str() < "z");
}

#[test]
fn shard_range_midpoint_full_range() {
    let range = ShardRange::full();
    let midpoint = range.midpoint();

    assert!(midpoint.is_some());
    let mid = midpoint.unwrap();
    assert!(range.contains(&mid));
}

#[test]
fn shard_range_midpoint_can_be_used_for_split() {
    let range = ShardRange::new("abc", "xyz");
    if let Some(mid) = range.midpoint() {
        let result = range.split(&mid);
        assert!(result.is_ok(), "midpoint should be valid for splitting");

        let (left, right) = result.unwrap();
        assert_eq!(left.end, right.start);
    }
}

#[test]
fn shard_info_new_has_compaction_done_status() {
    let info = ShardInfo::new(ShardId::new(), ShardRange::full());
    assert_eq!(info.cleanup_status, SplitCleanupStatus::CompactionDone);
    assert!(info.parent_shard_id.is_none());
}

#[test]
fn shard_info_from_split_has_cleanup_pending_status() {
    let parent_id = ShardId::new();
    let info = ShardInfo::from_split(ShardId::new(), ShardRange::new("a", "m"), parent_id);

    // [SILO-SPLIT-CLEANUP-START-1] Children from split have CleanupPending
    assert_eq!(info.cleanup_status, SplitCleanupStatus::CleanupPending);
    assert_eq!(info.parent_shard_id, Some(parent_id));
}

#[test]
fn shard_map_split_shard_basic() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");
    let original_shard_id = map.shards()[0].id;
    let original_version = map.version;

    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let (left, right) = map
        .split_shard(&original_shard_id, "m", left_child_id, right_child_id)
        .expect("split should succeed");

    // Map should now have 2 shards
    assert_eq!(map.len(), 2);

    // Version should be incremented
    assert!(map.version > original_version);

    // Original shard should be gone
    assert!(map.get_shard(&original_shard_id).is_none());

    // Children should exist with correct IDs
    assert!(map.get_shard(&left_child_id).is_some());
    assert!(map.get_shard(&right_child_id).is_some());

    // Children should have correct ranges
    assert!(left.range.is_start_unbounded());
    assert_eq!(left.range.end, "m");
    assert_eq!(right.range.start, "m");
    assert!(right.range.is_end_unbounded());

    // [SILO-COORD-INV-3] Children should be contiguous
    assert_eq!(left.range.end, right.range.start);

    // Children should have parent reference
    assert_eq!(left.parent_shard_id, Some(original_shard_id));
    assert_eq!(right.parent_shard_id, Some(original_shard_id));

    // Children should have CleanupPending status
    assert_eq!(left.cleanup_status, SplitCleanupStatus::CleanupPending);
    assert_eq!(right.cleanup_status, SplitCleanupStatus::CleanupPending);
}

#[test]
fn shard_map_split_preserves_keyspace_coverage() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");
    let shard_id = map.shards()[0].id;

    map.split_shard(&shard_id, "tenant-500", ShardId::new(), ShardId::new())
        .expect("split should succeed");

    // [SILO-COORD-INV-2] Map should still be valid (full coverage)
    map.validate().expect("map should be valid after split");

    // Should be able to find shards for all tenants
    assert!(map.shard_for_tenant("a").is_some());
    assert!(map.shard_for_tenant("tenant-100").is_some());
    assert!(map.shard_for_tenant("tenant-500").is_some());
    assert!(map.shard_for_tenant("tenant-900").is_some());
    assert!(map.shard_for_tenant("z").is_some());
}

#[test]
fn shard_map_split_tenant_routing() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");
    let shard_id = map.shards()[0].id;
    let left_id = ShardId::new();
    let right_id = ShardId::new();

    map.split_shard(&shard_id, "m", left_id, right_id)
        .expect("split should succeed");

    // Tenants before split point should route to left child
    let shard_for_a = map.shard_for_tenant("a").unwrap();
    assert_eq!(shard_for_a.id, left_id);

    let shard_for_l = map.shard_for_tenant("l").unwrap();
    assert_eq!(shard_for_l.id, left_id);

    // Tenants at or after split point should route to right child
    let shard_for_m = map.shard_for_tenant("m").unwrap();
    assert_eq!(shard_for_m.id, right_id);

    let shard_for_z = map.shard_for_tenant("z").unwrap();
    assert_eq!(shard_for_z.id, right_id);
}

#[test]
fn shard_map_split_shard_not_found() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");
    let nonexistent_id = ShardId::new();

    let result = map.split_shard(&nonexistent_id, "m", ShardId::new(), ShardId::new());
    assert!(matches!(result, Err(ShardMapError::ShardNotFound(_))));
}

#[test]
fn shard_map_split_invalid_split_point() {
    let mut map = ShardMap::create_initial(2).expect("create initial map with 2 shards");

    // Find a shard that doesn't cover "m"
    let shard_id = map
        .shards()
        .iter()
        .find(|s| !s.range.contains("m"))
        .map(|s| s.id);
    if let Some(id) = shard_id {
        let result = map.split_shard(&id, "m", ShardId::new(), ShardId::new());
        assert!(matches!(result, Err(ShardMapError::InvalidSplitPoint(_))));
    }
}

#[test]
fn shard_map_sequential_splits() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");

    // First split
    let shard_id = map.shards()[0].id;
    let left1 = ShardId::new();
    let right1 = ShardId::new();
    map.split_shard(&shard_id, "m", left1, right1)
        .expect("first split should succeed");
    assert_eq!(map.len(), 2);

    // Second split on left child
    let left2 = ShardId::new();
    let right2 = ShardId::new();
    map.split_shard(&left1, "g", left2, right2)
        .expect("second split should succeed");
    assert_eq!(map.len(), 3);

    // Third split on right child of first split
    let left3 = ShardId::new();
    let right3 = ShardId::new();
    map.split_shard(&right1, "t", left3, right3)
        .expect("third split should succeed");
    assert_eq!(map.len(), 4);

    // Map should still be valid
    map.validate()
        .expect("map should be valid after multiple splits");

    // All tenants should route correctly
    assert_eq!(map.shard_for_tenant("a").unwrap().id, left2);
    assert_eq!(map.shard_for_tenant("h").unwrap().id, right2);
    assert_eq!(map.shard_for_tenant("n").unwrap().id, left3);
    assert_eq!(map.shard_for_tenant("z").unwrap().id, right3);
}

#[test]
fn shard_map_update_cleanup_status() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");
    let shard_id = map.shards()[0].id;
    let left_id = ShardId::new();

    map.split_shard(&shard_id, "m", left_id, ShardId::new())
        .expect("split should succeed");

    // Child should start with CleanupPending
    assert_eq!(
        map.get_shard(&left_id).unwrap().cleanup_status,
        SplitCleanupStatus::CleanupPending
    );

    // Update to CleanupRunning
    map.update_cleanup_status(&left_id, SplitCleanupStatus::CleanupRunning)
        .expect("update should succeed");
    assert_eq!(
        map.get_shard(&left_id).unwrap().cleanup_status,
        SplitCleanupStatus::CleanupRunning
    );

    // Update to CleanupDone
    map.update_cleanup_status(&left_id, SplitCleanupStatus::CleanupDone)
        .expect("update should succeed");
    assert_eq!(
        map.get_shard(&left_id).unwrap().cleanup_status,
        SplitCleanupStatus::CleanupDone
    );

    // Update to CompactionDone
    map.update_cleanup_status(&left_id, SplitCleanupStatus::CompactionDone)
        .expect("update should succeed");
    assert_eq!(
        map.get_shard(&left_id).unwrap().cleanup_status,
        SplitCleanupStatus::CompactionDone
    );
}

#[test]
fn shard_map_shards_needing_cleanup() {
    let mut map = ShardMap::create_initial(1).expect("create initial map");
    let shard_id = map.shards()[0].id;

    // Initially no shards need cleanup
    assert!(map.shards_needing_cleanup().is_empty());

    // After split, both children need cleanup
    let left_id = ShardId::new();
    let right_id = ShardId::new();
    map.split_shard(&shard_id, "m", left_id, right_id)
        .expect("split should succeed");

    let needing_cleanup = map.shards_needing_cleanup();
    assert_eq!(needing_cleanup.len(), 2);

    // Complete cleanup on one shard
    map.update_cleanup_status(&left_id, SplitCleanupStatus::CompactionDone)
        .expect("update should succeed");

    let needing_cleanup = map.shards_needing_cleanup();
    assert_eq!(needing_cleanup.len(), 1);
    assert_eq!(needing_cleanup[0].id, right_id);
}

#[test]
fn cleanup_status_serialization_roundtrip() {
    let statuses = [
        SplitCleanupStatus::CleanupPending,
        SplitCleanupStatus::CleanupRunning,
        SplitCleanupStatus::CleanupDone,
        SplitCleanupStatus::CompactionDone,
    ];

    for status in statuses {
        let json = serde_json::to_string(&status).expect("serialize");
        let deserialized: SplitCleanupStatus = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(status, deserialized);
    }
}

#[test]
fn split_phase_serialization_roundtrip() {
    let phases = [
        SplitPhase::SplitRequested,
        SplitPhase::SplitPausing,
        SplitPhase::SplitCloning,
        SplitPhase::SplitUpdatingMap,
        SplitPhase::SplitComplete,
    ];

    for phase in phases {
        let json = serde_json::to_string(&phase).expect("serialize");
        let deserialized: SplitPhase = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(phase, deserialized);
    }
}

#[test]
fn split_in_progress_serialization_roundtrip() {
    let split = SplitInProgress::new(
        ShardId::new(),
        "split-point".to_string(),
        "node-1".to_string(),
    );

    let json = serde_json::to_string(&split).expect("serialize");
    let deserialized: SplitInProgress = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(split.parent_shard_id, deserialized.parent_shard_id);
    assert_eq!(split.split_point, deserialized.split_point);
    assert_eq!(split.left_child_id, deserialized.left_child_id);
    assert_eq!(split.right_child_id, deserialized.right_child_id);
    assert_eq!(split.phase, deserialized.phase);
    assert_eq!(split.initiator_node_id, deserialized.initiator_node_id);
}

#[test]
fn shard_info_with_cleanup_status_serialization() {
    let info = ShardInfo::from_split(ShardId::new(), ShardRange::new("a", "m"), ShardId::new());

    let json = serde_json::to_string(&info).expect("serialize");
    let deserialized: ShardInfo = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(info.cleanup_status, deserialized.cleanup_status);
    assert_eq!(info.parent_shard_id, deserialized.parent_shard_id);
}

#[test]
fn shard_info_default_cleanup_status_on_deserialize() {
    // Simulate old JSON without cleanup_status field
    let json = r#"{
        "id": "00000000-0000-0000-0000-000000000001",
        "range": {"start": "", "end": ""},
        "created_at_ms": 0,
        "parent_shard_id": null
    }"#;

    let deserialized: ShardInfo = serde_json::from_str(json).expect("deserialize");

    // Should default to CompactionDone (backward compatible)
    assert_eq!(
        deserialized.cleanup_status,
        SplitCleanupStatus::CompactionDone
    );
}
