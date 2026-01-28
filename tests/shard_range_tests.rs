use silo::shard_range::{ShardId, ShardInfo, ShardMap, ShardMapError, ShardRange};

#[silo::test]
fn test_shard_id_creation() {
    let id1 = ShardId::new();
    let id2 = ShardId::new();
    assert_ne!(id1, id2);
}

#[silo::test]
fn test_shard_id_parse() {
    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    let id = ShardId::parse(uuid_str).unwrap();
    assert_eq!(id.to_string(), uuid_str);
}

#[silo::test]
fn test_shard_range_contains() {
    // Full range
    let full = ShardRange::full();
    assert!(full.contains("anything"));
    assert!(full.contains(""));
    assert!(full.contains("zzzzz"));

    // Bounded range
    let bounded = ShardRange::new("a", "m");
    assert!(!bounded.contains("")); // Before start
    assert!(bounded.contains("a")); // At start (inclusive)
    assert!(bounded.contains("abc"));
    assert!(bounded.contains("lzzz"));
    assert!(!bounded.contains("m")); // At end (exclusive)
    assert!(!bounded.contains("zzz"));

    // Unbounded start
    let unbounded_start = ShardRange::new("", "m");
    assert!(unbounded_start.contains(""));
    assert!(unbounded_start.contains("a"));
    assert!(!unbounded_start.contains("m"));
    assert!(!unbounded_start.contains("z"));

    // Unbounded end
    let unbounded_end = ShardRange::new("m", "");
    assert!(!unbounded_end.contains("a"));
    assert!(unbounded_end.contains("m"));
    assert!(unbounded_end.contains("z"));
}

#[silo::test]
fn test_shard_map_create_initial_single() {
    let map = ShardMap::create_initial(1).unwrap();
    assert_eq!(map.len(), 1);
    assert!(map.shards()[0].range.is_full());
    assert!(map.validate().is_ok());
}

#[silo::test]
fn test_shard_map_create_initial_multiple() {
    for count in [2, 4, 8, 16, 32, 64, 128] {
        let map = ShardMap::create_initial(count).unwrap();
        assert_eq!(map.len(), count as usize);
        assert!(
            map.validate().is_ok(),
            "validation failed for count={}",
            count
        );

        // First shard should have unbounded start
        assert!(map.shards()[0].range.is_start_unbounded());
        // Last shard should have unbounded end
        assert!(map.shards()[map.len() - 1].range.is_end_unbounded());
    }
}

#[silo::test]
fn test_shard_map_create_initial_zero() {
    let result = ShardMap::create_initial(0);
    assert!(result.is_err());
}

#[silo::test]
fn test_shard_map_shard_for_tenant() {
    let map = ShardMap::create_initial(4).unwrap();

    // Every tenant should map to some shard
    for tenant in ["a", "m", "z", "0", "9", "abc123", "tenant-1", "zzz"] {
        let shard = map.shard_for_tenant(tenant);
        assert!(shard.is_some(), "tenant '{}' should map to a shard", tenant);
    }

    // Same tenant always maps to same shard
    let shard1 = map.shard_for_tenant("my-tenant").unwrap();
    let shard2 = map.shard_for_tenant("my-tenant").unwrap();
    assert_eq!(shard1.id, shard2.id);
}

#[silo::test]
fn test_shard_map_full_coverage() {
    let map = ShardMap::create_initial(16).unwrap();

    // Test a variety of tenant IDs
    let test_tenants = [
        "",
        "0",
        "00000",
        "1",
        "a",
        "abc",
        "m",
        "z",
        "zzzzz",
        "tenant-123",
        "TENANT-456",
        "~special~",
    ];

    for tenant in test_tenants {
        let shard = map.shard_for_tenant(tenant);
        assert!(shard.is_some(), "tenant '{}' should map to a shard", tenant);
        assert!(
            shard.unwrap().contains(tenant),
            "shard should contain tenant '{}'",
            tenant
        );
    }
}

#[silo::test]
fn test_shard_map_validation_detects_gaps() {
    let mut map = ShardMap::new();
    map.add_shard(ShardInfo::new(ShardId::new(), ShardRange::new("", "m")));
    // Gap: missing "m" to "z"
    map.add_shard(ShardInfo::new(ShardId::new(), ShardRange::new("z", "")));

    let result = map.validate();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ShardMapError::GapInCoverage(_)
    ));
}

#[silo::test]
fn test_shard_map_validation_detects_duplicate_ids() {
    let id = ShardId::new();
    let mut map = ShardMap::new();
    map.add_shard(ShardInfo::new(id, ShardRange::new("", "m")));
    map.add_shard(ShardInfo::new(id, ShardRange::new("m", "")));

    let result = map.validate();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ShardMapError::DuplicateShardId(_)
    ));
}

#[silo::test]
fn test_shard_info_serialization() {
    let info = ShardInfo::new(ShardId::new(), ShardRange::new("a", "z"));
    let json = serde_json::to_string(&info).unwrap();
    let deserialized: ShardInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(info.id, deserialized.id);
    assert_eq!(info.range, deserialized.range);
}

#[silo::test]
fn test_shard_map_serialization() {
    let map = ShardMap::create_initial(4).unwrap();
    let json = serde_json::to_string(&map).unwrap();
    let deserialized: ShardMap = serde_json::from_str(&json).unwrap();
    assert_eq!(map.len(), deserialized.len());
    assert_eq!(map.version, deserialized.version);
}
