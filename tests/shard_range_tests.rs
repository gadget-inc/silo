use silo::coordination::MemberInfo;
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

#[silo::test]
fn test_shard_info_serialization_with_placement_ring() {
    let mut info = ShardInfo::new(ShardId::new(), ShardRange::new("a", "z"));
    info.placement_ring = Some("gpu-ring".to_string());

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: ShardInfo = serde_json::from_str(&json).unwrap();

    assert_eq!(info.id, deserialized.id);
    assert_eq!(info.range, deserialized.range);
    assert_eq!(info.placement_ring, deserialized.placement_ring);
    assert_eq!(deserialized.placement_ring, Some("gpu-ring".to_string()));
}

#[silo::test]
fn test_shard_info_serialization_without_placement_ring() {
    let info = ShardInfo::new(ShardId::new(), ShardRange::new("a", "z"));
    assert!(info.placement_ring.is_none());

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: ShardInfo = serde_json::from_str(&json).unwrap();

    assert_eq!(info.id, deserialized.id);
    assert!(deserialized.placement_ring.is_none());
}

#[silo::test]
fn test_shard_map_get_shard_mut() {
    let mut map = ShardMap::create_initial(4).unwrap();
    let shard_id = map.shards()[0].id;

    // Initially no placement ring
    assert!(map.get_shard(&shard_id).unwrap().placement_ring.is_none());

    // Update via get_shard_mut
    map.get_shard_mut(&shard_id).unwrap().placement_ring = Some("test-ring".to_string());

    // Verify the change persists
    assert_eq!(
        map.get_shard(&shard_id).unwrap().placement_ring,
        Some("test-ring".to_string())
    );
}

#[silo::test]
fn test_member_in_ring_default() {
    use silo::coordination::member_in_ring;

    // Member with empty rings participates in default
    let member_default = MemberInfo {
        node_id: "default-node".to_string(),
        grpc_addr: "http://default:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec![],
    };
    assert!(member_in_ring(&member_default, None));
    assert!(!member_in_ring(&member_default, Some("gpu")));

    // Member with explicit rings does NOT participate in default
    let member_gpu = MemberInfo {
        node_id: "gpu-node".to_string(),
        grpc_addr: "http://gpu:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec!["gpu".to_string()],
    };
    assert!(!member_in_ring(&member_gpu, None));
    assert!(member_in_ring(&member_gpu, Some("gpu")));
    assert!(!member_in_ring(&member_gpu, Some("cpu")));

    // Member with explicit "default" ring participates in default
    let member_explicit_default = MemberInfo {
        node_id: "explicit-default".to_string(),
        grpc_addr: "http://explicit:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec!["default".to_string(), "gpu".to_string()],
    };
    assert!(member_in_ring(&member_explicit_default, None));
    assert!(member_in_ring(&member_explicit_default, Some("gpu")));
}

#[silo::test]
fn test_select_owner_for_shard_default() {
    use silo::coordination::select_owner_for_shard;

    // Create members: one default-only, one with gpu ring
    let members = vec![
        MemberInfo {
            node_id: "default-node".to_string(),
            grpc_addr: "http://default:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "gpu-node".to_string(),
            grpc_addr: "http://gpu:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
    ];

    // Default shard (no ring) should be owned by default-node
    let shard_id = ShardId::new();
    let owner = select_owner_for_shard(&shard_id, None, &members);
    assert_eq!(owner, Some("default-node".to_string()));
}

#[silo::test]
fn test_select_owner_for_shard_specific() {
    use silo::coordination::select_owner_for_shard;

    // Create members: one default-only, one with gpu ring
    let members = vec![
        MemberInfo {
            node_id: "default-node".to_string(),
            grpc_addr: "http://default:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "gpu-node".to_string(),
            grpc_addr: "http://gpu:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
    ];

    // GPU shard should be owned by gpu-node
    let shard_id = ShardId::new();
    let owner = select_owner_for_shard(&shard_id, Some("gpu"), &members);
    assert_eq!(owner, Some("gpu-node".to_string()));
}

#[silo::test]
fn test_member_with_multiple_rings_can_own_multiple_shard_types() {
    use silo::coordination::select_owner_for_shard;

    // Create member with multiple rings
    let members = vec![MemberInfo {
        node_id: "multi-ring-node".to_string(),
        grpc_addr: "http://multi:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec!["gpu".to_string(), "high-memory".to_string()],
    }];

    let shard_id = ShardId::new();

    // GPU shard - should work
    let owner = select_owner_for_shard(&shard_id, Some("gpu"), &members);
    assert_eq!(owner, Some("multi-ring-node".to_string()));

    // High-memory shard - should work
    let owner = select_owner_for_shard(&shard_id, Some("high-memory"), &members);
    assert_eq!(owner, Some("multi-ring-node".to_string()));

    // Default shard - should NOT work (node doesn't participate in default)
    let owner = select_owner_for_shard(&shard_id, None, &members);
    assert_eq!(owner, None);
}

#[silo::test]
fn test_no_eligible_owner_returns_none() {
    use silo::coordination::select_owner_for_shard;

    // Create only default members
    let members = vec![MemberInfo {
        node_id: "default-node".to_string(),
        grpc_addr: "http://default:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec![],
    }];

    // GPU shard has no eligible owner
    let shard_id = ShardId::new();
    let owner = select_owner_for_shard(&shard_id, Some("gpu"), &members);
    assert_eq!(owner, None);
}

#[silo::test]
fn test_rendezvous_hash_consistent_with_ring_filtering() {
    use silo::coordination::select_owner_for_shard;

    // Create multiple members in the same ring
    let members = vec![
        MemberInfo {
            node_id: "gpu-1".to_string(),
            grpc_addr: "http://gpu1:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
        MemberInfo {
            node_id: "gpu-2".to_string(),
            grpc_addr: "http://gpu2:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
        MemberInfo {
            node_id: "gpu-3".to_string(),
            grpc_addr: "http://gpu3:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
    ];

    let shard_id = ShardId::new();

    // Same shard always maps to same owner (consistent hashing)
    let owner1 = select_owner_for_shard(&shard_id, Some("gpu"), &members);
    let owner2 = select_owner_for_shard(&shard_id, Some("gpu"), &members);
    assert_eq!(owner1, owner2);
    assert!(owner1.is_some());

    // Owner must be one of the gpu nodes
    let owner = owner1.unwrap();
    assert!(owner.starts_with("gpu-"));
}

#[silo::test]
fn test_compute_desired_shards_for_node() {
    use silo::coordination::compute_desired_shards_for_node;

    // Create some shards with different rings
    let default_shard = ShardInfo::new(ShardId::new(), ShardRange::new("", "m"));
    let mut gpu_shard = ShardInfo::new(ShardId::new(), ShardRange::new("m", ""));
    gpu_shard.placement_ring = Some("gpu".to_string());

    let shards: Vec<&ShardInfo> = vec![&default_shard, &gpu_shard];

    // Member that participates in default ring only
    let members = vec![
        MemberInfo {
            node_id: "default-node".to_string(),
            grpc_addr: "http://default:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "gpu-node".to_string(),
            grpc_addr: "http://gpu:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
    ];

    // Default node should own the default shard
    let desired_default = compute_desired_shards_for_node(&shards, "default-node", &members);
    assert!(desired_default.contains(&default_shard.id));
    assert!(!desired_default.contains(&gpu_shard.id));

    // GPU node should own the GPU shard
    let desired_gpu = compute_desired_shards_for_node(&shards, "gpu-node", &members);
    assert!(!desired_gpu.contains(&default_shard.id));
    assert!(desired_gpu.contains(&gpu_shard.id));
}
