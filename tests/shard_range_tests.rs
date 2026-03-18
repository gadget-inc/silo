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
fn test_single_shard_assignment_default_ring() {
    use silo::coordination::compute_all_shard_assignments;

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

    let shard = ShardInfo::new(ShardId::new(), ShardRange::new("", ""));
    let shard_refs = vec![&shard];
    let assignments = compute_all_shard_assignments(&shard_refs, &members);

    // Default shard (no ring) should be owned by default-node.
    assert_eq!(
        assignments.get(&shard.id),
        Some(&"default-node".to_string())
    );
}

#[silo::test]
fn test_single_shard_assignment_specific_ring() {
    use silo::coordination::compute_all_shard_assignments;

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

    let mut shard = ShardInfo::new(ShardId::new(), ShardRange::new("", ""));
    shard.placement_ring = Some("gpu".to_string());
    let shard_refs = vec![&shard];
    let assignments = compute_all_shard_assignments(&shard_refs, &members);

    // GPU shard should be owned by gpu-node.
    assert_eq!(assignments.get(&shard.id), Some(&"gpu-node".to_string()));
}

#[silo::test]
fn test_member_with_multiple_rings_can_own_multiple_shard_types() {
    use silo::coordination::compute_all_shard_assignments;

    // Create member with multiple rings
    let members = vec![MemberInfo {
        node_id: "multi-ring-node".to_string(),
        grpc_addr: "http://multi:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec!["gpu".to_string(), "high-memory".to_string()],
    }];

    let mut gpu_shard = ShardInfo::new(ShardId::new(), ShardRange::new("", ""));
    gpu_shard.placement_ring = Some("gpu".to_string());
    let mut high_memory_shard = ShardInfo::new(ShardId::new(), ShardRange::new("m", ""));
    high_memory_shard.placement_ring = Some("high-memory".to_string());
    let default_shard = ShardInfo::new(ShardId::new(), ShardRange::new("z", ""));
    let shard_refs = vec![&gpu_shard, &high_memory_shard, &default_shard];
    let assignments = compute_all_shard_assignments(&shard_refs, &members);

    // Named-ring shards should be assigned to the multi-ring node.
    assert_eq!(
        assignments.get(&gpu_shard.id),
        Some(&"multi-ring-node".to_string())
    );
    assert_eq!(
        assignments.get(&high_memory_shard.id),
        Some(&"multi-ring-node".to_string())
    );

    // Default shard should not be assigned because there is no default-ring member.
    assert_eq!(assignments.get(&default_shard.id), None);
}

#[silo::test]
fn test_no_eligible_owner_returns_none() {
    use silo::coordination::compute_all_shard_assignments;

    // Create only default members
    let members = vec![MemberInfo {
        node_id: "default-node".to_string(),
        grpc_addr: "http://default:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec![],
    }];

    let mut shard = ShardInfo::new(ShardId::new(), ShardRange::new("", ""));
    shard.placement_ring = Some("gpu".to_string());
    let shard_refs = vec![&shard];
    let assignments = compute_all_shard_assignments(&shard_refs, &members);

    // GPU shard has no eligible owner.
    assert_eq!(assignments.get(&shard.id), None);
}

#[silo::test]
fn test_single_shard_assignment_is_deterministic_within_ring() {
    use silo::coordination::compute_all_shard_assignments;

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

    let mut shard = ShardInfo::new(ShardId::new(), ShardRange::new("", ""));
    shard.placement_ring = Some("gpu".to_string());
    let shard_refs = vec![&shard];

    // Same shard always maps to same owner.
    let owner1 = compute_all_shard_assignments(&shard_refs, &members)
        .get(&shard.id)
        .cloned();
    let owner2 = compute_all_shard_assignments(&shard_refs, &members)
        .get(&shard.id)
        .cloned();
    assert_eq!(owner1, owner2);
    assert!(owner1.is_some());

    // Owner must be one of the gpu nodes
    let owner = owner1.expect("single eligible ring should produce an owner");
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

#[silo::test]
fn test_bounded_load_balancing_even_distribution() {
    use silo::coordination::compute_all_shard_assignments;

    // Create 8 shards in the default ring, 3 nodes — should get 3/3/2 distribution
    let shards: Vec<ShardInfo> = (0..8)
        .map(|i| {
            ShardInfo::new(
                ShardId::new(),
                ShardRange::new(&format!("{}", i), &format!("{}", i + 1)),
            )
        })
        .collect();
    let shard_refs: Vec<&ShardInfo> = shards.iter().collect();

    let members = vec![
        MemberInfo {
            node_id: "node-0".to_string(),
            grpc_addr: "http://node0:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "node-1".to_string(),
            grpc_addr: "http://node1:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "node-2".to_string(),
            grpc_addr: "http://node2:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
    ];

    let assignments = compute_all_shard_assignments(&shard_refs, &members);

    // All shards should be assigned
    assert_eq!(assignments.len(), 8);

    // Count per node
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for owner in assignments.values() {
        *counts.entry(owner.as_str()).or_insert(0) += 1;
    }

    // With 8 shards and 3 nodes, max per node = ceil(8/3) = 3
    // So distribution must be some permutation of [3, 3, 2]
    let mut count_values: Vec<usize> = counts.values().copied().collect();
    count_values.sort();
    assert_eq!(
        count_values,
        vec![2, 3, 3],
        "Expected 3/3/2 distribution, got {:?}",
        counts
    );
}

#[silo::test]
fn test_bounded_load_balancing_deterministic() {
    use silo::coordination::compute_all_shard_assignments;

    let shards: Vec<ShardInfo> = (0..6)
        .map(|i| {
            ShardInfo::new(
                ShardId::new(),
                ShardRange::new(&format!("{}", i), &format!("{}", i + 1)),
            )
        })
        .collect();
    let shard_refs: Vec<&ShardInfo> = shards.iter().collect();

    let members = vec![
        MemberInfo {
            node_id: "a".to_string(),
            grpc_addr: "http://a:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "b".to_string(),
            grpc_addr: "http://b:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
    ];

    // Same inputs must always produce same outputs
    let a1 = compute_all_shard_assignments(&shard_refs, &members);
    let a2 = compute_all_shard_assignments(&shard_refs, &members);
    assert_eq!(a1, a2);

    // 6 shards / 2 nodes = exactly 3 each
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for owner in a1.values() {
        *counts.entry(owner.as_str()).or_insert(0) += 1;
    }
    assert_eq!(counts.get("a"), Some(&3));
    assert_eq!(counts.get("b"), Some(&3));
}

#[silo::test]
fn test_bounded_load_stability_on_node_add() {
    use silo::coordination::compute_all_shard_assignments;

    let shards: Vec<ShardInfo> = (0..12)
        .map(|i| {
            ShardInfo::new(
                ShardId::new(),
                ShardRange::new(&format!("{}", i), &format!("{}", i + 1)),
            )
        })
        .collect();
    let shard_refs: Vec<&ShardInfo> = shards.iter().collect();

    let members_2 = vec![
        MemberInfo {
            node_id: "node-a".to_string(),
            grpc_addr: "http://a:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "node-b".to_string(),
            grpc_addr: "http://b:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
    ];

    let mut members_3 = members_2.clone();
    members_3.push(MemberInfo {
        node_id: "node-c".to_string(),
        grpc_addr: "http://c:7450".to_string(),
        startup_time_ms: Some(1000),
        hostname: None,
        placement_rings: vec![],
    });

    let before = compute_all_shard_assignments(&shard_refs, &members_2);
    let after = compute_all_shard_assignments(&shard_refs, &members_3);

    // Count how many shards changed owner
    let mut moved = 0;
    for shard in &shards {
        if before.get(&shard.id) != after.get(&shard.id) {
            moved += 1;
        }
    }

    // With 12 shards going from 2 to 3 nodes, ideally ~4 move (1/3 of total).
    // Allow up to 6 (half) as a reasonable bound — the key property is that
    // the majority of shards stay put.
    assert!(
        moved <= 6,
        "Too many shards moved: {moved}/12. Before: {before:?}, After: {after:?}"
    );
    // At least some should move to the new node
    let new_node_count = after.values().filter(|v| v.as_str() == "node-c").count();
    assert!(
        new_node_count >= 2,
        "New node should get some shards, got {new_node_count}"
    );
}

#[silo::test]
fn test_bounded_load_per_ring() {
    use silo::coordination::compute_all_shard_assignments;

    // 4 default shards, 4 gpu shards, with different eligible nodes per ring
    let mut shards: Vec<ShardInfo> = Vec::new();
    for i in 0..4 {
        shards.push(ShardInfo::new(
            ShardId::new(),
            ShardRange::new(&format!("d{}", i), &format!("d{}", i + 1)),
        ));
    }
    for i in 0..4 {
        let mut s = ShardInfo::new(
            ShardId::new(),
            ShardRange::new(&format!("g{}", i), &format!("g{}", i + 1)),
        );
        s.placement_ring = Some("gpu".to_string());
        shards.push(s);
    }
    let shard_refs: Vec<&ShardInfo> = shards.iter().collect();

    let members = vec![
        MemberInfo {
            node_id: "default-1".to_string(),
            grpc_addr: "http://d1:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "default-2".to_string(),
            grpc_addr: "http://d2:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec![],
        },
        MemberInfo {
            node_id: "gpu-1".to_string(),
            grpc_addr: "http://g1:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
        MemberInfo {
            node_id: "gpu-2".to_string(),
            grpc_addr: "http://g2:7450".to_string(),
            startup_time_ms: Some(1000),
            hostname: None,
            placement_rings: vec!["gpu".to_string()],
        },
    ];

    let assignments = compute_all_shard_assignments(&shard_refs, &members);
    assert_eq!(assignments.len(), 8);

    // Default shards: 2 each across default-1 and default-2
    let default_counts: std::collections::HashMap<&str, usize> = shards[0..4]
        .iter()
        .filter_map(|s| assignments.get(&s.id).map(|o| o.as_str()))
        .fold(std::collections::HashMap::new(), |mut m, o| {
            *m.entry(o).or_insert(0) += 1;
            m
        });
    assert_eq!(default_counts.get("default-1"), Some(&2));
    assert_eq!(default_counts.get("default-2"), Some(&2));

    // GPU shards: 2 each across gpu-1 and gpu-2
    let gpu_counts: std::collections::HashMap<&str, usize> = shards[4..8]
        .iter()
        .filter_map(|s| assignments.get(&s.id).map(|o| o.as_str()))
        .fold(std::collections::HashMap::new(), |mut m, o| {
            *m.entry(o).or_insert(0) += 1;
            m
        });
    assert_eq!(gpu_counts.get("gpu-1"), Some(&2));
    assert_eq!(gpu_counts.get("gpu-2"), Some(&2));
}
