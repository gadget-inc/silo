use std::collections::HashSet;

use silo::shard_range::ShardMap;

/// Demonstrates that hash-based sharding distributes env-XXXXXXXXXX tenants
/// across multiple shards, avoiding the hotspot that occurred with
/// lexicographic range-based sharding.
#[silo::test]
fn env_tenants_with_10_digit_ids_spread_across_shards() {
    // Test across multiple shard counts that are realistic for production
    for shard_count in [2, 4, 8, 16] {
        let map = ShardMap::create_initial(shard_count).unwrap();

        // Generate a variety of env-XXXXXXXXXX tenant keys
        let tenants: Vec<String> = vec![
            "env-1000000000".to_string(),
            "env-1000000001".to_string(),
            "env-1234567890".to_string(),
            "env-2000000000".to_string(),
            "env-5555555555".to_string(),
            "env-9999999999".to_string(),
            "env-0000000001".to_string(),
            "env-4294967296".to_string(), // 2^32
        ];

        let mut shard_ids = HashSet::new();
        for tenant in &tenants {
            let shard = map
                .shard_for_tenant(tenant)
                .unwrap_or_else(|| panic!("tenant '{}' should map to a shard", tenant));
            shard_ids.insert(shard.id);
        }

        assert!(
            shard_ids.len() > 1,
            "With {} shards, env-XXXXXXXXXX tenants should spread across multiple shards, \
             but they all landed on {} shard(s)",
            shard_count,
            shard_ids.len(),
        );
    }
}

/// Shows that with many tenants, hash-based sharding distributes them well
/// across all available shards.
#[silo::test]
fn env_tenants_spread_across_shards_with_many_tenants() {
    let shard_count = 16;
    let map = ShardMap::create_initial(shard_count).unwrap();

    let mut shard_ids = HashSet::new();
    // Generate 1000 different env- tenant keys
    for i in 1_000_000_000u64..1_000_001_000u64 {
        let tenant = format!("env-{}", i);
        let shard = map.shard_for_tenant(&tenant).unwrap();
        shard_ids.insert(shard.id);
    }

    assert!(
        shard_ids.len() >= shard_count as usize - 1,
        "1000 env- tenants should spread across nearly all {} shards, \
         but only hit {} shards",
        shard_count,
        shard_ids.len()
    );
}

/// Confirms that both env- prefixed tenants and diverse tenants distribute
/// across shards, since hashing eliminates prefix-based clustering.
#[silo::test]
fn both_env_and_diverse_tenants_spread_across_shards() {
    let map = ShardMap::create_initial(16).unwrap();

    // Diverse tenants spread across shards
    let diverse_tenants = vec![
        "0-tenant", "1-tenant", "2-tenant", "3-tenant", "4-tenant", "5-tenant", "6-tenant",
        "7-tenant", "8-tenant", "9-tenant", "a-tenant", "b-tenant", "c-tenant", "d-tenant",
        "e-tenant", "f-tenant",
    ];

    let mut diverse_shard_ids = HashSet::new();
    for tenant in &diverse_tenants {
        let shard = map.shard_for_tenant(tenant).unwrap();
        diverse_shard_ids.insert(shard.id);
    }

    // env- tenants also spread across shards (use more tenants for statistical reliability)
    let env_tenants: Vec<String> = (0..100).map(|i| format!("env-{:010}", i)).collect();
    let mut env_shard_ids = HashSet::new();
    for tenant in &env_tenants {
        let shard = map.shard_for_tenant(tenant).unwrap();
        env_shard_ids.insert(shard.id);
    }

    assert!(
        diverse_shard_ids.len() > 1,
        "Diverse tenants should spread across multiple shards, got {}",
        diverse_shard_ids.len()
    );
    assert!(
        env_shard_ids.len() > 1,
        "env- tenants should spread across multiple shards, got {}",
        env_shard_ids.len()
    );
}

/// Verifies that the same tenant always maps to the same shard (determinism).
#[silo::test]
fn hash_based_sharding_is_deterministic() {
    let map = ShardMap::create_initial(16).unwrap();

    for tenant in ["env-1234567890", "env-0000000001", "tenant-abc", "foo"] {
        let shard1 = map.shard_for_tenant(tenant).unwrap();
        let shard2 = map.shard_for_tenant(tenant).unwrap();
        assert_eq!(
            shard1.id, shard2.id,
            "tenant '{}' should always map to the same shard",
            tenant
        );
    }
}
