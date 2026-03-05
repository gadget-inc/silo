use std::collections::HashSet;

use silo::shard_range::ShardMap;

/// Demonstrates that all tenants with the format "env-XXXXXXXXXX" (env- followed by a
/// 10-digit number, matching Gadget's environment ID tenant keys) land on the same shard
/// because silo uses lexicographic range-based sharding on the raw tenant string.
///
/// Since all these tenants share the "env-" prefix, they cluster in the same region of the
/// keyspace and will always be assigned to whichever shard's range covers that prefix.
#[silo::test]
fn all_env_tenants_with_10_digit_ids_land_on_same_shard() {
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

        assert_eq!(
            shard_ids.len(),
            1,
            "With {} shards, all env-XXXXXXXXXX tenants should land on the same shard, \
             but they were distributed across {} shards. \
             Shard ranges: {:?}",
            shard_count,
            shard_ids.len(),
            map.shards()
                .iter()
                .map(|s| format!("{}: {}", s.id, s.range))
                .collect::<Vec<_>>()
        );
    }
}

/// Shows that even with many more tenants, they all still land on the same shard
/// because the discriminating part (the numeric ID) comes after the shared "env-" prefix.
#[silo::test]
fn env_tenants_do_not_spread_across_shards_even_with_many_tenants() {
    let map = ShardMap::create_initial(16).unwrap();

    let mut shard_ids = HashSet::new();
    // Generate 1000 different env- tenant keys
    for i in 1_000_000_000u64..1_000_001_000u64 {
        let tenant = format!("env-{}", i);
        let shard = map.shard_for_tenant(&tenant).unwrap();
        shard_ids.insert(shard.id);
    }

    assert_eq!(
        shard_ids.len(),
        1,
        "All 1000 env- tenants should land on the same shard with 16 shards, \
         but they were distributed across {} shards",
        shard_ids.len()
    );
}

/// Confirms the root cause: the "env-" prefix falls entirely within one shard range
/// because range boundaries are hex characters (0-9, a-f) and "e" < "env-" < "f".
#[silo::test]
fn env_prefix_falls_in_single_range_because_boundaries_are_hex() {
    let map = ShardMap::create_initial(16).unwrap();

    // Find which shard owns "env-" prefixed tenants
    let shard = map.shard_for_tenant("env-1234567890").unwrap();

    // The shard range should contain the entire "env-" prefix space
    // since all env-XXXXXXXXXX strings are lexicographically between "e" and "f"
    let range = &shard.range;

    // Verify: the range start is <= "e" and the range end is >= "f" (or unbounded)
    // meaning all "env-*" strings fall within this single range
    let start_covers = range.start.is_empty() || range.start.as_str() <= "e";
    let end_covers = range.end.is_empty() || range.end.as_str() > "env-9999999999";

    assert!(
        start_covers && end_covers,
        "Expected a single shard range to cover all env- tenants. \
         Range: {}, start_covers: {}, end_covers: {}",
        range, start_covers, end_covers
    );
}

/// Contrasts env- tenants with tenants that have diverse prefixes to show that
/// diverse prefixes DO spread across shards, while env- does not.
#[silo::test]
fn diverse_tenant_prefixes_spread_across_shards_but_env_does_not() {
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

    // env- tenants all on one shard
    let env_tenants: Vec<String> = (0..16).map(|i| format!("env-{:010}", i)).collect();
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
    assert_eq!(
        env_shard_ids.len(),
        1,
        "All env- tenants should land on exactly 1 shard, got {}",
        env_shard_ids.len()
    );
}
