use silo::shard_range::{ShardId, ShardInfo, ShardMap, ShardRange, hash_tenant};

/// Verifies that the same tenant always maps to the same shard (determinism)
/// and that every tenant maps to some shard (full coverage).
///
/// Distribution quality is guaranteed by XXH64 (via the xxhash-rust crate)
/// and doesn't need to be tested here.
#[silo::test]
fn routing_is_deterministic_and_complete() {
    let map = ShardMap::create_initial(16).unwrap();

    for tenant in ["env-1234567890", "env-0000000001", "tenant-abc", "foo", ""] {
        let shard1 = map.shard_for_tenant(tenant).unwrap();
        let shard2 = map.shard_for_tenant(tenant).unwrap();
        assert_eq!(
            shard1.id, shard2.id,
            "tenant '{}' should always map to the same shard",
            tenant
        );
    }
}

/// A single-tenant shard can be created by using a range that contains exactly
/// one hash value: [hash, hash+1). This is useful for isolating a large tenant
/// onto its own shard.
#[silo::test]
fn single_tenant_shard_isolates_one_tenant() {
    let big_tenant = "env-42940";
    let hash = hash_tenant(big_tenant);
    let hash_val = u64::from_str_radix(&hash, 16).unwrap();

    // Build a 3-shard map: [0, hash), [hash, hash+1), [hash+1, MAX]
    // The middle shard contains exactly one hash value — the big tenant's.
    let shard_before = ShardInfo::new(ShardId::new(), ShardRange::new("", &hash));
    let shard_isolated = ShardInfo::new(
        ShardId::new(),
        ShardRange::new(&hash, format!("{:016x}", hash_val + 1)),
    );
    let shard_after = ShardInfo::new(
        ShardId::new(),
        ShardRange::new(format!("{:016x}", hash_val + 1), ""),
    );

    let isolated_id = shard_isolated.id.clone();
    let map = ShardMap::from_shards(vec![shard_before, shard_isolated, shard_after]);

    // The big tenant must route to the isolated shard
    let routed = map.shard_for_tenant(big_tenant).unwrap();
    assert_eq!(
        routed.id, isolated_id,
        "big tenant should land on isolated shard"
    );

    // Other tenants must NOT route to the isolated shard
    for other in [
        "env-other-1",
        "env-other-2",
        "tenant-small",
        "foo",
        "bar",
        "zzz",
    ] {
        let routed = map.shard_for_tenant(other).unwrap();
        assert_ne!(
            routed.id, isolated_id,
            "tenant '{}' should not land on the isolated shard",
            other
        );
    }

    // The isolated shard's range cannot be split further (too narrow)
    assert!(
        map.get_shard(&isolated_id)
            .unwrap()
            .range
            .midpoint()
            .is_none(),
        "single-hash range should not be splittable"
    );
}
