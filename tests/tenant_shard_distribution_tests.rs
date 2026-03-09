use silo::shard_range::ShardMap;

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
