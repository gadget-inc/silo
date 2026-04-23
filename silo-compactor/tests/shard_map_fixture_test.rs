//! Drift-detection test for the silo ShardMap JSON format.
//!
//! The fixture mirrors what silo writes to the etcd `/coord/shard_map` key and
//! the `<cluster_prefix>-shard-map` k8s ConfigMap. If silo's serialization
//! changes in a way that breaks compactor deserialization, this test fires.
//! Regenerate the fixture by serializing `silo::shard_range::ShardMap::create_initial(3)`.

use silo_compactor::shard_map::ShardMap;

#[test]
fn deserializes_silo_shard_map_fixture() {
    let json = include_str!("fixtures/shard_map.json");
    let map: ShardMap = serde_json::from_str(json).expect("valid shard map JSON");
    assert_eq!(map.version, 1);
    assert_eq!(map.shards.len(), 3);

    let unfiltered = map.shard_ids(None);
    assert_eq!(unfiltered.len(), 3);

    let default_ring_only = map.shard_ids(Some("default"));
    assert_eq!(default_ring_only.len(), 1);
    assert_eq!(
        default_ring_only[0].to_string(),
        "22222222-2222-4222-8222-222222222222"
    );
}

#[test]
fn ignores_unknown_fields_in_shard_info() {
    let json = r#"{
        "shards": [
            {"id": "00000000-0000-4000-8000-000000000001",
             "range": {"start": "", "end": ""},
             "created_at_ms": 0,
             "parent_shard_id": null,
             "placement_ring": null,
             "future_field": {"some": "value"}}
        ],
        "version": 5
    }"#;
    let map: ShardMap = serde_json::from_str(json).expect("ignore unknown fields");
    assert_eq!(map.version, 5);
    assert_eq!(map.shards.len(), 1);
}
