//! Tests for the placement metrics sampler: the pass that turns the
//! coordinator's owned shard list and owner map into `silo_shards_owned` and
//! `silo_shards_unassigned{ring}`, plus the pure transition diff that decides
//! when a strand is logged.

use std::collections::{BTreeMap, HashMap};

use silo::coordination::ShardOwnerMap;
use silo::metrics::{self, Metrics};
use silo::placement_metrics::{self, UnassignedShards, UnassignedTransitions};
use silo::shard_range::{ShardId, ShardMap};

/// Read a gauge back out of the metrics registry, matching `labels` exactly
/// (an empty slice selects an unlabelled gauge). `None` when no sample with
/// those labels is exported -- a labelled family with no live series is
/// omitted from `gather()` entirely.
fn gauge_value(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let family = metrics
        .registry()
        .gather()
        .into_iter()
        .find(|f| f.get_name() == name)?;
    family
        .get_metric()
        .iter()
        .find(|m| {
            let got: Vec<(&str, &str)> = m
                .get_label()
                .iter()
                .map(|l| (l.get_name(), l.get_value()))
                .collect();
            got == labels
        })
        .map(|m| m.get_gauge().get_value())
}

/// Build an owner map over `rings.len()` shards, pinning shard `i` to
/// `rings[i]` (`None` = default ring) and assigning an owner to every shard
/// except those listed in `unowned`.
fn owner_map_with_rings(rings: &[Option<&str>], unowned: &[usize]) -> ShardOwnerMap {
    let mut shard_map = ShardMap::create_initial(rings.len() as u32).expect("create shard map");
    let shard_ids = shard_map.shard_ids();
    for (i, ring) in rings.iter().enumerate() {
        shard_map
            .get_shard_mut(&shard_ids[i])
            .expect("shard exists")
            .set_placement_ring(ring.map(str::to_string));
    }

    let mut shard_to_addr = HashMap::new();
    let mut shard_to_node = HashMap::new();
    for (i, id) in shard_ids.iter().enumerate() {
        if unowned.contains(&i) {
            continue;
        }
        shard_to_addr.insert(*id, format!("http://node-{}", id));
        shard_to_node.insert(*id, format!("node-{}", id));
    }

    ShardOwnerMap {
        shard_map,
        shard_to_addr,
        shard_to_node,
    }
}

#[silo::test]
fn sample_sets_shards_owned_to_owned_count() {
    let metrics = metrics::init().expect("init metrics");
    let owned = vec![ShardId::new(), ShardId::new(), ShardId::new()];
    let owner_map = owner_map_with_rings(&[None], &[]);

    placement_metrics::sample(&metrics, &owned, Some(&owner_map), &UnassignedShards::new());

    assert_eq!(
        gauge_value(&metrics, "silo_shards_owned", &[]),
        Some(3.0),
        "silo_shards_owned after one pass over {} owned shards",
        owned.len()
    );
}

#[silo::test]
fn later_pass_replaces_shards_owned() {
    let metrics = metrics::init().expect("init metrics");
    let owner_map = owner_map_with_rings(&[None], &[]);
    let none = UnassignedShards::new();

    placement_metrics::sample(
        &metrics,
        &[ShardId::new(), ShardId::new(), ShardId::new()],
        Some(&owner_map),
        &none,
    );
    placement_metrics::sample(&metrics, &[ShardId::new()], Some(&owner_map), &none);

    assert_eq!(
        gauge_value(&metrics, "silo_shards_owned", &[]),
        Some(1.0),
        "silo_shards_owned after a second pass over one owned shard"
    );
}

#[silo::test]
fn sample_reports_unassigned_shard_under_its_ring_and_clears_when_assigned() {
    let metrics = metrics::init().expect("init metrics");
    let stranded = owner_map_with_rings(&[Some("heavy"), None], &[0]);
    let recovered = owner_map_with_rings(&[Some("heavy"), None], &[]);

    let unassigned =
        placement_metrics::sample(&metrics, &[], Some(&stranded), &UnassignedShards::new());

    assert_eq!(
        gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "heavy")]),
        Some(1.0),
        "silo_shards_unassigned{{ring=\"heavy\"}} while the heavy-ring shard has no owner"
    );
    assert_eq!(
        unassigned,
        BTreeMap::from([(stranded.shard_ids()[0], "heavy".to_string())]),
        "the pass returns the unassigned set for the next pass to diff against"
    );

    let unassigned = placement_metrics::sample(&metrics, &[], Some(&recovered), &unassigned);

    let heavy = gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "heavy")]);
    assert!(
        heavy.is_none_or(|v| v == 0.0),
        "silo_shards_unassigned{{ring=\"heavy\"}} after the shard regains an owner: expected absent or 0, got {heavy:?}"
    );
    assert!(unassigned.is_empty(), "no shard is unassigned any more");
}

#[silo::test]
fn sample_clears_only_the_ring_that_recovered() {
    let metrics = metrics::init().expect("init metrics");
    let both_stranded = owner_map_with_rings(&[Some("heavy"), None], &[0, 1]);
    let heavy_recovered = owner_map_with_rings(&[Some("heavy"), None], &[1]);

    let unassigned = placement_metrics::sample(
        &metrics,
        &[],
        Some(&both_stranded),
        &UnassignedShards::new(),
    );
    assert_eq!(
        gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "heavy")]),
        Some(1.0)
    );
    assert_eq!(
        gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "default")]),
        Some(1.0)
    );

    placement_metrics::sample(&metrics, &[], Some(&heavy_recovered), &unassigned);

    let heavy = gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "heavy")]);
    assert!(
        heavy.is_none_or(|v| v == 0.0),
        "heavy ring clears once its shard has an owner, got {heavy:?}"
    );
    assert_eq!(
        gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "default")]),
        Some(1.0),
        "the default ring's series is retained while its shard is still unassigned"
    );
}

#[silo::test]
fn sample_without_owner_map_still_updates_shards_owned() {
    let metrics = metrics::init().expect("init metrics");
    let stranded = owner_map_with_rings(&[Some("heavy")], &[0]);
    let previous =
        placement_metrics::sample(&metrics, &[], Some(&stranded), &UnassignedShards::new());

    let returned = placement_metrics::sample(&metrics, &[ShardId::new()], None, &previous);

    assert_eq!(
        gauge_value(&metrics, "silo_shards_owned", &[]),
        Some(1.0),
        "the owned gauge is set even when the owner map could not be read"
    );
    assert_eq!(
        gauge_value(&metrics, "silo_shards_unassigned", &[("ring", "heavy")]),
        Some(1.0),
        "the unassigned gauge is left as it was"
    );
    assert_eq!(
        returned, previous,
        "the previous unassigned set carries over to the next pass"
    );
}

#[silo::test]
fn unassigned_transitions_fire_only_on_changes() {
    let a = ShardId::new();
    let b = ShardId::new();
    let set = |entries: &[(ShardId, &str)]| -> BTreeMap<ShardId, String> {
        entries
            .iter()
            .map(|(id, ring)| (*id, ring.to_string()))
            .collect()
    };

    struct Case {
        name: &'static str,
        previous: BTreeMap<ShardId, String>,
        current: BTreeMap<ShardId, String>,
        want: UnassignedTransitions,
    }
    let cases = [
        Case {
            name: "newly unassigned",
            previous: set(&[]),
            current: set(&[(a, "heavy")]),
            want: UnassignedTransitions {
                newly_unassigned: vec![(a, "heavy".to_string())],
                resolved: vec![],
            },
        },
        Case {
            name: "newly resolved",
            previous: set(&[(a, "heavy")]),
            current: set(&[]),
            want: UnassignedTransitions {
                newly_unassigned: vec![],
                resolved: vec![(a, "heavy".to_string())],
            },
        },
        Case {
            name: "unchanged",
            previous: set(&[(a, "heavy"), (b, "default")]),
            current: set(&[(a, "heavy"), (b, "default")]),
            want: UnassignedTransitions::default(),
        },
        Case {
            name: "one resolved while another appears",
            previous: set(&[(a, "heavy")]),
            current: set(&[(b, "default")]),
            want: UnassignedTransitions {
                newly_unassigned: vec![(b, "default".to_string())],
                resolved: vec![(a, "heavy".to_string())],
            },
        },
    ];

    for case in cases {
        let got = placement_metrics::unassigned_transitions(&case.previous, &case.current);
        assert_eq!(got, case.want, "case `{}`", case.name);
    }
}
