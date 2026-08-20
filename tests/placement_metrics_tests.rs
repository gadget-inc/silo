//! Tests for the placement metrics sampler: the pass that turns the
//! coordinator's owned shard list into `silo_shards_owned`.

use silo::metrics::{self, Metrics};
use silo::placement_metrics;
use silo::shard_range::ShardId;

/// Read a plain (unlabelled) gauge back out of the metrics registry.
fn gauge_value(metrics: &Metrics, name: &str) -> f64 {
    let family = metrics
        .registry()
        .gather()
        .into_iter()
        .find(|f| f.get_name() == name)
        .unwrap_or_else(|| panic!("metric {name} not registered"));
    let metric = family
        .get_metric()
        .first()
        .unwrap_or_else(|| panic!("metric {name} has no samples"));
    metric.get_gauge().get_value()
}

#[silo::test]
fn sample_sets_shards_owned_to_owned_count() {
    let metrics = metrics::init().expect("init metrics");
    let owned = vec![ShardId::new(), ShardId::new(), ShardId::new()];

    placement_metrics::sample(&metrics, &owned);

    assert_eq!(
        gauge_value(&metrics, "silo_shards_owned"),
        3.0,
        "silo_shards_owned after one pass over {} owned shards",
        owned.len()
    );
}

#[silo::test]
fn later_pass_replaces_shards_owned() {
    let metrics = metrics::init().expect("init metrics");

    placement_metrics::sample(&metrics, &[ShardId::new(), ShardId::new(), ShardId::new()]);
    placement_metrics::sample(&metrics, &[ShardId::new()]);

    assert_eq!(
        gauge_value(&metrics, "silo_shards_owned"),
        1.0,
        "silo_shards_owned after a second pass over one owned shard"
    );
}
