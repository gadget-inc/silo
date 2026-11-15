//! Placement metrics sampled from the coordinator.
//!
//! The coordinator's owned set changes at shard acquisition, release and split
//! time across several backends, and the desired assignment changes with
//! membership. Rather than hooking every mutation site, a periodic driver
//! reads the owned shard list and the shard owner map from the `Coordinator`
//! trait object and feeds them to a single-pass sampler that sets the gauges
//! and logs strand transitions, so the gauges are scrape-time values that lag
//! reality by at most one interval.
//!
//! The owned set is inserted into after a shard is acquired and opened; the
//! one exception is a split, which pre-adds both children before opening them
//! and removes the ones this node does not keep, so `silo_shards_owned` can
//! transiently include a not-yet-open child while a split is committing.

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub use crate::coordination::UnassignedShards;
use crate::coordination::{Coordinator, ShardOwnerMap};
use crate::metrics::{Metrics, PLACEMENT_SAMPLE_INTERVAL};
use crate::shard_range::ShardId;

/// The shards that changed unassigned state between two sampler passes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UnassignedTransitions {
    /// Shards unassigned now that were assigned on the previous pass.
    pub newly_unassigned: Vec<(ShardId, String)>,
    /// Shards assigned now that were unassigned on the previous pass, with the
    /// ring they were stranded on.
    pub resolved: Vec<(ShardId, String)>,
}

/// Diff two passes' unassigned sets so transitions can be logged exactly once.
pub fn unassigned_transitions(
    previous: &UnassignedShards,
    current: &UnassignedShards,
) -> UnassignedTransitions {
    let only_in = |from: &UnassignedShards, other: &UnassignedShards| {
        from.iter()
            .filter(|(id, _)| !other.contains_key(id))
            .map(|(id, ring)| (*id, ring.clone()))
            .collect()
    };
    UnassignedTransitions {
        newly_unassigned: only_in(current, previous),
        resolved: only_in(previous, current),
    }
}

/// One sampler pass.
///
/// Always sets `silo_shards_owned` from this node's owned shard list. When the
/// owner map is available, also sets `silo_shards_unassigned{ring}` from it,
/// clears ring labels that no longer have unassigned shards, logs a `warn` for
/// each shard that became unassigned since `previous` and an `info` for each
/// that regained an owner, and returns the current unassigned set for the next
/// pass. Without an owner map (the driver could not read it) the unassigned
/// side is left untouched and `previous` is returned unchanged, so the owned
/// gauge never goes stale because of an owner-map failure.
pub fn sample(
    metrics: &Metrics,
    owned: &[ShardId],
    owner_map: Option<&ShardOwnerMap>,
    previous: &UnassignedShards,
) -> UnassignedShards {
    metrics.set_shards_owned(owned.len() as u64);

    let Some(owner_map) = owner_map else {
        return previous.clone();
    };
    let current = owner_map.unassigned_shards();

    let mut per_ring: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
    for ring in current.values() {
        *per_ring.entry(ring).or_default() += 1;
    }
    for (ring, count) in &per_ring {
        metrics.set_shards_unassigned(ring, *count);
    }
    let previous_rings: BTreeSet<&str> = previous.values().map(String::as_str).collect();
    for ring in previous_rings {
        if !per_ring.contains_key(ring) {
            metrics.clear_shards_unassigned(ring);
        }
    }

    let transitions = unassigned_transitions(previous, &current);
    for (shard_id, ring) in &transitions.newly_unassigned {
        warn!(
            shard_id = %shard_id,
            ring = %ring,
            "shard has no eligible owner: no member is in its placement ring"
        );
    }
    for (shard_id, ring) in &transitions.resolved {
        info!(
            shard_id = %shard_id,
            ring = %ring,
            "shard is assigned again: its placement ring has a member"
        );
    }

    current
}

/// Drive [`sample`] from the coordinator every [`PLACEMENT_SAMPLE_INTERVAL`]
/// until the shutdown broadcast fires. Spawned from `main` when metrics are
/// enabled. The owner-map read is raced against shutdown so a hung
/// coordination backend cannot delay process exit.
pub async fn run_sampler(
    coordinator: Arc<dyn Coordinator>,
    metrics: Metrics,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(PLACEMENT_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = UnassignedShards::new();

    debug!(
        interval_ms = PLACEMENT_SAMPLE_INTERVAL.as_millis() as u64,
        "placement metrics sampler started"
    );

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => break,
            _ = ticker.tick() => {}
        }

        let owner_map = tokio::select! {
            biased;
            _ = shutdown.recv() => break,
            result = coordinator.get_shard_owner_map() => match result {
                Ok(map) => Some(map),
                Err(e) => {
                    warn!(error = %e, "placement metrics sampler: could not read the shard owner map; unassigned gauge not updated this pass");
                    None
                }
            },
        };
        let owned = coordinator.owned_shards().await;
        previous = sample(&metrics, &owned, owner_map.as_ref(), &previous);
    }

    debug!("placement metrics sampler shutting down");
}
