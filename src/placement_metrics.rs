//! Placement metrics sampled from the coordinator.
//!
//! The coordinator's owned set changes at shard acquisition, release and split
//! time across several backends. Rather than hooking every mutation site, a
//! periodic driver reads the owned shard list from the `Coordinator` trait
//! object and feeds it to a single-pass sampler that sets the gauges, so the
//! gauges are scrape-time values that lag reality by at most one interval.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::debug;

use crate::coordination::Coordinator;
use crate::metrics::Metrics;
use crate::shard_range::ShardId;

/// How often the driver samples the coordinator. Bounds the staleness of the
/// placement gauges; the HELP text of each gauge quotes this value.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// One sampler pass: set `silo_shards_owned` from this node's owned shard list.
pub fn sample(metrics: &Metrics, owned: &[ShardId]) {
    metrics.set_shards_owned(owned.len() as u64);
}

/// Drive [`sample`] from the coordinator every [`SAMPLE_INTERVAL`] until the
/// shutdown broadcast fires. Spawned from `main` when metrics are enabled.
pub async fn run_sampler(
    coordinator: Arc<dyn Coordinator>,
    metrics: Metrics,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    debug!(
        interval_ms = SAMPLE_INTERVAL.as_millis() as u64,
        "placement metrics sampler started"
    );

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                debug!("placement metrics sampler shutting down");
                break;
            }
            _ = ticker.tick() => {
                let owned = coordinator.owned_shards().await;
                sample(&metrics, &owned);
            }
        }
    }
}
