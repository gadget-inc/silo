//! Jemalloc allocator stats exposed through the Prometheus registry.
//!
//! Surfaces the five canonical jemalloc memory counters so we can tell whether
//! the gap between live data and process RSS is purgeable retained pages
//! (`retained`), genuinely live allocations (`allocated`), or non-jemalloc
//! mmaps (RSS far above `mapped`).
//!
//! Jemalloc caches stats per "epoch" — `epoch::advance()` must be called
//! before reading or the values are stale.

use std::time::Duration;

use prometheus::{Gauge, Registry, core::Collector};
use tikv_jemalloc_ctl::{epoch, stats};
use tokio::sync::broadcast;
use tracing::debug;

#[derive(Clone)]
pub struct JemallocMetrics {
    allocated: Gauge,
    active: Gauge,
    resident: Gauge,
    retained: Gauge,
    mapped: Gauge,
}

impl JemallocMetrics {
    pub fn register(registry: &Registry) -> anyhow::Result<Self> {
        let allocated = register(
            registry,
            Gauge::new(
                "silo_jemalloc_allocated_bytes",
                "Bytes allocated by the application (stats.allocated).",
            )?,
        );
        let active = register(
            registry,
            Gauge::new(
                "silo_jemalloc_active_bytes",
                "Bytes in active pages allocated by the application (stats.active).",
            )?,
        );
        let resident = register(
            registry,
            Gauge::new(
                "silo_jemalloc_resident_bytes",
                "Bytes in physically resident data pages (stats.resident).",
            )?,
        );
        let retained = register(
            registry,
            Gauge::new(
                "silo_jemalloc_retained_bytes",
                "Bytes retained by jemalloc but not mapped (stats.retained, purgeable).",
            )?,
        );
        let mapped = register(
            registry,
            Gauge::new(
                "silo_jemalloc_mapped_bytes",
                "Bytes in mapped chunks (stats.mapped).",
            )?,
        );
        Ok(Self {
            allocated,
            active,
            resident,
            retained,
            mapped,
        })
    }

    pub fn scrape(&self) {
        if let Err(e) = epoch::advance() {
            debug!(error = %e, "jemalloc epoch advance failed");
            return;
        }
        if let Ok(v) = stats::allocated::read() {
            self.allocated.set(v as f64);
        }
        if let Ok(v) = stats::active::read() {
            self.active.set(v as f64);
        }
        if let Ok(v) = stats::resident::read() {
            self.resident.set(v as f64);
        }
        if let Ok(v) = stats::retained::read() {
            self.retained.set(v as f64);
        }
        if let Ok(v) = stats::mapped::read() {
            self.mapped.set(v as f64);
        }
    }
}

fn register<C: Collector + Clone + 'static>(registry: &Registry, metric: C) -> C {
    if let Err(e) = registry.register(Box::new(metric.clone())) {
        tracing::warn!(error = %e, "failed to register jemalloc metric");
    }
    metric
}

pub async fn run_scraper(
    metrics: JemallocMetrics,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    debug!(
        interval_ms = interval.as_millis() as u64,
        "jemalloc metrics scraper started"
    );

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                debug!("jemalloc metrics scraper shutting down");
                break;
            }
            _ = ticker.tick() => {
                metrics.scrape();
            }
        }
    }
}
