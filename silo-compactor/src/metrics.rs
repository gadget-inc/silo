//! Prometheus metrics for silo-compactor.
//!
//! Mirrors the pattern in `silo::metrics`: a single `Registry` is built up
//! with both compactor-specific instruments (worker restarts, shard
//! ownership) and the shared SlateDB per-shard instruments via
//! [`silo::metrics::SlatedbShardMetrics`]. The HTTP endpoint is served by
//! [`silo::metrics::serve_registry`].

use std::sync::Arc;

use prometheus::{CounterVec, Gauge, Opts, Registry};
use silo::metrics::SlatedbShardMetrics;

/// Compactor metrics handle. Cheaply cloneable.
#[derive(Clone)]
pub struct CompactorMetrics {
    registry: Arc<Registry>,
    shards_owned: Gauge,
    workers_running: Gauge,
    worker_restarts: CounterVec,
    compactions_started: CounterVec,
    compactor_run_errors: CounterVec,
    pub slatedb: SlatedbShardMetrics,
}

impl CompactorMetrics {
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn set_shards_owned(&self, count: usize) {
        self.shards_owned.set(count as f64);
    }

    pub fn set_workers_running(&self, count: usize) {
        self.workers_running.set(count as f64);
    }

    pub fn record_worker_restart(&self, shard: &str) {
        self.worker_restarts.with_label_values(&[shard]).inc();
    }

    /// Record that a slatedb compactor was successfully opened for `shard` and
    /// is about to start running.
    pub fn record_compactor_started(&self, shard: &str) {
        self.compactions_started.with_label_values(&[shard]).inc();
    }

    /// Record a compactor `run()` returning with an error.
    pub fn record_compactor_run_error(&self, shard: &str) {
        self.compactor_run_errors.with_label_values(&[shard]).inc();
    }
}

/// Build a fresh registry and register all compactor metrics.
pub fn init() -> anyhow::Result<CompactorMetrics> {
    let registry = Registry::new();

    let shards_owned = register(
        &registry,
        Gauge::new(
            "silo_compactor_shards_owned",
            "Number of shards currently assigned to this compactor pod",
        )?,
    )?;

    let workers_running = register(
        &registry,
        Gauge::new(
            "silo_compactor_workers_running",
            "Number of per-shard compactor workers currently spawned on this pod",
        )?,
    )?;

    let worker_restarts = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_compactor_worker_restarts_total",
                "Total per-shard compactor worker restarts (slatedb compactor errored and was retried)",
            ),
            &["shard"],
        )?,
    )?;

    let compactions_started = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_compactor_compactor_started_total",
                "Total times a slatedb compactor was successfully opened for a shard",
            ),
            &["shard"],
        )?,
    )?;

    let compactor_run_errors = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_compactor_run_errors_total",
                "Total times slatedb's compactor.run() returned an error",
            ),
            &["shard"],
        )?,
    )?;

    let slatedb = SlatedbShardMetrics::register(&registry, "silo_compactor_")?;

    Ok(CompactorMetrics {
        registry: Arc::new(registry),
        shards_owned,
        workers_running,
        worker_restarts,
        compactions_started,
        compactor_run_errors,
        slatedb,
    })
}

fn register<C>(registry: &Registry, metric: C) -> anyhow::Result<C>
where
    C: prometheus::core::Collector + Clone + 'static,
{
    registry.register(Box::new(metric.clone()))?;
    Ok(metric)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_registers_all_metrics() {
        let m = init().expect("init metrics");
        m.set_shards_owned(3);
        m.set_workers_running(3);
        m.record_worker_restart("shard-a");
        m.record_compactor_started("shard-a");

        // Sanity: gather and ensure our compactor-specific metrics are present
        // in the families. (slatedb_* counters/gauges only emit families once
        // a label combination is touched, which requires a real recorder.)
        let families = m.registry().gather();
        let names: Vec<_> = families.iter().map(|f| f.get_name().to_string()).collect();
        assert!(names.contains(&"silo_compactor_shards_owned".to_string()));
        assert!(names.contains(&"silo_compactor_worker_restarts_total".to_string()));
        assert!(names.contains(&"silo_compactor_compactor_started_total".to_string()));
    }
}
