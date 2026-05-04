//! Prometheus metrics for silo-compactor.
//!
//! Mirrors the pattern in `silo::metrics`: a single `Registry` is built up
//! with both compactor-specific instruments (worker restarts, shard
//! ownership, compaction-filter row counts) and the shared SlateDB per-shard
//! instruments via [`silo::metrics::SlatedbShardMetrics`]. The HTTP endpoint
//! is served by [`silo::metrics::serve_registry`].

use std::sync::Arc;

use prometheus::{CounterVec, Gauge, HistogramOpts, HistogramVec, Opts, Registry};
use silo::metrics::SlatedbShardMetrics;

/// Histogram buckets (seconds) for compaction-filter pre-scans, which iterate
/// IDX_STATUS_TIME in memory. Typical ranges are ms to a few seconds.
const PRE_SCAN_BUCKETS_SECS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Compactor metrics handle. Cheaply cloneable.
#[derive(Clone)]
pub struct CompactorMetrics {
    registry: Arc<Registry>,
    shards_owned: Gauge,
    workers_running: Gauge,
    worker_restarts: CounterVec,
    compactions_started: CounterVec,
    compactor_run_errors: CounterVec,
    compaction_filter_rows_dropped: CounterVec,
    compaction_filter_pre_scan_duration: HistogramVec,
    compaction_filter_pre_scan_entries: CounterVec,
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

    pub fn record_filter_rows_dropped(&self, shard: &str, prefix: &str, n: u64) {
        if n == 0 {
            return;
        }
        self.compaction_filter_rows_dropped
            .with_label_values(&[shard, prefix])
            .inc_by(n as f64);
    }

    pub fn record_filter_pre_scan(&self, shard: &str, duration_secs: f64, entries: u64) {
        self.compaction_filter_pre_scan_duration
            .with_label_values(&[shard])
            .observe(duration_secs);
        self.compaction_filter_pre_scan_entries
            .with_label_values(&[shard])
            .inc_by(entries as f64);
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

    let compaction_filter_rows_dropped = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_compactor_compaction_filter_rows_dropped_total",
                "Rows dropped by the completed-jobs compaction filter, by key prefix",
            ),
            &["shard", "prefix"],
        )?,
    )?;

    let compaction_filter_pre_scan_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_compactor_compaction_filter_pre_scan_duration_seconds",
                "Wall-clock duration of the IDX_STATUS_TIME pre-scan that builds the expired (tenant, job_id) set",
            )
            .buckets(PRE_SCAN_BUCKETS_SECS.to_vec()),
            &["shard"],
        )?,
    )?;

    let compaction_filter_pre_scan_entries = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_compactor_compaction_filter_pre_scan_entries_total",
                "Cumulative number of entries inserted into the expired-set during IDX_STATUS_TIME pre-scans",
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
        compaction_filter_rows_dropped,
        compaction_filter_pre_scan_duration,
        compaction_filter_pre_scan_entries,
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
        m.record_filter_rows_dropped("shard-a", "job_info", 7);
        m.record_filter_pre_scan("shard-a", 0.123, 42);

        // Sanity: gather and ensure our compactor-specific metrics are present
        // in the families. (slatedb_* counters/gauges only emit families once
        // a label combination is touched, which requires a real recorder.)
        let families = m.registry().gather();
        let names: Vec<_> = families.iter().map(|f| f.get_name().to_string()).collect();
        assert!(names.contains(&"silo_compactor_shards_owned".to_string()));
        assert!(names.contains(&"silo_compactor_worker_restarts_total".to_string()));
        assert!(names.contains(&"silo_compactor_compaction_filter_rows_dropped_total".to_string()));
    }
}
