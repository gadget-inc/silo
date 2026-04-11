//! Prometheus metrics for Silo.
//!
//! This module provides:
//! - Prometheus metrics using the `prometheus` crate
//! - Pre-defined metric instruments for key operations
//! - An HTTP server for the `/metrics` endpoint
//!
//! # Usage
//!
//! Initialize metrics once at startup:
//! ```ignore
//! let metrics = silo::metrics::init()?;
//! ```
//!
//! Then start the metrics server:
//! ```ignore
//! silo::metrics::run_metrics_server(addr, metrics.clone(), shutdown_rx).await;
//! ```
//!
//! Record metrics in your code:
//! ```ignore
//! metrics.jobs_enqueued.with_label_values(&["0", "default"]).inc();
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder, core::Collector,
};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::sync::broadcast;
use tower::{Layer, Service};
use tracing::{debug, error};

/// Default histogram buckets for request latencies (in seconds)
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Histogram buckets for background scan operations (broker scan, lease reaper) in seconds.
/// Concentrated at low values since these typically complete in milliseconds.
const SCAN_DURATION_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Histogram buckets for job wait times (in seconds) - wider range for queue latency
const WAIT_TIME_BUCKETS: &[f64] = &[
    0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0,
];

/// Histogram buckets for ready-to-start latency (in milliseconds)
const READY_TO_START_LATENCY_MS_BUCKETS: &[f64] = &[
    1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 30000.0, 60000.0, 300000.0, 600000.0,
    1800000.0, 3600000.0,
];

/// Silo metrics handle containing all metric instruments.
#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,

    // Job metrics
    jobs_enqueued: CounterVec,
    jobs_dequeued: CounterVec,
    jobs_completed: CounterVec,
    job_attempts: CounterVec,
    job_wait_time: HistogramVec,

    // gRPC metrics
    grpc_requests: CounterVec,
    grpc_request_duration: HistogramVec,

    // Shard/broker metrics
    shards_owned: Gauge,
    coordination_shards_open: Gauge,
    broker_buffer_size: GaugeVec,
    broker_inflight_size: GaugeVec,
    broker_scan_duration: HistogramVec,
    broker_scans_total: CounterVec,

    // Poll metrics
    polls_total: CounterVec,
    poll_duration: HistogramVec,

    // Lease metrics
    task_leases_active: GaugeVec,
    ready_to_start_latency_ms: HistogramVec,
    lease_reaper_duration: HistogramVec,
    lease_reaper_scans_total: CounterVec,

    // Concurrency metrics
    concurrency_tickets_granted: Counter,

    // SlateDB storage counters (per-shard, monotonically increasing in SlateDB)
    slatedb_get_requests: CounterVec,
    slatedb_scan_requests: CounterVec,
    slatedb_write_ops: CounterVec,
    slatedb_write_batch_count: CounterVec,
    slatedb_backpressure_count: CounterVec,
    slatedb_wal_buffer_flushes: CounterVec,
    slatedb_immutable_memtable_flushes: CounterVec,
    slatedb_sst_filter_positives: CounterVec,
    slatedb_sst_filter_negatives: CounterVec,
    slatedb_sst_filter_false_positives: CounterVec,
    slatedb_bytes_compacted: CounterVec,
    slatedb_flush_requests: CounterVec,

    // SlateDB storage gauges (per-shard, point-in-time values)
    slatedb_wal_buffer_estimated_bytes: GaugeVec,
    slatedb_running_compactions: GaugeVec,
    slatedb_last_compaction_ts_sec: GaugeVec,
    slatedb_l0_sst_count: GaugeVec,
    slatedb_total_mem_size_bytes: GaugeVec,

    // SlateDB cache counters (per-shard, monotonically increasing)
    slatedb_cache_data_block_hit: CounterVec,
    slatedb_cache_data_block_miss: CounterVec,
    slatedb_cache_index_hit: CounterVec,
    slatedb_cache_index_miss: CounterVec,
    slatedb_cache_filter_hit: CounterVec,
    slatedb_cache_filter_miss: CounterVec,

    /// Tracks previous SlateDB counter values per (stat_name, shard) for delta computation.
    /// SlateDB exposes counters as absolute values via `stat.get()`, but Prometheus counters
    /// only support `inc_by(delta)`, so we compute deltas between polls.
    slatedb_prev_values: Arc<Mutex<HashMap<(String, String), f64>>>,
}

impl Metrics {
    /// Get the prometheus registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Record a job enqueue event.
    pub fn record_enqueue(&self, shard: &str, tenant: &str) {
        self.jobs_enqueued.with_label_values(&[shard, tenant]).inc();
    }

    /// Record a job dequeue event.
    pub fn record_dequeue(&self, shard: &str, task_group: &str, count: u64) {
        self.jobs_dequeued
            .with_label_values(&[shard, task_group])
            .inc_by(count as f64);
    }

    /// Record a job completion event.
    pub fn record_completion(&self, shard: &str, status: &str) {
        self.jobs_completed
            .with_label_values(&[shard, status])
            .inc();
    }

    /// Record a job attempt starting execution.
    /// `is_retry` should be "true" if attempt_number > 1, "false" otherwise.
    pub fn record_attempt(&self, shard: &str, task_group: &str, is_retry: bool) {
        let is_retry_str = if is_retry { "true" } else { "false" };
        self.job_attempts
            .with_label_values(&[shard, task_group, is_retry_str])
            .inc();
    }

    /// Record job wait time (time from enqueue to dequeue) in seconds.
    pub fn record_job_wait_time(&self, shard: &str, task_group: &str, wait_time_secs: f64) {
        self.job_wait_time
            .with_label_values(&[shard, task_group])
            .observe(wait_time_secs);
    }

    /// Record a gRPC request.
    pub fn record_grpc_request(&self, method: &str, status_code: &str) {
        self.grpc_requests
            .with_label_values(&[method, status_code])
            .inc();
    }

    /// Record gRPC request duration in seconds.
    pub fn record_grpc_duration(&self, method: &str, duration_secs: f64) {
        self.grpc_request_duration
            .with_label_values(&[method])
            .observe(duration_secs);
    }

    /// Update the number of shards owned by this node (from coordinator).
    pub fn set_shards_owned(&self, count: u64) {
        self.shards_owned.set(count as f64);
    }

    /// Update the number of shards currently open in this process.
    pub fn set_coordination_shards_open(&self, count: u64) {
        self.coordination_shards_open.set(count as f64);
    }

    /// Update broker buffer size for a shard and task group.
    pub fn set_broker_buffer_size(&self, shard: &str, task_group: &str, size: u64) {
        self.broker_buffer_size
            .with_label_values(&[shard, task_group])
            .set(size as f64);
    }

    /// Update broker inflight size for a shard and task group.
    pub fn set_broker_inflight_size(&self, shard: &str, task_group: &str, size: u64) {
        self.broker_inflight_size
            .with_label_values(&[shard, task_group])
            .set(size as f64);
    }

    /// Record broker scan duration in seconds.
    pub fn record_broker_scan_duration(&self, shard: &str, duration_secs: f64) {
        self.broker_scan_duration
            .with_label_values(&[shard])
            .observe(duration_secs);
        self.broker_scans_total.with_label_values(&[shard]).inc();
    }

    /// Record a poll (lease_tasks call) for a shard.
    pub fn record_poll(&self, shard: &str) {
        self.polls_total.with_label_values(&[shard]).inc();
    }

    /// Record poll duration (time to service a lease_tasks call for a shard) in seconds.
    pub fn record_poll_duration(&self, shard: &str, duration_secs: f64) {
        self.poll_duration
            .with_label_values(&[shard])
            .observe(duration_secs);
    }

    /// Increment active task leases for a shard/task_group.
    pub fn inc_task_leases_active(&self, shard: &str, task_group: &str) {
        self.task_leases_active
            .with_label_values(&[shard, task_group])
            .inc();
    }

    /// Decrement active task leases for a shard/task_group.
    pub fn dec_task_leases_active(&self, shard: &str, task_group: &str) {
        self.task_leases_active
            .with_label_values(&[shard, task_group])
            .dec();
    }

    /// Record the latency between when a task became ready and when it was first leased, in milliseconds.
    pub fn record_ready_to_start_latency_ms(&self, shard: &str, task_group: &str, latency_ms: f64) {
        self.ready_to_start_latency_ms
            .with_label_values(&[shard, task_group])
            .observe(latency_ms);
    }

    /// Record lease reaper duration in seconds.
    pub fn record_lease_reaper_duration(&self, shard: &str, duration_secs: f64) {
        self.lease_reaper_duration
            .with_label_values(&[shard])
            .observe(duration_secs);
        self.lease_reaper_scans_total
            .with_label_values(&[shard])
            .inc();
    }

    /// Record a concurrency ticket being granted.
    pub fn record_concurrency_ticket_granted(&self) {
        self.concurrency_tickets_granted.inc();
    }

    /// Update SlateDB storage metrics from a shard's StatRegistry.
    ///
    /// Call this periodically (e.g., every second) to sync SlateDB's internal
    /// statistics to Prometheus metrics. Counter-type stats are tracked via deltas
    /// since Prometheus counters only support `inc_by()`, not `set()`.
    pub fn update_slatedb_stats(&self, shard: &str, recorder: &DefaultMetricsRecorder) {
        // Counter-type stats: monotonically increasing in SlateDB
        // REQUEST_COUNT uses an "op" label to distinguish get/scan/flush
        let labeled_counter_mappings: &[(&str, &[(&str, &str)], &CounterVec)] = &[
            (
                slatedb::db_stats::REQUEST_COUNT,
                &[("op", "get")],
                &self.slatedb_get_requests,
            ),
            (
                slatedb::db_stats::REQUEST_COUNT,
                &[("op", "scan")],
                &self.slatedb_scan_requests,
            ),
            (
                slatedb::db_stats::REQUEST_COUNT,
                &[("op", "flush")],
                &self.slatedb_flush_requests,
            ),
        ];
        let counter_mappings: &[(&str, &CounterVec)] = &[
            (slatedb::db_stats::WRITE_OPS, &self.slatedb_write_ops),
            (
                slatedb::db_stats::WRITE_BATCH_COUNT,
                &self.slatedb_write_batch_count,
            ),
            (
                slatedb::db_stats::BACKPRESSURE_COUNT,
                &self.slatedb_backpressure_count,
            ),
            (
                slatedb::db_stats::WAL_BUFFER_FLUSHES,
                &self.slatedb_wal_buffer_flushes,
            ),
            (
                slatedb::db_stats::IMMUTABLE_MEMTABLE_FLUSHES,
                &self.slatedb_immutable_memtable_flushes,
            ),
            (
                slatedb::db_stats::SST_FILTER_POSITIVE_COUNT,
                &self.slatedb_sst_filter_positives,
            ),
            (
                slatedb::db_stats::SST_FILTER_NEGATIVE_COUNT,
                &self.slatedb_sst_filter_negatives,
            ),
            (
                slatedb::db_stats::SST_FILTER_FALSE_POSITIVE_COUNT,
                &self.slatedb_sst_filter_false_positives,
            ),
            (
                slatedb::compactor::stats::BYTES_COMPACTED,
                &self.slatedb_bytes_compacted,
            ),
        ];

        // Labeled cache counters (ACCESS_COUNT with entry_kind + result labels)
        let labeled_counter_mappings_cache: &[(&str, &[(&str, &str)], &CounterVec)] = &[
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "data_block"), ("result", "hit")],
                &self.slatedb_cache_data_block_hit,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "data_block"), ("result", "miss")],
                &self.slatedb_cache_data_block_miss,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "index"), ("result", "hit")],
                &self.slatedb_cache_index_hit,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "index"), ("result", "miss")],
                &self.slatedb_cache_index_miss,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "filter"), ("result", "hit")],
                &self.slatedb_cache_filter_hit,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "filter"), ("result", "miss")],
                &self.slatedb_cache_filter_miss,
            ),
        ];

        // Gauge-type stats: point-in-time values
        let gauge_mappings: &[(&str, &GaugeVec)] = &[
            (
                slatedb::db_stats::WAL_BUFFER_ESTIMATED_BYTES,
                &self.slatedb_wal_buffer_estimated_bytes,
            ),
            (
                slatedb::compactor::stats::RUNNING_COMPACTIONS,
                &self.slatedb_running_compactions,
            ),
            (
                slatedb::compactor::stats::LAST_COMPACTION_TS_SEC,
                &self.slatedb_last_compaction_ts_sec,
            ),
            (slatedb::db_stats::L0_SST_COUNT, &self.slatedb_l0_sst_count),
            (
                slatedb::db_stats::TOTAL_MEM_SIZE_BYTES,
                &self.slatedb_total_mem_size_bytes,
            ),
        ];

        let snapshot = recorder.snapshot();

        {
            let mut prev_values = self.slatedb_prev_values.lock().expect("lock poisoned");

            // Helper to extract a numeric value from a metric
            let extract_value = |metric: &slatedb_common::metrics::Metric| -> Option<f64> {
                match &metric.value {
                    MetricValue::Counter(v) => Some(*v as f64),
                    MetricValue::Gauge(v) => Some(*v as f64),
                    MetricValue::UpDownCounter(v) => Some(*v as f64),
                    MetricValue::Histogram { .. } => None,
                }
            };

            // Labeled counters (REQUEST_COUNT with op label, ACCESS_COUNT with entry_kind/result labels)
            for (stat_name, labels, counter) in labeled_counter_mappings
                .iter()
                .chain(labeled_counter_mappings_cache.iter())
            {
                if let Some(metric) = snapshot.by_name_and_labels(stat_name, labels)
                    && let Some(current) = extract_value(metric)
                {
                    // Use stat_name + labels as key to distinguish get/scan/flush
                    let label_suffix = labels
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    let key = (format!("{stat_name}/{label_suffix}"), shard.to_string());
                    let prev = prev_values.get(&key).copied().unwrap_or(0.0);
                    if current > prev {
                        counter.with_label_values(&[shard]).inc_by(current - prev);
                    }
                    prev_values.insert(key, current);
                }
            }

            // Unlabeled counters
            for (stat_name, counter) in counter_mappings {
                if let Some(metric) = snapshot.by_name(stat_name).first()
                    && let Some(current) = extract_value(metric)
                {
                    let key = (stat_name.to_string(), shard.to_string());
                    let prev = prev_values.get(&key).copied().unwrap_or(0.0);
                    if current > prev {
                        counter.with_label_values(&[shard]).inc_by(current - prev);
                    }
                    prev_values.insert(key, current);
                }
            }
        }

        for (stat_name, gauge) in gauge_mappings {
            if let Some(metric) = snapshot.by_name(stat_name).first() {
                let value = match &metric.value {
                    MetricValue::Counter(v) => *v as f64,
                    MetricValue::Gauge(v) => *v as f64,
                    MetricValue::UpDownCounter(v) => *v as f64,
                    MetricValue::Histogram { .. } => continue,
                };
                gauge.with_label_values(&[shard]).set(value);
            }
        }
    }
}

/// Helper to register a metric, logging on failure.
fn register<C: Collector + Clone + 'static>(registry: &Registry, metric: C) -> C {
    if let Err(e) = registry.register(Box::new(metric.clone())) {
        // Log but don't fail - metric may already be registered
        tracing::warn!(error = %e, "failed to register metric");
    }
    metric
}

/// Initialize the metrics system with a Prometheus registry.
///
/// Returns a `Metrics` handle that can be cloned and passed to components.
pub fn init() -> anyhow::Result<Metrics> {
    let registry = Registry::new();

    // Job metrics
    let jobs_enqueued = register(
        &registry,
        CounterVec::new(
            Opts::new("silo_jobs_enqueued_total", "Total number of jobs enqueued"),
            &["shard", "tenant"],
        )?,
    );

    let jobs_dequeued = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_jobs_dequeued_total",
                "Total number of tasks dequeued for execution",
            ),
            &["shard", "task_group"],
        )?,
    );

    let jobs_completed = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_jobs_completed_total",
                "Total number of jobs completed (succeeded, failed, cancelled)",
            ),
            &["shard", "status"],
        )?,
    );

    let job_attempts = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_job_attempts_total",
                "Total number of job attempts started",
            ),
            &["shard", "task_group", "is_retry"],
        )?,
    );

    let job_wait_time = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_job_wait_time_seconds",
                "Time jobs spent waiting in queue before being dequeued (enqueue to dequeue)",
            )
            .buckets(WAIT_TIME_BUCKETS.to_vec()),
            &["shard", "task_group"],
        )?,
    );

    // gRPC metrics
    let grpc_requests = register(
        &registry,
        CounterVec::new(
            Opts::new("silo_grpc_requests_total", "Total number of gRPC requests"),
            &["method", "status"],
        )?,
    );

    let grpc_request_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_grpc_request_duration_seconds",
                "gRPC request duration in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["method"],
        )?,
    );

    // Shard/broker metrics
    let shards_owned = register(
        &registry,
        Gauge::new("silo_shards_owned", "Number of shards owned by this node")?,
    );

    let coordination_shards_open = register(
        &registry,
        Gauge::new(
            "silo_coordination_shards_open",
            "Number of shards currently open in this process",
        )?,
    );

    let broker_buffer_size = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_broker_buffer_size",
                "Number of tasks in the broker buffer",
            ),
            &["shard", "task_group"],
        )?,
    );

    let broker_inflight_size = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_broker_inflight_size",
                "Number of tasks currently in-flight (claimed but not durably leased)",
            ),
            &["shard", "task_group"],
        )?,
    );

    let broker_scan_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_broker_scan_duration_seconds",
                "Duration of broker task scanning operations",
            )
            .buckets(SCAN_DURATION_BUCKETS.to_vec()),
            &["shard"],
        )?,
    );

    let broker_scans_total = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_broker_scans_total",
                "Total number of broker task scan operations",
            ),
            &["shard"],
        )?,
    );

    // Poll metrics
    let polls_total = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_polls_total",
                "Total number of lease_tasks polls received per shard",
            ),
            &["shard"],
        )?,
    );

    let poll_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_poll_duration_seconds",
                "Duration of lease_tasks poll per shard in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["shard"],
        )?,
    );

    // Lease metrics
    let task_leases_active = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_task_leases_active",
                "Number of active task leases (tasks currently leased to workers)",
            ),
            &["shard", "task_group"],
        )?,
    );

    let ready_to_start_latency_ms = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_ready_to_start_latency_ms",
                "Latency between when a task became ready and when it was first leased (in milliseconds)",
            )
            .buckets(READY_TO_START_LATENCY_MS_BUCKETS.to_vec()),
            &["shard", "task_group"],
        )?,
    );

    let lease_reaper_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_lease_reaper_duration_seconds",
                "Duration of expired lease reaper scan operations",
            )
            .buckets(SCAN_DURATION_BUCKETS.to_vec()),
            &["shard"],
        )?,
    );

    let lease_reaper_scans_total = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_lease_reaper_scans_total",
                "Total number of expired lease reaper scan operations",
            ),
            &["shard"],
        )?,
    );

    // Concurrency metrics
    let concurrency_tickets_granted = register(
        &registry,
        Counter::new(
            "silo_concurrency_tickets_granted_total",
            "Total number of concurrency tickets granted",
        )?,
    );

    // SlateDB storage counters (monotonically increasing in SlateDB)
    let slatedb_get_requests = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_get_requests_total",
                "Total number of GET (read) requests to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_scan_requests = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_scan_requests_total",
                "Total number of scan (range query) requests to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_write_ops = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_write_ops_total",
                "Total number of individual write operations to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_write_batch_count = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_write_batch_count_total",
                "Total number of write batches to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_backpressure_count = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_backpressure_count_total",
                "Number of times writes were blocked by back-pressure in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_wal_buffer_flushes = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_wal_buffer_flushes_total",
                "Total number of WAL buffer flushes in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_immutable_memtable_flushes = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_immutable_memtable_flushes_total",
                "Total number of immutable memtable flushes to SSTs in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_sst_filter_positives = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_sst_filter_positives_total",
                "Total SST filter true positives (key exists, filter says yes)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_sst_filter_negatives = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_sst_filter_negatives_total",
                "Total SST filter true negatives (key absent, filter says no)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_sst_filter_false_positives = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_sst_filter_false_positives_total",
                "Total SST filter false positives (key absent, but filter said yes)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_bytes_compacted = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_bytes_compacted_total",
                "Total number of bytes compacted by SlateDB compactor",
            ),
            &["shard"],
        )?,
    );

    // SlateDB storage gauges (point-in-time values)
    let slatedb_wal_buffer_estimated_bytes = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_wal_buffer_estimated_bytes",
                "Estimated bytes buffered in the SlateDB WAL buffer",
            ),
            &["shard"],
        )?,
    );

    let slatedb_running_compactions = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_running_compactions",
                "Number of compactions currently running in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_last_compaction_ts_sec = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_last_compaction_ts_seconds",
                "Unix timestamp (seconds) of the last compaction in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_flush_requests = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_flush_requests_total",
                "Total number of flush requests to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_l0_sst_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_l0_sst_count",
                "Number of Level-0 SSTs in SlateDB (high values indicate compaction lag)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_total_mem_size_bytes = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_total_mem_size_bytes",
                "Total memory usage of SlateDB",
            ),
            &["shard"],
        )?,
    );

    // SlateDB cache counters
    let slatedb_cache_data_block_hit = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_cache_data_block_hit_total",
                "Total SlateDB data block cache hits",
            ),
            &["shard"],
        )?,
    );

    let slatedb_cache_data_block_miss = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_cache_data_block_miss_total",
                "Total SlateDB data block cache misses",
            ),
            &["shard"],
        )?,
    );

    let slatedb_cache_index_hit = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_cache_index_hit_total",
                "Total SlateDB index block cache hits",
            ),
            &["shard"],
        )?,
    );

    let slatedb_cache_index_miss = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_cache_index_miss_total",
                "Total SlateDB index block cache misses",
            ),
            &["shard"],
        )?,
    );

    let slatedb_cache_filter_hit = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_cache_filter_hit_total",
                "Total SlateDB bloom filter cache hits",
            ),
            &["shard"],
        )?,
    );

    let slatedb_cache_filter_miss = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_cache_filter_miss_total",
                "Total SlateDB bloom filter cache misses",
            ),
            &["shard"],
        )?,
    );

    Ok(Metrics {
        registry: Arc::new(registry),
        jobs_enqueued,
        jobs_dequeued,
        jobs_completed,
        job_attempts,
        job_wait_time,
        grpc_requests,
        grpc_request_duration,
        shards_owned,
        coordination_shards_open,
        broker_buffer_size,
        broker_inflight_size,
        broker_scan_duration,
        broker_scans_total,
        polls_total,
        poll_duration,
        task_leases_active,
        ready_to_start_latency_ms,
        lease_reaper_duration,
        lease_reaper_scans_total,
        concurrency_tickets_granted,
        slatedb_get_requests,
        slatedb_scan_requests,
        slatedb_write_ops,
        slatedb_write_batch_count,
        slatedb_backpressure_count,
        slatedb_wal_buffer_flushes,
        slatedb_immutable_memtable_flushes,
        slatedb_sst_filter_positives,
        slatedb_sst_filter_negatives,
        slatedb_sst_filter_false_positives,
        slatedb_bytes_compacted,
        slatedb_flush_requests,
        slatedb_wal_buffer_estimated_bytes,
        slatedb_running_compactions,
        slatedb_last_compaction_ts_sec,
        slatedb_l0_sst_count,
        slatedb_total_mem_size_bytes,
        slatedb_cache_data_block_hit,
        slatedb_cache_data_block_miss,
        slatedb_cache_index_hit,
        slatedb_cache_index_miss,
        slatedb_cache_filter_hit,
        slatedb_cache_filter_miss,
        slatedb_prev_values: Arc::new(Mutex::new(HashMap::new())),
    })
}

/// Axum handler for the `/metrics` endpoint.
async fn metrics_handler(State(metrics): State<Metrics>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();

    let mut buffer = Vec::new();
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            buffer,
        ),
        Err(e) => {
            error!(error = %e, "failed to encode metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "text/plain; charset=utf-8")],
                format!("Failed to encode metrics: {}", e).into_bytes(),
            )
        }
    }
}

/// Run the Prometheus metrics HTTP server.
///
/// Listens on the given address and serves metrics at `/metrics`.
/// Shuts down gracefully when shutdown signal is received.
pub async fn run_metrics_server(
    addr: SocketAddr,
    metrics: Metrics,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    debug!(addr = %addr, "metrics server started");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.recv().await;
            debug!("metrics server shutting down");
        })
        .await?;

    Ok(())
}

/// Map a tonic status code to a short uppercase label matching the gRPC spec
/// (e.g. "OK", "NOT_FOUND", "INTERNAL").
fn grpc_code_label(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// Tower layer that records `silo_grpc_requests_total` and
/// `silo_grpc_request_duration_seconds` for every gRPC request.
///
/// When metrics are `None` the layer is a no-op passthrough.
#[derive(Clone)]
pub struct GrpcMetricsLayer {
    metrics: Option<Metrics>,
}

impl GrpcMetricsLayer {
    pub fn new(metrics: Option<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

/// Tower service that wraps an inner service and records gRPC metrics.
#[derive(Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
    metrics: Option<Metrics>,
}

type HttpRequest<T> = tonic::codegen::http::Request<T>;
type HttpResponse<T> = tonic::codegen::http::Response<T>;

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for GrpcMetricsService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = HttpResponse<ResBody>> + Clone + Send + 'static,
    <S as Service<HttpRequest<ReqBody>>>::Future: Send + 'static,
    <S as Service<HttpRequest<ReqBody>>>::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = HttpResponse<ResBody>;
    type Error = <S as Service<HttpRequest<ReqBody>>>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let metrics = self.metrics.clone();
        // Extract method name before req is moved into the inner call.
        // The path string is borrowed from the request URI so we only allocate
        // when metrics are enabled.
        let method = if metrics.is_some() {
            let path = req.uri().path();
            Some(
                path.rsplit_once('/')
                    .map(|(_, m)| m.to_owned())
                    .unwrap_or_else(|| path.to_owned()),
            )
        } else {
            None
        };
        // Clone a fresh service for future poll_ready calls, then swap so we
        // use the already-ready instance for this call.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let start = std::time::Instant::now();
            let result = inner.call(req).await;
            if let Some(ref m) = metrics {
                let method = method.as_deref().unwrap_or("unknown");
                let duration = start.elapsed().as_secs_f64();
                let code = match &result {
                    Ok(resp) => tonic::Status::from_header_map(resp.headers())
                        .map_or(tonic::Code::Ok, |s| s.code()),
                    Err(_) => tonic::Code::Internal,
                };
                let status = grpc_code_label(code);
                m.record_grpc_request(method, status);
                m.record_grpc_duration(method, duration);
            }
            result
        })
    }
}
