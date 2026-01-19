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

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use prometheus::{
    core::Collector, Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec,
    Opts, Registry, TextEncoder,
};
use slatedb::stats::StatRegistry;
use tokio::sync::broadcast;
use tracing::{debug, error};

/// Default histogram buckets for request latencies (in seconds)
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Histogram buckets for job wait times (in seconds) - wider range for queue latency
const WAIT_TIME_BUCKETS: &[f64] = &[
    0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0,
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

    // Lease metrics
    task_leases_active: GaugeVec,

    // Concurrency metrics
    concurrency_holders: GaugeVec,
    concurrency_tickets_granted: Counter,

    // SlateDB storage metrics (per-shard)
    slatedb_get_requests: GaugeVec,
    slatedb_scan_requests: GaugeVec,
    slatedb_write_ops: GaugeVec,
    slatedb_write_batch_count: GaugeVec,
    slatedb_backpressure_count: GaugeVec,
    slatedb_wal_buffer_estimated_bytes: GaugeVec,
    slatedb_wal_buffer_flushes: GaugeVec,
    slatedb_immutable_memtable_flushes: GaugeVec,
    slatedb_sst_filter_positives: GaugeVec,
    slatedb_sst_filter_negatives: GaugeVec,
    slatedb_sst_filter_false_positives: GaugeVec,
    slatedb_bytes_compacted: GaugeVec,
    slatedb_running_compactions: GaugeVec,
    slatedb_last_compaction_ts_sec: GaugeVec,
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

    /// Update broker buffer size for a shard.
    pub fn set_broker_buffer_size(&self, shard: &str, size: u64) {
        self.broker_buffer_size
            .with_label_values(&[shard])
            .set(size as f64);
    }

    /// Update broker inflight size for a shard.
    pub fn set_broker_inflight_size(&self, shard: &str, size: u64) {
        self.broker_inflight_size
            .with_label_values(&[shard])
            .set(size as f64);
    }

    /// Record broker scan duration in seconds.
    pub fn record_broker_scan_duration(&self, shard: &str, duration_secs: f64) {
        self.broker_scan_duration
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

    /// Update concurrency holders count for a queue.
    pub fn set_concurrency_holders(&self, tenant: &str, queue: &str, count: u64) {
        self.concurrency_holders
            .with_label_values(&[tenant, queue])
            .set(count as f64);
    }

    /// Record a concurrency ticket being granted.
    pub fn record_concurrency_ticket_granted(&self) {
        self.concurrency_tickets_granted.inc();
    }

    /// Update SlateDB storage metrics from a shard's StatRegistry.
    ///
    /// Call this periodically (e.g., every second) to sync SlateDB's internal
    /// statistics to Prometheus gauges.
    pub fn update_slatedb_stats(&self, shard: &str, stats: &Arc<StatRegistry>) {
        // DB stats
        if let Some(stat) = stats.lookup(slatedb::db_stats::GET_REQUESTS) {
            self.slatedb_get_requests
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::SCAN_REQUESTS) {
            self.slatedb_scan_requests
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::WRITE_OPS) {
            self.slatedb_write_ops
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::WRITE_BATCH_COUNT) {
            self.slatedb_write_batch_count
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::BACKPRESSURE_COUNT) {
            self.slatedb_backpressure_count
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::WAL_BUFFER_ESTIMATED_BYTES) {
            self.slatedb_wal_buffer_estimated_bytes
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::WAL_BUFFER_FLUSHES) {
            self.slatedb_wal_buffer_flushes
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::IMMUTABLE_MEMTABLE_FLUSHES) {
            self.slatedb_immutable_memtable_flushes
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::SST_FILTER_POSITIVES) {
            self.slatedb_sst_filter_positives
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::SST_FILTER_NEGATIVES) {
            self.slatedb_sst_filter_negatives
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::db_stats::SST_FILTER_FALSE_POSITIVES) {
            self.slatedb_sst_filter_false_positives
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }

        // Compactor stats
        if let Some(stat) = stats.lookup(slatedb::compactor_stats::BYTES_COMPACTED) {
            self.slatedb_bytes_compacted
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::compactor_stats::RUNNING_COMPACTIONS) {
            self.slatedb_running_compactions
                .with_label_values(&[shard])
                .set(stat.get() as f64);
        }
        if let Some(stat) = stats.lookup(slatedb::compactor_stats::LAST_COMPACTION_TS_SEC) {
            self.slatedb_last_compaction_ts_sec
                .with_label_values(&[shard])
                .set(stat.get() as f64);
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
            &["shard"],
        )?,
    );

    let broker_inflight_size = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_broker_inflight_size",
                "Number of tasks currently in-flight (claimed but not durably leased)",
            ),
            &["shard"],
        )?,
    );

    let broker_scan_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_broker_scan_duration_seconds",
                "Duration of broker task scanning operations",
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

    // Concurrency metrics
    let concurrency_holders = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_concurrency_holders",
                "Number of active concurrency ticket holders per queue",
            ),
            &["tenant", "queue"],
        )?,
    );

    let concurrency_tickets_granted = register(
        &registry,
        Counter::new(
            "silo_concurrency_tickets_granted_total",
            "Total number of concurrency tickets granted",
        )?,
    );

    // SlateDB storage metrics
    let slatedb_get_requests = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_get_requests_total",
                "Total number of GET (read) requests to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_scan_requests = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_scan_requests_total",
                "Total number of scan (range query) requests to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_write_ops = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_write_ops_total",
                "Total number of individual write operations to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_write_batch_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_write_batch_count_total",
                "Total number of write batches to SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_backpressure_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_backpressure_count_total",
                "Number of times writes were blocked by back-pressure in SlateDB",
            ),
            &["shard"],
        )?,
    );

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

    let slatedb_wal_buffer_flushes = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_wal_buffer_flushes_total",
                "Total number of WAL buffer flushes in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_immutable_memtable_flushes = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_immutable_memtable_flushes_total",
                "Total number of immutable memtable flushes to SSTs in SlateDB",
            ),
            &["shard"],
        )?,
    );

    let slatedb_sst_filter_positives = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_sst_filter_positives_total",
                "Total SST filter true positives (key exists, filter says yes)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_sst_filter_negatives = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_sst_filter_negatives_total",
                "Total SST filter true negatives (key absent, filter says no)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_sst_filter_false_positives = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_sst_filter_false_positives_total",
                "Total SST filter false positives (key absent, but filter said yes)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_bytes_compacted = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_bytes_compacted_total",
                "Total number of bytes compacted by SlateDB compactor",
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
        task_leases_active,
        concurrency_holders,
        concurrency_tickets_granted,
        slatedb_get_requests,
        slatedb_scan_requests,
        slatedb_write_ops,
        slatedb_write_batch_count,
        slatedb_backpressure_count,
        slatedb_wal_buffer_estimated_bytes,
        slatedb_wal_buffer_flushes,
        slatedb_immutable_memtable_flushes,
        slatedb_sst_filter_positives,
        slatedb_sst_filter_negatives,
        slatedb_sst_filter_false_positives,
        slatedb_bytes_compacted,
        slatedb_running_compactions,
        slatedb_last_compaction_ts_sec,
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
