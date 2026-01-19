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
    core::Collector, CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use tokio::sync::broadcast;
use tracing::{debug, error};

/// Default histogram buckets for request latencies (in seconds)
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Silo metrics handle containing all metric instruments.
#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,

    // Job metrics
    jobs_enqueued: CounterVec,
    jobs_dequeued: CounterVec,
    jobs_completed: CounterVec,

    // gRPC metrics
    grpc_requests: CounterVec,
    grpc_request_duration: HistogramVec,

    // Shard/broker metrics
    shards_owned: Gauge,
    broker_buffer_size: GaugeVec,
    broker_inflight_size: GaugeVec,

    // Concurrency metrics
    concurrency_holders: GaugeVec,
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

    /// Update the number of shards owned by this node.
    pub fn set_shards_owned(&self, count: u64) {
        self.shards_owned.set(count as f64);
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

    /// Update concurrency holders count for a queue.
    pub fn set_concurrency_holders(&self, tenant: &str, queue: &str, count: u64) {
        self.concurrency_holders
            .with_label_values(&[tenant, queue])
            .set(count as f64);
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

    Ok(Metrics {
        registry: Arc::new(registry),
        jobs_enqueued,
        jobs_dequeued,
        jobs_completed,
        grpc_requests,
        grpc_request_duration,
        shards_owned,
        broker_buffer_size,
        broker_inflight_size,
        concurrency_holders,
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
