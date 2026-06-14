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

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use prometheus::{
    CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
    core::Collector,
};
use slatedb_common::metrics::{DefaultMetricsRecorder, LATENCY_BOUNDARIES, MetricValue};
use tokio::sync::broadcast;
use tower::{Layer, Service};
use tracing::{debug, error};

use crate::job_store_shard::counters::{
    BACKGROUND_ACTION_EXPLICIT_QUEUE_KIND, BACKGROUND_ACTION_IMPLICIT_QUEUE_KIND,
    BACKGROUND_ACTION_QUEUE_SIZE_BUCKET, BACKGROUND_ACTION_RUNNING_SIZE_BUCKET,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};

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

const BACKGROUND_ACTION_ZERO_RETENTION: Duration = Duration::from_secs(5 * 60);

// SlateDB's `instrumented_object_store::stats` module is `pub(crate)` upstream,
// so we hardcode these names. Once https://github.com/slatedb/slatedb/pull/1628
// lands and we upgrade, replace these with `slatedb::instrumented_object_store::stats::*`.
const OS_REQUEST_COUNT: &str = "slatedb.object_store.request_count";
const OS_ERROR_COUNT: &str = "slatedb.object_store.error_count";
const OS_REQUEST_DURATION_SECONDS: &str = "slatedb.object_store.request_duration_seconds";

/// Code path that produced a concurrency ticket grant. Becomes the `path`
/// label on `silo_concurrency_tickets_granted_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantPath {
    /// Synchronous enqueue path where `try_reserve` finds capacity and a
    /// holder is created right away (no request queue round-trip).
    Immediate,
    /// Async grant scanner or dequeue ticket-request processing path that
    /// drains queued ticket requests as capacity becomes available.
    Scanned,
}

impl GrantPath {
    pub fn as_str(self) -> &'static str {
        match self {
            GrantPath::Immediate => "immediate",
            GrantPath::Scanned => "scanned",
        }
    }
}

// (shard_id, tenant, task_group, queue, queue_kind)
type BackgroundActionMetricKey = (String, String, String, String, String);
type BackgroundActionGaugeState = Arc<Mutex<HashMap<BackgroundActionMetricKey, GaugeSeriesState>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GaugeSeriesState {
    NonZero,
    Zero { since: Instant },
}

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
    silo_background_actions_queue_size: GaugeVec,
    silo_background_actions_running_size: GaugeVec,
    background_actions_queue_state: BackgroundActionGaugeState,
    background_actions_running_state: BackgroundActionGaugeState,

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
    broker_scan_tasks_read: CounterVec,
    broker_tombstone_count: GaugeVec,

    // Poll metrics
    polls_total: CounterVec,
    poll_duration: HistogramVec,

    // Shard lifecycle metrics
    shard_open_duration: HistogramVec,
    shard_close_duration: HistogramVec,

    // Lease metrics
    task_leases_active: GaugeVec,
    ready_to_start_latency_ms: HistogramVec,
    lease_reaper_duration: HistogramVec,
    lease_reaper_scans_total: CounterVec,
    lease_reaper_leases_reaped_total: CounterVec,
    lease_reaper_errors_total: CounterVec,

    // Concurrency metrics
    concurrency_tickets_granted: CounterVec,
    concurrency_tickets_converted: CounterVec,
    concurrency_holders_cache_holders: GaugeVec,
    concurrency_holders_cache_queues: GaugeVec,
    concurrency_holders_cache_hydrated_queues: GaugeVec,
    /// Sum of `|in_memory_holders - durable_holders|` across hydrated queues.
    /// A non-zero value means a reconciler tick observed a drift between the
    /// authoritative durable holder rows and the in-memory `holders` set —
    /// the same signal that, if left to persist, would manifest as a wedge
    /// (the 300/300 production case).
    concurrency_holder_drift: GaugeVec,
    /// Count of `(tenant, queue, task_id)` tuples currently queued for the
    /// per-task_id reconciler (drained on each tick + on `reconcile_notify`).
    /// Steady-state expected to be 0; sustained growth means the reconciler
    /// is stuck (slatedb unavailable, etc.).
    concurrency_reconciliation_pending: GaugeVec,

    // SlateDB watcher metrics (driven by Db::subscribe)
    slatedb_durable_seq: GaugeVec,
    slatedb_manifest_revisions: CounterVec,
    slatedb_manifest_last_l0_seq: GaugeVec,
    slatedb_manifest_l0_count: GaugeVec,
    slatedb_manifest_compacted_count: GaugeVec,
    slatedb_manifest_checkpoints_count: GaugeVec,

    /// SlateDB per-shard counters and gauges. Shared with silo-compactor via
    /// the `metric_prefix` constructor argument.
    pub slatedb: SlatedbShardMetrics,

    /// Tokio runtime metrics (worker busy time, queue depths, mean poll time)
    /// driven by a periodic scraper spawned in `main.rs`.
    pub tokio_runtime: crate::tokio_runtime_metrics::TokioRuntimeMetrics,

    /// Jemalloc allocator stats driven by a periodic scraper spawned in `main.rs`.
    #[cfg(unix)]
    pub jemalloc: crate::jemalloc_metrics::JemallocMetrics,
}

/// Computed status gauges for Gadget background actions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackgroundActionMetricSnapshot {
    pub queue_size: HashMap<BackgroundActionMetricKey, u64>,
    pub running_size: HashMap<BackgroundActionMetricKey, u64>,
}

impl BackgroundActionMetricSnapshot {
    pub fn merge(&mut self, other: BackgroundActionMetricSnapshot) {
        for (key, value) in other.queue_size {
            increment(&mut self.queue_size, key, value);
        }
        for (key, value) in other.running_size {
            increment(&mut self.running_size, key, value);
        }
    }
}

#[derive(Debug, Default)]
struct BackgroundActionStatusCounts {
    // key: (shard, tenant, task_group, queue, queue_kind)
    by_queue: HashMap<BackgroundActionMetricKey, u64>,
}

/// SlateDB per-shard metric instruments plus the previous-value map needed
/// to translate SlateDB's absolute counters into Prometheus `inc_by(delta)`.
///
/// Registers a fixed set of `<prefix>slatedb_*` counters and gauges against a
/// shared `Registry`. Used by both the silo server (prefix `silo_`) and the
/// silo-compactor binary (prefix `silo_compactor_`) so the same translation
/// logic powers both.
#[derive(Clone)]
pub struct SlatedbShardMetrics {
    // Counters (monotonically increasing in SlateDB)
    get_requests: CounterVec,
    scan_requests: CounterVec,
    write_ops: CounterVec,
    write_batch_count: CounterVec,
    backpressure_count: CounterVec,
    wal_buffer_flushes: CounterVec,
    immutable_memtable_flushes: CounterVec,
    sst_filter_positives: CounterVec,
    sst_filter_negatives: CounterVec,
    sst_filter_false_positives: CounterVec,
    bytes_compacted: CounterVec,
    flush_requests: CounterVec,
    expired_entries_purged_value: CounterVec,
    expired_entries_purged_merge: CounterVec,

    // Gauges (point-in-time)
    wal_buffer_estimated_bytes: GaugeVec,
    running_compactions: GaugeVec,
    last_compaction_ts_sec: GaugeVec,
    l0_sst_count: GaugeVec,
    total_mem_size_bytes: GaugeVec,

    // Cache counters
    cache_data_block_hit: CounterVec,
    cache_data_block_miss: CounterVec,
    cache_index_hit: CounterVec,
    cache_index_miss: CounterVec,
    cache_filter_hit: CounterVec,
    cache_filter_miss: CounterVec,

    // Object store metrics (per component/store_type/op/api)
    object_store_requests: CounterVec,
    object_store_errors: CounterVec,
    object_store_request_duration: HistogramVec,

    /// Tracks previous SlateDB counter values per (stat_name, shard) for delta computation.
    /// SlateDB exposes counters as absolute values via `stat.get()`, but Prometheus counters
    /// only support `inc_by(delta)`, so we compute deltas between polls.
    #[allow(clippy::type_complexity)]
    prev_values: Arc<Mutex<HashMap<(String, String), f64>>>,

    /// Tracks previous cumulative bucket counts for SlateDB histograms, keyed
    /// the same way as `prev_values`. Needed because the `prometheus` crate
    /// exposes histograms only via `observe(x)`, so we re-observe deltas.
    #[allow(clippy::type_complexity)]
    prev_histogram_buckets: Arc<Mutex<HashMap<(String, String), Vec<u64>>>>,
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

    /// Record background action status gauge values for the current scrape.
    pub fn record_background_action_metrics(&self, snapshot: BackgroundActionMetricSnapshot) {
        let failed_shards = HashSet::new();
        self.record_background_action_metrics_except_shards(snapshot, &failed_shards);
    }

    /// Record background action status gauges while preserving previous values
    /// for shards whose background-action collection failed on this scrape.
    pub(crate) fn record_background_action_metrics_except_shards(
        &self,
        snapshot: BackgroundActionMetricSnapshot,
        failed_shards: &HashSet<String>,
    ) {
        self.record_background_action_gauge(
            &self.silo_background_actions_queue_size,
            &self.background_actions_queue_state,
            &snapshot.queue_size,
            failed_shards,
        );
        self.record_background_action_gauge(
            &self.silo_background_actions_running_size,
            &self.background_actions_running_state,
            &snapshot.running_size,
            failed_shards,
        );
    }

    fn record_background_action_gauge(
        &self,
        gauge: &GaugeVec,
        state: &BackgroundActionGaugeState,
        values: &HashMap<BackgroundActionMetricKey, u64>,
        failed_shards: &HashSet<String>,
    ) {
        record_background_action_gauge_inner(gauge, state, values, failed_shards);
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

    /// Record a SlateDB `DbStatus` snapshot observed via `Db::subscribe`.
    ///
    /// `manifest_changed` should be `true` only when `status.current_manifest`
    /// differs from the previously-observed manifest for this shard, so the
    /// `silo_slatedb_manifest_revisions_total` counter increments once per
    /// distinct manifest revision rather than on every status update.
    pub fn record_db_status(
        &self,
        shard: &str,
        status: &slatedb::DbStatus,
        manifest_changed: bool,
    ) {
        let manifest = &status.current_manifest;
        self.slatedb_durable_seq
            .with_label_values(&[shard])
            .set(status.durable_seq as f64);
        self.slatedb_manifest_last_l0_seq
            .with_label_values(&[shard])
            .set(manifest.last_l0_seq() as f64);
        self.slatedb_manifest_l0_count
            .with_label_values(&[shard])
            .set(manifest.l0().len() as f64);
        self.slatedb_manifest_compacted_count
            .with_label_values(&[shard])
            .set(manifest.compacted().len() as f64);
        self.slatedb_manifest_checkpoints_count
            .with_label_values(&[shard])
            .set(manifest.checkpoints().len() as f64);
        if manifest_changed {
            self.slatedb_manifest_revisions
                .with_label_values(&[shard])
                .inc();
        }
    }

    /// Record broker scan duration in seconds.
    pub fn record_broker_scan_duration(&self, shard: &str, duration_secs: f64) {
        self.broker_scan_duration
            .with_label_values(&[shard])
            .observe(duration_secs);
        self.broker_scans_total.with_label_values(&[shard]).inc();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_broker_scan_tasks(
        &self,
        shard: &str,
        task_group: &str,
        inserted: u64,
        skipped_future: u64,
        skipped_inflight: u64,
        skipped_tombstone: u64,
        skipped_already_buffered: u64,
        skipped_defunct: u64,
    ) {
        for (outcome, count) in [
            ("inserted", inserted),
            ("skipped_future", skipped_future),
            ("skipped_inflight", skipped_inflight),
            ("skipped_tombstone", skipped_tombstone),
            ("skipped_already_buffered", skipped_already_buffered),
            ("skipped_defunct", skipped_defunct),
        ] {
            self.broker_scan_tasks_read
                .with_label_values(&[shard, task_group, outcome])
                .inc_by(count as f64);
        }
    }

    /// Update the number of ack tombstones for a shard and task group.
    pub fn set_broker_tombstone_count(&self, shard: &str, task_group: &str, count: u64) {
        self.broker_tombstone_count
            .with_label_values(&[shard, task_group])
            .set(count as f64);
    }

    /// Record a poll (lease_tasks call) for a shard/task_group.
    pub fn record_poll(&self, shard: &str, task_group: &str) {
        self.polls_total
            .with_label_values(&[shard, task_group])
            .inc();
    }

    /// Record poll duration (time to service a lease_tasks call for a shard/task_group) in seconds.
    pub fn record_poll_duration(&self, shard: &str, task_group: &str, duration_secs: f64) {
        self.poll_duration
            .with_label_values(&[shard, task_group])
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

    /// Record the duration of a shard open phase (e.g. `db_build`, `hydrate`,
    /// `rebuild_counters`, `set_created_at`, `total`) in seconds.
    pub fn record_shard_open_phase(&self, shard: &str, phase: &str, duration_secs: f64) {
        self.shard_open_duration
            .with_label_values(&[shard, phase])
            .observe(duration_secs);
    }

    /// Record the duration of a shard close phase (e.g. `flush`, `db_close`,
    /// `wal_cleanup`, `total`) in seconds.
    pub fn record_shard_close_phase(&self, shard: &str, phase: &str, duration_secs: f64) {
        self.shard_close_duration
            .with_label_values(&[shard, phase])
            .observe(duration_secs);
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

    /// Record the number of expired leases reaped during a scan.
    pub fn record_lease_reaper_reaped(&self, shard: &str, count: u64) {
        self.lease_reaper_leases_reaped_total
            .with_label_values(&[shard])
            .inc_by(count as f64);
    }

    /// Record a lease reaper scan failure.
    pub fn record_lease_reaper_error(&self, shard: &str) {
        self.lease_reaper_errors_total
            .with_label_values(&[shard])
            .inc();
    }

    /// Record `n` concurrency tickets being granted via `path` on `shard`.
    pub fn record_concurrency_tickets_granted(&self, shard: &str, path: GrantPath, n: u64) {
        if n == 0 {
            return;
        }
        self.concurrency_tickets_granted
            .with_label_values(&[shard, path.as_str()])
            .inc_by(n as f64);
    }

    /// Record `n` at-capacity RequestTicket tasks converted into deferred
    /// concurrency request records at dequeue time on `shard`.
    pub fn record_concurrency_tickets_converted(&self, shard: &str, n: u64) {
        if n == 0 {
            return;
        }
        self.concurrency_tickets_converted
            .with_label_values(&[shard])
            .inc_by(n as f64);
    }

    /// Update the in-memory concurrency holders cache size gauges for a shard.
    pub fn set_concurrency_holders_cache_stats(
        &self,
        shard: &str,
        stats: crate::concurrency::ConcurrencyCacheStats,
    ) {
        self.concurrency_holders_cache_holders
            .with_label_values(&[shard])
            .set(stats.total_holders as f64);
        self.concurrency_holders_cache_queues
            .with_label_values(&[shard])
            .set(stats.queue_count as f64);
        self.concurrency_holders_cache_hydrated_queues
            .with_label_values(&[shard])
            .set(stats.hydrated_queue_count as f64);
    }

    /// Update per-shard reconciler signal gauges:
    /// * `drift` = sum of |in_memory - durable| holder counts across hydrated
    ///   queues, computed at the end of each periodic reconciler tick.
    /// * `pending` = current size of the per-task_id reconciliation queue.
    ///
    /// Non-zero `drift` or sustained-non-zero `pending` indicates the
    /// reconciler is observing inconsistency between in-memory and durable
    /// holder state — the same signal that, if it grows, manifests as the
    /// production queue wedge. Pair with an alert that fires if either
    /// remains non-zero across multiple scrape intervals.
    pub fn set_concurrency_reconciler_signals(&self, shard: &str, drift: u64, pending: u64) {
        self.concurrency_holder_drift
            .with_label_values(&[shard])
            .set(drift as f64);
        self.concurrency_reconciliation_pending
            .with_label_values(&[shard])
            .set(pending as f64);
    }

    /// Update SlateDB storage metrics from a shard's StatRegistry.
    ///
    /// Call this periodically (e.g., every second) to sync SlateDB's internal
    /// statistics to Prometheus metrics. Counter-type stats are tracked via deltas
    /// since Prometheus counters only support `inc_by()`, not `set()`.
    pub fn update_slatedb_stats(&self, shard: &str, recorder: &DefaultMetricsRecorder) {
        self.slatedb.update(shard, recorder);
    }
}

/// Build background action status gauges from all represented jobs in a shard.
pub async fn collect_background_action_metrics_for_shard(
    shard_id: &str,
    shard: &JobStoreShard,
) -> Result<BackgroundActionMetricSnapshot, JobStoreShardError> {
    let mut queued = BackgroundActionStatusCounts::default();
    let mut running = BackgroundActionStatusCounts::default();

    for counter in shard.snapshot_background_action_queue_counters() {
        match counter.status_bucket.as_str() {
            BACKGROUND_ACTION_QUEUE_SIZE_BUCKET => queued.add_counter(
                shard_id,
                &counter.tenant,
                &counter.task_group,
                &counter.queue_kind,
                &counter.queue,
                counter.count,
            ),
            BACKGROUND_ACTION_RUNNING_SIZE_BUCKET => running.add_counter(
                shard_id,
                &counter.tenant,
                &counter.task_group,
                &counter.queue_kind,
                &counter.queue,
                counter.count,
            ),
            _ => {}
        }
    }

    let mut snapshot = BackgroundActionMetricSnapshot::default();
    queued.finish_into(&mut snapshot.queue_size);
    running.finish_into(&mut snapshot.running_size);
    Ok(snapshot)
}

impl BackgroundActionStatusCounts {
    fn add_counter(
        &mut self,
        shard_id: &str,
        tenant: &str,
        task_group: &str,
        queue_kind: &str,
        queue: &str,
        count: i64,
    ) {
        let Ok(count) = u64::try_from(count) else {
            return;
        };
        if count == 0 {
            return;
        }
        // Reject unknown queue_kind values rather than silently bucketing them
        // — the counter-side constants are the source of truth.
        if queue_kind != BACKGROUND_ACTION_EXPLICIT_QUEUE_KIND
            && queue_kind != BACKGROUND_ACTION_IMPLICIT_QUEUE_KIND
        {
            return;
        }

        increment(
            &mut self.by_queue,
            (
                shard_id.to_string(),
                tenant.to_string(),
                task_group.to_string(),
                queue.to_string(),
                queue_kind.to_string(),
            ),
            count,
        );
    }

    fn finish_into(self, output: &mut HashMap<BackgroundActionMetricKey, u64>) {
        for (key, count) in self.by_queue {
            increment(output, key, count);
        }
    }
}

fn increment<K: Eq + std::hash::Hash>(map: &mut HashMap<K, u64>, key: K, amount: u64) {
    *map.entry(key).or_insert(0) += amount;
}

/// Set each `(shard, tenant, task_group, queue, queue_kind)` series in
/// `values`. Series that disappear from `values` are kept at `0` for a short
/// retention period before deletion so Prometheus sees drained queues report
/// zero before the label set disappears.
fn record_background_action_gauge_inner(
    gauge: &GaugeVec,
    state: &BackgroundActionGaugeState,
    values: &HashMap<BackgroundActionMetricKey, u64>,
    failed_shards: &HashSet<String>,
) {
    record_background_action_gauge_inner_at(
        gauge,
        state,
        values,
        failed_shards,
        Instant::now(),
        BACKGROUND_ACTION_ZERO_RETENTION,
    );
}

fn record_background_action_gauge_inner_at(
    gauge: &GaugeVec,
    state: &BackgroundActionGaugeState,
    values: &HashMap<BackgroundActionMetricKey, u64>,
    failed_shards: &HashSet<String>,
    now: Instant,
    zero_retention: Duration,
) {
    let mut to_remove = Vec::new();
    let mut to_set = Vec::new();
    {
        let mut state = state.lock().unwrap();

        for (key, value) in values {
            if *value > 0 {
                state.insert(key.clone(), GaugeSeriesState::NonZero);
                to_set.push((key.clone(), *value as f64));
            } else {
                retain_zero_or_expire(
                    &mut state,
                    key.clone(),
                    now,
                    zero_retention,
                    false,
                    &mut to_set,
                    &mut to_remove,
                );
            }
        }

        let missing: Vec<_> = state
            .keys()
            .filter(|key| !values.contains_key(*key) && !failed_shards.contains(&key.0))
            .cloned()
            .collect();
        for key in missing {
            retain_zero_or_expire(
                &mut state,
                key,
                now,
                zero_retention,
                true,
                &mut to_set,
                &mut to_remove,
            );
        }
    }

    for (shard_id, tenant, task_group, queue, queue_kind) in &to_remove {
        // `remove_label_values` returns Err if the series was never
        // registered (e.g. process restart with stale state); that's
        // fine — the goal is "no series in the scrape" either way.
        let _ = gauge.remove_label_values(&[shard_id, tenant, task_group, queue, queue_kind]);
    }
    for ((shard_id, tenant, task_group, queue, queue_kind), value) in &to_set {
        gauge
            .with_label_values(&[shard_id, tenant, task_group, queue, queue_kind])
            .set(*value);
    }
}

fn retain_zero_or_expire(
    state: &mut HashMap<BackgroundActionMetricKey, GaugeSeriesState>,
    key: BackgroundActionMetricKey,
    now: Instant,
    zero_retention: Duration,
    remember_new_zero: bool,
    to_set: &mut Vec<(BackgroundActionMetricKey, f64)>,
    to_remove: &mut Vec<BackgroundActionMetricKey>,
) {
    let zero_since = match state.entry(key.clone()) {
        Entry::Occupied(mut entry) => match *entry.get() {
            GaugeSeriesState::Zero { since } => since,
            GaugeSeriesState::NonZero => {
                entry.insert(GaugeSeriesState::Zero { since: now });
                now
            }
        },
        Entry::Vacant(entry) => {
            if !remember_new_zero {
                return;
            }
            entry.insert(GaugeSeriesState::Zero { since: now });
            now
        }
    };

    if now.saturating_duration_since(zero_since) >= zero_retention {
        state.remove(&key);
        to_remove.push(key);
    } else {
        to_set.push((key, 0.0));
    }
}

#[cfg(test)]
mod background_action_metric_tests {
    use super::*;

    fn key(
        shard: &str,
        tenant: &str,
        task_group: &str,
        queue: &str,
        queue_kind: &str,
    ) -> BackgroundActionMetricKey {
        (
            shard.to_string(),
            tenant.to_string(),
            task_group.to_string(),
            queue.to_string(),
            queue_kind.to_string(),
        )
    }

    #[test]
    fn explicit_and_implicit_queues_are_reported_with_queue_kind_label() {
        let mut counts = BackgroundActionStatusCounts::default();

        // An app that opts into a user queue contributes to BOTH the explicit
        // series (under its user queue name) and the implicit series (under
        // the platform queue it still acquires). Dashboards reconstruct
        // "actions without a user-specified queue" via
        // sum(implicit) - sum(explicit).
        counts.add_counter(
            "shard-a",
            "env-env-1",
            "default",
            BACKGROUND_ACTION_EXPLICIT_QUEUE_KIND,
            "user-a",
            1,
        );
        counts.add_counter(
            "shard-a",
            "env-env-1",
            "default",
            BACKGROUND_ACTION_EXPLICIT_QUEUE_KIND,
            "user-b",
            1,
        );
        counts.add_counter(
            "shard-a",
            "env-env-1",
            "default",
            BACKGROUND_ACTION_IMPLICIT_QUEUE_KIND,
            "platform-a",
            3,
        );
        counts.add_counter(
            "shard-b",
            "tenant-env-1",
            "background",
            BACKGROUND_ACTION_IMPLICIT_QUEUE_KIND,
            "x",
            1,
        );
        let mut output = HashMap::new();
        counts.finish_into(&mut output);

        assert_eq!(
            output.get(&key(
                "shard-a",
                "env-env-1",
                "default",
                "user-a",
                "explicit"
            )),
            Some(&1)
        );
        assert_eq!(
            output.get(&key(
                "shard-a",
                "env-env-1",
                "default",
                "user-b",
                "explicit"
            )),
            Some(&1)
        );
        assert_eq!(
            output.get(&key(
                "shard-a",
                "env-env-1",
                "default",
                "platform-a",
                "implicit"
            )),
            Some(&3)
        );
        assert_eq!(
            output.get(&key(
                "shard-b",
                "tenant-env-1",
                "background",
                "x",
                "implicit"
            )),
            Some(&1)
        );
        assert!(
            output
                .keys()
                .all(|(_, _, _, queue, _)| queue != "<no queue>"),
            "no `<no queue>` synthesis: {output:?}"
        );
    }

    #[test]
    fn unknown_queue_kind_is_rejected() {
        let mut counts = BackgroundActionStatusCounts::default();
        counts.add_counter("shard-a", "env-env-1", "default", "bogus", "q", 5);
        let mut output = HashMap::new();
        counts.finish_into(&mut output);
        assert!(output.is_empty());
    }

    #[test]
    fn explicit_only_emits_only_explicit_series() {
        let mut counts = BackgroundActionStatusCounts::default();

        counts.add_counter(
            "shard-a",
            "env-env-1",
            "default",
            BACKGROUND_ACTION_EXPLICIT_QUEUE_KIND,
            "user-a",
            1,
        );

        let mut output = HashMap::new();
        counts.finish_into(&mut output);

        assert_eq!(
            output.get(&key(
                "shard-a",
                "env-env-1",
                "default",
                "user-a",
                "explicit"
            )),
            Some(&1)
        );
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn drained_labels_emit_zero_until_retention_expires() {
        let registry = Registry::new();
        let gauge = GaugeVec::new(
            Opts::new("test_bg_q", "test"),
            &["shard", "tenant", "task_group", "queue", "queue_kind"],
        )
        .unwrap();
        registry.register(Box::new(gauge.clone())).unwrap();
        let state: BackgroundActionGaugeState = Arc::new(Mutex::new(HashMap::new()));
        let failed_shards = HashSet::new();
        let retention = Duration::from_secs(300);
        let t0 = Instant::now();

        let mut first = HashMap::new();
        first.insert(key("shard-a", "env-1", "default", "user-a", "explicit"), 3);
        first.insert(
            key("shard-a", "env-1", "default", "platform-1", "implicit"),
            3,
        );
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &first,
            &failed_shards,
            t0,
            retention,
        );
        assert_eq!(
            gauge
                .with_label_values(&["shard-a", "env-1", "default", "user-a", "explicit"])
                .get(),
            3.0
        );

        // user-a drops to 0 (removed from in-memory map -> absent from the
        // next snapshot). The next record pass should keep the series and
        // export zero during the retention window.
        let mut second = HashMap::new();
        second.insert(
            key("shard-a", "env-1", "default", "platform-1", "implicit"),
            2,
        );
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &second,
            &failed_shards,
            t0 + Duration::from_secs(1),
            retention,
        );

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let raw = String::from_utf8(buf).unwrap();
        assert!(
            raw.contains(
                "test_bg_q{queue=\"user-a\",queue_kind=\"explicit\",shard=\"shard-a\",task_group=\"default\",tenant=\"env-1\"} 0"
            ),
            "drained user-a series should be emitted as zero:\n{raw}"
        );
        assert!(
            raw.contains("platform-1"),
            "live series should remain:\n{raw}"
        );

        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &second,
            &failed_shards,
            t0 + retention + Duration::from_secs(1),
            retention,
        );

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let raw = String::from_utf8(buf).unwrap();
        assert!(
            !raw.contains("user-a"),
            "user-a series should be removed after zero retention expires:\n{raw}"
        );
        assert!(
            raw.contains("platform-1"),
            "live series should remain after stale expiry:\n{raw}"
        );
    }

    #[test]
    fn nonzero_values_reset_zero_retention() {
        let registry = Registry::new();
        let gauge = GaugeVec::new(
            Opts::new("test_bg_reset", "test"),
            &["shard", "tenant", "task_group", "queue", "queue_kind"],
        )
        .unwrap();
        registry.register(Box::new(gauge.clone())).unwrap();
        let state: BackgroundActionGaugeState = Arc::new(Mutex::new(HashMap::new()));
        let failed_shards = HashSet::new();
        let retention = Duration::from_secs(300);
        let t0 = Instant::now();
        let metric_key = key("shard-a", "env-1", "default", "user-a", "explicit");

        let mut values = HashMap::new();
        values.insert(metric_key.clone(), 3);
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &values,
            &failed_shards,
            t0,
            retention,
        );

        values.clear();
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &values,
            &failed_shards,
            t0 + Duration::from_secs(60),
            retention,
        );

        values.insert(metric_key.clone(), 2);
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &values,
            &failed_shards,
            t0 + Duration::from_secs(120),
            retention,
        );

        values.clear();
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &values,
            &failed_shards,
            t0 + Duration::from_secs(360),
            retention,
        );

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let raw = String::from_utf8(buf).unwrap();
        assert!(
            raw.contains(
                "test_bg_reset{queue=\"user-a\",queue_kind=\"explicit\",shard=\"shard-a\",task_group=\"default\",tenant=\"env-1\"} 0"
            ),
            "nonzero value should reset the zero-retention timer:\n{raw}"
        );

        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &values,
            &failed_shards,
            t0 + Duration::from_secs(661),
            retention,
        );

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let raw = String::from_utf8(buf).unwrap();
        assert!(
            !raw.contains("user-a"),
            "series should expire five minutes after the reset zero observation:\n{raw}"
        );
    }

    #[test]
    fn failed_shards_keep_previous_values() {
        let registry = Registry::new();
        let gauge = GaugeVec::new(
            Opts::new("test_bg_failed", "test"),
            &["shard", "tenant", "task_group", "queue", "queue_kind"],
        )
        .unwrap();
        registry.register(Box::new(gauge.clone())).unwrap();
        let state: BackgroundActionGaugeState = Arc::new(Mutex::new(HashMap::new()));
        let failed_shards = HashSet::new();
        let retention = Duration::from_secs(300);
        let t0 = Instant::now();

        let mut first = HashMap::new();
        first.insert(key("shard-a", "env-1", "default", "user-a", "explicit"), 3);
        first.insert(key("shard-b", "env-1", "default", "user-b", "explicit"), 4);
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &first,
            &failed_shards,
            t0,
            retention,
        );

        let mut second = HashMap::new();
        second.insert(key("shard-a", "env-1", "default", "user-a", "explicit"), 1);
        let mut failed_shards = HashSet::new();
        failed_shards.insert("shard-b".to_string());
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &second,
            &failed_shards,
            t0 + Duration::from_secs(1),
            retention,
        );

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let raw = String::from_utf8(buf).unwrap();
        assert!(
            raw.contains(
                "test_bg_failed{queue=\"user-a\",queue_kind=\"explicit\",shard=\"shard-a\",task_group=\"default\",tenant=\"env-1\"} 1"
            ),
            "successfully collected shard should update:\n{raw}"
        );
        assert!(
            raw.contains(
                "test_bg_failed{queue=\"user-b\",queue_kind=\"explicit\",shard=\"shard-b\",task_group=\"default\",tenant=\"env-1\"} 4"
            ),
            "failed shard should keep its previous value:\n{raw}"
        );

        let failed_shards = HashSet::new();
        record_background_action_gauge_inner_at(
            &gauge,
            &state,
            &second,
            &failed_shards,
            t0 + Duration::from_secs(2),
            retention,
        );

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let raw = String::from_utf8(buf).unwrap();
        assert!(
            raw.contains(
                "test_bg_failed{queue=\"user-b\",queue_kind=\"explicit\",shard=\"shard-b\",task_group=\"default\",tenant=\"env-1\"} 0"
            ),
            "once collection is no longer marked failed, omission should drain normally:\n{raw}"
        );
    }
}

impl SlatedbShardMetrics {
    /// Register the full set of SlateDB per-shard counters and gauges against
    /// `registry`. `metric_prefix` is prepended to every metric name so silo
    /// (`silo_`) and silo-compactor (`silo_compactor_`) can coexist on the same
    /// scrape config without collisions.
    pub fn register(registry: &Registry, metric_prefix: &str) -> anyhow::Result<Self> {
        let p = metric_prefix;
        let counter = |name: &str, help: &str| -> anyhow::Result<CounterVec> {
            Ok(register(
                registry,
                CounterVec::new(Opts::new(format!("{p}{name}"), help), &["shard"])?,
            ))
        };
        let gauge = |name: &str, help: &str| -> anyhow::Result<GaugeVec> {
            Ok(register(
                registry,
                GaugeVec::new(Opts::new(format!("{p}{name}"), help), &["shard"])?,
            ))
        };
        let labeled_counter =
            |name: &str, help: &str, labels: &[&str]| -> anyhow::Result<CounterVec> {
                Ok(register(
                    registry,
                    CounterVec::new(Opts::new(format!("{p}{name}"), help), labels)?,
                ))
            };
        let labeled_histogram =
            |name: &str, help: &str, labels: &[&str]| -> anyhow::Result<HistogramVec> {
                Ok(register(
                    registry,
                    HistogramVec::new(
                        HistogramOpts::new(format!("{p}{name}"), help)
                            .buckets(LATENCY_BOUNDARIES.to_vec()),
                        labels,
                    )?,
                ))
            };
        let os_labels: &[&str] = &["shard", "component", "store_type", "op", "api"];

        Ok(Self {
            get_requests: counter(
                "slatedb_get_requests_total",
                "Total number of GET (read) requests to SlateDB",
            )?,
            scan_requests: counter(
                "slatedb_scan_requests_total",
                "Total number of scan (range query) requests to SlateDB",
            )?,
            write_ops: counter(
                "slatedb_write_ops_total",
                "Total number of individual write operations to SlateDB",
            )?,
            write_batch_count: counter(
                "slatedb_write_batch_count_total",
                "Total number of write batches to SlateDB",
            )?,
            backpressure_count: counter(
                "slatedb_backpressure_count_total",
                "Number of times writes were blocked by back-pressure in SlateDB",
            )?,
            wal_buffer_flushes: counter(
                "slatedb_wal_buffer_flushes_total",
                "Total number of WAL buffer flushes in SlateDB",
            )?,
            immutable_memtable_flushes: counter(
                "slatedb_immutable_memtable_flushes_total",
                "Total number of immutable memtable flushes to SSTs in SlateDB",
            )?,
            sst_filter_positives: counter(
                "slatedb_sst_filter_positives_total",
                "Total SST filter true positives (key exists, filter says yes)",
            )?,
            sst_filter_negatives: counter(
                "slatedb_sst_filter_negatives_total",
                "Total SST filter true negatives (key absent, filter says no)",
            )?,
            sst_filter_false_positives: counter(
                "slatedb_sst_filter_false_positives_total",
                "Total SST filter false positives (key absent, but filter said yes)",
            )?,
            bytes_compacted: counter(
                "slatedb_bytes_compacted_total",
                "Total number of bytes compacted by SlateDB compactor",
            )?,
            flush_requests: counter(
                "slatedb_flush_requests_total",
                "Total number of flush requests to SlateDB",
            )?,
            expired_entries_purged_value: counter(
                "slatedb_expired_entries_purged_value_total",
                "Total expired value entries purged (rewritten as tombstones) by the SlateDB RetentionIterator during compaction",
            )?,
            expired_entries_purged_merge: counter(
                "slatedb_expired_entries_purged_merge_total",
                "Total expired merge entries dropped (without a tombstone) by the SlateDB RetentionIterator during compaction",
            )?,
            wal_buffer_estimated_bytes: gauge(
                "slatedb_wal_buffer_estimated_bytes",
                "Estimated bytes buffered in the SlateDB WAL buffer",
            )?,
            running_compactions: gauge(
                "slatedb_running_compactions",
                "Number of compactions currently running in SlateDB",
            )?,
            last_compaction_ts_sec: gauge(
                "slatedb_last_compaction_ts_seconds",
                "Unix timestamp (seconds) of the last compaction in SlateDB",
            )?,
            l0_sst_count: gauge(
                "slatedb_l0_sst_count",
                "Number of Level-0 SSTs in SlateDB (high values indicate compaction lag)",
            )?,
            total_mem_size_bytes: gauge(
                "slatedb_total_mem_size_bytes",
                "Total memory usage of SlateDB",
            )?,
            cache_data_block_hit: counter(
                "slatedb_cache_data_block_hit_total",
                "Total SlateDB data block cache hits",
            )?,
            cache_data_block_miss: counter(
                "slatedb_cache_data_block_miss_total",
                "Total SlateDB data block cache misses",
            )?,
            cache_index_hit: counter(
                "slatedb_cache_index_hit_total",
                "Total SlateDB index block cache hits",
            )?,
            cache_index_miss: counter(
                "slatedb_cache_index_miss_total",
                "Total SlateDB index block cache misses",
            )?,
            cache_filter_hit: counter(
                "slatedb_cache_filter_hit_total",
                "Total SlateDB bloom filter cache hits",
            )?,
            cache_filter_miss: counter(
                "slatedb_cache_filter_miss_total",
                "Total SlateDB bloom filter cache misses",
            )?,
            object_store_requests: labeled_counter(
                "slatedb_object_store_requests_total",
                "Total SlateDB object-store API calls (per component/store_type/op/api)",
                os_labels,
            )?,
            object_store_errors: labeled_counter(
                "slatedb_object_store_errors_total",
                "Total SlateDB object-store API errors (per component/store_type/op/api)",
                os_labels,
            )?,
            object_store_request_duration: labeled_histogram(
                "slatedb_object_store_request_duration_seconds",
                "SlateDB object-store API call latency (per component/store_type/op/api)",
                os_labels,
            )?,
            prev_values: Arc::new(Mutex::new(HashMap::new())),
            prev_histogram_buckets: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Translate a snapshot of SlateDB's internal counters/gauges into the
    /// registered Prometheus instruments. Counters are converted to deltas
    /// against the previous snapshot since Prometheus counters only support
    /// `inc_by()`.
    #[allow(clippy::type_complexity)]
    pub fn update(&self, shard: &str, recorder: &DefaultMetricsRecorder) {
        // Counter-type stats: monotonically increasing in SlateDB
        // REQUEST_COUNT uses an "op" label to distinguish get/scan/flush
        let labeled_counter_mappings: &[(&str, &[(&str, &str)], &CounterVec)] = &[
            (
                slatedb::db_stats::REQUEST_COUNT,
                &[("op", "get")],
                &self.get_requests,
            ),
            (
                slatedb::db_stats::REQUEST_COUNT,
                &[("op", "scan")],
                &self.scan_requests,
            ),
            (
                slatedb::db_stats::REQUEST_COUNT,
                &[("op", "flush")],
                &self.flush_requests,
            ),
            // RetentionIterator splits the EXPIRED_ENTRIES_PURGED counter by an
            // `entry_type` label; map each value into its own per-shard instrument.
            (
                slatedb::compactor::stats::EXPIRED_ENTRIES_PURGED,
                &[(
                    slatedb::compactor::stats::ENTRY_TYPE_LABEL,
                    slatedb::compactor::stats::ENTRY_TYPE_VALUE,
                )],
                &self.expired_entries_purged_value,
            ),
            (
                slatedb::compactor::stats::EXPIRED_ENTRIES_PURGED,
                &[(
                    slatedb::compactor::stats::ENTRY_TYPE_LABEL,
                    slatedb::compactor::stats::ENTRY_TYPE_MERGE,
                )],
                &self.expired_entries_purged_merge,
            ),
        ];
        let counter_mappings: &[(&str, &CounterVec)] = &[
            (slatedb::db_stats::WRITE_OPS, &self.write_ops),
            (
                slatedb::db_stats::WRITE_BATCH_COUNT,
                &self.write_batch_count,
            ),
            (
                slatedb::db_stats::BACKPRESSURE_COUNT,
                &self.backpressure_count,
            ),
            (
                slatedb::db_stats::WAL_BUFFER_FLUSHES,
                &self.wal_buffer_flushes,
            ),
            (
                slatedb::db_stats::IMMUTABLE_MEMTABLE_FLUSHES,
                &self.immutable_memtable_flushes,
            ),
            (
                slatedb::db_stats::SST_FILTER_POSITIVE_COUNT,
                &self.sst_filter_positives,
            ),
            (
                slatedb::db_stats::SST_FILTER_NEGATIVE_COUNT,
                &self.sst_filter_negatives,
            ),
            (
                slatedb::db_stats::SST_FILTER_FALSE_POSITIVE_COUNT,
                &self.sst_filter_false_positives,
            ),
            (
                slatedb::compactor::stats::BYTES_COMPACTED,
                &self.bytes_compacted,
            ),
        ];

        // Labeled cache counters (ACCESS_COUNT with entry_kind + result labels)
        let labeled_counter_mappings_cache: &[(&str, &[(&str, &str)], &CounterVec)] = &[
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "data_block"), ("result", "hit")],
                &self.cache_data_block_hit,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "data_block"), ("result", "miss")],
                &self.cache_data_block_miss,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "index"), ("result", "hit")],
                &self.cache_index_hit,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "index"), ("result", "miss")],
                &self.cache_index_miss,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "filter"), ("result", "hit")],
                &self.cache_filter_hit,
            ),
            (
                slatedb::db_cache_stats::ACCESS_COUNT,
                &[("entry_kind", "filter"), ("result", "miss")],
                &self.cache_filter_miss,
            ),
        ];

        // Gauge-type stats: point-in-time values
        let gauge_mappings: &[(&str, &GaugeVec)] = &[
            (
                slatedb::db_stats::WAL_BUFFER_ESTIMATED_BYTES,
                &self.wal_buffer_estimated_bytes,
            ),
            (
                slatedb::compactor::stats::RUNNING_COMPACTIONS,
                &self.running_compactions,
            ),
            (
                slatedb::compactor::stats::LAST_COMPACTION_TS_SEC,
                &self.last_compaction_ts_sec,
            ),
            (slatedb::db_stats::L0_SST_COUNT, &self.l0_sst_count),
            (
                slatedb::db_stats::TOTAL_MEM_SIZE_BYTES,
                &self.total_mem_size_bytes,
            ),
        ];

        let snapshot = recorder.snapshot();

        {
            let mut prev_values = self.prev_values.lock().expect("lock poisoned");

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

        // Object-store metrics: variable label sets (component/store_type/op/api).
        // Counters use the same delta-vs-previous trick as the rest of `update`;
        // the histogram is delta-translated bucket-by-bucket through `observe()`
        // since the `prometheus` crate has no public bucket-counter API.
        {
            let mut prev_values = self.prev_values.lock().expect("lock poisoned");
            let mut prev_histogram_buckets =
                self.prev_histogram_buckets.lock().expect("lock poisoned");

            for (stat_name, counter_vec) in [
                (OS_REQUEST_COUNT, &self.object_store_requests),
                (OS_ERROR_COUNT, &self.object_store_errors),
            ] {
                for metric in snapshot.by_name(stat_name) {
                    let current = match metric.value {
                        MetricValue::Counter(v) => v as f64,
                        _ => continue,
                    };
                    let vals = os_label_values(&metric.labels);
                    let key = (os_prev_key(stat_name, &vals), shard.to_string());
                    let prev = prev_values.get(&key).copied().unwrap_or(0.0);
                    if current > prev {
                        counter_vec
                            .with_label_values(&[shard, &vals[0], &vals[1], &vals[2], &vals[3]])
                            .inc_by(current - prev);
                    }
                    prev_values.insert(key, current);
                }
            }

            for metric in snapshot.by_name(OS_REQUEST_DURATION_SECONDS) {
                let (boundaries, bucket_counts) = match &metric.value {
                    MetricValue::Histogram {
                        boundaries,
                        bucket_counts,
                        ..
                    } => (boundaries, bucket_counts),
                    _ => continue,
                };
                let vals = os_label_values(&metric.labels);
                let key = (
                    os_prev_key(OS_REQUEST_DURATION_SECONDS, &vals),
                    shard.to_string(),
                );
                let prev = prev_histogram_buckets
                    .get(&key)
                    .cloned()
                    .unwrap_or_default();
                let prom_histogram = self
                    .object_store_request_duration
                    .with_label_values(&[shard, &vals[0], &vals[1], &vals[2], &vals[3]]);
                for (i, &current_count) in bucket_counts.iter().enumerate() {
                    let prev_count = prev.get(i).copied().unwrap_or(0);
                    let delta = current_count.saturating_sub(prev_count);
                    if delta == 0 {
                        continue;
                    }
                    let observe_value = value_for_bucket(boundaries, i);
                    for _ in 0..delta {
                        prom_histogram.observe(observe_value);
                    }
                }
                prev_histogram_buckets.insert(key, bucket_counts.clone());
            }
        }
    }
}

/// Pull the four object-store label values out of a `slatedb_common` metric's
/// labels, in the order our Prometheus instrument expects them.
fn os_label_values(labels: &[(String, String)]) -> [String; 4] {
    let lookup = |key: &str| {
        labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    [
        lookup("component"),
        lookup("store_type"),
        lookup("op"),
        lookup("api"),
    ]
}

/// Build a stable key for tracking previous values across polls for an
/// object-store series.
fn os_prev_key(stat_name: &str, vals: &[String; 4]) -> String {
    format!(
        "{stat_name}/component={},store_type={},op={},api={}",
        vals[0], vals[1], vals[2], vals[3]
    )
}

/// Pick a value strictly inside histogram bucket `idx` for both SlateDB's `<`
/// bucketing semantics and Prometheus's `<=` semantics, given the bucket
/// boundary list. SlateDB's `bucket_counts` has length `boundaries.len() + 1`
/// (the last entry being the overflow bucket).
fn value_for_bucket(boundaries: &[f64], idx: usize) -> f64 {
    if boundaries.is_empty() {
        return 0.0;
    }
    if idx == 0 {
        boundaries[0] / 2.0
    } else if idx < boundaries.len() {
        (boundaries[idx - 1] + boundaries[idx]) / 2.0
    } else {
        boundaries[boundaries.len() - 1] * 1.5
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

    // Process-level metrics (RSS, VSZ, FDs, CPU). Linux-only because the
    // prometheus crate's ProcessCollector reads /proc.
    #[cfg(target_os = "linux")]
    {
        let pc = prometheus::process_collector::ProcessCollector::for_self();
        if let Err(e) = registry.register(Box::new(pc)) {
            tracing::warn!(error = %e, "failed to register process collector");
        }
    }

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
    let silo_background_actions_queue_size = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_background_actions_queue_size",
                "Number of Silo background action jobs waiting or scheduled to run",
            ),
            &["shard", "tenant", "task_group", "queue", "queue_kind"],
        )?,
    );
    let silo_background_actions_running_size = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_background_actions_running_size",
                "Number of Silo background action jobs currently running",
            ),
            &["shard", "tenant", "task_group", "queue", "queue_kind"],
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

    let broker_scan_tasks_read = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_broker_scan_tasks_read_total",
                "Total tasks read during broker scans, broken down by outcome",
            ),
            &["shard", "task_group", "outcome"],
        )?,
    );

    let broker_tombstone_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_broker_tombstone_count",
                "Number of ack tombstones held by the broker",
            ),
            &["shard", "task_group"],
        )?,
    );

    // Poll metrics
    let polls_total = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_polls_total",
                "Total number of lease_tasks polls received per shard/task_group",
            ),
            &["shard", "task_group"],
        )?,
    );

    let poll_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_poll_duration_seconds",
                "Duration of lease_tasks poll per shard/task_group in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["shard", "task_group"],
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

    // Shard lifecycle metrics. Reuses WAIT_TIME_BUCKETS (up to 3600s) because
    // shard opens have been observed to take minutes in staging.
    let shard_open_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_shard_open_duration_seconds",
                "Duration of shard open, broken down by phase",
            )
            .buckets(WAIT_TIME_BUCKETS.to_vec()),
            &["shard", "phase"],
        )?,
    );

    let shard_close_duration = register(
        &registry,
        HistogramVec::new(
            HistogramOpts::new(
                "silo_shard_close_duration_seconds",
                "Duration of shard close, broken down by phase",
            )
            .buckets(WAIT_TIME_BUCKETS.to_vec()),
            &["shard", "phase"],
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

    let lease_reaper_leases_reaped_total = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_lease_reaper_leases_reaped_total",
                "Total number of expired leases reaped",
            ),
            &["shard"],
        )?,
    );

    let lease_reaper_errors_total = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_lease_reaper_errors_total",
                "Total number of expired lease reaper scan failures",
            ),
            &["shard"],
        )?,
    );

    // Concurrency metrics
    let concurrency_tickets_granted = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_concurrency_tickets_granted_total",
                "Total number of concurrency tickets granted, labelled by shard and grant path",
            ),
            &["shard", "path"],
        )?,
    );

    let concurrency_tickets_converted = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_concurrency_tickets_converted_total",
                "Total number of at-capacity RequestTicket tasks converted into deferred concurrency request records at dequeue time",
            ),
            &["shard"],
        )?,
    );

    let concurrency_holders_cache_holders = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_concurrency_holders_cache_holders",
                "Total number of holder entries in the in-memory concurrency holders cache",
            ),
            &["shard"],
        )?,
    );

    let concurrency_holders_cache_queues = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_concurrency_holders_cache_queues",
                "Number of (tenant, queue) pairs tracked in the in-memory concurrency holders cache",
            ),
            &["shard"],
        )?,
    );

    let concurrency_holders_cache_hydrated_queues = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_concurrency_holders_cache_hydrated_queues",
                "Number of (tenant, queue) pairs hydrated from durable storage into the concurrency holders cache",
            ),
            &["shard"],
        )?,
    );

    let concurrency_holder_drift = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_concurrency_holder_drift",
                "Per-shard sum of |in_memory - durable| holder counts across hydrated queues, observed at the last reconciler tick. Non-zero = drift the reconciler will release; sustained non-zero means the reconciler is stuck.",
            ),
            &["shard"],
        )?,
    );

    let concurrency_reconciliation_pending = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_concurrency_reconciliation_pending",
                "Number of (tenant, queue, task_id) tuples awaiting per-task_id reconciliation against durable holder rows. Steady-state expected 0; sustained > 0 means the reconciler is wedged.",
            ),
            &["shard"],
        )?,
    );

    // SlateDB watcher metrics (driven by Db::subscribe)
    let slatedb_durable_seq = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_durable_seq",
                "SlateDB durable sequence number from Db::subscribe (last seq persisted to object storage)",
            ),
            &["shard"],
        )?,
    );

    let slatedb_manifest_revisions = register(
        &registry,
        CounterVec::new(
            Opts::new(
                "silo_slatedb_manifest_revisions_total",
                "Number of distinct SlateDB manifest revisions observed via Db::subscribe",
            ),
            &["shard"],
        )?,
    );

    let slatedb_manifest_last_l0_seq = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_manifest_last_l0_seq",
                "last_l0_seq field of the current SlateDB manifest",
            ),
            &["shard"],
        )?,
    );

    let slatedb_manifest_l0_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_manifest_l0_count",
                "Number of L0 SSTs in the current SlateDB manifest",
            ),
            &["shard"],
        )?,
    );

    let slatedb_manifest_compacted_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_manifest_compacted_count",
                "Number of compacted sorted runs in the current SlateDB manifest",
            ),
            &["shard"],
        )?,
    );

    let slatedb_manifest_checkpoints_count = register(
        &registry,
        GaugeVec::new(
            Opts::new(
                "silo_slatedb_manifest_checkpoints_count",
                "Number of active checkpoints in the current SlateDB manifest",
            ),
            &["shard"],
        )?,
    );

    let slatedb = SlatedbShardMetrics::register(&registry, "silo_")?;
    let tokio_runtime = crate::tokio_runtime_metrics::TokioRuntimeMetrics::register(&registry)?;
    #[cfg(unix)]
    let jemalloc = crate::jemalloc_metrics::JemallocMetrics::register(&registry)?;

    Ok(Metrics {
        registry: Arc::new(registry),
        jobs_enqueued,
        jobs_dequeued,
        jobs_completed,
        job_attempts,
        job_wait_time,
        silo_background_actions_queue_size,
        silo_background_actions_running_size,
        background_actions_queue_state: Arc::new(Mutex::new(HashMap::new())),
        background_actions_running_state: Arc::new(Mutex::new(HashMap::new())),
        grpc_requests,
        grpc_request_duration,
        shards_owned,
        coordination_shards_open,
        broker_buffer_size,
        broker_inflight_size,
        broker_scan_duration,
        broker_scans_total,
        broker_scan_tasks_read,
        broker_tombstone_count,
        polls_total,
        poll_duration,
        shard_open_duration,
        shard_close_duration,
        task_leases_active,
        ready_to_start_latency_ms,
        lease_reaper_duration,
        lease_reaper_scans_total,
        lease_reaper_leases_reaped_total,
        lease_reaper_errors_total,
        concurrency_tickets_granted,
        concurrency_tickets_converted,
        concurrency_holders_cache_holders,
        concurrency_holders_cache_queues,
        concurrency_holders_cache_hydrated_queues,
        concurrency_holder_drift,
        concurrency_reconciliation_pending,
        slatedb_durable_seq,
        slatedb_manifest_revisions,
        slatedb_manifest_last_l0_seq,
        slatedb_manifest_l0_count,
        slatedb_manifest_compacted_count,
        slatedb_manifest_checkpoints_count,
        slatedb,
        tokio_runtime,
        #[cfg(unix)]
        jemalloc,
    })
}

/// Axum handler that gathers a registry and encodes it in Prometheus text format.
async fn registry_handler(State(registry): State<Arc<Registry>>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();

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

/// Serve a Prometheus `/metrics` endpoint backed by `registry`.
///
/// Used by both the silo server and silo-compactor binaries.
pub async fn serve_registry(
    addr: SocketAddr,
    registry: Arc<Registry>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/metrics", get(registry_handler))
        .with_state(registry);

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

/// Run the Prometheus metrics HTTP server backed by silo's `Metrics` registry.
pub async fn run_metrics_server(
    addr: SocketAddr,
    metrics: Metrics,
    shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_registry(addr, metrics.registry.clone(), shutdown).await
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
