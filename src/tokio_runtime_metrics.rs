//! Tokio runtime metrics exposed through the Prometheus registry.
//!
//! Surfaces per-worker busy time (event-loop utilization) and signals that
//! point at task starvation: long mean poll times, deep local queues, forced
//! budget yields, and noop wakeups. Tokio's `RuntimeMetrics` exposes these as
//! absolute values; this module translates them into Prometheus-shaped
//! counters (via `inc_by(delta)` against a remembered previous value) and
//! gauges (via `set`).
//!
//! Most fields require `--cfg tokio_unstable`, which the silo build already
//! sets in `.cargo/config.toml` and the nix/process-compose builds.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts, Registry, core::Collector};
use tokio::runtime::RuntimeMetrics;
use tokio::sync::broadcast;
use tracing::debug;

/// Sentinel worker index for global (non-per-worker) counters in `prev_values`.
const GLOBAL: usize = usize::MAX;

/// Prometheus instruments for tokio runtime metrics, plus the previous-value
/// table needed to translate absolute counters into `inc_by(delta)`.
#[derive(Clone)]
pub struct TokioRuntimeMetrics {
    // Per-worker counters
    worker_busy_seconds: CounterVec,
    worker_park_count: CounterVec,
    worker_poll_count: CounterVec,
    worker_noop_count: CounterVec,
    worker_steal_count: CounterVec,
    worker_steal_operations: CounterVec,
    worker_local_schedule_count: CounterVec,
    worker_overflow_count: CounterVec,

    // Per-worker gauges
    worker_local_queue_depth: GaugeVec,
    worker_mean_poll_time_seconds: GaugeVec,

    // Globals
    num_workers: Gauge,
    num_alive_tasks: Gauge,
    global_queue_depth: Gauge,
    blocking_queue_depth: Gauge,
    num_blocking_threads: Gauge,
    num_idle_blocking_threads: Gauge,

    spawned_tasks: Counter,
    budget_forced_yield_count: Counter,
    io_driver_ready_count: Counter,

    /// Previous absolute counter values, keyed by `(stat_name, worker_index)`.
    /// `worker_index` is `GLOBAL` for non-per-worker counters.
    prev_values: Arc<Mutex<HashMap<(&'static str, usize), u64>>>,
}

impl TokioRuntimeMetrics {
    pub fn register(registry: &Registry) -> anyhow::Result<Self> {
        let counter_vec = |name: &str, help: &str| -> anyhow::Result<CounterVec> {
            Ok(register(
                registry,
                CounterVec::new(Opts::new(name, help), &["worker"])?,
            ))
        };
        let gauge_vec = |name: &str, help: &str| -> anyhow::Result<GaugeVec> {
            Ok(register(
                registry,
                GaugeVec::new(Opts::new(name, help), &["worker"])?,
            ))
        };

        Ok(Self {
            worker_busy_seconds: counter_vec(
                "silo_tokio_worker_busy_seconds_total",
                "Cumulative seconds each tokio worker thread spent polling tasks (use rate() for utilization)",
            )?,
            worker_park_count: counter_vec(
                "silo_tokio_worker_park_count_total",
                "Number of times a tokio worker has parked (gone idle waiting for work)",
            )?,
            worker_poll_count: counter_vec(
                "silo_tokio_worker_poll_count_total",
                "Number of tasks each tokio worker has polled",
            )?,
            worker_noop_count: counter_vec(
                "silo_tokio_worker_noop_count_total",
                "Number of times a tokio worker unparked but found no work to poll",
            )?,
            worker_steal_count: counter_vec(
                "silo_tokio_worker_steal_count_total",
                "Number of tasks each tokio worker has stolen from another worker's queue",
            )?,
            worker_steal_operations: counter_vec(
                "silo_tokio_worker_steal_operations_total",
                "Number of work-stealing operations performed by each tokio worker",
            )?,
            worker_local_schedule_count: counter_vec(
                "silo_tokio_worker_local_schedule_count_total",
                "Number of tasks scheduled onto each worker's local queue from itself",
            )?,
            worker_overflow_count: counter_vec(
                "silo_tokio_worker_overflow_count_total",
                "Number of times each worker's local queue overflowed into the global queue",
            )?,
            worker_local_queue_depth: gauge_vec(
                "silo_tokio_worker_local_queue_depth",
                "Current depth of each tokio worker's local run queue (high values suggest starvation)",
            )?,
            worker_mean_poll_time_seconds: gauge_vec(
                "silo_tokio_worker_mean_poll_time_seconds",
                "Exponentially-weighted mean task poll duration per worker (long polls block other tasks on that worker)",
            )?,
            num_workers: register(
                registry,
                Gauge::new(
                    "silo_tokio_workers",
                    "Number of tokio worker threads in the multi-threaded runtime",
                )?,
            ),
            num_alive_tasks: register(
                registry,
                Gauge::new(
                    "silo_tokio_alive_tasks",
                    "Number of tokio tasks currently alive (spawned and not yet completed)",
                )?,
            ),
            global_queue_depth: register(
                registry,
                Gauge::new(
                    "silo_tokio_global_queue_depth",
                    "Number of tasks pending in the tokio global injection queue",
                )?,
            ),
            blocking_queue_depth: register(
                registry,
                Gauge::new(
                    "silo_tokio_blocking_queue_depth",
                    "Number of tasks pending in the tokio blocking thread pool queue",
                )?,
            ),
            num_blocking_threads: register(
                registry,
                Gauge::new(
                    "silo_tokio_blocking_threads",
                    "Number of threads in the tokio blocking thread pool",
                )?,
            ),
            num_idle_blocking_threads: register(
                registry,
                Gauge::new(
                    "silo_tokio_idle_blocking_threads",
                    "Number of currently idle threads in the tokio blocking thread pool",
                )?,
            ),
            spawned_tasks: register(
                registry,
                Counter::new(
                    "silo_tokio_spawned_tasks_total",
                    "Total number of tasks spawned on the tokio runtime since startup",
                )?,
            ),
            budget_forced_yield_count: register(
                registry,
                Counter::new(
                    "silo_tokio_budget_forced_yield_total",
                    "Total number of times tokio's coop budget forced a task to yield (high values indicate CPU-bound tasks starving the runtime)",
                )?,
            ),
            io_driver_ready_count: register(
                registry,
                Counter::new(
                    "silo_tokio_io_driver_ready_total",
                    "Total number of I/O readiness events processed by the tokio I/O driver",
                )?,
            ),
            prev_values: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Read the current `RuntimeMetrics` snapshot and update Prometheus
    /// instruments. Counters track absolute values via the `prev_values`
    /// table and increment by the delta.
    pub fn scrape(&self, rm: &RuntimeMetrics) {
        let num_workers = rm.num_workers();
        self.num_workers.set(num_workers as f64);
        self.num_alive_tasks.set(rm.num_alive_tasks() as f64);
        self.global_queue_depth.set(rm.global_queue_depth() as f64);
        self.blocking_queue_depth
            .set(rm.blocking_queue_depth() as f64);
        self.num_blocking_threads
            .set(rm.num_blocking_threads() as f64);
        self.num_idle_blocking_threads
            .set(rm.num_idle_blocking_threads() as f64);

        self.inc_global_counter(
            "spawned_tasks",
            rm.spawned_tasks_count(),
            &self.spawned_tasks,
        );
        self.inc_global_counter(
            "budget_forced_yield",
            rm.budget_forced_yield_count(),
            &self.budget_forced_yield_count,
        );
        self.inc_global_counter(
            "io_driver_ready",
            rm.io_driver_ready_count(),
            &self.io_driver_ready_count,
        );

        for w in 0..num_workers {
            let label = w.to_string();

            if let Some(delta) = self.delta(
                "worker_busy_ns",
                w,
                duration_to_nanos(rm.worker_total_busy_duration(w)),
            ) {
                self.worker_busy_seconds
                    .with_label_values(&[&label])
                    .inc_by((delta as f64) / 1e9);
            }

            self.inc_worker_counter(
                "worker_park",
                w,
                &label,
                rm.worker_park_count(w),
                &self.worker_park_count,
            );
            self.inc_worker_counter(
                "worker_poll",
                w,
                &label,
                rm.worker_poll_count(w),
                &self.worker_poll_count,
            );
            self.inc_worker_counter(
                "worker_noop",
                w,
                &label,
                rm.worker_noop_count(w),
                &self.worker_noop_count,
            );
            self.inc_worker_counter(
                "worker_steal",
                w,
                &label,
                rm.worker_steal_count(w),
                &self.worker_steal_count,
            );
            self.inc_worker_counter(
                "worker_steal_ops",
                w,
                &label,
                rm.worker_steal_operations(w),
                &self.worker_steal_operations,
            );
            self.inc_worker_counter(
                "worker_local_sched",
                w,
                &label,
                rm.worker_local_schedule_count(w),
                &self.worker_local_schedule_count,
            );
            self.inc_worker_counter(
                "worker_overflow",
                w,
                &label,
                rm.worker_overflow_count(w),
                &self.worker_overflow_count,
            );

            self.worker_local_queue_depth
                .with_label_values(&[&label])
                .set(rm.worker_local_queue_depth(w) as f64);
            self.worker_mean_poll_time_seconds
                .with_label_values(&[&label])
                .set(rm.worker_mean_poll_time(w).as_secs_f64());
        }
    }

    fn inc_global_counter(&self, key: &'static str, current: u64, counter: &Counter) {
        if let Some(delta) = self.delta(key, GLOBAL, current) {
            counter.inc_by(delta as f64);
        }
    }

    fn inc_worker_counter(
        &self,
        key: &'static str,
        worker: usize,
        label: &str,
        current: u64,
        counter: &CounterVec,
    ) {
        if let Some(delta) = self.delta(key, worker, current) {
            counter.with_label_values(&[label]).inc_by(delta as f64);
        }
    }

    /// Compute `current - prev` for `(key, worker)` and remember `current`.
    /// Returns `None` on the first observation (no baseline yet) so we don't
    /// emit a spike from process startup. A counter that goes backwards (e.g.
    /// runtime restart in tests) is treated as a fresh baseline.
    fn delta(&self, key: &'static str, worker: usize, current: u64) -> Option<u64> {
        let mut prev = self.prev_values.lock().expect("prev_values poisoned");
        match prev.entry((key, worker)) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(current);
                None
            }
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let prev_value = *o.get();
                o.insert(current);
                Some(current.saturating_sub(prev_value))
            }
        }
    }
}

fn duration_to_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn register<C: Collector + Clone + 'static>(registry: &Registry, metric: C) -> C {
    if let Err(e) = registry.register(Box::new(metric.clone())) {
        tracing::warn!(error = %e, "failed to register tokio runtime metric");
    }
    metric
}

/// Periodically scrape `Handle::current().metrics()` into `metrics` until
/// `shutdown` fires. Default interval is 1 second to align with the existing
/// SlateDB scrape cadence.
pub async fn run_scraper(
    metrics: TokioRuntimeMetrics,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
) {
    let handle = tokio::runtime::Handle::current();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    debug!(
        interval_ms = interval.as_millis() as u64,
        "tokio runtime metrics scraper started"
    );

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                debug!("tokio runtime metrics scraper shutting down");
                break;
            }
            _ = ticker.tick() => {
                metrics.scrape(&handle.metrics());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};

    fn worker_counter_total(metric: &CounterVec, num_workers: usize) -> f64 {
        (0..num_workers)
            .map(|w| metric.with_label_values(&[&w.to_string()]).get())
            .sum()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_scrape_does_not_emit_startup_spike() {
        let registry = Registry::new();
        let metrics = TokioRuntimeMetrics::register(&registry).unwrap();
        let handle = tokio::runtime::Handle::current();

        // Burn through some work *before* the first scrape so the runtime's
        // absolute counters are non-zero. The first scrape should still leave
        // every Prometheus counter at zero — it's just establishing a baseline.
        for _ in 0..16 {
            tokio::spawn(async { tokio::task::yield_now().await })
                .await
                .unwrap();
        }

        metrics.scrape(&handle.metrics());

        assert_eq!(metrics.spawned_tasks.get(), 0.0);
        assert_eq!(metrics.budget_forced_yield_count.get(), 0.0);
        assert_eq!(metrics.io_driver_ready_count.get(), 0.0);
        assert_eq!(worker_counter_total(&metrics.worker_poll_count, 2), 0.0);
        assert_eq!(worker_counter_total(&metrics.worker_busy_seconds, 2), 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scrape_records_deltas_after_baseline() {
        let registry = Registry::new();
        let metrics = TokioRuntimeMetrics::register(&registry).unwrap();
        let handle = tokio::runtime::Handle::current();

        // Establish baseline.
        metrics.scrape(&handle.metrics());

        // Generate work so the second scrape sees non-zero deltas.
        let mut joins = Vec::new();
        for _ in 0..32 {
            joins.push(tokio::spawn(async {
                tokio::task::yield_now().await;
            }));
        }
        for j in joins {
            j.await.unwrap();
        }

        metrics.scrape(&handle.metrics());

        // Spawned-task counter must reflect the 32 tasks we just spawned.
        assert!(
            metrics.spawned_tasks.get() >= 32.0,
            "spawned_tasks={} should be >= 32",
            metrics.spawned_tasks.get()
        );

        // Workers actually polled tasks, so poll-count delta must be non-zero.
        let polls = worker_counter_total(&metrics.worker_poll_count, 2);
        assert!(polls > 0.0, "worker_poll_count_total={polls} should be > 0");

        // Gauges should populate without needing a baseline.
        assert_eq!(metrics.num_workers.get(), 2.0);

        // Sanity-check the encoded output as well.
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let body = String::from_utf8(buf).unwrap();
        assert!(body.contains("silo_tokio_worker_poll_count_total"));
        assert!(body.contains("worker=\"0\""));
        assert!(body.contains("worker=\"1\""));
    }
}
