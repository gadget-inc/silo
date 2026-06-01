use std::sync::Arc;
use std::time::Duration;

use slatedb_common::metrics::DefaultMetricsRecorder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::error::CompactorError;
use crate::metrics::CompactorMetrics;
use crate::shard_map::ShardId;
use crate::storage::{Backend, path_for_shard, resolve_object_store};

/// Period at which per-shard slatedb stats are polled and translated into
/// Prometheus instruments. Roughly matches silo's per-shard cadence.
const SLATEDB_STAT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Handle to a per-shard compactor task. Drop the handle to stop the worker:
/// call `shutdown().await` first to wait for graceful shutdown.
pub struct WorkerHandle {
    pub shard_id: ShardId,
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl WorkerHandle {
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

/// Spawn a tokio task that runs slatedb's standalone compactor for `shard_id`,
/// restarting on transient errors with `restart_backoff` between attempts.
pub fn spawn_worker(
    shard_id: ShardId,
    backend: Backend,
    path_template: Arc<String>,
    compactor_options: Arc<Option<slatedb::config::CompactorOptions>>,
    restart_backoff: Duration,
    metrics: Option<Arc<CompactorMetrics>>,
) -> WorkerHandle {
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let join = tokio::spawn(async move {
        run_supervisor(
            shard_id,
            backend,
            path_template,
            compactor_options,
            restart_backoff,
            metrics,
            cancel_for_task,
        )
        .await;
    });
    WorkerHandle {
        shard_id,
        cancel,
        join,
    }
}

async fn run_supervisor(
    shard_id: ShardId,
    backend: Backend,
    path_template: Arc<String>,
    compactor_options: Arc<Option<slatedb::config::CompactorOptions>>,
    restart_backoff: Duration,
    metrics: Option<Arc<CompactorMetrics>>,
    cancel: CancellationToken,
) {
    let shard_label = shard_id.to_string();
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match run_once(
            &shard_id,
            &backend,
            &path_template,
            compactor_options.as_ref().clone(),
            metrics.as_ref(),
            &cancel,
        )
        .await
        {
            Ok(()) => {
                info!(shard = %shard_id, "compactor stopped cleanly");
                return;
            }
            Err(e) => {
                if let Some(m) = &metrics {
                    m.record_compactor_run_error(&shard_label);
                    m.record_worker_restart(&shard_label);
                }
                warn!(
                    shard = %shard_id,
                    error = %e,
                    backoff_secs = restart_backoff.as_secs(),
                    "compactor errored, restarting after backoff",
                );
                tokio::select! {
                    _ = tokio::time::sleep(restart_backoff) => {}
                    _ = cancel.cancelled() => return,
                }
            }
        }
    }
}

async fn run_once(
    shard_id: &ShardId,
    backend: &Backend,
    path_template: &str,
    compactor_options: Option<slatedb::config::CompactorOptions>,
    metrics: Option<&Arc<CompactorMetrics>>,
    cancel: &CancellationToken,
) -> Result<(), CompactorError> {
    let shard_label = shard_id.to_string();
    let shard_path = path_for_shard(path_template, shard_id);
    let resolved = resolve_object_store(backend, &shard_path)?;

    info!(
        shard = %shard_id,
        store_root = %resolved.root_path,
        canonical = %resolved.canonical_path,
        "opening compactor",
    );

    let canonical_path = resolved.canonical_path;
    let store = resolved.store;
    // Per-shard slatedb metrics recorder: passed to the CompactorBuilder so
    // slatedb populates it during `compactor.run()`, then translated into our
    // Prometheus registry by the poller task below.
    let recorder = metrics
        .is_some()
        .then(|| Arc::new(DefaultMetricsRecorder::new()));

    let mut builder = slatedb::CompactorBuilder::new(canonical_path, Arc::clone(&store))
        .with_merge_operator(silo::job_store_shard::counter_merge_operator());
    if let Some(opts) = compactor_options {
        builder = builder.with_options(opts);
    }
    if let Some(rec) = &recorder {
        builder = builder.with_metrics_recorder(
            Arc::clone(rec) as Arc<dyn slatedb_common::metrics::MetricsRecorder>
        );
    }
    let compactor = builder.build();

    if let Some(m) = metrics {
        m.record_compactor_started(&shard_label);
    }

    // Poller task: every SLATEDB_STAT_POLL_INTERVAL, snapshot the slatedb
    // recorder and translate into our Prometheus registry. Stops when the
    // worker is cancelled or the compactor exits.
    let poller = match (metrics, &recorder) {
        (Some(m), Some(rec)) => {
            let m = Arc::clone(m);
            let rec = Arc::clone(rec);
            let shard_label = shard_label.clone();
            let poller_cancel = cancel.clone();
            Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(SLATEDB_STAT_POLL_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            m.slatedb.update(&shard_label, &rec);
                        }
                        _ = poller_cancel.cancelled() => break,
                    }
                }
            }))
        }
        _ => None,
    };

    // Run slatedb's compactor inside a span carrying the shard so its log-bridged
    // events (e.g. "finished compaction" from slatedb::compactor_state) are tagged
    // with the shard. Note: slatedb spawns internal tokio tasks that don't
    // propagate the parent span, so logs emitted from those subtasks won't carry
    // the shard field.
    let run_span = info_span!("slatedb_compactor", shard = %shard_id);
    let mut run_task = tokio::spawn({
        let compactor = compactor.clone();
        async move { compactor.run().await }.instrument(run_span)
    });

    let result = tokio::select! {
        result = &mut run_task => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(CompactorError::Slatedb(e.to_string())),
            Err(join_err) => Err(CompactorError::Slatedb(format!("compactor task panic: {join_err}"))),
        },
        _ = cancel.cancelled() => {
            info!(shard = %shard_id, "shutdown signaled, stopping compactor");
            compactor
                .stop()
                .await
                .map_err(|e| CompactorError::Slatedb(e.to_string()))?;
            let _ = run_task.await;
            Ok(())
        }
    };

    // Stop the poller and flush one final snapshot so trailing counter
    // deltas (e.g. the last compaction's bytes_compacted) make it into
    // Prometheus before the recorder is dropped.
    if let Some(handle) = poller {
        handle.abort();
        let _ = handle.await;
    }
    if let (Some(m), Some(rec)) = (metrics, &recorder) {
        m.slatedb.update(&shard_label, rec);
    }

    result
}
