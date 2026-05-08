use std::sync::Arc;
use std::time::Duration;

use slatedb_common::metrics::DefaultMetricsRecorder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::compaction_filter::CompletedJobCompactionFilterSupplier;
use crate::config::CompactionFilterConfig;
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
#[allow(clippy::too_many_arguments)]
pub fn spawn_worker(
    shard_id: ShardId,
    backend: Backend,
    path_template: Arc<String>,
    wal_backend: Option<Backend>,
    wal_path_template: Option<Arc<String>>,
    compactor_options: Arc<Option<slatedb::config::CompactorOptions>>,
    filter_config: Arc<CompactionFilterConfig>,
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
            wal_backend,
            wal_path_template,
            compactor_options,
            filter_config,
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

#[allow(clippy::too_many_arguments)]
async fn run_supervisor(
    shard_id: ShardId,
    backend: Backend,
    path_template: Arc<String>,
    wal_backend: Option<Backend>,
    wal_path_template: Option<Arc<String>>,
    compactor_options: Arc<Option<slatedb::config::CompactorOptions>>,
    filter_config: Arc<CompactionFilterConfig>,
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
            wal_backend.as_ref(),
            wal_path_template.as_deref().map(String::as_str),
            compactor_options.as_ref().clone(),
            filter_config.as_ref(),
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

#[allow(clippy::too_many_arguments)]
async fn run_once(
    shard_id: &ShardId,
    backend: &Backend,
    path_template: &str,
    wal_backend: Option<&Backend>,
    wal_path_template: Option<&str>,
    compactor_options: Option<slatedb::config::CompactorOptions>,
    filter_config: &CompactionFilterConfig,
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

    // Resolve the per-shard WAL store when configured. The standalone slatedb
    // compactor doesn't need this — only the per-job DbReader inside the
    // completed_jobs filter does, because slatedb's manifest validator
    // refuses a reader open whose split-WAL flag doesn't match what silo
    // wrote into the manifest.
    let wal_store = match (wal_backend, wal_path_template) {
        (Some(wb), Some(wpt)) => {
            let wal_shard_path = path_for_shard(wpt, shard_id);
            let resolved_wal = resolve_object_store(wb, &wal_shard_path)?;
            info!(
                shard = %shard_id,
                wal_root = %resolved_wal.root_path,
                "resolved wal store",
            );
            Some(resolved_wal.store)
        }
        _ => None,
    };

    let canonical_path = resolved.canonical_path;
    let store = resolved.store;
    // Per-shard slatedb metrics recorder: passed to the CompactorBuilder so
    // slatedb populates it during `compactor.run()`, then translated into our
    // Prometheus registry by the poller task below.
    let recorder = metrics
        .is_some()
        .then(|| Arc::new(DefaultMetricsRecorder::new()));

    let mut builder = slatedb::CompactorBuilder::new(canonical_path.clone(), Arc::clone(&store))
        .with_merge_operator(silo::job_store_shard::counter_merge_operator());
    if let Some(opts) = compactor_options {
        builder = builder.with_options(opts);
    }
    if let Some(rec) = &recorder {
        builder = builder.with_metrics_recorder(
            Arc::clone(rec) as Arc<dyn slatedb_common::metrics::MetricsRecorder>
        );
    }
    if let CompactionFilterConfig::CompletedJobs {
        retention_secs,
        expired_set_max_entries,
    } = filter_config
    {
        let mut supplier = CompletedJobCompactionFilterSupplier::new(
            Duration::from_secs(*retention_secs),
            object_store::path::Path::from(canonical_path.as_str()),
            Arc::clone(&store),
        )
        .with_wal_store(wal_store.clone());
        if let Some(cap) = expired_set_max_entries {
            supplier = supplier.with_max_expired_entries(*cap);
        }
        if let Some(m) = metrics {
            supplier = supplier.with_metrics(Arc::clone(m), shard_label.clone());
        }
        info!(
            shard = %shard_id,
            retention_secs = *retention_secs,
            expired_set_max_entries = ?expired_set_max_entries,
            "enabling completed_jobs compaction filter",
        );
        builder = builder.with_compaction_filter_supplier(Arc::new(supplier));
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
