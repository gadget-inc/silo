use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::compaction_filter::CompletedJobCompactionFilterSupplier;
use crate::config::CompactionFilterConfig;
use crate::error::CompactorError;
use crate::shard_map::ShardId;
use crate::storage::{Backend, path_for_shard, resolve_object_store};

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
    filter_config: Arc<CompactionFilterConfig>,
    restart_backoff: Duration,
) -> WorkerHandle {
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let join = tokio::spawn(async move {
        run_supervisor(
            shard_id,
            backend,
            path_template,
            compactor_options,
            filter_config,
            restart_backoff,
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
    filter_config: Arc<CompactionFilterConfig>,
    restart_backoff: Duration,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match run_once(
            &shard_id,
            &backend,
            &path_template,
            compactor_options.as_ref().clone(),
            filter_config.as_ref(),
            &cancel,
        )
        .await
        {
            Ok(()) => {
                info!(shard = %shard_id, "compactor stopped cleanly");
                return;
            }
            Err(e) => {
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
    filter_config: &CompactionFilterConfig,
    cancel: &CancellationToken,
) -> Result<(), CompactorError> {
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
    let mut builder = slatedb::CompactorBuilder::new(canonical_path.clone(), Arc::clone(&store))
        .with_merge_operator(silo::job_store_shard::counter_merge_operator());
    if let Some(opts) = compactor_options {
        builder = builder.with_options(opts);
    }
    if let CompactionFilterConfig::CompletedJobs { retention_secs } = filter_config {
        let supplier = CompletedJobCompactionFilterSupplier::new(
            Duration::from_secs(*retention_secs),
            object_store::path::Path::from(canonical_path.as_str()),
            Arc::clone(&store),
        );
        info!(
            shard = %shard_id,
            retention_secs = *retention_secs,
            "enabling completed_jobs compaction filter",
        );
        builder = builder.with_compaction_filter_supplier(Arc::new(supplier));
    }
    let compactor = builder.build();

    let mut run_task = tokio::spawn({
        let compactor = compactor.clone();
        async move { compactor.run().await }
    });

    tokio::select! {
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
    }
}
