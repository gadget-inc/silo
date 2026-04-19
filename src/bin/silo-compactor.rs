//! Standalone silo compactor.
//!
//! Opens the same object store as a `silo` server but runs ONLY the SlateDB
//! compactor — it does not serve gRPC, does not own coordinator leases, and
//! does not touch any in-memory silo state. Used by the compaction A/B test
//! harness so the workload-generating writer can run with
//! `database.compaction.disable = true` while this process does the actual
//! compaction work, letting us measure compaction IO / filter behavior in
//! isolation.
//!
//! Example:
//! ```sh
//! silo-compactor -c example_configs/compaction-harness-compactor.toml \
//!     --shard 01HZ… --mode loop
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use slatedb::CompactorBuilder;
use slatedb::admin::AdminBuilder;
use slatedb::compactor::{CompactionSpec, SourceId};
use slatedb_common::metrics::DefaultMetricsRecorder;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use silo::compaction_filters;
use silo::factory::ShardFactory;
use silo::metrics::{self, Metrics};
use silo::settings::AppConfig;

#[derive(Debug, Copy, Clone, ValueEnum)]
enum Mode {
    /// Submit a single full compaction, wait until it completes (sorted_run_count <= 1),
    /// and exit. Useful for measuring a single compaction window end-to-end.
    Once,
    /// Run the SlateDB compactor in the foreground until SIGINT/SIGTERM. This
    /// is the mode to use when running the compactor alongside a live writer.
    Loop,
}

#[derive(Parser, Debug)]
#[clap(author, version, about = "Standalone SlateDB compactor for silo")]
struct Args {
    /// Path to a silo TOML config file (same schema as `silo`).
    #[arg(short = 'c', long = "config")]
    config: PathBuf,

    /// Shard identifier (directory name under the database template path).
    /// For the 1-shard harness this is the shard UUID that the writer's
    /// coordinator created.
    #[arg(long = "shard")]
    shard: String,

    /// How to run: once (single full compaction and exit) or loop (background
    /// compactor until SIGINT/SIGTERM).
    #[arg(long = "mode", value_enum, default_value_t = Mode::Loop)]
    mode: Mode,

    /// Polling interval for `--mode once` while waiting for the compaction to
    /// drain. Ignored in loop mode.
    #[arg(long = "poll-interval-ms", default_value_t = 500)]
    poll_interval_ms: u64,

    /// Maximum time to wait for a single compaction to complete in
    /// `--mode once` before giving up with an error.
    #[arg(long = "once-timeout-secs", default_value_t = 3600)]
    once_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cfg = AppConfig::load(Some(&args.config))
        .map_err(|e| anyhow::anyhow!("config error: {e}"))?;

    silo::trace::init(cfg.logging.format)?;

    // Initialize Prometheus metrics + serve on cfg.metrics.addr. This is the
    // only side of the harness that sees the compactor-side slatedb stats
    // (BYTES_COMPACTED, LAST_COMPACTION_TS_SEC, RUNNING_COMPACTIONS) — the
    // writer's compactor is disabled, so its /metrics endpoint never emits
    // those families.
    let metrics_init: Option<Metrics> = if cfg.metrics.enabled {
        match metrics::init() {
            Ok(m) => Some(m),
            Err(e) => {
                error!(error = %e, "failed to initialize metrics, continuing without");
                None
            }
        }
    } else {
        None
    };
    let (metrics_shutdown_tx, _) = broadcast::channel::<()>(1);
    let metrics_server_handle = if let (true, Some(m)) = (cfg.metrics.enabled, metrics_init.clone())
    {
        let addr: SocketAddr = cfg.metrics.addr.parse()?;
        let rx = metrics_shutdown_tx.subscribe();
        info!(addr = %addr, "silo-compactor metrics server starting");
        Some(tokio::spawn(async move {
            if let Err(e) = metrics::run_metrics_server(addr, m, rx).await {
                error!(error = %e, "metrics server error");
            }
        }))
    } else {
        None
    };

    let template = &cfg.database;
    let (resolved, db_path) =
        ShardFactory::resolve_at_root(&template.backend, &template.path, &args.shard)
            .map_err(|e| anyhow::anyhow!("resolve object store: {e}"))?;

    info!(
        shard = %args.shard,
        db_path = %db_path,
        root = %resolved.root_path,
        mode = ?args.mode,
        "silo-compactor starting",
    );

    // Build a CompactorOptions that honors silo's scheduler config. We reuse
    // `apply_compaction_config` against a throwaway Settings so the translation
    // logic stays in one place.
    let mut probe_settings = template
        .slatedb
        .clone()
        .unwrap_or_default();
    // Force `disable = false` here — this process IS the compactor.
    let mut compactor_config = template.compaction.clone();
    compactor_config.disable = false;
    compaction_filters::apply_compaction_config(
        &mut probe_settings,
        &compactor_config,
        &args.shard,
    );
    let compactor_options = probe_settings
        .compactor_options
        .clone()
        .expect("apply_compaction_config must leave compactor_options set when disable=false");

    let supplier = compaction_filters::build_supplier(&template.compaction, &args.shard);

    // Build the Compactor directly so we can configure options + merge
    // operator + filter supplier. Admin::run_compactor can't do all three at
    // once in slatedb 0.12 (it only passes the filter supplier through).
    let slatedb_recorder = Arc::new(DefaultMetricsRecorder::new());
    let mut compactor_builder =
        CompactorBuilder::new(db_path.clone(), Arc::clone(&resolved.store))
            .with_options(compactor_options)
            .with_merge_operator(silo::job_store_shard::counter_merge_operator())
            .with_metrics_recorder(slatedb_recorder.clone());
    if let Some(supplier) = supplier {
        compactor_builder = compactor_builder.with_compaction_filter_supplier(supplier);
    }
    let compactor = compactor_builder.build();

    // Pump slatedb stats from the recorder into the Prometheus exporter on a
    // 1s interval. Silo's Metrics::update_slatedb_stats handles the delta
    // math for monotonic counters.
    let slatedb_poll_handle = if let Some(m) = metrics_init.clone() {
        let shard_label = args.shard.clone();
        let recorder = slatedb_recorder.clone();
        let mut rx = metrics_shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = rx.recv() => break,
                    _ = ticker.tick() => {
                        m.update_slatedb_stats(&shard_label, &recorder);
                    }
                }
            }
        }))
    } else {
        None
    };

    // Admin is only used for read_compactor_state_view + submit_compaction,
    // which do not depend on compactor options or merge operator.
    let admin = AdminBuilder::new(db_path.clone(), Arc::clone(&resolved.store)).build();

    let result = match args.mode {
        Mode::Loop => run_loop_mode(compactor).await,
        Mode::Once => {
            run_once_mode(
                compactor,
                &admin,
                Duration::from_millis(args.poll_interval_ms),
                Duration::from_secs(args.once_timeout_secs),
            )
            .await
        }
    };

    // One last stats pump so the final snapshot reflects the last compaction
    // even if shutdown beats the 1s tick.
    if let Some(m) = metrics_init.as_ref() {
        m.update_slatedb_stats(&args.shard, &slatedb_recorder);
    }
    let _ = metrics_shutdown_tx.send(());
    if let Some(h) = slatedb_poll_handle {
        let _ = h.await;
    }
    if let Some(h) = metrics_server_handle {
        let _ = h.await;
    }

    result
}

async fn run_loop_mode(compactor: slatedb::compactor::Compactor) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    spawn_signal_handler(cancel.clone());
    info!("running SlateDB compactor until SIGINT/SIGTERM");

    let run_compactor = compactor.clone();
    let run_task = tokio::spawn(async move { run_compactor.run().await });

    cancel.cancelled().await;
    info!("cancellation requested, stopping compactor");
    if let Err(e) = compactor.stop().await {
        warn!(error = %e, "compactor stop returned error");
    }
    match run_task.await {
        Ok(Ok(())) => info!("compactor exited cleanly"),
        Ok(Err(e)) => return Err(anyhow::anyhow!("compactor run exited: {e}")),
        Err(e) => return Err(anyhow::anyhow!("compactor task join error: {e}")),
    }
    silo::trace::shutdown();
    Ok(())
}

async fn run_once_mode(
    compactor: slatedb::compactor::Compactor,
    admin: &slatedb::admin::Admin,
    poll_interval: Duration,
    overall_timeout: Duration,
) -> anyhow::Result<()> {
    let run_compactor = compactor.clone();
    let run_task = tokio::spawn(async move { run_compactor.run().await });
    // Let the compactor initialize before we submit.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let spec = build_full_compaction_spec(admin).await?;
    match spec {
        None => info!("nothing to compact (no L0 SSTs or sorted runs)"),
        Some(spec) => {
            admin
                .submit_compaction(spec)
                .await
                .map_err(|e| anyhow::anyhow!("submit_compaction: {e}"))?;
            info!("submitted full compaction; polling until sorted_run_count <= 1");
            poll_until_compacted(admin, poll_interval, overall_timeout).await?;
        }
    }

    if let Err(e) = compactor.stop().await {
        warn!(error = %e, "compactor stop returned error");
    }
    match run_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "background compactor task exited with error"),
        Err(e) => warn!(error = %e, "background compactor task join error"),
    }

    silo::trace::shutdown();
    Ok(())
}

async fn build_full_compaction_spec(
    admin: &slatedb::admin::Admin,
) -> anyhow::Result<Option<CompactionSpec>> {
    let state = admin
        .read_compactor_state_view()
        .await
        .map_err(|e| anyhow::anyhow!("read_compactor_state_view: {e}"))?;
    let manifest = state.manifest();
    let sources: Vec<SourceId> = manifest
        .l0
        .iter()
        .map(|sst| SourceId::SstView(sst.sst.id.unwrap_compacted_id()))
        .chain(
            manifest
                .compacted
                .iter()
                .map(|sr| SourceId::SortedRun(sr.id)),
        )
        .collect();
    if sources.is_empty() {
        return Ok(None);
    }
    let destination = manifest.compacted.iter().map(|sr| sr.id).min().unwrap_or(0);
    Ok(Some(CompactionSpec::new(sources, destination)))
}

async fn poll_until_compacted(
    admin: &slatedb::admin::Admin,
    poll_interval: Duration,
    overall_timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + overall_timeout;
    loop {
        let state = admin
            .read_compactor_state_view()
            .await
            .map_err(|e| anyhow::anyhow!("read_compactor_state_view: {e}"))?;
        let manifest = state.manifest();
        let sorted_run_count = manifest.compacted.len();
        let l0_count = manifest.l0.len();
        info!(
            sorted_runs = sorted_run_count,
            l0 = l0_count,
            "compaction progress"
        );
        if sorted_run_count <= 1 && l0_count == 0 {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timed out waiting for compaction to drain (sorted_runs={sorted_run_count}, l0={l0_count})"
            ));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn spawn_signal_handler(cancel: CancellationToken) {
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "failed to register SIGTERM handler");
                    return;
                }
            };
            tokio::select! {
                _ = ctrl_c => info!("received SIGINT, stopping compactor"),
                _ = sigterm.recv() => info!("received SIGTERM, stopping compactor"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
            info!("received SIGINT, stopping compactor");
        }
        cancel.cancel();
    });
}
