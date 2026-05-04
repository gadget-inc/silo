use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use silo_compactor::config::{AppConfig, LogFormat};
use silo_compactor::coordinator::Coordinator;
use silo_compactor::metrics::{self, CompactorMetrics};
use tracing::{error, info};

#[derive(Parser)]
#[command(
    name = "silo-compactor",
    about = "Standalone slatedb compactor for silo shards"
)]
struct Args {
    #[arg(short = 'c', long, env = "SILO_COMPACTOR_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = AppConfig::load(&args.config)?;
    init_tracing(cfg.logging.format);

    info!(config = %args.config.display(), "starting silo-compactor");

    let metrics: Option<Arc<CompactorMetrics>> = if cfg.metrics.enabled {
        match metrics::init() {
            Ok(m) => Some(Arc::new(m)),
            Err(e) => {
                error!(error = %e, "failed to initialize metrics, continuing without metrics");
                None
            }
        }
    } else {
        None
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    // Fan out shutdown to the metrics HTTP server, which needs a
    // broadcast::Receiver<()> for axum's graceful_shutdown.
    let (metrics_shutdown_tx, _metrics_shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let metrics_handle = match metrics.as_ref() {
        Some(m) => match cfg.metrics.addr.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                let registry = Arc::clone(m.registry());
                let shutdown_rx = metrics_shutdown_tx.subscribe();
                info!(addr = %addr, "starting metrics endpoint");
                Some(tokio::spawn(async move {
                    if let Err(e) = silo::metrics::serve_registry(addr, registry, shutdown_rx).await
                    {
                        error!(error = %e, "metrics server error");
                    }
                }))
            }
            Err(e) => {
                error!(addr = %cfg.metrics.addr, error = %e, "invalid metrics addr; metrics endpoint disabled");
                None
            }
        },
        None => None,
    };

    let coordinator = Coordinator::from_config(cfg, metrics.clone()).await?;

    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = shutdown_tx.send(true);
        let _ = metrics_shutdown_tx.send(());
    });

    if let Err(e) = coordinator.run(shutdown_rx).await {
        error!(error = %e, "coordinator exited with error");
    }
    signal_task.abort();
    if let Some(h) = metrics_handle {
        h.abort();
    }
    Ok(())
}

fn init_tracing(format: LogFormat) {
    let env = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,silo_compactor=debug"));
    let builder = tracing_subscriber::fmt().with_env_filter(env);
    match format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
