use std::path::PathBuf;

use clap::Parser;
use silo_compactor::config::{AppConfig, LogFormat};
use silo_compactor::coordinator::Coordinator;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "silo-compactor", about = "Standalone slatedb compactor for silo shards")]
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

    let coordinator = Coordinator::from_config(cfg).await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    if let Err(e) = coordinator.run(shutdown_rx).await {
        error!(error = %e, "coordinator exited with error");
    }
    signal_task.abort();
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
