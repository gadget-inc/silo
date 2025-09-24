use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;

use silo::coordination::Coordination;
use silo::factory::ShardFactory;
use silo::server::run_grpc_with_reaper;
use silo::settings;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[clap(author = "Harry Brundage", version, about)]
/// Application CLI arguments
struct Args {
    /// whether to be verbose
    #[arg(short = 'v')]
    verbose: bool,

    /// path to a TOML config file
    #[arg(short = 'c', long = "config")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing via shared helper (Perfetto or OTLP based on env)
    silo::trace::init_from_env()?;

    // Load configuration
    let cfg = settings::AppConfig::load(args.config.as_deref())?;

    // Initialize coordination client (etcd). Fail fast if it cannot connect.
    let _coord = Coordination::connect(&cfg.coordination).await?;
    tracing::info!(endpoints = ?cfg.coordination.etcd_endpoints, "connected to etcd");

    let mut shard_factory = ShardFactory::new(cfg.database.clone());

    // Start gRPC server and scheduled reaper together
    let addr: SocketAddr = cfg.server.grpc_addr.parse()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Bind first so we can fail fast if the port is unavailable
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(err) => {
            error!(addr = %addr, error = %err, "failed to bind gRPC listener");
            return Err(anyhow::anyhow!(err));
        }
    };

    let _handle = shard_factory.open(0).await?;

    let factory = Arc::new(shard_factory);

    let server = tokio::spawn(run_grpc_with_reaper(listener, factory.clone(), shutdown_rx));

    // Wait for Ctrl+C, then signal shutdown and wait for server
    tokio::signal::ctrl_c().await?;
    let _ = shutdown_tx.send(());
    let _ = server.await?;
    // Ensure all spans are flushed before process exit
    silo::trace::shutdown();
    Ok(())
}
