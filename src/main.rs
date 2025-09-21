use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;

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

    // Initialize tracing subscriber to log to stdout. Respect RUST_LOG if set, else default to info.
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_level(true)
        .compact()
        .init();

    // Load configuration
    let cfg = settings::AppConfig::load(args.config.as_deref())?;

    // Initialize shard factory with the template, defer opening shards until after server bind
    let mut shard_factory = ShardFactory::new(cfg.database.clone());

    // Start gRPC server and 1s reaper together
    let addr: SocketAddr = cfg.server.grpc_addr.parse()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let (graceful_tx, _graceful_rx) = tokio::sync::watch::channel(());

    // Bind first so we can fail fast if the port is unavailable
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(err) => {
            error!(addr = %addr, error = %err, "failed to bind gRPC listener");
            return Err(anyhow::anyhow!(err));
        }
    };

    // Now that the port is bound, open shards using the single template
    let _handle = shard_factory.open(0).await?;

    let factory = Arc::new(shard_factory);

    // Spawn Silo gRPC server task with pre-bound listener
    let my_node_id = cfg.raft.as_ref().map(|c| c.node_id as u32).unwrap_or(0);
    let shard_count = 128u32;
    let server = tokio::spawn(run_grpc_with_reaper(
        listener,
        factory.clone(),
        shutdown_rx,
        my_node_id,
        shard_count,
        None,
    ));

    // Wait for Ctrl+C, then signal shutdown and wait for servers
    tokio::signal::ctrl_c().await?;
    let _ = shutdown_tx.send(());
    drop(graceful_tx);
    let _ = server.await?;
    Ok(())
}
