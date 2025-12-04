use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

use silo::coordination::{create_coordinator, Coordinator};
use silo::factory::ShardFactory;
use silo::gubernator::{GubernatorClient, NullGubernatorClient, RateLimitClient};
use silo::server::run_grpc_with_reaper;
use silo::settings::{self, CoordinationBackend};
use silo::webui::run_webui;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[clap(author = "Harry Brundage", version, about)]
/// Application CLI arguments
struct Args {
    /// whether to be verbose
    #[arg(short = 'v', global = true)]
    verbose: bool,

    /// path to a TOML config file
    #[arg(short = 'c', long = "config", global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Validate a config file and exit
    ValidateConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Handle validate-config command
    if let Some(Command::ValidateConfig) = args.command {
        match settings::AppConfig::load(args.config.as_deref()) {
            Ok(_) => {
                println!("Config is valid");
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("Config validation failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Initialize tracing via shared helper (Perfetto or OTLP based on env)
    silo::trace::init_from_env()?;

    // Load configuration
    let cfg = settings::AppConfig::load(args.config.as_deref())?;

    // Initialize coordination based on configured backend
    let node_id = uuid::Uuid::new_v4().to_string();
    let coord_result = create_coordinator(
        &cfg.coordination,
        &node_id,
        &cfg.server.grpc_addr,
        1, // num_shards - single shard for now
    )
    .await;

    let (coordinator, _coord_handle): (
        Option<Arc<dyn Coordinator>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = match coord_result {
        Ok((coord, handle)) => {
            match cfg.coordination.backend {
                CoordinationBackend::None => {
                    tracing::info!("coordination: none (single-node mode)");
                }
                CoordinationBackend::Etcd => {
                    tracing::info!(endpoints = ?cfg.coordination.etcd_endpoints, "coordination: connected to etcd");
                }
                CoordinationBackend::K8s => {
                    tracing::info!(namespace = ?cfg.coordination.k8s_namespace, "coordination: connected to kubernetes");
                }
            }
            (Some(coord), handle)
        }
        Err(e) => {
            // In single-node mode, coordination errors are warnings not failures
            if cfg.coordination.backend == CoordinationBackend::None {
                tracing::warn!(error = %e, "coordination: failed but continuing in single-node mode");
                (None, None)
            } else {
                return Err(anyhow::anyhow!("coordination: {}", e));
            }
        }
    };

    // Create rate limiter based on configuration
    let rate_limiter: Arc<dyn RateLimitClient> = if let Some(guber_cfg) = cfg.gubernator.to_config()
    {
        let client = GubernatorClient::new(guber_cfg);
        client.connect().await?;
        tracing::info!(address = ?cfg.gubernator.address, "connected to Gubernator");
        client
    } else {
        NullGubernatorClient::new()
    };

    let mut shard_factory = ShardFactory::new(cfg.database.clone(), rate_limiter);

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

    // Start WebUI server if enabled
    let webui_handle = if cfg.webui.enabled {
        let webui_addr: SocketAddr = cfg.webui.addr.parse()?;
        let webui_shutdown = shutdown_tx.subscribe();
        let webui_factory = factory.clone();
        let webui_cfg = cfg.clone();
        let webui_coordinator = coordinator.clone();
        info!(grpc = %addr, webui = %webui_addr, "server started");
        Some(tokio::spawn(async move {
            if let Err(e) = run_webui(
                webui_addr,
                webui_factory,
                webui_coordinator,
                webui_cfg,
                webui_shutdown,
            )
            .await
            {
                error!(error = %e, "WebUI server error");
            }
        }))
    } else {
        info!(grpc = %addr, "server started");
        None
    };

    // Wait for Ctrl+C, then signal shutdown and wait for server
    tokio::signal::ctrl_c().await?;
    let _ = shutdown_tx.send(());
    let _ = server.await?;
    if let Some(handle) = webui_handle {
        let _ = handle.await;
    }
    // Ensure all spans are flushed before process exit
    silo::trace::shutdown();
    Ok(())
}
