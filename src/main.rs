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

    // Load configuration first (before tracing, so we can configure log format)
    let cfg = match settings::AppConfig::load(args.config.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Config error: {}", e);
            std::process::exit(1);
        }
    };

    // Handle validate-config command
    if let Some(Command::ValidateConfig) = args.command {
        println!("Config is valid");
        std::process::exit(0);
    }

    // Initialize tracing with configured log format
    silo::trace::init(cfg.logging.format)?;

    // Create rate limiter based on configuration
    let rate_limiter: Arc<dyn RateLimitClient> = if let Some(guber_cfg) = cfg.gubernator.to_config()
    {
        let client = GubernatorClient::new(guber_cfg);
        client.connect().await?;
        client
    } else {
        NullGubernatorClient::new()
    };

    // Create factory first - coordinator will own a reference and manage shard lifecycle
    let factory = Arc::new(ShardFactory::new(cfg.database.clone(), rate_limiter));

    // Initialize coordination - coordinator will open/close shards as ownership changes
    let node_id = uuid::Uuid::new_v4().to_string();
    // Use advertised_grpc_addr if set, otherwise fall back to the bind address.
    // This allows separating the bind address (e.g., 0.0.0.0:50051) from the
    // address other nodes should use to connect (e.g., pod-ip:50051).
    let advertised_grpc_addr = cfg
        .coordination
        .advertised_grpc_addr
        .as_ref()
        .unwrap_or(&cfg.server.grpc_addr);
    let coord_result = create_coordinator(
        &cfg.coordination,
        &node_id,
        advertised_grpc_addr,
        cfg.coordination.num_shards,
        factory.clone(),
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

    let server = tokio::spawn(run_grpc_with_reaper(
        listener,
        factory.clone(),
        coordinator.clone(),
        cfg.clone(),
        shutdown_rx,
    ));

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

    // Wait for shutdown signal (SIGINT or SIGTERM)
    let shutdown_signal = async {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => info!("received SIGINT, shutting down"),
                _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
            }
        }

        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            info!("received SIGINT, shutting down");
        }
    };

    shutdown_signal.await;
    let _ = shutdown_tx.send(());
    let _ = server.await?;
    if let Some(handle) = webui_handle {
        let _ = handle.await;
    }

    // Close all shards gracefully before exit
    if let Err(e) = factory.close_all().await {
        error!(error = %e, "failed to close some shards during shutdown");
    }

    // Ensure all spans are flushed before process exit
    silo::trace::shutdown();
    Ok(())
}
