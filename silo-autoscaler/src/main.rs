mod controller;
mod crd;
mod error;
mod orphan;
mod webhook;

use clap::{Parser, Subcommand};
use tracing::info;

#[derive(Parser)]
#[command(name = "silo-autoscaler", about = "Kubernetes controller for safe Silo StatefulSet scaling")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Port for the admission webhook server.
    #[arg(long, default_value = "8443")]
    webhook_port: u16,

    /// Disable the admission webhook server.
    #[arg(long, default_value_t = false)]
    disable_webhook: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Print the CRD YAML to stdout for installation.
    Crd,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Handle CRD generation subcommand
    if let Some(Command::Crd) = args.command {
        let crd = serde_yaml::to_string(&crd::crd_definition())?;
        println!("{}", crd);
        return Ok(());
    }

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,silo_autoscaler=debug")),
        )
        .json()
        .init();

    info!("starting silo-autoscaler");

    let client = kube::Client::try_default().await?;

    // Spawn webhook server if enabled
    if !args.disable_webhook {
        let webhook_port = args.webhook_port;
        tokio::spawn(async move {
            let app = webhook::webhook_router();
            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", webhook_port))
                .await
                .expect("failed to bind webhook listener");
            info!(port = webhook_port, "webhook server listening");
            axum::serve(listener, app)
                .await
                .expect("webhook server failed");
        });
    }

    // Run controller (blocks until shutdown)
    controller::run_controller(client).await?;

    Ok(())
}
