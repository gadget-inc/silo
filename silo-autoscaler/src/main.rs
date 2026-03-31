use clap::{Parser, Subcommand};
use tracing::info;

#[derive(Parser)]
#[command(name = "silo-autoscaler", about = "Kubernetes controller for safe Silo StatefulSet scaling")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
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
        let crd = serde_yaml::to_string(&silo_autoscaler::crd::crd_definition())?;
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

    // Run controller (blocks until shutdown)
    silo_autoscaler::controller::run_controller(client).await?;

    Ok(())
}
