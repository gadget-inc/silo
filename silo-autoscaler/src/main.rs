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

    /// Path to TLS certificate file for the webhook server.
    #[arg(long, default_value = "/certs/tls.crt")]
    tls_cert_path: String,

    /// Path to TLS private key file for the webhook server.
    #[arg(long, default_value = "/certs/tls.key")]
    tls_key_path: String,
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

    // Install the ring crypto provider for rustls (used by kube-rs and webhook TLS)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    info!("starting silo-autoscaler");

    let client = kube::Client::try_default().await?;

    // Spawn webhook server if enabled
    if !args.disable_webhook {
        let webhook_port = args.webhook_port;
        let tls_cert_path = args.tls_cert_path.clone();
        let tls_key_path = args.tls_key_path.clone();
        tokio::spawn(async move {
            let app = webhook::webhook_router();

            // Load TLS config
            let tls_config = load_tls_config(&tls_cert_path, &tls_key_path)
                .expect("failed to load TLS config");
            let tls_acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(tls_config));

            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", webhook_port))
                .await
                .expect("failed to bind webhook listener");
            info!(port = webhook_port, "webhook server listening (TLS)");

            loop {
                let (stream, addr) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to accept connection");
                        continue;
                    }
                };

                let tls_acceptor = tls_acceptor.clone();
                let app = app.clone();
                tokio::spawn(async move {
                    let tls_stream = match tls_acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::debug!(error = %e, addr = %addr, "TLS handshake failed");
                            return;
                        }
                    };

                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    if let Err(e) = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, service)
                    .await
                    {
                        tracing::debug!(error = %e, "connection error");
                    }
                });
            }
        });
    }

    // Run controller (blocks until shutdown)
    controller::run_controller(client).await?;

    Ok(())
}

fn load_tls_config(cert_path: &str, key_path: &str) -> anyhow::Result<rustls::ServerConfig> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut &key_pem[..])?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", key_path))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(config)
}
