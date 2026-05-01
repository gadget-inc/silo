#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use silo::coordination::NoneCoordinator;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::silo_client::SiloClient;
use silo::server::run_server;
use silo::settings::{AppConfig, Backend, DatabaseTemplate};
use silo::shard_range::ShardRange;
use tokio::net::TcpListener;

/// Helper to set up a test gRPC server with a factory and return the client and shutdown channel.
/// Returns (client, shutdown_tx, server_task, server_address)
pub async fn setup_test_server(
    factory: Arc<ShardFactory>,
    config: AppConfig,
) -> anyhow::Result<(
    SiloClient<tonic::transport::Channel>,
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    SocketAddr,
)> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Create a NoneCoordinator for single-node test mode, using factory's existing shards
    let coordinator = Arc::new(
        NoneCoordinator::from_factory("test-node", format!("http://{}", addr), factory.clone())
            .await,
    );

    let server = tokio::spawn(run_server(
        listener,
        factory.clone(),
        coordinator,
        config,
        None, // metrics
        shutdown_rx,
    ));

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let client = SiloClient::new(channel);

    Ok((client, shutdown_tx, server, addr))
}

use silo::shard_range::ShardId;

/// Predictable shard ID for testing - the zero UUID
pub const TEST_SHARD_ID: &str = "00000000-0000-0000-0000-000000000000";

/// Helper to create a shard factory with temp storage
/// Uses a predictable shard ID (TEST_SHARD_ID) for simplicity in tests
pub async fn create_test_factory() -> anyhow::Result<(Arc<ShardFactory>, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        concurrency_reconcile_interval_ms: 5000,
        concurrency_status_lookup_concurrency:
            silo::settings::DEFAULT_CONCURRENCY_STATUS_LOOKUP_CONCURRENCY,
        slatedb: None,
        memory_cache: None,
    };
    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(template, rate_limiter, None);
    // Use a predictable shard ID for testing
    let shard_id = ShardId::parse(TEST_SHARD_ID).expect("valid test shard ID");
    let _ = factory.open(&shard_id, &ShardRange::full()).await?;
    Ok((Arc::new(factory), tmp))
}

/// Helper to set up a multi-shard test server using NoneCoordinator.
///
/// Creates a temporary storage directory, a ShardFactory, and spawns a gRPC
/// server with the given number of initial shards. Returns the components
/// needed to interact with and shut down the server.
///
/// Callers that need a `SiloClient` can create one from the returned `SocketAddr`:
/// ```ignore
/// let endpoint = format!("http://{}", addr);
/// let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
/// let client = SiloClient::new(channel);
/// ```
pub async fn setup_multi_shard_server(
    initial_shard_count: u32,
    config: AppConfig,
) -> anyhow::Result<(
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    SocketAddr,
    Arc<ShardFactory>,
    tempfile::TempDir,
)> {
    let tmp = tempfile::tempdir()?;
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        concurrency_reconcile_interval_ms: 5000,
        concurrency_status_lookup_concurrency:
            silo::settings::DEFAULT_CONCURRENCY_STATUS_LOOKUP_CONCURRENCY,
        slatedb: None,
        memory_cache: None,
    };
    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = Arc::new(ShardFactory::new(template, rate_limiter, None));

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let coordinator = Arc::new(
        NoneCoordinator::new(
            "test-node",
            format!("http://{}", addr),
            initial_shard_count,
            factory.clone(),
            Vec::new(),
        )
        .await
        .unwrap(),
    );

    let server = tokio::spawn(run_server(
        listener,
        factory.clone(),
        coordinator,
        config,
        None,
        shutdown_rx,
    ));

    Ok((shutdown_tx, server, addr, factory, tmp))
}

/// Helper to gracefully shutdown the server
pub async fn shutdown_server(
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
) -> anyhow::Result<()> {
    let _ = shutdown_tx.send(());
    let join_result = server.await;
    match join_result {
        Ok(inner) => {
            if let Err(e) = inner {
                return Err(anyhow::anyhow!(e.to_string()));
            }
        }
        Err(e) => return Err(anyhow::anyhow!(e)),
    }
    Ok(())
}
