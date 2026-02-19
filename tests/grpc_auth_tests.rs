mod grpc_integration_helpers;

use std::net::SocketAddr;
use std::sync::Arc;

use grpc_integration_helpers::{create_test_factory, shutdown_server};
use silo::cluster_client::AuthInterceptor;
use silo::coordination::NoneCoordinator;
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::server::run_server;
use silo::settings::AppConfig;
use tokio::net::TcpListener;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

/// Helper: start a server with a given auth_token
async fn start_auth_server(
    auth_token: Option<String>,
) -> anyhow::Result<(
    SocketAddr,
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    Arc<silo::factory::ShardFactory>,
    tempfile::TempDir,
)> {
    let (factory, tmp) = create_test_factory().await?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let mut cfg = AppConfig::load(None).unwrap();
    cfg.server.auth_token = auth_token;

    let coordinator = Arc::new(
        NoneCoordinator::from_factory("test-node", format!("http://{}", addr), factory.clone())
            .await,
    );

    let server = tokio::spawn(run_server(
        listener,
        factory.clone(),
        coordinator,
        cfg,
        None,
        shutdown_rx,
    ));

    Ok((addr, shutdown_tx, server, factory, tmp))
}

#[silo::test(flavor = "multi_thread")]
async fn auth_unauthenticated_client_rejected() -> anyhow::Result<()> {
    let (addr, shutdown_tx, server, _factory, _tmp) =
        start_auth_server(Some("test-secret".to_string())).await?;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let mut client = SiloClient::new(channel);

    let result = client.get_cluster_info(GetClusterInfoRequest {}).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "unauthenticated client should be rejected"
    );

    shutdown_server(shutdown_tx, server).await?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn auth_wrong_token_rejected() -> anyhow::Result<()> {
    let (addr, shutdown_tx, server, _factory, _tmp) =
        start_auth_server(Some("test-secret".to_string())).await?;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let interceptor = AuthInterceptor::new(Some("wrong-token".to_string()));
    let mut client = SiloClient::with_interceptor(channel, interceptor);

    let result = client.get_cluster_info(GetClusterInfoRequest {}).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "client with wrong token should be rejected"
    );

    shutdown_server(shutdown_tx, server).await?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn auth_correct_token_succeeds() -> anyhow::Result<()> {
    let (addr, shutdown_tx, server, _factory, _tmp) =
        start_auth_server(Some("test-secret".to_string())).await?;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let interceptor = AuthInterceptor::new(Some("test-secret".to_string()));
    let mut client = SiloClient::with_interceptor(channel, interceptor);

    let result = client.get_cluster_info(GetClusterInfoRequest {}).await;

    assert!(
        result.is_ok(),
        "client with correct token should succeed: {:?}",
        result.err()
    );

    shutdown_server(shutdown_tx, server).await?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn auth_disabled_allows_unauthenticated() -> anyhow::Result<()> {
    let (addr, shutdown_tx, server, _factory, _tmp) = start_auth_server(None).await?;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let mut client = SiloClient::new(channel);

    let result = client.get_cluster_info(GetClusterInfoRequest {}).await;

    assert!(
        result.is_ok(),
        "unauthenticated client should work when auth is disabled: {:?}",
        result.err()
    );

    shutdown_server(shutdown_tx, server).await?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn auth_health_check_bypasses_auth() -> anyhow::Result<()> {
    let (addr, shutdown_tx, server, _factory, _tmp) =
        start_auth_server(Some("test-secret".to_string())).await?;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let mut health_client = HealthClient::new(channel);

    let resp = health_client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await?
        .into_inner();

    assert_eq!(
        resp.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
        "health check should bypass auth"
    );

    shutdown_server(shutdown_tx, server).await?;
    Ok(())
}
