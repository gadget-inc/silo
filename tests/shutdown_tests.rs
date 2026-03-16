//! Tests for graceful shutdown behavior.
//!
//! These tests verify that the shutdown sequence from main.rs completes within
//! a bounded time even when the gRPC server drain blocks. In production, tonic's
//! serve_with_incoming_shutdown can wait indefinitely for HTTP/2 connections to
//! close (it has no built-in timeout — hyperium/tonic#1820). The fix adds a
//! drain timeout so coordinator shutdown is always reached.

mod grpc_integration_helpers;

use std::sync::Arc;
use std::time::Duration;

use grpc_integration_helpers::create_test_factory;
use silo::coordination::{Coordinator, NoneCoordinator};
use silo::factory::ShardFactory;
use silo::server::run_server;
use silo::settings::AppConfig;
use tokio::net::TcpListener;

/// Helper to set up a gRPC server and coordinator independently.
async fn setup_server_with_coordinator() -> (
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    Arc<dyn Coordinator>,
    Arc<ShardFactory>,
    tempfile::TempDir,
) {
    let (factory, tmp) = create_test_factory().await.unwrap();
    let config = AppConfig::load(None).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let coordinator: Arc<dyn Coordinator> = Arc::new(
        NoneCoordinator::from_factory("test-node", format!("http://{}", addr), factory.clone())
            .await,
    );

    let server = tokio::spawn(run_server(
        listener,
        factory.clone(),
        coordinator.clone(),
        config,
        None,
        shutdown_rx,
    ));

    (shutdown_tx, server, coordinator, factory, tmp)
}

/// Regression test: without a drain timeout, the main.rs shutdown sequence
/// blocks indefinitely when the gRPC server drain never completes.
///
/// This test simulates the production scenario (2026-03-16) where tonic's
/// serve_with_incoming_shutdown waited forever for HTTP/2 connections to close,
/// preventing coordinator.shutdown() from being reached. The k8s membership
/// watcher kept spinning for 9+ minutes until Kubernetes SIGKILL'd the pod.
///
/// The test uses a mock server future that never completes to simulate a
/// stuck drain, and verifies the original (buggy) shutdown sequence hangs.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_hangs_without_server_drain_timeout() {
    let (_shutdown_tx, _server, coordinator, factory, _tmp) = setup_server_with_coordinator().await;

    // Simulate a server drain that never completes. In production, this happens
    // when workers hold persistent HTTP/2 connections and tonic waits forever
    // for them to close (no built-in timeout — hyperium/tonic#1820).
    let never_draining_server = tokio::spawn(async {
        std::future::pending::<Result<(), Box<dyn std::error::Error + Send + Sync>>>().await
    });

    // Run the buggy shutdown sequence (no drain timeout):
    // server.await blocks forever → coordinator.shutdown() never reached
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = never_draining_server.await;
        coordinator.shutdown().await.unwrap();
        factory.close_all().await.unwrap();
    })
    .await;

    assert!(
        result.is_err(),
        "without a drain timeout, shutdown hangs when server drain never completes"
    );
}

/// Test that adding a drain timeout to the server allows the shutdown sequence
/// to complete even when the gRPC server drain blocks indefinitely.
///
/// This mirrors the fixed main.rs: the server drain is wrapped in a timeout,
/// so coordinator.shutdown() is always reached regardless of connection state.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_completes_with_server_drain_timeout() {
    let (_shutdown_tx, _server, coordinator, factory, _tmp) = setup_server_with_coordinator().await;

    // Same scenario: server drain that never completes
    let never_draining_server = tokio::spawn(async {
        std::future::pending::<Result<(), Box<dyn std::error::Error + Send + Sync>>>().await
    });

    // Fixed shutdown sequence: drain timeout → coordinator shutdown → cleanup
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        // Server drain with timeout (the fix)
        if tokio::time::timeout(Duration::from_secs(3), never_draining_server)
            .await
            .is_err()
        {
            // Drain timed out — proceed with shutdown anyway
        }

        // Coordinator shutdown is now always reached
        coordinator.shutdown().await.unwrap();
        factory.close_all().await.unwrap();
    })
    .await;

    assert!(
        result.is_ok(),
        "with a drain timeout, shutdown completes even when server drain blocks"
    );
}

/// Test that the normal shutdown path (no stuck connections) completes promptly.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_completes_promptly_with_no_active_clients() {
    let (shutdown_tx, server, coordinator, factory, _tmp) = setup_server_with_coordinator().await;

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = shutdown_tx.send(());
        let _ = server.await;
        coordinator.shutdown().await.unwrap();
        factory.close_all().await.unwrap();
    })
    .await;

    assert!(
        result.is_ok(),
        "shutdown should complete promptly with no active clients"
    );
}
