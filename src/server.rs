use std::sync::Arc;

// use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::info;

use crate::factory::{CloseAllError, ShardFactory};
use crate::grpc::membership_service::MembershipServiceImpl;
use crate::grpc::silo_service::SiloService;
use crate::pb::silo_server::SiloServer;

/// Run the gRPC server and a periodic reaper task together until shutdown.
pub async fn run_grpc_with_reaper(
    listener: TcpListener,
    factory: Arc<ShardFactory>,
    mut shutdown: broadcast::Receiver<()>,
    my_node_id: u32,
    shard_count: u32,
    raft: Option<crate::membership::Raft>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Provide initial voters from shard_count if Raft metrics are not yet ready.
    let initial_voters: Vec<u64> = (1..=3).collect();
    let svc = SiloService::new(
        factory.clone(),
        my_node_id,
        shard_count,
        raft.clone(),
        initial_voters,
    );

    let server = SiloServer::new(svc);

    // If provided, mount membership gRPC alongside main service
    let raft_server = raft.map(|raft_inst| {
        let membership_svc = MembershipServiceImpl::new(raft_inst);
        crate::membershippb::membership_service_server::MembershipServiceServer::new(membership_svc)
    });

    // Periodic reaper that iterates all shards every second
    let (tick_tx, mut tick_rx) = broadcast::channel::<()>(1);
    let (ctrl_tx, mut ctrl_rx) = broadcast::channel::<()>(1);
    let reaper_factory = factory.clone();
    let reaper: JoinHandle<()> = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Iterate shards and reap
                    for shard in reaper_factory.instances().values() {
                        let _ = shard.reap_expired_leases().await;
                    }
                }
                _ = tick_rx.recv() => {
                    break;
                }
            }
        }
    });

    // Background shard map controller (disabled until OpenRaft is wired)
    let ctrl_handle: JoinHandle<()> = {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = interval.tick() => { /* No-op until OpenRaft is wired */ }
                    _ = ctrl_rx.recv() => { break; }
                }
            }
        })
    };

    let local_addr = listener.local_addr()?;
    // Log after successful bind (listener provided by main)
    tracing::info!(addr = %local_addr, "server started and listening");

    let incoming = TcpListenerStream::new(listener);

    // Serve with graceful shutdown
    let mut builder = tonic::transport::Server::builder().add_service(server);
    if let Some(raft_srv) = raft_server {
        builder = builder.add_service(raft_srv);
    }

    let serve = builder.serve_with_incoming_shutdown(incoming, async move {
        let _ = shutdown.recv().await;
        info!("graceful shutdown signal received");
        let _ = tick_tx.send(());
        let _ = ctrl_tx.send(());
    });

    serve.await?;
    info!("all connections drained, shutting down services");
    // After server has stopped accepting connections, close all shards
    match factory.close_all().await {
        Ok(()) => info!("closed all shards"),
        Err(CloseAllError { errors }) => {
            for (name, err) in errors {
                tracing::error!(shard = %name, error = %err, "failed to close shard");
            }
        }
    }
    reaper.await.ok();
    ctrl_handle.await.ok();
    Ok(())
}
