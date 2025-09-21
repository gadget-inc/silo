use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::factory::ShardFactory;
use crate::membership::{LogStore, Network, Raft, StateMachineStore};
use crate::server::run_grpc_with_reaper;
use crate::settings::AppConfig;
use std::collections::BTreeMap;
use crate::grpc::membership_service::MembershipServiceImpl;
use crate::membershippb::membership_service_server::MembershipServiceServer;

/// Start a Silo node similarly to main.rs, given a config path.
/// Returns the join handles to allow tests to await or abort.
pub async fn start_from_config_path(path: &Path) -> anyhow::Result<NodeHandle> {
    let cfg = AppConfig::load(Some(path))?;

    let addr: SocketAddr = cfg.server.grpc_addr.parse()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let (graceful_tx, _graceful_rx) = tokio::sync::watch::channel(());

    let listener = tokio::net::TcpListener::bind(addr).await?;

    let mut shard_factory = ShardFactory::new(cfg.database.clone());
    let _handle = shard_factory.open(0).await?;
    let factory = Arc::new(shard_factory);

    let node_id = cfg.raft.as_ref().map(|c| c.node_id as u64).unwrap_or(0);
    let shard_count = 128u32;

    // Construct Raft instance using provided config when not in single-node mode
    let raft_instance = if !cfg.server.single_node {
        let raft_config = std::sync::Arc::new(
            cfg.raft
                .as_ref()
                .map(|r| r.openraft.clone())
                .unwrap_or_default()
                .validate()?,
        );
        let log_store = LogStore::default();
        let state_machine_store = Arc::new(StateMachineStore::default());
        let network = Network {};

        let raft = Raft::new(
            node_id,
            raft_config,
            network,
            log_store,
            state_machine_store.clone(),
        )
        .await?;

        // Initialize cluster membership if provided in config
        if let Some(raft_cfg) = cfg.raft.as_ref() {
            if !raft_cfg.initial_cluster.is_empty() {
                let mut nodes: BTreeMap<u64, crate::membershippb::Node> = BTreeMap::new();
                for n in &raft_cfg.initial_cluster {
                    nodes.insert(
                        n.id,
                        crate::membershippb::Node {
                            node_id: n.id,
                            rpc_addr: n.address.clone(),
                        },
                    );
                }
                let _ = raft.initialize(nodes).await;

                // Start membership RPC server on the raft listen address
                let listen = raft_cfg.listen_address.parse()?;
                let raft_clone = raft.clone();
                tokio::spawn(async move {
                    let svc = MembershipServiceImpl::new(raft_clone);
                    let _ = tonic::transport::Server::builder()
                        .add_service(MembershipServiceServer::new(svc))
                        .serve(listen)
                        .await;
                });
            }
        }
        Some(raft)
    } else {
        None
    };

    let server = tokio::spawn(run_grpc_with_reaper(
        listener,
        factory.clone(),
        shutdown_rx,
        node_id.try_into().unwrap(),
        shard_count,
        raft_instance,
    ));

    Ok(NodeHandle {
        server,
        shutdown: shutdown_tx,
        graceful: graceful_tx,
    })
}

pub struct NodeHandle {
    pub server:
        tokio::task::JoinHandle<anyhow::Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    pub shutdown: tokio::sync::broadcast::Sender<()>,
    pub graceful: tokio::sync::watch::Sender<()>,
}

impl NodeHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.graceful.send(());
        let _ = self.server.abort();
    }
}
