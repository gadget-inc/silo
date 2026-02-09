//! K8s permanent lease scenarios: Tests crash recovery with permanent shard leases.
//!
//! These scenarios test the permanent shard lease behavior introduced to prevent
//! WAL data loss on crash. Unlike TTL-based leases that auto-expire, permanent
//! leases persist through crashes and must be explicitly reclaimed or force-released.
//!
//! Two variants:
//!
//! **Reclaim**: Crash a node, restart with same node_id. The restarted node
//! reclaims its existing leases via `reclaim_and_open_shards()`, triggering
//! WAL recovery. Verifies leases persist through crash and shards become
//! functional after reclaim.
//!
//! **Force-release**: Crash a node, then simulate operator intervention by
//! force-releasing the crashed node's shard leases. Remaining nodes acquire
//! the orphaned shards. Verifies no split-brain and continued job processing.
//!
//! Invariants verified:
//! - **leasePermanence**: Shard leases persist through crash (no auto-release)
//! - **noSplitBrain**: A shard is owned by at most one node at any time
//! - **validTransitions**: Only valid status transitions occur
//! - **jobCompleteness**: Sufficient jobs reach terminal state after recovery

use crate::helpers::{
    EnqueueRequest, HashMap, InvariantTracker, LeaseTasksRequest, ReportOutcomeRequest,
    RetryPolicy, SerializedBytes, create_turmoil_client, dst_turmoilfs_database_template,
    get_seed, report_outcome_request, run_scenario_impl, serialized_bytes, turmoil,
};
use crate::mock_k8s::{MockK8sBackend, MockK8sState};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::cluster_client::ClientConfig;
use silo::coordination::{Coordinator, K8sCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::Task;
use silo::server::run_server_with_incoming;
use silo::settings::{AppConfig, GubernatorSettings, LoggingConfig, WebUiConfig};
use silo::shard_range::ShardId;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;
use turmoil::net::TcpListener;

const NUM_SHARDS: u32 = 4;
const NUM_NODES: u32 = 3;
const BASE_PORT: u16 = 9960;
const CLUSTER_PREFIX: &str = "silo-perm-lease";
const NAMESPACE: &str = "default";
const STORAGE_ROOT: &str = "/data/silo-perm-lease-shards";
const LEASE_DURATION_SECS: i64 = 10;
const NUM_JOBS: u32 = 15;
const NUM_WORKERS: u32 = 2;

/// Node lifecycle state communicated via watch channel
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeState {
    /// Node is waiting to be activated
    #[allow(dead_code)]
    Inactive,
    /// Node should start its coordinator and server
    Active,
    /// Node should simulate a crash (drop coordinator without lease release)
    Crash,
    /// Node should shut down gracefully
    Shutdown,
}

fn make_shard_factory() -> Arc<ShardFactory> {
    let storage_root = std::path::Path::new(STORAGE_ROOT);
    Arc::new(ShardFactory::new(
        dst_turmoilfs_database_template(storage_root),
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Result from setting up a node server
struct NodeServerHandle {
    coordinator: Arc<K8sCoordinator<MockK8sBackend>>,
    _shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

/// Set up a server on a node with K8s coordination
async fn setup_node_server(
    port: u16,
    node_id: String,
    k8s_state: Arc<MockK8sState>,
    lease_duration_secs: i64,
) -> turmoil::Result<NodeServerHandle> {
    let backend = MockK8sBackend::new(k8s_state, NAMESPACE);
    let factory = make_shard_factory();

    let (coordinator, _handle) = K8sCoordinator::start_with_backend(
        backend,
        silo::coordination::K8sCoordinatorConfig {
            namespace: NAMESPACE.to_string(),
            cluster_prefix: CLUSTER_PREFIX.to_string(),
            node_id: node_id.clone(),
            grpc_addr: format!("http://{}:{}", node_id, port),
            initial_shard_count: NUM_SHARDS,
            lease_duration_secs,
            placement_rings: Vec::new(),
        },
        Arc::clone(&factory),
    )
    .await
    .map_err(|e| e.to_string())?;

    let coordinator = Arc::new(coordinator);

    // Wait for initial convergence
    let converged = coordinator.wait_converged(Duration::from_secs(30)).await;
    if converged {
        tracing::info!(node_id = %node_id, "initial convergence achieved");
    } else {
        tracing::warn!(node_id = %node_id, "convergence timeout");
    }

    // Start gRPC server
    let cfg = AppConfig {
        server: silo::settings::ServerConfig {
            grpc_addr: format!("0.0.0.0:{}", port),
            dev_mode: false,
        },
        coordination: silo::settings::CoordinationConfig::default(),
        tenancy: silo::settings::TenancyConfig { enabled: true },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        logging: LoggingConfig::default(),
        metrics: silo::settings::MetricsConfig::default(),
        database: dst_turmoilfs_database_template(std::path::Path::new(STORAGE_ROOT)),
    };

    let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), port);
    let listener = TcpListener::bind(addr).await.map_err(|e| e.to_string())?;

    struct Accepted(turmoil::net::TcpStream);
    impl tonic::transport::server::Connected for Accepted {
        type ConnectInfo = tonic::transport::server::TcpConnectInfo;
        fn connect_info(&self) -> Self::ConnectInfo {
            Self::ConnectInfo {
                local_addr: self.0.local_addr().ok(),
                remote_addr: self.0.peer_addr().ok(),
            }
        }
    }
    impl AsyncRead for Accepted {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for Accepted {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    let incoming = async_stream::stream! {
        loop {
            match listener.accept().await {
                Ok((s, _a)) => yield Ok::<_, std::io::Error>(Accepted(s)),
                Err(e) => {
                    tracing::trace!(error = %e, "accept error");
                    break;
                }
            }
        }
    };

    let (shutdown_tx, rx) = tokio::sync::broadcast::channel::<()>(1);

    let coordinator_for_server = Arc::clone(&coordinator) as Arc<dyn Coordinator>;
    tokio::spawn(async move {
        if let Err(e) =
            run_server_with_incoming(incoming, factory, coordinator_for_server, cfg, None, rx).await
        {
            tracing::trace!(error = %e, "server error");
        }
    });

    Ok(NodeServerHandle {
        coordinator,
        _shutdown_tx: shutdown_tx,
    })
}

/// Verify that no shard is owned by multiple nodes (checks ALL lease holders,
/// not just "active" ones — important for crash scenarios where a crashed node
/// may still appear as a holder).
async fn verify_no_split_brain_all_holders(
    k8s_state: &MockK8sState,
) -> bool {
    let holders = k8s_state
        .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
        .await;

    let mut shard_owners: std::collections::HashMap<ShardId, Vec<String>> =
        std::collections::HashMap::new();

    for (shard_id, holder) in &holders {
        shard_owners
            .entry(*shard_id)
            .or_default()
            .push(holder.clone());
    }

    for (shard_id, owners) in &shard_owners {
        if owners.len() > 1 {
            tracing::error!(
                shard_id = %shard_id,
                owners = ?owners,
                "SPLIT-BRAIN: shard has multiple owners"
            );
            return false;
        }
    }

    true
}

/// Shared scenario setup for both reclaim and force-release variants.
/// The `controller_fn` parameter customizes the crash/recovery behavior.
/// It receives shared state and returns a future that drives the crash/recovery timeline.
fn run_scenario<C, F>(name: &str, seed: u64, controller_fn: C)
where
    C: FnOnce(
            Arc<MockK8sState>,
            Arc<std::sync::Mutex<Vec<watch::Sender<NodeState>>>>,
            Arc<InvariantTracker>,
            Arc<AtomicBool>,
            u64,
        ) -> F
        + Send
        + 'static,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    run_scenario_impl(name, seed, 180, |sim| {
        tracing::info!(
            num_nodes = NUM_NODES,
            num_shards = NUM_SHARDS,
            num_jobs = NUM_JOBS,
            num_workers = NUM_WORKERS,
            "permanent_lease_scenario_config"
        );

        let scenario_done = Arc::new(AtomicBool::new(false));
        let membership_changes_done = Arc::new(AtomicBool::new(false));
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));

        let k8s_state = MockK8sState::new();
        let tracker = Arc::new(InvariantTracker::new());

        let client_config = ClientConfig::for_dst();

        // Create activation channels for each node
        let mut node_state_senders_vec: Vec<watch::Sender<NodeState>> = Vec::new();
        let mut node_state_receivers: Vec<watch::Receiver<NodeState>> = Vec::new();

        for _node_num in 0..NUM_NODES {
            let (tx, rx) = watch::channel(NodeState::Active);
            node_state_senders_vec.push(tx);
            node_state_receivers.push(rx);
        }

        let node_state_senders: Arc<std::sync::Mutex<Vec<watch::Sender<NodeState>>>> =
            Arc::new(std::sync::Mutex::new(node_state_senders_vec));

        // Create all node hosts — each runs a state machine handling Active, Crash, and restart
        for node_num in 0..NUM_NODES {
            let node_id = format!("node-{}", node_num);
            let k8s_state = k8s_state.clone();
            let host_name: &'static str = Box::leak(format!("node{}", node_num).into_boxed_str());
            let state_rx = node_state_receivers[node_num as usize].clone();
            let startup_delay_ms = (node_num as u64) * 150;
            let port = BASE_PORT + node_num as u16;

            sim.host(host_name, {
                let node_id = node_id.clone();
                move || {
                    let node_id = node_id.clone();
                    let k8s_state = k8s_state.clone();
                    let mut state_rx = state_rx.clone();
                    async move {
                        // Stagger startup
                        if startup_delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(startup_delay_ms)).await;
                        }

                        // Outer loop: handle crash + restart cycles
                        loop {
                            let state = *state_rx.borrow();
                            match state {
                                NodeState::Inactive => {
                                    // Wait for activation
                                    if state_rx.changed().await.is_err() {
                                        return Ok(());
                                    }
                                    continue;
                                }
                                NodeState::Shutdown => {
                                    tracing::info!(node_id = %node_id, "shutdown signal received");
                                    return Ok(());
                                }
                                NodeState::Crash => {
                                    // Should not start in Crash state; wait for next state
                                    if state_rx.changed().await.is_err() {
                                        return Ok(());
                                    }
                                    continue;
                                }
                                NodeState::Active => {
                                    // Fall through to start the server
                                }
                            }

                            tracing::info!(node_id = %node_id, "starting server");
                            let handle = setup_node_server(
                                port,
                                node_id.clone(),
                                k8s_state.clone(),
                                LEASE_DURATION_SECS,
                            )
                            .await?;

                            // Run until crash or shutdown signal
                            let exit_state = loop {
                                tokio::select! {
                                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                                        let owned = handle.coordinator.owned_shards().await;
                                        tracing::trace!(
                                            node_id = %node_id,
                                            owned = ?owned,
                                            count = owned.len(),
                                            "node_status"
                                        );
                                    }
                                    result = state_rx.changed() => {
                                        if result.is_err() {
                                            break NodeState::Shutdown;
                                        }
                                        let state = *state_rx.borrow();
                                        match state {
                                            NodeState::Crash => {
                                                tracing::info!(node_id = %node_id, "CRASH signal received");
                                                // Crash: stop guards without releasing leases
                                                handle.coordinator.crash().await;
                                                break NodeState::Crash;
                                            }
                                            NodeState::Shutdown => {
                                                tracing::info!(node_id = %node_id, "shutdown signal received");
                                                if let Err(e) = handle.coordinator.shutdown().await {
                                                    tracing::warn!(node_id = %node_id, error = %e, "shutdown error");
                                                }
                                                break NodeState::Shutdown;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            };

                            // Drop the handle to close the server and its broadcast channel
                            drop(handle);

                            match exit_state {
                                NodeState::Shutdown => return Ok(()),
                                NodeState::Crash => {
                                    tracing::info!(node_id = %node_id, "node crashed, waiting for restart signal");
                                    // Wait for next Active signal (restart) or Shutdown
                                    loop {
                                        if state_rx.changed().await.is_err() {
                                            return Ok(());
                                        }
                                        let state = *state_rx.borrow();
                                        match state {
                                            NodeState::Active => {
                                                tracing::info!(node_id = %node_id, "restarting after crash");
                                                break;
                                            }
                                            NodeState::Shutdown => {
                                                return Ok(());
                                            }
                                            _ => continue,
                                        }
                                    }
                                    // Loop back to create a new server (restart)
                                    continue;
                                }
                                _ => return Ok(()),
                            }
                        }
                    }
                }
            });
        }

        // Producer: Enqueues jobs to the cluster
        let producer_enqueued = Arc::clone(&total_enqueued);
        let producer_k8s_state = Arc::clone(&k8s_state);
        let producer_config = client_config.clone();
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));

            let shard_ids = loop {
                match producer_k8s_state.get_shard_ids(NAMESPACE, CLUSTER_PREFIX).await {
                    Ok(Some(ids)) if !ids.is_empty() => break ids,
                    Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                    Err(e) => {
                        tracing::trace!(error = %e, "producer shard discovery failed, retrying");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            };

            let mut current_client: Option<(u32, silo::pb::silo_client::SiloClient<_>)> = None;
            let mut consecutive_failures = 0u32;

            for i in 0..NUM_JOBS {
                let job_id = format!("perm-{}", i);
                let priority = rng.random_range(1..100);
                let delay_ms = rng.random_range(50..200);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                let shard_idx = (i as usize) % shard_ids.len();
                let shard_id = &shard_ids[shard_idx];
                let tenant = format!("shard-{}", shard_idx);

                let owner = match producer_k8s_state
                    .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, shard_id)
                    .await
                {
                    Ok(owner) => owner,
                    Err(e) => {
                        tracing::trace!(error = %e, "producer owner lookup failed");
                        continue;
                    }
                };

                let target_node = owner
                    .as_ref()
                    .and_then(|o| o.strip_prefix("node-"))
                    .and_then(|n| n.parse::<u32>().ok());

                let client = match (&mut current_client, target_node) {
                    (Some((node, client)), Some(target)) if *node == target => client,
                    (_, Some(target)) => {
                        let uri = format!("http://node{}:{}", target, BASE_PORT + target as u16);
                        match create_turmoil_client(&uri, &producer_config).await {
                            Ok(new_client) => {
                                current_client = Some((target, new_client));
                                &mut current_client.as_mut().unwrap().1
                            }
                            Err(e) => {
                                tracing::trace!(error = %e, "failed to create client");
                                continue;
                            }
                        }
                    }
                    _ => continue,
                };

                let retry_policy = Some(RetryPolicy {
                    retry_count: 5,
                    initial_interval_ms: 200,
                    max_interval_ms: 2000,
                    randomize_interval: true,
                    backoff_factor: 1.5,
                });

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: shard_id.to_string(),
                        id: job_id.clone(),
                        priority,
                        start_at_ms: 0,
                        retry_policy,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                rmp_serde::to_vec(&serde_json::json!({
                                    "scenario": "permanent_lease",
                                    "idx": i
                                }))
                                .unwrap(),
                            )),
                        }),
                        limits: vec![],
                        tenant: Some(tenant),
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        producer_enqueued.fetch_add(1, Ordering::SeqCst);
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                        consecutive_failures += 1;
                        if consecutive_failures >= 2 {
                            current_client = None;
                            consecutive_failures = 0;
                        }
                    }
                }
            }

            tracing::trace!(
                enqueued = producer_enqueued.load(Ordering::SeqCst),
                "producer_done"
            );
            Ok(())
        });

        // Workers
        for worker_num in 0..NUM_WORKERS {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("perm-worker-{}", worker_num);
            let worker_completed = Arc::clone(&total_completed);
            let worker_done_flag = Arc::clone(&scenario_done);
            let worker_config = client_config.clone();
            let worker_k8s_state = Arc::clone(&k8s_state);

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());

            sim.client(client_name, async move {
                let start_delay = 2000 + (worker_num as u64 * 100);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);

                let shard_ids = loop {
                    match worker_k8s_state
                        .get_shard_ids(NAMESPACE, CLUSTER_PREFIX)
                        .await
                    {
                        Ok(Some(ids)) if !ids.is_empty() => break ids,
                        Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                        Err(e) => {
                            tracing::trace!(worker = %worker_id, error = %e, "shard discovery failed");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                };

                let mut current_client: Option<(u32, silo::pb::silo_client::SiloClient<_>)> =
                    None;
                let mut completed = 0u32;
                let mut processing: Vec<(Task, ShardId)> = Vec::new();

                while !worker_done_flag.load(Ordering::SeqCst) {
                    let shard_idx = rng.random_range(0..shard_ids.len());
                    let shard_id = shard_ids[shard_idx];

                    let owner = match worker_k8s_state
                        .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, &shard_id)
                        .await
                    {
                        Ok(owner) => owner,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let target_node = owner
                        .as_ref()
                        .and_then(|o| o.strip_prefix("node-"))
                        .and_then(|n| n.parse::<u32>().ok());

                    let client = match (&mut current_client, target_node) {
                        (Some((node, client)), Some(target)) if *node == target => client,
                        (_, Some(target)) => {
                            let uri =
                                format!("http://node{}:{}", target, BASE_PORT + target as u16);
                            match create_turmoil_client(&uri, &worker_config).await {
                                Ok(new_client) => {
                                    current_client = Some((target, new_client));
                                    &mut current_client.as_mut().unwrap().1
                                }
                                Err(_) => {
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    continue;
                                }
                            }
                        }
                        _ => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let lease_result = client
                        .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                            shard: Some(shard_id.to_string()),
                            worker_id: worker_id.clone(),
                            max_tasks: rng.random_range(2..=4),
                            task_group: "default".to_string(),
                        }))
                        .await;

                    match lease_result {
                        Ok(resp) => {
                            for task in resp.into_inner().tasks {
                                processing.push((task, shard_id));
                            }
                        }
                        Err(_) => {
                            current_client = None;
                        }
                    }

                    if !processing.is_empty() {
                        let num_to_complete = rng.random_range(1..=processing.len());
                        let mut indices_to_remove: HashSet<usize> = HashSet::new();

                        for i in 0..num_to_complete {
                            if i >= processing.len() {
                                break;
                            }

                            let (task, task_shard_id) = &processing[i];

                            let task_owner = match worker_k8s_state
                                .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, task_shard_id)
                                .await
                            {
                                Ok(owner) => owner,
                                Err(_) => continue,
                            };

                            let task_target = task_owner
                                .as_ref()
                                .and_then(|o| o.strip_prefix("node-"))
                                .and_then(|n| n.parse::<u32>().ok());

                            let report_client = match (&mut current_client, task_target) {
                                (Some((node, client)), Some(target)) if *node == target => client,
                                (_, Some(target)) => {
                                    let uri = format!(
                                        "http://node{}:{}",
                                        target,
                                        BASE_PORT + target as u16
                                    );
                                    match create_turmoil_client(&uri, &worker_config).await {
                                        Ok(new_client) => {
                                            current_client = Some((target, new_client));
                                            &mut current_client.as_mut().unwrap().1
                                        }
                                        Err(_) => continue,
                                    }
                                }
                                _ => continue,
                            };

                            let process_time = rng.random_range(20..100);
                            tokio::time::sleep(Duration::from_millis(process_time)).await;

                            let outcome =
                                report_outcome_request::Outcome::Success(SerializedBytes {
                                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                                        rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                                    )),
                                });

                            match report_client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: task_shard_id.to_string(),
                                    task_id: task.id.clone(),
                                    outcome: Some(outcome),
                                }))
                                .await
                            {
                                Ok(_) => {
                                    completed += 1;
                                    worker_completed.fetch_add(1, Ordering::SeqCst);
                                    indices_to_remove.insert(i);
                                }
                                Err(_) => {
                                    indices_to_remove.insert(i);
                                }
                            }
                        }

                        let mut indices: Vec<_> = indices_to_remove.into_iter().collect();
                        indices.sort_by(|a, b| b.cmp(a));
                        for idx in indices {
                            processing.remove(idx);
                        }
                    }

                    let delay = rng.random_range(50..200);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }

                tracing::trace!(worker = %worker_id, completed = completed, "worker_done");
                Ok(())
            });
        }

        // Controller
        let controller_k8s_state = k8s_state.clone();
        let controller_node_senders = Arc::clone(&node_state_senders);
        let controller_tracker = Arc::clone(&tracker);
        let controller_membership_done = Arc::clone(&membership_changes_done);
        sim.client("controller", async move {
            controller_fn(
                controller_k8s_state,
                controller_node_senders,
                controller_tracker,
                controller_membership_done,
                seed,
            )
            .await;
            Ok(())
        });

        // Verifier
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_scenario_done = Arc::clone(&scenario_done);
        let verifier_membership_done = Arc::clone(&membership_changes_done);
        let verifier_node_senders = Arc::clone(&node_state_senders);

        sim.client("verifier", async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // Wait for membership changes to complete
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if verifier_membership_done.load(Ordering::SeqCst) {
                    tracing::info!("membership changes done, entering convergence phase");
                    break;
                }
            }

            // Wait for convergence
            let convergence_timeout_secs = 60;
            let convergence_start = turmoil::sim_elapsed().unwrap_or_default();
            let mut last_completed = 0u32;
            let mut stall_count = 0u32;

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);

                tracing::trace!(enqueued, completed, "convergence_check");

                if completed >= enqueued && enqueued > 0 {
                    tracing::info!(enqueued, completed, "convergence achieved");
                    break;
                }

                if completed == last_completed {
                    stall_count += 1;
                } else {
                    stall_count = 0;
                    last_completed = completed;
                }

                if stall_count >= 10 && completed > 0 {
                    tracing::warn!(enqueued, completed, stall_count, "progress stalled");
                }

                let elapsed = turmoil::sim_elapsed().unwrap_or_default();
                let convergence_duration = elapsed.saturating_sub(convergence_start);
                if convergence_duration.as_secs() >= convergence_timeout_secs {
                    tracing::warn!(enqueued, completed, "convergence timeout reached");
                    break;
                }
            }

            // Signal workers to stop
            verifier_scenario_done.store(true, Ordering::SeqCst);

            // Signal all nodes to shut down
            {
                let senders = verifier_node_senders.lock().unwrap();
                for (i, sender) in senders.iter().enumerate() {
                    if let Err(e) = sender.send(NodeState::Shutdown) {
                        tracing::trace!(node = i, error = %e, "shutdown signal failed");
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;

            // Final verification
            verifier_tracker.process_and_validate();
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            let transition_violations = verifier_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                for v in &transition_violations {
                    tracing::warn!(violation = %v, "transition_violation");
                }
                panic!(
                    "INVARIANT VIOLATION: {} invalid state transitions detected",
                    transition_violations.len()
                );
            }

            let enqueued = verifier_enqueued.load(Ordering::SeqCst);
            let completed = verifier_completed.load(Ordering::SeqCst);
            let shard_ownership_violations = verifier_tracker.shards.has_violations();

            tracing::info!(
                enqueued,
                completed,
                shard_ownership_violations,
                "final_verification"
            );

            verifier_tracker.shards.verify_no_split_brain();

            assert!(
                completed > 0 || enqueued == 0,
                "No progress made: completed={}, enqueued={}",
                completed,
                enqueued
            );

            if enqueued > 0 {
                let completion_rate = (completed as f64) / (enqueued as f64);
                let acceptable_rate = 0.5; // Lower threshold for crash scenarios
                let min_completed = 3;

                tracing::info!(
                    enqueued,
                    completed,
                    completion_rate = format!("{:.1}%", completion_rate * 100.0),
                    "job_completion_stats"
                );

                if completion_rate < acceptable_rate && completed < min_completed {
                    panic!(
                        "Too many jobs lost: completed={}, enqueued={}, rate={:.1}%",
                        completed, enqueued, completion_rate * 100.0,
                    );
                }
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}

/// Reclaim scenario: Crash node-0, verify leases persist, restart with same node_id.
pub fn run_reclaim() {
    let seed = get_seed();
    run_scenario("k8s_permanent_lease_reclaim", seed, move |k8s_state, node_senders, _tracker, membership_done, _seed| async move {
        // Wait for initial convergence
        tracing::info!("waiting for initial convergence");
        tokio::time::sleep(Duration::from_secs(8)).await;

        // Record which shards node-0 owns before crash
        let holders_before = k8s_state
            .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
            .await;
        let node0_shards: Vec<ShardId> = holders_before
            .iter()
            .filter(|(_, holder)| *holder == "node-0")
            .map(|(sid, _)| *sid)
            .collect();
        tracing::info!(
            node0_shard_count = node0_shards.len(),
            node0_shards = ?node0_shards,
            "pre-crash shard ownership"
        );

        // Crash node-0
        tracing::info!("CRASHING node-0");
        {
            let senders = node_senders.lock().unwrap();
            let _ = senders[0].send(NodeState::Crash);
        }

        // Wait for crash to take effect
        tokio::time::sleep(Duration::from_secs(2)).await;

        // PERMANENCE CHECK: Verify leases are still held by node-0
        let holders_after_crash = k8s_state
            .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
            .await;
        let node0_shards_after: Vec<ShardId> = holders_after_crash
            .iter()
            .filter(|(_, holder)| *holder == "node-0")
            .map(|(sid, _)| *sid)
            .collect();
        tracing::info!(
            node0_shard_count_after = node0_shards_after.len(),
            node0_shards_after = ?node0_shards_after,
            "post-crash shard ownership (should match pre-crash)"
        );
        assert_eq!(
            node0_shards.len(),
            node0_shards_after.len(),
            "Permanent leases should persist through crash! Before: {:?}, After: {:?}",
            node0_shards,
            node0_shards_after
        );

        // Restart node-0 with same node_id
        tracing::info!("RESTARTING node-0");
        {
            let senders = node_senders.lock().unwrap();
            let _ = senders[0].send(NodeState::Active);
        }

        // Wait for reclaim and re-convergence
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Verify no split-brain after reclaim
        assert!(
            verify_no_split_brain_all_holders(&k8s_state).await,
            "Split-brain detected after reclaim"
        );

        // Let work continue
        tokio::time::sleep(Duration::from_secs(10)).await;

        membership_done.store(true, Ordering::SeqCst);
        tracing::info!("reclaim controller done");
    });
}

/// Force-release scenario: Crash node-0, force-release its leases, let other nodes take over.
pub fn run_force_release() {
    let seed = get_seed();
    run_scenario("k8s_permanent_lease_force_release", seed, move |k8s_state, node_senders, tracker, membership_done, _seed| async move {
        // Wait for initial convergence
        tracing::info!("waiting for initial convergence");
        tokio::time::sleep(Duration::from_secs(8)).await;

        // Record which shards node-0 owns before crash
        let holders_before = k8s_state
            .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
            .await;
        let node0_shards: Vec<ShardId> = holders_before
            .iter()
            .filter(|(_, holder)| *holder == "node-0")
            .map(|(sid, _)| *sid)
            .collect();
        tracing::info!(
            node0_shard_count = node0_shards.len(),
            node0_shards = ?node0_shards,
            "pre-crash shard ownership"
        );

        // Crash node-0
        tracing::info!("CRASHING node-0");
        {
            let senders = node_senders.lock().unwrap();
            let _ = senders[0].send(NodeState::Crash);
        }

        // Wait for crash to take effect
        tokio::time::sleep(Duration::from_secs(2)).await;

        // PERMANENCE CHECK: Verify leases are still held by node-0
        let holders_after_crash = k8s_state
            .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
            .await;
        let node0_shards_after: Vec<ShardId> = holders_after_crash
            .iter()
            .filter(|(_, holder)| *holder == "node-0")
            .map(|(sid, _)| *sid)
            .collect();
        tracing::info!(
            node0_shard_count_after = node0_shards_after.len(),
            "post-crash shard ownership (should match pre-crash)"
        );
        assert_eq!(
            node0_shards.len(),
            node0_shards_after.len(),
            "Permanent leases should persist through crash!"
        );

        // Update tracker: node-0 crashed, clear its ownership records
        // so subsequent acquisitions by other nodes don't trigger false split-brain
        tracker.shards.node_crashed("node-0");

        // Delete node-0's membership lease (simulates K8s TTL expiry)
        let member_lease_name = format!("{}-member-node-0", CLUSTER_PREFIX);
        let _ = k8s_state
            .delete_lease(NAMESPACE, &member_lease_name)
            .await;
        tracing::info!("deleted node-0 membership lease");

        // Wait a bit for membership change to propagate
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Force-release each shard lease held by node-0
        for shard_id in &node0_shards {
            if let Err(e) = k8s_state
                .force_release_shard_lease(NAMESPACE, CLUSTER_PREFIX, shard_id)
                .await
            {
                tracing::warn!(shard_id = %shard_id, error = %e, "force-release failed");
            }
        }
        tracing::info!(
            count = node0_shards.len(),
            "force-released node-0's shard leases"
        );

        // Wait for remaining nodes to acquire orphaned shards
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Verify no split-brain after force-release
        assert!(
            verify_no_split_brain_all_holders(&k8s_state).await,
            "Split-brain detected after force-release"
        );

        // Let work continue
        tokio::time::sleep(Duration::from_secs(10)).await;

        membership_done.store(true, Ordering::SeqCst);
        tracing::info!("force-release controller done");
    });
}
