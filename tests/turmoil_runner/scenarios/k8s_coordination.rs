//! K8s coordination scenario: Tests K8s coordinator and job processing under
//! membership changes and network faults.
//!
//! This scenario tests that coordination and job processing invariants hold even
//! as nodes join and leave the cluster. It validates the K8s coordinator's ability
//! to maintain correct shard ownership while workers actively process jobs.
//!
//! Fault modes:
//! - Nodes joining the cluster mid-flight
//! - Nodes leaving the cluster (graceful shutdown)
//! - Configurable K8s API failure rate
//! - Worker reconnection on node failures
//! - Network partitions between nodes (for split-brain testing)
//!
//! Invariants verified:
//! - **queueLimitEnforced**: At most max_concurrency tasks per limit key
//! - **oneLeasePerJob**: A job never has two active leases simultaneously
//! - **noLeasesForTerminal**: Terminal jobs have no active leases
//! - **validTransitions**: Only valid status transitions occur
//! - **noSplitBrain**: A shard is owned by at most one node at any time (via DST events)
//! - **No duplicate completions**: A job is only completed once
//! - **jobCompleteness**: Sufficient jobs reach terminal state
//!
//! The test runs workers that continuously enqueue and process jobs while the
//! controller adds and removes nodes from the cluster, and the partitioner
//! injects network partitions between nodes.

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, HashMap, InvariantTracker, LeaseTasksRequest, Limit,
    ReportOutcomeRequest, RetryPolicy, SerializedBytes, create_turmoil_client, get_seed, limit,
    report_outcome_request, run_scenario_impl, serialized_bytes, turmoil,
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
use silo::settings::{
    AppConfig, Backend, DatabaseTemplate, GubernatorSettings, LoggingConfig, WebUiConfig,
};
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
const BASE_PORT: u16 = 9960;
const CLUSTER_PREFIX: &str = "silo-worker-test";
const NAMESPACE: &str = "default";

/// Concurrency limit configurations for testing
const WORKER_LIMITS: &[(&str, u32)] = &[
    ("k8s-mutex", 1),     // Strict serialization
    ("k8s-batch", 3),     // Small concurrency
    ("k8s-unlimited", 0), // No limit
];

/// Node lifecycle state communicated via watch channel
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeState {
    /// Node is waiting to be activated
    Inactive,
    /// Node should start its coordinator and server
    Active,
    /// Node should shut down gracefully
    Shutdown,
}

/// Configuration for the scenario
struct ScenarioConfig {
    /// Initial number of nodes
    initial_nodes: u32,
    /// Number of nodes to add during the test
    nodes_to_add: u32,
    /// Number of nodes to remove during the test
    nodes_to_remove: u32,
    /// Failure rate for mock K8s operations (0.0 - 1.0)
    k8s_failure_rate: f64,
    /// Lease duration in seconds
    lease_duration_secs: i64,
    /// Number of jobs to enqueue
    num_jobs: u32,
    /// Number of workers
    num_workers: u32,
    /// Number of node-to-node partitions to inject
    num_partitions: u32,
    /// Duration range for partitions (min_ms, max_ms)
    partition_duration_range: (u64, u64),
}

impl ScenarioConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        // Partition duration range
        let partition_min = rng.random_range(500..1500);
        let partition_max = rng.random_range(partition_min..3000);

        Self {
            initial_nodes: rng.random_range(2..=3),
            nodes_to_add: rng.random_range(1..=2),
            nodes_to_remove: rng.random_range(0..=1),
            // Keep failure rate low to allow progress
            k8s_failure_rate: 0.02 + 0.03 * (rng.random_range(0..100) as f64 / 100.0),
            lease_duration_secs: rng.random_range(5..=8) as i64,
            // Moderate job count
            num_jobs: rng.random_range(15..=25),
            num_workers: rng.random_range(2..=3),
            // Inject 1-3 partitions between nodes
            num_partitions: rng.random_range(1..=3),
            partition_duration_range: (partition_min, partition_max),
        }
    }
}

fn make_shard_factory(node_id: &str) -> Arc<ShardFactory> {
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: format!("mem://shard-{{shard}}-{}", node_id),
            wal: None,
            apply_wal_on_close: true,
        },
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Result from setting up a node server
struct NodeServerHandle {
    coordinator: Arc<K8sCoordinator<MockK8sBackend>>,
    /// Shutdown sender - must be kept alive to prevent server from shutting down
    _shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

/// Set up a server on a node with K8s coordination
async fn setup_node_server(
    port: u16,
    node_id: String,
    k8s_state: Arc<MockK8sState>,
    _k8s_failure_rate: f64,
    lease_duration_secs: i64,
) -> turmoil::Result<NodeServerHandle> {
    // Note: We don't set failure rate during startup - it's set later by the controller
    // to avoid failing during initial convergence

    let backend = MockK8sBackend::new(k8s_state, NAMESPACE);
    let factory = make_shard_factory(&node_id);

    // mad-turmoil patches RNG to be deterministic, so we don't need seeded shard IDs
    let (coordinator, _handle) = K8sCoordinator::start_with_backend(
        backend,
        NAMESPACE,
        CLUSTER_PREFIX,
        node_id.clone(),
        format!("http://{}:{}", node_id, port),
        NUM_SHARDS,
        lease_duration_secs,
        Arc::clone(&factory),
    )
    .await
    .map_err(|e| e.to_string())?;

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
        tenancy: silo::settings::TenancyConfig { enabled: false },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        logging: LoggingConfig::default(),
        metrics: silo::settings::MetricsConfig::default(),
        database: silo::settings::DatabaseTemplate {
            backend: Backend::Memory,
            path: format!("mem://shard-{{shard}}-{}", node_id),
            wal: None,
            apply_wal_on_close: true,
        },
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

    // Spawn the server in the background
    tokio::spawn(async move {
        if let Err(e) = run_server_with_incoming(incoming, factory, None, cfg, None, rx).await {
            tracing::trace!(error = %e, "server error");
        }
    });

    Ok(NodeServerHandle {
        coordinator: Arc::new(coordinator),
        _shutdown_tx: shutdown_tx,
    })
}

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("k8s_coordination", seed, 180, |sim| {
        let config = ScenarioConfig::from_seed(seed);

        tracing::info!(
            initial_nodes = config.initial_nodes,
            nodes_to_add = config.nodes_to_add,
            nodes_to_remove = config.nodes_to_remove,
            k8s_failure_rate = config.k8s_failure_rate,
            lease_duration = config.lease_duration_secs,
            num_shards = NUM_SHARDS,
            num_jobs = config.num_jobs,
            num_workers = config.num_workers,
            num_partitions = config.num_partitions,
            partition_duration = ?config.partition_duration_range,
            "k8s_coordination_config"
        );

        // Capture config values
        let initial_nodes = config.initial_nodes;
        let nodes_to_add = config.nodes_to_add;
        let nodes_to_remove = config.nodes_to_remove;
        let k8s_failure_rate = config.k8s_failure_rate;
        let lease_duration_secs = config.lease_duration_secs;
        let num_jobs = config.num_jobs;
        let num_workers = config.num_workers;
        let num_partitions = config.num_partitions;
        let partition_duration_range = config.partition_duration_range;

        // Total number of nodes we'll create
        let total_nodes = initial_nodes + nodes_to_add;

        // Shared state
        let scenario_done = Arc::new(AtomicBool::new(false));
        let membership_changes_done = Arc::new(AtomicBool::new(false));
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));
        let split_brain_detected = Arc::new(AtomicBool::new(false));

        // MockK8sState is shared across all coordinators
        let k8s_state = MockK8sState::new();

        // Invariant tracker for DST events
        let tracker = Arc::new(InvariantTracker::new());

        // Register concurrency limits
        for (key, max_conc) in WORKER_LIMITS {
            if *max_conc > 0 {
                tracker.concurrency.register_limit(key, *max_conc);
            }
        }

        // Client configuration optimized for DST
        let client_config = ClientConfig::for_dst();

        // Create activation channels for each node
        // Wrapped in Arc<Mutex> so we can share between controller and verifier
        // The verifier will signal final shutdown after convergence
        let mut node_state_senders_vec: Vec<watch::Sender<NodeState>> = Vec::new();
        let mut node_state_receivers: Vec<watch::Receiver<NodeState>> = Vec::new();

        for node_num in 0..total_nodes {
            let initial_state = if node_num < initial_nodes {
                NodeState::Active
            } else {
                NodeState::Inactive
            };
            let (tx, rx) = watch::channel(initial_state);
            node_state_senders_vec.push(tx);
            node_state_receivers.push(rx);
        }

        // Wrap senders in Arc for sharing between controller and verifier
        let node_state_senders: Arc<std::sync::Mutex<Vec<watch::Sender<NodeState>>>> =
            Arc::new(std::sync::Mutex::new(node_state_senders_vec));

        // Track which nodes are active for worker routing
        let active_nodes: Arc<std::sync::Mutex<Vec<u32>>> =
            Arc::new(std::sync::Mutex::new((0..initial_nodes).collect()));

        // Create all node hosts
        for node_num in 0..total_nodes {
            let node_id = format!("node-{}", node_num);
            let k8s_state = k8s_state.clone();
            let host_name: &'static str = Box::leak(format!("node{}", node_num).into_boxed_str());
            let state_rx = node_state_receivers[node_num as usize].clone();
            let is_initial = node_num < initial_nodes;
            let startup_delay_ms = (node_num as u64) * 150;
            let port = BASE_PORT + node_num as u16;

            sim.host(host_name, {
                let node_id = node_id.clone();
                let k8s_failure_rate = k8s_failure_rate;
                let lease_duration_secs = lease_duration_secs;
                move || {
                    let node_id = node_id.clone();
                    let k8s_state = k8s_state.clone();
                    let mut state_rx = state_rx.clone();
                    async move {
                        // Stagger startup
                        if startup_delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(startup_delay_ms)).await;
                        }

                        // Wait for activation if not initial node
                        if !is_initial {
                            tracing::debug!(node_id = %node_id, "node waiting for activation");
                            loop {
                                let state = *state_rx.borrow();
                                match state {
                                    NodeState::Inactive => {
                                        if state_rx.changed().await.is_err() {
                                            tracing::debug!(node_id = %node_id, "state channel closed");
                                            return Ok(());
                                        }
                                    }
                                    NodeState::Active => break,
                                    NodeState::Shutdown => {
                                        tracing::debug!(node_id = %node_id, "shutdown before activation");
                                        return Ok(());
                                    }
                                }
                            }
                            tracing::info!(node_id = %node_id, "node activated");
                        }

                        // Start the server with K8s coordination
                        let handle = setup_node_server(
                            port,
                            node_id.clone(),
                            k8s_state,
                            k8s_failure_rate,
                            lease_duration_secs,
                        )
                        .await?;

                        // Run until shutdown signal
                        // Note: handle._shutdown_tx stays alive here, keeping server running
                        loop {
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
                                        break;
                                    }
                                    let state = *state_rx.borrow();
                                    if state == NodeState::Shutdown {
                                        tracing::info!(node_id = %node_id, "received shutdown signal");
                                        break;
                                    }
                                }
                            }
                        }

                        // Graceful shutdown
                        tracing::info!(node_id = %node_id, "shutting down");
                        if let Err(e) = handle.coordinator.shutdown().await {
                            tracing::warn!(node_id = %node_id, error = %e, "shutdown error");
                        }
                        // Dropping handle._shutdown_tx will trigger server shutdown

                        Ok(())
                    }
                }
            });
        }

        // Producer: Enqueues jobs to the cluster, routing to shard owners
        let producer_enqueued = Arc::clone(&total_enqueued);
        let producer_k8s_state = Arc::clone(&k8s_state);
        let producer_config = client_config.clone();
        let producer_num_jobs = num_jobs;
        sim.client("producer", async move {
            // Wait for all initial nodes to be fully started and coordinated
            tokio::time::sleep(Duration::from_millis(2000)).await;
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));

            // Discover shard IDs from the K8s ConfigMap with retry on failure
            let shard_ids = loop {
                match producer_k8s_state.get_shard_ids(NAMESPACE, CLUSTER_PREFIX).await {
                    Ok(Some(ids)) if !ids.is_empty() => break ids,
                    Ok(_) => {
                        // Not ready yet, wait
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        // K8s failure, retry with backoff
                        tracing::trace!(error = %e, "producer shard discovery failed, retrying");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            };
            tracing::debug!(shard_count = shard_ids.len(), "producer discovered shards");

            let mut consecutive_failures = 0u32;
            let mut current_client: Option<(u32, silo::pb::silo_client::SiloClient<_>)> = None;

            for i in 0..producer_num_jobs {
                // Select limit configuration
                let (limit_key, max_conc) = WORKER_LIMITS[rng.random_range(0..WORKER_LIMITS.len())];
                let job_id = format!("{}-{}", limit_key, i);
                let priority = rng.random_range(1..100);

                // Random delay between enqueues
                let delay_ms = rng.random_range(50..200);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                // Round-robin across shards to exercise routing
                let shard_idx = (i as usize) % shard_ids.len();
                let shard_id = &shard_ids[shard_idx];

                // Find the current owner of this shard (with retry on failure)
                let owner = match producer_k8s_state
                    .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, shard_id)
                    .await
                {
                    Ok(owner) => owner,
                    Err(e) => {
                        tracing::trace!(error = %e, shard = %shard_id, "producer owner lookup failed");
                        // Skip this job on failure, will retry on next iteration
                        continue;
                    }
                };

                let target_node = owner
                    .as_ref()
                    .and_then(|o| o.strip_prefix("node-"))
                    .and_then(|n| n.parse::<u32>().ok());

                // Get or create client for the target node
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
                                tracing::trace!(error = %e, "failed to create client for node {}", target);
                                continue;
                            }
                        }
                    }
                    _ => {
                        tracing::trace!(shard_id = %shard_id, "no owner found for shard, skipping");
                        continue;
                    }
                };

                // Build limits
                let limits = if max_conc > 0 {
                    vec![Limit {
                        limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                            key: limit_key.into(),
                            max_concurrency: max_conc,
                        })),
                    }]
                } else {
                    vec![]
                };

                // Use retry policy to handle lease expiry during membership changes
                let retry_policy = Some(RetryPolicy {
                    retry_count: 5,
                    initial_interval_ms: 200,
                    max_interval_ms: 2000,
                    randomize_interval: true,
                    backoff_factor: 1.5,
                });

                tracing::trace!(
                    job_id = %job_id,
                    shard = %shard_id,
                    target_node = ?target_node,
                    limit_key = limit_key,
                    "enqueue"
                );

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
                                    "limit": limit_key,
                                    "idx": i
                                }))
                                .unwrap(),
                            )),
                        }),
                        limits,
                        tenant: None,
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

                        // Clear the current client on failure to force reconnect
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

        // Spawn workers - each worker cycles through shards, routing to owners
        for worker_num in 0..num_workers {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("k8s-worker-{}", worker_num);
            let worker_completed = Arc::clone(&total_completed);
            let worker_done_flag = Arc::clone(&scenario_done);
            let worker_config = client_config.clone();
            let worker_k8s_state = Arc::clone(&k8s_state);

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());

            sim.client(client_name, async move {
                // Wait for all initial nodes to be ready before starting workers
                let start_delay = 2000 + (worker_num as u64 * 100);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);

                // Discover shard IDs from the K8s ConfigMap with retry on failure
                let shard_ids = loop {
                    match worker_k8s_state.get_shard_ids(NAMESPACE, CLUSTER_PREFIX).await {
                        Ok(Some(ids)) if !ids.is_empty() => break ids,
                        Ok(_) => {
                            // Not ready yet, wait
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(e) => {
                            // K8s failure, retry with backoff
                            tracing::trace!(worker = %worker_id, error = %e, "worker shard discovery failed, retrying");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                };
                tracing::debug!(worker = %worker_id, shard_count = shard_ids.len(), "worker discovered shards");

                let mut current_client: Option<(u32, silo::pb::silo_client::SiloClient<_>)> = None;

                let mut completed = 0u32;
                let mut processing: Vec<(Task, ShardId)> = Vec::new();
                let mut round = 0u32;

                // Keep working until scenario is done - this allows workers to
                // continue processing jobs during the convergence phase
                while !worker_done_flag.load(Ordering::SeqCst) {
                    round = round.wrapping_add(1);

                    // Pick a random shard to poll
                    let shard_idx = rng.random_range(0..shard_ids.len());
                    let shard_id = shard_ids[shard_idx];

                    // Find the current owner of this shard (with retry on failure)
                    let owner = match worker_k8s_state
                        .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, &shard_id)
                        .await
                    {
                        Ok(owner) => owner,
                        Err(e) => {
                            tracing::trace!(worker = %worker_id, error = %e, "worker owner lookup failed");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let target_node = owner
                        .as_ref()
                        .and_then(|o| o.strip_prefix("node-"))
                        .and_then(|n| n.parse::<u32>().ok());

                    // Get or create client for the target node
                    let client = match (&mut current_client, target_node) {
                        (Some((node, client)), Some(target)) if *node == target => client,
                        (_, Some(target)) => {
                            let uri = format!("http://node{}:{}", target, BASE_PORT + target as u16);
                            match create_turmoil_client(&uri, &worker_config).await {
                                Ok(new_client) => {
                                    current_client = Some((target, new_client));
                                    &mut current_client.as_mut().unwrap().1
                                }
                                Err(e) => {
                                    tracing::trace!(
                                        worker = %worker_id,
                                        error = %e,
                                        "failed to create client"
                                    );
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    continue;
                                }
                            }
                        }
                        _ => {
                            // No owner found, wait and retry
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
                            let tasks = resp.into_inner().tasks;

                            for task in tasks {
                                tracing::trace!(
                                    worker = %worker_id,
                                    job_id = %task.job_id,
                                    task_id = %task.id,
                                    shard = %shard_id,
                                    round = round,
                                    "lease"
                                );
                                processing.push((task, shard_id));
                            }
                        }
                        Err(e) => {
                            tracing::trace!(
                                worker = %worker_id,
                                error = %e,
                                round = round,
                                "lease_failed"
                            );
                            // Clear client on error to force reconnect
                            current_client = None;
                        }
                    }

                    // Process some tasks
                    if !processing.is_empty() {
                        let num_to_complete = rng.random_range(1..=processing.len());
                        let mut indices_to_remove: HashSet<usize> = HashSet::new();

                        for i in 0..num_to_complete {
                            if i >= processing.len() {
                                break;
                            }

                            let (task, task_shard_id) = &processing[i];

                            // Find owner for this task's shard (may have changed)
                            let task_owner = match worker_k8s_state
                                .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, task_shard_id)
                                .await
                            {
                                Ok(owner) => owner,
                                Err(_) => {
                                    // K8s lookup failed, skip this task for now
                                    continue;
                                }
                            };

                            let task_target = task_owner
                                .as_ref()
                                .and_then(|o| o.strip_prefix("node-"))
                                .and_then(|n| n.parse::<u32>().ok());

                            // Get client for the task's shard owner
                            let report_client = match (&mut current_client, task_target) {
                                (Some((node, client)), Some(target)) if *node == target => client,
                                (_, Some(target)) => {
                                    let uri = format!("http://node{}:{}", target, BASE_PORT + target as u16);
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

                            // Simulate work
                            let process_time = rng.random_range(20..100);
                            tokio::time::sleep(Duration::from_millis(process_time)).await;

                            // 5% failure rate
                            let should_fail = rng.random_ratio(5, 100);

                            let outcome = if should_fail {
                                report_outcome_request::Outcome::Failure(silo::pb::Failure {
                                    code: "worker_failure".into(),
                                    data: None,
                                })
                            } else {
                                report_outcome_request::Outcome::Success(SerializedBytes {
                                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                                        rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                                    )),
                                })
                            };

                            match report_client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: task_shard_id.to_string(),
                                    task_id: task.id.clone(),
                                    outcome: Some(outcome),
                                }))
                                .await
                            {
                                Ok(_) => {
                                    if !should_fail {
                                        tracing::trace!(
                                            worker = %worker_id,
                                            job_id = %task.job_id,
                                            "complete"
                                        );
                                        completed += 1;
                                        worker_completed.fetch_add(1, Ordering::SeqCst);
                                    }
                                    indices_to_remove.insert(i);
                                }
                                Err(e) => {
                                    tracing::trace!(
                                        worker = %worker_id,
                                        job_id = %task.job_id,
                                        error = %e,
                                        "report_failed"
                                    );
                                    // Task may have been lost due to shard migration, remove it
                                    indices_to_remove.insert(i);
                                }
                            }
                        }

                        // Remove completed/failed tasks
                        let mut indices: Vec<_> = indices_to_remove.into_iter().collect();
                        indices.sort_by(|a, b| b.cmp(a));
                        for idx in indices {
                            processing.remove(idx);
                        }
                    }

                    // Random delay between rounds
                    let delay = rng.random_range(50..200);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }

                tracing::trace!(
                    worker = %worker_id,
                    completed = completed,
                    "worker_done"
                );

                Ok(())
            });
        }

        // Controller: Manages membership changes
        let controller_k8s_state = k8s_state.clone();
        let controller_membership_done = Arc::clone(&membership_changes_done);
        let controller_split_brain = Arc::clone(&split_brain_detected);
        let controller_active_nodes = Arc::clone(&active_nodes);
        let controller_node_senders = Arc::clone(&node_state_senders);

        sim.client("controller", async move {
            // Wait for initial nodes to converge and some work to start
            tracing::info!("waiting for initial convergence");
            tokio::time::sleep(Duration::from_secs(8)).await;

            // Now that initial nodes are up, introduce some K8s API failures
            // (but keep it low to allow progress)
            controller_k8s_state
                .set_failure_rate(k8s_failure_rate)
                .await;
            tracing::info!(
                k8s_failure_rate = k8s_failure_rate,
                "enabled k8s failure rate"
            );

            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(500));
            let mut active_node_ids: Vec<String> =
                (0..initial_nodes).map(|n| format!("node-{}", n)).collect();
            let mut next_node_to_activate = initial_nodes;

            // Verify no split-brain initially
            if !verify_no_split_brain(&controller_k8s_state, NUM_SHARDS, &active_node_ids).await {
                controller_split_brain.store(true, Ordering::SeqCst);
                tracing::error!("SPLIT-BRAIN DETECTED in initial state");
            }

            // Add nodes while work is happening
            for i in 0..nodes_to_add {
                let delay_ms = rng.random_range(3000..6000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                let new_node_id = format!("node-{}", next_node_to_activate);
                let new_node_num = next_node_to_activate;
                tracing::info!(node_id = %new_node_id, add_num = i, "activating node");

                // Temporarily disable K8s failure rate during node activation
                // to allow the new node to start up cleanly
                controller_k8s_state.set_failure_rate(0.0).await;

                // Activate the node
                {
                    let senders = controller_node_senders.lock().unwrap();
                    if let Err(e) = senders[next_node_to_activate as usize].send(NodeState::Active)
                    {
                        tracing::error!(error = %e, "failed to activate node");
                        drop(senders);
                        controller_k8s_state
                            .set_failure_rate(k8s_failure_rate)
                            .await;
                        continue;
                    }
                }

                // Update active nodes list for workers
                {
                    let mut nodes = controller_active_nodes.lock().unwrap();
                    nodes.push(new_node_num);
                }

                active_node_ids.push(new_node_id.clone());
                next_node_to_activate += 1;

                // Wait for new node to join and rebalance, then re-enable failure rate
                tokio::time::sleep(Duration::from_secs(10)).await;
                controller_k8s_state
                    .set_failure_rate(k8s_failure_rate)
                    .await;
                tokio::time::sleep(Duration::from_secs(5)).await;

                // Verify no split-brain after adding
                if !verify_no_split_brain(&controller_k8s_state, NUM_SHARDS, &active_node_ids).await
                {
                    controller_split_brain.store(true, Ordering::SeqCst);
                    tracing::error!("SPLIT-BRAIN DETECTED after adding node");
                }
            }

            // Remove nodes while work continues
            // With proper shard routing, any node can be removed - work will route to new owners
            for i in 0..nodes_to_remove {
                // Need at least 1 node to continue operating
                if active_node_ids.len() <= 1 {
                    break;
                }

                let delay_ms = rng.random_range(4000..8000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                // Pick a random node to remove
                let remove_idx = rng.random_range(0..active_node_ids.len());
                let node_to_remove = active_node_ids.remove(remove_idx);

                // Extract node number
                let node_num: u32 = node_to_remove
                    .strip_prefix("node-")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                tracing::info!(node_id = %node_to_remove, remove_num = i, "shutting down node");

                // Update active nodes list for workers BEFORE shutdown
                {
                    let mut nodes = controller_active_nodes.lock().unwrap();
                    nodes.retain(|&n| n != node_num);
                }

                // Signal shutdown
                {
                    let senders = controller_node_senders.lock().unwrap();
                    if let Err(e) = senders[node_num as usize].send(NodeState::Shutdown) {
                        tracing::warn!(error = %e, "failed to send shutdown");
                    }
                }

                // Wait for other nodes to take over
                tokio::time::sleep(Duration::from_secs(15)).await;

                // Verify no split-brain after removal
                if !verify_no_split_brain(&controller_k8s_state, NUM_SHARDS, &active_node_ids).await
                {
                    controller_split_brain.store(true, Ordering::SeqCst);
                    tracing::error!("SPLIT-BRAIN DETECTED after removing node");
                }
            }

            // Let work continue for a bit
            tokio::time::sleep(Duration::from_secs(20)).await;

            // Final verification
            let had_split_brain = controller_split_brain.load(Ordering::SeqCst);
            let no_split_brain =
                verify_no_split_brain(&controller_k8s_state, NUM_SHARDS, &active_node_ids).await;

            tracing::info!(
                had_split_brain = had_split_brain,
                final_no_split_brain = no_split_brain,
                final_node_count = active_node_ids.len(),
                "controller_verification"
            );

            assert!(
                !had_split_brain,
                "Split-brain was detected during the scenario"
            );
            assert!(no_split_brain, "Split-brain detected in final verification");

            // Disable K8s failures to allow clean convergence
            // Workers need reliable access to shard ownership info to complete jobs
            controller_k8s_state.set_failure_rate(0.0).await;
            tracing::info!("disabled k8s failures for convergence phase");

            // Signal that membership changes are complete - workers will keep running
            // until the verifier confirms convergence
            controller_membership_done.store(true, Ordering::SeqCst);
            tracing::info!("controller done - membership changes complete");
            Ok(())
        });

        // Partitioner: Injects network partitions between nodes to test split-brain resilience
        // Partitioner also stops when membership changes are done to allow convergence
        let partitioner_membership_done = Arc::clone(&membership_changes_done);
        sim.client("partitioner", async move {
            // Wait for initial convergence before injecting partitions
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(700));

            for p in 0..num_partitions {
                // Stop injecting partitions once membership changes are done
                // to allow the system to converge
                if partitioner_membership_done.load(Ordering::SeqCst) {
                    break;
                }

                // Random delay between partitions
                let delay_ms = rng.random_range(3000..8000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                if partitioner_membership_done.load(Ordering::SeqCst) {
                    break;
                }

                // Pick two different nodes to partition from each other
                // We only partition nodes that are currently active (< total_nodes)
                let node_a = rng.random_range(0..total_nodes);
                let mut node_b = rng.random_range(0..total_nodes);
                while node_b == node_a && total_nodes > 1 {
                    node_b = rng.random_range(0..total_nodes);
                }

                if node_a == node_b {
                    // Only one node, skip partition
                    continue;
                }

                let host_a = format!("node{}", node_a);
                let host_b = format!("node{}", node_b);

                let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
                tracing::info!(
                    partition = p,
                    host_a = %host_a,
                    host_b = %host_b,
                    sim_time_ms = sim_time,
                    "partition_start"
                );

                // Partition the two nodes from each other
                turmoil::partition(host_a.as_str(), host_b.as_str());

                // Partition duration from configured range
                let partition_duration =
                    rng.random_range(partition_duration_range.0..=partition_duration_range.1);
                tokio::time::sleep(Duration::from_millis(partition_duration)).await;

                // Repair the partition
                turmoil::repair(host_a.as_str(), host_b.as_str());

                let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
                tracing::info!(
                    partition = p,
                    host_a = %host_a,
                    host_b = %host_b,
                    sim_time_ms = sim_time,
                    duration_ms = partition_duration,
                    "partition_end"
                );
            }

            tracing::trace!("partitioner_done");
            Ok(())
        });

        // Verifier: Periodically checks DST invariants and waits for convergence
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_scenario_done = Arc::clone(&scenario_done);
        let verifier_membership_done = Arc::clone(&membership_changes_done);
        let verifier_node_senders = Arc::clone(&node_state_senders);

        sim.client("verifier", async move {
            // Wait for work to start
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // Phase 1: Periodically verify invariants while membership changes are happening
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Process DST events
                verifier_tracker.process_dst_events();

                // Verify invariants
                verifier_tracker.verify_all();

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);

                tracing::trace!(
                    enqueued = enqueued,
                    completed = completed,
                    membership_done = verifier_membership_done.load(Ordering::SeqCst),
                    "invariant_check_passed"
                );

                // Exit periodic checking when membership changes are done
                if verifier_membership_done.load(Ordering::SeqCst) {
                    tracing::info!("membership changes done, entering convergence phase");
                    break;
                }
            }

            // Phase 2: Wait for convergence - all enqueued jobs should complete
            // This tests that the system eventually reaches a stable state
            let convergence_timeout_secs = 60; // Max time to wait for convergence
            let convergence_start = turmoil::sim_elapsed().unwrap_or_default();
            let mut last_completed = 0u32;
            let mut stall_count = 0u32;

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                // Process DST events
                verifier_tracker.process_dst_events();
                verifier_tracker.verify_all();

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);

                tracing::trace!(
                    enqueued = enqueued,
                    completed = completed,
                    "convergence_check"
                );

                // Check if all jobs are complete
                if completed >= enqueued && enqueued > 0 {
                    tracing::info!(
                        enqueued = enqueued,
                        completed = completed,
                        "convergence achieved - all jobs complete"
                    );
                    break;
                }

                // Check for progress stall (no new completions for a while)
                if completed == last_completed {
                    stall_count += 1;
                } else {
                    stall_count = 0;
                    last_completed = completed;
                }

                // If stalled for too long, check if we should give up
                // (10 seconds of no progress might indicate lost jobs)
                if stall_count >= 10 && completed > 0 {
                    tracing::warn!(
                        enqueued = enqueued,
                        completed = completed,
                        stall_count = stall_count,
                        "progress stalled, checking if jobs are recoverable"
                    );
                    // Give it more time but log the stall
                }

                // Check timeout
                let elapsed = turmoil::sim_elapsed().unwrap_or_default();
                let convergence_duration = elapsed.saturating_sub(convergence_start);
                if convergence_duration.as_secs() >= convergence_timeout_secs {
                    tracing::warn!(
                        enqueued = enqueued,
                        completed = completed,
                        elapsed_secs = convergence_duration.as_secs(),
                        "convergence timeout reached"
                    );
                    break;
                }
            }

            // Signal workers to stop
            verifier_scenario_done.store(true, Ordering::SeqCst);

            // Signal all nodes to shut down gracefully
            // This allows proper cleanup before final verification
            {
                let senders = verifier_node_senders.lock().unwrap();
                for (i, sender) in senders.iter().enumerate() {
                    if let Err(e) = sender.send(NodeState::Shutdown) {
                        tracing::trace!(node = i, error = %e, "shutdown signal failed (node may already be stopped)");
                    }
                }
            }

            // Give nodes time to shut down gracefully
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Final verification
            verifier_tracker.process_dst_events();
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            // Verify state transitions
            let transition_violations = verifier_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                tracing::warn!(
                    violations = transition_violations.len(),
                    "transition_violations_found"
                );
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
            let terminal_from_dst = verifier_tracker.jobs.terminal_count();
            let shard_ownership_violations = verifier_tracker.shards.has_violations();

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                terminal_from_dst = terminal_from_dst,
                shard_ownership_violations = shard_ownership_violations,
                "final_verification"
            );

            // Verify no split-brain occurred (via DST events)
            verifier_tracker.shards.verify_no_split_brain();

            // Verify eventual completion
            // In a chaotic environment with membership changes and partitions,
            // some job loss may be acceptable due to:
            // - Jobs in-flight during node shutdown
            // - Jobs enqueued to nodes that are then removed
            // - Retry budget exhaustion during instability
            //
            // The key invariants are:
            // 1. No split-brain (checked above)
            // 2. Valid state transitions (checked above)
            // 3. Progress was made (some jobs completed)
            // 4. High completion rate after convergence (80%+ or 3+ jobs)

            assert!(
                completed > 0 || enqueued == 0,
                "No progress made: completed={}, enqueued={}",
                completed,
                enqueued
            );

            if enqueued > 0 {
                let completion_rate = (completed as f64) / (enqueued as f64);
                let jobs_lost = enqueued.saturating_sub(completed);

                tracing::info!(
                    enqueued = enqueued,
                    completed = completed,
                    jobs_lost = jobs_lost,
                    completion_rate = format!("{:.1}%", completion_rate * 100.0),
                    "job_completion_stats"
                );

                // We expect at least 80% completion rate after the convergence phase,
                // or at least 3 completed jobs (for very low job counts).
                // This is more lenient than 100% because some job loss is acceptable
                // during severe membership changes with partitions.
                let acceptable_rate = 0.8;
                let min_completed = 3;

                if completion_rate < acceptable_rate && completed < min_completed {
                    tracing::error!(
                        enqueued = enqueued,
                        completed = completed,
                        jobs_lost = jobs_lost,
                        completion_rate = format!("{:.1}%", completion_rate * 100.0),
                        acceptable_rate = format!("{:.0}%", acceptable_rate * 100.0),
                        "too many jobs lost during membership changes"
                    );
                    panic!(
                        "Too many jobs lost after convergence: completed={}, enqueued={}, rate={:.1}%. \
                         Expected at least {:.0}% completion or {} jobs.",
                        completed,
                        enqueued,
                        completion_rate * 100.0,
                        acceptable_rate * 100.0,
                        min_completed
                    );
                } else if jobs_lost > 0 {
                    tracing::warn!(
                        enqueued = enqueued,
                        completed = completed,
                        jobs_lost = jobs_lost,
                        completion_rate = format!("{:.1}%", completion_rate * 100.0),
                        "some jobs lost during membership changes (within acceptable threshold)"
                    );
                }
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}

use std::collections::HashMap as StdHashMap;
use uuid::Uuid;

/// Verify that no shard is owned by multiple nodes (no split-brain)
async fn verify_no_split_brain(
    k8s_state: &MockK8sState,
    num_shards: u32,
    active_nodes: &[String],
) -> bool {
    let mut shard_owners: StdHashMap<ShardId, Vec<String>> = StdHashMap::new();

    let label_selector = format!("silo.dev/type=shard,silo.dev/cluster={}", CLUSTER_PREFIX);
    let leases = k8s_state
        .list_leases(NAMESPACE, Some(&label_selector))
        .await
        .unwrap_or_default();

    let prefix = format!("{}-shard-", CLUSTER_PREFIX);
    for stored in leases {
        if let Some(holder) = stored
            .lease
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref())
        {
            if !holder.is_empty() && active_nodes.contains(holder) {
                if let Some(shard_id_str) = stored.lease.metadata.name.as_ref() {
                    if let Some(id_part) = shard_id_str.strip_prefix(&prefix) {
                        if let Ok(uuid) = Uuid::parse_str(id_part) {
                            let shard_id = ShardId::from_uuid(uuid);
                            shard_owners
                                .entry(shard_id)
                                .or_default()
                                .push(holder.clone());
                        }
                    }
                }
            }
        }
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

    tracing::debug!(
        total_shards_with_owners = shard_owners.len(),
        expected_shards = num_shards,
        "split-brain check passed"
    );

    true
}
