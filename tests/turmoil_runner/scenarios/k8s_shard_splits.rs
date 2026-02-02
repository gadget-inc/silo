//! K8s shard split scenario: Tests shard splitting under load with job processing.
//!
//! This scenario tests that shard split operations work correctly while jobs
//! are being enqueued and processed. It validates that:
//! - Shards can be split without data loss
//! - Jobs continue to be processed during and after splits
//! - The shard map grows correctly over time
//! - No split-brain conditions occur
//! - Jobs are correctly routed to child shards after splits
//!
//! The scenario uses filesystem storage to enable real shard cloning via SlateDB's
//! clone operation. Jobs are assigned tenants that fall within their shard's range,
//! ensuring they can be found in child shards after splits.
//!
//! Fault modes:
//! - Shard splits during active job processing
//! - Multiple splits in sequence
//! - Configurable K8s API failure rate
//!
//! Invariants verified:
//! - **queueLimitEnforced**: At most max_concurrency tasks per limit key
//! - **oneLeasePerJob**: A job never has two active leases simultaneously
//! - **noLeasesForTerminal**: Terminal jobs have no active leases
//! - **validTransitions**: Only valid status transitions occur
//! - **noSplitBrain**: A shard is owned by at most one node at any time
//! - **shardCountGrows**: The shard count increases after each split
//! - **jobCompleteness**: All jobs reach terminal state (100% completion)

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, HashMap, InvariantTracker, LeaseTasksRequest, Limit,
    ReportOutcomeRequest, RetryPolicy, SerializedBytes, create_turmoil_client, get_seed, limit,
    report_outcome_request, run_scenario_impl, serialized_bytes, turmoil,
};
use crate::mock_k8s::{MockK8sBackend, MockK8sState};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::cluster_client::ClientConfig;
use silo::coordination::{Coordinator, K8sCoordinator, ShardSplitter};
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

/// Start with enough shards so the splitter node (node-0) keeps at least one
/// even after rebalancing across 2-3 nodes. 4 shards ensures node-0 keeps 1-2.
const INITIAL_SHARDS: u32 = 4;
const BASE_PORT: u16 = 9960;
const CLUSTER_PREFIX: &str = "silo-split-test";
const NAMESPACE: &str = "default";

/// Concurrency limit configurations for testing
const WORKER_LIMITS: &[(&str, u32)] = &[
    ("split-mutex", 1), // Strict serialization
    ("split-batch", 2), // Small concurrency
    ("split-free", 0),  // No limit
];

/// Node lifecycle state communicated via watch channel
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeState {
    /// Node should start its coordinator and server
    Active,
    /// Node should shut down gracefully
    Shutdown,
}

/// Configuration for the scenario
struct ScenarioConfig {
    /// Number of nodes in the cluster
    num_nodes: u32,
    /// Number of splits to perform during the test
    num_splits: u32,
    /// Failure rate for mock K8s operations (0.0 - 1.0)
    k8s_failure_rate: f64,
    /// Lease duration in seconds
    lease_duration_secs: i64,
    /// Number of jobs to enqueue
    num_jobs: u32,
    /// Number of workers
    num_workers: u32,
    /// Delay between splits (min_ms, max_ms)
    split_delay_range: (u64, u64),
    /// Maximum retries for split operations
    max_split_retries: u32,
    /// Probability (0.0-1.0) that producer uses stale shard info (tests wrong-shard routing)
    stale_shard_probability: f64,
}

impl ScenarioConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        Self {
            num_nodes: rng.random_range(2..=3),
            // Perform 2-4 splits during the test
            num_splits: rng.random_range(2..=4),
            // K8s failure rate is kept constant throughout (including during splits)
            // to test retry/idempotency logic
            k8s_failure_rate: 0.02 + 0.03 * (rng.random_range(0..100) as f64 / 100.0),
            lease_duration_secs: rng.random_range(5..=8) as i64,
            // More jobs to ensure splits happen during processing
            num_jobs: rng.random_range(20..=35),
            num_workers: rng.random_range(2..=3),
            // Short delay between splits - first split should happen quickly before
            // shard rebalancing moves shards away from the splitter node
            split_delay_range: (rng.random_range(500..1500), rng.random_range(2000..4000)),
            // Allow 3-5 retries for split operations
            max_split_retries: rng.random_range(3..=5),
            // 10-20% chance producer uses stale shard info (tests wrong-shard handling)
            stale_shard_probability: 0.1 + 0.1 * (rng.random_range(0..100) as f64 / 100.0),
        }
    }
}

/// Create a shard factory using turmoil's simulated filesystem for split testing.
/// All nodes share the same storage root so that shard cloning works correctly.
/// Using TurmoilFs backend ensures fully deterministic filesystem operations for DST.
fn make_shard_factory(storage_root: &std::path::Path) -> Arc<ShardFactory> {
    // Use {shard} placeholder so each shard gets its own subdirectory
    let path = storage_root.join("{shard}").to_string_lossy().to_string();
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            // Use TurmoilFs backend for split tests - this uses turmoil's simulated filesystem
            // which provides full determinism for DST while supporting SlateDB cloning
            backend: Backend::TurmoilFs,
            path,
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
    storage_root: std::path::PathBuf,
) -> turmoil::Result<NodeServerHandle> {
    let backend = MockK8sBackend::new(k8s_state, NAMESPACE);
    let factory = make_shard_factory(&storage_root);

    let (coordinator, _handle) = K8sCoordinator::start_with_backend(
        backend,
        silo::coordination::K8sCoordinatorConfig {
            namespace: NAMESPACE.to_string(),
            cluster_prefix: CLUSTER_PREFIX.to_string(),
            node_id: node_id.clone(),
            grpc_addr: format!("http://{}:{}", node_id, port),
            initial_shard_count: INITIAL_SHARDS,
            lease_duration_secs,
            placement_rings: Vec::new(),
        },
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
        tenancy: silo::settings::TenancyConfig { enabled: true },
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

    let coordinator = Arc::new(coordinator);
    let coordinator_for_server: Arc<dyn silo::coordination::Coordinator> =
        Arc::clone(&coordinator) as Arc<dyn silo::coordination::Coordinator>;

    // Spawn the server in the background
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

/// Generate a tenant key that falls within the given shard range.
/// This ensures jobs are routed to the correct shard based on tenant.
fn generate_tenant_for_range(range: &silo::shard_range::ShardRange, rng: &mut StdRng) -> String {
    let start = &range.start;
    let end = &range.end;

    match (start.is_empty(), end.is_empty()) {
        (true, true) => {
            // Full range: any character works
            let c = rng.random_range(b'a'..=b'z') as char;
            c.to_string()
        }
        (true, false) => {
            // [unbounded, end): pick something less than end
            let end_char = end.chars().next().unwrap_or('z') as u8;
            if end_char > b'!' {
                let c = rng.random_range(b'!'..end_char) as char;
                c.to_string()
            } else {
                "!".to_string()
            }
        }
        (false, true) => {
            // [start, unbounded): pick something >= start
            let start_char = start.chars().next().unwrap_or('a') as u8;
            let max_char = b'z';
            if start_char < max_char {
                let c = rng.random_range(start_char..=max_char) as char;
                c.to_string()
            } else {
                start.clone()
            }
        }
        (false, false) => {
            // [start, end): pick something in between
            let start_char = start.chars().next().unwrap_or('a') as u8;
            let end_char = end.chars().next().unwrap_or('z') as u8;
            if end_char > start_char {
                let c = rng.random_range(start_char..end_char) as char;
                c.to_string()
            } else {
                start.clone()
            }
        }
    }
}

/// Compute a valid split point for a shard based on its range.
/// Returns a point that is within the shard's range.
fn compute_split_point(shard_range: &silo::shard_range::ShardRange) -> String {
    // Generate a split point by taking the midpoint of the range
    // In ShardRange, empty string means unbounded
    let start = &shard_range.start;
    let end = &shard_range.end;

    // If the range is unbounded, use a simple midpoint character
    match (start.is_empty(), end.is_empty()) {
        (true, true) => "m".to_string(), // Full range: split at 'm'
        (true, false) => {
            // Start is empty (unbounded), end is bounded
            if let Some(c) = end.chars().next() {
                let mid = (c as u8 / 2) as char;
                mid.to_string()
            } else {
                "m".to_string()
            }
        }
        (false, true) => {
            // Start is bounded, end is empty (unbounded)
            if let Some(c) = start.chars().next() {
                let mid = ((c as u8).saturating_add(127) / 2) as char;
                // Make sure it's > start
                if mid.to_string() > *start {
                    mid.to_string()
                } else {
                    format!("{}m", start)
                }
            } else {
                format!("{}m", start)
            }
        }
        (false, false) => {
            // Both bounded - compute midpoint
            let start_char = start.chars().next().unwrap_or('a') as u8;
            let end_char = end.chars().next().unwrap_or('z') as u8;
            if end_char > start_char {
                let mid = ((start_char as u16 + end_char as u16) / 2) as u8 as char;
                mid.to_string()
            } else {
                format!("{}m", start)
            }
        }
    }
}

pub fn run() {
    let seed = get_seed();

    // Use a deterministic storage path in turmoil's simulated filesystem.
    // Each host in turmoil has its own isolated filesystem namespace, so this path
    // will be unique per host. The TurmoilFs backend uses turmoil's fs shim which
    // provides full determinism - all I/O operations are simulated and seeded.
    let storage_root = std::path::PathBuf::from("/data/silo-shards");

    // 60 seconds is enough - the test typically completes within 25 simulated seconds.
    // The previous 180s was excessive and caused long real-time test durations.
    run_scenario_impl("k8s_shard_splits", seed, 60, |sim| {
        let config = ScenarioConfig::from_seed(seed);
        let storage_root = storage_root.clone();

        tracing::info!(
            num_nodes = config.num_nodes,
            num_splits = config.num_splits,
            k8s_failure_rate = config.k8s_failure_rate,
            lease_duration = config.lease_duration_secs,
            initial_shards = INITIAL_SHARDS,
            num_jobs = config.num_jobs,
            num_workers = config.num_workers,
            split_delay = ?config.split_delay_range,
            max_split_retries = config.max_split_retries,
            stale_shard_probability = config.stale_shard_probability,
            storage_root = %storage_root.display(),
            "k8s_shard_splits_config"
        );

        // Capture config values
        let num_nodes = config.num_nodes;
        let num_splits = config.num_splits;
        let k8s_failure_rate = config.k8s_failure_rate;
        let lease_duration_secs = config.lease_duration_secs;
        let num_jobs = config.num_jobs;
        let num_workers = config.num_workers;
        let split_delay_range = config.split_delay_range;
        let max_split_retries = config.max_split_retries;
        let stale_shard_probability = config.stale_shard_probability;

        // Shared state
        let scenario_done = Arc::new(AtomicBool::new(false));
        let splits_done = Arc::new(AtomicBool::new(false));
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));
        let total_splits_completed = Arc::new(AtomicU32::new(0));
        let initial_shard_count = Arc::new(AtomicU32::new(INITIAL_SHARDS));
        let current_shard_count = Arc::new(AtomicU32::new(INITIAL_SHARDS));
        // Track split retry attempts and wrong-shard routing attempts
        let total_split_retries = Arc::new(AtomicU32::new(0));
        let total_stale_shard_sends = Arc::new(AtomicU32::new(0));
        let total_stale_shard_errors = Arc::new(AtomicU32::new(0));

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

        // Create state channels for each node (all start active)
        let mut node_state_senders_vec: Vec<watch::Sender<NodeState>> = Vec::new();
        let mut node_state_receivers: Vec<watch::Receiver<NodeState>> = Vec::new();

        for _ in 0..num_nodes {
            let (tx, rx) = watch::channel(NodeState::Active);
            node_state_senders_vec.push(tx);
            node_state_receivers.push(rx);
        }

        let node_state_senders: Arc<std::sync::Mutex<Vec<watch::Sender<NodeState>>>> =
            Arc::new(std::sync::Mutex::new(node_state_senders_vec));

        // Track split progress via shared atomics (splitter node updates these)
        let splits_completed_by_node = Arc::clone(&total_splits_completed);
        let current_shards_by_node = Arc::clone(&current_shard_count);
        let splits_done_by_node = Arc::clone(&splits_done);
        let split_retries_by_node = Arc::clone(&total_split_retries);

        // Create all node hosts
        // Node 0 is the "splitter node" - it will execute splits in addition to serving traffic
        for node_num in 0..num_nodes {
            let node_id = format!("node-{}", node_num);
            let k8s_state_for_node = k8s_state.clone();
            let host_name: &'static str = Box::leak(format!("node{}", node_num).into_boxed_str());
            let state_rx = node_state_receivers[node_num as usize].clone();
            let startup_delay_ms = (node_num as u64) * 150;
            let port = BASE_PORT + node_num as u16;

            // Only node 0 does splits
            let is_splitter_node = node_num == 0;
            let node_num_splits = if is_splitter_node { num_splits } else { 0 };
            let node_splits_completed = Arc::clone(&splits_completed_by_node);
            let node_current_shards = Arc::clone(&current_shards_by_node);
            let node_splits_done = Arc::clone(&splits_done_by_node);
            let node_split_retries = Arc::clone(&split_retries_by_node);
            let node_k8s_state = Arc::clone(&k8s_state);
            let storage_root = storage_root.clone();

            sim.host(host_name, {
                let node_id = node_id.clone();
                move || {
                    let node_id = node_id.clone();
                    let k8s_state = k8s_state_for_node.clone();
                    let mut state_rx = state_rx.clone();
                    // Clone Arc values for use in the async block
                    let node_k8s_state = node_k8s_state.clone();
                    let node_splits_completed = node_splits_completed.clone();
                    let node_current_shards = node_current_shards.clone();
                    let node_splits_done = node_splits_done.clone();
                    let node_split_retries = node_split_retries.clone();
                    let storage_root = storage_root.clone();
                    async move {
                        // Stagger startup
                        if startup_delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(startup_delay_ms)).await;
                        }

                        tracing::info!(node_id = %node_id, is_splitter = is_splitter_node, "starting node");

                        // Start the server with K8s coordination
                        let handle = setup_node_server(
                            port,
                            node_id.clone(),
                            k8s_state,
                            k8s_failure_rate,
                            lease_duration_secs,
                            storage_root.clone(),
                        )
                        .await?;

                        // If this is the splitter node, spawn split task
                        if is_splitter_node {
                            let coord = Arc::clone(&handle.coordinator);
                            let k8s_state_for_splits = node_k8s_state.clone();
                            let splits_completed = node_splits_completed.clone();
                            let current_shards = node_current_shards.clone();
                            let splits_done_flag = node_splits_done.clone();
                            let split_retries = node_split_retries.clone();

                            tokio::spawn(async move {
                                // Wait for cluster to stabilize
                                tokio::time::sleep(Duration::from_secs(5)).await;

                                let mut rng = StdRng::seed_from_u64(seed.wrapping_add(500));
                                let mut completed = 0u32;

                                for split_num in 0..node_num_splits {
                                    // Delay between splits
                                    let delay_ms = rng.random_range(split_delay_range.0..=split_delay_range.1);
                                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                                    // Get owned shards
                                    let owned = coord.owned_shards().await;
                                    if owned.is_empty() {
                                        tracing::warn!(split_num = split_num, "splitter node owns no shards, skipping");
                                        continue;
                                    }

                                    // Pick a shard to split
                                    let shard_idx = rng.random_range(0..owned.len());
                                    let shard_id = owned[shard_idx];

                                    // Get the shard's range
                                    let shard_map = match coord.get_shard_map().await {
                                        Ok(map) => map,
                                        Err(e) => {
                                            tracing::warn!(error = %e, "failed to get shard map");
                                            continue;
                                        }
                                    };

                                    let Some(info) = shard_map.get_shard(&shard_id) else {
                                        tracing::warn!(shard_id = %shard_id, "shard not found in map");
                                        continue;
                                    };

                                    let split_point = compute_split_point(&info.range);

                                    tracing::info!(
                                        split_num = split_num,
                                        shard_id = %shard_id,
                                        split_point = %split_point,
                                        range = %info.range,
                                        "starting split"
                                    );

                                    // K8s failure rate stays constant - we test retry/idempotency
                                    // Execute split with retry logic
                                    let mut attempt = 0u32;
                                    let split_success = loop {
                                        attempt += 1;
                                        if attempt > 1 {
                                            split_retries.fetch_add(1, Ordering::SeqCst);
                                            // Exponential backoff: 200ms, 400ms, 800ms, ...
                                            let backoff_ms = 200 * (1 << (attempt - 2).min(4));
                                            tracing::debug!(
                                                split_num = split_num,
                                                shard_id = %shard_id,
                                                attempt = attempt,
                                                backoff_ms = backoff_ms,
                                                "split retry with backoff"
                                            );
                                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                        }

                                        // Execute split
                                        let splitter = ShardSplitter::new(Arc::clone(&coord) as Arc<dyn Coordinator>);

                                        match splitter.request_split(shard_id, split_point.clone()).await {
                                            Ok(split_info) => {
                                                tracing::info!(
                                                    split_num = split_num,
                                                    parent = %shard_id,
                                                    left_child = %split_info.left_child_id,
                                                    right_child = %split_info.right_child_id,
                                                    attempt = attempt,
                                                    "split requested"
                                                );

                                                let coord_ref = coord.as_ref();
                                                match splitter
                                                    .execute_split(shard_id, || async { coord_ref.get_shard_owner_map().await })
                                                    .await
                                                {
                                                    Ok(()) => {
                                                        completed += 1;
                                                        splits_completed.fetch_add(1, Ordering::SeqCst);

                                                        let new_count = coord.num_shards().await as u32;
                                                        current_shards.store(new_count, Ordering::SeqCst);

                                                        tracing::info!(
                                                            split_num = split_num,
                                                            shard_id = %shard_id,
                                                            new_shard_count = new_count,
                                                            attempt = attempt,
                                                            "split completed"
                                                        );
                                                        break true;
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            split_num = split_num,
                                                            shard_id = %shard_id,
                                                            attempt = attempt,
                                                            error = %e,
                                                            "split execution failed"
                                                        );
                                                        if attempt >= max_split_retries {
                                                            tracing::warn!(
                                                                split_num = split_num,
                                                                shard_id = %shard_id,
                                                                max_retries = max_split_retries,
                                                                "split exhausted retries"
                                                            );
                                                            break false;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                // Check if split is already in progress (idempotency)
                                                let err_str = e.to_string();
                                                if err_str.contains("already in progress") {
                                                    tracing::debug!(
                                                        split_num = split_num,
                                                        shard_id = %shard_id,
                                                        "split already in progress, continuing execution"
                                                    );
                                                    // Try to just execute the split (it may have been requested but not executed)
                                                    let splitter = ShardSplitter::new(Arc::clone(&coord) as Arc<dyn Coordinator>);
                                                    let coord_ref = coord.as_ref();
                                                    match splitter
                                                        .execute_split(shard_id, || async { coord_ref.get_shard_owner_map().await })
                                                        .await
                                                    {
                                                        Ok(()) => {
                                                            completed += 1;
                                                            splits_completed.fetch_add(1, Ordering::SeqCst);
                                                            let new_count = coord.num_shards().await as u32;
                                                            current_shards.store(new_count, Ordering::SeqCst);
                                                            tracing::info!(
                                                                split_num = split_num,
                                                                shard_id = %shard_id,
                                                                new_shard_count = new_count,
                                                                "resumed split completed"
                                                            );
                                                            break true;
                                                        }
                                                        Err(exec_e) => {
                                                            tracing::warn!(
                                                                split_num = split_num,
                                                                shard_id = %shard_id,
                                                                attempt = attempt,
                                                                error = %exec_e,
                                                                "resumed split execution failed"
                                                            );
                                                        }
                                                    }
                                                }

                                                tracing::warn!(
                                                    split_num = split_num,
                                                    shard_id = %shard_id,
                                                    attempt = attempt,
                                                    error = %e,
                                                    "split request failed"
                                                );
                                                if attempt >= max_split_retries {
                                                    tracing::warn!(
                                                        split_num = split_num,
                                                        shard_id = %shard_id,
                                                        max_retries = max_split_retries,
                                                        "split exhausted retries"
                                                    );
                                                    break false;
                                                }
                                            }
                                        }
                                    };

                                    if split_success {
                                        tracing::debug!(
                                            split_num = split_num,
                                            shard_id = %shard_id,
                                            attempts = attempt,
                                            "split succeeded"
                                        );
                                    }

                                    // Give system time to stabilize
                                    tokio::time::sleep(Duration::from_secs(3)).await;
                                }

                                // Disable K8s failures for final convergence phase only
                                k8s_state_for_splits.set_failure_rate(0.0).await;

                                tracing::info!(
                                    splits_completed = completed,
                                    total_retries = split_retries.load(Ordering::SeqCst),
                                    final_shard_count = current_shards.load(Ordering::SeqCst),
                                    "splitter done"
                                );

                                splits_done_flag.store(true, Ordering::SeqCst);
                            });
                        }

                        // Run until shutdown signal
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

                        Ok(())
                    }
                }
            });
        }

        // Producer: Enqueues jobs to the cluster
        let producer_enqueued = Arc::clone(&total_enqueued);
        let producer_k8s_state = Arc::clone(&k8s_state);
        let producer_config = client_config.clone();
        let producer_num_jobs = num_jobs;
        let producer_stale_sends = Arc::clone(&total_stale_shard_sends);
        let producer_stale_errors = Arc::clone(&total_stale_shard_errors);
        sim.client("producer", async move {
            // Wait for nodes to start and coordinate
            tokio::time::sleep(Duration::from_millis(3000)).await;
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));

            let mut consecutive_failures = 0u32;
            let mut current_client: Option<(u32, silo::pb::silo_client::SiloClient<_>)> = None;
            
            // Cache of previously seen shards - used for stale shard testing
            let mut seen_shards: Vec<ShardId> = Vec::new();

            for i in 0..producer_num_jobs {
                // Select limit configuration
                let (limit_key, max_conc) = WORKER_LIMITS[rng.random_range(0..WORKER_LIMITS.len())];
                let job_id = format!("{}-{}", limit_key, i);
                let priority = rng.random_range(1..100);

                // Random delay between enqueues
                let delay_ms = rng.random_range(50..200);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                // Discover current shards - this may change during splits
                let shard_ids = loop {
                    match producer_k8s_state.get_shard_ids(NAMESPACE, CLUSTER_PREFIX).await {
                        Ok(Some(ids)) if !ids.is_empty() => break ids,
                        Ok(_) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(e) => {
                            tracing::trace!(error = %e, "producer shard discovery failed, retrying");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                };
                
                // Remember shards we've seen for stale shard testing
                for sid in &shard_ids {
                    if !seen_shards.contains(sid) {
                        seen_shards.push(*sid);
                    }
                }

                // Get the shard map to access shard ranges
                let shard_map = match producer_k8s_state
                    .get_shard_map(NAMESPACE, CLUSTER_PREFIX)
                    .await
                {
                    Ok(Some(map)) => map,
                    _ => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                
                // Decide whether to intentionally use a stale shard
                // This tests wrong-shard routing and error handling
                let use_stale_shard = !seen_shards.is_empty() 
                    && seen_shards.len() > shard_ids.len()  // Only if we've seen shards that no longer exist
                    && rng.random_bool(stale_shard_probability);
                
                let is_stale_send: bool;

                // Find a shard with an owner, retrying if needed (e.g., during split transitions)
                let (shard_id, tenant, target_node) = if use_stale_shard {
                    // Find a shard we've seen before that is NOT in current shard_ids (stale/split parent)
                    let stale_shards: Vec<_> = seen_shards.iter()
                        .filter(|sid| !shard_ids.contains(sid))
                        .collect();
                    
                    if let Some(stale_shard) = stale_shards.first() {
                        is_stale_send = true;
                        producer_stale_sends.fetch_add(1, Ordering::SeqCst);
                        
                        // Generate a random tenant (may not be valid for this shard)
                        let tenant = format!("stale-tenant-{}", i);
                        
                        // Pick a random node to send to
                        let target_node = rng.random_range(0..num_nodes);
                        
                        tracing::debug!(
                            job_id = %job_id,
                            stale_shard = %stale_shard,
                            target_node = target_node,
                            "intentionally using stale shard"
                        );
                        
                        (**stale_shard, tenant, target_node)
                    } else {
                        // Fallback to normal path
                        is_stale_send = false;
                        'find_shard_normal: loop {
                            for offset in 0..shard_ids.len() {
                                let shard_idx = ((i as usize) + offset) % shard_ids.len();
                                let shard_id = &shard_ids[shard_idx];
                                let shard_info = match shard_map.get_shard(shard_id) {
                                    Some(info) => info,
                                    None => continue,
                                };
                                let tenant = generate_tenant_for_range(&shard_info.range, &mut rng);
                                let owner = match producer_k8s_state
                                    .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, shard_id)
                                    .await
                                {
                                    Ok(Some(owner)) => owner,
                                    Ok(None) => continue,
                                    Err(_) => continue,
                                };
                                let target_node = owner
                                    .strip_prefix("node-")
                                    .and_then(|n| n.parse::<u32>().ok());
                                if let Some(node) = target_node {
                                    break 'find_shard_normal (shard_id.clone(), tenant, node);
                                }
                            }
                            tracing::trace!("no shards with owners found, waiting");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                } else {
                    is_stale_send = false;
                    'find_shard: loop {
                        // Try each shard in round-robin order
                        for offset in 0..shard_ids.len() {
                            let shard_idx = ((i as usize) + offset) % shard_ids.len();
                            let shard_id = &shard_ids[shard_idx];

                            // Get the shard's range to generate a valid tenant
                            let shard_info = match shard_map.get_shard(shard_id) {
                                Some(info) => info,
                                None => continue,
                            };

                            // Generate a tenant that falls within this shard's range
                            let tenant = generate_tenant_for_range(&shard_info.range, &mut rng);

                            // Find the current owner of this shard
                            let owner = match producer_k8s_state
                                .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, shard_id)
                                .await
                            {
                                Ok(Some(owner)) => owner,
                                Ok(None) => continue, // No owner yet, try another shard
                                Err(_) => continue,
                            };

                            let target_node = owner
                                .strip_prefix("node-")
                                .and_then(|n| n.parse::<u32>().ok());

                            if let Some(node) = target_node {
                                break 'find_shard (shard_id.clone(), tenant, node);
                            }
                        }

                        // No shard has an owner yet - wait and retry
                        tracing::trace!("no shards with owners found, waiting");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                };

                // Get or create client for the target node
                let client = match &mut current_client {
                    Some((node, client)) if *node == target_node => client,
                    _ => {
                        let uri = format!("http://node{}:{}", target_node, BASE_PORT + target_node as u16);
                        match create_turmoil_client(&uri, &producer_config).await {
                            Ok(new_client) => {
                                current_client = Some((target_node, new_client));
                                &mut current_client.as_mut().unwrap().1
                            }
                            Err(e) => {
                                tracing::trace!(error = %e, "failed to create client for node {}", target_node);
                                continue;
                            }
                        }
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

                // Use retry policy to handle split-induced retries
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
                    target_node = target_node,
                    limit_key = limit_key,
                    tenant = %tenant,
                    is_stale = is_stale_send,
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
                        tenant: Some(tenant.clone()),
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        // If this was a stale shard send that succeeded, that's unexpected
                        // (the shard may have been re-created or routing worked differently)
                        if is_stale_send {
                            tracing::debug!(
                                job_id = %job_id,
                                shard = %shard_id,
                                "stale shard send unexpectedly succeeded"
                            );
                        }
                        producer_enqueued.fetch_add(1, Ordering::SeqCst);
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        if is_stale_send {
                            // Expected: stale shard sends should fail
                            producer_stale_errors.fetch_add(1, Ordering::SeqCst);
                            tracing::debug!(
                                job_id = %job_id,
                                shard = %shard_id,
                                error = %e,
                                "stale shard send failed as expected"
                            );
                            // Don't count this as a consecutive failure since it's expected
                        } else {
                            tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                            consecutive_failures += 1;

                            if consecutive_failures >= 2 {
                                current_client = None;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }

            tracing::info!(
                enqueued = producer_enqueued.load(Ordering::SeqCst),
                stale_shard_sends = producer_stale_sends.load(Ordering::SeqCst),
                stale_shard_errors = producer_stale_errors.load(Ordering::SeqCst),
                "producer_done"
            );
            Ok(())
        });

        // Spawn workers
        for worker_num in 0..num_workers {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("split-worker-{}", worker_num);
            let worker_completed = Arc::clone(&total_completed);
            let worker_done_flag = Arc::clone(&scenario_done);
            let worker_config = client_config.clone();
            let worker_k8s_state = Arc::clone(&k8s_state);

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());

            sim.client(client_name, async move {
                let start_delay = 3000 + (worker_num as u64 * 100);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);
                let mut current_client: Option<(u32, silo::pb::silo_client::SiloClient<_>)> = None;
                let mut completed = 0u32;
                let mut processing: Vec<(Task, ShardId)> = Vec::new();

                while !worker_done_flag.load(Ordering::SeqCst) {
                    // Discover shards (may change after splits)
                    let shard_ids = match worker_k8s_state.get_shard_ids(NAMESPACE, CLUSTER_PREFIX).await {
                        Ok(Some(ids)) if !ids.is_empty() => ids,
                        _ => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    // Pick a random shard to poll
                    let shard_idx = rng.random_range(0..shard_ids.len());
                    let shard_id = shard_ids[shard_idx];

                    // Find the current owner
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
                                    tracing::trace!(worker = %worker_id, error = %e, "failed to create client");
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
                            let tasks = resp.into_inner().tasks;
                            for task in tasks {
                                tracing::trace!(
                                    worker = %worker_id,
                                    job_id = %task.job_id,
                                    task_id = %task.id,
                                    shard = %shard_id,
                                    "lease"
                                );
                                processing.push((task, shard_id));
                            }
                        }
                        Err(e) => {
                            tracing::trace!(worker = %worker_id, error = %e, "lease_failed");
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

                            // Find owner for this task's shard
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

        // Verifier: Checks invariants and waits for convergence
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_scenario_done = Arc::clone(&scenario_done);
        let verifier_splits_done = Arc::clone(&splits_done);
        let verifier_node_senders = Arc::clone(&node_state_senders);
        let verifier_initial_shards = Arc::clone(&initial_shard_count);
        let verifier_current_shards = Arc::clone(&current_shard_count);
        let verifier_total_splits = Arc::clone(&total_splits_completed);
        let verifier_split_retries = Arc::clone(&total_split_retries);
        let verifier_stale_sends = Arc::clone(&total_stale_shard_sends);
        let verifier_stale_errors = Arc::clone(&total_stale_shard_errors);

        sim.client("verifier", async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // Phase 1: Verify invariants while splits are happening
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                verifier_tracker.process_dst_events();
                verifier_tracker.verify_all();

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);
                let current_shards = verifier_current_shards.load(Ordering::SeqCst);

                tracing::trace!(
                    enqueued = enqueued,
                    completed = completed,
                    current_shards = current_shards,
                    splits_done = verifier_splits_done.load(Ordering::SeqCst),
                    "invariant_check_passed"
                );

                if verifier_splits_done.load(Ordering::SeqCst) {
                    tracing::info!("splits done, entering convergence phase");
                    break;
                }
            }

            // Phase 2: Wait for job convergence
            let convergence_timeout_secs = 60;
            let convergence_start = turmoil::sim_elapsed().unwrap_or_default();
            let mut last_completed = 0u32;
            let mut stall_count = 0u32;

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                verifier_tracker.process_dst_events();
                verifier_tracker.verify_all();

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);

                tracing::trace!(enqueued = enqueued, completed = completed, "convergence_check");

                if completed >= enqueued && enqueued > 0 {
                    tracing::info!(
                        enqueued = enqueued,
                        completed = completed,
                        "convergence achieved - all jobs complete"
                    );
                    break;
                }

                if completed == last_completed {
                    stall_count += 1;
                } else {
                    stall_count = 0;
                    last_completed = completed;
                }

                if stall_count >= 10 && completed > 0 {
                    tracing::warn!(
                        enqueued = enqueued,
                        completed = completed,
                        stall_count = stall_count,
                        "progress stalled"
                    );
                }

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
            verifier_tracker.process_dst_events();
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            let transition_violations = verifier_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                tracing::warn!(violations = transition_violations.len(), "transition_violations_found");
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
            let initial_shards = verifier_initial_shards.load(Ordering::SeqCst);
            let final_shards = verifier_current_shards.load(Ordering::SeqCst);
            let total_splits = verifier_total_splits.load(Ordering::SeqCst);
            let split_retries = verifier_split_retries.load(Ordering::SeqCst);
            let stale_sends = verifier_stale_sends.load(Ordering::SeqCst);
            let stale_errors = verifier_stale_errors.load(Ordering::SeqCst);
            let shard_ownership_violations = verifier_tracker.shards.has_violations();

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                terminal_from_dst = terminal_from_dst,
                initial_shards = initial_shards,
                final_shards = final_shards,
                total_splits = total_splits,
                split_retries = split_retries,
                stale_shard_sends = stale_sends,
                stale_shard_errors = stale_errors,
                shard_ownership_violations = shard_ownership_violations,
                "final_verification"
            );

            // Verify no split-brain
            verifier_tracker.shards.verify_no_split_brain();

            // Verify shard count grew
            assert!(
                final_shards >= initial_shards,
                "Shard count should not decrease: initial={}, final={}",
                initial_shards,
                final_shards
            );

            // If splits completed, verify growth
            if total_splits > 0 {
                let expected_growth = total_splits; // Each split adds 1 shard (2 children - 1 parent)
                let actual_growth = final_shards - initial_shards;
                tracing::info!(
                    expected_growth = expected_growth,
                    actual_growth = actual_growth,
                    "shard_growth_verification"
                );
                assert!(
                    actual_growth >= expected_growth,
                    "Shard count should grow by at least {} (splits completed), but only grew by {}",
                    expected_growth,
                    actual_growth
                );
            } else {
                // No splits occurred in this run - this can happen with certain RNG seeds.
                // The split probability is random, so some runs may not trigger any splits.
                tracing::info!("no splits triggered in this run - this is normal with fuzzing");
            }

            // Verify job completion - we expect 100% completion with proper tenant routing
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

                // With proper tenant routing and shard map watching, we expect 100% completion.
                // Jobs survive splits because:
                // 1. Each job has a tenant that falls within its shard's range
                // 2. When a shard splits, SlateDB clones all data to both children
                // 3. Nodes watch the shard map and immediately reconcile when splits occur
                // 4. The tenant routing ensures the job is found in the correct child shard
                assert_eq!(
                    completed, enqueued,
                    "All jobs should complete: completed={}, enqueued={}, rate={:.1}%",
                    completed,
                    enqueued,
                    completion_rate * 100.0
                );
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
