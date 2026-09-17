//! K8s close-failure recovery scenario: a shard's close fails while its
//! ownership flaps away from a node and back, as in a rolling restart.
//!
//! Timeline:
//! 1. `node-0` owns every shard; a producer and workers run jobs against it.
//! 2. The controller picks a shard that a joining `node-1` takes over, stops
//!    workers leasing from it, plants a backlog job on it, quiesces the rest of
//!    the client traffic to it, and arms an object-store write stall for that
//!    shard's data.
//! 3. `node-1` joins. `node-0` starts releasing the shard; its close hangs on
//!    the stalled flush.
//! 4. While that first close attempt is still in flight, `node-1` leaves again,
//!    so `node-0` is re-selected as the shard's owner with its close failing.
//! 5. The close times out. The controller then lifts the stall.
//!
//! `node-0` must drive the close to completion, keep the lease throughout, and
//! reopen a fresh shard that leases the backlog and everything enqueued after.
//!
//! Client traffic to the target shard is quiesced before the stall arms because
//! the DST event protocol cannot describe a request whose handler is dropped
//! while its write is stalled: the event stays pending, neither confirmed nor
//! cancelled, whether or not the write later lands.
//!
//! Invariants verified:
//! - **jobCompleteness**: every job accepted before, during, and after the
//!   stall reaches a terminal state once the stall is lifted. That includes the
//!   backlog job, which can only be leased from the reopened shard
//! - **noClosingShardServed**: while the close is pending, a request for the
//!   shard is turned away by its node: `NOT_FOUND` with a redirect while the
//!   joining node is the computed owner, a retryable `UNAVAILABLE` once this
//!   node is again. No shard handed out by any node's factory has had `close()`
//!   called on it
//! - **closeFailedFirst**: the close is still pending after the first attempt's
//!   deadline, so the run exercises a failed close and not just a slow one
//! - **leaseHeldWhileClosing**: a node's lease on a shard is never released
//!   while that node's factory reports the shard as closing
//! - **noSplitBrain** / **closeBeforeRelease**: via DST events
//! - **reopenExercised**: at least one `ShardReopened` for the stalled shard, so
//!   the run cannot pass without taking the reopen path

use crate::helpers::{
    EnqueueRequest, GetJobRequest, HashMap, InvariantTracker, JobStatus, LeaseTasksRequest,
    ReportOutcomeRequest, SerializedBytes, create_turmoil_client, dst_turmoilfs_database_template,
    get_seed, report_outcome_request, run_scenario_impl, serialized_bytes, turmoil,
};
use crate::mock_k8s::{MockK8sBackend, MockK8sState};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::cluster_client::ClientConfig;
use silo::coordination::{
    Coordinator, K8sCoordinator, MemberInfo, compute_desired_shards_for_node,
};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::Task;
use silo::server::run_server_with_incoming;
use silo::settings::{AppConfig, GubernatorSettings, LoggingConfig, WebUiConfig};
use silo::shard_range::ShardId;
use silo::turmoil_object_store::{release_write_stalls, stall_writes_under};
use std::collections::{BTreeSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;
use turmoil::net::TcpListener;

const NUM_SHARDS: u32 = 4;
const BASE_PORT: u16 = 9980;
const CLUSTER_PREFIX: &str = "silo-close-recovery";
const NAMESPACE: &str = "default";
const LEASE_DURATION_SECS: i64 = 6;

/// Storage root for TurmoilFs - shared across all nodes so shard data is accessible
const STORAGE_ROOT: &str = "/data/silo-close-recovery-shards";

/// The node that owns every shard at the start and is re-selected mid-close.
const STABLE_NODE: u32 = 0;
/// The node whose join and departure flap shard ownership.
const FLAPPING_NODE: u32 = 1;

/// Factory close timeout. Long enough that the flapping node's whole departure
/// (up to its coordinator's 5s guard-shutdown wait) lands inside the stable
/// node's first close attempt, short next to the 30s default.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long after the flap the stall is held. It outlasts the first close
/// attempt, so the close provably times out, and ends well inside the reopen
/// timeout.
const STALL_HOLD_AFTER_FLAP: Duration = Duration::from_secs(13);

/// How long the target shard keeps taking enqueues after workers stop leasing
/// from it, so a backlog of queued jobs rides through the close failure.
const BACKLOG_WINDOW: Duration = Duration::from_secs(1);

/// How long client writes to the target shard are paused before the stall
/// arms. A client that passed the gate connects to the healthy stable node on
/// its first attempt (2s timeout), a worker then sleeps up to 80ms, and the
/// request takes at most 2s, so this covers every request that passed the gate.
const WRITE_DRAIN: Duration = Duration::from_millis(4500);

/// How long any single scenario step may take before the run fails with a
/// message naming the step, rather than as a bare simulation timeout.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Gates client traffic to the target shard around the stall.
#[derive(Default)]
struct TargetShardGate {
    target: Mutex<Option<ShardId>>,
    leasing_paused: AtomicBool,
    writes_paused: AtomicBool,
}

impl TargetShardGate {
    fn target(&self) -> Option<ShardId> {
        *self.target.lock().unwrap()
    }

    fn leasing_paused_for(&self, shard_id: &ShardId) -> bool {
        self.leasing_paused.load(Ordering::SeqCst) && self.target() == Some(*shard_id)
    }

    fn writes_paused_for(&self, shard_id: &ShardId) -> bool {
        self.writes_paused.load(Ordering::SeqCst) && self.target() == Some(*shard_id)
    }
}

/// Node lifecycle state communicated via watch channel
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeState {
    Inactive,
    Active,
    Shutdown,
}

/// Every node's factory, so the invariant checker can inspect what each node
/// serves. Ordered by registration for deterministic iteration.
type FactoryRegistry = Arc<Mutex<Vec<(String, Arc<ShardFactory>)>>>;

struct ScenarioConfig {
    num_jobs: u32,
    num_workers: u32,
    /// Range of delays between enqueues (min_ms, max_ms)
    enqueue_gap_ms: (u64, u64),
    /// How long jobs run before the stall is armed
    stall_start_ms: u64,
}

impl ScenarioConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        Self {
            // Enough jobs, spaced widely enough, that enqueues land before,
            // during, and after the stall window.
            num_jobs: rng.random_range(40..=60),
            num_workers: rng.random_range(2..=3),
            enqueue_gap_ms: (rng.random_range(150..300), rng.random_range(400..700)),
            stall_start_ms: rng.random_range(3500..6000),
        }
    }
}

fn job_id(job_num: u32) -> String {
    format!("close-recovery-{job_num}")
}

fn node_id(node_num: u32) -> String {
    format!("node-{node_num}")
}

fn node_uri(node_num: u32) -> String {
    format!("http://node{}:{}", node_num, BASE_PORT + node_num as u16)
}

fn owner_node_num(owner: Option<&String>) -> Option<u32> {
    owner
        .and_then(|o| o.strip_prefix("node-"))
        .and_then(|n| n.parse::<u32>().ok())
}

struct NodeServerHandle {
    coordinator: Arc<K8sCoordinator<MockK8sBackend>>,
    /// Shutdown sender - must be kept alive to prevent server from shutting down
    _shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

/// Set up a server on a node with K8s coordination and a short close timeout.
///
/// `wait_for_convergence` is false for the flapping node: it can never
/// converge while the stable node's close is stalled, and it must stay
/// responsive to its shutdown signal.
async fn setup_node_server(
    node_num: u32,
    k8s_state: Arc<MockK8sState>,
    registry: FactoryRegistry,
    wait_for_convergence: bool,
) -> turmoil::Result<NodeServerHandle> {
    let node_id = node_id(node_num);
    let port = BASE_PORT + node_num as u16;

    let mut factory = ShardFactory::new(
        dst_turmoilfs_database_template(std::path::Path::new(STORAGE_ROOT)),
        MockGubernatorClient::new_arc(),
        None,
    );
    factory.set_close_timeout(CLOSE_TIMEOUT);
    let factory = Arc::new(factory);
    registry
        .lock()
        .unwrap()
        .push((node_id.clone(), Arc::clone(&factory)));

    let (coordinator, _handle) = K8sCoordinator::start_with_backend(
        MockK8sBackend::new(k8s_state, NAMESPACE),
        silo::coordination::K8sCoordinatorConfig {
            namespace: NAMESPACE.to_string(),
            cluster_prefix: CLUSTER_PREFIX.to_string(),
            node_id: node_id.clone(),
            grpc_addr: format!("http://{}:{}", node_id, port),
            initial_shard_count: NUM_SHARDS,
            lease_duration_secs: LEASE_DURATION_SECS,
            placement_rings: Vec::new(),
        },
        Arc::clone(&factory),
    )
    .await
    .map_err(|e| e.to_string())?;
    let coordinator = Arc::new(coordinator);

    if wait_for_convergence {
        let converged = coordinator.wait_converged(Duration::from_secs(30)).await;
        tracing::info!(node_id = %node_id, converged, "initial convergence");
    }

    let cfg = AppConfig {
        server: silo::settings::ServerConfig {
            grpc_addr: format!("0.0.0.0:{}", port),
            dev_mode: false,
            statement_timeout_ms: Some(5_000),
            auth_token: None,
            ..Default::default()
        },
        coordination: silo::settings::CoordinationConfig::default(),
        tenancy: silo::settings::TenancyConfig { enabled: true },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        logging: LoggingConfig::default(),
        metrics: silo::settings::MetricsConfig::default(),
        // Database template for the server - actual shards are managed by the coordinator's factory
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

/// Wait for the cluster's shard map to exist in the mock K8s API.
async fn load_shard_map(k8s_state: &MockK8sState) -> silo::shard_range::ShardMap {
    loop {
        match k8s_state.get_shard_map(NAMESPACE, CLUSTER_PREFIX).await {
            Ok(Some(map)) if !map.shards().is_empty() => return map,
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// Every shard paired with a tenant whose hash falls in its range, in shard
/// map order. The server rejects an enqueue whose tenant is outside the
/// addressed shard's range.
async fn discover_shard_tenants(k8s_state: &MockK8sState) -> Vec<(ShardId, String)> {
    let shard_map = load_shard_map(k8s_state).await;
    shard_map
        .shards()
        .iter()
        .map(|shard| {
            let tenant = (0u32..)
                .map(|n| format!("close-recovery-tenant-{n}"))
                .find(|tenant| shard.contains(tenant))
                .expect("some tenant hashes into every shard's range");
            (shard.id, tenant)
        })
        .collect()
}

/// The shard the flapping node takes from the stable node when it joins.
/// Bounded-load placement gives a second node half of the shards, so one
/// always exists; the smallest id keeps the choice deterministic.
async fn pick_target_shard(k8s_state: &MockK8sState) -> ShardId {
    let shard_map = load_shard_map(k8s_state).await;
    let members: Vec<MemberInfo> = [STABLE_NODE, FLAPPING_NODE]
        .into_iter()
        .map(|node_num| MemberInfo {
            node_id: node_id(node_num),
            grpc_addr: node_uri(node_num),
            startup_time_ms: None,
            hostname: None,
            placement_rings: Vec::new(),
        })
        .collect();
    let shards: Vec<_> = shard_map.shards().iter().collect();
    let moving: BTreeSet<ShardId> =
        compute_desired_shards_for_node(&shards, &node_id(FLAPPING_NODE), &members)
            .into_iter()
            .collect();
    *moving
        .first()
        .expect("a joining node should take over at least one shard")
}

/// The shard and tenant a job is enqueued on. Producer jobs go round-robin
/// across shards; the job numbered `num_jobs` is the controller's backlog job
/// on the target shard.
fn job_route(
    shards: &[(ShardId, String)],
    target: Option<ShardId>,
    num_jobs: u32,
    job_num: u32,
) -> (ShardId, String) {
    if job_num == num_jobs
        && let Some(route) = shards.iter().find(|(shard_id, _)| Some(*shard_id) == target)
    {
        return route.clone();
    }
    shards[(job_num as usize) % shards.len()].clone()
}

/// Try once to enqueue a job on its shard's current owner. Returns true when
/// the cluster holds the job: a timed-out enqueue can still have landed, and
/// its retry then finds the job in place.
async fn try_enqueue(
    k8s_state: &MockK8sState,
    client_config: &ClientConfig,
    shard_id: ShardId,
    tenant: String,
    job_num: u32,
) -> bool {
    let job_id = job_id(job_num);
    let owner = k8s_state
        .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, &shard_id)
        .await
        .ok()
        .flatten();
    let Some(target) = owner_node_num(owner.as_ref()) else {
        return false;
    };
    let Ok(mut client) = create_turmoil_client(&node_uri(target), client_config).await else {
        return false;
    };

    tracing::trace!(job_id = %job_id, shard = %shard_id, target, "enqueue");
    let result = client
        .enqueue(tonic::Request::new(EnqueueRequest {
            shard: shard_id.to_string(),
            id: job_id.clone(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({ "idx": job_num })).unwrap(),
                )),
            }),
            limits: vec![],
            tenant: Some(tenant),
            metadata: HashMap::new(),
            task_group: "default".to_string(),
        }))
        .await;
    match result {
        Ok(_) => {
            tracing::trace!(job_id = %job_id, "enqueue_accepted");
            true
        }
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            tracing::trace!(job_id = %job_id, "enqueue_already_accepted");
            true
        }
        Err(status) => {
            tracing::trace!(job_id = %job_id, code = ?status.code(), "enqueue_retry_later");
            false
        }
    }
}

/// Poll `condition` every 10ms of simulated time until it holds. Panics, naming
/// `step`, when it has not held within [`STEP_DEADLINE`].
async fn wait_for_step<F, Fut>(step: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = turmoil::sim_elapsed().unwrap_or_default() + STEP_DEADLINE;
    while !condition().await {
        assert!(
            turmoil::sim_elapsed().unwrap_or_default() < deadline,
            "scenario step did not happen within {}s: {step}",
            STEP_DEADLINE.as_secs()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Fetch a job's status from its shard's current lease holder.
async fn get_job_status(
    k8s_state: &MockK8sState,
    client_config: &ClientConfig,
    shard_id: ShardId,
    tenant: String,
    job_num: u32,
) -> Result<i32, tonic::Status> {
    let owner = k8s_state
        .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, &shard_id)
        .await
        .ok()
        .flatten();
    let target = owner_node_num(owner.as_ref())
        .ok_or_else(|| tonic::Status::not_found("shard has no lease holder"))?;
    let mut client = create_turmoil_client(&node_uri(target), client_config)
        .await
        .map_err(|e| tonic::Status::unknown(format!("connect failed: {e}")))?;
    client
        .get_job(tonic::Request::new(GetJobRequest {
            shard: shard_id.to_string(),
            id: job_id(job_num),
            tenant: Some(tenant),
            include_attempts: false,
        }))
        .await
        .map(|resp| resp.into_inner().status)
}

/// Whether the job has reached a terminal status. Any failure to find out
/// counts as "not yet".
async fn job_is_terminal(
    k8s_state: &MockK8sState,
    client_config: &ClientConfig,
    shard_id: ShardId,
    tenant: String,
    job_num: u32,
) -> bool {
    get_job_status(k8s_state, client_config, shard_id, tenant, job_num)
        .await
        .is_ok_and(|status| {
            status == JobStatus::Succeeded as i32
                || status == JobStatus::Failed as i32
                || status == JobStatus::Cancelled as i32
        })
}

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("k8s_close_failure_recovery", seed, 240, |sim| {
        let config = ScenarioConfig::from_seed(seed);
        tracing::info!(
            num_jobs = config.num_jobs,
            num_workers = config.num_workers,
            enqueue_gap_ms = ?config.enqueue_gap_ms,
            stall_start_ms = config.stall_start_ms,
            "k8s_close_failure_recovery_config"
        );

        let scenario_done = Arc::new(AtomicBool::new(false));
        let stall_lifted = Arc::new(AtomicBool::new(false));
        let accepted_jobs: Arc<Mutex<BTreeSet<String>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let gate = Arc::new(TargetShardGate::default());

        let k8s_state = MockK8sState::new();
        let registry: FactoryRegistry = Arc::new(Mutex::new(Vec::new()));
        let tracker = Arc::new(InvariantTracker::new());
        let client_config = ClientConfig::for_dst();

        let (stable_tx, stable_rx) = watch::channel(NodeState::Active);
        let (flapping_tx, flapping_rx) = watch::channel(NodeState::Inactive);
        let stable_tx = Arc::new(stable_tx);
        let flapping_tx = Arc::new(flapping_tx);

        for (node_num, state_rx) in [(STABLE_NODE, stable_rx), (FLAPPING_NODE, flapping_rx)] {
            let k8s_state = k8s_state.clone();
            let registry = Arc::clone(&registry);
            let host_name: &'static str = Box::leak(format!("node{}", node_num).into_boxed_str());
            sim.host(host_name, move || {
                let k8s_state = k8s_state.clone();
                let registry = Arc::clone(&registry);
                let mut state_rx = state_rx.clone();
                async move {
                    let node_id = node_id(node_num);
                    loop {
                        let state = *state_rx.borrow();
                        match state {
                            NodeState::Active => break,
                            NodeState::Shutdown => return Ok(()),
                            NodeState::Inactive => {
                                if state_rx.changed().await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    tracing::info!(node_id = %node_id, "node activated");

                    let handle = setup_node_server(
                        node_num,
                        k8s_state,
                        registry,
                        node_num == STABLE_NODE,
                    )
                    .await?;

                    while *state_rx.borrow() != NodeState::Shutdown {
                        if state_rx.changed().await.is_err() {
                            break;
                        }
                    }

                    tracing::info!(node_id = %node_id, "shutting down");
                    if let Err(e) = handle.coordinator.shutdown().await {
                        tracing::warn!(node_id = %node_id, error = %e, "shutdown error");
                    }
                    Ok(())
                }
            });
        }

        // Producer: enqueues every job, retrying until the cluster accepts it.
        // A job whose shard is unavailable goes to the back of the queue, so the
        // stalled shard does not hold up enqueues to the healthy ones.
        let producer_k8s_state = Arc::clone(&k8s_state);
        let producer_config = client_config.clone();
        let producer_accepted = Arc::clone(&accepted_jobs);
        let producer_scenario_done = Arc::clone(&scenario_done);
        let producer_gate = Arc::clone(&gate);
        let num_jobs = config.num_jobs;
        let enqueue_gap_ms = config.enqueue_gap_ms;
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));
            let shards = discover_shard_tenants(&producer_k8s_state).await;

            let mut pending: VecDeque<u32> = (0..num_jobs).collect();
            while let Some(i) = pending.pop_front() {
                if producer_scenario_done.load(Ordering::SeqCst) {
                    break;
                }
                let gap = rng.random_range(enqueue_gap_ms.0..=enqueue_gap_ms.1);
                tokio::time::sleep(Duration::from_millis(gap)).await;

                let (shard_id, tenant) = job_route(&shards, None, num_jobs, i);
                let job_id = job_id(i);
                if producer_gate.writes_paused_for(&shard_id) {
                    pending.push_back(i);
                    continue;
                }

                if try_enqueue(&producer_k8s_state, &producer_config, shard_id, tenant, i).await {
                    producer_accepted.lock().unwrap().insert(job_id);
                } else {
                    pending.push_back(i);
                }
            }

            tracing::trace!(
                accepted = producer_accepted.lock().unwrap().len(),
                "producer_done"
            );
            Ok(())
        });

        // Workers: lease from each shard's current owner and report success.
        for worker_num in 0..config.num_workers {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("close-recovery-worker-{worker_num}");
            let worker_done_flag = Arc::clone(&scenario_done);
            let worker_config = client_config.clone();
            let worker_k8s_state = Arc::clone(&k8s_state);
            let worker_gate = Arc::clone(&gate);
            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());

            sim.client(client_name, async move {
                tokio::time::sleep(Duration::from_millis(2000 + worker_num as u64 * 100)).await;
                let mut rng = StdRng::seed_from_u64(worker_seed);
                let shard_ids: Vec<ShardId> = discover_shard_tenants(&worker_k8s_state)
                    .await
                    .into_iter()
                    .map(|(shard_id, _)| shard_id)
                    .collect();
                let mut processing: VecDeque<(Task, ShardId)> = VecDeque::new();

                while !worker_done_flag.load(Ordering::SeqCst) {
                    let shard_id = shard_ids[rng.random_range(0..shard_ids.len())];
                    let owner = worker_k8s_state
                        .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, &shard_id)
                        .await
                        .ok()
                        .flatten();
                    if !worker_gate.leasing_paused_for(&shard_id)
                        && let Some(target) = owner_node_num(owner.as_ref())
                        && let Ok(mut client) =
                            create_turmoil_client(&node_uri(target), &worker_config).await
                    {
                        let leased = client
                            .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                                shard: Some(shard_id.to_string()),
                                worker_id: worker_id.clone(),
                                max_tasks: 3,
                                task_group: "default".to_string(),
                            }))
                            .await;
                        match leased {
                            Ok(resp) => {
                                for task in resp.into_inner().tasks {
                                    tracing::trace!(worker = %worker_id, job_id = %task.job_id, shard = %shard_id, "lease");
                                    processing.push_back((task, shard_id));
                                }
                            }
                            Err(status) => {
                                tracing::trace!(worker = %worker_id, code = ?status.code(), "lease_failed");
                            }
                        }
                    }

                    // Report one leased task per round. A report that cannot
                    // land is dropped: the lease expires and the job's attempt
                    // fails, which is a terminal state too. A task on the gated
                    // shard waits its turn at the back of the queue.
                    let next = processing.pop_front();
                    let next = match next {
                        Some((task, task_shard)) if worker_gate.writes_paused_for(&task_shard) => {
                            processing.push_back((task, task_shard));
                            None
                        }
                        other => other,
                    };
                    if let Some((task, task_shard)) = next {
                        tokio::time::sleep(Duration::from_millis(rng.random_range(20..80))).await;
                        let owner = worker_k8s_state
                            .find_shard_owner(NAMESPACE, CLUSTER_PREFIX, &task_shard)
                            .await
                            .ok()
                            .flatten();
                        if let Some(target) = owner_node_num(owner.as_ref())
                            && let Ok(mut client) =
                                create_turmoil_client(&node_uri(target), &worker_config).await
                        {
                            let reported = client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: task_shard.to_string(),
                                    task_id: task.id.clone(),
                                    outcome: Some(report_outcome_request::Outcome::Success(
                                        SerializedBytes {
                                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                                rmp_serde::to_vec(&serde_json::json!("done"))
                                                    .unwrap(),
                                            )),
                                        },
                                    )),
                                    tenant_id: task.tenant_id.clone(),
                                }))
                                .await;
                            match reported {
                                Ok(_) => {
                                    tracing::trace!(worker = %worker_id, job_id = %task.job_id, "complete")
                                }
                                Err(status) => {
                                    tracing::trace!(worker = %worker_id, job_id = %task.job_id, code = ?status.code(), "report_failed")
                                }
                            }
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(rng.random_range(50..150))).await;
                }
                tracing::trace!(worker = %worker_id, "worker_done");
                Ok(())
            });
        }

        // Controller: arms the stall and flaps the target shard's ownership.
        let controller_k8s_state = Arc::clone(&k8s_state);
        let controller_registry = Arc::clone(&registry);
        let controller_gate = Arc::clone(&gate);
        let controller_config = client_config.clone();
        let controller_accepted = Arc::clone(&accepted_jobs);
        let controller_stall_lifted = Arc::clone(&stall_lifted);
        let controller_flapping_tx = Arc::clone(&flapping_tx);
        let stall_start_ms = config.stall_start_ms;
        sim.client("controller", async move {
            tokio::time::sleep(Duration::from_millis(stall_start_ms)).await;

            let shard_id = pick_target_shard(&controller_k8s_state).await;
            *controller_gate.target.lock().unwrap() = Some(shard_id);

            // Quiesce client traffic to the target: leasing stops first so the
            // enqueues that follow pile up as a backlog, then writes stop and
            // drain, so no request is in flight when the stall arms.
            controller_gate.leasing_paused.store(true, Ordering::SeqCst);

            // With leasing stopped, this job stays queued on the target through
            // the close failure: only the reopened shard can lease it.
            let shards = discover_shard_tenants(&controller_k8s_state).await;
            let (_, backlog_tenant) = job_route(&shards, Some(shard_id), num_jobs, num_jobs);
            wait_for_step("the target shard accepts the backlog job", || {
                try_enqueue(
                    &controller_k8s_state,
                    &controller_config,
                    shard_id,
                    backlog_tenant.clone(),
                    num_jobs,
                )
            })
            .await;
            controller_accepted.lock().unwrap().insert(job_id(num_jobs));
            tracing::trace!(job_id = %job_id(num_jobs), shard = %shard_id, "backlog_planted");

            tokio::time::sleep(BACKLOG_WINDOW).await;
            controller_gate.writes_paused.store(true, Ordering::SeqCst);
            tokio::time::sleep(WRITE_DRAIN).await;

            let stalled_prefix = std::path::Path::new(STORAGE_ROOT).join(shard_id.to_string());
            stall_writes_under(&stalled_prefix);
            tracing::trace!(shard = %shard_id, "stall_armed");

            // The flapping node joins and takes the target shard from the
            // stable node, whose close then hangs on the stalled flush.
            let _ = controller_flapping_tx.send(NodeState::Active);
            let stable_factory = controller_registry
                .lock()
                .unwrap()
                .iter()
                .find(|(id, _)| *id == node_id(STABLE_NODE))
                .map(|(_, factory)| Arc::clone(factory))
                .expect("the stable node registers its factory at startup");
            wait_for_step(
                "node-0 begins closing the target shard after node-1 joins",
                || async { stable_factory.is_closing(&shard_id) },
            )
            .await;
            tracing::trace!(shard = %shard_id, "close_began");

            // The stable node started releasing because the flapping node is
            // now the shard's computed owner, so it redirects requests there.
            let redirected = get_job_status(
                &controller_k8s_state,
                &controller_config,
                shard_id,
                backlog_tenant.clone(),
                num_jobs,
            )
            .await;
            let redirect = redirected.as_ref().err().map(|status| {
                let owner = status
                    .metadata()
                    .get(silo::server::SHARD_OWNER_NODE_METADATA_KEY)
                    .and_then(|node| node.to_str().ok())
                    .map(str::to_string);
                (status.code(), owner)
            });
            assert_eq!(
                redirect,
                Some((tonic::Code::NotFound, Some(node_id(FLAPPING_NODE)))),
                "INVARIANT VIOLATION (noClosingShardServed): a request for shard {shard_id} while its close is pending and another node is its computed owner should get NOT_FOUND redirecting there, got {redirected:?}"
            );
            tracing::trace!(shard = %shard_id, "close_pending_probe_redirected");

            // It leaves again while that first close attempt is in flight, so
            // the stable node is re-selected with its close failing.
            let _ = controller_flapping_tx.send(NodeState::Shutdown);
            tracing::trace!(shard = %shard_id, "ownership_flapped_back");

            // Just past the first attempt's deadline the guard is inside its
            // first backoff: the close has failed and is still pending.
            let past_first_attempt = CLOSE_TIMEOUT + Duration::from_millis(500);
            tokio::time::sleep(past_first_attempt).await;
            assert!(
                stable_factory.is_closing(&shard_id),
                "INVARIANT VIOLATION (closeFailedFirst): the close of shard {shard_id} is not pending after its first attempt's deadline"
            );
            let probe = get_job_status(
                &controller_k8s_state,
                &controller_config,
                shard_id,
                backlog_tenant.clone(),
                num_jobs,
            )
            .await;
            assert_eq!(
                probe.as_ref().err().map(tonic::Status::code),
                Some(tonic::Code::Unavailable),
                "INVARIANT VIOLATION (noClosingShardServed): a request for shard {shard_id} while its close is pending should get a retryable UNAVAILABLE, got {probe:?}"
            );
            tracing::trace!(shard = %shard_id, "close_pending_probe_unavailable");

            tokio::time::sleep(STALL_HOLD_AFTER_FLAP - past_first_attempt).await;
            release_write_stalls();
            controller_gate.writes_paused.store(false, Ordering::SeqCst);
            controller_gate.leasing_paused.store(false, Ordering::SeqCst);
            controller_stall_lifted.store(true, Ordering::SeqCst);
            tracing::trace!(shard = %shard_id, "stall_lifted");
            Ok(())
        });

        // Checker: continuously verifies what every node's factory serves and
        // that a closing shard's lease stays with its node.
        let checker_k8s_state = Arc::clone(&k8s_state);
        let checker_registry = Arc::clone(&registry);
        let checker_done_flag = Arc::clone(&scenario_done);
        sim.client("checker", async move {
            let shard_ids: Vec<ShardId> = discover_shard_tenants(&checker_k8s_state)
                .await
                .into_iter()
                .map(|(shard_id, _)| shard_id)
                .collect();

            while !checker_done_flag.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(50)).await;

                // Holders are read before `is_closing`: an entry stays closing
                // until its close completes, so a closing entry seen afterwards
                // was closing, or still open, when the holders were read.
                let holders = checker_k8s_state
                    .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
                    .await;
                let factories: Vec<(String, Arc<ShardFactory>)> =
                    checker_registry.lock().unwrap().clone();
                for (node_id, factory) in &factories {
                    for (shard_id, shard) in factory.instances() {
                        assert!(
                            !shard.is_closing(),
                            "INVARIANT VIOLATION (noClosingShardServed): {node_id} serves shard {shard_id} after close() was called on it"
                        );
                    }
                    for shard_id in &shard_ids {
                        if factory.is_closing(shard_id) {
                            assert_eq!(
                                holders.get(shard_id),
                                Some(node_id),
                                "INVARIANT VIOLATION (leaseHeldWhileClosing): the lease on shard {shard_id} is not with {node_id} while its close is pending"
                            );
                        }
                    }
                }
            }
            Ok(())
        });

        // Verifier: waits for every accepted job to finish, then checks invariants.
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_accepted = Arc::clone(&accepted_jobs);
        let verifier_stall_lifted = Arc::clone(&stall_lifted);
        let verifier_num_jobs = config.num_jobs;
        let verifier_scenario_done = Arc::clone(&scenario_done);
        let verifier_gate = Arc::clone(&gate);
        let verifier_k8s_state = Arc::clone(&k8s_state);
        let verifier_config = client_config.clone();
        let verifier_stable_tx = Arc::clone(&stable_tx);
        let verifier_flapping_tx = Arc::clone(&flapping_tx);
        sim.client("verifier", async move {
            // The controller's whole sequence, from arming to lifting the stall,
            // fits well inside this.
            let stall_deadline = Duration::from_secs(90);
            while !verifier_stall_lifted.load(Ordering::SeqCst) {
                assert!(
                    turmoil::sim_elapsed().unwrap_or_default() < stall_deadline,
                    "the controller never lifted the stall within {}s",
                    stall_deadline.as_secs()
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // Every job the producer submits must finish now that the stall is
            // gone. Convergence is read from the cluster itself: draining DST
            // events while work is in flight can take a confirmed event ahead
            // of an earlier pending one, so those are validated once, after
            // the cluster has gone quiet.
            let shards = discover_shard_tenants(&verifier_k8s_state).await;
            let mut unfinished: VecDeque<u32> = (0..=verifier_num_jobs).collect();
            let convergence_deadline =
                turmoil::sim_elapsed().unwrap_or_default() + Duration::from_secs(120);
            while !unfinished.is_empty()
                && turmoil::sim_elapsed().unwrap_or_default() < convergence_deadline
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
                for _ in 0..unfinished.len() {
                    let Some(i) = unfinished.pop_front() else {
                        break;
                    };
                    let (shard_id, tenant) =
                        job_route(&shards, verifier_gate.target(), verifier_num_jobs, i);
                    if !job_is_terminal(&verifier_k8s_state, &verifier_config, shard_id, tenant, i)
                        .await
                    {
                        unfinished.push_back(i);
                    }
                }
                tracing::trace!(unfinished = unfinished.len(), "convergence_check");
            }

            verifier_scenario_done.store(true, Ordering::SeqCst);
            let _ = verifier_stable_tx.send(NodeState::Shutdown);
            let _ = verifier_flapping_tx.send(NodeState::Shutdown);
            tokio::time::sleep(Duration::from_secs(3)).await;

            verifier_tracker.process_and_validate();
            verifier_tracker.verify_all();

            // A job the cluster never accepts is as much a violation as one it
            // accepts and never runs, so the check covers every job id.
            let all_jobs: Vec<String> = (0..=verifier_num_jobs).map(job_id).collect();
            let unfinished: Vec<String> = all_jobs
                .iter()
                .filter(|job_id| !verifier_tracker.jobs.is_terminal(job_id))
                .cloned()
                .collect();
            let never_accepted: Vec<&String> = {
                let accepted = verifier_accepted.lock().unwrap();
                unfinished
                    .iter()
                    .filter(|job_id| !accepted.contains(*job_id))
                    .collect()
            };
            tracing::info!(
                jobs = all_jobs.len(),
                unfinished = unfinished.len(),
                never_accepted = never_accepted.len(),
                "final_verification"
            );
            assert!(
                unfinished.is_empty(),
                "INVARIANT VIOLATION (jobCompleteness): {} of {} jobs never reached a terminal state ({} of them were never accepted): {:?}",
                unfinished.len(),
                all_jobs.len(),
                never_accepted.len(),
                unfinished
            );

            let target = verifier_gate
                .target()
                .expect("the controller should have picked a target shard");
            let reopens = verifier_tracker.shards.reopen_count(&target.to_string());
            assert!(
                reopens >= 1,
                "INVARIANT VIOLATION (reopenExercised): shard {target} was never reopened, so the run did not exercise close-failure recovery"
            );

            tracing::trace!(reopens, "verifier_done");
            Ok(())
        });
    });
}
