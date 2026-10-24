//! K8s membership stall scenario: one Kubernetes call made by a healthy node
//! hangs for longer than the membership lease TTL.
//!
//! This is what production saw several times a day: a pod stopped renewing its
//! membership lease for one kube-rs watch period (290s) because a single API
//! call never returned, while the pod itself stayed up and kept its shards.
//!
//! The scenario runs the stall twice against the same three-node cluster, once
//! per call the coordinator makes on a timer:
//!
//! 1. **renewal write**: the `replace_lease` that renews the membership lease
//!    hangs. One call hangs and the next one would be answered normally.
//! 2. **safety-reconcile list**: `list_leases` of the member leases hangs.
//!    Every list the node starts inside the window hangs, which includes the
//!    one the 5s safety reconcile makes.
//!
//! Each hung call sleeps for the stall and then returns successfully, the way
//! the production call did. The hang lasts well past the lease TTL.
//!
//! What a stall must not do, and what the scenario asserts for each phase:
//! - **membershipKept**: no healthy peer ever drops the stalled node from its
//!   member list
//! - **shardsServed**: every enqueue for one of the stalled node's shards, sent
//!   to whichever node a healthy peer computes as the owner, is accepted. None
//!   is answered `UNAVAILABLE` "shard not ready"
//! - **faultInjected**: at least one call really did hang, so the phase cannot
//!   pass without exercising the stall
//!
//! When the invariants fail the panic message carries the timeline: when the
//! node was dropped, for how long, how many enqueues were refused, and whether
//! the new computed owner sat in `Acquiring` behind the stalled node's
//! permanent shard lease.
//!
//! `DST_MEMBERSHIP_STALL_SECS` overrides the stall length (default 30s) to
//! replay production's 290s.

use crate::helpers::{
    EnqueueRequest, HashMap, SerializedBytes, SiloClient, create_turmoil_client,
    dst_turmoilfs_database_template, get_seed, run_scenario_impl, serialized_bytes, turmoil,
};
use crate::mock_k8s::{MockK8sBackend, MockK8sState, StalledCall};
use silo::cluster_client::ClientConfig;
use silo::coordination::{Coordinator, K8sCoordinator, ShardPhase};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::server::run_server_with_incoming;
use silo::settings::{AppConfig, GubernatorSettings, LoggingConfig, WebUiConfig};
use silo::shard_range::ShardId;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use turmoil::net::TcpListener;

const NUM_NODES: u32 = 3;
const NUM_SHARDS: u32 = 8;
const BASE_PORT: u16 = 9940;
const CLUSTER_PREFIX: &str = "silo-member-stall";
const NAMESPACE: &str = "default";
const STORAGE_ROOT: &str = "/data/silo-member-stall-shards";

/// The membership lease TTL production runs with. Renewal is every second.
const LEASE_DURATION_SECS: i64 = 5;

/// The node whose Kubernetes call hangs.
const STALLED_NODE: u32 = 1;

/// How long the planted call hangs unless `DST_MEMBERSHIP_STALL_SECS` says
/// otherwise. Six lease TTLs: long enough that a node which stops renewing is
/// dropped for most of it.
const DEFAULT_STALL: Duration = Duration::from_secs(30);

/// How long a phase keeps observing after the stall ends, so a recovery is
/// seen as well as an outage.
const RECOVERY_WINDOW: Duration = Duration::from_secs(15);

/// How often membership is sampled and the stalled node's shards are probed.
const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// How long any single wait may take before the run fails with a message
/// naming the step, rather than as a bare simulation timeout.
const STEP_DEADLINE: Duration = Duration::from_secs(60);

type Node = Arc<K8sCoordinator<MockK8sBackend>>;

/// Every node's coordinator, indexed by node number, so the controller can
/// read each node's own view of membership and placement.
type NodeRegistry = Arc<Mutex<Vec<Option<Node>>>>;

fn stall_duration() -> Duration {
    std::env::var("DST_MEMBERSHIP_STALL_SECS")
        .ok()
        .and_then(|secs| secs.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_STALL)
}

fn node_id(node_num: u32) -> String {
    format!("node-{node_num}")
}

fn node_uri(node_num: u32) -> String {
    format!("http://node{}:{}", node_num, BASE_PORT + node_num as u16)
}

fn node_num_of(node_id: &str) -> Option<u32> {
    node_id.strip_prefix("node-").and_then(|n| n.parse().ok())
}

/// The two timer-driven calls whose hang the scenario replays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StallPhase {
    RenewalWrite,
    SafetyReconcileList,
}

impl StallPhase {
    fn name(self) -> &'static str {
        match self {
            StallPhase::RenewalWrite => "renewal replace_lease",
            StallPhase::SafetyReconcileList => "safety-reconcile list_leases",
        }
    }

    /// Plant this phase's hang on the stalled node's Kubernetes client.
    fn plant(self, backend: &MockK8sBackend, stall: Duration) {
        match self {
            StallPhase::RenewalWrite => backend.stall_calls(
                StalledCall::ReplaceLease,
                format!("-member-{}", node_id(STALLED_NODE)),
                stall,
                Some(1),
            ),
            StallPhase::SafetyReconcileList => {
                backend.stall_calls(StalledCall::ListLeases, "silo.dev/type=member", stall, None)
            }
        }
    }
}

/// A span of the phase, as offsets from the moment the stall began.
#[derive(Clone, Copy, Debug)]
struct Span {
    from: Duration,
    until: Duration,
}

impl Span {
    fn extend(span: &mut Option<Span>, at: Duration) {
        match span {
            Some(span) => span.until = at,
            None => {
                *span = Some(Span {
                    from: at,
                    until: at,
                })
            }
        }
    }
}

/// What one phase observed.
#[derive(Debug)]
struct PhaseObservation {
    phase: StallPhase,
    stalled_calls: u64,
    /// When a healthy peer's member list was missing the stalled node.
    dropped: Option<Span>,
    /// Enqueues sent to the node a healthy peer computes as the shard's owner.
    routed_probes: u32,
    routed_accepted: u32,
    /// When routed enqueues were answered `UNAVAILABLE` "shard not ready".
    not_ready: Option<Span>,
    not_ready_count: u32,
    other_errors: Vec<String>,
    /// Whether a new computed owner was seen in `Acquiring` for a shard.
    saw_acquiring: bool,
    /// Whether any of the stalled node's shard leases changed holder.
    lease_moved: bool,
    /// Enqueues sent straight to the stalled node for a shard it holds.
    direct_probes: u32,
    direct_accepted: u32,
}

impl PhaseObservation {
    fn describe(&self) -> String {
        let span = |span: &Option<Span>| match span {
            Some(span) => format!(
                "from {:.1}s to {:.1}s after the stall began",
                span.from.as_secs_f64(),
                span.until.as_secs_f64()
            ),
            None => "never".to_string(),
        };
        format!(
            "[{}] calls stalled: {}; dropped by a healthy peer: {}; \
             routed enqueues: {} sent, {} accepted, {} refused \"shard not ready\" ({}); \
             new owner seen in Acquiring: {}; stalled node kept its shard leases: {}; \
             enqueues sent straight to the stalled node: {} of {} accepted; other errors: {:?}",
            self.phase.name(),
            self.stalled_calls,
            span(&self.dropped),
            self.routed_probes,
            self.routed_accepted,
            self.not_ready_count,
            span(&self.not_ready),
            self.saw_acquiring,
            !self.lease_moved,
            self.direct_accepted,
            self.direct_probes,
            self.other_errors,
        )
    }

    fn violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        if self.stalled_calls == 0 {
            violations.push("faultInjected: no call stalled".to_string());
        }
        if self.dropped.is_some() {
            violations.push("membershipKept: a healthy peer dropped the stalled node".to_string());
        }
        if self.routed_accepted != self.routed_probes {
            violations.push(format!(
                "shardsServed: {} of {} routed enqueues were not accepted",
                self.routed_probes - self.routed_accepted,
                self.routed_probes
            ));
        }
        violations
    }
}

fn make_shard_factory() -> Arc<ShardFactory> {
    Arc::new(ShardFactory::new(
        dst_turmoilfs_database_template(std::path::Path::new(STORAGE_ROOT)),
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Start a node: a K8s coordinator on `backend` and a gRPC server in front of it.
async fn setup_node_server(
    node_num: u32,
    backend: MockK8sBackend,
) -> turmoil::Result<(Node, tokio::sync::broadcast::Sender<()>)> {
    let node_id = node_id(node_num);
    let port = BASE_PORT + node_num as u16;
    let factory = make_shard_factory();

    let (coordinator, _handle) = K8sCoordinator::start_with_backend(
        backend,
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

    Ok((coordinator, shutdown_tx))
}

/// Which node's lease holds each shard, in shard order.
async fn shard_holders(k8s_state: &MockK8sState) -> BTreeMap<ShardId, String> {
    k8s_state
        .get_shard_holders(NAMESPACE, CLUSTER_PREFIX)
        .await
        .into_iter()
        .collect()
}

/// Wait until every node is up, sees every node as a member, and holds
/// exactly the shards placement assigns it. Returns the nodes and the holders.
async fn wait_for_settled_cluster(
    registry: &NodeRegistry,
    k8s_state: &MockK8sState,
    step: &str,
) -> (Vec<Node>, BTreeMap<ShardId, String>) {
    let deadline = tokio::time::Instant::now() + STEP_DEADLINE;
    loop {
        let nodes: Vec<Node> = registry.lock().unwrap().iter().flatten().cloned().collect();
        if nodes.len() == NUM_NODES as usize {
            let mut settled = true;
            for node in &nodes {
                let members = node.get_members().await.unwrap_or_default();
                settled &= members.len() == NUM_NODES as usize;
            }
            let holders = shard_holders(k8s_state).await;
            let computed: BTreeMap<ShardId, String> = match nodes[0].get_shard_owner_map().await {
                Ok(map) => map.shard_to_node.into_iter().collect(),
                Err(_) => BTreeMap::new(),
            };
            settled &= holders.len() == NUM_SHARDS as usize && holders == computed;
            if settled {
                return (nodes, holders);
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the cluster did not settle within {STEP_DEADLINE:?} ({step})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// A tenant whose hash falls in each shard's range. The server rejects an
/// enqueue whose tenant is outside the addressed shard's range.
async fn tenants_by_shard(node: &Node) -> BTreeMap<ShardId, String> {
    let shard_map = node.get_shard_map().await.expect("shard map");
    shard_map
        .shards()
        .iter()
        .map(|shard| {
            let tenant = (0u32..)
                .map(|n| format!("member-stall-tenant-{n}"))
                .find(|tenant| shard.contains(tenant))
                .expect("some tenant hashes into every shard's range");
            (shard.id, tenant)
        })
        .collect()
}

enum ProbeOutcome {
    Accepted,
    /// `UNAVAILABLE` "shard not ready": the node is the computed owner and has
    /// not acquired the shard.
    NotReady,
    Other(String),
}

/// gRPC clients to each node, connected on first use.
struct Prober {
    clients: Vec<Option<SiloClient<tonic::transport::Channel>>>,
    config: ClientConfig,
    next_job: u32,
}

impl Prober {
    fn new() -> Self {
        Self {
            clients: (0..NUM_NODES).map(|_| None).collect(),
            config: ClientConfig::for_dst(),
            next_job: 0,
        }
    }

    /// Enqueue one job for `shard_id` on `node_num`.
    async fn enqueue(&mut self, node_num: u32, shard_id: ShardId, tenant: &str) -> ProbeOutcome {
        let slot = &mut self.clients[node_num as usize];
        if slot.is_none() {
            match create_turmoil_client(&node_uri(node_num), &self.config).await {
                Ok(client) => *slot = Some(client),
                Err(e) => return ProbeOutcome::Other(format!("connect to node-{node_num}: {e}")),
            }
        }
        let Some(client) = slot.as_mut() else {
            return ProbeOutcome::Other(format!("no client for node-{node_num}"));
        };

        self.next_job += 1;
        let result = client
            .enqueue(tonic::Request::new(EnqueueRequest {
                shard: shard_id.to_string(),
                id: format!("member-stall-{}", self.next_job),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"probe": self.next_job})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: Some(tenant.to_string()),
                metadata: HashMap::new(),
                task_group: "default".to_string(),
            }))
            .await;
        match result {
            Ok(_) => ProbeOutcome::Accepted,
            Err(status)
                if status.code() == tonic::Code::Unavailable
                    && status.message().contains("shard not ready") =>
            {
                ProbeOutcome::NotReady
            }
            Err(status) => {
                *slot = None;
                ProbeOutcome::Other(format!(
                    "node-{node_num}: {:?} {}",
                    status.code(),
                    status.message()
                ))
            }
        }
    }
}

/// Plant one stall on the stalled node and watch the cluster through it.
async fn run_phase(
    phase: StallPhase,
    stall: Duration,
    stalled_backend: &MockK8sBackend,
    registry: &NodeRegistry,
    k8s_state: &MockK8sState,
    prober: &mut Prober,
) -> PhaseObservation {
    let (nodes, holders) =
        wait_for_settled_cluster(registry, k8s_state, &format!("before {}", phase.name())).await;
    let stalled_id = node_id(STALLED_NODE);
    let tenants = tenants_by_shard(&nodes[0]).await;
    let stalled_shards: Vec<(ShardId, String)> = holders
        .iter()
        .filter(|(_, holder)| **holder == stalled_id)
        .map(|(shard_id, _)| (*shard_id, tenants[shard_id].clone()))
        .collect();
    assert!(
        !stalled_shards.is_empty(),
        "the stalled node should hold at least one shard"
    );
    let healthy: Vec<&Node> = nodes
        .iter()
        .filter(|node| node.node_id() != stalled_id)
        .collect();

    // Plant the fault and wait for the first call to walk into it.
    let stalled_before = stalled_backend.stalled_call_count();
    phase.plant(stalled_backend, stall);
    let plant_deadline = tokio::time::Instant::now() + STEP_DEADLINE;
    while stalled_backend.stalled_call_count() == stalled_before {
        assert!(
            tokio::time::Instant::now() < plant_deadline,
            "no call stalled within {STEP_DEADLINE:?} of planting the {} stall",
            phase.name()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let stall_began = tokio::time::Instant::now();
    tracing::info!(
        phase = phase.name(),
        stall_secs = stall.as_secs(),
        shards = stalled_shards.len(),
        "stall_began"
    );

    let mut observation = PhaseObservation {
        phase,
        stalled_calls: 0,
        dropped: None,
        routed_probes: 0,
        routed_accepted: 0,
        not_ready: None,
        not_ready_count: 0,
        other_errors: Vec::new(),
        saw_acquiring: false,
        lease_moved: false,
        direct_probes: 0,
        direct_accepted: 0,
    };

    while stall_began.elapsed() < stall + RECOVERY_WINDOW {
        let at = stall_began.elapsed();

        // Membership as each healthy peer sees it.
        for peer in &healthy {
            let members = peer.get_members().await.unwrap_or_default();
            if !members.iter().any(|member| member.node_id == stalled_id) {
                Span::extend(&mut observation.dropped, at);
            }
        }

        // The stalled node's shards, addressed the way a client would: to the
        // owner a healthy peer computes.
        let owner_map = healthy[0].get_shard_owner_map().await.expect("owner map");
        let current_holders = shard_holders(k8s_state).await;
        for (shard_id, tenant) in &stalled_shards {
            if current_holders.get(shard_id) != Some(&stalled_id) {
                observation.lease_moved = true;
            }
            let Some(owner) = owner_map.shard_to_node.get(shard_id) else {
                observation
                    .other_errors
                    .push(format!("no computed owner for {shard_id}"));
                continue;
            };
            let owner_num = node_num_of(owner).expect("node id");
            if owner != &stalled_id
                && nodes[owner_num as usize].shard_phase(shard_id).await
                    == Some(ShardPhase::Acquiring)
            {
                observation.saw_acquiring = true;
            }
            observation.routed_probes += 1;
            match prober.enqueue(owner_num, *shard_id, tenant).await {
                ProbeOutcome::Accepted => observation.routed_accepted += 1,
                ProbeOutcome::NotReady => {
                    observation.not_ready_count += 1;
                    Span::extend(&mut observation.not_ready, at);
                }
                ProbeOutcome::Other(error) => observation.other_errors.push(error),
            }
        }

        // The stalled node itself is healthy: it still serves a shard it holds.
        let (shard_id, tenant) = &stalled_shards[0];
        observation.direct_probes += 1;
        if let ProbeOutcome::Accepted = prober.enqueue(STALLED_NODE, *shard_id, tenant).await {
            observation.direct_accepted += 1;
        }

        tokio::time::sleep(PROBE_INTERVAL).await;
    }

    observation.stalled_calls = stalled_backend.stalled_call_count() - stalled_before;
    tracing::info!(summary = %observation.describe(), "stall_phase_done");
    observation
}

pub fn run() {
    let seed = get_seed();
    let stall = stall_duration();
    // Startup, then two phases of stall plus recovery, each preceded by a wait
    // for the cluster to settle.
    let sim_secs = 120 + 2 * (stall + RECOVERY_WINDOW + STEP_DEADLINE).as_secs();

    run_scenario_impl("k8s_membership_stall", seed, sim_secs, |sim| {
        let k8s_state = MockK8sState::new();
        let registry: NodeRegistry = Arc::new(Mutex::new((0..NUM_NODES).map(|_| None).collect()));
        // One Kubernetes client per node; the controller keeps a handle on the
        // stalled node's so it can plant the hang there and nowhere else.
        let backends: Vec<MockK8sBackend> = (0..NUM_NODES)
            .map(|_| MockK8sBackend::new(k8s_state.clone(), NAMESPACE))
            .collect();

        for node_num in 0..NUM_NODES {
            let host_name: &'static str = Box::leak(format!("node{node_num}").into_boxed_str());
            let backend = backends[node_num as usize].clone();
            let registry = Arc::clone(&registry);
            sim.host(host_name, move || {
                let backend = backend.clone();
                let registry = Arc::clone(&registry);
                async move {
                    // Stagger startup so the nodes don't race to create the shard map.
                    tokio::time::sleep(Duration::from_millis(node_num as u64 * 150)).await;
                    let (coordinator, _server_shutdown) =
                        setup_node_server(node_num, backend).await?;
                    registry.lock().unwrap()[node_num as usize] = Some(coordinator);
                    std::future::pending::<()>().await;
                    Ok(())
                }
            });
        }

        let stalled_backend = backends[STALLED_NODE as usize].clone();
        sim.client("controller", async move {
            let mut prober = Prober::new();
            let mut observations = Vec::new();
            for phase in [StallPhase::RenewalWrite, StallPhase::SafetyReconcileList] {
                observations.push(
                    run_phase(
                        phase,
                        stall,
                        &stalled_backend,
                        &registry,
                        &k8s_state,
                        &mut prober,
                    )
                    .await,
                );
            }

            let report: Vec<String> = observations.iter().map(|o| o.describe()).collect();
            let violations: Vec<String> = observations
                .iter()
                .flat_map(|o| {
                    o.violations()
                        .into_iter()
                        .map(|violation| format!("[{}] {violation}", o.phase.name()))
                })
                .collect();
            assert!(
                violations.is_empty(),
                "a {}s stall of one Kubernetes call cost a live node its membership \
                 (lease TTL {LEASE_DURATION_SECS}s).\nViolations:\n  {}\nObserved:\n  {}",
                stall.as_secs(),
                violations.join("\n  "),
                report.join("\n  "),
            );
            Ok(())
        });
    });
}
