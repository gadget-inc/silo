//! etcd-based coordination backend.
//!
//! This backend uses etcd leases and the Lock API for distributed shard ownership.
//! It's the most battle-tested option for production deployments.

use anyhow::Context;
use async_trait::async_trait;
use etcd_client::{Client, ConnectOptions, GetOptions, LockOptions, PutOptions};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, Notify};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{
    compute_desired_shards_for_node, keys, CoordinationError, Coordinator, MemberInfo,
    ShardOwnerMap,
};

/// etcd-based coordinator for distributed shard ownership.
#[derive(Clone)]
pub struct EtcdCoordinator {
    client: Client,
    cluster_prefix: String,
    node_id: String,
    grpc_addr: String,
    num_shards: u32,
    membership_lease_id: i64,
    liveness_lease_id: i64,
    owned: Arc<Mutex<HashSet<u32>>>,
    shard_guards: Arc<Mutex<HashMap<u32, Arc<EtcdShardGuard>>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl EtcdCoordinator {
    /// Connect to etcd and start the coordinator.
    pub async fn start(
        endpoints: &[String],
        cluster_prefix: &str,
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
        ttl_secs: i64,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), CoordinationError> {
        let endpoints = if endpoints.is_empty() {
            vec!["http://127.0.0.1:2379".to_string()]
        } else {
            endpoints.to_vec()
        };

        let opts = ConnectOptions::default();
        let mut client = Client::connect(endpoints, Some(opts))
            .await
            .map_err(|e| CoordinationError::ConnectionFailed(e.to_string()))?;

        let cluster_prefix = cluster_prefix.to_string();
        let node_id = node_id.into();
        let grpc_addr = grpc_addr.into();

        // Create membership and liveness leases
        let membership_lease = client
            .lease_grant(ttl_secs, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?
            .id();
        let liveness_lease = client
            .lease_grant(ttl_secs, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?
            .id();

        // Write membership key
        let member_key = keys::member_key(&cluster_prefix, &node_id);
        let member_info = MemberInfo {
            node_id: node_id.clone(),
            grpc_addr: grpc_addr.clone(),
        };
        let member_value = serde_json::to_string(&member_info)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        client
            .kv_client()
            .put(
                member_key,
                member_value,
                Some(PutOptions::new().with_lease(membership_lease)),
            )
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let owned = Arc::new(Mutex::new(HashSet::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let me = Self {
            client: client.clone(),
            cluster_prefix: cluster_prefix.clone(),
            node_id: node_id.clone(),
            grpc_addr,
            num_shards,
            membership_lease_id: membership_lease,
            liveness_lease_id: liveness_lease,
            owned,
            shard_guards: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx,
            shutdown_rx: shutdown_rx.clone(),
        };

        let me_bg = me.clone();
        let cprefix = cluster_prefix.clone();
        let nid = node_id.clone();
        let mut lease_bg = client.lease_client();
        let mut watch_bg = client.watch_client();
        let mut lock_bg = client.lock_client();

        let handle = tokio::spawn(async move {
            // Start lease keepalives
            let (mut _memb_keeper, mut memb_stream) =
                match lease_bg.keep_alive(membership_lease).await {
                    Ok(x) => x,
                    Err(_) => loop {
                        if *shutdown_rx.borrow() {
                            return;
                        }
                        if let Ok(x) = lease_bg.keep_alive(membership_lease).await {
                            break x;
                        }
                        sleep(Duration::from_millis(200)).await;
                    },
                };

            let (mut _live_keeper, mut live_stream) =
                match lease_bg.keep_alive(liveness_lease).await {
                    Ok(x) => x,
                    Err(_) => loop {
                        if *shutdown_rx.borrow() {
                            return;
                        }
                        if let Ok(x) = lease_bg.keep_alive(liveness_lease).await {
                            break x;
                        }
                        sleep(Duration::from_millis(200)).await;
                    },
                };

            // Establish member watch
            let members_prefix = keys::members_prefix(&cprefix);
            debug!(node_id = %nid, ttl = membership_lease, "starting coordinator keepalives");

            let (_members_watcher, mut members_stream) = match watch_bg
                .watch(
                    members_prefix.clone(),
                    Some(etcd_client::WatchOptions::new().with_prefix()),
                )
                .await
            {
                Ok(w) => {
                    debug!(node_id = %nid, prefix = %members_prefix, "members watch established");
                    w
                }
                Err(_) => loop {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    sleep(Duration::from_millis(200)).await;
                },
            };

            // Initial reconcile
            if let Err(err) = me_bg.reconcile_shards(&mut lock_bg).await {
                warn!(node_id = %nid, error = %err, "initial reconcile failed");
            }

            // Main loop: reconcile on membership events or periodic resync
            let mut resync = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = resync.tick() => {
                        if *shutdown_rx.borrow() { break; }
                        if let Err(err) = me_bg.reconcile_shards(&mut lock_bg).await {
                            warn!(node_id = %nid, error = %err, "periodic reconcile failed");
                        }
                    }
                    resp = members_stream.message() => {
                        if *shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(_msg)) => {
                                info!(node_id = %nid, "membership changed; reconciling now");
                                if let Err(err) = me_bg.reconcile_shards(&mut lock_bg).await {
                                    warn!(node_id = %nid, error = %err, "watch-triggered reconcile failed");
                                }
                            }
                            Ok(None) => { break; }
                            Err(_) => { /* transient errors; rely on periodic resync */ }
                        }
                    }
                    _m = memb_stream.message() => { if *shutdown_rx.borrow() { break; } }
                    _m = live_stream.message() => { if *shutdown_rx.borrow() { break; } }
                }
            }
        });

        Ok((me, handle))
    }

    async fn reconcile_shards(
        &self,
        _lock: &mut etcd_client::LockClient,
    ) -> Result<(), etcd_client::Error> {
        let members = self.get_sorted_member_ids().await?;
        debug!(node_id = %self.node_id, members = ?members, "reconcile: begin");
        let desired = compute_desired_shards_for_node(self.num_shards, &self.node_id, &members);

        let current_locks: Vec<u32> = {
            let g = self.shard_guards.lock().await;
            let mut v = Vec::new();
            for (sid, guard) in g.iter() {
                if guard.state.lock().await.held_key.is_some() {
                    v.push(*sid);
                }
            }
            v
        };
        debug!(node_id = %self.node_id, desired_len = desired.len(), have_locks = ?current_locks, "reconcile: computed desired vs current");

        // Release undesired shards
        {
            let to_release: Vec<u32> = {
                let guards = self.shard_guards.lock().await;
                let mut v = Vec::new();
                for (sid, guard) in guards.iter() {
                    if !desired.contains(sid) && guard.state.lock().await.held_key.is_some() {
                        v.push(*sid);
                    }
                }
                v
            };
            for sid in to_release {
                self.ensure_shard_guard(sid).await.set_desired(false).await;
            }
        }

        // Acquire desired shards
        {
            let snapshot: Vec<u32> = desired.iter().copied().collect();
            for shard_id in snapshot {
                self.ensure_shard_guard(shard_id)
                    .await
                    .set_desired(true)
                    .await;
            }
        }

        Ok(())
    }

    async fn ensure_shard_guard(&self, shard_id: u32) -> Arc<EtcdShardGuard> {
        {
            let guards = self.shard_guards.lock().await;
            if let Some(g) = guards.get(&shard_id) {
                return g.clone();
            }
        }
        let mut guards = self.shard_guards.lock().await;
        if let Some(g) = guards.get(&shard_id) {
            return g.clone();
        }
        let guard = EtcdShardGuard::new(
            shard_id,
            self.client.clone(),
            self.cluster_prefix.clone(),
            self.liveness_lease_id,
            self.shutdown_rx.clone(),
        );
        let runner = guard.clone();
        let owned_arc = self.owned.clone();
        tokio::spawn(async move { runner.run(owned_arc).await });
        guards.insert(shard_id, guard.clone());
        guard
    }

    async fn get_sorted_member_ids(&self) -> Result<Vec<String>, etcd_client::Error> {
        let resp = self
            .client
            .kv_client()
            .get(
                keys::members_prefix(&self.cluster_prefix),
                Some(GetOptions::new().with_prefix()),
            )
            .await?;
        let mut member_ids: Vec<String> = resp
            .kvs()
            .iter()
            .filter_map(|kv| {
                let key = String::from_utf8_lossy(kv.key());
                key.split('/').next_back().map(|s| s.to_string())
            })
            .collect();
        member_ids.sort();
        Ok(member_ids)
    }
}

#[async_trait]
impl Coordinator for EtcdCoordinator {
    async fn owned_shards(&self) -> Vec<u32> {
        let guard = self.owned.lock().await;
        let mut v: Vec<u32> = guard.iter().copied().collect();
        v.sort_unstable();
        v
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        let _ = self.shutdown_tx.send(true);
        {
            let guards = self.shard_guards.lock().await;
            for (_sid, guard) in guards.iter() {
                guard.set_desired(false).await;
                guard.notify.notify_one();
            }
        }
        let _ = self
            .client
            .clone()
            .lease_revoke(self.membership_lease_id)
            .await;
        let _ = self
            .client
            .clone()
            .lease_revoke(self.liveness_lease_id)
            .await;
        Ok(())
    }

    async fn wait_converged(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let member_ids = match self.get_sorted_member_ids().await {
                Ok(m) => m,
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if member_ids.is_empty() {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            let desired: HashSet<u32> =
                compute_desired_shards_for_node(self.num_shards, &self.node_id, &member_ids);
            let guard = self.owned.lock().await;
            if *guard == desired {
                return true;
            }
            drop(guard);
            sleep(Duration::from_millis(100)).await;
        }

        // Log diff on timeout
        if let Ok(member_ids) = self.get_sorted_member_ids().await {
            let desired: HashSet<u32> =
                compute_desired_shards_for_node(self.num_shards, &self.node_id, &member_ids);
            let owned_now: HashSet<u32> = {
                let g = self.owned.lock().await;
                g.clone()
            };
            let missing: Vec<u32> = desired.difference(&owned_now).copied().collect();
            let extra: Vec<u32> = owned_now.difference(&desired).copied().collect();
            debug!(node_id = %self.node_id, missing = ?missing, extra = ?extra, "wait_converged: timed out");
        }
        false
    }

    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        let resp = self
            .client
            .kv_client()
            .get(
                keys::members_prefix(&self.cluster_prefix),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let mut members: Vec<MemberInfo> = resp
            .kvs()
            .iter()
            .filter_map(|kv| {
                let value = String::from_utf8_lossy(kv.value());
                serde_json::from_str(&value).ok()
            })
            .collect();
        members.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(members)
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
        let members = self.get_members().await?;
        let member_ids: Vec<String> = members.iter().map(|m| m.node_id.clone()).collect();

        let addr_map: HashMap<String, String> = members
            .into_iter()
            .map(|m| (m.node_id, m.grpc_addr))
            .collect();

        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_id in 0..self.num_shards {
            if let Some(owner_node) = super::select_owner_for_shard(shard_id, &member_ids) {
                if let Some(addr) = addr_map.get(&owner_node) {
                    shard_to_addr.insert(shard_id, addr.clone());
                    shard_to_node.insert(shard_id, owner_node);
                }
            }
        }

        Ok(ShardOwnerMap {
            num_shards: self.num_shards,
            shard_to_addr,
            shard_to_node,
        })
    }

    fn num_shards(&self) -> u32 {
        self.num_shards
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn grpc_addr(&self) -> &str {
        &self.grpc_addr
    }
}

// ============================================================================
// EtcdShardGuard - per-shard lock management (moved from shard_guard.rs)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardPhase {
    Idle,
    Acquiring,
    Held,
    Releasing,
    ShuttingDown,
    ShutDown,
}

impl std::fmt::Display for ShardPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardPhase::Idle => write!(f, "Idle"),
            ShardPhase::Acquiring => write!(f, "Acquiring"),
            ShardPhase::Held => write!(f, "Held"),
            ShardPhase::Releasing => write!(f, "Releasing"),
            ShardPhase::ShuttingDown => write!(f, "ShuttingDown"),
            ShardPhase::ShutDown => write!(f, "ShutDown"),
        }
    }
}

pub struct ShardState {
    pub desired: bool,
    pub phase: ShardPhase,
    pub held_key: Option<Vec<u8>>,
}

/// Per-shard lock guard for etcd backend.
pub struct EtcdShardGuard {
    pub shard_id: u32,
    pub client: Client,
    pub cluster_prefix: String,
    pub liveness_lease_id: i64,
    pub state: Mutex<ShardState>,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
}

impl EtcdShardGuard {
    pub fn new(
        shard_id: u32,
        client: Client,
        cluster_prefix: String,
        liveness_lease_id: i64,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            client,
            cluster_prefix,
            liveness_lease_id,
            state: Mutex::new(ShardState {
                desired: false,
                phase: ShardPhase::Idle,
                held_key: None,
            }),
            notify: Notify::new(),
            shutdown,
        })
    }

    fn owner_key(&self) -> String {
        keys::shard_owner_key(&self.cluster_prefix, self.shard_id)
    }

    pub async fn set_desired(&self, desired: bool) {
        let mut st = self.state.lock().await;
        if matches!(st.phase, ShardPhase::ShutDown | ShardPhase::ShuttingDown) {
            debug!(shard_id = self.shard_id, desired = desired, phase = %st.phase, "shard: can't change desired state after shut down");
            return;
        }

        if st.desired != desired {
            let prev_desired = st.desired;
            let prev_phase = st.phase;
            st.desired = desired;
            debug!(shard_id = self.shard_id, prev_desired = prev_desired, desired = desired, phase = %prev_phase, "shard: set_desired");
            self.notify.notify_one();
        }
    }

    pub async fn run(self: Arc<Self>, owned_arc: Arc<Mutex<HashSet<u32>>>) {
        use tracing::{info_span, Instrument};

        loop {
            if *self.shutdown.borrow() {
                let mut st = self.state.lock().await;
                st.phase = ShardPhase::ShuttingDown;
            }

            {
                let mut st = self.state.lock().await;
                match (st.phase, st.desired, st.held_key.is_some()) {
                    (ShardPhase::ShutDown, _, _) => {}
                    (ShardPhase::ShuttingDown, _, _) => {}
                    (ShardPhase::Idle, true, false) => {
                        debug!(
                            shard_id = self.shard_id,
                            "shard: transition Idle -> Acquiring"
                        );
                        st.phase = ShardPhase::Acquiring;
                    }
                    (ShardPhase::Held, false, true) => {
                        debug!(
                            shard_id = self.shard_id,
                            "shard: transition Held -> Releasing"
                        );
                        st.phase = ShardPhase::Releasing;
                    }
                    _ => {}
                }
            }

            let phase = { self.state.lock().await.phase };
            match phase {
                ShardPhase::ShutDown => break,
                ShardPhase::Acquiring => {
                    let name = self.owner_key();
                    let span =
                        info_span!("shard.acquire", shard_id = self.shard_id, lock_key = %name);
                    async {
                        let mut lock_cli = self.client.lock_client();
                        let mut attempt: u32 = 0;

                        let initial_jitter_ms = ((self.shard_id as u64).wrapping_mul(13)) % 80;
                        tokio::time::sleep(Duration::from_millis(initial_jitter_ms)).await;

                        loop {
                            if { self.state.lock().await.phase } != ShardPhase::Acquiring {
                                break;
                            }
                            {
                                let mut st = self.state.lock().await;
                                if !st.desired || st.phase != ShardPhase::Acquiring {
                                    if !st.desired && st.phase == ShardPhase::Acquiring {
                                        st.phase = ShardPhase::Idle;
                                    }
                                    info!(shard_id = self.shard_id, desired = st.desired, phase = %st.phase, "shard: acquire abort");
                                    break;
                                }
                            }
                            match tokio::time::timeout(
                                Duration::from_millis(500),
                                lock_cli.lock(
                                    name.as_bytes().to_vec(),
                                    Some(LockOptions::new().with_lease(self.liveness_lease_id)),
                                ),
                            )
                            .await
                            {
                                Ok(Ok(resp)) => {
                                    let key = resp.key().to_vec();
                                    {
                                        let mut st = self.state.lock().await;
                                        st.held_key = Some(key);
                                        st.phase = ShardPhase::Held;
                                    }
                                    let mut owned = owned_arc.lock().await;
                                    owned.insert(self.shard_id);
                                    debug!(shard_id = self.shard_id, attempts = attempt, "shard: acquire success");
                                    break;
                                }
                                Ok(Err(_)) | Err(_) => {
                                    attempt = attempt.wrapping_add(1);
                                    let jitter_ms = ((self.shard_id as u64)
                                        .wrapping_mul(31)
                                        .wrapping_add(attempt as u64 * 17))
                                        % 150;
                                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                                    if attempt % 8 == 0 {
                                        debug!(shard_id = self.shard_id, attempt = attempt, "shard: acquire retry");
                                    }
                                }
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::Releasing => {
                    let name = self.owner_key();
                    let span =
                        info_span!("shard.release", shard_id = self.shard_id, lock_key = %name);
                    async {
                        debug!(shard_id = self.shard_id, "shard: release start (delay)");
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        let mut cancelled = false;
                        let key_opt = {
                            let mut st = self.state.lock().await;
                            if st.phase == ShardPhase::ShuttingDown {
                                // fall through
                            } else if st.desired {
                                st.phase = ShardPhase::Held;
                                cancelled = true;
                                debug!(shard_id = self.shard_id, "shard: release cancelled");
                            }
                            st.held_key.clone()
                        };
                        if cancelled {
                            return;
                        }
                        if let Some(key) = key_opt {
                            let mut lock_cli = self.client.lock_client();
                            let _ = lock_cli.unlock(key).await;
                            {
                                let mut st = self.state.lock().await;
                                st.held_key = None;
                                st.phase = ShardPhase::Idle;
                            }
                            let mut owned = owned_arc.lock().await;
                            owned.remove(&self.shard_id);
                            debug!(shard_id = self.shard_id, "shard: release done");
                        } else {
                            let mut st = self.state.lock().await;
                            st.phase = ShardPhase::Idle;
                            debug!(shard_id = self.shard_id, "shard: release noop");
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::ShuttingDown => {
                    let key_opt = { self.state.lock().await.held_key.clone() };
                    if let Some(key) = key_opt {
                        let mut lock_cli = self.client.lock_client();
                        let _ = lock_cli.unlock(key).await;
                        let mut owned = owned_arc.lock().await;
                        owned.remove(&self.shard_id);
                    }
                    {
                        let mut st = self.state.lock().await;
                        st.held_key = None;
                        st.phase = ShardPhase::ShutDown;
                    }
                    break;
                }
                ShardPhase::Idle | ShardPhase::Held => {
                    let mut shutdown_rx = self.shutdown.clone();
                    tokio::select! {
                        _ = self.notify.notified() => {}
                        _ = shutdown_rx.changed() => {}
                    }
                }
            }
        }
    }
}

/// Legacy compatibility: Connect to etcd without starting a coordinator.
/// Used for tests that need raw etcd access.
pub struct EtcdConnection {
    client: Client,
}

impl EtcdConnection {
    pub async fn connect(cfg: &crate::settings::CoordinationConfig) -> anyhow::Result<Self> {
        let endpoints = if cfg.etcd_endpoints.is_empty() {
            vec!["http://127.0.0.1:2379".to_string()]
        } else {
            cfg.etcd_endpoints.clone()
        };
        let opts = ConnectOptions::default();
        let client = Client::connect(endpoints, Some(opts))
            .await
            .context("failed to connect to etcd")?;
        Ok(Self { client })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
