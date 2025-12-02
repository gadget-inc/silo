use anyhow::Context;
use etcd_client::{Client, ConnectOptions, GetOptions, PutOptions};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::settings::CoordinationConfig;
use crate::shard_guard::ShardGuard;

/// Information about a cluster member
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemberInfo {
    pub node_id: String,
    pub grpc_addr: String,
}

/// Mapping of shard IDs to their owning node's gRPC address
#[derive(Debug, Clone)]
pub struct ShardOwnerMap {
    /// Total number of shards in the cluster
    pub num_shards: u32,
    /// Maps shard_id -> grpc_addr of the owning node
    pub shard_to_addr: HashMap<u32, String>,
    /// Maps shard_id -> node_id of the owning node
    pub shard_to_node: HashMap<u32, String>,
}

/// Entity that coordinates between all the running nodes within a Silo cluster to decide who is going to serve which shards
pub struct Coordination {
    client: Client,
}

impl Coordination {
    /// Initialize the etcd client from configuration. Uses plaintext by default; TLS can be
    /// configured via etcd-client feature flags and endpoint schemas.
    #[tracing::instrument(level = "info")]
    pub async fn connect(cfg: &CoordinationConfig) -> anyhow::Result<Self> {
        let endpoints = if cfg.etcd_endpoints.is_empty() {
            vec!["http://127.0.0.1:2379".to_string()]
        } else {
            cfg.etcd_endpoints.clone()
        };

        let opts = ConnectOptions::default();
        let client = Client::connect(endpoints, Some(opts))
            .await
            .context("failed to connect to etcd endpoints")?;
        Ok(Self { client })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}

/// Helpers to build etcd key paths used for coordination.
pub mod keys {
    pub fn members_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/members/", cluster_prefix)
    }
    pub fn member_key(cluster_prefix: &str, node_id: &str) -> String {
        format!("{}{}", members_prefix(cluster_prefix), node_id)
    }
    pub fn shards_prefix(cluster_prefix: &str) -> String {
        format!("{}/coord/shards/", cluster_prefix)
    }
    pub fn shard_owner_key(cluster_prefix: &str, shard_id: u32) -> String {
        format!("{}{}/owner", shards_prefix(cluster_prefix), shard_id)
    }
}

/// Coordinator manages membership and per-shard ownership using etcd leases.
#[derive(Clone)]
pub struct Coordinator {
    client: Client,
    cluster_prefix: String,
    node_id: String,
    grpc_addr: String,
    num_shards: u32,
    membership_lease_id: i64,
    liveness_lease_id: i64,
    owned: Arc<Mutex<HashSet<u32>>>,
    // Per-shard guards encapsulate lock state; top-level map indexes guards
    shard_guards: Arc<Mutex<HashMap<u32, Arc<ShardGuard>>>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl Coordinator {
    /// Start a coordinator instance under a cluster prefix. TTL is seconds for leases.
    /// The grpc_addr should be the address other nodes can use to reach this node's gRPC server.
    pub async fn start(
        coord: &Coordination,
        cluster_prefix: impl Into<String>,
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
        ttl_secs: i64,
    ) -> anyhow::Result<(Self, tokio::task::JoinHandle<()>)> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
        let mut client = coord.client();
        let mut kv = client.kv_client();
        let cluster_prefix = cluster_prefix.into();
        let node_id = node_id.into();
        let grpc_addr = grpc_addr.into();

        // Create membership and liveness leases (we do not keepalive in this first cut;
        // tests complete well within TTL).
        let membership_lease = client.lease_grant(ttl_secs, None).await?.id();
        let liveness_lease = client.lease_grant(ttl_secs, None).await?.id();

        // Write membership key with node_id and grpc_addr
        let member_key = crate::coordination::keys::member_key(&cluster_prefix, &node_id);
        let member_info = MemberInfo {
            node_id: node_id.clone(),
            grpc_addr: grpc_addr.clone(),
        };
        let member_value = serde_json::to_string(&member_info)?;
        kv.put(
            member_key,
            member_value,
            Some(PutOptions::new().with_lease(membership_lease)),
        )
        .await?;

        let owned = Arc::new(Mutex::new(HashSet::new()));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let cprefix = cluster_prefix.clone();
        let nid = node_id.clone();
        let client_bg = client.clone();
        let mut lease_bg = client_bg.lease_client();
        let mut watch_bg = client_bg.watch_client();
        let mut lock_bg = client_bg.lock_client();

        let me = Self {
            client,
            cluster_prefix,
            node_id,
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
        let handle = tokio::spawn(async move {
            // Start lease keepalives for membership and liveness
            let (mut _memb_keeper, mut memb_stream) =
                match lease_bg.keep_alive(membership_lease).await {
                    Ok(x) => x,
                    Err(_) => {
                        // Retry until shutdown
                        loop {
                            if *shutdown_rx.borrow() {
                                return;
                            }
                            if let Ok(x) = lease_bg.keep_alive(membership_lease).await {
                                break x;
                            }
                            sleep(Duration::from_millis(200)).await;
                        }
                    }
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
            let members_prefix = crate::coordination::keys::members_prefix(&cprefix);
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
                Err(_) => {
                    // If watch cannot be established, loop until shutdown
                    loop {
                        if *shutdown_rx.borrow() {
                            return;
                        }
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            };

            // Initial reconcile
            if let Err(err) = me_bg.reconcile_shards_with_locks(&mut lock_bg).await {
                warn!(node_id = %nid, error = %err, "initial reconcile failed");
            }

            // Immediate reconcile on membership events; periodic resync as a safety net
            let mut resync = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = resync.tick() => {
                        if *shutdown_rx.borrow() { break; }
                        if let Err(err) = me_bg.reconcile_shards_with_locks(&mut lock_bg).await {
                            warn!(node_id = %nid, error = %err, "periodic reconcile failed");
                        }
                    }
                    resp = members_stream.message() => {
                        if *shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(_msg)) => {
                                info!(node_id = %nid, "membership changed; reconciling now");
                                if let Err(err) = me_bg.reconcile_shards_with_locks(&mut lock_bg).await {
                                    warn!(node_id = %nid, error = %err, "watch-triggered reconcile failed");
                                }
                            }
                            Ok(None) => { break; }
                            Err(_) => { /* transient errors; rely on periodic resync */ }
                        }
                    }
                    // Keepalive streams; if either terminates, exit loop to stop serving
                    _m = memb_stream.message() => { if *shutdown_rx.borrow() { break; } }
                    _m = live_stream.message() => { if *shutdown_rx.borrow() { break; } }
                }
            }
        });

        Ok((me, handle))
    }

    pub async fn owned_shards(&self) -> Vec<u32> {
        let guard = self.owned.lock().await;
        let mut v: Vec<u32> = guard.iter().copied().collect();
        v.sort_unstable();
        v
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        // Broadcast shutdown: stop background loop and tell shard guards to exit
        let _ = self.shutdown_tx.send(true);
        {
            let guards = self.shard_guards.lock().await;
            for (_sid, guard) in guards.iter() {
                // Ensure guards stop desiring and wake up
                guard.set_desired(false).await;
                guard.notify.notify_one();
            }
        }
        // Revoke leases deletes owner keys and membership
        let _ = self.client.lease_revoke(self.membership_lease_id).await;
        let _ = self.client.lease_revoke(self.liveness_lease_id).await;
        Ok(())
    }

    /// Poll etcd until the current owners exactly match the deterministic selection for the current membership, or until timeout. Returns true if converged.
    #[tracing::instrument(level = "info", skip(self, timeout), fields(node_id = %self.node_id))]
    pub async fn wait_converged(&self, timeout: Duration) -> bool {
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
                // Avoid false convergence when membership snapshot is temporarily empty
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
        // On timeout, log the diff to aid diagnosis
        if let Ok(member_ids) = self.get_sorted_member_ids().await {
            let desired: HashSet<u32> =
                compute_desired_shards_for_node(self.num_shards, &self.node_id, &member_ids);
            let owned_now: HashSet<u32> = {
                let g = self.owned.lock().await;
                g.clone()
            };
            let missing: Vec<u32> = desired.difference(&owned_now).copied().collect();
            let extra: Vec<u32> = owned_now.difference(&desired).copied().collect();
            debug!(node_id = %self.node_id, missing = ?missing, extra = ?extra, desired_len = desired.len(), owned_len = owned_now.len(), "wait_converged: timed out; diff");

            // Log the current state of all shard locks to aid debugging
            let lock_states: Vec<String> = {
                let guards = self.shard_guards.lock().await;
                let mut v = Vec::new();
                for (sid, guard) in guards.iter() {
                    let st = guard.state.lock().await;
                    let held = st.held_key.is_some();
                    v.push(format!(
                        "shard={} desired={} phase={} held={}",
                        sid, st.desired, st.phase, held
                    ));
                }
                v
            };
            debug!(node_id = %self.node_id, locks = ?lock_states, "wait_converged: lock state snapshot");
        }
        false
    }

    /*
    Reconciliation strategy (high-level):

    Invariant: Only a node holding the per-shard owner lock (backed by this node's liveness lease) may serve that shard. Desired ownership is computed deterministically from current membership.

    Triggers:
    - Immediately on any membership watch event
    - Periodically (safety net) on a fixed interval

    Steps:
    1) Compute membership snapshot and the desired shard set for this node using rendezvous hashing.
    2) Bump a reconciliation generation. Any in-flight acquisitions observe this and abort quickly,
       so we never acquire shards we no longer desire.
    3) For shards currently held but no longer desired: schedule a delayed release (≈300ms). The delay
       gives future owners time to start their acquire loops, minimizing downtime. If a new reconcile
       begins during the delay, the release task is cancelled (so we do not release based on stale intent).
    4) For desired shards not currently held: start non-blocking acquisition loops using the etcd Lock API
       with the node's liveness lease. Each loop retries with short timeouts and jitter, and aborts
       immediately if a new reconciliation generation is observed.
    5) Update the in-memory `owned` set from the locks map after scheduling releases and acquires.

    Properties this design guarantees:
    - Immediate reaction to membership changes: we reconcile right away on watch events.
    - Acquires are promptly aborted on newer membership information (generation bump check).
    - Releases are not aborted immediately; they wait briefly to allow the new owner to begin acquiring,
      but are cancelled if a fresher reconcile supersedes them.
    - A periodic resync reconciles even if a watch event is transiently missed.
    - The data plane never queries etcd per request; ownership state is maintained locally and derived
      from the locks held under our liveness lease.
    */
    /// Reconcile desired shards for this node against current locks using per-shard actors.
    #[tracing::instrument(level = "debug", skip(self, _lock), fields(node_id = %self.node_id))]
    async fn reconcile_shards_with_locks(
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

        // Tell per-shard actors to release undesired shards
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

        // Tell per-shard actors to acquire desired shards
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

    async fn ensure_shard_guard(&self, shard_id: u32) -> Arc<ShardGuard> {
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
        let guard = ShardGuard::new(
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

    /// Helper: read and parse sorted member ids under the given prefix.
    async fn get_sorted_member_ids(&self) -> Result<Vec<String>, etcd_client::Error> {
        let resp = self
            .client
            .kv_client()
            .get(
                crate::coordination::keys::members_prefix(&self.cluster_prefix).to_string(),
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

    /// Get all member information from the cluster
    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, etcd_client::Error> {
        let resp = self
            .client
            .kv_client()
            .get(
                crate::coordination::keys::members_prefix(&self.cluster_prefix).to_string(),
                Some(GetOptions::new().with_prefix()),
            )
            .await?;
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

    /// Compute a mapping of shard IDs to their owning node's address.
    /// Uses rendezvous hashing to deterministically assign shards to nodes.
    pub async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, etcd_client::Error> {
        let members = self.get_members().await?;
        let member_ids: Vec<String> = members.iter().map(|m| m.node_id.clone()).collect();
        
        // Build node_id -> grpc_addr lookup
        let addr_map: HashMap<String, String> = members
            .into_iter()
            .map(|m| (m.node_id, m.grpc_addr))
            .collect();

        let mut shard_to_addr = HashMap::new();
        let mut shard_to_node = HashMap::new();

        for shard_id in 0..self.num_shards {
            if let Some(owner_node) = select_owner_for_shard(shard_id, &member_ids) {
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

    /// Get the number of shards in the cluster
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    /// Get this node's ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get this node's gRPC address
    pub fn grpc_addr(&self) -> &str {
        &self.grpc_addr
    }
}

/// Deterministically select the owner node for a shard using rendezvous hashing.
fn select_owner_for_shard(shard_id: u32, member_ids: &[String]) -> Option<String> {
    if member_ids.is_empty() {
        return None;
    }
    let mut best: Option<(u64, &String)> = None;
    for m in member_ids {
        // Mix a deterministic 64-bit score for (shard, member). Using a high-quality bit mixer
        // reduces bias that could appear with simple string hashing.
        let member_hash = fnv1a64(m.as_bytes());
        let shard_hash = (shard_id as u64).wrapping_mul(0x9e3779b97f4a7c15);
        let score = mix64(member_hash ^ shard_hash);
        let h = score;
        if let Some((cur, _)) = best {
            if h > cur {
                best = Some((h, m));
            }
        } else {
            best = Some((h, m));
        }
    }
    best.map(|(_, m)| m.clone())
}

fn fnv1a64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for b in data {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Helper: compute the desired set of shard ids for this node.
fn compute_desired_shards_for_node(
    num_shards: u32,
    node_id: &str,
    member_ids: &[String],
) -> HashSet<u32> {
    let mut desired: HashSet<u32> = HashSet::new();
    for s in 0..num_shards {
        if let Some(owner) = select_owner_for_shard(s, member_ids) {
            if owner == node_id {
                desired.insert(s);
            }
        }
    }
    desired
}
