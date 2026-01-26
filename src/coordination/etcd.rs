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
use tokio::sync::{Mutex, Notify, watch};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::factory::ShardFactory;

// Re-export ShardPhase for backwards compatibility (tests reference it from here)
pub use super::ShardPhase;

use super::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap,
    compute_desired_shards_for_node, get_hostname, keys,
};

/// etcd-based coordinator for distributed shard ownership.
#[derive(Clone)]
pub struct EtcdCoordinator {
    /// Shared coordinator state (node_id, grpc_addr, owned set, shutdown, factory)
    base: Arc<CoordinatorBase>,
    client: Client,
    cluster_prefix: String,
    membership_lease_id: i64,
    liveness_lease_id: i64,
    shard_guards: Arc<Mutex<HashMap<u32, Arc<EtcdShardGuard>>>>,
}

impl EtcdCoordinator {
    /// Connect to etcd and start the coordinator.
    ///
    /// The coordinator will manage shard ownership and automatically open/close
    /// shards in the factory as ownership changes.
    pub async fn start(
        endpoints: &[String],
        cluster_prefix: &str,
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
        ttl_secs: i64,
        factory: Arc<ShardFactory>,
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

        let base = Arc::new(CoordinatorBase::new(
            node_id.clone(),
            grpc_addr.clone(),
            num_shards,
            factory,
        ));

        // Write membership key
        let member_key = keys::member_key(&cluster_prefix, &node_id);
        let member_info = MemberInfo {
            node_id: node_id.clone(),
            grpc_addr: grpc_addr.clone(),
            startup_time_ms: base.startup_time_ms,
            hostname: get_hostname(),
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

        let me = Self {
            base: base.clone(),
            client: client.clone(),
            cluster_prefix: cluster_prefix.clone(),
            membership_lease_id: membership_lease,
            liveness_lease_id: liveness_lease,
            shard_guards: Arc::new(Mutex::new(HashMap::new())),
        };
        let shutdown_rx = base.shutdown_rx.clone();

        let me_bg = me.clone();
        let cprefix = cluster_prefix.clone();
        let nid = node_id.clone();
        let mut lease_bg = client.lease_client();
        let mut watch_bg = client.watch_client();
        let mut lock_bg = client.lock_client();

        // CRITICAL: Establish keepalive streams BEFORE spawning any tasks.
        //
        // This prevents a race condition where the main coordinator task starts
        // reconciliation (which triggers lock acquisition) before the keepalive
        // streams are established.
        //
        // When many shard guards concurrently acquire etcd locks (e.g., 128 shards
        // per node), we can exceed etcd's HTTP/2 MAX_CONCURRENT_STREAMS limit (~100).
        // Each lock() call is a unary RPC requiring a NEW HTTP/2 stream, so excess
        // requests queue. This causes simple operations like kv.get() to take 1-4s.
        //
        // Lease keepalives use a BIDIRECTIONAL STREAMING RPC that establishes ONE
        // long-lived stream. By establishing these streams BEFORE any lock requests:
        // 1. The keepalive streams are ready before we hit stream limits
        // 2. All keepalives flow through these established streams
        // 3. Keepalives bypass the "new stream" queue entirely
        // 4. Leases remain alive regardless of lock contention

        // Establish membership keepalive stream (with retries)
        let (mut memb_keeper, memb_stream) = loop {
            if *shutdown_rx.borrow() {
                return Err(CoordinationError::BackendError(
                    "shutdown during keepalive setup".to_string(),
                ));
            }
            match lease_bg.keep_alive(membership_lease).await {
                Ok(x) => {
                    debug!(node_id = %nid, lease_id = membership_lease, "membership lease keepalive channel established");
                    break x;
                }
                Err(e) => {
                    warn!(node_id = %nid, error = %e, "failed to start membership keepalive, retrying...");
                    sleep(Duration::from_millis(200)).await;
                }
            }
        };

        // Establish liveness keepalive stream (with retries)
        let (mut live_keeper, live_stream) = loop {
            if *shutdown_rx.borrow() {
                return Err(CoordinationError::BackendError(
                    "shutdown during keepalive setup".to_string(),
                ));
            }
            match lease_bg.keep_alive(liveness_lease).await {
                Ok(x) => {
                    debug!(node_id = %nid, lease_id = liveness_lease, "liveness lease keepalive channel established");
                    break x;
                }
                Err(e) => {
                    warn!(node_id = %nid, error = %e, "failed to start liveness keepalive, retrying...");
                    sleep(Duration::from_millis(200)).await;
                }
            }
        };

        // Send initial keepalive requests - this is REQUIRED to keep the lease alive!
        // etcd-client's keep_alive() only establishes the channel, you must call
        // keeper.keep_alive() to actually send keepalive requests.
        if let Err(e) = memb_keeper.keep_alive().await {
            error!(node_id = %nid, error = %e, "failed to send initial membership keepalive");
        }
        if let Err(e) = live_keeper.keep_alive().await {
            error!(node_id = %nid, error = %e, "failed to send initial liveness keepalive");
        }

        // NOW spawn the keepalive loop task - streams are already established
        let keepalive_shutdown_rx = shutdown_rx.clone();
        let keepalive_nid = nid.clone();
        tokio::spawn(async move {
            let mut memb_stream = memb_stream;
            let mut live_stream = live_stream;

            // Send keepalives at 1/3 of TTL to ensure lease doesn't expire
            let keepalive_interval_secs = (ttl_secs / 3).max(1) as u64;
            let mut keepalive_timer =
                tokio::time::interval(Duration::from_secs(keepalive_interval_secs));
            debug!(node_id = %keepalive_nid, interval_secs = keepalive_interval_secs, "starting keepalive timer");

            loop {
                tokio::select! {
                    _ = keepalive_timer.tick() => {
                        if *keepalive_shutdown_rx.borrow() { break; }
                        // Send keepalive requests to both leases
                        if let Err(e) = memb_keeper.keep_alive().await {
                            error!(node_id = %keepalive_nid, error = %e, "failed to send membership keepalive");
                        }
                        if let Err(e) = live_keeper.keep_alive().await {
                            error!(node_id = %keepalive_nid, error = %e, "failed to send liveness keepalive");
                        }
                    }
                    resp = memb_stream.message() => {
                        if *keepalive_shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(ka_resp)) => {
                                debug!(node_id = %keepalive_nid, ttl = ka_resp.ttl(), "membership keepalive response received");
                            }
                            Ok(None) => {
                                error!(node_id = %keepalive_nid, "membership lease keepalive stream closed! Lease will expire.");
                                break;
                            }
                            Err(e) => {
                                error!(node_id = %keepalive_nid, error = %e, "membership lease keepalive error");
                            }
                        }
                    }
                    resp = live_stream.message() => {
                        if *keepalive_shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(ka_resp)) => {
                                debug!(node_id = %keepalive_nid, ttl = ka_resp.ttl(), "liveness keepalive response received");
                            }
                            Ok(None) => {
                                error!(node_id = %keepalive_nid, "liveness lease keepalive stream closed! Lease will expire.");
                                break;
                            }
                            Err(e) => {
                                error!(node_id = %keepalive_nid, error = %e, "liveness lease keepalive error");
                            }
                        }
                    }
                }
            }
        });

        let handle = tokio::spawn(async move {
            // Establish member watch with retries
            let members_prefix = keys::members_prefix(&cprefix);
            debug!(node_id = %nid, "starting coordinator background tasks");

            let (_members_watcher, mut members_stream) = loop {
                if *shutdown_rx.borrow() {
                    return;
                }
                match watch_bg
                    .watch(
                        members_prefix.clone(),
                        Some(etcd_client::WatchOptions::new().with_prefix()),
                    )
                    .await
                {
                    Ok(w) => {
                        debug!(node_id = %nid, prefix = %members_prefix, "members watch established");
                        break w;
                    }
                    Err(e) => {
                        warn!(node_id = %nid, error = %e, "failed to establish members watch, retrying...");
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            };

            // Initial reconcile
            if let Err(err) = me_bg.reconcile_shards(&mut lock_bg).await {
                warn!(node_id = %nid, error = %err, "initial reconcile failed");
            }

            // Main loop: reconcile on membership events and periodic resync
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
                            Ok(None) => {
                                warn!(node_id = %nid, "members watch stream closed, coordinator exiting");
                                break;
                            }
                            Err(e) => {
                                warn!(node_id = %nid, error = %e, "members watch error, continuing with periodic resync");
                            }
                        }
                    }
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
        debug!(node_id = %self.base.node_id, members = ?members, "reconcile: begin");

        // Safety check: if we don't see ourselves in the member list, our membership
        // lease may have expired or there's a connectivity issue. Don't reconcile
        // based on incomplete membership data - it would cause us to release all shards.
        if members.is_empty() {
            warn!(node_id = %self.base.node_id, "reconcile: no members found, skipping reconcile");
            return Ok(());
        }
        if !members.contains(&self.base.node_id) {
            warn!(node_id = %self.base.node_id, members = ?members, "reconcile: our node not in member list, skipping reconcile (membership may have expired)");
            return Ok(());
        }

        let desired =
            compute_desired_shards_for_node(self.base.num_shards, &self.base.node_id, &members);

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
        debug!(node_id = %self.base.node_id, desired_len = desired.len(), have_locks = ?current_locks, "reconcile: computed desired vs current");

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
            self.base.shutdown_rx.clone(),
        );
        let runner = guard.clone();
        let owned_arc = self.base.owned.clone();
        let factory = self.base.factory.clone();
        tokio::spawn(async move { runner.run(owned_arc, factory).await });
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
        self.base.owned_shards().await
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        self.base.signal_shutdown();
        {
            let guards = self.shard_guards.lock().await;
            for (_sid, guard) in guards.iter() {
                guard.set_desired(false).await;
                guard.notify.notify_one();
            }
        }

        // Give guards time to release shards and cancel ongoing acquisitions
        tokio::time::sleep(Duration::from_millis(100)).await;

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
        // Use the shared base implementation with our member-fetching closure
        self.base
            .wait_converged(timeout, || async {
                self.get_sorted_member_ids()
                    .await
                    .map_err(|e| CoordinationError::BackendError(e.to_string()))
            })
            .await
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
        Ok(self.base.compute_shard_owner_map(&members))
    }

    fn num_shards(&self) -> u32 {
        self.base.num_shards
    }

    fn node_id(&self) -> &str {
        &self.base.node_id
    }

    fn grpc_addr(&self) -> &str {
        &self.base.grpc_addr
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

    pub async fn run(
        self: Arc<Self>,
        owned_arc: Arc<Mutex<HashSet<u32>>>,
        factory: Arc<ShardFactory>,
    ) {
        use tracing::{Instrument, info_span};

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
                                    // Open the shard BEFORE marking as Held - if open fails,
                                    // we should release the lock and not claim ownership
                                    match factory.open(self.shard_id as usize).await {
                                        Ok(_) => {
                                            {
                                                let mut st = self.state.lock().await;
                                                st.held_key = Some(key);
                                                st.phase = ShardPhase::Held;
                                            }
                                            {
                                                let mut owned = owned_arc.lock().await;
                                                owned.insert(self.shard_id);
                                            }
                                            info!(shard_id = self.shard_id, attempts = attempt, "shard: acquired and opened");
                                            break;
                                        }
                                        Err(e) => {
                                            // Failed to open - release the lock and retry
                                            tracing::error!(shard_id = self.shard_id, error = %e, "failed to open shard, releasing lock");
                                            let mut lock_cli = self.client.lock_client();
                                            let _ = lock_cli.unlock(key).await;
                                            // Exponential backoff before retry
                                            let backoff_ms = 200 * (1 << attempt.min(5));
                                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                            attempt = attempt.wrapping_add(1);
                                            continue;
                                        }
                                    }
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
                            // Close the shard before releasing the lock
                            if let Err(e) = factory.close(self.shard_id as usize).await {
                                tracing::error!(shard_id = self.shard_id, error = %e, "failed to close shard before releasing lock");
                            }
                            let mut lock_cli = self.client.lock_client();
                            let _ = lock_cli.unlock(key).await;
                            {
                                let mut st = self.state.lock().await;
                                st.held_key = None;
                                st.phase = ShardPhase::Idle;
                            }
                            {
                                let mut owned = owned_arc.lock().await;
                                owned.remove(&self.shard_id);
                            }
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
                        // Close the shard before releasing the lock
                        if let Err(e) = factory.close(self.shard_id as usize).await {
                            tracing::error!(shard_id = self.shard_id, error = %e, "failed to close shard during shutdown");
                        }
                        let mut lock_cli = self.client.lock_client();
                        let _ = lock_cli.unlock(key).await;
                        {
                            let mut owned = owned_arc.lock().await;
                            owned.remove(&self.shard_id);
                        }
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
