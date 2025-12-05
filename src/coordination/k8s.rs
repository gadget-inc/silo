//! Kubernetes Lease-based coordination backend.
//!
//! This backend uses Kubernetes Lease objects for distributed shard ownership.
//! It's designed for deployments where Silo runs inside a Kubernetes cluster.
//!
//! ## Key Design Decisions
//!
//! - **Reactive membership detection**: Uses K8s watches for immediate notification of
//!   membership changes, with a 30-second safety reconciliation as a fallback.
//!
//! - **Optimistic concurrency (CAS)**: All lease operations use `replace` with
//!   `resourceVersion` to ensure atomic updates. This prevents races where two nodes
//!   could both think they acquired the same shard.
//!
//! - **Safe release**: Shard leases are released by clearing `holderIdentity` instead
//!   of deleting the lease object. This avoids races where deleting could remove
//!   another node's lease if they acquired it between our check and delete.
//!
//! - **Fencing via resourceVersion**: Each guard tracks its `resourceVersion` and
//!   verifies ownership before every operation. If the version changes unexpectedly
//!   or we're no longer the holder, we consider the lease lost.
//!
//! ## Known Limitations
//!
//! - **Clock skew sensitivity**: Lease expiry is checked using local time compared to
//!   the lease's `renewTime`. Significant clock skew between nodes could cause
//!   incorrect expiry detection. We recommend using NTP or similar time sync.
//!
//! - **API server dependency**: All coordination goes through the K8s API server.
//!   If the API server is unavailable, coordination operations will fail.

use async_trait::async_trait;
use chrono::Utc;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::{
    api::{Api, ListParams, Patch, PatchParams, PostParams},
    runtime::watcher::{watcher, Config as WatcherConfig, Event},
    Client,
};
use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, Notify};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::factory::ShardFactory;

use super::{
    compute_desired_shards_for_node, keys, CoordinationError, Coordinator, CoordinatorBase,
    MemberInfo, ShardOwnerMap, ShardPhase,
};

/// Format a chrono DateTime for K8S MicroTime (RFC3339 with microseconds and Z suffix)
fn format_microtime(dt: chrono::DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

/// Kubernetes Lease-based coordinator for distributed shard ownership.
#[derive(Clone)]
pub struct K8sCoordinator {
    /// Shared coordinator state (node_id, grpc_addr, owned set, shutdown, factory)
    base: Arc<CoordinatorBase>,
    client: Client,
    namespace: String,
    cluster_prefix: String,
    lease_duration_secs: i32,
    shard_guards: Arc<Mutex<HashMap<u32, Arc<K8sShardGuard>>>>,
    /// Cache of active members, updated by the watcher
    members_cache: Arc<Mutex<Vec<MemberInfo>>>,
    /// Notifier for membership changes (from watcher)
    membership_changed: Arc<Notify>,
}

impl K8sCoordinator {
    /// Start the K8S coordinator.
    ///
    /// The coordinator will manage shard ownership and automatically open/close
    /// shards in the factory as ownership changes.
    pub async fn start(
        namespace: &str,
        cluster_prefix: &str,
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
        lease_duration_secs: i64,
        factory: Arc<ShardFactory>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), CoordinationError> {
        let client = Client::try_default()
            .await
            .map_err(|e| CoordinationError::ConnectionFailed(e.to_string()))?;

        let namespace = namespace.to_string();
        let cluster_prefix = cluster_prefix.to_string();
        let node_id = node_id.into();
        let grpc_addr = grpc_addr.into();

        // Create or update membership lease
        let leases: Api<Lease> = Api::namespaced(client.clone(), &namespace);
        let member_lease_name = keys::k8s_member_lease_name(&cluster_prefix, &node_id);

        let member_info = MemberInfo {
            node_id: node_id.clone(),
            grpc_addr: grpc_addr.clone(),
        };
        let member_info_json = serde_json::to_string(&member_info)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let lease_spec = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": member_lease_name,
                "labels": {
                    "silo.dev/type": "member",
                    "silo.dev/cluster": cluster_prefix,
                    "silo.dev/node": node_id
                },
                "annotations": {
                    "silo.dev/member-info": member_info_json
                }
            },
            "spec": {
                "holderIdentity": node_id,
                "leaseDurationSeconds": lease_duration_secs as i32,
                "acquireTime": format_microtime(Utc::now()),
                "renewTime": format_microtime(Utc::now())
            }
        });

        leases
            .patch(
                &member_lease_name,
                &PatchParams::apply("silo-coordinator").force(),
                &Patch::Apply(&lease_spec),
            )
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let base = Arc::new(CoordinatorBase::new(
            node_id.clone(),
            grpc_addr,
            num_shards,
            factory,
        ));
        let members_cache = Arc::new(Mutex::new(Vec::new()));
        let membership_changed = Arc::new(Notify::new());

        let me = Self {
            base: base.clone(),
            client: client.clone(),
            namespace: namespace.clone(),
            cluster_prefix: cluster_prefix.clone(),
            lease_duration_secs: lease_duration_secs as i32,
            shard_guards: Arc::new(Mutex::new(HashMap::new())),
            members_cache,
            membership_changed,
        };

        // Do initial member list fetch before starting background tasks
        me.refresh_members_cache().await?;

        let me_bg = me.clone();
        let nid = node_id.clone();

        let handle = tokio::spawn(async move {
            // Start the membership watcher task
            let watcher_me = me_bg.clone();
            let watcher_shutdown = me_bg.base.shutdown_rx.clone();
            let watcher_handle = tokio::spawn(async move {
                watcher_me.run_membership_watcher(watcher_shutdown).await;
            });

            // Run the main coordination loop
            me_bg
                .run_coordination_loop(me_bg.base.shutdown_rx.clone())
                .await;

            // Clean up watcher on shutdown
            watcher_handle.abort();
            debug!(node_id = %nid, "k8s coordinator background task exiting");
        });

        Ok((me, handle))
    }

    /// Run the membership watcher - watches for lease changes and updates the members cache
    async fn run_membership_watcher(&self, mut shutdown_rx: watch::Receiver<bool>) {
        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);

        // Create watcher with automatic reconnection
        let watcher_config = WatcherConfig::default().labels(&format!(
            "silo.dev/type=member,silo.dev/cluster={}",
            self.cluster_prefix
        ));
        let watch_stream = watcher(leases, watcher_config);
        let mut watch_stream = pin!(watch_stream);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!(node_id = %self.base.node_id, "membership watcher shutting down");
                        break;
                    }
                }
                event = watch_stream.next() => {
                    match event {
                        Some(Ok(ev)) => {
                            match ev {
                                Event::Apply(lease) | Event::InitApply(lease) => {
                                    let lease_name = lease.metadata.name.as_deref().unwrap_or("unknown");
                                    debug!(node_id = %self.base.node_id, lease = %lease_name, "membership lease changed");
                                    // Refresh the full member list from cache
                                    if let Err(e) = self.refresh_members_cache().await {
                                        warn!(node_id = %self.base.node_id, error = %e, "failed to refresh members cache");
                                    }
                                    self.membership_changed.notify_waiters();
                                }
                                Event::Delete(lease) => {
                                    let lease_name = lease.metadata.name.as_deref().unwrap_or("unknown");
                                    debug!(node_id = %self.base.node_id, lease = %lease_name, "membership lease deleted");
                                    if let Err(e) = self.refresh_members_cache().await {
                                        warn!(node_id = %self.base.node_id, error = %e, "failed to refresh members cache");
                                    }
                                    self.membership_changed.notify_waiters();
                                }
                                Event::Init => {
                                    debug!(node_id = %self.base.node_id, "membership watcher initialized");
                                }
                                Event::InitDone => {
                                    debug!(node_id = %self.base.node_id, "membership watcher init done");
                                    if let Err(e) = self.refresh_members_cache().await {
                                        warn!(node_id = %self.base.node_id, error = %e, "failed to refresh members cache on init");
                                    }
                                    self.membership_changed.notify_waiters();
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(node_id = %self.base.node_id, error = %e, "membership watcher error, will retry");
                            // The watcher automatically reconnects on errors
                        }
                        None => {
                            debug!(node_id = %self.base.node_id, "membership watcher stream ended");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Main coordination loop - renews membership and reconciles shards.
    ///
    /// Reconciliation is primarily reactive (triggered by the membership watcher),
    /// but we also do periodic reconciliation as a safety net in case watches miss
    /// events due to network issues.
    async fn run_coordination_loop(&self, mut shutdown_rx: watch::Receiver<bool>) {
        // Renew membership lease periodically (at 1/3 of lease duration)
        let renew_interval = Duration::from_secs((self.lease_duration_secs / 3).max(1) as u64);
        let mut renew_timer = tokio::time::interval(renew_interval);

        // Periodic reconciliation as a safety net - much less frequent since we have watches
        // This catches any missed watch events due to network issues
        let safety_reconcile_interval = Duration::from_secs(30);
        let mut safety_reconcile_timer = tokio::time::interval(safety_reconcile_interval);

        // Do initial reconciliation immediately
        debug!(node_id = %self.base.node_id, "performing initial shard reconciliation");
        if let Err(e) = self.reconcile_shards().await {
            warn!(node_id = %self.base.node_id, error = %e, "initial reconcile failed");
        }

        loop {
            tokio::select! {
                biased;

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!(node_id = %self.base.node_id, "coordination loop shutting down");
                        break;
                    }
                }
                // React to membership changes from the watcher (primary path)
                _ = self.membership_changed.notified() => {
                    info!(node_id = %self.base.node_id, "membership changed, reconciling shards");
                    if let Err(e) = self.reconcile_shards().await {
                        warn!(node_id = %self.base.node_id, error = %e, "reconcile after membership change failed");
                    }
                }
                // Periodic membership lease renewal
                _ = renew_timer.tick() => {
                    if let Err(e) = self.renew_membership_lease().await {
                        warn!(node_id = %self.base.node_id, error = %e, "failed to renew membership lease");
                    }
                }
                // Safety net: periodic reconciliation in case watches missed events
                _ = safety_reconcile_timer.tick() => {
                    debug!(node_id = %self.base.node_id, "periodic safety reconciliation");
                    // Refresh the cache in case it's stale
                    if let Err(e) = self.refresh_members_cache().await {
                        warn!(node_id = %self.base.node_id, error = %e, "failed to refresh members cache in safety reconcile");
                    }
                    if let Err(e) = self.reconcile_shards().await {
                        warn!(node_id = %self.base.node_id, error = %e, "periodic safety reconcile failed");
                    }
                }
            }
        }
    }

    /// Refresh the members cache by listing all active member leases
    async fn refresh_members_cache(&self) -> Result<(), CoordinationError> {
        let members = self.list_active_members().await?;
        let mut cache = self.members_cache.lock().await;
        *cache = members;
        Ok(())
    }

    /// List all active (non-expired) members from K8s
    async fn list_active_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lp = ListParams::default().labels(&format!(
            "silo.dev/type=member,silo.dev/cluster={}",
            self.cluster_prefix
        ));

        let lease_list = leases
            .list(&lp)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let now = Utc::now();
        let mut members = Vec::new();

        for lease in lease_list {
            let spec = match &lease.spec {
                Some(s) => s,
                None => continue,
            };

            // Check if lease is expired
            let renew_time = spec
                .renew_time
                .as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t.0.to_rfc3339()).ok());
            let duration_secs = spec.lease_duration_seconds.unwrap_or(10);

            if let Some(renew) = renew_time {
                let expiry = renew + chrono::Duration::seconds(duration_secs as i64);
                if now > expiry {
                    // Lease is expired, skip this member
                    continue;
                }
            }

            // Parse member info from annotation
            if let Some(metadata) = &lease.metadata.annotations {
                if let Some(info_json) = metadata.get("silo.dev/member-info") {
                    if let Ok(info) = serde_json::from_str::<MemberInfo>(info_json) {
                        members.push(info);
                    }
                }
            }
        }

        members.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(members)
    }

    /// Renew the membership lease.
    ///
    /// We verify that we're still the holder before updating, and use CAS to ensure
    /// we don't accidentally renew someone else's lease if ours expired.
    async fn renew_membership_lease(&self) -> Result<(), CoordinationError> {
        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let member_lease_name =
            keys::k8s_member_lease_name(&self.cluster_prefix, &self.base.node_id);

        // Get current lease to verify we still own it
        let existing = match leases.get(&member_lease_name).await {
            Ok(l) => l,
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // Our lease was deleted - this is a critical error
                return Err(CoordinationError::BackendError(
                    "membership lease was deleted".into(),
                ));
            }
            Err(e) => return Err(CoordinationError::BackendError(e.to_string())),
        };

        // Verify we're still the holder
        let holder = existing
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());
        if holder != Some(&self.base.node_id) {
            return Err(CoordinationError::BackendError(format!(
                "we are no longer the membership lease holder (holder={:?})",
                holder
            )));
        }

        let current_rv = existing
            .metadata
            .resource_version
            .as_ref()
            .ok_or_else(|| CoordinationError::BackendError("no resourceVersion".into()))?;

        // Build updated lease with CAS
        let member_info = MemberInfo {
            node_id: self.base.node_id.clone(),
            grpc_addr: self.base.grpc_addr.clone(),
        };
        let member_info_json = serde_json::to_string(&member_info)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let updated_lease = Lease {
            metadata: kube::api::ObjectMeta {
                name: Some(member_lease_name.clone()),
                namespace: Some(self.namespace.clone()),
                resource_version: Some(current_rv.clone()),
                uid: existing.metadata.uid.clone(),
                labels: existing.metadata.labels.clone(),
                annotations: Some([("silo.dev/member-info".to_string(), member_info_json)].into()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                holder_identity: Some(self.base.node_id.clone()),
                lease_duration_seconds: Some(self.lease_duration_secs),
                acquire_time: existing.spec.as_ref().and_then(|s| s.acquire_time.clone()),
                renew_time: Some(MicroTime(Utc::now())),
                ..Default::default()
            }),
        };

        match leases
            .replace(&member_lease_name, &PostParams::default(), &updated_lease)
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 409 => {
                // Conflict - someone else modified our lease
                Err(CoordinationError::BackendError(
                    "CAS conflict during membership renewal".into(),
                ))
            }
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn reconcile_shards(&self) -> Result<(), CoordinationError> {
        let members = {
            let cache = self.members_cache.lock().await;
            cache.iter().map(|m| m.node_id.clone()).collect::<Vec<_>>()
        };

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
        debug!(node_id = %self.base.node_id, members = ?members, desired = ?desired, "reconcile: begin");

        // Release undesired shards
        {
            let to_release: Vec<u32> = {
                let guards = self.shard_guards.lock().await;
                let mut v = Vec::new();
                for (sid, guard) in guards.iter() {
                    if !desired.contains(sid) && guard.is_held().await {
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

    async fn ensure_shard_guard(&self, shard_id: u32) -> Arc<K8sShardGuard> {
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
        debug!(shard_id, "creating new shard guard");
        let guard = K8sShardGuard::new(
            shard_id,
            self.client.clone(),
            self.namespace.clone(),
            self.cluster_prefix.clone(),
            self.base.node_id.clone(),
            self.lease_duration_secs,
            self.base.shutdown_rx.clone(),
        );
        let runner = guard.clone();
        let owned_arc = self.base.owned.clone();
        let factory = self.base.factory.clone();
        tokio::spawn(async move { runner.run(owned_arc, factory).await });
        guards.insert(shard_id, guard.clone());
        guard
    }

    async fn get_active_member_ids(&self) -> Result<Vec<String>, CoordinationError> {
        let cache = self.members_cache.lock().await;
        Ok(cache.iter().map(|m| m.node_id.clone()).collect())
    }
}

#[async_trait]
impl Coordinator for K8sCoordinator {
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

        // Give guards time to release
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Delete membership lease
        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let member_lease_name =
            keys::k8s_member_lease_name(&self.cluster_prefix, &self.base.node_id);
        let _ = leases.delete(&member_lease_name, &Default::default()).await;

        Ok(())
    }

    async fn wait_converged(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            // Refresh the members cache to get current membership.
            // This is important because the watcher runs asynchronously and might not
            // have processed membership changes yet (e.g., after a node shutdown).
            if let Err(e) = self.refresh_members_cache().await {
                debug!(node_id = %self.base.node_id, error = %e, "wait_converged: failed to refresh members cache");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let member_ids = match self.get_active_member_ids().await {
                Ok(m) => m,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if member_ids.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let desired: HashSet<u32> = compute_desired_shards_for_node(
                self.base.num_shards,
                &self.base.node_id,
                &member_ids,
            );
            let guard = self.base.owned.lock().await;
            if *guard == desired {
                return true;
            }
            drop(guard);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        // Return cached members for consistency
        let cache = self.members_cache.lock().await;
        Ok(cache.clone())
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

/// State for a shard guard, including the resourceVersion for fencing
pub struct ShardState {
    pub desired: bool,
    pub phase: ShardPhase,
    /// The resourceVersion of the lease when we acquired it - used for fencing
    pub resource_version: Option<String>,
    /// The UID of the lease - used for conditional operations
    pub lease_uid: Option<String>,
}

/// Per-shard lease guard for K8s backend with CAS semantics.
///
/// Key invariants:
/// - We only consider ourselves the owner if we have a valid resource_version
/// - All lease modifications use optimistic concurrency (resourceVersion checks)
/// - Release clears holderIdentity instead of deleting to avoid races
pub struct K8sShardGuard {
    pub shard_id: u32,
    pub client: Client,
    pub namespace: String,
    pub cluster_prefix: String,
    pub node_id: String,
    pub lease_duration_secs: i32,
    pub state: Mutex<ShardState>,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
}

impl K8sShardGuard {
    pub fn new(
        shard_id: u32,
        client: Client,
        namespace: String,
        cluster_prefix: String,
        node_id: String,
        lease_duration_secs: i32,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            client,
            namespace,
            cluster_prefix,
            node_id,
            lease_duration_secs,
            state: Mutex::new(ShardState {
                desired: false,
                phase: ShardPhase::Idle,
                resource_version: None,
                lease_uid: None,
            }),
            notify: Notify::new(),
            shutdown,
        })
    }

    fn lease_name(&self) -> String {
        keys::k8s_shard_lease_name(&self.cluster_prefix, self.shard_id)
    }

    pub async fn is_held(&self) -> bool {
        let st = self.state.lock().await;
        st.phase == ShardPhase::Held && st.resource_version.is_some()
    }

    pub async fn set_desired(&self, desired: bool) {
        let mut st = self.state.lock().await;
        if matches!(st.phase, ShardPhase::ShutDown | ShardPhase::ShuttingDown) {
            debug!(
                shard_id = self.shard_id,
                desired, "set_desired: guard is shutting down, ignoring"
            );
            return;
        }
        if st.desired != desired {
            debug!(shard_id = self.shard_id, desired, phase = ?st.phase, "set_desired: changing desired state");
            st.desired = desired;
            self.notify.notify_one();
        }
    }

    pub async fn run(
        self: Arc<Self>,
        owned_arc: Arc<Mutex<HashSet<u32>>>,
        factory: Arc<ShardFactory>,
    ) {
        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lease_name = self.lease_name();

        loop {
            if *self.shutdown.borrow() {
                let mut st = self.state.lock().await;
                st.phase = ShardPhase::ShuttingDown;
            }

            {
                let mut st = self.state.lock().await;
                match (st.phase, st.desired, st.resource_version.is_some()) {
                    (ShardPhase::ShutDown, _, _) => {}
                    (ShardPhase::ShuttingDown, _, _) => {}
                    (ShardPhase::Idle, true, _) => {
                        st.phase = ShardPhase::Acquiring;
                    }
                    (ShardPhase::Held, false, _) => {
                        st.phase = ShardPhase::Releasing;
                    }
                    _ => {}
                }
            }

            let phase = { self.state.lock().await.phase };
            match phase {
                ShardPhase::ShutDown => break,
                ShardPhase::Acquiring => {
                    let mut attempt: u32 = 0;
                    let initial_jitter_ms = ((self.shard_id as u64).wrapping_mul(13)) % 80;
                    tokio::time::sleep(Duration::from_millis(initial_jitter_ms)).await;

                    loop {
                        {
                            let mut st = self.state.lock().await;
                            if !st.desired || st.phase != ShardPhase::Acquiring {
                                if !st.desired && st.phase == ShardPhase::Acquiring {
                                    st.phase = ShardPhase::Idle;
                                }
                                break;
                            }
                        }

                        // Try to acquire the lease with CAS semantics
                        match self.try_acquire_lease_cas(&leases, &lease_name).await {
                            Ok((rv, uid)) => {
                                // Open the shard BEFORE marking as Held - if open fails,
                                // we should release the lease and not claim ownership
                                match factory.open(self.shard_id as usize).await {
                                    Ok(_) => {
                                        {
                                            let mut st = self.state.lock().await;
                                            st.resource_version = Some(rv.clone());
                                            st.lease_uid = uid;
                                            st.phase = ShardPhase::Held;
                                        }
                                        {
                                            let mut owned = owned_arc.lock().await;
                                            owned.insert(self.shard_id);
                                        }
                                        info!(shard_id = self.shard_id, rv = %rv, attempts = attempt, "k8s shard: acquired and opened");

                                        // Start renewal loop
                                        self.clone()
                                            .run_renewal_loop(
                                                owned_arc.clone(),
                                                factory.clone(),
                                                &leases,
                                                &lease_name,
                                            )
                                            .await;
                                        break;
                                    }
                                    Err(e) => {
                                        // Failed to open - release the lease and retry
                                        tracing::error!(shard_id = self.shard_id, error = %e, "failed to open shard, releasing lease");
                                        let _ = self.release_lease_cas(&leases, &lease_name).await;
                                        // Exponential backoff before retry
                                        let backoff_ms = 200 * (1 << attempt.min(5));
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                        attempt = attempt.wrapping_add(1);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                debug!(shard_id = self.shard_id, attempt, error = %e, "failed to acquire shard lease, retrying");
                                attempt = attempt.wrapping_add(1);
                                let jitter_ms = ((self.shard_id as u64)
                                    .wrapping_mul(31)
                                    .wrapping_add(attempt as u64 * 17))
                                    % 150;
                                tokio::time::sleep(Duration::from_millis(100 + jitter_ms)).await;
                            }
                        }
                    }
                }
                ShardPhase::Releasing => {
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    let cancelled = {
                        let mut st = self.state.lock().await;
                        if st.phase == ShardPhase::ShuttingDown {
                            false
                        } else if st.desired {
                            st.phase = ShardPhase::Held;
                            true
                        } else {
                            false
                        }
                    };

                    if !cancelled {
                        // Close the shard before releasing the lease
                        if let Err(e) = factory.close(self.shard_id as usize).await {
                            tracing::error!(shard_id = self.shard_id, error = %e, "failed to close shard before releasing lease");
                        }

                        // Release the lease by clearing holderIdentity with CAS
                        let has_rv = {
                            let st = self.state.lock().await;
                            st.resource_version.is_some()
                        };

                        if has_rv {
                            match self.release_lease_cas(&leases, &lease_name).await {
                                Ok(_) => {
                                    debug!(
                                        shard_id = self.shard_id,
                                        "k8s shard: released with CAS"
                                    );
                                }
                                Err(e) => {
                                    // This is okay - we may have already lost the lease
                                    debug!(shard_id = self.shard_id, error = %e, "k8s shard: release CAS failed (may have lost lease)");
                                }
                            }
                        }

                        {
                            let mut st = self.state.lock().await;
                            st.resource_version = None;
                            st.lease_uid = None;
                            st.phase = ShardPhase::Idle;
                        }
                        let mut owned = owned_arc.lock().await;
                        owned.remove(&self.shard_id);
                        debug!(shard_id = self.shard_id, "k8s shard: released");
                    }
                }
                ShardPhase::ShuttingDown => {
                    // Close the shard before releasing the lease
                    if let Err(e) = factory.close(self.shard_id as usize).await {
                        tracing::error!(shard_id = self.shard_id, error = %e, "failed to close shard during shutdown");
                    }

                    // Release our lease if we hold it
                    let has_rv = {
                        let st = self.state.lock().await;
                        st.resource_version.is_some()
                    };

                    if has_rv {
                        let _ = self.release_lease_cas(&leases, &lease_name).await;
                    }

                    {
                        let mut st = self.state.lock().await;
                        st.resource_version = None;
                        st.lease_uid = None;
                        st.phase = ShardPhase::ShutDown;
                    }
                    let mut owned = owned_arc.lock().await;
                    owned.remove(&self.shard_id);
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

    /// Try to acquire the lease using compare-and-swap semantics.
    ///
    /// Returns (resourceVersion, Option<uid>) on success.
    async fn try_acquire_lease_cas(
        &self,
        leases: &Api<Lease>,
        lease_name: &str,
    ) -> Result<(String, Option<String>), CoordinationError> {
        let now = Utc::now();
        debug!(
            shard_id = self.shard_id,
            lease_name, "try_acquire_lease_cas: checking lease"
        );

        // First, try to get existing lease
        match leases.get(lease_name).await {
            Ok(existing) => {
                let existing_rv = existing.metadata.resource_version.clone();
                let existing_uid = existing.metadata.uid.clone();
                let spec = existing.spec.as_ref();
                let holder = spec.and_then(|s| s.holder_identity.as_ref());
                let renew_time = spec
                    .and_then(|s| s.renew_time.as_ref())
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t.0.to_rfc3339()).ok());
                let duration_secs = spec
                    .and_then(|s| s.lease_duration_seconds)
                    .unwrap_or(self.lease_duration_secs);

                let is_expired = renew_time
                    .map(|rt| now > rt + chrono::Duration::seconds(duration_secs as i64))
                    .unwrap_or(true);

                // Check if holder is empty (released) or expired or already ours
                let holder_is_empty = holder.map(|h| h.is_empty()).unwrap_or(true);
                let is_ours = holder.map(|h| h == &self.node_id).unwrap_or(false);
                let can_acquire = holder_is_empty || is_expired || is_ours;

                if can_acquire {
                    // Use JSON Patch with test operation for true CAS
                    // This ensures we only update if the resourceVersion matches
                    let rv = existing_rv.ok_or_else(|| {
                        CoordinationError::BackendError(
                            "existing lease has no resourceVersion".into(),
                        )
                    })?;

                    // Build the full lease for replace (includes resourceVersion check)
                    let updated_lease = Lease {
                        metadata: kube::api::ObjectMeta {
                            name: Some(lease_name.to_string()),
                            namespace: Some(self.namespace.clone()),
                            resource_version: Some(rv.clone()),
                            uid: existing_uid.clone(),
                            labels: Some(
                                [
                                    ("silo.dev/type".to_string(), "shard".to_string()),
                                    ("silo.dev/cluster".to_string(), self.cluster_prefix.clone()),
                                    ("silo.dev/shard".to_string(), self.shard_id.to_string()),
                                ]
                                .into(),
                            ),
                            ..Default::default()
                        },
                        spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                            holder_identity: Some(self.node_id.clone()),
                            lease_duration_seconds: Some(self.lease_duration_secs),
                            acquire_time: Some(MicroTime(now)),
                            renew_time: Some(MicroTime(now)),
                            ..Default::default()
                        }),
                    };

                    // Use replace which does CAS based on resourceVersion
                    match leases
                        .replace(lease_name, &PostParams::default(), &updated_lease)
                        .await
                    {
                        Ok(result) => {
                            let new_rv = result.metadata.resource_version.ok_or_else(|| {
                                CoordinationError::BackendError(
                                    "no resource_version in response".into(),
                                )
                            })?;
                            debug!(
                                shard_id = self.shard_id,
                                old_rv = %rv,
                                new_rv = %new_rv,
                                "try_acquire_lease_cas: CAS succeeded"
                            );
                            return Ok((new_rv, result.metadata.uid));
                        }
                        Err(kube::Error::Api(e)) if e.code == 409 => {
                            // Conflict - someone else modified the lease
                            debug!(
                                shard_id = self.shard_id,
                                "try_acquire_lease_cas: CAS conflict, someone else acquired"
                            );
                            return Err(CoordinationError::BackendError(
                                "CAS conflict: lease was modified".into(),
                            ));
                        }
                        Err(e) => {
                            return Err(CoordinationError::BackendError(e.to_string()));
                        }
                    }
                }

                // Lease is held by someone else and not expired
                debug!(
                    shard_id = self.shard_id,
                    holder = ?holder,
                    is_expired,
                    "try_acquire_lease_cas: lease held by another node"
                );
                Err(CoordinationError::BackendError(
                    "lease held by another node".into(),
                ))
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // Lease doesn't exist, create it
                debug!(
                    shard_id = self.shard_id,
                    lease_name, "try_acquire_lease_cas: lease not found, creating"
                );
                let lease_spec = Lease {
                    metadata: kube::api::ObjectMeta {
                        name: Some(lease_name.to_string()),
                        labels: Some(
                            [
                                ("silo.dev/type".to_string(), "shard".to_string()),
                                ("silo.dev/cluster".to_string(), self.cluster_prefix.clone()),
                                ("silo.dev/shard".to_string(), self.shard_id.to_string()),
                            ]
                            .into(),
                        ),
                        ..Default::default()
                    },
                    spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                        holder_identity: Some(self.node_id.clone()),
                        lease_duration_seconds: Some(self.lease_duration_secs),
                        acquire_time: Some(MicroTime(now)),
                        renew_time: Some(MicroTime(now)),
                        ..Default::default()
                    }),
                };

                match leases.create(&PostParams::default(), &lease_spec).await {
                    Ok(result) => {
                        let rv = result.metadata.resource_version.ok_or_else(|| {
                            CoordinationError::BackendError("no resource_version".into())
                        })?;
                        debug!(
                            shard_id = self.shard_id,
                            lease_name,
                            rv = %rv,
                            "try_acquire_lease_cas: created successfully"
                        );
                        Ok((rv, result.metadata.uid))
                    }
                    Err(kube::Error::Api(e)) if e.code == 409 => {
                        // Already exists - race with another node
                        debug!(
                            shard_id = self.shard_id,
                            "try_acquire_lease_cas: create conflict, lease already exists"
                        );
                        Err(CoordinationError::BackendError(
                            "create conflict: lease already exists".into(),
                        ))
                    }
                    Err(e) => {
                        debug!(
                            shard_id = self.shard_id,
                            lease_name,
                            error = %e,
                            "try_acquire_lease_cas: create failed"
                        );
                        Err(CoordinationError::BackendError(e.to_string()))
                    }
                }
            }
            Err(e) => {
                debug!(
                    shard_id = self.shard_id,
                    error = %e,
                    "try_acquire_lease_cas: unexpected error getting lease"
                );
                Err(CoordinationError::BackendError(e.to_string()))
            }
        }
    }

    /// Release the lease using CAS - clears holderIdentity instead of deleting.
    /// We always verify we're still the holder before releasing to avoid clearing
    /// someone else's lease.
    async fn release_lease_cas(
        &self,
        leases: &Api<Lease>,
        lease_name: &str,
    ) -> Result<String, CoordinationError> {
        let now = Utc::now();

        // Get current lease to verify we still own it
        let existing = leases
            .get(lease_name)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let current_rv = existing
            .metadata
            .resource_version
            .as_ref()
            .ok_or_else(|| CoordinationError::BackendError("no resourceVersion".into()))?;

        // Always verify we're still the holder before releasing
        let holder = existing
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());
        if holder != Some(&self.node_id) {
            return Err(CoordinationError::BackendError(
                "we are no longer the holder".into(),
            ));
        }

        // Clear holder identity using replace with CAS
        let updated_lease = Lease {
            metadata: kube::api::ObjectMeta {
                name: Some(lease_name.to_string()),
                namespace: Some(self.namespace.clone()),
                resource_version: Some(current_rv.clone()),
                uid: existing.metadata.uid.clone(),
                labels: existing.metadata.labels.clone(),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                holder_identity: Some(String::new()), // Clear holder
                lease_duration_seconds: Some(self.lease_duration_secs),
                acquire_time: existing.spec.as_ref().and_then(|s| s.acquire_time.clone()),
                renew_time: Some(MicroTime(now)),
                ..Default::default()
            }),
        };

        let result = leases
            .replace(lease_name, &PostParams::default(), &updated_lease)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let new_rv = result
            .metadata
            .resource_version
            .ok_or_else(|| CoordinationError::BackendError("no resource_version".into()))?;

        debug!(
            shard_id = self.shard_id,
            old_rv = %current_rv,
            new_rv = %new_rv,
            "release_lease_cas: cleared holder"
        );
        Ok(new_rv)
    }

    async fn run_renewal_loop(
        self: Arc<Self>,
        owned_arc: Arc<Mutex<HashSet<u32>>>,
        factory: Arc<ShardFactory>,
        leases: &Api<Lease>,
        lease_name: &str,
    ) {
        let renew_interval = Duration::from_secs((self.lease_duration_secs / 3).max(1) as u64);

        loop {
            // Wait for renewal interval, but wake up early if notified
            tokio::select! {
                _ = tokio::time::sleep(renew_interval) => {}
                _ = self.notify.notified() => {}
            }

            let (phase, desired, expected_rv) = {
                let st = self.state.lock().await;
                (st.phase, st.desired, st.resource_version.clone())
            };

            // Exit if no longer held, or if we should release
            if phase != ShardPhase::Held || !desired {
                break;
            }

            if *self.shutdown.borrow() {
                break;
            }

            if expected_rv.is_none() {
                warn!(
                    shard_id = self.shard_id,
                    "renewal loop: no resourceVersion, exiting"
                );
                break;
            }

            // Renew with CAS to detect if we lost the lease
            match self.renew_lease_cas(leases, lease_name).await {
                Ok(new_rv) => {
                    // Update our fencing token
                    let mut st = self.state.lock().await;
                    st.resource_version = Some(new_rv);
                }
                Err(e) => {
                    warn!(shard_id = self.shard_id, error = %e, "failed to renew shard lease (lost ownership)");
                    // We lost the lease - close the shard and update state
                    if let Err(close_err) = factory.close(self.shard_id as usize).await {
                        tracing::error!(shard_id = self.shard_id, error = %close_err, "failed to close shard after losing lease");
                    }
                    {
                        let mut st = self.state.lock().await;
                        st.resource_version = None;
                        st.lease_uid = None;
                        st.phase = ShardPhase::Idle;
                    }
                    let mut owned = owned_arc.lock().await;
                    owned.remove(&self.shard_id);
                    self.notify.notify_one();
                    break;
                }
            }
        }
    }

    /// Renew the lease with CAS semantics.
    /// We fetch the current lease, verify we're still the holder, then update with CAS.
    async fn renew_lease_cas(
        &self,
        leases: &Api<Lease>,
        lease_name: &str,
    ) -> Result<String, CoordinationError> {
        let now = Utc::now();

        // Get current lease to verify we still own it and get current state
        let existing = leases
            .get(lease_name)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let current_rv = existing
            .metadata
            .resource_version
            .as_ref()
            .ok_or_else(|| CoordinationError::BackendError("no resourceVersion".into()))?;

        // Verify we're still the holder
        let holder = existing
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());

        if holder != Some(&self.node_id) {
            return Err(CoordinationError::BackendError(format!(
                "we are no longer the holder (holder={:?})",
                holder
            )));
        }

        // Renew using replace with CAS
        let updated_lease = Lease {
            metadata: kube::api::ObjectMeta {
                name: Some(lease_name.to_string()),
                namespace: Some(self.namespace.clone()),
                resource_version: Some(current_rv.clone()),
                uid: existing.metadata.uid.clone(),
                labels: existing.metadata.labels.clone(),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                holder_identity: Some(self.node_id.clone()),
                lease_duration_seconds: Some(self.lease_duration_secs),
                acquire_time: existing.spec.as_ref().and_then(|s| s.acquire_time.clone()),
                renew_time: Some(MicroTime(now)),
                ..Default::default()
            }),
        };

        match leases
            .replace(lease_name, &PostParams::default(), &updated_lease)
            .await
        {
            Ok(result) => {
                let new_rv = result.metadata.resource_version.ok_or_else(|| {
                    CoordinationError::BackendError("no resource_version in response".into())
                })?;
                Ok(new_rv)
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                // Conflict - someone else modified the lease
                Err(CoordinationError::BackendError(
                    "CAS conflict during renewal".into(),
                ))
            }
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }
}
