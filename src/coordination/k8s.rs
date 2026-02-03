use async_trait::async_trait;
use chrono::Utc;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, watch};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::dst_events::{self, DstEvent};
use crate::factory::ShardFactory;
use crate::shard_range::{ShardId, ShardMap, SplitInProgress};

use super::k8s_backend::{ConfigMapWatchEvent, K8sBackend, KubeBackend, LeaseWatchEvent};
use super::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap, ShardPhase,
    SplitStorageBackend, compute_desired_shards_for_node, get_hostname, keys,
};

/// Format a chrono DateTime for K8S MicroTime (RFC3339 with microseconds and Z suffix)
fn format_microtime(dt: chrono::DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

/// Configuration for starting a K8sCoordinator
pub struct K8sCoordinatorConfig {
    pub namespace: String,
    pub cluster_prefix: String,
    pub node_id: String,
    pub grpc_addr: String,
    pub initial_shard_count: u32,
    pub lease_duration_secs: i64,
    /// Placement rings this node participates in.
    /// Empty means the node participates in the default ring only.
    pub placement_rings: Vec<String>,
}

/// Kubernetes Lease-based coordinator for distributed shard ownership.
///
/// This coordinator is generic over `K8sBackend`, allowing it to work with either:
/// - `KubeBackend`: Real Kubernetes API via kube-rs (production)
/// - Test backends: Simulated K8s API for deterministic testing
#[derive(Clone)]
pub struct K8sCoordinator<B: K8sBackend> {
    /// Shared coordinator state (node_id, grpc_addr, owned set, shutdown, factory)
    base: Arc<CoordinatorBase>,
    backend: B,
    namespace: String,
    cluster_prefix: String,
    lease_duration_secs: i32,
    shard_guards: Arc<Mutex<HashMap<ShardId, Arc<K8sShardGuard<B>>>>>,
    /// Cache of active members, updated by the watcher
    members_cache: Arc<Mutex<Vec<MemberInfo>>>,
    /// Notifier for membership changes (from watcher)
    membership_changed: Arc<Notify>,
    /// Notifier for shard map changes (from watcher)
    shard_map_changed: Arc<Notify>,
    /// Per-shard notifiers for when a shard lease becomes available (holder released).
    /// Guards waiting to acquire a shard can subscribe to these instead of blind retrying.
    shard_available_notifiers: Arc<Mutex<HashMap<ShardId, Arc<Notify>>>>,
}

/// Convenience type alias for K8sCoordinator with the real Kubernetes backend.
pub type K8sCoordinatorDefault = K8sCoordinator<KubeBackend>;

impl K8sCoordinator<KubeBackend> {
    /// Start the K8S coordinator using the default Kubernetes client.
    pub async fn start(
        config: K8sCoordinatorConfig,
        factory: Arc<ShardFactory>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), CoordinationError> {
        let backend = KubeBackend::try_default().await?;
        Self::start_with_backend(backend, config, factory).await
    }
}

impl<B: K8sBackend> K8sCoordinator<B> {
    pub async fn start_with_backend(
        backend: B,
        config: K8sCoordinatorConfig,
        factory: Arc<ShardFactory>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), CoordinationError> {
        let K8sCoordinatorConfig {
            namespace,
            cluster_prefix,
            node_id,
            grpc_addr,
            initial_shard_count,
            lease_duration_secs,
            placement_rings,
        } = config;

        // Load or create the shard map in a ConfigMap
        let shard_map = Self::load_or_create_shard_map(
            &backend,
            &namespace,
            &cluster_prefix,
            initial_shard_count,
        )
        .await?;

        let base = Arc::new(CoordinatorBase::new(
            node_id.clone(),
            grpc_addr.clone(),
            shard_map,
            factory,
            placement_rings.clone(),
        ));

        // Create or update membership lease
        let member_lease_name = keys::k8s_member_lease_name(&cluster_prefix, &node_id);

        let member_info = MemberInfo {
            node_id: node_id.clone(),
            grpc_addr: grpc_addr.clone(),
            startup_time_ms: base.startup_time_ms,
            hostname: get_hostname(),
            placement_rings: placement_rings.clone(),
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

        backend
            .patch_lease_apply(&namespace, &member_lease_name, lease_spec)
            .await?;

        let members_cache = Arc::new(Mutex::new(Vec::new()));
        let membership_changed = Arc::new(Notify::new());
        let shard_map_changed = Arc::new(Notify::new());
        let shard_available_notifiers = Arc::new(Mutex::new(HashMap::new()));

        let me = Self {
            base: base.clone(),
            backend: backend.clone(),
            namespace: namespace.clone(),
            cluster_prefix: cluster_prefix.clone(),
            lease_duration_secs: lease_duration_secs as i32,
            shard_guards: Arc::new(Mutex::new(HashMap::new())),
            members_cache,
            membership_changed,
            shard_map_changed,
            shard_available_notifiers,
        };

        // Do initial member list fetch before starting background tasks
        me.refresh_members_cache().await?;

        let me_bg = me.clone();
        let nid = node_id.clone();

        let handle = tokio::spawn(async move {
            // Start the membership watcher task
            let membership_watcher_me = me_bg.clone();
            let membership_watcher_shutdown = me_bg.base.shutdown_rx.clone();
            let membership_watcher_handle = tokio::spawn(async move {
                membership_watcher_me
                    .run_membership_watcher(membership_watcher_shutdown)
                    .await;
            });

            // Start the shard map watcher task
            let shard_map_watcher_me = me_bg.clone();
            let shard_map_watcher_shutdown = me_bg.base.shutdown_rx.clone();
            let shard_map_watcher_handle = tokio::spawn(async move {
                shard_map_watcher_me
                    .run_shard_map_watcher(shard_map_watcher_shutdown)
                    .await;
            });

            // Start the shard lease watcher task - watches for when shard leases become available
            let shard_lease_watcher_me = me_bg.clone();
            let shard_lease_watcher_shutdown = me_bg.base.shutdown_rx.clone();
            let shard_lease_watcher_handle = tokio::spawn(async move {
                shard_lease_watcher_me
                    .run_shard_lease_watcher(shard_lease_watcher_shutdown)
                    .await;
            });

            // Run the main coordination loop
            me_bg
                .run_coordination_loop(me_bg.base.shutdown_rx.clone())
                .await;

            // Clean up watchers on shutdown
            membership_watcher_handle.abort();
            shard_map_watcher_handle.abort();
            shard_lease_watcher_handle.abort();
            debug!(node_id = %nid, "k8s coordinator background task exiting");
        });

        Ok((me, handle))
    }

    /// Load the shard map from a ConfigMap, or create a new one if it doesn't exist.
    ///
    /// The shard map is stored in a ConfigMap with the key "shard_map.json". This function handles the initial cluster bootstrapping where the first node to start creates the shard map.
    async fn load_or_create_shard_map(
        backend: &B,
        namespace: &str,
        cluster_prefix: &str,
        initial_shard_count: u32,
    ) -> Result<ShardMap, CoordinationError> {
        let configmap_name = format!("{}-shard-map", cluster_prefix);

        // Try to get the existing ConfigMap
        match backend.get_configmap(namespace, &configmap_name).await? {
            Some(cm) => {
                // ConfigMap exists, parse the shard map
                let data = cm
                    .data
                    .as_ref()
                    .and_then(|d| d.get("shard_map.json"))
                    .ok_or_else(|| {
                        CoordinationError::BackendError(
                            "shard map ConfigMap exists but has no data".into(),
                        )
                    })?;

                let shard_map: ShardMap = serde_json::from_str(data).map_err(|e| {
                    CoordinationError::BackendError(format!("failed to parse shard map: {}", e))
                })?;

                info!(
                    num_shards = shard_map.len(),
                    version = shard_map.version,
                    "loaded existing shard map from ConfigMap"
                );
                Ok(shard_map)
            }
            None => {
                // ConfigMap doesn't exist, create initial shard map
                let shard_map = ShardMap::create_initial(initial_shard_count).map_err(|e| {
                    CoordinationError::BackendError(format!(
                        "failed to create initial shard map: {}",
                        e
                    ))
                })?;

                let shard_map_json = serde_json::to_string(&shard_map)
                    .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

                let cm = ConfigMap {
                    metadata: kube::api::ObjectMeta {
                        name: Some(configmap_name.clone()),
                        labels: Some(
                            [
                                ("silo.dev/type".to_string(), "shard-map".to_string()),
                                ("silo.dev/cluster".to_string(), cluster_prefix.to_string()),
                            ]
                            .into(),
                        ),
                        ..Default::default()
                    },
                    data: Some([("shard_map.json".to_string(), shard_map_json)].into()),
                    ..Default::default()
                };

                // Use create with optimistic concurrency - if another node creates it first, we'll get a conflict and retry
                match backend.create_configmap(namespace, &cm).await {
                    Ok(_) => {
                        info!(
                            num_shards = shard_map.len(),
                            "created new shard map ConfigMap"
                        );
                        Ok(shard_map)
                    }
                    Err(CoordinationError::BackendError(msg))
                        if msg.contains("conflict") || msg.contains("already exists") =>
                    {
                        // Another node created it first - load it
                        debug!("shard map ConfigMap already created by another node, loading");
                        let cm = backend
                            .get_configmap(namespace, &configmap_name)
                            .await?
                            .ok_or_else(|| {
                                CoordinationError::BackendError(
                                    "shard map ConfigMap disappeared".into(),
                                )
                            })?;

                        let data = cm
                            .data
                            .as_ref()
                            .and_then(|d| d.get("shard_map.json"))
                            .ok_or_else(|| {
                                CoordinationError::BackendError(
                                    "shard map ConfigMap exists but has no data".into(),
                                )
                            })?;

                        let shard_map: ShardMap = serde_json::from_str(data).map_err(|e| {
                            CoordinationError::BackendError(format!(
                                "failed to parse shard map: {}",
                                e
                            ))
                        })?;
                        Ok(shard_map)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Run the membership watcher - watches for lease changes and updates the members cache
    async fn run_membership_watcher(&self, mut shutdown_rx: watch::Receiver<bool>) {
        let label_selector = format!(
            "silo.dev/type=member,silo.dev/cluster={}",
            self.cluster_prefix
        );
        let mut watch_stream =
            std::pin::pin!(self.backend.watch_leases(&self.namespace, &label_selector));

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
                                LeaseWatchEvent::Applied(lease) => {
                                    let lease_name = lease.metadata.name.as_deref().unwrap_or("unknown");
                                    debug!(node_id = %self.base.node_id, lease = %lease_name, "membership lease changed");
                                    // Refresh the full member list from cache
                                    if let Err(e) = self.refresh_members_cache().await {
                                        warn!(node_id = %self.base.node_id, error = %e, "failed to refresh members cache");
                                    }
                                    self.membership_changed.notify_waiters();
                                }
                                LeaseWatchEvent::Deleted(lease) => {
                                    let lease_name = lease.metadata.name.as_deref().unwrap_or("unknown");
                                    debug!(node_id = %self.base.node_id, lease = %lease_name, "membership lease deleted");
                                    if let Err(e) = self.refresh_members_cache().await {
                                        warn!(node_id = %self.base.node_id, error = %e, "failed to refresh members cache");
                                    }
                                    self.membership_changed.notify_waiters();
                                }
                                LeaseWatchEvent::Init => {
                                    debug!(node_id = %self.base.node_id, "membership watcher initialized");
                                }
                                LeaseWatchEvent::InitDone => {
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
                            // Watchers should automatically reconnect on errors
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

    /// Run the shard map watcher - watches for shard map ConfigMap changes and triggers reconciliation
    async fn run_shard_map_watcher(&self, mut shutdown_rx: watch::Receiver<bool>) {
        let configmap_name = format!("{}-shard-map", self.cluster_prefix);
        let mut watch_stream = std::pin::pin!(
            self.backend
                .watch_configmap(&self.namespace, &configmap_name)
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!(node_id = %self.base.node_id, "shard map watcher shutting down");
                        break;
                    }
                }
                event = watch_stream.next() => {
                    match event {
                        Some(Ok(ev)) => {
                            match ev {
                                ConfigMapWatchEvent::Applied(cm) => {
                                    let cm_name = cm.metadata.name.as_deref().unwrap_or("unknown");
                                    info!(node_id = %self.base.node_id, configmap = %cm_name, "shard map ConfigMap changed");
                                    // Reload the shard map from the ConfigMap into local cache
                                    if let Err(e) = self.reload_shard_map().await {
                                        warn!(node_id = %self.base.node_id, error = %e, "failed to reload shard map");
                                    }
                                    self.shard_map_changed.notify_waiters();
                                }
                                ConfigMapWatchEvent::Deleted(cm) => {
                                    let cm_name = cm.metadata.name.as_deref().unwrap_or("unknown");
                                    warn!(node_id = %self.base.node_id, configmap = %cm_name, "shard map ConfigMap deleted");
                                    // Don't notify on delete - this shouldn't happen normally
                                }
                                ConfigMapWatchEvent::Init => {
                                    debug!(node_id = %self.base.node_id, "shard map watcher initialized");
                                }
                                ConfigMapWatchEvent::InitDone => {
                                    debug!(node_id = %self.base.node_id, "shard map watcher init done");
                                    // Notify on init done in case we missed events
                                    self.shard_map_changed.notify_waiters();
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(node_id = %self.base.node_id, error = %e, "shard map watcher error, will retry");
                            // Watchers should automatically reconnect on errors
                        }
                        None => {
                            debug!(node_id = %self.base.node_id, "shard map watcher stream ended");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Get or create a notifier for when a specific shard lease becomes available.
    /// Guards waiting to acquire a shard can await on this notifier instead of blind retrying.
    pub async fn get_shard_available_notifier(&self, shard_id: ShardId) -> Arc<Notify> {
        let mut notifiers = self.shard_available_notifiers.lock().await;
        notifiers
            .entry(shard_id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Run the shard lease watcher - watches for when shard leases become available.
    /// When a lease is released (holderIdentity cleared) or deleted, notify any guards
    /// waiting to acquire that shard.
    async fn run_shard_lease_watcher(&self, mut shutdown_rx: watch::Receiver<bool>) {
        let label_selector = format!(
            "silo.dev/type=shard,silo.dev/cluster={}",
            self.cluster_prefix
        );
        let mut watch_stream =
            std::pin::pin!(self.backend.watch_leases(&self.namespace, &label_selector));

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!(node_id = %self.base.node_id, "shard lease watcher shutting down");
                        break;
                    }
                }
                event = watch_stream.next() => {
                    match event {
                        Some(Ok(ev)) => {
                            match ev {
                                LeaseWatchEvent::Applied(lease) => {
                                    // Check if the lease holder was cleared (released)
                                    let holder = lease.spec.as_ref()
                                        .and_then(|s| s.holder_identity.as_ref())
                                        .map(|h| h.as_str());
                                    let holder_is_empty = holder.map(|h| h.is_empty()).unwrap_or(true);

                                    if holder_is_empty {
                                        // Lease was released - notify any waiting guards
                                        if let Some(shard_id_str) = lease.metadata.labels.as_ref()
                                            .and_then(|l| l.get("silo.dev/shard"))
                                            && let Ok(shard_id) = ShardId::parse(shard_id_str) {
                                                debug!(
                                                    node_id = %self.base.node_id,
                                                    shard_id = %shard_id,
                                                    "shard lease released, notifying waiters"
                                                );
                                                let notifiers = self.shard_available_notifiers.lock().await;
                                                if let Some(notifier) = notifiers.get(&shard_id) {
                                                    notifier.notify_waiters();
                                                }
                                            }
                                    }
                                }
                                LeaseWatchEvent::Deleted(lease) => {
                                    // Lease was deleted - also makes the shard available
                                    if let Some(shard_id_str) = lease.metadata.labels.as_ref()
                                        .and_then(|l| l.get("silo.dev/shard"))
                                        && let Ok(shard_id) = ShardId::parse(shard_id_str) {
                                            debug!(
                                                node_id = %self.base.node_id,
                                                shard_id = %shard_id,
                                                "shard lease deleted, notifying waiters"
                                            );
                                            let notifiers = self.shard_available_notifiers.lock().await;
                                            if let Some(notifier) = notifiers.get(&shard_id) {
                                                notifier.notify_waiters();
                                            }
                                        }
                                }
                                LeaseWatchEvent::Init => {
                                    debug!(node_id = %self.base.node_id, "shard lease watcher initialized");
                                }
                                LeaseWatchEvent::InitDone => {
                                    debug!(node_id = %self.base.node_id, "shard lease watcher init done");
                                    // After initial sync completes, notify all waiting guards to retry.
                                    // This handles the race where guards started waiting before the watcher
                                    // finished syncing and saw available leases. Without this, guards would
                                    // have to wait for the 500ms timeout before retrying.
                                    let notifiers = self.shard_available_notifiers.lock().await;
                                    for notifier in notifiers.values() {
                                        notifier.notify_waiters();
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(node_id = %self.base.node_id, error = %e, "shard lease watcher error, will retry");
                        }
                        None => {
                            debug!(node_id = %self.base.node_id, "shard lease watcher stream ended");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Main coordination loop - renews membership and reconciles shards.
    ///
    /// Reconciliation is primarily reactive (triggered by the membership watcher), but we also do periodic reconciliation as a safety net in case watches miss events due to network issues.
    async fn run_coordination_loop(&self, mut shutdown_rx: watch::Receiver<bool>) {
        // Renew membership lease periodically (at 1/3 of lease duration)
        let renew_interval = Duration::from_secs((self.lease_duration_secs / 3).max(1) as u64);
        let mut renew_timer = tokio::time::interval(renew_interval);

        // Periodic reconciliation as a safety net to catch any missed watch events due to network issues
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
                // React to shard map changes from the watcher (for split handling)
                _ = self.shard_map_changed.notified() => {
                    info!(node_id = %self.base.node_id, "shard map changed, reconciling shards");
                    if let Err(e) = self.reconcile_shards().await {
                        warn!(node_id = %self.base.node_id, error = %e, "reconcile after shard map change failed");
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
        let label_selector = format!(
            "silo.dev/type=member,silo.dev/cluster={}",
            self.cluster_prefix
        );

        let lease_list = self
            .backend
            .list_leases(&self.namespace, &label_selector)
            .await?;

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
            if let Some(metadata) = &lease.metadata.annotations
                && let Some(info_json) = metadata.get("silo.dev/member-info")
                && let Ok(info) = serde_json::from_str::<MemberInfo>(info_json)
            {
                members.push(info);
            }
        }

        members.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(members)
    }

    /// Renew the membership lease.
    ///
    /// We verify that we're still the holder before updating, and use CAS to ensure we don't accidentally renew someone else's lease if ours expired.
    async fn renew_membership_lease(&self) -> Result<(), CoordinationError> {
        let member_lease_name =
            keys::k8s_member_lease_name(&self.cluster_prefix, &self.base.node_id);

        // Get current lease to verify we still own it
        let existing = self
            .backend
            .get_lease(&self.namespace, &member_lease_name)
            .await?
            .ok_or_else(|| {
                CoordinationError::BackendError("membership lease was deleted".into())
            })?;

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
            startup_time_ms: self.base.startup_time_ms,
            hostname: get_hostname(),
            placement_rings: self.base.placement_rings.clone(),
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

        self.backend
            .replace_lease(&self.namespace, &member_lease_name, &updated_lease)
            .await?;
        Ok(())
    }

    async fn reconcile_shards(&self) -> Result<(), CoordinationError> {
        let members = {
            let cache = self.members_cache.lock().await;
            cache.clone()
        };
        let member_ids: Vec<String> = members.iter().map(|m| m.node_id.clone()).collect();

        // Safety check: if we don't see ourselves in the member list, our membership lease may have expired or there's a connectivity issue. Don't reconcile based on incomplete membership data - it would cause us to release all shards.
        if members.is_empty() {
            warn!(node_id = %self.base.node_id, "reconcile: no members found, skipping reconcile");
            return Ok(());
        }
        if !member_ids.contains(&self.base.node_id) {
            warn!(node_id = %self.base.node_id, members = ?member_ids, "reconcile: our node not in member list, skipping reconcile (membership may have expired)");
            return Ok(());
        }

        let shard_map = self.base.shard_map.lock().await;
        let shards: Vec<&crate::shard_range::ShardInfo> = shard_map.shards().iter().collect();
        let desired = compute_desired_shards_for_node(&shards, &self.base.node_id, &members);
        drop(shard_map);
        debug!(node_id = %self.base.node_id, members = ?member_ids, desired = ?desired, "reconcile: begin");

        // Release undesired shards (sorted for deterministic ordering)
        {
            let mut to_release: Vec<ShardId> = {
                let guards = self.shard_guards.lock().await;
                let mut v = Vec::new();
                for (sid, guard) in guards.iter() {
                    if !desired.contains(sid) && guard.is_held().await {
                        v.push(*sid);
                    }
                }
                v
            };
            to_release.sort_unstable();
            for sid in to_release {
                self.ensure_shard_guard(sid).await.set_desired(false).await;
            }
        }

        // Acquire desired shards (sorted for deterministic ordering)
        {
            let mut snapshot: Vec<ShardId> = desired.iter().copied().collect();
            snapshot.sort_unstable();
            for shard_id in snapshot {
                self.ensure_shard_guard(shard_id)
                    .await
                    .set_desired(true)
                    .await;
            }
        }

        Ok(())
    }

    async fn ensure_shard_guard(&self, shard_id: ShardId) -> Arc<K8sShardGuard<B>> {
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
        debug!(shard_id = %shard_id, "creating new shard guard");
        // Get the shard_available notifier for this shard (used for fast handoffs)
        let shard_available = self.get_shard_available_notifier(shard_id).await;
        let guard = K8sShardGuard::new(
            shard_id,
            self.backend.clone(),
            self.namespace.clone(),
            self.cluster_prefix.clone(),
            self.base.node_id.clone(),
            self.lease_duration_secs,
            self.base.shutdown_rx.clone(),
            shard_available,
        );
        let runner = guard.clone();
        let owned_arc = self.base.owned.clone();
        let factory = self.base.factory.clone();
        let shard_map = self.base.shard_map.clone();
        let coordinator: Arc<dyn Coordinator> = Arc::new(self.clone());
        tokio::spawn(async move { runner.run(owned_arc, factory, shard_map, coordinator).await });
        guards.insert(shard_id, guard.clone());
        guard
    }

    /// Reload the shard map from ConfigMap into local cache.
    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        let configmap_name = format!("{}-shard-map", self.cluster_prefix);

        let cm = self
            .backend
            .get_configmap(&self.namespace, &configmap_name)
            .await?
            .ok_or_else(|| {
                CoordinationError::BackendError("shard map ConfigMap not found".into())
            })?;

        let data = cm
            .data
            .as_ref()
            .and_then(|d| d.get("shard_map.json"))
            .ok_or_else(|| {
                CoordinationError::BackendError("shard map ConfigMap has no data".into())
            })?;

        let shard_map: ShardMap = serde_json::from_str(data)
            .map_err(|e| CoordinationError::BackendError(format!("invalid shard map: {}", e)))?;

        *self.base.shard_map.lock().await = shard_map;
        Ok(())
    }

    /// Modify the shard map ConfigMap with CAS (Compare-And-Swap) semantics.
    ///
    /// This is a generic helper for all shard map modifications. It:
    /// 1. Reads the current shard map from the ConfigMap
    /// 2. Calls the provided `modify` closure to transform the shard map
    /// 3. Writes the updated shard map back with CAS using resourceVersion
    /// 4. Updates the local cache on success
    ///
    /// Returns the result from the modify closure on success.
    async fn modify_shard_map_cas<T, F>(&self, modify: F) -> Result<T, CoordinationError>
    where
        F: FnOnce(&mut ShardMap) -> Result<T, CoordinationError>,
    {
        let configmap_name = format!("{}-shard-map", self.cluster_prefix);

        // Read current shard map
        let existing = self
            .backend
            .get_configmap(&self.namespace, &configmap_name)
            .await?
            .ok_or_else(|| {
                CoordinationError::BackendError("shard map ConfigMap not found".into())
            })?;

        let current_rv = existing.metadata.resource_version.as_ref().ok_or_else(|| {
            CoordinationError::BackendError("no resourceVersion on shard map".into())
        })?;

        let data = existing
            .data
            .as_ref()
            .and_then(|d| d.get("shard_map.json"))
            .ok_or_else(|| {
                CoordinationError::BackendError("shard map ConfigMap has no data".into())
            })?;

        let mut shard_map: ShardMap = serde_json::from_str(data)
            .map_err(|e| CoordinationError::BackendError(format!("invalid shard map: {}", e)))?;

        // Apply the modification
        let result = modify(&mut shard_map)?;

        let new_map_json = serde_json::to_string(&shard_map)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        // Use replace with resourceVersion for CAS
        let updated_cm = ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some(configmap_name.clone()),
                namespace: Some(self.namespace.clone()),
                resource_version: Some(current_rv.clone()),
                uid: existing.metadata.uid.clone(),
                labels: existing.metadata.labels.clone(),
                ..Default::default()
            },
            data: Some([("shard_map.json".to_string(), new_map_json)].into()),
            ..Default::default()
        };

        match self
            .backend
            .replace_configmap(&self.namespace, &configmap_name, &updated_cm)
            .await
        {
            Ok(_) => {
                // Update local shard map cache
                *self.base.shard_map.lock().await = shard_map;
                Ok(result)
            }
            Err(e) => {
                // Check if this is a CAS conflict (409 Conflict)
                let err_str = e.to_string();
                if err_str.contains("409") || err_str.contains("Conflict") {
                    Err(CoordinationError::BackendError(
                        "CAS conflict: resourceVersion mismatch".into(),
                    ))
                } else {
                    Err(CoordinationError::BackendError(err_str))
                }
            }
        }
    }

    /// Modify the shard map with automatic retry on CAS conflicts.
    ///
    /// Wraps `modify_shard_map_cas` with exponential backoff retry logic.
    async fn modify_shard_map_with_retry<T, F>(
        &self,
        operation_name: &str,
        mut modify: F,
    ) -> Result<T, CoordinationError>
    where
        F: FnMut(&mut ShardMap) -> Result<T, CoordinationError>,
    {
        let max_retries = 10;
        let mut delay = Duration::from_millis(10);

        for attempt in 0..max_retries {
            match self.modify_shard_map_cas(&mut modify).await {
                Ok(result) => return Ok(result),
                Err(CoordinationError::BackendError(msg))
                    if msg.contains("CAS conflict: resourceVersion mismatch") =>
                {
                    if attempt < max_retries - 1 {
                        debug!(
                            operation = operation_name,
                            attempt = attempt,
                            "shard map update hit CAS conflict, retrying"
                        );
                        tokio::time::sleep(delay).await;
                        delay = std::cmp::min(delay * 2, Duration::from_secs(1));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(CoordinationError::BackendError(format!(
            "failed to {} after {} retries",
            operation_name, max_retries
        )))
    }

    /// Update the shard map ConfigMap after a split with retry logic.
    async fn update_shard_map_for_split(
        &self,
        split: &SplitInProgress,
    ) -> Result<(), CoordinationError> {
        let parent_id = split.parent_shard_id;
        let left_id = split.left_child_id;
        let right_id = split.right_child_id;
        let split_point = split.split_point.clone();

        self.modify_shard_map_with_retry("update shard map for split", |shard_map| {
            shard_map.split_shard(&parent_id, &split_point, left_id, right_id)?;
            Ok(())
        })
        .await?;

        info!(
            parent_shard_id = %split.parent_shard_id,
            left_child_id = %split.left_child_id,
            right_child_id = %split.right_child_id,
            split_point = %split.split_point,
            "shard map updated for split (k8s)"
        );

        Ok(())
    }
}

#[async_trait]
impl<B: K8sBackend> SplitStorageBackend for K8sCoordinator<B> {
    async fn load_split(
        &self,
        parent_shard_id: &ShardId,
    ) -> Result<Option<SplitInProgress>, CoordinationError> {
        let configmap_name = keys::k8s_split_configmap_name(&self.cluster_prefix, parent_shard_id);

        match self
            .backend
            .get_configmap(&self.namespace, &configmap_name)
            .await?
        {
            Some(cm) => {
                let data = cm
                    .data
                    .as_ref()
                    .and_then(|d| d.get("split.json"))
                    .ok_or_else(|| {
                        CoordinationError::BackendError(
                            "split ConfigMap exists but has no data".into(),
                        )
                    })?;

                let split: SplitInProgress = serde_json::from_str(data).map_err(|e| {
                    CoordinationError::BackendError(format!("invalid split JSON: {}", e))
                })?;

                Ok(Some(split))
            }
            None => Ok(None),
        }
    }

    async fn store_split(&self, split: &SplitInProgress) -> Result<(), CoordinationError> {
        let configmap_name =
            keys::k8s_split_configmap_name(&self.cluster_prefix, &split.parent_shard_id);

        let split_json = serde_json::to_string(split)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let cm = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": configmap_name,
                "labels": {
                    "silo.dev/type": "split",
                    "silo.dev/cluster": self.cluster_prefix,
                    "silo.dev/parent-shard": split.parent_shard_id.to_string()
                }
            },
            "data": {
                "split.json": split_json
            }
        });

        self.backend
            .patch_configmap_apply(&self.namespace, &configmap_name, cm)
            .await?;

        Ok(())
    }

    async fn delete_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
        let configmap_name = keys::k8s_split_configmap_name(&self.cluster_prefix, parent_shard_id);
        self.backend
            .delete_configmap(&self.namespace, &configmap_name)
            .await
    }

    async fn update_shard_map_for_split(
        &self,
        split: &SplitInProgress,
    ) -> Result<(), CoordinationError> {
        K8sCoordinator::update_shard_map_for_split(self, split).await
    }

    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        K8sCoordinator::reload_shard_map(self).await
    }

    async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
        let label_selector = format!(
            "silo.dev/type=split,silo.dev/cluster={}",
            self.cluster_prefix
        );

        let cm_list = self
            .backend
            .list_configmaps(&self.namespace, &label_selector)
            .await?;

        let mut splits = Vec::new();
        for cm in cm_list {
            let data = match cm.data.as_ref().and_then(|d| d.get("split.json")) {
                Some(d) => d,
                None => continue,
            };

            if let Ok(split) = serde_json::from_str::<SplitInProgress>(data) {
                splits.push(split);
            }
        }
        Ok(splits)
    }
}

#[async_trait]
impl<B: K8sBackend> Coordinator for K8sCoordinator<B> {
    fn base(&self) -> &CoordinatorBase {
        &self.base
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        // Collect guards in sorted order for deterministic shutdown trigger order
        let guards_sorted: Vec<(ShardId, Arc<K8sShardGuard<B>>)> = {
            let guards = self.shard_guards.lock().await;
            let mut shard_ids: Vec<ShardId> = guards.keys().copied().collect();
            shard_ids.sort_unstable();
            shard_ids
                .into_iter()
                .filter_map(|sid| guards.get(&sid).map(|g| (sid, g.clone())))
                .collect()
        };

        // Trigger shutdown on all guards first (in sorted order for determinism),
        // then wait for all of them in parallel. This is much faster than sequential
        // shutdown because shard close + lease release can happen concurrently.
        for (_sid, guard) in &guards_sorted {
            guard.trigger_shutdown().await;
            guard.notify.notify_one();
        }

        // Wait for all guards to complete shutdown in parallel
        let wait_futures: Vec<_> = guards_sorted
            .iter()
            .map(|(_sid, guard)| guard.wait_shutdown(Duration::from_millis(5000)))
            .collect();
        futures::future::join_all(wait_futures).await;

        // Signal shutdown to stop background tasks
        self.base.signal_shutdown();

        // Delete membership lease
        let member_lease_name =
            keys::k8s_member_lease_name(&self.cluster_prefix, &self.base.node_id);
        let _ = self
            .backend
            .delete_lease(&self.namespace, &member_lease_name)
            .await;

        Ok(())
    }

    async fn wait_converged(&self, timeout: Duration) -> bool {
        // Use tokio::time::Instant for compatibility with turmoil's simulated time in DST
        let start = tokio::time::Instant::now();
        while start.elapsed() < timeout {
            // Refresh the members cache to get current membership. This is important because the watcher runs asynchronously and might not have processed membership changes yet (e.g., after a node shutdown).
            if let Err(e) = self.refresh_members_cache().await {
                debug!(node_id = %self.base.node_id, error = %e, "wait_converged: failed to refresh members cache");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Notify the coordination loop to reconcile with the refreshed membership.
            // This ensures reconciliation runs even if the watcher hasn't fired an event.
            self.membership_changed.notify_one();

            // Get full member info for ring-aware convergence check
            let members = {
                let cache = self.members_cache.lock().await;
                cache.clone()
            };
            let member_ids: Vec<String> = members.iter().map(|m| m.node_id.clone()).collect();
            if members.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let shard_map = self.base.shard_map.lock().await;
            let shards: Vec<&crate::shard_range::ShardInfo> = shard_map.shards().iter().collect();
            let desired: HashSet<ShardId> =
                compute_desired_shards_for_node(&shards, &self.base.node_id, &members);
            drop(shard_map);
            let owned_now: HashSet<ShardId> = {
                let guard = self.base.owned.lock().await;
                if *guard == desired {
                    return true;
                }
                guard.clone()
            };

            // Log the diff periodically for debugging (only at debug level to avoid spam)
            let missing: Vec<ShardId> = desired.difference(&owned_now).copied().collect();
            let extra: Vec<ShardId> = owned_now.difference(&desired).copied().collect();
            if !missing.is_empty() || !extra.is_empty() {
                debug!(
                    node_id = %self.base.node_id,
                    members = ?member_ids,
                    missing = ?missing,
                    extra = ?extra,
                    "wait_converged: not yet converged"
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log diff on timeout
        let members = {
            let cache = self.members_cache.lock().await;
            cache.clone()
        };
        let member_ids: Vec<String> = members.iter().map(|m| m.node_id.clone()).collect();
        let shard_map = self.base.shard_map.lock().await;
        let shards: Vec<&crate::shard_range::ShardInfo> = shard_map.shards().iter().collect();
        let desired: HashSet<ShardId> =
            compute_desired_shards_for_node(&shards, &self.base.node_id, &members);
        drop(shard_map);
        let owned_now: HashSet<ShardId> = {
            let g = self.base.owned.lock().await;
            g.clone()
        };
        let missing: Vec<ShardId> = desired.difference(&owned_now).copied().collect();
        let extra: Vec<ShardId> = owned_now.difference(&desired).copied().collect();
        warn!(
            node_id = %self.base.node_id,
            members = ?member_ids,
            missing = ?missing,
            extra = ?extra,
            "wait_converged: timed out"
        );
        false
    }

    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        let cache = self.members_cache.lock().await;
        Ok(cache.clone())
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
        let members = self.get_members().await?;
        Ok(self.base.compute_shard_owner_map(&members).await)
    }

    async fn update_shard_placement_ring(
        &self,
        shard_id: &crate::shard_range::ShardId,
        ring: Option<&str>,
    ) -> Result<(Option<String>, Option<String>), CoordinationError> {
        let sid = *shard_id;
        let new_ring = ring.map(|s| s.to_string());

        self.modify_shard_map_with_retry("update shard placement ring", move |shard_map| {
            let shard = shard_map
                .get_shard_mut(&sid)
                .ok_or(CoordinationError::ShardNotFound(sid))?;

            let previous = shard.placement_ring.clone();
            shard.placement_ring = new_ring.clone();
            Ok((previous, new_ring.clone()))
        })
        .await
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
pub struct K8sShardGuard<B: K8sBackend> {
    pub shard_id: ShardId,
    pub backend: B,
    pub namespace: String,
    pub cluster_prefix: String,
    pub node_id: String,
    pub lease_duration_secs: i32,
    pub state: Mutex<ShardState>,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
    /// Notifier that fires when this shard's lease becomes available (released by another node).
    /// Used to avoid blind retry with exponential backoff during shard handoffs.
    pub shard_available: Arc<Notify>,
}

impl<B: K8sBackend> K8sShardGuard<B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: ShardId,
        backend: B,
        namespace: String,
        cluster_prefix: String,
        node_id: String,
        lease_duration_secs: i32,
        shutdown: watch::Receiver<bool>,
        shard_available: Arc<Notify>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            backend,
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
            shard_available,
        })
    }

    fn lease_name(&self) -> String {
        keys::k8s_shard_lease_name(&self.cluster_prefix, &self.shard_id)
    }

    pub async fn is_held(&self) -> bool {
        let st = self.state.lock().await;
        st.phase == ShardPhase::Held && st.resource_version.is_some()
    }

    pub async fn set_desired(&self, desired: bool) {
        let mut st = self.state.lock().await;
        if matches!(st.phase, ShardPhase::ShutDown | ShardPhase::ShuttingDown) {
            debug!(
                shard_id = %self.shard_id,
                desired, "set_desired: guard is shutting down, ignoring"
            );
            return;
        }
        if st.desired != desired {
            debug!(shard_id = %self.shard_id, desired, phase = ?st.phase, "set_desired: changing desired state");
            st.desired = desired;
            self.notify.notify_one();
        }
    }

    /// Trigger shutdown for this specific guard (sets phase to ShuttingDown).
    pub async fn trigger_shutdown(&self) {
        let mut st = self.state.lock().await;
        if !matches!(st.phase, ShardPhase::ShutDown | ShardPhase::ShuttingDown) {
            debug!(shard_id = %self.shard_id, "trigger_shutdown: transitioning to ShuttingDown");
            st.phase = ShardPhase::ShuttingDown;
        }
    }

    /// Wait for this guard to complete shutdown (phase becomes ShutDown).
    /// Returns immediately if already shut down.
    pub async fn wait_shutdown(&self, timeout: Duration) {
        let poll_interval = Duration::from_millis(10);
        let max_iterations = (timeout.as_millis() / poll_interval.as_millis()).max(1) as usize;

        for _ in 0..max_iterations {
            {
                let st = self.state.lock().await;
                if st.phase == ShardPhase::ShutDown {
                    return;
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
        debug!(shard_id = %self.shard_id, "wait_shutdown: timed out");
    }

    pub async fn run(
        self: Arc<Self>,
        owned_arc: Arc<Mutex<HashSet<ShardId>>>,
        factory: Arc<ShardFactory>,
        shard_map: Arc<Mutex<ShardMap>>,
        _coordinator: Arc<dyn Coordinator>,
    ) {
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
                    debug!(shard_id = %self.shard_id, "k8s shard: starting acquisition");
                    let mut attempt: u32 = 0;
                    let initial_jitter_ms =
                        (self.shard_id.as_uuid().as_u64_pair().0.wrapping_mul(13)) % 80;
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
                        match self.try_acquire_lease_cas(&lease_name).await {
                            Ok((rv, uid)) => {
                                // Look up the shard's range from the shard map
                                let range = {
                                    let map = shard_map.lock().await;
                                    match map.get_shard(&self.shard_id) {
                                        Some(info) => info.range.clone(),
                                        None => {
                                            tracing::error!(shard_id = %self.shard_id, "shard not found in shard map");
                                            let _ = self.release_lease_cas(&lease_name).await;
                                            continue;
                                        }
                                    }
                                };
                                // Open the shard BEFORE marking as Held - if open fails,
                                // we should release the lease and not claim ownership.
                                let _shard = match factory.open(&self.shard_id, &range).await {
                                    Ok(shard) => shard,
                                    Err(e) => {
                                        // Failed to open - release the lease and retry
                                        tracing::error!(shard_id = %self.shard_id, error = %e, "failed to open shard, releasing lease");
                                        let _ = self.release_lease_cas(&lease_name).await;
                                        // Exponential backoff before retry
                                        let backoff_ms = 200 * (1 << attempt.min(5));
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                        attempt = attempt.wrapping_add(1);
                                        continue;
                                    }
                                };

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
                                dst_events::emit(DstEvent::ShardAcquired {
                                    node_id: self.node_id.clone(),
                                    shard_id: self.shard_id.to_string(),
                                });
                                info!(shard_id = %self.shard_id, rv = %rv, attempts = attempt, "k8s shard: acquired and opened");

                                // Start renewal loop
                                self.clone()
                                    .run_renewal_loop(
                                        owned_arc.clone(),
                                        factory.clone(),
                                        &lease_name,
                                    )
                                    .await;
                                break;
                            }
                            Err(e) => {
                                debug!(shard_id = %self.shard_id, attempt, error = %e, "failed to acquire shard lease, waiting for release");
                                attempt = attempt.wrapping_add(1);

                                // Wait for the shard_available notification (from the lease watcher)
                                // with a timeout. This is MUCH faster than exponential backoff because
                                // we get notified immediately when the other node releases the lease.
                                // The timeout is a safety net in case the watch misses an event.
                                let wait_timeout = Duration::from_millis(500);
                                let _ = tokio::time::timeout(
                                    wait_timeout,
                                    self.shard_available.notified(),
                                )
                                .await;

                                // Small jitter after notification to avoid thundering herd
                                // when multiple nodes are waiting for the same shard
                                let jitter_ms = (self
                                    .shard_id
                                    .as_uuid()
                                    .as_u64_pair()
                                    .0
                                    .wrapping_mul(31)
                                    .wrapping_add(attempt as u64 * 17))
                                    % 20;
                                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                            }
                        }
                    }
                }
                ShardPhase::Releasing => {
                    // Small jitter based on shard ID for DST determinism
                    // This ensures releases happen in a consistent order when multiple
                    // guards are released simultaneously
                    let jitter_ms = (self.shard_id.as_uuid().as_u64_pair().0.wrapping_mul(17)) % 20;
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

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
                        if let Err(e) = factory.close(&self.shard_id).await {
                            tracing::error!(shard_id = %self.shard_id, error = %e, "failed to close shard before releasing lease");
                        }

                        // Release the lease by clearing holderIdentity with CAS
                        let has_rv = {
                            let st = self.state.lock().await;
                            st.resource_version.is_some()
                        };

                        if has_rv {
                            match self.release_lease_cas(&lease_name).await {
                                Ok(_) => {
                                    debug!(
                                        shard_id = %self.shard_id,
                                        "k8s shard: released with CAS"
                                    );
                                }
                                Err(e) => {
                                    // This is okay - we may have already lost the lease
                                    debug!(shard_id = %self.shard_id, error = %e, "k8s shard: release CAS failed (may have lost lease)");
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
                        dst_events::emit(DstEvent::ShardReleased {
                            node_id: self.node_id.clone(),
                            shard_id: self.shard_id.to_string(),
                        });
                        debug!(shard_id = %self.shard_id, "k8s shard: released");
                    }
                }
                ShardPhase::ShuttingDown => {
                    // Close the shard before releasing the lease
                    if let Err(e) = factory.close(&self.shard_id).await {
                        tracing::error!(shard_id = %self.shard_id, error = %e, "failed to close shard during shutdown");
                    }

                    // Release our lease if we hold it
                    let has_rv = {
                        let st = self.state.lock().await;
                        st.resource_version.is_some()
                    };

                    if has_rv {
                        let _ = self.release_lease_cas(&lease_name).await;
                    }

                    {
                        let mut st = self.state.lock().await;
                        st.resource_version = None;
                        st.lease_uid = None;
                        st.phase = ShardPhase::ShutDown;
                    }
                    let mut owned = owned_arc.lock().await;
                    owned.remove(&self.shard_id);
                    dst_events::emit(DstEvent::ShardReleased {
                        node_id: self.node_id.clone(),
                        shard_id: self.shard_id.to_string(),
                    });
                    break;
                }
                ShardPhase::Idle | ShardPhase::Held => {
                    debug!(shard_id = %self.shard_id, phase = ?phase, "k8s shard: waiting for state change");
                    let mut shutdown_rx = self.shutdown.clone();
                    tokio::select! {
                        _ = self.notify.notified() => {
                            debug!(shard_id = %self.shard_id, "k8s shard: received notification");
                        }
                        _ = shutdown_rx.changed() => {
                            debug!(shard_id = %self.shard_id, "k8s shard: received shutdown");
                        }
                    }
                }
            }
        }
    }

    /// Try to acquire the lease using compare-and-swap semantics.
    async fn try_acquire_lease_cas(
        &self,
        lease_name: &str,
    ) -> Result<(String, Option<String>), CoordinationError> {
        let now = Utc::now();
        debug!(
            shard_id = %self.shard_id,
            lease_name, "try_acquire_lease_cas: checking lease"
        );

        // First, try to get existing lease
        match self.backend.get_lease(&self.namespace, lease_name).await? {
            Some(existing) => {
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
                    let result = self
                        .backend
                        .replace_lease(&self.namespace, lease_name, &updated_lease)
                        .await?;
                    let new_rv = result.metadata.resource_version.ok_or_else(|| {
                        CoordinationError::BackendError("no resource_version in response".into())
                    })?;
                    debug!(
                        shard_id = %self.shard_id,
                        old_rv = %rv,
                        new_rv = %new_rv,
                        "try_acquire_lease_cas: CAS succeeded"
                    );
                    return Ok((new_rv, result.metadata.uid));
                }

                // Lease is held by someone else and not expired
                debug!(
                    shard_id = %self.shard_id,
                    holder = ?holder,
                    is_expired,
                    "try_acquire_lease_cas: lease held by another node"
                );
                Err(CoordinationError::BackendError(
                    "lease held by another node".into(),
                ))
            }
            None => {
                // Lease doesn't exist, create it
                debug!(
                    shard_id = %self.shard_id,
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

                let result = self
                    .backend
                    .create_lease(&self.namespace, &lease_spec)
                    .await?;
                let rv = result
                    .metadata
                    .resource_version
                    .ok_or_else(|| CoordinationError::BackendError("no resource_version".into()))?;
                debug!(
                    shard_id = %self.shard_id,
                    lease_name,
                    rv = %rv,
                    "try_acquire_lease_cas: created successfully"
                );
                Ok((rv, result.metadata.uid))
            }
        }
    }

    /// Release the lease using CAS - clears holderIdentity instead of deleting.
    /// We always verify we're still the holder before releasing to avoid clearing someone else's lease.
    async fn release_lease_cas(&self, lease_name: &str) -> Result<String, CoordinationError> {
        let now = Utc::now();

        // Get current lease to verify we still own it
        let existing = self
            .backend
            .get_lease(&self.namespace, lease_name)
            .await?
            .ok_or_else(|| CoordinationError::BackendError("lease not found".into()))?;

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

        let result = self
            .backend
            .replace_lease(&self.namespace, lease_name, &updated_lease)
            .await?;

        let new_rv = result
            .metadata
            .resource_version
            .ok_or_else(|| CoordinationError::BackendError("no resource_version".into()))?;

        debug!(
            shard_id = %self.shard_id,
            old_rv = %current_rv,
            new_rv = %new_rv,
            "release_lease_cas: cleared holder"
        );
        Ok(new_rv)
    }

    async fn run_renewal_loop(
        self: Arc<Self>,
        owned_arc: Arc<Mutex<HashSet<ShardId>>>,
        factory: Arc<ShardFactory>,
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
                    shard_id = %self.shard_id,
                    "renewal loop: no resourceVersion, exiting"
                );
                break;
            }

            // Renew with CAS to detect if we lost the lease
            match self.renew_lease_cas(lease_name).await {
                Ok(new_rv) => {
                    // Update our fencing token
                    let mut st = self.state.lock().await;
                    st.resource_version = Some(new_rv);
                }
                Err(e) => {
                    warn!(shard_id = %self.shard_id, error = %e, "failed to renew shard lease (lost ownership)");
                    // We lost the lease - close the shard and update state
                    if let Err(close_err) = factory.close(&self.shard_id).await {
                        tracing::error!(shard_id = %self.shard_id, error = %close_err, "failed to close shard after losing lease");
                    }
                    {
                        let mut st = self.state.lock().await;
                        st.resource_version = None;
                        st.lease_uid = None;
                        st.phase = ShardPhase::Idle;
                    }
                    let mut owned = owned_arc.lock().await;
                    owned.remove(&self.shard_id);
                    dst_events::emit(DstEvent::ShardReleased {
                        node_id: self.node_id.clone(),
                        shard_id: self.shard_id.to_string(),
                    });
                    self.notify.notify_one();
                    break;
                }
            }
        }
    }

    /// Renew the lease with CAS semantics.
    /// We fetch the current lease, verify we're still the holder, then update with CAS.
    async fn renew_lease_cas(&self, lease_name: &str) -> Result<String, CoordinationError> {
        let now = Utc::now();

        // Get current lease to verify we still own it and get current state
        let existing = self
            .backend
            .get_lease(&self.namespace, lease_name)
            .await?
            .ok_or_else(|| CoordinationError::BackendError("lease not found".into()))?;

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

        let result = self
            .backend
            .replace_lease(&self.namespace, lease_name, &updated_lease)
            .await?;
        let new_rv = result.metadata.resource_version.ok_or_else(|| {
            CoordinationError::BackendError("no resource_version in response".into())
        })?;
        Ok(new_rv)
    }
}
