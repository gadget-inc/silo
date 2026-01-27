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
use crate::shard_range::{ShardId, ShardMap, SplitInProgress};

use super::SplitCleanupStatus;

// Re-export ShardPhase for backwards compatibility (tests reference it from here)
pub use super::ShardPhase;

use super::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap,
    SplitStorageBackend, compute_desired_shards_for_node, get_hostname, keys,
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
    shard_guards: Arc<Mutex<HashMap<ShardId, Arc<EtcdShardGuard>>>>,
}

impl EtcdCoordinator {
    /// Connect to etcd and start the coordinator.
    ///
    /// The coordinator will manage shard ownership and automatically open/close
    /// shards in the factory as ownership changes.
    ///
    /// If the cluster doesn't have a shard map yet, one will be created with
    /// `initial_shard_count` shards. If the cluster already has a shard map,
    /// the existing one is used and `initial_shard_count` is ignored.
    pub async fn start(
        endpoints: &[String],
        cluster_prefix: &str,
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        initial_shard_count: u32,
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

        // Load or create the shard map
        let shard_map =
            Self::load_or_create_shard_map(&mut client, &cluster_prefix, initial_shard_count)
                .await?;

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
            shard_map,
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

        let handle = tokio::spawn(async move {
            // Start lease keepalives with retries
            let (mut memb_keeper, mut memb_stream) = loop {
                if *shutdown_rx.borrow() {
                    return;
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

            let (mut live_keeper, mut live_stream) = loop {
                if *shutdown_rx.borrow() {
                    return;
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

            // Establish shard map watch with retries
            let shard_map_key = keys::shard_map_key(&cprefix);
            let (_shard_map_watcher, mut shard_map_stream) = loop {
                if *shutdown_rx.borrow() {
                    return;
                }
                match watch_bg.watch(shard_map_key.clone(), None).await {
                    Ok(w) => {
                        debug!(node_id = %nid, key = %shard_map_key, "shard map watch established");
                        break w;
                    }
                    Err(e) => {
                        warn!(node_id = %nid, error = %e, "failed to establish shard map watch, retrying...");
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            };

            // Initial reconcile
            if let Err(err) = me_bg.reconcile_shards(&mut lock_bg).await {
                warn!(node_id = %nid, error = %err, "initial reconcile failed");
            }

            // Main loop: reconcile on membership events, periodic resync, and send keepalives
            let mut resync = tokio::time::interval(Duration::from_secs(1));
            // Send keepalives at 1/3 of TTL to ensure lease doesn't expire
            let keepalive_interval_secs = (ttl_secs / 3).max(1) as u64;
            let mut keepalive_timer =
                tokio::time::interval(Duration::from_secs(keepalive_interval_secs));
            debug!(node_id = %nid, interval_secs = keepalive_interval_secs, "starting keepalive timer");

            loop {
                tokio::select! {
                    _ = keepalive_timer.tick() => {
                        if *shutdown_rx.borrow() { break; }
                        // Send keepalive requests to both leases
                        if let Err(e) = memb_keeper.keep_alive().await {
                            error!(node_id = %nid, error = %e, "failed to send membership keepalive");
                        }
                        if let Err(e) = live_keeper.keep_alive().await {
                            error!(node_id = %nid, error = %e, "failed to send liveness keepalive");
                        }
                    }
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
                    resp = shard_map_stream.message() => {
                        if *shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(_msg)) => {
                                debug!(node_id = %nid, "shard map changed; reloading");
                                if let Err(err) = me_bg.reload_shard_map().await {
                                    warn!(node_id = %nid, error = %err, "failed to reload shard map");
                                } else {
                                    // Reconcile after shard map change to pick up ownership changes
                                    if let Err(err) = me_bg.reconcile_shards(&mut lock_bg).await {
                                        warn!(node_id = %nid, error = %err, "post-shard-map-change reconcile failed");
                                    }
                                }
                            }
                            Ok(None) => {
                                warn!(node_id = %nid, "shard map watch stream closed");
                            }
                            Err(e) => {
                                warn!(node_id = %nid, error = %e, "shard map watch error");
                            }
                        }
                    }
                    resp = memb_stream.message() => {
                        if *shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(ka_resp)) => {
                                debug!(node_id = %nid, ttl = ka_resp.ttl(), "membership keepalive response received");
                            }
                            Ok(None) => {
                                error!(node_id = %nid, "membership lease keepalive stream closed! Lease will expire.");
                            }
                            Err(e) => {
                                error!(node_id = %nid, error = %e, "membership lease keepalive error");
                            }
                        }
                    }
                    resp = live_stream.message() => {
                        if *shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(ka_resp)) => {
                                debug!(node_id = %nid, ttl = ka_resp.ttl(), "liveness keepalive response received");
                            }
                            Ok(None) => {
                                error!(node_id = %nid, "liveness lease keepalive stream closed! Lease will expire.");
                            }
                            Err(e) => {
                                error!(node_id = %nid, error = %e, "liveness lease keepalive error");
                            }
                        }
                    }
                }
            }
        });

        Ok((me, handle))
    }

    /// Load an existing shard map from etcd, or create a new one if none exists.
    async fn load_or_create_shard_map(
        client: &mut Client,
        cluster_prefix: &str,
        initial_shard_count: u32,
    ) -> Result<ShardMap, CoordinationError> {
        let shard_map_key = keys::shard_map_key(cluster_prefix);

        // Try to read existing shard map
        let resp = client
            .kv_client()
            .get(shard_map_key.clone(), None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if let Some(kv) = resp.kvs().first() {
            // Existing shard map found - parse and use it
            let value = String::from_utf8_lossy(kv.value());
            let shard_map: ShardMap = serde_json::from_str(&value).map_err(|e| {
                CoordinationError::BackendError(format!("invalid shard map JSON: {}", e))
            })?;
            info!(
                num_shards = shard_map.len(),
                version = shard_map.version,
                "loaded existing shard map from etcd"
            );
            return Ok(shard_map);
        }

        // No existing shard map - create a new one
        let shard_map = ShardMap::create_initial(initial_shard_count)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let shard_map_json = serde_json::to_string(&shard_map)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        // Try to create it (may fail if another node created it first)
        // Use a transaction to ensure we only create if it doesn't exist
        let txn = etcd_client::Txn::new()
            .when([etcd_client::Compare::version(
                shard_map_key.clone(),
                etcd_client::CompareOp::Equal,
                0,
            )])
            .and_then([etcd_client::TxnOp::put(
                shard_map_key.clone(),
                shard_map_json.clone(),
                None,
            )])
            .or_else([etcd_client::TxnOp::get(shard_map_key.clone(), None)]);

        let txn_resp = client
            .kv_client()
            .txn(txn)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if txn_resp.succeeded() {
            info!(
                num_shards = initial_shard_count,
                "created new shard map in etcd"
            );
            Ok(shard_map)
        } else {
            // Another node created it first - read and use theirs
            if let Some(etcd_client::TxnOpResponse::Get(get_resp)) = txn_resp.op_responses().first()
                && let Some(kv) = get_resp.kvs().first()
            {
                let value = String::from_utf8_lossy(kv.value());
                let existing_map: ShardMap = serde_json::from_str(&value).map_err(|e| {
                    CoordinationError::BackendError(format!("invalid shard map JSON: {}", e))
                })?;
                info!(
                    num_shards = existing_map.len(),
                    "using shard map created by another node"
                );
                return Ok(existing_map);
            }
            Err(CoordinationError::BackendError(
                "failed to load shard map after create race".to_string(),
            ))
        }
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

        let shard_ids = self.base.shard_ids().await;
        let desired = compute_desired_shards_for_node(&shard_ids, &self.base.node_id, &members);

        let current_locks: Vec<ShardId> = {
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

        // Release undesired shards (sorted for deterministic ordering)
        {
            let mut to_release: Vec<ShardId> = {
                let guards = self.shard_guards.lock().await;
                let mut v = Vec::new();
                for (sid, guard) in guards.iter() {
                    if !desired.contains(sid) && guard.state.lock().await.held_key.is_some() {
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

    async fn ensure_shard_guard(&self, shard_id: ShardId) -> Arc<EtcdShardGuard> {
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
        let shard_map = self.base.shard_map.clone();
        tokio::spawn(async move { runner.run(owned_arc, factory, shard_map).await });
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

    /// Reload the shard map from etcd into local cache.
    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        let shard_map_key = keys::shard_map_key(&self.cluster_prefix);
        let resp = self
            .client
            .kv_client()
            .get(shard_map_key, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if let Some(kv) = resp.kvs().first() {
            let value = String::from_utf8_lossy(kv.value());
            let shard_map: ShardMap = serde_json::from_str(&value).map_err(|e| {
                CoordinationError::BackendError(format!("invalid shard map JSON: {}", e))
            })?;
            *self.base.shard_map.lock().await = shard_map;
        }
        Ok(())
    }
}

#[async_trait]
impl SplitStorageBackend for EtcdCoordinator {
    async fn load_split(
        &self,
        parent_shard_id: &ShardId,
    ) -> Result<Option<SplitInProgress>, CoordinationError> {
        let split_key = keys::split_key(&self.cluster_prefix, parent_shard_id);
        let resp = self
            .client
            .kv_client()
            .get(split_key, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if let Some(kv) = resp.kvs().first() {
            let value = String::from_utf8_lossy(kv.value());
            let split: SplitInProgress = serde_json::from_str(&value).map_err(|e| {
                CoordinationError::BackendError(format!("invalid split JSON: {}", e))
            })?;
            Ok(Some(split))
        } else {
            Ok(None)
        }
    }

    async fn store_split(&self, split: &SplitInProgress) -> Result<(), CoordinationError> {
        let split_key = keys::split_key(&self.cluster_prefix, &split.parent_shard_id);
        let split_json = serde_json::to_string(split)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        self.client
            .kv_client()
            .put(split_key, split_json, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        Ok(())
    }

    async fn delete_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
        let split_key = keys::split_key(&self.cluster_prefix, parent_shard_id);
        self.client
            .kv_client()
            .delete(split_key, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn update_shard_map_for_split(
        &self,
        split: &SplitInProgress,
    ) -> Result<(), CoordinationError> {
        let shard_map_key = keys::shard_map_key(&self.cluster_prefix);

        // Read current shard map
        let resp = self
            .client
            .kv_client()
            .get(shard_map_key.clone(), None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let kv = resp.kvs().first().ok_or_else(|| {
            CoordinationError::BackendError("shard map not found in etcd".to_string())
        })?;

        let current_version = kv.mod_revision();
        let value = String::from_utf8_lossy(kv.value());
        let mut shard_map: ShardMap = serde_json::from_str(&value)
            .map_err(|e| CoordinationError::BackendError(format!("invalid shard map: {}", e)))?;

        // Apply the split to the shard map
        shard_map.split_shard(
            &split.parent_shard_id,
            &split.split_point,
            split.left_child_id,
            split.right_child_id,
        )?;

        let new_map_json = serde_json::to_string(&shard_map)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        // Use CAS to update atomically
        let txn = etcd_client::Txn::new()
            .when([etcd_client::Compare::mod_revision(
                shard_map_key.clone(),
                etcd_client::CompareOp::Equal,
                current_version,
            )])
            .and_then([etcd_client::TxnOp::put(
                shard_map_key.clone(),
                new_map_json,
                None,
            )]);

        let txn_resp = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if !txn_resp.succeeded() {
            return Err(CoordinationError::BackendError(
                "shard map was modified concurrently".to_string(),
            ));
        }

        // Update local shard map
        *self.base.shard_map.lock().await = shard_map;

        info!(
            parent_shard_id = %split.parent_shard_id,
            left_child_id = %split.left_child_id,
            right_child_id = %split.right_child_id,
            split_point = %split.split_point,
            "shard map updated for split"
        );

        Ok(())
    }

    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        // Delegate to the inherent method
        EtcdCoordinator::reload_shard_map(self).await
    }

    async fn update_cleanup_status_in_shard_map(
        &self,
        shard_id: ShardId,
        status: SplitCleanupStatus,
    ) -> Result<(), CoordinationError> {
        let shard_map_key = keys::shard_map_key(&self.cluster_prefix);

        // Read current shard map
        let resp = self
            .client
            .kv_client()
            .get(shard_map_key.clone(), None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let kv = resp
            .kvs()
            .first()
            .ok_or_else(|| CoordinationError::BackendError("shard map not found".to_string()))?;

        let current_version = kv.mod_revision();
        let value = String::from_utf8_lossy(kv.value());
        let mut shard_map: ShardMap = serde_json::from_str(&value)
            .map_err(|e| CoordinationError::BackendError(format!("invalid shard map: {}", e)))?;

        // Update cleanup status
        shard_map.update_cleanup_status(&shard_id, status)?;

        let new_map_json = serde_json::to_string(&shard_map)
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        // Use CAS to update atomically
        let txn = etcd_client::Txn::new()
            .when([etcd_client::Compare::mod_revision(
                shard_map_key.clone(),
                etcd_client::CompareOp::Equal,
                current_version,
            )])
            .and_then([etcd_client::TxnOp::put(
                shard_map_key.clone(),
                new_map_json,
                None,
            )]);

        let txn_resp = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if !txn_resp.succeeded() {
            return Err(CoordinationError::BackendError(
                "shard map was modified concurrently".to_string(),
            ));
        }

        // Update local shard map
        *self.base.shard_map.lock().await = shard_map;

        debug!(shard_id = %shard_id, status = %status, "cleanup status updated");
        Ok(())
    }

    async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
        let splits_prefix = keys::splits_prefix(&self.cluster_prefix);
        let resp = self
            .client
            .kv_client()
            .get(splits_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let mut splits = Vec::new();
        for kv in resp.kvs() {
            let value = String::from_utf8_lossy(kv.value());
            if let Ok(split) = serde_json::from_str::<SplitInProgress>(&value) {
                splits.push(split);
            }
        }
        Ok(splits)
    }
}

#[async_trait]
impl Coordinator for EtcdCoordinator {
    fn base(&self) -> &CoordinatorBase {
        &self.base
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        // Collect guards in sorted order for deterministic shutdown
        let guards_sorted: Vec<(ShardId, Arc<EtcdShardGuard>)> = {
            let guards = self.shard_guards.lock().await;
            let mut shard_ids: Vec<ShardId> = guards.keys().copied().collect();
            shard_ids.sort_unstable();
            shard_ids
                .into_iter()
                .filter_map(|sid| guards.get(&sid).map(|g| (sid, g.clone())))
                .collect()
        };

        // Shut down each guard sequentially for deterministic ordering
        // We trigger shutdown on each guard individually rather than using
        // signal_shutdown() which would notify all guards concurrently
        for (_sid, guard) in guards_sorted {
            guard.trigger_shutdown().await;
            guard.notify.notify_one();
            guard.wait_shutdown(Duration::from_millis(5000)).await;
        }

        // Signal shutdown to stop background tasks
        self.base.signal_shutdown();

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
        Ok(self.base.compute_shard_owner_map(&members).await)
    }
}

pub struct ShardState {
    pub desired: bool,
    pub phase: ShardPhase,
    pub held_key: Option<Vec<u8>>,
}

/// Per-shard lock guard for etcd backend.
pub struct EtcdShardGuard {
    pub shard_id: ShardId,
    pub client: Client,
    pub cluster_prefix: String,
    pub liveness_lease_id: i64,
    pub state: Mutex<ShardState>,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
}

impl EtcdShardGuard {
    pub fn new(
        shard_id: ShardId,
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
        keys::shard_owner_key(&self.cluster_prefix, &self.shard_id)
    }

    pub async fn set_desired(&self, desired: bool) {
        let mut st = self.state.lock().await;
        if matches!(st.phase, ShardPhase::ShutDown | ShardPhase::ShuttingDown) {
            debug!(shard_id = %self.shard_id, desired = desired, phase = %st.phase, "shard: can't change desired state after shut down");
            return;
        }

        if st.desired != desired {
            let prev_desired = st.desired;
            let prev_phase = st.phase;
            st.desired = desired;
            debug!(shard_id = %self.shard_id, prev_desired = prev_desired, desired = desired, phase = %prev_phase, "shard: set_desired");
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
                            shard_id = %self.shard_id,
                            "shard: transition Idle -> Acquiring"
                        );
                        st.phase = ShardPhase::Acquiring;
                    }
                    (ShardPhase::Held, false, true) => {
                        debug!(
                            shard_id = %self.shard_id,
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
                        info_span!("shard.acquire", shard_id = %self.shard_id, lock_key = %name);
                    async {
                        let mut lock_cli = self.client.lock_client();
                        let mut attempt: u32 = 0;

                        // Use first 8 bytes of UUID for jitter seed
                        let uuid_bytes = self.shard_id.as_uuid().as_bytes();
                        let jitter_seed = u64::from_le_bytes(uuid_bytes[0..8].try_into().unwrap());
                        let initial_jitter_ms = jitter_seed % 80;
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
                                    info!(shard_id = %self.shard_id, desired = st.desired, phase = %st.phase, "shard: acquire abort");
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
                                    // Look up the shard's range from the shard map
                                    let range = {
                                        let map = shard_map.lock().await;
                                        match map.get_shard(&self.shard_id) {
                                            Some(info) => info.range.clone(),
                                            None => {
                                                tracing::error!(shard_id = %self.shard_id, "shard not found in shard map");
                                                let mut lock_cli = self.client.lock_client();
                                                let _ = lock_cli.unlock(key).await;
                                                continue;
                                            }
                                        }
                                    };
                                    // Open the shard BEFORE marking as Held - if open fails,
                                    // we should release the lock and not claim ownership
                                    match factory.open(&self.shard_id, &range).await {
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
                                            info!(shard_id = %self.shard_id, attempts = attempt, "shard: acquired and opened");
                                            break;
                                        }
                                        Err(e) => {
                                            // Failed to open - release the lock and retry
                                            tracing::error!(shard_id = %self.shard_id, error = %e, "failed to open shard, releasing lock");
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
                                    let jitter_ms = (jitter_seed
                                        .wrapping_mul(31)
                                        .wrapping_add(attempt as u64 * 17))
                                        % 150;
                                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                                    if attempt % 8 == 0 {
                                        debug!(shard_id = %self.shard_id, attempt = attempt, "shard: acquire retry");
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
                        info_span!("shard.release", shard_id = %self.shard_id, lock_key = %name);
                    async {
                        debug!(shard_id = %self.shard_id, "shard: release start (delay)");
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        let mut cancelled = false;
                        let key_opt = {
                            let mut st = self.state.lock().await;
                            if st.phase == ShardPhase::ShuttingDown {
                                // fall through
                            } else if st.desired {
                                st.phase = ShardPhase::Held;
                                cancelled = true;
                                debug!(shard_id = %self.shard_id, "shard: release cancelled");
                            }
                            st.held_key.clone()
                        };
                        if cancelled {
                            return;
                        }
                        if let Some(key) = key_opt {
                            // Close the shard before releasing the lock
                            if let Err(e) = factory.close(&self.shard_id).await {
                                tracing::error!(shard_id = %self.shard_id, error = %e, "failed to close shard before releasing lock");
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
                            debug!(shard_id = %self.shard_id, "shard: release done");
                        } else {
                            let mut st = self.state.lock().await;
                            st.phase = ShardPhase::Idle;
                            debug!(shard_id = %self.shard_id, "shard: release noop");
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::ShuttingDown => {
                    let key_opt = { self.state.lock().await.held_key.clone() };
                    if let Some(key) = key_opt {
                        // Close the shard before releasing the lock
                        if let Err(e) = factory.close(&self.shard_id).await {
                            tracing::error!(shard_id = %self.shard_id, error = %e, "failed to close shard during shutdown");
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
