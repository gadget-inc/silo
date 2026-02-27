use async_trait::async_trait;
use etcd_client::{Client, ConnectOptions, GetOptions, PutOptions};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::dst_events::{self, DstEvent};
use crate::factory::ShardFactory;
use crate::shard_range::{ShardId, ShardMap, SplitInProgress};

use crate::coordination::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardGuardContext,
    ShardGuardState, ShardOwnerMap, ShardPhase, SplitStorageBackend, get_hostname, keys,
};

/// etcd-based coordinator for distributed shard ownership.
///
/// Shard leases are permanent (KV-based, not tied to any etcd lease).
/// Only membership leases use etcd's built-in TTL/keepalive mechanism.
#[derive(Clone)]
pub struct EtcdCoordinator {
    /// Shared coordinator state (node_id, grpc_addr, owned set, shutdown, factory)
    base: Arc<CoordinatorBase>,
    client: Client,
    cluster_prefix: String,
    membership_lease_id: i64,
    shard_guards: Arc<Mutex<HashMap<ShardId, Arc<EtcdShardGuard>>>>,
}

impl EtcdCoordinator {
    /// Connect to etcd and start the coordinator.
    ///
    /// The coordinator will manage shard ownership and automatically open/close shards in the factory as ownership changes.
    ///
    /// If the cluster doesn't have a shard map yet, one will be created with `initial_shard_count` shards. If the cluster already has a shard map,  the existing one is used and `initial_shard_count` is ignored.
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        endpoints: &[String],
        cluster_prefix: &str,
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        initial_shard_count: u32,
        ttl_secs: i64,
        factory: Arc<ShardFactory>,
        placement_rings: Vec<String>,
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

        // Create membership lease (keepalive-based for liveness detection).
        // Shard leases are permanent KV entries, not tied to any etcd lease.
        let membership_lease = client
            .lease_grant(ttl_secs, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?
            .id();

        let base = Arc::new(CoordinatorBase::new(
            node_id.clone(),
            grpc_addr.clone(),
            shard_map,
            factory,
            placement_rings.clone(),
        ));

        // Write membership key
        let member_key = keys::member_key(&cluster_prefix, &node_id);
        let member_info = MemberInfo {
            node_id: node_id.clone(),
            grpc_addr: grpc_addr.clone(),
            startup_time_ms: base.startup_time_ms,
            hostname: get_hostname(),
            placement_rings,
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
            shard_guards: Arc::new(Mutex::new(HashMap::new())),
        };
        let shutdown_rx = base.shutdown_rx.clone();

        let me_bg = me.clone();
        let cprefix = cluster_prefix.clone();
        let nid = node_id.clone();
        let mut lease_bg = client.lease_client();
        let mut watch_bg = client.watch_client();

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

            if let Err(e) = memb_keeper.keep_alive().await {
                error!(node_id = %nid, error = %e, "failed to send initial membership keepalive");
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

            // Reclaim shards from a previous run before initial reconciliation.
            // This opens shards we still own (triggering WAL recovery) and installs
            // guards in Held state so reconciliation can manage them normally.
            if let Err(e) = me_bg.reclaim_and_open_shards().await {
                warn!(node_id = %nid, error = %e, "failed to reclaim shards from previous run");
            }

            // Initial reconcile
            if let Err(err) = me_bg.reconcile_shards().await {
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
                    biased;
                    _ = keepalive_timer.tick() => {
                        if *shutdown_rx.borrow() { break; }
                        // Send keepalive for membership lease
                        if let Err(e) = memb_keeper.keep_alive().await {
                            error!(node_id = %nid, error = %e, "failed to send membership keepalive");
                        }
                    }
                    _ = resync.tick() => {
                        if *shutdown_rx.borrow() { break; }
                        if let Err(err) = me_bg.reconcile_shards().await {
                            warn!(node_id = %nid, error = %err, "periodic reconcile failed");
                        }
                    }
                    resp = members_stream.message() => {
                        if *shutdown_rx.borrow() { break; }
                        match resp {
                            Ok(Some(_msg)) => {
                                info!(node_id = %nid, "membership changed; reconciling now");
                                if let Err(err) = me_bg.reconcile_shards().await {
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
                                    if let Err(err) = me_bg.reconcile_shards().await {
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

    /// Reclaim shards from a previous run and open them before normal reconciliation.
    ///
    /// This is called once at startup to recover shards this node still owns
    /// from before a crash/restart. Each reclaimed shard is opened via the factory
    /// (triggering SlateDB WAL recovery) and a guard in `Held` state is installed.
    async fn reclaim_and_open_shards(&self) -> Result<(), CoordinationError> {
        let reclaimed = self.reclaim_existing_leases().await?;
        if reclaimed.is_empty() {
            return Ok(());
        }

        let mut opened_count = 0u32;

        for shard_id in &reclaimed {
            // Look up range from shard map (short-lived lock)
            let range = {
                let shard_map = self.base.shard_map.lock().await;
                match shard_map.get_shard(shard_id) {
                    Some(info) => info.range.clone(),
                    None => {
                        warn!(shard_id = %shard_id, "reclaim: shard not found in shard map, skipping");
                        continue;
                    }
                }
            };

            let shard = match self.base.factory.open(shard_id, &range).await {
                Ok(shard) => shard,
                Err(e) => {
                    error!(shard_id = %shard_id, error = %e, "reclaim: failed to open shard, skipping");
                    continue;
                }
            };

            shard.maybe_spawn_background_cleanup(range);

            {
                let mut owned = self.base.owned.lock().await;
                owned.insert(*shard_id);
            }

            let guard = EtcdShardGuard::new_reclaimed(
                *shard_id,
                self.client.clone(),
                self.cluster_prefix.clone(),
                self.base.node_id.clone(),
                self.base.shutdown_rx.clone(),
            );
            let runner = guard.clone();
            let owned_arc = self.base.owned.clone();
            let factory = self.base.factory.clone();
            let shard_map = self.base.shard_map.clone();
            let coordinator: Arc<dyn Coordinator> = Arc::new(self.clone());
            tokio::spawn(
                async move { runner.run(owned_arc, factory, shard_map, coordinator).await },
            );

            {
                let mut guards = self.shard_guards.lock().await;
                guards.insert(*shard_id, guard);
            }

            opened_count += 1;
        }

        if opened_count > 0 {
            info!(
                count = opened_count,
                "reclaimed and opened shards from previous run"
            );
        }

        Ok(())
    }

    async fn reconcile_shards(&self) -> Result<(), CoordinationError> {
        let members = self.get_members().await?;

        let guard_shard_ids: HashSet<ShardId> = {
            let guards = self.shard_guards.lock().await;
            guards.keys().copied().collect()
        };

        let actions = match self
            .base
            .compute_reconcile_actions(&members, &guard_shard_ids)
            .await
        {
            Some(a) => a,
            None => return Ok(()),
        };

        for sid in actions.to_cancel {
            self.ensure_shard_guard(sid)
                .await
                .ctx
                .set_desired(false)
                .await;
        }
        for sid in actions.to_acquire {
            self.ensure_shard_guard(sid)
                .await
                .ctx
                .set_desired(true)
                .await;
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
            self.base.node_id.clone(),
            self.base.shutdown_rx.clone(),
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

    /// Helper method for CAS-based shard map update (used by update_shard_map_for_split).
    async fn try_update_shard_map_for_split(
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

    /// CAS-based shard placement ring update.
    async fn try_update_shard_placement_ring_cas(
        &self,
        shard_id: &crate::shard_range::ShardId,
        new_ring: Option<String>,
    ) -> Result<(Option<String>, Option<String>), CoordinationError> {
        let shard_map_key = keys::shard_map_key(&self.cluster_prefix);

        // Read current shard map from etcd
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

        // Update the shard's placement ring
        let shard = shard_map
            .get_shard_mut(shard_id)
            .ok_or(CoordinationError::ShardNotFound(*shard_id))?;

        let previous = shard.placement_ring.clone();
        shard.placement_ring = new_ring.clone();

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

        // Update local shard map cache
        *self.base.shard_map.lock().await = shard_map;

        Ok((previous, new_ring))
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
        // Retry with exponential backoff to handle concurrent modifications
        // (e.g., cleanup tasks updating cleanup status while a split is in progress)
        let max_retries = 10;
        let mut delay = std::time::Duration::from_millis(10);

        for attempt in 0..max_retries {
            match self.try_update_shard_map_for_split(split).await {
                Ok(()) => return Ok(()),
                Err(CoordinationError::BackendError(msg))
                    if msg.contains("modified concurrently") =>
                {
                    if attempt < max_retries - 1 {
                        debug!(
                            parent_shard_id = %split.parent_shard_id,
                            attempt = attempt,
                            "shard map update hit CAS conflict, retrying"
                        );
                        tokio::time::sleep(delay).await;
                        delay = std::cmp::min(delay * 2, std::time::Duration::from_secs(1));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(CoordinationError::BackendError(format!(
            "failed to update shard map for split after {} retries",
            max_retries
        )))
    }

    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        // Delegate to the inherent method
        EtcdCoordinator::reload_shard_map(self).await
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
        // We trigger shutdown on each guard individually rather than using signal_shutdown() which would notify all guards concurrently
        for (_sid, guard) in guards_sorted {
            guard.ctx.trigger_shutdown().await;
            guard.ctx.notify.notify_one();
            guard.ctx.wait_shutdown(Duration::from_millis(5000)).await;
        }

        // Signal shutdown to stop background tasks
        self.base.signal_shutdown();

        let _ = self
            .client
            .clone()
            .lease_revoke(self.membership_lease_id)
            .await;
        Ok(())
    }

    async fn wait_converged(&self, timeout: Duration) -> bool {
        // Refresh shard map before waiting to avoid converging on stale placement data.
        if let Err(e) = self.reload_shard_map().await {
            debug!(
                node_id = %self.base.node_id,
                error = %e,
                "wait_converged: failed to reload shard map"
            );
        }
        self.base
            .wait_converged(timeout, || async { self.get_members().await })
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

    async fn update_shard_placement_ring(
        &self,
        shard_id: &crate::shard_range::ShardId,
        ring: Option<&str>,
    ) -> Result<(Option<String>, Option<String>), CoordinationError> {
        let sid = *shard_id;
        let new_ring = ring.map(|s| s.to_string());

        // Use CAS-based update with retry to avoid race conditions with the watcher
        let max_retries = 10;
        let mut delay = Duration::from_millis(10);

        for attempt in 0..max_retries {
            match self
                .try_update_shard_placement_ring_cas(&sid, new_ring.clone())
                .await
            {
                Ok(result) => return Ok(result),
                Err(CoordinationError::BackendError(msg))
                    if msg.contains("was modified concurrently") =>
                {
                    if attempt < max_retries - 1 {
                        debug!(
                            shard_id = %sid,
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
            "failed to update shard placement ring after {} retries",
            max_retries
        )))
    }

    async fn force_release_shard_lease(&self, shard_id: &ShardId) -> Result<(), CoordinationError> {
        let owner_key = keys::shard_owner_key(&self.cluster_prefix, shard_id);
        let resp = self
            .client
            .kv_client()
            .delete(owner_key, None)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        if resp.deleted() == 0 {
            return Err(CoordinationError::ShardNotFound(*shard_id));
        }

        info!(shard_id = %shard_id, "force-released shard lease");
        Ok(())
    }

    async fn reclaim_existing_leases(&self) -> Result<Vec<ShardId>, CoordinationError> {
        let shards_prefix = keys::shards_prefix(&self.cluster_prefix);
        let resp = self
            .client
            .kv_client()
            .get(shards_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;

        let mut reclaimed = Vec::new();
        for kv in resp.kvs() {
            let value = String::from_utf8_lossy(kv.value());
            if value == self.base.node_id {
                // This shard's owner key has our node_id - parse the shard ID from the key path
                let key_str = String::from_utf8_lossy(kv.key());
                // Key format: {prefix}/coord/shards/{shard_id}/owner
                if let Some(shard_id_str) = key_str
                    .strip_suffix("/owner")
                    .and_then(|s| s.rsplit('/').next())
                {
                    match shard_id_str.parse::<uuid::Uuid>() {
                        Ok(uuid) => {
                            let shard_id = ShardId::from(uuid);
                            info!(shard_id = %shard_id, "found existing shard lease to reclaim");
                            reclaimed.push(shard_id);
                        }
                        Err(e) => {
                            warn!(key = %key_str, error = %e, "failed to parse shard ID from owner key");
                        }
                    }
                }
            }
        }

        Ok(reclaimed)
    }
}

/// Per-shard ownership guard for etcd backend.
///
/// Uses KV transactions (put-if-absent / delete-if-owner) for permanent shard
/// ownership instead of the Lock API. Shard owner keys are not tied to any etcd
/// lease, so they persist until explicitly deleted.
pub struct EtcdShardGuard {
    pub ctx: ShardGuardContext<()>,
    pub client: Client,
    pub cluster_prefix: String,
    pub node_id: String,
}

impl EtcdShardGuard {
    pub fn new(
        shard_id: ShardId,
        client: Client,
        cluster_prefix: String,
        node_id: String,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ctx: ShardGuardContext::new(shard_id, shutdown),
            client,
            cluster_prefix,
            node_id,
        })
    }

    /// Create a guard already in Held state for a shard reclaimed from a previous run.
    ///
    /// The guard starts in `Held` phase with `desired: true` and token `Some(())`,
    /// meaning it will sit in the `Idle | Held` wait arm until reconciliation
    /// decides what to do with it.
    pub fn new_reclaimed(
        shard_id: ShardId,
        client: Client,
        cluster_prefix: String,
        node_id: String,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ctx: ShardGuardContext::new_with_state(
                shard_id,
                shutdown,
                ShardGuardState {
                    desired: true,
                    phase: ShardPhase::Held,
                    ownership_token: Some(()),
                },
            ),
            client,
            cluster_prefix,
            node_id,
        })
    }

    fn owner_key(&self) -> String {
        keys::shard_owner_key(&self.cluster_prefix, &self.ctx.shard_id)
    }

    /// Try to acquire shard ownership using a KV put-if-absent transaction.
    /// Returns Ok(true) if acquired, Ok(false) if already owned by another node.
    async fn try_acquire_ownership(&self) -> Result<bool, etcd_client::Error> {
        let owner_key = self.owner_key();
        let txn = etcd_client::Txn::new()
            .when([etcd_client::Compare::create_revision(
                owner_key.clone(),
                etcd_client::CompareOp::Equal,
                0,
            )])
            .and_then([etcd_client::TxnOp::put(
                owner_key.clone(),
                self.node_id.clone(),
                None, // No lease - permanent ownership
            )])
            .or_else([etcd_client::TxnOp::get(owner_key, None)]);

        let txn_resp = self.client.kv_client().txn(txn).await?;

        if txn_resp.succeeded() {
            return Ok(true);
        }

        // Key already exists - check if it's ours (reclaim scenario)
        if let Some(etcd_client::TxnOpResponse::Get(get_resp)) = txn_resp.op_responses().first()
            && let Some(kv) = get_resp.kvs().first()
        {
            let current_owner = String::from_utf8_lossy(kv.value());
            if current_owner == self.node_id {
                // Already ours from a previous run - reclaim
                return Ok(true);
            }
            debug!(
                shard_id = %self.ctx.shard_id,
                current_owner = %current_owner,
                "shard owned by another node"
            );
        }

        Ok(false)
    }

    /// Release shard ownership using a KV delete-if-owner transaction.
    async fn release_ownership(&self) -> Result<(), etcd_client::Error> {
        let owner_key = self.owner_key();
        let txn = etcd_client::Txn::new()
            .when([etcd_client::Compare::value(
                owner_key.clone(),
                etcd_client::CompareOp::Equal,
                self.node_id.clone(),
            )])
            .and_then([etcd_client::TxnOp::delete(owner_key, None)]);

        let _ = self.client.kv_client().txn(txn).await?;
        Ok(())
    }

    pub async fn run(
        self: Arc<Self>,
        owned_arc: Arc<Mutex<HashSet<ShardId>>>,
        factory: Arc<ShardFactory>,
        shard_map: Arc<Mutex<ShardMap>>,
        _coordinator: Arc<dyn Coordinator>,
    ) {
        use tracing::{Instrument, info_span};

        loop {
            if self.ctx.is_shutdown() {
                let mut st = self.ctx.state.lock().await;
                st.phase = ShardPhase::ShuttingDown;
            }

            {
                let mut st = self.ctx.state.lock().await;
                st.maybe_transition();
            }

            let phase = { self.ctx.state.lock().await.phase };
            match phase {
                ShardPhase::ShutDown => break,
                ShardPhase::Acquiring => {
                    let owner_key = self.owner_key();
                    let span = info_span!("shard.acquire", shard_id = %self.ctx.shard_id, owner_key = %owner_key);
                    async {
                        let mut attempt: u32 = 0;

                        // Use first 8 bytes of UUID for jitter seed
                        let uuid_bytes = self.ctx.shard_id.as_uuid().as_bytes();
                        let jitter_seed = u64::from_le_bytes(uuid_bytes[0..8].try_into().unwrap());
                        let initial_jitter_ms = jitter_seed % 80;
                        tokio::time::sleep(Duration::from_millis(initial_jitter_ms)).await;

                        loop {
                            if { self.ctx.state.lock().await.phase } != ShardPhase::Acquiring {
                                break;
                            }
                            {
                                let mut st = self.ctx.state.lock().await;
                                if !st.desired || st.phase != ShardPhase::Acquiring {
                                    if !st.desired && st.phase == ShardPhase::Acquiring {
                                        st.phase = ShardPhase::Idle;
                                    }
                                    info!(shard_id = %self.ctx.shard_id, desired = st.desired, phase = %st.phase, "shard: acquire abort");
                                    break;
                                }
                            }

                            // Try to acquire ownership via KV put-if-absent transaction
                            match self.try_acquire_ownership().await {
                                Ok(true) => {
                                    // Look up the shard's range from the shard map
                                    let range = {
                                        let map = shard_map.lock().await;
                                        match map.get_shard(&self.ctx.shard_id) {
                                            Some(info) => info.range.clone(),
                                            None => {
                                                tracing::error!(shard_id = %self.ctx.shard_id, "shard not found in shard map");
                                                let _ = self.release_ownership().await;
                                                continue;
                                            }
                                        }
                                    };
                                    // Open the shard BEFORE marking as Held - if open fails, we should release ownership and not claim it.
                                    let shard = match factory.open(&self.ctx.shard_id, &range).await {
                                        Ok(shard) => shard,
                                        Err(e) => {
                                            tracing::error!(shard_id = %self.ctx.shard_id, error = %e, "failed to open shard, releasing ownership");
                                            let _ = self.release_ownership().await;
                                            // Exponential backoff before retry
                                            let backoff_ms = 200 * (1 << attempt.min(5));
                                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                            attempt = attempt.wrapping_add(1);
                                            continue;
                                        }
                                    };

                                    // Spawn background cleanup if this shard has pending cleanup work
                                    // (e.g., it's a split child or was re-acquired after a crash)
                                    shard.maybe_spawn_background_cleanup(range.clone());

                                    {
                                        let mut st = self.ctx.state.lock().await;
                                        st.ownership_token = Some(());
                                        st.phase = ShardPhase::Held;
                                    }
                                    {
                                        let mut owned = owned_arc.lock().await;
                                        owned.insert(self.ctx.shard_id);
                                    }
                                    dst_events::emit(DstEvent::ShardAcquired {
                                        node_id: self.node_id.clone(),
                                        shard_id: self.ctx.shard_id.to_string(),
                                    });
                                    info!(shard_id = %self.ctx.shard_id, attempts = attempt, "shard: acquired and opened");
                                    break;
                                }
                                Ok(false) => {
                                    // Owned by another node - retry with jitter
                                    attempt = attempt.wrapping_add(1);
                                    let jitter_ms = (jitter_seed
                                        .wrapping_mul(31)
                                        .wrapping_add(attempt as u64 * 17))
                                        % 150;
                                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                                    if attempt % 8 == 0 {
                                        debug!(shard_id = %self.ctx.shard_id, attempt = attempt, "shard: acquire retry");
                                    }
                                }
                                Err(e) => {
                                    warn!(shard_id = %self.ctx.shard_id, error = %e, "shard: acquire txn error, retrying");
                                    attempt = attempt.wrapping_add(1);
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::Releasing => {
                    let owner_key = self.owner_key();
                    let span = info_span!("shard.release", shard_id = %self.ctx.shard_id, owner_key = %owner_key);
                    async {
                        debug!(shard_id = %self.ctx.shard_id, "shard: release start (delay)");
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        let mut cancelled = false;
                        let was_held = {
                            let mut st = self.ctx.state.lock().await;
                            if st.phase == ShardPhase::ShuttingDown {
                                // fall through
                            } else if st.desired {
                                st.phase = ShardPhase::Held;
                                cancelled = true;
                                debug!(shard_id = %self.ctx.shard_id, "shard: release cancelled");
                            }
                            st.has_token()
                        };
                        if cancelled {
                            return;
                        }
                        if was_held {
                            // Close the shard before releasing ownership
                            match factory.close(&self.ctx.shard_id).await {
                                Ok(()) => {
                                    dst_events::emit(DstEvent::ShardClosed {
                                        node_id: self.node_id.clone(),
                                        shard_id: self.ctx.shard_id.to_string(),
                                    });
                                    if let Err(e) = self.release_ownership().await {
                                        tracing::error!(shard_id = %self.ctx.shard_id, error = %e, "failed to release shard ownership");
                                    }
                                    {
                                        let mut st = self.ctx.state.lock().await;
                                        st.ownership_token = None;
                                        st.phase = ShardPhase::Idle;
                                    }
                                    {
                                        let mut owned = owned_arc.lock().await;
                                        owned.remove(&self.ctx.shard_id);
                                    }
                                    dst_events::emit(DstEvent::ShardReleased {
                                        node_id: self.node_id.clone(),
                                        shard_id: self.ctx.shard_id.to_string(),
                                    });
                                    debug!(shard_id = %self.ctx.shard_id, "shard: release done");
                                }
                                Err(e) => {
                                    // Close failed - revert to Held so reconciliation retries
                                    tracing::error!(
                                        shard_id = %self.ctx.shard_id,
                                        error = %e,
                                        "failed to close shard, reverting to Held to prevent data loss"
                                    );
                                    let mut st = self.ctx.state.lock().await;
                                    st.phase = ShardPhase::Held;
                                }
                            }
                        } else {
                            let mut st = self.ctx.state.lock().await;
                            st.phase = ShardPhase::Idle;
                            debug!(shard_id = %self.ctx.shard_id, "shard: release noop");
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::ShuttingDown => {
                    let was_held = self.ctx.state.lock().await.has_token();
                    if was_held {
                        // Close the shard before releasing ownership
                        let close_succeeded = match factory.close(&self.ctx.shard_id).await {
                            Ok(()) => {
                                dst_events::emit(DstEvent::ShardClosed {
                                    node_id: self.node_id.clone(),
                                    shard_id: self.ctx.shard_id.to_string(),
                                });
                                true
                            }
                            Err(e) => {
                                // Close failed - do NOT release ownership.
                                // This mimics crash behavior: the permanent lease stays held
                                // until operator force-release, preventing another node from
                                // acquiring unflushed data.
                                tracing::error!(
                                    shard_id = %self.ctx.shard_id,
                                    error = %e,
                                    "failed to close shard during shutdown, keeping ownership to prevent data loss"
                                );
                                false
                            }
                        };

                        if close_succeeded {
                            if let Err(e) = self.release_ownership().await {
                                tracing::error!(shard_id = %self.ctx.shard_id, error = %e, "failed to release shard ownership during shutdown");
                            }
                            {
                                let mut owned = owned_arc.lock().await;
                                owned.remove(&self.ctx.shard_id);
                            }
                            dst_events::emit(DstEvent::ShardReleased {
                                node_id: self.node_id.clone(),
                                shard_id: self.ctx.shard_id.to_string(),
                            });
                        }
                    }
                    {
                        let mut st = self.ctx.state.lock().await;
                        st.ownership_token = None;
                        st.phase = ShardPhase::ShutDown;
                    }
                    break;
                }
                ShardPhase::Idle | ShardPhase::Held => {
                    self.ctx.wait_for_change().await;
                }
            }
        }
    }
}
