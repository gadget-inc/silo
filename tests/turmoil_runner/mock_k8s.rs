//! Mock Kubernetes coordination for DST testing.
//!
//! This module provides:
//!
//! 1. `MockK8sState` - An in-memory store for Kubernetes Lease and ConfigMap objects with:
//!    - Resource versioning for CAS semantics
//!    - Watch event broadcasting
//!    - Label-based filtering
//!
//! 2. `MockK8sBackend` - Implementation of the K8sBackend trait that wraps MockK8sState,
//!    allowing the real K8sCoordinator to be tested with the mock state.
//!
//! This allows testing K8s coordination logic with deterministic fault injection,
//! including message loss, network partitions, and timing variations.

use async_trait::async_trait;
use chrono::Utc;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use rand::Rng;
use silo::coordination::{
    ConfigMapWatchEvent, ConfigMapWatchStream, CoordinationError, K8sBackend, LeaseWatchEvent,
    LeaseWatchStream,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
use tracing::trace;

/// In-memory state for the mock K8s API, simulating the K8s API server.
/// This can be shared between multiple coordinators to test multi-node scenarios.
#[derive(Debug)]
pub struct MockK8sState {
    /// Leases stored by namespace -> name -> lease
    leases: Mutex<HashMap<String, HashMap<String, StoredLease>>>,
    /// ConfigMaps stored by namespace -> name -> configmap
    configmaps: Mutex<HashMap<String, HashMap<String, StoredConfigMap>>>,
    /// Monotonically increasing resource version counter
    resource_version: AtomicU64,
    /// Broadcast channel for Lease watch events
    watch_tx: broadcast::Sender<WatchEvent>,
    /// Broadcast channel for ConfigMap watch events
    configmap_watch_tx: broadcast::Sender<ConfigMapWatchBroadcastEvent>,
    /// Simulated operation latency (for testing with delays)
    pub operation_latency: Mutex<Option<Duration>>,
    /// Simulated failure rate (0.0 - 1.0, probability of operation failing)
    pub failure_rate: Mutex<f64>,
    /// Simulated delay between write acknowledgment and watch event delivery.
    /// In real Kubernetes, there's latency between when the API server persists a write
    /// and when watchers receive the notification. This simulates that behavior.
    pub watch_event_delay: Mutex<Option<Duration>>,
}

/// A ConfigMap stored in the mock server with metadata
#[derive(Debug, Clone)]
pub struct StoredConfigMap {
    pub name: String,
    pub namespace: String,
    pub data: HashMap<String, String>,
    pub labels: HashMap<String, String>,
    pub resource_version: String,
    pub uid: String,
}

/// A lease stored in the mock server with metadata
#[derive(Debug, Clone)]
pub struct StoredLease {
    pub lease: Lease,
    pub resource_version: String,
    pub uid: String,
}

/// Watch event sent to watchers
/// Watch event for Lease changes
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub namespace: String,
    pub name: String,
    pub object: Lease,
}

/// Watch event for ConfigMap changes
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConfigMapWatchBroadcastEvent {
    pub event_type: WatchEventType,
    pub namespace: String,
    pub name: String,
    pub object: ConfigMap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
}

impl MockK8sState {
    pub fn new() -> Arc<Self> {
        let (watch_tx, _) = broadcast::channel(1024);
        let (configmap_watch_tx, _) = broadcast::channel(1024);
        Arc::new(Self {
            leases: Mutex::new(HashMap::new()),
            configmaps: Mutex::new(HashMap::new()),
            resource_version: AtomicU64::new(1),
            watch_tx,
            configmap_watch_tx,
            operation_latency: Mutex::new(None),
            failure_rate: Mutex::new(0.0),
            watch_event_delay: Mutex::new(Some(Duration::from_millis(50))),
        })
    }

    fn next_resource_version(&self) -> String {
        self.resource_version
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
    }

    fn generate_uid(&self) -> String {
        let rv = self.resource_version.load(Ordering::SeqCst);
        // Use rand::rng() which goes through getrandom, which mad-turmoil patches
        // for deterministic simulation. Do NOT use fastrand as it has its own PRNG
        // that isn't properly tracked across async task scheduling.
        format!("uid-{}-{}", rv, rand::rng().random::<u64>())
    }

    /// Check if an operation should fail based on failure_rate
    async fn should_fail(&self) -> bool {
        let rate = *self.failure_rate.lock().await;
        if rate > 0.0 {
            rand::rng().random::<f64>() < rate
        } else {
            false
        }
    }

    /// Apply operation latency if configured
    async fn apply_latency(&self) {
        if let Some(latency) = *self.operation_latency.lock().await {
            tokio::time::sleep(latency).await;
        }
    }

    /// Get the configured watch event delay
    async fn get_watch_delay(&self) -> Option<Duration> {
        *self.watch_event_delay.lock().await
    }

    /// Emit a lease watch event, possibly with delay
    fn emit_lease_watch_event(&self, event: WatchEvent, delay: Option<Duration>) {
        let tx = self.watch_tx.clone();
        if let Some(delay) = delay {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx.send(event);
            });
        } else {
            let _ = self.watch_tx.send(event);
        }
    }

    /// Emit a ConfigMap watch event, possibly with delay
    fn emit_configmap_watch_event(
        &self,
        event: ConfigMapWatchBroadcastEvent,
        delay: Option<Duration>,
    ) {
        let tx = self.configmap_watch_tx.clone();
        if let Some(delay) = delay {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx.send(event);
            });
        } else {
            let _ = self.configmap_watch_tx.send(event);
        }
    }

    /// Get a lease by namespace and name
    pub async fn get_lease(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<StoredLease>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let leases = self.leases.lock().await;
        Ok(leases.get(namespace).and_then(|ns| ns.get(name)).cloned())
    }

    /// List leases in a namespace, optionally filtered by labels
    pub async fn list_leases(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<StoredLease>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let leases = self.leases.lock().await;
        let Some(ns_leases) = leases.get(namespace) else {
            return Ok(Vec::new());
        };

        let mut results: Vec<_> = ns_leases
            .values()
            .filter(|stored| match_labels(&stored.lease, label_selector))
            .cloned()
            .collect();
        // Sort by name for deterministic ordering in DST
        results.sort_by(|a, b| {
            let a_name = a.lease.metadata.name.as_deref().unwrap_or("");
            let b_name = b.lease.metadata.name.as_deref().unwrap_or("");
            a_name.cmp(b_name)
        });
        Ok(results)
    }

    /// Find the current holder of a shard lease (for client routing).
    /// Subject to failure injection like other K8s operations.
    pub async fn find_shard_owner(
        &self,
        namespace: &str,
        cluster_prefix: &str,
        shard_id: &silo::shard_range::ShardId,
    ) -> Result<Option<String>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let lease_name = format!("{}-shard-{}", cluster_prefix, shard_id.as_uuid());
        let leases = self.leases.lock().await;
        Ok(leases
            .get(namespace)
            .and_then(|ns| ns.get(&lease_name))
            .and_then(|stored| {
                stored
                    .lease
                    .spec
                    .as_ref()
                    .and_then(|s| s.holder_identity.as_ref())
                    .filter(|h| !h.is_empty())
                    .cloned()
            }))
    }

    /// Get the shard IDs from the cluster's shard map ConfigMap.
    /// Subject to failure injection like other K8s operations.
    pub async fn get_shard_ids(
        &self,
        namespace: &str,
        cluster_prefix: &str,
    ) -> Result<Option<Vec<silo::shard_range::ShardId>>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let configmap_name = format!("{}-shard-map", cluster_prefix);
        let configmaps = self.configmaps.lock().await;
        Ok(configmaps
            .get(namespace)
            .and_then(|ns| ns.get(&configmap_name))
            .and_then(|cm| cm.data.get("shard_map.json"))
            .and_then(|json| {
                serde_json::from_str::<silo::shard_range::ShardMap>(json)
                    .ok()
                    .map(|map| map.shards().iter().map(|s| s.id).collect())
            }))
    }

    /// Get the full shard map from the cluster's ConfigMap.
    /// Subject to failure injection like other K8s operations.
    #[allow(dead_code)]
    pub async fn get_shard_map(
        &self,
        namespace: &str,
        cluster_prefix: &str,
    ) -> Result<Option<silo::shard_range::ShardMap>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let configmap_name = format!("{}-shard-map", cluster_prefix);
        let configmaps = self.configmaps.lock().await;
        Ok(configmaps
            .get(namespace)
            .and_then(|ns| ns.get(&configmap_name))
            .and_then(|cm| cm.data.get("shard_map.json"))
            .and_then(|json| serde_json::from_str::<silo::shard_range::ShardMap>(json).ok()))
    }

    /// Create a new lease (returns error if already exists)
    pub async fn create_lease(
        &self,
        namespace: &str,
        mut lease: Lease,
    ) -> Result<StoredLease, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let name = lease
            .metadata
            .name
            .clone()
            .ok_or_else(|| CoordinationError::BackendError("name is required".into()))?;

        let mut leases = self.leases.lock().await;
        let ns_leases = leases.entry(namespace.to_string()).or_default();

        if ns_leases.contains_key(&name) {
            return Err(CoordinationError::BackendError(format!(
                "lease {} already exists (conflict)",
                name
            )));
        }

        let rv = self.next_resource_version();
        let uid = self.generate_uid();

        lease.metadata.resource_version = Some(rv.clone());
        lease.metadata.uid = Some(uid.clone());
        lease.metadata.namespace = Some(namespace.to_string());
        lease.metadata.creation_timestamp = Some(
            k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(Utc::now()),
        );

        let stored = StoredLease {
            lease: lease.clone(),
            resource_version: rv,
            uid,
        };
        ns_leases.insert(name.clone(), stored.clone());

        // Get delay before dropping the lock
        let delay = self.get_watch_delay().await;
        self.emit_lease_watch_event(
            WatchEvent {
                event_type: WatchEventType::Added,
                namespace: namespace.to_string(),
                name: name.clone(),
                object: lease,
            },
            delay,
        );

        trace!(namespace, name = %name, "mock: created lease");
        Ok(stored)
    }

    /// Replace a lease (CAS via resourceVersion)
    pub async fn replace_lease(
        &self,
        namespace: &str,
        name: &str,
        mut lease: Lease,
    ) -> Result<StoredLease, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut leases = self.leases.lock().await;
        let ns_leases = leases.entry(namespace.to_string()).or_default();

        let existing = ns_leases
            .get(name)
            .ok_or_else(|| CoordinationError::BackendError(format!("lease {} not found", name)))?;

        // Check resourceVersion for CAS
        if let Some(provided_rv) = &lease.metadata.resource_version {
            if provided_rv != &existing.resource_version {
                return Err(CoordinationError::BackendError(format!(
                    "CAS conflict: resourceVersion mismatch (provided {} != current {})",
                    provided_rv, existing.resource_version
                )));
            }
        }

        let new_rv = self.next_resource_version();

        lease.metadata.resource_version = Some(new_rv.clone());
        lease.metadata.uid = existing.lease.metadata.uid.clone();
        lease.metadata.namespace = Some(namespace.to_string());
        lease.metadata.creation_timestamp = existing.lease.metadata.creation_timestamp.clone();

        let stored = StoredLease {
            lease: lease.clone(),
            resource_version: new_rv,
            uid: existing.uid.clone(),
        };
        ns_leases.insert(name.to_string(), stored.clone());

        // Get delay before dropping the lock
        let delay = self.get_watch_delay().await;
        self.emit_lease_watch_event(
            WatchEvent {
                event_type: WatchEventType::Modified,
                namespace: namespace.to_string(),
                name: name.to_string(),
                object: lease,
            },
            delay,
        );

        trace!(namespace, name, "mock: replaced lease");
        Ok(stored)
    }

    /// Apply/Patch a lease (create or update with server-side apply semantics)
    pub async fn apply_lease(
        &self,
        namespace: &str,
        name: &str,
        mut lease: Lease,
    ) -> Result<StoredLease, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut leases = self.leases.lock().await;
        let ns_leases = leases.entry(namespace.to_string()).or_default();

        let new_rv = self.next_resource_version();

        let (stored, event_type) = if let Some(existing) = ns_leases.get(name) {
            // Update existing - merge the spec
            let mut updated = existing.lease.clone();
            updated.spec = lease.spec.clone();
            updated.metadata.resource_version = Some(new_rv.clone());

            // Merge labels
            if let Some(new_labels) = lease.metadata.labels {
                let labels = updated.metadata.labels.get_or_insert_with(Default::default);
                labels.extend(new_labels);
            }

            // Merge annotations
            if let Some(new_annotations) = lease.metadata.annotations {
                let annotations = updated
                    .metadata
                    .annotations
                    .get_or_insert_with(Default::default);
                annotations.extend(new_annotations);
            }

            let stored = StoredLease {
                lease: updated.clone(),
                resource_version: new_rv,
                uid: existing.uid.clone(),
            };
            (stored, WatchEventType::Modified)
        } else {
            // Create new
            let uid = self.generate_uid();
            lease.metadata.resource_version = Some(new_rv.clone());
            lease.metadata.uid = Some(uid.clone());
            lease.metadata.namespace = Some(namespace.to_string());
            lease.metadata.name = Some(name.to_string());
            lease.metadata.creation_timestamp = Some(
                k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(Utc::now()),
            );

            let stored = StoredLease {
                lease: lease.clone(),
                resource_version: new_rv,
                uid,
            };
            (stored, WatchEventType::Added)
        };

        ns_leases.insert(name.to_string(), stored.clone());

        // Get delay before dropping the lock
        let delay = self.get_watch_delay().await;
        self.emit_lease_watch_event(
            WatchEvent {
                event_type,
                namespace: namespace.to_string(),
                name: name.to_string(),
                object: stored.lease.clone(),
            },
            delay,
        );

        trace!(namespace, name, event = ?event_type, "mock: applied lease");
        Ok(stored)
    }

    /// Delete a lease
    pub async fn delete_lease(&self, namespace: &str, name: &str) -> Result<(), CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut leases = self.leases.lock().await;
        let ns_leases = leases.entry(namespace.to_string()).or_default();

        if let Some(stored) = ns_leases.remove(name) {
            // Get delay before dropping the lock
            let delay = self.get_watch_delay().await;
            self.emit_lease_watch_event(
                WatchEvent {
                    event_type: WatchEventType::Deleted,
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    object: stored.lease,
                },
                delay,
            );
            trace!(namespace, name, "mock: deleted lease");
            Ok(())
        } else {
            // Not found is ok for delete (idempotent)
            Ok(())
        }
    }

    /// Subscribe to Lease watch events
    pub fn subscribe_watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.watch_tx.subscribe()
    }

    /// Subscribe to ConfigMap watch events
    pub fn subscribe_configmap_watch(&self) -> broadcast::Receiver<ConfigMapWatchBroadcastEvent> {
        self.configmap_watch_tx.subscribe()
    }

    /// Set failure rate for testing (0.0 - 1.0)
    pub async fn set_failure_rate(&self, rate: f64) {
        *self.failure_rate.lock().await = rate.clamp(0.0, 1.0);
    }

    /// Force-release a shard lease by clearing its holderIdentity.
    /// Simulates an operator action or automated recovery clearing a crashed node's lease.
    /// Bypasses failure injection so this always succeeds.
    pub async fn force_release_shard_lease(
        &self,
        namespace: &str,
        cluster_prefix: &str,
        shard_id: &silo::shard_range::ShardId,
    ) -> Result<(), CoordinationError> {
        let lease_name = format!("{}-shard-{}", cluster_prefix, shard_id.as_uuid());

        let mut leases = self.leases.lock().await;
        let ns_leases = leases.entry(namespace.to_string()).or_default();

        let existing = ns_leases
            .get(&lease_name)
            .ok_or_else(|| {
                CoordinationError::BackendError(format!("lease {} not found", lease_name))
            })?
            .clone();

        let new_rv = self.next_resource_version();

        let mut updated_lease = existing.lease.clone();
        if let Some(spec) = updated_lease.spec.as_mut() {
            spec.holder_identity = Some(String::new());
            spec.renew_time = Some(MicroTime(Utc::now()));
        }
        updated_lease.metadata.resource_version = Some(new_rv.clone());

        let stored = StoredLease {
            lease: updated_lease.clone(),
            resource_version: new_rv,
            uid: existing.uid.clone(),
        };
        ns_leases.insert(lease_name.clone(), stored);

        let delay = self.get_watch_delay().await;
        self.emit_lease_watch_event(
            WatchEvent {
                event_type: WatchEventType::Modified,
                namespace: namespace.to_string(),
                name: lease_name.clone(),
                object: updated_lease,
            },
            delay,
        );

        trace!(namespace, name = %lease_name, "mock: force-released shard lease");
        Ok(())
    }

    /// Get all shard lease holders for a cluster prefix.
    /// Returns a map of shard_id -> holder node_id (only non-empty holders).
    /// Bypasses failure injection.
    pub async fn get_shard_holders(
        &self,
        namespace: &str,
        cluster_prefix: &str,
    ) -> std::collections::HashMap<silo::shard_range::ShardId, String> {
        let label_selector = format!("silo.dev/type=shard,silo.dev/cluster={}", cluster_prefix);
        let leases = self.leases.lock().await;
        let Some(ns_leases) = leases.get(namespace) else {
            return std::collections::HashMap::new();
        };

        let mut result = std::collections::HashMap::new();
        let prefix = format!("{}-shard-", cluster_prefix);
        for stored in ns_leases.values() {
            if !match_labels(&stored.lease, Some(&label_selector)) {
                continue;
            }
            if let Some(holder) = stored
                .lease
                .spec
                .as_ref()
                .and_then(|s| s.holder_identity.as_ref())
                .filter(|h| !h.is_empty())
                && let Some(name) = stored.lease.metadata.name.as_ref()
                && let Some(id_part) = name.strip_prefix(&prefix)
                && let Ok(uuid) = uuid::Uuid::parse_str(id_part)
            {
                result.insert(silo::shard_range::ShardId::from_uuid(uuid), holder.clone());
            }
        }
        result
    }

    /// Get a ConfigMap by namespace and name
    pub async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<StoredConfigMap>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let configmaps = self.configmaps.lock().await;
        Ok(configmaps
            .get(namespace)
            .and_then(|ns| ns.get(name))
            .cloned())
    }

    /// List ConfigMaps in a namespace, optionally filtered by labels
    pub async fn list_configmaps(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<StoredConfigMap>, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let configmaps = self.configmaps.lock().await;
        let Some(ns_configmaps) = configmaps.get(namespace) else {
            return Ok(Vec::new());
        };

        let mut results: Vec<_> = ns_configmaps
            .values()
            .filter(|stored| match_configmap_labels(stored, label_selector))
            .cloned()
            .collect();
        // Sort by name for deterministic ordering in DST
        results.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(results)
    }

    /// Create a new ConfigMap (returns error if already exists)
    pub async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        data: HashMap<String, String>,
        labels: HashMap<String, String>,
    ) -> Result<StoredConfigMap, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut configmaps = self.configmaps.lock().await;
        let ns_configmaps = configmaps.entry(namespace.to_string()).or_default();

        if ns_configmaps.contains_key(name) {
            return Err(CoordinationError::BackendError(format!(
                "configmap {} already exists (conflict)",
                name
            )));
        }

        let rv = self.next_resource_version();
        let uid = self.generate_uid();

        let stored = StoredConfigMap {
            name: name.to_string(),
            namespace: namespace.to_string(),
            data,
            labels,
            resource_version: rv,
            uid,
        };
        ns_configmaps.insert(name.to_string(), stored.clone());

        // Emit watch event with delay
        let configmap = stored_configmap_to_configmap_internal(&stored);
        let delay = self.get_watch_delay().await;
        self.emit_configmap_watch_event(
            ConfigMapWatchBroadcastEvent {
                event_type: WatchEventType::Added,
                namespace: namespace.to_string(),
                name: name.to_string(),
                object: configmap,
            },
            delay,
        );

        trace!(namespace, name, "mock: created configmap");
        Ok(stored)
    }

    /// Replace a ConfigMap (CAS via resourceVersion)
    pub async fn replace_configmap(
        &self,
        namespace: &str,
        name: &str,
        data: HashMap<String, String>,
        expected_rv: &str,
    ) -> Result<StoredConfigMap, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut configmaps = self.configmaps.lock().await;
        let ns_configmaps = configmaps.entry(namespace.to_string()).or_default();

        let existing = ns_configmaps.get(name).ok_or_else(|| {
            CoordinationError::BackendError(format!("configmap {} not found", name))
        })?;

        // Check resourceVersion for CAS
        if expected_rv != existing.resource_version {
            return Err(CoordinationError::BackendError(format!(
                "CAS conflict: resourceVersion mismatch (provided {} != current {})",
                expected_rv, existing.resource_version
            )));
        }

        let new_rv = self.next_resource_version();

        let stored = StoredConfigMap {
            name: name.to_string(),
            namespace: namespace.to_string(),
            data,
            labels: existing.labels.clone(),
            resource_version: new_rv,
            uid: existing.uid.clone(),
        };
        ns_configmaps.insert(name.to_string(), stored.clone());

        // Emit watch event with delay
        let configmap = stored_configmap_to_configmap_internal(&stored);
        let delay = self.get_watch_delay().await;
        self.emit_configmap_watch_event(
            ConfigMapWatchBroadcastEvent {
                event_type: WatchEventType::Modified,
                namespace: namespace.to_string(),
                name: name.to_string(),
                object: configmap,
            },
            delay,
        );

        trace!(namespace, name, "mock: replaced configmap");
        Ok(stored)
    }

    /// Apply/Patch a ConfigMap (create or update with server-side apply semantics)
    pub async fn apply_configmap(
        &self,
        namespace: &str,
        name: &str,
        data: HashMap<String, String>,
        labels: HashMap<String, String>,
    ) -> Result<StoredConfigMap, CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut configmaps = self.configmaps.lock().await;
        let ns_configmaps = configmaps.entry(namespace.to_string()).or_default();

        let new_rv = self.next_resource_version();

        let is_update = ns_configmaps.contains_key(name);
        let stored = if let Some(existing) = ns_configmaps.get(name) {
            // Update existing - merge data and labels
            let mut merged_data = existing.data.clone();
            merged_data.extend(data);
            let mut merged_labels = existing.labels.clone();
            merged_labels.extend(labels);

            StoredConfigMap {
                name: name.to_string(),
                namespace: namespace.to_string(),
                data: merged_data,
                labels: merged_labels,
                resource_version: new_rv,
                uid: existing.uid.clone(),
            }
        } else {
            // Create new
            let uid = self.generate_uid();
            StoredConfigMap {
                name: name.to_string(),
                namespace: namespace.to_string(),
                data,
                labels,
                resource_version: new_rv,
                uid,
            }
        };

        ns_configmaps.insert(name.to_string(), stored.clone());

        // Emit watch event with delay
        let configmap = stored_configmap_to_configmap_internal(&stored);
        let event_type = if is_update {
            WatchEventType::Modified
        } else {
            WatchEventType::Added
        };
        let delay = self.get_watch_delay().await;
        self.emit_configmap_watch_event(
            ConfigMapWatchBroadcastEvent {
                event_type,
                namespace: namespace.to_string(),
                name: name.to_string(),
                object: configmap,
            },
            delay,
        );

        trace!(namespace, name, "mock: applied configmap");
        Ok(stored)
    }

    /// Delete a ConfigMap
    pub async fn delete_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(), CoordinationError> {
        self.apply_latency().await;
        if self.should_fail().await {
            return Err(CoordinationError::BackendError(
                "simulated network failure".into(),
            ));
        }

        let mut configmaps = self.configmaps.lock().await;
        let ns_configmaps = configmaps.entry(namespace.to_string()).or_default();

        if let Some(stored) = ns_configmaps.remove(name) {
            // Emit watch event with delay
            let configmap = stored_configmap_to_configmap_internal(&stored);
            let delay = self.get_watch_delay().await;
            self.emit_configmap_watch_event(
                ConfigMapWatchBroadcastEvent {
                    event_type: WatchEventType::Deleted,
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    object: configmap,
                },
                delay,
            );
            trace!(namespace, name, "mock: deleted configmap");
        }
        // Not found is ok for delete (idempotent)
        Ok(())
    }
}

/// Check if a lease matches a label selector
fn match_labels(lease: &Lease, selector: Option<&str>) -> bool {
    let Some(selector) = selector else {
        return true;
    };

    let Some(labels) = &lease.metadata.labels else {
        return selector.is_empty();
    };

    // Parse simple label selector (e.g., "key=value,key2=value2")
    for part in selector.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((key, value)) = part.split_once('=') {
            if labels.get(key) != Some(&value.to_string()) {
                return false;
            }
        }
    }

    true
}

/// Check if a ConfigMap matches a label selector
fn match_configmap_labels(configmap: &StoredConfigMap, selector: Option<&str>) -> bool {
    let Some(selector) = selector else {
        return true;
    };

    if configmap.labels.is_empty() {
        return selector.is_empty();
    }

    // Parse simple label selector (e.g., "key=value,key2=value2")
    for part in selector.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((key, value)) = part.split_once('=') {
            if configmap.labels.get(key) != Some(&value.to_string()) {
                return false;
            }
        }
    }

    true
}

/// Wrapper around Arc<MockK8sState> that implements K8sBackend trait.
/// This allows using the mock state as a backend for the real K8sCoordinator.
#[derive(Clone)]
pub struct MockK8sBackend {
    pub state: Arc<MockK8sState>,
}

impl MockK8sBackend {
    pub fn new(state: Arc<MockK8sState>, _namespace: impl Into<String>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl K8sBackend for MockK8sBackend {
    async fn get_lease(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Lease>, CoordinationError> {
        let result = self.state.get_lease(namespace, name).await?;
        Ok(result.map(|stored| stored.lease))
    }

    async fn create_lease(
        &self,
        namespace: &str,
        lease: &Lease,
    ) -> Result<Lease, CoordinationError> {
        let stored = self.state.create_lease(namespace, lease.clone()).await?;
        Ok(stored.lease)
    }

    async fn replace_lease(
        &self,
        namespace: &str,
        name: &str,
        lease: &Lease,
    ) -> Result<Lease, CoordinationError> {
        let stored = self
            .state
            .replace_lease(namespace, name, lease.clone())
            .await?;
        Ok(stored.lease)
    }

    async fn list_leases(
        &self,
        namespace: &str,
        label_selector: &str,
    ) -> Result<Vec<Lease>, CoordinationError> {
        let selector = if label_selector.is_empty() {
            None
        } else {
            Some(label_selector)
        };
        let stored_leases = self.state.list_leases(namespace, selector).await?;
        Ok(stored_leases.into_iter().map(|s| s.lease).collect())
    }

    async fn delete_lease(&self, namespace: &str, name: &str) -> Result<(), CoordinationError> {
        self.state.delete_lease(namespace, name).await
    }

    async fn patch_lease_apply(
        &self,
        namespace: &str,
        name: &str,
        lease_json: serde_json::Value,
    ) -> Result<Lease, CoordinationError> {
        // Parse the JSON into a Lease struct
        let lease: Lease = serde_json::from_value(lease_json)
            .map_err(|e| CoordinationError::BackendError(format!("invalid lease JSON: {}", e)))?;
        let stored = self.state.apply_lease(namespace, name, lease).await?;
        Ok(stored.lease)
    }

    fn watch_leases(&self, namespace: &str, label_selector: &str) -> LeaseWatchStream {
        let mut watch_rx = self.state.subscribe_watch();
        let namespace = namespace.to_string();
        let label_selector = label_selector.to_string();

        Box::pin(async_stream::stream! {
            // Send InitDone immediately since the mock doesn't have real initial sync
            yield Ok(LeaseWatchEvent::InitDone);

            loop {
                match watch_rx.recv().await {
                    Ok(event) => {
                        // Filter by namespace
                        if event.namespace != namespace {
                            continue;
                        }

                        // Filter by label selector
                        if !match_labels(&event.object, Some(&label_selector)) {
                            continue;
                        }

                        // Convert to LeaseWatchEvent
                        let watch_event = match event.event_type {
                            WatchEventType::Added | WatchEventType::Modified => {
                                LeaseWatchEvent::Applied(event.object)
                            }
                            WatchEventType::Deleted => LeaseWatchEvent::Deleted(event.object),
                        };

                        yield Ok(watch_event);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        trace!("watch lagged by {} events", n);
                        // Continue watching after lag
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, end stream
                        break;
                    }
                }
            }
        })
    }

    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, CoordinationError> {
        let result = self.state.get_configmap(namespace, name).await?;
        Ok(result.map(stored_configmap_to_configmap))
    }

    async fn create_configmap(
        &self,
        namespace: &str,
        configmap: &ConfigMap,
    ) -> Result<ConfigMap, CoordinationError> {
        let name = configmap
            .metadata
            .name
            .as_ref()
            .ok_or_else(|| CoordinationError::BackendError("name is required".into()))?;
        let data: HashMap<String, String> = configmap
            .data
            .as_ref()
            .map(|d| d.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let labels: HashMap<String, String> = configmap
            .metadata
            .labels
            .as_ref()
            .map(|l| l.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let stored = self
            .state
            .create_configmap(namespace, name, data, labels)
            .await?;
        Ok(stored_configmap_to_configmap(stored))
    }

    async fn replace_configmap(
        &self,
        namespace: &str,
        name: &str,
        configmap: &ConfigMap,
    ) -> Result<ConfigMap, CoordinationError> {
        let expected_rv = configmap
            .metadata
            .resource_version
            .as_ref()
            .ok_or_else(|| CoordinationError::BackendError("resourceVersion is required".into()))?;
        let data: HashMap<String, String> = configmap
            .data
            .as_ref()
            .map(|d| d.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let stored = self
            .state
            .replace_configmap(namespace, name, data, expected_rv)
            .await?;
        Ok(stored_configmap_to_configmap(stored))
    }

    async fn list_configmaps(
        &self,
        namespace: &str,
        label_selector: &str,
    ) -> Result<Vec<ConfigMap>, CoordinationError> {
        let selector = if label_selector.is_empty() {
            None
        } else {
            Some(label_selector)
        };
        let stored_configmaps = self.state.list_configmaps(namespace, selector).await?;
        Ok(stored_configmaps
            .into_iter()
            .map(stored_configmap_to_configmap)
            .collect())
    }

    async fn delete_configmap(&self, namespace: &str, name: &str) -> Result<(), CoordinationError> {
        self.state.delete_configmap(namespace, name).await
    }

    async fn patch_configmap_apply(
        &self,
        namespace: &str,
        name: &str,
        configmap_json: serde_json::Value,
    ) -> Result<ConfigMap, CoordinationError> {
        // Parse the JSON into a ConfigMap struct
        let configmap: ConfigMap = serde_json::from_value(configmap_json).map_err(|e| {
            CoordinationError::BackendError(format!("invalid configmap JSON: {}", e))
        })?;
        let data: HashMap<String, String> = configmap
            .data
            .as_ref()
            .map(|d| d.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let labels: HashMap<String, String> = configmap
            .metadata
            .labels
            .as_ref()
            .map(|l| l.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let stored = self
            .state
            .apply_configmap(namespace, name, data, labels)
            .await?;
        Ok(stored_configmap_to_configmap(stored))
    }

    fn watch_configmap(&self, namespace: &str, name: &str) -> ConfigMapWatchStream {
        let mut watch_rx = self.state.subscribe_configmap_watch();
        let namespace = namespace.to_string();
        let name = name.to_string();

        Box::pin(async_stream::stream! {
            // Send InitDone immediately since the mock doesn't have real initial sync
            yield Ok(ConfigMapWatchEvent::InitDone);

            loop {
                match watch_rx.recv().await {
                    Ok(event) => {
                        // Filter by namespace and name
                        if event.namespace != namespace || event.name != name {
                            continue;
                        }

                        // Convert to ConfigMapWatchEvent
                        let watch_event = match event.event_type {
                            WatchEventType::Added | WatchEventType::Modified => {
                                ConfigMapWatchEvent::Applied(event.object)
                            }
                            WatchEventType::Deleted => ConfigMapWatchEvent::Deleted(event.object),
                        };

                        yield Ok(watch_event);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        trace!("configmap watch lagged by {} events", n);
                        // Continue watching after lag
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, end stream
                        break;
                    }
                }
            }
        })
    }
}

/// Convert a StoredConfigMap to a k8s ConfigMap (takes ownership)
fn stored_configmap_to_configmap(stored: StoredConfigMap) -> ConfigMap {
    stored_configmap_to_configmap_internal(&stored)
}

/// Convert a StoredConfigMap reference to a k8s ConfigMap
fn stored_configmap_to_configmap_internal(stored: &StoredConfigMap) -> ConfigMap {
    ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some(stored.name.clone()),
            namespace: Some(stored.namespace.clone()),
            resource_version: Some(stored.resource_version.clone()),
            uid: Some(stored.uid.clone()),
            labels: if stored.labels.is_empty() {
                None
            } else {
                Some(
                    stored
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                )
            },
            ..Default::default()
        },
        data: if stored.data.is_empty() {
            None
        } else {
            Some(
                stored
                    .data
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            )
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::coordination::v1::LeaseSpec;

    fn make_test_lease(name: &str, holder: Option<&str>) -> Lease {
        let mut lease = Lease::default();
        lease.metadata.name = Some(name.to_string());
        lease.metadata.labels = Some(
            [
                ("silo.dev/type".to_string(), "test".to_string()),
                ("silo.dev/cluster".to_string(), "test-cluster".to_string()),
            ]
            .into(),
        );
        lease.spec = Some(LeaseSpec {
            holder_identity: holder.map(|s| s.to_string()),
            lease_duration_seconds: Some(10),
            acquire_time: Some(MicroTime(Utc::now())),
            renew_time: Some(MicroTime(Utc::now())),
            ..Default::default()
        });
        lease
    }

    #[silo::test]
    async fn mock_k8s_create_lease() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let result = state.create_lease("default", lease).await;
        assert!(result.is_ok());

        let stored = result.unwrap();
        assert!(stored.lease.metadata.resource_version.is_some());
        assert!(stored.lease.metadata.uid.is_some());
        assert_eq!(stored.lease.metadata.name.as_deref(), Some("test-lease"));
    }

    #[silo::test]
    async fn mock_k8s_create_duplicate_fails() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let _ = state.create_lease("default", lease.clone()).await.unwrap();
        let result = state.create_lease("default", lease).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("conflict"));
    }

    #[silo::test]
    async fn mock_k8s_get_lease() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let _ = state.create_lease("default", lease).await.unwrap();

        let result = state.get_lease("default", "test-lease").await;
        assert!(result.is_ok());
        let stored = result.unwrap();
        assert!(stored.is_some());
        assert_eq!(
            stored.unwrap().lease.metadata.name.as_deref(),
            Some("test-lease")
        );
    }

    #[silo::test]
    async fn mock_k8s_get_nonexistent_returns_none() {
        let state = MockK8sState::new();

        let result = state.get_lease("default", "nonexistent").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[silo::test]
    async fn mock_k8s_replace_lease_with_cas() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let created = state.create_lease("default", lease).await.unwrap();

        // Update with correct resourceVersion
        let mut updated = created.lease.clone();
        updated.spec.as_mut().unwrap().holder_identity = Some("node-2".to_string());

        let result = state.replace_lease("default", "test-lease", updated).await;
        assert!(result.is_ok());

        let stored = result.unwrap();
        assert_eq!(
            stored
                .lease
                .spec
                .as_ref()
                .unwrap()
                .holder_identity
                .as_deref(),
            Some("node-2")
        );
        assert_ne!(stored.resource_version, created.resource_version);
    }

    #[silo::test]
    async fn mock_k8s_replace_with_wrong_rv_fails() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let _ = state.create_lease("default", lease.clone()).await.unwrap();

        // Try to update with wrong resourceVersion
        let mut updated = lease.clone();
        updated.metadata.resource_version = Some("wrong-version".to_string());
        updated.spec.as_mut().unwrap().holder_identity = Some("node-2".to_string());

        let result = state.replace_lease("default", "test-lease", updated).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CAS conflict"));
    }

    #[silo::test]
    async fn mock_k8s_list_leases_with_label_filter() {
        let state = MockK8sState::new();

        // Create leases with different labels
        let mut lease1 = make_test_lease("lease-1", Some("node-1"));
        lease1.metadata.labels = Some(
            [
                ("silo.dev/type".to_string(), "member".to_string()),
                ("silo.dev/cluster".to_string(), "test".to_string()),
            ]
            .into(),
        );

        let mut lease2 = make_test_lease("lease-2", Some("node-2"));
        lease2.metadata.labels = Some(
            [
                ("silo.dev/type".to_string(), "shard".to_string()),
                ("silo.dev/cluster".to_string(), "test".to_string()),
            ]
            .into(),
        );

        let _ = state.create_lease("default", lease1).await.unwrap();
        let _ = state.create_lease("default", lease2).await.unwrap();

        // Filter by type=member
        let members = state
            .list_leases("default", Some("silo.dev/type=member"))
            .await
            .unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].lease.metadata.name.as_deref(), Some("lease-1"));

        // Filter by type=shard
        let shards = state
            .list_leases("default", Some("silo.dev/type=shard"))
            .await
            .unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].lease.metadata.name.as_deref(), Some("lease-2"));

        // List all
        let all = state.list_leases("default", None).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[silo::test]
    async fn mock_k8s_delete_lease() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let _ = state.create_lease("default", lease).await.unwrap();

        let result = state.delete_lease("default", "test-lease").await;
        assert!(result.is_ok());

        // Verify deleted
        let get_result = state.get_lease("default", "test-lease").await.unwrap();
        assert!(get_result.is_none());
    }

    #[silo::test]
    async fn mock_k8s_delete_nonexistent_is_ok() {
        let state = MockK8sState::new();

        // Delete nonexistent lease should be ok (idempotent)
        let result = state.delete_lease("default", "nonexistent").await;
        assert!(result.is_ok());
    }

    #[silo::test]
    async fn mock_k8s_apply_creates_if_not_exists() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let result = state.apply_lease("default", "test-lease", lease).await;
        assert!(result.is_ok());

        let stored = state.get_lease("default", "test-lease").await.unwrap();
        assert!(stored.is_some());
    }

    #[silo::test]
    async fn mock_k8s_apply_updates_if_exists() {
        let state = MockK8sState::new();
        let lease = make_test_lease("test-lease", Some("node-1"));

        let _ = state.create_lease("default", lease).await.unwrap();

        let update = make_test_lease("test-lease", Some("node-2"));
        let result = state.apply_lease("default", "test-lease", update).await;
        assert!(result.is_ok());

        let stored = state
            .get_lease("default", "test-lease")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .lease
                .spec
                .as_ref()
                .unwrap()
                .holder_identity
                .as_deref(),
            Some("node-2")
        );
    }

    #[silo::test]
    async fn mock_k8s_watch_events_emitted() {
        let state = MockK8sState::new();
        // Disable watch event delay so events are sent synchronously
        *state.watch_event_delay.lock().await = None;
        let mut rx = state.subscribe_watch();

        let lease = make_test_lease("test-lease", Some("node-1"));
        let _ = state.create_lease("default", lease).await.unwrap();

        // Should receive ADDED event
        let event = rx.try_recv();
        assert!(event.is_ok());
        let event = event.unwrap();
        assert_eq!(event.event_type, WatchEventType::Added);
        assert_eq!(event.name, "test-lease");
    }

    #[silo::test]
    async fn mock_k8s_failure_rate_simulation() {
        let state = MockK8sState::new();
        state.set_failure_rate(1.0).await; // 100% failure rate

        let lease = make_test_lease("test-lease", Some("node-1"));
        let result = state.create_lease("default", lease).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("simulated"));
    }
}
