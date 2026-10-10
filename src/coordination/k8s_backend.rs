//! Abstraction over Kubernetes API operations for testability.
//!
//! This module defines the `K8sBackend` trait which abstracts Kubernetes API operations,
//! allowing the K8sCoordinator to work with either a real Kubernetes cluster or a
//! simulated backend for deterministic testing.

use async_trait::async_trait;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::ConfigMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;

use crate::coordination::CoordinationError;

/// Watch event types for lease changes
#[derive(Debug, Clone)]
pub enum LeaseWatchEvent {
    /// A lease was added or modified
    Applied(Lease),
    /// A lease was deleted
    Deleted(Lease),
    /// Initial sync started
    Init,
    /// Initial sync completed
    InitDone,
}

/// A boxed stream of lease watch events
pub type LeaseWatchStream =
    Pin<Box<dyn Stream<Item = Result<LeaseWatchEvent, CoordinationError>> + Send>>;

/// Watch event types for ConfigMap changes
#[derive(Debug, Clone)]
pub enum ConfigMapWatchEvent {
    /// A ConfigMap was added or modified
    Applied(ConfigMap),
    /// A ConfigMap was deleted
    Deleted(ConfigMap),
    /// Initial sync started
    Init,
    /// Initial sync completed
    InitDone,
}

/// A boxed stream of ConfigMap watch events
pub type ConfigMapWatchStream =
    Pin<Box<dyn Stream<Item = Result<ConfigMapWatchEvent, CoordinationError>> + Send>>;

/// Trait abstracting Kubernetes API operations.
///
/// This allows the K8sCoordinator to work with either:
/// - `KubeBackend`: Real Kubernetes API via kube-rs
/// - Test implementations: Simulated K8s API for deterministic testing
#[async_trait]
pub trait K8sBackend: Send + Sync + Clone + 'static {
    /// Get a lease by namespace and name.
    /// Returns None if the lease doesn't exist.
    async fn get_lease(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Lease>, CoordinationError>;

    /// Create a new lease.
    /// Returns an error if the lease already exists.
    async fn create_lease(
        &self,
        namespace: &str,
        lease: &Lease,
    ) -> Result<Lease, CoordinationError>;

    /// Replace an existing lease using CAS semantics.
    /// The lease's resourceVersion must match the current version.
    async fn replace_lease(
        &self,
        namespace: &str,
        name: &str,
        lease: &Lease,
    ) -> Result<Lease, CoordinationError>;

    /// List leases matching the given label selector.
    async fn list_leases(
        &self,
        namespace: &str,
        label_selector: &str,
    ) -> Result<Vec<Lease>, CoordinationError>;

    /// Delete a lease by name.
    /// Not finding the lease is not an error (idempotent).
    async fn delete_lease(&self, namespace: &str, name: &str) -> Result<(), CoordinationError>;

    /// Apply a lease using server-side apply semantics.
    /// Creates the lease if it doesn't exist, or updates it if it does.
    async fn patch_lease_apply(
        &self,
        namespace: &str,
        name: &str,
        lease_json: serde_json::Value,
    ) -> Result<Lease, CoordinationError>;

    /// Watch for lease changes matching the given label selector.
    /// Returns a stream of watch events.
    fn watch_leases(&self, namespace: &str, label_selector: &str) -> LeaseWatchStream;

    /// Get a ConfigMap by namespace and name.
    /// Returns None if the ConfigMap doesn't exist.
    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, CoordinationError>;

    /// Create a new ConfigMap.
    /// Returns an error if the ConfigMap already exists.
    async fn create_configmap(
        &self,
        namespace: &str,
        configmap: &ConfigMap,
    ) -> Result<ConfigMap, CoordinationError>;

    /// Replace an existing ConfigMap using CAS semantics.
    /// The ConfigMap's resourceVersion must match the current version.
    async fn replace_configmap(
        &self,
        namespace: &str,
        name: &str,
        configmap: &ConfigMap,
    ) -> Result<ConfigMap, CoordinationError>;

    /// List ConfigMaps matching the given label selector.
    async fn list_configmaps(
        &self,
        namespace: &str,
        label_selector: &str,
    ) -> Result<Vec<ConfigMap>, CoordinationError>;

    /// Delete a ConfigMap by name.
    /// Not finding the ConfigMap is not an error (idempotent).
    async fn delete_configmap(&self, namespace: &str, name: &str) -> Result<(), CoordinationError>;

    /// Apply a ConfigMap using server-side apply semantics.
    /// Creates the ConfigMap if it doesn't exist, or updates it if it does.
    async fn patch_configmap_apply(
        &self,
        namespace: &str,
        name: &str,
        configmap_json: serde_json::Value,
    ) -> Result<ConfigMap, CoordinationError>;

    /// Watch a specific ConfigMap by name.
    /// Returns a stream of watch events for changes to the named ConfigMap.
    fn watch_configmap(&self, namespace: &str, name: &str) -> ConfigMapWatchStream;
}

/// Real Kubernetes backend using kube-rs client.
///
/// One-shot calls (get, list, create, replace, patch, delete) go through
/// `client`. Each watch stream gets a `kube::Client` of its own, built from
/// `config`, and so a connection pool of its own.
///
/// The separation matters because kube-rs talks HTTP/1.1, where a connection
/// carries one response at a time and a watch holds its connection for the
/// whole watch (290s by default). A race in hyper's connection dispatch can
/// leave a connection marked ready while a response is still streaming on it,
/// and the pool then hands that connection to the next request, which is only
/// written once the stream ends. With one shared pool that next request can
/// be a membership renewal queued behind a watch. With a pool per watch
/// stream, nothing else can be queued behind a watch.
#[derive(Clone)]
pub struct KubeBackend {
    client: kube::Client,
    config: kube::Config,
}

impl KubeBackend {
    /// Create a new KubeBackend that connects with the given configuration.
    pub fn new(config: kube::Config) -> Result<Self, CoordinationError> {
        let client = Self::build_client(&config)?;
        Ok(Self { client, config })
    }

    /// Create a new KubeBackend using the default in-cluster or kubeconfig configuration.
    pub async fn try_default() -> Result<Self, CoordinationError> {
        let config = kube::Config::infer()
            .await
            .map_err(|e| CoordinationError::ConnectionFailed(e.to_string()))?;
        Self::new(config)
    }

    fn build_client(config: &kube::Config) -> Result<kube::Client, CoordinationError> {
        kube::Client::try_from(config.clone())
            .map_err(|e| CoordinationError::ConnectionFailed(e.to_string()))
    }
}

/// A watch stream that reports `error` and ends, for a watch that could not
/// be started.
fn failed_watch<T: Send + 'static>(
    error: CoordinationError,
) -> Pin<Box<dyn Stream<Item = Result<T, CoordinationError>> + Send>> {
    Box::pin(tokio_stream::once(Err(error)))
}

#[async_trait]
impl K8sBackend for KubeBackend {
    async fn get_lease(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Lease>, CoordinationError> {
        let leases: kube::Api<Lease> = kube::Api::namespaced(self.client.clone(), namespace);
        match leases.get(name).await {
            Ok(lease) => Ok(Some(lease)),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn create_lease(
        &self,
        namespace: &str,
        lease: &Lease,
    ) -> Result<Lease, CoordinationError> {
        let leases: kube::Api<Lease> = kube::Api::namespaced(self.client.clone(), namespace);
        leases
            .create(&kube::api::PostParams::default(), lease)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))
    }

    async fn replace_lease(
        &self,
        namespace: &str,
        name: &str,
        lease: &Lease,
    ) -> Result<Lease, CoordinationError> {
        let leases: kube::Api<Lease> = kube::Api::namespaced(self.client.clone(), namespace);
        match leases
            .replace(name, &kube::api::PostParams::default(), lease)
            .await
        {
            Ok(l) => Ok(l),
            Err(kube::Error::Api(e)) if e.code == 409 => Err(CoordinationError::BackendError(
                "CAS conflict: resourceVersion mismatch".into(),
            )),
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn list_leases(
        &self,
        namespace: &str,
        label_selector: &str,
    ) -> Result<Vec<Lease>, CoordinationError> {
        let leases: kube::Api<Lease> = kube::Api::namespaced(self.client.clone(), namespace);
        let lp = kube::api::ListParams::default().labels(label_selector);
        let list = leases
            .list(&lp)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;
        Ok(list.items)
    }

    async fn delete_lease(&self, namespace: &str, name: &str) -> Result<(), CoordinationError> {
        let leases: kube::Api<Lease> = kube::Api::namespaced(self.client.clone(), namespace);
        match leases.delete(name, &Default::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()), // Already deleted
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn patch_lease_apply(
        &self,
        namespace: &str,
        name: &str,
        lease_json: serde_json::Value,
    ) -> Result<Lease, CoordinationError> {
        let leases: kube::Api<Lease> = kube::Api::namespaced(self.client.clone(), namespace);
        leases
            .patch(
                name,
                &kube::api::PatchParams::apply("silo-coordinator").force(),
                &kube::api::Patch::Apply(&lease_json),
            )
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))
    }

    fn watch_leases(&self, namespace: &str, label_selector: &str) -> LeaseWatchStream {
        use futures::StreamExt;
        use kube::runtime::watcher::{Config as WatcherConfig, Event, watcher};

        let client = match Self::build_client(&self.config) {
            Ok(client) => client,
            Err(e) => return failed_watch(e),
        };
        let leases: kube::Api<Lease> = kube::Api::namespaced(client, namespace);
        let watcher_config = WatcherConfig::default().labels(label_selector);
        let watch_stream = watcher(leases, watcher_config);

        Box::pin(watch_stream.map(|result| {
            result
                .map(|ev| match ev {
                    Event::Apply(lease) | Event::InitApply(lease) => {
                        LeaseWatchEvent::Applied(lease)
                    }
                    Event::Delete(lease) => LeaseWatchEvent::Deleted(lease),
                    Event::Init => LeaseWatchEvent::Init,
                    Event::InitDone => LeaseWatchEvent::InitDone,
                })
                .map_err(|e| CoordinationError::BackendError(e.to_string()))
        }))
    }

    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, CoordinationError> {
        let configmaps: kube::Api<ConfigMap> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match configmaps.get(name).await {
            Ok(cm) => Ok(Some(cm)),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn create_configmap(
        &self,
        namespace: &str,
        configmap: &ConfigMap,
    ) -> Result<ConfigMap, CoordinationError> {
        let configmaps: kube::Api<ConfigMap> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match configmaps
            .create(&kube::api::PostParams::default(), configmap)
            .await
        {
            Ok(cm) => Ok(cm),
            Err(kube::Error::Api(e)) if e.code == 409 => Err(CoordinationError::BackendError(
                "ConfigMap already exists (conflict)".into(),
            )),
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn replace_configmap(
        &self,
        namespace: &str,
        name: &str,
        configmap: &ConfigMap,
    ) -> Result<ConfigMap, CoordinationError> {
        let configmaps: kube::Api<ConfigMap> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match configmaps
            .replace(name, &kube::api::PostParams::default(), configmap)
            .await
        {
            Ok(cm) => Ok(cm),
            Err(kube::Error::Api(e)) if e.code == 409 => Err(CoordinationError::BackendError(
                "CAS conflict: resourceVersion mismatch".into(),
            )),
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn list_configmaps(
        &self,
        namespace: &str,
        label_selector: &str,
    ) -> Result<Vec<ConfigMap>, CoordinationError> {
        let configmaps: kube::Api<ConfigMap> =
            kube::Api::namespaced(self.client.clone(), namespace);
        let lp = kube::api::ListParams::default().labels(label_selector);
        let list = configmaps
            .list(&lp)
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))?;
        Ok(list.items)
    }

    async fn delete_configmap(&self, namespace: &str, name: &str) -> Result<(), CoordinationError> {
        let configmaps: kube::Api<ConfigMap> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match configmaps.delete(name, &Default::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()), // Already deleted
            Err(e) => Err(CoordinationError::BackendError(e.to_string())),
        }
    }

    async fn patch_configmap_apply(
        &self,
        namespace: &str,
        name: &str,
        configmap_json: serde_json::Value,
    ) -> Result<ConfigMap, CoordinationError> {
        let configmaps: kube::Api<ConfigMap> =
            kube::Api::namespaced(self.client.clone(), namespace);
        configmaps
            .patch(
                name,
                &kube::api::PatchParams::apply("silo-coordinator").force(),
                &kube::api::Patch::Apply(&configmap_json),
            )
            .await
            .map_err(|e| CoordinationError::BackendError(e.to_string()))
    }

    fn watch_configmap(&self, namespace: &str, name: &str) -> ConfigMapWatchStream {
        use futures::StreamExt;
        use kube::runtime::watcher::{Config as WatcherConfig, Event, watcher};

        let client = match Self::build_client(&self.config) {
            Ok(client) => client,
            Err(e) => return failed_watch(e),
        };
        let configmaps: kube::Api<ConfigMap> = kube::Api::namespaced(client, namespace);
        // Use field selector to watch only the specific ConfigMap by name
        let watcher_config = WatcherConfig::default().fields(&format!("metadata.name={}", name));
        let watch_stream = watcher(configmaps, watcher_config);

        Box::pin(watch_stream.map(|result| {
            result
                .map(|ev| match ev {
                    Event::Apply(cm) | Event::InitApply(cm) => ConfigMapWatchEvent::Applied(cm),
                    Event::Delete(cm) => ConfigMapWatchEvent::Deleted(cm),
                    Event::Init => ConfigMapWatchEvent::Init,
                    Event::InitDone => ConfigMapWatchEvent::InitDone,
                })
                .map_err(|e| CoordinationError::BackendError(e.to_string()))
        }))
    }
}

/// Helper to wrap a K8sBackend in an Arc for sharing.
pub type SharedK8sBackend<B> = Arc<B>;
