use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::assignment::compute_assignment;
use crate::config::{AppConfig, CompactionFilterConfig, CoordinatorMode, ShardDiscoveryBackend};
use crate::error::CompactorError;
use crate::pod_discovery::{K8sPodDiscovery, PodDiscovery, StaticPodDiscovery};
use crate::shard_loader::ShardMapLoader;
use crate::shard_loader::etcd::EtcdShardMapLoader;
use crate::shard_loader::k8s::K8sConfigMapShardMapLoader;
use crate::shard_map::ShardId;
use crate::storage::Backend;
use crate::worker::{WorkerHandle, spawn_worker};

pub struct Coordinator {
    self_pod_name: String,
    backend: Backend,
    path_template: Arc<String>,
    compactor_options: Arc<Option<slatedb::config::CompactorOptions>>,
    filter_config: Arc<CompactionFilterConfig>,
    discovery: Arc<dyn PodDiscovery>,
    shard_loader: Arc<dyn ShardMapLoader>,
    placement_ring: Option<String>,
    max_shards: usize,
    rebalance_interval: Duration,
    worker_backoff: Duration,
    workers: HashMap<ShardId, WorkerHandle>,
    pod_change_rx: Option<mpsc::Receiver<()>>,
}

impl Coordinator {
    pub async fn from_config(cfg: AppConfig) -> Result<Self, CompactorError> {
        let self_pod_name = cfg
            .resolve_self_pod_name()
            .map_err(|e| CompactorError::Config(e.to_string()))?;
        info!(self_pod_name = %self_pod_name, "resolved self pod name");

        let backend = cfg.storage.backend.clone();
        let path_template = Arc::new(cfg.storage.path.clone());
        let compactor_options = Arc::new(cfg.compactor_options.clone());
        let filter_config = Arc::new(cfg.compaction_filter.clone());

        let needs_kube = matches!(cfg.coordinator.mode, CoordinatorMode::K8sDeployment)
            || matches!(
                cfg.shard_discovery.backend,
                ShardDiscoveryBackend::K8sConfigmap
            );
        let kube_client = if needs_kube {
            Some(kube::Client::try_default().await?)
        } else {
            None
        };

        let namespace = cfg
            .coordinator
            .namespace
            .clone()
            .filter(|n| !n.is_empty())
            .or_else(read_sa_namespace)
            .unwrap_or_else(|| "default".into());

        let (discovery, pod_change_rx): (Arc<dyn PodDiscovery>, _) = match cfg.coordinator.mode {
            CoordinatorMode::K8sDeployment => {
                let client = kube_client.clone().ok_or_else(|| {
                    CompactorError::Config("k8s_deployment mode requires kube client".into())
                })?;
                let d = K8sPodDiscovery::new(
                    client,
                    &namespace,
                    cfg.coordinator.pod_label_selector.clone(),
                );
                let rx = d.spawn_change_watcher();
                (Arc::new(d), Some(rx))
            }
            CoordinatorMode::Single => (
                Arc::new(StaticPodDiscovery {
                    self_name: self_pod_name.clone(),
                }),
                None,
            ),
        };

        let shard_loader: Arc<dyn ShardMapLoader> = match cfg.shard_discovery.backend {
            ShardDiscoveryBackend::Etcd => {
                if cfg.shard_discovery.etcd_endpoints.is_empty() {
                    return Err(CompactorError::Config(
                        "etcd backend requires shard_discovery.etcd_endpoints".into(),
                    ));
                }
                Arc::new(
                    EtcdShardMapLoader::connect(
                        cfg.shard_discovery.etcd_endpoints.clone(),
                        cfg.shard_discovery.cluster_prefix.clone(),
                    )
                    .await?,
                )
            }
            ShardDiscoveryBackend::K8sConfigmap => {
                let client = kube_client.ok_or_else(|| {
                    CompactorError::Config("k8s_configmap backend requires kube client".into())
                })?;
                Arc::new(K8sConfigMapShardMapLoader::new(
                    client,
                    &cfg.shard_discovery.k8s_namespace,
                    &cfg.shard_discovery.cluster_prefix,
                ))
            }
        };

        Ok(Self {
            self_pod_name,
            backend,
            path_template,
            compactor_options,
            filter_config,
            discovery,
            shard_loader,
            placement_ring: cfg.shard_discovery.placement_ring,
            max_shards: cfg.coordinator.max_shards_per_pod,
            rebalance_interval: cfg.coordinator.rebalance_interval,
            worker_backoff: cfg.coordinator.worker_restart_backoff,
            workers: HashMap::new(),
            pod_change_rx,
        })
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), CompactorError> {
        let mut interval = tokio::time::interval(self.rebalance_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        if let Err(e) = self.reconcile().await {
            warn!(error = %e, "initial reconcile failed; will retry");
        }

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.reconcile().await {
                        warn!(error = %e, "reconcile failed, will retry next interval");
                    }
                }
                Some(()) = recv_pod_change(&mut self.pod_change_rx) => {
                    debug!("pod set changed, reconciling");
                    if let Err(e) = self.reconcile().await {
                        warn!(error = %e, "reconcile after pod change failed");
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("shutdown signaled");
                        break;
                    }
                }
            }
        }
        self.shutdown_all().await;
        Ok(())
    }

    async fn reconcile(&mut self) -> Result<(), CompactorError> {
        let pods = self.discovery.list_sibling_pod_names().await?;
        let shard_map = self.shard_loader.load().await?;
        let shards = shard_map.shard_ids(self.placement_ring.as_deref());

        let assignment = compute_assignment(
            &self.self_pod_name,
            pods.clone(),
            shards.clone(),
            self.max_shards,
        );
        let desired: HashSet<ShardId> = assignment.iter().copied().collect();
        let current: HashSet<ShardId> = self.workers.keys().copied().collect();

        let to_stop: Vec<ShardId> = current.difference(&desired).copied().collect();
        for shard in to_stop {
            if let Some(handle) = self.workers.remove(&shard) {
                info!(shard = %shard, "stopping worker (no longer assigned)");
                handle.shutdown().await;
            }
        }

        for shard in desired.difference(&current) {
            info!(shard = %shard, "starting worker");
            let handle = spawn_worker(
                *shard,
                self.backend.clone(),
                Arc::clone(&self.path_template),
                Arc::clone(&self.compactor_options),
                Arc::clone(&self.filter_config),
                self.worker_backoff,
            );
            self.workers.insert(*shard, handle);
        }

        info!(
            assigned = self.workers.len(),
            cap = self.max_shards,
            total_shards = shards.len(),
            sibling_pods = pods.len(),
            "reconcile complete",
        );
        Ok(())
    }

    async fn shutdown_all(&mut self) {
        let workers = std::mem::take(&mut self.workers);
        info!(count = workers.len(), "shutting down all workers");
        let futures: Vec<_> = workers.into_values().map(|w| w.shutdown()).collect();
        futures::future::join_all(futures).await;
    }
}

async fn recv_pod_change(rx: &mut Option<mpsc::Receiver<()>>) -> Option<()> {
    match rx.as_mut() {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

fn read_sa_namespace() -> Option<String> {
    std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace").ok()
}
