use async_trait::async_trait;
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::ListParams;
use kube::runtime::watcher::{self, Event};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::CompactorError;

#[async_trait]
pub trait PodDiscovery: Send + Sync {
    async fn list_sibling_pod_names(&self) -> Result<Vec<String>, CompactorError>;
}

pub struct K8sPodDiscovery {
    api: Api<Pod>,
    label_selector: String,
}

impl K8sPodDiscovery {
    pub fn new(client: kube::Client, namespace: &str, label_selector: String) -> Self {
        let api = Api::namespaced(client, namespace);
        Self {
            api,
            label_selector,
        }
    }

    /// Spawn a watcher that emits `()` whenever a pod matching the selector is
    /// added, modified, or removed. Used to trigger an immediate reconcile
    /// without waiting for the next periodic tick.
    pub fn spawn_change_watcher(&self) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel(8);
        let api = self.api.clone();
        let cfg = watcher::Config::default().labels(&self.label_selector);
        tokio::spawn(async move {
            let mut s = std::pin::pin!(watcher::watcher(api, cfg));
            while let Some(ev) = s.next().await {
                match ev {
                    Ok(Event::Apply(_) | Event::Delete(_) | Event::InitDone) => {
                        let _ = tx.try_send(());
                    }
                    Ok(Event::Init | Event::InitApply(_)) => {}
                    Err(e) => warn!(error = %e, "pod watcher error"),
                }
            }
            debug!("pod watcher stream ended");
        });
        rx
    }
}

#[async_trait]
impl PodDiscovery for K8sPodDiscovery {
    async fn list_sibling_pod_names(&self) -> Result<Vec<String>, CompactorError> {
        let lp = ListParams::default().labels(&self.label_selector);
        let list = self.api.list(&lp).await?;
        Ok(list
            .items
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .filter_map(|p| p.metadata.name)
            .collect())
    }
}

/// Single-pod mode for local dev — always returns just self.
pub struct StaticPodDiscovery {
    pub self_name: String,
}

#[async_trait]
impl PodDiscovery for StaticPodDiscovery {
    async fn list_sibling_pod_names(&self) -> Result<Vec<String>, CompactorError> {
        Ok(vec![self.self_name.clone()])
    }
}
