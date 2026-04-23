use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::Api;
use tracing::debug;

use crate::error::CompactorError;
use crate::shard_loader::ShardMapLoader;
use crate::shard_map::ShardMap;

/// Reads silo's shard map from a k8s ConfigMap. The ConfigMap is named
/// `{cluster_prefix}-shard-map` and stores the JSON under the `shard_map.json`
/// key. Mirrors the format silo writes in `silo::coordination::k8s` (around
/// silo/src/coordination/k8s.rs:231-340).
pub struct K8sConfigMapShardMapLoader {
    api: Api<ConfigMap>,
    name: String,
}

impl K8sConfigMapShardMapLoader {
    pub fn new(client: kube::Client, namespace: &str, cluster_prefix: &str) -> Self {
        let api = Api::namespaced(client, namespace);
        let name = format!("{}-shard-map", cluster_prefix);
        Self { api, name }
    }
}

#[async_trait]
impl ShardMapLoader for K8sConfigMapShardMapLoader {
    async fn load(&self) -> Result<ShardMap, CompactorError> {
        debug!(configmap = %self.name, "fetching shard map from k8s ConfigMap");
        let cm = self.api.get(&self.name).await?;
        let json = cm
            .data
            .as_ref()
            .and_then(|d| d.get("shard_map.json"))
            .ok_or_else(|| {
                CompactorError::ShardLoader(format!(
                    "ConfigMap {} missing shard_map.json key",
                    self.name
                ))
            })?;
        serde_json::from_str(json)
            .map_err(|e| CompactorError::ShardLoader(format!("invalid shard map JSON: {e}")))
    }
}
