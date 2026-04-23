use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::debug;

use crate::error::CompactorError;
use crate::shard_loader::ShardMapLoader;
use crate::shard_map::ShardMap;

/// Reads silo's shard map from etcd at `{cluster_prefix}/coord/shard_map`.
/// Matches `silo::coordination::keys::shard_map_key` (silo/src/coordination/mod.rs:922).
pub struct EtcdShardMapLoader {
    cluster_prefix: String,
    client: Mutex<etcd_client::Client>,
}

impl EtcdShardMapLoader {
    pub async fn connect(
        endpoints: Vec<String>,
        cluster_prefix: String,
    ) -> Result<Self, CompactorError> {
        let client = etcd_client::Client::connect(&endpoints, None)
            .await
            .map_err(|e| CompactorError::Etcd(e.to_string()))?;
        Ok(Self {
            cluster_prefix,
            client: Mutex::new(client),
        })
    }

    fn key(&self) -> String {
        format!("{}/coord/shard_map", self.cluster_prefix)
    }
}

#[async_trait]
impl ShardMapLoader for EtcdShardMapLoader {
    async fn load(&self) -> Result<ShardMap, CompactorError> {
        let key = self.key();
        debug!(key = %key, "fetching shard map from etcd");
        let mut client = self.client.lock().await;
        let resp = client
            .get(key.clone(), None)
            .await
            .map_err(|e| CompactorError::Etcd(e.to_string()))?;
        let kv = resp.kvs().first().ok_or_else(|| {
            CompactorError::ShardLoader(format!("shard map key {} not found", key))
        })?;
        let value = std::str::from_utf8(kv.value())
            .map_err(|e| CompactorError::ShardLoader(format!("non-utf8 shard map value: {e}")))?;
        serde_json::from_str(value)
            .map_err(|e| CompactorError::ShardLoader(format!("invalid shard map JSON: {e}")))
    }
}
