use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::gubernator::RateLimitClient;
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::metrics::Metrics;
use crate::settings::{DatabaseConfig, DatabaseTemplate, WalConfig};
use crate::shard_range::ShardId;
use thiserror::Error;

/// Factory for opening and holding `Shard` instances by ShardId.
///
/// Uses interior mutability (RwLock) so it can be shared across tasks
/// and shards can be opened/closed dynamically as ownership changes.
pub struct ShardFactory {
    instances: RwLock<HashMap<ShardId, Arc<JobStoreShard>>>,
    template: DatabaseTemplate,
    rate_limiter: Arc<dyn RateLimitClient>,
    metrics: Option<Metrics>,
}

impl ShardFactory {
    pub fn new(
        template: DatabaseTemplate,
        rate_limiter: Arc<dyn RateLimitClient>,
        metrics: Option<Metrics>,
    ) -> Self {
        Self {
            instances: RwLock::new(HashMap::new()),
            template,
            rate_limiter,
            metrics,
        }
    }

    /// Get a shard by its ID.
    pub fn get(&self, shard_id: &ShardId) -> Option<Arc<JobStoreShard>> {
        // Use try_read to avoid blocking; if locked, return None
        self.instances
            .try_read()
            .ok()
            .and_then(|guard| guard.get(shard_id).map(Arc::clone))
    }

    /// Open a shard using the shared database template.
    ///
    /// The shard's UUID is used to construct the storage path.
    pub async fn open(&self, shard_id: &ShardId) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let shard_id = *shard_id;
        let name = shard_id.to_string();

        // Check if already open
        {
            let instances = self.instances.read().await;
            if let Some(shard) = instances.get(&shard_id) {
                return Ok(Arc::clone(shard));
            }
        }

        let path = self
            .template
            .path
            .replace("%shard%", &name)
            .replace("{shard}", &name);
        // Resolve WAL config with shard placeholder substitution if present
        let wal = self.template.wal.as_ref().map(|wal_template| WalConfig {
            backend: wal_template.backend.clone(),
            path: wal_template
                .path
                .replace("%shard%", &name)
                .replace("{shard}", &name),
        });
        let cfg = DatabaseConfig {
            name: name.clone(),
            backend: self.template.backend.clone(),
            path,
            flush_interval_ms: None, // Use SlateDB's default in production
            wal,
            apply_wal_on_close: self.template.apply_wal_on_close,
        };
        let shard_arc =
            JobStoreShard::open(&cfg, Arc::clone(&self.rate_limiter), self.metrics.clone()).await?;

        let mut instances = self.instances.write().await;
        instances.insert(shard_id, Arc::clone(&shard_arc));
        tracing::info!(shard_id = %shard_id, "opened shard");
        Ok(shard_arc)
    }

    /// Close a specific shard and remove it from the factory.
    pub async fn close(&self, shard_id: &ShardId) -> Result<(), JobStoreShardError> {
        let shard = {
            let mut instances = self.instances.write().await;
            instances.remove(shard_id)
        };
        if let Some(shard) = shard {
            shard.close().await?;
            tracing::info!(shard_id = %shard_id, "closed shard");
        }
        Ok(())
    }

    /// Reset a specific shard: close it, delete all data, and reopen fresh.
    /// This is intended for testing/development only.
    pub async fn reset(
        &self,
        shard_id: &ShardId,
    ) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let name = shard_id.to_string();

        // 1. Close and remove the shard if it exists
        let shard = {
            let mut instances = self.instances.write().await;
            instances.remove(shard_id)
        };
        if let Some(shard) = shard {
            shard.close().await?;
            tracing::info!(shard_id = %shard_id, "closed shard for reset");
        }

        // 2. Delete the data directory
        let path = self
            .template
            .path
            .replace("%shard%", &name)
            .replace("{shard}", &name);

        // Delete the main data path
        if let Err(e) = tokio::fs::remove_dir_all(&path).await {
            // Ignore "not found" errors - the directory might not exist yet
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(shard_id = %shard_id, path = %path, error = %e, "failed to delete shard data directory");
            }
        } else {
            tracing::info!(shard_id = %shard_id, path = %path, "deleted shard data directory");
        }

        // Delete WAL directory if configured separately
        if let Some(wal_cfg) = &self.template.wal {
            let wal_path = wal_cfg
                .path
                .replace("%shard%", &name)
                .replace("{shard}", &name);
            if let Err(e) = tokio::fs::remove_dir_all(&wal_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(shard_id = %shard_id, path = %wal_path, error = %e, "failed to delete shard WAL directory");
                }
            } else {
                tracing::debug!(shard_id = %shard_id, path = %wal_path, "deleted shard WAL directory");
            }
        }

        // 3. Reopen the shard fresh
        tracing::info!(shard_id = %shard_id, "reopening shard after reset");
        self.open(shard_id).await
    }

    /// Check if this factory owns a shard by its ID.
    pub fn owns_shard(&self, shard_id: &ShardId) -> bool {
        self.instances
            .try_read()
            .ok()
            .map(|guard| guard.contains_key(shard_id))
            .unwrap_or(false)
    }

    /// Get a snapshot of all currently open instances.
    pub fn instances(&self) -> HashMap<ShardId, Arc<JobStoreShard>> {
        self.instances
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Close all shards gracefully. Returns all errors if any shards fail to close.
    pub async fn close_all(&self) -> Result<(), CloseAllError> {
        let mut errors: Vec<(ShardId, JobStoreShardError)> = Vec::new();
        let instances = self.instances.read().await;
        for (shard_id, shard) in instances.iter() {
            if let Err(e) = shard.close().await {
                errors.push((*shard_id, e));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(CloseAllError { errors })
        }
    }
}

#[derive(Debug, Error)]
pub struct CloseAllError {
    pub errors: Vec<(ShardId, JobStoreShardError)>,
}

impl std::fmt::Display for CloseAllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} shard(s) failed to close", self.errors.len())
    }
}
