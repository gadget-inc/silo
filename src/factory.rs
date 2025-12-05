use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::gubernator::RateLimitClient;
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::settings::{DatabaseConfig, DatabaseTemplate, WalConfig};
use thiserror::Error;

/// Factory for opening and holding `Shard` instances by name.
///
/// Uses interior mutability (RwLock) so it can be shared across tasks
/// and shards can be opened/closed dynamically as ownership changes.
pub struct ShardFactory {
    instances: RwLock<HashMap<String, Arc<JobStoreShard>>>,
    template: DatabaseTemplate,
    rate_limiter: Arc<dyn RateLimitClient>,
}

impl ShardFactory {
    pub fn new(template: DatabaseTemplate, rate_limiter: Arc<dyn RateLimitClient>) -> Self {
        Self {
            instances: RwLock::new(HashMap::new()),
            template,
            rate_limiter,
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<JobStoreShard>> {
        // Use try_read to avoid blocking; if locked, return None
        self.instances
            .try_read()
            .ok()
            .and_then(|guard| guard.get(name).map(Arc::clone))
    }

    /// Open a shard using the shared database template.
    pub async fn open(
        &self,
        shard_number: usize,
    ) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let name = shard_number.to_string();

        // Check if already open
        {
            let instances = self.instances.read().await;
            if let Some(shard) = instances.get(&name) {
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
        };
        let shard_arc =
            JobStoreShard::open_with_rate_limiter(&cfg, Arc::clone(&self.rate_limiter)).await?;

        let mut instances = self.instances.write().await;
        instances.insert(cfg.name.clone(), Arc::clone(&shard_arc));
        tracing::info!(shard = shard_number, "opened shard");
        Ok(shard_arc)
    }

    /// Close a specific shard and remove it from the factory.
    pub async fn close(&self, shard_number: usize) -> Result<(), JobStoreShardError> {
        let name = shard_number.to_string();
        let shard = {
            let mut instances = self.instances.write().await;
            instances.remove(&name)
        };
        if let Some(shard) = shard {
            shard.close().await?;
            tracing::info!(shard = shard_number, "closed shard");
        }
        Ok(())
    }

    /// Reset a specific shard: close it, delete all data, and reopen fresh.
    /// This is intended for testing/development only.
    pub async fn reset(&self, shard_number: usize) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let name = shard_number.to_string();
        
        // 1. Close and remove the shard if it exists
        let shard = {
            let mut instances = self.instances.write().await;
            instances.remove(&name)
        };
        if let Some(shard) = shard {
            shard.close().await?;
            tracing::info!(shard = shard_number, "closed shard for reset");
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
                tracing::warn!(shard = shard_number, path = %path, error = %e, "failed to delete shard data directory");
            }
        } else {
            tracing::info!(shard = shard_number, path = %path, "deleted shard data directory");
        }

        // Delete WAL directory if configured separately
        if let Some(wal_cfg) = &self.template.wal {
            let wal_path = wal_cfg
                .path
                .replace("%shard%", &name)
                .replace("{shard}", &name);
            if let Err(e) = tokio::fs::remove_dir_all(&wal_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(shard = shard_number, path = %wal_path, error = %e, "failed to delete shard WAL directory");
                }
            } else {
                tracing::info!(shard = shard_number, path = %wal_path, "deleted shard WAL directory");
            }
        }

        // 3. Reopen the shard fresh
        tracing::info!(shard = shard_number, "reopening shard after reset");
        self.open(shard_number).await
    }

    /// Get a snapshot of all currently open instances.
    pub fn instances(&self) -> HashMap<String, Arc<JobStoreShard>> {
        self.instances
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Close all shards gracefully. Returns all errors if any shards fail to close.
    pub async fn close_all(&self) -> Result<(), CloseAllError> {
        let mut errors: Vec<(String, JobStoreShardError)> = Vec::new();
        let instances = self.instances.read().await;
        for (name, shard) in instances.iter() {
            if let Err(e) = shard.close().await {
                errors.push((name.clone(), e));
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
    pub errors: Vec<(String, JobStoreShardError)>,
}

impl std::fmt::Display for CloseAllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} shard(s) failed to close", self.errors.len())
    }
}
