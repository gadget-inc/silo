use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::gubernator::RateLimitClient;
use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::metrics::Metrics;
use crate::settings::{DatabaseConfig, DatabaseTemplate, WalConfig};
use crate::shard_range::{ShardId, ShardRange};
use crate::storage::resolve_object_store;
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

    /// Create a no-op factory for testing orchestrator logic without real shards.
    ///
    /// This factory cannot actually open shards; it's only useful for tests that
    /// need a factory reference but don't call `open()` or `clone_shard()`.
    #[doc(hidden)]
    pub fn new_noop() -> Self {
        use crate::gubernator::NullGubernatorClient;
        use crate::settings::Backend;

        Self {
            instances: RwLock::new(HashMap::new()),
            template: DatabaseTemplate {
                backend: Backend::Memory,
                path: "/noop/%shard%".to_string(),
                wal: None,
                apply_wal_on_close: false,
            },
            rate_limiter: NullGubernatorClient::new(),
            metrics: None,
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
    /// The shard's UUID is used to construct the storage path. The `range` parameter
    /// specifies the tenant keyspace this shard is responsible for - this is immutable
    /// after opening.
    pub async fn open(
        &self,
        shard_id: &ShardId,
        range: &ShardRange,
    ) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
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
        let shard_arc = JobStoreShard::open(
            &cfg,
            Arc::clone(&self.rate_limiter),
            self.metrics.clone(),
            range.clone(),
        )
        .await?;

        let mut instances = self.instances.write().await;
        instances.insert(shard_id, Arc::clone(&shard_arc));
        tracing::info!(shard_id = %shard_id, range = %range, "opened shard");
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
        range: &ShardRange,
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
        self.open(shard_id, range).await
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

    /// [SILO-SPLIT-CLONE-1] Clone a shard to create a child shard for splitting.
    ///
    /// This creates a point-in-time clone of the parent shard's database using
    /// SlateDB's checkpoint and clone functionality. The clone shares SST files
    /// with the parent via object storage, so only metadata is copied.
    ///
    /// # Arguments
    /// * `parent_id` - The shard being split (must be open)
    /// * `child_id` - The new child shard ID
    ///
    /// # Errors
    /// Returns an error if:
    /// - The parent shard is not open
    /// - The clone operation fails
    pub async fn clone_shard(
        &self,
        parent_id: &ShardId,
        child_id: &ShardId,
    ) -> Result<(), ShardFactoryError> {
        let parent_name = parent_id.to_string();
        let child_name = child_id.to_string();

        // Get the parent shard (must be open)
        let parent_shard = {
            let instances = self.instances.read().await;
            instances
                .get(parent_id)
                .cloned()
                .ok_or_else(|| ShardFactoryError::ShardNotOpen(*parent_id))?
        };

        // Ensure all data is flushed to object storage before creating checkpoint
        // This is required for SlateDB cloning to work correctly
        parent_shard.db().flush().await.map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to flush before checkpoint: {}", e))
        })?;

        // Create a checkpoint in the parent database
        // The checkpoint captures a consistent view of the database
        let checkpoint_options = slatedb::config::CheckpointOptions {
            lifetime: Some(Duration::from_secs(300)), // 5 minutes should be enough for the clone
            ..Default::default()
        };
        let checkpoint = parent_shard
            .db()
            .create_checkpoint(slatedb::config::CheckpointScope::All, &checkpoint_options)
            .await
            .map_err(|e| {
                ShardFactoryError::CloneError(format!("failed to create checkpoint: {}", e))
            })?;

        tracing::info!(
            parent_shard_id = %parent_id,
            child_shard_id = %child_id,
            checkpoint_id = ?checkpoint.id,
            "created checkpoint for shard cloning"
        );

        // For cloning to work, both parent and child must be accessible from the same
        // object store. We extract the common root from the template and create relative
        // paths for both.
        //
        // Template path example: "/tmp/silo-data/%shard%"
        // Parent full path: "/tmp/silo-data/parent-uuid"
        // Child full path:  "/tmp/silo-data/child-uuid"
        // Common root: "/tmp/silo-data"
        // Parent relative: "parent-uuid"
        // Child relative: "child-uuid"

        // Find the placeholder in the template path
        let placeholder_pos = self
            .template
            .path
            .find("%shard%")
            .or_else(|| self.template.path.find("{shard}"));

        let pos = placeholder_pos.ok_or_else(|| {
            ShardFactoryError::CloneError(format!(
                "database template path must contain a shard placeholder (%shard% or {{shard}}), got: {}",
                self.template.path
            ))
        })?;

        // Extract the root path (everything before the placeholder)
        let root = &self.template.path[..pos];
        // Trim trailing slashes but keep at least one character
        let root_trimmed = root.trim_end_matches('/');
        let root_path = if root_trimmed.is_empty() {
            "/".to_string()
        } else {
            root_trimmed.to_string()
        };
        let parent_relative = parent_name.clone();
        let child_relative = child_name.clone();

        // Resolve the object store at the common root
        let resolved = resolve_object_store(&self.template.backend, &root_path).map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to resolve storage root: {}", e))
        })?;

        tracing::debug!(
            root_path = %root_path,
            canonical_root = %resolved.canonical_path,
            parent_relative = %parent_relative,
            child_relative = %child_relative,
            "preparing to clone shard database"
        );

        // Create an Admin for the child path (relative to the root) and clone from parent
        let admin =
            slatedb::admin::Admin::builder(child_relative.as_str(), Arc::clone(&resolved.store))
                .build();

        admin
            .create_clone(parent_relative.as_str(), Some(checkpoint.id))
            .await
            .map_err(|e| {
                ShardFactoryError::CloneError(format!(
                    "failed to clone database: {} (root={}, parent={}, child={})",
                    e, resolved.canonical_path, parent_relative, child_relative
                ))
            })?;

        tracing::info!(
            parent_shard_id = %parent_id,
            child_shard_id = %child_id,
            root_path = %root_path,
            parent_relative = %parent_relative,
            child_relative = %child_relative,
            "cloned shard database"
        );

        Ok(())
    }

    /// Get the database template used by this factory.
    pub fn template(&self) -> &DatabaseTemplate {
        &self.template
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

/// Errors that can occur during shard factory operations.
#[derive(Debug, Error)]
pub enum ShardFactoryError {
    #[error("shard not open: {0}")]
    ShardNotOpen(ShardId),

    #[error("clone error: {0}")]
    CloneError(String),

    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    #[error("shard error: {0}")]
    ShardError(#[from] JobStoreShardError),
}
