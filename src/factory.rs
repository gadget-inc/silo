use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::OnceCell;

use crate::gubernator::RateLimitClient;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, OpenShardOptions};
use crate::metrics::Metrics;
use crate::settings::DatabaseTemplate;
use crate::shard_range::{ShardId, ShardRange};
use crate::storage::resolve_object_store;

/// A shard entry that supports atomic initialization.
/// Uses OnceCell to ensure only one caller opens each database even under concurrent access.
struct ShardEntry {
    cell: OnceCell<Arc<JobStoreShard>>,
}

impl ShardEntry {
    fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }

    fn get(&self) -> Option<Arc<JobStoreShard>> {
        self.cell.get().cloned()
    }

    async fn get_or_try_init<F, Fut>(&self, f: F) -> Result<Arc<JobStoreShard>, JobStoreShardError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<JobStoreShard>, JobStoreShardError>>,
    {
        self.cell.get_or_try_init(f).await.map(Arc::clone)
    }
}

/// Factory for opening and holding `Shard` instances by ShardId.
///
/// Uses interior mutability (DashMap) so it can be shared across tasks
/// and shards can be opened/closed dynamically as ownership changes.
/// Per-shard OnceCell ensures that concurrent opens for the same shard
/// are serialized while opens for different shards proceed in parallel.
pub struct ShardFactory {
    instances: DashMap<ShardId, Arc<ShardEntry>>,
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
            instances: DashMap::new(),
            template,
            rate_limiter,
            metrics,
        }
    }

    /// Create a no-op factory for testing splitter logic without real shards.
    ///
    /// This factory cannot actually open shards; it's only useful for tests that
    /// need a factory reference but don't call `open()` or `clone_shard()`.
    #[doc(hidden)]
    pub fn new_noop() -> Self {
        use crate::gubernator::NullGubernatorClient;
        use crate::settings::Backend;

        Self {
            instances: DashMap::new(),
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
        self.instances.get(shard_id).and_then(|entry| entry.get())
    }

    /// Open a shard using the shared database template.
    ///
    /// The shard's UUID is used to construct the storage path. The `range` parameter
    /// specifies the tenant keyspace this shard is responsible for - this is immutable
    /// after opening.
    ///
    /// Uses per-shard OnceCell to ensure atomic initialization: if two callers try to
    /// open the same shard concurrently, only one will actually open the database and
    /// the other will wait and receive the same instance.
    ///
    /// **Note on path resolution:**
    /// For `Backend::Fs`, we resolve the object store at the storage root level (not
    /// the shard-specific path) and pass the shard name to DbBuilder. This is required
    /// for cloned databases to work correctly - they store relative paths to parent SST
    /// files that must resolve correctly from the storage root.
    pub async fn open(
        &self,
        shard_id: &ShardId,
        range: &ShardRange,
    ) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let shard_id = *shard_id;

        // Get or create the entry for this shard. The entry contains a OnceCell
        // that ensures only one caller actually opens the database.
        let entry = self
            .instances
            .entry(shard_id)
            .or_insert_with(|| Arc::new(ShardEntry::new()))
            .clone();

        // OnceCell::get_or_try_init ensures only one caller opens the database,
        // even if multiple callers reach this point concurrently.
        let name = shard_id.to_string();
        let range = range.clone();
        let template = &self.template;
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let metrics = self.metrics.clone();

        entry
            .get_or_try_init(|| async {
                // For Backend::Fs, we need to open at the storage root level so that
                // cloned databases can correctly resolve their relative parent SST paths.
                // Extract the root from the template and use shard name as the db path.
                let (resolved, db_path) =
                    Self::resolve_at_root(&template.backend, &template.path, &name)?;

                // Configure separate WAL object store if specified
                let (wal_store, wal_close_config) = if let Some(wal_template) = &template.wal {
                    let wal_path = wal_template
                        .path
                        .replace("%shard%", &name)
                        .replace("{shard}", &name);
                    let wal_resolved = resolve_object_store(&wal_template.backend, &wal_path)?;

                    // Only set up WAL cleanup for local (Fs) storage backends
                    let close_config = if wal_template.is_local_storage() {
                        Some(crate::job_store_shard::WalCloseConfig {
                            path: wal_resolved.root_path,
                            flush_on_close: template.apply_wal_on_close,
                        })
                    } else {
                        None
                    };
                    (Some(wal_resolved.store), close_config)
                } else {
                    (None, None)
                };

                let shard_arc = JobStoreShard::open_with_resolved_store(
                    name.clone(),
                    &db_path,
                    OpenShardOptions {
                        store: resolved.store,
                        wal_store,
                        wal_close_config,
                        flush_interval: None, // No custom flush interval from factory
                        rate_limiter,
                        metrics,
                    },
                    range.clone(),
                )
                .await?;

                tracing::info!(shard_id = %shard_id, range = %range, "opened shard");
                Ok(shard_arc)
            })
            .await
    }

    /// Resolve the object store at the storage root level.
    ///
    /// For Backend::Fs, this extracts the root path before the placeholder and
    /// returns the shard name as the db_path. For cloud backends, this works
    /// the same as before since the object store is already at bucket level.
    fn resolve_at_root(
        backend: &crate::settings::Backend,
        template_path: &str,
        shard_name: &str,
    ) -> Result<(crate::storage::ResolvedStore, String), JobStoreShardError> {
        use crate::settings::Backend;

        match backend {
            Backend::Fs => {
                // Extract root path from template (everything before the placeholder)
                let placeholder_pos = template_path
                    .find("%shard%")
                    .or_else(|| template_path.find("{shard}"));

                let root_path = match placeholder_pos {
                    Some(pos) => {
                        let root = &template_path[..pos];
                        let root_trimmed = root.trim_end_matches('/');
                        if root_trimmed.is_empty() {
                            "/"
                        } else {
                            root_trimmed
                        }
                    }
                    None => template_path, // No placeholder, use as-is
                };

                let resolved = resolve_object_store(backend, root_path)?;
                // For Fs, db_path is the shard name (relative to root)
                Ok((resolved, shard_name.to_string()))
            }
            Backend::Memory => {
                // Memory backend: use full path with placeholder replaced
                let full_path = template_path
                    .replace("%shard%", shard_name)
                    .replace("{shard}", shard_name);
                let resolved = resolve_object_store(backend, &full_path)?;
                let db_path = resolved.canonical_path.clone();
                Ok((resolved, db_path))
            }
            Backend::S3 | Backend::Gcs | Backend::Url => {
                // Cloud backends: resolve full URL, canonical_path is the db_path
                let full_path = template_path
                    .replace("%shard%", shard_name)
                    .replace("{shard}", shard_name);
                let resolved = resolve_object_store(backend, &full_path)?;
                let db_path = resolved.canonical_path.clone();
                Ok((resolved, db_path))
            }
        }
    }

    /// Close a specific shard and remove it from the factory.
    pub async fn close(&self, shard_id: &ShardId) -> Result<(), JobStoreShardError> {
        if let Some((_, entry)) = self.instances.remove(shard_id)
            && let Some(shard) = entry.get()
        {
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
        if let Some((_, entry)) = self.instances.remove(shard_id)
            && let Some(shard) = entry.get()
        {
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
    /// Returns true only if the shard entry exists AND the shard has been initialized.
    pub fn owns_shard(&self, shard_id: &ShardId) -> bool {
        self.instances
            .get(shard_id)
            .and_then(|entry| entry.get())
            .is_some()
    }

    /// Get a snapshot of all currently open instances.
    pub fn instances(&self) -> HashMap<ShardId, Arc<JobStoreShard>> {
        self.instances
            .iter()
            .filter_map(|entry| entry.value().get().map(|shard| (*entry.key(), shard)))
            .collect()
    }

    /// Close all shards gracefully. Returns all errors if any shards fail to close.
    pub async fn close_all(&self) -> Result<(), CloseAllError> {
        let mut errors: Vec<(ShardId, JobStoreShardError)> = Vec::new();
        // Collect and sort shard IDs for deterministic shutdown order
        let mut shard_ids: Vec<ShardId> = self.instances.iter().map(|e| *e.key()).collect();
        shard_ids.sort_unstable();
        for shard_id in shard_ids {
            let Some(entry) = self.instances.get(&shard_id) else {
                continue;
            };
            let Some(shard) = entry.get() else {
                continue;
            };
            if let Err(e) = shard.close().await {
                errors.push((shard_id, e));
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
        let parent_shard = self
            .instances
            .get(parent_id)
            .and_then(|entry| entry.get())
            .ok_or_else(|| ShardFactoryError::ShardNotOpen(*parent_id))?;

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

        // Validate that the placeholder is at a path boundary (preceded by / or at start of path).
        // This ensures that when we split the template into root + relative paths for cloning,
        // the relative path will be the shard ID as a subdirectory, not concatenated with a prefix.
        // e.g., "/data/%shard%" is valid (root="/data", relative="<uuid>")
        //       "/data/shard-%shard%" is INVALID - would create path mismatch during clone
        if pos > 0 {
            let char_before = self.template.path.chars().nth(pos - 1);
            if char_before != Some('/') {
                return Err(ShardFactoryError::CloneError(format!(
                    "shard placeholder in database template path must be preceded by '/' for cloning to work correctly. \
                     Got: '{}'. Change to something like '/data/%shard%' where the shard ID is a directory name.",
                    self.template.path
                )));
            }
        }

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
