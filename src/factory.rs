use dashmap::DashMap;
use futures::TryStreamExt;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path as ObjectPath;
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
/// Default timeout for shard close operations (30 seconds).
/// SlateDB's internal retrying_object_store retries indefinitely on transient errors,
/// so we need a timeout to prevent close from hanging forever if the object store is
/// unreachable or the filesystem is read-only.
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ShardFactory {
    instances: DashMap<ShardId, Arc<ShardEntry>>,
    template: DatabaseTemplate,
    rate_limiter: Arc<dyn RateLimitClient>,
    metrics: Option<Metrics>,
    close_timeout: Duration,
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
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
        }
    }

    /// Create a no-op factory for testing splitter logic without real shards.
    ///
    /// This factory cannot actually open shards; it's only useful for tests that
    /// need a factory reference but don't call `open()` or `clone_closed_shard()`.
    #[doc(hidden)]
    pub fn new_noop() -> Self {
        use crate::gubernator::NullGubernatorClient;
        use crate::settings::Backend;

        Self {
            instances: DashMap::new(),
            template: DatabaseTemplate {
                backend: Backend::Memory,
                path: "/noop".to_string(),
                wal: None,
                apply_wal_on_close: false,
                slatedb: None,
            },
            rate_limiter: NullGubernatorClient::new(),
            metrics: None,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
        }
    }

    /// Set the timeout for shard close operations.
    /// SlateDB retries indefinitely on transient errors, so this timeout prevents
    /// close from hanging forever. Useful for tests that inject storage failures.
    pub fn set_close_timeout(&mut self, timeout: Duration) {
        self.close_timeout = timeout;
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
                        slatedb_settings: template.slatedb.clone(),
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

    /// Validate that the template path has the shard placeholder at a directory boundary.
    ///
    /// The placeholder (`%shard%` or `{shard}`) must be preceded by `/` (or be at the start)
    /// so that the shard ID forms a complete directory name, not a suffix of another name.
    /// This is required for clone/split operations and for consistent path handling.
    ///
    /// Returns the position of the placeholder if valid.
    fn validate_template_path(template_path: &str) -> Result<usize, JobStoreShardError> {
        let placeholder_pos = template_path
            .find("%shard%")
            .or_else(|| template_path.find("{shard}"));

        let pos = placeholder_pos.ok_or_else(|| {
            JobStoreShardError::Rkyv(format!(
                "database template path must contain a shard placeholder (%shard% or {{shard}}), got: {}",
                template_path
            ))
        })?;

        // Validate that the placeholder is at a path boundary (preceded by / or at start of path)
        if pos > 0 {
            let char_before = template_path.chars().nth(pos - 1);
            if char_before != Some('/') {
                return Err(JobStoreShardError::Rkyv(format!(
                    "shard placeholder in database template path must be preceded by '/' for correct path handling. \
                     Got: '{}'. Change to something like '/data/%shard%' where the shard ID is a directory name.",
                    template_path
                )));
            }
        }

        Ok(pos)
    }

    /// Resolve the object store at the storage root level.
    ///
    /// For Backend::Fs, this extracts the root path before the placeholder and
    /// returns the shard name as the db_path. For cloud backends, this works
    /// the same as before since the object store is already at bucket level.
    ///
    /// The returned `ResolvedStore.root_path` combined with `db_path` gives the
    /// full path where shard data is stored.
    fn resolve_at_root(
        backend: &crate::settings::Backend,
        template_path: &str,
        shard_name: &str,
    ) -> Result<(crate::storage::ResolvedStore, String), JobStoreShardError> {
        if backend.is_local_fs() {
            // Local filesystem backends: resolve at the storage root so that
            // cloned databases can correctly resolve their relative parent SST paths.
            let pos = Self::validate_template_path(template_path)?;

            let root = &template_path[..pos];
            let root_trimmed = root.trim_end_matches('/');
            let root_path = if root_trimmed.is_empty() {
                "/"
            } else {
                root_trimmed
            };

            let resolved = resolve_object_store(backend, root_path)?;
            Ok((resolved, shard_name.to_string()))
        } else {
            // Object store backends (Memory, S3, GCS, URL): resolve full path
            let full_path = template_path
                .replace("%shard%", shard_name)
                .replace("{shard}", shard_name);
            let resolved = resolve_object_store(backend, &full_path)?;
            let db_path = resolved.canonical_path.clone();
            Ok((resolved, db_path))
        }
    }

    /// Get the filesystem path where a shard's data is stored.
    /// This is only valid for local filesystem backends (Fs, TurmoilFs).
    fn get_shard_data_path(
        &self,
        shard_name: &str,
    ) -> Result<std::path::PathBuf, JobStoreShardError> {
        let (resolved, db_path) =
            Self::resolve_at_root(&self.template.backend, &self.template.path, shard_name)?;
        // For Fs backends, root_path is the canonical filesystem root and db_path is the shard name
        // For other backends, root_path might be a URL path component
        Ok(std::path::Path::new(&resolved.root_path).join(&db_path))
    }

    /// Close a specific shard and remove it from the factory.
    ///
    /// If `shard.close()` fails or times out, the shard is re-inserted into instances so that close can be retried later. This prevents silent data loss where a failed close is followed by a lease release.
    ///
    /// A timeout is applied because SlateDB's internal retrying_object_store retries indefinitely on transient errors, which would cause close to hang forever if the object store is unreachable.
    pub async fn close(&self, shard_id: &ShardId) -> Result<(), JobStoreShardError> {
        tracing::trace!(shard_id = %shard_id, "factory.close: removing from instances");
        if let Some((id, entry)) = self.instances.remove(shard_id) {
            if let Some(shard) = entry.get() {
                tracing::trace!(shard_id = %shard_id, "factory.close: calling shard.close()");
                let close_result = tokio::time::timeout(self.close_timeout, shard.close()).await;
                match close_result {
                    Ok(Ok(())) => {
                        tracing::info!(shard_id = %shard_id, "closed shard");
                    }
                    Ok(Err(e)) => {
                        // If the DB is already closed (e.g. a previous timed-out close
                        // actually completed internally), treat it as a successful close
                        // rather than re-inserting. The shard is already shut down.
                        if let JobStoreShardError::Slate(ref slate_err) = e
                            && matches!(slate_err.kind(), slatedb::ErrorKind::Closed(_))
                        {
                            tracing::info!(shard_id = %shard_id, "factory.close: shard already closed, treating as success");
                        } else {
                            // Close returned an error - re-insert so close can be retried
                            tracing::error!(shard_id = %shard_id, error = %e, "factory.close: shard.close() failed, re-inserting into instances");
                            self.instances.insert(id, entry);
                            return Err(e);
                        }
                    }
                    Err(_elapsed) => {
                        // Timeout - re-insert so close can be retried
                        tracing::error!(
                            shard_id = %shard_id,
                            timeout_secs = self.close_timeout.as_secs(),
                            "factory.close: shard.close() timed out (object store may be unreachable), re-inserting into instances"
                        );
                        self.instances.insert(id, entry);
                        return Err(JobStoreShardError::Rkyv(format!(
                            "shard close timed out after {}s",
                            self.close_timeout.as_secs()
                        )));
                    }
                }
            } else {
                tracing::trace!(shard_id = %shard_id, "factory.close: shard not initialized");
            }
        } else {
            tracing::trace!(shard_id = %shard_id, "factory.close: shard not found in instances");
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

        // 2. Delete the data using the appropriate method for the backend
        self.delete_shard_data(&name).await?;

        // Delete WAL directory if configured separately
        if let Some(wal_cfg) = &self.template.wal {
            self.delete_wal_data(&name, wal_cfg).await?;
        }

        // 3. Reopen the shard fresh
        tracing::info!(shard_id = %shard_id, "reopening shard after reset");
        self.open(shard_id, range).await
    }

    /// Delete all data for a shard from storage.
    /// Uses the same path resolution as opening to ensure we delete the correct directory.
    async fn delete_shard_data(&self, shard_name: &str) -> Result<(), JobStoreShardError> {
        if self.template.backend.is_local_fs() {
            let data_path = self.get_shard_data_path(shard_name)?;
            let path_str = data_path.to_string_lossy();

            if let Err(e) = tokio::fs::remove_dir_all(&data_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(shard_name = %shard_name, path = %path_str, error = %e, "failed to delete shard data directory");
                }
            } else {
                tracing::info!(shard_name = %shard_name, path = %path_str, "deleted shard data directory");
            }
        } else {
            // For object store backends, use the object store API to delete all objects
            let (resolved, db_path) =
                Self::resolve_at_root(&self.template.backend, &self.template.path, shard_name)?;

            // List and delete all objects under the shard's path
            let prefix = ObjectPath::from(db_path.as_str());
            let objects: Vec<_> = resolved
                .store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|e| {
                    JobStoreShardError::Rkyv(format!("failed to list objects for deletion: {}", e))
                })?;

            let count = objects.len();
            for obj in objects {
                if let Err(e) = resolved.store.delete(&obj.location).await {
                    tracing::warn!(
                        shard_name = %shard_name,
                        path = %obj.location,
                        error = %e,
                        "failed to delete object"
                    );
                }
            }
            tracing::info!(shard_name = %shard_name, objects_deleted = count, "deleted shard data from object store");
        }

        Ok(())
    }

    /// Delete WAL data for a shard.
    async fn delete_wal_data(
        &self,
        shard_name: &str,
        wal_cfg: &crate::settings::WalConfig,
    ) -> Result<(), JobStoreShardError> {
        let wal_path = wal_cfg
            .path
            .replace("%shard%", shard_name)
            .replace("{shard}", shard_name);

        if wal_cfg.is_local_storage() {
            // Local WAL - use filesystem deletion
            if let Err(e) = tokio::fs::remove_dir_all(&wal_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(shard_name = %shard_name, path = %wal_path, error = %e, "failed to delete shard WAL directory");
                }
            } else {
                tracing::debug!(shard_name = %shard_name, path = %wal_path, "deleted shard WAL directory");
            }
        } else {
            // Object store WAL - use object store API
            let resolved = resolve_object_store(&wal_cfg.backend, &wal_path)?;
            let prefix = ObjectPath::from(resolved.canonical_path.as_str());
            let objects: Vec<_> = resolved
                .store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|e| {
                    JobStoreShardError::Rkyv(format!(
                        "failed to list WAL objects for deletion: {}",
                        e
                    ))
                })?;

            for obj in objects {
                if let Err(e) = resolved.store.delete(&obj.location).await {
                    tracing::warn!(
                        shard_name = %shard_name,
                        path = %obj.location,
                        error = %e,
                        "failed to delete WAL object"
                    );
                }
            }
        }

        Ok(())
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

    /// Clone a closed shard's database to create child shards for splitting.
    ///
    /// This is used during shard splits after the parent shard has been fully closed.
    /// It opens a raw SlateDB database at the parent's path (without WAL, broker, or
    /// any silo-level processing), creates a single checkpoint, clones to both children,
    /// then closes the raw database.
    ///
    /// By operating on a fully closed parent, we guarantee that no in-flight writes
    /// can land after the checkpoint, ensuring children get a consistent snapshot.
    pub async fn clone_closed_shard(
        &self,
        parent_id: &ShardId,
        left_child_id: &ShardId,
        right_child_id: &ShardId,
    ) -> Result<(), ShardFactoryError> {
        let parent_name = parent_id.to_string();
        let left_child_name = left_child_id.to_string();
        let right_child_name = right_child_id.to_string();

        // Resolve paths relative to storage root
        let (parent_resolved, parent_db_path) =
            Self::resolve_at_root(&self.template.backend, &self.template.path, &parent_name)?;
        let (_, left_child_db_path) = Self::resolve_at_root(
            &self.template.backend,
            &self.template.path,
            &left_child_name,
        )?;
        let (_, right_child_db_path) = Self::resolve_at_root(
            &self.template.backend,
            &self.template.path,
            &right_child_name,
        )?;

        // Open a raw SlateDB database at the parent's path. The parent shard has been
        // fully closed (data flushed, WAL cleaned up), so we don't need a WAL. We still
        // need the merge operator because the parent may contain counter merge entries
        // that haven't been fully compacted yet.
        let db =
            slatedb::DbBuilder::new(parent_db_path.as_str(), Arc::clone(&parent_resolved.store))
                .with_merge_operator(crate::job_store_shard::counter_merge_operator())
                .build()
                .await
                .map_err(|e| {
                    ShardFactoryError::CloneError(format!(
                        "failed to reopen parent DB for cloning: {}",
                        e
                    ))
                })?;

        // Flush to ensure all data is in object storage before checkpointing
        db.flush().await.map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to flush before checkpoint: {}", e))
        })?;

        // Create a single checkpoint shared by both children. Use no lifetime so
        // the checkpoint persists until the children have fully compacted away their
        // dependency on the parent's SSTs.
        let checkpoint_options = slatedb::config::CheckpointOptions {
            lifetime: None,
            ..Default::default()
        };
        let checkpoint = db
            .create_checkpoint(slatedb::config::CheckpointScope::All, &checkpoint_options)
            .await
            .map_err(|e| {
                ShardFactoryError::CloneError(format!("failed to create checkpoint: {}", e))
            })?;

        tracing::info!(
            parent_shard_id = %parent_id,
            left_child_id = %left_child_id,
            right_child_id = %right_child_id,
            checkpoint_id = ?checkpoint.id,
            "created checkpoint for shard cloning (from closed parent)"
        );

        // Clone for left child
        let left_admin = slatedb::admin::Admin::builder(
            left_child_db_path.as_str(),
            Arc::clone(&parent_resolved.store),
        )
        .build();

        left_admin
            .create_clone(parent_db_path.as_str(), Some(checkpoint.id))
            .await
            .map_err(|e| {
                ShardFactoryError::CloneError(format!(
                    "failed to clone left child database: {} (parent={}, child={})",
                    e, parent_db_path, left_child_db_path
                ))
            })?;

        // Clone for right child
        let right_admin = slatedb::admin::Admin::builder(
            right_child_db_path.as_str(),
            Arc::clone(&parent_resolved.store),
        )
        .build();

        right_admin
            .create_clone(parent_db_path.as_str(), Some(checkpoint.id))
            .await
            .map_err(|e| {
                ShardFactoryError::CloneError(format!(
                    "failed to clone right child database: {} (parent={}, child={})",
                    e, parent_db_path, right_child_db_path
                ))
            })?;

        // Close the raw database
        db.close().await.map_err(|e| {
            ShardFactoryError::CloneError(format!(
                "failed to close raw parent DB after cloning: {}",
                e
            ))
        })?;

        tracing::info!(
            parent_shard_id = %parent_id,
            left_child_id = %left_child_id,
            right_child_id = %right_child_id,
            parent_db_path = %parent_db_path,
            left_child_db_path = %left_child_db_path,
            right_child_db_path = %right_child_db_path,
            "cloned closed shard database to both children"
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
    #[error("clone error: {0}")]
    CloneError(String),

    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    #[error("shard error: {0}")]
    ShardError(#[from] JobStoreShardError),
}
