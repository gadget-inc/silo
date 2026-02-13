//! Job store shard - a single shard of the distributed job storage system.
mod cancel;
mod cleanup;
mod counters;
mod dequeue;
mod enqueue;
mod expedite;
mod floating;
pub(crate) mod helpers;
pub mod import;
mod lease;
mod lease_task;
mod rate_limit;
mod restart;
mod scan;

pub use cleanup::{CleanupProgress, CleanupResult};

pub use counters::{ShardCounters, counter_merge_operator};
pub use expedite::JobNotExpediteableError;
pub use lease_task::JobNotLeaseableError;
pub use restart::JobNotRestartableError;

pub(crate) use enqueue::LimitTaskParams;
use helpers::DbWriteBatcher;
pub use helpers::now_epoch_ms;

use slatedb::Db;
use std::sync::{Arc, OnceLock};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::codec::CodecError;
use crate::concurrency::ConcurrencyManager;
use crate::gubernator::RateLimitClient;
use crate::job::{JobStatus, JobStatusKind, JobView};
use crate::job_attempt::JobAttemptView;
use crate::keys::{attempt_key, job_info_key, job_status_key};
use crate::metrics::Metrics;
use crate::query::ShardQueryEngine;
use crate::settings::DatabaseConfig;
use crate::shard_range::ShardRange;
use crate::storage::resolve_object_store;
use crate::task::{LeasedRefreshTask, LeasedTask};
use crate::task_broker::TaskBroker;

/// Configuration for WAL cleanup during shard close
#[derive(Debug, Clone)]
pub struct WalCloseConfig {
    /// Path to the local WAL directory to clean up
    pub path: String,
    /// Whether to flush memtable to SST before closing
    pub flush_on_close: bool,
}

/// Options for opening a shard with a pre-resolved object store.
pub struct OpenShardOptions {
    /// Pre-resolved object store at the storage root level
    pub store: Arc<dyn slatedb::object_store::ObjectStore>,
    /// Optional separate object store for WAL
    pub wal_store: Option<Arc<dyn slatedb::object_store::ObjectStore>>,
    /// Optional WAL cleanup configuration for local storage
    pub wal_close_config: Option<WalCloseConfig>,
    /// Optional SlateDB settings for tuning database performance.
    /// The `merge_operator` field will be overridden with Silo's counter merge operator.
    pub slatedb_settings: Option<slatedb::config::Settings>,
    /// Rate limiter client for this shard
    pub rate_limiter: Arc<dyn RateLimitClient>,
    /// Optional metrics collector
    pub metrics: Option<Metrics>,
}

/// Result of a dequeue operation - contains both job tasks and floating limit refresh tasks
#[derive(Debug, Default)]
pub struct DequeueResult {
    pub tasks: Vec<LeasedTask>,
    pub refresh_tasks: Vec<LeasedRefreshTask>,
}

/// Represents a single shard of the system. Owns the SlateDB instance.
pub struct JobStoreShard {
    pub(crate) name: String,
    pub(crate) db: Arc<Db>,
    pub(crate) broker: Arc<TaskBroker>,
    pub(crate) concurrency: Arc<ConcurrencyManager>,
    query_engine: OnceLock<ShardQueryEngine>,
    pub(crate) rate_limiter: Arc<dyn RateLimitClient>,
    /// Optional WAL close configuration - present when using local WAL storage
    wal_close_config: Option<WalCloseConfig>,
    /// Optional metrics for recording shard-level stats
    pub(crate) metrics: Option<Metrics>,
    /// Cancellation token for background tasks like cleanup.
    /// Signaled when the shard is closing.
    cancellation: CancellationToken,
}

#[derive(Debug, Error)]
pub enum JobStoreShardError {
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
    #[error(transparent)]
    Slate(#[from] slatedb::Error),
    #[error("json serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("rkyv serialization error: {0}")]
    Rkyv(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("lease not found for task id {0}")]
    LeaseNotFound(String),
    #[error("lease owner mismatch for task id {task_id}: expected {expected}, got {got}")]
    LeaseOwnerMismatch {
        task_id: String,
        expected: String,
        got: String,
    },
    #[error("job already exists with id {0}")]
    JobAlreadyExists(String),
    #[error("job not found with id {0}")]
    JobNotFound(String),
    #[error("cannot delete job {0}: job is currently running or has pending requests")]
    JobInProgress(String),
    #[error("job already cancelled with id {0}")]
    JobAlreadyCancelled(String),
    #[error("cannot cancel job {0}: job is already in terminal state {1:?}")]
    JobAlreadyTerminal(String, JobStatusKind),
    #[error("cannot restart job: {0}")]
    JobNotRestartable(#[from] JobNotRestartableError),
    #[error("cannot expedite job: {0}")]
    JobNotExpediteable(#[from] JobNotExpediteableError),
    #[error("cannot lease job: {0}")]
    JobNotLeaseable(#[from] JobNotLeaseableError),
    #[error("transaction conflict during {0}, exceeded max retries")]
    TransactionConflict(String),
}

impl From<CodecError> for JobStoreShardError {
    fn from(e: CodecError) -> Self {
        JobStoreShardError::Rkyv(e.to_string())
    }
}

impl From<crate::concurrency::ConcurrencyError> for JobStoreShardError {
    fn from(e: crate::concurrency::ConcurrencyError) -> Self {
        match e {
            crate::concurrency::ConcurrencyError::Slate(e) => JobStoreShardError::Slate(e),
            crate::concurrency::ConcurrencyError::Encoding(s) => JobStoreShardError::Rkyv(s),
        }
    }
}

impl JobStoreShard {
    /// Open a shard with a rate limit client, range, and optional metrics.
    ///
    /// The `range` parameter specifies the tenant keyspace this shard is responsible for.
    /// This is immutable after opening - when shards split, new child shards are created
    /// with new identities and ranges.
    pub async fn open(
        cfg: &DatabaseConfig,
        rate_limiter: Arc<dyn RateLimitClient>,
        metrics: Option<Metrics>,
        range: ShardRange,
    ) -> Result<Arc<Self>, JobStoreShardError> {
        let resolved = resolve_object_store(&cfg.backend, &cfg.path)?;

        // Configure separate WAL object store if specified, and track for cleanup on close
        let (wal_store, wal_close_config) = if let Some(wal_cfg) = &cfg.wal {
            let wal_resolved = resolve_object_store(&wal_cfg.backend, &wal_cfg.path)?;

            // Only set up WAL cleanup for local (Fs) storage backends
            let close_config = if wal_cfg.is_local_storage() {
                Some(WalCloseConfig {
                    path: wal_resolved.root_path,
                    flush_on_close: cfg.apply_wal_on_close,
                })
            } else {
                None
            };
            (Some(wal_resolved.store), close_config)
        } else {
            (None, None)
        };

        // Use SlateDB settings if configured
        let slatedb_settings = cfg.slatedb.clone();

        Self::open_with_resolved_store(
            cfg.name.clone(),
            &resolved.canonical_path,
            OpenShardOptions {
                store: resolved.store,
                wal_store,
                wal_close_config,
                slatedb_settings,
                rate_limiter,
                metrics,
            },
            range,
        )
        .await
    }

    /// Open a shard using a pre-resolved object store and relative path.
    ///
    /// This is the unified method for opening shards - it works for both normal shards
    /// and cloned shards because it operates at the storage root level.
    ///
    /// **Why root-level opening is required:**
    /// SlateDB clones store relative paths to parent SST files (e.g., `parent-uuid/compacted/file.sst`).
    /// If we opened with a LocalFileSystem rooted at the child path, these would resolve incorrectly
    /// as `child-uuid/parent-uuid/compacted/...` instead of `parent-uuid/compacted/...`.
    ///
    /// # Arguments
    /// * `name` - The shard's name (typically its UUID)
    /// * `db_path` - Path to the database relative to the object store root
    /// * `options` - Storage and configuration options for the shard
    /// * `range` - The tenant keyspace range for this shard
    pub async fn open_with_resolved_store(
        name: String,
        db_path: &str,
        options: OpenShardOptions,
        range: ShardRange,
    ) -> Result<Arc<Self>, JobStoreShardError> {
        let OpenShardOptions {
            store,
            wal_store,
            wal_close_config,
            slatedb_settings,
            rate_limiter,
            metrics,
        } = options;

        let mut db_builder = slatedb::DbBuilder::new(db_path, store)
            .with_merge_operator(counters::counter_merge_operator());

        // Configure separate WAL object store if provided
        if let Some(wal) = wal_store {
            db_builder = db_builder.with_wal_object_store(wal);
        }

        // Apply custom SlateDB settings if specified
        // Note: The merge_operator field in settings is ignored because we already
        // set it above via with_merge_operator() for counter support
        if let Some(settings) = slatedb_settings {
            db_builder = db_builder.with_settings(settings);
        }

        let db = db_builder.build().await?;
        let db = Arc::new(db);
        let concurrency = Arc::new(ConcurrencyManager::new());

        // Note: concurrency counts are hydrated lazily on first access to each queue.
        // This avoids blocking shard startup while scanning potentially large holder sets.

        let broker = TaskBroker::new(Arc::clone(&db), name.clone(), metrics.clone(), range);
        broker.start();

        let shard = Arc::new(Self {
            name,
            db,
            broker,
            concurrency,
            query_engine: OnceLock::new(),
            rate_limiter,
            wal_close_config,
            metrics,
            cancellation: CancellationToken::new(),
        });

        // Set the shard creation timestamp if this is the first time opening
        shard.set_created_at_ms_if_unset().await?;

        Ok(shard)
    }

    /// Get the query engine for this shard, lazily initializing it on first access.
    ///
    /// This is the low-level query engine for single-shard queries, typically used
    /// by gRPC handlers. For cluster-wide queries, use `ClusterQueryEngine`.
    pub fn query_engine(self: &Arc<Self>) -> &ShardQueryEngine {
        self.query_engine.get_or_init(|| {
            ShardQueryEngine::new(Arc::clone(self), "jobs").expect("Failed to create query engine")
        })
    }

    /// Close the underlying SlateDB instance gracefully.
    ///
    /// When a separate local WAL is configured with `apply_wal_on_close: true`:
    /// 1. Flushes all memtable data to SSTs in object storage
    /// 2. Closes the database
    /// 3. Deletes the local WAL directory
    ///
    /// This ensures all data is durably stored in object storage before closing,
    /// allowing the shard to be safely reopened elsewhere (e.g., on a different node).
    pub async fn close(&self) -> Result<(), JobStoreShardError> {
        tracing::trace!(shard = %self.name, "shard.close: signaling cancellation for background tasks");
        self.cancellation.cancel();

        tracing::trace!(shard = %self.name, "shard.close: stopping broker");
        self.broker.stop();

        // If we have a local WAL with flush_on_close enabled, flush memtable to SSTs first
        if let Some(ref wal_config) = self.wal_close_config
            && wal_config.flush_on_close
        {
            tracing::info!(
                shard = %self.name,
                wal_path = %wal_config.path,
                "flushing memtable to object storage before close"
            );

            // Flush memtable to SST to ensure all data is in object storage
            // This converts all in-memory data (and WAL data) to SSTs
            self.db
                .flush_with_options(slatedb::config::FlushOptions {
                    flush_type: slatedb::config::FlushType::MemTable,
                })
                .await
                .map_err(JobStoreShardError::from)?;

            tracing::debug!(
                shard = %self.name,
                "memtable flushed to SST, closing database"
            );
        }

        // Close the database
        tracing::trace!(shard = %self.name, "shard.close: calling db.close()");
        self.db.close().await.map_err(JobStoreShardError::from)?;
        tracing::trace!(shard = %self.name, "shard.close: db.close() completed");

        // After closing, clean up the local WAL directory if configured
        if let Some(ref wal_config) = self.wal_close_config
            && wal_config.flush_on_close
        {
            tracing::info!(
                shard = %self.name,
                wal_path = %wal_config.path,
                "removing local WAL directory after successful close"
            );

            if let Err(e) = tokio::fs::remove_dir_all(&wal_config.path).await {
                // Log but don't fail if WAL directory removal fails
                // The data is already durably in object storage
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        shard = %self.name,
                        wal_path = %wal_config.path,
                        error = %e,
                        "failed to remove local WAL directory (data is still safe in object storage)"
                    );
                }
            } else {
                tracing::debug!(
                    shard = %self.name,
                    wal_path = %wal_config.path,
                    "local WAL directory removed successfully"
                );
            }
        }

        Ok(())
    }

    /// Returns the WAL close configuration, if any.
    /// This is primarily used for testing to verify WAL cleanup behavior.
    pub fn wal_close_config(&self) -> Option<&WalCloseConfig> {
        self.wal_close_config.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Get the SlateDB metrics registry for this shard.
    /// Use this to collect storage-level statistics for observability.
    pub fn slatedb_stats(&self) -> std::sync::Arc<slatedb::stats::StatRegistry> {
        self.db.metrics()
    }

    /// Get the shard's tenant range.
    ///
    /// The range is immutable after the shard is opened. When shards split,
    /// new child shards are created with new identities and ranges.
    pub fn get_range(&self) -> ShardRange {
        self.broker.get_range()
    }

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobView>, JobStoreShardError> {
        let key = job_info_key(tenant, id);
        match self.db.get(&key).await? {
            Some(raw) => Ok(Some(JobView::new(raw)?)),
            None => Ok(None),
        }
    }

    /// Fetch multiple jobs by id concurrently. Returns a map of job_id -> JobView.
    /// Jobs that don't exist will not be present in the result map.
    pub async fn get_jobs_batch(
        &self,
        tenant: &str,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, JobView>, JobStoreShardError> {
        use std::collections::HashMap;

        let mut map = HashMap::with_capacity(ids.len());
        for id in ids {
            let key = job_info_key(tenant, id);
            if let Some(raw) = self.db.get(&key).await? {
                map.insert(id.clone(), JobView::new(raw)?);
            }
        }
        Ok(map)
    }

    /// Fetch a job status by id as a zero-copy archived view.
    pub async fn get_job_status(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobStatus>, JobStoreShardError> {
        let key = job_status_key(tenant, id);
        let maybe_raw = self.db.get(&key).await?;
        let Some(raw) = maybe_raw else {
            return Ok(None);
        };

        Ok(Some(helpers::decode_job_status_owned(&raw)?))
    }

    /// Fetch multiple job statuses by id concurrently. Returns a map of job_id -> JobStatus.
    /// Jobs that don't exist will not be present in the result map.
    pub async fn get_jobs_status_batch(
        &self,
        tenant: &str,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, JobStatus>, JobStoreShardError> {
        use std::collections::HashMap;

        let mut map = HashMap::with_capacity(ids.len());
        for id in ids {
            let key = job_status_key(tenant, id);
            if let Some(raw) = self.db.get(&key).await? {
                map.insert(id.clone(), helpers::decode_job_status_owned(&raw)?);
            }
        }
        Ok(map)
    }

    /// Fetch a job attempt by job id and attempt number.
    pub async fn get_job_attempt(
        &self,
        tenant: &str,
        job_id: &str,
        attempt_number: u32,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let key = attempt_key(tenant, job_id, attempt_number);
        match self.db.get(&key).await? {
            Some(raw) => Ok(Some(JobAttemptView::new(raw)?)),
            None => Ok(None),
        }
    }

    /// Fetch all attempts for a job, ordered by attempt number (ascending).
    /// Returns an empty vector if the job has no attempts.
    pub async fn get_job_attempts(
        &self,
        tenant: &str,
        job_id: &str,
    ) -> Result<Vec<JobAttemptView>, JobStoreShardError> {
        use slatedb::DbIterator;

        let start = crate::keys::attempt_prefix(tenant, job_id);
        let end = crate::keys::end_bound(&start);

        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut attempts = Vec::new();

        while let Some(kv) = iter.next().await? {
            let view = JobAttemptView::new(&kv.value)?;
            attempts.push(view);
        }

        Ok(attempts)
    }

    /// Get the latest (highest numbered) attempt for a job, if any.
    /// This is typically the attempt that contains the final result for completed jobs.
    pub async fn get_latest_job_attempt(
        &self,
        tenant: &str,
        job_id: &str,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let attempts = self.get_job_attempts(tenant, job_id).await?;
        Ok(attempts.into_iter().last())
    }

    /// Delete a job by id.
    ///
    /// Returns an error if the job is currently running (has active leases/holders) or has pending tasks/requests. Jobs must finish or permanently fail before deletion.
    pub async fn delete_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        use crate::keys::idx_metadata_key;
        use slatedb::WriteBatch;

        // Check if job is running or has pending state
        let status = self.get_job_status(tenant, id).await?;
        if let Some(ref status) = status
            && !status.is_terminal()
        {
            return Err(JobStoreShardError::JobInProgress(id.to_string()));
        }

        // Check if job even exists (status.is_none() means job doesn't exist)
        // If job doesn't exist, just return Ok (idempotent delete)
        let job_info_key_bytes = job_info_key(tenant, id);
        if status.is_none() && self.db.get(&job_info_key_bytes).await?.is_none() {
            return Ok(());
        }

        let job_status_key_bytes = job_status_key(tenant, id);
        let mut batch = WriteBatch::new();
        // Clean up secondary index entries if present
        if let Some(ref status) = status {
            let timek = crate::keys::idx_status_time_key(
                tenant,
                status.kind.as_str(),
                status.changed_at_ms,
                id,
            );
            batch.delete(&timek);
        }
        // Clean up metadata index entries (load job info to enumerate metadata)
        if let Some(raw) = self.db.get(&job_info_key_bytes).await? {
            let view = JobView::new(raw)?;
            for (mk, mv) in view.metadata().into_iter() {
                let mkey = idx_metadata_key(tenant, &mk, &mv, id);
                batch.delete(&mkey);
            }
        }
        batch.delete(&job_info_key_bytes);
        batch.delete(&job_status_key_bytes);
        // Also delete cancellation record if present
        batch.delete(crate::keys::job_cancelled_key(tenant, id));

        // Update counters in the same batch
        let mut writer = DbWriteBatcher::new(&self.db, &mut batch);
        self.decrement_total_jobs_counter(&mut writer)?;
        if status.is_some() {
            // Job was in terminal state (we already checked it's terminal above)
            self.decrement_completed_jobs_counter(&mut writer)?;
        }

        self.db.write(batch).await?;
        self.db.flush().await?;

        Ok(())
    }
}
