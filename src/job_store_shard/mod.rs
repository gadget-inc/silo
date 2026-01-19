//! Job store shard - a single shard of the distributed job storage system.
//!
//! This module contains the core `JobStoreShard` type and its implementation,
//! split across multiple submodules for organization:
//!
//! - `helpers`: Utility functions for encoding, timestamps, etc.
//! - `enqueue`: Job enqueue logic
//! - `dequeue`: Task dequeue and processing
//! - `lease`: Lease management, heartbeat, and reaping
//! - `cancel`: Job cancellation
//! - `floating`: Floating concurrency limit operations
//! - `rate_limit`: Rate limit checking via Gubernator
//! - `scan`: Scanning and query operations

mod cancel;
mod dequeue;
mod enqueue;
mod expedite;
mod floating;
mod helpers;
mod lease;
mod rate_limit;
mod restart;
mod scan;

pub use expedite::JobNotExpediteableError;
pub use restart::JobNotRestartableError;

pub use helpers::now_epoch_ms;

use slatedb::Db;
use std::sync::{Arc, OnceLock};
use thiserror::Error;

use crate::codec::CodecError;
use crate::concurrency::ConcurrencyManager;
use crate::gubernator::RateLimitClient;
use crate::job::{JobStatus, JobStatusKind, JobView};
use crate::job_attempt::JobAttemptView;
use crate::keys::{attempt_key, job_info_key, job_status_key};
use crate::metrics::Metrics;
use crate::query::ShardQueryEngine;
use crate::settings::DatabaseConfig;
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
    #[error("transaction conflict during {0}, exceeded max retries")]
    TransactionConflict(String),
}

impl From<CodecError> for JobStoreShardError {
    fn from(e: CodecError) -> Self {
        JobStoreShardError::Rkyv(e.to_string())
    }
}

impl JobStoreShard {
    /// Open a shard with a rate limit client and optional metrics
    pub async fn open(
        cfg: &DatabaseConfig,
        rate_limiter: Arc<dyn RateLimitClient>,
        metrics: Option<Metrics>,
    ) -> Result<Arc<Self>, JobStoreShardError> {
        let resolved = resolve_object_store(&cfg.backend, &cfg.path)?;

        // Use the canonical path for both object store AND DbBuilder to ensure consistency
        let mut db_builder =
            slatedb::DbBuilder::new(resolved.canonical_path.as_str(), resolved.store);

        // Configure separate WAL object store if specified, and track for cleanup on close
        let wal_close_config = if let Some(wal_cfg) = &cfg.wal {
            let wal_resolved = resolve_object_store(&wal_cfg.backend, &wal_cfg.path)?;
            db_builder = db_builder.with_wal_object_store(wal_resolved.store);

            // Only set up WAL cleanup for local (Fs) storage backends
            if wal_cfg.is_local_storage() {
                Some(WalCloseConfig {
                    path: wal_resolved.canonical_path,
                    flush_on_close: cfg.apply_wal_on_close,
                })
            } else {
                None
            }
        } else {
            None
        };

        // Apply custom flush interval if specified
        if let Some(flush_ms) = cfg.flush_interval_ms {
            let settings = slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(flush_ms)),
                ..Default::default()
            };
            db_builder = db_builder.with_settings(settings);
        }

        let db = db_builder.build().await?;
        let db = Arc::new(db);
        let concurrency = Arc::new(ConcurrencyManager::new());
        let broker = TaskBroker::new(
            Arc::clone(&db),
            Arc::clone(&concurrency),
            cfg.name.clone(),
            metrics.clone(),
        );
        broker.start();

        let shard = Arc::new(Self {
            name: cfg.name.clone(),
            db,
            broker,
            concurrency,
            query_engine: OnceLock::new(),
            rate_limiter,
            wal_close_config,
            metrics,
        });

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
        self.broker.stop();

        // If we have a local WAL with flush_on_close enabled, flush memtable to SSTs first
        if let Some(ref wal_config) = self.wal_close_config {
            if wal_config.flush_on_close {
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
        }

        // Close the database
        self.db.close().await.map_err(JobStoreShardError::from)?;

        // After closing, clean up the local WAL directory if configured
        if let Some(ref wal_config) = self.wal_close_config {
            if wal_config.flush_on_close {
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

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobView>, JobStoreShardError> {
        let key = job_info_key(tenant, id);
        match self.db.get(key.as_bytes()).await? {
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

        let mut handles = Vec::new();
        for id in ids {
            let db = Arc::clone(&self.db);
            let key = job_info_key(tenant, id);
            let id_clone = id.clone();
            let handle = tokio::spawn(async move {
                let maybe_raw = db.get(key.as_bytes()).await?;
                if let Some(raw) = maybe_raw {
                    let view = JobView::new(raw)?;
                    Ok::<_, JobStoreShardError>(Some((id_clone, view)))
                } else {
                    Ok(None)
                }
            });
            handles.push(handle);
        }

        let mut result = HashMap::with_capacity(ids.len());
        for handle in handles {
            match handle.await {
                Ok(Ok(Some((id, view)))) => {
                    result.insert(id, view);
                }
                Ok(Ok(None)) => {
                    // Job doesn't exist, skip
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(JobStoreShardError::Rkyv(format!("Task join error: {}", e))),
            }
        }
        Ok(result)
    }

    /// Fetch a job status by id as a zero-copy archived view.
    pub async fn get_job_status(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobStatus>, JobStoreShardError> {
        let key = job_status_key(tenant, id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
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

        let mut handles = Vec::new();
        for id in ids {
            let db = Arc::clone(&self.db);
            let key = job_status_key(tenant, id);
            let id_clone = id.clone();
            let handle = tokio::spawn(async move {
                let maybe_raw = db.get(key.as_bytes()).await?;
                let Some(raw) = maybe_raw else {
                    return Ok::<_, JobStoreShardError>(None);
                };
                let status = helpers::decode_job_status_owned(&raw)?;
                Ok(Some((id_clone, status)))
            });
            handles.push(handle);
        }

        let mut result = HashMap::with_capacity(ids.len());
        for handle in handles {
            match handle.await {
                Ok(Ok(Some((id, status)))) => {
                    result.insert(id, status);
                }
                Ok(Ok(None)) => {
                    // Status doesn't exist, skip
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(JobStoreShardError::Rkyv(format!("Task join error: {}", e))),
            }
        }
        Ok(result)
    }

    /// Fetch a job attempt by job id and attempt number.
    pub async fn get_job_attempt(
        &self,
        tenant: &str,
        job_id: &str,
        attempt_number: u32,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let key = attempt_key(tenant, job_id, attempt_number);
        match self.db.get(key.as_bytes()).await? {
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

        let prefix = crate::keys::attempt_prefix(tenant, job_id);
        let start = prefix.as_bytes().to_vec();
        let mut end = start.clone();
        end.push(0xFF);

        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;
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
        if let Some(status) = status {
            if !status.is_terminal() {
                return Err(JobStoreShardError::JobInProgress(id.to_string()));
            }
        }

        let job_info_key_str: String = job_info_key(tenant, id);
        let job_status_key_str: String = job_status_key(tenant, id);
        let mut batch = WriteBatch::new();
        // Clean up secondary index entries if present
        if let Some(status) = self.get_job_status(tenant, id).await? {
            let timek = crate::keys::idx_status_time_key(
                tenant,
                status.kind.as_str(),
                status.changed_at_ms,
                id,
            );
            batch.delete(timek.as_bytes());
        }
        // Clean up metadata index entries (load job info to enumerate metadata)
        if let Some(raw) = self.db.get(job_info_key_str.as_bytes()).await? {
            let view = JobView::new(raw)?;
            for (mk, mv) in view.metadata().into_iter() {
                let mkey = idx_metadata_key(tenant, &mk, &mv, id);
                batch.delete(mkey.as_bytes());
            }
        }
        batch.delete(job_info_key_str.as_bytes());
        batch.delete(job_status_key_str.as_bytes());
        // Also delete cancellation record if present
        batch.delete(crate::keys::job_cancelled_key(tenant, id).as_bytes());
        self.db.write(batch).await?;
        self.db.flush().await?;
        Ok(())
    }
}
