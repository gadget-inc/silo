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
mod floating;

pub(crate) use enqueue::NextLimitContext;
mod helpers;
mod lease;
mod rate_limit;
mod restart;
mod scan;
mod start_now;

pub use restart::JobNotRestartableError;
pub use start_now::JobNotStartableError;

pub use helpers::now_epoch_ms;

use slatedb::Db;
use std::sync::{Arc, OnceLock};
use thiserror::Error;

use crate::codec::CodecError;
use crate::concurrency::ConcurrencyManager;
use crate::gubernator::{NullGubernatorClient, RateLimitClient};
use crate::job::{JobStatus, JobStatusKind, JobView};
use crate::job_attempt::JobAttemptView;
use crate::keys::{attempt_key, job_info_key, job_status_key};
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

/// A cross-shard RPC task that needs to be executed by the caller.
/// These are returned from dequeue because they require network calls that
/// the JobStoreShard layer doesn't handle directly.
#[derive(Debug, Clone)]
pub enum PendingCrossShardTask {
    /// Request a concurrency ticket from a remote queue owner
    RequestRemoteTicket {
        queue_owner_shard: u32,
        tenant: String,
        queue_key: String,
        job_id: String,
        request_id: String,
        attempt_number: u32,
        priority: u8,
        start_time_ms: i64,
        max_concurrency: u32,
        /// Key to delete from DB on successful RPC
        task_key: String,
        /// For floating limits, includes the full definition so queue owner can create state.
        /// None for regular ConcurrencyLimit.
        floating_limit: Option<crate::task::FloatingConcurrencyLimitData>,
    },
    /// Notify a remote job shard that a ticket was granted
    NotifyRemoteTicketGrant {
        job_shard: u32,
        tenant: String,
        job_id: String,
        queue_key: String,
        request_id: String,
        holder_task_id: String,
        attempt_number: u32,
        /// Key to delete from DB on successful RPC
        task_key: String,
    },
    /// Release a ticket back to a remote queue owner
    ReleaseRemoteTicket {
        queue_owner_shard: u32,
        tenant: String,
        queue_key: String,
        job_id: String,
        holder_task_id: String,
        /// Key to delete from DB on successful RPC
        task_key: String,
    },
}

/// Result of a dequeue operation - contains both job tasks and floating limit refresh tasks
#[derive(Debug, Default)]
pub struct DequeueResult {
    pub tasks: Vec<LeasedTask>,
    pub refresh_tasks: Vec<LeasedRefreshTask>,
    /// Cross-shard tasks that need to be processed by the caller via RPC.
    /// These are returned because they require network calls.
    pub cross_shard_tasks: Vec<PendingCrossShardTask>,
}

/// Represents a single shard of the system. Owns the SlateDB instance.
pub struct JobStoreShard {
    pub(crate) name: String,
    /// This shard's number in the cluster (0-indexed)
    pub(crate) shard_number: u32,
    /// Total number of shards in the cluster
    pub(crate) num_shards: u32,
    pub(crate) db: Arc<Db>,
    pub(crate) broker: Arc<TaskBroker>,
    pub(crate) concurrency: Arc<ConcurrencyManager>,
    query_engine: OnceLock<ShardQueryEngine>,
    pub(crate) rate_limiter: Arc<dyn RateLimitClient>,
    /// Optional WAL close configuration - present when using local WAL storage
    wal_close_config: Option<WalCloseConfig>,
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
    #[error("cannot start job now: {0}")]
    JobNotStartable(#[from] JobNotStartableError),
    #[error("transaction conflict during {0}, exceeded max retries")]
    TransactionConflict(String),
}

impl From<CodecError> for JobStoreShardError {
    fn from(e: CodecError) -> Self {
        JobStoreShardError::Rkyv(e.to_string())
    }
}

impl JobStoreShard {
    /// Open a shard with a NullGubernatorClient (rate limits will error).
    /// Uses single-shard mode (shard 0 of 1) for backwards compatibility.
    pub async fn open(cfg: &DatabaseConfig) -> Result<Arc<Self>, JobStoreShardError> {
        Self::open_with_rate_limiter(cfg, NullGubernatorClient::new(), 0, 1).await
    }

    /// Open a shard with a rate limit client and cluster configuration.
    ///
    /// # Arguments
    /// * `cfg` - Database configuration
    /// * `rate_limiter` - Rate limit client implementation
    /// * `shard_number` - This shard's number in the cluster (0-indexed)
    /// * `num_shards` - Total number of shards in the cluster
    pub async fn open_with_rate_limiter(
        cfg: &DatabaseConfig,
        rate_limiter: Arc<dyn RateLimitClient>,
        shard_number: u32,
        num_shards: u32,
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
        let broker = TaskBroker::new(Arc::clone(&db), Arc::clone(&concurrency));
        broker.start();

        let shard = Arc::new(Self {
            name: cfg.name.clone(),
            shard_number,
            num_shards,
            db,
            broker,
            concurrency,
            query_engine: OnceLock::new(),
            rate_limiter,
            wal_close_config,
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

    /// Returns this shard's number in the cluster (0-indexed).
    pub fn shard_number(&self) -> u32 {
        self.shard_number
    }

    /// Returns the total number of shards in the cluster.
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    // ============================================================================
    // Debug assertion helpers for verifying shard ownership
    // ============================================================================

    /// Debug assertion: verify this shard owns the given queue.
    /// Panics in debug builds if the queue routes to a different shard.
    #[inline]
    pub(crate) fn debug_assert_owns_queue(&self, tenant: &str, queue_key: &str) {
        debug_assert_eq!(
            crate::routing::queue_to_shard(tenant, queue_key, self.num_shards),
            self.shard_number,
            "Queue '{}' for tenant '{}' routes to shard {}, but this is shard {}",
            queue_key,
            tenant,
            crate::routing::queue_to_shard(tenant, queue_key, self.num_shards),
            self.shard_number
        );
    }

    /// Debug assertion: verify this shard owns the given job.
    /// Panics in debug builds if the job routes to a different shard.
    #[inline]
    pub(crate) fn debug_assert_owns_job(&self, job_id: &str) {
        debug_assert_eq!(
            crate::routing::job_to_shard(job_id, self.num_shards),
            self.shard_number,
            "Job '{}' routes to shard {}, but this is shard {}",
            job_id,
            crate::routing::job_to_shard(job_id, self.num_shards),
            self.shard_number
        );
    }

    /// Debug assertion: verify this shard does NOT own the given queue (it's remote).
    /// Panics in debug builds if the queue routes to this shard.
    #[inline]
    pub(crate) fn debug_assert_queue_is_remote(&self, tenant: &str, queue_key: &str) {
        debug_assert_ne!(
            crate::routing::queue_to_shard(tenant, queue_key, self.num_shards),
            self.shard_number,
            "Queue '{}' for tenant '{}' routes to shard {} (same as this shard). \
             Expected a remote queue.",
            queue_key,
            tenant,
            crate::routing::queue_to_shard(tenant, queue_key, self.num_shards)
        );
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

    // ============================================================================
    // Cross-shard concurrency ticket coordination methods
    // ============================================================================

    /// Receive a remote ticket request from another shard (queue owner perspective).
    /// This stores the request and potentially grants it immediately if there's capacity.
    /// For floating limits, also creates/updates the floating limit state on this shard.
    ///
    /// Returns true if the ticket was granted immediately.
    #[allow(clippy::too_many_arguments)]
    pub async fn receive_remote_ticket_request(
        &self,
        tenant: &str,
        queue_key: &str,
        job_id: &str,
        job_shard: u32,
        request_id: &str,
        attempt_number: u32,
        priority: u8,
        start_time_ms: i64,
        max_concurrency: u32,
        floating_limit: Option<&crate::task::FloatingConcurrencyLimitData>,
    ) -> Result<bool, JobStoreShardError> {
        use crate::codec::{
            decode_floating_limit_state, encode_remote_concurrency_request,
            encode_remote_holder_record,
        };
        use crate::keys::{concurrency_request_key, floating_limit_state_key};
        use crate::task::{RemoteConcurrencyRequest, RemoteHolderRecord, Task};
        use slatedb::WriteBatch;

        // Debug assertion: verify this shard owns the queue
        self.debug_assert_owns_queue(tenant, queue_key);

        let now_ms = helpers::now_epoch_ms();

        // Generate a unique holder task ID for this request
        let holder_task_id = uuid::Uuid::new_v4().to_string();

        let mut batch = WriteBatch::new();

        // Determine effective max concurrency, handling floating limits
        let effective_max_concurrency = if let Some(fl) = floating_limit {
            // Floating limit - ensure state exists and potentially schedule refresh
            self.ensure_floating_limit_state_for_remote(&mut batch, tenant, queue_key, fl, now_ms)
                .await?
        } else {
            // Not a floating limit - check if there's existing floating state anyway
            // (could happen if same queue is used with both limit types)
            let state_key = floating_limit_state_key(tenant, queue_key);
            if let Some(state_bytes) = self.db.get(state_key.as_bytes()).await? {
                if let Ok(decoded) = decode_floating_limit_state(&state_bytes) {
                    decoded.archived().current_max_concurrency
                } else {
                    max_concurrency
                }
            } else {
                max_concurrency
            }
        };

        // [SILO-GRANT-REMOTE-1] Check if we can grant immediately (queue has capacity)
        let can_grant = self
            .concurrency
            .counts()
            .can_grant(tenant, queue_key, effective_max_concurrency as usize);

        // [SILO-GRANT-REMOTE-2] Check if there is a pending request (this is the request)
        // [SILO-GRANT-REMOTE-3] Job cancellation is checked via gRPC layer/job shard
        // [SILO-GRANT-REMOTE-4] Queue is remote from job's shard (caller verified this)
        if can_grant && start_time_ms <= now_ms {
            // Grant immediately:
            // 1. Create RemoteHolderRecord
            // 2. Create NotifyRemoteTicketGrant task
            // Note: held_queues are tracked on the job shard side, not passed through RPC

            // [SILO-GRANT-REMOTE-5] Create holder for this task/queue (on queue owner)
            let holder = RemoteHolderRecord {
                granted_at_ms: now_ms,
                source_shard: job_shard,
                job_id: job_id.to_string(),
            };
            let holder_val = encode_remote_holder_record(&holder)?;
            batch.put(
                crate::keys::concurrency_holder_key(tenant, queue_key, &holder_task_id).as_bytes(),
                &holder_val,
            );

            // [SILO-GRANT-REMOTE-6] Remove the request (it's being granted inline, no stored request)
            // [SILO-GRANT-REMOTE-7] Create NotifyRemoteTicketGrant task in DB queue
            let notify_task = Task::NotifyRemoteTicketGrant {
                task_id: uuid::Uuid::new_v4().to_string(),
                job_shard,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                queue_key: queue_key.to_string(),
                request_id: request_id.to_string(),
                holder_task_id: holder_task_id.clone(),
                attempt_number,
            };
            // [SILO-GRANT-REMOTE-8] Create NotifyRemoteTicketGrantTask record
            let task_val = crate::codec::encode_task(&notify_task)?;
            batch.put(
                crate::keys::task_key(now_ms, 0, &holder_task_id, 0).as_bytes(),
                &task_val,
            );

            // Update in-memory counts
            self.concurrency
                .counts()
                .record_grant(tenant, queue_key, &holder_task_id);

            self.db.write(batch).await?;

            tracing::debug!(
                tenant = tenant,
                queue_key = queue_key,
                job_id = job_id,
                job_shard = job_shard,
                request_id = request_id,
                holder_task_id = holder_task_id,
                "receive_remote_ticket_request: granted immediately"
            );

            Ok(true)
        } else {
            // No capacity or future start time: store request for later
            let request = RemoteConcurrencyRequest {
                source_shard: job_shard,
                job_id: job_id.to_string(),
                request_id: request_id.to_string(),
                attempt_number,
                holder_task_id: holder_task_id.clone(),
                max_concurrency,
            };
            let request_val = encode_remote_concurrency_request(&request)?;

            // Store under requests/<tenant>/<queue>/<start_time_ms>/<priority>/<request_id>
            let req_key = concurrency_request_key(tenant, queue_key, start_time_ms, priority, request_id);
            batch.put(req_key.as_bytes(), &request_val);

            self.db.write(batch).await?;

            tracing::debug!(
                tenant = tenant,
                queue_key = queue_key,
                job_id = job_id,
                job_shard = job_shard,
                request_id = request_id,
                start_time_ms = start_time_ms,
                can_grant = can_grant,
                "receive_remote_ticket_request: queued for later"
            );

            Ok(false)
        }
    }

    /// Receive notification that a remote ticket was granted (job shard perspective).
    /// This creates a RunAttempt task or continues to the next limit.
    #[allow(clippy::too_many_arguments)]
    pub async fn receive_remote_ticket_grant(
        &self,
        tenant: &str,
        job_id: &str,
        queue_key: &str,
        request_id: &str,
        holder_task_id: &str,
        queue_owner_shard: u32,
        attempt_number: u32,
    ) -> Result<(), JobStoreShardError> {
        use crate::codec::decode_held_ticket_state;
        use crate::job::Limit;
        use crate::keys::held_ticket_state_key;
        use crate::task::Task;
        use slatedb::WriteBatch;

        // Debug assertion: verify this shard owns the job
        self.debug_assert_owns_job(job_id);

        // Debug assertion: verify the queue is NOT on this shard (it's remote)
        self.debug_assert_queue_is_remote(tenant, queue_key);

        let now_ms = helpers::now_epoch_ms();

        // Load pending ticket state from local storage to get held_queues
        let state_key = held_ticket_state_key(job_id, attempt_number, request_id);
        let held_queues = if let Some(state_bytes) = self.db.get(state_key.as_bytes()).await? {
            let state = decode_held_ticket_state(&state_bytes)?;
            state.held_queues()
        } else {
            // No state found - this is the first limit (no prior held queues)
            Vec::new()
        };

        // Look up the job to get priority, limits, and other info
        let job = self
            .get_job(tenant, job_id)
            .await?
            .ok_or_else(|| JobStoreShardError::JobNotFound(job_id.to_string()))?;

        let priority = job.priority();
        let start_at_ms = job.enqueue_time_ms();
        let limits = job.limits();

        // Track this remote queue in the held_queues format.
        // This allows report_attempt_outcome to know to send a release RPC for remote queues.
        let mut all_held_queues = held_queues;
        all_held_queues.push(crate::task::RemoteQueueRef::format(
            queue_owner_shard,
            queue_key,
            holder_task_id,
        ));

        // Find the index of the current limit by matching queue_key
        let current_limit_index = limits
            .iter()
            .position(|l| match l {
                Limit::Concurrency(cl) => cl.key == queue_key,
                Limit::FloatingConcurrency(fl) => fl.key == queue_key,
                Limit::RateLimit(_) => false,
            })
            .unwrap_or(0) as u32;

        let mut batch = WriteBatch::new();

        // Delete the pending ticket state (we've consumed it)
        batch.delete(state_key.as_bytes());

        // Check if there are more limits to process
        if (current_limit_index + 1) < limits.len() as u32 {
            // More limits to process - enqueue the next limit task
            self.enqueue_next_limit_task(
                &mut batch,
                NextLimitContext {
                    tenant,
                    task_id: request_id, // Use same task_id for holder consistency
                    job_id,
                    attempt_number,
                    current_limit_index,
                    limits: &limits,
                    priority,
                    start_at_ms,
                    now_ms,
                    held_queues: all_held_queues,
                },
            )?;

            tracing::debug!(
                tenant = tenant,
                job_id = job_id,
                queue_key = queue_key,
                current_limit_index = current_limit_index,
                next_limit_index = current_limit_index + 1,
                "receive_remote_ticket_grant: more limits to process, enqueued next limit task"
            );
        } else {
            // This was the last limit - create RunAttempt
            let task = Task::RunAttempt {
                id: request_id.to_string(),
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                held_queues: all_held_queues,
            };

            let task_val = crate::codec::encode_task(&task)?;
            let task_key_str = crate::keys::task_key(now_ms, priority, job_id, attempt_number);
            batch.put(task_key_str.as_bytes(), &task_val);

            tracing::debug!(
                tenant = tenant,
                job_id = job_id,
                queue_key = queue_key,
                request_id = request_id,
                holder_task_id = holder_task_id,
                queue_owner_shard = queue_owner_shard,
                task_key = task_key_str,
                "receive_remote_ticket_grant: all limits satisfied, created RunAttempt task"
            );
        }

        self.db.write(batch).await?;
        Ok(())
    }

    /// Delete a task key after a cross-shard RPC has been successfully processed.
    /// Called by the caller after processing a PendingCrossShardTask.
    pub async fn delete_cross_shard_task(&self, task_key: &str) -> Result<(), JobStoreShardError> {
        use slatedb::WriteBatch;
        let mut batch = WriteBatch::new();
        batch.delete(task_key.as_bytes());
        self.db.write(batch).await?;
        Ok(())
    }

    /// Release a remote ticket (queue owner perspective).
    /// This removes the holder and potentially grants to the next requester.
    pub async fn release_remote_ticket(
        &self,
        tenant: &str,
        queue_key: &str,
        job_id: &str,
        holder_task_id: &str,
    ) -> Result<(), JobStoreShardError> {
        use slatedb::WriteBatch;

        // Debug assertion: verify this shard owns the queue
        self.debug_assert_owns_queue(tenant, queue_key);

        let now_ms = helpers::now_epoch_ms();
        let mut batch = WriteBatch::new();

        // Release the holder and grant to the next request
        // This reuses the existing release_and_grant_next logic
        let events = self
            .concurrency
            .release_and_grant_next(
                &self.db,
                &mut batch,
                tenant,
                &[queue_key.to_string()],
                holder_task_id,
                now_ms,
            )
            .await
            .map_err(|e| JobStoreShardError::Rkyv(e))?;

        self.db.write(batch).await?;

        // Update in-memory counts
        for event in &events {
            match event {
                crate::concurrency::MemoryEvent::Released { queue, task_id } => {
                    self.concurrency.counts().record_release(tenant, queue, task_id);
                }
                crate::concurrency::MemoryEvent::Granted { queue, task_id } => {
                    self.concurrency.counts().record_grant(tenant, queue, task_id);
                }
            }
        }

        tracing::debug!(
            tenant = tenant,
            queue_key = queue_key,
            job_id = job_id,
            holder_task_id = holder_task_id,
            events_count = events.len(),
            "release_remote_ticket: released and granted next"
        );

        Ok(())
    }
}
