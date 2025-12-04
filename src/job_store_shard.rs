use rkyv::Deserialize as RkyvDeserialize;
use serde_json::Value as JsonValue;
use slatedb::Db;
use slatedb::DbIterator;
use slatedb::ErrorKind as SlateErrorKind;
use slatedb::IsolationLevel;
use slatedb::WriteBatch;
use std::sync::{Arc, OnceLock};
use thiserror::Error;
use uuid::Uuid;

use crate::codec::{
    decode_floating_limit_state, decode_job_cancellation, decode_job_status, decode_lease,
    decode_task, encode_attempt, encode_floating_limit_state, encode_job_cancellation,
    encode_job_info, encode_job_status, encode_lease, encode_task, CodecError,
};
use crate::concurrency::{
    ConcurrencyManager, MemoryEvent, RequestTicketOutcome, RequestTicketTaskOutcome,
};
use crate::gubernator::{GubernatorError, NullGubernatorClient, RateLimitClient, RateLimitResult};
use crate::job::{
    FloatingConcurrencyLimit, FloatingLimitState, JobCancellation, JobInfo, JobStatus,
    JobStatusKind, JobView, Limit,
};
use crate::job_attempt::{AttemptOutcome, AttemptStatus, JobAttempt, JobAttemptView};
use crate::keys::{
    attempt_key, floating_limit_state_key, idx_metadata_key, idx_metadata_prefix,
    idx_status_time_key, job_cancelled_key, job_info_key, job_status_key, leased_task_key,
    task_key,
};
use crate::query::JobSql;
use crate::retry::RetryPolicy;
use crate::settings::DatabaseConfig;
use crate::storage::{resolve_object_store, StorageError};
use crate::task::GubernatorRateLimitData;
use crate::task_broker::{BrokerTask, TaskBroker};
use tracing::{debug, info, info_span};

// Re-export commonly used types from task module for convenience
pub use crate::task::{
    ConcurrencyAction, HeartbeatResult, HolderRecord, LeaseRecord, LeasedTask, Task,
    DEFAULT_LEASE_MS,
};

/// A leased refresh task for floating concurrency limits
#[derive(Debug, Clone)]
pub struct LeasedRefreshTask {
    pub task_id: String,
    pub queue_key: String,
    pub current_max_concurrency: u32,
    pub last_refreshed_at_ms: i64,
    pub metadata: Vec<(String, String)>,
}

/// Result of a dequeue operation - contains both job tasks and floating limit refresh tasks
#[derive(Debug, Default)]
pub struct DequeueResult {
    pub tasks: Vec<LeasedTask>,
    pub refresh_tasks: Vec<LeasedRefreshTask>,
}

/// Represents a single shard of the system. Owns the SlateDB instance.
pub struct JobStoreShard {
    name: String,
    db: Arc<Db>,
    broker: Arc<TaskBroker>,
    concurrency: Arc<ConcurrencyManager>,
    query_engine: OnceLock<JobSql>,
    rate_limiter: Arc<dyn RateLimitClient>,
}

#[derive(Debug, Error)]
pub enum JobStoreShardError {
    #[error(transparent)]
    Storage(#[from] StorageError),
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
    #[error("transaction conflict during {0}, exceeded max retries")]
    TransactionConflict(String),
}

impl JobStoreShard {
    /// Open a shard with a NullGubernatorClient (rate limits will error)
    pub async fn open(cfg: &DatabaseConfig) -> Result<Arc<Self>, JobStoreShardError> {
        Self::open_with_rate_limiter(cfg, NullGubernatorClient::new()).await
    }

    /// Open a shard with a rate limit client
    pub async fn open_with_rate_limiter(
        cfg: &DatabaseConfig,
        rate_limiter: Arc<dyn RateLimitClient>,
    ) -> Result<Arc<Self>, JobStoreShardError> {
        let resolved = resolve_object_store(&cfg.backend, &cfg.path)?;

        // Use the canonical path for both object store AND DbBuilder to ensure consistency
        let mut db_builder =
            slatedb::DbBuilder::new(resolved.canonical_path.as_str(), resolved.store);

        // Configure separate WAL object store if specified
        if let Some(wal_cfg) = &cfg.wal {
            let wal_resolved = resolve_object_store(&wal_cfg.backend, &wal_cfg.path)?;
            db_builder = db_builder.with_wal_object_store(wal_resolved.store);
        }

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
            db,
            broker,
            concurrency,
            query_engine: OnceLock::new(),
            rate_limiter,
        });

        Ok(shard)
    }

    /// Get the query engine for this shard, lazily initializing it on first access.
    pub fn query_engine(self: &Arc<Self>) -> &JobSql {
        self.query_engine.get_or_init(|| {
            JobSql::new(Arc::clone(self), "jobs").expect("Failed to create query engine")
        })
    }

    /// Close the underlying SlateDB instance gracefully.
    pub async fn close(&self) -> Result<(), JobStoreShardError> {
        self.broker.stop();
        self.db.close().await.map_err(JobStoreShardError::from)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Enqueue a new job with optional limits (concurrency and/or rate limits).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        tenant: &str,
        id: Option<String>,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: JsonValue,
        limits: Vec<Limit>,
        metadata: Option<Vec<(String, String)>>,
    ) -> Result<String, JobStoreShardError> {
        let job_id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        // [SILO-ENQ-1] If caller provided an id, ensure it doesn't already exist
        if self
            .db
            .get(job_info_key(tenant, &job_id).as_bytes())
            .await?
            .is_some()
        {
            return Err(JobStoreShardError::JobAlreadyExists(job_id));
        }
        let payload_bytes = serde_json::to_vec(&payload)?;

        let job = JobInfo {
            id: job_id.clone(),
            priority,
            enqueue_time_ms: start_at_ms,
            payload: payload_bytes,
            retry_policy,
            metadata: metadata.unwrap_or_default(),
            limits: limits.clone(),
        };
        let job_value = encode_job_info(&job).map_err(codec_error_to_shard_error)?;

        let first_task_id = Uuid::new_v4().to_string();
        let now_ms = now_epoch_ms();
        // [SILO-ENQ-2] Create job with status Scheduled
        let job_status = JobStatus::scheduled(now_ms);

        // Atomically write job info, job status, and handle first limit
        let mut batch = WriteBatch::new();
        batch.put(job_info_key(tenant, &job_id).as_bytes(), &job_value);
        // Maintain metadata secondary index (metadata is immutable post-enqueue)
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, &job_id);
            batch.put(mkey.as_bytes(), []);
        }
        self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
            .await?;

        // Process the first limit in the list
        let first_limit = limits.first();
        let mut concurrency_outcome: Option<RequestTicketOutcome> = None;

        match first_limit {
            None => {
                // No limits - write RunAttempt task directly
                let first_task = Task::RunAttempt {
                    id: first_task_id.clone(),
                    tenant: tenant.to_string(),
                    job_id: job_id.clone(),
                    attempt_number: 1,
                    held_queues: Vec::new(),
                };
                put_task(&mut batch, start_at_ms, priority, &job_id, 1, &first_task)?;
            }
            Some(Limit::Concurrency(cl)) => {
                // First limit is a concurrency limit - use existing logic
                concurrency_outcome = self
                    .concurrency
                    .handle_enqueue(
                        &mut batch,
                        tenant,
                        &first_task_id,
                        &job_id,
                        priority,
                        start_at_ms,
                        now_ms,
                        &[cl.clone()],
                    )
                    .map_err(JobStoreShardError::Rkyv)?;

                // If no concurrency limits blocked, check if there are more limits
                if concurrency_outcome.is_none() {
                    // Granted immediately, but we need to proceed to next limit
                    self.enqueue_next_limit_task(
                        &mut batch,
                        tenant,
                        &first_task_id,
                        &job_id,
                        1, // attempt number
                        0, // current limit index
                        &limits,
                        priority,
                        start_at_ms,
                        now_ms,
                        vec![cl.key.clone()], // held queues
                    )?;
                }
            }
            Some(Limit::RateLimit(rl)) => {
                // First limit is a rate limit - create CheckRateLimit task
                let check_task = Task::CheckRateLimit {
                    task_id: first_task_id.clone(),
                    tenant: tenant.to_string(),
                    job_id: job_id.clone(),
                    attempt_number: 1,
                    limit_index: 0,
                    rate_limit: GubernatorRateLimitData::from(rl),
                    retry_count: 0,
                    started_at_ms: now_ms,
                    priority,
                    held_queues: Vec::new(),
                };
                put_task(&mut batch, start_at_ms, priority, &job_id, 1, &check_task)?;
            }
            Some(Limit::FloatingConcurrency(fl)) => {
                // First limit is a floating concurrency limit
                // Get or create the floating limit state
                let (state, _created) = self
                    .get_or_create_floating_limit_state(&mut batch, tenant, fl, now_ms)
                    .await?;

                // Maybe schedule a refresh task if needed
                self.maybe_schedule_floating_limit_refresh(&mut batch, tenant, fl, &state, now_ms)?;

                // Create a temporary ConcurrencyLimit with the current max concurrency
                let temp_cl = crate::job::ConcurrencyLimit {
                    key: fl.key.clone(),
                    max_concurrency: state.current_max_concurrency,
                };

                // Use the concurrency system with the current floating limit value
                concurrency_outcome = self
                    .concurrency
                    .handle_enqueue(
                        &mut batch,
                        tenant,
                        &first_task_id,
                        &job_id,
                        priority,
                        start_at_ms,
                        now_ms,
                        &[temp_cl.clone()],
                    )
                    .map_err(JobStoreShardError::Rkyv)?;

                // If no concurrency limits blocked, check if there are more limits
                if concurrency_outcome.is_none() {
                    // Granted immediately, but we need to proceed to next limit
                    self.enqueue_next_limit_task(
                        &mut batch,
                        tenant,
                        &first_task_id,
                        &job_id,
                        1, // attempt number
                        0, // current limit index
                        &limits,
                        priority,
                        start_at_ms,
                        now_ms,
                        vec![fl.key.clone()], // held queues
                    )?;
                }
            }
        }

        self.db.write(batch).await?;
        self.db.flush().await?;

        // Apply memory events and log after durable commit
        if let Some(outcome) = concurrency_outcome {
            match outcome {
                RequestTicketOutcome::GrantedImmediately { events, .. } => {
                    for ev in events {
                        if let MemoryEvent::Granted { queue, task_id } = ev {
                            let span = info_span!("concurrency.grant", queue = %queue, task_id = %task_id, job_id = %job_id, attempt = 1u32, source = "immediate");
                            let _g = span.enter();
                            self.concurrency
                                .counts()
                                .record_grant(tenant, &queue, &task_id);
                        }
                    }
                }
                RequestTicketOutcome::TicketRequested { queue } => {
                    let span = info_span!("concurrency.request", queue = %queue, job_id = %job_id, attempt = 1u32, start_at_ms = start_at_ms, priority = priority);
                    let _g = span.enter();
                }
                RequestTicketOutcome::FutureRequestTaskWritten { queue, .. } => {
                    let span = info_span!("concurrency.ticket", queue = %queue, job_id = %job_id, attempt = 1u32, start_at_ms = start_at_ms, priority = priority);
                    let _g = span.enter();
                }
            }
        }

        // If ready now, wake the scanner to refill promptly
        if start_at_ms <= now_epoch_ms() {
            self.broker.wakeup();
        }

        Ok(job_id)
    }

    /// Helper to enqueue the next limit check task after passing a limit
    #[allow(clippy::too_many_arguments)]
    fn enqueue_next_limit_task(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        attempt_number: u32,
        current_limit_index: u32,
        limits: &[Limit],
        priority: u8,
        start_at_ms: i64,
        now_ms: i64,
        held_queues: Vec<String>,
    ) -> Result<(), JobStoreShardError> {
        let next_index = current_limit_index + 1;

        if next_index as usize >= limits.len() {
            // No more limits - enqueue RunAttempt
            let run_task = Task::RunAttempt {
                id: task_id.to_string(),
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                held_queues,
            };
            return put_task(
                batch,
                start_at_ms,
                priority,
                job_id,
                attempt_number,
                &run_task,
            );
        }

        // Enqueue task for the next limit
        let next_task = match &limits[next_index as usize] {
            Limit::Concurrency(cl) => Task::RequestTicket {
                queue: cl.key.clone(),
                start_time_ms: start_at_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                request_id: Uuid::new_v4().to_string(),
            },
            Limit::RateLimit(rl) => Task::CheckRateLimit {
                task_id: Uuid::new_v4().to_string(),
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                limit_index: next_index,
                rate_limit: GubernatorRateLimitData::from(rl),
                retry_count: 0,
                started_at_ms: now_ms,
                priority,
                held_queues,
            },
            // Floating concurrency limits use the same RequestTicket mechanism as regular concurrency limits
            Limit::FloatingConcurrency(fl) => Task::RequestTicket {
                queue: fl.key.clone(),
                start_time_ms: start_at_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                request_id: Uuid::new_v4().to_string(),
            },
        };
        put_task(
            batch,
            start_at_ms,
            priority,
            job_id,
            attempt_number,
            &next_task,
        )
    }

    /// Delete a job by id.
    ///
    /// Returns an error if the job is currently running (has active leases/holders) or has pending tasks/requests. Jobs must finish or permanently fail before deletion.
    pub async fn delete_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        // Check if job is running or has pending state
        let status = self.get_job_status(tenant, id).await?;
        if let Some(status) = status {
            if !status.is_terminal() {
                return Err(JobStoreShardError::JobInProgress(id.to_string()));
            }
        }

        let job_info_key: String = job_info_key(tenant, id);
        let job_status_key: String = job_status_key(tenant, id);
        let mut batch = WriteBatch::new();
        // Clean up secondary index entries if present
        if let Some(status) = self.get_job_status(tenant, id).await? {
            let kind = status.kind;
            let changed = status.changed_at_ms;
            let timek = idx_status_time_key(
                tenant,
                match kind {
                    JobStatusKind::Scheduled => "Scheduled",
                    JobStatusKind::Running => "Running",
                    JobStatusKind::Failed => "Failed",
                    JobStatusKind::Cancelled => "Cancelled",
                    JobStatusKind::Succeeded => "Succeeded",
                },
                changed,
                id,
            );
            batch.delete(timek.as_bytes());
        }
        // Clean up metadata index entries (load job info to enumerate metadata)
        if let Some(raw) = self.db.get(job_info_key.as_bytes()).await? {
            let view = JobView::new(raw)?;
            for (mk, mv) in view.metadata().into_iter() {
                let mkey = idx_metadata_key(tenant, &mk, &mv, id);
                batch.delete(mkey.as_bytes());
            }
        }
        batch.delete(job_info_key.as_bytes());
        batch.delete(job_status_key.as_bytes());
        // Also delete cancellation record if present
        batch.delete(job_cancelled_key(tenant, id).as_bytes());
        self.db.write(batch).await?;
        self.db.flush().await?;
        Ok(())
    }

    /// Cancel a job by id. Prevents further execution and signals running workers to stop.
    ///
    /// Cancellation semantics:
    /// - Cancellation is tracked separately from status for performance
    /// - For scheduled jobs: immediately removes from queue and sets status to Cancelled
    /// - For running jobs: sets cancellation flag; worker discovers on heartbeat
    /// - For terminal jobs: returns error (cannot cancel completed jobs)
    /// - Cancellation is monotonic: once cancelled, always cancelled
    ///
    /// Uses a transaction with optimistic concurrency control to detect if the job state
    /// changes during the cancellation flow. Retries automatically on conflict.
    pub async fn cancel_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            match self.cancel_job_inner(tenant, id).await {
                Ok(()) => return Ok(()),
                Err(JobStoreShardError::Slate(ref e))
                    if e.kind() == SlateErrorKind::Transaction =>
                {
                    // Transaction conflict - retry with exponential backoff
                    if attempt + 1 < MAX_RETRIES {
                        let delay_ms = 10 * (1 << attempt); // 10ms, 20ms, 40ms, 80ms
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        debug!(
                            job_id = %id,
                            attempt = attempt + 1,
                            "cancel_job transaction conflict, retrying"
                        );
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(JobStoreShardError::TransactionConflict(
            "cancel_job".to_string(),
        ))
    }

    /// Inner implementation of cancel_job that runs within a single transaction attempt.
    /// All business logic checks are performed within the transaction so they are re-evaluated
    /// on retry if the transaction conflicts.
    ///
    /// Note: We do NOT scan/delete tasks or requests here. Instead:
    /// - Tasks are cleaned up lazily when dequeued (we check cancellation flag)
    /// - Requests are skipped when granting (we check cancellation flag)
    /// This avoids O(n) scans on the tasks/requests namespaces.
    async fn cancel_job_inner(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();

        // Start a transaction with SerializableSnapshot isolation for conflict detection
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-CXL-1] Pre: job must exist and not already be cancelled
        // Read status within transaction to detect concurrent modifications
        let status_key = job_status_key(tenant, id);
        let maybe_status_raw = txn.get(status_key.as_bytes()).await?;
        let Some(status_raw) = maybe_status_raw else {
            // No status means job doesn't exist
            return Err(JobStoreShardError::JobNotFound(id.to_string()));
        };

        let decoded = decode_job_status(&status_raw).map_err(codec_error_to_shard_error)?;
        let mut des = rkyv::Infallible;
        let status: JobStatus = RkyvDeserialize::deserialize(decoded.archived(), &mut des)
            .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus"));

        // Check if already cancelled within transaction
        let cancelled_key = job_cancelled_key(tenant, id);
        let maybe_cancelled = txn.get(cancelled_key.as_bytes()).await?;
        if maybe_cancelled.is_some() {
            return Err(JobStoreShardError::JobAlreadyCancelled(id.to_string()));
        }

        // Cannot cancel jobs in terminal states (Succeeded/Failed are truly terminal)
        if status.kind == JobStatusKind::Succeeded || status.kind == JobStatusKind::Failed {
            return Err(JobStoreShardError::JobAlreadyTerminal(
                id.to_string(),
                status.kind,
            ));
        }

        // [SILO-CXL-2] Post: Mark job as cancelled (write cancellation record)
        let cancellation = JobCancellation {
            cancelled_at_ms: now_ms,
        };
        let cancellation_value =
            encode_job_cancellation(&cancellation).map_err(codec_error_to_shard_error)?;
        txn.put(cancelled_key.as_bytes(), &cancellation_value)?;

        // [SILO-CXL-3] For Scheduled jobs, update status to Cancelled immediately
        // Tasks/requests are NOT deleted here - they will be cleaned up lazily:
        // - On dequeue: cancelled tasks are skipped and deleted
        // - On grant: cancelled requests are skipped and deleted
        // For Running jobs, status stays Running - worker discovers on heartbeat
        if status.kind == JobStatusKind::Scheduled {
            // Delete old status index entry
            let old_time =
                idx_status_time_key(tenant, status.kind.as_str(), status.changed_at_ms, id);
            txn.delete(old_time.as_bytes())?;

            // Set status to Cancelled immediately since job never started
            let cancelled_status = JobStatus::cancelled(now_ms);
            let status_value =
                encode_job_status(&cancelled_status).map_err(codec_error_to_shard_error)?;
            txn.put(status_key.as_bytes(), &status_value)?;

            // Insert new status index entry
            let new_time = idx_status_time_key(
                tenant,
                cancelled_status.kind.as_str(),
                cancelled_status.changed_at_ms,
                id,
            );
            txn.put(new_time.as_bytes(), &[])?;
        }

        // Commit the transaction - this will detect conflicts with concurrent modifications
        txn.commit().await?;

        Ok(())
    }

    /// Check if a job has been cancelled.
    /// Returns true if the job has a cancellation record, false otherwise.
    pub async fn is_job_cancelled(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<bool, JobStoreShardError> {
        let key = job_cancelled_key(tenant, id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        Ok(maybe_raw.is_some())
    }

    /// Get the cancellation timestamp if job was cancelled.
    /// Returns None if job was not cancelled.
    pub async fn get_job_cancellation(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<i64>, JobStoreShardError> {
        let key = job_cancelled_key(tenant, id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        if let Some(raw) = maybe_raw {
            let decoded = decode_job_cancellation(&raw).map_err(codec_error_to_shard_error)?;
            Ok(Some(decoded.archived().cancelled_at_ms))
        } else {
            Ok(None)
        }
    }

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobView>, JobStoreShardError> {
        let key = job_info_key(tenant, id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        if let Some(raw) = maybe_raw {
            Ok(Some(JobView::new(raw)?))
        } else {
            Ok(None)
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

    /// Fetch a job statuby id as a zero-copy archived view.
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

        let decoded = decode_job_status(&raw).map_err(codec_error_to_shard_error)?;
        let mut des = rkyv::Infallible;
        let status: JobStatus = RkyvDeserialize::deserialize(decoded.archived(), &mut des)
            .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus"));
        Ok(Some(status))
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

                let decoded =
                    decode_job_status(&raw).map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                let mut des = rkyv::Infallible;
                let status: JobStatus = RkyvDeserialize::deserialize(decoded.archived(), &mut des)
                    .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus"));
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
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        if let Some(raw) = maybe_raw {
            Ok(Some(JobAttemptView::new(raw)?))
        } else {
            Ok(None)
        }
    }

    /// Peek up to `max_tasks` available tasks (time <= now), without deleting them.
    pub async fn peek_tasks(&self, max_tasks: usize) -> Result<Vec<Task>, JobStoreShardError> {
        let (tasks, _keys) = self.scan_ready_tasks(max_tasks).await?;
        Ok(tasks)
    }

    /// Dequeue up to `max_tasks` tasks available now, ordered by time then priority.
    ///
    /// This method processes internal tasks (CheckRateLimit, RequestTicket) transparently and returns
    /// RunAttempt tasks for job execution and RefreshFloatingLimit tasks for workers to refresh floating limits.
    pub async fn dequeue(
        &self,
        worker_id: &str,
        max_tasks: usize,
    ) -> Result<DequeueResult, JobStoreShardError> {
        let _ts_enter = now_epoch_ms();
        if max_tasks == 0 {
            return Ok(DequeueResult {
                tasks: Vec::new(),
                refresh_tasks: Vec::new(),
            });
        }

        let mut out: Vec<LeasedTask> = Vec::new();
        let mut refresh_out: Vec<LeasedRefreshTask> = Vec::new();
        let mut pending_attempts: Vec<(String, JobView, String, u32)> = Vec::new();

        // Loop to process internal tasks until we have tasks that are destined for the worker, or no more ready tasks at all.
        const MAX_INTERNAL_ITERATIONS: usize = 10;
        for _iteration in 0..MAX_INTERNAL_ITERATIONS {
            let remaining = max_tasks.saturating_sub(out.len() + pending_attempts.len());
            if remaining == 0 {
                break;
            }

            // [SILO-DEQ-1] Claim from the broker buffer (tenant-agnostic)
            let claimed: Vec<BrokerTask> = self.broker.claim_ready_or_nudge(remaining).await;

            if claimed.is_empty() {
                break;
            }

            let now_ms = now_epoch_ms();
            let expiry_ms = now_ms + DEFAULT_LEASE_MS;
            let mut batch = WriteBatch::new();
            let mut ack_keys: Vec<String> = Vec::with_capacity(claimed.len());
            let mut processed_internal = false;

            for entry in &claimed {
                let task = &entry.task;
                // Internal tasks are processed inside the store and not leased
                match task {
                    Task::RequestTicket {
                        queue,
                        start_time_ms: _,
                        priority: _priority,
                        tenant,
                        job_id,
                        attempt_number,
                        request_id,
                    } => {
                        // Process ticket request internally
                        processed_internal = true;
                        let tenant = tenant.to_string();

                        // [SILO-DEQ-CXL] Check if job is cancelled - if so, skip and clean up task
                        if self.is_job_cancelled(&tenant, job_id).await? {
                            batch.delete(entry.key.as_bytes());
                            ack_keys.push(entry.key.clone());
                            tracing::debug!(job_id = %job_id, "dequeue: skipping cancelled job RequestTicket");
                            continue;
                        }

                        // Load job info
                        let job_key = job_info_key(&tenant, job_id);
                        let maybe_job = self.db.get(job_key.as_bytes()).await?;
                        let job_view = maybe_job
                            .as_ref()
                            .and_then(|bytes| JobView::new(bytes.clone()).ok());

                        // Process ticket via concurrency manager
                        let outcome = self
                            .concurrency
                            .process_ticket_request_task(
                                &mut batch,
                                &entry.key,
                                &tenant,
                                queue,
                                request_id,
                                job_id,
                                *attempt_number,
                                now_ms,
                                job_view.as_ref(),
                            )
                            .map_err(JobStoreShardError::Rkyv)?;

                        match outcome {
                            RequestTicketTaskOutcome::Granted { request_id, queue } => {
                                // Create lease and attempt records
                                let run = Task::RunAttempt {
                                    id: request_id.clone(),
                                    tenant: tenant.clone(),
                                    job_id: job_id.clone(),
                                    attempt_number: *attempt_number,
                                    held_queues: vec![queue.clone()],
                                };
                                let lease_key = leased_task_key(&request_id);
                                let record = LeaseRecord {
                                    worker_id: worker_id.to_string(),
                                    task: run,
                                    expiry_ms,
                                };
                                let leased_value =
                                    encode_lease(&record).map_err(codec_error_to_shard_error)?;
                                batch.put(lease_key.as_bytes(), &leased_value);

                                // Mark job as running
                                let job_status = JobStatus::running(now_ms);
                                self.set_job_status_with_index(
                                    &mut batch, &tenant, job_id, job_status,
                                )
                                .await?;

                                // Attempt record
                                let attempt = JobAttempt {
                                    job_id: job_id.clone(),
                                    attempt_number: *attempt_number,
                                    task_id: request_id.clone(),
                                    status: AttemptStatus::Running {
                                        started_at_ms: now_ms,
                                    },
                                };
                                let attempt_val =
                                    encode_attempt(&attempt).map_err(codec_error_to_shard_error)?;
                                let akey = attempt_key(&tenant, job_id, *attempt_number);
                                batch.put(akey.as_bytes(), &attempt_val);

                                // Track for response and in-memory counts
                                let view = job_view.unwrap();
                                pending_attempts.push((
                                    tenant.clone(),
                                    view,
                                    job_id.clone(),
                                    *attempt_number,
                                ));
                                ack_keys.push(entry.key.clone());
                                self.concurrency.counts().record_grant(
                                    &tenant,
                                    &queue,
                                    &request_id,
                                );
                            }
                            RequestTicketTaskOutcome::Requested
                            | RequestTicketTaskOutcome::JobMissing => {
                                // Release inflight, task will be picked up later or cleaned up
                                ack_keys.push(entry.key.clone());
                            }
                        }
                        continue;
                    }
                    Task::CheckRateLimit {
                        task_id: check_task_id,
                        tenant,
                        job_id,
                        attempt_number,
                        limit_index,
                        rate_limit,
                        retry_count,
                        started_at_ms,
                        priority,
                        held_queues,
                    } => {
                        // Process rate limit check internally
                        processed_internal = true;
                        let tenant = tenant.to_string();
                        batch.delete(entry.key.as_bytes());
                        ack_keys.push(entry.key.clone());

                        // Load job info to get the full limits list
                        let job_key = job_info_key(&tenant, job_id);
                        let maybe_job = self.db.get(job_key.as_bytes()).await?;
                        let job_view = match maybe_job {
                            Some(bytes) => match JobView::new(bytes) {
                                Ok(v) => v,
                                Err(_) => continue, // Skip malformed job
                            },
                            None => continue, // Job deleted, skip
                        };

                        // Check the rate limit via Gubernator
                        let rate_limit_result = self.check_gubernator_rate_limit(rate_limit).await;

                        match rate_limit_result {
                            Ok(result) if result.under_limit => {
                                // Rate limit passed! Proceed to next limit or RunAttempt
                                self.enqueue_next_limit_task(
                                    &mut batch,
                                    &tenant,
                                    check_task_id,
                                    job_id,
                                    *attempt_number,
                                    *limit_index,
                                    &job_view.limits(),
                                    *priority,
                                    now_ms,
                                    now_ms,
                                    held_queues.clone(),
                                )?;
                            }
                            Ok(result) => {
                                // Over limit - schedule retry
                                let max_retries = rate_limit.retry_max_retries;
                                if max_retries > 0 && *retry_count >= max_retries {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        retry_count = retry_count,
                                        max_retries = max_retries,
                                        "rate limit check exceeded max retries"
                                    );
                                    continue;
                                }

                                let retry_backoff = self.calculate_rate_limit_backoff(
                                    rate_limit,
                                    *retry_count,
                                    result.reset_time_ms,
                                    now_ms,
                                );
                                self.schedule_rate_limit_retry(
                                    &mut batch,
                                    &tenant,
                                    job_id,
                                    *attempt_number,
                                    *limit_index,
                                    rate_limit,
                                    *retry_count,
                                    *started_at_ms,
                                    *priority,
                                    held_queues,
                                    retry_backoff,
                                )?;
                            }
                            Err(e) => {
                                tracing::warn!(job_id = %job_id, error = %e, "gubernator rate limit check failed, will retry");
                                let retry_backoff = now_ms + rate_limit.retry_initial_backoff_ms;
                                self.schedule_rate_limit_retry(
                                    &mut batch,
                                    &tenant,
                                    job_id,
                                    *attempt_number,
                                    *limit_index,
                                    rate_limit,
                                    *retry_count,
                                    *started_at_ms,
                                    *priority,
                                    held_queues,
                                    retry_backoff,
                                )?;
                            }
                        }
                        continue;
                    }
                    Task::RefreshFloatingLimit {
                        task_id,
                        tenant: task_tenant,
                        queue_key,
                        current_max_concurrency,
                        last_refreshed_at_ms,
                        metadata,
                    } => {
                        // RefreshFloatingLimit tasks are sent to workers - create lease
                        let lease_key = leased_task_key(task_id);
                        let record = LeaseRecord {
                            worker_id: worker_id.to_string(),
                            task: task.clone(),
                            expiry_ms,
                        };
                        let leased_value =
                            encode_lease(&record).map_err(codec_error_to_shard_error)?;
                        batch.put(lease_key.as_bytes(), &leased_value);
                        batch.delete(entry.key.as_bytes());

                        refresh_out.push(LeasedRefreshTask {
                            task_id: task_id.clone(),
                            queue_key: queue_key.clone(),
                            current_max_concurrency: *current_max_concurrency,
                            last_refreshed_at_ms: *last_refreshed_at_ms,
                            metadata: metadata.clone(),
                        });
                        ack_keys.push(entry.key.clone());
                        let _ = task_tenant; // suppress unused warning
                        continue;
                    }
                    Task::RunAttempt { .. } => {}
                }
                let (task_id, tenant, job_id, attempt_number) = match task {
                    Task::RunAttempt {
                        id,
                        tenant,
                        job_id,
                        attempt_number,
                        ..
                    } => (
                        id.clone(),
                        tenant.to_string(),
                        job_id.to_string(),
                        *attempt_number,
                    ),
                    Task::RequestTicket { .. }
                    | Task::CheckRateLimit { .. }
                    | Task::RefreshFloatingLimit { .. } => unreachable!(),
                };

                // [SILO-DEQ-2] Look up job info; if missing, delete the task and skip
                let job_key = job_info_key(&tenant, &job_id);
                let maybe_job = self.db.get(job_key.as_bytes()).await?;
                if let Some(job_bytes) = maybe_job {
                    // [SILO-DEQ-CXL] Check if job is cancelled - if so, skip and clean up task
                    if self.is_job_cancelled(&tenant, &job_id).await? {
                        batch.delete(entry.key.as_bytes());
                        ack_keys.push(entry.key.clone());

                        // [SILO-DEQ-CXL-REL] Release any held concurrency tickets
                        // This is required to maintain invariant: holders can only exist for active tasks
                        let held_queues = match task {
                            Task::RunAttempt { held_queues, .. } => held_queues.clone(),
                            Task::CheckRateLimit { held_queues, .. } => held_queues.clone(),
                            Task::RequestTicket { .. } | Task::RefreshFloatingLimit { .. } => {
                                Vec::new()
                            }
                        };
                        if !held_queues.is_empty() {
                            let release_events = self
                                .concurrency
                                .release_and_grant_next(
                                    &self.db,
                                    &mut batch,
                                    &tenant,
                                    &held_queues,
                                    &task_id,
                                    now_ms,
                                )
                                .await
                                .map_err(JobStoreShardError::Rkyv)?;
                            self.apply_concurrency_events(&tenant, release_events);
                            tracing::debug!(job_id = %job_id, queues = ?held_queues, "dequeue: released tickets for cancelled job task");
                        }

                        tracing::debug!(job_id = %job_id, "dequeue: skipping cancelled job task");
                        continue;
                    }

                    let view = JobView::new(job_bytes)?;

                    // [SILO-DEQ-CONC] Implicit: If job requires concurrency, task must hold all required queues.
                    // This is guaranteed by construction: RunAttempt tasks are only created when concurrency
                    // is granted (at enqueue or grant_next), with held_queues populated.

                    // [SILO-DEQ-4] Create lease record
                    let lease_key = leased_task_key(&task_id);
                    let record = LeaseRecord {
                        worker_id: worker_id.to_string(),
                        task: task.clone(),
                        expiry_ms,
                    };
                    let leased_value = encode_lease(&record).map_err(codec_error_to_shard_error)?;

                    batch.put(lease_key.as_bytes(), &leased_value);
                    // [SILO-DEQ-3] Delete task from task queue
                    batch.delete(entry.key.as_bytes());

                    // [SILO-DEQ-6] Mark job as running (pure write, no status read)
                    let job_status = JobStatus::running(now_ms);
                    self.set_job_status_with_index(&mut batch, &tenant, &job_id, job_status)
                        .await?;

                    // [SILO-DEQ-5] Also mark attempt as running
                    let attempt = JobAttempt {
                        job_id: job_id.clone(),
                        attempt_number,
                        task_id: task_id.clone(),
                        status: AttemptStatus::Running {
                            started_at_ms: now_ms,
                        },
                    };
                    let attempt_val =
                        encode_attempt(&attempt).map_err(codec_error_to_shard_error)?;
                    let akey = attempt_key(&tenant, &job_id, attempt_number);
                    batch.put(akey.as_bytes(), &attempt_val);

                    // Defer constructing AttemptView; fetch from DB after batch is written
                    pending_attempts.push((tenant.clone(), view, job_id.clone(), attempt_number));
                    ack_keys.push(entry.key.clone());
                } else {
                    // If job missing, delete task key to clean up
                    batch.delete(entry.key.as_bytes());
                    ack_keys.push(entry.key.clone());
                }
            }

            // Try to commit durable state. On failure, requeue the tasks and return error.
            if let Err(e) = self.db.write(batch).await {
                // Put back all claimed entries since we didn't lease them durably
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }
            if let Err(e) = self.db.flush().await {
                self.broker.requeue(claimed);
                return Err(JobStoreShardError::Slate(e));
            }

            // [SILO-DEQ-3] Ack durable and evict from buffer; we no longer use TTL tombstones.
            self.broker.ack_durable(&ack_keys);
            self.broker.evict_keys(&ack_keys);
            tracing::debug!(
                ack_keys = ack_keys.len(),
                pending_attempts = pending_attempts.len(),
                buffer_size = self.broker.buffer_len(),
                inflight = self.broker.inflight_len(),
                processed_internal = processed_internal,
                "dequeue: acked and evicted keys"
            );

            // If we only processed internal tasks and haven't filled max_tasks, loop again
            // to pick up the follow-up tasks we just inserted into the broker
            if !processed_internal {
                // We processed real RunAttempt tasks, no need to loop
                break;
            }
            // Continue looping to process any follow-up tasks
        }

        // Build LeasedTask results from pending_attempts
        for (tenant, job_view, job_id, attempt_number) in pending_attempts.into_iter() {
            let attempt_view = self
                .get_job_attempt(tenant.as_str(), &job_id, attempt_number)
                .await?
                .ok_or_else(|| {
                    JobStoreShardError::Rkyv("attempt not found after dequeue".to_string())
                })?;
            out.push(LeasedTask::new(job_view, attempt_view));
        }
        tracing::debug!(
            worker_id = %worker_id,
            returned = out.len(),
            refresh_tasks = refresh_out.len(),
            "dequeue: completed"
        );
        Ok(DequeueResult {
            tasks: out,
            refresh_tasks: refresh_out,
        })
    }

    /// Internal: scan up to `max_tasks` ready tasks and return them with their keys.
    async fn scan_ready_tasks(
        &self,
        max_tasks: usize,
    ) -> Result<(Vec<Task>, Vec<Vec<u8>>), JobStoreShardError> {
        if max_tasks == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // Scan tasks under tasks/
        let start: Vec<u8> = b"tasks/".to_vec();
        let mut end: Vec<u8> = b"tasks/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        let mut tasks: Vec<Task> = Vec::with_capacity(max_tasks);
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(max_tasks);
        let now_ms = now_epoch_ms();
        while tasks.len() < max_tasks {
            let maybe_kv = iter.next().await?;
            let Some(kv) = maybe_kv else {
                break;
            };

            // Only process task keys: tasks/...
            let key_str = String::from_utf8_lossy(&kv.key);

            // Enforce time cutoff: only keys with ts <= now_ms
            // Format: tasks/<ts>/...
            let mut parts = key_str.split('/');
            if parts.next() != Some("tasks") {
                continue;
            }
            let ts_part = match parts.next() {
                Some(v) => v,
                None => continue,
            };
            if let Ok(ts) = ts_part.parse::<u64>() {
                if ts > now_ms as u64 {
                    continue;
                }
            }

            let task = match decode_task(&kv.value) {
                Ok(t) => t,
                Err(_) => continue, // Skip malformed tasks
            };
            tasks.push(task);
            keys.push(kv.key.to_vec());
        }

        Ok((tasks, keys))
    }

    /// Heartbeat a lease to renew it if the worker id matches. Bumps expiry by DEFAULT_LEASE_MS.
    ///
    /// [SILO-HB-3]: Heartbeat ALWAYS renews the lease, even for cancelled jobs.
    /// Worker discovers cancellation from the heartbeat response
    /// Worker can keep heartbeating during graceful shutdown until they report completion.
    pub async fn heartbeat_task(
        &self,
        tenant: &str,
        worker_id: &str,
        task_id: &str,
    ) -> Result<HeartbeatResult, JobStoreShardError> {
        // [SILO-HB-2] Directly read the lease for this task id
        let key = leased_task_key(task_id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes).map_err(codec_error_to_shard_error)?;

        // [SILO-HB-1] Check worker id matches
        let current_owner = decoded.worker_id();
        if current_owner != worker_id {
            return Err(JobStoreShardError::LeaseOwnerMismatch {
                task_id: task_id.to_string(),
                expected: current_owner.to_string(),
                got: worker_id.to_string(),
            });
        }

        // Extract job_id for cancellation checking (use helper method)
        let job_id = decoded.job_id().to_string();

        // [SILO-HB-3] Renew by creating new record with updated expiry
        // Note: to_task() allocates, but we need owned Task for LeaseRecord
        let record = LeaseRecord {
            worker_id: current_owner.to_string(),
            task: decoded.to_task(),
            expiry_ms: now_epoch_ms() + DEFAULT_LEASE_MS,
        };
        let value = encode_lease(&record).map_err(codec_error_to_shard_error)?;

        let mut batch = WriteBatch::new();
        batch.put(key.as_bytes(), &value);
        self.db.write(batch).await?;
        self.db.flush().await?;

        // Check cancellation status to return in response
        // Worker discovers cancellation via heartbeat response per Alloy spec
        let cancelled_at_ms = self.get_job_cancellation(tenant, &job_id).await?;

        Ok(HeartbeatResult {
            cancelled: cancelled_at_ms.is_some(),
            cancelled_at_ms,
        })
    }

    /// Report the outcome of a running attempt identified by task id.
    /// Removes the lease and finalizes the attempt state.
    pub async fn report_attempt_outcome(
        &self,
        tenant: &str,
        task_id: &str,
        outcome: AttemptOutcome,
    ) -> Result<(), JobStoreShardError> {
        // [SILO-SUCC-1][SILO-FAIL-1][SILO-RETRY-1] Load lease; must exist
        let leased_task_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(leased_task_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes).map_err(codec_error_to_shard_error)?;

        // Extract fields using zero-copy accessors
        let job_id = decoded.job_id().to_string();
        let attempt_number = decoded.attempt_number();
        let held_queues_local = decoded.held_queues(); // Only allocation needed

        let now_ms = now_epoch_ms();
        let attempt_status = match &outcome {
            AttemptOutcome::Success { result } => AttemptStatus::Succeeded {
                finished_at_ms: now_ms,
                result: result.clone(),
            },
            AttemptOutcome::Error { error_code, error } => AttemptStatus::Failed {
                finished_at_ms: now_ms,
                error_code: error_code.clone(),
                error: error.clone(),
            },
            AttemptOutcome::Cancelled => AttemptStatus::Cancelled {
                finished_at_ms: now_ms,
            },
        };
        let attempt = JobAttempt {
            job_id: job_id.clone(),
            attempt_number,
            task_id: task_id.to_string(),
            status: attempt_status,
        };
        let attempt_val = encode_attempt(&attempt).map_err(codec_error_to_shard_error)?;
        let attempt_key = attempt_key(tenant, &job_id, attempt_number);

        // Atomically update attempt and remove lease
        let mut batch = WriteBatch::new();
        // [SILO-SUCC-4][SILO-FAIL-4][SILO-RETRY-4] Update attempt status
        batch.put(attempt_key.as_bytes(), &attempt_val);
        // [SILO-SUCC-2][SILO-FAIL-2][SILO-RETRY-2] Release lease
        batch.delete(leased_task_key.as_bytes());

        let mut job_missing_error: Option<JobStoreShardError> = None;
        let mut followup_next_time: Option<i64> = None;

        match &outcome {
            // [SILO-SUCC-3] If success: mark job succeeded now (pure write)
            AttemptOutcome::Success { .. } => {
                let job_status = JobStatus::succeeded(now_ms);
                self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                    .await?;
            }
            // Worker acknowledges cancellation - set job status to Cancelled
            AttemptOutcome::Cancelled => {
                let job_status = JobStatus::cancelled(now_ms);
                self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                    .await?;
            }
            // Error: maybe enqueue next attempt; otherwise mark job failed
            AttemptOutcome::Error { .. } => {
                let mut scheduled_followup: bool = false;

                // Load job info to get priority and retry policy
                let job_info_key = job_info_key(tenant, &job_id);
                let maybe_job = self.db.get(job_info_key.as_bytes()).await?;
                if let Some(jbytes) = maybe_job {
                    let view = JobView::new(jbytes)?;
                    let priority = view.priority();
                    let failures_so_far = attempt_number;
                    if let Some(policy_rt) = view.retry_policy() {
                        if let Some(next_time) =
                            crate::retry::next_retry_time_ms(now_ms, failures_so_far, &policy_rt)
                        {
                            // [SILO-RETRY-5] Enqueue new task to DB queue for retry
                            let next_attempt_number = attempt_number + 1;
                            let next_task = Task::RunAttempt {
                                id: Uuid::new_v4().to_string(),
                                tenant: tenant.to_string(),
                                job_id: job_id.clone(),
                                attempt_number: next_attempt_number,
                                held_queues: held_queues_local.clone(),
                            };
                            put_task(
                                &mut batch,
                                next_time,
                                priority,
                                &job_id,
                                next_attempt_number,
                                &next_task,
                            )?;

                            // [SILO-RETRY-3] Set job status to Scheduled
                            let job_status = JobStatus::scheduled(now_ms);
                            self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                                .await?;
                            scheduled_followup = true;
                            followup_next_time = Some(next_time);
                        }
                    }
                    // [SILO-FAIL-3] If no follow-up scheduled, mark job as failed (pure write)
                    if !scheduled_followup {
                        let job_status = JobStatus::failed(now_ms);
                        self.set_job_status_with_index(&mut batch, tenant, &job_id, job_status)
                            .await?;
                    }
                } else {
                    job_missing_error = Some(JobStoreShardError::JobNotFound(job_id.clone()));
                }
            }
        }

        // [SILO-REL-1] Release any held concurrency tickets
        // This also handles [SILO-GRANT-*] granting to next waiting request
        let release_events: Vec<MemoryEvent> = self
            .concurrency
            .release_and_grant_next(
                &self.db,
                &mut batch,
                tenant,
                &held_queues_local,
                task_id,
                now_ms,
            )
            .await
            .map_err(JobStoreShardError::Rkyv)?;

        self.db.write(batch).await?;
        self.db.flush().await?;
        // Update in-memory broker counts after durable release and emit spans for release/grant
        for ev in release_events.into_iter() {
            match ev {
                MemoryEvent::Released {
                    queue,
                    task_id: tid,
                } => {
                    let span =
                        info_span!("concurrency.release", queue = %queue, finished_task_id = %tid);
                    let _g = span.enter();
                    info!("released ticket for finished task");
                    self.concurrency
                        .counts()
                        .record_release(tenant, &queue, &tid);
                    // Wake broker; durable grant-from-release already enqueues run task if ready
                    self.broker.wakeup();
                }
                MemoryEvent::Granted { queue, task_id } => {
                    // We granted on release: bump in-memory counts now and wake the broker to scan promptly.
                    self.concurrency
                        .counts()
                        .record_grant(tenant, &queue, &task_id);
                    let span = info_span!("task.enqueue_from_grant", queue = %queue, task_id = %task_id, cause = "release");
                    let _g = span.enter();
                    debug!("enqueued task for next requester after release");
                    self.broker.wakeup();
                }
            }
        }
        // If we scheduled a follow-up that is ready now, wake the scanner
        if let Some(nt) = followup_next_time {
            if nt <= now_epoch_ms() {
                self.broker.wakeup();
            }
        }
        if let Some(err) = job_missing_error {
            return Err(err);
        }
        tracing::debug!(task_id = %task_id, "report_attempt_outcome: completed");
        Ok(())
    }

    /// Scan all held leases and mark any expired ones as failed with a WORKER_CRASHED error code, or as Cancelled if the job was cancelled.
    /// Returns the number of expired leases reaped.
    pub async fn reap_expired_leases(&self, tenant: &str) -> Result<usize, JobStoreShardError> {
        // Scan leases under lease/
        let start: Vec<u8> = b"lease/".to_vec();
        let mut end: Vec<u8> = b"lease/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        let now_ms = now_epoch_ms();
        let mut reaped: usize = 0;

        while let Some(kv) = iter.next().await? {
            let key_str = String::from_utf8_lossy(&kv.key);
            if !key_str.starts_with("lease/") {
                continue;
            }

            let decoded = match decode_lease(&kv.value) {
                Ok(l) => l,
                Err(_) => continue,
            };

            // [SILO-REAP-1] Pre: Lease exists (we found it)
            // [SILO-REAP-2] Pre: Check if lease has expired
            if decoded.expiry_ms() > now_ms {
                continue;
            }

            // Get task_id and job_id using helper methods
            let Some(task_id) = decoded.task_id() else {
                continue; // Not a RunAttempt lease
            };
            let job_id = decoded.job_id().to_string();

            // Check if job was cancelled - if so, report Cancelled instead of WORKER_CRASHED
            let was_cancelled = self
                .is_job_cancelled(tenant, &job_id)
                .await
                .unwrap_or(false);

            let outcome = if was_cancelled {
                // Job was cancelled - report as Cancelled (clean termination)
                AttemptOutcome::Cancelled
            } else {
                // [SILO-REAP-3][SILO-REAP-4] Report as worker crashed
                // SILO-REAP-3: Post: Set job status to Failed (via report_attempt_outcome)
                // SILO-REAP-4: Post: Set attempt status to AttemptFailed
                AttemptOutcome::Error {
                    error_code: "WORKER_CRASHED".to_string(),
                    error: format!(
                        "lease expired at {} (now {}), worker={}",
                        decoded.expiry_ms(),
                        now_ms,
                        decoded.worker_id()
                    )
                    .into_bytes(),
                }
            };

            // [SILO-REAP-REL] Release lease and update job/attempt status via report_attempt_outcome
            let _ = self.report_attempt_outcome(tenant, task_id, outcome).await;
            reaped += 1;
        }

        Ok(reaped)
    }

    /// Update job status and maintain secondary indexes in the same write batch.
    async fn set_job_status_with_index(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        job_id: &str,
        new_status: JobStatus,
    ) -> Result<(), JobStoreShardError> {
        // Delete old index entries if present
        if let Some(old) = self.get_job_status(tenant, job_id).await? {
            let old_kind = old.kind;
            let old_changed = old.changed_at_ms;
            let old_time = idx_status_time_key(tenant, old_kind.as_str(), old_changed, job_id);
            batch.delete(old_time.as_bytes());
        }

        // Write new status value
        put_job_status(batch, tenant, job_id, &new_status)?;

        // Insert new index entries
        let new_kind = new_status.kind;
        let changed = new_status.changed_at_ms;
        let timek = idx_status_time_key(tenant, new_kind.as_str(), changed, job_id);
        batch.put(timek.as_bytes(), []);
        Ok(())
    }

    /// Apply concurrency release/grant events to in-memory counts and wake broker.
    /// Called after durably committing releases to update optimistic counts.
    fn apply_concurrency_events(&self, tenant: &str, events: Vec<MemoryEvent>) {
        for ev in events {
            match ev {
                MemoryEvent::Released { queue, task_id } => {
                    self.concurrency
                        .counts()
                        .record_release(tenant, &queue, &task_id);
                    self.broker.wakeup();
                }
                MemoryEvent::Granted { queue, task_id } => {
                    self.concurrency
                        .counts()
                        .record_grant(tenant, &queue, &task_id);
                    self.broker.wakeup();
                }
            }
        }
    }

    /// Scan all jobs for a tenant ordered by job id (lexicographic), unfiltered.
    pub async fn scan_jobs(
        &self,
        tenant: &str,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Prefix: jobs/<tenant>/
        let prefix = crate::keys::job_info_key(tenant, "");
        let start = prefix.as_bytes().to_vec();
        let mut end = start.clone();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;
        let mut out: Vec<String> = Vec::with_capacity(limit);
        while out.len() < limit {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };
            let key_str = String::from_utf8_lossy(&kv.key);
            // key format: jobs/<tenant>/<job-id>
            if let Some(job_id) = key_str.rsplit('/').next() {
                if !job_id.is_empty() {
                    out.push(job_id.to_string());
                }
            }
        }
        Ok(out)
    }

    /// Scan newest-first job IDs by status using the time-ordered index.
    pub async fn scan_jobs_by_status(
        &self,
        tenant: &str,
        status: JobStatusKind,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let tenant_enc = crate::keys::job_info_key(tenant, "")
            .trim_start_matches("jobs/")
            .trim_end_matches('/')
            .to_string();
        let prefix = format!("idx/status_ts/{}/{}/", tenant_enc, status.as_str());
        // Build range [prefix, prefix + 0xFF]
        let start = prefix.as_bytes().to_vec();
        let mut end = start.clone();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;
        let mut out: Vec<String> = Vec::with_capacity(limit);
        while out.len() < limit {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };
            let key_str = String::from_utf8_lossy(&kv.key);
            // key format: idx/status_ts/<tenant>/<status>/<inv_ts>/<job-id>
            if let Some(job_id) = key_str.rsplit('/').next() {
                if !job_id.is_empty() {
                    out.push(job_id.to_string());
                }
            }
        }
        Ok(out)
    }

    /// Scan jobs by metadata key/value. Order is not specified.
    pub async fn scan_jobs_by_metadata(
        &self,
        tenant: &str,
        key: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<String>, JobStoreShardError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = idx_metadata_prefix(tenant, key, value);
        let start = prefix.as_bytes().to_vec();
        let mut end = start.clone();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;
        let mut out: Vec<String> = Vec::with_capacity(limit);
        while out.len() < limit {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };
            let key_str = String::from_utf8_lossy(&kv.key);
            // key format: idx/meta/<tenant>/<key>/<value>/<job-id>
            if let Some(job_id) = key_str.rsplit('/').next() {
                if !job_id.is_empty() {
                    out.push(job_id.to_string());
                }
            }
        }
        Ok(out)
    }

    // =========================================================================
    // Floating Concurrency Limit Management
    // =========================================================================

    /// Get or create the floating limit state for a given queue key.
    /// Returns the state and whether it was newly created.
    async fn get_or_create_floating_limit_state(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        fl: &FloatingConcurrencyLimit,
        _now_ms: i64,
    ) -> Result<(FloatingLimitState, bool), JobStoreShardError> {
        let state_key = floating_limit_state_key(tenant, &fl.key);
        let maybe_raw = self.db.get(state_key.as_bytes()).await?;

        if let Some(raw) = maybe_raw {
            let decoded = decode_floating_limit_state(&raw).map_err(codec_error_to_shard_error)?;
            let archived = decoded.archived();
            let metadata: Vec<(String, String)> = archived
                .metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect();
            let state = FloatingLimitState {
                current_max_concurrency: archived.current_max_concurrency,
                last_refreshed_at_ms: archived.last_refreshed_at_ms,
                refresh_task_scheduled: archived.refresh_task_scheduled,
                refresh_interval_ms: archived.refresh_interval_ms,
                default_max_concurrency: archived.default_max_concurrency,
                retry_count: archived.retry_count,
                next_retry_at_ms: archived.next_retry_at_ms.as_ref().copied(),
                metadata,
            };
            return Ok((state, false));
        }

        // Create new state with default values
        let state = FloatingLimitState {
            current_max_concurrency: fl.default_max_concurrency,
            last_refreshed_at_ms: 0, // Never refreshed
            refresh_task_scheduled: false,
            refresh_interval_ms: fl.refresh_interval_ms,
            default_max_concurrency: fl.default_max_concurrency,
            retry_count: 0,
            next_retry_at_ms: None,
            metadata: fl.metadata.clone(),
        };

        let state_value =
            encode_floating_limit_state(&state).map_err(codec_error_to_shard_error)?;
        batch.put(state_key.as_bytes(), &state_value);

        Ok((state, true))
    }

    /// Check if a floating limit refresh is needed and schedule it if so.
    /// This method is called during enqueue and dequeue operations to lazily trigger refreshes.
    fn maybe_schedule_floating_limit_refresh(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        fl: &FloatingConcurrencyLimit,
        state: &FloatingLimitState,
        now_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        // Check if refresh is already scheduled
        if state.refresh_task_scheduled {
            return Ok(());
        }

        // Check if we need to refresh based on interval
        let next_refresh_due = state.last_refreshed_at_ms + state.refresh_interval_ms;
        let should_refresh = now_ms >= next_refresh_due;

        // Also check if we're in backoff from a failed refresh
        let in_backoff = state.next_retry_at_ms.map(|t| now_ms < t).unwrap_or(false);

        if !should_refresh || in_backoff {
            return Ok(());
        }

        // Schedule a refresh task
        let task_id = Uuid::new_v4().to_string();
        let refresh_task = Task::RefreshFloatingLimit {
            task_id: task_id.clone(),
            tenant: tenant.to_string(),
            queue_key: fl.key.clone(),
            current_max_concurrency: state.current_max_concurrency,
            last_refreshed_at_ms: state.last_refreshed_at_ms,
            metadata: state.metadata.clone(),
        };

        // Use a synthetic task key based on the queue key to avoid collisions
        let task_value = encode_task(&refresh_task).map_err(codec_error_to_shard_error)?;
        let task_key_str = format!(
            "tasks/{:020}/{:02}/floating_refresh/{}/{}",
            now_ms,
            0, // highest priority for refresh tasks
            fl.key,
            task_id
        );
        batch.put(task_key_str.as_bytes(), &task_value);

        // Update state to mark refresh as scheduled
        let mut new_state = state.clone();
        new_state.refresh_task_scheduled = true;
        let state_key = floating_limit_state_key(tenant, &fl.key);
        let state_value =
            encode_floating_limit_state(&new_state).map_err(codec_error_to_shard_error)?;
        batch.put(state_key.as_bytes(), &state_value);

        tracing::debug!(
            queue_key = %fl.key,
            current_max = state.current_max_concurrency,
            last_refreshed = state.last_refreshed_at_ms,
            "scheduled floating limit refresh task"
        );

        Ok(())
    }

    /// Report a successful floating limit refresh from a worker.
    /// Updates the floating limit state with the new max concurrency value.
    pub async fn report_refresh_success(
        &self,
        tenant: &str,
        task_id: &str,
        new_max_concurrency: u32,
    ) -> Result<(), JobStoreShardError> {
        // Load the lease to get the queue key
        let lease_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(lease_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes).map_err(codec_error_to_shard_error)?;
        let queue_key = match decoded.to_task() {
            Task::RefreshFloatingLimit { queue_key, .. } => queue_key,
            _ => {
                return Err(JobStoreShardError::Rkyv(
                    "task is not a RefreshFloatingLimit".to_string(),
                ))
            }
        };

        let now_ms = now_epoch_ms();
        let state_key = floating_limit_state_key(tenant, &queue_key);

        // Load and update the state
        let maybe_state = self.db.get(state_key.as_bytes()).await?;
        let mut state = if let Some(raw) = maybe_state {
            let decoded = decode_floating_limit_state(&raw).map_err(codec_error_to_shard_error)?;
            let archived = decoded.archived();
            FloatingLimitState {
                current_max_concurrency: archived.current_max_concurrency,
                last_refreshed_at_ms: archived.last_refreshed_at_ms,
                refresh_task_scheduled: archived.refresh_task_scheduled,
                refresh_interval_ms: archived.refresh_interval_ms,
                default_max_concurrency: archived.default_max_concurrency,
                retry_count: archived.retry_count,
                next_retry_at_ms: archived.next_retry_at_ms.as_ref().copied(),
                metadata: archived
                    .metadata
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                    .collect(),
            }
        } else {
            // State doesn't exist - this shouldn't happen, but handle it gracefully
            return Err(JobStoreShardError::Rkyv(format!(
                "floating limit state not found for queue {}",
                queue_key
            )));
        };

        // Update state with new values
        state.current_max_concurrency = new_max_concurrency;
        state.last_refreshed_at_ms = now_ms;
        state.refresh_task_scheduled = false;
        state.retry_count = 0;
        state.next_retry_at_ms = None;

        let mut batch = WriteBatch::new();
        let state_value =
            encode_floating_limit_state(&state).map_err(codec_error_to_shard_error)?;
        batch.put(state_key.as_bytes(), &state_value);
        batch.delete(lease_key.as_bytes());

        self.db.write(batch).await?;
        self.db.flush().await?;

        tracing::debug!(
            queue_key = %queue_key,
            new_max_concurrency = new_max_concurrency,
            "floating limit refresh succeeded"
        );

        Ok(())
    }

    /// Report a failed floating limit refresh from a worker.
    /// Schedules a retry with exponential backoff.
    pub async fn report_refresh_failure(
        &self,
        tenant: &str,
        task_id: &str,
        error_code: &str,
        error_message: &str,
    ) -> Result<(), JobStoreShardError> {
        // Load the lease to get the task details
        let lease_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(lease_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes).map_err(codec_error_to_shard_error)?;
        let (queue_key, current_max_concurrency, last_refreshed_at_ms, metadata) =
            match decoded.to_task() {
                Task::RefreshFloatingLimit {
                    queue_key,
                    current_max_concurrency,
                    last_refreshed_at_ms,
                    metadata,
                    ..
                } => (
                    queue_key,
                    current_max_concurrency,
                    last_refreshed_at_ms,
                    metadata,
                ),
                _ => {
                    return Err(JobStoreShardError::Rkyv(
                        "task is not a RefreshFloatingLimit".to_string(),
                    ))
                }
            };

        let now_ms = now_epoch_ms();
        let state_key = floating_limit_state_key(tenant, &queue_key);

        // Load and update the state
        let maybe_state = self.db.get(state_key.as_bytes()).await?;
        let mut state = if let Some(raw) = maybe_state {
            let decoded = decode_floating_limit_state(&raw).map_err(codec_error_to_shard_error)?;
            let archived = decoded.archived();
            FloatingLimitState {
                current_max_concurrency: archived.current_max_concurrency,
                last_refreshed_at_ms: archived.last_refreshed_at_ms,
                refresh_task_scheduled: archived.refresh_task_scheduled,
                refresh_interval_ms: archived.refresh_interval_ms,
                default_max_concurrency: archived.default_max_concurrency,
                retry_count: archived.retry_count,
                next_retry_at_ms: archived.next_retry_at_ms.as_ref().copied(),
                metadata: archived
                    .metadata
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                    .collect(),
            }
        } else {
            return Err(JobStoreShardError::Rkyv(format!(
                "floating limit state not found for queue {}",
                queue_key
            )));
        };

        // Calculate exponential backoff
        const INITIAL_BACKOFF_MS: i64 = 1000; // 1 second
        const MAX_BACKOFF_MS: i64 = 60_000; // 1 minute
        const BACKOFF_MULTIPLIER: f64 = 2.0;

        let retry_count = state.retry_count + 1;
        let backoff_ms = ((INITIAL_BACKOFF_MS as f64)
            * BACKOFF_MULTIPLIER.powi(state.retry_count as i32))
        .round() as i64;
        let capped_backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
        let next_retry_at = now_ms + capped_backoff_ms;

        // Schedule a new refresh task
        let new_task_id = Uuid::new_v4().to_string();
        let refresh_task = Task::RefreshFloatingLimit {
            task_id: new_task_id.clone(),
            tenant: tenant.to_string(),
            queue_key: queue_key.clone(),
            current_max_concurrency,
            last_refreshed_at_ms,
            metadata,
        };

        let task_value = encode_task(&refresh_task).map_err(codec_error_to_shard_error)?;
        let task_key_str = format!(
            "tasks/{:020}/{:02}/floating_refresh/{}/{}",
            next_retry_at,
            0, // highest priority
            queue_key,
            new_task_id
        );

        // Update state - keep refresh_task_scheduled true since we're re-enqueueing
        state.retry_count = retry_count;
        state.next_retry_at_ms = Some(next_retry_at);
        state.refresh_task_scheduled = true;

        let mut batch = WriteBatch::new();
        let state_value =
            encode_floating_limit_state(&state).map_err(codec_error_to_shard_error)?;
        batch.put(state_key.as_bytes(), &state_value);
        batch.put(task_key_str.as_bytes(), &task_value);
        batch.delete(lease_key.as_bytes());

        self.db.write(batch).await?;
        self.db.flush().await?;

        tracing::warn!(
            queue_key = %queue_key,
            error_code = %error_code,
            error_message = %error_message,
            retry_count = retry_count,
            next_retry_at_ms = next_retry_at,
            "floating limit refresh failed, scheduled retry"
        );

        Ok(())
    }

    /// Schedule a rate limit check retry task
    #[allow(clippy::too_many_arguments)]
    fn schedule_rate_limit_retry(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        job_id: &str,
        attempt_number: u32,
        limit_index: u32,
        rate_limit: &GubernatorRateLimitData,
        retry_count: u32,
        started_at_ms: i64,
        priority: u8,
        held_queues: &[String],
        retry_at_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let retry_task = Task::CheckRateLimit {
            task_id: Uuid::new_v4().to_string(),
            tenant: tenant.to_string(),
            job_id: job_id.to_string(),
            attempt_number,
            limit_index,
            rate_limit: rate_limit.clone(),
            retry_count: retry_count + 1,
            started_at_ms,
            priority,
            held_queues: held_queues.to_vec(),
        };
        put_task(
            batch,
            retry_at_ms,
            priority,
            job_id,
            attempt_number,
            &retry_task,
        )
    }

    /// Check a rate limit via the rate limit client
    async fn check_gubernator_rate_limit(
        &self,
        rate_limit: &GubernatorRateLimitData,
    ) -> Result<RateLimitResult, GubernatorError> {
        use crate::pb::gubernator::Algorithm;
        let algorithm = match rate_limit.algorithm {
            0 => Algorithm::TokenBucket,
            1 => Algorithm::LeakyBucket,
            _ => Algorithm::TokenBucket,
        };

        self.rate_limiter
            .check_rate_limit(
                &rate_limit.name,
                &rate_limit.unique_key,
                rate_limit.hits as i64,
                rate_limit.limit,
                rate_limit.duration_ms,
                algorithm,
                rate_limit.behavior,
            )
            .await
    }

    /// Calculate the backoff time for a rate limit retry
    fn calculate_rate_limit_backoff(
        &self,
        rate_limit: &GubernatorRateLimitData,
        retry_count: u32,
        reset_time_ms: i64,
        now_ms: i64,
    ) -> i64 {
        // If we have a reset time from Gubernator, use it if it's reasonable
        if reset_time_ms > now_ms && reset_time_ms < now_ms + rate_limit.retry_max_backoff_ms {
            return reset_time_ms;
        }

        // Otherwise use exponential backoff
        let base = rate_limit.retry_initial_backoff_ms as f64;
        let multiplier = rate_limit.retry_backoff_multiplier;
        let backoff = (base * multiplier.powi(retry_count as i32)).round() as i64;
        let capped = backoff.min(rate_limit.retry_max_backoff_ms);

        now_ms + capped
    }
}

pub(crate) fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

fn put_job_status(
    batch: &mut WriteBatch,
    tenant: &str,
    job_id: &str,
    status: &JobStatus,
) -> Result<(), JobStoreShardError> {
    let job_status_value = encode_job_status(status).map_err(codec_error_to_shard_error)?;
    batch.put(job_status_key(tenant, job_id).as_bytes(), &job_status_value);
    Ok(())
}

/// Helper to encode and write a task to the batch at the standard task key location
fn put_task(
    batch: &mut WriteBatch,
    time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
    task: &Task,
) -> Result<(), JobStoreShardError> {
    let task_value = encode_task(task).map_err(codec_error_to_shard_error)?;
    batch.put(
        task_key(time_ms, priority, job_id, attempt).as_bytes(),
        &task_value,
    );
    Ok(())
}

fn codec_error_to_shard_error(e: CodecError) -> JobStoreShardError {
    JobStoreShardError::Rkyv(e.to_string())
}
