use rkyv::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde_json::Value as JsonValue;
use slatedb::Db;
use slatedb::DbIterator;
use slatedb::WriteBatch;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::job::{ConcurrencyLimit, JobInfo, JobStatus, JobView};
use crate::job_attempt::{AttemptOutcome, AttemptStatus, JobAttempt, JobAttemptView};
use crate::keys::{
    attempt_key, concurrency_holder_key, concurrency_request_key, job_info_key, job_status_key,
    leased_task_key, task_key,
};
use crate::retry::RetryPolicy;
use crate::settings::DatabaseConfig;
use crate::storage::{resolve_object_store, StorageError};
use crate::task_broker::{BrokerTask, TaskBroker};

/// Represents a single shard of the system. Owns the SlateDB instance.
pub struct JobStoreShard {
    name: String,
    db: Arc<Db>,
    broker: Arc<TaskBroker>,
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
}

/// Default lease duration for dequeued tasks (milliseconds)
pub const DEFAULT_LEASE_MS: i64 = 10_000;

/// Represents a leased task with the associated job metadata necessary to execute it.
#[derive(Debug, Clone)]
pub struct LeasedTask {
    job: JobView,
    attempt: JobAttemptView,
}

impl LeasedTask {
    pub fn job(&self) -> &JobView {
        &self.job
    }
    pub fn attempt(&self) -> &JobAttemptView {
        &self.attempt
    }
}

// key builders moved to crate::keys

/// A task is a unit of work that a worker needs to pickup and action to move the system forward.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum Task {
    /// Execute a specific attempt for a job
    RunAttempt {
        id: String,
        job_id: String,
        attempt_number: u32,
        held_queues: Vec<String>,
    },
}

/// Stored representation for a lease record. Value at `lease/<expiry>/<task-id>`
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct LeaseRecord {
    pub worker_id: String,
    pub task: Task,
    pub expiry_ms: i64,
}

/// Stored representation for a concurrency holder record: value at holders/<queue>/<task-id>
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct HolderRecord {
    pub granted_at_ms: i64,
}

/// Action stored at requests/<queue>/<time>/<request-id>
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum ConcurrencyAction {
    /// When ticket is granted, enqueue the specified task
    EnqueueTask {
        start_time_ms: i64,
        priority: u8,
        job_id: String,
        attempt_number: u32,
    },
}

impl JobStoreShard {
    pub async fn open(cfg: &DatabaseConfig) -> Result<Self, JobStoreShardError> {
        let object_store = resolve_object_store(&cfg.backend, &cfg.path)?;
        let db = Db::open(cfg.path.as_str(), object_store).await?;
        let db = Arc::new(db);
        let broker = TaskBroker::new(Arc::clone(&db));
        broker.start();
        Ok(Self {
            name: cfg.name.clone(),
            db,
            broker,
        })
    }

    /// Release tickets for the given queues and grant next requests atomically into the batch
    async fn release_and_grant_next(
        &self,
        batch: &mut WriteBatch,
        queues: &Vec<String>,
        finished_task_id: &str,
    ) -> Result<(), JobStoreShardError> {
        let now_ms = now_epoch_ms();
        for queue in queues.iter() {
            // Remove holder for this finished task id in the same batch
            batch.delete(concurrency_holder_key(queue, finished_task_id).as_bytes());
            // Update in-memory broker after commit; we cannot mutate yet here
            // Find one request to grant if capacity allows
            // Load limit if exists (future enhancement). We grant one per release to keep steady-state.
            // Scan first request
            let start = format!("requests/{}/", queue).into_bytes();
            let mut end: Vec<u8> = format!("requests/{}/", queue).into_bytes();
            end.push(0xFF);
            let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;
            let maybe_req = iter.next().await?;
            if let Some(kv) = maybe_req {
                // Decode action
                type ArchivedAction = <ConcurrencyAction as Archive>::Archived;
                let a: &ArchivedAction =
                    unsafe { rkyv::archived_root::<ConcurrencyAction>(&kv.value) };
                match a {
                    ArchivedAction::EnqueueTask {
                        start_time_ms,
                        priority,
                        job_id,
                        attempt_number,
                    } => {
                        // Grant: write holder for a synthetic task id equal to request id, and enqueue task
                        let req_key_str = String::from_utf8_lossy(&kv.key).to_string();
                        let request_id = req_key_str.split('/').last().unwrap_or("").to_string();
                        let holder = HolderRecord {
                            granted_at_ms: now_ms,
                        };
                        let holder_val: AlignedVec =
                            rkyv::to_bytes::<HolderRecord, 256>(&holder)
                                .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                        batch.put(
                            concurrency_holder_key(queue, &request_id).as_bytes(),
                            &holder_val,
                        );
                        // Build task with held queue
                        let task = Task::RunAttempt {
                            id: request_id.clone(),
                            job_id: job_id.as_str().to_string(),
                            attempt_number: *attempt_number,
                            held_queues: vec![queue.clone()],
                        };
                        let task_value: AlignedVec = rkyv::to_bytes::<Task, 256>(&task)
                            .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                        batch.put(
                            task_key(*start_time_ms, *priority, job_id.as_str(), *attempt_number)
                                .as_bytes(),
                            &task_value,
                        );
                        // Remove request
                        batch.delete(&kv.key);
                        // Note: after commit, we'll increment in-memory grant count when the task is dequeued and work begins
                    }
                }
            }
        }
        Ok(())
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
        &*self.db
    }

    /// Enqueue a new job with optional concurrency limits.
    pub async fn enqueue(
        &self,
        id: Option<String>,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: JsonValue,
        concurrency_limits: Vec<ConcurrencyLimit>,
    ) -> Result<String, JobStoreShardError> {
        let job_id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        // If caller provided an id, ensure it doesn't already exist
        if self
            .db
            .get(job_info_key(&job_id).as_bytes())
            .await?
            .is_some()
        {
            return Err(JobStoreShardError::JobAlreadyExists(job_id));
        }
        let payload_bytes = serde_json::to_vec(&payload)?;
        // Capture the first concurrency limit (if any) before moving the vector into JobInfo
        let first_limit_opt: Option<ConcurrencyLimit> = concurrency_limits.get(0).cloned();
        let job = JobInfo {
            id: job_id.clone(),
            priority,
            enqueue_time_ms: start_at_ms,
            payload: payload_bytes,
            retry_policy,
            concurrency_limits,
        };
        let job_value: AlignedVec = rkyv::to_bytes::<JobInfo, 256>(&job)
            .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;

        let first_task_id = Uuid::new_v4().to_string();

        let job_status = JobStatus::Scheduled {};

        // Atomically write job info, job status, and the first task
        let mut batch = WriteBatch::new();
        batch.put(job_info_key(&job_id).as_bytes(), &job_value);
        put_job_status(&mut batch, &job_id, &job_status)?;

        // Concurrency gating: if limits present, try to grant immediately (in-memory); else write a request
        if let Some(limit) = first_limit_opt.as_ref() {
            let now_ms = now_epoch_ms();
            let queue = &limit.key;
            let max_allowed = limit.max_concurrency as usize;
            if self.broker.concurrency_can_grant(queue, max_allowed) {
                // Grant immediately: write holder and the task
                let holder = HolderRecord {
                    granted_at_ms: now_ms,
                };
                let holder_val: AlignedVec = rkyv::to_bytes::<HolderRecord, 256>(&holder)
                    .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                batch.put(
                    concurrency_holder_key(queue, &first_task_id).as_bytes(),
                    &holder_val,
                );
                let first_task = Task::RunAttempt {
                    id: first_task_id.clone(),
                    job_id: job_id.clone(),
                    attempt_number: 1,
                    held_queues: vec![queue.clone()],
                };
                let task_value: AlignedVec = rkyv::to_bytes::<Task, 256>(&first_task)
                    .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                batch.put(
                    task_key(start_at_ms, priority, &job_id, 1).as_bytes(),
                    &task_value,
                );
            } else {
                // Persist a request: when granted, enqueue the task
                let action = ConcurrencyAction::EnqueueTask {
                    start_time_ms: start_at_ms,
                    priority,
                    job_id: job_id.clone(),
                    attempt_number: 1,
                };
                let action_val: AlignedVec = rkyv::to_bytes::<ConcurrencyAction, 256>(&action)
                    .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                let req_key = concurrency_request_key(queue, now_ms, &Uuid::new_v4().to_string());
                batch.put(req_key.as_bytes(), &action_val);
            }
        } else {
            // No concurrency limits; write task directly
            let first_task = Task::RunAttempt {
                id: first_task_id.clone(),
                job_id: job_id.clone(),
                attempt_number: 1,
                held_queues: Vec::new(),
            };
            let task_value: AlignedVec = rkyv::to_bytes::<Task, 256>(&first_task)
                .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
            batch.put(
                task_key(start_at_ms, priority, &job_id, 1).as_bytes(),
                &task_value,
            );
        }

        self.db.write(batch).await?;
        self.db.flush().await?;
        // Update in-memory concurrency count after durability if we granted immediately and wrote holder
        if let Some(limit) = first_limit_opt.as_ref() {
            let queue = &limit.key;
            if self
                .db
                .get(concurrency_holder_key(queue, &first_task_id).as_bytes())
                .await?
                .is_some()
            {
                self.broker.concurrency_record_grant(queue, &first_task_id);
            }
        }

        // If ready now, wake the scanner to refill promptly
        if start_at_ms <= now_epoch_ms() {
            self.broker.wakeup();
        }

        Ok(job_id)
    }

    /// Delete a job by id.
    pub async fn delete_job(&self, id: &str) -> Result<(), JobStoreShardError> {
        let job_info_key: String = job_info_key(id);
        let job_status_key: String = job_status_key(id);
        let mut batch = WriteBatch::new();
        batch.delete(job_info_key.as_bytes());
        batch.delete(job_status_key.as_bytes());
        self.db.write(batch).await?;
        self.db.flush().await?;
        Ok(())
    }

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobView>, JobStoreShardError> {
        let key = job_info_key(id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        if let Some(raw) = maybe_raw {
            Ok(Some(JobView::new(raw)?))
        } else {
            Ok(None)
        }
    }

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job_status(&self, id: &str) -> Result<Option<JobStatus>, JobStoreShardError> {
        let key = job_status_key(id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        let Some(raw) = maybe_raw else {
            return Ok(None);
        };

        #[cfg(debug_assertions)]
        {
            let _ = rkyv::check_archived_root::<JobStatus>(&raw)
                .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
        }

        type ArchivedStatus = <JobStatus as Archive>::Archived;
        let archived: &ArchivedStatus = unsafe { rkyv::archived_root::<JobStatus>(&raw) };
        let mut des = rkyv::Infallible;
        let status: JobStatus = RkyvDeserialize::deserialize(archived, &mut des)
            .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus"));
        Ok(Some(status))
    }

    /// Fetch a job attempt by job id and attempt number.
    pub async fn get_job_attempt(
        &self,
        job_id: &str,
        attempt_number: u32,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let key = attempt_key(job_id, attempt_number);
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
    pub async fn dequeue(
        &self,
        worker_id: &str,
        max_tasks: usize,
    ) -> Result<Vec<LeasedTask>, JobStoreShardError> {
        if max_tasks == 0 {
            return Ok(Vec::new());
        }
        let claimed: Vec<BrokerTask> = self.broker.claim_ready_or_nudge(max_tasks).await;
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = now_epoch_ms();
        let expiry_ms = now_ms + DEFAULT_LEASE_MS;

        let mut batch = WriteBatch::new();
        let mut out: Vec<LeasedTask> = Vec::new();
        let mut pending_attempts: Vec<(JobView, String, u32)> = Vec::new();
        let mut ack_keys: Vec<String> = Vec::with_capacity(claimed.len());

        for entry in &claimed {
            let task = &entry.task;
            // Only RunAttempt exists currently
            let (task_id, job_id, attempt_number, held_queues) = match task {
                Task::RunAttempt {
                    id,
                    job_id,
                    attempt_number,
                    held_queues,
                } => (
                    id.clone(),
                    job_id.to_string(),
                    *attempt_number,
                    held_queues.clone(),
                ),
            };

            // Look up job info; if missing, delete the task and skip
            let job_key = job_info_key(&job_id);
            let maybe_job = self.db.get(job_key.as_bytes()).await?;
            if let Some(job_bytes) = maybe_job {
                let view = JobView::new(job_bytes)?;

                // Create lease record and delete task from task queue
                let lease_key = leased_task_key(&task_id);
                let record = LeaseRecord {
                    worker_id: worker_id.to_string(),
                    task: task.clone(),
                    expiry_ms,
                };
                let leased_value: AlignedVec = rkyv::to_bytes::<LeaseRecord, 256>(&record)
                    .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;

                batch.put(lease_key.as_bytes(), &leased_value);
                batch.delete(entry.key.as_bytes());

                // Mark job as running
                let job_status = JobStatus::Running {};
                put_job_status(&mut batch, &job_id, &job_status)?;

                // Also mark attempt as running
                let attempt = JobAttempt {
                    job_id: job_id.clone(),
                    attempt_number,
                    task_id: task_id.clone(),
                    status: AttemptStatus::Running {
                        started_at_ms: now_ms,
                    },
                };
                let attempt_val: AlignedVec = rkyv::to_bytes::<JobAttempt, 256>(&attempt)
                    .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
                let akey = attempt_key(&job_id, attempt_number);
                batch.put(akey.as_bytes(), &attempt_val);

                // Defer constructing AttemptView; fetch from DB after batch is written
                pending_attempts.push((view, job_id.clone(), attempt_number));
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

        // Ack durable and evict from buffer; we no longer use TTL tombstones.
        self.broker.ack_durable(&ack_keys);
        self.broker.evict_keys(&ack_keys);

        for (job_view, job_id, attempt_number) in pending_attempts.into_iter() {
            let attempt_view = self
                .get_job_attempt(&job_id, attempt_number)
                .await?
                .ok_or_else(|| {
                    JobStoreShardError::Rkyv("attempt not found after dequeue".to_string())
                })?;
            out.push(LeasedTask {
                job: job_view,
                attempt: attempt_view,
            });
        }

        Ok(out)
    }

    /// Internal: scan up to `max_tasks` ready tasks and return them with their keys.
    async fn scan_ready_tasks(
        &self,
        max_tasks: usize,
    ) -> Result<(Vec<Task>, Vec<Vec<u8>>), JobStoreShardError> {
        if max_tasks == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        let start: Vec<u8> = b"tasks/".to_vec();
        let end_prefix = task_key(now_epoch_ms(), 99, "~", u32::MAX);
        let mut end: Vec<u8> = end_prefix.into_bytes();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        let mut tasks: Vec<Task> = Vec::with_capacity(max_tasks);
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(max_tasks);
        while tasks.len() < max_tasks {
            let maybe_kv = iter.next().await?;
            let Some(kv) = maybe_kv else {
                break;
            };

            type ArchivedTask = <Task as Archive>::Archived;
            let archived: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&kv.value) };
            match archived {
                ArchivedTask::RunAttempt {
                    id,
                    job_id,
                    attempt_number,
                    held_queues,
                } => {
                    tasks.push(Task::RunAttempt {
                        id: id.as_str().to_string(),
                        job_id: job_id.as_str().to_string(),
                        attempt_number: *attempt_number,
                        held_queues: held_queues
                            .iter()
                            .map(|s| s.as_str().to_string())
                            .collect::<Vec<String>>(),
                    });
                }
            }
            keys.push(kv.key.to_vec());
        }

        Ok((tasks, keys))
    }

    /// Heartbeat a lease to renew it if the worker id matches. Bumps expiry by DEFAULT_LEASE_MS
    pub async fn heartbeat_task(
        &self,
        worker_id: &str,
        task_id: &str,
    ) -> Result<(), JobStoreShardError> {
        // Directly read the lease for this task id
        let key = leased_task_key(task_id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        type ArchivedLease = <LeaseRecord as Archive>::Archived;
        type ArchivedTask = <Task as Archive>::Archived;
        let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&value_bytes) };
        let current_owner = archived.worker_id.as_str();
        if current_owner != worker_id {
            return Err(JobStoreShardError::LeaseOwnerMismatch {
                task_id: task_id.to_string(),
                expected: current_owner.to_string(),
                got: worker_id.to_string(),
            });
        }

        // Renew by updating value with a new expiry
        let now_ms = now_epoch_ms();
        let new_expiry = now_ms + DEFAULT_LEASE_MS;

        // Reconstruct record (clone from archived)
        let task = match &archived.task {
            ArchivedTask::RunAttempt {
                id,
                job_id,
                attempt_number,
                held_queues,
            } => Task::RunAttempt {
                id: id.as_str().to_string(),
                job_id: job_id.as_str().to_string(),
                attempt_number: *attempt_number,
                held_queues: held_queues
                    .iter()
                    .map(|s| s.as_str().to_string())
                    .collect::<Vec<String>>(),
            },
        };
        let record = LeaseRecord {
            worker_id: current_owner.to_string(),
            task,
            expiry_ms: new_expiry,
        };
        let value: AlignedVec = rkyv::to_bytes::<LeaseRecord, 256>(&record)
            .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;

        let mut batch = WriteBatch::new();
        batch.put(key.as_bytes(), &value);
        self.db.write(batch).await?;
        self.db.flush().await?;
        Ok(())
    }

    /// Report the outcome of a running attempt identified by task id.
    /// Removes the lease and finalizes the attempt state.
    pub async fn report_attempt_outcome(
        &self,
        task_id: &str,
        outcome: AttemptOutcome,
    ) -> Result<(), JobStoreShardError> {
        // Load lease; must exist
        let leased_task_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(leased_task_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        type ArchivedLease = <LeaseRecord as Archive>::Archived;
        type ArchivedTask = <Task as Archive>::Archived;
        let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&value_bytes) };
        let (job_id, attempt_number, held_queues): (String, u32, Vec<String>) = match &archived.task
        {
            ArchivedTask::RunAttempt {
                id: _tid,
                job_id,
                attempt_number,
                held_queues,
            } => (
                job_id.as_str().to_string(),
                *attempt_number,
                held_queues
                    .iter()
                    .map(|s| s.as_str().to_string())
                    .collect::<Vec<String>>(),
            ),
        };

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
        };
        let attempt = JobAttempt {
            job_id: job_id.clone(),
            attempt_number,
            task_id: task_id.to_string(),
            status: attempt_status,
        };
        let attempt_val: AlignedVec = rkyv::to_bytes::<JobAttempt, 256>(&attempt)
            .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
        let attempt_key = attempt_key(&job_id, attempt_number);

        // Atomically update attempt and remove lease
        let mut batch = WriteBatch::new();
        batch.put(attempt_key.as_bytes(), &attempt_val);
        batch.delete(leased_task_key.as_bytes());

        let mut job_missing_error: Option<JobStoreShardError> = None;
        let mut followup_next_time: Option<i64> = None;

        // If success: mark job succeeded now.
        if let AttemptOutcome::Success { .. } = outcome {
            let job_status = JobStatus::Succeeded {};
            put_job_status(&mut batch, &job_id, &job_status)?;
        } else {
            // if error, maybe enqueue next attempt; otherwise mark job failed
            let mut scheduled_followup: bool = false;

            if let AttemptOutcome::Error { .. } = outcome {
                // Load job info to get priority and retry policy
                let job_info_key = job_info_key(&job_id);
                let maybe_job = self.db.get(job_info_key.as_bytes()).await?;
                if let Some(jbytes) = maybe_job {
                    let view = JobView::new(jbytes)?;
                    let priority = view.priority();
                    let failures_so_far = attempt_number;
                    if let Some(policy_rt) = view.retry_policy() {
                        if let Some(next_time) =
                            crate::retry::next_retry_time_ms(now_ms, failures_so_far, &policy_rt)
                        {
                            let next_attempt_number = attempt_number + 1;
                            let next_task = Task::RunAttempt {
                                id: Uuid::new_v4().to_string(),
                                job_id: job_id.clone(),
                                attempt_number: next_attempt_number,
                                // Retain held tickets across retries until completion
                                held_queues: held_queues.clone(),
                            };
                            let next_bytes: AlignedVec = rkyv::to_bytes::<Task, 256>(&next_task)
                                .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;

                            batch.put(
                                task_key(next_time, priority, &job_id, next_attempt_number)
                                    .as_bytes(),
                                &next_bytes,
                            );

                            let job_status = JobStatus::Scheduled {};
                            put_job_status(&mut batch, &job_id, &job_status)?;
                            scheduled_followup = true;
                            followup_next_time = Some(next_time);
                        }
                    }
                    // If no follow-up scheduled, mark job as failed
                    if !scheduled_followup {
                        let job_status = JobStatus::Failed {};
                        put_job_status(&mut batch, &job_id, &job_status)?;
                    }
                } else {
                    job_missing_error = Some(JobStoreShardError::JobNotFound(job_id.clone()));
                }
            }
        }

        // If job finished (success or no follow-up), release any held concurrency tickets
        if !(matches!(outcome, AttemptOutcome::Error { .. }) && followup_next_time.is_some()) {
            self.release_and_grant_next(&mut batch, &held_queues, task_id)
                .await?;
        }

        self.db.write(batch).await?;
        self.db.flush().await?;
        // Update in-memory broker counts after durable release/grant
        if !(matches!(outcome, AttemptOutcome::Error { .. }) && followup_next_time.is_some()) {
            for q in held_queues.iter() {
                self.broker.concurrency_record_release(q, task_id);
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
        Ok(())
    }

    /// Scan all held leases and mark any expired ones as failed with a WORKER_CRASHED error code.
    /// Returns the number of expired leases reaped.
    pub async fn reap_expired_leases(&self) -> Result<usize, JobStoreShardError> {
        let start: Vec<u8> = b"lease/".to_vec();
        let mut end: Vec<u8> = b"lease/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        let now_ms = now_epoch_ms();
        let mut reaped: usize = 0;
        loop {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };

            type ArchivedLease = <LeaseRecord as Archive>::Archived;
            type ArchivedTask = <Task as Archive>::Archived;
            let lease: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&kv.value) };
            if lease.expiry_ms > now_ms {
                continue;
            }

            // Determine the task id from the archived task
            let task_id = match &lease.task {
                ArchivedTask::RunAttempt { id, .. } => id.as_str().to_string(),
            };

            // Report as worker crashed; ignore LeaseNotFound in case of concurrent cleanup
            let _ = self
                .report_attempt_outcome(
                    &task_id,
                    AttemptOutcome::Error {
                        error_code: "WORKER_CRASHED".to_string(),
                        error: format!(
                            "lease expired at {} (now {}), worker={}",
                            lease.expiry_ms,
                            now_ms,
                            lease.worker_id.as_str()
                        )
                        .into_bytes(),
                    },
                )
                .await;
            reaped += 1;
        }

        Ok(reaped)
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
    job_id: &str,
    status: &JobStatus,
) -> Result<(), JobStoreShardError> {
    let job_status_value: AlignedVec = rkyv::to_bytes::<JobStatus, 256>(status)
        .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
    batch.put(job_status_key(job_id).as_bytes(), &job_status_value);
    Ok(())
}
