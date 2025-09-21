use bytes::Bytes;
use rkyv::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde_json::Value as JsonValue;
use slatedb::Db;
use slatedb::DbIterator;
use slatedb::WriteBatch;
use thiserror::Error;
use uuid::Uuid;

use crate::keys::{attempt_key, job_info_key, leased_task_key, task_key};
use crate::retry::RetryPolicy;
use crate::settings::DatabaseConfig;
use crate::storage::{resolve_object_store, StorageError};

/// Represents a single shard of the system. Owns the SlateDB instance.
pub struct Shard {
    name: String,
    db: Db,
}

#[derive(Debug, Error)]
pub enum ShardError {
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
}

/// Default lease duration for dequeued tasks (milliseconds)
const DEFAULT_LEASE_MS: i64 = 10_000;

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobInfo {
    pub id: String,
    pub priority: u8,         // 0..=99, 0 is highest priority and will run first
    pub enqueue_time_ms: i64, // epoch millis
    pub payload: Vec<u8>,     // JSON bytes for now (opaque to rkyv)
    pub retry_policy: Option<RetryPolicy>,
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
    },
}

/// Outcome passed by callers when reporting an attempt's completion.
#[derive(Debug, Clone)]
pub enum AttemptOutcome {
    Success { result: Vec<u8> },
    Error { error_code: String, error: Vec<u8> },
}

/// Attempt state lifecycle for a job attempt
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum AttemptState {
    Running {
        started_at_ms: i64,
    },
    Succeeded {
        finished_at_ms: i64,
        result: Vec<u8>,
    },
    Failed {
        finished_at_ms: i64,
        error_code: String,
        error: Vec<u8>,
    },
}

/// Stored representation of a job attempt
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobAttempt {
    pub job_id: String,
    pub attempt_number: u32,
    pub task_id: String,
    pub state: AttemptState,
}

/// Zero-copy view over an archived `JobAttempt`
pub struct JobAttemptView {
    bytes: Bytes,
}

impl JobAttemptView {
    pub fn new(bytes: Bytes) -> Result<Self, ShardError> {
        #[cfg(debug_assertions)]
        {
            let _ = rkyv::check_archived_root::<JobAttempt>(&bytes)
                .map_err(|e| ShardError::Rkyv(e.to_string()))?;
        }
        Ok(Self { bytes })
    }

    fn archived(&self) -> &<JobAttempt as Archive>::Archived {
        unsafe { rkyv::archived_root::<JobAttempt>(&self.bytes) }
    }

    pub fn job_id(&self) -> &str {
        self.archived().job_id.as_str()
    }
    pub fn attempt_number(&self) -> u32 {
        self.archived().attempt_number
    }
    pub fn task_id(&self) -> &str {
        self.archived().task_id.as_str()
    }

    pub fn state(&self) -> AttemptState {
        type ArchivedAttemptState = <AttemptState as Archive>::Archived;
        match &self.archived().state {
            ArchivedAttemptState::Running { started_at_ms } => AttemptState::Running {
                started_at_ms: *started_at_ms,
            },
            ArchivedAttemptState::Succeeded {
                finished_at_ms,
                result,
            } => AttemptState::Succeeded {
                finished_at_ms: *finished_at_ms,
                result: result.to_vec(),
            },
            ArchivedAttemptState::Failed {
                finished_at_ms,
                error_code,
                error,
            } => AttemptState::Failed {
                finished_at_ms: *finished_at_ms,
                error_code: error_code.as_str().to_string(),
                error: error.to_vec(),
            },
        }
    }
}

/// Stored representation for a lease record. Value at `lease/<expiry>/<task-id>`
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct LeaseRecord {
    pub worker_id: String,
    pub task: Task,
    pub expiry_ms: i64,
}

fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

/// Zero-copy view over an archived `JobInfo` backed by owned bytes.
pub struct JobView {
    info_bytes: Bytes,
}

impl JobView {
    /// Validate bytes and construct a zero-copy view.
    pub fn new(bytes: Bytes) -> Result<Self, ShardError> {
        // Validate once up front in debug builds; skip in release for performance.
        #[cfg(debug_assertions)]
        {
            let _ = rkyv::check_archived_root::<JobInfo>(&bytes)
                .map_err(|e| ShardError::Rkyv(e.to_string()))?;
        }
        Ok(Self { info_bytes: bytes })
    }

    pub fn id(&self) -> &str {
        self.archived().id.as_str()
    }
    pub fn priority(&self) -> u8 {
        self.archived().priority
    }
    pub fn enqueue_time_ms(&self) -> i64 {
        self.archived().enqueue_time_ms
    }
    pub fn payload_bytes(&self) -> &[u8] {
        self.archived().payload.as_ref()
    }
    pub fn payload_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::from_slice(self.payload_bytes())
    }

    /// Unsafe-free accessor to the archived root (validated at construction).
    fn archived(&self) -> &<JobInfo as Archive>::Archived {
        // Safe because we validated in new() and bytes are owned by self
        unsafe { rkyv::archived_root::<JobInfo>(&self.info_bytes) }
    }

    /// Return the job's retry policy as a runtime struct, if present, by copying
    /// primitive fields from the archived view.
    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        let a = self.archived();
        let Some(pol) = a.retry_policy.as_ref() else {
            return None;
        };
        Some(RetryPolicy {
            retry_count: pol.retry_count,
            initial_interval_ms: pol.initial_interval_ms,
            max_interval_ms: pol.max_interval_ms,
            randomize_interval: pol.randomize_interval,
            backoff_factor: pol.backoff_factor,
        })
    }
}

impl Shard {
    pub async fn open(cfg: &DatabaseConfig) -> Result<Self, ShardError> {
        let object_store = resolve_object_store(&cfg.backend, &cfg.path)?;
        let db = Db::open(cfg.path.as_str(), object_store).await?;
        Ok(Self {
            name: cfg.name.clone(),
            db,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Enqueue a new job.
    pub async fn enqueue(
        &self,
        id: Option<String>,
        priority: u8,
        start_at_ms: i64,
        retry_policy: Option<RetryPolicy>,
        payload: JsonValue,
    ) -> Result<String, ShardError> {
        let job_id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        // If caller provided an id, ensure it doesn't already exist
        if self
            .db
            .get(job_info_key(&job_id).as_bytes())
            .await?
            .is_some()
        {
            return Err(ShardError::JobAlreadyExists(job_id));
        }
        let payload_bytes = serde_json::to_vec(&payload)?;
        let job = JobInfo {
            id: job_id.clone(),
            priority,
            enqueue_time_ms: start_at_ms,
            payload: payload_bytes,
            retry_policy,
        };
        let job_value: AlignedVec =
            rkyv::to_bytes::<JobInfo, 256>(&job).map_err(|e| ShardError::Rkyv(e.to_string()))?;

        let first_task = Task::RunAttempt {
            id: Uuid::new_v4().to_string(),
            job_id: job_id.clone(),
            attempt_number: 1,
        };
        let task_value: AlignedVec = rkyv::to_bytes::<Task, 256>(&first_task)
            .map_err(|e| ShardError::Rkyv(e.to_string()))?;

        // Atomically write both job info and the first task
        let mut batch = WriteBatch::new();
        batch.put(job_info_key(&job_id).as_bytes(), &job_value);
        batch.put(
            task_key(start_at_ms, priority, &job_id, 1).as_bytes(),
            &task_value,
        );
        self.db.write(batch).await?;
        self.db.flush().await?;

        Ok(job_id)
    }

    /// Delete a job by id.
    pub async fn delete_job(&self, id: &str) -> Result<(), ShardError> {
        let key = job_info_key(id);
        let mut batch = WriteBatch::new();
        batch.delete(key.as_bytes());
        self.db.write(batch).await?;
        self.db.flush().await?;
        Ok(())
    }

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobView>, ShardError> {
        let key = job_info_key(id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        if let Some(raw) = maybe_raw {
            Ok(Some(JobView::new(raw)?))
        } else {
            Ok(None)
        }
    }

    /// Fetch a job attempt by job id and attempt number.
    pub async fn get_job_attempt(
        &self,
        job_id: &str,
        attempt_number: u32,
    ) -> Result<Option<JobAttemptView>, ShardError> {
        let key = attempt_key(job_id, attempt_number);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        if let Some(raw) = maybe_raw {
            Ok(Some(JobAttemptView::new(raw)?))
        } else {
            Ok(None)
        }
    }

    /// Peek up to `max_tasks` available tasks (time <= now), without deleting them.
    pub async fn peek_tasks(&self, max_tasks: usize) -> Result<Vec<Task>, ShardError> {
        let (tasks, _keys) = self.scan_ready_tasks(max_tasks).await?;
        Ok(tasks)
    }

    /// Dequeue up to `max_tasks` tasks available now, ordered by time then priority.
    pub async fn dequeue(
        &self,
        worker_id: &str,
        max_tasks: usize,
    ) -> Result<Vec<Task>, ShardError> {
        let (tasks, to_delete) = self.scan_ready_tasks(max_tasks).await?;
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = now_epoch_ms();
        let expiry_ms = now_ms + DEFAULT_LEASE_MS;

        let mut batch = WriteBatch::new();
        for (task, orig_key) in tasks.iter().zip(to_delete.iter()) {
            let task_id = match task {
                Task::RunAttempt { id, .. } => id,
            };
            let leased_key = leased_task_key(task_id);
            let record = LeaseRecord {
                worker_id: worker_id.to_string(),
                task: task.clone(),
                expiry_ms,
            };
            let leased_value: AlignedVec = rkyv::to_bytes::<LeaseRecord, 256>(&record)
                .map_err(|e| ShardError::Rkyv(e.to_string()))?;
            batch.put(leased_key.as_bytes(), &leased_value);
            batch.delete(orig_key);

            // Also mark attempt as running
            let Task::RunAttempt {
                id,
                job_id,
                attempt_number,
            } = task;
            let attempt = JobAttempt {
                job_id: job_id.clone(),
                attempt_number: *attempt_number,
                task_id: id.clone(),
                state: AttemptState::Running {
                    started_at_ms: now_ms,
                },
            };
            let attempt_val: AlignedVec = rkyv::to_bytes::<JobAttempt, 256>(&attempt)
                .map_err(|e| ShardError::Rkyv(e.to_string()))?;
            let akey = attempt_key(job_id, *attempt_number);
            batch.put(akey.as_bytes(), &attempt_val);
        }
        self.db.write(batch).await?;
        self.db.flush().await?;

        Ok(tasks)
    }

    /// Internal: scan up to `max_tasks` ready tasks and return them with their keys.
    async fn scan_ready_tasks(
        &self,
        max_tasks: usize,
    ) -> Result<(Vec<Task>, Vec<Vec<u8>>), ShardError> {
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
                } => {
                    tasks.push(Task::RunAttempt {
                        id: id.as_str().to_string(),
                        job_id: job_id.as_str().to_string(),
                        attempt_number: *attempt_number,
                    });
                }
            }
            keys.push(kv.key.to_vec());
        }

        Ok((tasks, keys))
    }

    /// Heartbeat a lease to renew it if the worker id matches. Bumps expiry by DEFAULT_LEASE_MS
    pub async fn heartbeat_task(&self, worker_id: &str, task_id: &str) -> Result<(), ShardError> {
        // Directly read the lease for this task id
        let key = leased_task_key(task_id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(ShardError::LeaseNotFound(task_id.to_string()));
        };

        type ArchivedLease = <LeaseRecord as Archive>::Archived;
        type ArchivedTask = <Task as Archive>::Archived;
        let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&value_bytes) };
        let current_owner = archived.worker_id.as_str();
        if current_owner != worker_id {
            return Err(ShardError::LeaseOwnerMismatch {
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
            } => Task::RunAttempt {
                id: id.as_str().to_string(),
                job_id: job_id.as_str().to_string(),
                attempt_number: *attempt_number,
            },
        };
        let record = LeaseRecord {
            worker_id: current_owner.to_string(),
            task,
            expiry_ms: new_expiry,
        };
        let value: AlignedVec = rkyv::to_bytes::<LeaseRecord, 256>(&record)
            .map_err(|e| ShardError::Rkyv(e.to_string()))?;

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
    ) -> Result<(), ShardError> {
        // Load lease; must exist
        let key = leased_task_key(task_id);
        let maybe_raw = self.db.get(key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(ShardError::LeaseNotFound(task_id.to_string()));
        };

        type ArchivedLease = <LeaseRecord as Archive>::Archived;
        type ArchivedTask = <Task as Archive>::Archived;
        let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&value_bytes) };
        let (job_id, attempt_number) = match &archived.task {
            ArchivedTask::RunAttempt {
                id: _tid,
                job_id,
                attempt_number,
            } => (job_id.as_str().to_string(), *attempt_number),
        };

        let now_ms = now_epoch_ms();
        let state = match &outcome {
            AttemptOutcome::Success { result } => AttemptState::Succeeded {
                finished_at_ms: now_ms,
                result: result.clone(),
            },
            AttemptOutcome::Error { error_code, error } => AttemptState::Failed {
                finished_at_ms: now_ms,
                error_code: error_code.clone(),
                error: error.clone(),
            },
        };
        let attempt = JobAttempt {
            job_id: job_id.clone(),
            attempt_number,
            task_id: task_id.to_string(),
            state,
        };
        let attempt_val: AlignedVec = rkyv::to_bytes::<JobAttempt, 256>(&attempt)
            .map_err(|e| ShardError::Rkyv(e.to_string()))?;
        let akey = attempt_key(&job_id, attempt_number);

        // Atomically update attempt and remove lease; if error, maybe enqueue next attempt
        let mut batch = WriteBatch::new();
        batch.put(akey.as_bytes(), &attempt_val);
        batch.delete(key.as_bytes());

        if let AttemptOutcome::Error { .. } = outcome {
            // Load job info to get priority and retry policy
            let jkey = job_info_key(&job_id);
            if let Some(jbytes) = self.db.get(jkey.as_bytes()).await? {
                let view = JobView::new(jbytes)?;
                let priority = view.priority();
                let failures_so_far = attempt_number;
                if let Some(policy_rt) = view.retry_policy() {
                    if let Some(next_time) =
                        crate::retry::next_retry_time_ms(now_ms, failures_so_far, &policy_rt)
                    {
                        let next_task = Task::RunAttempt {
                            id: Uuid::new_v4().to_string(),
                            job_id: job_id.clone(),
                            attempt_number: attempt_number + 1,
                        };
                        let next_bytes: AlignedVec = rkyv::to_bytes::<Task, 256>(&next_task)
                            .map_err(|e| ShardError::Rkyv(e.to_string()))?;
                        let tkey = task_key(next_time, priority, &job_id, attempt_number + 1);
                        batch.put(tkey.as_bytes(), &next_bytes);
                    }
                }
            }
        }

        self.db.write(batch).await?;
        self.db.flush().await?;
        Ok(())
    }

    /// Scan all held leases and mark any expired ones as failed with a WORKER_CRASHED error code.
    /// Returns the number of expired leases reaped.
    pub async fn reap_expired_leases(&self) -> Result<usize, ShardError> {
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
