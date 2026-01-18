//! Task types and related structures for the job store.
//!
//! This module contains the core task types that represent units of work
//! in the system, along with associated records for leases and concurrency.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::job::{GubernatorRateLimit, JobView};
use crate::job_attempt::JobAttemptView;

/// Default lease duration for dequeued tasks (milliseconds)
pub const DEFAULT_LEASE_MS: i64 = 10_000;

/// A task is a unit of work that a worker needs to pickup and action to move the system forward.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum Task {
    /// Execute a specific attempt for a job
    RunAttempt {
        id: String,
        tenant: String,
        job_id: String,
        attempt_number: u32,
        held_queues: Vec<String>,
        task_group: String,
    },
    /// Internal: request a concurrency ticket for a queue at or after a specific time
    RequestTicket {
        queue: String,
        start_time_ms: i64,
        priority: u8,
        tenant: String,
        job_id: String,
        attempt_number: u32,
        request_id: String,
        task_group: String,
    },
    /// Internal: check a Gubernator rate limit before proceeding
    CheckRateLimit {
        task_id: String,
        tenant: String,
        job_id: String,
        attempt_number: u32,
        /// Index of the current limit being checked in the job's limits array
        limit_index: u32,
        /// Rate limit parameters (serialized for storage)
        rate_limit: GubernatorRateLimitData,
        /// Number of retry attempts so far
        retry_count: u32,
        /// When we started trying to acquire this rate limit
        started_at_ms: i64,
        /// Priority for enqueueing subsequent tasks
        priority: u8,
        /// Queues already held from previous concurrency limits
        held_queues: Vec<String>,
        task_group: String,
    },
    /// Worker task: refresh a floating concurrency limit's max concurrency value
    RefreshFloatingLimit {
        task_id: String,
        tenant: String,
        queue_key: String,
        current_max_concurrency: u32,
        last_refreshed_at_ms: i64,
        /// Opaque metadata from the floating limit definition
        metadata: Vec<(String, String)>,
        task_group: String,
    },
}

/// Serializable rate limit data stored with CheckRateLimit tasks
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct GubernatorRateLimitData {
    pub name: String,
    pub unique_key: String,
    pub limit: i64,
    pub duration_ms: i64,
    pub hits: i32,
    pub algorithm: u8, // 0 = TokenBucket, 1 = LeakyBucket
    pub behavior: i32,
    pub retry_initial_backoff_ms: i64,
    pub retry_max_backoff_ms: i64,
    pub retry_backoff_multiplier: f64,
    pub retry_max_retries: u32,
}

impl From<&GubernatorRateLimit> for GubernatorRateLimitData {
    fn from(rl: &GubernatorRateLimit) -> Self {
        Self {
            name: rl.name.clone(),
            unique_key: rl.unique_key.clone(),
            limit: rl.limit,
            duration_ms: rl.duration_ms,
            hits: rl.hits,
            algorithm: rl.algorithm.as_u8(),
            behavior: rl.behavior,
            retry_initial_backoff_ms: rl.retry_policy.initial_backoff_ms,
            retry_max_backoff_ms: rl.retry_policy.max_backoff_ms,
            retry_backoff_multiplier: rl.retry_policy.backoff_multiplier,
            retry_max_retries: rl.retry_policy.max_retries,
        }
    }
}

/// Stored representation for a lease record. Value at `lease/<task-id>`
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
        task_group: String,
    },
}

/// Represents a leased task with the associated job metadata necessary to execute it.
#[derive(Debug, Clone)]
pub struct LeasedTask {
    job: JobView,
    attempt: JobAttemptView,
}

impl LeasedTask {
    pub fn new(job: JobView, attempt: JobAttemptView) -> Self {
        Self { job, attempt }
    }

    pub fn job(&self) -> &JobView {
        &self.job
    }

    pub fn attempt(&self) -> &JobAttemptView {
        &self.attempt
    }
}

/// Result from heartbeat indicating if the job has been cancelled.
/// Workers should check this and begin graceful shutdown if cancelled.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    /// True if the job has been cancelled. Worker should stop work and report Cancelled outcome.
    pub cancelled: bool,
    /// Timestamp when cancellation was requested, if cancelled
    pub cancelled_at_ms: Option<i64>,
}

/// A leased refresh task for floating concurrency limits
#[derive(Debug, Clone)]
pub struct LeasedRefreshTask {
    pub task_id: String,
    pub queue_key: String,
    pub current_max_concurrency: u32,
    pub last_refreshed_at_ms: i64,
    pub metadata: Vec<(String, String)>,
    pub task_group: String,
}
