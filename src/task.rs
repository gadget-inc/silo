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
    },
    /// Internal: request a concurrency ticket for a LOCAL queue at or after a specific time.
    /// Used when the queue owner shard == job shard.
    RequestTicket {
        queue: String,
        start_time_ms: i64,
        priority: u8,
        tenant: String,
        job_id: String,
        attempt_number: u32,
        request_id: String,
        /// Queues already held from previous concurrency limits
        held_queues: Vec<String>,
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
    },

    // =========================================================================
    // Cross-shard concurrency coordination tasks (queue owner != job shard)
    // =========================================================================

    /// Request a concurrency ticket from a REMOTE queue owner shard.
    /// Created by the job shard when enqueue determines the queue is remote.
    /// Processing this task sends an RPC to the queue owner shard.
    RequestRemoteTicket {
        /// Task ID for this request task
        task_id: String,
        /// Target shard that owns the queue
        queue_owner_shard: u32,
        tenant: String,
        queue_key: String,
        job_id: String,
        attempt_number: u32,
        priority: u8,
        start_time_ms: i64,
        /// Unique ID for correlating request -> grant
        request_id: String,
        /// For floating limits, includes the full definition so queue owner can create state.
        /// None for regular ConcurrencyLimit (not FloatingConcurrencyLimit).
        floating_limit: Option<FloatingConcurrencyLimitData>,
    },

    /// Notify a job shard that a ticket has been granted.
    /// Created by the queue owner shard when a ticket becomes available.
    /// Processing this task sends an RPC to the job shard.
    NotifyRemoteTicketGrant {
        /// Task ID for this notification task
        task_id: String,
        /// Target shard where the job lives
        job_shard: u32,
        tenant: String,
        job_id: String,
        queue_key: String,
        /// Request ID for correlation
        request_id: String,
        /// Task ID to use as the holder (for release correlation)
        holder_task_id: String,
        /// Attempt number for the job (needed to create correct RunAttempt on job shard)
        attempt_number: u32,
    },

    /// Release a concurrency ticket back to a REMOTE queue owner shard.
    /// Created by the job shard when a job completes (success/failure/cancel).
    /// Processing this task sends an RPC to the queue owner shard.
    ReleaseRemoteTicket {
        /// Task ID for this release task
        task_id: String,
        /// Target shard that owns the queue
        queue_owner_shard: u32,
        tenant: String,
        queue_key: String,
        job_id: String,
        /// The task ID that held the ticket (for holder lookup)
        holder_task_id: String,
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

/// Serializable floating concurrency limit data stored with RequestRemoteTicket tasks
/// Allows the queue owner shard to create floating limit state for remote jobs.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct FloatingConcurrencyLimitData {
    pub default_max_concurrency: u32,
    pub refresh_interval_ms: i64,
    pub metadata: Vec<(String, String)>,
}

impl From<&crate::job::FloatingConcurrencyLimit> for FloatingConcurrencyLimitData {
    fn from(fl: &crate::job::FloatingConcurrencyLimit) -> Self {
        Self {
            default_max_concurrency: fl.default_max_concurrency,
            refresh_interval_ms: fl.refresh_interval_ms,
            metadata: fl.metadata.clone(),
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

/// Parsed remote queue reference for cross-shard ticket tracking.
///
/// Remote queues are stored in held_queues with the format:
/// `"remote:<queue_owner_shard>:<queue_key>:<holder_task_id>"`
///
/// This struct parses that format and provides a way to create it.
#[derive(Debug, Clone)]
pub struct RemoteQueueRef {
    pub queue_owner_shard: u32,
    pub queue_key: String,
    pub holder_task_id: String,
}

impl RemoteQueueRef {
    /// Parse a remote queue reference from a held_queues entry.
    /// Returns None if the string doesn't match the expected format.
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.splitn(4, ':').collect();
        if parts.len() != 4 || parts[0] != "remote" {
            return None;
        }
        Some(Self {
            queue_owner_shard: parts[1].parse().ok()?,
            queue_key: parts[2].to_string(),
            holder_task_id: parts[3].to_string(),
        })
    }

    /// Format a remote queue reference for storage in held_queues.
    pub fn format(queue_owner_shard: u32, queue_key: &str, holder_task_id: &str) -> String {
        format!("remote:{}:{}:{}", queue_owner_shard, queue_key, holder_task_id)
    }
}

/// Partition held_queues into local and remote queues.
///
/// Local queues are just queue keys (e.g., "api-limit").
/// Remote queues start with "remote:" and encode the queue owner shard info.
///
/// Returns (local_queues, remote_queues).
pub fn partition_held_queues(held_queues: &[String]) -> (Vec<String>, Vec<String>) {
    held_queues
        .iter()
        .cloned()
        .partition(|q| !q.starts_with("remote:"))
}

/// Stored representation for a concurrency holder record: value at holders/<queue>/<task-id>
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct HolderRecord {
    pub granted_at_ms: i64,
}

/// Action stored at requests/<queue>/<time>/<request-id> for LOCAL requests
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum ConcurrencyAction {
    /// When ticket is granted, enqueue the specified task (local queue)
    EnqueueTask {
        start_time_ms: i64,
        priority: u8,
        job_id: String,
        attempt_number: u32,
    },
}

/// Remote concurrency request stored at requests/<queue>/<time>/<priority>/<request-id>
/// Used when the requester is on a different shard from the queue owner.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct RemoteConcurrencyRequest {
    /// Shard where the job lives (for callback RPC)
    pub source_shard: u32,
    pub job_id: String,
    pub request_id: String,
    pub attempt_number: u32,
    /// Task ID that will be the holder when granted
    pub holder_task_id: String,
    /// Max concurrency for this queue (from the job's limit definition)
    pub max_concurrency: u32,
}

/// Holder record for remote tickets - tracks additional info needed for release
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct RemoteHolderRecord {
    pub granted_at_ms: i64,
    /// Shard where the job lives
    pub source_shard: u32,
    pub job_id: String,
}

/// Tracks tickets already acquired while a job is acquiring more via remote RPCs.
/// Stored on the job shard to avoid passing held_queues through cross-shard RPCs.
/// Key format: held_tickets/<job-id>/<attempt>/<request_id>
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct HeldTicketState {
    /// Queues already acquired from previous concurrency limits
    pub held_queues: Vec<String>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_queue_ref_parse_valid() {
        let input = "remote:2:api-limit:task-abc123";
        let parsed = RemoteQueueRef::parse(input).unwrap();

        assert_eq!(parsed.queue_owner_shard, 2);
        assert_eq!(parsed.queue_key, "api-limit");
        assert_eq!(parsed.holder_task_id, "task-abc123");
    }

    #[test]
    fn test_remote_queue_ref_parse_with_colons_in_values() {
        // queue_key and holder_task_id might contain colons
        let input = "remote:5:queue:with:colons:holder:id:here";
        let parsed = RemoteQueueRef::parse(input).unwrap();

        assert_eq!(parsed.queue_owner_shard, 5);
        assert_eq!(parsed.queue_key, "queue");
        // Everything after the third colon goes into holder_task_id
        assert_eq!(parsed.holder_task_id, "with:colons:holder:id:here");
    }

    #[test]
    fn test_remote_queue_ref_parse_invalid_prefix() {
        assert!(RemoteQueueRef::parse("local:2:api-limit:task-abc").is_none());
        assert!(RemoteQueueRef::parse("api-limit").is_none());
        assert!(RemoteQueueRef::parse("").is_none());
    }

    #[test]
    fn test_remote_queue_ref_parse_invalid_shard() {
        assert!(RemoteQueueRef::parse("remote:invalid:api-limit:task").is_none());
        assert!(RemoteQueueRef::parse("remote:-1:api-limit:task").is_none());
    }

    #[test]
    fn test_remote_queue_ref_parse_too_few_parts() {
        assert!(RemoteQueueRef::parse("remote:2:api-limit").is_none());
        assert!(RemoteQueueRef::parse("remote:2").is_none());
        assert!(RemoteQueueRef::parse("remote").is_none());
    }

    #[test]
    fn test_remote_queue_ref_format() {
        let formatted = RemoteQueueRef::format(3, "my-queue", "holder-123");
        assert_eq!(formatted, "remote:3:my-queue:holder-123");
    }

    #[test]
    fn test_remote_queue_ref_roundtrip() {
        let original = RemoteQueueRef {
            queue_owner_shard: 7,
            queue_key: "rate-limit".to_string(),
            holder_task_id: "uuid-abc-123".to_string(),
        };
        let formatted = RemoteQueueRef::format(
            original.queue_owner_shard,
            &original.queue_key,
            &original.holder_task_id,
        );
        let parsed = RemoteQueueRef::parse(&formatted).unwrap();

        assert_eq!(parsed.queue_owner_shard, original.queue_owner_shard);
        assert_eq!(parsed.queue_key, original.queue_key);
        assert_eq!(parsed.holder_task_id, original.holder_task_id);
    }

    #[test]
    fn test_partition_held_queues_empty() {
        let (local, remote) = partition_held_queues(&[]);
        assert!(local.is_empty());
        assert!(remote.is_empty());
    }

    #[test]
    fn test_partition_held_queues_only_local() {
        let queues = vec![
            "api-limit".to_string(),
            "db-connections".to_string(),
            "rate-limit".to_string(),
        ];
        let (local, remote) = partition_held_queues(&queues);

        assert_eq!(local.len(), 3);
        assert!(remote.is_empty());
        assert!(local.contains(&"api-limit".to_string()));
        assert!(local.contains(&"db-connections".to_string()));
        assert!(local.contains(&"rate-limit".to_string()));
    }

    #[test]
    fn test_partition_held_queues_only_remote() {
        let queues = vec![
            "remote:1:api-limit:task-1".to_string(),
            "remote:2:db-conn:task-2".to_string(),
        ];
        let (local, remote) = partition_held_queues(&queues);

        assert!(local.is_empty());
        assert_eq!(remote.len(), 2);
    }

    #[test]
    fn test_partition_held_queues_mixed() {
        let queues = vec![
            "local-queue".to_string(),
            "remote:1:api-limit:task-1".to_string(),
            "another-local".to_string(),
            "remote:3:rate-limit:task-2".to_string(),
        ];
        let (local, remote) = partition_held_queues(&queues);

        assert_eq!(local.len(), 2);
        assert_eq!(remote.len(), 2);
        assert!(local.contains(&"local-queue".to_string()));
        assert!(local.contains(&"another-local".to_string()));
        assert!(remote.contains(&"remote:1:api-limit:task-1".to_string()));
        assert!(remote.contains(&"remote:3:rate-limit:task-2".to_string()));
    }
}
