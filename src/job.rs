use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::codec::{CodecError, DecodedJobInfo, decode_job_info};
use crate::job_store_shard::JobStoreShardError;
use crate::retry::RetryPolicy;

/// Cancellation record stored at job_cancelled/<tenant>/<job-id>.
/// Cancellation is tracked separately from status to allow dequeue to blindly write Running without losing cancellation info.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobCancellation {
    /// Timestamp (epoch ms) when cancellation was requested
    pub cancelled_at_ms: i64,
}

fn codec_error_to_shard_error(e: CodecError) -> JobStoreShardError {
    JobStoreShardError::Rkyv(e.to_string())
}

/// Per-job concurrency limit declaration
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct ConcurrencyLimit {
    pub key: String,
    pub max_concurrency: u32,
}

/// Floating concurrency limit - max concurrency is dynamic and refreshed by workers
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct FloatingConcurrencyLimit {
    pub key: String,
    pub default_max_concurrency: u32,
    pub refresh_interval_ms: i64,
    pub metadata: Vec<(String, String)>,
}

/// State of a floating concurrency limit stored in the DB
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct FloatingLimitState {
    /// Current max concurrency value in use
    pub current_max_concurrency: u32,
    /// When the value was last successfully refreshed (epoch ms)
    pub last_refreshed_at_ms: i64,
    /// True if a refresh task is currently scheduled/in-progress
    pub refresh_task_scheduled: bool,
    /// Refresh interval in milliseconds
    pub refresh_interval_ms: i64,
    /// Default max concurrency for new limits
    pub default_max_concurrency: u32,
    /// Number of consecutive refresh failures (for exponential backoff)
    pub retry_count: u32,
    /// When the next retry should happen (epoch ms) if in backoff
    pub next_retry_at_ms: Option<i64>,
    /// Opaque metadata passed to workers during refresh
    pub metadata: Vec<(String, String)>,
}

impl FloatingLimitState {
    /// Create an owned copy from an archived (zero-copy) view.
    /// Callers can then mutate whichever fields they need before writing back.
    pub fn from_archived(archived: &ArchivedFloatingLimitState) -> Self {
        Self {
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
    }
}

/// Rate limiting algorithm used by Gubernator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum GubernatorAlgorithm {
    TokenBucket,
    LeakyBucket,
}

impl Default for GubernatorAlgorithm {
    fn default() -> Self {
        Self::TokenBucket
    }
}

impl GubernatorAlgorithm {
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::TokenBucket => 0,
            Self::LeakyBucket => 1,
        }
    }
}

/// Retry policy for rate limit checks when the limit is exceeded
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct RateLimitRetryPolicy {
    /// Initial backoff time when rate limited (ms)
    pub initial_backoff_ms: i64,
    /// Maximum backoff time (ms)
    pub max_backoff_ms: i64,
    /// Multiplier for exponential backoff (default 2.0)
    pub backoff_multiplier: f64,
    /// Maximum number of retries (0 = retry until reset_time)
    pub max_retries: u32,
}

impl Default for RateLimitRetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
            max_retries: 0, // Infinite retries until reset_time by default
        }
    }
}

/// Gubernator-based rate limit declaration
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct GubernatorRateLimit {
    /// Name identifying this rate limit (for debugging/metrics)
    pub name: String,
    /// Unique key for this specific rate limit instance
    pub unique_key: String,
    /// Maximum requests allowed in the duration
    pub limit: i64,
    /// Duration window in milliseconds
    pub duration_ms: i64,
    /// Number of hits to consume (usually 1)
    pub hits: i32,
    /// Rate limiting algorithm
    pub algorithm: GubernatorAlgorithm,
    /// Behavior flags (bitwise OR of GubernatorBehavior values)
    pub behavior: i32,
    /// How to retry when rate limited
    pub retry_policy: RateLimitRetryPolicy,
}

impl GubernatorRateLimit {
    /// Create a new rate limit with default settings
    pub fn new(
        name: impl Into<String>,
        unique_key: impl Into<String>,
        limit: i64,
        duration_ms: i64,
    ) -> Self {
        Self {
            name: name.into(),
            unique_key: unique_key.into(),
            limit,
            duration_ms,
            hits: 1,
            algorithm: GubernatorAlgorithm::default(),
            behavior: 0, // BATCHING (default)
            retry_policy: RateLimitRetryPolicy::default(),
        }
    }
}

/// A unified limit type that can be either a concurrency limit, rate limit, or floating concurrency limit
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum Limit {
    /// A concurrency-based limit (slot-based)
    Concurrency(ConcurrencyLimit),
    /// A rate-based limit (checked via Gubernator)
    RateLimit(GubernatorRateLimit),
    /// A floating concurrency limit with dynamic max concurrency refreshed by workers
    FloatingConcurrency(FloatingConcurrencyLimit),
}

/// Discriminant for job status kinds
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize, PartialEq, Eq, Copy)]
#[archive(check_bytes)]
pub enum JobStatusKind {
    Scheduled,
    Running,
    Failed,
    Cancelled,
    Succeeded,
}

impl JobStatusKind {
    /// Returns true if the job is in a final state where no more attempts will be created.
    /// This covers Succeeded and Failed, but NOT Cancelled (which can be restarted).
    pub fn is_final(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }

    /// Return the string name of this status kind for indexing
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "Scheduled",
            Self::Running => "Running",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::Succeeded => "Succeeded",
        }
    }
}

/// Job status with the last change time for secondary indexing
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobStatus {
    pub kind: JobStatusKind,
    pub changed_at_ms: i64,
    /// Unix timestamp (ms) when the next attempt will start.
    /// Present for scheduled jobs (initial or retry), absent for running or terminal jobs.
    pub next_attempt_starts_after_ms: Option<i64>,
    /// The current attempt number for this job's pending task.
    /// Present when a task exists in the task queue (Scheduled status), absent when the job is running (task became a lease) or terminal.
    /// Used for O(1) task key reconstruction in expedite operations.
    pub current_attempt: Option<u32>,
}

impl JobStatus {
    pub fn new(
        kind: JobStatusKind,
        changed_at_ms: i64,
        next_attempt_starts_after_ms: Option<i64>,
        current_attempt: Option<u32>,
    ) -> Self {
        Self {
            kind,
            changed_at_ms,
            next_attempt_starts_after_ms,
            current_attempt,
        }
    }

    /// Create a Scheduled status with the next attempt start time and attempt number.
    pub fn scheduled(
        changed_at_ms: i64,
        next_attempt_starts_after_ms: i64,
        attempt_number: u32,
    ) -> Self {
        Self::new(
            JobStatusKind::Scheduled,
            changed_at_ms,
            Some(next_attempt_starts_after_ms),
            Some(attempt_number),
        )
    }

    pub fn running(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Running, changed_at_ms, None, None)
    }

    pub fn failed(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Failed, changed_at_ms, None, None)
    }

    pub fn cancelled(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Cancelled, changed_at_ms, None, None)
    }

    pub fn succeeded(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Succeeded, changed_at_ms, None, None)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.kind,
            JobStatusKind::Succeeded | JobStatusKind::Failed | JobStatusKind::Cancelled
        )
    }
}

/// Zero-copy view over an archived `JobInfo` backed by owned aligned data.
#[derive(Clone)]
pub struct JobView {
    decoded: DecodedJobInfo,
}

impl std::fmt::Debug for JobView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobView")
            .field("id", &self.id())
            .field("priority", &self.priority())
            .field("enqueue_time_ms", &self.enqueue_time_ms())
            .field("task_group", &self.task_group())
            .finish()
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobInfo {
    pub id: String,
    pub priority: u8,         // 0..=99, 0 is highest priority and will run first
    pub enqueue_time_ms: i64, // epoch millis
    pub payload: Vec<u8>,     // MessagePack bytes (opaque to rkyv)
    pub retry_policy: Option<RetryPolicy>,
    pub metadata: Vec<(String, String)>,
    pub limits: Vec<Limit>, // Ordered list of limits to check before execution
    pub task_group: String, // Task group for organizing tasks. Immutable after enqueue.
}

impl JobView {
    /// Validate bytes and construct a zero-copy view.
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self, JobStoreShardError> {
        // Validate and decode up front; reject invalid data early.
        let decoded = decode_job_info(bytes.as_ref()).map_err(codec_error_to_shard_error)?;
        Ok(Self { decoded })
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
    pub fn task_group(&self) -> &str {
        self.archived().task_group.as_str()
    }

    /// Decode the payload from MessagePack bytes into a serde_json::Value for display.
    pub fn payload_as_json(&self) -> Result<serde_json::Value, rmp_serde::decode::Error> {
        rmp_serde::from_slice(self.payload_bytes())
    }

    /// Decode the payload from MessagePack bytes into a typed value.
    pub fn payload_msgpack<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<T, rmp_serde::decode::Error> {
        rmp_serde::from_slice(self.payload_bytes())
    }

    /// Accessor to the archived root (validated at construction).
    fn archived(&self) -> &<JobInfo as Archive>::Archived {
        self.decoded.archived()
    }

    /// Return the job's retry policy as a runtime struct, if present, by copying primitive fields from the archived view.
    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        let a = self.archived();
        let pol = a.retry_policy.as_ref()?;
        Some(RetryPolicy {
            retry_count: pol.retry_count,
            initial_interval_ms: pol.initial_interval_ms,
            max_interval_ms: pol.max_interval_ms,
            randomize_interval: pol.randomize_interval,
            backoff_factor: pol.backoff_factor,
        })
    }

    /// Return declared concurrency limits extracted from the limits field
    pub fn concurrency_limits(&self) -> Vec<ConcurrencyLimit> {
        self.limits()
            .into_iter()
            .filter_map(|l| match l {
                Limit::Concurrency(c) => Some(c),
                _ => None,
            })
            .collect()
    }

    /// Return metadata as owned key/value string pairs
    pub fn metadata(&self) -> Vec<(String, String)> {
        let a = self.archived();
        let mut out: Vec<(String, String)> = Vec::with_capacity(a.metadata.len());
        for pair in a.metadata.iter() {
            let (k, v) = pair;
            out.push((k.as_str().to_string(), v.as_str().to_string()));
        }
        out
    }

    /// Return the ordered list of limits (concurrency, rate limits, and floating concurrency limits)
    pub fn limits(&self) -> Vec<Limit> {
        let a = self.archived();
        let mut out = Vec::with_capacity(a.limits.len());
        for lim in a.limits.iter() {
            match lim {
                ArchivedLimit::Concurrency(c) => {
                    out.push(Limit::Concurrency(ConcurrencyLimit {
                        key: c.key.as_str().to_string(),
                        max_concurrency: c.max_concurrency,
                    }));
                }
                ArchivedLimit::RateLimit(r) => {
                    let algorithm = match &r.algorithm {
                        ArchivedGubernatorAlgorithm::TokenBucket => {
                            GubernatorAlgorithm::TokenBucket
                        }
                        ArchivedGubernatorAlgorithm::LeakyBucket => {
                            GubernatorAlgorithm::LeakyBucket
                        }
                    };
                    out.push(Limit::RateLimit(GubernatorRateLimit {
                        name: r.name.as_str().to_string(),
                        unique_key: r.unique_key.as_str().to_string(),
                        limit: r.limit,
                        duration_ms: r.duration_ms,
                        hits: r.hits,
                        algorithm,
                        behavior: r.behavior,
                        retry_policy: RateLimitRetryPolicy {
                            initial_backoff_ms: r.retry_policy.initial_backoff_ms,
                            max_backoff_ms: r.retry_policy.max_backoff_ms,
                            backoff_multiplier: r.retry_policy.backoff_multiplier,
                            max_retries: r.retry_policy.max_retries,
                        },
                    }));
                }
                ArchivedLimit::FloatingConcurrency(f) => {
                    let metadata: Vec<(String, String)> = f
                        .metadata
                        .iter()
                        .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                        .collect();
                    out.push(Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                        key: f.key.as_str().to_string(),
                        default_max_concurrency: f.default_max_concurrency,
                        refresh_interval_ms: f.refresh_interval_ms,
                        metadata,
                    }));
                }
            }
        }
        out
    }
}
