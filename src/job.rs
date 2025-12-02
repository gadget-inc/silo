use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::codec::{decode_job_info, CodecError, DecodedJobInfo};
use crate::job_store_shard::JobStoreShardError;
use crate::retry::RetryPolicy;

/// Cancellation record stored at job_cancelled/<tenant>/<job-id>.
/// Cancellation is tracked separately from status per the Alloy spec:
/// this allows dequeue to blindly write Running without losing cancellation info.
/// Once cancelled, always cancelled (monotonic).
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

/// A unified limit type that can be either a concurrency limit or a rate limit
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum Limit {
    /// A concurrency-based limit (slot-based)
    Concurrency(ConcurrencyLimit),
    /// A rate-based limit (checked via Gubernator)
    RateLimit(GubernatorRateLimit),
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
}

impl JobStatus {
    pub fn new(kind: JobStatusKind, changed_at_ms: i64) -> Self {
        Self {
            kind,
            changed_at_ms,
        }
    }

    pub fn scheduled(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Scheduled, changed_at_ms)
    }

    pub fn running(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Running, changed_at_ms)
    }

    pub fn failed(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Failed, changed_at_ms)
    }

    pub fn cancelled(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Cancelled, changed_at_ms)
    }

    pub fn succeeded(changed_at_ms: i64) -> Self {
        Self::new(JobStatusKind::Succeeded, changed_at_ms)
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
            .finish()
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobInfo {
    pub id: String,
    pub priority: u8,         // 0..=99, 0 is highest priority and will run first
    pub enqueue_time_ms: i64, // epoch millis
    pub payload: Vec<u8>,     // JSON bytes for now (opaque to rkyv)
    pub retry_policy: Option<RetryPolicy>,
    pub metadata: Vec<(String, String)>,
    pub limits: Vec<Limit>, // Ordered list of limits to check before execution
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
    pub fn payload_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::from_slice(self.payload_bytes())
    }

    /// Accessor to the archived root (validated at construction).
    fn archived(&self) -> &<JobInfo as Archive>::Archived {
        self.decoded.archived()
    }

    /// Return the job's retry policy as a runtime struct, if present, by copying
    /// primitive fields from the archived view.
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

    /// Return the ordered list of limits (both concurrency and rate limits)
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
            }
        }
        out
    }
}
