use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::codec::{decode_job_info, CodecError, DecodedJobInfo};
use crate::job_store_shard::JobStoreShardError;
use crate::retry::RetryPolicy;

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

/// Discriminant for job status kinds, independent of timestamps
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize, PartialEq, Eq, Copy)]
#[archive(check_bytes)]
pub enum JobStatusKind {
    Scheduled,
    Running,
    Failed,
    Cancelled,
    Succeeded,
}

/// Job status lifecycle with the last change time for secondary indexing
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum JobStatus {
    Scheduled { changed_at_ms: i64 },
    Running { changed_at_ms: i64 },
    Failed { changed_at_ms: i64 },
    Cancelled { changed_at_ms: i64 },
    Succeeded { changed_at_ms: i64 },
}

impl JobStatus {
    pub fn changed_at_ms(&self) -> i64 {
        match self {
            JobStatus::Scheduled { changed_at_ms }
            | JobStatus::Running { changed_at_ms }
            | JobStatus::Failed { changed_at_ms }
            | JobStatus::Cancelled { changed_at_ms }
            | JobStatus::Succeeded { changed_at_ms } => *changed_at_ms,
        }
    }

    pub fn kind(&self) -> JobStatusKind {
        match self {
            JobStatus::Scheduled { .. } => JobStatusKind::Scheduled,
            JobStatus::Running { .. } => JobStatusKind::Running,
            JobStatus::Failed { .. } => JobStatusKind::Failed,
            JobStatus::Cancelled { .. } => JobStatusKind::Cancelled,
            JobStatus::Succeeded { .. } => JobStatusKind::Succeeded,
        }
    }

    pub fn from_kind(kind: JobStatusKind, changed_at_ms: i64) -> Self {
        match kind {
            JobStatusKind::Scheduled => JobStatus::Scheduled { changed_at_ms },
            JobStatusKind::Running => JobStatus::Running { changed_at_ms },
            JobStatusKind::Failed => JobStatus::Failed { changed_at_ms },
            JobStatusKind::Cancelled => JobStatus::Cancelled { changed_at_ms },
            JobStatusKind::Succeeded => JobStatus::Succeeded { changed_at_ms },
        }
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
    pub concurrency_limits: Vec<ConcurrencyLimit>,
    pub metadata: Vec<(String, String)>,
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

    /// Return declared concurrency limits as owned runtime structs by copying fields
    pub fn concurrency_limits(&self) -> Vec<ConcurrencyLimit> {
        let a = self.archived();
        let mut out = Vec::with_capacity(a.concurrency_limits.len());
        for lim in a.concurrency_limits.iter() {
            out.push(ConcurrencyLimit {
                key: lim.key.as_str().to_string(),
                max_concurrency: lim.max_concurrency,
            });
        }
        out
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
}
