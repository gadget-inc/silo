use bytes::Bytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::job_store_shard::JobStoreShardError;
use crate::retry::RetryPolicy;

/// Zero-copy view over an archived `JobInfo` backed by owned bytes.
#[derive(Clone, Debug)]
pub struct JobView {
    info_bytes: Bytes,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobInfo {
    pub id: String,
    pub priority: u8,         // 0..=99, 0 is highest priority and will run first
    pub enqueue_time_ms: i64, // epoch millis
    pub payload: Vec<u8>,     // JSON bytes for now (opaque to rkyv)
    pub retry_policy: Option<RetryPolicy>,
}

impl JobView {
    /// Validate bytes and construct a zero-copy view.
    pub fn new(bytes: Bytes) -> Result<Self, JobStoreShardError> {
        // Validate once up front in debug builds; skip in release for performance.
        #[cfg(debug_assertions)]
        {
            let _ = rkyv::check_archived_root::<JobInfo>(&bytes)
                .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
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
        let pol = a.retry_policy.as_ref()?;
        Some(RetryPolicy {
            retry_count: pol.retry_count,
            initial_interval_ms: pol.initial_interval_ms,
            max_interval_ms: pol.max_interval_ms,
            randomize_interval: pol.randomize_interval,
            backoff_factor: pol.backoff_factor,
        })
    }
}
