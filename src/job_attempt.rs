use bytes::Bytes;

use crate::codec::{DecodedAttempt, decode_attempt_bytes};
use crate::job_store_shard::JobStoreShardError;

/// Outcome passed by callers when reporting an attempt's completion.
#[derive(Debug, Clone)]
pub enum AttemptOutcome {
    Success {
        result: Vec<u8>,
    },
    Error {
        error_code: String,
        error: Vec<u8>,
    },
    /// Worker acknowledges cancellation after discovering it via heartbeat.
    /// This is the clean shutdown path where worker reports it has stopped work.
    Cancelled,
}

/// Attempt status lifecycle
#[derive(Debug, Clone)]
pub enum AttemptStatus {
    Running,
    Succeeded {
        finished_at_ms: i64,
        result: Vec<u8>,
    },
    Failed {
        finished_at_ms: i64,
        error_code: String,
        error: Vec<u8>,
    },
    /// Attempt was cancelled (either worker acknowledged or lease expired while job was cancelled)
    Cancelled {
        finished_at_ms: i64,
    },
}

/// Stored representation of a job attempt
#[derive(Debug, Clone)]
pub struct JobAttempt {
    pub job_id: String,
    /// Total attempt number (monotonically increasing, 1-based)
    pub attempt_number: u32,
    /// Attempt number within current run (resets to 1 on job restart)
    pub relative_attempt_number: u32,
    pub task_id: String,
    /// When the attempt started (epoch ms). Set at dequeue time for running attempts,
    /// or preserved from the source system for imported attempts.
    pub started_at_ms: i64,
    pub status: AttemptStatus,
}

/// Zero-copy view over serialized `JobAttempt` data.
#[derive(Clone)]
pub struct JobAttemptView {
    decoded: DecodedAttempt,
}

impl std::fmt::Debug for JobAttemptView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobAttemptView")
            .field("job_id", &self.job_id())
            .field("attempt_number", &self.attempt_number())
            .field("task_id", &self.task_id())
            .finish()
    }
}

impl JobAttemptView {
    pub fn new(bytes: impl Into<Bytes>) -> Result<Self, JobStoreShardError> {
        // Validate and decode up front; reject invalid data early.
        let decoded = decode_attempt_bytes(bytes.into())?;
        Ok(Self { decoded })
    }

    pub fn job_id(&self) -> &str {
        self.decoded.job_id()
    }

    pub fn attempt_number(&self) -> u32 {
        self.decoded.attempt_number()
    }

    pub fn relative_attempt_number(&self) -> u32 {
        self.decoded.relative_attempt_number()
    }

    pub fn task_id(&self) -> &str {
        self.decoded.task_id()
    }

    pub fn started_at_ms(&self) -> i64 {
        self.decoded.started_at_ms()
    }

    pub fn state(&self) -> AttemptStatus {
        self.decoded.state()
    }
}
