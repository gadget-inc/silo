use crate::codec::{CodecError, DecodedAttempt, decode_attempt};
use crate::fb::silo::fb;
use crate::job_store_shard::JobStoreShardError;

fn codec_error_to_shard_error(e: CodecError) -> JobStoreShardError {
    JobStoreShardError::Codec(e.to_string())
}

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

/// Zero-copy view alias over a FlatBuffer `JobAttempt` backed by owned data.
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
    pub fn new(bytes: impl Into<bytes::Bytes>) -> Result<Self, JobStoreShardError> {
        // Validate and decode up front; reject invalid data early.
        let decoded = decode_attempt(bytes.into()).map_err(codec_error_to_shard_error)?;
        Ok(Self { decoded })
    }

    fn fb(&self) -> fb::JobAttempt<'_> {
        self.decoded.fb()
    }

    pub fn job_id(&self) -> &str {
        self.fb().job_id().unwrap_or_default()
    }
    pub fn attempt_number(&self) -> u32 {
        self.fb().attempt_number()
    }
    pub fn relative_attempt_number(&self) -> u32 {
        self.fb().relative_attempt_number()
    }
    pub fn task_id(&self) -> &str {
        self.fb().task_id().unwrap_or_default()
    }
    pub fn started_at_ms(&self) -> i64 {
        self.fb().started_at_ms()
    }

    pub fn state(&self) -> AttemptStatus {
        let a = self.fb();
        match a.status_kind() {
            fb::AttemptStatusKind::Running => AttemptStatus::Running,
            fb::AttemptStatusKind::Succeeded => AttemptStatus::Succeeded {
                finished_at_ms: a.finished_at_ms().unwrap_or(0),
                result: a.result().map(|v| v.bytes().to_vec()).unwrap_or_default(),
            },
            fb::AttemptStatusKind::Failed => AttemptStatus::Failed {
                finished_at_ms: a.finished_at_ms().unwrap_or(0),
                error_code: a.error_code().unwrap_or_default().to_string(),
                error: a.error().map(|v| v.bytes().to_vec()).unwrap_or_default(),
            },
            fb::AttemptStatusKind::Cancelled => AttemptStatus::Cancelled {
                finished_at_ms: a.finished_at_ms().unwrap_or(0),
            },
            _ => AttemptStatus::Running, // fallback for unknown variants
        }
    }
}
