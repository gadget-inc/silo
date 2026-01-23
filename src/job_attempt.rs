use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;

use crate::codec::{CodecError, DecodedAttempt, decode_attempt};
use crate::job_store_shard::JobStoreShardError;

fn codec_error_to_shard_error(e: CodecError) -> JobStoreShardError {
    JobStoreShardError::Rkyv(e.to_string())
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
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum AttemptStatus {
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
    /// Attempt was cancelled (either worker acknowledged or lease expired while job was cancelled)
    Cancelled {
        finished_at_ms: i64,
    },
}

/// Stored representation of a job attempt
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct JobAttempt {
    pub job_id: String,
    pub attempt_number: u32,
    pub task_id: String,
    pub status: AttemptStatus,
}

/// Zero-copy view alias over an archived `JobAttempt` backed by owned aligned data.
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
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self, JobStoreShardError> {
        // Validate and decode up front; reject invalid data early.
        let decoded = decode_attempt(bytes.as_ref()).map_err(codec_error_to_shard_error)?;
        Ok(Self { decoded })
    }

    pub(crate) fn archived(&self) -> &<JobAttempt as Archive>::Archived {
        self.decoded.archived()
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

    pub fn state(&self) -> AttemptStatus {
        pub(crate) type ArchivedAttemptState = <AttemptStatus as Archive>::Archived;
        match &self.archived().status {
            ArchivedAttemptState::Running { started_at_ms } => AttemptStatus::Running {
                started_at_ms: *started_at_ms,
            },
            ArchivedAttemptState::Succeeded {
                finished_at_ms,
                result,
            } => AttemptStatus::Succeeded {
                finished_at_ms: *finished_at_ms,
                result: result.to_vec(),
            },
            ArchivedAttemptState::Failed {
                finished_at_ms,
                error_code,
                error,
            } => AttemptStatus::Failed {
                finished_at_ms: *finished_at_ms,
                error_code: error_code.as_str().to_string(),
                error: error.to_vec(),
            },
            ArchivedAttemptState::Cancelled { finished_at_ms } => AttemptStatus::Cancelled {
                finished_at_ms: *finished_at_ms,
            },
        }
    }
}
