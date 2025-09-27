use bytes::Bytes;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;

use crate::job_store_shard::JobStoreShardError;

/// Outcome passed by callers when reporting an attempt's completion.
#[derive(Debug, Clone)]
pub enum AttemptOutcome {
    Success { result: Vec<u8> },
    Error { error_code: String, error: Vec<u8> },
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

/// Zero-copy view alias over an archived `JobAttempt` backed by owned bytes.
#[derive(Clone, Debug)]
pub struct JobAttemptView {
    pub(crate) bytes: Bytes,
}

impl JobAttemptView {
    pub fn new(bytes: Bytes) -> Result<Self, JobStoreShardError> {
        #[cfg(debug_assertions)]
        {
            let _ = rkyv::check_archived_root::<JobAttempt>(&bytes)
                .map_err(|e| JobStoreShardError::Rkyv(e.to_string()))?;
        }
        Ok(Self { bytes })
    }

    pub(crate) fn archived(&self) -> &<JobAttempt as Archive>::Archived {
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
        }
    }
}
