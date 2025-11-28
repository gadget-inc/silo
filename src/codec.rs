use rkyv::{AlignedVec, Archive};

use crate::job::{JobInfo, JobStatus};
use crate::job_attempt::JobAttempt;
use crate::job_store_shard::{ConcurrencyAction, HolderRecord, LeaseRecord, Task};

/// Error type for versioned codec operations
#[derive(Debug, Clone)]
pub enum CodecError {
    /// Data is too short to contain a version header
    TooShort,
    /// Version byte doesn't match expected version
    UnsupportedVersion { expected: u8, found: u8 },
    /// Underlying rkyv serialization/deserialization error
    Rkyv(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::TooShort => write!(f, "data too short to contain version header"),
            CodecError::UnsupportedVersion { expected, found } => {
                write!(
                    f,
                    "unsupported version: expected {}, found {}",
                    expected, found
                )
            }
            CodecError::Rkyv(e) => write!(f, "rkyv error: {}", e),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<CodecError> for String {
    fn from(e: CodecError) -> String {
        e.to_string()
    }
}

// Version constants for each serializable type.
// When evolving schemas, bump these and add migration logic in the decode functions.

/// Version for Task serialization format
pub const TASK_VERSION: u8 = 1;
/// Version for LeaseRecord serialization format
pub const LEASE_RECORD_VERSION: u8 = 1;
/// Version for JobAttempt serialization format
pub const JOB_ATTEMPT_VERSION: u8 = 1;
/// Version for JobInfo serialization format
pub const JOB_INFO_VERSION: u8 = 1;
/// Version for JobStatus serialization format
pub const JOB_STATUS_VERSION: u8 = 1;
/// Version for HolderRecord serialization format
pub const HOLDER_RECORD_VERSION: u8 = 1;
/// Version for ConcurrencyAction serialization format
pub const CONCURRENCY_ACTION_VERSION: u8 = 1;

/// Size of the version header - just a single byte.
/// Alignment is handled at decode time by copying into an AlignedVec.
const VERSION_HEADER_SIZE: usize = 1;

/// Prepend a single version byte to the rkyv-serialized data.
#[inline]
fn prepend_version(version: u8, data: AlignedVec) -> Vec<u8> {
    let mut result = Vec::with_capacity(VERSION_HEADER_SIZE + data.len());
    result.push(version);
    result.extend_from_slice(&data);
    result
}

/// Strip the version byte and return the remaining data, validating the version matches.
/// Copies into an AlignedVec to ensure proper alignment for rkyv deserialization.
#[inline]
fn strip_version(expected: u8, data: &[u8]) -> Result<AlignedVec, CodecError> {
    if data.len() < VERSION_HEADER_SIZE {
        return Err(CodecError::TooShort);
    }
    let found = data[0];
    if found != expected {
        return Err(CodecError::UnsupportedVersion { expected, found });
    }
    // Copy into an AlignedVec to ensure proper alignment for rkyv
    let rkyv_data = &data[VERSION_HEADER_SIZE..];
    let mut aligned = AlignedVec::with_capacity(rkyv_data.len());
    aligned.extend_from_slice(rkyv_data);
    Ok(aligned)
}

// ============================================================================
// Task encoding/decoding
// ============================================================================

#[inline]
pub fn encode_task(task: &Task) -> Result<Vec<u8>, CodecError> {
    let data = rkyv::to_bytes::<Task, 256>(task).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(TASK_VERSION, data))
}

#[inline]
pub fn decode_task(bytes: &[u8]) -> Result<Task, CodecError> {
    let data = strip_version(TASK_VERSION, bytes)?;
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedTask =
        rkyv::check_archived_root::<Task>(&data).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(match archived {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            held_queues,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: held_queues
                .iter()
                .map(|s| s.as_str().to_string())
                .collect::<Vec<String>>(),
        },
        ArchivedTask::RequestTicket {
            queue,
            start_time_ms,
            priority,
            job_id,
            attempt_number,
            request_id,
        } => Task::RequestTicket {
            queue: queue.as_str().to_string(),
            start_time_ms: *start_time_ms,
            priority: *priority,
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            request_id: request_id.as_str().to_string(),
        },
    })
}

// ============================================================================
// LeaseRecord encoding/decoding
// ============================================================================

#[inline]
pub fn encode_lease(record: &LeaseRecord) -> Result<Vec<u8>, CodecError> {
    let data =
        rkyv::to_bytes::<LeaseRecord, 256>(record).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(LEASE_RECORD_VERSION, data))
}

/// Decoded lease record that owns its aligned data
#[derive(Clone)]
pub struct DecodedLease {
    data: AlignedVec,
}

impl DecodedLease {
    pub fn archived(&self) -> &<LeaseRecord as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_lease
        unsafe { rkyv::archived_root::<LeaseRecord>(&self.data) }
    }
}

#[inline]
pub fn decode_lease(bytes: &[u8]) -> Result<DecodedLease, CodecError> {
    let data = strip_version(LEASE_RECORD_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<LeaseRecord>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedLease { data })
}

// ============================================================================
// JobAttempt encoding/decoding
// ============================================================================

#[inline]
pub fn encode_attempt(attempt: &JobAttempt) -> Result<Vec<u8>, CodecError> {
    let data =
        rkyv::to_bytes::<JobAttempt, 256>(attempt).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(JOB_ATTEMPT_VERSION, data))
}

/// Decoded job attempt that owns its aligned data
#[derive(Clone)]
pub struct DecodedAttempt {
    data: AlignedVec,
}

impl DecodedAttempt {
    pub fn archived(&self) -> &<JobAttempt as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_attempt
        unsafe { rkyv::archived_root::<JobAttempt>(&self.data) }
    }
}

#[inline]
pub fn decode_attempt(bytes: &[u8]) -> Result<DecodedAttempt, CodecError> {
    let data = strip_version(JOB_ATTEMPT_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<JobAttempt>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedAttempt { data })
}

// ============================================================================
// JobInfo encoding/decoding
// ============================================================================

#[inline]
pub fn encode_job_info(job: &JobInfo) -> Result<Vec<u8>, CodecError> {
    let data = rkyv::to_bytes::<JobInfo, 256>(job).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(JOB_INFO_VERSION, data))
}

/// Decoded job info that owns its aligned data
#[derive(Clone)]
pub struct DecodedJobInfo {
    data: AlignedVec,
}

impl DecodedJobInfo {
    pub fn archived(&self) -> &<JobInfo as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_job_info
        unsafe { rkyv::archived_root::<JobInfo>(&self.data) }
    }
}

#[inline]
pub fn decode_job_info(bytes: &[u8]) -> Result<DecodedJobInfo, CodecError> {
    let data = strip_version(JOB_INFO_VERSION, bytes)?;
    // Validate the data
    let _ =
        rkyv::check_archived_root::<JobInfo>(&data).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedJobInfo { data })
}

// ============================================================================
// JobStatus encoding/decoding
// ============================================================================

#[inline]
pub fn encode_job_status(status: &JobStatus) -> Result<Vec<u8>, CodecError> {
    let data =
        rkyv::to_bytes::<JobStatus, 256>(status).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(JOB_STATUS_VERSION, data))
}

/// Decoded job status that owns its aligned data
#[derive(Clone)]
pub struct DecodedJobStatus {
    data: AlignedVec,
}

impl DecodedJobStatus {
    pub fn archived(&self) -> &<JobStatus as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_job_status
        unsafe { rkyv::archived_root::<JobStatus>(&self.data) }
    }
}

#[inline]
pub fn decode_job_status(bytes: &[u8]) -> Result<DecodedJobStatus, CodecError> {
    let data = strip_version(JOB_STATUS_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<JobStatus>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedJobStatus { data })
}

// ============================================================================
// HolderRecord encoding/decoding
// ============================================================================

#[inline]
pub fn encode_holder(holder: &HolderRecord) -> Result<Vec<u8>, CodecError> {
    let data =
        rkyv::to_bytes::<HolderRecord, 256>(holder).map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(HOLDER_RECORD_VERSION, data))
}

/// Decoded holder record that owns its aligned data
#[derive(Clone)]
pub struct DecodedHolder {
    data: AlignedVec,
}

impl DecodedHolder {
    pub fn archived(&self) -> &<HolderRecord as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_holder
        unsafe { rkyv::archived_root::<HolderRecord>(&self.data) }
    }
}

#[inline]
pub fn decode_holder(bytes: &[u8]) -> Result<DecodedHolder, CodecError> {
    let data = strip_version(HOLDER_RECORD_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<HolderRecord>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedHolder { data })
}

// ============================================================================
// ConcurrencyAction encoding/decoding
// ============================================================================

#[inline]
pub fn encode_concurrency_action(action: &ConcurrencyAction) -> Result<Vec<u8>, CodecError> {
    let data = rkyv::to_bytes::<ConcurrencyAction, 256>(action)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(CONCURRENCY_ACTION_VERSION, data))
}

/// Decoded concurrency action that owns its aligned data
#[derive(Clone)]
pub struct DecodedConcurrencyAction {
    data: AlignedVec,
}

impl DecodedConcurrencyAction {
    pub fn archived(&self) -> &<ConcurrencyAction as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_concurrency_action
        unsafe { rkyv::archived_root::<ConcurrencyAction>(&self.data) }
    }
}

#[inline]
pub fn decode_concurrency_action(bytes: &[u8]) -> Result<DecodedConcurrencyAction, CodecError> {
    let data = strip_version(CONCURRENCY_ACTION_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<ConcurrencyAction>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedConcurrencyAction { data })
}
