use rkyv::{AlignedVec, Archive};

use crate::job::{JobCancellation, JobInfo, JobStatus};
use crate::job_attempt::JobAttempt;
use crate::task::{ConcurrencyAction, GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

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
/// Version for JobCancellation serialization format
pub const JOB_CANCELLATION_VERSION: u8 = 1;

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
            tenant,
            job_id,
            attempt_number,
            held_queues,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            tenant: tenant.as_str().to_string(),
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
            tenant,
            job_id,
            attempt_number,
            request_id,
        } => Task::RequestTicket {
            queue: queue.as_str().to_string(),
            start_time_ms: *start_time_ms,
            priority: *priority,
            tenant: tenant.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            request_id: request_id.as_str().to_string(),
        },
        ArchivedTask::CheckRateLimit {
            task_id,
            tenant,
            job_id,
            attempt_number,
            limit_index,
            rate_limit,
            retry_count,
            started_at_ms,
            priority,
            held_queues,
        } => Task::CheckRateLimit {
            task_id: task_id.as_str().to_string(),
            tenant: tenant.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            limit_index: *limit_index,
            rate_limit: GubernatorRateLimitData {
                name: rate_limit.name.as_str().to_string(),
                unique_key: rate_limit.unique_key.as_str().to_string(),
                limit: rate_limit.limit,
                duration_ms: rate_limit.duration_ms,
                hits: rate_limit.hits,
                algorithm: rate_limit.algorithm,
                behavior: rate_limit.behavior,
                retry_initial_backoff_ms: rate_limit.retry_initial_backoff_ms,
                retry_max_backoff_ms: rate_limit.retry_max_backoff_ms,
                retry_backoff_multiplier: rate_limit.retry_backoff_multiplier,
                retry_max_retries: rate_limit.retry_max_retries,
            },
            retry_count: *retry_count,
            started_at_ms: *started_at_ms,
            priority: *priority,
            held_queues: held_queues
                .iter()
                .map(|s| s.as_str().to_string())
                .collect::<Vec<String>>(),
        },
        ArchivedTask::RefreshFloatingLimit {
            task_id,
            tenant,
            queue_key,
            current_max_concurrency,
            last_refreshed_at_ms,
            metadata,
        } => Task::RefreshFloatingLimit {
            task_id: task_id.as_str().to_string(),
            tenant: tenant.as_str().to_string(),
            queue_key: queue_key.as_str().to_string(),
            current_max_concurrency: *current_max_concurrency,
            last_refreshed_at_ms: *last_refreshed_at_ms,
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect(),
        },
    })
}

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

type ArchivedTask = <Task as Archive>::Archived;

impl DecodedLease {
    pub fn archived(&self) -> &<LeaseRecord as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_lease
        unsafe { rkyv::archived_root::<LeaseRecord>(&self.data) }
    }

    /// Get the worker_id from this lease (zero-copy)
    pub fn worker_id(&self) -> &str {
        self.archived().worker_id.as_str()
    }

    /// Get the expiry time from this lease
    pub fn expiry_ms(&self) -> i64 {
        self.archived().expiry_ms
    }

    /// Get a reference to the archived task (zero-copy)
    pub fn archived_task(&self) -> &ArchivedTask {
        &self.archived().task
    }

    /// Extract task_id from the leased task (zero-copy). Only valid for RunAttempt tasks.
    pub fn task_id(&self) -> Option<&str> {
        match self.archived_task() {
            ArchivedTask::RunAttempt { id, .. } => Some(id.as_str()),
            _ => None,
        }
    }

    /// Extract tenant from the leased task (zero-copy).
    pub fn tenant(&self) -> &str {
        match self.archived_task() {
            ArchivedTask::RunAttempt { tenant, .. } => tenant.as_str(),
            ArchivedTask::RequestTicket { tenant, .. } => tenant.as_str(),
            ArchivedTask::CheckRateLimit { tenant, .. } => tenant.as_str(),
            ArchivedTask::RefreshFloatingLimit { tenant, .. } => tenant.as_str(),
        }
    }

    /// Extract job_id from the leased task (zero-copy). Returns empty string for RefreshFloatingLimit.
    pub fn job_id(&self) -> &str {
        match self.archived_task() {
            ArchivedTask::RunAttempt { job_id, .. } => job_id.as_str(),
            ArchivedTask::RequestTicket { job_id, .. } => job_id.as_str(),
            ArchivedTask::CheckRateLimit { job_id, .. } => job_id.as_str(),
            ArchivedTask::RefreshFloatingLimit { .. } => "",
        }
    }

    /// Extract attempt_number from the leased task (zero-copy). Returns 0 for RefreshFloatingLimit.
    pub fn attempt_number(&self) -> u32 {
        match self.archived_task() {
            ArchivedTask::RunAttempt { attempt_number, .. } => *attempt_number,
            ArchivedTask::RequestTicket { attempt_number, .. } => *attempt_number,
            ArchivedTask::CheckRateLimit { attempt_number, .. } => *attempt_number,
            ArchivedTask::RefreshFloatingLimit { .. } => 0,
        }
    }

    /// Extract held_queues from the leased task as owned strings (allocation required)
    pub fn held_queues(&self) -> Vec<String> {
        match self.archived_task() {
            ArchivedTask::RunAttempt { held_queues, .. } => {
                held_queues.iter().map(|s| s.as_str().to_string()).collect()
            }
            ArchivedTask::CheckRateLimit { held_queues, .. } => {
                held_queues.iter().map(|s| s.as_str().to_string()).collect()
            }
            ArchivedTask::RequestTicket { .. } | ArchivedTask::RefreshFloatingLimit { .. } => {
                Vec::new()
            }
        }
    }

    /// Convert to an owned Task - only call when you need to create a new record
    pub fn to_task(&self) -> Task {
        match self.archived_task() {
            ArchivedTask::RunAttempt {
                id,
                tenant,
                job_id,
                attempt_number,
                held_queues,
            } => Task::RunAttempt {
                id: id.as_str().to_string(),
                tenant: tenant.as_str().to_string(),
                job_id: job_id.as_str().to_string(),
                attempt_number: *attempt_number,
                held_queues: held_queues.iter().map(|s| s.as_str().to_string()).collect(),
            },
            ArchivedTask::RequestTicket {
                queue,
                start_time_ms,
                priority,
                tenant,
                job_id,
                attempt_number,
                request_id,
            } => Task::RequestTicket {
                queue: queue.as_str().to_string(),
                start_time_ms: *start_time_ms,
                priority: *priority,
                tenant: tenant.as_str().to_string(),
                job_id: job_id.as_str().to_string(),
                attempt_number: *attempt_number,
                request_id: request_id.as_str().to_string(),
            },
            ArchivedTask::CheckRateLimit {
                task_id,
                tenant,
                job_id,
                attempt_number,
                limit_index,
                rate_limit,
                retry_count,
                started_at_ms,
                priority,
                held_queues,
            } => Task::CheckRateLimit {
                task_id: task_id.as_str().to_string(),
                tenant: tenant.as_str().to_string(),
                job_id: job_id.as_str().to_string(),
                attempt_number: *attempt_number,
                limit_index: *limit_index,
                rate_limit: GubernatorRateLimitData {
                    name: rate_limit.name.as_str().to_string(),
                    unique_key: rate_limit.unique_key.as_str().to_string(),
                    limit: rate_limit.limit,
                    duration_ms: rate_limit.duration_ms,
                    hits: rate_limit.hits,
                    algorithm: rate_limit.algorithm,
                    behavior: rate_limit.behavior,
                    retry_initial_backoff_ms: rate_limit.retry_initial_backoff_ms,
                    retry_max_backoff_ms: rate_limit.retry_max_backoff_ms,
                    retry_backoff_multiplier: rate_limit.retry_backoff_multiplier,
                    retry_max_retries: rate_limit.retry_max_retries,
                },
                retry_count: *retry_count,
                started_at_ms: *started_at_ms,
                priority: *priority,
                held_queues: held_queues.iter().map(|s| s.as_str().to_string()).collect(),
            },
            ArchivedTask::RefreshFloatingLimit {
                task_id,
                tenant,
                queue_key,
                current_max_concurrency,
                last_refreshed_at_ms,
                metadata,
            } => Task::RefreshFloatingLimit {
                task_id: task_id.as_str().to_string(),
                tenant: tenant.as_str().to_string(),
                queue_key: queue_key.as_str().to_string(),
                current_max_concurrency: *current_max_concurrency,
                last_refreshed_at_ms: *last_refreshed_at_ms,
                metadata: metadata
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                    .collect(),
            },
        }
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

    /// Get the granted_at time from this holder record
    pub fn granted_at_ms(&self) -> i64 {
        self.archived().granted_at_ms
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

#[inline]
pub fn encode_job_cancellation(cancellation: &JobCancellation) -> Result<Vec<u8>, CodecError> {
    let data = rkyv::to_bytes::<JobCancellation, 64>(cancellation)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(JOB_CANCELLATION_VERSION, data))
}

/// Decoded job cancellation that owns its aligned data
#[derive(Clone)]
pub struct DecodedJobCancellation {
    data: AlignedVec,
}

impl DecodedJobCancellation {
    pub fn archived(&self) -> &<JobCancellation as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_job_cancellation
        unsafe { rkyv::archived_root::<JobCancellation>(&self.data) }
    }
}

#[inline]
pub fn decode_job_cancellation(bytes: &[u8]) -> Result<DecodedJobCancellation, CodecError> {
    let data = strip_version(JOB_CANCELLATION_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<JobCancellation>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedJobCancellation { data })
}

use crate::job::FloatingLimitState;

/// Version for FloatingLimitState serialization format
pub const FLOATING_LIMIT_STATE_VERSION: u8 = 1;

#[inline]
pub fn encode_floating_limit_state(state: &FloatingLimitState) -> Result<Vec<u8>, CodecError> {
    let data = rkyv::to_bytes::<FloatingLimitState, 256>(state)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(prepend_version(FLOATING_LIMIT_STATE_VERSION, data))
}

/// Decoded floating limit state that owns its aligned data
#[derive(Clone)]
pub struct DecodedFloatingLimitState {
    data: AlignedVec,
}

impl DecodedFloatingLimitState {
    pub fn archived(&self) -> &<FloatingLimitState as Archive>::Archived {
        // SAFETY: data was validated at construction in decode_floating_limit_state
        unsafe { rkyv::archived_root::<FloatingLimitState>(&self.data) }
    }
}

#[inline]
pub fn decode_floating_limit_state(bytes: &[u8]) -> Result<DecodedFloatingLimitState, CodecError> {
    let data = strip_version(FLOATING_LIMIT_STATE_VERSION, bytes)?;
    // Validate the data
    let _ = rkyv::check_archived_root::<FloatingLimitState>(&data)
        .map_err(|e| CodecError::Rkyv(e.to_string()))?;
    Ok(DecodedFloatingLimitState { data })
}
