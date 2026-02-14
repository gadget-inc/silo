//! All decode (deserialize) functions and zero-copy view types for silo's internal storage format.

use bytes::Bytes;

use crate::codec::{
    CodecError, fb_error, from_fb_gubernator_algorithm, from_fb_job_status_kind,
    parse_gubernator_rate_limit_data, parse_string_pairs, parse_string_values, required_str,
};
use crate::flatbuf::silo_internal as fb;
use crate::job::{
    ConcurrencyLimit, FloatingConcurrencyLimit, FloatingLimitState, GubernatorRateLimit,
    JobCancellation, JobStatus, Limit, RateLimitRetryPolicy,
};
use crate::job_attempt::AttemptStatus;
use crate::retry::RetryPolicy;
use crate::task::{HolderRecord, Task};

// ===========================================================================
// Task
// ===========================================================================

/// Decoded task — a validated, zero-copy view over a flatbuffer `Task` union.
///
/// Consumers match directly on variants to access the underlying fb type:
/// ```ignore
/// match task {
///     DecodedTask::RunAttempt(ra) => ra.tenant().unwrap_or_default(),
///     DecodedTask::RequestTicket(rt) => rt.tenant().unwrap_or_default(),
///     // ...
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub enum DecodedTask<'a> {
    RunAttempt(fb::TaskRunAttempt<'a>),
    RequestTicket(fb::TaskRequestTicket<'a>),
    CheckRateLimit(fb::TaskCheckRateLimit<'a>),
    RefreshFloatingLimit(fb::TaskRefreshFloatingLimit<'a>),
}

impl<'a> DecodedTask<'a> {
    fn from_fb(task: fb::Task<'a>) -> Result<Self, CodecError> {
        match task.data_type() {
            fb::TaskData::TaskRunAttempt => Ok(Self::RunAttempt(
                task.data_as_task_run_attempt()
                    .ok_or(CodecError::UnknownVariant("TaskData::TaskRunAttempt"))?,
            )),
            fb::TaskData::TaskRequestTicket => Ok(Self::RequestTicket(
                task.data_as_task_request_ticket()
                    .ok_or(CodecError::UnknownVariant("TaskData::TaskRequestTicket"))?,
            )),
            fb::TaskData::TaskCheckRateLimit => Ok(Self::CheckRateLimit(
                task.data_as_task_check_rate_limit()
                    .ok_or(CodecError::UnknownVariant("TaskData::TaskCheckRateLimit"))?,
            )),
            fb::TaskData::TaskRefreshFloatingLimit => Ok(Self::RefreshFloatingLimit(
                task.data_as_task_refresh_floating_limit()
                    .ok_or(CodecError::UnknownVariant(
                        "TaskData::TaskRefreshFloatingLimit",
                    ))?,
            )),
            _ => Err(CodecError::UnknownVariant("TaskData")),
        }
    }

    pub fn tenant(&self) -> &str {
        match self {
            Self::RunAttempt(d) => d.tenant().unwrap_or_default(),
            Self::RequestTicket(d) => d.tenant().unwrap_or_default(),
            Self::CheckRateLimit(d) => d.tenant().unwrap_or_default(),
            Self::RefreshFloatingLimit(d) => d.tenant().unwrap_or_default(),
        }
    }

    pub fn run_attempt(&self) -> Option<fb::TaskRunAttempt<'a>> {
        match self {
            Self::RunAttempt(d) => Some(*d),
            _ => None,
        }
    }

    pub fn request_ticket(&self) -> Option<fb::TaskRequestTicket<'a>> {
        match self {
            Self::RequestTicket(d) => Some(*d),
            _ => None,
        }
    }

    pub fn check_rate_limit(&self) -> Option<fb::TaskCheckRateLimit<'a>> {
        match self {
            Self::CheckRateLimit(d) => Some(*d),
            _ => None,
        }
    }

    pub fn refresh_floating_limit(&self) -> Option<fb::TaskRefreshFloatingLimit<'a>> {
        match self {
            Self::RefreshFloatingLimit(d) => Some(*d),
            _ => None,
        }
    }

    pub fn to_owned(&self) -> Result<Task, CodecError> {
        parse_task(*self)
    }
}

fn parse_task(task: DecodedTask<'_>) -> Result<Task, CodecError> {
    match task {
        DecodedTask::RunAttempt(data) => Ok(Task::RunAttempt {
            id: required_str(data.id(), "task.run_attempt.id")?.to_string(),
            tenant: required_str(data.tenant(), "task.run_attempt.tenant")?.to_string(),
            job_id: required_str(data.job_id(), "task.run_attempt.job_id")?.to_string(),
            attempt_number: data.attempt_number(),
            relative_attempt_number: data.relative_attempt_number(),
            held_queues: parse_string_values(data.held_queues()),
            task_group: required_str(data.task_group(), "task.run_attempt.task_group")?.to_string(),
        }),
        DecodedTask::RequestTicket(data) => Ok(Task::RequestTicket {
            queue: required_str(data.queue(), "task.request_ticket.queue")?.to_string(),
            start_time_ms: data.start_time_ms(),
            priority: data.priority(),
            tenant: required_str(data.tenant(), "task.request_ticket.tenant")?.to_string(),
            job_id: required_str(data.job_id(), "task.request_ticket.job_id")?.to_string(),
            attempt_number: data.attempt_number(),
            relative_attempt_number: data.relative_attempt_number(),
            request_id: required_str(data.request_id(), "task.request_ticket.request_id")?
                .to_string(),
            task_group: required_str(data.task_group(), "task.request_ticket.task_group")?
                .to_string(),
        }),
        DecodedTask::CheckRateLimit(data) => {
            let rate_limit = data
                .rate_limit()
                .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
            Ok(Task::CheckRateLimit {
                task_id: required_str(data.task_id(), "task.check_rate_limit.task_id")?.to_string(),
                tenant: required_str(data.tenant(), "task.check_rate_limit.tenant")?.to_string(),
                job_id: required_str(data.job_id(), "task.check_rate_limit.job_id")?.to_string(),
                attempt_number: data.attempt_number(),
                relative_attempt_number: data.relative_attempt_number(),
                limit_index: data.limit_index(),
                rate_limit: parse_gubernator_rate_limit_data(rate_limit)?,
                retry_count: data.retry_count(),
                started_at_ms: data.started_at_ms(),
                priority: data.priority(),
                held_queues: parse_string_values(data.held_queues()),
                task_group: required_str(data.task_group(), "task.check_rate_limit.task_group")?
                    .to_string(),
            })
        }
        DecodedTask::RefreshFloatingLimit(data) => Ok(Task::RefreshFloatingLimit {
            task_id: required_str(data.task_id(), "task.refresh_floating_limit.task_id")?
                .to_string(),
            tenant: required_str(data.tenant(), "task.refresh_floating_limit.tenant")?.to_string(),
            queue_key: required_str(data.queue_key(), "task.refresh_floating_limit.queue_key")?
                .to_string(),
            current_max_concurrency: data.current_max_concurrency(),
            last_refreshed_at_ms: data.last_refreshed_at_ms(),
            metadata: parse_string_pairs(data.metadata()),
            task_group: required_str(data.task_group(), "task.refresh_floating_limit.task_group")?
                .to_string(),
        }),
    }
}

/// Validate required string fields on a decoded task without full materialization.
fn validate_decoded_task_fields(task: DecodedTask<'_>) -> Result<(), CodecError> {
    match task {
        DecodedTask::RunAttempt(d) => {
            required_str(d.id(), "task.run_attempt.id")?;
            required_str(d.tenant(), "task.run_attempt.tenant")?;
            required_str(d.job_id(), "task.run_attempt.job_id")?;
            required_str(d.task_group(), "task.run_attempt.task_group")?;
        }
        DecodedTask::RequestTicket(d) => {
            required_str(d.tenant(), "task.request_ticket.tenant")?;
            required_str(d.job_id(), "task.request_ticket.job_id")?;
            required_str(d.request_id(), "task.request_ticket.request_id")?;
            required_str(d.task_group(), "task.request_ticket.task_group")?;
        }
        DecodedTask::CheckRateLimit(d) => {
            required_str(d.task_id(), "task.check_rate_limit.task_id")?;
            required_str(d.tenant(), "task.check_rate_limit.tenant")?;
            required_str(d.job_id(), "task.check_rate_limit.job_id")?;
            required_str(d.task_group(), "task.check_rate_limit.task_group")?;
            let rate_limit = d
                .rate_limit()
                .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
            required_str(rate_limit.name(), "task.check_rate_limit.rate_limit.name")?;
            required_str(
                rate_limit.unique_key(),
                "task.check_rate_limit.rate_limit.unique_key",
            )?;
        }
        DecodedTask::RefreshFloatingLimit(d) => {
            required_str(d.task_id(), "task.refresh_floating_limit.task_id")?;
            required_str(d.tenant(), "task.refresh_floating_limit.tenant")?;
            required_str(d.queue_key(), "task.refresh_floating_limit.queue_key")?;
            required_str(d.task_group(), "task.refresh_floating_limit.task_group")?;
        }
    }
    Ok(())
}

#[inline]
pub fn decode_task_view(bytes: &[u8]) -> Result<DecodedTask<'_>, CodecError> {
    let root = flatbuffers::root::<fb::Task>(bytes).map_err(|e| fb_error("Task", e))?;
    DecodedTask::from_fb(root)
}

#[inline]
pub fn decode_task(bytes: &[u8]) -> Result<Task, CodecError> {
    decode_task_view(bytes)?.to_owned()
}

#[inline]
pub fn validate_task(bytes: &[u8]) -> Result<(), CodecError> {
    let _ = decode_task_view(bytes)?;
    Ok(())
}

#[inline]
pub fn decode_task_tenant(bytes: &[u8]) -> Result<&str, CodecError> {
    let task = decode_task_view(bytes)?;
    // Validate tenant is present (not just unwrap_or_default)
    match task {
        DecodedTask::RunAttempt(d) => required_str(d.tenant(), "task.run_attempt.tenant"),
        DecodedTask::RequestTicket(d) => required_str(d.tenant(), "task.request_ticket.tenant"),
        DecodedTask::CheckRateLimit(d) => required_str(d.tenant(), "task.check_rate_limit.tenant"),
        DecodedTask::RefreshFloatingLimit(d) => {
            required_str(d.tenant(), "task.refresh_floating_limit.tenant")
        }
    }
}

#[inline]
pub fn task_is_refresh_floating_limit(bytes: &[u8]) -> Result<bool, CodecError> {
    Ok(matches!(
        decode_task_view(bytes)?,
        DecodedTask::RefreshFloatingLimit(_)
    ))
}

// ===========================================================================
// JobView (merged from DecodedJobInfo + job.rs JobView)
// ===========================================================================

/// Zero-copy view over serialized `JobInfo` data.
#[derive(Clone)]
pub struct JobView {
    data: Bytes,
    limits: Vec<Limit>,
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

impl JobView {
    /// Validate bytes and construct a zero-copy view.
    pub fn new(bytes: impl Into<Bytes>) -> Result<Self, CodecError> {
        decode_job_info_bytes(bytes.into())
    }

    fn job_info(&self) -> fb::JobInfo<'_> {
        // SAFETY: validated before construction.
        unsafe { flatbuffers::root_unchecked::<fb::JobInfo>(&self.data) }
    }

    pub fn id(&self) -> &str {
        self.job_info().id().unwrap_or_default()
    }

    pub fn priority(&self) -> u8 {
        self.job_info().priority()
    }

    pub fn enqueue_time_ms(&self) -> i64 {
        self.job_info().enqueue_time_ms()
    }

    pub fn payload_bytes(&self) -> &[u8] {
        match self.job_info().payload() {
            Some(payload) => payload.bytes(),
            None => &[],
        }
    }

    pub fn task_group(&self) -> &str {
        self.job_info().task_group().unwrap_or_default()
    }

    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        self.job_info().retry_policy().map(parse_retry_policy)
    }

    pub fn metadata(&self) -> Vec<(String, String)> {
        parse_string_pairs(self.job_info().metadata())
    }

    pub fn limits(&self) -> &[Limit] {
        &self.limits
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

    /// Return declared concurrency limits extracted from the limits field
    pub fn concurrency_limits(&self) -> Vec<crate::job::ConcurrencyLimit> {
        self.limits
            .iter()
            .filter_map(|l| match l {
                Limit::Concurrency(c) => Some(c.clone()),
                _ => None,
            })
            .collect()
    }
}

fn parse_retry_policy(retry_policy: fb::RetryPolicy<'_>) -> RetryPolicy {
    RetryPolicy {
        retry_count: retry_policy.retry_count(),
        initial_interval_ms: retry_policy.initial_interval_ms(),
        max_interval_ms: retry_policy.max_interval_ms(),
        randomize_interval: retry_policy.randomize_interval(),
        backoff_factor: retry_policy.backoff_factor(),
    }
}

fn parse_limit(limit: fb::Limit<'_>) -> Result<Limit, CodecError> {
    match limit.data_type() {
        fb::LimitData::ConcurrencyLimit => {
            let data = limit
                .data_as_concurrency_limit()
                .ok_or(CodecError::UnknownVariant("LimitData::ConcurrencyLimit"))?;
            Ok(Limit::Concurrency(ConcurrencyLimit {
                key: required_str(data.key(), "job_info.limit.concurrency.key")?.to_string(),
                max_concurrency: data.max_concurrency(),
            }))
        }
        fb::LimitData::GubernatorRateLimit => {
            let data = limit
                .data_as_gubernator_rate_limit()
                .ok_or(CodecError::UnknownVariant("LimitData::GubernatorRateLimit"))?;
            let retry_policy = data.retry_policy().ok_or(CodecError::MissingField(
                "job_info.limit.rate_limit.retry_policy",
            ))?;
            Ok(Limit::RateLimit(GubernatorRateLimit {
                name: required_str(data.name(), "job_info.limit.rate_limit.name")?.to_string(),
                unique_key: required_str(
                    data.unique_key(),
                    "job_info.limit.rate_limit.unique_key",
                )?
                .to_string(),
                limit: data.limit(),
                duration_ms: data.duration_ms(),
                hits: data.hits(),
                algorithm: from_fb_gubernator_algorithm(data.algorithm())?,
                behavior: data.behavior(),
                retry_policy: RateLimitRetryPolicy {
                    initial_backoff_ms: retry_policy.initial_backoff_ms(),
                    max_backoff_ms: retry_policy.max_backoff_ms(),
                    backoff_multiplier: retry_policy.backoff_multiplier(),
                    max_retries: retry_policy.max_retries(),
                },
            }))
        }
        fb::LimitData::FloatingConcurrencyLimit => {
            let data =
                limit
                    .data_as_floating_concurrency_limit()
                    .ok_or(CodecError::UnknownVariant(
                        "LimitData::FloatingConcurrencyLimit",
                    ))?;
            Ok(Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                key: required_str(data.key(), "job_info.limit.floating.key")?.to_string(),
                default_max_concurrency: data.default_max_concurrency(),
                refresh_interval_ms: data.refresh_interval_ms(),
                metadata: parse_string_pairs(data.metadata()),
            }))
        }
        _ => Err(CodecError::UnknownVariant("LimitData")),
    }
}

#[inline]
pub fn decode_job_info(bytes: &[u8]) -> Result<JobView, CodecError> {
    decode_job_info_bytes(Bytes::copy_from_slice(bytes))
}

#[inline]
pub(crate) fn decode_job_info_bytes(data: Bytes) -> Result<JobView, CodecError> {
    let job_info = flatbuffers::root::<fb::JobInfo>(&data).map_err(|e| fb_error("JobInfo", e))?;
    let _ = required_str(job_info.id(), "job_info.id")?;
    let _ = job_info
        .payload()
        .ok_or(CodecError::MissingField("job_info.payload"))?;
    let _ = required_str(job_info.task_group(), "job_info.task_group")?;
    let mut parsed_limits = Vec::new();
    if let Some(limits) = job_info.limits() {
        parsed_limits.reserve(limits.len());
        for limit in limits.iter() {
            parsed_limits.push(parse_limit(limit)?);
        }
    }
    Ok(JobView {
        data,
        limits: parsed_limits,
    })
}

// ===========================================================================
// JobStatus — returns owned JobStatus directly
// ===========================================================================

#[inline]
pub fn decode_job_status(bytes: &[u8]) -> Result<JobStatus, CodecError> {
    let status = flatbuffers::root::<fb::JobStatus>(bytes).map_err(|e| fb_error("JobStatus", e))?;
    let kind = from_fb_job_status_kind(status.kind())?;
    let next_attempt_starts_after_ms = if status.has_next_attempt_starts_after_ms() {
        Some(status.next_attempt_starts_after_ms())
    } else {
        None
    };
    let current_attempt = if status.has_current_attempt() {
        Some(status.current_attempt())
    } else {
        None
    };
    Ok(JobStatus {
        kind,
        changed_at_ms: status.changed_at_ms(),
        next_attempt_starts_after_ms,
        current_attempt,
    })
}

// ===========================================================================
// JobAttemptView (merged from DecodedAttempt + job_attempt.rs JobAttemptView)
// ===========================================================================

/// Zero-copy view over serialized `JobAttempt` data.
#[derive(Clone)]
pub struct JobAttemptView {
    data: Bytes,
    state: AttemptStatus,
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
    pub fn new(bytes: impl Into<Bytes>) -> Result<Self, CodecError> {
        decode_attempt_bytes(bytes.into())
    }

    fn attempt(&self) -> fb::JobAttempt<'_> {
        // SAFETY: validated before construction.
        unsafe { flatbuffers::root_unchecked::<fb::JobAttempt>(&self.data) }
    }

    pub fn job_id(&self) -> &str {
        self.attempt().job_id().unwrap_or_default()
    }

    pub fn attempt_number(&self) -> u32 {
        self.attempt().attempt_number()
    }

    pub fn relative_attempt_number(&self) -> u32 {
        self.attempt().relative_attempt_number()
    }

    pub fn task_id(&self) -> &str {
        self.attempt().task_id().unwrap_or_default()
    }

    pub fn started_at_ms(&self) -> i64 {
        self.attempt().started_at_ms()
    }

    pub fn state(&self) -> AttemptStatus {
        self.state.clone()
    }
}

fn parse_attempt_status(status: fb::AttemptStatus<'_>) -> Result<AttemptStatus, CodecError> {
    match status.data_type() {
        fb::AttemptStatusData::AttemptRunning => Ok(AttemptStatus::Running),
        fb::AttemptStatusData::AttemptSucceeded => {
            let succeeded =
                status
                    .data_as_attempt_succeeded()
                    .ok_or(CodecError::UnknownVariant(
                        "AttemptStatusData::AttemptSucceeded",
                    ))?;
            let result = match succeeded.result() {
                Some(result) => result.bytes().to_vec(),
                None => Vec::new(),
            };
            Ok(AttemptStatus::Succeeded {
                finished_at_ms: succeeded.finished_at_ms(),
                result,
            })
        }
        fb::AttemptStatusData::AttemptFailed => {
            let failed = status
                .data_as_attempt_failed()
                .ok_or(CodecError::UnknownVariant(
                    "AttemptStatusData::AttemptFailed",
                ))?;
            let error = match failed.error() {
                Some(error) => error.bytes().to_vec(),
                None => Vec::new(),
            };
            Ok(AttemptStatus::Failed {
                finished_at_ms: failed.finished_at_ms(),
                error_code: required_str(failed.error_code(), "attempt.failed.error_code")?
                    .to_string(),
                error,
            })
        }
        fb::AttemptStatusData::AttemptCancelled => {
            let cancelled =
                status
                    .data_as_attempt_cancelled()
                    .ok_or(CodecError::UnknownVariant(
                        "AttemptStatusData::AttemptCancelled",
                    ))?;
            Ok(AttemptStatus::Cancelled {
                finished_at_ms: cancelled.finished_at_ms(),
            })
        }
        _ => Err(CodecError::UnknownVariant("AttemptStatusData")),
    }
}

#[inline]
pub fn decode_attempt(bytes: &[u8]) -> Result<JobAttemptView, CodecError> {
    decode_attempt_bytes(Bytes::copy_from_slice(bytes))
}

#[inline]
pub(crate) fn decode_attempt_bytes(data: Bytes) -> Result<JobAttemptView, CodecError> {
    let attempt =
        flatbuffers::root::<fb::JobAttempt>(&data).map_err(|e| fb_error("JobAttempt", e))?;
    let _ = required_str(attempt.job_id(), "attempt.job_id")?;
    let _ = required_str(attempt.task_id(), "attempt.task_id")?;
    let status = attempt
        .status()
        .ok_or(CodecError::MissingField("attempt.status"))?;
    let state = parse_attempt_status(status)?;
    Ok(JobAttemptView { data, state })
}

// ===========================================================================
// Lease
// ===========================================================================

/// Decoded lease record view over validated flatbuffer bytes.
#[derive(Clone, Copy)]
pub struct DecodedLease<'a> {
    lease: fb::LeaseRecord<'a>,
    task: DecodedTask<'a>,
}

impl<'a> DecodedLease<'a> {
    pub fn worker_id(&self) -> &str {
        self.lease.worker_id().unwrap_or_default()
    }

    pub fn expiry_ms(&self) -> i64 {
        self.lease.expiry_ms()
    }

    pub fn started_at_ms(&self) -> i64 {
        self.lease.started_at_ms()
    }

    /// Get the decoded task enum for pattern matching.
    pub fn task(&self) -> DecodedTask<'a> {
        self.task
    }

    /// Convenience: tenant from the embedded task (dispatches across all variants).
    pub fn tenant(&self) -> &str {
        self.task.tenant()
    }

    /// Convenience: job_id from the embedded task.
    pub fn job_id(&self) -> &str {
        match self.task {
            DecodedTask::RunAttempt(d) => d.job_id().unwrap_or_default(),
            DecodedTask::RequestTicket(d) => d.job_id().unwrap_or_default(),
            DecodedTask::CheckRateLimit(d) => d.job_id().unwrap_or_default(),
            DecodedTask::RefreshFloatingLimit(_) => "",
        }
    }

    /// Convenience: attempt_number from the embedded task.
    pub fn attempt_number(&self) -> u32 {
        match self.task {
            DecodedTask::RunAttempt(d) => d.attempt_number(),
            DecodedTask::RequestTicket(d) => d.attempt_number(),
            DecodedTask::CheckRateLimit(d) => d.attempt_number(),
            DecodedTask::RefreshFloatingLimit(_) => 0,
        }
    }

    /// Convenience: relative_attempt_number from the embedded task.
    pub fn relative_attempt_number(&self) -> u32 {
        match self.task {
            DecodedTask::RunAttempt(d) => d.relative_attempt_number(),
            DecodedTask::RequestTicket(d) => d.relative_attempt_number(),
            DecodedTask::CheckRateLimit(d) => d.relative_attempt_number(),
            DecodedTask::RefreshFloatingLimit(_) => 0,
        }
    }

    /// Convenience: held_queues from the embedded task.
    pub fn held_queues(&self) -> Vec<String> {
        match self.task {
            DecodedTask::RunAttempt(d) => parse_string_values(d.held_queues()),
            DecodedTask::CheckRateLimit(d) => parse_string_values(d.held_queues()),
            DecodedTask::RequestTicket(_) | DecodedTask::RefreshFloatingLimit(_) => Vec::new(),
        }
    }

    /// Convenience: task_group from the embedded task.
    pub fn task_group(&self) -> &str {
        match self.task {
            DecodedTask::RunAttempt(d) => d.task_group().unwrap_or_default(),
            DecodedTask::RequestTicket(d) => d.task_group().unwrap_or_default(),
            DecodedTask::CheckRateLimit(d) => d.task_group().unwrap_or_default(),
            DecodedTask::RefreshFloatingLimit(d) => d.task_group().unwrap_or_default(),
        }
    }

    /// Convenience: task_id for RunAttempt leases (None for other types).
    pub fn task_id(&self) -> Option<&str> {
        match self.task {
            DecodedTask::RunAttempt(d) => d.id(),
            _ => None,
        }
    }

    /// Convenience: access the inner RunAttempt task if present.
    pub fn run_attempt_task(&self) -> Option<fb::TaskRunAttempt<'a>> {
        self.task.run_attempt()
    }

    /// Convenience: access the inner RefreshFloatingLimit task if present.
    pub fn refresh_floating_limit_task(&self) -> Option<fb::TaskRefreshFloatingLimit<'a>> {
        self.task.refresh_floating_limit()
    }

    /// Convenience: (task_id, queue_key) for RefreshFloatingLimit leases.
    pub fn refresh_floating_limit_info(&self) -> Option<(&str, &str)> {
        match self.task {
            DecodedTask::RefreshFloatingLimit(d) => Some((
                d.task_id().unwrap_or_default(),
                d.queue_key().unwrap_or_default(),
            )),
            _ => None,
        }
    }

    /// Materialize an owned Task from this lease's embedded task.
    pub fn to_task(&self) -> Result<Task, CodecError> {
        parse_task(self.task)
    }
}

#[inline]
pub fn decode_lease(bytes: &[u8]) -> Result<DecodedLease<'_>, CodecError> {
    let lease =
        flatbuffers::root::<fb::LeaseRecord>(bytes).map_err(|e| fb_error("LeaseRecord", e))?;
    let _ = required_str(lease.worker_id(), "lease.worker_id")?;
    let fb_task = lease.task().ok_or(CodecError::MissingField("lease.task"))?;
    let task = DecodedTask::from_fb(fb_task)?;
    // Validate required fields on the task without full materialization
    validate_decoded_task_fields(task)?;
    Ok(DecodedLease { lease, task })
}

// ===========================================================================
// Holder — returns owned HolderRecord directly
// ===========================================================================

#[inline]
pub fn decode_holder(bytes: &[u8]) -> Result<HolderRecord, CodecError> {
    let holder =
        flatbuffers::root::<fb::HolderRecord>(bytes).map_err(|e| fb_error("HolderRecord", e))?;
    Ok(HolderRecord {
        granted_at_ms: holder.granted_at_ms(),
    })
}

// ===========================================================================
// ConcurrencyAction
// ===========================================================================

#[derive(Debug, Clone, Copy)]
pub struct DecodedEnqueueTaskAction<'a> {
    pub start_time_ms: i64,
    pub priority: u8,
    pub job_id: &'a str,
    pub attempt_number: u32,
    pub relative_attempt_number: u32,
    pub task_group: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedConcurrencyAction<'a> {
    action: fb::ConcurrencyAction<'a>,
}

impl DecodedConcurrencyAction<'_> {
    pub fn enqueue_task(&self) -> Option<DecodedEnqueueTaskAction<'_>> {
        let enqueue = self.action.data_as_concurrency_action_enqueue_task()?;
        Some(DecodedEnqueueTaskAction {
            start_time_ms: enqueue.start_time_ms(),
            priority: enqueue.priority(),
            job_id: enqueue.job_id().unwrap_or_default(),
            attempt_number: enqueue.attempt_number(),
            relative_attempt_number: enqueue.relative_attempt_number(),
            task_group: enqueue.task_group().unwrap_or_default(),
        })
    }
}

#[inline]
pub fn decode_concurrency_action(bytes: &[u8]) -> Result<DecodedConcurrencyAction<'_>, CodecError> {
    let action = flatbuffers::root::<fb::ConcurrencyAction>(bytes)
        .map_err(|e| fb_error("ConcurrencyAction", e))?;

    match action.data_type() {
        fb::ConcurrencyActionData::ConcurrencyActionEnqueueTask => {
            let enqueue = action.data_as_concurrency_action_enqueue_task().ok_or(
                CodecError::UnknownVariant("ConcurrencyActionData::ConcurrencyActionEnqueueTask"),
            )?;
            let _ = required_str(enqueue.job_id(), "concurrency_action.enqueue.job_id")?;
            let _ = required_str(
                enqueue.task_group(),
                "concurrency_action.enqueue.task_group",
            )?;
        }
        _ => return Err(CodecError::UnknownVariant("ConcurrencyActionData")),
    }

    Ok(DecodedConcurrencyAction { action })
}

// ===========================================================================
// JobCancellation — returns owned JobCancellation directly
// ===========================================================================

#[inline]
pub fn decode_job_cancellation(bytes: &[u8]) -> Result<JobCancellation, CodecError> {
    let cancellation = flatbuffers::root::<fb::JobCancellation>(bytes)
        .map_err(|e| fb_error("JobCancellation", e))?;
    Ok(JobCancellation {
        cancelled_at_ms: cancellation.cancelled_at_ms(),
    })
}

// ===========================================================================
// FloatingLimitState
// ===========================================================================

#[derive(Clone)]
pub struct DecodedFloatingLimitState {
    data: Bytes,
}

impl DecodedFloatingLimitState {
    fn state(&self) -> fb::FloatingLimitState<'_> {
        // SAFETY: validated before construction.
        unsafe { flatbuffers::root_unchecked::<fb::FloatingLimitState>(&self.data) }
    }

    pub fn current_max_concurrency(&self) -> u32 {
        self.state().current_max_concurrency()
    }

    pub fn last_refreshed_at_ms(&self) -> i64 {
        self.state().last_refreshed_at_ms()
    }

    pub fn refresh_task_scheduled(&self) -> bool {
        self.state().refresh_task_scheduled()
    }

    pub fn refresh_interval_ms(&self) -> i64 {
        self.state().refresh_interval_ms()
    }

    pub fn default_max_concurrency(&self) -> u32 {
        self.state().default_max_concurrency()
    }

    pub fn retry_count(&self) -> u32 {
        self.state().retry_count()
    }

    pub fn next_retry_at_ms(&self) -> Option<i64> {
        let state = self.state();
        if state.has_next_retry_at_ms() {
            Some(state.next_retry_at_ms())
        } else {
            None
        }
    }

    pub fn metadata(&self) -> Vec<(String, String)> {
        parse_string_pairs(self.state().metadata())
    }

    pub fn to_owned(&self) -> FloatingLimitState {
        FloatingLimitState {
            current_max_concurrency: self.current_max_concurrency(),
            last_refreshed_at_ms: self.last_refreshed_at_ms(),
            refresh_task_scheduled: self.refresh_task_scheduled(),
            refresh_interval_ms: self.refresh_interval_ms(),
            default_max_concurrency: self.default_max_concurrency(),
            retry_count: self.retry_count(),
            next_retry_at_ms: self.next_retry_at_ms(),
            metadata: self.metadata(),
        }
    }
}

#[inline]
pub fn decode_floating_limit_state(bytes: &[u8]) -> Result<DecodedFloatingLimitState, CodecError> {
    decode_floating_limit_state_bytes(Bytes::copy_from_slice(bytes))
}

#[inline]
pub(crate) fn decode_floating_limit_state_bytes(
    data: Bytes,
) -> Result<DecodedFloatingLimitState, CodecError> {
    let _ = flatbuffers::root::<fb::FloatingLimitState>(&data)
        .map_err(|e| fb_error("FloatingLimitState", e))?;
    Ok(DecodedFloatingLimitState { data })
}
