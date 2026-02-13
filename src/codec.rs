use flatbuffers::FlatBufferBuilder;

use crate::flatbuf::silo_internal as fb;
use crate::job::{
    ConcurrencyLimit, FloatingConcurrencyLimit, FloatingLimitState, GubernatorAlgorithm,
    GubernatorRateLimit, JobCancellation, JobInfo, JobStatus, JobStatusKind, Limit,
    RateLimitRetryPolicy,
};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::retry::RetryPolicy;
use crate::task::{ConcurrencyAction, GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

/// Error type for codec operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum CodecError {
    /// Invalid flatbuffer payload
    #[error("flatbuffer error: {0}")]
    Flatbuffer(String),
    /// Required field was not present
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    /// Unknown union/enum variant
    #[error("unknown variant: {0}")]
    UnknownVariant(&'static str),
}

impl From<CodecError> for String {
    fn from(e: CodecError) -> String {
        e.to_string()
    }
}

fn fb_error(context: &'static str, err: flatbuffers::InvalidFlatbuffer) -> CodecError {
    CodecError::Flatbuffer(format!("{}: {}", context, err))
}

fn required_str<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, CodecError> {
    value.ok_or(CodecError::MissingField(field))
}

fn to_fb_gubernator_algorithm(alg: GubernatorAlgorithm) -> fb::GubernatorAlgorithm {
    match alg {
        GubernatorAlgorithm::TokenBucket => fb::GubernatorAlgorithm::TokenBucket,
        GubernatorAlgorithm::LeakyBucket => fb::GubernatorAlgorithm::LeakyBucket,
    }
}

fn from_fb_gubernator_algorithm(
    alg: fb::GubernatorAlgorithm,
) -> Result<GubernatorAlgorithm, CodecError> {
    match alg {
        fb::GubernatorAlgorithm::TokenBucket => Ok(GubernatorAlgorithm::TokenBucket),
        fb::GubernatorAlgorithm::LeakyBucket => Ok(GubernatorAlgorithm::LeakyBucket),
        _ => Err(CodecError::UnknownVariant("GubernatorAlgorithm")),
    }
}

fn to_fb_job_status_kind(kind: JobStatusKind) -> fb::JobStatusKind {
    match kind {
        JobStatusKind::Scheduled => fb::JobStatusKind::Scheduled,
        JobStatusKind::Running => fb::JobStatusKind::Running,
        JobStatusKind::Failed => fb::JobStatusKind::Failed,
        JobStatusKind::Cancelled => fb::JobStatusKind::Cancelled,
        JobStatusKind::Succeeded => fb::JobStatusKind::Succeeded,
    }
}

fn from_fb_job_status_kind(kind: fb::JobStatusKind) -> Result<JobStatusKind, CodecError> {
    match kind {
        fb::JobStatusKind::Scheduled => Ok(JobStatusKind::Scheduled),
        fb::JobStatusKind::Running => Ok(JobStatusKind::Running),
        fb::JobStatusKind::Failed => Ok(JobStatusKind::Failed),
        fb::JobStatusKind::Cancelled => Ok(JobStatusKind::Cancelled),
        fb::JobStatusKind::Succeeded => Ok(JobStatusKind::Succeeded),
        _ => Err(CodecError::UnknownVariant("JobStatusKind")),
    }
}

fn create_string_vector<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    values: &[String],
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<&'a str>>> {
    let mut offsets = Vec::with_capacity(values.len());
    for value in values {
        offsets.push(builder.create_string(value));
    }
    builder.create_vector(&offsets)
}

fn create_string_pair_vector<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    values: &[(String, String)],
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<fb::StringPair<'a>>>>
{
    let mut offsets = Vec::with_capacity(values.len());
    for (key, value) in values {
        let key_offset = builder.create_string(key);
        let value_offset = builder.create_string(value);
        let pair = fb::StringPair::create(
            builder,
            &fb::StringPairArgs {
                key: Some(key_offset),
                value: Some(value_offset),
            },
        );
        offsets.push(pair);
    }
    builder.create_vector(&offsets)
}

fn parse_string_pairs(
    values: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<fb::StringPair<'_>>>>,
) -> Vec<(String, String)> {
    match values {
        Some(values) => values
            .iter()
            .map(|pair| {
                (
                    pair.key().unwrap_or_default().to_string(),
                    pair.value().unwrap_or_default().to_string(),
                )
            })
            .collect(),
        None => Vec::new(),
    }
}

fn parse_gubernator_rate_limit_data(
    rate_limit: fb::GubernatorRateLimitData<'_>,
) -> Result<GubernatorRateLimitData, CodecError> {
    Ok(GubernatorRateLimitData {
        name: required_str(rate_limit.name(), "task.check_rate_limit.rate_limit.name")?.to_string(),
        unique_key: required_str(
            rate_limit.unique_key(),
            "task.check_rate_limit.rate_limit.unique_key",
        )?
        .to_string(),
        limit: rate_limit.limit(),
        duration_ms: rate_limit.duration_ms(),
        hits: rate_limit.hits(),
        algorithm: rate_limit.algorithm(),
        behavior: rate_limit.behavior(),
        retry_initial_backoff_ms: rate_limit.retry_initial_backoff_ms(),
        retry_max_backoff_ms: rate_limit.retry_max_backoff_ms(),
        retry_backoff_multiplier: rate_limit.retry_backoff_multiplier(),
        retry_max_retries: rate_limit.retry_max_retries(),
    })
}

fn build_gubernator_rate_limit_data<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    rate_limit: &GubernatorRateLimitData,
) -> flatbuffers::WIPOffset<fb::GubernatorRateLimitData<'a>> {
    let name = builder.create_string(&rate_limit.name);
    let unique_key = builder.create_string(&rate_limit.unique_key);
    fb::GubernatorRateLimitData::create(
        builder,
        &fb::GubernatorRateLimitDataArgs {
            name: Some(name),
            unique_key: Some(unique_key),
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
    )
}

fn validate_task_fields(task: fb::Task<'_>) -> Result<(), CodecError> {
    match task.data_type() {
        fb::TaskData::TaskRunAttempt => {
            let data = task
                .data_as_task_run_attempt()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskRunAttempt"))?;
            let _ = required_str(data.id(), "task.run_attempt.id")?;
            let _ = required_str(data.tenant(), "task.run_attempt.tenant")?;
            let _ = required_str(data.job_id(), "task.run_attempt.job_id")?;
            let _ = required_str(data.task_group(), "task.run_attempt.task_group")?;
        }
        fb::TaskData::TaskRequestTicket => {
            let data = task
                .data_as_task_request_ticket()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskRequestTicket"))?;
            let _ = required_str(data.queue(), "task.request_ticket.queue")?;
            let _ = required_str(data.tenant(), "task.request_ticket.tenant")?;
            let _ = required_str(data.job_id(), "task.request_ticket.job_id")?;
            let _ = required_str(data.request_id(), "task.request_ticket.request_id")?;
            let _ = required_str(data.task_group(), "task.request_ticket.task_group")?;
        }
        fb::TaskData::TaskCheckRateLimit => {
            let data = task
                .data_as_task_check_rate_limit()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskCheckRateLimit"))?;
            let _ = required_str(data.task_id(), "task.check_rate_limit.task_id")?;
            let _ = required_str(data.tenant(), "task.check_rate_limit.tenant")?;
            let _ = required_str(data.job_id(), "task.check_rate_limit.job_id")?;
            let _ = required_str(data.task_group(), "task.check_rate_limit.task_group")?;
            let rate_limit = data
                .rate_limit()
                .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
            let _ = required_str(rate_limit.name(), "task.check_rate_limit.rate_limit.name")?;
            let _ = required_str(
                rate_limit.unique_key(),
                "task.check_rate_limit.rate_limit.unique_key",
            )?;
        }
        fb::TaskData::TaskRefreshFloatingLimit => {
            let data =
                task.data_as_task_refresh_floating_limit()
                    .ok_or(CodecError::UnknownVariant(
                        "TaskData::TaskRefreshFloatingLimit",
                    ))?;
            let _ = required_str(data.task_id(), "task.refresh_floating_limit.task_id")?;
            let _ = required_str(data.tenant(), "task.refresh_floating_limit.tenant")?;
            let _ = required_str(data.queue_key(), "task.refresh_floating_limit.queue_key")?;
            let _ = required_str(data.task_group(), "task.refresh_floating_limit.task_group")?;
        }
        _ => return Err(CodecError::UnknownVariant("TaskData")),
    }

    Ok(())
}

fn parse_task_tenant<'a>(task: fb::Task<'a>) -> Result<&'a str, CodecError> {
    match task.data_type() {
        fb::TaskData::TaskRunAttempt => {
            let data = task
                .data_as_task_run_attempt()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskRunAttempt"))?;
            required_str(data.tenant(), "task.run_attempt.tenant")
        }
        fb::TaskData::TaskRequestTicket => {
            let data = task
                .data_as_task_request_ticket()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskRequestTicket"))?;
            required_str(data.tenant(), "task.request_ticket.tenant")
        }
        fb::TaskData::TaskCheckRateLimit => {
            let data = task
                .data_as_task_check_rate_limit()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskCheckRateLimit"))?;
            required_str(data.tenant(), "task.check_rate_limit.tenant")
        }
        fb::TaskData::TaskRefreshFloatingLimit => {
            let data =
                task.data_as_task_refresh_floating_limit()
                    .ok_or(CodecError::UnknownVariant(
                        "TaskData::TaskRefreshFloatingLimit",
                    ))?;
            required_str(data.tenant(), "task.refresh_floating_limit.tenant")
        }
        _ => Err(CodecError::UnknownVariant("TaskData")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedTaskKind {
    RunAttempt,
    RequestTicket,
    CheckRateLimit,
    RefreshFloatingLimit,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedRunAttemptTask<'a> {
    task: fb::TaskRunAttempt<'a>,
}

impl DecodedRunAttemptTask<'_> {
    pub fn id(&self) -> &str {
        self.task.id().unwrap_or_default()
    }

    pub fn tenant(&self) -> &str {
        self.task.tenant().unwrap_or_default()
    }

    pub fn job_id(&self) -> &str {
        self.task.job_id().unwrap_or_default()
    }

    pub fn attempt_number(&self) -> u32 {
        self.task.attempt_number()
    }

    pub fn relative_attempt_number(&self) -> u32 {
        self.task.relative_attempt_number()
    }

    pub fn task_group(&self) -> &str {
        self.task.task_group().unwrap_or_default()
    }

    pub fn held_queues(&self) -> Vec<String> {
        match self.task.held_queues() {
            Some(values) => values.iter().map(|value| value.to_string()).collect(),
            None => Vec::new(),
        }
    }

    pub fn to_owned(&self) -> Task {
        Task::RunAttempt {
            id: self.id().to_string(),
            tenant: self.tenant().to_string(),
            job_id: self.job_id().to_string(),
            attempt_number: self.attempt_number(),
            relative_attempt_number: self.relative_attempt_number(),
            held_queues: self.held_queues(),
            task_group: self.task_group().to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedRequestTicketTask<'a> {
    task: fb::TaskRequestTicket<'a>,
}

impl DecodedRequestTicketTask<'_> {
    pub fn queue(&self) -> &str {
        self.task.queue().unwrap_or_default()
    }

    pub fn start_time_ms(&self) -> i64 {
        self.task.start_time_ms()
    }

    pub fn priority(&self) -> u8 {
        self.task.priority()
    }

    pub fn tenant(&self) -> &str {
        self.task.tenant().unwrap_or_default()
    }

    pub fn job_id(&self) -> &str {
        self.task.job_id().unwrap_or_default()
    }

    pub fn attempt_number(&self) -> u32 {
        self.task.attempt_number()
    }

    pub fn relative_attempt_number(&self) -> u32 {
        self.task.relative_attempt_number()
    }

    pub fn request_id(&self) -> &str {
        self.task.request_id().unwrap_or_default()
    }

    pub fn task_group(&self) -> &str {
        self.task.task_group().unwrap_or_default()
    }

    pub fn to_owned(&self) -> Task {
        Task::RequestTicket {
            queue: self.queue().to_string(),
            start_time_ms: self.start_time_ms(),
            priority: self.priority(),
            tenant: self.tenant().to_string(),
            job_id: self.job_id().to_string(),
            attempt_number: self.attempt_number(),
            relative_attempt_number: self.relative_attempt_number(),
            request_id: self.request_id().to_string(),
            task_group: self.task_group().to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedCheckRateLimitTask<'a> {
    task: fb::TaskCheckRateLimit<'a>,
}

impl DecodedCheckRateLimitTask<'_> {
    pub fn task_id(&self) -> &str {
        self.task.task_id().unwrap_or_default()
    }

    pub fn tenant(&self) -> &str {
        self.task.tenant().unwrap_or_default()
    }

    pub fn job_id(&self) -> &str {
        self.task.job_id().unwrap_or_default()
    }

    pub fn attempt_number(&self) -> u32 {
        self.task.attempt_number()
    }

    pub fn relative_attempt_number(&self) -> u32 {
        self.task.relative_attempt_number()
    }

    pub fn limit_index(&self) -> u32 {
        self.task.limit_index()
    }

    pub fn retry_count(&self) -> u32 {
        self.task.retry_count()
    }

    pub fn started_at_ms(&self) -> i64 {
        self.task.started_at_ms()
    }

    pub fn priority(&self) -> u8 {
        self.task.priority()
    }

    pub fn task_group(&self) -> &str {
        self.task.task_group().unwrap_or_default()
    }

    pub fn held_queues(&self) -> Vec<String> {
        match self.task.held_queues() {
            Some(values) => values.iter().map(|value| value.to_string()).collect(),
            None => Vec::new(),
        }
    }

    pub fn rate_limit(&self) -> Result<GubernatorRateLimitData, CodecError> {
        let rate_limit = self
            .task
            .rate_limit()
            .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
        parse_gubernator_rate_limit_data(rate_limit)
    }

    pub fn to_owned(&self) -> Result<Task, CodecError> {
        Ok(Task::CheckRateLimit {
            task_id: self.task_id().to_string(),
            tenant: self.tenant().to_string(),
            job_id: self.job_id().to_string(),
            attempt_number: self.attempt_number(),
            relative_attempt_number: self.relative_attempt_number(),
            limit_index: self.limit_index(),
            rate_limit: self.rate_limit()?,
            retry_count: self.retry_count(),
            started_at_ms: self.started_at_ms(),
            priority: self.priority(),
            held_queues: self.held_queues(),
            task_group: self.task_group().to_string(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedTask<'a> {
    task: fb::Task<'a>,
}

impl<'a> DecodedTask<'a> {
    fn new(task: fb::Task<'a>) -> Result<Self, CodecError> {
        validate_task_fields(task)?;
        Ok(Self { task })
    }

    pub fn kind(&self) -> DecodedTaskKind {
        match self.task.data_type() {
            fb::TaskData::TaskRunAttempt => DecodedTaskKind::RunAttempt,
            fb::TaskData::TaskRequestTicket => DecodedTaskKind::RequestTicket,
            fb::TaskData::TaskCheckRateLimit => DecodedTaskKind::CheckRateLimit,
            fb::TaskData::TaskRefreshFloatingLimit => DecodedTaskKind::RefreshFloatingLimit,
            _ => unreachable!("task variants validated in decode_task_view"),
        }
    }

    pub fn tenant(&self) -> &str {
        parse_task_tenant(self.task).unwrap_or_default()
    }

    pub fn run_attempt(&self) -> Option<DecodedRunAttemptTask<'a>> {
        Some(DecodedRunAttemptTask {
            task: self.task.data_as_task_run_attempt()?,
        })
    }

    pub fn request_ticket(&self) -> Option<DecodedRequestTicketTask<'a>> {
        Some(DecodedRequestTicketTask {
            task: self.task.data_as_task_request_ticket()?,
        })
    }

    pub fn check_rate_limit(&self) -> Option<DecodedCheckRateLimitTask<'a>> {
        Some(DecodedCheckRateLimitTask {
            task: self.task.data_as_task_check_rate_limit()?,
        })
    }

    pub fn refresh_floating_limit(&self) -> Option<DecodedRefreshFloatingLimitTask<'a>> {
        Some(DecodedRefreshFloatingLimitTask {
            task: self.task.data_as_task_refresh_floating_limit()?,
        })
    }

    pub fn to_owned(&self) -> Result<Task, CodecError> {
        parse_task(self.task)
    }
}

fn parse_task(task: fb::Task<'_>) -> Result<Task, CodecError> {
    match task.data_type() {
        fb::TaskData::TaskRunAttempt => {
            let data = task
                .data_as_task_run_attempt()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskRunAttempt"))?;
            let held_queues = match data.held_queues() {
                Some(values) => values.iter().map(|value| value.to_string()).collect(),
                None => Vec::new(),
            };
            Ok(Task::RunAttempt {
                id: required_str(data.id(), "task.run_attempt.id")?.to_string(),
                tenant: required_str(data.tenant(), "task.run_attempt.tenant")?.to_string(),
                job_id: required_str(data.job_id(), "task.run_attempt.job_id")?.to_string(),
                attempt_number: data.attempt_number(),
                relative_attempt_number: data.relative_attempt_number(),
                held_queues,
                task_group: required_str(data.task_group(), "task.run_attempt.task_group")?
                    .to_string(),
            })
        }
        fb::TaskData::TaskRequestTicket => {
            let data = task
                .data_as_task_request_ticket()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskRequestTicket"))?;
            Ok(Task::RequestTicket {
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
            })
        }
        fb::TaskData::TaskCheckRateLimit => {
            let data = task
                .data_as_task_check_rate_limit()
                .ok_or(CodecError::UnknownVariant("TaskData::TaskCheckRateLimit"))?;
            let rate_limit = data
                .rate_limit()
                .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
            let held_queues = match data.held_queues() {
                Some(values) => values.iter().map(|value| value.to_string()).collect(),
                None => Vec::new(),
            };
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
                held_queues,
                task_group: required_str(data.task_group(), "task.check_rate_limit.task_group")?
                    .to_string(),
            })
        }
        fb::TaskData::TaskRefreshFloatingLimit => {
            let data =
                task.data_as_task_refresh_floating_limit()
                    .ok_or(CodecError::UnknownVariant(
                        "TaskData::TaskRefreshFloatingLimit",
                    ))?;
            let metadata = parse_string_pairs(data.metadata());
            Ok(Task::RefreshFloatingLimit {
                task_id: required_str(data.task_id(), "task.refresh_floating_limit.task_id")?
                    .to_string(),
                tenant: required_str(data.tenant(), "task.refresh_floating_limit.tenant")?
                    .to_string(),
                queue_key: required_str(data.queue_key(), "task.refresh_floating_limit.queue_key")?
                    .to_string(),
                current_max_concurrency: data.current_max_concurrency(),
                last_refreshed_at_ms: data.last_refreshed_at_ms(),
                metadata,
                task_group: required_str(
                    data.task_group(),
                    "task.refresh_floating_limit.task_group",
                )?
                .to_string(),
            })
        }
        _ => Err(CodecError::UnknownVariant("TaskData")),
    }
}

fn build_task<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    task: &Task,
) -> flatbuffers::WIPOffset<fb::Task<'a>> {
    match task {
        Task::RunAttempt {
            id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            held_queues,
            task_group,
        } => {
            let id = builder.create_string(id);
            let tenant = builder.create_string(tenant);
            let job_id = builder.create_string(job_id);
            let held_queues = create_string_vector(builder, held_queues);
            let task_group = builder.create_string(task_group);
            let run_attempt = fb::TaskRunAttempt::create(
                builder,
                &fb::TaskRunAttemptArgs {
                    id: Some(id),
                    tenant: Some(tenant),
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    held_queues: Some(held_queues),
                    task_group: Some(task_group),
                },
            );
            fb::Task::create(
                builder,
                &fb::TaskArgs {
                    data_type: fb::TaskData::TaskRunAttempt,
                    data: Some(run_attempt.as_union_value()),
                },
            )
        }
        Task::RequestTicket {
            queue,
            start_time_ms,
            priority,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            request_id,
            task_group,
        } => {
            let queue = builder.create_string(queue);
            let tenant = builder.create_string(tenant);
            let job_id = builder.create_string(job_id);
            let request_id = builder.create_string(request_id);
            let task_group = builder.create_string(task_group);
            let request_ticket = fb::TaskRequestTicket::create(
                builder,
                &fb::TaskRequestTicketArgs {
                    queue: Some(queue),
                    start_time_ms: *start_time_ms,
                    priority: *priority,
                    tenant: Some(tenant),
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    request_id: Some(request_id),
                    task_group: Some(task_group),
                },
            );
            fb::Task::create(
                builder,
                &fb::TaskArgs {
                    data_type: fb::TaskData::TaskRequestTicket,
                    data: Some(request_ticket.as_union_value()),
                },
            )
        }
        Task::CheckRateLimit {
            task_id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            limit_index,
            rate_limit,
            retry_count,
            started_at_ms,
            priority,
            held_queues,
            task_group,
        } => {
            let task_id = builder.create_string(task_id);
            let tenant = builder.create_string(tenant);
            let job_id = builder.create_string(job_id);
            let rate_limit = build_gubernator_rate_limit_data(builder, rate_limit);
            let held_queues = create_string_vector(builder, held_queues);
            let task_group = builder.create_string(task_group);
            let check_rate_limit = fb::TaskCheckRateLimit::create(
                builder,
                &fb::TaskCheckRateLimitArgs {
                    task_id: Some(task_id),
                    tenant: Some(tenant),
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    limit_index: *limit_index,
                    rate_limit: Some(rate_limit),
                    retry_count: *retry_count,
                    started_at_ms: *started_at_ms,
                    priority: *priority,
                    held_queues: Some(held_queues),
                    task_group: Some(task_group),
                },
            );
            fb::Task::create(
                builder,
                &fb::TaskArgs {
                    data_type: fb::TaskData::TaskCheckRateLimit,
                    data: Some(check_rate_limit.as_union_value()),
                },
            )
        }
        Task::RefreshFloatingLimit {
            task_id,
            tenant,
            queue_key,
            current_max_concurrency,
            last_refreshed_at_ms,
            metadata,
            task_group,
        } => {
            let task_id = builder.create_string(task_id);
            let tenant = builder.create_string(tenant);
            let queue_key = builder.create_string(queue_key);
            let metadata = create_string_pair_vector(builder, metadata);
            let task_group = builder.create_string(task_group);
            let refresh = fb::TaskRefreshFloatingLimit::create(
                builder,
                &fb::TaskRefreshFloatingLimitArgs {
                    task_id: Some(task_id),
                    tenant: Some(tenant),
                    queue_key: Some(queue_key),
                    current_max_concurrency: *current_max_concurrency,
                    last_refreshed_at_ms: *last_refreshed_at_ms,
                    metadata: Some(metadata),
                    task_group: Some(task_group),
                },
            );
            fb::Task::create(
                builder,
                &fb::TaskArgs {
                    data_type: fb::TaskData::TaskRefreshFloatingLimit,
                    data: Some(refresh.as_union_value()),
                },
            )
        }
    }
}

#[inline]
pub fn encode_task(task: &Task) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let encoded = build_task(&mut builder, task);
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn decode_task_view(bytes: &[u8]) -> Result<DecodedTask<'_>, CodecError> {
    let root = flatbuffers::root::<fb::Task>(bytes).map_err(|e| fb_error("Task", e))?;
    DecodedTask::new(root)
}

#[inline]
pub fn decode_task_view_unchecked(bytes: &[u8]) -> Result<DecodedTask<'_>, CodecError> {
    let root = flatbuffers::root::<fb::Task>(bytes).map_err(|e| fb_error("Task", e))?;
    Ok(DecodedTask { task: root })
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
    let root = flatbuffers::root::<fb::Task>(bytes).map_err(|e| fb_error("Task", e))?;
    parse_task_tenant(root)
}

#[inline]
pub fn task_is_refresh_floating_limit(bytes: &[u8]) -> Result<bool, CodecError> {
    Ok(matches!(
        decode_task_view(bytes)?.kind(),
        DecodedTaskKind::RefreshFloatingLimit
    ))
}

#[inline]
pub fn encode_lease(record: &LeaseRecord) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(&record.worker_id);
    let task = build_task(&mut builder, &record.task);
    let lease = fb::LeaseRecord::create(
        &mut builder,
        &fb::LeaseRecordArgs {
            worker_id: Some(worker_id),
            task: Some(task),
            expiry_ms: record.expiry_ms,
            started_at_ms: record.started_at_ms,
        },
    );
    builder.finish(lease, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded lease record that owns serialized data
#[derive(Clone)]
pub struct DecodedLease {
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedRefreshFloatingLimitTask<'a> {
    task: fb::TaskRefreshFloatingLimit<'a>,
}

impl DecodedRefreshFloatingLimitTask<'_> {
    pub fn task_id(&self) -> &str {
        self.task.task_id().unwrap_or_default()
    }

    pub fn tenant(&self) -> &str {
        self.task.tenant().unwrap_or_default()
    }

    pub fn queue_key(&self) -> &str {
        self.task.queue_key().unwrap_or_default()
    }

    pub fn current_max_concurrency(&self) -> u32 {
        self.task.current_max_concurrency()
    }

    pub fn last_refreshed_at_ms(&self) -> i64 {
        self.task.last_refreshed_at_ms()
    }

    pub fn task_group(&self) -> &str {
        self.task.task_group().unwrap_or_default()
    }

    pub fn metadata(&self) -> Vec<(String, String)> {
        parse_string_pairs(self.task.metadata())
    }

    pub fn to_owned(&self) -> Task {
        Task::RefreshFloatingLimit {
            task_id: self.task_id().to_string(),
            tenant: self.tenant().to_string(),
            queue_key: self.queue_key().to_string(),
            current_max_concurrency: self.current_max_concurrency(),
            last_refreshed_at_ms: self.last_refreshed_at_ms(),
            metadata: self.metadata(),
            task_group: self.task_group().to_string(),
        }
    }
}

impl DecodedLease {
    fn lease(&self) -> fb::LeaseRecord<'_> {
        // SAFETY: validated in decode_lease before construction.
        unsafe { flatbuffers::root_unchecked::<fb::LeaseRecord>(&self.data) }
    }

    fn task(&self) -> Option<fb::Task<'_>> {
        self.lease().task()
    }

    fn refresh_task(&self) -> Option<fb::TaskRefreshFloatingLimit<'_>> {
        self.task()?.data_as_task_refresh_floating_limit()
    }

    /// Get the worker_id from this lease (zero-copy)
    pub fn worker_id(&self) -> &str {
        self.lease().worker_id().unwrap_or_default()
    }

    /// Get the expiry time from this lease
    pub fn expiry_ms(&self) -> i64 {
        self.lease().expiry_ms()
    }

    /// Get started_at_ms from this lease (when the attempt started)
    pub fn started_at_ms(&self) -> i64 {
        self.lease().started_at_ms()
    }

    /// Extract task_id from the leased task (zero-copy). Only valid for RunAttempt tasks.
    pub fn task_id(&self) -> Option<&str> {
        let task = self.task()?;
        let run_attempt = task.data_as_task_run_attempt()?;
        run_attempt.id()
    }

    /// Extract refresh floating limit task info (zero-copy). Returns (task_id, queue_key) if this is a RefreshFloatingLimit task.
    pub fn refresh_floating_limit_info(&self) -> Option<(&str, &str)> {
        let refresh = self.refresh_task()?;
        Some((
            refresh.task_id().unwrap_or_default(),
            refresh.queue_key().unwrap_or_default(),
        ))
    }

    pub fn refresh_floating_limit_task(&self) -> Option<DecodedRefreshFloatingLimitTask<'_>> {
        Some(DecodedRefreshFloatingLimitTask {
            task: self.refresh_task()?,
        })
    }

    /// Extract tenant from the leased task (zero-copy).
    pub fn tenant(&self) -> &str {
        let Some(task) = self.task() else {
            return "";
        };
        if let Some(run_attempt) = task.data_as_task_run_attempt() {
            return run_attempt.tenant().unwrap_or_default();
        }
        if let Some(request_ticket) = task.data_as_task_request_ticket() {
            return request_ticket.tenant().unwrap_or_default();
        }
        if let Some(check_rate_limit) = task.data_as_task_check_rate_limit() {
            return check_rate_limit.tenant().unwrap_or_default();
        }
        if let Some(refresh) = task.data_as_task_refresh_floating_limit() {
            return refresh.tenant().unwrap_or_default();
        }
        ""
    }

    /// Extract job_id from the leased task (zero-copy). Returns empty string for RefreshFloatingLimit.
    pub fn job_id(&self) -> &str {
        let Some(task) = self.task() else {
            return "";
        };
        if let Some(run_attempt) = task.data_as_task_run_attempt() {
            return run_attempt.job_id().unwrap_or_default();
        }
        if let Some(request_ticket) = task.data_as_task_request_ticket() {
            return request_ticket.job_id().unwrap_or_default();
        }
        if let Some(check_rate_limit) = task.data_as_task_check_rate_limit() {
            return check_rate_limit.job_id().unwrap_or_default();
        }
        ""
    }

    /// Extract attempt_number from the leased task (zero-copy). Returns 0 for RefreshFloatingLimit.
    pub fn attempt_number(&self) -> u32 {
        let Some(task) = self.task() else {
            return 0;
        };
        if let Some(run_attempt) = task.data_as_task_run_attempt() {
            return run_attempt.attempt_number();
        }
        if let Some(request_ticket) = task.data_as_task_request_ticket() {
            return request_ticket.attempt_number();
        }
        if let Some(check_rate_limit) = task.data_as_task_check_rate_limit() {
            return check_rate_limit.attempt_number();
        }
        0
    }

    /// Extract relative_attempt_number from the leased task (zero-copy). Returns 0 for RefreshFloatingLimit.
    pub fn relative_attempt_number(&self) -> u32 {
        let Some(task) = self.task() else {
            return 0;
        };
        if let Some(run_attempt) = task.data_as_task_run_attempt() {
            return run_attempt.relative_attempt_number();
        }
        if let Some(request_ticket) = task.data_as_task_request_ticket() {
            return request_ticket.relative_attempt_number();
        }
        if let Some(check_rate_limit) = task.data_as_task_check_rate_limit() {
            return check_rate_limit.relative_attempt_number();
        }
        0
    }

    /// Extract held_queues from the leased task as owned strings (allocation required)
    pub fn held_queues(&self) -> Vec<String> {
        let Some(task) = self.task() else {
            return Vec::new();
        };
        if let Some(run_attempt) = task.data_as_task_run_attempt() {
            return match run_attempt.held_queues() {
                Some(values) => values.iter().map(|value| value.to_string()).collect(),
                None => Vec::new(),
            };
        }
        if let Some(check_rate_limit) = task.data_as_task_check_rate_limit() {
            return match check_rate_limit.held_queues() {
                Some(values) => values.iter().map(|value| value.to_string()).collect(),
                None => Vec::new(),
            };
        }
        Vec::new()
    }

    /// Extract task_group from the leased task (zero-copy).
    pub fn task_group(&self) -> &str {
        let Some(task) = self.task() else {
            return "";
        };
        if let Some(run_attempt) = task.data_as_task_run_attempt() {
            return run_attempt.task_group().unwrap_or_default();
        }
        if let Some(request_ticket) = task.data_as_task_request_ticket() {
            return request_ticket.task_group().unwrap_or_default();
        }
        if let Some(check_rate_limit) = task.data_as_task_check_rate_limit() {
            return check_rate_limit.task_group().unwrap_or_default();
        }
        if let Some(refresh) = task.data_as_task_refresh_floating_limit() {
            return refresh.task_group().unwrap_or_default();
        }
        ""
    }

    /// Convert to an owned Task - only call when you need to create a new record.
    pub fn to_task(&self) -> Result<Task, CodecError> {
        let task = self.task().ok_or(CodecError::MissingField("lease.task"))?;
        parse_task(task)
    }
}

#[inline]
pub fn decode_lease(bytes: &[u8]) -> Result<DecodedLease, CodecError> {
    let data = bytes.to_vec();
    let lease =
        flatbuffers::root::<fb::LeaseRecord>(&data).map_err(|e| fb_error("LeaseRecord", e))?;
    let _ = required_str(lease.worker_id(), "lease.worker_id")?;
    let task = lease.task().ok_or(CodecError::MissingField("lease.task"))?;
    validate_task_fields(task)?;
    Ok(DecodedLease { data })
}

fn build_attempt_status<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    status: &AttemptStatus,
) -> flatbuffers::WIPOffset<fb::AttemptStatus<'a>> {
    match status {
        AttemptStatus::Running => {
            let running = fb::AttemptRunning::create(builder, &fb::AttemptRunningArgs::default());
            fb::AttemptStatus::create(
                builder,
                &fb::AttemptStatusArgs {
                    data_type: fb::AttemptStatusData::AttemptRunning,
                    data: Some(running.as_union_value()),
                },
            )
        }
        AttemptStatus::Succeeded {
            finished_at_ms,
            result,
        } => {
            let result = builder.create_vector(result);
            let succeeded = fb::AttemptSucceeded::create(
                builder,
                &fb::AttemptSucceededArgs {
                    finished_at_ms: *finished_at_ms,
                    result: Some(result),
                },
            );
            fb::AttemptStatus::create(
                builder,
                &fb::AttemptStatusArgs {
                    data_type: fb::AttemptStatusData::AttemptSucceeded,
                    data: Some(succeeded.as_union_value()),
                },
            )
        }
        AttemptStatus::Failed {
            finished_at_ms,
            error_code,
            error,
        } => {
            let error_code = builder.create_string(error_code);
            let error = builder.create_vector(error);
            let failed = fb::AttemptFailed::create(
                builder,
                &fb::AttemptFailedArgs {
                    finished_at_ms: *finished_at_ms,
                    error_code: Some(error_code),
                    error: Some(error),
                },
            );
            fb::AttemptStatus::create(
                builder,
                &fb::AttemptStatusArgs {
                    data_type: fb::AttemptStatusData::AttemptFailed,
                    data: Some(failed.as_union_value()),
                },
            )
        }
        AttemptStatus::Cancelled { finished_at_ms } => {
            let cancelled = fb::AttemptCancelled::create(
                builder,
                &fb::AttemptCancelledArgs {
                    finished_at_ms: *finished_at_ms,
                },
            );
            fb::AttemptStatus::create(
                builder,
                &fb::AttemptStatusArgs {
                    data_type: fb::AttemptStatusData::AttemptCancelled,
                    data: Some(cancelled.as_union_value()),
                },
            )
        }
    }
}

fn validate_attempt_status_fields(status: fb::AttemptStatus<'_>) -> Result<(), CodecError> {
    match status.data_type() {
        fb::AttemptStatusData::AttemptRunning => {
            let _ = status
                .data_as_attempt_running()
                .ok_or(CodecError::UnknownVariant(
                    "AttemptStatusData::AttemptRunning",
                ))?;
        }
        fb::AttemptStatusData::AttemptSucceeded => {
            let _ = status
                .data_as_attempt_succeeded()
                .ok_or(CodecError::UnknownVariant(
                    "AttemptStatusData::AttemptSucceeded",
                ))?;
        }
        fb::AttemptStatusData::AttemptFailed => {
            let failed = status
                .data_as_attempt_failed()
                .ok_or(CodecError::UnknownVariant(
                    "AttemptStatusData::AttemptFailed",
                ))?;
            let _ = required_str(failed.error_code(), "attempt.failed.error_code")?;
        }
        fb::AttemptStatusData::AttemptCancelled => {
            let _ = status
                .data_as_attempt_cancelled()
                .ok_or(CodecError::UnknownVariant(
                    "AttemptStatusData::AttemptCancelled",
                ))?;
        }
        _ => return Err(CodecError::UnknownVariant("AttemptStatusData")),
    }
    Ok(())
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
pub fn encode_attempt(attempt: &JobAttempt) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let job_id = builder.create_string(&attempt.job_id);
    let task_id = builder.create_string(&attempt.task_id);
    let status = build_attempt_status(&mut builder, &attempt.status);
    let encoded = fb::JobAttempt::create(
        &mut builder,
        &fb::JobAttemptArgs {
            job_id: Some(job_id),
            attempt_number: attempt.attempt_number,
            relative_attempt_number: attempt.relative_attempt_number,
            task_id: Some(task_id),
            started_at_ms: attempt.started_at_ms,
            status: Some(status),
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded job attempt that owns serialized data
#[derive(Clone)]
pub struct DecodedAttempt {
    data: Vec<u8>,
}

impl DecodedAttempt {
    fn attempt(&self) -> fb::JobAttempt<'_> {
        // SAFETY: validated in decode_attempt before construction.
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
        let Some(status) = self.attempt().status() else {
            return AttemptStatus::Running;
        };
        match parse_attempt_status(status) {
            Ok(status) => status,
            Err(_) => AttemptStatus::Running,
        }
    }
}

#[inline]
pub fn decode_attempt(bytes: &[u8]) -> Result<DecodedAttempt, CodecError> {
    let data = bytes.to_vec();
    let attempt =
        flatbuffers::root::<fb::JobAttempt>(&data).map_err(|e| fb_error("JobAttempt", e))?;
    let _ = required_str(attempt.job_id(), "attempt.job_id")?;
    let _ = required_str(attempt.task_id(), "attempt.task_id")?;
    let status = attempt
        .status()
        .ok_or(CodecError::MissingField("attempt.status"))?;
    validate_attempt_status_fields(status)?;
    Ok(DecodedAttempt { data })
}

fn build_retry_policy<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    retry_policy: &RetryPolicy,
) -> flatbuffers::WIPOffset<fb::RetryPolicy<'a>> {
    fb::RetryPolicy::create(
        builder,
        &fb::RetryPolicyArgs {
            retry_count: retry_policy.retry_count,
            initial_interval_ms: retry_policy.initial_interval_ms,
            max_interval_ms: retry_policy.max_interval_ms,
            randomize_interval: retry_policy.randomize_interval,
            backoff_factor: retry_policy.backoff_factor,
        },
    )
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

fn build_limit<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    limit: &Limit,
) -> flatbuffers::WIPOffset<fb::Limit<'a>> {
    match limit {
        Limit::Concurrency(limit) => {
            let key = builder.create_string(&limit.key);
            let inner = fb::ConcurrencyLimit::create(
                builder,
                &fb::ConcurrencyLimitArgs {
                    key: Some(key),
                    max_concurrency: limit.max_concurrency,
                },
            );
            fb::Limit::create(
                builder,
                &fb::LimitArgs {
                    data_type: fb::LimitData::ConcurrencyLimit,
                    data: Some(inner.as_union_value()),
                },
            )
        }
        Limit::RateLimit(limit) => {
            let name = builder.create_string(&limit.name);
            let unique_key = builder.create_string(&limit.unique_key);
            let retry_policy = fb::RateLimitRetryPolicy::create(
                builder,
                &fb::RateLimitRetryPolicyArgs {
                    initial_backoff_ms: limit.retry_policy.initial_backoff_ms,
                    max_backoff_ms: limit.retry_policy.max_backoff_ms,
                    backoff_multiplier: limit.retry_policy.backoff_multiplier,
                    max_retries: limit.retry_policy.max_retries,
                },
            );
            let inner = fb::GubernatorRateLimit::create(
                builder,
                &fb::GubernatorRateLimitArgs {
                    name: Some(name),
                    unique_key: Some(unique_key),
                    limit: limit.limit,
                    duration_ms: limit.duration_ms,
                    hits: limit.hits,
                    algorithm: to_fb_gubernator_algorithm(limit.algorithm),
                    behavior: limit.behavior,
                    retry_policy: Some(retry_policy),
                },
            );
            fb::Limit::create(
                builder,
                &fb::LimitArgs {
                    data_type: fb::LimitData::GubernatorRateLimit,
                    data: Some(inner.as_union_value()),
                },
            )
        }
        Limit::FloatingConcurrency(limit) => {
            let key = builder.create_string(&limit.key);
            let metadata = create_string_pair_vector(builder, &limit.metadata);
            let inner = fb::FloatingConcurrencyLimit::create(
                builder,
                &fb::FloatingConcurrencyLimitArgs {
                    key: Some(key),
                    default_max_concurrency: limit.default_max_concurrency,
                    refresh_interval_ms: limit.refresh_interval_ms,
                    metadata: Some(metadata),
                },
            );
            fb::Limit::create(
                builder,
                &fb::LimitArgs {
                    data_type: fb::LimitData::FloatingConcurrencyLimit,
                    data: Some(inner.as_union_value()),
                },
            )
        }
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

fn validate_limit_fields(limit: fb::Limit<'_>) -> Result<(), CodecError> {
    match limit.data_type() {
        fb::LimitData::ConcurrencyLimit => {
            let data = limit
                .data_as_concurrency_limit()
                .ok_or(CodecError::UnknownVariant("LimitData::ConcurrencyLimit"))?;
            let _ = required_str(data.key(), "job_info.limit.concurrency.key")?;
        }
        fb::LimitData::GubernatorRateLimit => {
            let data = limit
                .data_as_gubernator_rate_limit()
                .ok_or(CodecError::UnknownVariant("LimitData::GubernatorRateLimit"))?;
            let _ = required_str(data.name(), "job_info.limit.rate_limit.name")?;
            let _ = required_str(data.unique_key(), "job_info.limit.rate_limit.unique_key")?;
            let _ = data.retry_policy().ok_or(CodecError::MissingField(
                "job_info.limit.rate_limit.retry_policy",
            ))?;
            let _ = from_fb_gubernator_algorithm(data.algorithm())?;
        }
        fb::LimitData::FloatingConcurrencyLimit => {
            let data =
                limit
                    .data_as_floating_concurrency_limit()
                    .ok_or(CodecError::UnknownVariant(
                        "LimitData::FloatingConcurrencyLimit",
                    ))?;
            let _ = required_str(data.key(), "job_info.limit.floating.key")?;
        }
        _ => return Err(CodecError::UnknownVariant("LimitData")),
    }

    Ok(())
}

#[inline]
pub fn encode_job_info(job: &JobInfo) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);

    let id = builder.create_string(&job.id);
    let payload = builder.create_vector(&job.payload);
    let retry_policy = job
        .retry_policy
        .as_ref()
        .map(|retry_policy| build_retry_policy(&mut builder, retry_policy));
    let metadata = create_string_pair_vector(&mut builder, &job.metadata);

    let mut limit_offsets = Vec::with_capacity(job.limits.len());
    for limit in &job.limits {
        limit_offsets.push(build_limit(&mut builder, limit));
    }
    let limits = builder.create_vector(&limit_offsets);
    let task_group = builder.create_string(&job.task_group);

    let encoded = fb::JobInfo::create(
        &mut builder,
        &fb::JobInfoArgs {
            id: Some(id),
            priority: job.priority,
            enqueue_time_ms: job.enqueue_time_ms,
            payload: Some(payload),
            retry_policy,
            metadata: Some(metadata),
            limits: Some(limits),
            task_group: Some(task_group),
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded job info that owns serialized data
#[derive(Clone)]
pub struct DecodedJobInfo {
    data: Vec<u8>,
}

impl DecodedJobInfo {
    fn job_info(&self) -> fb::JobInfo<'_> {
        // SAFETY: validated in decode_job_info before construction.
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

    pub fn limits(&self) -> Vec<Limit> {
        let Some(limits) = self.job_info().limits() else {
            return Vec::new();
        };

        let mut parsed = Vec::with_capacity(limits.len());
        for limit in limits.iter() {
            if let Ok(limit) = parse_limit(limit) {
                parsed.push(limit);
            }
        }
        parsed
    }
}

#[inline]
pub fn decode_job_info(bytes: &[u8]) -> Result<DecodedJobInfo, CodecError> {
    let data = bytes.to_vec();
    let job_info = flatbuffers::root::<fb::JobInfo>(&data).map_err(|e| fb_error("JobInfo", e))?;
    let _ = required_str(job_info.id(), "job_info.id")?;
    let _ = job_info
        .payload()
        .ok_or(CodecError::MissingField("job_info.payload"))?;
    let _ = required_str(job_info.task_group(), "job_info.task_group")?;
    if let Some(limits) = job_info.limits() {
        for limit in limits.iter() {
            validate_limit_fields(limit)?;
        }
    }
    Ok(DecodedJobInfo { data })
}

#[inline]
pub fn encode_job_status(status: &JobStatus) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(128);
    let encoded = fb::JobStatus::create(
        &mut builder,
        &fb::JobStatusArgs {
            kind: to_fb_job_status_kind(status.kind),
            changed_at_ms: status.changed_at_ms,
            has_next_attempt_starts_after_ms: status.next_attempt_starts_after_ms.is_some(),
            next_attempt_starts_after_ms: status.next_attempt_starts_after_ms.unwrap_or_default(),
            has_current_attempt: status.current_attempt.is_some(),
            current_attempt: status.current_attempt.unwrap_or_default(),
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded job status that owns serialized data
#[derive(Clone)]
pub struct DecodedJobStatus {
    data: Vec<u8>,
}

impl DecodedJobStatus {
    fn job_status(&self) -> fb::JobStatus<'_> {
        // SAFETY: validated in decode_job_status before construction.
        unsafe { flatbuffers::root_unchecked::<fb::JobStatus>(&self.data) }
    }

    pub fn kind(&self) -> JobStatusKind {
        from_fb_job_status_kind(self.job_status().kind()).unwrap_or(JobStatusKind::Scheduled)
    }

    pub fn changed_at_ms(&self) -> i64 {
        self.job_status().changed_at_ms()
    }

    pub fn next_attempt_starts_after_ms(&self) -> Option<i64> {
        let status = self.job_status();
        if status.has_next_attempt_starts_after_ms() {
            Some(status.next_attempt_starts_after_ms())
        } else {
            None
        }
    }

    pub fn current_attempt(&self) -> Option<u32> {
        let status = self.job_status();
        if status.has_current_attempt() {
            Some(status.current_attempt())
        } else {
            None
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.kind(),
            JobStatusKind::Succeeded | JobStatusKind::Failed | JobStatusKind::Cancelled
        )
    }

    pub fn to_owned(&self) -> JobStatus {
        JobStatus {
            kind: self.kind(),
            changed_at_ms: self.changed_at_ms(),
            next_attempt_starts_after_ms: self.next_attempt_starts_after_ms(),
            current_attempt: self.current_attempt(),
        }
    }
}

#[inline]
pub fn decode_job_status(bytes: &[u8]) -> Result<DecodedJobStatus, CodecError> {
    let data = bytes.to_vec();
    let status = flatbuffers::root::<fb::JobStatus>(&data).map_err(|e| fb_error("JobStatus", e))?;
    let _ = from_fb_job_status_kind(status.kind())?;
    Ok(DecodedJobStatus { data })
}

#[inline]
pub fn encode_holder(holder: &HolderRecord) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(64);
    let encoded = fb::HolderRecord::create(
        &mut builder,
        &fb::HolderRecordArgs {
            granted_at_ms: holder.granted_at_ms,
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded holder record that owns serialized data
#[derive(Clone)]
pub struct DecodedHolder {
    data: Vec<u8>,
}

impl DecodedHolder {
    fn holder(&self) -> fb::HolderRecord<'_> {
        // SAFETY: validated in decode_holder before construction.
        unsafe { flatbuffers::root_unchecked::<fb::HolderRecord>(&self.data) }
    }

    /// Get the granted_at time from this holder record
    pub fn granted_at_ms(&self) -> i64 {
        self.holder().granted_at_ms()
    }
}

#[inline]
pub fn decode_holder(bytes: &[u8]) -> Result<DecodedHolder, CodecError> {
    let data = bytes.to_vec();
    let _ =
        flatbuffers::root::<fb::HolderRecord>(&data).map_err(|e| fb_error("HolderRecord", e))?;
    Ok(DecodedHolder { data })
}

#[inline]
pub fn encode_concurrency_action(action: &ConcurrencyAction) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(128);

    let encoded = match action {
        ConcurrencyAction::EnqueueTask {
            start_time_ms,
            priority,
            job_id,
            attempt_number,
            relative_attempt_number,
            task_group,
        } => {
            let job_id = builder.create_string(job_id);
            let task_group = builder.create_string(task_group);
            let enqueue = fb::ConcurrencyActionEnqueueTask::create(
                &mut builder,
                &fb::ConcurrencyActionEnqueueTaskArgs {
                    start_time_ms: *start_time_ms,
                    priority: *priority,
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    task_group: Some(task_group),
                },
            );
            fb::ConcurrencyAction::create(
                &mut builder,
                &fb::ConcurrencyActionArgs {
                    data_type: fb::ConcurrencyActionData::ConcurrencyActionEnqueueTask,
                    data: Some(enqueue.as_union_value()),
                },
            )
        }
    };

    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedEnqueueTaskAction<'a> {
    pub start_time_ms: i64,
    pub priority: u8,
    pub job_id: &'a str,
    pub attempt_number: u32,
    pub relative_attempt_number: u32,
    pub task_group: &'a str,
}

/// Decoded concurrency action that owns serialized data
#[derive(Clone)]
pub struct DecodedConcurrencyAction {
    data: Vec<u8>,
}

impl DecodedConcurrencyAction {
    fn action(&self) -> fb::ConcurrencyAction<'_> {
        // SAFETY: validated in decode_concurrency_action before construction.
        unsafe { flatbuffers::root_unchecked::<fb::ConcurrencyAction>(&self.data) }
    }

    pub fn enqueue_task(&self) -> Option<DecodedEnqueueTaskAction<'_>> {
        let action = self.action();
        let enqueue = action.data_as_concurrency_action_enqueue_task()?;
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
pub fn decode_concurrency_action(bytes: &[u8]) -> Result<DecodedConcurrencyAction, CodecError> {
    let data = bytes.to_vec();
    let action = flatbuffers::root::<fb::ConcurrencyAction>(&data)
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

    Ok(DecodedConcurrencyAction { data })
}

#[inline]
pub fn encode_job_cancellation(cancellation: &JobCancellation) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(64);
    let encoded = fb::JobCancellation::create(
        &mut builder,
        &fb::JobCancellationArgs {
            cancelled_at_ms: cancellation.cancelled_at_ms,
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded job cancellation that owns serialized data
#[derive(Clone)]
pub struct DecodedJobCancellation {
    data: Vec<u8>,
}

impl DecodedJobCancellation {
    fn cancellation(&self) -> fb::JobCancellation<'_> {
        // SAFETY: validated in decode_job_cancellation before construction.
        unsafe { flatbuffers::root_unchecked::<fb::JobCancellation>(&self.data) }
    }

    pub fn cancelled_at_ms(&self) -> i64 {
        self.cancellation().cancelled_at_ms()
    }
}

#[inline]
pub fn decode_job_cancellation(bytes: &[u8]) -> Result<DecodedJobCancellation, CodecError> {
    let data = bytes.to_vec();
    let _ = flatbuffers::root::<fb::JobCancellation>(&data)
        .map_err(|e| fb_error("JobCancellation", e))?;
    Ok(DecodedJobCancellation { data })
}

#[inline]
pub fn encode_floating_limit_state(state: &FloatingLimitState) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let metadata = create_string_pair_vector(&mut builder, &state.metadata);
    let encoded = fb::FloatingLimitState::create(
        &mut builder,
        &fb::FloatingLimitStateArgs {
            current_max_concurrency: state.current_max_concurrency,
            last_refreshed_at_ms: state.last_refreshed_at_ms,
            refresh_task_scheduled: state.refresh_task_scheduled,
            refresh_interval_ms: state.refresh_interval_ms,
            default_max_concurrency: state.default_max_concurrency,
            retry_count: state.retry_count,
            has_next_retry_at_ms: state.next_retry_at_ms.is_some(),
            next_retry_at_ms: state.next_retry_at_ms.unwrap_or_default(),
            metadata: Some(metadata),
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}

/// Decoded floating limit state that owns serialized data
#[derive(Clone)]
pub struct DecodedFloatingLimitState {
    data: Vec<u8>,
}

impl DecodedFloatingLimitState {
    fn state(&self) -> fb::FloatingLimitState<'_> {
        // SAFETY: validated in decode_floating_limit_state before construction.
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
    let data = bytes.to_vec();
    let _ = flatbuffers::root::<fb::FloatingLimitState>(&data)
        .map_err(|e| fb_error("FloatingLimitState", e))?;
    Ok(DecodedFloatingLimitState { data })
}
