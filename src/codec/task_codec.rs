use flatbuffers::FlatBufferBuilder;

use crate::codec::common::{
    CodecError, build_gubernator_rate_limit_data, create_string_pair_vector, create_string_vector,
    create_string_vector_from_fb, fb_error, parse_gubernator_rate_limit_data, parse_string_pairs,
    parse_string_values, required_str,
};
use crate::flatbuf::silo_internal as fb;
use crate::task::{GubernatorRateLimitData, Task};

fn parse_task_tenant<'a>(task: fb::Task<'a>) -> Result<&'a str, CodecError> {
    TaskDataView::from_task(task)?.tenant_required()
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TaskDataView<'a> {
    RunAttempt(fb::TaskRunAttempt<'a>),
    RequestTicket(fb::TaskRequestTicket<'a>),
    CheckRateLimit(fb::TaskCheckRateLimit<'a>),
    RefreshFloatingLimit(fb::TaskRefreshFloatingLimit<'a>),
}

impl<'a> TaskDataView<'a> {
    pub(crate) fn from_task(task: fb::Task<'a>) -> Result<Self, CodecError> {
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

    pub(crate) fn kind(&self) -> DecodedTaskKind {
        match self {
            Self::RunAttempt(_) => DecodedTaskKind::RunAttempt,
            Self::RequestTicket(_) => DecodedTaskKind::RequestTicket,
            Self::CheckRateLimit(_) => DecodedTaskKind::CheckRateLimit,
            Self::RefreshFloatingLimit(_) => DecodedTaskKind::RefreshFloatingLimit,
        }
    }

    pub(crate) fn tenant_default(&self) -> &str {
        match self {
            Self::RunAttempt(data) => data.tenant().unwrap_or_default(),
            Self::RequestTicket(data) => data.tenant().unwrap_or_default(),
            Self::CheckRateLimit(data) => data.tenant().unwrap_or_default(),
            Self::RefreshFloatingLimit(data) => data.tenant().unwrap_or_default(),
        }
    }

    fn tenant_required(&self) -> Result<&'a str, CodecError> {
        match self {
            Self::RunAttempt(data) => required_str(data.tenant(), "task.run_attempt.tenant"),
            Self::RequestTicket(data) => required_str(data.tenant(), "task.request_ticket.tenant"),
            Self::CheckRateLimit(data) => {
                required_str(data.tenant(), "task.check_rate_limit.tenant")
            }
            Self::RefreshFloatingLimit(data) => {
                required_str(data.tenant(), "task.refresh_floating_limit.tenant")
            }
        }
    }

    pub(crate) fn run_attempt(&self) -> Option<DecodedRunAttemptTask<'a>> {
        if let Self::RunAttempt(task) = self {
            return Some(DecodedRunAttemptTask { task: *task });
        }
        None
    }

    pub(crate) fn request_ticket(&self) -> Option<DecodedRequestTicketTask<'a>> {
        if let Self::RequestTicket(task) = self {
            return Some(DecodedRequestTicketTask { task: *task });
        }
        None
    }

    pub(crate) fn check_rate_limit(&self) -> Option<DecodedCheckRateLimitTask<'a>> {
        if let Self::CheckRateLimit(task) = self {
            return Some(DecodedCheckRateLimitTask { task: *task });
        }
        None
    }

    pub(crate) fn refresh_floating_limit(&self) -> Option<DecodedRefreshFloatingLimitTask<'a>> {
        if let Self::RefreshFloatingLimit(task) = self {
            return Some(DecodedRefreshFloatingLimitTask { task: *task });
        }
        None
    }

    pub(crate) fn run_attempt_task_id(&self) -> Option<&str> {
        if let Self::RunAttempt(data) = self {
            return data.id();
        }
        None
    }

    pub(crate) fn refresh_info(&self) -> Option<(&str, &str)> {
        if let Self::RefreshFloatingLimit(data) = self {
            return Some((
                data.task_id().unwrap_or_default(),
                data.queue_key().unwrap_or_default(),
            ));
        }
        None
    }

    pub(crate) fn job_id_default(&self) -> &str {
        match self {
            Self::RunAttempt(data) => data.job_id().unwrap_or_default(),
            Self::RequestTicket(data) => data.job_id().unwrap_or_default(),
            Self::CheckRateLimit(data) => data.job_id().unwrap_or_default(),
            Self::RefreshFloatingLimit(_) => "",
        }
    }

    pub(crate) fn attempt_number(&self) -> u32 {
        match self {
            Self::RunAttempt(data) => data.attempt_number(),
            Self::RequestTicket(data) => data.attempt_number(),
            Self::CheckRateLimit(data) => data.attempt_number(),
            Self::RefreshFloatingLimit(_) => 0,
        }
    }

    pub(crate) fn relative_attempt_number(&self) -> u32 {
        match self {
            Self::RunAttempt(data) => data.relative_attempt_number(),
            Self::RequestTicket(data) => data.relative_attempt_number(),
            Self::CheckRateLimit(data) => data.relative_attempt_number(),
            Self::RefreshFloatingLimit(_) => 0,
        }
    }

    pub(crate) fn held_queues(&self) -> Vec<String> {
        match self {
            Self::RunAttempt(data) => parse_string_values(data.held_queues()),
            Self::CheckRateLimit(data) => parse_string_values(data.held_queues()),
            Self::RequestTicket(_) | Self::RefreshFloatingLimit(_) => Vec::new(),
        }
    }

    pub(crate) fn task_group_default(&self) -> &str {
        match self {
            Self::RunAttempt(data) => data.task_group().unwrap_or_default(),
            Self::RequestTicket(data) => data.task_group().unwrap_or_default(),
            Self::CheckRateLimit(data) => data.task_group().unwrap_or_default(),
            Self::RefreshFloatingLimit(data) => data.task_group().unwrap_or_default(),
        }
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
        parse_string_values(self.task.held_queues())
    }

    pub(crate) fn held_queues_vector(
        &self,
    ) -> Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<&str>>> {
        self.task.held_queues()
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
        parse_string_values(self.task.held_queues())
    }

    pub(crate) fn held_queues_vector(
        &self,
    ) -> Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<&str>>> {
        self.task.held_queues()
    }

    pub(crate) fn rate_limit_fb(&self) -> Option<fb::GubernatorRateLimitData<'_>> {
        self.task.rate_limit()
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

#[derive(Debug, Clone, Copy)]
pub struct DecodedTask<'a> {
    task: TaskDataView<'a>,
}

impl<'a> DecodedTask<'a> {
    fn new(task: fb::Task<'a>) -> Result<Self, CodecError> {
        let task = TaskDataView::from_task(task)?;
        Ok(Self { task })
    }

    pub fn kind(&self) -> DecodedTaskKind {
        self.task.kind()
    }

    pub fn tenant(&self) -> &str {
        self.task.tenant_default()
    }

    pub fn run_attempt(&self) -> Option<DecodedRunAttemptTask<'a>> {
        self.task.run_attempt()
    }

    pub fn request_ticket(&self) -> Option<DecodedRequestTicketTask<'a>> {
        self.task.request_ticket()
    }

    pub fn check_rate_limit(&self) -> Option<DecodedCheckRateLimitTask<'a>> {
        self.task.check_rate_limit()
    }

    pub fn refresh_floating_limit(&self) -> Option<DecodedRefreshFloatingLimitTask<'a>> {
        self.task.refresh_floating_limit()
    }

    pub fn to_owned(&self) -> Result<Task, CodecError> {
        parse_task(self.task)
    }
}

pub(crate) fn parse_task(task: TaskDataView<'_>) -> Result<Task, CodecError> {
    match task {
        TaskDataView::RunAttempt(data) => Ok(Task::RunAttempt {
            id: required_str(data.id(), "task.run_attempt.id")?.to_string(),
            tenant: required_str(data.tenant(), "task.run_attempt.tenant")?.to_string(),
            job_id: required_str(data.job_id(), "task.run_attempt.job_id")?.to_string(),
            attempt_number: data.attempt_number(),
            relative_attempt_number: data.relative_attempt_number(),
            held_queues: parse_string_values(data.held_queues()),
            task_group: required_str(data.task_group(), "task.run_attempt.task_group")?.to_string(),
        }),
        TaskDataView::RequestTicket(data) => Ok(Task::RequestTicket {
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
        TaskDataView::CheckRateLimit(data) => {
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
        TaskDataView::RefreshFloatingLimit(data) => Ok(Task::RefreshFloatingLimit {
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

pub(crate) fn build_task<'a>(
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
pub fn decode_task(bytes: &[u8]) -> Result<Task, CodecError> {
    decode_task_view(bytes)?.to_owned()
}

#[inline]
pub fn validate_task(bytes: &[u8]) -> Result<(), CodecError> {
    let _ = decode_task(bytes)?;
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

/// Encode a retry `Task::CheckRateLimit` directly from a decoded task view.
#[inline]
pub fn encode_check_rate_limit_retry_from_task(
    task: DecodedCheckRateLimitTask<'_>,
    retry_task_id: &str,
    next_retry_count: u32,
) -> Result<Vec<u8>, CodecError> {
    let rate_limit = task
        .rate_limit_fb()
        .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let task_id = builder.create_string(retry_task_id);
    let tenant = builder.create_string(task.tenant());
    let job_id = builder.create_string(task.job_id());
    let rate_limit_name = builder.create_string(required_str(
        rate_limit.name(),
        "task.check_rate_limit.rate_limit.name",
    )?);
    let rate_limit_unique_key = builder.create_string(required_str(
        rate_limit.unique_key(),
        "task.check_rate_limit.rate_limit.unique_key",
    )?);
    let rate_limit = fb::GubernatorRateLimitData::create(
        &mut builder,
        &fb::GubernatorRateLimitDataArgs {
            name: Some(rate_limit_name),
            unique_key: Some(rate_limit_unique_key),
            limit: rate_limit.limit(),
            duration_ms: rate_limit.duration_ms(),
            hits: rate_limit.hits(),
            algorithm: rate_limit.algorithm(),
            behavior: rate_limit.behavior(),
            retry_initial_backoff_ms: rate_limit.retry_initial_backoff_ms(),
            retry_max_backoff_ms: rate_limit.retry_max_backoff_ms(),
            retry_backoff_multiplier: rate_limit.retry_backoff_multiplier(),
            retry_max_retries: rate_limit.retry_max_retries(),
        },
    );
    let held_queues = create_string_vector_from_fb(&mut builder, task.held_queues_vector());
    let task_group = builder.create_string(task.task_group());
    let check_rate_limit = fb::TaskCheckRateLimit::create(
        &mut builder,
        &fb::TaskCheckRateLimitArgs {
            task_id: Some(task_id),
            tenant: Some(tenant),
            job_id: Some(job_id),
            attempt_number: task.attempt_number(),
            relative_attempt_number: task.relative_attempt_number(),
            limit_index: task.limit_index(),
            rate_limit: Some(rate_limit),
            retry_count: next_retry_count,
            started_at_ms: task.started_at_ms(),
            priority: task.priority(),
            held_queues: Some(held_queues),
            task_group: Some(task_group),
        },
    );
    let encoded = fb::Task::create(
        &mut builder,
        &fb::TaskArgs {
            data_type: fb::TaskData::TaskCheckRateLimit,
            data: Some(check_rate_limit.as_union_value()),
        },
    );
    builder.finish(encoded, None);
    Ok(builder.finished_data().to_vec())
}
