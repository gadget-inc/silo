//! All encode (serialize) functions for silo's internal storage format.

use flatbuffers::FlatBufferBuilder;

use crate::codec::{
    CodecError, build_gubernator_rate_limit_data, create_string_pair_vector, create_string_vector,
    create_string_vector_from_fb, required_str, to_fb_gubernator_algorithm, to_fb_job_status_kind,
};
use crate::flatbuf::silo_internal as fb;
use crate::job::{FloatingLimitState, JobCancellation, JobInfo, JobStatus, Limit};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::retry::RetryPolicy;
use crate::task::{ConcurrencyAction, HolderRecord, LeaseRecord, Task};

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

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

/// Encode a retry `Task::CheckRateLimit` directly from a decoded fb task view.
#[inline]
pub fn encode_check_rate_limit_retry_from_task(
    task: fb::TaskCheckRateLimit<'_>,
    retry_task_id: &str,
    next_retry_count: u32,
) -> Result<Vec<u8>, CodecError> {
    let rate_limit = task
        .rate_limit()
        .ok_or(CodecError::MissingField("task.check_rate_limit.rate_limit"))?;
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let task_id = builder.create_string(retry_task_id);
    let tenant = builder.create_string(task.tenant().unwrap_or_default());
    let job_id = builder.create_string(task.job_id().unwrap_or_default());
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
    let held_queues = create_string_vector_from_fb(&mut builder, task.held_queues());
    let task_group = builder.create_string(task.task_group().unwrap_or_default());
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

// ---------------------------------------------------------------------------
// JobInfo
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// JobStatus
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// JobAttempt
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

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

/// Encode a lease directly from a decoded RunAttempt fb task, avoiding Task materialization.
#[inline]
pub fn encode_lease_from_run_attempt(
    worker_id: &str,
    task: fb::TaskRunAttempt<'_>,
    expiry_ms: i64,
    started_at_ms: i64,
) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(worker_id);
    let id = builder.create_string(task.id().unwrap_or_default());
    let tenant = builder.create_string(task.tenant().unwrap_or_default());
    let job_id = builder.create_string(task.job_id().unwrap_or_default());
    let held_queues = create_string_vector_from_fb(&mut builder, task.held_queues());
    let task_group = builder.create_string(task.task_group().unwrap_or_default());
    let run_attempt = fb::TaskRunAttempt::create(
        &mut builder,
        &fb::TaskRunAttemptArgs {
            id: Some(id),
            tenant: Some(tenant),
            job_id: Some(job_id),
            attempt_number: task.attempt_number(),
            relative_attempt_number: task.relative_attempt_number(),
            held_queues: Some(held_queues),
            task_group: Some(task_group),
        },
    );
    let task = fb::Task::create(
        &mut builder,
        &fb::TaskArgs {
            data_type: fb::TaskData::TaskRunAttempt,
            data: Some(run_attempt.as_union_value()),
        },
    );
    let lease = fb::LeaseRecord::create(
        &mut builder,
        &fb::LeaseRecordArgs {
            worker_id: Some(worker_id),
            task: Some(task),
            expiry_ms,
            started_at_ms,
        },
    );
    builder.finish(lease, None);
    Ok(builder.finished_data().to_vec())
}

/// Encode a granted RequestTicket as a RunAttempt lease directly from decoded fb task bytes.
#[inline]
pub fn encode_lease_from_request_ticket_grant(
    worker_id: &str,
    task: fb::TaskRequestTicket<'_>,
    held_queue: &str,
    expiry_ms: i64,
    started_at_ms: i64,
) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(worker_id);
    let id = builder.create_string(task.request_id().unwrap_or_default());
    let tenant = builder.create_string(task.tenant().unwrap_or_default());
    let job_id = builder.create_string(task.job_id().unwrap_or_default());
    let queue = builder.create_string(held_queue);
    let held_queues = builder.create_vector(&[queue]);
    let task_group = builder.create_string(task.task_group().unwrap_or_default());
    let run_attempt = fb::TaskRunAttempt::create(
        &mut builder,
        &fb::TaskRunAttemptArgs {
            id: Some(id),
            tenant: Some(tenant),
            job_id: Some(job_id),
            attempt_number: task.attempt_number(),
            relative_attempt_number: task.relative_attempt_number(),
            held_queues: Some(held_queues),
            task_group: Some(task_group),
        },
    );
    let task = fb::Task::create(
        &mut builder,
        &fb::TaskArgs {
            data_type: fb::TaskData::TaskRunAttempt,
            data: Some(run_attempt.as_union_value()),
        },
    );
    let lease = fb::LeaseRecord::create(
        &mut builder,
        &fb::LeaseRecordArgs {
            worker_id: Some(worker_id),
            task: Some(task),
            expiry_ms,
            started_at_ms,
        },
    );
    builder.finish(lease, None);
    Ok(builder.finished_data().to_vec())
}

/// Encode a lease directly from a decoded RefreshFloatingLimit fb task, avoiding Task materialization.
#[inline]
pub fn encode_lease_from_refresh_floating_limit(
    worker_id: &str,
    task: fb::TaskRefreshFloatingLimit<'_>,
    expiry_ms: i64,
) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(worker_id);
    let task_id = builder.create_string(task.task_id().unwrap_or_default());
    let tenant = builder.create_string(task.tenant().unwrap_or_default());
    let queue_key = builder.create_string(task.queue_key().unwrap_or_default());
    let metadata = crate::codec::create_string_pair_vector_from_fb(&mut builder, task.metadata());
    let task_group = builder.create_string(task.task_group().unwrap_or_default());
    let refresh = fb::TaskRefreshFloatingLimit::create(
        &mut builder,
        &fb::TaskRefreshFloatingLimitArgs {
            task_id: Some(task_id),
            tenant: Some(tenant),
            queue_key: Some(queue_key),
            current_max_concurrency: task.current_max_concurrency(),
            last_refreshed_at_ms: task.last_refreshed_at_ms(),
            metadata: Some(metadata),
            task_group: Some(task_group),
        },
    );
    let task = fb::Task::create(
        &mut builder,
        &fb::TaskArgs {
            data_type: fb::TaskData::TaskRefreshFloatingLimit,
            data: Some(refresh.as_union_value()),
        },
    );
    let lease = fb::LeaseRecord::create(
        &mut builder,
        &fb::LeaseRecordArgs {
            worker_id: Some(worker_id),
            task: Some(task),
            expiry_ms,
            started_at_ms: 0, // Not applicable for RefreshFloatingLimit tasks
        },
    );
    builder.finish(lease, None);
    Ok(builder.finished_data().to_vec())
}

// ---------------------------------------------------------------------------
// Holder
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ConcurrencyAction
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// JobCancellation
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// FloatingLimitState
// ---------------------------------------------------------------------------

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
