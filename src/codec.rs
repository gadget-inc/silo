use flatbuffers::FlatBufferBuilder;

use crate::fb::silo::fb;
use crate::job::{FloatingLimitState, JobCancellation, JobInfo, JobStatus, JobStatusKind, Limit};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::task::{ConcurrencyAction, GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

/// Error type for codec operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum CodecError {
    #[error("flatbuffer error: {0}")]
    Flatbuffer(String),
}

impl From<CodecError> for String {
    fn from(e: CodecError) -> String {
        e.to_string()
    }
}

// ---------------------------------------------------------------------------
// Encode helpers
// ---------------------------------------------------------------------------

/// Build a Task union (TaskVariant) into the builder. Returns (type, offset).
fn build_task_union<'a, A: flatbuffers::Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    task: &Task,
) -> (
    fb::TaskVariant,
    flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
) {
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
            let hq: Vec<_> = held_queues
                .iter()
                .map(|s| builder.create_string(s))
                .collect();
            let held_queues = builder.create_vector(&hq);
            let task_group = builder.create_string(task_group);
            let ra = fb::RunAttempt::create(
                builder,
                &fb::RunAttemptArgs {
                    id: Some(id),
                    tenant: Some(tenant),
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    held_queues: Some(held_queues),
                    task_group: Some(task_group),
                },
            );
            (fb::TaskVariant::RunAttempt, ra.as_union_value())
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
            let rt = fb::RequestTicket::create(
                builder,
                &fb::RequestTicketArgs {
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
            (fb::TaskVariant::RequestTicket, rt.as_union_value())
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
            let task_id_s = builder.create_string(task_id);
            let tenant = builder.create_string(tenant);
            let job_id = builder.create_string(job_id);
            let rl_name = builder.create_string(&rate_limit.name);
            let rl_unique_key = builder.create_string(&rate_limit.unique_key);
            let rl = fb::GubernatorRateLimitData::create(
                builder,
                &fb::GubernatorRateLimitDataArgs {
                    name: Some(rl_name),
                    unique_key: Some(rl_unique_key),
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
            );
            let hq: Vec<_> = held_queues
                .iter()
                .map(|s| builder.create_string(s))
                .collect();
            let held_queues = builder.create_vector(&hq);
            let task_group = builder.create_string(task_group);
            let crl = fb::CheckRateLimit::create(
                builder,
                &fb::CheckRateLimitArgs {
                    task_id: Some(task_id_s),
                    tenant: Some(tenant),
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    limit_index: *limit_index,
                    rate_limit: Some(rl),
                    retry_count: *retry_count,
                    started_at_ms: *started_at_ms,
                    priority: *priority,
                    held_queues: Some(held_queues),
                    task_group: Some(task_group),
                },
            );
            (fb::TaskVariant::CheckRateLimit, crl.as_union_value())
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
            let task_id_s = builder.create_string(task_id);
            let tenant = builder.create_string(tenant);
            let queue_key = builder.create_string(queue_key);
            let md: Vec<_> = metadata
                .iter()
                .map(|(k, v)| {
                    let k = builder.create_string(k);
                    let v = builder.create_string(v);
                    fb::KeyValuePair::create(
                        builder,
                        &fb::KeyValuePairArgs {
                            key: Some(k),
                            value: Some(v),
                        },
                    )
                })
                .collect();
            let metadata = builder.create_vector(&md);
            let task_group = builder.create_string(task_group);
            let rfl = fb::RefreshFloatingLimit::create(
                builder,
                &fb::RefreshFloatingLimitArgs {
                    task_id: Some(task_id_s),
                    tenant: Some(tenant),
                    queue_key: Some(queue_key),
                    current_max_concurrency: *current_max_concurrency,
                    last_refreshed_at_ms: *last_refreshed_at_ms,
                    metadata: Some(metadata),
                    task_group: Some(task_group),
                },
            );
            (fb::TaskVariant::RefreshFloatingLimit, rfl.as_union_value())
        }
    }
}

// ---------------------------------------------------------------------------
// Encode functions
// ---------------------------------------------------------------------------

#[inline]
pub fn encode_task(task: &Task) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let (vtype, voff) = build_task_union(&mut builder, task);
    let root = fb::Task::create(
        &mut builder,
        &fb::TaskArgs {
            variant_type: vtype,
            variant: Some(voff),
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_lease(record: &LeaseRecord) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(&record.worker_id);
    let (vtype, voff) = build_task_union(&mut builder, &record.task);
    let root = fb::LeaseRecord::create(
        &mut builder,
        &fb::LeaseRecordArgs {
            worker_id: Some(worker_id),
            task_type: vtype,
            task: Some(voff),
            expiry_ms: record.expiry_ms,
            started_at_ms: record.started_at_ms,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_attempt(attempt: &JobAttempt) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let job_id = builder.create_string(&attempt.job_id);
    let task_id = builder.create_string(&attempt.task_id);

    let (status_kind, finished_at_ms, result, error_code, error) = match &attempt.status {
        AttemptStatus::Running => (fb::AttemptStatusKind::Running, None, None, None, None),
        AttemptStatus::Succeeded {
            finished_at_ms,
            result,
        } => {
            let r = builder.create_vector(result);
            (
                fb::AttemptStatusKind::Succeeded,
                Some(*finished_at_ms),
                Some(r),
                None,
                None,
            )
        }
        AttemptStatus::Failed {
            finished_at_ms,
            error_code,
            error,
        } => {
            let ec = builder.create_string(error_code);
            let e = builder.create_vector(error);
            (
                fb::AttemptStatusKind::Failed,
                Some(*finished_at_ms),
                None,
                Some(ec),
                Some(e),
            )
        }
        AttemptStatus::Cancelled { finished_at_ms } => (
            fb::AttemptStatusKind::Cancelled,
            Some(*finished_at_ms),
            None,
            None,
            None,
        ),
    };

    let root = fb::JobAttempt::create(
        &mut builder,
        &fb::JobAttemptArgs {
            job_id: Some(job_id),
            attempt_number: attempt.attempt_number,
            relative_attempt_number: attempt.relative_attempt_number,
            task_id: Some(task_id),
            started_at_ms: attempt.started_at_ms,
            status_kind,
            finished_at_ms,
            result,
            error_code,
            error,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_job_info(job: &JobInfo) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(512);
    let id = builder.create_string(&job.id);
    let payload = builder.create_vector(&job.payload);
    let task_group = builder.create_string(&job.task_group);

    let retry_policy = job.retry_policy.as_ref().map(|rp| {
        fb::RetryPolicy::create(
            &mut builder,
            &fb::RetryPolicyArgs {
                retry_count: rp.retry_count,
                initial_interval_ms: rp.initial_interval_ms,
                max_interval_ms: rp.max_interval_ms,
                randomize_interval: rp.randomize_interval,
                backoff_factor: rp.backoff_factor,
            },
        )
    });

    let md: Vec<_> = job
        .metadata
        .iter()
        .map(|(k, v)| {
            let k = builder.create_string(k);
            let v = builder.create_string(v);
            fb::KeyValuePair::create(
                &mut builder,
                &fb::KeyValuePairArgs {
                    key: Some(k),
                    value: Some(v),
                },
            )
        })
        .collect();
    let metadata = builder.create_vector(&md);

    let limits: Vec<_> = job
        .limits
        .iter()
        .map(|lim| {
            let (vtype, voff) = match lim {
                Limit::Concurrency(cl) => {
                    let key = builder.create_string(&cl.key);
                    let entry = fb::ConcurrencyLimitEntry::create(
                        &mut builder,
                        &fb::ConcurrencyLimitEntryArgs {
                            key: Some(key),
                            max_concurrency: cl.max_concurrency,
                        },
                    );
                    (
                        fb::LimitVariant::ConcurrencyLimitEntry,
                        entry.as_union_value(),
                    )
                }
                Limit::RateLimit(rl) => {
                    let name = builder.create_string(&rl.name);
                    let unique_key = builder.create_string(&rl.unique_key);
                    let rp = fb::RateLimitRetryPolicy::create(
                        &mut builder,
                        &fb::RateLimitRetryPolicyArgs {
                            initial_backoff_ms: rl.retry_policy.initial_backoff_ms,
                            max_backoff_ms: rl.retry_policy.max_backoff_ms,
                            backoff_multiplier: rl.retry_policy.backoff_multiplier,
                            max_retries: rl.retry_policy.max_retries,
                        },
                    );
                    let entry = fb::RateLimitEntry::create(
                        &mut builder,
                        &fb::RateLimitEntryArgs {
                            name: Some(name),
                            unique_key: Some(unique_key),
                            limit: rl.limit,
                            duration_ms: rl.duration_ms,
                            hits: rl.hits,
                            algorithm: rl.algorithm.as_u8(),
                            behavior: rl.behavior,
                            retry_policy: Some(rp),
                        },
                    );
                    (fb::LimitVariant::RateLimitEntry, entry.as_union_value())
                }
                Limit::FloatingConcurrency(fl) => {
                    let key = builder.create_string(&fl.key);
                    let md: Vec<_> = fl
                        .metadata
                        .iter()
                        .map(|(k, v)| {
                            let k = builder.create_string(k);
                            let v = builder.create_string(v);
                            fb::KeyValuePair::create(
                                &mut builder,
                                &fb::KeyValuePairArgs {
                                    key: Some(k),
                                    value: Some(v),
                                },
                            )
                        })
                        .collect();
                    let metadata = builder.create_vector(&md);
                    let entry = fb::FloatingConcurrencyLimitEntry::create(
                        &mut builder,
                        &fb::FloatingConcurrencyLimitEntryArgs {
                            key: Some(key),
                            default_max_concurrency: fl.default_max_concurrency,
                            refresh_interval_ms: fl.refresh_interval_ms,
                            metadata: Some(metadata),
                        },
                    );
                    (
                        fb::LimitVariant::FloatingConcurrencyLimitEntry,
                        entry.as_union_value(),
                    )
                }
            };
            fb::LimitEntry::create(
                &mut builder,
                &fb::LimitEntryArgs {
                    variant_type: vtype,
                    variant: Some(voff),
                },
            )
        })
        .collect();
    let limits = builder.create_vector(&limits);

    let root = fb::JobInfo::create(
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
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_job_status(status: &JobStatus) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(64);
    let kind = match status.kind {
        JobStatusKind::Scheduled => fb::JobStatusKind::Scheduled,
        JobStatusKind::Running => fb::JobStatusKind::Running,
        JobStatusKind::Failed => fb::JobStatusKind::Failed,
        JobStatusKind::Cancelled => fb::JobStatusKind::Cancelled,
        JobStatusKind::Succeeded => fb::JobStatusKind::Succeeded,
    };
    let root = fb::JobStatus::create(
        &mut builder,
        &fb::JobStatusArgs {
            kind,
            changed_at_ms: status.changed_at_ms,
            next_attempt_starts_after_ms: status.next_attempt_starts_after_ms,
            current_attempt: status.current_attempt,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_holder(holder: &HolderRecord) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(32);
    let root = fb::HolderRecord::create(
        &mut builder,
        &fb::HolderRecordArgs {
            granted_at_ms: holder.granted_at_ms,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_concurrency_action(action: &ConcurrencyAction) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(128);
    match action {
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
            let et = fb::EnqueueTask::create(
                &mut builder,
                &fb::EnqueueTaskArgs {
                    start_time_ms: *start_time_ms,
                    priority: *priority,
                    job_id: Some(job_id),
                    attempt_number: *attempt_number,
                    relative_attempt_number: *relative_attempt_number,
                    task_group: Some(task_group),
                },
            );
            let root = fb::ConcurrencyAction::create(
                &mut builder,
                &fb::ConcurrencyActionArgs {
                    variant_type: fb::ConcurrencyActionVariant::EnqueueTask,
                    variant: Some(et.as_union_value()),
                },
            );
            builder.finish(root, None);
        }
    }
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_job_cancellation(cancellation: &JobCancellation) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(32);
    let root = fb::JobCancellation::create(
        &mut builder,
        &fb::JobCancellationArgs {
            cancelled_at_ms: cancellation.cancelled_at_ms,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

#[inline]
pub fn encode_floating_limit_state(state: &FloatingLimitState) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(128);
    let md: Vec<_> = state
        .metadata
        .iter()
        .map(|(k, v)| {
            let k = builder.create_string(k);
            let v = builder.create_string(v);
            fb::KeyValuePair::create(
                &mut builder,
                &fb::KeyValuePairArgs {
                    key: Some(k),
                    value: Some(v),
                },
            )
        })
        .collect();
    let metadata = builder.create_vector(&md);
    let root = fb::FloatingLimitState::create(
        &mut builder,
        &fb::FloatingLimitStateArgs {
            current_max_concurrency: state.current_max_concurrency,
            last_refreshed_at_ms: state.last_refreshed_at_ms,
            refresh_task_scheduled: state.refresh_task_scheduled,
            refresh_interval_ms: state.refresh_interval_ms,
            default_max_concurrency: state.default_max_concurrency,
            retry_count: state.retry_count,
            next_retry_at_ms: state.next_retry_at_ms,
            metadata: Some(metadata),
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

// ---------------------------------------------------------------------------
// Decode helpers - materialize a Task from a FlatBuffer TaskVariant union
// ---------------------------------------------------------------------------

fn task_from_fb_variant(
    vtype: fb::TaskVariant,
    table: flatbuffers::Table<'_>,
) -> Result<Task, CodecError> {
    match vtype {
        fb::TaskVariant::RunAttempt => {
            // SAFETY: union type was verified by flatbuffers
            let ra = unsafe { fb::RunAttempt::init_from_table(table) };
            Ok(Task::RunAttempt {
                id: ra.id().unwrap_or_default().to_string(),
                tenant: ra.tenant().unwrap_or_default().to_string(),
                job_id: ra.job_id().unwrap_or_default().to_string(),
                attempt_number: ra.attempt_number(),
                relative_attempt_number: ra.relative_attempt_number(),
                held_queues: ra
                    .held_queues()
                    .map(|v| v.iter().map(|s| s.to_string()).collect())
                    .unwrap_or_default(),
                task_group: ra.task_group().unwrap_or_default().to_string(),
            })
        }
        fb::TaskVariant::RequestTicket => {
            let rt = unsafe { fb::RequestTicket::init_from_table(table) };
            Ok(Task::RequestTicket {
                queue: rt.queue().unwrap_or_default().to_string(),
                start_time_ms: rt.start_time_ms(),
                priority: rt.priority(),
                tenant: rt.tenant().unwrap_or_default().to_string(),
                job_id: rt.job_id().unwrap_or_default().to_string(),
                attempt_number: rt.attempt_number(),
                relative_attempt_number: rt.relative_attempt_number(),
                request_id: rt.request_id().unwrap_or_default().to_string(),
                task_group: rt.task_group().unwrap_or_default().to_string(),
            })
        }
        fb::TaskVariant::CheckRateLimit => {
            let crl = unsafe { fb::CheckRateLimit::init_from_table(table) };
            let rl = crl.rate_limit();
            let rate_limit = match rl {
                Some(rl) => GubernatorRateLimitData {
                    name: rl.name().unwrap_or_default().to_string(),
                    unique_key: rl.unique_key().unwrap_or_default().to_string(),
                    limit: rl.limit(),
                    duration_ms: rl.duration_ms(),
                    hits: rl.hits(),
                    algorithm: rl.algorithm(),
                    behavior: rl.behavior(),
                    retry_initial_backoff_ms: rl.retry_initial_backoff_ms(),
                    retry_max_backoff_ms: rl.retry_max_backoff_ms(),
                    retry_backoff_multiplier: rl.retry_backoff_multiplier(),
                    retry_max_retries: rl.retry_max_retries(),
                },
                None => {
                    return Err(CodecError::Flatbuffer(
                        "missing rate_limit in CheckRateLimit".to_string(),
                    ));
                }
            };
            Ok(Task::CheckRateLimit {
                task_id: crl.task_id().unwrap_or_default().to_string(),
                tenant: crl.tenant().unwrap_or_default().to_string(),
                job_id: crl.job_id().unwrap_or_default().to_string(),
                attempt_number: crl.attempt_number(),
                relative_attempt_number: crl.relative_attempt_number(),
                limit_index: crl.limit_index(),
                rate_limit,
                retry_count: crl.retry_count(),
                started_at_ms: crl.started_at_ms(),
                priority: crl.priority(),
                held_queues: crl
                    .held_queues()
                    .map(|v| v.iter().map(|s| s.to_string()).collect())
                    .unwrap_or_default(),
                task_group: crl.task_group().unwrap_or_default().to_string(),
            })
        }
        fb::TaskVariant::RefreshFloatingLimit => {
            let rfl = unsafe { fb::RefreshFloatingLimit::init_from_table(table) };
            Ok(Task::RefreshFloatingLimit {
                task_id: rfl.task_id().unwrap_or_default().to_string(),
                tenant: rfl.tenant().unwrap_or_default().to_string(),
                queue_key: rfl.queue_key().unwrap_or_default().to_string(),
                current_max_concurrency: rfl.current_max_concurrency(),
                last_refreshed_at_ms: rfl.last_refreshed_at_ms(),
                metadata: rfl
                    .metadata()
                    .map(|v| {
                        v.iter()
                            .map(|kv| {
                                (
                                    kv.key().unwrap_or_default().to_string(),
                                    kv.value().unwrap_or_default().to_string(),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                task_group: rfl.task_group().unwrap_or_default().to_string(),
            })
        }
        _ => Err(CodecError::Flatbuffer(format!(
            "unknown task variant type: {:?}",
            vtype
        ))),
    }
}

// ---------------------------------------------------------------------------
// Decode: Task (fully materialized)
// ---------------------------------------------------------------------------

#[inline]
pub fn decode_task(bytes: &[u8]) -> Result<Task, CodecError> {
    let t =
        flatbuffers::root::<fb::Task>(bytes).map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    let vtype = t.variant_type();
    let table = t
        .variant()
        .ok_or_else(|| CodecError::Flatbuffer("missing task variant".to_string()))?;
    task_from_fb_variant(vtype, table)
}

// ---------------------------------------------------------------------------
// Decode: Lease (zero-copy wrapper)
// ---------------------------------------------------------------------------

/// Decoded lease record that owns its data for zero-copy access.
#[derive(Clone)]
pub struct DecodedLease {
    data: Vec<u8>,
}

impl DecodedLease {
    fn fb(&self) -> fb::LeaseRecord<'_> {
        // SAFETY: data was validated at construction in decode_lease
        unsafe { flatbuffers::root_unchecked::<fb::LeaseRecord>(&self.data) }
    }

    pub fn worker_id(&self) -> &str {
        self.fb().worker_id().unwrap_or_default()
    }

    pub fn expiry_ms(&self) -> i64 {
        self.fb().expiry_ms()
    }

    pub fn started_at_ms(&self) -> i64 {
        self.fb().started_at_ms()
    }

    pub fn tenant(&self) -> &str {
        let lr = self.fb();
        let vtype = lr.task_type();
        match vtype {
            fb::TaskVariant::RunAttempt => lr
                .task_as_run_attempt()
                .and_then(|r| r.tenant())
                .unwrap_or_default(),
            fb::TaskVariant::RequestTicket => lr
                .task_as_request_ticket()
                .and_then(|r| r.tenant())
                .unwrap_or_default(),
            fb::TaskVariant::CheckRateLimit => lr
                .task_as_check_rate_limit()
                .and_then(|r| r.tenant())
                .unwrap_or_default(),
            fb::TaskVariant::RefreshFloatingLimit => lr
                .task_as_refresh_floating_limit()
                .and_then(|r| r.tenant())
                .unwrap_or_default(),
            _ => "",
        }
    }

    pub fn job_id(&self) -> &str {
        let lr = self.fb();
        match lr.task_type() {
            fb::TaskVariant::RunAttempt => lr
                .task_as_run_attempt()
                .and_then(|r| r.job_id())
                .unwrap_or_default(),
            fb::TaskVariant::RequestTicket => lr
                .task_as_request_ticket()
                .and_then(|r| r.job_id())
                .unwrap_or_default(),
            fb::TaskVariant::CheckRateLimit => lr
                .task_as_check_rate_limit()
                .and_then(|r| r.job_id())
                .unwrap_or_default(),
            fb::TaskVariant::RefreshFloatingLimit => "",
            _ => "",
        }
    }

    pub fn attempt_number(&self) -> u32 {
        let lr = self.fb();
        match lr.task_type() {
            fb::TaskVariant::RunAttempt => lr
                .task_as_run_attempt()
                .map(|r| r.attempt_number())
                .unwrap_or(0),
            fb::TaskVariant::RequestTicket => lr
                .task_as_request_ticket()
                .map(|r| r.attempt_number())
                .unwrap_or(0),
            fb::TaskVariant::CheckRateLimit => lr
                .task_as_check_rate_limit()
                .map(|r| r.attempt_number())
                .unwrap_or(0),
            _ => 0,
        }
    }

    pub fn relative_attempt_number(&self) -> u32 {
        let lr = self.fb();
        match lr.task_type() {
            fb::TaskVariant::RunAttempt => lr
                .task_as_run_attempt()
                .map(|r| r.relative_attempt_number())
                .unwrap_or(0),
            fb::TaskVariant::RequestTicket => lr
                .task_as_request_ticket()
                .map(|r| r.relative_attempt_number())
                .unwrap_or(0),
            fb::TaskVariant::CheckRateLimit => lr
                .task_as_check_rate_limit()
                .map(|r| r.relative_attempt_number())
                .unwrap_or(0),
            _ => 0,
        }
    }

    pub fn task_id(&self) -> Option<&str> {
        let lr = self.fb();
        match lr.task_type() {
            fb::TaskVariant::RunAttempt => lr.task_as_run_attempt().and_then(|r| r.id()),
            _ => None,
        }
    }

    pub fn refresh_floating_limit_info(&self) -> Option<(&str, &str)> {
        let lr = self.fb();
        if lr.task_type() == fb::TaskVariant::RefreshFloatingLimit {
            let rfl = lr.task_as_refresh_floating_limit()?;
            Some((rfl.task_id()?, rfl.queue_key()?))
        } else {
            None
        }
    }

    pub fn held_queues(&self) -> Vec<String> {
        let lr = self.fb();
        match lr.task_type() {
            fb::TaskVariant::RunAttempt => lr
                .task_as_run_attempt()
                .and_then(|r| r.held_queues())
                .map(|v| v.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
            fb::TaskVariant::CheckRateLimit => lr
                .task_as_check_rate_limit()
                .and_then(|r| r.held_queues())
                .map(|v| v.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    pub fn task_group(&self) -> &str {
        let lr = self.fb();
        match lr.task_type() {
            fb::TaskVariant::RunAttempt => lr
                .task_as_run_attempt()
                .and_then(|r| r.task_group())
                .unwrap_or_default(),
            fb::TaskVariant::RequestTicket => lr
                .task_as_request_ticket()
                .and_then(|r| r.task_group())
                .unwrap_or_default(),
            fb::TaskVariant::CheckRateLimit => lr
                .task_as_check_rate_limit()
                .and_then(|r| r.task_group())
                .unwrap_or_default(),
            fb::TaskVariant::RefreshFloatingLimit => lr
                .task_as_refresh_floating_limit()
                .and_then(|r| r.task_group())
                .unwrap_or_default(),
            _ => "",
        }
    }

    /// Convert to an owned Task
    pub fn to_task(&self) -> Task {
        let lr = self.fb();
        let vtype = lr.task_type();
        let table = lr.task().expect("validated at decode");
        task_from_fb_variant(vtype, table).expect("validated at decode")
    }
}

#[inline]
pub fn decode_lease(bytes: &[u8]) -> Result<DecodedLease, CodecError> {
    // Validate
    flatbuffers::root::<fb::LeaseRecord>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(DecodedLease {
        data: bytes.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Decode: JobAttempt (zero-copy wrapper)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DecodedAttempt {
    data: Vec<u8>,
}

impl DecodedAttempt {
    pub fn fb(&self) -> fb::JobAttempt<'_> {
        unsafe { flatbuffers::root_unchecked::<fb::JobAttempt>(&self.data) }
    }
}

#[inline]
pub fn decode_attempt(bytes: &[u8]) -> Result<DecodedAttempt, CodecError> {
    flatbuffers::root::<fb::JobAttempt>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(DecodedAttempt {
        data: bytes.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Decode: JobInfo (zero-copy wrapper)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DecodedJobInfo {
    data: Vec<u8>,
}

impl DecodedJobInfo {
    pub fn fb(&self) -> fb::JobInfo<'_> {
        unsafe { flatbuffers::root_unchecked::<fb::JobInfo>(&self.data) }
    }
}

#[inline]
pub fn decode_job_info(bytes: &[u8]) -> Result<DecodedJobInfo, CodecError> {
    flatbuffers::root::<fb::JobInfo>(bytes).map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(DecodedJobInfo {
        data: bytes.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Decode: JobStatus (directly to owned)
// ---------------------------------------------------------------------------

#[inline]
pub fn decode_job_status_owned(bytes: &[u8]) -> Result<JobStatus, CodecError> {
    let s = flatbuffers::root::<fb::JobStatus>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    let kind = match s.kind() {
        fb::JobStatusKind::Scheduled => JobStatusKind::Scheduled,
        fb::JobStatusKind::Running => JobStatusKind::Running,
        fb::JobStatusKind::Failed => JobStatusKind::Failed,
        fb::JobStatusKind::Cancelled => JobStatusKind::Cancelled,
        fb::JobStatusKind::Succeeded => JobStatusKind::Succeeded,
        _ => {
            return Err(CodecError::Flatbuffer(format!(
                "unknown status kind: {:?}",
                s.kind()
            )));
        }
    };
    Ok(JobStatus {
        kind,
        changed_at_ms: s.changed_at_ms(),
        next_attempt_starts_after_ms: s.next_attempt_starts_after_ms(),
        current_attempt: s.current_attempt(),
    })
}

// ---------------------------------------------------------------------------
// Decode: HolderRecord (direct scalar extraction)
// ---------------------------------------------------------------------------

#[inline]
pub fn decode_holder_granted_at_ms(bytes: &[u8]) -> Result<i64, CodecError> {
    let h = flatbuffers::root::<fb::HolderRecord>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(h.granted_at_ms())
}

// ---------------------------------------------------------------------------
// Decode: ConcurrencyAction (zero-copy wrapper)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DecodedConcurrencyAction {
    data: Vec<u8>,
}

impl DecodedConcurrencyAction {
    pub fn fb(&self) -> fb::ConcurrencyAction<'_> {
        unsafe { flatbuffers::root_unchecked::<fb::ConcurrencyAction>(&self.data) }
    }
}

#[inline]
pub fn decode_concurrency_action(bytes: &[u8]) -> Result<DecodedConcurrencyAction, CodecError> {
    flatbuffers::root::<fb::ConcurrencyAction>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(DecodedConcurrencyAction {
        data: bytes.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Decode: JobCancellation (direct scalar extraction)
// ---------------------------------------------------------------------------

#[inline]
pub fn decode_cancellation_at_ms(bytes: &[u8]) -> Result<i64, CodecError> {
    let c = flatbuffers::root::<fb::JobCancellation>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(c.cancelled_at_ms())
}

// ---------------------------------------------------------------------------
// Decode: FloatingLimitState (zero-copy wrapper with field accessors)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DecodedFloatingLimitState {
    data: Vec<u8>,
}

impl DecodedFloatingLimitState {
    fn fb(&self) -> fb::FloatingLimitState<'_> {
        unsafe { flatbuffers::root_unchecked::<fb::FloatingLimitState>(&self.data) }
    }

    pub fn current_max_concurrency(&self) -> u32 {
        self.fb().current_max_concurrency()
    }

    pub fn last_refreshed_at_ms(&self) -> i64 {
        self.fb().last_refreshed_at_ms()
    }

    pub fn refresh_task_scheduled(&self) -> bool {
        self.fb().refresh_task_scheduled()
    }

    pub fn refresh_interval_ms(&self) -> i64 {
        self.fb().refresh_interval_ms()
    }

    pub fn default_max_concurrency(&self) -> u32 {
        self.fb().default_max_concurrency()
    }

    pub fn retry_count(&self) -> u32 {
        self.fb().retry_count()
    }

    pub fn next_retry_at_ms(&self) -> Option<i64> {
        self.fb().next_retry_at_ms()
    }

    pub fn metadata(&self) -> Vec<(String, String)> {
        self.fb()
            .metadata()
            .map(|v| {
                v.iter()
                    .map(|kv| {
                        (
                            kv.key().unwrap_or_default().to_string(),
                            kv.value().unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Materialize an owned FloatingLimitState from the FlatBuffer data.
    pub fn to_owned(&self) -> FloatingLimitState {
        let f = self.fb();
        FloatingLimitState {
            current_max_concurrency: f.current_max_concurrency(),
            last_refreshed_at_ms: f.last_refreshed_at_ms(),
            refresh_task_scheduled: f.refresh_task_scheduled(),
            refresh_interval_ms: f.refresh_interval_ms(),
            default_max_concurrency: f.default_max_concurrency(),
            retry_count: f.retry_count(),
            next_retry_at_ms: f.next_retry_at_ms(),
            metadata: self.metadata(),
        }
    }
}

#[inline]
pub fn decode_floating_limit_state(bytes: &[u8]) -> Result<DecodedFloatingLimitState, CodecError> {
    flatbuffers::root::<fb::FloatingLimitState>(bytes)
        .map_err(|e| CodecError::Flatbuffer(e.to_string()))?;
    Ok(DecodedFloatingLimitState {
        data: bytes.to_vec(),
    })
}
