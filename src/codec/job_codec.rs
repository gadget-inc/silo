use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;

use crate::codec::common::{
    CodecError, create_string_pair_vector, fb_error, from_fb_gubernator_algorithm,
    from_fb_job_status_kind, parse_string_pairs, required_str, to_fb_gubernator_algorithm,
    to_fb_job_status_kind,
};
use crate::flatbuf::silo_internal as fb;
use crate::job::{
    ConcurrencyLimit, FloatingConcurrencyLimit, GubernatorRateLimit, JobInfo, JobStatus,
    JobStatusKind, Limit, RateLimitRetryPolicy,
};
use crate::retry::RetryPolicy;

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

#[derive(Clone)]
pub struct DecodedJobInfo {
    data: Bytes,
    limits: Vec<Limit>,
}

impl DecodedJobInfo {
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

    pub fn limits(&self) -> Vec<Limit> {
        self.limits.clone()
    }
}

#[inline]
pub fn decode_job_info(bytes: &[u8]) -> Result<DecodedJobInfo, CodecError> {
    decode_job_info_bytes(Bytes::copy_from_slice(bytes))
}

#[inline]
pub(crate) fn decode_job_info_bytes(data: Bytes) -> Result<DecodedJobInfo, CodecError> {
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
    Ok(DecodedJobInfo {
        data,
        limits: parsed_limits,
    })
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

#[derive(Clone, Copy)]
pub struct DecodedJobStatus<'a> {
    job_status: fb::JobStatus<'a>,
    kind: JobStatusKind,
}

impl DecodedJobStatus<'_> {
    pub fn kind(&self) -> JobStatusKind {
        self.kind
    }

    pub fn changed_at_ms(&self) -> i64 {
        self.job_status.changed_at_ms()
    }

    pub fn next_attempt_starts_after_ms(&self) -> Option<i64> {
        let status = self.job_status;
        if status.has_next_attempt_starts_after_ms() {
            Some(status.next_attempt_starts_after_ms())
        } else {
            None
        }
    }

    pub fn current_attempt(&self) -> Option<u32> {
        let status = self.job_status;
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
pub fn decode_job_status(bytes: &[u8]) -> Result<DecodedJobStatus<'_>, CodecError> {
    let status = flatbuffers::root::<fb::JobStatus>(bytes).map_err(|e| fb_error("JobStatus", e))?;
    let kind = from_fb_job_status_kind(status.kind())?;
    Ok(DecodedJobStatus {
        job_status: status,
        kind,
    })
}
