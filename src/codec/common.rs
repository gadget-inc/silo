use flatbuffers::FlatBufferBuilder;

use crate::flatbuf::silo_internal as fb;
use crate::job::{GubernatorAlgorithm, JobStatusKind};
use crate::task::GubernatorRateLimitData;

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

pub(crate) fn fb_error(context: &'static str, err: flatbuffers::InvalidFlatbuffer) -> CodecError {
    CodecError::Flatbuffer(format!("{}: {}", context, err))
}

pub(crate) fn required_str<'a>(
    value: Option<&'a str>,
    field: &'static str,
) -> Result<&'a str, CodecError> {
    value.ok_or(CodecError::MissingField(field))
}

pub(crate) fn to_fb_gubernator_algorithm(alg: GubernatorAlgorithm) -> fb::GubernatorAlgorithm {
    match alg {
        GubernatorAlgorithm::TokenBucket => fb::GubernatorAlgorithm::TokenBucket,
        GubernatorAlgorithm::LeakyBucket => fb::GubernatorAlgorithm::LeakyBucket,
    }
}

pub(crate) fn from_fb_gubernator_algorithm(
    alg: fb::GubernatorAlgorithm,
) -> Result<GubernatorAlgorithm, CodecError> {
    match alg {
        fb::GubernatorAlgorithm::TokenBucket => Ok(GubernatorAlgorithm::TokenBucket),
        fb::GubernatorAlgorithm::LeakyBucket => Ok(GubernatorAlgorithm::LeakyBucket),
        _ => Err(CodecError::UnknownVariant("GubernatorAlgorithm")),
    }
}

pub(crate) fn to_fb_job_status_kind(kind: JobStatusKind) -> fb::JobStatusKind {
    match kind {
        JobStatusKind::Scheduled => fb::JobStatusKind::Scheduled,
        JobStatusKind::Running => fb::JobStatusKind::Running,
        JobStatusKind::Failed => fb::JobStatusKind::Failed,
        JobStatusKind::Cancelled => fb::JobStatusKind::Cancelled,
        JobStatusKind::Succeeded => fb::JobStatusKind::Succeeded,
    }
}

pub(crate) fn from_fb_job_status_kind(
    kind: fb::JobStatusKind,
) -> Result<JobStatusKind, CodecError> {
    match kind {
        fb::JobStatusKind::Scheduled => Ok(JobStatusKind::Scheduled),
        fb::JobStatusKind::Running => Ok(JobStatusKind::Running),
        fb::JobStatusKind::Failed => Ok(JobStatusKind::Failed),
        fb::JobStatusKind::Cancelled => Ok(JobStatusKind::Cancelled),
        fb::JobStatusKind::Succeeded => Ok(JobStatusKind::Succeeded),
        _ => Err(CodecError::UnknownVariant("JobStatusKind")),
    }
}

pub(crate) fn create_string_vector<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    values: &[String],
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<&'a str>>> {
    let mut offsets = Vec::with_capacity(values.len());
    for value in values {
        offsets.push(builder.create_string(value));
    }
    builder.create_vector(&offsets)
}

pub(crate) fn create_string_vector_from_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    values: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<&str>>>,
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<&'a str>>> {
    let mut offsets = Vec::with_capacity(values.map(|v| v.len()).unwrap_or_default());
    if let Some(values) = values {
        for value in values.iter() {
            offsets.push(builder.create_string(value));
        }
    }
    builder.create_vector(&offsets)
}

pub(crate) fn create_string_pair_vector<'a>(
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

pub(crate) fn parse_string_pairs(
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

pub(crate) fn parse_string_values(
    values: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<&str>>>,
) -> Vec<String> {
    match values {
        Some(values) => values.iter().map(|value| value.to_string()).collect(),
        None => Vec::new(),
    }
}

pub(crate) fn parse_gubernator_rate_limit_data(
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

pub(crate) fn build_gubernator_rate_limit_data<'a>(
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
