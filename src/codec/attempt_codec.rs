use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;

use crate::codec::common::{CodecError, fb_error, required_str};
use crate::flatbuf::silo_internal as fb;
use crate::job_attempt::{AttemptStatus, JobAttempt};

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

#[derive(Clone)]
pub struct DecodedAttempt {
    data: Bytes,
    state: AttemptStatus,
}

impl DecodedAttempt {
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

#[inline]
pub fn decode_attempt(bytes: &[u8]) -> Result<DecodedAttempt, CodecError> {
    decode_attempt_bytes(Bytes::copy_from_slice(bytes))
}

#[inline]
pub(crate) fn decode_attempt_bytes(data: Bytes) -> Result<DecodedAttempt, CodecError> {
    let attempt =
        flatbuffers::root::<fb::JobAttempt>(&data).map_err(|e| fb_error("JobAttempt", e))?;
    let _ = required_str(attempt.job_id(), "attempt.job_id")?;
    let _ = required_str(attempt.task_id(), "attempt.task_id")?;
    let status = attempt
        .status()
        .ok_or(CodecError::MissingField("attempt.status"))?;
    let state = parse_attempt_status(status)?;
    Ok(DecodedAttempt { data, state })
}
