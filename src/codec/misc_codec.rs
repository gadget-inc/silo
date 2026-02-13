use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;

use crate::codec::common::{
    CodecError, create_string_pair_vector, fb_error, parse_string_pairs, required_str,
};
use crate::flatbuf::silo_internal as fb;
use crate::job::{FloatingLimitState, JobCancellation};
use crate::task::{ConcurrencyAction, HolderRecord};

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

#[derive(Clone, Copy)]
pub struct DecodedHolder<'a> {
    holder: fb::HolderRecord<'a>,
}

impl DecodedHolder<'_> {
    pub fn granted_at_ms(&self) -> i64 {
        self.holder.granted_at_ms()
    }
}

#[inline]
pub fn decode_holder(bytes: &[u8]) -> Result<DecodedHolder<'_>, CodecError> {
    let holder =
        flatbuffers::root::<fb::HolderRecord>(bytes).map_err(|e| fb_error("HolderRecord", e))?;
    Ok(DecodedHolder { holder })
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
pub struct DecodedJobCancellation<'a> {
    cancellation: fb::JobCancellation<'a>,
}

impl DecodedJobCancellation<'_> {
    pub fn cancelled_at_ms(&self) -> i64 {
        self.cancellation.cancelled_at_ms()
    }
}

#[inline]
pub fn decode_job_cancellation(bytes: &[u8]) -> Result<DecodedJobCancellation<'_>, CodecError> {
    let cancellation = flatbuffers::root::<fb::JobCancellation>(bytes)
        .map_err(|e| fb_error("JobCancellation", e))?;
    Ok(DecodedJobCancellation { cancellation })
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
