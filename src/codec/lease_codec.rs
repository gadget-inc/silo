use flatbuffers::FlatBufferBuilder;

use crate::codec::common::{CodecError, create_string_vector_from_fb, fb_error, required_str};
use crate::codec::task_codec::{
    DecodedRefreshFloatingLimitTask, DecodedRequestTicketTask, DecodedRunAttemptTask, TaskDataView,
    build_task, parse_task,
};
use crate::flatbuf::silo_internal as fb;
use crate::task::{LeaseRecord, Task};

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

/// Encode a lease directly from a decoded RunAttempt task view, avoiding Task materialization.
#[inline]
pub fn encode_lease_from_run_attempt(
    worker_id: &str,
    task: DecodedRunAttemptTask<'_>,
    expiry_ms: i64,
    started_at_ms: i64,
) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(worker_id);
    let id = builder.create_string(task.id());
    let tenant = builder.create_string(task.tenant());
    let job_id = builder.create_string(task.job_id());
    let held_queues = create_string_vector_from_fb(&mut builder, task.held_queues_vector());
    let task_group = builder.create_string(task.task_group());
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

/// Encode a granted RequestTicket as a RunAttempt lease directly from decoded task bytes.
#[inline]
pub fn encode_lease_from_request_ticket_grant(
    worker_id: &str,
    task: DecodedRequestTicketTask<'_>,
    held_queue: &str,
    expiry_ms: i64,
    started_at_ms: i64,
) -> Result<Vec<u8>, CodecError> {
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let worker_id = builder.create_string(worker_id);
    let id = builder.create_string(task.request_id());
    let tenant = builder.create_string(task.tenant());
    let job_id = builder.create_string(task.job_id());
    let queue = builder.create_string(held_queue);
    let held_queues = builder.create_vector(&[queue]);
    let task_group = builder.create_string(task.task_group());
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

/// Decoded lease record view over validated flatbuffer bytes.
#[derive(Clone, Copy)]
pub struct DecodedLease<'a> {
    lease: fb::LeaseRecord<'a>,
    task: TaskDataView<'a>,
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

    pub fn task_id(&self) -> Option<&str> {
        self.task.run_attempt_task_id()
    }

    pub fn refresh_floating_limit_info(&self) -> Option<(&str, &str)> {
        self.task.refresh_info()
    }

    pub fn refresh_floating_limit_task(&self) -> Option<DecodedRefreshFloatingLimitTask<'_>> {
        self.task.refresh_floating_limit()
    }

    pub fn run_attempt_task(&self) -> Option<DecodedRunAttemptTask<'_>> {
        self.task.run_attempt()
    }

    pub fn tenant(&self) -> &str {
        self.task.tenant_default()
    }

    pub fn job_id(&self) -> &str {
        self.task.job_id_default()
    }

    pub fn attempt_number(&self) -> u32 {
        self.task.attempt_number()
    }

    pub fn relative_attempt_number(&self) -> u32 {
        self.task.relative_attempt_number()
    }

    pub fn held_queues(&self) -> Vec<String> {
        self.task.held_queues()
    }

    pub fn task_group(&self) -> &str {
        self.task.task_group_default()
    }

    pub fn to_task(&self) -> Result<Task, CodecError> {
        parse_task(self.task)
    }
}

#[inline]
pub fn decode_lease(bytes: &[u8]) -> Result<DecodedLease<'_>, CodecError> {
    let lease =
        flatbuffers::root::<fb::LeaseRecord>(bytes).map_err(|e| fb_error("LeaseRecord", e))?;
    let _ = required_str(lease.worker_id(), "lease.worker_id")?;
    let task = lease.task().ok_or(CodecError::MissingField("lease.task"))?;
    let task = TaskDataView::from_task(task)?;
    // Reuse task owned parse as strict validation.
    let _ = parse_task(task)?;
    Ok(DecodedLease { lease, task })
}
