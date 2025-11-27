use rkyv::{AlignedVec, Archive};

use crate::job_attempt::JobAttempt;
use crate::job_store_shard::{LeaseRecord, Task};

#[inline]
pub fn encode_task(task: &Task) -> Result<AlignedVec, String> {
    rkyv::to_bytes::<Task, 256>(task).map_err(|e| e.to_string())
}

#[inline]
pub fn decode_task(bytes: &[u8]) -> Result<Task, String> {
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedTask =
        rkyv::check_archived_root::<Task>(bytes).map_err(|e| e.to_string())?;
    Ok(match archived {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            held_queues,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: held_queues
                .iter()
                .map(|s| s.as_str().to_string())
                .collect::<Vec<String>>(),
        },
        ArchivedTask::RequestTicket {
            queue,
            start_time_ms,
            priority,
            job_id,
            attempt_number,
            request_id,
        } => Task::RequestTicket {
            queue: queue.as_str().to_string(),
            start_time_ms: *start_time_ms,
            priority: *priority,
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            request_id: request_id.as_str().to_string(),
        },
    })
}

#[inline]
pub fn encode_lease(record: &LeaseRecord) -> Result<AlignedVec, String> {
    rkyv::to_bytes::<LeaseRecord, 256>(record).map_err(|e| e.to_string())
}

#[inline]
pub fn decode_lease(bytes: &[u8]) -> Result<&<LeaseRecord as Archive>::Archived, String> {
    rkyv::check_archived_root::<LeaseRecord>(bytes).map_err(|e| e.to_string())
}

#[inline]
pub fn encode_attempt(attempt: &JobAttempt) -> Result<AlignedVec, String> {
    rkyv::to_bytes::<JobAttempt, 256>(attempt).map_err(|e| e.to_string())
}
