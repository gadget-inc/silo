use rkyv::{AlignedVec, Archive};

use crate::job_attempt::JobAttempt;
use crate::job_store_shard::{LeaseRecord, Task};

#[inline]
pub fn encode_task(task: &Task) -> Result<AlignedVec, String> {
    rkyv::to_bytes::<Task, 256>(task).map_err(|e| e.to_string())
}

#[inline]
pub fn decode_task(bytes: &[u8]) -> Task {
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(bytes) };
    match archived {
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
    }
}

#[inline]
pub fn encode_lease(record: &LeaseRecord) -> Result<AlignedVec, String> {
    rkyv::to_bytes::<LeaseRecord, 256>(record).map_err(|e| e.to_string())
}

#[inline]
pub fn decode_lease<'a>(bytes: &'a [u8]) -> &'a <LeaseRecord as Archive>::Archived {
    unsafe { rkyv::archived_root::<LeaseRecord>(bytes) }
}

#[inline]
pub fn encode_attempt(attempt: &JobAttempt) -> Result<AlignedVec, String> {
    rkyv::to_bytes::<JobAttempt, 256>(attempt).map_err(|e| e.to_string())
}
