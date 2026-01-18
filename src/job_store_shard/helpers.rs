//! Helper functions shared across job_store_shard submodules.

use rkyv::Deserialize as RkyvDeserialize;
use slatedb::WriteBatch;

use crate::codec::{decode_job_status, encode_task};
use crate::job::JobStatus;
use crate::job_store_shard::JobStoreShardError;
use crate::keys::task_key;
use crate::task::Task;

/// Get current epoch time in milliseconds.
pub fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

/// Encode and write a task to the batch at the standard task key location.
pub(crate) fn put_task(
    batch: &mut WriteBatch,
    task_group: &str,
    time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
    task: &Task,
) -> Result<(), JobStoreShardError> {
    let task_value = encode_task(task)?;
    batch.put(
        task_key(task_group, time_ms, priority, job_id, attempt).as_bytes(),
        &task_value,
    );
    Ok(())
}

/// Decode a `JobStatus` from raw rkyv bytes into an owned value.
pub(crate) fn decode_job_status_owned(raw: &[u8]) -> Result<JobStatus, JobStoreShardError> {
    let decoded = decode_job_status(raw)?;
    let mut des = rkyv::Infallible;
    Ok(RkyvDeserialize::deserialize(decoded.archived(), &mut des)
        .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus")))
}
