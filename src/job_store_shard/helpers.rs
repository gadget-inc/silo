//! Helper functions shared across job_store_shard submodules.

use rkyv::Deserialize as RkyvDeserialize;
use slatedb::{Db, WriteBatch};
use uuid::Uuid;

use crate::codec::{decode_job_status, encode_held_ticket_state, encode_task};
use crate::concurrency::{ConcurrencyManager, MemoryEvent};
use crate::job::{FloatingConcurrencyLimit, JobStatus, Limit};
use crate::job_store_shard::JobStoreShardError;
use crate::keys::{held_ticket_state_key, task_key};
use crate::routing::queue_to_shard;
use crate::task::{
    partition_held_queues, FloatingConcurrencyLimitData, HeldTicketState, RemoteQueueRef, Task,
};

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
    time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
    task: &Task,
) -> Result<(), JobStoreShardError> {
    let task_value = encode_task(task)?;
    batch.put(
        task_key(time_ms, priority, job_id, attempt).as_bytes(),
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

/// Extract floating limit data from a FloatingConcurrencyLimit.
fn floating_limit_data(fl: &FloatingConcurrencyLimit) -> FloatingConcurrencyLimitData {
    FloatingConcurrencyLimitData::from(fl)
}

/// Result of creating a ticket request task.
/// For remote queues, includes a HeldTicketState that must be written.
pub(crate) struct TicketRequestTaskResult {
    pub task: Task,
    /// For remote ticket requests, the key and encoded value of HeldTicketState to write.
    /// None for local ticket requests.
    pub held_state_write: Option<(String, Vec<u8>)>,
}

/// Create a ticket request task for a concurrency limit (fixed or floating).
///
/// Returns the task and optionally the HeldTicketState key+value to write
/// (only needed for remote queues).
///
/// This consolidates the repeated pattern of:
/// 1. Getting queue_key and floating_limit_data from a Limit
/// 2. Checking if queue is local or remote
/// 3. Creating RequestTicket or RequestRemoteTicket accordingly
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_ticket_request_task(
    limit: &Limit,
    tenant: &str,
    job_id: &str,
    attempt_number: u32,
    priority: u8,
    start_time_ms: i64,
    task_id: &str,
    held_queues: Vec<String>,
    shard_number: u32,
    num_shards: u32,
) -> Result<TicketRequestTaskResult, JobStoreShardError> {
    let (queue_key, fl_data) = match limit {
        Limit::Concurrency(cl) => (cl.key.clone(), None),
        Limit::FloatingConcurrency(fl) => (fl.key.clone(), Some(floating_limit_data(fl))),
        Limit::RateLimit(_) => {
            // Should not be called for rate limits
            return Err(JobStoreShardError::Rkyv(
                "create_ticket_request_task called for rate limit".to_string(),
            ));
        }
    };

    let queue_owner_shard = queue_to_shard(tenant, &queue_key, num_shards);
    let is_local = queue_owner_shard == shard_number;

    if is_local {
        // Local queue - use RequestTicket
        Ok(TicketRequestTaskResult {
            task: Task::RequestTicket {
                queue: queue_key,
                start_time_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                request_id: task_id.to_string(),
                held_queues,
            },
            held_state_write: None,
        })
    } else {
        // Remote queue - use RequestRemoteTicket
        let request_id = Uuid::new_v4().to_string();

        // Prepare held ticket state for caller to write
        let state = HeldTicketState { held_queues };
        let state_key = held_ticket_state_key(job_id, attempt_number, &request_id);
        let state_value = encode_held_ticket_state(&state)?;

        Ok(TicketRequestTaskResult {
            task: Task::RequestRemoteTicket {
                task_id: task_id.to_string(),
                queue_owner_shard,
                tenant: tenant.to_string(),
                queue_key,
                job_id: job_id.to_string(),
                attempt_number,
                priority,
                start_time_ms,
                request_id,
                floating_limit: fl_data,
            },
            held_state_write: Some((state_key, state_value)),
        })
    }
}

/// Convenience wrapper that writes the HeldTicketState directly to a WriteBatch.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_ticket_request_task_with_batch(
    batch: &mut WriteBatch,
    limit: &Limit,
    tenant: &str,
    job_id: &str,
    attempt_number: u32,
    priority: u8,
    start_time_ms: i64,
    task_id: &str,
    held_queues: Vec<String>,
    shard_number: u32,
    num_shards: u32,
) -> Result<Task, JobStoreShardError> {
    let result = create_ticket_request_task(
        limit,
        tenant,
        job_id,
        attempt_number,
        priority,
        start_time_ms,
        task_id,
        held_queues,
        shard_number,
        num_shards,
    )?;

    // Write held state if present (for remote queues)
    if let Some((key, value)) = result.held_state_write {
        batch.put(key.as_bytes(), &value);
    }

    Ok(result.task)
}

/// Release held tickets - both local and remote.
///
/// This handles the common pattern of:
/// 1. Partitioning held_queues into local vs remote
/// 2. Releasing local tickets via ConcurrencyManager (which may grant to next requester)
/// 3. Creating ReleaseRemoteTicket tasks for remote tickets
///
/// Returns the memory events for local releases (caller should apply these to concurrency counts).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn release_held_tickets(
    db: &Db,
    batch: &mut WriteBatch,
    concurrency: &ConcurrencyManager,
    tenant: &str,
    job_id: &str,
    task_id: &str,
    held_queues: &[String],
    now_ms: i64,
) -> Result<Vec<MemoryEvent>, JobStoreShardError> {
    let (local_queues, remote_queues) = partition_held_queues(held_queues);

    // Release LOCAL tickets directly
    let release_events = concurrency
        .release_and_grant_next(db, batch, tenant, &local_queues, task_id, now_ms)
        .await
        .map_err(JobStoreShardError::Rkyv)?;

    // Create ReleaseRemoteTicket tasks for REMOTE tickets
    for remote_queue in &remote_queues {
        let Some(remote_ref) = RemoteQueueRef::parse(remote_queue) else {
            tracing::warn!(
                remote_queue = %remote_queue,
                "malformed remote queue format, skipping release"
            );
            continue;
        };

        let release_task = Task::ReleaseRemoteTicket {
            task_id: Uuid::new_v4().to_string(),
            queue_owner_shard: remote_ref.queue_owner_shard,
            tenant: tenant.to_string(),
            queue_key: remote_ref.queue_key.clone(),
            job_id: job_id.to_string(),
            holder_task_id: remote_ref.holder_task_id.clone(),
        };
        put_task(batch, now_ms, 0, job_id, 0, &release_task)?;
        tracing::debug!(
            tenant = tenant,
            job_id = %job_id,
            queue_key = %remote_ref.queue_key,
            queue_owner_shard = remote_ref.queue_owner_shard,
            holder_task_id = %remote_ref.holder_task_id,
            "created ReleaseRemoteTicket task for remote queue"
        );
    }

    if !local_queues.is_empty() || !remote_queues.is_empty() {
        tracing::debug!(
            job_id = %job_id,
            local = local_queues.len(),
            remote = remote_queues.len(),
            "released held tickets"
        );
    }

    Ok(release_events)
}
