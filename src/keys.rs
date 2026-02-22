//! Binary key encoding for SlateDB using the storekey crate.
//!
//! This module provides lexicographically-sortable binary keys for the KV store. All keys use storekey's order-preserving encoding, ensuring that keys sort correctly when compared as byte strings.
//!
//! # Encoding Details
//!
//! - Strings use null-terminated escape encoding (embedded nulls are escaped)
//! - Integers use big-endian encoding for correct lexicographic ordering
//! - Tuples encode each field sequentially

use storekey::{decode, encode_vec};

use crate::job::{JobStatus, JobStatusKind};

pub(crate) mod prefix {
    pub const JOB_INFO: u8 = 0x01;
    pub const JOB_STATUS: u8 = 0x02;
    pub const IDX_STATUS_TIME: u8 = 0x03;
    pub const IDX_METADATA: u8 = 0x04;
    pub const TASK: u8 = 0x05;
    pub const LEASE: u8 = 0x06;
    pub const ATTEMPT: u8 = 0x07;
    pub const CONCURRENCY_REQUEST: u8 = 0x08;
    pub const CONCURRENCY_HOLDER: u8 = 0x09;
    pub const JOB_CANCELLED: u8 = 0x0A;
    pub const FLOATING_LIMIT: u8 = 0x0B;
    pub const COUNTER_TOTAL_JOBS: u8 = 0xF0;
    pub const COUNTER_COMPLETED_JOBS: u8 = 0xF1;
    pub const CLEANUP_PROGRESS: u8 = 0xF2;
    pub const CLEANUP_COMPLETE: u8 = 0xF3;
    pub const CLEANUP_STATUS: u8 = 0xF4;
    pub const SHARD_CREATED_AT: u8 = 0xF5;
    pub const CLEANUP_COMPLETED_AT: u8 = 0xF6;
}

/// Encode a key with its namespace prefix.
fn encode_with_prefix<T: storekey::Encode>(prefix: u8, data: &T) -> Vec<u8> {
    let mut buf = vec![prefix];
    let encoded = encode_vec(data).expect("storekey encoding should not fail for valid data");
    buf.extend(encoded);
    buf
}

/// The KV store key for a given job's info by id.
pub fn job_info_key(tenant: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::JOB_INFO, &(tenant, job_id))
}

/// Prefix for scanning all jobs for a tenant.
pub fn job_info_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::JOB_INFO, &(tenant,))
}

/// Prefix for scanning all jobs across all tenants.
pub fn jobs_prefix() -> Vec<u8> {
    vec![prefix::JOB_INFO]
}

/// Parsed job info key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedJobInfoKey {
    pub tenant: String,
    pub job_id: String,
}

/// Parse a job info key back to its components.
pub fn parse_job_info_key(key: &[u8]) -> Option<ParsedJobInfoKey> {
    if key.first() != Some(&prefix::JOB_INFO) {
        return None;
    }
    let (tenant, job_id): (String, String) = decode(&key[1..]).ok()?;
    Some(ParsedJobInfoKey { tenant, job_id })
}

/// The KV store key for a given job's status.
pub fn job_status_key(tenant: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::JOB_STATUS, &(tenant, job_id))
}

/// Parsed job status key components (same structure as job info).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedJobStatusKey {
    pub tenant: String,
    pub job_id: String,
}

/// Parse a job status key back to its components.
pub fn parse_job_status_key(key: &[u8]) -> Option<ParsedJobStatusKey> {
    if key.first() != Some(&prefix::JOB_STATUS) {
        return None;
    }
    let (tenant, job_id): (String, String) = decode(&key[1..]).ok()?;
    Some(ParsedJobStatusKey { tenant, job_id })
}

/// Index: time-ordered by status, newest-first using inverted timestamp.
pub fn idx_status_time_key(
    tenant: &str,
    status: &str,
    changed_at_ms: i64,
    job_id: &str,
) -> Vec<u8> {
    let inv: u64 = u64::MAX - (changed_at_ms.max(0) as u64);
    encode_with_prefix(prefix::IDX_STATUS_TIME, &(tenant, status, inv, job_id))
}

/// Prefix for scanning status index for a tenant and status.
pub fn idx_status_time_prefix(tenant: &str, status: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_STATUS_TIME, &(tenant, status))
}

/// Prefix for scanning status index for a tenant, status, and time bound.
/// Used to construct range scan boundaries for waiting/future-scheduled queries.
pub fn idx_status_time_prefix_with_time(
    tenant: &str,
    status: &str,
    inverted_timestamp: u64,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::IDX_STATUS_TIME,
        &(tenant, status, inverted_timestamp),
    )
}

/// Prefix for scanning all status/time index entries for a single tenant (all statuses).
pub fn idx_status_time_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_STATUS_TIME, &(tenant,))
}

/// Prefix for scanning all status index entries (cross-tenant).
pub fn idx_status_time_all_prefix() -> Vec<u8> {
    vec![prefix::IDX_STATUS_TIME]
}

/// Parsed status/time index key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedStatusTimeIndexKey {
    pub tenant: String,
    pub status: String,
    pub inverted_timestamp: u64,
    pub job_id: String,
}

impl ParsedStatusTimeIndexKey {
    /// Get the original timestamp from the inverted value.
    pub fn changed_at_ms(&self) -> i64 {
        (u64::MAX - self.inverted_timestamp) as i64
    }
}

/// Parse a status/time index key back to its components.
pub fn parse_status_time_index_key(key: &[u8]) -> Option<ParsedStatusTimeIndexKey> {
    if key.first() != Some(&prefix::IDX_STATUS_TIME) {
        return None;
    }
    let (tenant, status, inverted_timestamp, job_id): (String, String, u64, String) =
        decode(&key[1..]).ok()?;
    Some(ParsedStatusTimeIndexKey {
        tenant,
        status,
        inverted_timestamp,
        job_id,
    })
}

/// For Scheduled statuses, returns next_attempt_starts_after_ms for index keying.
/// For all other statuses, returns changed_at_ms.
/// This allows the status/time index to order Scheduled jobs by their start time,
/// enabling efficient range scans to split waiting vs future-scheduled jobs.
pub fn status_index_timestamp(status: &JobStatus) -> i64 {
    if status.kind == JobStatusKind::Scheduled {
        status
            .next_attempt_starts_after_ms
            .unwrap_or(status.changed_at_ms)
    } else {
        status.changed_at_ms
    }
}

/// Index: jobs by metadata key/value (unsorted within key/value).
pub fn idx_metadata_key(tenant: &str, key: &str, value: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_METADATA, &(tenant, key, value, job_id))
}

/// Prefix for scanning jobs by metadata key/value.
pub fn idx_metadata_prefix(tenant: &str, key: &str, value: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_METADATA, &(tenant, key, value))
}

/// Prefix for scanning jobs by metadata key only (all values for a given tenant + key).
/// Used as the end boundary for prefix scans over metadata values.
pub fn idx_metadata_key_only_prefix(tenant: &str, key: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_METADATA, &(tenant, key))
}

/// Parsed metadata index key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMetadataIndexKey {
    pub tenant: String,
    pub key: String,
    pub value: String,
    pub job_id: String,
}

/// Parse a metadata index key back to its components.
pub fn parse_metadata_index_key(key: &[u8]) -> Option<ParsedMetadataIndexKey> {
    if key.first() != Some(&prefix::IDX_METADATA) {
        return None;
    }
    let (tenant, k, value, job_id): (String, String, String, String) = decode(&key[1..]).ok()?;
    Some(ParsedMetadataIndexKey {
        tenant,
        key: k,
        value,
        job_id,
    })
}

/// Construct the key for a task, ordered by task group then start time.
pub fn task_key(
    task_group: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::TASK,
        &(
            task_group,
            start_time_ms.max(0) as u64,
            priority,
            job_id,
            attempt,
        ),
    )
}

/// Get the prefix for scanning all tasks in a specific task group.
pub fn task_group_prefix(task_group: &str) -> Vec<u8> {
    encode_with_prefix(prefix::TASK, &(task_group,))
}

/// Get the prefix for scanning all task groups.
pub fn tasks_prefix() -> Vec<u8> {
    vec![prefix::TASK]
}

/// Parsed task key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTaskKey {
    pub task_group: String,
    pub start_time_ms: u64,
    pub priority: u8,
    pub job_id: String,
    pub attempt: u32,
}

/// Parse a task key back to its components.
pub fn parse_task_key(key: &[u8]) -> Option<ParsedTaskKey> {
    if key.first() != Some(&prefix::TASK) {
        return None;
    }
    let (task_group, start_time_ms, priority, job_id, attempt): (String, u64, u8, String, u32) =
        decode(&key[1..]).ok()?;
    Some(ParsedTaskKey {
        task_group,
        start_time_ms,
        priority,
        job_id,
        attempt,
    })
}

/// Construct the key for a leased task by task id.
pub fn leased_task_key(task_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::LEASE, &(task_id,))
}

/// Prefix for scanning all lease keys.
pub fn leases_prefix() -> Vec<u8> {
    vec![prefix::LEASE]
}

/// Parsed lease key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLeaseKey {
    pub task_id: String,
}

/// Parse a lease key back to its components.
pub fn parse_lease_key(key: &[u8]) -> Option<ParsedLeaseKey> {
    if key.first() != Some(&prefix::LEASE) {
        return None;
    }
    let (task_id,): (String,) = decode(&key[1..]).ok()?;
    Some(ParsedLeaseKey { task_id })
}

/// Construct the key for an attempt record.
pub fn attempt_key(tenant: &str, job_id: &str, attempt: u32) -> Vec<u8> {
    encode_with_prefix(prefix::ATTEMPT, &(tenant, job_id, attempt))
}

/// Construct the prefix for scanning all attempts of a job.
pub fn attempt_prefix(tenant: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::ATTEMPT, &(tenant, job_id))
}

/// Parsed attempt key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAttemptKey {
    pub tenant: String,
    pub job_id: String,
    pub attempt: u32,
}

/// Parse an attempt key back to its components.
pub fn parse_attempt_key(key: &[u8]) -> Option<ParsedAttemptKey> {
    if key.first() != Some(&prefix::ATTEMPT) {
        return None;
    }
    let (tenant, job_id, attempt): (String, String, u32) = decode(&key[1..]).ok()?;
    Some(ParsedAttemptKey {
        tenant,
        job_id,
        attempt,
    })
}

/// Concurrency request queue key.
/// Ordered by start time (when job should run), then priority (lower = higher), then request ID.
pub fn concurrency_request_key(
    tenant: &str,
    queue: &str,
    start_time_ms: i64,
    priority: u8,
    request_id: &str,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::CONCURRENCY_REQUEST,
        &(
            tenant,
            queue,
            start_time_ms.max(0) as u64,
            priority,
            request_id,
        ),
    )
}

/// Prefix for scanning all requests for a tenant/queue.
pub fn concurrency_request_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    encode_with_prefix(prefix::CONCURRENCY_REQUEST, &(tenant, queue))
}

/// Prefix for scanning all requests for a tenant.
pub fn concurrency_request_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::CONCURRENCY_REQUEST, &(tenant,))
}

/// Prefix for scanning all concurrency requests (cross-tenant).
pub fn concurrency_requests_prefix() -> Vec<u8> {
    vec![prefix::CONCURRENCY_REQUEST]
}

/// Parsed concurrency request key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedConcurrencyRequestKey {
    pub tenant: String,
    pub queue: String,
    pub start_time_ms: u64,
    pub priority: u8,
    pub request_id: String,
}

/// Parse a concurrency request key back to its components.
pub fn parse_concurrency_request_key(key: &[u8]) -> Option<ParsedConcurrencyRequestKey> {
    if key.first() != Some(&prefix::CONCURRENCY_REQUEST) {
        return None;
    }
    let (tenant, queue, start_time_ms, priority, request_id): (String, String, u64, u8, String) =
        decode(&key[1..]).ok()?;
    Some(ParsedConcurrencyRequestKey {
        tenant,
        queue,
        start_time_ms,
        priority,
        request_id,
    })
}

/// Concurrency holders set key.
pub fn concurrency_holder_key(tenant: &str, queue: &str, task_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::CONCURRENCY_HOLDER, &(tenant, queue, task_id))
}

/// Prefix for scanning all holders (cross-tenant).
pub fn concurrency_holders_prefix() -> Vec<u8> {
    vec![prefix::CONCURRENCY_HOLDER]
}

/// Prefix for scanning all holders for a tenant.
pub fn concurrency_holders_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::CONCURRENCY_HOLDER, &(tenant,))
}

/// Prefix for scanning all holders for a tenant/queue.
pub fn concurrency_holders_queue_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    encode_with_prefix(prefix::CONCURRENCY_HOLDER, &(tenant, queue))
}

/// Parsed concurrency holder key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedConcurrencyHolderKey {
    pub tenant: String,
    pub queue: String,
    pub task_id: String,
}

/// Parse a concurrency holder key back to its components.
pub fn parse_concurrency_holder_key(key: &[u8]) -> Option<ParsedConcurrencyHolderKey> {
    if key.first() != Some(&prefix::CONCURRENCY_HOLDER) {
        return None;
    }
    let (tenant, queue, task_id): (String, String, String) = decode(&key[1..]).ok()?;
    Some(ParsedConcurrencyHolderKey {
        tenant,
        queue,
        task_id,
    })
}

/// The KV store key for a job's cancellation flag.
/// Cancellation is stored separately from status to allow dequeue to blindly write Running without losing cancellation info.
pub fn job_cancelled_key(tenant: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::JOB_CANCELLED, &(tenant, job_id))
}

/// Parsed job cancelled key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedJobCancelledKey {
    pub tenant: String,
    pub job_id: String,
}

/// Parse a job cancelled key back to its components.
pub fn parse_job_cancelled_key(key: &[u8]) -> Option<ParsedJobCancelledKey> {
    if key.first() != Some(&prefix::JOB_CANCELLED) {
        return None;
    }
    let (tenant, job_id): (String, String) = decode(&key[1..]).ok()?;
    Some(ParsedJobCancelledKey { tenant, job_id })
}

/// The KV store key for a floating concurrency limit's state.
/// Stores the current max concurrency, last refresh time, and refresh task status.
pub fn floating_limit_state_key(tenant: &str, queue_key: &str) -> Vec<u8> {
    encode_with_prefix(prefix::FLOATING_LIMIT, &(tenant, queue_key))
}

/// Parsed floating limit state key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedFloatingLimitKey {
    pub tenant: String,
    pub queue_key: String,
}

/// Parse a floating limit state key back to its components.
pub fn parse_floating_limit_key(key: &[u8]) -> Option<ParsedFloatingLimitKey> {
    if key.first() != Some(&prefix::FLOATING_LIMIT) {
        return None;
    }
    let (tenant, queue_key): (String, String) = decode(&key[1..]).ok()?;
    Some(ParsedFloatingLimitKey { tenant, queue_key })
}

/// Key for the total jobs counter for this shard.
/// This counts all jobs that exist in the shard (not deleted).
pub fn shard_total_jobs_counter_key() -> Vec<u8> {
    vec![prefix::COUNTER_TOTAL_JOBS]
}

/// Key for the completed jobs counter for this shard.
/// This counts jobs that have reached a terminal state (Succeeded, Failed, or Cancelled).
pub fn shard_completed_jobs_counter_key() -> Vec<u8> {
    vec![prefix::COUNTER_COMPLETED_JOBS]
}

/// Key for storing cleanup progress checkpoint (for resumption after crash).
pub fn cleanup_progress_key() -> Vec<u8> {
    vec![prefix::CLEANUP_PROGRESS]
}

/// Key for storing cleanup completion marker.
pub fn cleanup_complete_key() -> Vec<u8> {
    vec![prefix::CLEANUP_COMPLETE]
}

/// Key for storing the authoritative cleanup status for this shard.
/// This is the source of truth for whether the shard needs cleanup work.
pub fn cleanup_status_key() -> Vec<u8> {
    vec![prefix::CLEANUP_STATUS]
}

/// Key for storing the timestamp (ms) when the shard was first created/initialized.
pub fn shard_created_at_key() -> Vec<u8> {
    vec![prefix::SHARD_CREATED_AT]
}

/// Key for storing the timestamp (ms) when cleanup completed for this shard.
/// Only set after a split cleanup finishes.
pub fn cleanup_completed_at_key() -> Vec<u8> {
    vec![prefix::CLEANUP_COMPLETED_AT]
}

/// Construct a composite key for in-memory concurrency queue tracking.
/// Uses storekey encoding for collision-safe (tenant, queue) pairing.
pub fn concurrency_counts_key(tenant: &str, queue: &str) -> Vec<u8> {
    encode_vec(&(tenant, queue)).expect("storekey encoding should not fail")
}

/// Create an exclusive end bound for range scanning.
///
/// This computes the lexicographically smallest key that is greater than all keys starting with the given prefix. This is done by incrementing the prefix as if it were a big-endian integer, handling carry-over for 0xFF bytes.
///
/// Use this with the prefix functions to create a range like `prefix..end_bound(prefix)`.
///
/// Returns None if the prefix is all 0xFF bytes (no valid exclusive upper bound exists).
pub fn end_bound(prefix: &[u8]) -> Vec<u8> {
    // Find the rightmost byte that isn't 0xFF
    let mut end = prefix.to_vec();

    // Increment from the right, handling overflow
    while let Some(last) = end.pop() {
        if last < 0xFF {
            // Found a byte we can increment
            end.push(last + 1);
            return end;
        }
        // last was 0xFF, continue to next byte (carry)
    }

    // All bytes were 0xFF - append 0xFF to create an upper bound
    // This handles the edge case where prefix is all 0xFF bytes.
    // In this case, we return prefix + [0xFF] as a fallback.
    let mut fallback = prefix.to_vec();
    fallback.push(0xFF);
    fallback
}
