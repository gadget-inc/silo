//! Binary key encoding for SlateDB using the storekey crate.
//!
//! This module provides lexicographically-sortable binary keys for the KV store. All keys use storekey's order-preserving encoding, ensuring that keys sort correctly when compared as byte strings.
//!
//! # Encoding Details
//!
//! - Strings use null-terminated escape encoding (embedded nulls are escaped)
//! - Integers use big-endian encoding for correct lexicographic ordering
//! - Tuples encode each field sequentially

use std::borrow::Cow;
use std::io;

use storekey::{BorrowDecode, Encode, decode_borrow, encode};

use crate::job::{JobStatus, JobStatusKind, ListingSegment};

pub mod prefix {
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
    pub const REFRESH_TASK: u8 = 0x0C;
    pub const IDX_ENQUEUE_TIME: u8 = 0x0D;
    pub const IDX_STATUS_ENQUEUE_TIME: u8 = 0x0E;
    pub const COUNTER_TOTAL_JOBS: u8 = 0xF0;
    pub const COUNTER_COMPLETED_JOBS: u8 = 0xF1;
    pub const CLEANUP_PROGRESS: u8 = 0xF2;
    pub const CLEANUP_COMPLETE: u8 = 0xF3;
    pub const CLEANUP_STATUS: u8 = 0xF4;
    pub const SHARD_CREATED_AT: u8 = 0xF5;
    pub const CLEANUP_COMPLETED_AT: u8 = 0xF6;
    pub const COUNTER_CONCURRENCY_REQUESTERS: u8 = 0xF7;
    pub const COUNTER_TENANT_STATUS: u8 = 0xF8;
    pub const HOLDER_RECONCILE_BARRIER: u8 = 0xF9;
    pub const ENQUEUE_TIME_INDEX_BACKFILL_PROGRESS: u8 = 0xFB;
    pub const ENQUEUE_TIME_INDEX_BACKFILL_COMPLETE: u8 = 0xFC;
}

/// A writer that only counts the bytes written to it.
struct EncodedLen(usize);

impl io::Write for EncodedLen {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0 += buf.len();
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Encode `data`, after `prefix` when given, into a buffer allocated once at
/// the exact encoded length. A first pass counts the bytes, so the buffer
/// never grows and never carries spare capacity.
fn encode_exact<T: Encode>(prefix: Option<u8>, data: &T) -> Vec<u8> {
    let mut len = EncodedLen(usize::from(prefix.is_some()));
    encode(&mut len, data).expect("storekey encoding should not fail for valid data");
    let mut buf = Vec::with_capacity(len.0);
    buf.extend(prefix);
    encode(&mut buf, data).expect("storekey encoding should not fail for valid data");
    buf
}

/// Encode a key with its namespace prefix.
fn encode_with_prefix<T: Encode>(prefix: u8, data: &T) -> Vec<u8> {
    encode_exact(Some(prefix), data)
}

/// Decode the fields of a key after its namespace prefix. String fields
/// decoded as `Cow<str>` borrow from `key` unless they hold escaped bytes.
fn decode_after_prefix<'a, T: BorrowDecode<'a>>(key: &'a [u8], prefix: u8) -> Option<T> {
    decode_borrow(key.strip_prefix(&[prefix])?).ok()
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
    let (tenant, job_id): (Cow<str>, Cow<str>) = decode_after_prefix(key, prefix::JOB_INFO)?;
    Some(ParsedJobInfoKey {
        tenant: tenant.into_owned(),
        job_id: job_id.into_owned(),
    })
}

/// The KV store key for a given job's status.
pub fn job_status_key(tenant: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::JOB_STATUS, &(tenant, job_id))
}

/// Prefix for scanning all status records for a tenant.
pub fn job_status_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::JOB_STATUS, &(tenant,))
}

/// Prefix for scanning all status records across all tenants.
pub fn jobs_status_prefix() -> Vec<u8> {
    vec![prefix::JOB_STATUS]
}

/// Parsed job status key components (same structure as job info).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedJobStatusKey {
    pub tenant: String,
    pub job_id: String,
}

/// Parse a job status key back to its components.
pub fn parse_job_status_key(key: &[u8]) -> Option<ParsedJobStatusKey> {
    let (tenant, job_id): (Cow<str>, Cow<str>) = decode_after_prefix(key, prefix::JOB_STATUS)?;
    Some(ParsedJobStatusKey {
        tenant: tenant.into_owned(),
        job_id: job_id.into_owned(),
    })
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
    let (tenant, status, inverted_timestamp, job_id): (Cow<str>, Cow<str>, u64, Cow<str>) =
        decode_after_prefix(key, prefix::IDX_STATUS_TIME)?;
    Some(ParsedStatusTimeIndexKey {
        tenant: tenant.into_owned(),
        status: status.into_owned(),
        inverted_timestamp,
        job_id: job_id.into_owned(),
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

/// Map an `i64` onto `u64` preserving order over the whole range, then
/// reverse it, so that ascending byte order of the result equals descending
/// `enqueue_time_ms`. Unlike the status/time index this does not clamp at
/// zero: enqueue stores the caller's raw `start_at_ms`, which can be negative.
fn invert_enqueue_time(enqueue_time_ms: i64) -> u64 {
    u64::MAX - ((enqueue_time_ms as u64) ^ (1u64 << 63))
}

fn uninvert_enqueue_time(inverted: u64) -> i64 {
    ((u64::MAX - inverted) ^ (1u64 << 63)) as i64
}

/// Index: a tenant's jobs ordered by `enqueue_time_ms` descending, then job id
/// ascending. Written once at job creation; the value is empty.
pub fn idx_enqueue_time_key(tenant: &str, enqueue_time_ms: i64, job_id: &str) -> Vec<u8> {
    encode_with_prefix(
        prefix::IDX_ENQUEUE_TIME,
        &(tenant, invert_enqueue_time(enqueue_time_ms), job_id),
    )
}

/// Prefix for scanning a tenant's enqueue-time index newest-first.
pub fn idx_enqueue_time_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_ENQUEUE_TIME, &(tenant,))
}

/// Parsed enqueue-time index key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEnqueueTimeIndexKey {
    pub tenant: String,
    pub inverted_enqueue_time: u64,
    pub job_id: String,
}

impl ParsedEnqueueTimeIndexKey {
    /// Get the original enqueue time from the inverted value.
    pub fn enqueue_time_ms(&self) -> i64 {
        uninvert_enqueue_time(self.inverted_enqueue_time)
    }
}

/// Parse an enqueue-time index key back to its components.
pub fn parse_enqueue_time_index_key(key: &[u8]) -> Option<ParsedEnqueueTimeIndexKey> {
    let (tenant, inverted_enqueue_time, job_id): (Cow<str>, u64, Cow<str>) =
        decode_after_prefix(key, prefix::IDX_ENQUEUE_TIME)?;
    Some(ParsedEnqueueTimeIndexKey {
        tenant: tenant.into_owned(),
        inverted_enqueue_time,
        job_id: job_id.into_owned(),
    })
}

/// Index: a tenant's jobs by listing segment and priority, each arm ordered
/// by `enqueue_time_ms` descending, then job id ascending. A job has one
/// entry, in the segment of its stored status; the value is a
/// [`StatusEnqueueTimeIndexValue`].
pub fn idx_status_enqueue_time_key(
    tenant: &str,
    segment: ListingSegment,
    priority: u8,
    enqueue_time_ms: i64,
    job_id: &str,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::IDX_STATUS_ENQUEUE_TIME,
        &(
            tenant,
            segment.as_u8(),
            priority,
            invert_enqueue_time(enqueue_time_ms),
            job_id,
        ),
    )
}

/// Prefix covering every segment of a tenant's status-segmented index.
pub fn idx_status_enqueue_time_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_STATUS_ENQUEUE_TIME, &(tenant,))
}

/// Prefix covering every priority arm of one segment.
pub fn idx_status_enqueue_time_segment_prefix(tenant: &str, segment: ListingSegment) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_STATUS_ENQUEUE_TIME, &(tenant, segment.as_u8()))
}

/// Prefix covering one `(segment, priority)` arm, newest enqueue first.
pub fn idx_status_enqueue_time_priority_prefix(
    tenant: &str,
    segment: ListingSegment,
    priority: u8,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::IDX_STATUS_ENQUEUE_TIME,
        &(tenant, segment.as_u8(), priority),
    )
}

/// The job's key in every segment, in [`ListingSegment::ALL`] order. Enqueue
/// time and priority are fixed per job, so these are the only keys the job
/// can ever occupy.
pub fn idx_status_enqueue_time_candidate_keys(
    tenant: &str,
    priority: u8,
    enqueue_time_ms: i64,
    job_id: &str,
) -> [Vec<u8>; 6] {
    ListingSegment::ALL.map(|segment| {
        idx_status_enqueue_time_key(tenant, segment, priority, enqueue_time_ms, job_id)
    })
}

/// Parsed status-segmented enqueue-time index key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedStatusEnqueueTimeIndexKey {
    pub tenant: String,
    pub segment: ListingSegment,
    pub priority: u8,
    pub inverted_enqueue_time: u64,
    pub job_id: String,
}

impl ParsedStatusEnqueueTimeIndexKey {
    /// Get the original enqueue time from the inverted value.
    pub fn enqueue_time_ms(&self) -> i64 {
        uninvert_enqueue_time(self.inverted_enqueue_time)
    }
}

/// Parse a status-segmented enqueue-time index key back to its components.
pub fn parse_status_enqueue_time_index_key(key: &[u8]) -> Option<ParsedStatusEnqueueTimeIndexKey> {
    let (tenant, segment, priority, inverted_enqueue_time, job_id): (
        Cow<str>,
        u8,
        u8,
        u64,
        Cow<str>,
    ) = decode_after_prefix(key, prefix::IDX_STATUS_ENQUEUE_TIME)?;
    Some(ParsedStatusEnqueueTimeIndexKey {
        tenant: tenant.into_owned(),
        segment: ListingSegment::from_u8(segment)?,
        priority,
        inverted_enqueue_time,
        job_id: job_id.into_owned(),
    })
}

/// An encoded index value: empty, or one big-endian `i64`. Held inline, so
/// encoding one does not allocate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedIndexValue {
    bytes: [u8; 8],
    len: usize,
}

impl EncodedIndexValue {
    fn new(ms: Option<i64>) -> Self {
        match ms {
            Some(ms) => Self {
                bytes: ms.to_be_bytes(),
                len: 8,
            },
            None => Self {
                bytes: [0; 8],
                len: 0,
            },
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl std::ops::Deref for EncodedIndexValue {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for EncodedIndexValue {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// The value stored under a status-segmented enqueue-time index key: the
/// status record's next-attempt time, present only for the two Scheduled
/// segments. Encoded as empty when absent, else eight big-endian bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusEnqueueTimeIndexValue {
    pub next_attempt_starts_after_ms: Option<i64>,
}

impl StatusEnqueueTimeIndexValue {
    /// The value a job with `status` lists under: its next-attempt time for
    /// a Scheduled status, absent otherwise.
    pub fn for_status(status: &JobStatus) -> Self {
        Self {
            next_attempt_starts_after_ms: (status.kind == JobStatusKind::Scheduled)
                .then_some(status.next_attempt_starts_after_ms)
                .flatten(),
        }
    }

    pub fn encode(self) -> EncodedIndexValue {
        EncodedIndexValue::new(self.next_attempt_starts_after_ms)
    }

    pub fn decode(value: &[u8]) -> Option<Self> {
        let next_attempt_starts_after_ms = match value {
            [] => None,
            bytes => Some(i64::from_be_bytes(bytes.try_into().ok()?)),
        };
        Some(Self {
            next_attempt_starts_after_ms,
        })
    }
}

/// Index: jobs by metadata key/value (unsorted within key/value). The value
/// is a [`MetadataIndexValue`].
pub fn idx_metadata_key(tenant: &str, key: &str, value: &str, job_id: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_METADATA, &(tenant, key, value, job_id))
}

/// The value stored under a metadata index key: the job's `enqueue_time_ms`,
/// so a metadata-filtered listing of ids and enqueue times is answered from
/// the index alone. Encoded as eight big-endian bytes; an entry written
/// before the listing index reached version 3 has an empty value and no
/// enqueue time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataIndexValue {
    pub enqueue_time_ms: Option<i64>,
}

impl MetadataIndexValue {
    pub fn new(enqueue_time_ms: i64) -> Self {
        Self {
            enqueue_time_ms: Some(enqueue_time_ms),
        }
    }

    pub fn encode(self) -> EncodedIndexValue {
        EncodedIndexValue::new(self.enqueue_time_ms)
    }

    pub fn decode(value: &[u8]) -> Option<Self> {
        let enqueue_time_ms = match value {
            [] => None,
            bytes => Some(i64::from_be_bytes(bytes.try_into().ok()?)),
        };
        Some(Self { enqueue_time_ms })
    }
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

/// Prefix covering every metadata index entry of a tenant.
pub fn idx_metadata_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::IDX_METADATA, &(tenant,))
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
    let (tenant, k, value, job_id): (Cow<str>, Cow<str>, Cow<str>, Cow<str>) =
        decode_after_prefix(key, prefix::IDX_METADATA)?;
    Some(ParsedMetadataIndexKey {
        tenant: tenant.into_owned(),
        key: k.into_owned(),
        value: value.into_owned(),
        job_id: job_id.into_owned(),
    })
}

/// Construct the key for a task, ordered by task group then start time.
///
/// `epoch_ms` is a trailing write-time disambiguator that does NOT participate
/// in cross-job ordering (it sorts after every other field). It exists so a
/// chain rewrite can re-emit a task at the same logical `start_time_ms` without
/// colliding with the broker ack-tombstone left by the just-deleted prior key.
/// No reader reconstructs it — point lookups use [`task_key_lookup_prefix`].
pub fn task_key(
    task_group: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
    epoch_ms: i64,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::TASK,
        &(
            task_group,
            start_time_ms.max(0) as u64,
            priority,
            job_id,
            attempt,
            epoch_ms.max(0) as u64,
        ),
    )
}

/// Prefix that uniquely identifies the at-most-one live task for a given
/// `(task_group, start_time_ms, priority, job_id, attempt)`, omitting the
/// trailing `epoch_ms`. Scanning `[P, end_bound(P))` yields that single task
/// regardless of which `epoch_ms` it carries — the lookup path for callers that
/// know a task's identity but not its (write-only) epoch.
pub fn task_key_lookup_prefix(
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

/// Start key for scanning a task group's tasks whose `start_time_ms` is at
/// least the given time: a prefix of every [`task_key`] in the group with that
/// start time, sorting after every key with an earlier one.
pub fn task_group_time_prefix(task_group: &str, start_time_ms: i64) -> Vec<u8> {
    encode_with_prefix(prefix::TASK, &(task_group, start_time_ms.max(0) as u64))
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
    /// Write-time disambiguator; see [`task_key`]. Not part of task identity.
    pub epoch_ms: u64,
}

/// Task key components borrowed from the key bytes. String fields borrow
/// unless they hold escaped bytes, so parsing allocates nothing for the keys
/// silo writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTaskKeyRef<'a> {
    pub task_group: Cow<'a, str>,
    pub start_time_ms: u64,
    pub priority: u8,
    pub job_id: Cow<'a, str>,
    pub attempt: u32,
    /// Write-time disambiguator; see [`task_key`]. Not part of task identity.
    pub epoch_ms: u64,
}

impl ParsedTaskKeyRef<'_> {
    pub fn into_owned(self) -> ParsedTaskKey {
        ParsedTaskKey {
            task_group: self.task_group.into_owned(),
            start_time_ms: self.start_time_ms,
            priority: self.priority,
            job_id: self.job_id.into_owned(),
            attempt: self.attempt,
            epoch_ms: self.epoch_ms,
        }
    }
}

/// Parse a task key back to its components, borrowing from `key`.
pub fn parse_task_key_ref(key: &[u8]) -> Option<ParsedTaskKeyRef<'_>> {
    let (task_group, start_time_ms, priority, job_id, attempt, epoch_ms) =
        decode_after_prefix(key, prefix::TASK)?;
    Some(ParsedTaskKeyRef {
        task_group,
        start_time_ms,
        priority,
        job_id,
        attempt,
        epoch_ms,
    })
}

/// Parse a task key back to its components.
pub fn parse_task_key(key: &[u8]) -> Option<ParsedTaskKey> {
    parse_task_key_ref(key).map(ParsedTaskKeyRef::into_owned)
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
    let (task_id,): (Cow<str>,) = decode_after_prefix(key, prefix::LEASE)?;
    Some(ParsedLeaseKey {
        task_id: task_id.into_owned(),
    })
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
    let (tenant, job_id, attempt): (Cow<str>, Cow<str>, u32) =
        decode_after_prefix(key, prefix::ATTEMPT)?;
    Some(ParsedAttemptKey {
        tenant: tenant.into_owned(),
        job_id: job_id.into_owned(),
        attempt,
    })
}

/// Concurrency request queue key.
/// Ordered by start time (when job should run), then priority (lower = higher), then (job_id, attempt_number, suffix).
/// The composed request_id used downstream is `{job_id}:{attempt_number}:{suffix}`.
///
/// Uses nested tuples to encode 7 fields (storekey supports tuples up to 6 elements).
/// Since storekey encodes tuple elements sequentially, the nested encoding is byte-identical
/// to a flat 7-element encoding.
pub fn concurrency_request_key(
    tenant: &str,
    queue: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
    suffix: &str,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::CONCURRENCY_REQUEST,
        &(
            (tenant, queue, start_time_ms.max(0) as u64, priority),
            (job_id, attempt_number, suffix),
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

/// Prefix for scanning a specific job's concurrency requests within a (tenant, queue, start_time, priority) bucket.
/// Enables narrow O(1) prefix scan instead of scanning the entire queue.
pub fn concurrency_request_job_prefix(
    tenant: &str,
    queue: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
) -> Vec<u8> {
    encode_with_prefix(
        prefix::CONCURRENCY_REQUEST,
        &(
            tenant,
            queue,
            start_time_ms.max(0) as u64,
            priority,
            job_id,
            attempt_number,
        ),
    )
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
    pub job_id: String,
    pub attempt_number: u32,
    pub suffix: String,
}

impl ParsedConcurrencyRequestKey {
    /// Compose the request_id used downstream (holder keys, task IDs).
    pub fn request_id(&self) -> String {
        format!("{}:{}:{}", self.job_id, self.attempt_number, self.suffix)
    }
}

/// Parse a concurrency request key back to its components.
/// Decodes 7 fields using nested tuples (storekey supports tuples up to 6 elements,
/// but nested tuples encode identically to flat tuples since encoding is sequential).
#[allow(clippy::type_complexity)]
pub fn parse_concurrency_request_key(key: &[u8]) -> Option<ParsedConcurrencyRequestKey> {
    let ((tenant, queue, start_time_ms, priority), (job_id, attempt_number, suffix)): (
        (Cow<str>, Cow<str>, u64, u8),
        (Cow<str>, u32, Cow<str>),
    ) = decode_after_prefix(key, prefix::CONCURRENCY_REQUEST)?;
    Some(ParsedConcurrencyRequestKey {
        tenant: tenant.into_owned(),
        queue: queue.into_owned(),
        start_time_ms,
        priority,
        job_id: job_id.into_owned(),
        attempt_number,
        suffix: suffix.into_owned(),
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
    let (tenant, queue, task_id): (Cow<str>, Cow<str>, Cow<str>) =
        decode_after_prefix(key, prefix::CONCURRENCY_HOLDER)?;
    Some(ParsedConcurrencyHolderKey {
        tenant: tenant.into_owned(),
        queue: queue.into_owned(),
        task_id: task_id.into_owned(),
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
    let (tenant, job_id): (Cow<str>, Cow<str>) = decode_after_prefix(key, prefix::JOB_CANCELLED)?;
    Some(ParsedJobCancelledKey {
        tenant: tenant.into_owned(),
        job_id: job_id.into_owned(),
    })
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
    let (tenant, queue_key): (Cow<str>, Cow<str>) =
        decode_after_prefix(key, prefix::FLOATING_LIMIT)?;
    Some(ParsedFloatingLimitKey {
        tenant: tenant.into_owned(),
        queue_key: queue_key.into_owned(),
    })
}

/// The refresh index row for a floating queue's pending refresh task. One
/// row per queue per task group; a drain for `task_group` scans its prefix
/// and hands every claimable row to the polling worker, independent of the
/// group's job backlog in the task line.
pub fn refresh_task_key(task_group: &str, tenant: &str, queue_key: &str) -> Vec<u8> {
    encode_with_prefix(prefix::REFRESH_TASK, &(task_group, tenant, queue_key))
}

/// Prefix for scanning the refresh index rows of one task group.
pub fn refresh_task_group_prefix(task_group: &str) -> Vec<u8> {
    encode_with_prefix(prefix::REFRESH_TASK, &(task_group,))
}

/// Prefix for scanning the refresh index rows of one tenant within a task
/// group.
pub fn refresh_task_tenant_prefix(task_group: &str, tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::REFRESH_TASK, &(task_group, tenant))
}

/// Prefix for scanning every refresh index row.
pub fn refresh_tasks_prefix() -> Vec<u8> {
    vec![prefix::REFRESH_TASK]
}

/// Parsed refresh index key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRefreshTaskKey {
    pub task_group: String,
    pub tenant: String,
    pub queue_key: String,
}

/// Parse a refresh index key back to its components.
pub fn parse_refresh_task_key(key: &[u8]) -> Option<ParsedRefreshTaskKey> {
    let (task_group, tenant, queue_key): (Cow<str>, Cow<str>, Cow<str>) =
        decode_after_prefix(key, prefix::REFRESH_TASK)?;
    Some(ParsedRefreshTaskKey {
        task_group: task_group.into_owned(),
        tenant: tenant.into_owned(),
        queue_key: queue_key.into_owned(),
    })
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

/// Key for the enqueue-time index backfill sweep's progress checkpoint.
pub fn enqueue_time_index_backfill_progress_key() -> Vec<u8> {
    vec![prefix::ENQUEUE_TIME_INDEX_BACKFILL_PROGRESS]
}

/// Key for the marker recording that the enqueue-time index backfill sweep
/// has completed on this shard.
pub fn enqueue_time_index_backfill_complete_key() -> Vec<u8> {
    vec![prefix::ENQUEUE_TIME_INDEX_BACKFILL_COMPLETE]
}

/// Key the holder reconciler overwrites before its point reads. SlateDB
/// applies batches from one writer loop in submission order, so once this
/// write is applied every batch submitted before it is visible to reads.
pub fn holder_reconcile_barrier_key() -> Vec<u8> {
    vec![prefix::HOLDER_RECONCILE_BARRIER]
}

/// Key for the per-queue concurrency requester counter.
/// Tracks the number of pending concurrency requests for a specific tenant/queue.
pub fn concurrency_requester_counter_key(tenant: &str, queue: &str) -> Vec<u8> {
    encode_with_prefix(prefix::COUNTER_CONCURRENCY_REQUESTERS, &(tenant, queue))
}

/// Prefix for scanning all concurrency requester counters for a specific tenant.
pub fn concurrency_requester_counter_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::COUNTER_CONCURRENCY_REQUESTERS, &(tenant,))
}

/// Parsed concurrency requester counter key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedConcurrencyRequesterCounterKey {
    pub tenant: String,
    pub queue: String,
}

/// Parse a concurrency requester counter key back to its components.
pub fn parse_concurrency_requester_counter_key(
    key: &[u8],
) -> Option<ParsedConcurrencyRequesterCounterKey> {
    let (tenant, queue): (Cow<str>, Cow<str>) =
        decode_after_prefix(key, prefix::COUNTER_CONCURRENCY_REQUESTERS)?;
    Some(ParsedConcurrencyRequesterCounterKey {
        tenant: tenant.into_owned(),
        queue: queue.into_owned(),
    })
}

/// Construct a composite key for in-memory concurrency queue tracking.
/// Uses storekey encoding for collision-safe (tenant, queue) pairing.
pub fn concurrency_counts_key(tenant: &str, queue: &str) -> Vec<u8> {
    encode_exact(None, &(tenant, queue))
}

/// Key for a per-tenant, per-status counter.
/// Tracks the number of jobs for each (tenant, status_kind) combination.
pub fn tenant_status_counter_key(tenant: &str, status_kind: &str) -> Vec<u8> {
    encode_with_prefix(prefix::COUNTER_TENANT_STATUS, &(tenant, status_kind))
}

/// Prefix for scanning all tenant status counters.
pub fn tenant_status_counter_prefix() -> Vec<u8> {
    vec![prefix::COUNTER_TENANT_STATUS]
}

/// Prefix for scanning all tenant status counters for a specific tenant.
pub fn tenant_status_counter_tenant_prefix(tenant: &str) -> Vec<u8> {
    encode_with_prefix(prefix::COUNTER_TENANT_STATUS, &(tenant,))
}

/// Parsed tenant status counter key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTenantStatusCounterKey {
    pub tenant: String,
    pub status_kind: String,
}

/// Parse a tenant status counter key back to its components.
pub fn parse_tenant_status_counter_key(key: &[u8]) -> Option<ParsedTenantStatusCounterKey> {
    let (tenant, status_kind): (Cow<str>, Cow<str>) =
        decode_after_prefix(key, prefix::COUNTER_TENANT_STATUS)?;
    Some(ParsedTenantStatusCounterKey {
        tenant: tenant.into_owned(),
        status_kind: status_kind.into_owned(),
    })
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
