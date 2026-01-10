use rkyv::Deserialize as RkyvDeserialize;
use silo::codec::{
    decode_attempt, decode_concurrency_action, decode_holder, decode_job_status, decode_lease,
    decode_remote_concurrency_request, decode_remote_holder_record, decode_task, encode_attempt,
    encode_concurrency_action, encode_holder, encode_job_status, encode_lease,
    encode_remote_concurrency_request, encode_remote_holder_record, encode_task, CodecError,
    CONCURRENCY_ACTION_VERSION, HOLDER_RECORD_VERSION, JOB_ATTEMPT_VERSION, JOB_STATUS_VERSION,
    LEASE_RECORD_VERSION, REMOTE_CONCURRENCY_REQUEST_VERSION, REMOTE_HOLDER_RECORD_VERSION,
    TASK_VERSION,
};
use silo::job::{JobStatus, JobStatusKind};
use silo::job_attempt::{AttemptStatus, JobAttempt};
use silo::task::{
    ConcurrencyAction, HolderRecord, LeaseRecord, RemoteConcurrencyRequest, RemoteHolderRecord,
    Task,
};

#[test]
fn test_task_roundtrip() {
    let task = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "tenant-1".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        held_queues: vec!["queue-1".to_string()],
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::RunAttempt {
            id,
            tenant,
            job_id,
            attempt_number,
            held_queues,
        } => {
            assert_eq!(id, "task-1");
            assert_eq!(tenant, "tenant-1");
            assert_eq!(job_id, "job-1");
            assert_eq!(attempt_number, 1);
            assert_eq!(held_queues, vec!["queue-1"]);
        }
        _ => panic!("unexpected task variant"),
    }
}

#[test]
fn test_version_mismatch() {
    let task = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "-".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        held_queues: vec![],
    };
    let mut encoded = encode_task(&task).unwrap();
    encoded[0] = 99; // Wrong version
    let result = decode_task(&encoded);
    assert!(matches!(
        result,
        Err(CodecError::UnsupportedVersion {
            expected: 1,
            found: 99
        })
    ));
}

#[test]
fn test_empty_data() {
    let result = decode_task(&[]);
    assert!(matches!(result, Err(CodecError::TooShort)));
}

#[test]
fn test_lease_roundtrip() {
    let lease = LeaseRecord {
        worker_id: "worker-1".to_string(),
        task: Task::RunAttempt {
            id: "task-1".to_string(),
            tenant: "-".to_string(),
            job_id: "job-1".to_string(),
            attempt_number: 1,
            held_queues: vec![],
        },
        expiry_ms: 12345,
    };
    let encoded = encode_lease(&lease).unwrap();
    assert_eq!(encoded[0], LEASE_RECORD_VERSION);
    let decoded = decode_lease(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.worker_id.as_str(), "worker-1");
    assert_eq!(archived.expiry_ms, 12345);
}

#[test]
fn test_job_attempt_roundtrip() {
    let attempt = JobAttempt {
        job_id: "job-1".to_string(),
        attempt_number: 1,
        task_id: "task-1".to_string(),
        status: AttemptStatus::Running {
            started_at_ms: 1000,
        },
    };
    let encoded = encode_attempt(&attempt).unwrap();
    assert_eq!(encoded[0], JOB_ATTEMPT_VERSION);
    let decoded = decode_attempt(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.job_id.as_str(), "job-1");
    assert_eq!(archived.attempt_number, 1);
}

#[test]
fn test_job_status_roundtrip() {
    let status = JobStatus::running(5000);
    let encoded = encode_job_status(&status).unwrap();
    assert_eq!(encoded[0], JOB_STATUS_VERSION);
    let decoded = decode_job_status(&encoded).unwrap();
    let archived = decoded.archived();
    // Use deserialize to get back the original type for comparison
    let mut des = rkyv::Infallible;
    let deserialized: JobStatus = RkyvDeserialize::deserialize(archived, &mut des)
        .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus"));
    assert_eq!(deserialized.kind, JobStatusKind::Running);
    assert_eq!(deserialized.changed_at_ms, 5000i64);
}

#[test]
fn test_holder_roundtrip() {
    let holder = HolderRecord {
        granted_at_ms: 9999,
    };
    let encoded = encode_holder(&holder).unwrap();
    assert_eq!(encoded[0], HOLDER_RECORD_VERSION);
    let decoded = decode_holder(&encoded).unwrap();
    assert_eq!(decoded.archived().granted_at_ms, 9999);
}

#[test]
fn test_concurrency_action_roundtrip() {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms: 1000,
        priority: 5,
        job_id: "job-1".to_string(),
        attempt_number: 1,
    };
    let encoded = encode_concurrency_action(&action).unwrap();
    assert_eq!(encoded[0], CONCURRENCY_ACTION_VERSION);
    let decoded = decode_concurrency_action(&encoded).unwrap();
    match decoded.archived() {
        silo::task::ArchivedConcurrencyAction::EnqueueTask {
            start_time_ms,
            priority,
            job_id,
            attempt_number,
        } => {
            assert_eq!(*start_time_ms, 1000);
            assert_eq!(*priority, 5);
            assert_eq!(job_id.as_str(), "job-1");
            assert_eq!(*attempt_number, 1);
        }
    }
}

// ============================================================================
// Cross-Shard Task Type Codec Tests
// ============================================================================

#[test]
fn test_request_remote_ticket_task_roundtrip() {
    let task = Task::RequestRemoteTicket {
        task_id: "task-remote-1".to_string(),
        queue_owner_shard: 2,
        tenant: "tenant-1".to_string(),
        queue_key: "api-limit".to_string(),
        job_id: "job-123".to_string(),
        attempt_number: 1,
        priority: 5,
        start_time_ms: 1704067200000,
        request_id: "req-abc".to_string(),
        floating_limit: None,
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::RequestRemoteTicket {
            task_id,
            queue_owner_shard,
            tenant,
            queue_key,
            job_id,
            attempt_number,
            priority,
            start_time_ms,
            request_id,
            floating_limit,
        } => {
            assert_eq!(task_id, "task-remote-1");
            assert_eq!(queue_owner_shard, 2);
            assert_eq!(tenant, "tenant-1");
            assert_eq!(queue_key, "api-limit");
            assert_eq!(job_id, "job-123");
            assert_eq!(attempt_number, 1);
            assert_eq!(priority, 5);
            assert_eq!(start_time_ms, 1704067200000);
            assert_eq!(request_id, "req-abc");
            assert!(floating_limit.is_none());
        }
        _ => panic!("unexpected task variant"),
    }
}

#[test]
fn test_notify_remote_ticket_grant_task_roundtrip() {
    let task = Task::NotifyRemoteTicketGrant {
        task_id: "task-notify-1".to_string(),
        job_shard: 0,
        tenant: "tenant-1".to_string(),
        job_id: "job-456".to_string(),
        queue_key: "api-limit".to_string(),
        request_id: "req-xyz".to_string(),
        holder_task_id: "holder-789".to_string(),
        attempt_number: 1,
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::NotifyRemoteTicketGrant {
            task_id,
            job_shard,
            tenant,
            job_id,
            queue_key,
            request_id,
            holder_task_id,
            attempt_number,
        } => {
            assert_eq!(task_id, "task-notify-1");
            assert_eq!(job_shard, 0);
            assert_eq!(tenant, "tenant-1");
            assert_eq!(job_id, "job-456");
            assert_eq!(queue_key, "api-limit");
            assert_eq!(request_id, "req-xyz");
            assert_eq!(holder_task_id, "holder-789");
            assert_eq!(attempt_number, 1);
        }
        _ => panic!("unexpected task variant"),
    }
}

#[test]
fn test_release_remote_ticket_task_roundtrip() {
    let task = Task::ReleaseRemoteTicket {
        task_id: "task-release-1".to_string(),
        queue_owner_shard: 3,
        tenant: "tenant-2".to_string(),
        queue_key: "db-connections".to_string(),
        job_id: "job-789".to_string(),
        holder_task_id: "holder-abc".to_string(),
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::ReleaseRemoteTicket {
            task_id,
            queue_owner_shard,
            tenant,
            queue_key,
            job_id,
            holder_task_id,
        } => {
            assert_eq!(task_id, "task-release-1");
            assert_eq!(queue_owner_shard, 3);
            assert_eq!(tenant, "tenant-2");
            assert_eq!(queue_key, "db-connections");
            assert_eq!(job_id, "job-789");
            assert_eq!(holder_task_id, "holder-abc");
        }
        _ => panic!("unexpected task variant"),
    }
}

// ============================================================================
// Remote Concurrency Struct Codec Tests
// ============================================================================

#[test]
fn test_remote_concurrency_request_roundtrip() {
    let request = RemoteConcurrencyRequest {
        source_shard: 1,
        job_id: "job-remote-1".to_string(),
        request_id: "req-123".to_string(),
        attempt_number: 2,
        holder_task_id: "holder-456".to_string(),
        max_concurrency: 5,
    };
    let encoded = encode_remote_concurrency_request(&request).unwrap();
    assert_eq!(encoded[0], REMOTE_CONCURRENCY_REQUEST_VERSION);
    let decoded = decode_remote_concurrency_request(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.source_shard, 1);
    assert_eq!(archived.job_id.as_str(), "job-remote-1");
    assert_eq!(archived.request_id.as_str(), "req-123");
    assert_eq!(archived.attempt_number, 2);
    assert_eq!(archived.holder_task_id.as_str(), "holder-456");
    assert_eq!(archived.max_concurrency, 5);
}

#[test]
fn test_remote_holder_record_roundtrip() {
    let holder = RemoteHolderRecord {
        granted_at_ms: 1704067200000,
        source_shard: 2,
        job_id: "job-holder-1".to_string(),
    };
    let encoded = encode_remote_holder_record(&holder).unwrap();
    assert_eq!(encoded[0], REMOTE_HOLDER_RECORD_VERSION);
    let decoded = decode_remote_holder_record(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.granted_at_ms, 1704067200000);
    assert_eq!(archived.source_shard, 2);
    assert_eq!(archived.job_id.as_str(), "job-holder-1");
}

#[test]
fn test_remote_concurrency_request_version_mismatch() {
    let request = RemoteConcurrencyRequest {
        source_shard: 0,
        job_id: "job".to_string(),
        request_id: "req".to_string(),
        attempt_number: 1,
        holder_task_id: "holder".to_string(),
        max_concurrency: 1,
    };
    let mut encoded = encode_remote_concurrency_request(&request).unwrap();
    encoded[0] = 99; // Wrong version
    let result = decode_remote_concurrency_request(&encoded);
    assert!(matches!(
        result,
        Err(CodecError::UnsupportedVersion {
            expected: 1,
            found: 99
        })
    ));
}

#[test]
fn test_remote_holder_record_version_mismatch() {
    let holder = RemoteHolderRecord {
        granted_at_ms: 1000,
        source_shard: 0,
        job_id: "job".to_string(),
    };
    let mut encoded = encode_remote_holder_record(&holder).unwrap();
    encoded[0] = 99; // Wrong version
    let result = decode_remote_holder_record(&encoded);
    assert!(matches!(
        result,
        Err(CodecError::UnsupportedVersion {
            expected: 1,
            found: 99
        })
    ));
}

// ============================================================================
// Lease with Cross-Shard Task Tests
// ============================================================================

#[test]
fn test_lease_with_request_remote_ticket_roundtrip() {
    let lease = LeaseRecord {
        worker_id: "internal-worker".to_string(),
        task: Task::RequestRemoteTicket {
            task_id: "internal-task-1".to_string(),
            queue_owner_shard: 1,
            tenant: "tenant".to_string(),
            queue_key: "queue".to_string(),
            job_id: "job".to_string(),
            attempt_number: 1,
            priority: 10,
            start_time_ms: 1000,
            request_id: "req".to_string(),
            floating_limit: None,
        },
        expiry_ms: 5000,
    };
    let encoded = encode_lease(&lease).unwrap();
    let decoded = decode_lease(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.worker_id.as_str(), "internal-worker");
    assert_eq!(archived.expiry_ms, 5000);
}
