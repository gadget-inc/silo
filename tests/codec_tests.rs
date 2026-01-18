use rkyv::Deserialize as RkyvDeserialize;
use silo::codec::{
    decode_attempt, decode_concurrency_action, decode_holder, decode_job_status, decode_lease,
    decode_task, encode_attempt, encode_concurrency_action, encode_holder, encode_job_status,
    encode_lease, encode_task, CodecError, CONCURRENCY_ACTION_VERSION, HOLDER_RECORD_VERSION,
    JOB_ATTEMPT_VERSION, JOB_STATUS_VERSION, LEASE_RECORD_VERSION, TASK_VERSION,
};
use silo::job::{JobStatus, JobStatusKind};
use silo::job_attempt::{AttemptStatus, JobAttempt};
use silo::task::{ConcurrencyAction, HolderRecord, LeaseRecord, Task};

#[test]
fn test_task_roundtrip() {
    let task = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "tenant-1".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        held_queues: vec!["queue-1".to_string()],
        task_group: "default".to_string(),
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
            task_group,
        } => {
            assert_eq!(id, "task-1");
            assert_eq!(tenant, "tenant-1");
            assert_eq!(job_id, "job-1");
            assert_eq!(attempt_number, 1);
            assert_eq!(held_queues, vec!["queue-1"]);
            assert_eq!(task_group, "default");
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
        task_group: "default".to_string(),
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
            task_group: "default".to_string(),
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
        task_group: "default".to_string(),
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
            task_group,
        } => {
            assert_eq!(*start_time_ms, 1000);
            assert_eq!(*priority, 5);
            assert_eq!(job_id.as_str(), "job-1");
            assert_eq!(*attempt_number, 1);
            assert_eq!(task_group.as_str(), "default");
        }
    }
}
