use rkyv::Deserialize as RkyvDeserialize;
use silo::codec::{
    CONCURRENCY_ACTION_VERSION, CodecError, FLOATING_LIMIT_STATE_VERSION, HOLDER_RECORD_VERSION,
    JOB_ATTEMPT_VERSION, JOB_CANCELLATION_VERSION, JOB_INFO_VERSION, JOB_STATUS_VERSION,
    LEASE_RECORD_VERSION, TASK_VERSION, decode_attempt, decode_concurrency_action,
    decode_floating_limit_state, decode_holder, decode_job_cancellation, decode_job_info,
    decode_job_status, decode_lease, decode_task, encode_attempt, encode_concurrency_action,
    encode_floating_limit_state, encode_holder, encode_job_cancellation, encode_job_info,
    encode_job_status, encode_lease, encode_task,
};
use silo::job::{FloatingLimitState, JobCancellation, JobInfo, JobStatus, JobStatusKind};
use silo::job_attempt::{AttemptStatus, JobAttempt};
use silo::task::{ConcurrencyAction, GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

#[silo::test]
fn test_task_roundtrip() {
    let task = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "tenant-1".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
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
            relative_attempt_number,
            held_queues,
            task_group,
        } => {
            assert_eq!(id, "task-1");
            assert_eq!(tenant, "tenant-1");
            assert_eq!(job_id, "job-1");
            assert_eq!(attempt_number, 1);
            assert_eq!(relative_attempt_number, 1);
            assert_eq!(held_queues, vec!["queue-1"]);
            assert_eq!(task_group, "default");
        }
        _ => panic!("unexpected task variant"),
    }
}

#[silo::test]
fn test_version_mismatch() {
    let task = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "-".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
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

#[silo::test]
fn test_empty_data() {
    let result = decode_task(&[]);
    assert!(matches!(result, Err(CodecError::TooShort)));
}

#[silo::test]
fn test_lease_roundtrip() {
    let lease = LeaseRecord {
        worker_id: "worker-1".to_string(),
        task: Task::RunAttempt {
            id: "task-1".to_string(),
            tenant: "-".to_string(),
            job_id: "job-1".to_string(),
            attempt_number: 1,
            relative_attempt_number: 1,
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

#[silo::test]
fn test_job_attempt_roundtrip() {
    let attempt = JobAttempt {
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
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

#[silo::test]
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

#[silo::test]
fn test_holder_roundtrip() {
    let holder = HolderRecord {
        granted_at_ms: 9999,
    };
    let encoded = encode_holder(&holder).unwrap();
    assert_eq!(encoded[0], HOLDER_RECORD_VERSION);
    let decoded = decode_holder(&encoded).unwrap();
    assert_eq!(decoded.archived().granted_at_ms, 9999);
}

#[silo::test]
fn test_concurrency_action_roundtrip() {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms: 1000,
        priority: 5,
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
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
            relative_attempt_number,
            task_group,
        } => {
            assert_eq!(*start_time_ms, 1000);
            assert_eq!(*priority, 5);
            assert_eq!(job_id.as_str(), "job-1");
            assert_eq!(*attempt_number, 1);
            assert_eq!(*relative_attempt_number, 1);
            assert_eq!(task_group.as_str(), "default");
        }
    }
}

#[silo::test]
fn test_request_ticket_roundtrip() {
    let task = Task::RequestTicket {
        queue: "q1".to_string(),
        start_time_ms: 5000,
        priority: 3,
        tenant: "tenant-1".to_string(),
        job_id: "job-42".to_string(),
        attempt_number: 2,
        relative_attempt_number: 1,
        request_id: "req-abc".to_string(),
        task_group: "fast".to_string(),
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::RequestTicket {
            queue,
            start_time_ms,
            priority,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            request_id,
            task_group,
        } => {
            assert_eq!(queue, "q1");
            assert_eq!(start_time_ms, 5000);
            assert_eq!(priority, 3);
            assert_eq!(tenant, "tenant-1");
            assert_eq!(job_id, "job-42");
            assert_eq!(attempt_number, 2);
            assert_eq!(relative_attempt_number, 1);
            assert_eq!(request_id, "req-abc");
            assert_eq!(task_group, "fast");
        }
        _ => panic!("expected RequestTicket variant"),
    }
}

#[silo::test]
fn test_check_rate_limit_roundtrip() {
    let task = Task::CheckRateLimit {
        task_id: "task-99".to_string(),
        tenant: "tenant-2".to_string(),
        job_id: "job-7".to_string(),
        attempt_number: 3,
        relative_attempt_number: 2,
        limit_index: 1,
        rate_limit: GubernatorRateLimitData {
            name: "api-limit".to_string(),
            unique_key: "user-123".to_string(),
            limit: 100,
            duration_ms: 60000,
            hits: 1,
            algorithm: 0,
            behavior: 2,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 5000,
            retry_backoff_multiplier: 2.0,
            retry_max_retries: 5,
        },
        retry_count: 1,
        started_at_ms: 9000,
        priority: 7,
        held_queues: vec!["hq-1".to_string(), "hq-2".to_string()],
        task_group: "default".to_string(),
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::CheckRateLimit {
            task_id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            limit_index,
            rate_limit,
            retry_count,
            started_at_ms,
            priority,
            held_queues,
            task_group,
        } => {
            assert_eq!(task_id, "task-99");
            assert_eq!(tenant, "tenant-2");
            assert_eq!(job_id, "job-7");
            assert_eq!(attempt_number, 3);
            assert_eq!(relative_attempt_number, 2);
            assert_eq!(limit_index, 1);
            assert_eq!(rate_limit.name, "api-limit");
            assert_eq!(rate_limit.unique_key, "user-123");
            assert_eq!(rate_limit.limit, 100);
            assert_eq!(rate_limit.duration_ms, 60000);
            assert_eq!(rate_limit.hits, 1);
            assert_eq!(rate_limit.algorithm, 0);
            assert_eq!(rate_limit.behavior, 2);
            assert_eq!(rate_limit.retry_initial_backoff_ms, 100);
            assert_eq!(rate_limit.retry_max_backoff_ms, 5000);
            assert_eq!(rate_limit.retry_backoff_multiplier, 2.0);
            assert_eq!(rate_limit.retry_max_retries, 5);
            assert_eq!(retry_count, 1);
            assert_eq!(started_at_ms, 9000);
            assert_eq!(priority, 7);
            assert_eq!(held_queues, vec!["hq-1", "hq-2"]);
            assert_eq!(task_group, "default");
        }
        _ => panic!("expected CheckRateLimit variant"),
    }
}

#[silo::test]
fn test_refresh_floating_limit_roundtrip() {
    let task = Task::RefreshFloatingLimit {
        task_id: "refresh-1".to_string(),
        tenant: "tenant-3".to_string(),
        queue_key: "queue-key-abc".to_string(),
        current_max_concurrency: 50,
        last_refreshed_at_ms: 123456,
        metadata: vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ],
        task_group: "workers".to_string(),
    };
    let encoded = encode_task(&task).unwrap();
    assert_eq!(encoded[0], TASK_VERSION);
    let decoded = decode_task(&encoded).unwrap();
    match decoded {
        Task::RefreshFloatingLimit {
            task_id,
            tenant,
            queue_key,
            current_max_concurrency,
            last_refreshed_at_ms,
            metadata,
            task_group,
        } => {
            assert_eq!(task_id, "refresh-1");
            assert_eq!(tenant, "tenant-3");
            assert_eq!(queue_key, "queue-key-abc");
            assert_eq!(current_max_concurrency, 50);
            assert_eq!(last_refreshed_at_ms, 123456);
            assert_eq!(metadata.len(), 2);
            assert_eq!(metadata[0], ("key1".to_string(), "val1".to_string()));
            assert_eq!(metadata[1], ("key2".to_string(), "val2".to_string()));
            assert_eq!(task_group, "workers");
        }
        _ => panic!("expected RefreshFloatingLimit variant"),
    }
}

#[silo::test]
fn test_job_info_roundtrip() {
    let job_info = JobInfo {
        id: "job-info-1".to_string(),
        priority: 42,
        enqueue_time_ms: 1700000000000,
        payload: vec![1, 2, 3, 4],
        retry_policy: None,
        metadata: vec![("env".to_string(), "prod".to_string())],
        limits: vec![],
        task_group: "default".to_string(),
    };
    let encoded = encode_job_info(&job_info).unwrap();
    assert_eq!(encoded[0], JOB_INFO_VERSION);
    let decoded = decode_job_info(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.id.as_str(), "job-info-1");
    assert_eq!(archived.priority, 42);
    assert_eq!(archived.enqueue_time_ms, 1700000000000);
    assert_eq!(archived.payload.as_slice(), &[1, 2, 3, 4]);
    assert_eq!(archived.metadata.len(), 1);
    assert_eq!(archived.task_group.as_str(), "default");
}

#[silo::test]
fn test_job_cancellation_roundtrip() {
    let cancellation = JobCancellation {
        cancelled_at_ms: 9999999,
    };
    let encoded = encode_job_cancellation(&cancellation).unwrap();
    assert_eq!(encoded[0], JOB_CANCELLATION_VERSION);
    let decoded = decode_job_cancellation(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.cancelled_at_ms, 9999999);
}

#[silo::test]
fn test_floating_limit_state_roundtrip() {
    let state = FloatingLimitState {
        current_max_concurrency: 10,
        last_refreshed_at_ms: 5000,
        refresh_task_scheduled: true,
        refresh_interval_ms: 30000,
        default_max_concurrency: 5,
        retry_count: 2,
        next_retry_at_ms: Some(8000),
        metadata: vec![("source".to_string(), "api".to_string())],
    };
    let encoded = encode_floating_limit_state(&state).unwrap();
    assert_eq!(encoded[0], FLOATING_LIMIT_STATE_VERSION);
    let decoded = decode_floating_limit_state(&encoded).unwrap();
    let archived = decoded.archived();
    assert_eq!(archived.current_max_concurrency, 10);
    assert_eq!(archived.last_refreshed_at_ms, 5000);
    assert_eq!(archived.refresh_task_scheduled, true);
    assert_eq!(archived.refresh_interval_ms, 30000);
    assert_eq!(archived.default_max_concurrency, 5);
    assert_eq!(archived.retry_count, 2);
    assert_eq!(archived.metadata.len(), 1);
}

#[silo::test]
fn test_decoded_lease_accessors_run_attempt() {
    let lease = LeaseRecord {
        worker_id: "w1".to_string(),
        task: Task::RunAttempt {
            id: "task-10".to_string(),
            tenant: "t1".to_string(),
            job_id: "j1".to_string(),
            attempt_number: 3,
            relative_attempt_number: 2,
            held_queues: vec!["q1".to_string(), "q2".to_string()],
            task_group: "grp".to_string(),
        },
        expiry_ms: 50000,
    };
    let encoded = encode_lease(&lease).unwrap();
    let decoded = decode_lease(&encoded).unwrap();

    assert_eq!(decoded.worker_id(), "w1");
    assert_eq!(decoded.expiry_ms(), 50000);
    assert_eq!(decoded.tenant(), "t1");
    assert_eq!(decoded.job_id(), "j1");
    assert_eq!(decoded.attempt_number(), 3);
    assert_eq!(decoded.relative_attempt_number(), 2);
    assert_eq!(decoded.held_queues(), vec!["q1", "q2"]);
    assert_eq!(decoded.task_group(), "grp");
    assert_eq!(decoded.task_id(), Some("task-10"));

    // to_task should reconstruct the original task
    let task = decoded.to_task();
    match task {
        Task::RunAttempt { id, job_id, .. } => {
            assert_eq!(id, "task-10");
            assert_eq!(job_id, "j1");
        }
        _ => panic!("expected RunAttempt"),
    }
}

#[silo::test]
fn test_decoded_lease_accessors_refresh_floating_limit() {
    let lease = LeaseRecord {
        worker_id: "w2".to_string(),
        task: Task::RefreshFloatingLimit {
            task_id: "refresh-5".to_string(),
            tenant: "t2".to_string(),
            queue_key: "qk".to_string(),
            current_max_concurrency: 25,
            last_refreshed_at_ms: 7777,
            metadata: vec![("a".to_string(), "b".to_string())],
            task_group: "workers".to_string(),
        },
        expiry_ms: 60000,
    };
    let encoded = encode_lease(&lease).unwrap();
    let decoded = decode_lease(&encoded).unwrap();

    // RefreshFloatingLimit returns "" for job_id and 0 for attempt_number
    assert_eq!(decoded.tenant(), "t2");
    assert_eq!(decoded.job_id(), "");
    assert_eq!(decoded.attempt_number(), 0);
    assert_eq!(decoded.relative_attempt_number(), 0);
    assert_eq!(decoded.held_queues(), Vec::<String>::new());
    assert_eq!(decoded.task_group(), "workers");
    assert_eq!(decoded.task_id(), None);

    // refresh_floating_limit_info should return Some
    let (task_id, queue_key) = decoded.refresh_floating_limit_info().unwrap();
    assert_eq!(task_id, "refresh-5");
    assert_eq!(queue_key, "qk");

    // to_task should reconstruct
    let task = decoded.to_task();
    match task {
        Task::RefreshFloatingLimit {
            task_id,
            current_max_concurrency,
            ..
        } => {
            assert_eq!(task_id, "refresh-5");
            assert_eq!(current_max_concurrency, 25);
        }
        _ => panic!("expected RefreshFloatingLimit"),
    }
}
