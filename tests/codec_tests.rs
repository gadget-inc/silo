use silo::codec::{
    decode_attempt, decode_cancellation_at_ms, decode_concurrency_action,
    decode_floating_limit_state, decode_holder_granted_at_ms, decode_job_info,
    decode_job_status_owned, decode_lease, decode_task, decode_task_validated, encode_attempt,
    encode_concurrency_action, encode_floating_limit_state, encode_holder, encode_job_cancellation,
    encode_job_info, encode_job_status, encode_lease, encode_lease_from_task_bytes,
    encode_lease_with_new_expiry, encode_task,
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
    let encoded = encode_task(&task);
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
fn test_invalid_data() {
    let result = decode_task(&[0xFF, 0xFF]);
    assert!(result.is_err());
}

#[silo::test]
fn test_empty_data() {
    let result = decode_task(&[]);
    assert!(result.is_err());
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
        started_at_ms: 10000,
    };
    let encoded = encode_lease(&lease);
    let decoded = decode_lease(encoded).unwrap();
    assert_eq!(decoded.worker_id(), "worker-1");
    assert_eq!(decoded.expiry_ms(), 12345);
    assert_eq!(decoded.started_at_ms(), 10000);
}

#[silo::test]
fn test_job_attempt_roundtrip() {
    let attempt = JobAttempt {
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_id: "task-1".to_string(),
        started_at_ms: 1000,
        status: AttemptStatus::Running,
    };
    let encoded = encode_attempt(&attempt);
    let decoded = decode_attempt(encoded).unwrap();
    let fb = decoded.fb();
    assert_eq!(fb.job_id().unwrap(), "job-1");
    assert_eq!(fb.attempt_number(), 1);
}

#[silo::test]
fn test_job_status_roundtrip() {
    let status = JobStatus::running(5000);
    let encoded = encode_job_status(&status);
    let decoded = decode_job_status_owned(&encoded).unwrap();
    assert_eq!(decoded.kind, JobStatusKind::Running);
    assert_eq!(decoded.changed_at_ms, 5000i64);
}

#[silo::test]
fn test_holder_roundtrip() {
    let holder = HolderRecord {
        granted_at_ms: 9999,
    };
    let encoded = encode_holder(&holder);
    let granted_at_ms = decode_holder_granted_at_ms(&encoded).unwrap();
    assert_eq!(granted_at_ms, 9999);
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
    let encoded = encode_concurrency_action(&action);
    let decoded = decode_concurrency_action(encoded).unwrap();
    let et = decoded.fb().variant_as_enqueue_task().unwrap();
    assert_eq!(et.start_time_ms(), 1000);
    assert_eq!(et.priority(), 5);
    assert_eq!(et.job_id().unwrap(), "job-1");
    assert_eq!(et.attempt_number(), 1);
    assert_eq!(et.relative_attempt_number(), 1);
    assert_eq!(et.task_group().unwrap(), "default");
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
    let encoded = encode_task(&task);
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
    let encoded = encode_task(&task);
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
    let encoded = encode_task(&task);
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
    let encoded = encode_job_info(&job_info);
    let decoded = decode_job_info(encoded).unwrap();
    let fb = decoded.fb();
    assert_eq!(fb.id().unwrap(), "job-info-1");
    assert_eq!(fb.priority(), 42);
    assert_eq!(fb.enqueue_time_ms(), 1700000000000);
    assert_eq!(fb.payload().unwrap().bytes(), &[1, 2, 3, 4]);
    assert_eq!(fb.metadata().unwrap().len(), 1);
    assert_eq!(fb.task_group().unwrap(), "default");
}

#[silo::test]
fn test_job_cancellation_roundtrip() {
    let cancellation = JobCancellation {
        cancelled_at_ms: 9999999,
    };
    let encoded = encode_job_cancellation(&cancellation);
    let cancelled_at_ms = decode_cancellation_at_ms(&encoded).unwrap();
    assert_eq!(cancelled_at_ms, 9999999);
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
    let encoded = encode_floating_limit_state(&state);
    let decoded = decode_floating_limit_state(encoded).unwrap();
    assert_eq!(decoded.current_max_concurrency(), 10);
    assert_eq!(decoded.last_refreshed_at_ms(), 5000);
    assert_eq!(decoded.refresh_task_scheduled(), true);
    assert_eq!(decoded.refresh_interval_ms(), 30000);
    assert_eq!(decoded.default_max_concurrency(), 5);
    assert_eq!(decoded.retry_count(), 2);
    assert_eq!(decoded.metadata().len(), 1);
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
        started_at_ms: 40000,
    };
    let encoded = encode_lease(&lease);
    let decoded = decode_lease(encoded).unwrap();

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
    let task = decoded.to_task().unwrap();
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
        started_at_ms: 0,
    };
    let encoded = encode_lease(&lease);
    let decoded = decode_lease(encoded).unwrap();

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
    let task = decoded.to_task().unwrap();
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

// ---------------------------------------------------------------------------
// DecodedTask (zero-copy wrapper) tests
// ---------------------------------------------------------------------------

#[silo::test]
fn test_decoded_task_run_attempt() {
    let task = Task::RunAttempt {
        id: "task-zc-1".to_string(),
        tenant: "tenant-zc".to_string(),
        job_id: "job-zc-1".to_string(),
        attempt_number: 5,
        relative_attempt_number: 3,
        held_queues: vec!["q1".to_string(), "q2".to_string()],
        task_group: "default".to_string(),
    };
    let encoded = encode_task(&task);
    let decoded = decode_task_validated(encoded).unwrap();

    assert_eq!(decoded.tenant(), "tenant-zc");
    assert_eq!(decoded.task_group(), "default");

    let ra = decoded.as_run_attempt().unwrap();
    assert_eq!(ra.id().unwrap(), "task-zc-1");
    assert_eq!(ra.job_id().unwrap(), "job-zc-1");
    assert_eq!(ra.attempt_number(), 5);
    assert_eq!(ra.relative_attempt_number(), 3);
    assert_eq!(ra.held_queues().unwrap().len(), 2);

    // to_task should roundtrip
    let owned = decoded.to_task().unwrap();
    match owned {
        Task::RunAttempt { id, tenant, .. } => {
            assert_eq!(id, "task-zc-1");
            assert_eq!(tenant, "tenant-zc");
        }
        _ => panic!("expected RunAttempt"),
    }
}

#[silo::test]
fn test_decoded_task_request_ticket() {
    let task = Task::RequestTicket {
        queue: "q-rt".to_string(),
        start_time_ms: 9999,
        priority: 2,
        tenant: "t-rt".to_string(),
        job_id: "j-rt".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        request_id: "req-rt".to_string(),
        task_group: "fast".to_string(),
    };
    let encoded = encode_task(&task);
    let decoded = decode_task_validated(encoded).unwrap();

    assert_eq!(decoded.tenant(), "t-rt");
    assert_eq!(decoded.task_group(), "fast");
    assert!(decoded.as_request_ticket().is_some());
    assert!(decoded.as_run_attempt().is_none());
}

#[silo::test]
fn test_decoded_task_check_rate_limit() {
    let task = Task::CheckRateLimit {
        task_id: "crl-1".to_string(),
        tenant: "t-crl".to_string(),
        job_id: "j-crl".to_string(),
        attempt_number: 2,
        relative_attempt_number: 1,
        limit_index: 0,
        rate_limit: GubernatorRateLimitData {
            name: "rl".to_string(),
            unique_key: "uk".to_string(),
            limit: 10,
            duration_ms: 1000,
            hits: 1,
            algorithm: 0,
            behavior: 0,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 5000,
            retry_backoff_multiplier: 2.0,
            retry_max_retries: 3,
        },
        retry_count: 0,
        started_at_ms: 5000,
        priority: 1,
        held_queues: vec![],
        task_group: "default".to_string(),
    };
    let encoded = encode_task(&task);
    let decoded = decode_task_validated(encoded).unwrap();

    assert_eq!(decoded.tenant(), "t-crl");
    assert!(decoded.as_check_rate_limit().is_some());
}

#[silo::test]
fn test_decoded_task_refresh_floating_limit() {
    let task = Task::RefreshFloatingLimit {
        task_id: "rfl-1".to_string(),
        tenant: "t-rfl".to_string(),
        queue_key: "qk-rfl".to_string(),
        current_max_concurrency: 100,
        last_refreshed_at_ms: 7000,
        metadata: vec![("k".to_string(), "v".to_string())],
        task_group: "workers".to_string(),
    };
    let encoded = encode_task(&task);
    let decoded = decode_task_validated(encoded).unwrap();

    assert_eq!(decoded.tenant(), "t-rfl");
    assert_eq!(decoded.task_group(), "workers");
    assert!(decoded.as_refresh_floating_limit().is_some());
}

#[silo::test]
fn test_decoded_task_as_bytes_passthrough() {
    let task = Task::RunAttempt {
        id: "pt-1".to_string(),
        tenant: "t-pt".to_string(),
        job_id: "j-pt".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        held_queues: vec![],
        task_group: "default".to_string(),
    };
    let encoded = encode_task(&task);
    let decoded = decode_task_validated(encoded.clone()).unwrap();

    // as_bytes should return the original encoded data
    assert_eq!(decoded.as_bytes().as_ref(), &encoded);
}

#[silo::test]
fn test_decoded_task_invalid_data() {
    let result = decode_task_validated(vec![0xFF, 0xFF]);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// encode_lease_with_new_expiry tests
// ---------------------------------------------------------------------------

#[silo::test]
fn test_encode_lease_with_new_expiry() {
    let lease = LeaseRecord {
        worker_id: "w-exp".to_string(),
        task: Task::RunAttempt {
            id: "t-exp".to_string(),
            tenant: "tenant-exp".to_string(),
            job_id: "j-exp".to_string(),
            attempt_number: 2,
            relative_attempt_number: 1,
            held_queues: vec!["hq1".to_string()],
            task_group: "grp".to_string(),
        },
        expiry_ms: 10000,
        started_at_ms: 5000,
    };
    let encoded = encode_lease(&lease);
    let decoded = decode_lease(encoded).unwrap();

    // Encode with new expiry
    let new_encoded = encode_lease_with_new_expiry(&decoded, 99999).unwrap();
    let re_decoded = decode_lease(new_encoded).unwrap();

    // Verify only expiry changed
    assert_eq!(re_decoded.worker_id(), "w-exp");
    assert_eq!(re_decoded.expiry_ms(), 99999);
    assert_eq!(re_decoded.started_at_ms(), 5000);
    assert_eq!(re_decoded.tenant(), "tenant-exp");
    assert_eq!(re_decoded.job_id(), "j-exp");
    assert_eq!(re_decoded.attempt_number(), 2);
    assert_eq!(re_decoded.relative_attempt_number(), 1);
    assert_eq!(re_decoded.held_queues(), vec!["hq1"]);
    assert_eq!(re_decoded.task_group(), "grp");
    assert_eq!(re_decoded.task_id(), Some("t-exp"));
}

#[silo::test]
fn test_encode_lease_with_new_expiry_refresh_floating_limit() {
    let lease = LeaseRecord {
        worker_id: "w-rfl".to_string(),
        task: Task::RefreshFloatingLimit {
            task_id: "rfl-exp".to_string(),
            tenant: "t-rfl-exp".to_string(),
            queue_key: "qk-exp".to_string(),
            current_max_concurrency: 42,
            last_refreshed_at_ms: 3000,
            metadata: vec![("a".to_string(), "b".to_string())],
            task_group: "workers".to_string(),
        },
        expiry_ms: 20000,
        started_at_ms: 0,
    };
    let encoded = encode_lease(&lease);
    let decoded = decode_lease(encoded).unwrap();

    let new_encoded = encode_lease_with_new_expiry(&decoded, 55555).unwrap();
    let re_decoded = decode_lease(new_encoded).unwrap();

    assert_eq!(re_decoded.worker_id(), "w-rfl");
    assert_eq!(re_decoded.expiry_ms(), 55555);
    assert_eq!(re_decoded.tenant(), "t-rfl-exp");
    let (task_id, queue_key) = re_decoded.refresh_floating_limit_info().unwrap();
    assert_eq!(task_id, "rfl-exp");
    assert_eq!(queue_key, "qk-exp");
}

// ---------------------------------------------------------------------------
// encode_lease_from_task_bytes tests
// ---------------------------------------------------------------------------

#[silo::test]
fn test_encode_lease_from_task_bytes() {
    let task = Task::RunAttempt {
        id: "t-from".to_string(),
        tenant: "tenant-from".to_string(),
        job_id: "j-from".to_string(),
        attempt_number: 3,
        relative_attempt_number: 2,
        held_queues: vec!["hq-a".to_string(), "hq-b".to_string()],
        task_group: "default".to_string(),
    };
    let task_bytes = encode_task(&task);

    let lease_bytes =
        encode_lease_from_task_bytes("worker-from", &task_bytes, 88888, 44444).unwrap();
    let decoded = decode_lease(lease_bytes).unwrap();

    assert_eq!(decoded.worker_id(), "worker-from");
    assert_eq!(decoded.expiry_ms(), 88888);
    assert_eq!(decoded.started_at_ms(), 44444);
    assert_eq!(decoded.tenant(), "tenant-from");
    assert_eq!(decoded.job_id(), "j-from");
    assert_eq!(decoded.attempt_number(), 3);
    assert_eq!(decoded.relative_attempt_number(), 2);
    assert_eq!(decoded.held_queues(), vec!["hq-a", "hq-b"]);
    assert_eq!(decoded.task_group(), "default");
    assert_eq!(decoded.task_id(), Some("t-from"));
}

#[silo::test]
fn test_encode_lease_from_task_bytes_check_rate_limit() {
    let task = Task::CheckRateLimit {
        task_id: "crl-from".to_string(),
        tenant: "t-crl-from".to_string(),
        job_id: "j-crl-from".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        limit_index: 2,
        rate_limit: GubernatorRateLimitData {
            name: "rl-n".to_string(),
            unique_key: "uk-n".to_string(),
            limit: 50,
            duration_ms: 30000,
            hits: 2,
            algorithm: 1,
            behavior: 3,
            retry_initial_backoff_ms: 200,
            retry_max_backoff_ms: 10000,
            retry_backoff_multiplier: 1.5,
            retry_max_retries: 10,
        },
        retry_count: 3,
        started_at_ms: 7777,
        priority: 4,
        held_queues: vec!["hq-x".to_string()],
        task_group: "fast".to_string(),
    };
    let task_bytes = encode_task(&task);

    let lease_bytes = encode_lease_from_task_bytes("w-crl", &task_bytes, 77777, 33333).unwrap();
    let decoded = decode_lease(lease_bytes).unwrap();

    assert_eq!(decoded.worker_id(), "w-crl");
    assert_eq!(decoded.expiry_ms(), 77777);
    assert_eq!(decoded.started_at_ms(), 33333);
    assert_eq!(decoded.tenant(), "t-crl-from");
    assert_eq!(decoded.task_group(), "fast");
}

#[silo::test]
fn test_encode_lease_from_task_bytes_invalid() {
    let result = encode_lease_from_task_bytes("w", &[0xFF, 0xFF], 0, 0);
    assert!(result.is_err());
}
