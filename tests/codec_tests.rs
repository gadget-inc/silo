use silo::codec::{
    decode_attempt, decode_concurrency_action, decode_floating_limit_state, decode_holder,
    decode_job_cancellation, decode_job_info, decode_job_status, decode_lease, decode_task,
    decode_task_tenant, decode_task_view, encode_attempt, encode_check_rate_limit_retry_from_task,
    encode_concurrency_action, encode_floating_limit_state, encode_holder, encode_job_cancellation,
    encode_job_info, encode_job_status, encode_lease, encode_lease_from_request_ticket_grant,
    encode_task, task_is_refresh_floating_limit, validate_task,
};
use silo::job::{FloatingLimitState, JobCancellation, JobInfo, JobStatus, JobStatusKind};
use silo::job_attempt::{AttemptStatus, JobAttempt};
use silo::task::{ConcurrencyAction, GubernatorRateLimitData, HolderRecord, LeaseRecord, Task};

#[silo::test]
fn test_task_roundtrip_run_attempt() {
    let task = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "tenant-1".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        held_queues: vec!["queue-1".to_string()],
        task_group: "default".to_string(),
    };

    let encoded = encode_task(&task).expect("encode task");
    let decoded = decode_task(&encoded).expect("decode task");

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
fn test_task_roundtrip_request_ticket() {
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

    let encoded = encode_task(&task).expect("encode task");
    let decoded = decode_task(&encoded).expect("decode task");

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
fn test_task_roundtrip_check_rate_limit() {
    let task = Task::CheckRateLimit {
        task_id: "task-99".to_string(),
        tenant: "tenant-2".to_string(),
        job_id: "job-7".to_string(),
        attempt_number: 3,
        relative_attempt_number: 2,
        limit_index: 1,
        rate_limit: GubernatorRateLimitData {
            name: "rl-name".to_string(),
            unique_key: "rl-key".to_string(),
            limit: 100,
            duration_ms: 60_000,
            hits: 1,
            algorithm: 0,
            behavior: 1,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 30_000,
            retry_backoff_multiplier: 2.0,
            retry_max_retries: 5,
        },
        retry_count: 1,
        started_at_ms: 1234,
        priority: 7,
        held_queues: vec!["a".to_string(), "b".to_string()],
        task_group: "workers".to_string(),
    };

    let encoded = encode_task(&task).expect("encode task");
    let decoded = decode_task(&encoded).expect("decode task");

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
            assert_eq!(rate_limit.name, "rl-name");
            assert_eq!(retry_count, 1);
            assert_eq!(started_at_ms, 1234);
            assert_eq!(priority, 7);
            assert_eq!(held_queues, vec!["a", "b"]);
            assert_eq!(task_group, "workers");
        }
        _ => panic!("expected CheckRateLimit variant"),
    }
}

#[silo::test]
fn test_task_roundtrip_refresh_floating_limit() {
    let task = Task::RefreshFloatingLimit {
        task_id: "refresh-1".to_string(),
        tenant: "tenant-r".to_string(),
        queue_key: "queue-r".to_string(),
        current_max_concurrency: 10,
        last_refreshed_at_ms: 1000,
        metadata: vec![("k1".to_string(), "v1".to_string())],
        task_group: "workers".to_string(),
    };

    let encoded = encode_task(&task).expect("encode task");
    let decoded = decode_task(&encoded).expect("decode task");

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
            assert_eq!(tenant, "tenant-r");
            assert_eq!(queue_key, "queue-r");
            assert_eq!(current_max_concurrency, 10);
            assert_eq!(last_refreshed_at_ms, 1000);
            assert_eq!(metadata, vec![("k1".to_string(), "v1".to_string())]);
            assert_eq!(task_group, "workers");
        }
        _ => panic!("expected RefreshFloatingLimit variant"),
    }
}

#[silo::test]
fn test_malformed_task_bytes_fail_decode() {
    let result = decode_task(&[1, 2, 3]);
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

    let encoded = encode_lease(&lease).expect("encode lease");
    let decoded = decode_lease(&encoded).expect("decode lease");

    assert_eq!(decoded.worker_id(), "worker-1");
    assert_eq!(decoded.expiry_ms(), 12345);
    assert_eq!(decoded.started_at_ms(), 10000);
    assert_eq!(decoded.tenant(), "-");
    assert_eq!(decoded.job_id(), "job-1");
    assert_eq!(decoded.to_task().expect("to task").tenant(), "-");
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

    let encoded = encode_attempt(&attempt).expect("encode attempt");
    let decoded = decode_attempt(&encoded).expect("decode attempt");

    assert_eq!(decoded.job_id(), "job-1");
    assert_eq!(decoded.attempt_number(), 1);
    assert_eq!(decoded.relative_attempt_number(), 1);
    assert_eq!(decoded.task_id(), "task-1");
    assert_eq!(decoded.started_at_ms(), 1000);
    assert!(matches!(decoded.state(), AttemptStatus::Running));
}

#[silo::test]
fn test_job_status_roundtrip() {
    let status = JobStatus::running(5000);
    let encoded = encode_job_status(&status).expect("encode status");
    let decoded = decode_job_status(&encoded).expect("decode status");
    let owned = decoded.to_owned();

    assert_eq!(owned.kind, JobStatusKind::Running);
    assert_eq!(owned.changed_at_ms, 5000);
    assert_eq!(owned.next_attempt_starts_after_ms, None);
    assert_eq!(owned.current_attempt, None);
}

#[silo::test]
fn test_holder_roundtrip() {
    let holder = HolderRecord {
        granted_at_ms: 9999,
    };
    let encoded = encode_holder(&holder).expect("encode holder");
    let decoded = decode_holder(&encoded).expect("decode holder");
    assert_eq!(decoded.granted_at_ms(), 9999);
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

    let encoded = encode_concurrency_action(&action).expect("encode action");
    let decoded = decode_concurrency_action(&encoded).expect("decode action");
    let enqueue = decoded.enqueue_task().expect("enqueue action");

    assert_eq!(enqueue.start_time_ms, 1000);
    assert_eq!(enqueue.priority, 5);
    assert_eq!(enqueue.job_id, "job-1");
    assert_eq!(enqueue.attempt_number, 1);
    assert_eq!(enqueue.relative_attempt_number, 1);
    assert_eq!(enqueue.task_group, "default");
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

    let encoded = encode_job_info(&job_info).expect("encode job info");
    let decoded = decode_job_info(&encoded).expect("decode job info");

    assert_eq!(decoded.id(), "job-info-1");
    assert_eq!(decoded.priority(), 42);
    assert_eq!(decoded.enqueue_time_ms(), 1700000000000);
    assert_eq!(decoded.payload_bytes(), &[1, 2, 3, 4]);
    assert_eq!(decoded.metadata().len(), 1);
    assert_eq!(decoded.task_group(), "default");
}

#[silo::test]
fn test_job_cancellation_roundtrip() {
    let cancellation = JobCancellation {
        cancelled_at_ms: 9_999_999,
    };

    let encoded = encode_job_cancellation(&cancellation).expect("encode cancellation");
    let decoded = decode_job_cancellation(&encoded).expect("decode cancellation");

    assert_eq!(decoded.cancelled_at_ms(), 9_999_999);
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

    let encoded = encode_floating_limit_state(&state).expect("encode state");
    let decoded = decode_floating_limit_state(&encoded).expect("decode state");

    assert_eq!(decoded.current_max_concurrency(), 10);
    assert_eq!(decoded.last_refreshed_at_ms(), 5000);
    assert!(decoded.refresh_task_scheduled());
    assert_eq!(decoded.refresh_interval_ms(), 30000);
    assert_eq!(decoded.default_max_concurrency(), 5);
    assert_eq!(decoded.retry_count(), 2);
    assert_eq!(decoded.next_retry_at_ms(), Some(8000));
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

    let encoded = encode_lease(&lease).expect("encode lease");
    let decoded = decode_lease(&encoded).expect("decode lease");

    assert_eq!(decoded.worker_id(), "w1");
    assert_eq!(decoded.expiry_ms(), 50000);
    assert_eq!(decoded.tenant(), "t1");
    assert_eq!(decoded.job_id(), "j1");
    assert_eq!(decoded.attempt_number(), 3);
    assert_eq!(decoded.relative_attempt_number(), 2);
    assert_eq!(decoded.held_queues(), vec!["q1", "q2"]);
    assert_eq!(decoded.task_group(), "grp");
    assert_eq!(decoded.task_id(), Some("task-10"));
}

#[silo::test]
fn test_decoded_lease_accessors_refresh_task() {
    let lease = LeaseRecord {
        worker_id: "w2".to_string(),
        task: Task::RefreshFloatingLimit {
            task_id: "rf-1".to_string(),
            tenant: "t2".to_string(),
            queue_key: "queue-a".to_string(),
            current_max_concurrency: 8,
            last_refreshed_at_ms: 123,
            metadata: vec![("region".to_string(), "us".to_string())],
            task_group: "workers".to_string(),
        },
        expiry_ms: 80000,
        started_at_ms: 70000,
    };

    let encoded = encode_lease(&lease).expect("encode lease");
    let decoded = decode_lease(&encoded).expect("decode lease");

    assert_eq!(decoded.tenant(), "t2");
    assert_eq!(decoded.job_id(), "");
    assert_eq!(decoded.attempt_number(), 0);
    assert_eq!(decoded.relative_attempt_number(), 0);
    assert_eq!(decoded.held_queues(), Vec::<String>::new());
    assert_eq!(decoded.task_group(), "workers");
    assert_eq!(decoded.task_id(), None);

    let (task_id, queue_key) = decoded.refresh_floating_limit_info().expect("refresh info");
    assert_eq!(task_id, "rf-1");
    assert_eq!(queue_key, "queue-a");

    let refresh = decoded.refresh_floating_limit_task().expect("refresh task");
    assert_eq!(refresh.task_id(), "rf-1");
    assert_eq!(refresh.tenant(), "t2");
    assert_eq!(refresh.queue_key(), "queue-a");
    assert_eq!(refresh.current_max_concurrency(), 8);
    assert_eq!(refresh.last_refreshed_at_ms(), 123);
    assert_eq!(refresh.task_group(), "workers");
    assert_eq!(
        refresh.metadata(),
        vec![("region".to_string(), "us".to_string())]
    );
}

#[silo::test]
fn test_task_zero_copy_helpers() {
    let run = Task::RunAttempt {
        id: "task-1".to_string(),
        tenant: "tenant-1".to_string(),
        job_id: "job-1".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        held_queues: vec![],
        task_group: "default".to_string(),
    };
    let run_bytes = encode_task(&run).expect("encode run task");
    validate_task(&run_bytes).expect("validate run task");
    assert_eq!(decode_task_tenant(&run_bytes).expect("tenant"), "tenant-1");
    assert!(!task_is_refresh_floating_limit(&run_bytes).expect("kind check"));

    let refresh = Task::RefreshFloatingLimit {
        task_id: "refresh-1".to_string(),
        tenant: "tenant-r".to_string(),
        queue_key: "queue-r".to_string(),
        current_max_concurrency: 10,
        last_refreshed_at_ms: 1000,
        metadata: vec![],
        task_group: "workers".to_string(),
    };
    let refresh_bytes = encode_task(&refresh).expect("encode refresh task");
    validate_task(&refresh_bytes).expect("validate refresh task");
    assert_eq!(
        decode_task_tenant(&refresh_bytes).expect("tenant"),
        "tenant-r"
    );
    assert!(task_is_refresh_floating_limit(&refresh_bytes).expect("kind check"));
}

#[silo::test]
fn test_decoded_job_status_accessors() {
    let status = JobStatus::scheduled(1234, 5678, 9);
    let encoded = encode_job_status(&status).expect("encode status");
    let decoded = decode_job_status(&encoded).expect("decode status");

    assert_eq!(decoded.kind(), JobStatusKind::Scheduled);
    assert_eq!(decoded.changed_at_ms(), 1234);
    assert_eq!(decoded.next_attempt_starts_after_ms(), Some(5678));
    assert_eq!(decoded.current_attempt(), Some(9));
    assert!(!decoded.is_terminal());

    let cancelled = JobStatus::cancelled(42);
    let cancelled_bytes = encode_job_status(&cancelled).expect("encode cancelled");
    let cancelled_decoded = decode_job_status(&cancelled_bytes).expect("decode cancelled");
    assert!(cancelled_decoded.is_terminal());
}

#[silo::test]
fn test_encode_lease_from_request_ticket_grant_roundtrip() {
    let ticket = Task::RequestTicket {
        queue: "q-main".to_string(),
        start_time_ms: 10,
        priority: 5,
        tenant: "tenant-rt".to_string(),
        job_id: "job-rt".to_string(),
        attempt_number: 4,
        relative_attempt_number: 2,
        request_id: "req-77".to_string(),
        task_group: "workers".to_string(),
    };
    let ticket_bytes = encode_task(&ticket).expect("encode ticket");
    let decoded = decode_task_view(&ticket_bytes).expect("decode ticket");
    let request_ticket = decoded.request_ticket().expect("request ticket view");

    let lease_bytes = encode_lease_from_request_ticket_grant(
        "worker-rt",
        request_ticket,
        "queue-granted",
        999,
        555,
    )
    .expect("encode lease from ticket");
    let lease = decode_lease(&lease_bytes).expect("decode lease");

    assert_eq!(lease.worker_id(), "worker-rt");
    assert_eq!(lease.expiry_ms(), 999);
    assert_eq!(lease.started_at_ms(), 555);
    assert_eq!(lease.task_id(), Some("req-77"));
    assert_eq!(lease.tenant(), "tenant-rt");
    assert_eq!(lease.job_id(), "job-rt");
    assert_eq!(lease.attempt_number(), 4);
    assert_eq!(lease.relative_attempt_number(), 2);
    assert_eq!(lease.held_queues(), vec!["queue-granted"]);
    assert_eq!(lease.task_group(), "workers");
}

#[silo::test]
fn test_encode_check_rate_limit_retry_from_task_roundtrip() {
    let task = Task::CheckRateLimit {
        task_id: "orig-task".to_string(),
        tenant: "tenant-crl".to_string(),
        job_id: "job-crl".to_string(),
        attempt_number: 3,
        relative_attempt_number: 1,
        limit_index: 2,
        rate_limit: GubernatorRateLimitData {
            name: "name".to_string(),
            unique_key: "key".to_string(),
            limit: 10,
            duration_ms: 1000,
            hits: 1,
            algorithm: 0,
            behavior: 0,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 1000,
            retry_backoff_multiplier: 2.0,
            retry_max_retries: 10,
        },
        retry_count: 7,
        started_at_ms: 123,
        priority: 9,
        held_queues: vec!["qa".to_string(), "qb".to_string()],
        task_group: "tg".to_string(),
    };
    let task_bytes = encode_task(&task).expect("encode check task");
    let decoded = decode_task_view(&task_bytes).expect("decode check task");
    let check = decoded.check_rate_limit().expect("check task view");

    let retry_bytes =
        encode_check_rate_limit_retry_from_task(check, "retry-task", 8).expect("encode retry task");
    let decoded_retry = decode_task(&retry_bytes).expect("decode retry task");

    match decoded_retry {
        Task::CheckRateLimit {
            task_id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            limit_index,
            retry_count,
            started_at_ms,
            priority,
            held_queues,
            task_group,
            ..
        } => {
            assert_eq!(task_id, "retry-task");
            assert_eq!(tenant, "tenant-crl");
            assert_eq!(job_id, "job-crl");
            assert_eq!(attempt_number, 3);
            assert_eq!(relative_attempt_number, 1);
            assert_eq!(limit_index, 2);
            assert_eq!(retry_count, 8);
            assert_eq!(started_at_ms, 123);
            assert_eq!(priority, 9);
            assert_eq!(held_queues, vec!["qa", "qb"]);
            assert_eq!(task_group, "tg");
        }
        _ => panic!("expected CheckRateLimit variant"),
    }
}
