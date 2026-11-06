use silo::codec::{
    decode_attempt, decode_cancellation_at_ms, decode_concurrency_action,
    decode_floating_limit_state, decode_holder, decode_holder_granted_at_ms, decode_job_info,
    decode_job_status_owned, decode_lease, decode_task, decode_task_validated, encode_attempt,
    encode_attempt_parts, encode_concurrency_action, encode_floating_limit_state, encode_holder,
    encode_job_cancellation, encode_job_info, encode_job_info_parts, encode_job_status,
    encode_lease, encode_lease_parts, encode_refresh_index_row, encode_run_attempt,
    encode_run_attempt_lease, encode_task,
};
use silo::job::{
    ConcurrencyLimit, FloatingLimitState, JobCancellation, JobInfo, JobStatus, JobStatusKind, Limit,
};
use silo::job_attempt::{AttemptStatus, JobAttempt};
use silo::retry::RetryPolicy;
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
    assert_eq!(decoded.enqueue_time_ms, None);
    assert_eq!(decoded.priority, None);
}

#[silo::test]
fn job_status_roundtrips_enqueue_time_and_priority() {
    let mut status = JobStatus::scheduled(5000, 6000, 2);
    status.enqueue_time_ms = Some(-1_234);
    status.priority = Some(99);
    let decoded = decode_job_status_owned(&encode_job_status(&status)).unwrap();
    assert_eq!(decoded.kind, JobStatusKind::Scheduled);
    assert_eq!(decoded.next_attempt_starts_after_ms, Some(6000));
    assert_eq!(decoded.current_attempt, Some(2));
    assert_eq!(decoded.enqueue_time_ms, Some(-1_234));
    assert_eq!(decoded.priority, Some(99));
}

#[silo::test]
fn job_status_encoded_without_job_fields_decodes_with_both_absent() {
    use silo::fb::silo::fb;
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let root = fb::JobStatus::create(
        &mut builder,
        &fb::JobStatusArgs {
            kind: fb::JobStatusKind::Failed,
            changed_at_ms: 7000,
            next_attempt_starts_after_ms: None,
            current_attempt: None,
            ..Default::default()
        },
    );
    builder.finish(root, None);
    let decoded = decode_job_status_owned(builder.finished_data()).unwrap();
    assert_eq!(decoded.kind, JobStatusKind::Failed);
    assert_eq!(decoded.changed_at_ms, 7000);
    assert_eq!(decoded.enqueue_time_ms, None);
    assert_eq!(decoded.priority, None);
}

#[silo::test]
fn test_holder_roundtrip() {
    let holder = HolderRecord {
        granted_at_ms: 9999,
        job_id: Some("job-1".to_string()),
        attempt_number: Some(3),
    };
    let encoded = encode_holder(&holder);
    let granted_at_ms = decode_holder_granted_at_ms(&encoded).unwrap();
    assert_eq!(granted_at_ms, 9999);

    let decoded = decode_holder(&encoded).unwrap();
    assert_eq!(decoded.granted_at_ms, 9999);
    assert_eq!(decoded.job_id.as_deref(), Some("job-1"));
    assert_eq!(decoded.attempt_number, Some(3));
}

/// A holder value whose flatbuffer carries only `granted_at_ms` decodes with
/// both owner fields absent rather than failing.
#[silo::test]
fn test_holder_bytes_with_only_granted_at_decode_without_owner() {
    // HolderRecord { granted_at_ms: 9999 } with a one-slot vtable: root offset
    // 12; vtable [6, 12, 4] (vtable size, table size, field 0 at +4); table
    // soffset 6; then 0x270F as a little-endian int64 at +4.
    let bytes: [u8; 24] = [
        12, 0, 0, 0, 0, 0, 6, 0, 12, 0, 4, 0, 6, 0, 0, 0, 15, 39, 0, 0, 0, 0, 0, 0,
    ];
    let decoded = decode_holder(&bytes).unwrap();
    assert_eq!(decoded.granted_at_ms, 9999);
    assert_eq!(decoded.job_id, None);
    assert_eq!(decoded.attempt_number, None);
    assert_eq!(decode_holder_granted_at_ms(&bytes).unwrap(), 9999);
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
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: "tid".to_string(),
        limits: Vec::new(),
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
        task_id: "req-abc".to_string(),
        task_group: "fast".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        limits: Vec::new(),
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
            task_id,
            task_group,
            limit_index,
            held_queues,
            limits,
        } => {
            assert_eq!(queue, "q1");
            assert_eq!(start_time_ms, 5000);
            assert_eq!(priority, 3);
            assert_eq!(tenant, "tenant-1");
            assert_eq!(job_id, "job-42");
            assert_eq!(attempt_number, 2);
            assert_eq!(relative_attempt_number, 1);
            assert_eq!(task_id, "req-abc");
            assert_eq!(task_group, "fast");
            assert_eq!(limit_index, 0);
            assert!(held_queues.is_empty());
            assert!(limits.is_empty());
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
        refresh_scheduled_at_ms: None,
        stale_reset_count: 0,
    };
    let encoded = encode_floating_limit_state(&state);
    let decoded = decode_floating_limit_state(encoded).unwrap();
    assert_eq!(decoded.current_max_concurrency(), 10);
    assert_eq!(decoded.last_refreshed_at_ms(), 5000);
    assert_eq!(decoded.refresh_task_scheduled(), true);
    assert_eq!(decoded.refresh_interval_ms(), 30000);
    assert_eq!(decoded.default_max_concurrency(), 5);
    assert_eq!(decoded.retry_count(), 2);
    assert_eq!(decoded.next_retry_at_ms(), Some(8000));
    assert_eq!(decoded.metadata().len(), 1);
}

#[silo::test]
fn test_floating_limit_state_roundtrip_none_retry() {
    let state = FloatingLimitState {
        current_max_concurrency: 5,
        last_refreshed_at_ms: 1000,
        refresh_task_scheduled: false,
        refresh_interval_ms: 30000,
        default_max_concurrency: 5,
        retry_count: 0,
        next_retry_at_ms: None,
        metadata: vec![],
        refresh_scheduled_at_ms: None,
        stale_reset_count: 0,
    };
    let encoded = encode_floating_limit_state(&state);
    let decoded = decode_floating_limit_state(encoded).unwrap();
    assert_eq!(decoded.next_retry_at_ms(), None);
    assert_eq!(decoded.retry_count(), 0);
    assert!(decoded.metadata().is_empty());
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
        task_id: "req-rt".to_string(),
        task_group: "fast".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        limits: Vec::new(),
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

/// A floating limit state row whose table has eight vtable slots ending at
/// `metadata` and no `refresh_scheduled_at_ms` slot. It carries
/// `current_max_concurrency: 19`, `last_refreshed_at_ms: 1_756_400_000_000`,
/// `refresh_task_scheduled: true`, `refresh_interval_ms: 500`,
/// `default_max_concurrency: 5`, `retry_count: 3`,
/// `next_retry_at_ms: Some(1_756_400_060_000)`, and two metadata pairs.
const FLOATING_LIMIT_STATE_WITHOUT_SCHEDULED_AT: &[u8] = &[
    24, 0, 0, 0, 20, 0, 48, 0, 8, 0, 24, 0, 7, 0, 32, 0, 12, 0, 16, 0, 40, 0, 20, 0, 20, 0, 0, 0,
    0, 0, 0, 1, 19, 0, 0, 0, 5, 0, 0, 0, 3, 0, 0, 0, 28, 0, 0, 0, 0, 28, 153, 241, 152, 1, 0, 0,
    244, 1, 0, 0, 0, 0, 0, 0, 96, 6, 154, 241, 152, 1, 0, 0, 2, 0, 0, 0, 48, 0, 0, 0, 4, 0, 0, 0,
    224, 255, 255, 255, 16, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0, 97, 112, 105, 0, 6, 0, 0, 0, 115, 111,
    117, 114, 99, 101, 0, 0, 8, 0, 12, 0, 4, 0, 8, 0, 8, 0, 0, 0, 40, 0, 0, 0, 4, 0, 0, 0, 26, 0,
    0, 0, 112, 108, 97, 116, 102, 111, 114, 109, 45, 116, 101, 110, 97, 110, 116, 45, 101, 110,
    118, 45, 54, 49, 52, 54, 57, 54, 0, 0, 3, 0, 0, 0, 101, 110, 118, 0,
];

#[silo::test]
fn test_floating_limit_state_without_scheduled_at_decodes_with_trailing_fields_intact() {
    let decoded =
        decode_floating_limit_state(FLOATING_LIMIT_STATE_WITHOUT_SCHEDULED_AT.to_vec()).unwrap();
    assert_eq!(decoded.current_max_concurrency(), 19);
    assert_eq!(decoded.last_refreshed_at_ms(), 1_756_400_000_000);
    assert!(decoded.refresh_task_scheduled());
    assert_eq!(decoded.refresh_interval_ms(), 500);
    assert_eq!(decoded.default_max_concurrency(), 5);
    assert_eq!(decoded.retry_count(), 3);
    assert_eq!(decoded.next_retry_at_ms(), Some(1_756_400_060_000));
    assert_eq!(
        decoded.metadata(),
        vec![
            ("env".to_string(), "platform-tenant-env-614696".to_string()),
            ("source".to_string(), "api".to_string()),
        ]
    );
    assert_eq!(decoded.refresh_scheduled_at_ms(), None);
    assert_eq!(decoded.to_owned().refresh_scheduled_at_ms, None);
}

#[silo::test]
fn test_floating_limit_state_roundtrip_refresh_scheduled_at() {
    let state = FloatingLimitState {
        current_max_concurrency: 10,
        last_refreshed_at_ms: 5000,
        refresh_task_scheduled: true,
        refresh_interval_ms: 30000,
        default_max_concurrency: 5,
        retry_count: 0,
        next_retry_at_ms: None,
        metadata: vec![("source".to_string(), "api".to_string())],
        refresh_scheduled_at_ms: Some(5500),
        stale_reset_count: 0,
    };
    let encoded = encode_floating_limit_state(&state);
    let decoded = decode_floating_limit_state(encoded).unwrap();
    assert_eq!(decoded.refresh_scheduled_at_ms(), Some(5500));
    assert_eq!(decoded.next_retry_at_ms(), None);
    assert_eq!(decoded.metadata().len(), 1);
    assert_eq!(decoded.to_owned().refresh_scheduled_at_ms, Some(5500));
}

/// A `RefreshFloatingLimit` task encoded for the task line or a lease
/// carries no `not_before_ms`, so its decoded view reads the field as null.
#[silo::test]
fn test_refresh_floating_limit_without_not_before_decodes_as_null() {
    let task = Task::RefreshFloatingLimit {
        task_id: "refresh-1".to_string(),
        tenant: "tenant-3".to_string(),
        queue_key: "queue-key-abc".to_string(),
        current_max_concurrency: 50,
        last_refreshed_at_ms: 123456,
        metadata: vec![],
        task_group: "workers".to_string(),
    };
    let decoded = decode_task_validated(encode_task(&task)).unwrap();
    assert_eq!(decoded.not_before_ms(), None);
    assert_eq!(
        decoded
            .as_refresh_floating_limit()
            .and_then(|r| r.queue_key())
            .unwrap_or_default(),
        "queue-key-abc"
    );
}

#[silo::test]
fn test_refresh_index_row_roundtrip_carries_not_before() {
    let task = Task::RefreshFloatingLimit {
        task_id: "refresh-2".to_string(),
        tenant: "tenant-3".to_string(),
        queue_key: "queue-key-abc".to_string(),
        current_max_concurrency: 50,
        last_refreshed_at_ms: 123456,
        metadata: vec![("key1".to_string(), "val1".to_string())],
        task_group: "workers".to_string(),
    };
    let claimable = decode_task_validated(encode_refresh_index_row(&task, None)).unwrap();
    assert_eq!(claimable.not_before_ms(), None);

    let backed_off =
        decode_task_validated(encode_refresh_index_row(&task, Some(1_756_400_060_000))).unwrap();
    assert_eq!(backed_off.not_before_ms(), Some(1_756_400_060_000));
    let owned = backed_off.to_task().unwrap();
    match owned {
        Task::RefreshFloatingLimit {
            task_id,
            queue_key,
            current_max_concurrency,
            metadata,
            task_group,
            ..
        } => {
            assert_eq!(task_id, "refresh-2");
            assert_eq!(queue_key, "queue-key-abc");
            assert_eq!(current_max_concurrency, 50);
            assert_eq!(metadata, vec![("key1".to_string(), "val1".to_string())]);
            assert_eq!(task_group, "workers");
        }
        _ => panic!("expected RefreshFloatingLimit variant"),
    }
}

/// A floating limit state row with nine vtable slots ending at
/// `refresh_scheduled_at_ms` and no `stale_reset_count` slot. It carries the
/// same fields as `FLOATING_LIMIT_STATE_WITHOUT_SCHEDULED_AT` plus
/// `refresh_scheduled_at_ms: Some(1_756_400_030_000)` and one metadata pair.
const FLOATING_LIMIT_STATE_WITHOUT_STALE_RESET_COUNT: &[u8] = &[
    32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 22, 0, 60, 0, 8, 0, 24, 0, 7, 0, 32, 0, 12, 0, 16, 0, 40, 0, 20,
    0, 48, 0, 22, 0, 0, 0, 0, 0, 0, 1, 19, 0, 0, 0, 5, 0, 0, 0, 3, 0, 0, 0, 40, 0, 0, 0, 0, 28,
    153, 241, 152, 1, 0, 0, 244, 1, 0, 0, 0, 0, 0, 0, 96, 6, 154, 241, 152, 1, 0, 0, 48, 145, 153,
    241, 152, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 8, 0, 12, 0, 4, 0, 8, 0, 8, 0, 0, 0,
    40, 0, 0, 0, 4, 0, 0, 0, 26, 0, 0, 0, 112, 108, 97, 116, 102, 111, 114, 109, 45, 116, 101, 110,
    97, 110, 116, 45, 101, 110, 118, 45, 54, 49, 52, 54, 57, 54, 0, 0, 3, 0, 0, 0, 101, 110, 118,
    0,
];

#[silo::test]
fn test_floating_limit_state_without_stale_reset_count_decodes_as_zero() {
    let decoded =
        decode_floating_limit_state(FLOATING_LIMIT_STATE_WITHOUT_STALE_RESET_COUNT.to_vec())
            .unwrap();
    assert_eq!(decoded.current_max_concurrency(), 19);
    assert_eq!(decoded.last_refreshed_at_ms(), 1_756_400_000_000);
    assert!(decoded.refresh_task_scheduled());
    assert_eq!(decoded.refresh_interval_ms(), 500);
    assert_eq!(decoded.default_max_concurrency(), 5);
    assert_eq!(decoded.retry_count(), 3);
    assert_eq!(decoded.next_retry_at_ms(), Some(1_756_400_060_000));
    assert_eq!(decoded.refresh_scheduled_at_ms(), Some(1_756_400_030_000));
    assert_eq!(
        decoded.metadata(),
        vec![("env".to_string(), "platform-tenant-env-614696".to_string())]
    );
    assert_eq!(decoded.stale_reset_count(), 0);
    assert_eq!(decoded.to_owned().stale_reset_count, 0);

    let older =
        decode_floating_limit_state(FLOATING_LIMIT_STATE_WITHOUT_SCHEDULED_AT.to_vec()).unwrap();
    assert_eq!(older.stale_reset_count(), 0);
}

#[silo::test]
fn test_floating_limit_state_roundtrip_stale_reset_count() {
    let state = FloatingLimitState {
        current_max_concurrency: 10,
        last_refreshed_at_ms: 5000,
        refresh_task_scheduled: true,
        refresh_interval_ms: 30000,
        default_max_concurrency: 5,
        retry_count: 0,
        next_retry_at_ms: None,
        metadata: vec![],
        refresh_scheduled_at_ms: Some(5500),
        stale_reset_count: 7,
    };
    let encoded = encode_floating_limit_state(&state);
    let decoded = decode_floating_limit_state(encoded).unwrap();
    assert_eq!(decoded.stale_reset_count(), 7);
    assert_eq!(decoded.refresh_scheduled_at_ms(), Some(5500));
    assert_eq!(decoded.to_owned().stale_reset_count, 7);
}

#[silo::test]
fn test_refresh_index_row_zero_not_before_is_distinct_from_absent() {
    let task = Task::RefreshFloatingLimit {
        task_id: "refresh-3".to_string(),
        tenant: "-".to_string(),
        queue_key: "q".to_string(),
        current_max_concurrency: 1,
        last_refreshed_at_ms: 0,
        metadata: vec![],
        task_group: "workers".to_string(),
    };
    let zero = decode_task_validated(encode_refresh_index_row(&task, Some(0))).unwrap();
    assert_eq!(zero.not_before_ms(), Some(0));
    let absent = decode_task_validated(encode_refresh_index_row(&task, None)).unwrap();
    assert_eq!(absent.not_before_ms(), None);
}

fn sample_run_attempt(held_queues: Vec<String>) -> Task {
    Task::RunAttempt {
        id: "7c9e6679-7425-40de-944b-e07fc1f90ae7".to_string(),
        tenant: "tenant-1".to_string(),
        job_id: "0f8fad5b-d9cb-469f-a165-70867728950e".to_string(),
        attempt_number: 3,
        relative_attempt_number: 2,
        held_queues,
        task_group: "default".to_string(),
    }
}

/// The borrowed-field `RunAttempt` encoder writes the same bytes as
/// `encode_task` of the owned task.
#[silo::test]
fn encode_run_attempt_matches_encode_task() {
    for held_queues in [vec![], vec!["q1".to_string(), "q2".to_string()]] {
        let task = sample_run_attempt(held_queues.clone());
        let Task::RunAttempt {
            id,
            tenant,
            job_id,
            attempt_number,
            relative_attempt_number,
            task_group,
            ..
        } = &task
        else {
            unreachable!()
        };
        let borrowed = encode_run_attempt(
            id,
            tenant,
            job_id,
            *attempt_number,
            *relative_attempt_number,
            &held_queues,
            task_group,
        );
        assert_eq!(borrowed, encode_task(&task));
    }
}

/// A lease copied straight from a `RunAttempt` table writes the same bytes
/// as one encoded from the owned task, and both match `encode_lease`.
#[silo::test]
fn encode_run_attempt_lease_matches_owned_lease() {
    for held_queues in [vec![], vec!["q1".to_string()]] {
        let task = sample_run_attempt(held_queues);
        let record = LeaseRecord {
            worker_id: "worker-1".to_string(),
            task: task.clone(),
            expiry_ms: 1_700_000_030_000,
            started_at_ms: 1_700_000_000_000,
        };
        let owned = encode_lease(&record);
        assert_eq!(
            encode_lease_parts("worker-1", &task, record.expiry_ms, record.started_at_ms),
            owned
        );
        let decoded = decode_task_validated(encode_task(&task)).unwrap();
        let ra = decoded.as_run_attempt().expect("run attempt");
        assert_eq!(
            encode_run_attempt_lease("worker-1", ra, record.expiry_ms, record.started_at_ms),
            owned
        );
        // Renewing a lease copies the task out of the stored lease itself.
        let lease = decode_lease(owned.clone()).unwrap();
        let ra = lease.fb().task_as_run_attempt().expect("run attempt");
        assert_eq!(
            encode_run_attempt_lease("worker-1", ra, record.expiry_ms, record.started_at_ms),
            owned
        );
    }
}

#[silo::test]
fn encode_attempt_parts_matches_encode_attempt() {
    for status in [
        AttemptStatus::Running,
        AttemptStatus::Succeeded {
            finished_at_ms: 5,
            result: vec![1, 2, 3],
        },
        AttemptStatus::Failed {
            finished_at_ms: 6,
            error_code: "E_BOOM".to_string(),
            error: vec![9; 40],
        },
        AttemptStatus::Cancelled { finished_at_ms: 7 },
    ] {
        let attempt = JobAttempt {
            job_id: "job-1".to_string(),
            attempt_number: 2,
            relative_attempt_number: 1,
            task_id: "task-1".to_string(),
            started_at_ms: 4,
            status,
        };
        assert_eq!(
            encode_attempt_parts(
                &attempt.job_id,
                attempt.attempt_number,
                attempt.relative_attempt_number,
                &attempt.task_id,
                attempt.started_at_ms,
                &attempt.status,
            ),
            encode_attempt(&attempt)
        );
    }
}

/// Builders are sized from the payload up front; a payload far larger than
/// the old fixed capacity still encodes the same bytes and round-trips.
#[silo::test]
fn encode_job_info_parts_matches_encode_job_info() {
    for payload_len in [0, 100, 70_000] {
        let job = JobInfo {
            id: "job-1".to_string(),
            priority: 10,
            enqueue_time_ms: 1_700_000_000_000,
            payload: (0..payload_len).map(|i| i as u8).collect(),
            retry_policy: Some(RetryPolicy {
                retry_count: 3,
                initial_interval_ms: 100,
                max_interval_ms: 1000,
                randomize_interval: false,
                backoff_factor: 2.0,
            }),
            metadata: vec![("queue".to_string(), "emails".to_string())],
            limits: vec![Limit::Concurrency(ConcurrencyLimit {
                key: "ck".to_string(),
                max_concurrency: 4,
            })],
            task_group: "default".to_string(),
        };
        let encoded = encode_job_info(&job);
        assert_eq!(
            encode_job_info_parts(
                &job.id,
                job.priority,
                job.enqueue_time_ms,
                &job.payload,
                job.retry_policy.as_ref(),
                &job.metadata,
                &job.limits,
                &job.task_group,
            ),
            encoded
        );
        let view = decode_job_info(encoded).unwrap();
        assert_eq!(
            view.fb().payload().map(|p| p.bytes().to_vec()),
            Some(job.payload.clone())
        );
    }
}
