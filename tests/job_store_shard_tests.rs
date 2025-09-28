use rkyv::Archive;
use silo::job::JobStatus;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};

use silo::job_store_shard::{JobStoreShard, JobStoreShardError, LeaseRecord, Task};
use silo::keys::concurrency_holder_key;
use silo::retry::{next_retry_time_ms, RetryPolicy};
use silo::settings::{Backend, DatabaseConfig};
use slatedb::{Db, DbIterator};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

fn parse_time_from_task_key(key: &str) -> Option<u64> {
    // Format: tasks/{:020}/{:02}/{job_id}/{attempt}
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() < 5 || parts[0] != "tasks" {
        return None;
    }
    parts[1].parse::<u64>().ok()
}

async fn open_temp_shard() -> (tempfile::TempDir, JobStoreShard) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };
    let shard = JobStoreShard::open(&cfg).await.expect("open shard");
    (tmp, shard)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn first_kv_with_prefix(db: &Db, prefix: &str) -> Option<(String, bytes::Bytes)> {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.ok()?;
    let first = iter.next().await.ok()?;
    first.map(|kv| (String::from_utf8_lossy(&kv.key).to_string(), kv.value))
}

async fn count_tasks_before(db: &Db, cutoff_ms: i64) -> usize {
    let start: Vec<u8> = b"tasks/".to_vec();
    let mut end: Vec<u8> = b"tasks/".to_vec();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        let Some(kv) = maybe else { break };
        let key_str = String::from_utf8_lossy(&kv.key).to_string();
        if let Some(ts) = parse_time_from_task_key(&key_str) {
            if (ts as i64) < cutoff_ms {
                count += 1;
            }
        }
    }
    count
}

async fn count_with_prefix(db: &Db, prefix: &str) -> usize {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        if maybe.is_none() {
            break;
        }
        count += 1;
    }
    count
}

#[tokio::test]
async fn enqueue_round_trip_with_explicit_id() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"hello": "world"});
    let id = "job-123".to_string();
    let priority = 50u8;
    let start_time_ms = 1_700_000_000_000i64;

    let got_id = shard
        .enqueue(
            Some(id.clone()),
            priority,
            start_time_ms,
            None,
            payload.clone(),
            vec![],
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

    // Job status should be Scheduled after enqueue
    let status = shard
        .get_job_status(&id)
        .await
        .expect("get status")
        .expect("exists");
    match status {
        JobStatus::Scheduled {} => {}
        other => panic!("expected Scheduled, got {:?}", other),
    }

    let view = shard.get_job(&id).await.expect("get_job").expect("exists");
    assert_eq!(view.id(), id);
    assert_eq!(view.priority(), priority);
    assert_eq!(view.enqueue_time_ms(), start_time_ms);
    let got_payload = view.payload_json().unwrap();
    assert_eq!(got_payload, payload);

    // A task should be present and point at attempt 1 for this job
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(!tasks.is_empty());
    assert!(
        matches!(tasks[0], Task::RunAttempt { ref job_id, attempt_number, .. } if job_id == &id && attempt_number == 1)
    );
}

#[tokio::test]
async fn enqueue_generates_uuid_when_none_provided() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let start_time_ms = 12345i64;

    let got_id = shard
        .enqueue(None, priority, start_time_ms, None, payload.clone(), vec![])
        .await
        .expect("enqueue");
    assert!(!got_id.is_empty());
    assert!(uuid::Uuid::parse_str(&got_id).is_ok(), "expected UUIDv4 id");

    let view = shard
        .get_job(&got_id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.id(), got_id);
    assert_eq!(view.priority(), priority);
    assert_eq!(view.enqueue_time_ms(), start_time_ms);
    let got_payload = view.payload_json().unwrap();
    assert_eq!(got_payload, payload);

    // A task should be present and point at attempt 1 for this job
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(!tasks.is_empty());
    assert!(
        matches!(tasks[0], Task::RunAttempt { ref job_id, attempt_number, .. } if job_id == &got_id && attempt_number == 1)
    );
}

#[tokio::test]
async fn delete_job_removes_key_and_is_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue a job
    let payload = serde_json::json!({"hello": "world"});
    let id = "job-to-delete".to_string();
    let priority = 42u8;
    let start_time_ms = 1_700_000_000_001i64;

    let got_id = shard
        .enqueue(
            Some(id.clone()),
            priority,
            start_time_ms,
            None,
            payload.clone(),
            vec![],
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

    // Ensure it exists
    assert!(shard.get_job(&id).await.expect("get_job").is_some());

    // Delete it
    shard.delete_job(&id).await.expect("delete_job");

    // Ensure it's gone
    let got = shard.get_job(&id).await.expect("get_job");
    assert!(got.is_none(), "job should be deleted");

    // Delete again (idempotent)
    shard.delete_job(&id).await.expect("delete_job again");
    let got = shard.get_job(&id).await.expect("get_job");
    assert!(got.is_none(), "job should remain deleted");
}

#[tokio::test]
async fn peek_omits_future_scheduled_tasks() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();
    let future_ms = now_ms + 60_000;

    let _ = shard
        .enqueue(None, priority, future_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    // Should not see the task yet
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(tasks.is_empty(), "future task should not be visible");
}

#[tokio::test]
async fn dequeue_moves_tasks_to_leased_with_uuid() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");
    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    // Job status should transition to Running after dequeue
    let status = shard
        .get_job_status(&job_id)
        .await
        .expect("get status")
        .expect("exists");
    match status {
        JobStatus::Running {} => {}
        other => panic!("expected Running, got {:?}", other),
    }
    let leased_task_id = {
        let t = &tasks[0];
        assert_eq!(t.job().id(), job_id);
        assert_eq!(t.attempt().attempt_number(), 1);
        assert!(
            uuid::Uuid::parse_str(t.attempt().task_id()).is_ok(),
            "task id is UUID"
        );
        t.attempt().task_id().to_string()
    };

    // Verify a leased entry exists, and includes worker id
    let first = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("scan leased");
    let key_str = first.0;
    let kv_value = first.1;
    assert!(key_str.starts_with("lease/"));

    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&kv_value) };
    assert_eq!(archived.worker_id.as_str(), "worker-1");
    match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id: jid,
            attempt_number,
            ..
        } => {
            assert_eq!(id.as_str(), leased_task_id);
            assert_eq!(jid.as_str(), job_id);
            assert_eq!(*attempt_number, 1);
        }
    }

    // Ensure original task queue is empty now
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no tasks should remain after dequeue");
}

#[tokio::test]
async fn heartbeat_renews_lease_when_worker_matches() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let _job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    // Read current lease key and expiry
    let first = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("scan lease");
    let old_key = first.0;
    assert!(old_key.ends_with(&task_id));
    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    let archived_lease: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&first.1) };
    let old_expiry = archived_lease.expiry_ms as u64;

    // Heartbeat to renew
    shard
        .heartbeat_task("worker-1", &task_id)
        .await
        .expect("heartbeat ok");

    // Scan again, expect one lease for task with a higher expiry
    let second = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("scan lease 2");
    let new_key = second.0;
    assert!(new_key.ends_with(&task_id));
    let archived_lease2: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&second.1) };
    let new_expiry = archived_lease2.expiry_ms as u64;
    assert!(new_expiry > old_expiry, "new expiry should be greater");

    // Validate owner remains the same
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&second.1) };
    assert_eq!(archived.worker_id.as_str(), "worker-1");
}

#[tokio::test]
async fn heartbeat_rejects_mismatched_worker() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let _job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    let err = shard
        .heartbeat_task("worker-2", &task_id)
        .await
        .expect_err("heartbeat should fail");

    match err {
        JobStoreShardError::LeaseOwnerMismatch {
            task_id: tid,
            expected,
            got,
        } => {
            assert_eq!(tid, task_id);
            assert_eq!(expected, "worker-1".to_string());
            assert_eq!(got, "worker-2".to_string());
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn reporting_attempt_outcome_updates_attempt_and_deletes_lease() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    // Outcome: success
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect("report ok");

    // Lease should be gone
    let lease_key = format!("lease/{}", task_id);
    let lease = shard
        .db()
        .get(lease_key.as_bytes())
        .await
        .expect("get lease");
    assert!(lease.is_none(), "lease should be deleted");

    // Attempt state should be updated
    let attempt_view = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get attempt view")
        .expect("attempt exists");
    match attempt_view.state() {
        AttemptStatus::Succeeded { .. } => {}
        _ => panic!("expected Succeeded"),
    }
}

#[tokio::test]
async fn error_with_no_retries_does_not_enqueue_next_attempt() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();

    // Enqueue with retry_count = 0 (only first attempt, no retries)
    let policy = RetryPolicy {
        retry_count: 0,
        initial_interval_ms: 1000,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 2.0,
    };
    let _job_id = shard
        .enqueue(None, priority, now, Some(policy), payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"x".to_vec(),
            },
        )
        .await
        .expect("report");

    // No new tasks should be present
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no follow-up task should be enqueued");
}

#[tokio::test]
async fn error_with_retries_enqueues_next_attempt_until_limit() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();

    // Allow 2 retries (so attempts 1 -> error → attempt 2 scheduled; error again → attempt 3 scheduled; then stop)
    let policy = RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let _job_id = shard
        .enqueue(None, priority, now, Some(policy), payload, vec![])
        .await
        .expect("enqueue");

    // Run attempt 1 and error
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e1".to_vec(),
            },
        )
        .await
        .expect("report");

    // One new task should exist (attempt 2)
    let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("task2");
    type ArchivedTask = <Task as Archive>::Archived;
    let a2: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v2) };
    let attempt = match a2 {
        ArchivedTask::RunAttempt { attempt_number, .. } => *attempt_number,
    };
    assert_eq!(attempt, 2);

    // Dequeue attempt 2 and error again
    let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2");
    let t2 = tasks2[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t2,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e2".to_vec(),
            },
        )
        .await
        .expect("report2");

    // One new task should exist (attempt 3), but no more beyond that after another error
    let (_k3, v3) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("task3");
    let a3: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v3) };
    let attempt3 = match a3 {
        ArchivedTask::RunAttempt { attempt_number, .. } => *attempt_number,
    };
    assert_eq!(attempt3, 3);

    // Dequeue attempt 3 and error again — but no further tasks since retries exhausted
    let tasks3 = shard.dequeue("w", 1).await.expect("dequeue3");
    let t3 = tasks3[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t3,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e3".to_vec(),
            },
        )
        .await
        .expect("report3");
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no attempt 4 should be enqueued");
}

#[tokio::test]
async fn double_reporting_same_attempt_is_idempotent_success_then_success() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    // First report: success
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect("report ok");

    // Second report: expect LeaseNotFound since lease was removed on first report
    let err = shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect_err("second report should error with LeaseNotFound");
    match err {
        JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, task_id),
        other => panic!("unexpected error: {other:?}"),
    }

    // Lease should remain gone
    let lease_key = format!("lease/{}", task_id);
    let lease = shard
        .db()
        .get(lease_key.as_bytes())
        .await
        .expect("get lease");
    assert!(lease.is_none(), "lease should be deleted");

    // Attempt state should be succeeded
    let attempt_view = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get attempt view")
        .expect("attempt exists");
    match attempt_view.state() {
        AttemptStatus::Succeeded { .. } => {}
        _ => panic!("expected Succeeded"),
    }

    // No additional tasks should be enqueued
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no follow-up task should be enqueued");
}

#[tokio::test]
async fn double_reporting_same_attempt_is_idempotent_success_then_error() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v2"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    // First report: success
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect("report ok");

    // Second report: error, expect LeaseNotFound
    let err = shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"boom".to_vec(),
            },
        )
        .await
        .expect_err("second report should error with LeaseNotFound");
    match err {
        JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, task_id),
        other => panic!("unexpected error: {other:?}"),
    }

    // Attempt state should still be succeeded
    let attempt_view = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get attempt view")
        .expect("attempt exists");
    match attempt_view.state() {
        AttemptStatus::Succeeded { .. } => {}
        _ => panic!("expected Succeeded"),
    }

    // No tasks should be enqueued
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no follow-up task should be enqueued");
}

#[tokio::test]
async fn enqueue_fails_when_id_already_exists_and_db_unchanged() {
    let (_tmp, shard) = open_temp_shard().await;

    let id = "dup-job".to_string();
    let payload1 = serde_json::json!({"v": 1});
    let priority1 = 10u8;
    let start1 = 1_700_000_123_000i64;

    let got_id = shard
        .enqueue(
            Some(id.clone()),
            priority1,
            start1,
            None,
            payload1.clone(),
            vec![],
        )
        .await
        .expect("first enqueue ok");
    assert_eq!(got_id, id);

    // Pre-duplicate snapshot
    let jobs_before = count_with_prefix(shard.db(), "jobs/").await;
    let tasks_before = count_with_prefix(shard.db(), "tasks/").await;

    // Attempt duplicate enqueue with different values to ensure no overwrite occurs
    let payload2 = serde_json::json!({"v": 2});
    let priority2 = 20u8;
    let start2 = start1 + 999_000;

    let err = shard
        .enqueue(
            Some(id.clone()),
            priority2,
            start2,
            None,
            payload2.clone(),
            vec![],
        )
        .await
        .expect_err("duplicate enqueue should fail");

    match err {
        JobStoreShardError::JobAlreadyExists(got) => assert_eq!(got, id),
        other => panic!("unexpected error: {other:?}"),
    }

    // DB should be unchanged
    let jobs_after = count_with_prefix(shard.db(), "jobs/").await;
    let tasks_after = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(jobs_after, jobs_before, "job count should be unchanged");
    assert_eq!(tasks_after, tasks_before, "task count should be unchanged");

    // Original job info should be intact (not overwritten)
    let view = shard.get_job(&id).await.expect("get_job").expect("exists");
    assert_eq!(view.priority(), priority1);
    assert_eq!(view.enqueue_time_ms(), start1);
    let got_payload = view.payload_json().unwrap();
    assert_eq!(got_payload, payload1);
}

#[tokio::test]
async fn retry_count_one_boundary_enqueues_attempt2_then_stops_on_second_error() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(None, priority, now, Some(policy.clone()), payload, vec![])
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 should be enqueued
    let tasks = shard.dequeue("w", 1).await.expect("dequeue1");
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e1".to_vec(),
            },
        )
        .await
        .expect("report1");

    // Verify attempt 2 task exists
    let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("task2");
    type ArchivedTask = <Task as Archive>::Archived;
    let a2: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v2) };
    let attempt2 = match a2 {
        ArchivedTask::RunAttempt { attempt_number, .. } => *attempt_number,
    };
    assert_eq!(attempt2, 2);

    // Run attempt 2 and fail -> no attempt 3
    let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2");
    let t2 = tasks2[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t2,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e2".to_vec(),
            },
        )
        .await
        .expect("report2");

    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no attempt 3 should be enqueued");

    // Attempt 2 state should be Failed
    let a2_view = shard
        .get_job_attempt(&job_id, 2)
        .await
        .expect("get attempt2 view")
        .expect("attempt2 exists");
    match a2_view.state() {
        AttemptStatus::Failed { .. } => {}
        _ => panic!("expected Failed"),
    }
}

#[tokio::test]
async fn next_retry_time_matches_scheduled_time_smoke() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 25,
        max_interval_ms: 1_000_000,
        randomize_interval: false,
        backoff_factor: 2.0,
    };
    let job_id = shard
        .enqueue(
            None,
            priority,
            now,
            Some(policy.clone()),
            payload.clone(),
            vec![],
        )
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e".to_vec(),
            },
        )
        .await
        .expect("report1");

    // Read attempt1 finished_at and next task key time
    let attempt1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    let finished_at = match attempt1.state() {
        AttemptStatus::Failed { finished_at_ms, .. } => finished_at_ms,
        _ => panic!("expected Failed"),
    };
    let (k2, _v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("task2");
    let scheduled_ms = parse_time_from_task_key(&k2).expect("parse time");
    let expected = next_retry_time_ms(finished_at, 1, &policy).unwrap() as u64;
    assert_eq!(
        scheduled_ms, expected,
        "scheduled time should equal next_retry_time_ms"
    );
}

#[tokio::test]
async fn duplicate_reporting_error_then_error_is_rejected_and_no_extra_tasks() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let _job_id = shard
        .enqueue(None, priority, now, Some(policy), payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"boom".to_vec(),
            },
        )
        .await
        .expect("report1");

    // Duplicate error report should fail with LeaseNotFound
    let err = shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"boom2".to_vec(),
            },
        )
        .await
        .expect_err("dup should fail");
    match err {
        JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, t1),
        other => panic!("unexpected error: {other:?}"),
    }

    // Only one follow-up task exists
    let count = count_with_prefix(shard.db(), "tasks/").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn duplicate_reporting_error_then_success_is_rejected_and_state_persists() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let job_id = shard
        .enqueue(None, priority, now, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let t1 = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e".to_vec(),
            },
        )
        .await
        .expect("report1");
    let err = shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect_err("dup");
    match err {
        JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, t1),
        other => panic!("unexpected error: {other:?}"),
    }

    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get a1")
        .expect("exists");
    match a1.state() {
        AttemptStatus::Failed { .. } => {}
        _ => panic!("a1 should remain Failed"),
    }
}

#[tokio::test]
async fn heartbeat_after_outcome_returns_lease_not_found() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let _job_id = shard
        .enqueue(None, priority, now, None, payload, vec![])
        .await
        .expect("enqueue");
    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect("report ok");
    let err = shard
        .heartbeat_task("worker-1", &task_id)
        .await
        .expect_err("hb should fail");
    match err {
        JobStoreShardError::LeaseNotFound(t) => assert_eq!(t, task_id),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn job_deletion_race_before_success_outcome() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let job_id = shard
        .enqueue(None, priority, now, None, payload, vec![])
        .await
        .expect("enqueue");
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();
    shard.delete_job(&job_id).await.expect("delete");
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: b"ok".to_vec(),
            },
        )
        .await
        .expect("report ok");
    let lease = shard
        .db()
        .get(format!("lease/{}", task_id).as_bytes())
        .await
        .expect("get");
    assert!(lease.is_none(), "lease removed");
}

#[tokio::test]
async fn job_deletion_race_before_error_outcome_no_followup_task() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let job_id = shard
        .enqueue(None, priority, now, None, payload, vec![])
        .await
        .expect("enqueue");
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();
    shard.delete_job(&job_id).await.expect("delete");
    let err = shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e".to_vec(),
            },
        )
        .await
        .expect_err("report should return JobNotFound when job is missing");
    match err {
        JobStoreShardError::JobNotFound(jid) => assert_eq!(jid, job_id),
        other => panic!("unexpected error: {other:?}"),
    }
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(
        none_left.is_none(),
        "no follow-up task should be enqueued when job is missing"
    );
}

#[tokio::test]
async fn attempt_records_exist_across_retries_and_task_ids_distinct() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(None, priority, now, Some(policy), payload, vec![])
        .await
        .expect("enqueue");

    let tasks1 = shard.dequeue("w", 1).await.expect("dequeue1");
    let t1 = tasks1[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e1".to_vec(),
            },
        )
        .await
        .expect("report1");
    let (_k2, _v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("task2");
    let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2");
    let t2 = tasks2[0].attempt().task_id().to_string();
    assert_ne!(t1, t2, "task ids should be distinct across attempts");
    shard
        .report_attempt_outcome(
            &t2,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"e2".to_vec(),
            },
        )
        .await
        .expect("report2");

    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    let a2 = shard
        .get_job_attempt(&job_id, 2)
        .await
        .expect("get a2")
        .expect("a2 exists");
    match a1.state() {
        AttemptStatus::Failed { .. } => {}
        _ => panic!("a1 should be Failed"),
    }
    match a2.state() {
        AttemptStatus::Failed { .. } => {}
        _ => panic!("a2 should be Failed"),
    }
}

#[tokio::test]
async fn outcome_payload_edge_cases_empty_vectors_round_trip() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let job_id = shard
        .enqueue(None, priority, now, None, payload, vec![])
        .await
        .expect("enqueue");
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
        .await
        .expect("report ok");
    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get")
        .expect("exists");
    match a1.state() {
        AttemptStatus::Succeeded { result, .. } => {
            assert_eq!(result.len(), 0)
        }
        _ => panic!("expected Succeeded"),
    }

    // Re-enqueue a new job for error case
    let job_id2 = shard
        .enqueue(
            None,
            priority,
            now,
            None,
            serde_json::json!({"k": "v2"}),
            vec![],
        )
        .await
        .expect("enqueue2");
    let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2");
    let task2 = tasks2[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &task2,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: Vec::new(),
            },
        )
        .await
        .expect("report err");
    let a_err = shard
        .get_job_attempt(&job_id2, 1)
        .await
        .expect("get2")
        .expect("exists2");
    match a_err.state() {
        AttemptStatus::Failed { error, .. } => {
            assert_eq!(error.len(), 0)
        }
        _ => panic!("expected Failed"),
    }
}

#[tokio::test]
async fn concurrent_dequeue_many_workers_no_duplicates() {
    let (_tmp, shard) = open_temp_shard().await;
    let shard = Arc::new(shard);

    let total_jobs: usize = 200;
    let workers: usize = 8;
    let now = now_ms();

    // Enqueue many ready jobs
    for i in 0..total_jobs {
        let payload = serde_json::json!({"i": i});
        shard
            .enqueue(None, (i % 50) as u8, now, None, payload, vec![])
            .await
            .expect("enqueue");
    }

    // Shared trackers
    let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let processed = Arc::new(AtomicUsize::new(0));

    // Spawn workers
    let mut handles = Vec::new();
    for wi in 0..workers {
        let shard_cl = Arc::clone(&shard);
        let seen_cl = Arc::clone(&seen);
        let processed_cl = Arc::clone(&processed);
        let worker_id = format!("w-{wi}");
        handles.push(tokio::spawn(async move {
            loop {
                let tasks = shard_cl.dequeue(&worker_id, 1).await.expect("dequeue");
                if tasks.is_empty() {
                    if processed_cl.load(Ordering::Relaxed) >= total_jobs {
                        break;
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                let t = &tasks[0];
                let tid = t.attempt().task_id().to_string();
                // Validate uniqueness
                {
                    let mut g = seen_cl.lock().unwrap();
                    assert!(g.insert(tid.clone()), "duplicate task id dequeued: {tid}");
                }
                shard_cl
                    .report_attempt_outcome(&tid, AttemptOutcome::Success { result: Vec::new() })
                    .await
                    .expect("report ok");
                processed_cl.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(processed.load(Ordering::Relaxed), total_jobs);
    // No remaining tasks or leases
    assert_eq!(count_with_prefix(shard.db(), "tasks/").await, 0);
    assert_eq!(count_with_prefix(shard.db(), "lease/").await, 0);
}

#[tokio::test]
async fn future_tasks_are_not_dequeued_under_concurrency() {
    let (_tmp, shard) = open_temp_shard().await;
    let shard = Arc::new(shard);

    let ready_jobs: usize = 100;
    let future_jobs: usize = 100;
    let now = now_ms();
    let future = now + 60_000; // 60s in the future to avoid becoming ready during the test

    // Enqueue ready tasks
    for i in 0..ready_jobs {
        shard
            .enqueue(
                None,
                (i % 10) as u8,
                now,
                None,
                serde_json::json!({"r": i}),
                vec![],
            )
            .await
            .expect("enqueue ready");
    }
    // Enqueue future tasks
    for i in 0..future_jobs {
        shard
            .enqueue(
                None,
                (i % 10) as u8,
                future,
                None,
                serde_json::json!({"f": i}),
                vec![],
            )
            .await
            .expect("enqueue future");
    }

    // Concurrently drain ready tasks
    let processed = Arc::new(AtomicUsize::new(0));
    let workers = 6usize;
    let mut handles = Vec::new();
    for wi in 0..workers {
        let shard_cl = Arc::clone(&shard);
        let processed_cl = Arc::clone(&processed);
        let worker_id = format!("wf-{wi}");
        handles.push(tokio::spawn(async move {
            loop {
                let tasks = shard_cl.dequeue(&worker_id, 4).await.expect("dequeue");
                if tasks.is_empty() {
                    if processed_cl.load(Ordering::Relaxed) >= ready_jobs {
                        break;
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                for t in tasks {
                    let tid = t.attempt().task_id().to_string();
                    shard_cl
                        .report_attempt_outcome(&tid, AttemptOutcome::Success { result: vec![] })
                        .await
                        .expect("report ok");
                    processed_cl.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Only future tasks should remain in the queue
    // Ensure no ready tasks remain; any tasks left must be scheduled at or after `future`
    let ready_remaining = count_tasks_before(shard.db(), future).await;
    assert_eq!(ready_remaining, 0, "no ready tasks should remain");
}

#[tokio::test]
async fn large_outcome_payloads_round_trip() {
    let (_tmp, shard) = open_temp_shard().await;
    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now = now_ms();
    let job_id = shard
        .enqueue(None, priority, now, None, payload, vec![])
        .await
        .expect("enqueue");
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();
    let big_ok = vec![1u8; 2_000_000];
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Success {
                result: big_ok.clone(),
            },
        )
        .await
        .expect("report ok");
    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get")
        .expect("exists");
    match a1.state() {
        AttemptStatus::Succeeded { result, .. } => {
            assert_eq!(result.len(), big_ok.len())
        }
        _ => panic!("expected Succeeded"),
    }

    let job_id2 = shard
        .enqueue(
            None,
            priority,
            now,
            None,
            serde_json::json!({"k": "v2"}),
            vec![],
        )
        .await
        .expect("enqueue2");
    let tasks2 = shard.dequeue("w", 1).await.expect("dequeue2");
    let task2 = tasks2[0].attempt().task_id().to_string();
    let big_err = vec![2u8; 2_000_000];
    shard
        .report_attempt_outcome(
            &task2,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: big_err.clone(),
            },
        )
        .await
        .expect("report err");
    let a2 = shard
        .get_job_attempt(&job_id2, 1)
        .await
        .expect("get2")
        .expect("exists2");
    match a2.state() {
        AttemptStatus::Failed { error, .. } => {
            assert_eq!(error.len(), big_err.len())
        }
        _ => panic!("expected Failed"),
    }
}

#[tokio::test]
async fn priority_ordering_when_start_times_equal() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue two jobs with identical start_at_ms but different priorities
    let job_hi = shard
        .enqueue(
            None,
            1u8, // higher priority
            now,
            None,
            serde_json::json!({"j": "hi"}),
            vec![],
        )
        .await
        .expect("enqueue hi");
    let job_lo = shard
        .enqueue(
            None,
            50u8, // lower priority
            now,
            None,
            serde_json::json!({"j": "lo"}),
            vec![],
        )
        .await
        .expect("enqueue lo");

    // Dequeue two tasks; with equal times, lower priority number should come first
    let tasks = shard.dequeue("w", 2).await.expect("dequeue");
    assert_eq!(tasks.len(), 2);
    let t1 = &tasks[0];
    let t2 = &tasks[1];
    assert_eq!(t1.job().id(), job_hi);
    assert_eq!(t2.job().id(), job_lo);
    assert_eq!(t1.attempt().attempt_number(), 1);
    assert_eq!(t2.attempt().attempt_number(), 1);
}

#[tokio::test]
async fn concurrency_immediate_grant_enqueues_task_and_writes_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let payload = serde_json::json!({"k": "v"});
    let queue = "q1".to_string();
    // enqueue with limit 1
    let job_id = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            payload,
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue");

    // Task should be ready immediately
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let t = &tasks[0];
    assert_eq!(t.job().id(), job_id);

    // Holder should exist for this attempt's task id (holder is per-attempt)
    let holder = shard
        .db()
        .get(concurrency_holder_key(&queue, t.attempt().task_id()).as_bytes())
        .await
        .expect("get holder");
    assert!(
        holder.is_some(),
        "holder should be written for granted ticket"
    );
}

#[tokio::test]
async fn concurrency_queues_when_full_and_grants_on_release() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q2".to_string();

    // First job takes the single slot
    let _j1 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 1}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w1", 1).await.expect("deq1");
    assert_eq!(tasks1.len(), 1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Second job should queue a request (no immediate task visible)
    let _j2 = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"j": 2}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue2");
    // No new tasks yet
    let none = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none.is_none(), "no new task while holder is occupied");

    // Complete first task; this should release and grant next request, enqueuing its task
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");

    // Now there should be a new task for the queued request
    let some = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(some.is_some(), "task should be enqueued for next requester");
}

#[tokio::test]
async fn concurrency_held_queues_propagate_across_retries_and_release_on_finish() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q3".to_string();

    let _job = shard
        .enqueue(
            None,
            10u8,
            now,
            Some(silo::retry::RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            serde_json::json!({"j": 3}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue");

    let t1 = shard.dequeue("w", 1).await.expect("deq")[0]
        .attempt()
        .task_id()
        .to_string();

    // Fail attempt 1, should schedule attempt 2 carrying held_queues
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");

    // Attempt 2 should be present
    let t2 = shard.dequeue("w", 1).await.expect("deq2")[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2, which should release holder
    shard
        .report_attempt_outcome(&t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // No holders should remain after success of follow-up attempt (released after each attempt)
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
}

#[tokio::test]
async fn concurrency_retry_releases_original_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q3-retry".to_string();

    // Enqueue with a retry policy so we get a second attempt
    let _job_id = shard
        .enqueue(
            None,
            10u8,
            now,
            Some(silo::retry::RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 1,
                max_interval_ms: i64::MAX,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            serde_json::json!({"j": 33}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue");

    // Attempt 1 fails -> attempt 2 scheduled
    let t1 = shard.dequeue("w", 1).await.expect("deq1")[0]
        .attempt()
        .task_id()
        .to_string();
    shard
        .report_attempt_outcome(
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");
    let t2 = shard.dequeue("w", 1).await.expect("deq2")[0]
        .attempt()
        .task_id()
        .to_string();

    // Finish attempt 2
    shard
        .report_attempt_outcome(&t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report2");

    // BUG (current impl): holder created for attempt 1 task id remains. We assert no holders remain.
    assert_eq!(
        count_with_prefix(shard.db(), "holders/").await,
        0,
        "holders should be fully released after retries complete"
    );
}

#[tokio::test]
async fn concurrency_no_overgrant_after_release() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "q-overgrant".to_string();

    // A occupies the single slot
    let _a = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"a": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue a");
    let a_task = shard.dequeue("wa", 1).await.expect("deq a");
    assert_eq!(a_task.len(), 1);
    let a_tid = a_task[0].attempt().task_id().to_string();

    // B queues as a request
    let _b = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"b": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue b");

    // Complete A -> should grant B (durably create one holder)
    shard
        .report_attempt_outcome(&a_tid, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report a success");

    // Immediately enqueue C; if in-memory counts weren't bumped on grant-from-release,
    // implementation wrongly grants immediately, yielding 2 holders.
    let _c = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"c": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue c");

    // Count durable holders should never exceed 1
    let holders = count_with_prefix(shard.db(), "holders/").await;
    assert!(
        holders <= 1,
        "must not over-grant: holders={}, expected <= 1",
        holders
    );
}

#[tokio::test]
async fn stress_single_queue_no_double_grant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stress-q".to_string();

    // Enqueue many jobs concurrently into the same queue with limit 1
    let total = 50usize;
    for i in 0..total {
        let _ = shard
            .enqueue(
                None,
                (i % 10) as u8,
                now,
                None,
                serde_json::json!({"i": i}),
                vec![silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                }],
            )
            .await
            .expect("enqueue");
    }

    let mut processed = 0usize;
    loop {
        let tasks = shard.dequeue("w-stress", 1).await.expect("deq");
        if tasks.is_empty() {
            if processed >= total {
                break;
            } else {
                tokio::task::yield_now().await;
                continue;
            }
        }
        // Capacity is enforced via durable holders + in-memory gating; no double-grant observed via uniqueness assertions above
        let tid = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&tid, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report");
        processed += 1;
    }

    assert_eq!(processed, total);
    // No remaining durable state for holders/requests
    assert_eq!(count_with_prefix(shard.db(), "holders/").await, 0);
    assert_eq!(count_with_prefix(shard.db(), "requests/").await, 0);
}

#[tokio::test]
async fn concurrent_enqueues_while_holding_dont_bypass_limit() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "hold-q".to_string();

    // Take the only slot
    let _ = shard
        .enqueue(
            None,
            10u8,
            now,
            None,
            serde_json::json!({"first": true}),
            vec![silo::job::ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            }],
        )
        .await
        .expect("enqueue1");
    let tasks1 = shard.dequeue("w-hold", 1).await.expect("deq1");
    assert_eq!(tasks1.len(), 1);
    let t1 = tasks1[0].attempt().task_id().to_string();

    // Concurrently enqueue more jobs; they should queue as requests
    let add = 10usize;
    for i in 0..add {
        let _ = shard
            .enqueue(
                None,
                (i % 5) as u8,
                now,
                None,
                serde_json::json!({"i": i}),
                vec![silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                }],
            )
            .await
            .expect("enqueue add");
    }
    // There should be no ready tasks until we release
    assert!(first_kv_with_prefix(shard.db(), "tasks/").await.is_none());

    // Release first; only one new task should appear immediately
    shard
        .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report1");
    let after = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(after.is_some(), "one task should be enqueued after release");
}

#[tokio::test]
async fn reap_marks_expired_lease_as_failed_and_enqueues_retry() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let now = now_ms();
    let policy = RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(None, 10u8, now, Some(policy), payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let _leased_task_id = tasks[0].attempt().task_id().to_string();

    // Find the lease and rewrite expiry to the past
    let (lease_key, lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");
    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&lease_value) };
    let task = match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            held_queues: _,
        } => Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: Vec::new(),
        },
    };
    let expired_ms = now_ms() - 1;
    let new_record = LeaseRecord {
        worker_id: archived.worker_id.as_str().to_string(),
        task,
        expiry_ms: expired_ms,
    };
    let new_val = rkyv::to_bytes::<LeaseRecord, 256>(&new_record).unwrap();
    shard
        .db()
        .put(lease_key.as_bytes(), &new_val)
        .await
        .expect("put mutated lease");
    shard.db().flush().await.expect("flush mutated lease");

    let reaped = shard.reap_expired_leases().await.expect("reap");
    assert_eq!(reaped, 1);

    // Lease removed
    let lease = shard
        .db()
        .get(lease_key.as_bytes())
        .await
        .expect("get lease after reap");
    assert!(lease.is_none(), "lease should be removed by reaper");

    // Attempt 1 marked failed with WORKER_CRASHED
    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Failed { error_code, .. } => {
            assert_eq!(error_code, "WORKER_CRASHED")
        }
        _ => panic!("expected Failed"),
    }

    // Attempt 2 should be scheduled due to retry policy
    let (_k2, v2) = first_kv_with_prefix(shard.db(), "tasks/")
        .await
        .expect("attempt2 task exists");
    let a2_arch: &ArchivedTask = unsafe { rkyv::archived_root::<Task>(&v2) };
    let attempt2 = match a2_arch {
        ArchivedTask::RunAttempt { attempt_number, .. } => *attempt_number,
    };
    assert_eq!(attempt2, 2);
}

#[tokio::test]
async fn reap_ignores_unexpired_leases() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let now = now_ms();
    let job_id = shard
        .enqueue(None, 10u8, now, None, payload, vec![])
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    let _task_id = tasks[0].attempt().task_id().to_string();

    // Do not mutate the lease; it should not be reaped
    let (lease_key, _lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");

    let reaped = shard.reap_expired_leases().await.expect("reap");
    assert_eq!(reaped, 0);

    // Lease should still exist
    let lease = shard
        .db()
        .get(lease_key.as_bytes())
        .await
        .expect("get lease");
    assert!(lease.is_some(), "lease should remain when not expired");

    // Attempt state remains Running
    let a1 = shard
        .get_job_attempt(&job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Running { .. } => {}
        other => panic!("expected Running, got {:?}", other),
    }
}

#[tokio::test]
async fn delete_job_before_dequeue_skips_task_and_no_lease_created() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload, vec![])
        .await
        .expect("enqueue");

    // Verify a task exists in the ready queue
    let peek = shard.peek_tasks(10).await.expect("peek");
    assert_eq!(peek.len(), 1);

    // Delete the job before dequeue
    shard.delete_job(&job_id).await.expect("delete job");

    // Dequeue should skip the task (since job missing) and return nothing
    let tasks = shard.dequeue("w", 1).await.expect("dequeue");
    assert!(
        tasks.is_empty(),
        "no tasks should be returned when job missing"
    );

    // Ensure original task key was deleted (since we skipped it)
    let none_left = first_kv_with_prefix(shard.db(), "tasks/").await;
    assert!(none_left.is_none(), "no tasks should remain after skip");

    // Ensure no lease was created
    let lease_any = first_kv_with_prefix(shard.db(), "lease/").await;
    assert!(
        lease_any.is_none(),
        "no lease should be created for skipped task"
    );
}
