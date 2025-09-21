use rkyv::Archive;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard::{AttemptOutcome, LeaseRecord, Shard, ShardError, Task};
use slatedb::DbIterator;

#[tokio::test]
async fn enqueue_round_trip_with_explicit_id() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

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
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

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
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let start_time_ms = 12345i64;

    let got_id = shard
        .enqueue(None, priority, start_time_ms, None, payload.clone())
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
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

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
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let future_ms = now_ms + 60_000;

    let _ = shard
        .enqueue(None, priority, future_ms, None, payload)
        .await
        .expect("enqueue");

    // Should not see the task yet
    let tasks = shard.peek_tasks(10).await.expect("peek");
    assert!(tasks.is_empty(), "future task should not be visible");
}

#[tokio::test]
async fn dequeue_moves_tasks_to_leased_with_uuid() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload)
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let leased_task_id = match &tasks[0] {
        Task::RunAttempt {
            id,
            job_id: jid,
            attempt_number,
        } => {
            assert_eq!(jid, &job_id);
            assert_eq!(*attempt_number, 1);
            assert!(uuid::Uuid::parse_str(id).is_ok(), "task id is UUID");
            id.clone()
        }
    };

    // Verify a leased entry exists, and includes worker id
    let start: Vec<u8> = b"lease/".to_vec();
    let mut end: Vec<u8> = "lease/".to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = shard
        .db()
        .scan::<Vec<u8>, _>(start..=end)
        .await
        .expect("scan leased");
    let first = iter.next().await.expect("iter");
    assert!(first.is_some(), "leased entry should exist");
    let kv = first.unwrap();
    let key_str = std::str::from_utf8(&kv.key).unwrap();
    assert!(key_str.starts_with("lease/"));

    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    type ArchivedTask = <Task as Archive>::Archived;
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&kv.value) };
    assert_eq!(archived.worker_id.as_str(), "worker-1");
    match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id: jid,
            attempt_number,
        } => {
            assert_eq!(id.as_str(), leased_task_id);
            assert_eq!(jid.as_str(), job_id);
            assert_eq!(*attempt_number, 1);
        }
    }

    // Ensure original task queue is empty now
    let start_t: Vec<u8> = b"tasks/".to_vec();
    let mut end_t: Vec<u8> = "tasks/".to_string().into_bytes();
    end_t.push(0xFF);
    let mut iter_t: DbIterator = shard
        .db()
        .scan::<Vec<u8>, _>(start_t..=end_t)
        .await
        .expect("scan tasks");
    let none_left = iter_t.next().await.expect("iter tasks");
    assert!(none_left.is_none(), "no tasks should remain after dequeue");
}

// Lease expiry now lives in value, not in key

#[tokio::test]
async fn heartbeat_renews_lease_when_worker_matches() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let _job_id = shard
        .enqueue(None, priority, now_ms, None, payload)
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = match &tasks[0] {
        Task::RunAttempt { id, .. } => id.clone(),
    };

    // Read current lease key and expiry
    let start: Vec<u8> = b"lease/".to_vec();
    let mut end: Vec<u8> = "lease/".to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = shard
        .db()
        .scan::<Vec<u8>, _>(start..=end)
        .await
        .expect("scan lease");
    let first = iter.next().await.expect("iter").expect("some lease");
    let old_key = std::str::from_utf8(&first.key).unwrap().to_string();
    assert!(old_key.ends_with(&task_id));
    type ArchivedLease = <LeaseRecord as Archive>::Archived;
    let archived_lease: &ArchivedLease =
        unsafe { rkyv::archived_root::<LeaseRecord>(&first.value) };
    let old_expiry = archived_lease.expiry_ms as u64;

    // Heartbeat to renew
    shard
        .heartbeat_task("worker-1", &task_id)
        .await
        .expect("heartbeat ok");

    // Scan again, expect one lease for task with a higher expiry
    let mut iter2: DbIterator = shard
        .db()
        .scan::<Vec<u8>, _>(
            b"lease/".to_vec()..={
                let mut e = "lease/".to_string().into_bytes();
                e.push(0xFF);
                e
            },
        )
        .await
        .expect("scan lease 2");
    let second = iter2.next().await.expect("iter2").expect("some lease");
    let new_key = std::str::from_utf8(&second.key).unwrap().to_string();
    assert!(new_key.ends_with(&task_id));
    let archived_lease2: &ArchivedLease =
        unsafe { rkyv::archived_root::<LeaseRecord>(&second.value) };
    let new_expiry = archived_lease2.expiry_ms as u64;
    assert!(new_expiry > old_expiry, "new expiry should be greater");

    // Validate owner remains the same
    let archived: &ArchivedLease = unsafe { rkyv::archived_root::<LeaseRecord>(&second.value) };
    assert_eq!(archived.worker_id.as_str(), "worker-1");
}

#[tokio::test]
async fn heartbeat_rejects_mismatched_worker() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let _job_id = shard
        .enqueue(None, priority, now_ms, None, payload)
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = match &tasks[0] {
        Task::RunAttempt { id, .. } => id.clone(),
    };

    let err = shard
        .heartbeat_task("worker-2", &task_id)
        .await
        .expect_err("heartbeat should fail");

    match err {
        ShardError::LeaseOwnerMismatch {
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
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");

    let payload = serde_json::json!({"k": "v"});
    let priority = 10u8;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let job_id = shard
        .enqueue(None, priority, now_ms, None, payload)
        .await
        .expect("enqueue");

    let tasks = shard.dequeue("worker-1", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);
    let task_id = match &tasks[0] {
        Task::RunAttempt { id, .. } => id.clone(),
    };

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
    let attempt_key = format!("attempts/{}/{}", job_id, 1);
    let attempt_bytes = shard
        .db()
        .get(attempt_key.as_bytes())
        .await
        .expect("get attempt")
        .expect("attempt exists");

    use rkyv::Archive;
    type ArchivedAttempt = <silo::shard::JobAttempt as Archive>::Archived;
    type ArchivedState = <silo::shard::AttemptState as Archive>::Archived;
    let attempt: &ArchivedAttempt =
        unsafe { rkyv::archived_root::<silo::shard::JobAttempt>(&attempt_bytes) };
    if let ArchivedState::Succeeded { .. } = &attempt.state {
        // ok
    } else {
        panic!("expected Succeeded");
    }
}

#[tokio::test]
async fn reporting_attempt_outcome_errors_if_no_lease() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
    };

    let shard = Shard::open(&cfg).await.expect("open shard");
    let err = shard
        .report_attempt_outcome(
            "non-existent-task",
            AttemptOutcome::Error {
                error: b"boom".to_vec(),
            },
        )
        .await
        .expect_err("should error");
    match err {
        ShardError::LeaseNotFound(t) => assert_eq!(t, "non-existent-task"),
        other => panic!("unexpected error: {other:?}"),
    }
}
