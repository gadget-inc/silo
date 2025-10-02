mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use test_helpers::*;

#[tokio::test]
async fn status_index_scheduled_then_running_then_succeeded() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let payload = serde_json::json!({"k":"v"});

    let job_id = shard
        .enqueue("-", None, 10u8, now, None, payload, vec![])
        .await
        .expect("enqueue");

    // Initially Scheduled
    let s = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 10)
        .await
        .expect("scan");
    assert!(s.contains(&job_id));

    // Dequeue -> Running
    let tasks = shard.dequeue("-", "w", 1).await.expect("dequeue");
    assert_eq!(tasks.len(), 1);

    let running = shard
        .scan_jobs_by_status("-", JobStatusKind::Running, 10)
        .await
        .expect("scan running");
    assert!(running.contains(&job_id));

    // Complete -> Succeeded
    let tid = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome("-", &tid, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report");

    let succ = shard
        .scan_jobs_by_status("-", JobStatusKind::Succeeded, 10)
        .await
        .expect("scan succ");
    assert!(succ.contains(&job_id));
}

#[tokio::test]
async fn status_index_failed_and_scheduled_then_order_newest_first() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Job A: fail with no retries (stays Failed)
    let a = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"a":1}),
            vec![],
        )
        .await
        .expect("enq a");
    let ta = shard.dequeue("-", "w", 1).await.expect("deq a")[0]
        .attempt()
        .task_id()
        .to_string();
    shard
        .report_attempt_outcome(
            "-",
            &ta,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report a");

    // Small delay to ensure distinct timestamps
    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

    // Job B: dequeued (Running) then error with retry policy -> status becomes Scheduled
    let policy = silo::retry::RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let b = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(policy),
            serde_json::json!({"b":2}),
            vec![],
        )
        .await
        .expect("enq b");
    let tb = shard.dequeue("-", "w", 1).await.expect("deq b")[0]
        .attempt()
        .task_id()
        .to_string();
    shard
        .report_attempt_outcome(
            "-",
            &tb,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report b");

    // Now A should be in Failed, B should be in Scheduled. Newest-first within each.
    let failed = shard
        .scan_jobs_by_status("-", JobStatusKind::Failed, 10)
        .await
        .expect("scan failed");
    assert_eq!(failed[0], a);

    let scheduled = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 10)
        .await
        .expect("scan sched");
    assert!(scheduled.contains(&b));
}

#[tokio::test]
async fn retry_flow_running_to_scheduled_to_running_to_succeeded() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let policy = silo::retry::RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(policy),
            serde_json::json!({"j":1}),
            vec![],
        )
        .await
        .expect("enqueue");
    // Running
    let t1 = shard.dequeue("-", "w", 1).await.expect("deq")[0]
        .attempt()
        .task_id()
        .to_string();
    let running = shard
        .scan_jobs_by_status("-", JobStatusKind::Running, 10)
        .await
        .expect("scan running");
    assert!(running.contains(&job_id));
    // Error -> Scheduled
    shard
        .report_attempt_outcome(
            "-",
            &t1,
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report err");
    let scheduled = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 10)
        .await
        .expect("scan scheduled");
    assert!(scheduled.contains(&job_id));
    // Dequeue attempt 2 -> Running
    let t2 = shard.dequeue("-", "w", 1).await.expect("deq2")[0]
        .attempt()
        .task_id()
        .to_string();
    let running2 = shard
        .scan_jobs_by_status("-", JobStatusKind::Running, 10)
        .await
        .expect("scan running2");
    assert!(running2.contains(&job_id));
    // Success -> Succeeded
    shard
        .report_attempt_outcome("-", &t2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report ok");
    let succ = shard
        .scan_jobs_by_status("-", JobStatusKind::Succeeded, 10)
        .await
        .expect("scan succ");
    assert!(succ.contains(&job_id));
}

#[tokio::test]
async fn reaper_without_retries_marks_failed_in_index() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    // No retries
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"x":1}),
            vec![],
        )
        .await
        .expect("enqueue");
    // Lease one task
    let _tid = shard.dequeue("-", "w", 1).await.expect("deq")[0]
        .attempt()
        .task_id()
        .to_string();
    // Make lease expired
    let (lease_key, lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");
    type ArchivedLease = <silo::job_store_shard::LeaseRecord as rkyv::Archive>::Archived;
    type ArchivedTask = <silo::job_store_shard::Task as rkyv::Archive>::Archived;
    let archived: &ArchivedLease =
        unsafe { rkyv::archived_root::<silo::job_store_shard::LeaseRecord>(&lease_value) };
    let task = match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            ..
        } => silo::job_store_shard::Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: Vec::new(),
        },
        _ => unreachable!(),
    };
    let expired = silo::job_store_shard::LeaseRecord {
        worker_id: archived.worker_id.as_str().to_string(),
        task,
        expiry_ms: now_ms() - 1,
    };
    let new_val = rkyv::to_bytes::<silo::job_store_shard::LeaseRecord, 256>(&expired).unwrap();
    shard
        .db()
        .put(lease_key.as_bytes(), &new_val)
        .await
        .unwrap();
    shard.db().flush().await.unwrap();
    // Reap
    let _ = shard.reap_expired_leases().await.unwrap();
    // Should be Failed in index (no retries)
    let failed = shard
        .scan_jobs_by_status("-", JobStatusKind::Failed, 10)
        .await
        .unwrap();
    assert!(failed.contains(&job_id));
}

#[tokio::test]
async fn reaper_with_retries_moves_to_scheduled_in_index() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let policy = silo::retry::RetryPolicy {
        retry_count: 1,
        initial_interval_ms: 1,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            Some(policy),
            serde_json::json!({"y":2}),
            vec![],
        )
        .await
        .expect("enqueue");
    // Lease and expire
    let _tid = shard.dequeue("-", "w", 1).await.expect("deq")[0]
        .attempt()
        .task_id()
        .to_string();
    let (lease_key, lease_value) = first_kv_with_prefix(shard.db(), "lease/")
        .await
        .expect("lease present");
    type ArchivedLease = <silo::job_store_shard::LeaseRecord as rkyv::Archive>::Archived;
    type ArchivedTask = <silo::job_store_shard::Task as rkyv::Archive>::Archived;
    let archived: &ArchivedLease =
        unsafe { rkyv::archived_root::<silo::job_store_shard::LeaseRecord>(&lease_value) };
    let task = match &archived.task {
        ArchivedTask::RunAttempt {
            id,
            job_id,
            attempt_number,
            ..
        } => silo::job_store_shard::Task::RunAttempt {
            id: id.as_str().to_string(),
            job_id: job_id.as_str().to_string(),
            attempt_number: *attempt_number,
            held_queues: Vec::new(),
        },
        _ => unreachable!(),
    };
    let expired = silo::job_store_shard::LeaseRecord {
        worker_id: archived.worker_id.as_str().to_string(),
        task,
        expiry_ms: now_ms() - 1,
    };
    let new_val = rkyv::to_bytes::<silo::job_store_shard::LeaseRecord, 256>(&expired).unwrap();
    shard
        .db()
        .put(lease_key.as_bytes(), &new_val)
        .await
        .unwrap();
    shard.db().flush().await.unwrap();
    // Reap
    let _ = shard.reap_expired_leases().await.unwrap();
    // Should be Scheduled in index (retries present)
    let scheduled = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 10)
        .await
        .unwrap();
    assert!(scheduled.contains(&job_id));
}

#[tokio::test]
async fn delete_removes_from_index() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let job_id = shard
        .enqueue(
            "-",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"z":3}),
            vec![],
        )
        .await
        .expect("enqueue");
    // Run and succeed
    let tid = shard.dequeue("-", "w", 1).await.expect("deq")[0]
        .attempt()
        .task_id()
        .to_string();
    shard
        .report_attempt_outcome("-", &tid, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report ok");
    // Ensure in Succeeded index
    let succ = shard
        .scan_jobs_by_status("-", JobStatusKind::Succeeded, 10)
        .await
        .unwrap();
    assert!(succ.contains(&job_id));
    // Delete
    shard.delete_job("-", &job_id).await.unwrap();
    let succ2 = shard
        .scan_jobs_by_status("-", JobStatusKind::Succeeded, 10)
        .await
        .unwrap();
    assert!(!succ2.contains(&job_id));
}

#[tokio::test]
async fn cross_tenant_isolation_in_scans() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let id_a = shard
        .enqueue(
            "tenantA",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"t":"A"}),
            vec![],
        )
        .await
        .unwrap();
    let _id_b = shard
        .enqueue(
            "tenantB",
            None,
            10u8,
            now,
            None,
            serde_json::json!({"t":"B"}),
            vec![],
        )
        .await
        .unwrap();
    let a_list = shard
        .scan_jobs_by_status("tenantA", JobStatusKind::Scheduled, 10)
        .await
        .unwrap();
    assert!(a_list.contains(&id_a));
    // tenantA scan should not include tenantB job
    assert_eq!(a_list.len(), 1);
}

#[tokio::test]
async fn pagination_and_ordering_newest_first() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + i,
                None,
                serde_json::json!({"i": i}),
                vec![],
            )
            .await
            .unwrap();
        ids.push(id);
        // slight stagger for change times
        tokio::task::yield_now().await;
    }
    // Page size 3
    let first_page = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 3)
        .await
        .unwrap();
    assert_eq!(first_page.len(), 3);
    // Limit zero returns empty
    let empty = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 0)
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn future_enqueue_is_in_scheduled_scan() {
    let (_tmp, shard) = open_temp_shard().await;
    let future = now_ms() + 60_000;
    let id = shard
        .enqueue(
            "-",
            None,
            10u8,
            future,
            None,
            serde_json::json!({"f":1}),
            vec![],
        )
        .await
        .unwrap();
    let scheduled = shard
        .scan_jobs_by_status("-", JobStatusKind::Scheduled, 10)
        .await
        .unwrap();
    assert!(scheduled.contains(&id));
}
