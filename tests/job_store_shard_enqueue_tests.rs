mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::task::Task;

use test_helpers::*;

#[silo::test]
async fn enqueue_round_trip_with_explicit_id() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"hello": "world"});
    let payload_bytes = test_helpers::msgpack_payload(&payload);
    let id = "job-123".to_string();
    let priority = 50u8;
    let start_time_ms = 1_700_000_000_000i64;

    let got_id = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority,
            start_time_ms,
            None,
            payload_bytes,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

    // Job status should be Scheduled after enqueue
    let status = shard
        .get_job_status("-", &id)
        .await
        .expect("get status")
        .expect("exists");
    assert_eq!(status.kind, JobStatusKind::Scheduled);

    let view = shard
        .get_job("-", &id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.id(), id);
    assert_eq!(view.priority(), priority);
    assert_eq!(view.enqueue_time_ms(), start_time_ms);
    let got_payload = view.payload_as_json().unwrap();
    assert_eq!(got_payload, payload);

    // A task should be present and point at attempt 1 for this job
    let tasks = shard.peek_tasks("default", 10).await.expect("peek");
    assert!(!tasks.is_empty());
    assert!(
        matches!(tasks[0], Task::RunAttempt { ref job_id, attempt_number, .. } if job_id == &id && attempt_number == 1)
    );
}

#[silo::test]
async fn enqueue_with_metadata_round_trips_in_job_view() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"m": true});
    let payload_bytes = test_helpers::msgpack_payload(&payload);
    let priority = 5u8;
    let start_at_ms = 123i64;
    let md: Vec<(String, String)> = vec![
        ("a".to_string(), "1".to_string()),
        ("b".to_string(), "two".to_string()),
    ];

    let job_id = shard
        .enqueue(
            "-",
            None,
            priority,
            start_at_ms,
            None,
            payload_bytes,
            vec![],
            Some(md.clone()),
            "default",
        )
        .await
        .expect("enqueue_with_metadata");

    let view = shard
        .get_job("-", &job_id)
        .await
        .expect("get_job")
        .expect("exists");
    let got_md = view.metadata();
    // Convert to map for easy comparison
    let mut map: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (k, v) in got_md {
        map.insert(k, v);
    }
    let mut exp: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (k, v) in md {
        exp.insert(k, v);
    }
    assert_eq!(map, exp, "metadata should roundtrip in JobView");
}

#[silo::test]
async fn enqueue_generates_uuid_when_none_provided() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let payload_bytes = test_helpers::msgpack_payload(&payload);
    let priority = 10u8;
    let start_time_ms = 12345i64;

    let got_id = shard
        .enqueue(
            "-",
            None,
            priority,
            start_time_ms,
            None,
            payload_bytes,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    assert!(!got_id.is_empty());
    assert!(uuid::Uuid::parse_str(&got_id).is_ok(), "expected UUIDv4 id");

    let view = shard
        .get_job("-", &got_id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.id(), got_id);
    assert_eq!(view.priority(), priority);
    assert_eq!(view.enqueue_time_ms(), start_time_ms);
    let got_payload = view.payload_as_json().unwrap();
    assert_eq!(got_payload, payload);

    // A task should be present and point at attempt 1 for this job
    let tasks = shard.peek_tasks("default", 10).await.expect("peek");
    assert!(!tasks.is_empty());
    assert!(
        matches!(tasks[0], Task::RunAttempt { ref job_id, attempt_number, .. } if job_id == &got_id && attempt_number == 1)
    );
}

#[silo::test]
async fn delete_job_removes_key_and_is_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue a job
    let payload = serde_json::json!({"hello": "world"});
    let payload_bytes = test_helpers::msgpack_payload(&payload);
    let id = "job-to-delete".to_string();
    let priority = 42u8;
    let start_time_ms = 1_700_000_000_001i64;

    let got_id = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority,
            start_time_ms,
            None,
            payload_bytes,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    assert_eq!(got_id, id);

    // Ensure it exists
    assert!(shard.get_job("-", &id).await.expect("get_job").is_some());

    // Complete the job first (can't delete while scheduled/running)
    let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("complete job");

    // Now delete it
    shard.delete_job("-", &id).await.expect("delete_job");

    // Ensure it's gone
    let got = shard.get_job("-", &id).await.expect("get_job");
    assert!(got.is_none(), "job should be deleted");

    // Delete again (idempotent)
    shard.delete_job("-", &id).await.expect("delete_job again");
    let got = shard.get_job("-", &id).await.expect("get_job");
    assert!(got.is_none(), "job should remain deleted");
}

#[silo::test]
async fn peek_omits_future_scheduled_tasks() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = serde_json::json!({"k": "v"});
    let payload_bytes = test_helpers::msgpack_payload(&payload);
    let priority = 10u8;
    let now_ms = now_ms();
    let future_ms = now_ms + 60_000;

    let _ = shard
        .enqueue("-", None, priority, future_ms, None, payload_bytes, vec![], None, "default")
        .await
        .expect("enqueue");

    // Should not see the task yet
    let tasks = shard.peek_tasks("default", 10).await.expect("peek");
    assert!(tasks.is_empty(), "future task should not be visible");
}

#[silo::test]
async fn enqueue_fails_when_id_already_exists_and_db_unchanged() {
    let (_tmp, shard) = open_temp_shard().await;

    let id = "dup-job".to_string();
    let payload1 = serde_json::json!({"v": 1});
    let payload1_bytes = test_helpers::msgpack_payload(&payload1);
    let priority1 = 10u8;
    let start1 = 1_700_000_123_000i64;

    let got_id = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority1,
            start1,
            None,
            payload1_bytes,
            vec![],
            None,
            "default",
        )
        .await
        .expect("first enqueue ok");
    assert_eq!(got_id, id);

    // Pre-duplicate snapshot
    let jobs_before = count_with_prefix(shard.db(), "jobs/").await;
    let tasks_before = count_with_prefix(shard.db(), "tasks/").await;

    // Attempt duplicate enqueue with different values to ensure no overwrite occurs
    let payload2 = serde_json::json!({"v": 2});
    let payload2_bytes = test_helpers::msgpack_payload(&payload2);
    let priority2 = 20u8;
    let start2 = start1 + 999_000;

    let err = shard
        .enqueue(
            "-",
            Some(id.clone()),
            priority2,
            start2,
            None,
            payload2_bytes,
            vec![],
            None,
            "default",
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
    let view = shard
        .get_job("-", &id)
        .await
        .expect("get_job")
        .expect("exists");
    assert_eq!(view.priority(), priority1);
    assert_eq!(view.enqueue_time_ms(), start1);
    let got_payload = view.payload_as_json().unwrap();
    assert_eq!(got_payload, payload1);
}

#[silo::test]
async fn cannot_delete_running_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload_bytes = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload_bytes, vec![], None, "default")
            .await
            .expect("enqueue");
        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Attempt to delete while running - should fail
        let err = shard
            .delete_job("-", &job_id)
            .await
            .expect_err("delete should fail");
        match err {
            JobStoreShardError::JobInProgress(jid) => assert_eq!(jid, job_id),
            other => panic!("expected JobInProgress, got {:?}", other),
        }

        // Complete the job
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"ok".to_vec(),
                },
            )
            .await
            .expect("report ok");

        // Now delete should succeed
        shard
            .delete_job("-", &job_id)
            .await
            .expect("delete after completion");

        // Job should be gone
        let job = shard.get_job("-", &job_id).await.expect("get job");
        assert!(job.is_none(), "job should be deleted");
    });
}

#[silo::test]
async fn cannot_delete_scheduled_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload_bytes = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let job_id = shard
            .enqueue("-", None, priority, now, None, payload_bytes, vec![], None, "default")
            .await
            .expect("enqueue");

        // Job is scheduled but not yet dequeued
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Attempt to delete - should fail
        let err = shard
            .delete_job("-", &job_id)
            .await
            .expect_err("delete should fail");
        match err {
            JobStoreShardError::JobInProgress(jid) => assert_eq!(jid, job_id),
            other => panic!("expected JobInProgress, got {:?}", other),
        }

        // Complete the job first
        let tasks = shard.dequeue("w", "default", 1).await.expect("dequeue").tasks;
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

        // Now delete should succeed
        shard
            .delete_job("-", &job_id)
            .await
            .expect("delete after completion");
    });
}

#[silo::test]
async fn priority_ordering_when_start_times_equal() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let now = now_ms();

        // Enqueue two jobs with identical start_at_ms but different priorities
        let job_hi = shard
            .enqueue(
                "-",
                None,
                1u8, // higher priority
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": "hi"})),
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue hi");
        let job_lo = shard
            .enqueue(
                "-",
                None,
                50u8, // lower priority
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": "lo"})),
                vec![],
                None,
            "default",
            )
            .await
            .expect("enqueue lo");

        // Dequeue two tasks; with equal times, lower priority number should come first
        let tasks = shard.dequeue("w", "default", 2).await.expect("dequeue").tasks;
        assert_eq!(tasks.len(), 2);
        let t1 = &tasks[0];
        let t2 = &tasks[1];
        assert_eq!(t1.job().id(), job_hi);
        assert_eq!(t2.job().id(), job_lo);
        assert_eq!(t1.attempt().attempt_number(), 1);
        assert_eq!(t2.attempt().attempt_number(), 1);
    });
}
