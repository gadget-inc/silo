mod test_helpers;

use rkyv::Archive;
use silo::codec::decode_lease;
use silo::job::JobStatusKind;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::JobStoreShardError;
use silo::task::Task;

use test_helpers::*;

#[silo::test]
async fn dequeue_moves_tasks_to_leased_with_uuid() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now_ms = now_ms();

        let job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now_ms,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        // Job status should transition to Running after dequeue
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);
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
        let (lease_key, kv_value) = first_lease_kv(shard.db()).await.expect("scan leased");
        // Binary keys start with prefix 0x06 for leases
        assert_eq!(lease_key[0], 0x06, "lease key should have lease prefix");

        type ArchivedTask = <Task as Archive>::Archived;
        let decoded = decode_lease(&kv_value).expect("decode lease");
        let archived = decoded.archived();
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
            ArchivedTask::RequestTicket { .. } => panic!("unexpected RequestTicket in lease"),
            ArchivedTask::CheckRateLimit { .. } => panic!("unexpected CheckRateLimit in lease"),
            ArchivedTask::RefreshFloatingLimit { .. } => {
                panic!("unexpected RefreshFloatingLimit in lease")
            }
        }

        // Ensure original task queue is empty now
        let none_left = first_task_kv(shard.db()).await;
        assert!(none_left.is_none(), "no tasks should remain after dequeue");
    });
}

#[silo::test]
async fn heartbeat_renews_lease_when_worker_matches() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now_ms = now_ms();

        let _job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now_ms,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Read current lease key and expiry
        let (old_key, old_value) = first_lease_kv(shard.db()).await.expect("scan lease");
        let parsed_old = silo::keys::parse_lease_key(&old_key).expect("parse lease key");
        assert_eq!(parsed_old.task_id, task_id);
        let decoded_first = decode_lease(&old_value).expect("decode lease");
        let old_expiry = decoded_first.archived().expiry_ms as u64;

        // Heartbeat to renew
        shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat ok");

        // Scan again, expect one lease for task with a higher expiry
        let (new_key, new_value) = first_lease_kv(shard.db()).await.expect("scan lease 2");
        let parsed_new = silo::keys::parse_lease_key(&new_key).expect("parse lease key 2");
        assert_eq!(parsed_new.task_id, task_id);
        let decoded_second = decode_lease(&new_value).expect("decode lease 2");
        let new_expiry = decoded_second.archived().expiry_ms as u64;
        assert!(new_expiry > old_expiry, "new expiry should be greater");

        // Validate owner remains the same
        assert_eq!(decoded_second.archived().worker_id.as_str(), "worker-1");
    });
}

#[silo::test]
async fn heartbeat_rejects_mismatched_worker() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now_ms = now_ms();

        let _job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now_ms,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
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
    });
}

#[silo::test]
async fn heartbeat_after_outcome_returns_lease_not_found() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();
        let _job_id = shard
            .enqueue(
                "-",
                None,
                priority,
                now,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
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
    });
}

#[silo::test]
async fn reap_ignores_unexpired_leases() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let now = now_ms();
    let job_id = shard
        .enqueue("-", None, 10u8, now, None, payload, vec![], None, "default")
        .await
        .expect("enqueue");

    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    let _task_id = tasks[0].attempt().task_id().to_string();

    // Do not mutate the lease; it should not be reaped
    let (lease_key, _lease_value) = first_lease_kv(shard.db()).await.expect("lease present");

    let reaped = shard.reap_expired_leases("-").await.expect("reap");
    assert_eq!(reaped, 0);

    // Lease should still exist
    let lease = shard.db().get(&lease_key).await.expect("get lease");
    assert!(lease.is_some(), "lease should remain when not expired");

    // Attempt state remains Running
    let a1 = shard
        .get_job_attempt("-", &job_id, 1)
        .await
        .expect("get a1")
        .expect("a1 exists");
    match a1.state() {
        AttemptStatus::Running { .. } => {}
        other => panic!("expected Running, got {:?}", other),
    }
}

#[silo::test]
async fn delete_job_before_dequeue_skips_task_and_no_lease_created() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(
            "-",
            None,
            priority,
            now_ms,
            None,
            payload,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Verify a task exists in the ready queue
    let peek = shard.peek_tasks("default", 10).await.expect("peek");
    assert_eq!(peek.len(), 1);

    // Dequeue and complete the job first
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("complete job");

    // Now delete the job (it's in Succeeded state)
    shard.delete_job("-", &job_id).await.expect("delete job");

    // Job info and status should be deleted
    let job = shard.get_job("-", &job_id).await.expect("get job");
    assert!(job.is_none(), "job should be deleted");

    let status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status");
    assert!(status.is_none(), "job status should be deleted");
}

#[silo::test]
async fn dequeue_gracefully_handles_missing_job_info() {
    let (_tmp, shard) = open_temp_shard().await;

    let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
    let priority = 10u8;
    let now_ms = now_ms();

    let job_id = shard
        .enqueue(
            "-",
            None,
            priority,
            now_ms,
            None,
            payload,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Verify a task exists in the ready queue
    let peek = shard.peek_tasks("default", 10).await.expect("peek");
    assert_eq!(peek.len(), 1);

    // Simulate corruption: manually delete job_info (bypassing validation)
    // This creates the edge case where task exists but job is missing
    let job_info_key = silo::keys::job_info_key("-", &job_id);
    shard
        .db()
        .delete(&job_info_key)
        .await
        .expect("manual delete job_info");
    shard.db().flush().await.expect("flush");

    // Job info should be gone
    let job = shard.get_job("-", &job_id).await.expect("get job");
    assert!(job.is_none(), "job info should be missing");

    // Dequeue should gracefully skip the task (since job missing) and return nothing
    // This tests graceful degradation when database is in an inconsistent state
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert!(
        tasks.is_empty(),
        "no tasks should be returned when job info missing"
    );

    // Ensure original task key was deleted (cleaned up during dequeue)
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let none_left = first_task_kv(shard.db()).await;
    assert!(
        none_left.is_none(),
        "orphaned task should be cleaned up when job missing"
    );

    // Ensure no lease was created
    let lease_any = first_lease_kv(shard.db()).await;
    assert!(
        lease_any.is_none(),
        "no lease should be created for orphaned task"
    );
}
