mod test_helpers;

use silo::codec::{decode_lease, encode_lease};
use silo::job::JobStatusKind;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::JobStoreShardError;
use silo::task::LeaseRecord;

use test_helpers::*;

#[tokio::test]
async fn cancel_scheduled_job_sets_cancelled_and_removes_task() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Verify counters after enqueue
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 0);

        // Verify job is Scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Verify task exists in queue
        let tasks_before = shard.peek_tasks("default", 10).await.expect("peek");
        assert!(!tasks_before.is_empty());

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify counters after cancel - completed_jobs incremented because scheduled job immediately becomes Cancelled
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(
            counters.completed_jobs, 1,
            "completed_jobs should be 1 after cancelling scheduled job"
        );

        // Verify status is now Cancelled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Cancelled);

        // Verify cancellation flag is set
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "job should be marked as cancelled"
        );

        // Task should be eagerly removed from DB queue by cancel (not lazily on dequeue)
        let none_left = first_task_kv(shard.db()).await;
        assert!(
            none_left.is_none(),
            "task should be removed from queue immediately by cancel"
        );

        // Dequeue should return empty (no tasks remain)
        let dequeued = shard
            .dequeue("w1", "default", 10)
            .await
            .expect("dequeue")
            .tasks;
        assert!(
            dequeued.is_empty(),
            "dequeue should return empty after cancel"
        );
    });
}

/// Cancel preserves scheduling fields (current_attempt, next_attempt_starts_after_ms)
/// in the Cancelled status so task keys can always be reconstructed O(1).

#[tokio::test]
async fn cancel_preserves_scheduling_fields_in_status() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Get the scheduled status to see original scheduling fields
        let status_before = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_before.kind, JobStatusKind::Scheduled);
        let original_attempt = status_before.current_attempt;
        let original_starts_after = status_before.next_attempt_starts_after_ms;

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify status is Cancelled with scheduling fields preserved
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Cancelled);
        assert_eq!(
            status_after.current_attempt, original_attempt,
            "current_attempt should be preserved in Cancelled status"
        );
        assert_eq!(
            status_after.next_attempt_starts_after_ms, original_starts_after,
            "next_attempt_starts_after_ms should be preserved in Cancelled status"
        );
    });
}

/// Cancel running job sets cancellation flag but keeps status as Running
/// Worker discovers cancellation on heartbeat

#[tokio::test]
async fn cancel_running_job_sets_flag_status_stays_running() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Dequeue to make it Running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        // Verify job is Running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify cancellation flag is set
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "job should be marked as cancelled"
        );

        // Per Alloy spec: status stays Running - worker discovers on heartbeat
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Running,
            "status should stay Running until worker reports outcome"
        );
    });
}

/// Heartbeat returns cancellation info when job is cancelled

#[tokio::test]
async fn heartbeat_returns_cancelled_flag() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Dequeue
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // First heartbeat - not cancelled
        let response1 = shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat");
        assert!(
            !response1.cancelled,
            "should not be cancelled before cancel_job"
        );

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Second heartbeat - should report cancelled
        let response2 = shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat 2");
        assert!(response2.cancelled, "should be cancelled after cancel_job");
        assert!(
            response2.cancelled_at_ms.is_some(),
            "cancelled_at_ms should be set"
        );
    });
}

/// Worker reports Cancelled outcome after discovering cancellation

#[tokio::test]
async fn worker_reports_cancelled_outcome() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Dequeue
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Cancel the job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Worker reports Cancelled outcome
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Verify job status is Cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Verify attempt status is Cancelled
        let attempt = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt")
            .expect("exists");
        match attempt.state() {
            AttemptStatus::Cancelled { .. } => {}
            other => panic!("expected Cancelled, got {:?}", other),
        }

        // Verify lease is removed
        let lease = first_lease_kv(shard.db()).await;
        assert!(lease.is_none(), "lease should be removed");
    });
}

/// Lease expiry with cancelled job sets Cancelled status (not Failed)

#[tokio::test]
async fn reap_expired_lease_cancelled_job_sets_cancelled_status() {
    with_timeout!(20000, {
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
        let _leased_task_id = tasks[0].attempt().task_id().to_string();

        // Cancel the job while it's running
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Find the lease and rewrite expiry to the past (simulate worker crash)
        let (lease_key, lease_value) = first_lease_kv(shard.db()).await.expect("lease present");
        let decoded = decode_lease(lease_value).expect("decode lease");
        let expired_ms = now_ms() - 1;
        let new_record = LeaseRecord {
            worker_id: decoded.worker_id().to_string(),
            task: decoded.to_task().unwrap(),
            expiry_ms: expired_ms,
            started_at_ms: decoded.started_at_ms(),
        };
        let new_val = encode_lease(&new_record);
        shard
            .db()
            .put(&lease_key, &new_val)
            .await
            .expect("put mutated lease");
        shard.db().flush().await.expect("flush mutated lease");

        // Reap expired leases
        let reaped = shard.reap_expired_leases("-").await.expect("reap");
        assert_eq!(reaped, 1);

        // Verify job status is Cancelled (not Failed)
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status.kind,
            JobStatusKind::Cancelled,
            "expired lease on cancelled job should result in Cancelled status"
        );

        // Verify attempt status is Cancelled
        let attempt = shard
            .get_job_attempt("-", &job_id, 1)
            .await
            .expect("get attempt")
            .expect("exists");
        match attempt.state() {
            AttemptStatus::Cancelled { .. } => {}
            other => panic!("expected Cancelled, got {:?}", other),
        }
    });
}

/// Cannot cancel already cancelled job

#[tokio::test]
async fn cancel_already_cancelled_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Cancel once
        shard.cancel_job("-", &job_id).await.expect("first cancel");

        // Second cancel should fail
        let err = shard
            .cancel_job("-", &job_id)
            .await
            .expect_err("second cancel should fail");

        match err {
            JobStoreShardError::JobAlreadyCancelled(id) => assert_eq!(id, job_id),
            other => panic!("expected JobAlreadyCancelled, got {:?}", other),
        }
    });
}

/// Cannot cancel Succeeded job

#[tokio::test]
async fn cancel_succeeded_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Dequeue and complete successfully
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");

        // Verify Succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);

        // Try to cancel
        let err = shard
            .cancel_job("-", &job_id)
            .await
            .expect_err("cancel should fail");

        match err {
            JobStoreShardError::JobAlreadyTerminal(id, kind) => {
                assert_eq!(id, job_id);
                assert_eq!(kind, JobStatusKind::Succeeded);
            }
            other => panic!("expected JobAlreadyTerminal, got {:?}", other),
        }
    });
}

/// Cannot cancel Failed job

#[tokio::test]
async fn cancel_failed_job_returns_error() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        // No retry policy - will fail permanently
        let job_id = shard
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

        // Dequeue and fail
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        // Verify Failed
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Try to cancel
        let err = shard
            .cancel_job("-", &job_id)
            .await
            .expect_err("cancel should fail");

        match err {
            JobStoreShardError::JobAlreadyTerminal(id, kind) => {
                assert_eq!(id, job_id);
                assert_eq!(kind, JobStatusKind::Failed);
            }
            other => panic!("expected JobAlreadyTerminal, got {:?}", other),
        }
    });
}

/// Cancellation is monotonic - flag persists even when dequeue overwrites status
/// This tests the exampleCancellationPreservedOnDequeue scenario from Alloy:
/// Job scheduled, task in buffer, cancel arrives, worker dequeues stale task,
/// status becomes Running BUT cancellation flag is preserved

#[tokio::test]
async fn cancellation_preserved_through_stale_dequeue() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let priority = 10u8;
        let now = now_ms();

        let job_id = shard
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

        // Dequeue to make job Running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].job().id(), job_id);

        // Status is now Running (dequeue wrote this)
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Running);

        // Cancel while running
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Key property: Cancellation flag is set independently of status
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled")
        );

        // Status still shows Running (per Alloy spec: cancellation is orthogonal to status)
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Running,
            "status stays Running - cancellation is tracked separately"
        );

        // Worker discovers cancellation on heartbeat
        let task_id = tasks[0].attempt().task_id().to_string();
        let response = shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat");
        assert!(response.cancelled, "heartbeat should report cancellation");

        // Worker reports Cancelled outcome
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Now status is Cancelled (terminal)
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get final status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Cancelled);
    });
}

/// Cannot cancel non-existent job

#[tokio::test]
async fn cancel_nonexistent_job_returns_not_found() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let err = shard
            .cancel_job("-", "does-not-exist")
            .await
            .expect_err("cancel should fail");

        match err {
            JobStoreShardError::JobNotFound(id) => assert_eq!(id, "does-not-exist"),
            other => panic!("expected JobNotFound, got {:?}", other),
        }
    });
}

/// Delete cancelled job works

#[tokio::test]
async fn delete_cancelled_job_succeeds() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Cancel
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Delete (Cancelled is terminal, so delete should work)
        shard.delete_job("-", &job_id).await.expect("delete_job");

        // Job should be gone
        let job = shard.get_job("-", &job_id).await.expect("get_job");
        assert!(job.is_none(), "job should be deleted");

        // Cancellation record should also be gone
        assert!(
            !shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "cancellation record should be deleted"
        );
    });
}

/// Cancel job with concurrency limits eagerly removes request from queue.
/// When the slot opens, there's nothing to grant.

#[tokio::test]
async fn cancel_scheduled_job_with_concurrency_removes_request() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-cancel".to_string();

        // First job takes the slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue1");
        let tasks1 = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
        assert_eq!(tasks1.len(), 1);

        // Second job queues a request
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue2");

        // Cancel the second job before it gets a chance to run
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        // Verify j2 is cancelled
        let status = shard
            .get_job_status("-", &j2)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Complete first job - should NOT grant to cancelled j2
        let t1 = tasks1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report1");

        // No tasks should be ready (j2's request was cancelled)
        let tasks2 = shard.dequeue("w2", "default", 1).await.expect("deq2").tasks;
        assert!(
            tasks2.is_empty(),
            "cancelled job's request should not be granted"
        );
    });
}

/// Test that cancel with start_at_ms=0 correctly finds and deletes concurrency requests.
/// When start_at_ms <= 0, the status stores next_attempt_starts_after_ms = now_ms,
/// but the concurrency request key uses start_time_ms = 0 (via .max(0)).
/// The cancel path must fall back to scanning with start_time_ms=0 to find the request.
#[tokio::test]
async fn cancel_concurrency_request_with_zero_start_time() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let queue = "q-zero-start".to_string();

        // First job takes the slot (use start_at_ms=0)
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                0, // start_at_ms = 0
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j1");
        let tasks1 = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
        assert_eq!(tasks1.len(), 1);

        // Second job with start_at_ms=0 queues a request (slot is full)
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                0, // start_at_ms = 0
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j2");

        // Cancel j2 - must find and delete the concurrency request via fallback scan
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        let status = shard
            .get_job_status("-", &j2)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Complete j1 - should NOT grant to cancelled j2
        let t1 = tasks1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&t1, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report1");

        // No tasks should be ready (j2's request was deleted by cancel)
        let tasks2 = shard.dequeue("w2", "default", 1).await.expect("deq2").tasks;
        assert!(
            tasks2.is_empty(),
            "cancelled job's request should not be granted after start_time=0 fallback"
        );
    });
}

/// Test that cancel eagerly removes RunAttempt tasks from the queue.
/// Dequeue only sees the non-cancelled job.

#[tokio::test]
async fn cancel_eagerly_removes_run_attempt_tasks() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();

        // Enqueue two jobs
        let j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue j1");

        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + 1, // Slightly later so j1 comes first
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue j2");

        // Both tasks should be in the queue
        assert_eq!(count_task_keys(shard.db()).await, 2);

        // Cancel j1 - should eagerly remove its task
        shard.cancel_job("-", &j1).await.expect("cancel j1");

        // Only j2's task should remain
        assert_eq!(count_task_keys(shard.db()).await, 1);

        // Dequeue returns j2 only
        let tasks = shard
            .dequeue("w1", "default", 10)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1, "should return one task");
        assert_eq!(
            tasks[0].job().id(),
            j2,
            "should return j2, not cancelled j1"
        );
    });
}

/// Test that cancel eagerly removes RequestTicket tasks from the queue.
/// When the slot opens, j3 gets granted instead of cancelled j2.

#[tokio::test]
async fn cancel_eagerly_removes_request_ticket_tasks() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-skip-req".to_string();

        // j1 takes the concurrency slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j1");

        // Dequeue j1 to take the slot
        let t1 = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
        assert_eq!(t1.len(), 1);

        // j2 and j3 enqueue requests (future time so they become RequestTicket tasks)
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + 100,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j2");

        let j3 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now + 101,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j3");

        // Cancel j2 - should eagerly remove its RequestTicket task
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        // Complete j1 to release the slot
        let t1_id = t1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report j1 success");

        // Wait for the future time to pass
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        // Dequeue should return j3 (j2's task was deleted by cancel)
        let tasks = shard
            .dequeue("w2", "default", 10)
            .await
            .expect("dequeue")
            .tasks;

        // We should get j3, not j2
        if !tasks.is_empty() {
            assert_eq!(tasks[0].job().id(), j3, "should get j3, not cancelled j2");
        }
    });
}

/// Test that cancel eagerly removes requests, so grant-on-release finds valid requests directly

#[tokio::test]
async fn grant_on_release_skips_multiple_cancelled_requests() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-multi-cancel".to_string();

        // j1 takes the slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j1");

        let t1 = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
        assert_eq!(t1.len(), 1);

        // j2, j3, j4 queue requests (j2 and j3 will be cancelled)
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j2");

        let j3 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j3");

        let j4 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 4})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j4");

        // Cancel j2 and j3
        shard.cancel_job("-", &j2).await.expect("cancel j2");
        shard.cancel_job("-", &j3).await.expect("cancel j3");

        // Complete j1 - should skip j2 and j3, grant to j4
        let t1_id = t1[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&t1_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report j1 success");

        // Give broker time to pick up the granted task
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Dequeue should return j4
        let tasks = shard
            .dequeue("w2", "default", 10)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1, "should return one task");
        assert_eq!(tasks[0].job().id(), j4, "should return j4");

        // j2 and j3 should remain Cancelled, not be granted
        let j2_status = shard
            .get_job_status("-", &j2)
            .await
            .expect("get j2 status")
            .expect("j2 exists");
        assert_eq!(j2_status.kind, JobStatusKind::Cancelled);

        let j3_status = shard
            .get_job_status("-", &j3)
            .await
            .expect("get j3 status")
            .expect("j3 exists");
        assert_eq!(j3_status.kind, JobStatusKind::Cancelled);
    });
}

/// Test that dequeue works normally when no jobs are cancelled

#[tokio::test]
async fn dequeue_works_normally_without_cancellation() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();

        // Enqueue several jobs
        let mut job_ids = vec![];
        for i in 0..5 {
            let j = shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now + i,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"j": i})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
            job_ids.push(j);
        }

        // Dequeue all - should work normally
        let tasks = shard
            .dequeue("w1", "default", 10)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 5, "should return all 5 tasks");

        // Verify order matches enqueue order (by start time)
        for (i, task) in tasks.iter().enumerate() {
            assert_eq!(
                task.job().id(),
                job_ids[i],
                "task {} should match job {}",
                i,
                i
            );
        }
    });
}

/// Test that cancel eagerly cleans up concurrency requests from the request queue.
/// The request is deleted at cancel time, not lazily during grant-next.

#[tokio::test]
async fn cancel_eagerly_cleans_up_concurrency_requests() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();
        let queue = "q-cleanup".to_string();

        // j1 takes the slot
        let _j1 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j1");

        let t1 = shard.dequeue("w1", "default", 1).await.expect("deq1").tasks;
        assert_eq!(t1.len(), 1);

        // j2 queues a request
        let j2 = shard
            .enqueue(
                "-",
                None,
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
                vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue j2");

        // Verify request exists
        let requests_before = first_concurrency_request_kv(shard.db()).await;
        assert!(requests_before.is_some(), "request should exist");

        // Cancel j2 - should eagerly delete the request
        shard.cancel_job("-", &j2).await.expect("cancel j2");

        // Request should be gone immediately after cancel (eager cleanup)
        let requests_after_cancel = first_concurrency_request_kv(shard.db()).await;
        assert!(
            requests_after_cancel.is_none(),
            "request should be deleted immediately by cancel"
        );
    });
}
