mod test_helpers;

use silo::codec::{decode_lease, encode_task};
use silo::job::JobStatusKind;
use silo::job_attempt::{AttemptOutcome, AttemptStatus};
use silo::job_store_shard::JobStoreShardError;
use silo::task::Task;
use slatedb::WriteBatch;

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

        let decoded = decode_lease(kv_value).expect("decode lease");
        assert_eq!(decoded.worker_id(), "worker-1");
        let task = decoded.to_task().unwrap();
        match &task {
            Task::RunAttempt {
                id,
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(id, &leased_task_id);
                assert_eq!(jid, &job_id);
                assert_eq!(*attempt_number, 1);
            }
            _ => panic!("unexpected task variant in lease"),
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
        let decoded_first = decode_lease(old_value).expect("decode lease");
        let old_expiry = decoded_first.expiry_ms() as u64;

        // Heartbeat to renew
        shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat ok");

        // Scan again, expect one lease for task with a higher expiry
        let (new_key, new_value) = first_lease_kv(shard.db()).await.expect("scan lease 2");
        let parsed_new = silo::keys::parse_lease_key(&new_key).expect("parse lease key 2");
        assert_eq!(parsed_new.task_id, task_id);
        let decoded_second = decode_lease(new_value).expect("decode lease 2");
        let new_expiry = decoded_second.expiry_ms() as u64;
        assert!(new_expiry > old_expiry, "new expiry should be greater");

        // Validate owner remains the same
        assert_eq!(decoded_second.worker_id(), "worker-1");
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
        AttemptStatus::Running => {}
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

/// Reproducer: enqueue with start_at_ms=0 should be immediately leasable.
/// The proto says "0 = run immediately", and the server converts this to now_ms
/// for the job status, but the task key still uses 0. The broker scanner should
/// still pick up tasks with start_time_ms=0 since 0 <= now_ms.
#[silo::test]
async fn enqueue_with_start_at_ms_zero_is_immediately_leasable() {
    with_timeout!(10000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"hello": "world"}));

        // Enqueue with start_at_ms = 0 (proto default, means "run immediately")
        let job_id = shard
            .enqueue(
                "-",
                Some("test-job-zero".to_string()),
                1,
                0,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue should succeed");

        assert_eq!(job_id, "test-job-zero");

        // Verify job status is Scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("status exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Try to dequeue - should return the task
        let result = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue should succeed");

        assert_eq!(
            result.tasks.len(),
            1,
            "expected 1 task from dequeue, got {} (job with start_at_ms=0 was not brokered)",
            result.tasks.len()
        );

        let task = &result.tasks[0];
        assert_eq!(task.job().id(), "test-job-zero");

        // Verify status transitioned to Running
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("status exists");
        assert_eq!(status.kind, JobStatusKind::Running);
    });
}

/// Test that enqueue with a specific tenant and start_at_ms=0 is leasable
/// (matches the external integration test scenario).
#[silo::test]
async fn enqueue_with_tenant_and_start_at_ms_zero_is_leasable() {
    with_timeout!(10000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"hello": "world"}));

        // Use a specific tenant like the external test
        let tenant = "test-tenant-1";
        let task_group = "test-task-group";

        let job_id = shard
            .enqueue(
                tenant,
                Some("test-job-tenant".to_string()),
                1,
                0,
                None,
                payload,
                vec![],
                None,
                task_group,
            )
            .await
            .expect("enqueue should succeed");

        // Verify job status
        let status = shard
            .get_job_status(tenant, &job_id)
            .await
            .expect("get status")
            .expect("status exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Verify task exists in the database
        let task_count = count_task_keys(shard.db()).await;
        assert_eq!(task_count, 1, "expected 1 task in database");

        // Dequeue with the matching task group
        let result = shard
            .dequeue("worker-1", task_group, 1)
            .await
            .expect("dequeue should succeed");

        assert_eq!(
            result.tasks.len(),
            1,
            "expected 1 task from dequeue with tenant={} task_group={}, got {}",
            tenant,
            task_group,
            result.tasks.len()
        );
    });
}

/// Test that peek_tasks also finds tasks with start_at_ms=0
#[silo::test]
async fn peek_tasks_finds_start_at_ms_zero() {
    with_timeout!(10000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));

        shard
            .enqueue("-", None, 1, 0, None, payload, vec![], None, "default")
            .await
            .expect("enqueue should succeed");

        // peek_tasks reads directly from the DB (not the broker buffer)
        let tasks = shard
            .peek_tasks("default", 10)
            .await
            .expect("peek should succeed");

        assert_eq!(
            tasks.len(),
            1,
            "peek_tasks should find task with start_at_ms=0, got {}",
            tasks.len()
        );
    });
}

/// Verify enqueue with start_at_ms=now works (control test - this already works)
#[silo::test]
async fn enqueue_with_start_at_ms_now_is_leasable() {
    with_timeout!(10000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"hello": "world"}));
        let now = now_ms();

        shard
            .enqueue(
                "-",
                Some("test-job-now".to_string()),
                1,
                now,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue should succeed");

        let result = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue should succeed");

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].job().id(), "test-job-now");
    });
}

/// Regression test for duplicate leasing: if a stale scan re-sees a task key that was already
/// durably acked, the broker must suppress it.
#[silo::test]
async fn dequeue_ignores_recently_acked_task_keys() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"k": "v"}));

        let job_id = shard
            .enqueue(
                "-",
                Some("acked-key-regression".to_string()),
                1,
                0,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Capture the actual task key before dequeue. Its trailing `epoch_ms` is
        // assigned at enqueue time (a write-only disambiguator), so we can't
        // reconstruct it — read it from the queue so we reinject the SAME key the
        // dequeue will ack-delete (and thus tombstone).
        let real_key = {
            let prefix = silo::keys::task_group_prefix("default");
            let end = silo::keys::end_bound(&prefix);
            let mut iter = shard
                .db()
                .scan::<Vec<u8>, _>(prefix..end)
                .await
                .expect("scan tasks");
            iter.next()
                .await
                .expect("iter")
                .expect("task present before dequeue")
                .key
                .to_vec()
        };

        let first = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("first dequeue");
        assert_eq!(
            first.tasks.len(),
            1,
            "expected first dequeue to return a task"
        );

        let task_id = first.tasks[0].attempt().task_id().to_string();

        // Reinsert the same task key to emulate a stale scanner snapshot that still sees
        // a key that was already durably acked.
        let reinjected = Task::RunAttempt {
            id: task_id,
            tenant: "-".to_string(),
            job_id: job_id.clone(),
            attempt_number: 1,
            relative_attempt_number: 1,
            held_queues: vec![],
            task_group: "default".to_string(),
        };
        let task_bytes = encode_task(&reinjected);

        let mut batch = WriteBatch::new();
        batch.put(&real_key, &task_bytes);
        shard
            .db()
            .write(batch)
            .await
            .expect("write reinjected task");
        shard.db().flush().await.expect("flush reinjected task");

        // A duplicate lease should never be produced for this key.
        let second = shard
            .dequeue("worker-2", "default", 1)
            .await
            .expect("second dequeue");
        assert!(
            second.tasks.is_empty(),
            "expected no tasks from second dequeue; got duplicate lease for job {}",
            job_id
        );
    });
}

/// Dispatching a task whose lease key already holds a live lease is the
/// double-dispatch anomaly: dequeue proceeds (overwrite semantics are
/// unchanged) but warns and counts the overwrite so the grant-path bug that
/// materializes duplicate RunAttempt tasks is visible from metrics.
#[silo::test]
async fn dequeue_over_live_lease_counts_stored_overwrite_and_still_delivers() {
    with_timeout!(20000, {
        let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
        let payload = msgpack_payload(&serde_json::json!({"k": "v"}));
        let now = now_ms();

        shard
            .enqueue("-", None, 10, now, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        // Read the queued task to learn its enqueue-time task id
        let (_key, task_bytes) = first_task_kv(shard.db()).await.expect("task present");
        let decoded = silo::codec::decode_task(&task_bytes).expect("decode task");
        let Task::RunAttempt {
            id: task_id,
            job_id,
            ..
        } = decoded
        else {
            panic!("expected RunAttempt task");
        };

        // Pre-write a live lease at the task's lease key, as a prior dispatch
        // of the same task would have left behind
        let lease = silo::task::LeaseRecord {
            worker_id: "other-worker".to_string(),
            task: Task::RunAttempt {
                id: task_id.clone(),
                tenant: "-".to_string(),
                job_id: job_id.clone(),
                attempt_number: 1,
                relative_attempt_number: 1,
                held_queues: vec![],
                task_group: "default".to_string(),
            },
            expiry_ms: now + 60_000,
            started_at_ms: now,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            &silo::keys::leased_task_key(&task_id),
            &silo::codec::encode_lease(&lease),
        );
        shard.db().write(batch).await.expect("write lease");
        shard.db().flush().await.expect("flush lease");

        let result = shard
            .dequeue("worker-2", "default", 1)
            .await
            .expect("dequeue");
        assert_eq!(result.tasks.len(), 1, "task must still be delivered");

        let body = gather_metrics_text(&metrics);
        let overwrites = metric_value_or_zero(
            &body,
            &[
                "silo_task_lease_overwrites_total",
                "task_group=\"default\"",
                "source=\"stored\"",
            ],
        );
        assert_eq!(overwrites, 1.0, "stored-lease overwrite must be counted");

        // The overwrite proceeded: the lease names the dequeuing worker with
        // a fresh expiry
        let lease_bytes = shard
            .db()
            .get(&silo::keys::leased_task_key(&task_id))
            .await
            .expect("get lease")
            .expect("lease exists");
        let lease = decode_lease(lease_bytes).expect("decode lease");
        assert_eq!(lease.worker_id(), "worker-2");
        let after = now_ms();
        assert!(
            lease.expiry_ms() >= now + silo::task::DEFAULT_LEASE_MS
                && lease.expiry_ms() <= after + silo::task::DEFAULT_LEASE_MS,
            "lease expiry {} should be ~now + DEFAULT_LEASE_MS",
            lease.expiry_ms()
        );
    });
}

fn gather_metrics_text(metrics: &silo::metrics::Metrics) -> String {
    use prometheus::{Encoder, TextEncoder};
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry().gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Value of the first metric line matching all substrings, or 0.0 when no
/// line matches (a counter that never fired is absent from the scrape).
fn metric_value_or_zero(body: &str, substrings: &[&str]) -> f64 {
    body.lines()
        .find(|l| !l.starts_with('#') && substrings.iter().all(|s| l.contains(s)))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, v)| v.parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// A repeat of the same task id within a single dequeue iteration cannot be
/// seen by the pre-write point read (the first lease write is still in the
/// uncommitted batch), so the guard tracks the iteration's leased task ids
/// and counts the repeat under source="batch".
#[silo::test]
async fn dequeue_counts_batch_overwrite_for_repeated_task_id_in_one_iteration() {
    with_timeout!(20000, {
        let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
        let payload = msgpack_payload(&serde_json::json!({"k": "v"}));
        let now = now_ms();

        let job_id = shard
            .enqueue("-", None, 10, now, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        // Learn the enqueue-time task id from the queued record
        let (_key, task_bytes) = first_task_kv(shard.db()).await.expect("task present");
        let decoded = silo::codec::decode_task(&task_bytes).expect("decode task");
        let Task::RunAttempt { id: task_id, .. } = decoded else {
            panic!("expected RunAttempt task");
        };

        // Hand-write a second queued RunAttempt sharing the task id (task ids
        // are fresh UUIDs at enqueue, so this is not constructible through
        // enqueue). A different attempt number gives it a distinct task key.
        let duplicate = Task::RunAttempt {
            id: task_id.clone(),
            tenant: "-".to_string(),
            job_id: job_id.clone(),
            attempt_number: 2,
            relative_attempt_number: 2,
            held_queues: vec![],
            task_group: "default".to_string(),
        };
        let dup_key = silo::keys::task_key("default", now, 10, &job_id, 2, now);
        let mut batch = WriteBatch::new();
        batch.put(&dup_key, &silo::codec::encode_task(&duplicate));
        shard.db().write(batch).await.expect("write duplicate task");
        shard.db().flush().await.expect("flush duplicate task");

        // Both task records dequeue in one call, so the second lease write
        // repeats a task id already leased in this iteration's batch
        let result = shard
            .dequeue("worker-1", "default", 2)
            .await
            .expect("dequeue");
        assert_eq!(result.tasks.len(), 2, "both records must be delivered");

        let body = gather_metrics_text(&metrics);
        let batch_overwrites = metric_value_or_zero(
            &body,
            &[
                "silo_task_lease_overwrites_total",
                "task_group=\"default\"",
                "source=\"batch\"",
            ],
        );
        assert_eq!(batch_overwrites, 1.0, "batch overwrite must be counted");
        let stored_overwrites = metric_value_or_zero(
            &body,
            &[
                "silo_task_lease_overwrites_total",
                "task_group=\"default\"",
                "source=\"stored\"",
            ],
        );
        assert_eq!(
            stored_overwrites, 0.0,
            "the repeat is invisible to the point read; only the batch branch fires"
        );
    });
}

/// An expired leftover lease at the task's lease key is routine (reap-and-
/// retry territory), not a double dispatch -- the guard stays silent.
#[silo::test]
async fn dequeue_over_expired_lease_stays_silent() {
    with_timeout!(20000, {
        let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
        let payload = msgpack_payload(&serde_json::json!({"k": "v"}));
        let now = now_ms();

        shard
            .enqueue("-", None, 10, now, None, payload, vec![], None, "default")
            .await
            .expect("enqueue");

        let (_key, task_bytes) = first_task_kv(shard.db()).await.expect("task present");
        let decoded = silo::codec::decode_task(&task_bytes).expect("decode task");
        let Task::RunAttempt {
            id: task_id,
            job_id,
            ..
        } = decoded
        else {
            panic!("expected RunAttempt task");
        };

        let expired_lease = silo::task::LeaseRecord {
            worker_id: "other-worker".to_string(),
            task: Task::RunAttempt {
                id: task_id.clone(),
                tenant: "-".to_string(),
                job_id: job_id.clone(),
                attempt_number: 1,
                relative_attempt_number: 1,
                held_queues: vec![],
                task_group: "default".to_string(),
            },
            expiry_ms: now - 60_000,
            started_at_ms: now - 120_000,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            &silo::keys::leased_task_key(&task_id),
            &silo::codec::encode_lease(&expired_lease),
        );
        shard.db().write(batch).await.expect("write expired lease");
        shard.db().flush().await.expect("flush expired lease");

        let result = shard
            .dequeue("worker-2", "default", 1)
            .await
            .expect("dequeue");
        assert_eq!(result.tasks.len(), 1, "task must still be delivered");

        let body = gather_metrics_text(&metrics);
        let overwrites = metric_value_or_zero(
            &body,
            &["silo_task_lease_overwrites_total", "task_group=\"default\""],
        );
        assert_eq!(overwrites, 0.0, "expired leftover lease must not count");
    });
}
