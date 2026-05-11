mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShardError;
use silo::retry::RetryPolicy;
use silo::task::Task;

use test_helpers::*;

#[silo::test]
async fn restart_cancelled_scheduled_job() {
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

        // Verify counters after enqueue
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 0);

        // Cancel the scheduled job
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Verify counters after cancel - completed_jobs should be 1 (terminal state)
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(counters.completed_jobs, 1);

        // Verify job is cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        assert!(
            shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled")
        );

        // [SILO-RESTART-*] Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Verify counters after restart - completed_jobs should be 0 (back to scheduled)
        let counters = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 1);
        assert_eq!(
            counters.completed_jobs, 0,
            "completed_jobs should be 0 after restart"
        );

        // [SILO-RESTART-6] Verify status is now Scheduled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Scheduled,
            "job should be Scheduled after restart"
        );

        // [SILO-RESTART-4] Verify cancellation flag is cleared
        assert!(
            !shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "cancellation flag should be cleared after restart"
        );

        // [SILO-RESTART-5] Verify a new task exists in queue
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        assert!(!tasks.is_empty(), "new task should exist after restart");
        let new_task = &tasks[0];
        match new_task {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 1,
                    "restart should reset attempt number to 1"
                );
            }
            _ => panic!("expected RunAttempt task, got {:?}", new_task),
        }
    });
}

/// [SILO-RESTART-1][SILO-RESTART-5][SILO-RESTART-6]
/// Restart a failed job (no retry policy) - creates new task with fresh retries

#[silo::test]
async fn restart_failed_job() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        // No retry policy - job will fail permanently on first error
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

        // Dequeue and fail the job
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST_ERROR".to_string(),
                    error: b"test failure".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Verify job is Failed
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // [SILO-RESTART-*] Restart the failed job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // [SILO-RESTART-6] Verify status is now Scheduled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(
            status_after.kind,
            JobStatusKind::Scheduled,
            "failed job should be Scheduled after restart"
        );

        // [SILO-RESTART-5] Verify a new task exists with monotonically increasing attempt number
        // After 1 failed attempt, restart creates attempt 2 (not reset to 1)
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        assert!(!tasks.is_empty(), "new task should exist after restart");
        match &tasks[0] {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 2,
                    "restart should continue attempt numbers monotonically (was 1, now 2)"
                );
            }
            _ => panic!("expected RunAttempt task"),
        }

        // Dequeue and complete successfully this time
        let tasks2 = shard
            .dequeue("worker-2", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks2.len(), 1);
        let task_id2 = tasks2[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id2,
                AttemptOutcome::Success {
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");

        // Verify job succeeded
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// [SILO-RESTART-2] Cannot restart a succeeded job

#[silo::test]
async fn restart_succeeded_job_returns_error() {
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

        // Complete the job successfully
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
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");

        // Verify job is Succeeded
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);

        // Try to restart - should fail
        let err = shard
            .restart_job("-", &job_id)
            .await
            .expect_err("restart should fail for succeeded job");

        match err {
            JobStoreShardError::JobNotRestartable(e) => {
                assert_eq!(e.job_id, job_id);
                assert_eq!(e.status, JobStatusKind::Succeeded);
                assert!(e.reason.contains("succeeded"));
            }
            other => panic!("expected JobNotRestartable, got {:?}", other),
        }
    });
}

/// [SILO-RESTART-3] Cannot restart a running job

#[silo::test]
async fn restart_running_job_returns_error() {
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

        // Try to restart - should fail
        let err = shard
            .restart_job("-", &job_id)
            .await
            .expect_err("restart should fail for running job");

        match err {
            JobStoreShardError::JobNotRestartable(e) => {
                assert_eq!(e.job_id, job_id);
                assert_eq!(e.status, JobStatusKind::Running);
                assert!(e.reason.contains("in progress"));
            }
            other => panic!("expected JobNotRestartable, got {:?}", other),
        }

        // Clean up - complete the job
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");
    });
}

/// [SILO-RESTART-3] Cannot restart a scheduled job (not cancelled or failed)

#[silo::test]
async fn restart_scheduled_job_returns_error() {
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

        // Verify job is Scheduled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        // Try to restart - should fail
        let err = shard
            .restart_job("-", &job_id)
            .await
            .expect_err("restart should fail for scheduled job");

        match err {
            JobStoreShardError::JobNotRestartable(e) => {
                assert_eq!(e.job_id, job_id);
                assert_eq!(e.status, JobStatusKind::Scheduled);
                assert!(e.reason.contains("in progress"));
            }
            other => panic!("expected JobNotRestartable, got {:?}", other),
        }
    });
}

/// Cannot restart a non-existent job

#[silo::test]
async fn restart_nonexistent_job_returns_not_found() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let err = shard
            .restart_job("-", "does-not-exist")
            .await
            .expect_err("restart should fail");

        match err {
            JobStoreShardError::JobNotFound(id) => assert_eq!(id, "does-not-exist"),
            other => panic!("expected JobNotFound, got {:?}", other),
        }
    });
}

/// When `terminal_job_expire_ms` is set, the records written by the terminal
/// outcome path carry a SlateDB row TTL with `expire_ts ≈ now + ttl_ms`.
/// SlateDB applies the TTL at compaction time, so this test verifies the
/// per-row metadata directly rather than waiting for compaction to drop the
/// row.
///
/// This is the safety hook for the spec's `expireTerminalJob` transition:
/// once compaction passes the TTL, the rows disappear from the shard's view
/// and `restartFailedJob`'s `j in jobExistsAt[t]` precondition stops holding.
#[silo::test]
async fn terminal_records_are_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_info_key, job_status_key};
        let ttl_ms: u64 = 7 * 24 * 60 * 60 * 1000; // 7 days
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(ttl_ms).await;

        let before_ms = now_ms();
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

        // Drive the job to Failed (no retry policy → first error is permanent).
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
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report failure");
        let after_ms = now_ms();

        // JOB_STATUS and JOB_INFO must both carry an `expire_ts` set to roughly
        // `now + ttl_ms`. Allow a generous window around the wall-clock bounds
        // (clocks can skew under load and the implementation samples `now_ms`
        // once per termination call).
        let raw_db = shard.db();
        for key in [job_status_key("-", &job_id), job_info_key("-", &job_id)] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            let expire_ts = kv
                .expire_ts
                .expect("terminal record should carry expire_ts");
            let lower = before_ms + ttl_ms as i64 - 5_000;
            let upper = after_ms + ttl_ms as i64 + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} outside expected window [{lower}, {upper}] for key {key:?}"
            );
        }
    });
}

/// Regression: when terminal_job_expire_ms is set, the ATTEMPT row for the
/// job's current attempt must reflect the **terminal** outcome status (e.g.
/// Succeeded), not the prior Running status that was on disk before
/// `report_attempt_outcome` ran. An earlier implementation re-scanned
/// ATTEMPT rows after writing the new terminal attempt and clobbered the
/// new value with the old Running value because WriteBatch is last-write-wins
/// per key. This test reads the ATTEMPT row directly and asserts both the
/// terminal status and the TTL are present.
#[silo::test]
async fn terminal_attempt_row_keeps_terminal_status_under_ttl() {
    with_timeout!(20000, {
        use silo::job_attempt::{AttemptStatus, JobAttemptView};
        use silo::keys::attempt_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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

        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
            .await
            .expect("report success");

        // The committed attempt row for attempt 1 must be Succeeded with a TTL.
        let kv = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row present");
        assert!(
            kv.expire_ts.is_some(),
            "terminal attempt row should carry expire_ts"
        );
        let view = JobAttemptView::new(kv.value).expect("decode attempt");
        match view.state() {
            AttemptStatus::Succeeded { .. } => {}
            other => panic!("expected AttemptStatus::Succeeded, got {:?}", other),
        }
    });
}

/// IDX_METADATA rows are the reason `expire_terminal_job_records` reads
/// JOB_INFO back: we extract the metadata pairs from JOB_INFO so we know
/// which IDX_METADATA keys to TTL. This test pins down that every
/// IDX_METADATA row for a terminal job carries the row TTL.
#[silo::test]
async fn terminal_idx_metadata_rows_are_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::idx_metadata_key;

        let ttl_ms: u64 = 60_000;
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(ttl_ms).await;

        let metadata = vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
            ("kind".to_string(), "ingest".to_string()),
        ];
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
                Some(metadata.clone()),
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
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
            .await
            .expect("report success");

        for (k, v) in &metadata {
            let kv = shard
                .db()
                .get_key_value(&idx_metadata_key("-", k, v, &job_id))
                .await
                .expect("get_key_value")
                .unwrap_or_else(|| panic!("IDX_METADATA row missing for {k}={v}"));
            assert!(
                kv.expire_ts.is_some(),
                "IDX_METADATA row for {k}={v} must carry expire_ts"
            );
        }
    });
}

/// IDX_STATUS_TIME row written for the new terminal status must carry the
/// row TTL. This is written inside `write_job_status_with_index_opts` rather
/// than the helper, so it's a separate code path from the JOB_INFO/ATTEMPT
/// re-puts and worth its own assertion.
#[silo::test]
async fn terminal_idx_status_time_row_is_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::job::JobStatusKind;
        use silo::keys::{idx_status_time_key, status_index_timestamp};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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

        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
            .await
            .expect("report success");

        // Re-derive the IDX_STATUS_TIME key for the new (Succeeded) status from
        // the recorded JobStatus so this test stays in lockstep with however
        // `set_job_status_with_index_opts` constructs the key.
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get_job_status")
            .expect("status present");
        assert_eq!(status.kind, JobStatusKind::Succeeded);
        let ts = status_index_timestamp(&status);
        let idx_key = idx_status_time_key("-", status.kind.as_str(), ts, &job_id);
        let kv = shard
            .db()
            .get_key_value(&idx_key)
            .await
            .expect("get_key_value")
            .expect("IDX_STATUS_TIME row present");
        assert!(
            kv.expire_ts.is_some(),
            "IDX_STATUS_TIME row for terminal status must carry expire_ts"
        );
    });
}

/// JOB_CANCELLED rows are written on the cancellation path and must also
/// be TTL'd when the cancellation results in a terminal outcome. Exercises
/// the Cancelled-acknowledgement branch which neither of the other new
/// tests touches.
#[silo::test]
async fn cancelled_terminal_job_cancellation_row_is_tagged_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::job_cancelled_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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

        // Dequeue so the job is Running; only then does worker-acknowledged
        // cancellation flow through report_attempt_outcome(Cancelled).
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();

        // Cancel from the API side first (writes JOB_CANCELLED), then have the
        // worker acknowledge with AttemptOutcome::Cancelled.
        shard.cancel_job("-", &job_id).await.expect("cancel_job");
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        let kv = shard
            .db()
            .get_key_value(&job_cancelled_key("-", &job_id))
            .await
            .expect("get_key_value")
            .expect("JOB_CANCELLED row present");
        assert!(
            kv.expire_ts.is_some(),
            "JOB_CANCELLED row for terminal Cancelled job must carry expire_ts"
        );
    });
}

/// When `report_attempt_outcome(Error)` schedules a retry (job goes back to
/// Scheduled, not terminal), the just-finalized attempt row must NOT carry
/// a TTL. Otherwise retried jobs that never reach terminal status would
/// silently lose their attempt history once the next compaction runs.
/// This is the inverse of `terminal_attempt_row_keeps_terminal_status_under_ttl`
/// and pins down the doc-comment claim in lease.rs.
#[silo::test]
async fn retry_branch_attempt_row_has_no_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::attempt_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

        // Retry policy so the first error leads to retry-scheduling, not Failed.
        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        };

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                Some(retry_policy),
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
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report error");

        // Job should be back to Scheduled (retry scheduled), not terminal.
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("status")
            .expect("present");
        assert_eq!(status.kind, JobStatusKind::Scheduled);

        let kv = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row present");
        assert!(
            kv.expire_ts.is_none(),
            "attempt row written under the retry-scheduling branch must NOT carry expire_ts \
             (job is still alive and could be retried for hours); got {:?}",
            kv.expire_ts
        );
    });
}

/// End-to-end counter drift: enable a 50ms TTL with a fast slatedb
/// compactor, complete a job, wait long enough for the row to be dropped,
/// and assert (a) JOB_STATUS reads as None (compaction dropped it), and
/// (b) the tenant-status counter is "stale-high" relative to the live
/// scan. Running the counter reconciler then re-derives the truth and
/// brings the counter back in line. This exercises the exact production
/// loop the example config recommends pairing with
/// `terminal_job_expire_ms`.
#[silo::test]
async fn ttl_dropped_row_drifts_counter_and_reconciler_fixes_it() {
    use silo::settings::{Backend, DatabaseConfig};
    use silo::shard_range::ShardRange;
    use std::time::Duration;

    with_timeout!(60000, {
        // Aggressive slatedb compaction so terminal rows actually get dropped
        // within the test budget.
        let tmp = tempfile::tempdir().unwrap();
        let cfg = DatabaseConfig {
            name: "drift-test".to_string(),
            backend: Backend::Fs,
            path: tmp.path().to_string_lossy().to_string(),
            slatedb: Some(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(10)),
                l0_sst_size_bytes: 256,
                l0_max_ssts: 1,
                compactor_options: Some(slatedb::config::CompactorOptions {
                    poll_interval: Duration::from_millis(50),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            terminal_job_expire_ms: Some(50),
            ..Default::default()
        };
        let rate_limiter = silo::gubernator::NullGubernatorClient::new();
        let shard = silo::job_store_shard::JobStoreShard::open(
            &cfg,
            rate_limiter,
            None,
            ShardRange::full(),
        )
        .await
        .expect("open shard");

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
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
            .await
            .expect("report success");

        // Force the rows from memtable to L0 so compaction can see them.
        // With l0_max_ssts=1 the compactor needs >1 L0 SST to fire, so churn
        // a few more flushes by enqueuing+cancelling unrelated jobs.
        shard.db().flush().await.expect("flush");
        tokio::time::sleep(Duration::from_millis(200)).await;
        for i in 0..12 {
            let payload = test_helpers::msgpack_payload(&serde_json::json!({"churn": i}));
            let jid = shard
                .enqueue(
                    "other",
                    None,
                    50u8,
                    now_ms(),
                    None,
                    payload,
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue churn");
            // Cancel so it leaves terminal (Cancelled) rows.
            let _ = shard.cancel_job("other", &jid).await;
            shard.db().flush().await.expect("flush");
            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        // Poll for up to ~10s for the row to actually drop. Compaction is
        // non-deterministic, but with the aggressive churn above it should
        // fire well within the budget. Reading early lets the test finish
        // fast on healthy machines while still tolerating slow CI.
        let mut live_status = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            live_status = shard
                .get_job_status("-", &job_id)
                .await
                .expect("get_job_status");
            if live_status.is_none() {
                break;
            }
        }

        // Read the tenant-status (Succeeded) counter pre-reconcile.
        let pre = shard.scan_tenant_status_counters(None).await.expect("scan");
        let pre_succeeded = pre
            .iter()
            .find(|(t, kind, _)| t == "-" && kind == "Succeeded")
            .map(|(_, _, c)| *c)
            .unwrap_or(0);
        eprintln!(
            "post-TTL: live_status_present={} pre_succeeded_counter={}",
            live_status.is_some(),
            pre_succeeded
        );

        // Drift-and-reconcile is gated on compaction actually dropping the
        // row within the test budget. We can't deterministically force
        // slatedb compaction from a test, so this is soft-skipped (with a
        // loud warning) when the row is still present — the structural
        // tests above cover the wiring; this test is the only end-to-end
        // smoke for the reconciler path under TTL.
        //
        // Don't convert the skip to a hard assertion: prior attempts to do
        // so flaked because compaction is async and L0→L1 promotion
        // depends on slatedb internals the test can't drive. If you want
        // hard coverage of the reconciler, add a unit test that injects a
        // stale counter directly rather than waiting on compaction.
        let drifted = live_status.is_none() && pre_succeeded == 1;
        if drifted {
            let summary = shard.reconcile_counters(&ShardRange::full()).await;
            assert!(
                summary.failed == 0,
                "reconciler should not report failures: {:?}",
                summary
            );
            let post = shard.scan_tenant_status_counters(None).await.expect("scan");
            let post_succeeded = post
                .iter()
                .find(|(t, kind, _)| t == "-" && kind == "Succeeded")
                .map(|(_, _, c)| *c)
                .unwrap_or(0);
            assert_eq!(
                post_succeeded, 0,
                "reconciler should bring Succeeded counter back to live row count (0) \
                 after compaction drops the terminal job; got {post_succeeded}"
            );
        } else {
            eprintln!(
                "\n  !! SKIPPED reconciler drift assertion: compaction did not drop the\n  \
                 !! terminal JOB_STATUS row within the test budget\n  \
                 !! (live_status_present={}, pre_succeeded_counter={}).\n  \
                 !! The structural TTL tests still cover record tagging; this test\n  \
                 !! exists only as an end-to-end smoke and is a no-op when slatedb\n  \
                 !! compaction is too slow. Investigate slatedb settings if this\n  \
                 !! happens routinely.\n",
                live_status.is_some(),
                pre_succeeded
            );
        }
    });
}

/// Even with TTL configured, a Failed job is restartable **before** the TTL
/// elapses. Guards against accidentally writing the TTL with `expire_ts` in
/// the past, or applying the row TTL on the wrong write path.
#[silo::test]
async fn ttl_unexpired_failed_job_is_still_restartable() {
    with_timeout!(30000, {
        // Long enough that the records can't expire during the test.
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report failure");

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Restart well before the TTL — should succeed.
        shard
            .restart_job("-", &job_id)
            .await
            .expect("restart of not-yet-expired job should succeed");

        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Scheduled);
    });
}

/// Restart a job with retry policy that failed after exhausting retries

#[silo::test]
async fn restart_failed_job_with_retry_policy_resets_retries() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let retry_policy = RetryPolicy {
            retry_count: 2,
            initial_interval_ms: 1,
            max_interval_ms: 10,
            randomize_interval: false,
            backoff_factor: 1.0,
        };

        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                Some(retry_policy),
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Fail the job 3 times (1 initial + 2 retries = 3 attempts)
        for attempt in 1..=3 {
            let tasks = shard
                .dequeue("worker-1", "default", 1)
                .await
                .expect("dequeue")
                .tasks;
            assert_eq!(tasks.len(), 1, "should have task for attempt {}", attempt);
            let task_id = tasks[0].attempt().task_id().to_string();
            shard
                .report_attempt_outcome(
                    &task_id,
                    AttemptOutcome::Error {
                        error_code: "TEST_ERROR".to_string(),
                        error: format!("attempt {} failed", attempt).into_bytes(),
                    },
                )
                .await
                .expect("report error");

            // Wait a bit for scheduled retry task to become available (if not last attempt)
            if attempt < 3 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }

        // Verify job is Failed after exhausting retries
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Failed);

        // Verify we had 3 attempts
        let attempts = shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("get attempts");
        assert_eq!(attempts.len(), 3, "should have 3 failed attempts");

        // Restart the job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Verify status is Scheduled
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Scheduled);

        // Verify new task has monotonically increasing attempt number
        // After 3 failed attempts, restart creates attempt 4 (not reset to 1)
        // The retry SCHEDULE resets, but attempt numbers continue monotonically
        let tasks = shard.peek_tasks("default", 10).await.expect("peek");
        match &tasks[0] {
            Task::RunAttempt {
                job_id: jid,
                attempt_number,
                ..
            } => {
                assert_eq!(jid, &job_id);
                assert_eq!(
                    *attempt_number, 4,
                    "restart should continue attempt numbers monotonically (was 3, now 4)"
                );
            }
            _ => panic!("expected RunAttempt task"),
        }

        // Successfully complete the restarted job
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
            .expect("report success");

        // Verify job succeeded
        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// Restart a cancelled job that was running - after worker acknowledges cancellation

#[silo::test]
async fn restart_cancelled_running_job_after_acknowledgement() {
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

        // Dequeue to make job Running
        let tasks = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        let task_id = tasks[0].attempt().task_id().to_string();

        // Cancel while running
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        // Worker discovers cancellation via heartbeat
        let hb_result = shard
            .heartbeat_task("worker-1", &task_id)
            .await
            .expect("heartbeat");
        assert!(
            hb_result.cancelled,
            "heartbeat should indicate cancellation"
        );

        // Worker acknowledges cancellation
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
            .await
            .expect("report cancelled");

        // Verify job is Cancelled
        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);

        // Restart the cancelled job
        shard.restart_job("-", &job_id).await.expect("restart_job");

        // Verify status is Scheduled and cancellation cleared
        let status_after = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status_after.kind, JobStatusKind::Scheduled);
        assert!(
            !shard
                .is_job_cancelled("-", &job_id)
                .await
                .expect("is_cancelled"),
            "cancellation should be cleared"
        );

        // Complete the restarted job
        let tasks = shard
            .dequeue("worker-2", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Success {
                    result: b"{}".to_vec(),
                },
            )
            .await
            .expect("report success");

        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// Multiple restarts of the same job

#[silo::test]
async fn multiple_restarts_of_same_job() {
    with_timeout!(30000, {
        let (_tmp, shard) = open_temp_shard().await;

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"iteration": 0}));
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

        for iteration in 1..=3 {
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
                        error_code: "TEMP_ERROR".to_string(),
                        error: vec![],
                    },
                )
                .await
                .expect("report error");

            // Verify failed
            let status = shard
                .get_job_status("-", &job_id)
                .await
                .expect("status")
                .expect("exists");
            assert_eq!(status.kind, JobStatusKind::Failed);

            // Restart
            shard.restart_job("-", &job_id).await.expect("restart_job");

            // Verify scheduled
            let status_after = shard
                .get_job_status("-", &job_id)
                .await
                .expect("status")
                .expect("exists");
            assert_eq!(
                status_after.kind,
                JobStatusKind::Scheduled,
                "iteration {} should be Scheduled after restart",
                iteration
            );
        }

        // Finally succeed
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
                    result: b"done".to_vec(),
                },
            )
            .await
            .expect("report success");

        let final_status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("status")
            .expect("exists");
        assert_eq!(final_status.kind, JobStatusKind::Succeeded);
    });
}

/// When `cancel_job` transitions a Scheduled job straight to terminal
/// (Cancelled), the records written inside the cancel transaction must carry
/// a SlateDB row TTL — the same invariant `terminal_records_are_tagged_with_expire_ts`
/// asserts for `report_attempt_outcome`. Without TTL on this path the
/// cancellation-driven terminal records would stick around indefinitely while
/// success/failure-driven ones would not.
#[silo::test]
async fn cancel_scheduled_job_tags_records_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_cancelled_key, job_info_key, job_status_key};

        let ttl_ms: u64 = 7 * 24 * 60 * 60 * 1000; // 7 days
        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(ttl_ms).await;

        let before_ms = now_ms();
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

        shard.cancel_job("-", &job_id).await.expect("cancel_job");
        let after_ms = now_ms();

        // JOB_STATUS, JOB_INFO, and JOB_CANCELLED must all carry an `expire_ts`
        // set to roughly `now + ttl_ms`. Allow a generous window around the
        // wall-clock bounds (the implementation samples `now_ms` once per
        // cancellation call).
        let raw_db = shard.db();
        for key in [
            job_status_key("-", &job_id),
            job_info_key("-", &job_id),
            job_cancelled_key("-", &job_id),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            let expire_ts = kv
                .expire_ts
                .expect("cancellation-terminal record should carry expire_ts");
            let lower = before_ms + ttl_ms as i64 - 5_000;
            let upper = after_ms + ttl_ms as i64 + 5_000;
            assert!(
                expire_ts >= lower && expire_ts <= upper,
                "expire_ts {expire_ts} outside expected window [{lower}, {upper}] for key {key:?}"
            );
        }
    });
}

/// The new IDX_STATUS_TIME row written for the Cancelled status by
/// `set_job_status_with_index_opts` must carry the row TTL when the cancel
/// path transitions a Scheduled job to terminal.
#[silo::test]
async fn cancel_scheduled_job_tags_idx_status_time_with_expire_ts() {
    with_timeout!(20000, {
        use silo::job::JobStatusKind;
        use silo::keys::{idx_status_time_key, status_index_timestamp};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        let ts = status_index_timestamp(&status);
        let kv = shard
            .db()
            .get_key_value(&idx_status_time_key(
                "-",
                JobStatusKind::Cancelled.as_str(),
                ts,
                &job_id,
            ))
            .await
            .expect("get_key_value")
            .expect("IDX_STATUS_TIME row present");
        assert!(
            kv.expire_ts.is_some(),
            "IDX_STATUS_TIME row for terminal Cancelled status must carry expire_ts"
        );
    });
}

/// IDX_METADATA rows must also be re-put with the row TTL on the cancel
/// path. Mirrors `terminal_idx_metadata_rows_are_tagged_with_expire_ts` for
/// the cancel-driven terminal transition.
#[silo::test]
async fn cancel_scheduled_job_tags_idx_metadata_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::idx_metadata_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

        let metadata = vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ];
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
                Some(metadata.clone()),
                "default",
            )
            .await
            .expect("enqueue");

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        for (k, v) in &metadata {
            let kv = shard
                .db()
                .get_key_value(&idx_metadata_key("-", k, v, &job_id))
                .await
                .expect("get_key_value")
                .unwrap_or_else(|| panic!("IDX_METADATA row missing for {k}={v}"));
            assert!(
                kv.expire_ts.is_some(),
                "IDX_METADATA row for {k}={v} must carry expire_ts after cancel"
            );
        }
    });
}

/// A prior ATTEMPT row written by a retry-scheduling outcome was left
/// without a TTL on purpose (job wasn't yet terminal). When the job is
/// later cancelled via the cancel RPC, the cancel path must re-put that
/// attempt row with the row TTL — that's what
/// `expire_terminal_job_records` does, and we want to pin it down for the
/// cancel call site.
#[silo::test]
async fn cancel_scheduled_job_tags_prior_attempt_rows_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::attempt_key;

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

        let retry_policy = RetryPolicy {
            retry_count: 3,
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            randomize_interval: false,
            backoff_factor: 2.0,
        };

        let payload = test_helpers::msgpack_payload(&serde_json::json!({"k": "v"}));
        let job_id = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                Some(retry_policy),
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Run-and-fail attempt 1 → retry-scheduling branch (job back to
        // Scheduled, attempt 1 row has NO TTL).
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
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report failure");

        // Sanity: attempt 1 row currently has no TTL.
        let attempt_kv = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row present");
        assert!(
            attempt_kv.expire_ts.is_none(),
            "pre-cancel attempt row should not yet carry expire_ts"
        );

        // Cancel — the job is back to Scheduled, so cancel transitions it
        // straight to terminal Cancelled and the prior attempt row must
        // pick up the TTL.
        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let attempt_kv_after = shard
            .db()
            .get_key_value(&attempt_key("-", &job_id, 1))
            .await
            .expect("get_key_value")
            .expect("attempt row still present");
        assert!(
            attempt_kv_after.expire_ts.is_some(),
            "prior attempt row must carry expire_ts after terminal cancel"
        );
    });
}

/// Running-job cancellation does NOT immediately tag records with the row
/// TTL. The job is still Running until the worker acknowledges via
/// `report_attempt_outcome(Cancelled)` — only then is it terminal. Tagging
/// records on the cancel call would mean records expire before the worker
/// even hears about the cancellation. The worker-ack path
/// (`cancelled_terminal_job_cancellation_row_is_tagged_with_expire_ts`)
/// covers TTL for that case.
#[silo::test]
async fn cancel_running_job_does_not_tag_records_with_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_cancelled_key, job_info_key, job_status_key};

        let (_tmp, shard) = open_temp_shard_with_terminal_expire_ms(60_000).await;

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

        // Dequeue first so the job is Running.
        let _ = shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks;

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let status = shard
            .get_job_status("-", &job_id)
            .await
            .expect("status")
            .expect("exists");
        assert_eq!(
            status.kind,
            JobStatusKind::Running,
            "status should still be Running until worker acknowledges"
        );

        let raw_db = shard.db();
        for key in [
            job_status_key("-", &job_id),
            job_info_key("-", &job_id),
            job_cancelled_key("-", &job_id),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_none(),
                "running-cancel must not pre-tag record with expire_ts: key={key:?}"
            );
        }
    });
}

/// Sanity: when `terminal_job_expire_ms` is *not* configured, cancellation
/// of a Scheduled job must NOT tag any record with `expire_ts`. Guards
/// against an accidental hard-coded default that would enable the TTL
/// feature for operators that haven't opted in.
#[silo::test]
async fn cancel_without_terminal_expire_config_writes_no_expire_ts() {
    with_timeout!(20000, {
        use silo::keys::{job_cancelled_key, job_info_key, job_status_key};

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

        shard.cancel_job("-", &job_id).await.expect("cancel_job");

        let raw_db = shard.db();
        for key in [
            job_status_key("-", &job_id),
            job_info_key("-", &job_id),
            job_cancelled_key("-", &job_id),
        ] {
            let kv = raw_db
                .get_key_value(&key)
                .await
                .expect("get_key_value")
                .expect("row present");
            assert!(
                kv.expire_ts.is_none(),
                "cancel without TTL config must not tag record: key={key:?}"
            );
        }
    });
}
