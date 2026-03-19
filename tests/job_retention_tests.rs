mod grpc_integration_helpers;
mod test_helpers;

use std::collections::HashMap;
use std::time::Duration;

use grpc_integration_helpers::{
    TEST_SHARD_ID, create_test_factory, setup_test_server, shutdown_server,
};
use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::pb::{EnqueueRequest, GetJobRequest, SerializedBytes, serialized_bytes};
use silo::settings::{AppConfig, DEFAULT_TERMINAL_RETENTION_S};

async fn wait_for_job_deleted(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    job_id: &str,
) {
    for _ in 0..100 {
        if shard
            .get_job(tenant, job_id)
            .await
            .expect("get_job")
            .is_none()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status = shard
        .get_job_status(tenant, job_id)
        .await
        .expect("get status on timeout");
    let attempts = shard
        .get_job_attempts(tenant, job_id)
        .await
        .expect("get attempts on timeout");
    let ready_tasks = shard
        .peek_tasks("default", 20)
        .await
        .expect("peek tasks on timeout");
    panic!(
        "job {job_id} was not deleted in time; status={status:?}; attempts={}; ready_tasks={ready_tasks:?}",
        attempts.len()
    );
}

fn msgpack_bytes(value: &serde_json::Value) -> Vec<u8> {
    test_helpers::msgpack_payload(value)
}

#[silo::test]
async fn enqueue_stores_default_terminal_retention() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-default-retention".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"hello": "world"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let job = shard
        .get_job("-", &job_id)
        .await
        .expect("get_job")
        .expect("job exists");
    assert_eq!(
        job.terminal_retention_s(),
        Some(DEFAULT_TERMINAL_RETENTION_S)
    );
}

#[silo::test]
async fn enqueue_override_stores_terminal_retention() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let job_id = shard
        .enqueue_with_retention(
            "-",
            Some("job-custom-retention".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"hello": "world"})),
            vec![],
            None,
            Some(42),
            "default",
        )
        .await
        .expect("enqueue");

    let job = shard
        .get_job("-", &job_id)
        .await
        .expect("get_job")
        .expect("job exists");
    assert_eq!(job.terminal_retention_s(), Some(42));
}

#[silo::test]
async fn grpc_get_job_returns_terminal_retention() {
    let (factory, _tmp) = create_test_factory().await.expect("create factory");
    let config = AppConfig::load(None).expect("load default config");
    let (mut client, shutdown_tx, server, _addr) = setup_test_server(factory, config)
        .await
        .expect("setup server");

    let job_id = "grpc-terminal-retention";
    client
        .enqueue(tonic::Request::new(EnqueueRequest {
            shard: TEST_SHARD_ID.to_string(),
            id: job_id.to_string(),
            priority: 50,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_bytes(
                    &serde_json::json!({"hello": "grpc"}),
                ))),
            }),
            limits: vec![],
            tenant: None,
            metadata: HashMap::new(),
            task_group: "default".to_string(),
            terminal_retention_s: Some(123),
        }))
        .await
        .expect("enqueue");

    let job = client
        .get_job(tonic::Request::new(GetJobRequest {
            shard: TEST_SHARD_ID.to_string(),
            id: job_id.to_string(),
            tenant: None,
            include_attempts: false,
        }))
        .await
        .expect("get job")
        .into_inner();

    assert_eq!(job.terminal_retention_s, Some(123));

    shutdown_server(shutdown_tx, server)
        .await
        .expect("shutdown server");
}

#[silo::test]
async fn success_cleanup_deletes_job_and_attempts() {
    let (_tmp, shard) = test_helpers::open_temp_shard_with_default_terminal_retention_s(0).await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-success-cleanup".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"ok": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let task = shard
        .dequeue("worker-1", "default", 1)
        .await
        .expect("dequeue")
        .tasks
        .pop()
        .expect("task");
    shard
        .report_attempt_outcome(
            task.attempt().task_id(),
            AttemptOutcome::Success {
                result: vec![1, 2, 3],
            },
        )
        .await
        .expect("report outcome");

    wait_for_job_deleted(&shard, "-", &job_id).await;
    assert!(
        shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("attempts")
            .is_empty(),
        "cleanup should delete attempts",
    );

    let counters = shard.get_counters().await.expect("get counters");
    assert_eq!(counters.total_jobs, 0);
    assert_eq!(counters.completed_jobs, 0);
}

#[silo::test]
async fn failed_cleanup_deletes_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard_with_default_terminal_retention_s(0).await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-failed-cleanup".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"ok": false})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let task = shard
        .dequeue("worker-1", "default", 1)
        .await
        .expect("dequeue")
        .tasks
        .pop()
        .expect("task");
    shard
        .report_attempt_outcome(
            task.attempt().task_id(),
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![9, 9, 9],
            },
        )
        .await
        .expect("report outcome");

    wait_for_job_deleted(&shard, "-", &job_id).await;
}

#[silo::test]
async fn scheduled_cancel_cleanup_deletes_job() {
    let (_tmp, shard) = test_helpers::open_temp_shard_with_default_terminal_retention_s(0).await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-scheduled-cancel-cleanup".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"cancel": "scheduled"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    shard.cancel_job("-", &job_id).await.expect("cancel job");

    wait_for_job_deleted(&shard, "-", &job_id).await;
    assert!(
        shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("attempts")
            .is_empty(),
        "scheduled cancellation should not leave attempt records",
    );
}

#[silo::test]
async fn running_cancel_only_cleans_after_worker_reports_cancelled() {
    let (_tmp, shard) = test_helpers::open_temp_shard_with_default_terminal_retention_s(0).await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-running-cancel-cleanup".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"cancel": "running"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let task = shard
        .dequeue("worker-1", "default", 1)
        .await
        .expect("dequeue")
        .tasks
        .pop()
        .expect("task");

    shard.cancel_job("-", &job_id).await.expect("cancel job");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status")
        .expect("job exists");
    assert_eq!(status.kind, JobStatusKind::Running);

    shard
        .report_attempt_outcome(task.attempt().task_id(), AttemptOutcome::Cancelled)
        .await
        .expect("report cancelled");

    wait_for_job_deleted(&shard, "-", &job_id).await;
}

#[silo::test]
async fn restart_clears_pending_terminal_cleanup() {
    let (_tmp, shard) = test_helpers::open_temp_shard_with_default_terminal_retention_s(1).await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-restart-clears-cleanup".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"restart": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let task = shard
        .dequeue("worker-1", "default", 1)
        .await
        .expect("dequeue")
        .tasks
        .pop()
        .expect("task");
    shard
        .report_attempt_outcome(
            task.attempt().task_id(),
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![1],
            },
        )
        .await
        .expect("report failure");

    let failed_status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status")
        .expect("job exists");
    assert_eq!(failed_status.kind, JobStatusKind::Failed);

    shard.restart_job("-", &job_id).await.expect("restart job");
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let restarted_status = shard
        .get_job_status("-", &job_id)
        .await
        .expect("get status")
        .expect("job exists");
    assert_eq!(restarted_status.kind, JobStatusKind::Scheduled);
}

#[silo::test]
async fn explicit_delete_removes_terminal_job_before_retention_elapses() {
    let (_tmp, shard) = test_helpers::open_temp_shard_with_default_terminal_retention_s(1).await;

    let job_id = shard
        .enqueue(
            "-",
            Some("job-explicit-delete-before-retention".to_string()),
            50,
            0,
            None,
            msgpack_bytes(&serde_json::json!({"delete": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let task = shard
        .dequeue("worker-1", "default", 1)
        .await
        .expect("dequeue")
        .tasks
        .pop()
        .expect("task");
    shard
        .report_attempt_outcome(
            task.attempt().task_id(),
            AttemptOutcome::Success { result: vec![7] },
        )
        .await
        .expect("report outcome");

    shard.delete_job("-", &job_id).await.expect("delete job");
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    assert!(
        shard
            .get_job("-", &job_id)
            .await
            .expect("get_job")
            .is_none()
    );
    assert!(
        shard
            .get_job_attempts("-", &job_id)
            .await
            .expect("attempts")
            .is_empty(),
        "explicit delete should remove attempts",
    );
}

#[silo::test]
async fn importing_terminal_job_respects_retention_cleanup() {
    let (_tmp, shard) = test_helpers::open_temp_shard().await;

    let params = ImportJobParams {
        id: "job-import-retention-cleanup".to_string(),
        priority: 50,
        enqueue_time_ms: 1_700_000_000_000,
        start_at_ms: 0,
        terminal_retention_s: Some(0),
        retry_policy: None,
        payload: msgpack_bytes(&serde_json::json!({"imported": true})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts: vec![ImportedAttempt {
            status: ImportedAttemptStatus::Succeeded {
                result: vec![4, 5, 6],
            },
            started_at_ms: 1_700_000_001_000,
            finished_at_ms: 1_700_000_002_000,
        }],
    };

    let results = shard
        .import_jobs("-", vec![params])
        .await
        .expect("import jobs");
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    wait_for_job_deleted(&shard, "-", "job-import-retention-cleanup").await;
}
