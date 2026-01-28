mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;

// =============================================================================
// RestartJob gRPC Integration Tests
// =============================================================================

/// Test restarting a cancelled job via gRPC
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_cancelled_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "restart-test-job".to_string(),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Verify job is scheduled
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        // Cancel the job
        client
            .cancel_job(CancelJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Verify job is cancelled
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Cancelled as i32);

        // Restart the job
        client
            .restart_job(RestartJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Verify job is scheduled again
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        // Lease and complete the restarted job
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                    )),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Succeeded as i32);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restarting a failed job via gRPC
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_failed_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job without retry policy (will fail permanently on error)
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "restart-failed-job".to_string(),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease and fail the job
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Failure(Failure {
                    code: "TEST_ERROR".to_string(),
                    data: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            b"test failure".to_vec(),
                        )),
                    }),
                })),
            })
            .await?;

        // Verify job is failed
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Failed as i32);

        // Restart the failed job
        client
            .restart_job(RestartJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Verify job is scheduled again
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        // Complete the restarted job successfully
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "worker-2".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!("success after restart")).unwrap(),
                    )),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Succeeded as i32);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restart returns NOT_FOUND for non-existent job
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_nonexistent_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Try to restart non-existent job
        let result = client
            .restart_job(RestartJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "does-not-exist".to_string(),
                tenant: None,
            })
            .await;

        match result {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::NotFound);
            }
            Ok(_) => panic!("expected NOT_FOUND error"),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restart returns FAILED_PRECONDITION for running job
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_running_job_fails() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "running-job".to_string(),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease the job to make it Running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        // Verify job is running
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Running as i32);

        // Try to restart - should fail
        let result = client
            .restart_job(RestartJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await;

        match result {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("in progress"),
                    "error message should indicate job is in progress, got: {}",
                    status.message()
                );
            }
            Ok(_) => panic!("expected FAILED_PRECONDITION error"),
        }

        // Clean up - complete the job
        client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                })),
            })
            .await?;

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restart returns FAILED_PRECONDITION for succeeded job
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_succeeded_job_fails() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue and complete a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "succeeded-job".to_string(),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease and complete
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Succeeded as i32);

        // Try to restart - should fail
        let result = client
            .restart_job(RestartJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await;

        match result {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("succeeded"),
                    "error message should indicate job already succeeded, got: {}",
                    status.message()
                );
            }
            Ok(_) => panic!("expected FAILED_PRECONDITION error"),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
