mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;

/// Test leasing a specific job's task via gRPC - full round-trip.
#[silo::test(flavor = "multi_thread")]
async fn lease_task_grpc_basic() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "lease-task-test-job".to_string(),
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

        // Lease the specific task
        let lease_resp = client
            .lease_task(LeaseTaskRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                worker_id: "test-worker".to_string(),
            })
            .await?
            .into_inner();

        let task = lease_resp.task.expect("should have task");
        assert_eq!(task.job_id, job_id);
        assert_eq!(task.attempt_number, 1);
        assert_eq!(task.shard, crate::grpc_integration_helpers::TEST_SHARD_ID);

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

        // Complete the job via report outcome
        client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!("leased success")).unwrap(),
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

/// Test lease_task returns NOT_FOUND for non-existent job.
#[silo::test(flavor = "multi_thread")]
async fn lease_task_grpc_not_found() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let result = client
            .lease_task(LeaseTaskRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "does-not-exist".to_string(),
                tenant: None,
                worker_id: "test-worker".to_string(),
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

/// Test lease_task returns FAILED_PRECONDITION for already running job.
#[silo::test(flavor = "multi_thread")]
async fn lease_task_grpc_already_running() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "lease-task-already-running".to_string(),
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

        // Lease via LeaseTasks to make it running
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

        // Try to lease again - should fail
        let result = client
            .lease_task(LeaseTaskRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: job_id.clone(),
                tenant: None,
                worker_id: "test-worker".to_string(),
            })
            .await;

        match result {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("running"),
                    "error should mention running, got: {}",
                    status.message()
                );
            }
            Ok(_) => panic!("expected FAILED_PRECONDITION error"),
        }

        // Clean up
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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
