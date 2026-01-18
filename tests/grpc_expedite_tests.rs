mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;

// =============================================================================
// ExpediteJob gRPC Integration Tests
// =============================================================================

/// Test expediting a future-scheduled job via gRPC
#[silo::test(flavor = "multi_thread")]
async fn grpc_expedite_future_scheduled_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job scheduled 1 hour in the future
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let future_time_ms = now_ms + 3_600_000; // 1 hour from now

        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "expedite-test-job".to_string(),
                priority: 10,
                start_at_ms: future_time_ms,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
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
                shard: 0,
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        // Try to lease - should get nothing (job is future-scheduled)
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert!(lease_resp.tasks.is_empty(), "future job should not be leased yet");

        // Expedite the job
        client
            .expedite_job(ExpediteJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Now the job should be leasable
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1, "expedited job should be leased");
        let task = &lease_resp.tasks[0];
        assert_eq!(task.job_id, job_id);

        // Complete the job
        client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!("expedited success")).unwrap(),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
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

/// Test expedite returns NOT_FOUND for non-existent job
#[silo::test(flavor = "multi_thread")]
async fn grpc_expedite_nonexistent_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Try to expedite non-existent job
        let result = client
            .expedite_job(ExpediteJobRequest {
                shard: 0,
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

/// Test expedite returns FAILED_PRECONDITION for running job
#[silo::test(flavor = "multi_thread")]
async fn grpc_expedite_running_job_fails() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "expedite-running-job".to_string(),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease the job to make it running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
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
                shard: 0,
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Running as i32);

        // Try to expedite - should fail
        let result = client
            .expedite_job(ExpediteJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await;

        match result {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("running"),
                    "error message should indicate job is running, got: {}",
                    status.message()
                );
            }
            Ok(_) => panic!("expected FAILED_PRECONDITION error"),
        }

        // Complete the job to clean up
        client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
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

/// Test expedite returns FAILED_PRECONDITION for cancelled job
#[silo::test(flavor = "multi_thread")]
async fn grpc_expedite_cancelled_job_fails() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a future-scheduled job
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let future_time_ms = now_ms + 3_600_000;

        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "expedite-cancelled-job".to_string(),
                priority: 10,
                start_at_ms: future_time_ms,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Cancel the job
        client
            .cancel_job(CancelJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Verify job is cancelled
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Cancelled as i32);

        // Try to expedite - should fail
        let result = client
            .expedite_job(ExpediteJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await;

        match result {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("cancelled"),
                    "error message should indicate job is cancelled, got: {}",
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
