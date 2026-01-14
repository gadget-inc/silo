mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;

/// Test that GetJob returns status information correctly
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_includes_status() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "status_test_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Get job - should be Scheduled initially
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "status_test_job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job.id, "status_test_job");
        assert_eq!(job.status, JobStatus::Scheduled as i32);
        assert!(
            job.status_changed_at_ms > 0,
            "status_changed_at_ms should be set"
        );

        // Lease the task to start running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "w1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();

        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        // Get job - should be Running now
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "status_test_job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job.status, JobStatus::Running as i32);

        // Report success
        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"{\"result\":\"done\"}".to_vec(),
                })),
            })
            .await?;

        // Get job - should be Succeeded now
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "status_test_job".to_string(),
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

/// Test GetJobResult returns NOT_FOUND for non-existent job
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_not_found() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Try to get result for non-existent job
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "nonexistent_job".to_string(),
                tenant: None,
            })
            .await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code(), tonic::Code::NotFound);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns FAILED_PRECONDITION for scheduled/running jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_not_terminal() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "scheduled_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Try to get result for scheduled job - should fail with FAILED_PRECONDITION
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "scheduled_job".to_string(),
                tenant: None,
            })
            .await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("not complete"));

        // Lease the task to make it running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "w1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);

        // Try to get result for running job - should also fail with FAILED_PRECONDITION
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "scheduled_job".to_string(),
                tenant: None,
            })
            .await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns success data for succeeded jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_success() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "success_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease and complete the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "w1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];
        let result_data = b"{\"answer\":42}".to_vec();

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: result_data.clone(),
                })),
            })
            .await?;

        // Get job result - should return success data
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "success_job".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(result.id, "success_job");
        assert_eq!(result.status, JobStatus::Succeeded as i32);
        assert!(result.finished_at_ms > 0);

        match result.result {
            Some(get_job_result_response::Result::SuccessData(data)) => {
                assert_eq!(data.data, result_data);
            }
            _ => panic!("expected success_data, got {:?}", result.result),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns failure info for failed jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_failure() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job with no retries
        let enq = EnqueueRequest {
            shard: 0,
            id: "failed_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: Some(RetryPolicy {
                retry_count: 0, // No retries
                initial_interval_ms: 1000,
                max_interval_ms: 1000,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease and fail the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "w1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];
        let error_code = "SOMETHING_WENT_WRONG".to_string();
        let error_data = b"{\"reason\":\"test failure\"}".to_vec();

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Failure(Failure {
                    code: error_code.clone(),
                    data: error_data.clone(),
                })),
            })
            .await?;

        // Get job result - should return failure info
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "failed_job".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(result.id, "failed_job");
        assert_eq!(result.status, JobStatus::Failed as i32);
        assert!(result.finished_at_ms > 0);

        match result.result {
            Some(get_job_result_response::Result::Failure(failure)) => {
                assert_eq!(failure.error_code, error_code);
                assert_eq!(failure.error_data, error_data);
            }
            _ => panic!("expected failure, got {:?}", result.result),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns cancelled info for cancelled jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_cancelled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "cancelled_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Cancel the job while it's scheduled (before running)
        let _ = client
            .cancel_job(CancelJobRequest {
                shard: 0,
                id: "cancelled_job".to_string(),
                tenant: None,
            })
            .await?;

        // Get job result - should return cancelled info
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "cancelled_job".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(result.id, "cancelled_job");
        assert_eq!(result.status, JobStatus::Cancelled as i32);
        assert!(result.finished_at_ms > 0);

        match result.result {
            Some(get_job_result_response::Result::Cancelled(cancelled)) => {
                assert!(cancelled.cancelled_at_ms > 0);
            }
            _ => panic!("expected cancelled, got {:?}", result.result),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult for a running job that gets cancelled
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_cancelled_while_running() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "cancel_running_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease the task to start running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "w1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];

        // Cancel the job while it's running
        let _ = client
            .cancel_job(CancelJobRequest {
                shard: 0,
                id: "cancel_running_job".to_string(),
                tenant: None,
            })
            .await?;

        // Worker reports Cancelled outcome after seeing cancellation in heartbeat
        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Cancelled(Cancelled {})),
            })
            .await?;

        // Get job result - should return cancelled info
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "cancel_running_job".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(result.id, "cancel_running_job");
        assert_eq!(result.status, JobStatus::Cancelled as i32);
        assert!(result.finished_at_ms > 0);

        match result.result {
            Some(get_job_result_response::Result::Cancelled(cancelled)) => {
                assert!(cancelled.cancelled_at_ms > 0);
            }
            _ => panic!("expected cancelled, got {:?}", result.result),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_get_job_result_for_non_terminal_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "non-terminal-job".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
            })
            .await?;

        // Get result for scheduled (non-terminal) job - should fail
        let res = client
            .get_job_result(GetJobResultRequest {
                shard: 0,
                id: "non-terminal-job".to_string(),
                tenant: None,
            })
            .await;

        match res {
            Ok(_) => panic!("expected error for non-terminal job"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("not complete"),
                    "error should mention not complete: {}",
                    status.message()
                );
            }
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
