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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "status_test_job".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Get job - should be Scheduled initially
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        // Get job - should be Running now
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"result": "done"})).unwrap(),
                    )),
                })),
            })
            .await?;

        // Get job - should be Succeeded now
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "scheduled_job".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Try to get result for scheduled job - should fail with FAILED_PRECONDITION
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);

        // Try to get result for running job - should also fail with FAILED_PRECONDITION
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "success_job".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Lease and complete the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];
        let result_data = rmp_serde::to_vec(&serde_json::json!({"answer": 42})).unwrap();

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(result_data.clone())),
                })),
            })
            .await?;

        // Get job result - should return success data
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                let actual_data = match data.encoding {
                    Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                    None => vec![],
                };
                assert_eq!(actual_data, result_data);
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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                )),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
            task_group: "default".to_string(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease and fail the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];
        let error_code = "SOMETHING_WENT_WRONG".to_string();
        let error_data = b"{\"reason\":\"test failure\"}".to_vec();

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Failure(Failure {
                    code: error_code.clone(),
                    data: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(error_data.clone())),
                    }),
                })),
            })
            .await?;

        // Get job result - should return failure info
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                let actual_error_data = failure
                    .error_data
                    .and_then(|e| match e.encoding {
                        Some(serialized_bytes::Encoding::Msgpack(d)) => Some(d),
                        None => None,
                    })
                    .unwrap_or_default();
                assert_eq!(actual_error_data, error_data);
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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "cancelled_job".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Cancel the job while it's scheduled (before running)
        let _ = client
            .cancel_job(CancelJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "cancelled_job".to_string(),
                tenant: None,
            })
            .await?;

        // Get job result - should return cancelled info
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "cancel_running_job".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Lease the task to start running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];

        // Cancel the job while it's running
        let _ = client
            .cancel_job(CancelJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "cancel_running_job".to_string(),
                tenant: None,
            })
            .await?;

        // Worker reports Cancelled outcome after seeing cancellation in heartbeat
        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Cancelled(Cancelled {})),
            })
            .await?;

        // Get job result - should return cancelled info
        let result = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "non-terminal-job".to_string(),
                priority: 5,
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
            .await?;

        // Get result for scheduled (non-terminal) job - should fail
        let res = client
            .get_job_result(GetJobResultRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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

/// Test that GetJob returns next_attempt_starts_after_ms correctly across job lifecycle
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_next_attempt_starts_after() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Test 1: Enqueue with start_at_ms=0 should have next_attempt_starts_after_ms close to now
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "next_attempt_test_1".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "next_attempt_test_1".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job.status, JobStatus::Scheduled as i32);
        assert!(
            job.next_attempt_starts_after_ms.is_some(),
            "scheduled job should have next_attempt_starts_after_ms"
        );
        // The next_attempt_starts_after_ms should be close to when the job was enqueued (start_at_ms=0 means now)
        let next_starts = job.next_attempt_starts_after_ms.unwrap();
        assert!(
            (next_starts - now_ms).abs() < 5000,
            "next_attempt_starts_after_ms should be close to now for immediate jobs"
        );

        // Test 2: Enqueue with future start_at_ms
        let future_ms = now_ms + 60000; // 1 minute in future
        let enq_future = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "next_attempt_test_future".to_string(),
            priority: 10,
            start_at_ms: future_ms,
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
        };
        let _ = client.enqueue(enq_future).await?;

        let job_future = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "next_attempt_test_future".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job_future.status, JobStatus::Scheduled as i32);
        assert_eq!(
            job_future.next_attempt_starts_after_ms,
            Some(future_ms),
            "future-scheduled job should have next_attempt_starts_after_ms set to start_at_ms"
        );

        // Test 3: Running job should NOT have next_attempt_starts_after_ms
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        assert!(!lease_resp.tasks.is_empty());
        let task = &lease_resp.tasks[0];

        let running_job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "next_attempt_test_1".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(running_job.status, JobStatus::Running as i32);
        assert!(
            running_job.next_attempt_starts_after_ms.is_none(),
            "running job should NOT have next_attempt_starts_after_ms"
        );

        // Test 4: Complete the job - terminal jobs should NOT have next_attempt_starts_after_ms
        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"result": "done"})).unwrap(),
                    )),
                })),
            })
            .await?;

        let succeeded_job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "next_attempt_test_1".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(succeeded_job.status, JobStatus::Succeeded as i32);
        assert!(
            succeeded_job.next_attempt_starts_after_ms.is_none(),
            "succeeded job should NOT have next_attempt_starts_after_ms"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that GetJob returns next_attempt_starts_after_ms correctly for retrying jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_next_attempt_after_retry() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Enqueue a job with retry policy
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "retry_next_attempt_test".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: Some(RetryPolicy {
                retry_count: 3,
                initial_interval_ms: 1000, // 1 second retry interval
                max_interval_ms: 10000,
                randomize_interval: false,
                backoff_factor: 2.0,
            }),
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                )),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
            task_group: "default".to_string(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease and fail the task to trigger retry
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Failure(Failure {
                    code: "TEST_FAILURE".to_string(),
                    data: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(b"{}".to_vec())),
                    }),
                })),
            })
            .await?;

        // After failure with retry, job should be Scheduled with next_attempt_starts_after_ms in the future
        let retrying_job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "retry_next_attempt_test".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(retrying_job.status, JobStatus::Scheduled as i32);
        assert!(
            retrying_job.next_attempt_starts_after_ms.is_some(),
            "retrying job should have next_attempt_starts_after_ms"
        );

        // The next attempt should be in the future (after the retry interval)
        let next_attempt = retrying_job.next_attempt_starts_after_ms.unwrap();
        assert!(
            next_attempt > now_ms,
            "next_attempt_starts_after_ms ({}) should be after now ({})",
            next_attempt,
            now_ms
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that cancelled jobs preserve next_attempt_starts_after_ms (for O(1) task key reconstruction)
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_next_attempt_cancelled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "cancel_next_attempt_test".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Verify scheduled job has next_attempt_starts_after_ms
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "cancel_next_attempt_test".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert!(job.next_attempt_starts_after_ms.is_some());

        // Cancel the job
        let _ = client
            .cancel_job(CancelJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "cancel_next_attempt_test".to_string(),
                tenant: None,
            })
            .await?;

        // Cancelled job preserves scheduling fields for O(1) task key reconstruction
        let cancelled_job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "cancel_next_attempt_test".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(cancelled_job.status, JobStatus::Cancelled as i32);
        // Cancelled status preserves scheduling fields for O(1) task key reconstruction
        assert!(
            cancelled_job.next_attempt_starts_after_ms.is_some(),
            "cancelled job should preserve next_attempt_starts_after_ms"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that leased tasks include job limits
#[silo::test(flavor = "multi_thread")]
async fn grpc_lease_tasks_includes_limits() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Create limits to enqueue with the job
        let concurrency_limit = Limit {
            limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                key: "test-concurrency-key".to_string(),
                max_concurrency: 5,
            })),
        };

        let rate_limit = Limit {
            limit: Some(limit::Limit::RateLimit(GubernatorRateLimit {
                name: "test-rate-limit".to_string(),
                unique_key: "test-rate-key".to_string(),
                limit: 100,
                duration_ms: 60_000,
                hits: 1,
                algorithm: GubernatorAlgorithm::TokenBucket as i32,
                behavior: 0,
                retry_policy: Some(RateLimitRetryPolicy {
                    initial_backoff_ms: 1000,
                    max_backoff_ms: 30000,
                    backoff_multiplier: 2.0,
                    max_retries: 5,
                }),
            })),
        };

        // Enqueue a job with limits
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "limits_test_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({"test": "data"})).unwrap(),
                )),
            }),
            limits: vec![concurrency_limit.clone(), rate_limit.clone()],
            tenant: None,
            metadata: std::collections::HashMap::new(),
            task_group: "default".to_string(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        // Verify task was leased
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];
        assert_eq!(task.job_id, "limits_test_job");

        // Verify limits are included in the task
        assert_eq!(
            task.limits.len(),
            2,
            "task should have 2 limits, got {:?}",
            task.limits
        );

        // Verify first limit is the concurrency limit
        let first_limit = &task.limits[0];
        match &first_limit.limit {
            Some(limit::Limit::Concurrency(c)) => {
                assert_eq!(c.key, "test-concurrency-key");
                assert_eq!(c.max_concurrency, 5);
            }
            other => panic!("expected concurrency limit, got {:?}", other),
        }

        // Verify second limit is the rate limit
        let second_limit = &task.limits[1];
        match &second_limit.limit {
            Some(limit::Limit::RateLimit(r)) => {
                assert_eq!(r.name, "test-rate-limit");
                assert_eq!(r.unique_key, "test-rate-key");
                assert_eq!(r.limit, 100);
                assert_eq!(r.duration_ms, 60_000);
            }
            other => panic!("expected rate limit, got {:?}", other),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that leased tasks include floating concurrency limits
#[silo::test(flavor = "multi_thread")]
async fn grpc_lease_tasks_includes_floating_concurrency_limit() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Create a floating concurrency limit
        let floating_limit = Limit {
            limit: Some(limit::Limit::FloatingConcurrency(
                FloatingConcurrencyLimit {
                    key: "test-floating-key".to_string(),
                    default_max_concurrency: 10,
                    refresh_interval_ms: 30_000,
                    metadata: [
                        ("org_id".to_string(), "org-123".to_string()),
                        ("tier".to_string(), "premium".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                },
            )),
        };

        // Enqueue a job with the floating limit
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "floating_limits_test_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({"test": "data"})).unwrap(),
                )),
            }),
            limits: vec![floating_limit.clone()],
            tenant: None,
            metadata: std::collections::HashMap::new(),
            task_group: "default".to_string(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        // Verify task was leased
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];
        assert_eq!(task.job_id, "floating_limits_test_job");

        // Verify floating concurrency limit is included
        assert_eq!(
            task.limits.len(),
            1,
            "task should have 1 limit, got {:?}",
            task.limits
        );

        let limit = &task.limits[0];
        match &limit.limit {
            Some(limit::Limit::FloatingConcurrency(f)) => {
                assert_eq!(f.key, "test-floating-key");
                assert_eq!(f.default_max_concurrency, 10);
                assert_eq!(f.refresh_interval_ms, 30_000);
                assert_eq!(f.metadata.get("org_id"), Some(&"org-123".to_string()));
                assert_eq!(f.metadata.get("tier"), Some(&"premium".to_string()));
            }
            other => panic!("expected floating concurrency limit, got {:?}", other),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that leased tasks with no limits have empty limits array
#[silo::test(flavor = "multi_thread")]
async fn grpc_lease_tasks_empty_limits() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job without limits
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "no_limits_test_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({"test": "data"})).unwrap(),
                )),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
            task_group: "default".to_string(),
        };
        let _ = client.enqueue(enq).await?;

        // Lease the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        // Verify task was leased
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];
        assert_eq!(task.job_id, "no_limits_test_job");

        // Verify limits array is empty
        assert!(
            task.limits.is_empty(),
            "task should have no limits, got {:?}",
            task.limits
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that GetJob returns the result field for succeeded jobs without include_attempts
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_field_without_attempts() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "result_field_no_attempts".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // GetJob for a scheduled job should have no result
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "result_field_no_attempts".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert!(job.result.is_none(), "scheduled job should have no result");

        // Lease and complete the task with a result
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let task = &lease_resp.tasks[0];
        let result_data = rmp_serde::to_vec(&serde_json::json!({"answer": 42})).unwrap();

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(result_data.clone())),
                })),
            })
            .await?;

        // GetJob without include_attempts should still have the result field populated
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "result_field_no_attempts".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job.status, JobStatus::Succeeded as i32);
        assert!(job.attempts.is_empty(), "attempts should not be populated");
        let result = job.result.expect("succeeded job should have result");
        let actual_data = match result.encoding {
            Some(serialized_bytes::Encoding::Msgpack(d)) => d,
            None => vec![],
        };
        assert_eq!(actual_data, result_data);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that GetJob returns the result field for succeeded jobs with include_attempts
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_field_with_attempts() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "result_field_with_attempts".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Lease and complete the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let task = &lease_resp.tasks[0];
        let result_data = rmp_serde::to_vec(&serde_json::json!({"value": "hello"})).unwrap();

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(result_data.clone())),
                })),
            })
            .await?;

        // GetJob with include_attempts should have both attempts and result
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "result_field_with_attempts".to_string(),
                tenant: None,
                include_attempts: true,
            })
            .await?
            .into_inner();

        assert_eq!(job.status, JobStatus::Succeeded as i32);
        assert!(!job.attempts.is_empty(), "attempts should be populated");

        // Verify the top-level result matches the attempt's result
        let result = job.result.expect("succeeded job should have result");
        let actual_data = match result.encoding {
            Some(serialized_bytes::Encoding::Msgpack(d)) => d,
            None => vec![],
        };
        assert_eq!(actual_data, result_data);

        // Also verify the attempt has the same result
        let last_attempt = job.attempts.last().unwrap();
        let attempt_result = last_attempt
            .result
            .as_ref()
            .expect("succeeded attempt should have result");
        let attempt_data = match &attempt_result.encoding {
            Some(serialized_bytes::Encoding::Msgpack(d)) => d.clone(),
            None => vec![],
        };
        assert_eq!(attempt_data, result_data);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that GetJob result field is None for failed jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_field_none_for_failed() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "result_field_failed".to_string(),
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
        };
        let _ = client.enqueue(enq).await?;

        // Lease and fail the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        let task = &lease_resp.tasks[0];

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Failure(Failure {
                    code: "TEST_ERROR".to_string(),
                    data: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"msg": "failed"})).unwrap(),
                        )),
                    }),
                })),
            })
            .await?;

        // GetJob for a failed job should have no result
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "result_field_failed".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job.status, JobStatus::Failed as i32);
        assert!(job.result.is_none(), "failed job should have no result");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
