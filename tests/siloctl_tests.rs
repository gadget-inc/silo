//! Integration tests for the siloctl library.
//!
//! These tests spin up a test gRPC server and call siloctl functions directly
//! to verify the CLI behaves correctly.

mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;
use silo::siloctl::{self, GlobalOptions};

fn opts_for_addr(addr: &std::net::SocketAddr) -> GlobalOptions {
    GlobalOptions {
        address: format!("http://{}", addr),
        tenant: None,
        json: false,
    }
}

fn opts_for_addr_json(addr: &std::net::SocketAddr) -> GlobalOptions {
    GlobalOptions {
        address: format!("http://{}", addr),
        tenant: None,
        json: true,
    }
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_cluster_info() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Test cluster info command
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::cluster_info(&opts, &mut output).await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("Cluster Information"),
            "output should contain header: {}",
            stdout
        );
        assert!(
            stdout.contains("Total shards:"),
            "output should contain shard count: {}",
            stdout
        );

        // Test JSON output
        let opts_json = opts_for_addr_json(&addr);
        let mut json_output = Vec::new();
        siloctl::cluster_info(&opts_json, &mut json_output).await?;
        let json_stdout = String::from_utf8(json_output)?;

        let parsed: serde_json::Value = serde_json::from_str(&json_stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_stdout));
        assert!(
            parsed.get("num_shards").is_some(),
            "JSON should have num_shards"
        );
        assert!(
            parsed.get("shard_owners").is_some(),
            "JSON should have shard_owners"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_get() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job via gRPC
        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "siloctl-test-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"test": "data"})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: None,
                metadata: [("key1".to_string(), "value1".to_string())]
                    .into_iter()
                    .collect(),
                task_group: "default".to_string(),
            })
            .await?;

        // Test job get command
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_get(&opts, &mut output, 0, "siloctl-test-job", false).await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("Job Details"),
            "output should contain header: {}",
            stdout
        );
        assert!(
            stdout.contains("siloctl-test-job"),
            "output should contain job ID: {}",
            stdout
        );
        assert!(
            stdout.contains("scheduled"),
            "output should show scheduled status: {}",
            stdout
        );
        assert!(
            stdout.contains("key1"),
            "output should show metadata: {}",
            stdout
        );

        // Test JSON output
        let opts_json = opts_for_addr_json(&addr);
        let mut json_output = Vec::new();
        siloctl::job_get(&opts_json, &mut json_output, 0, "siloctl-test-job", false).await?;
        let json_stdout = String::from_utf8(json_output)?;

        let parsed: serde_json::Value = serde_json::from_str(&json_stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_stdout));
        assert_eq!(parsed["id"], "siloctl-test-job");
        assert_eq!(parsed["status"], "scheduled");
        assert_eq!(parsed["priority"], 50);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_get_not_found() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Test job get for non-existent job
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        let result = siloctl::job_get(&opts, &mut output, 0, "nonexistent-job", false).await;

        assert!(result.is_err(), "should fail for nonexistent job");
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("not found") || err_str.contains("NotFound"),
            "error should mention not found: {}",
            err_str
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_cancel() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job scheduled in the future so it won't be picked up
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60000;

        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "cancel-test-job".to_string(),
                priority: 50,
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
            })
            .await?;

        // Cancel via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_cancel(&opts, &mut output, 0, "cancel-test-job").await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("cancelled successfully"),
            "output should confirm cancellation: {}",
            stdout
        );

        // Verify job is cancelled via gRPC
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "cancel-test-job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Cancelled as i32);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_restart() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a future job and cancel it
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60000;

        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "restart-test-job".to_string(),
                priority: 50,
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
            })
            .await?;

        let _ = client
            .cancel_job(CancelJobRequest {
                shard: 0,
                id: "restart-test-job".to_string(),
                tenant: None,
            })
            .await?;

        // Restart via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_restart(&opts, &mut output, 0, "restart-test-job").await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("restarted successfully"),
            "output should confirm restart: {}",
            stdout
        );

        // Verify job is scheduled again via gRPC
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "restart-test-job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_expedite() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a future-scheduled job
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60000; // 1 minute in future

        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "expedite-test-job".to_string(),
                priority: 50,
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
            })
            .await?;

        // Verify job is scheduled in the future before expediting
        let job_before = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "expedite-test-job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(
            job_before.next_attempt_starts_after_ms,
            Some(future_ms),
            "job should be scheduled for future"
        );

        // Expedite via siloctl - main test is that this succeeds without error
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_expedite(&opts, &mut output, 0, "expedite-test-job").await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("expedited successfully"),
            "output should confirm expedite: {}",
            stdout
        );

        // Verify job still exists and is still scheduled (not cancelled/failed)
        let job_after = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "expedite-test-job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(
            job_after.status,
            JobStatus::Scheduled as i32,
            "job should still be scheduled after expedite"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_delete() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job scheduled in the future so it won't be picked up by the broker
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60000;

        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "delete-test-job".to_string(),
                priority: 50,
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
            })
            .await?;

        // Cancel the job first (required before delete for non-terminal jobs)
        let _ = client
            .cancel_job(CancelJobRequest {
                shard: 0,
                id: "delete-test-job".to_string(),
                tenant: None,
            })
            .await?;

        // Delete via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_delete(&opts, &mut output, 0, "delete-test-job").await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("deleted successfully"),
            "output should confirm deletion: {}",
            stdout
        );

        // Verify job is gone via gRPC
        let result = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "delete-test-job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await;
        assert!(result.is_err(), "job should not exist after deletion");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_query() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue some jobs (scheduled in future so they don't get picked up)
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60000;

        for i in 0..3 {
            let _ = client
                .enqueue(EnqueueRequest {
                    shard: 0,
                    id: format!("query-test-job-{}", i),
                    priority: 50,
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
                })
                .await?;
        }

        // Query via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::query(&opts, &mut output, 0, "SELECT id FROM jobs LIMIT 10").await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("query-test-job"),
            "output should contain job IDs: {}",
            stdout
        );
        assert!(
            stdout.contains("row(s) returned"),
            "output should show row count: {}",
            stdout
        );

        // Test JSON output
        let opts_json = opts_for_addr_json(&addr);
        let mut json_output = Vec::new();
        siloctl::query(&opts_json, &mut json_output, 0, "SELECT id FROM jobs LIMIT 10").await?;
        let json_stdout = String::from_utf8(json_output)?;

        let parsed: serde_json::Value = serde_json::from_str(&json_stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_stdout));
        assert!(parsed.get("columns").is_some(), "JSON should have columns");
        assert!(parsed.get("rows").is_some(), "JSON should have rows");
        assert!(
            parsed["row_count"].as_i64().unwrap() >= 3,
            "should have at least 3 rows"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_job_get_with_attempts() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "attempts-test-job".to_string(),
                priority: 50,
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

        // Lease and complete the task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "test-worker".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        let task = &lease_resp.tasks[0];

        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"result": "done"})).unwrap(),
                    )),
                })),
            })
            .await?;

        // Get job with attempts via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_get(&opts, &mut output, 0, "attempts-test-job", true).await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("Attempts:"),
            "output should show attempts section: {}",
            stdout
        );
        assert!(
            stdout.contains("succeeded"),
            "output should show succeeded attempt: {}",
            stdout
        );

        // Test JSON output with attempts
        let opts_json = opts_for_addr_json(&addr);
        let mut json_output = Vec::new();
        siloctl::job_get(&opts_json, &mut json_output, 0, "attempts-test-job", true).await?;
        let json_stdout = String::from_utf8(json_output)?;

        let parsed: serde_json::Value = serde_json::from_str(&json_stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_stdout));
        assert!(
            parsed.get("attempts").is_some(),
            "JSON should have attempts"
        );
        let attempts = parsed["attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), 1, "should have 1 attempt");
        assert_eq!(attempts[0]["status"], "succeeded");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that siloctl handles connection errors gracefully
#[silo::test(flavor = "multi_thread")]
async fn siloctl_connection_error() -> anyhow::Result<()> {
    // Try to connect to a non-existent server
    let opts = GlobalOptions {
        address: "http://127.0.0.1:59999".to_string(),
        tenant: None,
        json: false,
    };
    let mut output = Vec::new();
    let result = siloctl::cluster_info(&opts, &mut output).await;

    assert!(result.is_err(), "should fail when server is not running");

    Ok(())
}
