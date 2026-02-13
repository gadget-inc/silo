//! Integration tests for the siloctl library.
//!
//! These tests spin up a test gRPC server and call siloctl functions directly
//! to verify the CLI behaves correctly.

mod grpc_integration_helpers;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::coordination::NoneCoordinator;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::server::run_server;
use silo::settings::{AppConfig, Backend, DatabaseTemplate};
use silo::siloctl::{self, GlobalOptions};
use tokio::net::TcpListener;

// Global mutex to serialize profile tests - only one CPU profiler can run at a time system-wide
static PROFILE_TEST_MUTEX: Mutex<()> = Mutex::new(());

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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
        siloctl::job_get(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "siloctl-test-job",
            false,
        )
        .await?;
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
        siloctl::job_get(
            &opts_json,
            &mut json_output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "siloctl-test-job",
            false,
        )
        .await?;
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
        let result = siloctl::job_get(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "nonexistent-job",
            false,
        )
        .await;

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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
        siloctl::job_cancel(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "cancel-test-job",
        )
        .await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("cancelled successfully"),
            "output should confirm cancellation: {}",
            stdout
        );

        // Verify job is cancelled via gRPC
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "restart-test-job".to_string(),
                tenant: None,
            })
            .await?;

        // Restart via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_restart(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "restart-test-job",
        )
        .await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("restarted successfully"),
            "output should confirm restart: {}",
            stdout
        );

        // Verify job is scheduled again via gRPC
        let job = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
        siloctl::job_expedite(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "expedite-test-job",
        )
        .await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("expedited successfully"),
            "output should confirm expedite: {}",
            stdout
        );

        // Verify job still exists and is still scheduled (not cancelled/failed)
        let job_after = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "delete-test-job".to_string(),
                tenant: None,
            })
            .await?;

        // Delete via siloctl
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::job_delete(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "delete-test-job",
        )
        .await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("deleted successfully"),
            "output should confirm deletion: {}",
            stdout
        );

        // Verify job is gone via gRPC
        let result = client
            .get_job(GetJobRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                    shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
        siloctl::query(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "SELECT id FROM jobs LIMIT 10",
        )
        .await?;
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
        siloctl::query(
            &opts_json,
            &mut json_output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "SELECT id FROM jobs LIMIT 10",
        )
        .await?;
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                shard: Some(crate::grpc_integration_helpers::TEST_SHARD_ID.to_string()),
                worker_id: "test-worker".to_string(),
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
        siloctl::job_get(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "attempts-test-job",
            true,
        )
        .await?;
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
        siloctl::job_get(
            &opts_json,
            &mut json_output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "attempts-test-job",
            true,
        )
        .await?;
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

#[silo::test(flavor = "multi_thread")]
async fn siloctl_profile() -> anyhow::Result<()> {
    // Only one CPU profiler can run at a time system-wide
    let _profile_lock = PROFILE_TEST_MUTEX.lock().unwrap();

    let _guard = tokio::time::timeout(std::time::Duration::from_millis(60000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();

        // Create temp directory for profile output
        let tmp_dir = tempfile::tempdir()?;
        let output_path = tmp_dir.path().join("test-profile.pb.gz");

        // Run a short profile (2 seconds)
        siloctl::profile(
            &opts,
            &mut output,
            2, // 2 second duration for test
            100,
            Some(output_path.to_string_lossy().to_string()),
        )
        .await?;

        let stdout = String::from_utf8(output)?;
        assert!(
            stdout.contains("Profile saved to:"),
            "should confirm save: {}",
            stdout
        );
        assert!(output_path.exists(), "profile file should exist");

        // Verify it's a valid gzip file with some data
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0, "profile should not be empty");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_profile_json_output() -> anyhow::Result<()> {
    // Only one CPU profiler can run at a time system-wide
    let _profile_lock = PROFILE_TEST_MUTEX.lock().unwrap();

    let _guard = tokio::time::timeout(std::time::Duration::from_millis(60000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let opts = opts_for_addr_json(&addr);
        let mut output = Vec::new();

        // Create temp directory for profile output
        let tmp_dir = tempfile::tempdir()?;
        let output_path = tmp_dir.path().join("test-profile-json.pb.gz");

        // Run a short profile (2 seconds) with JSON output
        siloctl::profile(
            &opts,
            &mut output,
            2,
            100,
            Some(output_path.to_string_lossy().to_string()),
        )
        .await?;

        let stdout = String::from_utf8(output)?;

        // Parse JSON output
        let parsed: serde_json::Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", stdout));

        assert_eq!(parsed["status"], "completed", "status should be completed");
        assert!(
            parsed.get("output_file").is_some(),
            "JSON should have output_file"
        );
        assert!(
            parsed.get("duration_seconds").is_some(),
            "JSON should have duration_seconds"
        );
        assert!(parsed.get("samples").is_some(), "JSON should have samples");
        assert!(
            parsed.get("profile_bytes").is_some(),
            "JSON should have profile_bytes"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_shard_configure() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Test shard configure command - set a ring
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::shard_configure(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            Some("gpu-ring".to_string()),
        )
        .await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("configured"),
            "output should confirm configuration: {}",
            stdout
        );
        assert!(
            stdout.contains("(default)") || stdout.contains("Previous ring:"),
            "output should show previous ring: {}",
            stdout
        );
        assert!(
            stdout.contains("gpu-ring"),
            "output should show new ring: {}",
            stdout
        );

        // Now clear the ring (set to default)
        let mut output2 = Vec::new();
        siloctl::shard_configure(
            &opts,
            &mut output2,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            None,
        )
        .await?;
        let stdout2 = String::from_utf8(output2)?;

        assert!(
            stdout2.contains("configured"),
            "output should confirm configuration: {}",
            stdout2
        );
        assert!(
            stdout2.contains("gpu-ring"),
            "output should show previous ring was gpu-ring: {}",
            stdout2
        );
        assert!(
            stdout2.contains("(default)"),
            "output should show current ring is default: {}",
            stdout2
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_shard_configure_json_output() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Test shard configure command with JSON output - set a ring
        let opts = opts_for_addr_json(&addr);
        let mut output = Vec::new();
        siloctl::shard_configure(
            &opts,
            &mut output,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            Some("high-memory".to_string()),
        )
        .await?;
        let stdout = String::from_utf8(output)?;

        let parsed: serde_json::Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", stdout));

        assert_eq!(
            parsed["shard_id"],
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            "JSON should have correct shard_id"
        );
        assert!(
            parsed.get("previous_ring").is_some(),
            "JSON should have previous_ring"
        );
        assert_eq!(
            parsed["current_ring"], "high-memory",
            "JSON should show current ring"
        );

        // Configure again to verify previous_ring is populated correctly
        let mut output2 = Vec::new();
        siloctl::shard_configure(
            &opts,
            &mut output2,
            crate::grpc_integration_helpers::TEST_SHARD_ID,
            Some("gpu-ring".to_string()),
        )
        .await?;
        let stdout2 = String::from_utf8(output2)?;

        let parsed2: serde_json::Value = serde_json::from_str(&stdout2)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", stdout2));

        assert_eq!(
            parsed2["previous_ring"], "high-memory",
            "previous_ring should be the last configured ring"
        );
        assert_eq!(
            parsed2["current_ring"], "gpu-ring",
            "current_ring should be the new ring"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn siloctl_shard_configure_invalid_shard() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(30000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Test shard configure command with non-existent shard
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        let result = siloctl::shard_configure(
            &opts,
            &mut output,
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            Some("test-ring".to_string()),
        )
        .await;

        assert!(result.is_err(), "should fail for non-existent shard");
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("not found")
                || err_str.contains("NotFound")
                || err_str.contains("ShardNotFound"),
            "error should mention shard not found: {}",
            err_str
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test]
async fn siloctl_validate_config() -> anyhow::Result<()> {
    use std::path::Path;

    let opts = GlobalOptions::default();

    // Test with a valid config file
    let valid_config_path = Path::new("example_configs/local-dev.toml");
    let mut output = Vec::new();
    let result = siloctl::validate_config(&opts, &mut output, valid_config_path).await;
    assert!(result.is_ok(), "should succeed for valid config");

    let stdout = String::from_utf8(output)?;
    assert!(
        stdout.contains("Config is valid"),
        "output should confirm validity: {}",
        stdout
    );

    // Test JSON output for valid config
    let opts_json = GlobalOptions {
        json: true,
        ..Default::default()
    };
    let mut json_output = Vec::new();
    siloctl::validate_config(&opts_json, &mut json_output, valid_config_path).await?;
    let json_stdout = String::from_utf8(json_output)?;

    let parsed: serde_json::Value = serde_json::from_str(&json_stdout)
        .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_stdout));
    assert_eq!(parsed["status"], "valid", "JSON status should be valid");

    // Test with non-existent config file
    let invalid_path = Path::new("nonexistent-config.toml");
    let mut error_output = Vec::new();
    let result = siloctl::validate_config(&opts, &mut error_output, invalid_path).await;
    assert!(result.is_err(), "should fail for nonexistent config");

    let error_stdout = String::from_utf8(error_output)?;
    assert!(
        error_stdout.contains("Config error"),
        "output should show error: {}",
        error_stdout
    );

    Ok(())
}

// Unit tests for compute_midpoint

#[silo::test]
fn test_compute_midpoint_normal() {
    let mid = siloctl::compute_midpoint("a", "z").unwrap();
    assert!(mid.as_str() > "a", "midpoint should be > start");
    assert!(mid.as_str() < "z", "midpoint should be < end");
}

#[silo::test]
fn test_compute_midpoint_equal_strings() {
    assert!(siloctl::compute_midpoint("abc", "abc").is_none());
}

#[silo::test]
fn test_compute_midpoint_start_greater_than_end() {
    assert!(siloctl::compute_midpoint("z", "a").is_none());
}

#[silo::test]
fn test_compute_midpoint_prefix_case() {
    let mid = siloctl::compute_midpoint("abc", "abcdef").unwrap();
    assert!(mid.as_str() > "abc", "midpoint should be > start");
    assert!(mid.as_str() < "abcdef", "midpoint should be < end");
}

#[silo::test]
fn test_compute_midpoint_adjacent_chars() {
    // 'a' and 'b' are adjacent, so midpoint extends with '~'
    let mid = siloctl::compute_midpoint("a", "b").unwrap();
    assert!(mid.as_str() > "a", "midpoint should be > start");
    assert!(mid.as_str() < "b", "midpoint should be < end");
}

#[silo::test]
fn test_compute_midpoint_same_prefix() {
    let mid = siloctl::compute_midpoint("hello", "world").unwrap();
    assert!(mid.as_str() > "hello", "midpoint should be > start");
    assert!(mid.as_str() < "world", "midpoint should be < end");
}

// ============================================================================
// Multi-shard routing tests
// ============================================================================

async fn setup_multi_shard_server(
    initial_shard_count: u32,
    config: AppConfig,
) -> anyhow::Result<(
    SiloClient<tonic::transport::Channel>,
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    SocketAddr,
    Arc<ShardFactory>,
    tempfile::TempDir,
)> {
    let tmp = tempfile::tempdir()?;
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: None,
    };
    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = Arc::new(ShardFactory::new(template, rate_limiter, None));

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let coordinator = Arc::new(
        NoneCoordinator::new(
            "test-node",
            format!("http://{}", addr),
            initial_shard_count,
            factory.clone(),
            Vec::new(),
        )
        .await
        .unwrap(),
    );

    let server = tokio::spawn(run_server(
        listener,
        factory.clone(),
        coordinator,
        config,
        None,
        shutdown_rx,
    ));

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let client = SiloClient::new(channel);

    Ok((client, shutdown_tx, server, addr, factory, tmp))
}

/// Test that siloctl commands auto-route to the correct shard owner
/// even when there are multiple shards in the cluster.
#[silo::test(flavor = "multi_thread")]
async fn siloctl_auto_routes_job_commands_multi_shard() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let cfg = AppConfig::load(None).unwrap();

        let (mut client, shutdown_tx, server, addr, _factory, _tmp) =
            setup_multi_shard_server(8, cfg).await?;

        // Discover topology
        let cluster_info = client
            .get_cluster_info(GetClusterInfoRequest {})
            .await?
            .into_inner();
        assert_eq!(cluster_info.shard_owners.len(), 8);

        // Enqueue a job on each shard (no tenant, so no range validation)
        for (i, owner) in cluster_info.shard_owners.iter().enumerate() {
            client
                .enqueue(EnqueueRequest {
                    shard: owner.shard_id.clone(),
                    id: format!("routing-test-job-{}", i),
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
        }

        let opts = opts_for_addr(&addr);

        // Use siloctl to get jobs from different shards -- auto-routing should work
        for (i, owner) in cluster_info.shard_owners.iter().enumerate() {
            let mut output = Vec::new();
            siloctl::job_get(
                &opts,
                &mut output,
                &owner.shard_id,
                &format!("routing-test-job-{}", i),
                false,
            )
            .await?;
            let stdout = String::from_utf8(output)?;
            assert!(
                stdout.contains(&format!("routing-test-job-{}", i)),
                "should find job on shard {}: {}",
                owner.shard_id,
                stdout
            );
        }

        // Use siloctl to query different shards
        for owner in &cluster_info.shard_owners {
            let mut output = Vec::new();
            siloctl::query(
                &opts,
                &mut output,
                &owner.shard_id,
                "SELECT id FROM jobs LIMIT 5",
            )
            .await?;
            let stdout = String::from_utf8(output)?;
            assert!(
                stdout.contains("row(s) returned"),
                "query should succeed on shard {}: {}",
                owner.shard_id,
                stdout
            );
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that cluster info output includes range information.
#[silo::test(flavor = "multi_thread")]
async fn siloctl_cluster_info_shows_ranges() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let cfg = AppConfig::load(None).unwrap();

        let (_client, shutdown_tx, server, addr, _factory, _tmp) =
            setup_multi_shard_server(4, cfg).await?;

        // Test human-readable output includes ranges
        let opts = opts_for_addr(&addr);
        let mut output = Vec::new();
        siloctl::cluster_info(&opts, &mut output).await?;
        let stdout = String::from_utf8(output)?;

        assert!(
            stdout.contains("Range"),
            "output should have Range column header: {}",
            stdout
        );

        // Test JSON output includes range_start and range_end
        let opts_json = opts_for_addr_json(&addr);
        let mut json_output = Vec::new();
        siloctl::cluster_info(&opts_json, &mut json_output).await?;
        let json_stdout = String::from_utf8(json_output)?;

        let parsed: serde_json::Value = serde_json::from_str(&json_stdout)
            .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_stdout));
        let shard_owners = parsed["shard_owners"].as_array().unwrap();
        assert_eq!(shard_owners.len(), 4);
        for owner in shard_owners {
            assert!(
                owner.get("range_start").is_some(),
                "JSON shard owner should have range_start"
            );
            assert!(
                owner.get("range_end").is_some(),
                "JSON shard owner should have range_end"
            );
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that shard split-status auto-routes to the correct node.
#[silo::test(flavor = "multi_thread")]
async fn siloctl_shard_split_status_auto_routes() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let cfg = AppConfig::load(None).unwrap();

        let (mut client, shutdown_tx, server, addr, _factory, _tmp) =
            setup_multi_shard_server(4, cfg).await?;

        // Discover topology
        let cluster_info = client
            .get_cluster_info(GetClusterInfoRequest {})
            .await?
            .into_inner();

        let opts = opts_for_addr(&addr);

        // Check split status on each shard -- all should succeed (no split in progress)
        for owner in &cluster_info.shard_owners {
            let mut output = Vec::new();
            siloctl::shard_split_status(&opts, &mut output, &owner.shard_id).await?;
            let stdout = String::from_utf8(output)?;
            assert!(
                stdout.contains("No split in progress"),
                "split status should work for shard {}: {}",
                owner.shard_id,
                stdout
            );
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that connect_to_shard_owner returns a clear error for unknown shards.
#[silo::test(flavor = "multi_thread")]
async fn siloctl_unknown_shard_returns_clear_error() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let cfg = AppConfig::load(None).unwrap();

        let (_client, shutdown_tx, server, addr, _factory, _tmp) =
            setup_multi_shard_server(4, cfg).await?;

        let opts = opts_for_addr(&addr);

        // Try to get a job from a non-existent shard
        let mut output = Vec::new();
        let result = siloctl::job_get(
            &opts,
            &mut output,
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "some-job",
            false,
        )
        .await;

        assert!(result.is_err(), "should fail for non-existent shard");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in cluster"),
            "error should indicate shard not found in cluster: {}",
            err
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
