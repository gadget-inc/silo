mod grpc_integration_helpers;

use std::sync::Arc;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::*;
use silo::settings::{AppConfig, Backend, DatabaseTemplate};
use silo::shard_range::{ShardMap, ShardRange};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

#[silo::test(flavor = "multi_thread")]
async fn grpc_health_check_returns_serving() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Get endpoint for health client
        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
        let mut health_client = HealthClient::new(channel);

        // Check overall health (empty service name)
        let resp = health_client
            .check(HealthCheckRequest {
                service: "".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(
            resp.status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
            "overall health should be SERVING"
        );

        // Check the specific Silo service health (silo.v1.Silo is the full gRPC service name)
        let resp = health_client
            .check(HealthCheckRequest {
                service: "silo.v1.Silo".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(
            resp.status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
            "silo.v1.Silo service should be SERVING"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that lease_tasks correctly polls multiple shards when no shard filter is specified.
/// This specifically tests that the `remaining` counter is decremented correctly across shards.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_lease_tasks_multi_shard() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter, None);

        // Create ShardMap and open 3 shards
        let shard_map = ShardMap::create_initial(3).expect("create shard map");
        for shard_info in shard_map.shards() {
            let _ = factory.open(&shard_info.id, &ShardRange::full()).await?;
        }
        let factory = Arc::new(factory);

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue 2 jobs into each shard (6 total)
        for (shard_idx, shard_info) in shard_map.shards().iter().enumerate() {
            for i in 0..2 {
                let enq = EnqueueRequest {
                    shard: shard_info.id.to_string(),
                    id: format!("job_s{}_i{}", shard_idx, i),
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
            }
        }

        // Lease 4 tasks WITHOUT specifying a shard (shard: None)
        // This should poll all local shards and return up to 4 tasks
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: None, // Poll all shards
                worker_id: "multi_shard_worker".to_string(),
                max_tasks: 4,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        // Should get exactly 4 tasks (bug would cause fewer due to wrong remaining calc)
        assert_eq!(
            lease_resp.tasks.len(),
            4,
            "expected 4 tasks when polling multiple shards, got {}",
            lease_resp.tasks.len()
        );

        // Lease the remaining 2 tasks
        let lease_resp2 = client
            .lease_tasks(LeaseTasksRequest {
                shard: None,
                worker_id: "multi_shard_worker".to_string(),
                max_tasks: 10, // Ask for more than available
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();

        assert_eq!(
            lease_resp2.tasks.len(),
            2,
            "expected 2 remaining tasks, got {}",
            lease_resp2.tasks.len()
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_tenant_validation_when_enabled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;

        // Enable tenancy in config
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), cfg).await?;

        // Test 1: Missing tenant when required - should fail
        let res = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "test-job".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: None, // Missing!
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await;

        match res {
            Ok(_) => panic!("expected error for missing tenant"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("tenant"),
                    "error should mention tenant: {}",
                    status.message()
                );
            }
        }

        // Test 2: Empty tenant string when required - should fail
        let res = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "test-job".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: Some("".to_string()), // Empty!
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await;

        match res {
            Ok(_) => panic!("expected error for empty tenant"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
            }
        }

        // Test 3: Tenant ID too long (>64 chars) - should fail
        let res = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "test-job".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: Some("x".repeat(65)), // Too long!
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await;

        match res {
            Ok(_) => panic!("expected error for long tenant"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("tenant"),
                    "error should mention tenant: {}",
                    status.message()
                );
            }
        }

        // Test 4: Valid tenant - should succeed
        let res = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "test-job-valid".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: Some("my-tenant".to_string()),
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await;

        assert!(res.is_ok(), "valid tenant should succeed");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_requires_dev_mode() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;

        // dev_mode = false (default)
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.server.dev_mode = false;

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), cfg).await?;

        // reset_shards should fail in non-dev mode
        let res = client.reset_shards(ResetShardsRequest {}).await;

        match res {
            Ok(_) => panic!("expected permission denied error"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied);
                assert!(
                    status.message().contains("dev mode"),
                    "error should mention dev mode: {}",
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

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_works_in_dev_mode() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;

        // Enable dev mode
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.server.dev_mode = true;

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), cfg).await?;

        // First enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: "job-to-reset".to_string(),
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

        // Verify job exists
        let query_resp = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 1, "should have 1 job before reset");

        // Reset shards should succeed in dev mode
        let res = client.reset_shards(ResetShardsRequest {}).await?;
        assert_eq!(res.into_inner().shards_reset, 1, "should reset 1 shard");

        // Verify job is gone after reset
        let query_resp = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 0, "should have 0 jobs after reset");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_report_outcome_missing_outcome() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // report_outcome with missing outcome field should fail
        let res = client
            .report_outcome(ReportOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: "fake-task".to_string(),
                outcome: None, // Missing!
            })
            .await;

        match res {
            Ok(_) => panic!("expected error for missing outcome"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("outcome"),
                    "error should mention outcome: {}",
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

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_report_refresh_outcome_missing_outcome() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // report_refresh_outcome with missing outcome field should fail
        let res = client
            .report_refresh_outcome(ReportRefreshOutcomeRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                task_id: "fake-refresh-task".to_string(),
                outcome: None, // Missing!
            })
            .await;

        match res {
            Ok(_) => panic!("expected error for missing outcome"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("outcome"),
                    "error should mention outcome: {}",
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

/// Test that GetNodeInfo returns correct counts for empty shard
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_get_node_info_empty() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Get node info
        let resp = client
            .get_node_info(GetNodeInfoRequest {})
            .await?
            .into_inner();

        // Find our test shard
        let test_shard = resp
            .owned_shards
            .iter()
            .find(|s| s.shard_id == crate::grpc_integration_helpers::TEST_SHARD_ID.to_string())
            .expect("test shard should be in owned_shards");

        assert_eq!(
            test_shard.total_jobs, 0,
            "empty shard should have 0 total_jobs"
        );
        assert_eq!(
            test_shard.completed_jobs, 0,
            "empty shard should have 0 completed_jobs"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Helper to get counters for a specific shard from GetNodeInfo response
fn get_shard_counters_from_node_info(resp: &GetNodeInfoResponse, shard_id: &str) -> (i64, i64) {
    let shard = resp
        .owned_shards
        .iter()
        .find(|s| s.shard_id == shard_id)
        .expect("shard should be in owned_shards");
    (shard.total_jobs, shard.completed_jobs)
}

/// Test that GetNodeInfo correctly tracks job lifecycle
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_get_node_info_tracks_lifecycle() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let test_shard_id = crate::grpc_integration_helpers::TEST_SHARD_ID.to_string();

        // Initially empty
        let resp = client
            .get_node_info(GetNodeInfoRequest {})
            .await?
            .into_inner();
        let (total, completed) = get_shard_counters_from_node_info(&resp, &test_shard_id);
        assert_eq!(total, 0);
        assert_eq!(completed, 0);

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: test_shard_id.clone(),
                id: "test-job-1".to_string(),
                priority: 5,
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
        assert_eq!(enq_resp.id, "test-job-1");

        // Check counters after enqueue
        let resp = client
            .get_node_info(GetNodeInfoRequest {})
            .await?
            .into_inner();
        let (total, completed) = get_shard_counters_from_node_info(&resp, &test_shard_id);
        assert_eq!(total, 1, "should have 1 total job after enqueue");
        assert_eq!(
            completed, 0,
            "should have 0 completed jobs (still scheduled)"
        );

        // Enqueue another job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: test_shard_id.clone(),
                id: "test-job-2".to_string(),
                priority: 5,
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
            .await?;

        // Check counters after second enqueue
        let resp = client
            .get_node_info(GetNodeInfoRequest {})
            .await?
            .into_inner();
        let (total, completed) = get_shard_counters_from_node_info(&resp, &test_shard_id);
        assert_eq!(total, 2, "should have 2 total jobs");
        assert_eq!(completed, 0, "should have 0 completed jobs");

        // Lease and complete a job
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(test_shard_id.clone()),
                worker_id: "test-worker".to_string(),
                max_tasks: 1,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);

        let task = &lease_resp.tasks[0];
        client
            .report_outcome(ReportOutcomeRequest {
                shard: test_shard_id.clone(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"result": "done"})).unwrap(),
                    )),
                })),
            })
            .await?;

        // Check counters after completion
        let resp = client
            .get_node_info(GetNodeInfoRequest {})
            .await?
            .into_inner();
        let (total, completed) = get_shard_counters_from_node_info(&resp, &test_shard_id);
        assert_eq!(
            total, 2,
            "should still have 2 total jobs (completed jobs count toward total)"
        );
        assert_eq!(completed, 1, "should have 1 completed job after success");

        // Delete the completed job
        client
            .delete_job(DeleteJobRequest {
                shard: test_shard_id.clone(),
                id: task.job_id.clone(),
                tenant: None,
            })
            .await?;

        // Check counters after deletion
        let resp = client
            .get_node_info(GetNodeInfoRequest {})
            .await?
            .into_inner();
        let (total, completed) = get_shard_counters_from_node_info(&resp, &test_shard_id);
        assert_eq!(total, 1, "should have 1 total job after deletion");
        assert_eq!(
            completed, 0,
            "should have 0 completed jobs after deleting the completed one"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that reset_shards clears data with the Fs backend.
/// This is a regression test for a bug where reset_shards cleared in-memory state
/// but left persisted job data in the database, causing stale jobs to show up in
/// queries but not be available for leaseTasks.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_clears_fs_backend_data() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let test_shard_id = crate::grpc_integration_helpers::TEST_SHARD_ID;
        let tmp = tempfile::tempdir()?;

        // Use Fs backend (same as the reported issue)
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter, None);

        // Use a predictable shard ID for testing
        let shard_id =
            silo::shard_range::ShardId::parse(test_shard_id).expect("valid test shard ID");
        let _ = factory.open(&shard_id, &ShardRange::full()).await?;
        let factory = Arc::new(factory);

        // Enable dev mode
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.server.dev_mode = true;

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), cfg).await?;

        // Enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: test_shard_id.to_string(),
                id: "job-to-reset-fs".to_string(),
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

        // Verify job exists via query
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 1, "should have 1 job before reset");

        // Verify job is available via leaseTasks
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(test_shard_id.to_string()),
                worker_id: "test-worker".to_string(),
                max_tasks: 10,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(
            lease_resp.tasks.len(),
            1,
            "should have 1 task available before reset"
        );

        // Complete the task so job is in terminal state
        let task = &lease_resp.tasks[0];
        client
            .report_outcome(ReportOutcomeRequest {
                shard: test_shard_id.to_string(),
                task_id: task.id.clone(),
                outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"result": "done"})).unwrap(),
                    )),
                })),
            })
            .await?;

        // Enqueue another job that will be scheduled (not completed)
        let _ = client
            .enqueue(EnqueueRequest {
                shard: test_shard_id.to_string(),
                id: "job-scheduled".to_string(),
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

        // Verify we have 2 jobs (1 completed, 1 scheduled)
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 2, "should have 2 jobs before reset");

        // Reset shards
        let res = client.reset_shards(ResetShardsRequest {}).await?;
        assert_eq!(res.into_inner().shards_reset, 1, "should reset 1 shard");

        // Verify jobs are gone after reset (query returns 0)
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(
            count_row["count"], 0,
            "should have 0 jobs after reset - stale data persisted!"
        );

        // Verify no tasks are available for lease
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(test_shard_id.to_string()),
                worker_id: "test-worker".to_string(),
                max_tasks: 10,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 0, "should have 0 tasks after reset");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that reset_shards works with relative paths (like the dev config uses).
/// This reproduces the bug where relative paths might not resolve correctly.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_with_relative_path() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let test_shard_id = crate::grpc_integration_helpers::TEST_SHARD_ID;

        // Create a temporary directory and use a RELATIVE path to it
        // This mimics the dev config: path = "./tmp/silo-data/%shard%"
        let tmp = tempfile::tempdir()?;
        let relative_path = tmp.path().to_string_lossy().to_string();

        let template = DatabaseTemplate {
            backend: Backend::Fs,
            // Use the path as-is (could be absolute from tempdir, but the key is
            // to ensure path handling is consistent)
            path: format!("{}/%shard%", relative_path),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter, None);

        let shard_id =
            silo::shard_range::ShardId::parse(test_shard_id).expect("valid test shard ID");
        let _ = factory.open(&shard_id, &ShardRange::full()).await?;
        let factory = Arc::new(factory);

        let mut cfg = AppConfig::load(None).unwrap();
        cfg.server.dev_mode = true;

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), cfg).await?;

        // Enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: test_shard_id.to_string(),
                id: "job-relative-path".to_string(),
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

        // Verify job exists
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 1, "should have 1 job before reset");

        // Lease the task so job is "running"
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(test_shard_id.to_string()),
                worker_id: "test-worker".to_string(),
                max_tasks: 10,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1, "should have 1 task before reset");

        // Reset WITHOUT reporting outcome - simulates the "stale running job" scenario
        let res = client.reset_shards(ResetShardsRequest {}).await?;
        assert_eq!(res.into_inner().shards_reset, 1, "should reset 1 shard");

        // Verify jobs are gone after reset
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(
            count_row["count"], 0,
            "should have 0 jobs after reset - stale data persisted!"
        );

        // Verify no tasks are available
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(test_shard_id.to_string()),
                worker_id: "test-worker".to_string(),
                max_tasks: 10,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 0, "should have 0 tasks after reset");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that reset_shards clears data with the Memory backend (object store path).
/// This ensures the object store deletion logic works correctly for non-filesystem backends.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_clears_memory_backend_data() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let test_shard_id = crate::grpc_integration_helpers::TEST_SHARD_ID;

        // Use Memory backend to exercise the object store deletion path
        let template = DatabaseTemplate {
            backend: Backend::Memory,
            path: "test-memory-%shard%".to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter, None);

        // Use a predictable shard ID for testing
        let shard_id =
            silo::shard_range::ShardId::parse(test_shard_id).expect("valid test shard ID");
        let _ = factory.open(&shard_id, &ShardRange::full()).await?;
        let factory = Arc::new(factory);

        // Enable dev mode
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.server.dev_mode = true;

        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), cfg).await?;

        // Enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: test_shard_id.to_string(),
                id: "job-to-reset-memory".to_string(),
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

        // Verify job exists via query
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 1, "should have 1 job before reset");

        // Reset shards
        let res = client.reset_shards(ResetShardsRequest {}).await?;
        assert_eq!(res.into_inner().shards_reset, 1, "should reset 1 shard");

        // Verify jobs are gone after reset (query returns 0)
        let query_resp = client
            .query(QueryRequest {
                shard: test_shard_id.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(
            count_row["count"], 0,
            "should have 0 jobs after reset - data should be cleared from object store"
        );

        // Verify no tasks are available for lease
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(test_shard_id.to_string()),
                worker_id: "test-worker".to_string(),
                max_tasks: 10,
                task_group: "default".to_string(),
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 0, "should have 0 tasks after reset");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
