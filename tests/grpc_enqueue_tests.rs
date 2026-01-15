mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;

// Integration test that boots the real gRPC server and talks to it over TCP.
#[silo::test(flavor = "multi_thread")] // multi_thread to match server expectations
async fn grpc_server_enqueue_and_workflow() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job
        let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "hello": "world" })).unwrap();
        let mut md = std::collections::HashMap::new();
        md.insert("env".to_string(), "test".to_string());
        let enq = EnqueueRequest {
            shard: 0,
            id: "".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: payload_bytes.clone(),
            }),
            limits: vec![],
            tenant: None,
            metadata: md,
        };
        let enq_resp = client.enqueue(enq).await?.into_inner();
        let job_id = enq_resp.id;

        // Lease a task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "w1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1, "expected one leased task");
        let task = &lease_resp.tasks[0];
        assert_eq!(task.job_id, job_id);
        assert_eq!(
            task.payload.as_ref().map(|p| p.data.clone()),
            Some(payload_bytes.clone())
        );

        // Report success
        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                })),
            })
            .await?;

        // After success, deleting job should work or get_job should be not found eventually.
        // First try get_job to ensure job exists pre-delete
        let _ = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?;
        // Verify metadata via get_job
        let got = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();
        assert!(got.metadata.iter().any(|(k, v)| k == "env" && v == "test"));
        // Now delete it
        let _ = client
            .delete_job(DeleteJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")] // multi_thread to match server expectations
async fn grpc_server_metadata_validation_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Helper to assert error contains substring
        async fn expect_invalid_arg(
            client: &mut silo::pb::silo_client::SiloClient<tonic::transport::Channel>,
            req: EnqueueRequest,
            msg: &str,
        ) {
            let res = client.enqueue(req).await;
            match res {
                Ok(_) => panic!("expected error"),
                Err(status) => {
                    assert_eq!(status.code(), tonic::Code::InvalidArgument);
                    assert!(status.message().contains(msg), "got: {}", status.message());
                }
            }
        }

        // 1) Too many keys (>16)
        {
            let mut md = std::collections::HashMap::new();
            for i in 0..17 {
                md.insert(format!("k{}", i), "v".to_string());
            }
            let req = EnqueueRequest {
                shard: 0,
                id: "".to_string(),
                priority: 1,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                }),
                limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "too many entries").await;
        }

        // 2) Key too long (>=64 chars)
        {
            let mut md = std::collections::HashMap::new();
            let long_key = "x".repeat(64);
            md.insert(long_key, "v".to_string());
            let req = EnqueueRequest {
                shard: 0,
                id: "".to_string(),
                priority: 1,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                }),
                limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "key too long").await;
        }

        // 3) Value too long (>= u16::MAX)
        {
            let mut md = std::collections::HashMap::new();
            let long_val = vec![b'a'; u16::MAX as usize];
            md.insert(
                "k".to_string(),
                String::from_utf8(long_val).unwrap_or_default(),
            );
            let req = EnqueueRequest {
                shard: 0,
                id: "".to_string(),
                priority: 1,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                }),
                limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "value too long").await;
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_enqueue_with_rate_limit() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue a job with a rate limit
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "rate-limited-job".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                }),
                limits: vec![Limit {
                    limit: Some(limit::Limit::RateLimit(silo::pb::GubernatorRateLimit {
                        name: "my-rate-limit".to_string(),
                        unique_key: "user-123".to_string(),
                        limit: 100,
                        duration_ms: 60000,
                        hits: 1,
                        algorithm: 0, // TokenBucket
                        behavior: 0,
                        retry_policy: Some(silo::pb::RateLimitRetryPolicy {
                            initial_backoff_ms: 100,
                            max_backoff_ms: 5000,
                            backoff_multiplier: 2.0,
                            max_retries: 3,
                        }),
                    })),
                }],
                tenant: None,
                metadata: std::collections::HashMap::new(),
            })
            .await?
            .into_inner();

        assert!(!enq_resp.id.is_empty());

        // Verify job was created with rate limit via get_job
        let job_resp = client
            .get_job(GetJobRequest {
                shard: 0,
                id: "rate-limited-job".to_string(),
                tenant: None,
                include_attempts: false,
            })
            .await?
            .into_inner();

        assert_eq!(job_resp.limits.len(), 1);
        match &job_resp.limits[0].limit {
            Some(limit::Limit::RateLimit(rl)) => {
                assert_eq!(rl.name, "my-rate-limit");
                assert_eq!(rl.unique_key, "user-123");
                assert_eq!(rl.limit, 100);
            }
            _ => panic!("expected rate limit, got {:?}", job_resp.limits[0]),
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that enqueue fails with a clear error when tenancy is enabled but no tenant is specified
#[silo::test(flavor = "multi_thread")]
async fn grpc_enqueue_requires_tenant_when_tenancy_enabled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;

        // Create a config with tenancy enabled
        let mut config = AppConfig::load(None).unwrap();
        config.tenancy.enabled = true;

        let (mut client, shutdown_tx, server, _addr) = setup_test_server(factory.clone(), config).await?;

        // Test 1: Enqueue without tenant (None)
        let req = EnqueueRequest {
            shard: 0,
            id: "".to_string(),
            priority: 1,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let res = client.enqueue(req).await;
        match res {
            Ok(_) => panic!("expected error when tenant is missing with tenancy enabled"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("tenant") && status.message().contains("required"),
                    "error message should mention tenant is required, got: {}",
                    status.message()
                );
            }
        }

        // Test 2: Enqueue with empty tenant string
        let req = EnqueueRequest {
            shard: 0,
            id: "".to_string(),
            priority: 1,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
            }),
            limits: vec![],
            tenant: Some("".to_string()),
            metadata: std::collections::HashMap::new(),
        };
        let res = client.enqueue(req).await;
        match res {
            Ok(_) => panic!("expected error when tenant is empty with tenancy enabled"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("tenant") && status.message().contains("required"),
                    "error message should mention tenant is required for empty string, got: {}",
                    status.message()
                );
            }
        }

        // Test 3: Enqueue WITH a valid tenant should succeed
        let req = EnqueueRequest {
            shard: 0,
            id: "".to_string(),
            priority: 1,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
            }),
            limits: vec![],
            tenant: Some("my-tenant".to_string()),
            metadata: std::collections::HashMap::new(),
        };
        let res = client.enqueue(req).await;
        assert!(res.is_ok(), "enqueue with valid tenant should succeed");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
