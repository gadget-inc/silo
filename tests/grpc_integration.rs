use std::net::SocketAddr;
use std::sync::Arc;

use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::server::run_server;
use silo::settings::{Backend, DatabaseTemplate};
use tokio::net::TcpListener;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

// Integration test that boots the real gRPC server and talks to it over TCP.
#[silo::test(flavor = "multi_thread")] // multi_thread to match server expectations
async fn grpc_server_enqueue_and_workflow() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        // Temp storage per test
        let tmp = tempfile::tempdir()?;

        // Create a shard factory with a path template pointing inside tmpdir
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        // Open shard 0 so the server can find it
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        // Bind an ephemeral port
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;

        // Shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Start server
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        // Connect real client
        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let payload = serde_json::json!({ "hello": "world" });
        let payload_bytes = serde_json::to_vec(&payload)?;
        let mut md = std::collections::HashMap::new();
        md.insert("env".to_string(), "test".to_string());
        let enq = EnqueueRequest {
            shard: 0,
            id: "".to_string(),
            priority: 50,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
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
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"{}".to_vec(),
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
            })
            .await?;
        // Verify metadata via get_job
        let got = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
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

        // Shut down server gracefully
        let _ = shutdown_tx.send(());
        // server is JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>
        // Await join, then unwrap inner result explicitly to avoid From conversion issues
        let join_result = server.await; // Result<Result<(), Box<dyn ..>>, JoinError>
        match join_result {
            Ok(inner) => {
                // Propagate inner error if server returned Err
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!(e));
            }
        }

        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")] // multi_thread to match server expectations
async fn grpc_server_metadata_validation_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Helper to assert error contains substring
        async fn expect_invalid_arg(
            client: &mut SiloClient<tonic::transport::Channel>,
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
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
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
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
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
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "value too long").await;
        }

        // 4) Non-default priority without concurrency limit
        {
            let req = EnqueueRequest {
                shard: 0,
                id: "".to_string(),
                priority: 10, // Non-default priority
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![], // No concurrency limit
                tenant: None,
                metadata: std::collections::HashMap::new(),
            };
            expect_invalid_arg(&mut client, req, "priority requires a concurrency limit").await;
        }

        // 5) Non-default priority WITH concurrency limit should succeed
        {
            let req = EnqueueRequest {
                shard: 0,
                id: "".to_string(),
                priority: 10, // Non-default priority
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![Limit {
                    limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                        key: "test-queue".to_string(),
                        max_concurrency: 10,
                    })),
                }],
                tenant: None,
                metadata: std::collections::HashMap::new(),
            };
            let res = client.enqueue(req).await;
            assert!(res.is_ok(), "priority with concurrency limit should succeed");
        }

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_basic() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a few jobs with metadata
        for i in 0..3 {
            let payload = serde_json::json!({ "index": i });
            let payload_bytes = serde_json::to_vec(&payload)?;
            let mut md = std::collections::HashMap::new();
            md.insert("env".to_string(), "test".to_string());
            md.insert("batch".to_string(), "batch1".to_string());
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("job{}", i),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: payload_bytes,
                }),
                limits: vec![],
                tenant: None,
                metadata: md,
            };
            let _ = client.enqueue(enq).await?;
        }

        // Test 1: Basic SELECT query
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT * FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 3, "expected 3 rows");
        assert!(
            query_resp.columns.iter().any(|c| c.name == "id"),
            "expected id column"
        );
        assert!(
            query_resp.columns.iter().any(|c| c.name == "priority"),
            "expected priority column"
        );

        // Verify rows are returned as JSON
        let first_row: serde_json::Value = serde_json::from_slice(&query_resp.rows[0].data)?;
        assert!(first_row.get("id").is_some(), "row should have id field");

        // Test 2: Query with WHERE clause
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT id FROM jobs WHERE priority >= 10".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(
            query_resp.row_count, 3,
            "expected 3 rows with priority >= 10"
        );

        // Test 3: Aggregation query
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 1, "aggregation should return 1 row");
        let count_row: serde_json::Value = serde_json::from_slice(&query_resp.rows[0].data)?;
        assert_eq!(count_row["count"], 3, "count should be 3");

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Test 1: SQL syntax error
        let res = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT FROM WHERE".to_string(),
                tenant: None,
            })
            .await;

        match res {
            Ok(_) => panic!("expected SQL error"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert!(
                    status.message().contains("SQL error"),
                    "error message should mention SQL error: {}",
                    status.message()
                );
            }
        }

        // Test 2: Invalid column name
        let res = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT nonexistent_column FROM jobs".to_string(),
                tenant: None,
            })
            .await;

        match res {
            Ok(_) => panic!("expected column error"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
            }
        }

        // Test 3: Shard not found
        let res = client
            .query(QueryRequest {
                shard: 999,
                sql: "SELECT * FROM jobs".to_string(),
                tenant: None,
            })
            .await;

        match res {
            Ok(_) => panic!("expected shard not found error"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::NotFound);
            }
        }

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_empty_results() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Query on empty shard
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT * FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 0, "expected 0 rows on empty shard");
        assert!(!query_resp.columns.is_empty(), "should still have schema");
        assert!(query_resp.rows.is_empty(), "should have no rows");

        // Add a job, then query with WHERE that matches nothing
        let payload = serde_json::json!({ "test": "data" });
        let payload_bytes = serde_json::to_vec(&payload)?;
        let enq = EnqueueRequest {
            shard: 0,
            id: "test_job".to_string(),
            priority: 50,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: payload_bytes,
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
        };
        let _ = client.enqueue(enq).await?;

        // Query that doesn't match
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT * FROM jobs WHERE id = 'nonexistent'".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(
            query_resp.row_count, 0,
            "expected 0 rows for nonexistent id"
        );
        assert!(!query_resp.columns.is_empty(), "should still have schema");

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_typescript_friendly() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job with complex metadata
        let payload = serde_json::json!({
            "task": "process",
            "nested": { "field": "value" }
        });
        let payload_bytes = serde_json::to_vec(&payload)?;
        let mut md = std::collections::HashMap::new();
        md.insert("user_id".to_string(), "12345".to_string());
        md.insert("region".to_string(), "us-west".to_string());

        let enq = EnqueueRequest {
            shard: 0,
            id: "complex_job".to_string(),
            priority: 50,
            start_at_ms: 1234567890,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: payload_bytes,
            }),
            limits: vec![],
            tenant: None,
            metadata: md,
        };
        let _ = client.enqueue(enq).await?;

        // Query with multiple columns to verify schema
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT id, priority, enqueue_time_ms, payload FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        // Verify schema includes data types (helpful for TypeScript codegen)
        assert!(query_resp
            .columns
            .iter()
            .any(|c| c.name == "id" && c.data_type.contains("Utf8")));
        assert!(query_resp
            .columns
            .iter()
            .any(|c| c.name == "priority" && c.data_type.contains("UInt8")));
        assert!(query_resp
            .columns
            .iter()
            .any(|c| c.name == "enqueue_time_ms" && c.data_type.contains("Int64")));

        // Verify row is valid JSON that TypeScript can deserialize
        let row: serde_json::Value = serde_json::from_slice(&query_resp.rows[0].data)?;
        assert_eq!(row["id"], "complex_job");
        assert_eq!(row["priority"], 50);
        assert_eq!(row["enqueue_time_ms"], 1234567890);

        // Verify payload is a JSON string that can be parsed again
        let payload_str = row["payload"].as_str().expect("payload should be string");
        let parsed_payload: serde_json::Value = serde_json::from_str(payload_str)?;
        assert_eq!(parsed_payload["task"], "process");
        assert_eq!(parsed_payload["nested"]["field"], "value");

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that the Query endpoint doesn't require a tenant parameter and can query all data.
/// This validates that admin/operator queries work without tenant restrictions.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_without_tenant() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue jobs via gRPC (will use default tenant "-")
        for i in 0..3 {
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("job{}", i),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: None, // Will use default tenant
                metadata: std::collections::HashMap::new(),
            };
            let _ = client.enqueue(enq).await?;
        }

        // Query without specifying tenant - should work and return all jobs
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT id FROM jobs ORDER BY id".to_string(),
                tenant: None, // No tenant required for query
            })
            .await?
            .into_inner();

        // Should get all 3 jobs
        assert_eq!(
            query_resp.row_count, 3,
            "expected 3 rows from query without tenant"
        );

        // Can also query with SQL tenant filter
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(
            query_resp.row_count, 3,
            "expected 3 rows with tenant filter in SQL"
        );

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that QueryArrow endpoint works without tenant parameter.
/// This is the streaming Arrow IPC endpoint used by ClusterQueryEngine for remote shard queries.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_arrow_without_tenant() -> anyhow::Result<()> {
    use tokio_stream::StreamExt;

    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue jobs via gRPC
        for i in 0..2 {
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("arrow_job{}", i),
                priority: 50,
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
        }

        // Use QueryArrow without tenant parameter
        let response = client
            .query_arrow(QueryArrowRequest {
                shard: 0,
                sql: "SELECT id FROM jobs ORDER BY id".to_string(),
                tenant: None, // No tenant required
            })
            .await?;

        let mut stream = response.into_inner();
        let mut total_messages = 0;

        while let Some(msg) = stream.next().await {
            let _arrow_msg = msg?;
            total_messages += 1;
        }

        // Should have received at least one Arrow IPC message with the results
        assert!(
            total_messages >= 1,
            "expected at least one Arrow IPC message"
        );

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_health_check_returns_serving() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
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
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);

        // Open 3 shards
        let _ = factory.open(0).await?;
        let _ = factory.open(1).await?;
        let _ = factory.open(2).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue 2 jobs into each shard (6 total)
        for shard in 0..3 {
            for i in 0..2 {
                let enq = EnqueueRequest {
                    shard,
                    id: format!("job_s{}_i{}", shard, i),
                    priority: 50,
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
            }
        }

        // Lease 4 tasks WITHOUT specifying a shard (shard: None)
        // This should poll all local shards and return up to 4 tasks
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: None, // Poll all shards
                worker_id: "multi_shard_worker".to_string(),
                max_tasks: 4,
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
            })
            .await?
            .into_inner();

        assert_eq!(
            lease_resp2.tasks.len(),
            2,
            "expected 2 remaining tasks, got {}",
            lease_resp2.tasks.len()
        );

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that GetJob returns status information correctly
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_includes_status() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "status_test_job".to_string(),
            priority: 50,
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
            })
            .await?
            .into_inner();

        assert_eq!(job.status, JobStatus::Succeeded as i32);

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns NOT_FOUND for non-existent job
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_not_found() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns FAILED_PRECONDITION for scheduled/running jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_not_terminal() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "scheduled_job".to_string(),
            priority: 50,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns success data for succeeded jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_success() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "success_job".to_string(),
            priority: 50,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns failure info for failed jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_failure() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job with no retries
        let enq = EnqueueRequest {
            shard: 0,
            id: "failed_job".to_string(),
            priority: 50,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult returns cancelled info for cancelled jobs
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_cancelled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "cancelled_job".to_string(),
            priority: 50,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test GetJobResult for a running job that gets cancelled
#[silo::test(flavor = "multi_thread")]
async fn grpc_get_job_result_cancelled_while_running() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq = EnqueueRequest {
            shard: 0,
            id: "cancel_running_job".to_string(),
            priority: 50,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_tenant_validation_when_enabled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Enable tenancy in config
        let mut cfg = silo::settings::AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            cfg,
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Test 1: Missing tenant when required - should fail
        let res = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "test-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: None, // Missing!
                metadata: std::collections::HashMap::new(),
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
                shard: 0,
                id: "test-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: Some("".to_string()), // Empty!
                metadata: std::collections::HashMap::new(),
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
                shard: 0,
                id: "test-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: Some("x".repeat(65)), // Too long!
                metadata: std::collections::HashMap::new(),
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
                shard: 0,
                id: "test-job-valid".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: Some("my-tenant".to_string()),
                metadata: std::collections::HashMap::new(),
            })
            .await;

        assert!(res.is_ok(), "valid tenant should succeed");

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_requires_dev_mode() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // dev_mode = false (default)
        let mut cfg = silo::settings::AppConfig::load(None).unwrap();
        cfg.server.dev_mode = false;

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            cfg,
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_reset_shards_works_in_dev_mode() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Enable dev mode
        let mut cfg = silo::settings::AppConfig::load(None).unwrap();
        cfg.server.dev_mode = true;

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            cfg,
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // First enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "job-to-reset".to_string(),
                priority: 50,
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

        // Verify job exists
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value = serde_json::from_slice(&query_resp.rows[0].data)?;
        assert_eq!(count_row["count"], 1, "should have 1 job before reset");

        // Reset shards should succeed in dev mode
        let res = client.reset_shards(ResetShardsRequest {}).await?;
        assert_eq!(res.into_inner().shards_reset, 1, "should reset 1 shard");

        // Verify job is gone after reset
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
            })
            .await?
            .into_inner();
        let count_row: serde_json::Value = serde_json::from_slice(&query_resp.rows[0].data)?;
        assert_eq!(count_row["count"], 0, "should have 0 jobs after reset");

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_report_outcome_missing_outcome() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // report_outcome with missing outcome field should fail
        let res = client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: "fake-task".to_string(),
                tenant: None,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_get_job_result_for_non_terminal_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let _ = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "non-terminal-job".to_string(),
                priority: 50,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_enqueue_with_rate_limit() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job with a rate limit
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "rate-limited-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_report_refresh_outcome_missing_outcome() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // report_refresh_outcome with missing outcome field should fail
        let res = client
            .report_refresh_outcome(ReportRefreshOutcomeRequest {
                shard: 0,
                task_id: "fake-refresh-task".to_string(),
                tenant: None,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that enqueue fails with a clear error when tenancy is enabled but no tenant is specified
#[silo::test(flavor = "multi_thread")]
async fn grpc_enqueue_requires_tenant_when_tenancy_enabled() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Create a config with tenancy enabled
        let mut config = silo::settings::AppConfig::load(None).unwrap();
        config.tenancy.enabled = true;

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            config,
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Test 1: Enqueue without tenant (None)
        let req = EnqueueRequest {
            shard: 0,
            id: "".to_string(),
            priority: 50,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
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
            priority: 50,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
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
            priority: 50,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: b"{}".to_vec(),
            }),
            limits: vec![],
            tenant: Some("my-tenant".to_string()),
            metadata: std::collections::HashMap::new(),
        };
        let res = client.enqueue(req).await;
        assert!(res.is_ok(), "enqueue with valid tenant should succeed");

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

// =============================================================================
// RestartJob gRPC Integration Tests
// =============================================================================

/// Test restarting a cancelled job via gRPC
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_cancelled_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "restart-test-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{\"test\": true}".to_vec(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
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
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

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
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Cancelled as i32);

        // Restart the job
        client
            .restart_job(RestartJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Verify job is scheduled again
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        // Lease and complete the restarted job
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"\"done\"".to_vec(),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Succeeded as i32);

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restarting a failed job via gRPC
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_failed_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job without retry policy (will fail permanently on error)
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "restart-failed-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{\"test\": true}".to_vec(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease and fail the job
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Failure(Failure {
                    code: "TEST_ERROR".to_string(),
                    data: b"test failure".to_vec(),
                })),
            })
            .await?;

        // Verify job is failed
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Failed as i32);

        // Restart the failed job
        client
            .restart_job(RestartJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Verify job is scheduled again
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Scheduled as i32);

        // Complete the restarted job successfully
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-2".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1);
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"\"success after restart\"".to_vec(),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Succeeded as i32);

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restart returns NOT_FOUND for non-existent job
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_nonexistent_job() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Try to restart non-existent job
        let result = client
            .restart_job(RestartJobRequest {
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restart returns FAILED_PRECONDITION for running job
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_running_job_fails() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "running-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease the job to make it Running
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
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
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Running as i32);

        // Try to restart - should fail
        let result = client
            .restart_job(RestartJobRequest {
                shard: 0,
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
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"{}".to_vec(),
                })),
            })
            .await?;

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test restart returns FAILED_PRECONDITION for succeeded job
#[silo::test(flavor = "multi_thread")]
async fn grpc_restart_succeeded_job_fails() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        };
        let rate_limiter = MockGubernatorClient::new_arc();
        let factory = ShardFactory::new(template, rate_limiter);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let server = tokio::spawn(run_server(
            listener,
            factory.clone(),
            None,
            silo::settings::AppConfig::load(None).unwrap(),
            shutdown_rx,
        ));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue and complete a job
        let enq_resp = client
            .enqueue(EnqueueRequest {
                shard: 0,
                id: "succeeded-job".to_string(),
                priority: 50,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
            })
            .await?
            .into_inner();
        let job_id = enq_resp.id;

        // Lease and complete
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: Some(0),
                worker_id: "worker-1".to_string(),
                max_tasks: 1,
            })
            .await?
            .into_inner();
        let task = &lease_resp.tasks[0];

        client
            .report_outcome(ReportOutcomeRequest {
                shard: 0,
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"{}".to_vec(),
                })),
            })
            .await?;

        // Verify job succeeded
        let job = client
            .get_job(GetJobRequest {
                shard: 0,
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(job.status, JobStatus::Succeeded as i32);

        // Try to restart - should fail
        let result = client
            .restart_job(RestartJobRequest {
                shard: 0,
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

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
