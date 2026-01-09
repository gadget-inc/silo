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
            priority: 10,
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
                priority: 1,
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
                priority: 1,
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
                priority: 1,
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
                priority: (10 + i) as u32,
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
            priority: 10,
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
            priority: 5,
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
        assert_eq!(row["priority"], 5);
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
                priority: 10,
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
