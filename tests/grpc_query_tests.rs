mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::*;
use silo::settings::AppConfig;

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_basic() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_empty_results() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_typescript_friendly() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
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
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
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
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

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

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
