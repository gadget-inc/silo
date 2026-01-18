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
            let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "index": i })).unwrap();
            let mut md = std::collections::HashMap::new();
            md.insert("env".to_string(), "test".to_string());
            md.insert("batch".to_string(), "batch1".to_string());
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("job{}", i),
                priority: (10 + i) as u32,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: payload_bytes,
                }),
                limits: vec![],
                tenant: None,
                metadata: md,
            task_group: "default".to_string(),
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

        // Verify rows are returned as MessagePack
        let first_row: serde_json::Value = rmp_serde::from_slice(&query_resp.rows[0].data)?;
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
        let count_row: serde_json::Value = rmp_serde::from_slice(&query_resp.rows[0].data)?;
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
        let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "test": "data" })).unwrap();
        let enq = EnqueueRequest {
            shard: 0,
            id: "test_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: payload_bytes,
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
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
        let payload_bytes = rmp_serde::to_vec(&payload)?;
        let mut md = std::collections::HashMap::new();
        md.insert("user_id".to_string(), "12345".to_string());
        md.insert("region".to_string(), "us-west".to_string());

        let enq = EnqueueRequest {
            shard: 0,
            id: "complex_job".to_string(),
            priority: 5,
            start_at_ms: 1234567890,
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: payload_bytes,
            }),
            limits: vec![],
            tenant: None,
            metadata: md,
            task_group: "default".to_string(),
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
        let row: serde_json::Value = rmp_serde::from_slice(&query_resp.rows[0].data)?;
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
        let empty_payload = rmp_serde::to_vec(&serde_json::json!({})).unwrap();
        for i in 0..3 {
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("job{}", i),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: empty_payload.clone(),
                }),
                limits: vec![],
                tenant: None, // Will use default tenant
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
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

/// Test that MessagePack serialization handles all Arrow data types correctly.
/// This exercises the streaming serializer's type-specific encoding paths.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_msgpack_data_types() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Enqueue jobs with different priorities and start times to test numeric types
        for i in 0..3 {
            let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "value": i * 100 })).unwrap();
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("type_test_{}", i),
                priority: (i * 50) as u32, // 0, 50, 100
                start_at_ms: 1000000000 + (i as i64 * 1000), // Different timestamps
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: payload_bytes,
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            };
            let _ = client.enqueue(enq).await?;
        }

        // Query with various computed columns to exercise different types
        // Columns: shard_id (UInt32), tenant (Utf8), id (Utf8), priority (UInt8),
        //          enqueue_time_ms (Int64), payload (Utf8), status_kind (Utf8),
        //          status_changed_at_ms (Int64), metadata (Map)
        let query_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: r#"
                    SELECT 
                        id,
                        shard_id,
                        priority,
                        enqueue_time_ms,
                        status_changed_at_ms,
                        priority > 25 as is_high_priority,
                        CAST(priority AS DOUBLE) / 100.0 as priority_ratio,
                        CASE WHEN priority = 0 THEN NULL ELSE enqueue_time_ms END as nullable_time
                    FROM jobs 
                    ORDER BY id
                "#.to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 3, "expected 3 rows");

        // Deserialize all rows and verify types
        let rows: Vec<serde_json::Value> = query_resp
            .rows
            .iter()
            .map(|r| rmp_serde::from_slice(&r.data).unwrap())
            .collect();

        // Row 0: priority=0, enqueue_time_ms=1000000000
        assert_eq!(rows[0]["id"], "type_test_0");
        assert_eq!(rows[0]["shard_id"], 0); // UInt32
        assert_eq!(rows[0]["priority"], 0); // UInt8
        assert_eq!(rows[0]["enqueue_time_ms"], 1000000000_i64); // Int64
        assert_eq!(rows[0]["is_high_priority"], false); // Boolean false
        assert!((rows[0]["priority_ratio"].as_f64().unwrap() - 0.0).abs() < 0.001); // Float64
        assert!(rows[0]["nullable_time"].is_null()); // Null (because priority = 0)

        // Row 1: priority=50, enqueue_time_ms=1000001000
        assert_eq!(rows[1]["id"], "type_test_1");
        assert_eq!(rows[1]["priority"], 50);
        assert_eq!(rows[1]["enqueue_time_ms"], 1000001000_i64);
        assert_eq!(rows[1]["is_high_priority"], true); // Boolean true
        assert!((rows[1]["priority_ratio"].as_f64().unwrap() - 0.5).abs() < 0.001);
        assert!(!rows[1]["nullable_time"].is_null()); // Not null (priority != 0)

        // Row 2: priority=100, enqueue_time_ms=1000002000
        assert_eq!(rows[2]["id"], "type_test_2");
        assert_eq!(rows[2]["priority"], 100);
        assert_eq!(rows[2]["enqueue_time_ms"], 1000002000_i64);
        assert!((rows[2]["priority_ratio"].as_f64().unwrap() - 1.0).abs() < 0.001);

        // Test aggregation with different result types
        let agg_resp = client
            .query(QueryRequest {
                shard: 0,
                sql: r#"
                    SELECT 
                        COUNT(*) as count_val,
                        SUM(priority) as sum_val,
                        AVG(priority) as avg_val,
                        MIN(priority) as min_val,
                        MAX(priority) as max_val
                    FROM jobs
                "#.to_string(),
                tenant: None,
            })
            .await?
            .into_inner();

        assert_eq!(agg_resp.row_count, 1);
        let agg_row: serde_json::Value = rmp_serde::from_slice(&agg_resp.rows[0].data)?;

        assert_eq!(agg_row["count_val"], 3); // COUNT returns Int64
        assert_eq!(agg_row["sum_val"], 150); // SUM of 0+50+100
        assert!((agg_row["avg_val"].as_f64().unwrap() - 50.0).abs() < 0.001); // AVG returns Float64
        assert_eq!(agg_row["min_val"], 0);
        assert_eq!(agg_row["max_val"], 100);

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
        let empty_payload = rmp_serde::to_vec(&serde_json::json!({})).unwrap();
        for i in 0..2 {
            let enq = EnqueueRequest {
                shard: 0,
                id: format!("arrow_job{}", i),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(MsgpackBytes {
                    data: empty_payload.clone(),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
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
