mod grpc_integration_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::settings::AppConfig;

fn config_with_statement_timeout_ms(timeout_ms: u64) -> AppConfig {
    let mut config = AppConfig::load(None).expect("load default config");
    config.server.statement_timeout_ms = Some(timeout_ms);
    config
}

fn query_param_string(value: &str) -> QueryParameter {
    QueryParameter {
        value: Some(query_parameter::Value::StringValue(value.to_string())),
    }
}

fn query_param_int64(value: i64) -> QueryParameter {
    QueryParameter {
        value: Some(query_parameter::Value::Int64Value(value)),
    }
}

async fn enqueue_jobs_for_heavy_query(
    client: &mut SiloClient<tonic::transport::Channel>,
    count: usize,
) -> anyhow::Result<()> {
    let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "timeout_test": true }))?;
    for i in 0..count {
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: format!("timeout-job-{}", i),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes.clone())),
            }),
            limits: vec![],
            tenant: None,
            metadata: std::collections::HashMap::new(),
            task_group: "default".to_string(),
        };
        let _ = client.enqueue(enq).await?;
    }
    Ok(())
}

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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: format!("job{}", i),
                priority: (10 + i) as u32,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes)),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT * FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
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
        let first_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert!(first_row.get("id").is_some(), "row should have id field");

        // Test 2: Query with WHERE clause
        let query_resp = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs WHERE priority >= 10".to_string(),
                tenant: None,
                parameters: vec![],
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 1, "aggregation should return 1 row");
        let count_row: serde_json::Value =
            rmp_serde::from_slice(match &query_resp.rows[0].encoding {
                Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                None => panic!("expected msgpack encoding"),
            })?;
        assert_eq!(count_row["count"], 3, "count should be 3");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_statement_timeout_aborts_execution() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let config = config_with_statement_timeout_ms(100);
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), config).await?;

        enqueue_jobs_for_heavy_query(&mut client, 120).await?;

        // 120^4 cross product count should exceed a 100ms statement timeout.
        let heavy_query = r#"
            SELECT COUNT(*) as count
            FROM jobs j1
            CROSS JOIN jobs j2
            CROSS JOIN jobs j3
            CROSS JOIN jobs j4
        "#;

        let result = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: heavy_query.to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await;

        match result {
            Ok(_) => panic!("expected statement timeout"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
                assert!(
                    status.message().contains("statement timeout"),
                    "expected timeout message, got: {}",
                    status.message()
                );
            }
        }

        // Follow-up query should still execute successfully.
        let count_resp = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT COUNT(*) as count FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await?
            .into_inner();
        assert_eq!(count_resp.row_count, 1);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_arrow_statement_timeout() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let config = config_with_statement_timeout_ms(100);
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), config).await?;

        enqueue_jobs_for_heavy_query(&mut client, 120).await?;

        let heavy_query = r#"
            SELECT COUNT(*) as count
            FROM jobs j1
            CROSS JOIN jobs j2
            CROSS JOIN jobs j3
            CROSS JOIN jobs j4
        "#;

        let result = client
            .query_arrow(QueryArrowRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: heavy_query.to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await;

        match result {
            Ok(_) => panic!("expected statement timeout for query_arrow"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
                assert!(
                    status.message().contains("statement timeout"),
                    "expected timeout message, got: {}",
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
async fn grpc_server_query_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        // Test 1: SQL syntax error
        let res = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT FROM WHERE".to_string(),
                tenant: None,
                parameters: vec![],
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT nonexistent_column FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await;

        match res {
            Ok(_) => panic!("expected column error"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
            }
        }

        // Test 3: Shard not found (use a valid UUID that doesn't exist)
        let res = client
            .query(QueryRequest {
                shard: "99999999-9999-9999-9999-999999999999".to_string(),
                sql: "SELECT * FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
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
async fn grpc_server_query_bind_parameters_positional() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        for i in 0..3 {
            let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "index": i })).unwrap();
            let enq = EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: format!("job{}", i),
                priority: (10 + i) as u32,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes)),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            };
            let _ = client.enqueue(enq).await?;
        }

        let response = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs WHERE tenant = $1 AND priority >= $2 ORDER BY id"
                    .to_string(),
                tenant: None,
                parameters: vec![query_param_string("-"), query_param_int64(11)],
            })
            .await?
            .into_inner();
        assert_eq!(response.row_count, 2);
        let ids: Vec<String> = response
            .rows
            .iter()
            .map(|row| {
                let json: serde_json::Value = rmp_serde::from_slice(match &row.encoding {
                    Some(serialized_bytes::Encoding::Msgpack(data)) => data,
                    None => panic!("expected msgpack encoding"),
                })
                .expect("decode row");
                json["id"].as_str().expect("row id string").to_string()
            })
            .collect();
        assert_eq!(ids, vec!["job1", "job2"]);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_bind_parameter_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let missing_value_err = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs WHERE id = $1".to_string(),
                tenant: None,
                parameters: vec![QueryParameter { value: None }],
            })
            .await
            .expect_err("expected missing value error");
        assert_eq!(missing_value_err.code(), tonic::Code::InvalidArgument);
        assert!(
            missing_value_err
                .message()
                .contains("query parameter is missing a value"),
            "unexpected error: {}",
            missing_value_err.message()
        );

        let too_few_values_err = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs WHERE id = $2".to_string(),
                tenant: None,
                parameters: vec![query_param_string("job1")],
            })
            .await
            .expect_err("expected missing positional parameter");
        assert_eq!(too_few_values_err.code(), tonic::Code::InvalidArgument);
        assert!(
            too_few_values_err
                .message()
                .contains("No value found for placeholder with id $2"),
            "unexpected error: {}",
            too_few_values_err.message()
        );

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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT * FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 0, "expected 0 rows on empty shard");
        assert!(!query_resp.columns.is_empty(), "should still have schema");
        assert!(query_resp.rows.is_empty(), "should have no rows");

        // Add a job, then query with WHERE that matches nothing
        let payload_bytes = rmp_serde::to_vec(&serde_json::json!({ "test": "data" })).unwrap();
        let enq = EnqueueRequest {
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "test_job".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes)),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT * FROM jobs WHERE id = 'nonexistent'".to_string(),
                tenant: None,
                parameters: vec![],
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
            shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            id: "complex_job".to_string(),
            priority: 5,
            start_at_ms: 1234567890,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes)),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id, priority, enqueue_time_ms, payload FROM jobs".to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await?
            .into_inner();

        // Verify schema includes data types (helpful for TypeScript codegen)
        assert!(
            query_resp
                .columns
                .iter()
                .any(|c| c.name == "id" && c.data_type.contains("Utf8"))
        );
        assert!(
            query_resp
                .columns
                .iter()
                .any(|c| c.name == "priority" && c.data_type.contains("UInt8"))
        );
        assert!(
            query_resp
                .columns
                .iter()
                .any(|c| c.name == "enqueue_time_ms" && c.data_type.contains("Int64"))
        );

        // Verify row is valid JSON that TypeScript can deserialize
        let row: serde_json::Value = rmp_serde::from_slice(match &query_resp.rows[0].encoding {
            Some(serialized_bytes::Encoding::Msgpack(d)) => d,
            None => panic!("expected msgpack encoding"),
        })?;
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: format!("job{}", i),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(empty_payload.clone())),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs ORDER BY id".to_string(),
                tenant: None,
                parameters: vec![], // No tenant required for query
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id".to_string(),
                tenant: None,
                parameters: vec![],
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
            let payload_bytes =
                rmp_serde::to_vec(&serde_json::json!({ "value": i * 100 })).unwrap();
            let enq = EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: format!("type_test_{}", i),
                priority: ([0, 50, 99][i as usize]) as u32, // 0, 50, 99
                start_at_ms: 1000000000 + (i as i64 * 1000), // Different timestamps
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes)),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
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
                "#
                .to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await?
            .into_inner();

        assert_eq!(query_resp.row_count, 3, "expected 3 rows");

        // Deserialize all rows and verify types
        let rows: Vec<serde_json::Value> = query_resp
            .rows
            .iter()
            .map(|r| {
                let data = match &r.encoding {
                    Some(serialized_bytes::Encoding::Msgpack(d)) => d,
                    None => panic!("expected msgpack encoding"),
                };
                rmp_serde::from_slice(data).unwrap()
            })
            .collect();

        // Row 0: priority=0, enqueue_time_ms=1000000000
        assert_eq!(rows[0]["id"], "type_test_0");
        assert_eq!(
            rows[0]["shard_id"],
            crate::grpc_integration_helpers::TEST_SHARD_ID
        ); // String (UUID)
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

        // Row 2: priority=99, enqueue_time_ms=1000002000
        assert_eq!(rows[2]["id"], "type_test_2");
        assert_eq!(rows[2]["priority"], 99);
        assert_eq!(rows[2]["enqueue_time_ms"], 1000002000_i64);
        assert!((rows[2]["priority_ratio"].as_f64().unwrap() - 0.99).abs() < 0.001);

        // Test aggregation with different result types
        let agg_resp = client
            .query(QueryRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: r#"
                    SELECT 
                        COUNT(*) as count_val,
                        SUM(priority) as sum_val,
                        AVG(priority) as avg_val,
                        MIN(priority) as min_val,
                        MAX(priority) as max_val
                    FROM jobs
                "#
                .to_string(),
                tenant: None,
                parameters: vec![],
            })
            .await?
            .into_inner();

        assert_eq!(agg_resp.row_count, 1);
        let agg_row: serde_json::Value = rmp_serde::from_slice(match &agg_resp.rows[0].encoding {
            Some(serialized_bytes::Encoding::Msgpack(d)) => d,
            None => panic!("expected msgpack encoding"),
        })?;

        assert_eq!(agg_row["count_val"], 3); // COUNT returns Int64
        assert_eq!(agg_row["sum_val"], 149); // SUM of 0+50+99
        assert!((agg_row["avg_val"].as_f64().unwrap() - 49.666).abs() < 0.01); // AVG returns Float64
        assert_eq!(agg_row["min_val"], 0);
        assert_eq!(agg_row["max_val"], 99);

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that QueryArrow endpoint supports SQL bind parameters.
/// This validates bind parameter handling for the Arrow IPC streaming path.
#[silo::test(flavor = "multi_thread")]
async fn grpc_server_query_arrow_with_bind_parameters() -> anyhow::Result<()> {
    use tokio_stream::StreamExt;

    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (mut client, shutdown_tx, server, _addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let empty_payload = rmp_serde::to_vec(&serde_json::json!({})).unwrap();
        for i in 0..2 {
            let enq = EnqueueRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: format!("arrow_bind_job{}", i),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(empty_payload.clone())),
                }),
                limits: vec![],
                tenant: None,
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            };
            let _ = client.enqueue(enq).await?;
        }

        let response = client
            .query_arrow(QueryArrowRequest {
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs WHERE id = $1".to_string(),
                tenant: None,
                parameters: vec![query_param_string("arrow_bind_job1")],
            })
            .await?;

        let mut stream = response.into_inner();
        let mut total_messages = 0;
        while let Some(msg) = stream.next().await {
            let _arrow_msg = msg?;
            total_messages += 1;
        }
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                id: format!("arrow_job{}", i),
                priority: 10,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(empty_payload.clone())),
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
                shard: crate::grpc_integration_helpers::TEST_SHARD_ID.to_string(),
                sql: "SELECT id FROM jobs ORDER BY id".to_string(),
                tenant: None,
                parameters: vec![], // No tenant required
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
