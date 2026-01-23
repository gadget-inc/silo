//! Tests for cluster-wide query functionality.
//!
//! These tests verify that the ClusterQueryEngine can correctly query
//! data across multiple shards with proper aggregation.

mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use silo::cluster_query::ClusterQueryEngine;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use test_helpers::now_ms;

/// Helper to create a multi-shard factory with temp directories
async fn create_multi_shard_factory(
    num_shards: usize,
) -> (Vec<tempfile::TempDir>, Arc<ShardFactory>) {
    let mut temps = Vec::new();
    let base_dir = tempfile::tempdir().unwrap();
    let base_path = base_dir.path().to_string_lossy().to_string();
    temps.push(base_dir);

    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: format!("{}/shard_%shard%", base_path),
        wal: None,
        apply_wal_on_close: true,
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = Arc::new(ShardFactory::new(template, rate_limiter, None));

    // Open all shards
    for i in 0..num_shards {
        factory.open(i).await.expect("open shard");
    }

    (temps, factory)
}

// Helper to extract string column values from batches
fn extract_string_column(batches: &[RecordBatch], col_idx: usize) -> Vec<String> {
    let mut result = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        let sa = col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        for i in 0..sa.len() {
            if !sa.is_null(i) {
                result.push(sa.value(i).to_string());
            }
        }
    }
    result
}

// Helper to extract i64 column values from batches
fn extract_i64_column(batches: &[RecordBatch], col_idx: usize) -> Vec<i64> {
    let mut result = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 column");
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                result.push(arr.value(i));
            }
        }
    }
    result
}

// Helper to run SQL query and collect results
async fn query_collect(engine: &ClusterQueryEngine, query: &str) -> Vec<RecordBatch> {
    engine
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
}

// =============================================================================
// Single Shard Tests (verify cluster engine works with single local shard)
// =============================================================================

#[silo::test]
async fn cluster_query_single_shard_basic() {
    let (_temps, factory) = create_multi_shard_factory(1).await;
    let now = now_ms();

    // Enqueue jobs to the single shard
    let shard = factory.get("0").unwrap();
    for id in ["a1", "a2", "b1"] {
        shard
            .enqueue(
                "-",
                Some(id.to_string()),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    // Create cluster query engine (no coordinator = local only)
    let engine = ClusterQueryEngine::new(factory.clone(), None, 1)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id",
    )
    .await;
    let ids = extract_string_column(&batches, 0);

    assert_eq!(ids, vec!["a1", "a2", "b1"]);
}

#[silo::test]
async fn cluster_query_single_shard_count() {
    let (_temps, factory) = create_multi_shard_factory(1).await;
    let now = now_ms();

    let shard = factory.get("0").unwrap();
    for i in 0..5 {
        shard
            .enqueue(
                "-",
                Some(format!("job{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 1)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT COUNT(*) as cnt FROM jobs WHERE tenant = '-'",
    )
    .await;
    let counts = extract_i64_column(&batches, 0);

    assert_eq!(counts, vec![5]);
}

// =============================================================================
// Multi-Shard Local Tests (all shards local, no remote)
// =============================================================================

#[silo::test]
async fn cluster_query_multi_shard_combines_results() {
    let (_temps, factory) = create_multi_shard_factory(3).await;
    let now = now_ms();

    // Enqueue jobs to different shards
    for shard_id in 0..3 {
        let shard = factory.get(&shard_id.to_string()).unwrap();
        shard
            .enqueue(
                "-",
                Some(format!("shard{}_job", shard_id)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id",
    )
    .await;
    let ids = extract_string_column(&batches, 0);

    // Should get jobs from all 3 shards
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&"shard0_job".to_string()));
    assert!(ids.contains(&"shard1_job".to_string()));
    assert!(ids.contains(&"shard2_job".to_string()));
}

#[silo::test]
async fn cluster_query_shard_id_column() {
    let (_temps, factory) = create_multi_shard_factory(3).await;
    let now = now_ms();

    // Enqueue jobs to different shards
    for shard_id in 0..3 {
        let shard = factory.get(&shard_id.to_string()).unwrap();
        shard
            .enqueue(
                "-",
                Some(format!("shard{}_job", shard_id)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    // Query with shard_id column
    let batches = query_collect(
        &engine,
        "SELECT shard_id, id FROM jobs WHERE tenant = '-' ORDER BY shard_id, id",
    )
    .await;

    // Extract shard_ids
    let mut shard_ids: Vec<u32> = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        let arr = col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .expect("u32 column");
        for i in 0..arr.len() {
            shard_ids.push(arr.value(i));
        }
    }

    // Should have shard_ids 0, 1, 2
    assert_eq!(shard_ids.len(), 3);
    assert!(shard_ids.contains(&0));
    assert!(shard_ids.contains(&1));
    assert!(shard_ids.contains(&2));
}

#[silo::test]
async fn cluster_query_group_by_shard_id() {
    let (_temps, factory) = create_multi_shard_factory(3).await;
    let now = now_ms();

    // Enqueue different numbers of jobs to each shard
    // Shard 0: 2 jobs, Shard 1: 3 jobs, Shard 2: 1 job
    let shard0 = factory.get("0").unwrap();
    for i in 0..2 {
        shard0
            .enqueue(
                "-",
                Some(format!("s0_job{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let shard1 = factory.get("1").unwrap();
    for i in 0..3 {
        shard1
            .enqueue(
                "-",
                Some(format!("s1_job{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let shard2 = factory.get("2").unwrap();
    shard2
        .enqueue(
            "-",
            Some("s2_job0".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    // GROUP BY shard_id to get counts per shard
    let batches = query_collect(
        &engine,
        "SELECT shard_id, COUNT(*) as cnt FROM jobs WHERE tenant = '-' GROUP BY shard_id ORDER BY shard_id",
    )
    .await;

    // Extract results
    let mut results: Vec<(u32, i64)> = Vec::new();
    for batch in &batches {
        let shard_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .expect("shard_id column");
        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count column");

        for i in 0..batch.num_rows() {
            results.push((shard_col.value(i), count_col.value(i)));
        }
    }

    // Should have 3 groups with correct counts
    assert_eq!(results.len(), 3);
    assert!(results.contains(&(0, 2))); // shard 0 has 2 jobs
    assert!(results.contains(&(1, 3))); // shard 1 has 3 jobs
    assert!(results.contains(&(2, 1))); // shard 2 has 1 job
}

#[silo::test]
async fn cluster_query_multi_shard_count_aggregates_correctly() {
    let (_temps, factory) = create_multi_shard_factory(3).await;
    let now = now_ms();

    // Enqueue different numbers of jobs to each shard
    // Shard 0: 2 jobs, Shard 1: 3 jobs, Shard 2: 5 jobs = 10 total
    for shard_id in 0..3 {
        let shard = factory.get(&shard_id.to_string()).unwrap();
        let job_count = match shard_id {
            0 => 2,
            1 => 3,
            2 => 5,
            _ => 0,
        };
        for i in 0..job_count {
            shard
                .enqueue(
                    "-",
                    Some(format!("shard{}_job{}", shard_id, i)),
                    10,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
        }
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT COUNT(*) as cnt FROM jobs WHERE tenant = '-'",
    )
    .await;
    let counts = extract_i64_column(&batches, 0);

    // DataFusion should aggregate counts from all partitions
    assert_eq!(counts, vec![10]);
}

#[silo::test]
async fn cluster_query_multi_shard_group_by() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Shard 0: 2 high priority, 1 low priority
    let shard0 = factory.get("0").unwrap();
    for i in 0..2 {
        shard0
            .enqueue(
                "-",
                Some(format!("s0_high{}", i)),
                5, // high priority
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }
    shard0
        .enqueue(
            "-",
            Some("s0_low".to_string()),
            50, // low priority
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Shard 1: 1 high priority, 2 low priority
    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "-",
            Some("s1_high".to_string()),
            5, // high priority
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    for i in 0..2 {
        shard1
            .enqueue(
                "-",
                Some(format!("s1_low{}", i)),
                50, // low priority
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT priority, COUNT(*) as cnt FROM jobs WHERE tenant = '-' GROUP BY priority ORDER BY priority",
    )
    .await;

    // Should have 2 groups: priority 5 with count 3, priority 50 with count 3
    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // 2 distinct priority groups
}

#[silo::test]
async fn cluster_query_multi_shard_filter_pushdown() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Enqueue jobs with different statuses
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "-",
            Some("s0_scheduled".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "-",
            Some("s1_scheduled".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Make one job running
    shard0
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query only scheduled jobs
    let batches = query_collect(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled'",
    )
    .await;
    let ids = extract_string_column(&batches, 0);

    // Should only get the scheduled job from shard 1
    assert_eq!(ids.len(), 1);
    assert!(ids.contains(&"s1_scheduled".to_string()));
}

#[silo::test]
async fn cluster_query_multi_shard_empty_some_shards() {
    let (_temps, factory) = create_multi_shard_factory(3).await;
    let now = now_ms();

    // Only put jobs in shard 1
    let shard1 = factory.get("1").unwrap();
    for i in 0..3 {
        shard1
            .enqueue(
                "-",
                Some(format!("job{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT COUNT(*) as cnt FROM jobs WHERE tenant = '-'",
    )
    .await;
    let counts = extract_i64_column(&batches, 0);

    // Should still get correct count even though 2 shards are empty
    assert_eq!(counts, vec![3]);
}

#[silo::test]
async fn cluster_query_multi_shard_all_empty() {
    let (_temps, factory) = create_multi_shard_factory(3).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT COUNT(*) as cnt FROM jobs WHERE tenant = '-'",
    )
    .await;
    let counts = extract_i64_column(&batches, 0);

    // Count should be 0
    assert_eq!(counts, vec![0]);
}

#[silo::test]
async fn cluster_query_multi_shard_limit() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Put 10 jobs in each shard
    for shard_id in 0..2 {
        let shard = factory.get(&shard_id.to_string()).unwrap();
        for i in 0..10 {
            shard
                .enqueue(
                    "-",
                    Some(format!("s{}_job{}", shard_id, i)),
                    10,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
        }
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    let batches = query_collect(&engine, "SELECT id FROM jobs WHERE tenant = '-' LIMIT 5").await;
    let ids = extract_string_column(&batches, 0);

    // Should get exactly 5 results
    assert_eq!(ids.len(), 5);
}

// =============================================================================
// Queues Table Tests
// =============================================================================

#[silo::test]
async fn cluster_query_queues_table_multi_shard() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Create concurrency queues in both shards
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "-",
            Some("s0_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: "queue-a".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    shard0
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");

    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "-",
            Some("s1_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: "queue-b".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    shard1
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    let batches = query_collect(
        &engine,
        "SELECT queue_name FROM queues WHERE tenant = '-' ORDER BY queue_name",
    )
    .await;
    let queue_names = extract_string_column(&batches, 0);

    // Should see queues from both shards
    assert_eq!(queue_names.len(), 2);
    assert!(queue_names.contains(&"queue-a".to_string()));
    assert!(queue_names.contains(&"queue-b".to_string()));
}

// =============================================================================
// Explain Plan Tests
// =============================================================================

#[silo::test]
async fn cluster_query_explain_shows_partitions() {
    let (_temps, factory) = create_multi_shard_factory(3).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 3)
        .await
        .expect("create engine");

    let plan = engine
        .explain("SELECT id FROM jobs WHERE tenant = '-'")
        .await
        .expect("explain");

    // Plan should mention our ClusterExecutionPlan
    assert!(
        plan.contains("Cluster") || plan.contains("partition"),
        "Plan should show cluster execution: {}",
        plan
    );
}

// =============================================================================
// Metadata Filter Tests
// =============================================================================

#[silo::test]
async fn cluster_query_metadata_filter_multi_shard() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Shard 0: job with env=prod
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "-",
            Some("prod_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            Some(vec![("env".to_string(), "prod".to_string())]),
            "default",
        )
        .await
        .expect("enqueue");

    // Shard 1: job with env=staging
    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "-",
            Some("staging_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            Some(vec![("env".to_string(), "staging".to_string())]),
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query for prod jobs only
    let batches = query_collect(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' AND array_contains(element_at(metadata, 'env'), 'prod')",
    )
    .await;
    let ids = extract_string_column(&batches, 0);

    assert_eq!(ids, vec!["prod_job"]);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[silo::test]
async fn cluster_query_malformed_sql_syntax_error() {
    let (_temps, factory) = create_multi_shard_factory(2).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Incomplete SQL - missing condition
    let result = engine.sql("SELECT id FROM jobs WHERE").await;
    assert!(result.is_err(), "Should fail with incomplete SQL");

    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("SQL") || err_msg.contains("syntax") || err_msg.contains("error"),
        "Error should mention SQL/syntax issue: {}",
        err_msg
    );
}

#[silo::test]
async fn cluster_query_invalid_column_name() {
    let (_temps, factory) = create_multi_shard_factory(2).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query with non-existent column
    let result = engine.sql("SELECT nonexistent_column FROM jobs").await;
    assert!(result.is_err(), "Should fail with invalid column name");
}

#[silo::test]
async fn cluster_query_invalid_table_name() {
    let (_temps, factory) = create_multi_shard_factory(2).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query with non-existent table
    let result = engine.sql("SELECT * FROM nonexistent_table").await;
    assert!(result.is_err(), "Should fail with invalid table name");
}

#[silo::test]
async fn cluster_query_type_mismatch_in_filter() {
    let (_temps, factory) = create_multi_shard_factory(2).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // priority is a UInt8, trying to compare with string should fail or be handled
    let result = engine
        .sql("SELECT id FROM jobs WHERE priority = 'not_a_number'")
        .await;

    // This might fail at planning or execution time
    if result.is_ok() {
        // If planning succeeded, execution should fail
        let df = result.unwrap();
        let collect_result = df.collect().await;
        // Either way, we shouldn't get valid results
        assert!(
            collect_result.is_err() || collect_result.unwrap().is_empty(),
            "Type mismatch should fail or return empty"
        );
    }
}

#[silo::test]
async fn cluster_query_empty_sql() {
    let (_temps, factory) = create_multi_shard_factory(1).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 1)
        .await
        .expect("create engine");

    let result = engine.sql("").await;
    assert!(result.is_err(), "Empty SQL should fail");
}

#[silo::test]
async fn cluster_query_sql_with_only_whitespace() {
    let (_temps, factory) = create_multi_shard_factory(1).await;

    let engine = ClusterQueryEngine::new(factory.clone(), None, 1)
        .await
        .expect("create engine");

    let result = engine.sql("   \n\t  ").await;
    assert!(result.is_err(), "Whitespace-only SQL should fail");
}

#[silo::test]
async fn cluster_query_division_by_zero() {
    let (_temps, factory) = create_multi_shard_factory(1).await;
    let now = now_ms();

    let shard = factory.get("0").unwrap();
    shard
        .enqueue(
            "-",
            Some("job1".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 1)
        .await
        .expect("create engine");

    // Division by zero - DataFusion handles this gracefully
    let result = engine
        .sql("SELECT priority / 0 as bad FROM jobs WHERE tenant = '-'")
        .await;

    // DataFusion may return NULL or an error, either is acceptable
    if let Ok(df) = result {
        let batches = df.collect().await;
        // Either error or NULL result is fine
        assert!(
            batches.is_err()
                || batches
                    .as_ref()
                    .map(|b| b
                        .iter()
                        .all(|batch| batch.num_rows() == 0 || batch.column(0).is_null(0)))
                    .unwrap_or(true),
            "Division by zero should error or return NULL"
        );
    }
}

// =============================================================================
// Partial Shard Failure Tests (using shards with missing data paths)
// =============================================================================

#[silo::test]
async fn cluster_query_handles_missing_shard_gracefully() {
    // Create a factory with only shard 0, but tell engine there are 2 shards
    let (_temps, factory) = create_multi_shard_factory(1).await;
    let now = now_ms();

    let shard = factory.get("0").unwrap();
    shard
        .enqueue(
            "-",
            Some("job1".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Engine expects 2 shards but only 1 exists locally and no coordinator
    // This tests the case where build_shard_configs skips unavailable shards
    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Should still be able to query the available shard
    let batches = query_collect(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id",
    )
    .await;
    let ids = extract_string_column(&batches, 0);

    // Should get results from the available shard
    assert_eq!(ids, vec!["job1"]);
}

#[silo::test]
async fn cluster_query_count_with_missing_shards() {
    // Create factory with 2 shards but tell engine there are 4
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Add jobs to both available shards
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "-",
            Some("s0_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "-",
            Some("s1_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Engine expects 4 shards but only 2 exist
    let engine = ClusterQueryEngine::new(factory.clone(), None, 4)
        .await
        .expect("create engine");

    // Count should work and return results from available shards
    let batches = query_collect(
        &engine,
        "SELECT COUNT(*) as cnt FROM jobs WHERE tenant = '-'",
    )
    .await;
    let counts = extract_i64_column(&batches, 0);

    // Should count jobs from the 2 available shards
    assert_eq!(counts, vec![2]);
}

#[silo::test]
async fn cluster_query_group_by_shard_id_with_missing_shards() {
    // Create factory with 2 shards but tell engine there are 4
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "-",
            Some("s0_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "-",
            Some("s1_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 4)
        .await
        .expect("create engine");

    // GROUP BY shard_id should only show available shards
    let batches = query_collect(
        &engine,
        "SELECT shard_id, COUNT(*) as cnt FROM jobs WHERE tenant = '-' GROUP BY shard_id ORDER BY shard_id",
    )
    .await;

    let mut results: Vec<(u32, i64)> = Vec::new();
    for batch in &batches {
        let shard_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .expect("shard_id column");
        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count column");

        for i in 0..batch.num_rows() {
            results.push((shard_col.value(i), count_col.value(i)));
        }
    }

    // Should only have results for shards 0 and 1, not 2 and 3
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(0, 1)));
    assert!(results.contains(&(1, 1)));
}

// =============================================================================
// Tenant Isolation Tests
// =============================================================================

#[silo::test]
async fn cluster_query_tenant_isolation_multi_shard() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Shard 0: tenant A job
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "tenant_a",
            Some("a_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Shard 1: tenant B job
    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "tenant_b",
            Some("b_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query tenant A
    let batches = query_collect(&engine, "SELECT id FROM jobs WHERE tenant = 'tenant_a'").await;
    let ids = extract_string_column(&batches, 0);
    assert_eq!(ids, vec!["a_job"]);

    // Query tenant B
    let batches = query_collect(&engine, "SELECT id FROM jobs WHERE tenant = 'tenant_b'").await;
    let ids = extract_string_column(&batches, 0);
    assert_eq!(ids, vec!["b_job"]);
}

// =============================================================================
// Cross-Tenant Query Tests (no tenant filter - admin/cluster-wide visibility)
// =============================================================================

/// Test that queries WITHOUT a tenant filter return jobs from ALL tenants
#[silo::test]
async fn cluster_query_no_tenant_filter_returns_all_tenants() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Shard 0: tenant A jobs
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "tenant_a",
            Some("a_job1".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    shard0
        .enqueue(
            "tenant_a",
            Some("a_job2".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Shard 1: tenant B jobs
    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "tenant_b",
            Some("b_job1".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Shard 0: also a "default" tenant job
    shard0
        .enqueue(
            "default",
            Some("default_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query WITHOUT tenant filter - should return ALL jobs from ALL tenants
    let batches = query_collect(&engine, "SELECT id FROM jobs ORDER BY id").await;
    let ids = extract_string_column(&batches, 0);

    // Should have all 4 jobs from 3 different tenants
    assert_eq!(ids.len(), 4);
    assert!(ids.contains(&"a_job1".to_string()));
    assert!(ids.contains(&"a_job2".to_string()));
    assert!(ids.contains(&"b_job1".to_string()));
    assert!(ids.contains(&"default_job".to_string()));
}

/// Test that COUNT(*) without tenant filter counts ALL jobs across ALL tenants
#[silo::test]
async fn cluster_query_count_no_tenant_filter() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Shard 0: 2 jobs in tenant A
    let shard0 = factory.get("0").unwrap();
    for i in 0..2 {
        shard0
            .enqueue(
                "tenant_a",
                Some(format!("a{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    // Shard 1: 3 jobs in tenant B
    let shard1 = factory.get("1").unwrap();
    for i in 0..3 {
        shard1
            .enqueue(
                "tenant_b",
                Some(format!("b{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // COUNT without tenant filter should return 5
    let batches = query_collect(&engine, "SELECT COUNT(*) as cnt FROM jobs").await;
    let counts = extract_i64_column(&batches, 0);
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 5);
}

/// Test that querying by ID without tenant filter finds the job in any tenant
#[silo::test]
async fn cluster_query_by_id_no_tenant_filter() {
    let (_temps, factory) = create_multi_shard_factory(1).await;
    let now = now_ms();

    let shard = factory.get("0").unwrap();

    // Create jobs with same ID pattern but different tenants
    shard
        .enqueue(
            "tenant_x",
            Some("unique_job_123".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"data": "from_x"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    shard
        .enqueue(
            "tenant_y",
            Some("other_job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 1)
        .await
        .expect("create engine");

    // Query by specific ID without tenant filter - should find it
    let batches = query_collect(
        &engine,
        "SELECT tenant, id FROM jobs WHERE id = 'unique_job_123'",
    )
    .await;

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let tenants = extract_string_column(&batches, 0);
    assert_eq!(tenants, vec!["tenant_x"]);
}

/// Test that status filter without tenant returns jobs from all tenants
#[silo::test]
async fn cluster_query_by_status_no_tenant_filter() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Create scheduled jobs in different tenants
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "alpha",
            Some("alpha_scheduled".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "beta",
            Some("beta_scheduled".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // Query by status without tenant filter
    let batches = query_collect(
        &engine,
        "SELECT tenant, id FROM jobs WHERE status_kind = 'Scheduled' ORDER BY id",
    )
    .await;

    let tenants = extract_string_column(&batches, 0);
    let ids = extract_string_column(&batches, 1);

    // Should have both jobs from both tenants
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"alpha_scheduled".to_string()));
    assert!(ids.contains(&"beta_scheduled".to_string()));
    assert!(tenants.contains(&"alpha".to_string()));
    assert!(tenants.contains(&"beta".to_string()));
}

/// Test the webui-style query (what the index page uses) without tenant filter
#[silo::test]
async fn cluster_query_webui_style_no_tenant_filter() {
    let (_temps, factory) = create_multi_shard_factory(2).await;
    let now = now_ms();

    // Create jobs across multiple tenants
    let shard0 = factory.get("0").unwrap();
    shard0
        .enqueue(
            "customer_1",
            Some("c1_job".to_string()),
            5,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let shard1 = factory.get("1").unwrap();
    shard1
        .enqueue(
            "customer_2",
            Some("c2_job".to_string()),
            10,
            now + 1000,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    let engine = ClusterQueryEngine::new(factory.clone(), None, 2)
        .await
        .expect("create engine");

    // This is the exact query pattern the webui uses (without tenant filter)
    let batches = query_collect(
        &engine,
        "SELECT shard_id, id, status_kind, enqueue_time_ms, priority FROM jobs ORDER BY enqueue_time_ms DESC LIMIT 100",
    )
    .await;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should see jobs from all tenants");
}
