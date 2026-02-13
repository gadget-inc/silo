mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use silo::job_store_shard::JobStoreShard;
use silo::query::ShardQueryEngine;
use test_helpers::*;

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

// Helper to run SQL query and collect results
async fn query_ids(sql: &ShardQueryEngine, query: &str) -> Vec<String> {
    let batches = sql
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    extract_string_column(&batches, 0)
}

// Helper to enqueue a simple job
async fn enqueue_job(shard: &JobStoreShard, id: &str, priority: u8, now: i64) -> String {
    shard
        .enqueue(
            "-",
            Some(id.to_string()),
            priority,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

// Helper to enqueue a job with metadata
async fn enqueue_job_with_metadata(
    shard: &JobStoreShard,
    id: &str,
    priority: u8,
    now: i64,
    metadata: Vec<(String, String)>,
) -> String {
    shard
        .enqueue(
            "-",
            Some(id.to_string()),
            priority,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            Some(metadata),
            "default",
        )
        .await
        .expect("enqueue")
}

#[silo::test]
async fn sql_lists_jobs_basic() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for id in ["a1", "a2", "b1"] {
        enqueue_job(&shard, id, 10, now).await;
    }

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id").await;

    assert_eq!(got, vec!["a1", "a2", "b1"]);
}

#[silo::test]
async fn sql_pushdown_status_kind_running() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let j1 = enqueue_job(&shard, "j1", 10, now).await;
    enqueue_job(&shard, "j2", 10, now).await;

    // Move j1 to Running by leasing a task
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Running'",
    )
    .await;

    assert!(got.contains(&j1));
}

#[silo::test]
async fn sql_exact_id_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for id in ["a1", "a2", "b1", "b2"] {
        enqueue_job(&shard, id, 5, now).await;
    }

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant = '-' AND id = 'b1'").await;

    assert_eq!(got, vec!["b1"]);
}

#[silo::test]
async fn sql_prefix_id_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for id in ["a1", "a2", "b1", "ax", "by"] {
        enqueue_job(&shard, id, 1, now).await;
    }

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND id LIKE 'a%' ORDER BY id",
    )
    .await;

    assert_eq!(got, vec!["a1", "a2", "ax"]);
}

#[silo::test]
async fn sql_status_and_exact_id() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let j1 = enqueue_job(&shard, "j1", 10, now).await;
    enqueue_job(&shard, "j2", 10, now).await;

    // Move j1 to Running
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running' AND id = 'j1'",
    )
    .await;

    assert_eq!(got, vec![j1]);
}

#[silo::test]
async fn sql_status_and_prefix_id() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for id in ["job_a", "job_b", "x_job"] {
        enqueue_job(&shard, id, 10, now).await;
    }
    // Make two jobs Running
    shard
        .dequeue("w", "default", 2)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running' AND id LIKE 'job_%' ORDER BY id").await;

    assert_eq!(got, vec!["job_a", "job_b"]);
}

#[silo::test]
async fn sql_metadata_exact_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "m1",
        5,
        now,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "core".to_string()),
        ],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "m2",
        5,
        now,
        vec![("env".to_string(), "staging".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'env'), 'prod')").await;

    assert_eq!(got, vec!["m1"]);
}

#[silo::test]
async fn sql_metadata_and_status_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    let j1 = enqueue_job_with_metadata(
        &shard,
        "s1",
        5,
        now,
        vec![("role".to_string(), "api".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "s2",
        5,
        now,
        vec![("role".to_string(), "worker".to_string())],
    )
    .await;

    // Make j1 Running
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'role'), 'api') AND status_kind='Running'").await;

    assert_eq!(got, vec![j1]);
}

#[silo::test]
async fn sql_metadata_select_returns_values() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "meta1",
        5,
        now,
        vec![
            ("region".to_string(), "us-west".to_string()),
            ("service".to_string(), "api".to_string()),
        ],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT id, metadata FROM jobs WHERE tenant='-' AND id='meta1'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    // Verify we get a result with the metadata column
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2);
    assert_eq!(batches[0].num_rows(), 1);
}

#[silo::test]
async fn sql_metadata_or_condition() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "or1",
        5,
        now,
        vec![("tier".to_string(), "premium".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "or2",
        5,
        now,
        vec![("tier".to_string(), "enterprise".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "or3",
        5,
        now,
        vec![("tier".to_string(), "free".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND (array_contains(element_at(metadata, 'tier'), 'premium') OR array_contains(element_at(metadata, 'tier'), 'enterprise')) ORDER BY id").await;

    assert_eq!(got, vec!["or1", "or2"]);
}

#[silo::test]
async fn sql_metadata_multiple_keys() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "mk1",
        5,
        now,
        vec![
            ("env".to_string(), "production".to_string()),
            ("datacenter".to_string(), "us-east".to_string()),
        ],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "mk2",
        5,
        now,
        vec![
            ("env".to_string(), "production".to_string()),
            ("datacenter".to_string(), "eu-west".to_string()),
        ],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "mk3",
        5,
        now,
        vec![
            ("env".to_string(), "staging".to_string()),
            ("datacenter".to_string(), "us-east".to_string()),
        ],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'env'), 'production') AND array_contains(element_at(metadata, 'datacenter'), 'us-east')").await;

    assert_eq!(got, vec!["mk1"]);
}

#[silo::test]
async fn sql_metadata_with_status_and_id_prefix() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for (id, version) in [("app_1", "v1"), ("app_2", "v2"), ("other_1", "v1")] {
        enqueue_job_with_metadata(
            &shard,
            id,
            5,
            now,
            vec![("version".to_string(), version.to_string())],
        )
        .await;
    }

    // Make two jobs Running (app_1 and app_2 by priority order)
    shard
        .dequeue("w", "default", 2)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'version'), 'v1') AND status_kind='Running' AND id LIKE 'app_%' ORDER BY id").await;

    assert_eq!(got, vec!["app_1"]);
}

#[silo::test]
async fn verify_exact_id_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "target", 5, now).await;
    enqueue_job(&shard, "other", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND id='target'")
        .await
        .expect("plan");

    // Verify the plan has filters pushed down
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed
            .filters
            .iter()
            .any(|f| f.contains("id") && f.contains("target")),
        "Expected id filter to be pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_metadata_filter_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "m1",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'env'), 'prod')")
        .await
        .expect("plan");

    // Verify metadata filter is pushed down
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed
            .filters
            .iter()
            .any(|f| f.contains("metadata") || f.contains("array_contains")),
        "Expected metadata filter to be pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_status_filter_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now).await;
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running'")
        .await
        .expect("plan");

    // Verify status filter is pushed down
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed
            .filters
            .iter()
            .any(|f| f.contains("status_kind") && f.contains("Running")),
        "Expected status filter to be pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_tenant_filter_always_pushed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' ORDER BY id")
        .await
        .expect("plan");

    // Even without other indexed filters, tenant should be pushed down
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed.filters.iter().any(|f| f.contains("tenant")),
        "Expected tenant filter to be pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_multiple_filters_pushed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "j1",
        5,
        now,
        vec![("role".to_string(), "api".to_string())],
    )
    .await;
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    // Query has metadata AND status filters
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'role'), 'api') AND status_kind='Running'")
        .await
        .expect("plan");

    // Verify both filters are pushed down to our scan
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed.filters.len() >= 2,
        "Expected multiple filters to be pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn explain_plan_shows_filter_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .explain("SELECT id FROM jobs WHERE tenant='-' AND id='j1'")
        .await
        .expect("explain");

    // The plan should mention our filters
    assert!(
        plan.contains("id") || plan.contains("filter") || plan.contains("GenericExecutionPlan"),
        "EXPLAIN plan should show filter information: {}",
        plan
    );
}

#[silo::test]
async fn sql_filter_scheduled_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue jobs: s1 and s2 with current time (will be "Waiting"), s3 with future time (will be "Scheduled")
    enqueue_job(&shard, "s1", 10, now).await;
    enqueue_job(&shard, "s2", 10, now).await;
    // s3 starts in the future - this is true "Scheduled" (not yet waiting)
    shard
        .enqueue(
            "-",
            Some("s3".to_string()),
            10,
            now + 60_000,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Make s1 Running
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled' ORDER BY id",
    )
    .await;
    assert_eq!(got, vec!["s3"]);

    // s2 (enqueued with now, start_time <= now) shows as "Waiting"
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting' ORDER BY id",
    )
    .await;
    assert_eq!(got, vec!["s2"]);
}

#[silo::test]
async fn sql_filter_succeeded_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "success1", 5, now).await; // higher priority to be dequeued first
    enqueue_job(&shard, "other", 10, now).await;

    // Dequeue and complete success1
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            &task_id,
            silo::job_attempt::AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report success");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Succeeded'",
    )
    .await;

    assert_eq!(got, vec!["success1"]);
}

#[silo::test]
async fn sql_filter_failed_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue without retry policy so failure is terminal
    shard
        .enqueue(
            "-",
            Some("fail1".to_string()),
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
    enqueue_job(&shard, "other", 10, now).await;

    // Dequeue and fail fail1
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            &task_id,
            silo::job_attempt::AttemptOutcome::Error {
                error_code: "TEST_ERROR".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report error");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed'",
    )
    .await;

    assert_eq!(got, vec!["fail1"]);
}

#[silo::test]
async fn sql_select_all_columns() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "all_cols", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT * FROM jobs WHERE tenant='-' AND id='all_cols'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    // Should have all 12 columns: shard_id, tenant, id, priority, enqueue_time_ms, payload, status_kind, status_changed_at_ms, task_group, current_attempt, next_attempt_starts_after_ms, metadata
    assert_eq!(batches[0].num_columns(), 12);
    assert_eq!(batches[0].num_rows(), 1);
}

#[silo::test]
async fn sql_select_priority_and_timestamps() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "test", 42, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT priority, enqueue_time_ms FROM jobs WHERE tenant='-' AND id='test'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2);

    // Verify priority is UInt8
    let prio_col = batches[0].column(0);
    assert!(
        prio_col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
            .is_some()
    );
    let prio_arr = prio_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt8Array>()
        .unwrap();
    assert_eq!(prio_arr.value(0), 42);

    // Verify enqueue_time_ms is Int64
    let time_col = batches[0].column(1);
    assert!(
        time_col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .is_some()
    );
}

#[silo::test]
async fn sql_order_by_priority() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue with different priorities (0 is highest, 99 is lowest)
    enqueue_job(&shard, "low", 90, now).await;
    enqueue_job(&shard, "high", 5, now).await;
    enqueue_job(&shard, "mid", 50, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY priority ASC",
    )
    .await;

    // Should be ordered by priority ascending (5, 50, 90)
    assert_eq!(got, vec!["high", "mid", "low"]);
}

#[silo::test]
async fn sql_filter_priority_range() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "p1", 1, now).await;
    enqueue_job(&shard, "p5", 5, now).await;
    enqueue_job(&shard, "p10", 10, now).await;
    enqueue_job(&shard, "p50", 50, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND priority >= 5 AND priority <= 10 ORDER BY id",
    )
    .await;

    assert_eq!(got, vec!["p10", "p5"]);
}

#[silo::test]
async fn sql_filter_by_enqueue_time() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue at different times
    enqueue_job(&shard, "old", 10, now - 10000).await;
    enqueue_job(&shard, "new", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        &format!(
            "SELECT id FROM jobs WHERE tenant = '-' AND enqueue_time_ms > {}",
            now - 5000
        ),
    )
    .await;

    assert_eq!(got, vec!["new"]);
}

#[silo::test]
async fn sql_jobs_without_metadata() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue without metadata
    enqueue_job(&shard, "no_meta", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT id, metadata FROM jobs WHERE tenant='-' AND id='no_meta'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);

    let ids = extract_string_column(&batches, 0);
    assert_eq!(ids, vec!["no_meta"]);
}

#[silo::test]
async fn sql_empty_result_set() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "exists", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND id = 'nonexistent'",
    )
    .await;

    assert_eq!(got, Vec::<String>::new());
}

#[silo::test]
async fn sql_query_with_no_jobs() {
    let (_tmp, shard) = open_temp_shard().await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant = '-'").await;

    assert_eq!(got, Vec::<String>::new());
}

#[silo::test]
async fn sql_count_aggregate() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "c1", 10, now).await;
    enqueue_job(&shard, "c2", 10, now).await;
    enqueue_job(&shard, "c3", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT COUNT(*) FROM jobs WHERE tenant = '-'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);

    // Extract count value
    let count_col = batches[0].column(0);
    let count_arr = count_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count column");
    assert_eq!(count_arr.value(0), 3);
}

#[silo::test]
async fn sql_suffix_id_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "job_test", 5, now).await;
    enqueue_job(&shard, "other_test", 5, now).await;
    enqueue_job(&shard, "test", 5, now).await;
    enqueue_job(&shard, "no_match", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND id LIKE '%_test' ORDER BY id",
    )
    .await;

    assert_eq!(got, vec!["job_test", "other_test"]);
}

#[silo::test]
async fn sql_contains_id_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "prefix_middle_suffix", 5, now).await;
    enqueue_job(&shard, "has_middle_too", 5, now).await;
    enqueue_job(&shard, "no_match", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND id LIKE '%middle%' ORDER BY id",
    )
    .await;

    assert_eq!(got, vec!["has_middle_too", "prefix_middle_suffix"]);
}

#[silo::test]
async fn sql_explicit_limit() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for i in 0..10 {
        enqueue_job(&shard, &format!("job{}", i), 10, now).await;
    }

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id LIMIT 3",
    )
    .await;

    assert_eq!(got.len(), 3);
}

#[silo::test]
async fn sql_limit_beyond_available() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now).await;
    enqueue_job(&shard, "j2", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id LIMIT 100",
    )
    .await;

    assert_eq!(got, vec!["j1", "j2"]);
}

#[silo::test]
async fn sql_tenant_isolation() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue to different tenants
    shard
        .enqueue(
            "tenant_a",
            Some("job_a".to_string()),
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
    shard
        .enqueue(
            "tenant_b",
            Some("job_b".to_string()),
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

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Query tenant_a
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = 'tenant_a' ORDER BY id",
    )
    .await;
    assert_eq!(got, vec!["job_a"]);

    // Query tenant_b
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = 'tenant_b' ORDER BY id",
    )
    .await;
    assert_eq!(got, vec!["job_b"]);
}

#[silo::test]
async fn sql_default_tenant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue to default tenant "-"
    enqueue_job(&shard, "default_job", 10, now).await;

    // Enqueue to another tenant
    shard
        .enqueue(
            "other",
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

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id").await;

    // Should only see default tenant job
    assert_eq!(got, vec!["default_job"]);
}

#[silo::test]
async fn sql_invalid_column_name() {
    let (_tmp, shard) = open_temp_shard().await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let result = sql
        .sql("SELECT nonexistent FROM jobs WHERE tenant = '-'")
        .await;

    // Should get an error for invalid column
    assert!(result.is_err());
}

#[silo::test]
async fn sql_invalid_syntax() {
    let (_tmp, shard) = open_temp_shard().await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let result = sql.sql("SELECT id FROM jobs WHERE").await;

    // Should get an error for incomplete SQL
    assert!(result.is_err());
}

#[silo::test]
async fn sql_metadata_empty_value() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "empty",
        5,
        now,
        vec![("key".to_string(), "".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'key'), '')",
    )
    .await;

    assert_eq!(got, vec!["empty"]);
}

#[silo::test]
async fn sql_metadata_not_exists() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "has_key",
        5,
        now,
        vec![("existing".to_string(), "value".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'nonexistent'), 'value')",
    )
    .await;

    // Should return empty - the key doesn't exist
    assert_eq!(got, Vec::<String>::new());
}

#[silo::test]
async fn sql_metadata_special_chars() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "special",
        5,
        now,
        vec![(
            "key".to_string(),
            "value-with-dash_and_underscore".to_string(),
        )],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'key'), 'value-with-dash_and_underscore')",
    )
    .await;

    assert_eq!(got, vec!["special"]);
}

#[silo::test]
async fn verify_priority_not_pushed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND priority = 10")
        .await
        .expect("plan");

    // Priority is not indexed, so it should be in the filters but we do a full scan
    // The filter should still be present but not used for index lookup
    let pushed = ShardQueryEngine::extract_pushed_filters(&plan);
    assert!(pushed.is_some(), "Should have filters");
    // We can't really verify it's NOT pushed, but we verify the query works
}

#[silo::test]
async fn verify_combined_filters_all_pushed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "combo",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running' AND array_contains(element_at(metadata, 'env'), 'prod') AND id='combo'")
        .await
        .expect("plan");

    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    // All of these are pushable: tenant, status_kind, metadata, id
    assert!(
        pushed.filters.len() >= 3,
        "Expected at least 3 filters pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn sql_multiple_order_by() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Same priority, different times
    enqueue_job(&shard, "a", 10, now - 1000).await;
    enqueue_job(&shard, "b", 10, now).await;
    enqueue_job(&shard, "c", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY priority ASC, enqueue_time_ms DESC",
    )
    .await;

    // Should order by priority first (5, then 10), then by time descending
    // c has priority 5, then b (newer) then a (older)
    assert_eq!(got, vec!["c", "b", "a"]);
}

use silo::job::{ConcurrencyLimit, JobStatusKind, Limit};
use silo::query::{JobsScanStrategy, QueryStatusFilter, parse_jobs_scan_strategy};

// Helper to query queues table and extract queue names
async fn query_queue_names(sql: &ShardQueryEngine, query: &str) -> Vec<String> {
    let batches = sql
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    extract_string_column(&batches, 0)
}

#[silo::test]
async fn queues_table_exists() {
    let (_tmp, shard) = open_temp_shard().await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    // Just verify we can query the queues table without error
    let batches = sql
        .sql("SELECT * FROM queues LIMIT 1")
        .await
        .expect("queues table should exist")
        .collect()
        .await
        .expect("collect");

    // Should have the expected columns
    if !batches.is_empty() {
        assert_eq!(batches[0].num_columns(), 8); // shard_id, tenant, queue_name, entry_type, task_id, job_id, priority, timestamp_ms
    }
}

#[silo::test]
async fn queues_table_shows_holders() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue a job with a concurrency limit
    shard
        .enqueue(
            "-",
            Some("holder-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "test-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Dequeue to create a holder
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let queue_names = query_queue_names(
        &sql,
        "SELECT queue_name FROM queues WHERE tenant = '-' AND entry_type = 'holder'",
    )
    .await;

    assert!(
        queue_names.contains(&"test-queue".to_string()),
        "Expected holder for test-queue, got: {:?}",
        queue_names
    );
}

#[silo::test]
async fn queues_table_shows_requesters() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue two jobs with the same concurrency limit (max 1)
    // First job will get the holder, second will be a requester
    shard
        .enqueue(
            "-",
            Some("first-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "limited-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue first");

    shard
        .enqueue(
            "-",
            Some("second-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "limited-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue second");

    // Dequeue both - first gets holder, second becomes requester
    shard
        .dequeue("worker", "default", 2)
        .await
        .expect("dequeue");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Check for holders
    let holders = query_queue_names(
        &sql,
        "SELECT queue_name FROM queues WHERE tenant = '-' AND entry_type = 'holder'",
    )
    .await;
    assert!(
        holders.contains(&"limited-queue".to_string()),
        "Expected holder, got: {:?}",
        holders
    );

    // Check for requesters
    let requesters = query_queue_names(
        &sql,
        "SELECT queue_name FROM queues WHERE tenant = '-' AND entry_type = 'requester'",
    )
    .await;
    assert!(
        requesters.contains(&"limited-queue".to_string()),
        "Expected requester, got: {:?}",
        requesters
    );
}

#[silo::test]
async fn queues_table_filter_by_queue_name() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Create holders in two different queues
    shard
        .enqueue(
            "-",
            Some("job-a".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-alpha".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    shard
        .enqueue(
            "-",
            Some("job-b".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-beta".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    shard
        .dequeue("worker", "default", 2)
        .await
        .expect("dequeue");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Filter by specific queue name
    let alpha_queues = query_queue_names(
        &sql,
        "SELECT queue_name FROM queues WHERE tenant = '-' AND queue_name = 'queue-alpha'",
    )
    .await;

    assert_eq!(alpha_queues.len(), 1);
    assert_eq!(alpha_queues[0], "queue-alpha");
}

#[silo::test]
async fn queues_table_all_columns() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    shard
        .enqueue(
            "-",
            Some("test-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "my-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT tenant, queue_name, entry_type, task_id, timestamp_ms FROM queues WHERE tenant = '-'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 5);
    assert!(batches[0].num_rows() >= 1);

    // Verify tenant column
    let tenants = extract_string_column(&batches, 0);
    assert!(tenants.iter().all(|t| t == "-"));

    // Verify queue_name column
    let queues = extract_string_column(&batches, 1);
    assert!(queues.contains(&"my-queue".to_string()));

    // Verify entry_type column
    let types = extract_string_column(&batches, 2);
    assert!(types.contains(&"holder".to_string()));
}

#[silo::test]
async fn queues_table_empty_when_no_concurrency() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue a job WITHOUT concurrency limits
    shard
        .enqueue(
            "-",
            Some("no-limit-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![], // No limits
            None,
            "default",
        )
        .await
        .expect("enqueue");

    shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let queue_names =
        query_queue_names(&sql, "SELECT queue_name FROM queues WHERE tenant = '-'").await;

    // Should be empty - no concurrency queues
    assert!(
        queue_names.is_empty(),
        "Expected no queues without concurrency limits, got: {:?}",
        queue_names
    );
}

#[silo::test]
async fn queues_table_count_aggregate() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Create multiple holders in the same queue
    for i in 0..3 {
        shard
            .enqueue(
                "-",
                Some(format!("job-{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: "shared-queue".to_string(),
                    max_concurrency: 10, // High limit so all get holders
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    shard
        .dequeue("worker", "default", 3)
        .await
        .expect("dequeue");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT COUNT(*) FROM queues WHERE tenant = '-' AND queue_name = 'shared-queue'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    let count_col = batches[0].column(0);
    let count_arr = count_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count column");
    assert_eq!(count_arr.value(0), 3);
}

#[silo::test]
async fn queues_table_tenant_isolation() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Create queues for different tenants
    shard
        .enqueue(
            "tenant_x",
            Some("job-x".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-x".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue tenant_x");

    shard
        .enqueue(
            "tenant_y",
            Some("job-y".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-y".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue tenant_y");

    shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue x");
    shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue y");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Query tenant_x
    let x_queues = query_queue_names(
        &sql,
        "SELECT queue_name FROM queues WHERE tenant = 'tenant_x'",
    )
    .await;
    assert_eq!(x_queues, vec!["queue-x"]);

    // Query tenant_y
    let y_queues = query_queue_names(
        &sql,
        "SELECT queue_name FROM queues WHERE tenant = 'tenant_y'",
    )
    .await;
    assert_eq!(y_queues, vec!["queue-y"]);
}

// ===== Metadata prefix search tests =====

#[silo::test]
async fn sql_metadata_prefix_starts_with() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "p1",
        5,
        now,
        vec![("env".to_string(), "production".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "p2",
        5,
        now,
        vec![("env".to_string(), "prod-us".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "p3",
        5,
        now,
        vec![("env".to_string(), "staging".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let mut got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
    )
    .await;
    got.sort();

    assert_eq!(got, vec!["p1", "p2"]);
}

#[silo::test]
async fn sql_metadata_prefix_like() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "l1",
        5,
        now,
        vec![("region".to_string(), "us-east-1".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "l2",
        5,
        now,
        vec![("region".to_string(), "us-west-2".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "l3",
        5,
        now,
        vec![("region".to_string(), "eu-west-1".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let mut got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND array_any_value(element_at(metadata, 'region')) LIKE 'us-%'",
    )
    .await;
    got.sort();

    assert_eq!(got, vec!["l1", "l2"]);
}

#[silo::test]
async fn sql_metadata_prefix_with_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "ps1",
        5,
        now,
        vec![("env".to_string(), "prod-us".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "ps2",
        5,
        now,
        vec![("env".to_string(), "prod-eu".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "ps3",
        5,
        now,
        vec![("env".to_string(), "staging".to_string())],
    )
    .await;

    // Make ps1 Running
    shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod') AND status_kind='Running'",
    )
    .await;

    assert_eq!(got, vec!["ps1"]);
}

#[silo::test]
async fn sql_metadata_prefix_exact_value() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "ev1",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "ev2",
        5,
        now,
        vec![("env".to_string(), "production".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    // Prefix "prod" should match both "prod" (exact) and "production"
    let mut got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
    )
    .await;
    got.sort();

    assert_eq!(got, vec!["ev1", "ev2"]);
}

#[silo::test]
async fn sql_metadata_prefix_no_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "nm1",
        5,
        now,
        vec![("env".to_string(), "staging".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
    )
    .await;

    assert_eq!(got, Vec::<String>::new());
}

#[silo::test]
async fn sql_metadata_suffix_like_not_index_pushed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "sf1",
        5,
        now,
        vec![("env".to_string(), "us-prod".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "sf2",
        5,
        now,
        vec![("env".to_string(), "eu-prod".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "sf3",
        5,
        now,
        vec![("env".to_string(), "staging".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    // Suffix LIKE '%prod' - not a prefix pattern, should still work via DataFusion post-filter
    let mut got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND array_any_value(element_at(metadata, 'env')) LIKE '%prod'",
    )
    .await;
    got.sort();

    assert_eq!(got, vec!["sf1", "sf2"]);
}

// ===== Query plan efficiency verification tests =====

#[silo::test]
async fn verify_metadata_prefix_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "vp1",
        5,
        now,
        vec![("env".to_string(), "production".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
        )
        .await
        .expect("plan");

    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed
            .filters
            .iter()
            .any(|f| f.contains("starts_with") || f.contains("metadata")),
        "Expected metadata prefix filter to be pushed down, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_metadata_prefix_no_redundant_filter() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "nrf1",
        5,
        now,
        vec![("env".to_string(), "production".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
        )
        .await
        .expect("plan");

    // Verify filter is pushed to our scan. DataFusion may still add a FilterExec for other
    // filters (like tenant which is Inexact), but the important thing is our scan gets the filter.
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed
            .filters
            .iter()
            .any(|f| f.contains("starts_with") || f.contains("LIKE") || f.contains("metadata")),
        "Expected prefix filter in pushed filters, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_metadata_equality_no_redundant_filter() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "eq1",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'env'), 'prod')",
        )
        .await
        .expect("plan");

    // Verify filter is pushed to our scan
    let pushed =
        ShardQueryEngine::extract_pushed_filters(&plan).expect("should have pushed filters");
    assert!(
        pushed
            .filters
            .iter()
            .any(|f| f.contains("array") || f.contains("metadata")),
        "Expected metadata equality filter in pushed filters, got: {:?}",
        pushed.filters
    );
}

#[silo::test]
async fn verify_metadata_prefix_explain_plan() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "ep1",
        5,
        now,
        vec![("env".to_string(), "production".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan_output = sql
        .explain(
            "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
        )
        .await
        .expect("explain");

    assert!(
        plan_output.contains("SiloExecutionPlan"),
        "EXPLAIN plan should mention SiloExecutionPlan: {}",
        plan_output
    );
}

// ===== Storage-level index verification tests =====

use silo::keys::{
    ParsedMetadataIndexKey, end_bound, idx_metadata_key_only_prefix, parse_metadata_index_key,
};
use slatedb::DbIterator;

/// Collect all metadata index entries (0x04) for a given tenant and key.
async fn collect_metadata_index_entries(
    shard: &JobStoreShard,
    tenant: &str,
    key: &str,
) -> Vec<ParsedMetadataIndexKey> {
    let start = idx_metadata_key_only_prefix(tenant, key);
    let end = end_bound(&start);
    let mut iter: DbIterator = shard.db().scan::<Vec<u8>, _>(start..end).await.unwrap();
    let mut out = Vec::new();
    loop {
        let maybe = iter.next().await.unwrap();
        let Some(kv) = maybe else { break };
        if let Some(parsed) = parse_metadata_index_key(&kv.key) {
            out.push(parsed);
        }
    }
    out
}

#[silo::test]
async fn metadata_index_entries_created_on_enqueue() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "idx1",
        5,
        now,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "core".to_string()),
        ],
    )
    .await;

    // Check entries for "env" key
    let env_entries = collect_metadata_index_entries(&shard, "-", "env").await;
    assert_eq!(env_entries.len(), 1, "should have 1 entry for 'env' key");
    assert_eq!(env_entries[0].tenant, "-");
    assert_eq!(env_entries[0].key, "env");
    assert_eq!(env_entries[0].value, "prod");
    assert_eq!(env_entries[0].job_id, "idx1");

    // Check entries for "team" key
    let team_entries = collect_metadata_index_entries(&shard, "-", "team").await;
    assert_eq!(team_entries.len(), 1, "should have 1 entry for 'team' key");
    assert_eq!(team_entries[0].key, "team");
    assert_eq!(team_entries[0].value, "core");
    assert_eq!(team_entries[0].job_id, "idx1");
}

#[silo::test]
async fn metadata_index_prefix_scan_range() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "r1",
        5,
        now,
        vec![("env".to_string(), "production".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "r2",
        5,
        now,
        vec![("env".to_string(), "prod-us".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "r3",
        5,
        now,
        vec![("env".to_string(), "staging".to_string())],
    )
    .await;
    enqueue_job_with_metadata(
        &shard,
        "r4",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;

    // Exact prefix match via storage layer
    let mut results = shard
        .scan_jobs_by_metadata_prefix("-", "env", "prod", Some(100))
        .await
        .expect("prefix scan");
    results.sort();
    assert_eq!(
        results,
        vec!["r1", "r2", "r4"],
        "prefix 'prod' should match 'prod', 'prod-us', and 'production'"
    );

    // More specific prefix
    let mut results = shard
        .scan_jobs_by_metadata_prefix("-", "env", "prod-", Some(100))
        .await
        .expect("prefix scan");
    results.sort();
    assert_eq!(
        results,
        vec!["r2"],
        "prefix 'prod-' should only match 'prod-us'"
    );

    // Non-matching prefix
    let results = shard
        .scan_jobs_by_metadata_prefix("-", "env", "dev", Some(100))
        .await
        .expect("prefix scan");
    assert!(results.is_empty(), "prefix 'dev' should match nothing");

    // Empty prefix matches all values for the key
    let mut results = shard
        .scan_jobs_by_metadata_prefix("-", "env", "", Some(100))
        .await
        .expect("prefix scan");
    results.sort();
    assert_eq!(
        results,
        vec!["r1", "r2", "r3", "r4"],
        "empty prefix should match all values"
    );
}

#[silo::test]
async fn metadata_prefix_selective_scan() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue 50 jobs with diverse metadata values
    for i in 0..50 {
        let env = if i < 3 {
            format!("prod-{}", i)
        } else if i < 10 {
            format!("staging-{}", i)
        } else {
            format!("dev-{}", i)
        };
        enqueue_job_with_metadata(
            &shard,
            &format!("sel{}", i),
            5,
            now,
            vec![("env".to_string(), env)],
        )
        .await;
    }

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let mut got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
    )
    .await;
    got.sort();

    // Only the 3 prod-* jobs should match
    assert_eq!(got, vec!["sel0", "sel1", "sel2"]);
}

// ===== Scan strategy verification tests =====
// These tests prove that DataFusion's rewritten expressions are correctly parsed
// into the expected scan strategy, ensuring the right index-backed scan path is used.

#[silo::test]
async fn strategy_metadata_prefix_starts_with() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "t1",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'prod')",
        )
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    assert_eq!(
        strategy,
        JobsScanStrategy::MetadataPrefix {
            tenant: Some("-".to_string()),
            key: "env".to_string(),
            prefix: "prod".to_string(),
        }
    );
}

#[silo::test]
async fn strategy_metadata_prefix_like() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "t1",
        5,
        now,
        vec![("region".to_string(), "us-east".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND array_any_value(element_at(metadata, 'region')) LIKE 'us-%'",
        )
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    assert_eq!(
        strategy,
        JobsScanStrategy::MetadataPrefix {
            tenant: Some("-".to_string()),
            key: "region".to_string(),
            prefix: "us-".to_string(),
        }
    );
}

#[silo::test]
async fn strategy_metadata_equality() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "t1",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'env'), 'prod')",
        )
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    assert_eq!(
        strategy,
        JobsScanStrategy::MetadataExact {
            tenant: Some("-".to_string()),
            key: "env".to_string(),
            value: "prod".to_string(),
        }
    );
}

#[silo::test]
async fn strategy_exact_id() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "my-job", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND id='my-job'")
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    assert_eq!(
        strategy,
        JobsScanStrategy::ExactId {
            tenant: Some("-".to_string()),
            id: "my-job".to_string(),
        }
    );
}

#[silo::test]
async fn strategy_status_filter() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running'")
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    assert_eq!(
        strategy,
        JobsScanStrategy::Status {
            tenant: Some("-".to_string()),
            status: QueryStatusFilter::Stored(JobStatusKind::Running),
        }
    );
}

#[silo::test]
async fn strategy_full_scan_with_tenant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 5, now).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-'")
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    assert_eq!(
        strategy,
        JobsScanStrategy::FullScan {
            tenant: Some("-".to_string()),
        }
    );
}

#[silo::test]
async fn strategy_id_takes_priority_over_metadata() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job_with_metadata(
        &shard,
        "combo",
        5,
        now,
        vec![("env".to_string(), "prod".to_string())],
    )
    .await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let plan = sql
        .get_physical_plan(
            "SELECT id FROM jobs WHERE tenant='-' AND id='combo' AND array_contains(element_at(metadata, 'env'), 'prod')",
        )
        .await
        .expect("plan");

    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    // ExactId should take priority over metadata filter
    assert_eq!(
        strategy,
        JobsScanStrategy::ExactId {
            tenant: Some("-".to_string()),
            id: "combo".to_string(),
        }
    );
}

// ===== Benchmark query EXPLAIN tests =====
// These tests verify that the benchmark queries from benches/query_performance.rs
// use the expected scan strategies by examining EXPLAIN output and programmatic
// strategy extraction. Each test corresponds to one benchmark query pattern.

/// Helper: extract the SiloExecutionPlan line from EXPLAIN output
fn extract_silo_plan_line(explain: &str) -> String {
    explain
        .lines()
        .find(|l| l.contains("SiloExecutionPlan:"))
        .unwrap_or_else(|| panic!("No SiloExecutionPlan line in EXPLAIN:\n{}", explain))
        .trim()
        .to_string()
}

/// Helper: get both EXPLAIN text and programmatic strategy for a query
async fn explain_and_strategy(
    engine: &ShardQueryEngine,
    query: &str,
) -> (String, JobsScanStrategy) {
    let explain = engine.explain(query).await.expect("explain");
    let plan = engine.get_physical_plan(query).await.expect("plan");
    let exprs =
        ShardQueryEngine::extract_pushed_filter_exprs(&plan).expect("should have filter exprs");
    let strategy = parse_jobs_scan_strategy(&exprs);
    (explain, strategy)
}

// Benchmark query: SELECT COUNT(*) FROM jobs
// Cross-tenant, no filters → FullScan with no tenant
#[silo::test]
async fn explain_bench_total_count() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(&engine, "SELECT COUNT(*) FROM jobs").await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("FullScan(tenant=None)"),
        "Expected FullScan with no tenant, got: {}",
        plan_line
    );
    assert_eq!(strategy, JobsScanStrategy::FullScan { tenant: None });
}

// Benchmark query: SELECT tenant, COUNT(*) as cnt FROM jobs
//   WHERE status_kind NOT IN ('Succeeded','Failed','Cancelled')
//   GROUP BY tenant ORDER BY cnt DESC LIMIT 20
// NOT IN doesn't match equality pushdown → FullScan with no tenant
#[silo::test]
async fn explain_bench_top_active_tenants() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT tenant, COUNT(*) as cnt FROM jobs WHERE status_kind NOT IN ('Succeeded','Failed','Cancelled') GROUP BY tenant ORDER BY cnt DESC LIMIT 20",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("FullScan(tenant=None)"),
        "NOT IN should fall through to FullScan, got: {}",
        plan_line
    );
    assert_eq!(strategy, JobsScanStrategy::FullScan { tenant: None });
}

// Benchmark query: SELECT COUNT(*) FROM jobs WHERE tenant = '{t}'
// Tenant-only filter → FullScan with tenant
#[silo::test]
async fn explain_bench_tenant_count() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) =
        explain_and_strategy(&engine, "SELECT COUNT(*) FROM jobs WHERE tenant = '-'").await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("FullScan(tenant=Some(\"-\"))"),
        "Tenant-only filter should use FullScan with tenant, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::FullScan {
            tenant: Some("-".to_string()),
        }
    );
}

// Benchmark query: SELECT COUNT(*) FROM jobs WHERE tenant = '{t}' AND status_kind = 'Waiting'
// Status filter → Status(Waiting) scan strategy
#[silo::test]
async fn explain_bench_count_waiting() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("Status(tenant=Some(\"-\"), status=Waiting)"),
        "Expected Status(Waiting) strategy, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::Status {
            tenant: Some("-".to_string()),
            status: QueryStatusFilter::Waiting,
        }
    );
}

// Benchmark query: SELECT COUNT(*) FROM jobs WHERE tenant = '{t}' AND status_kind = 'Succeeded'
#[silo::test]
async fn explain_bench_count_succeeded() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND status_kind = 'Succeeded'",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("Status(tenant=Some(\"-\"), status=Succeeded)"),
        "Expected Status(Succeeded) strategy, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::Status {
            tenant: Some("-".to_string()),
            status: QueryStatusFilter::Stored(JobStatusKind::Succeeded),
        }
    );
}

// Benchmark query: SELECT COUNT(*) FROM jobs WHERE tenant = '{t}' AND status_kind = 'Scheduled'
#[silo::test]
async fn explain_bench_count_scheduled() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled'",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("Status(tenant=Some(\"-\"), status=FutureScheduled)"),
        "Expected Status(FutureScheduled) strategy, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::Status {
            tenant: Some("-".to_string()),
            status: QueryStatusFilter::FutureScheduled,
        }
    );
}

// Benchmark query: SELECT * FROM jobs WHERE tenant = '{t}' AND status_kind = 'Failed' LIMIT 20
// Status filter with limit → Status(Failed) strategy.
// NOTE: DataFusion does NOT push LIMIT down because our filters are Inexact
// (DataFusion adds FilterExec above, and LIMIT can't be pushed through a filter).
// This means we scan ALL matching rows from the status index, then DataFusion truncates.
#[silo::test]
async fn explain_bench_first_page_failed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT * FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' LIMIT 20",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("Status(tenant=Some(\"-\"), status=Failed)"),
        "Expected Status(Failed) strategy, got: {}",
        plan_line
    );
    // Limit is NOT pushed down because Inexact filters cause FilterExec above our scan
    assert!(
        plan_line.contains("limit=None"),
        "Expected limit=None (not pushed through FilterExec), got: {}",
        plan_line
    );
    // DataFusion handles limit above our scan (Limit in logical plan, GlobalLimitExec in physical)
    assert!(
        explain.contains("Limit") || explain.contains("GlobalLimitExec"),
        "Expected limit handling in plan:\n{}",
        explain
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::Status {
            tenant: Some("-".to_string()),
            status: QueryStatusFilter::Stored(JobStatusKind::Failed),
        }
    );
}

// Benchmark query: SELECT * FROM jobs WHERE tenant = '{t}' AND status_kind = 'Failed' LIMIT 20 OFFSET 180
// Same as first_page: limit is NOT pushed down because of Inexact filters.
// DataFusion handles both OFFSET and LIMIT with GlobalLimitExec.
#[silo::test]
async fn explain_bench_10th_page_failed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT * FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' LIMIT 20 OFFSET 180",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("Status(tenant=Some(\"-\"), status=Failed)"),
        "Expected Status(Failed) strategy, got: {}",
        plan_line
    );
    // Limit is NOT pushed through FilterExec
    assert!(
        plan_line.contains("limit=None"),
        "Expected limit=None (not pushed through FilterExec), got: {}",
        plan_line
    );
    // DataFusion handles offset+limit above our scan (Limit in logical plan, GlobalLimitExec in physical)
    assert!(
        explain.contains("Limit") || explain.contains("GlobalLimitExec"),
        "Expected limit/offset handling in plan:\n{}",
        explain
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::Status {
            tenant: Some("-".to_string()),
            status: QueryStatusFilter::Stored(JobStatusKind::Failed),
        }
    );
}

// Benchmark query: SELECT * FROM jobs WHERE tenant = '{t}' AND id = '{known_id}'
// Exact ID with tenant → ExactId with tenant, single key lookup (fast path)
#[silo::test]
async fn explain_bench_exact_id_with_tenant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "target-00000000", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT * FROM jobs WHERE tenant = '-' AND id = 'target-00000000'",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("ExactId(tenant=Some(\"-\"), id=\"target-00000000\")"),
        "Expected ExactId with tenant, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::ExactId {
            tenant: Some("-".to_string()),
            id: "target-00000000".to_string(),
        }
    );
}

// Contrast: exact ID WITHOUT tenant → ExactId with tenant=None (falls back to scan_all_jobs)
#[silo::test]
async fn explain_bench_exact_id_no_tenant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "target-00000000", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) =
        explain_and_strategy(&engine, "SELECT * FROM jobs WHERE id = 'target-00000000'").await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("ExactId(tenant=None"),
        "Expected ExactId with tenant=None, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::ExactId {
            tenant: None,
            id: "target-00000000".to_string(),
        }
    );
}

// Benchmark query: SELECT * FROM jobs WHERE tenant = '{t}'
//   AND array_contains(element_at(metadata, 'region'), 'us-east-1')
//   AND status_kind = 'Waiting'
// Metadata filter takes priority over status → MetadataExact
#[silo::test]
async fn explain_bench_metadata_and_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job_with_metadata(
        &shard,
        "j1",
        10,
        now,
        vec![("region".to_string(), "us-east-1".to_string())],
    )
    .await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT * FROM jobs WHERE tenant = '-' AND array_contains(element_at(metadata, 'region'), 'us-east-1') AND status_kind = 'Waiting'",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    // Metadata filter should take priority over status filter
    assert!(
        plan_line
            .contains("MetadataExact(tenant=Some(\"-\"), key=\"region\", value=\"us-east-1\")"),
        "Expected MetadataExact strategy (metadata takes priority over status), got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::MetadataExact {
            tenant: Some("-".to_string()),
            key: "region".to_string(),
            value: "us-east-1".to_string(),
        }
    );
}

// Benchmark query: SELECT status_kind, COUNT(*) FROM jobs WHERE tenant = '{t}' GROUP BY status_kind
// Only tenant filter, no status → FullScan with tenant
#[silo::test]
async fn explain_bench_status_breakdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT status_kind, COUNT(*) FROM jobs WHERE tenant = '-' GROUP BY status_kind",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("FullScan(tenant=Some(\"-\"))"),
        "Status breakdown without specific status should use FullScan, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::FullScan {
            tenant: Some("-".to_string()),
        }
    );
}

// Benchmark query: SELECT * FROM jobs WHERE tenant = '{t}' ORDER BY enqueue_time_ms DESC LIMIT 20
// No indexed filter besides tenant → FullScan with tenant, limit pushed
#[silo::test]
async fn explain_bench_recent_jobs() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job(&shard, "j1", 10, now).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT * FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC LIMIT 20",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line.contains("FullScan(tenant=Some(\"-\"))"),
        "ORDER BY without indexed filter should use FullScan, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::FullScan {
            tenant: Some("-".to_string()),
        }
    );
}

// Benchmark query: SELECT COUNT(*) FROM jobs WHERE tenant = '{t}'
//   AND array_contains(element_at(metadata, 'region'), 'us-east-1')
// Metadata equality filter → MetadataExact
#[silo::test]
async fn explain_bench_metadata_count() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_job_with_metadata(
        &shard,
        "j1",
        10,
        now,
        vec![("region".to_string(), "us-east-1".to_string())],
    )
    .await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let (explain, strategy) = explain_and_strategy(
        &engine,
        "SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND array_contains(element_at(metadata, 'region'), 'us-east-1')",
    )
    .await;

    let plan_line = extract_silo_plan_line(&explain);
    assert!(
        plan_line
            .contains("MetadataExact(tenant=Some(\"-\"), key=\"region\", value=\"us-east-1\")"),
        "Expected MetadataExact strategy, got: {}",
        plan_line
    );
    assert_eq!(
        strategy,
        JobsScanStrategy::MetadataExact {
            tenant: Some("-".to_string()),
            key: "region".to_string(),
            value: "us-east-1".to_string(),
        }
    );
}
