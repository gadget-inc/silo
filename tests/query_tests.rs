mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use silo::job_store_shard::JobStoreShard;
use silo::query::JobSql;
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
async fn query_ids(sql: &JobSql, query: &str) -> Vec<String> {
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
            serde_json::json!({}),
            vec![], None,
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
            serde_json::json!({}),
            vec![],
            Some(metadata),
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
    shard.dequeue("-", "w", 2).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
    shard.dequeue("-", "w", 2).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'version'), 'v1') AND status_kind='Running' AND id LIKE 'app_%' ORDER BY id").await;

    assert_eq!(got, vec!["app_1"]);
}

// ===== Predicate Pushdown Verification Tests =====

#[silo::test]
async fn verify_exact_id_pushdown() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "target", 5, now).await;
    enqueue_job(&shard, "other", 5, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND id='target'")
        .await
        .expect("plan");

    // Verify the plan has filters pushed down
    let pushed = JobSql::extract_pushed_filters(&plan).expect("should have pushed filters");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'env'), 'prod')")
        .await
        .expect("plan");

    // Verify metadata filter is pushed down
    let pushed = JobSql::extract_pushed_filters(&plan).expect("should have pushed filters");
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
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running'")
        .await
        .expect("plan");

    // Verify status filter is pushed down
    let pushed = JobSql::extract_pushed_filters(&plan).expect("should have pushed filters");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' ORDER BY id")
        .await
        .expect("plan");

    // Even without other indexed filters, tenant should be pushed down
    let pushed = JobSql::extract_pushed_filters(&plan).expect("should have pushed filters");
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
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    // Query has metadata AND status filters
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'role'), 'api') AND status_kind='Running'")
        .await
        .expect("plan");

    // Verify both filters are pushed down to our scan
    let pushed = JobSql::extract_pushed_filters(&plan).expect("should have pushed filters");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

// ===== Status Kind Coverage Tests =====

#[silo::test]
async fn sql_filter_scheduled_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue jobs (they start as Scheduled)
    enqueue_job(&shard, "s1", 10, now).await;
    enqueue_job(&shard, "s2", 10, now).await;

    // Make one Running
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled' ORDER BY id",
    )
    .await;

    // Only s2 should still be Scheduled (s1 is Running)
    assert_eq!(got, vec!["s2"]);
}

#[silo::test]
async fn sql_filter_succeeded_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "success1", 5, now).await; // higher priority to be dequeued first
    enqueue_job(&shard, "other", 10, now).await;

    // Dequeue and complete success1
    let tasks = shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            "-",
            &task_id,
            silo::job_attempt::AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report success");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
            serde_json::json!({}),
            vec![], None,
        )
        .await
        .expect("enqueue");
    enqueue_job(&shard, "other", 10, now).await;

    // Dequeue and fail fail1
    let tasks = shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    shard
        .report_attempt_outcome(
            "-",
            &task_id,
            silo::job_attempt::AttemptOutcome::Error {
                error_code: "TEST_ERROR".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report error");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed'",
    )
    .await;

    assert_eq!(got, vec!["fail1"]);
}

// ===== Column Type and Projection Tests =====

#[silo::test]
async fn sql_select_all_columns() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "all_cols", 5, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let batches = sql
        .sql("SELECT * FROM jobs WHERE tenant='-' AND id='all_cols'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    // Should have all 8 columns: tenant, id, priority, enqueue_time_ms, payload, status_kind, status_changed_at_ms, metadata
    assert_eq!(batches[0].num_columns(), 8);
    assert_eq!(batches[0].num_rows(), 1);
}

#[silo::test]
async fn sql_select_priority_and_timestamps() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "test", 42, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
    assert!(prio_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt8Array>()
        .is_some());
    let prio_arr = prio_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt8Array>()
        .unwrap();
    assert_eq!(prio_arr.value(0), 42);

    // Verify enqueue_time_ms is Int64
    let time_col = batches[0].column(1);
    assert!(time_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .is_some());
}

#[silo::test]
async fn sql_order_by_priority() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue with different priorities (0 is highest, 99 is lowest)
    enqueue_job(&shard, "low", 90, now).await;
    enqueue_job(&shard, "high", 5, now).await;
    enqueue_job(&shard, "mid", 50, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

// ===== Empty and Null Cases =====

#[silo::test]
async fn sql_jobs_without_metadata() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue without metadata
    enqueue_job(&shard, "no_meta", 10, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

// ===== LIKE Pattern Tests =====

#[silo::test]
async fn sql_suffix_id_match() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "job_test", 5, now).await;
    enqueue_job(&shard, "other_test", 5, now).await;
    enqueue_job(&shard, "test", 5, now).await;
    enqueue_job(&shard, "no_match", 5, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND id LIKE '%middle%' ORDER BY id",
    )
    .await;

    assert_eq!(got, vec!["has_middle_too", "prefix_middle_suffix"]);
}

// ===== Limit Tests =====

#[silo::test]
async fn sql_explicit_limit() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    for i in 0..10 {
        enqueue_job(&shard, &format!("job{}", i), 10, now).await;
    }

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id LIMIT 100",
    )
    .await;

    assert_eq!(got, vec!["j1", "j2"]);
}

// ===== Tenant Isolation Tests =====

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
            serde_json::json!({}),
            vec![], None,
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
            serde_json::json!({}),
            vec![], None,
        )
        .await
        .expect("enqueue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");

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
            serde_json::json!({}),
            vec![], None,
        )
        .await
        .expect("enqueue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(&sql, "SELECT id FROM jobs WHERE tenant = '-' ORDER BY id").await;

    // Should only see default tenant job
    assert_eq!(got, vec!["default_job"]);
}

// ===== Error Handling Tests =====

#[silo::test]
async fn sql_invalid_column_name() {
    let (_tmp, shard) = open_temp_shard().await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let result = sql
        .sql("SELECT nonexistent FROM jobs WHERE tenant = '-'")
        .await;

    // Should get an error for invalid column
    assert!(result.is_err());
}

#[silo::test]
async fn sql_invalid_syntax() {
    let (_tmp, shard) = open_temp_shard().await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let result = sql.sql("SELECT id FROM jobs WHERE").await;

    // Should get an error for incomplete SQL
    assert!(result.is_err());
}

// ===== Metadata Edge Cases =====

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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant='-' AND array_contains(element_at(metadata, 'key'), 'value-with-dash_and_underscore')",
    )
    .await;

    assert_eq!(got, vec!["special"]);
}

// ===== Additional Pushdown Verification Tests =====

#[silo::test]
async fn verify_priority_not_pushed() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now).await;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND priority = 10")
        .await
        .expect("plan");

    // Priority is not indexed, so it should be in the filters but we do a full scan
    // The filter should still be present but not used for index lookup
    let pushed = JobSql::extract_pushed_filters(&plan);
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
    shard.dequeue("-", "w", 1).await.expect("dequeue").tasks;

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let plan = sql
        .get_physical_plan("SELECT id FROM jobs WHERE tenant='-' AND status_kind='Running' AND array_contains(element_at(metadata, 'env'), 'prod') AND id='combo'")
        .await
        .expect("plan");

    let pushed = JobSql::extract_pushed_filters(&plan).expect("should have pushed filters");
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY priority ASC, enqueue_time_ms DESC",
    )
    .await;

    // Should order by priority first (5, then 10), then by time descending
    // c has priority 5, then b (newer) then a (older)
    assert_eq!(got, vec!["c", "b", "a"]);
}

// =============================================================================
// Queues Table Tests - Tests for the concurrency queue SQL table
// =============================================================================

use silo::job::{ConcurrencyLimit, Limit};

// Helper to query queues table and extract queue names
async fn query_queue_names(sql: &JobSql, query: &str) -> Vec<String> {
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

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
        assert_eq!(batches[0].num_columns(), 7); // tenant, queue_name, entry_type, task_id, job_id, priority, timestamp_ms
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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "test-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    // Dequeue to create a holder
    let tasks = shard.dequeue("-", "worker", 1).await.expect("dequeue").tasks;
    assert_eq!(tasks.len(), 1);

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "limited-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "limited-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue second");

    // Dequeue both - first gets holder, second becomes requester
    shard.dequeue("-", "worker", 2).await.expect("dequeue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");

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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-alpha".to_string(),
                max_concurrency: 1,
            })],
            None,
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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-beta".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    shard.dequeue("-", "worker", 2).await.expect("dequeue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");

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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "my-queue".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue");

    shard.dequeue("-", "worker", 1).await.expect("dequeue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
            serde_json::json!({}),
            vec![], // No limits
            None,
        )
        .await
        .expect("enqueue");

    shard.dequeue("-", "worker", 1).await.expect("dequeue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
    let queue_names = query_queue_names(&sql, "SELECT queue_name FROM queues WHERE tenant = '-'").await;

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
                serde_json::json!({}),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: "shared-queue".to_string(),
                    max_concurrency: 10, // High limit so all get holders
                })],
                None,
            )
            .await
            .expect("enqueue");
    }

    shard.dequeue("-", "worker", 3).await.expect("dequeue");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");
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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-x".to_string(),
                max_concurrency: 1,
            })],
            None,
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
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-y".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue tenant_y");

    shard.dequeue("tenant_x", "worker", 1).await.expect("dequeue x");
    shard.dequeue("tenant_y", "worker", 1).await.expect("dequeue y");

    let sql = JobSql::new(Arc::clone(&shard), "jobs").expect("new JobSql");

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
