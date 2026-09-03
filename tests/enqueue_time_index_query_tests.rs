//! The jobs scanner's enqueue-time index path: selected for tenant-scoped,
//! key-served projections on a shard whose index backfill has completed, and
//! resolved once at planning time.

mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use silo::job_store_shard::JobStoreShard;
use silo::query::ShardQueryEngine;
use test_helpers::*;

const ENQUEUE_INDEX_LABEL: &str = "path=EnqueueTimeIndex";

async fn seed(shard: &JobStoreShard, count: usize) {
    let now = now_ms();
    for i in 0..count {
        // Mix of immediate (stored as 0), past, and future enqueue times.
        let start_at_ms = match i % 3 {
            0 => 0,
            1 => now - (i as i64) * 1_000,
            _ => now + (i as i64) * 1_000,
        };
        shard
            .enqueue(
                "-",
                Some(format!("job-{i:04}")),
                10u8,
                start_at_ms,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                Some(vec![("k".to_string(), format!("v{}", i % 2))]),
                "default",
            )
            .await
            .expect("enqueue");
    }
}

async fn mark_complete(shard: &JobStoreShard, complete: bool) {
    shard
        .set_enqueue_time_index_complete(complete)
        .await
        .expect("set marker");
}

fn plan_line(explain: &str) -> String {
    explain
        .lines()
        .find(|l| l.contains("SiloExecutionPlan:"))
        .unwrap_or_else(|| panic!("no SiloExecutionPlan line in EXPLAIN:\n{explain}"))
        .trim()
        .to_string()
}

async fn explain_line(engine: &ShardQueryEngine, query: &str) -> String {
    plan_line(&engine.explain(query).await.expect("explain"))
}

/// `(id, enqueue_time_ms)` rows from a query projecting exactly those columns.
async fn id_time_rows(engine: &ShardQueryEngine, query: &str) -> Vec<(String, i64)> {
    let batches = engine
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    rows_of(&batches)
}

fn rows_of(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("enqueue_time_ms column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i).to_string(), times.value(i)));
        }
    }
    rows
}

#[silo::test]
async fn key_served_tenant_projections_take_the_index_path_when_complete() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 6).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let queries = [
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-'",
        "SELECT id FROM jobs WHERE tenant = '-' LIMIT 5",
        "SELECT shard_id, tenant, id, enqueue_time_ms FROM jobs WHERE tenant = '-'",
    ];

    mark_complete(&shard, true).await;
    for query in queries {
        let line = explain_line(&engine, query).await;
        assert!(
            line.contains(ENQUEUE_INDEX_LABEL),
            "{query}: expected the enqueue-index path, got {line}"
        );
    }

    mark_complete(&shard, false).await;
    let line = explain_line(&engine, queries[0]).await;
    assert!(
        line.contains("path=FullScanJoin"),
        "{}: job-record projection must fall back to the join path, got {line}",
        queries[0]
    );
    let line = explain_line(&engine, queries[1]).await;
    assert!(
        line.contains("path=StatusIndex"),
        "{}: id-only projection must fall back to the status index, got {line}",
        queries[1]
    );
}

#[silo::test]
async fn other_shapes_keep_their_path_regardless_of_the_flag() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 6).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let cases = [
        (
            "SELECT COUNT(*) FROM jobs WHERE tenant = '-'",
            "path=CountFromCounters",
        ),
        (
            "SELECT id, status_kind FROM jobs WHERE tenant = '-'",
            "path=StatusIndex",
        ),
        (
            "SELECT id, priority FROM jobs WHERE tenant = '-'",
            "path=FullScanJoin",
        ),
        ("SELECT id, enqueue_time_ms FROM jobs", "path=FullScanJoin"),
        ("SELECT id FROM jobs", "path=StatusIndex"),
        (
            "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
            "path=JobPairs",
        ),
        (
            "SELECT id FROM jobs WHERE tenant = '-' AND array_contains(element_at(metadata, 'k'), 'v0')",
            "path=JobPairs",
        ),
    ];

    for (query, expected) in cases {
        mark_complete(&shard, false).await;
        let cleared = explain_line(&engine, query).await;
        mark_complete(&shard, true).await;
        let set = explain_line(&engine, query).await;
        assert!(
            cleared.contains(expected),
            "{query}: expected {expected} with the flag cleared, got {cleared}"
        );
        assert_eq!(
            set, cleared,
            "{query}: EXPLAIN must not change when the flag is set"
        );
        assert!(
            !set.contains(ENQUEUE_INDEX_LABEL),
            "{query}: must never take the enqueue-index path"
        );
    }
}

#[silo::test]
async fn other_tables_keep_their_explain_output() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 2).await;
    mark_complete(&shard, true).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let cases = [
        (
            "SELECT * FROM queues WHERE tenant = '-'",
            "SiloExecutionPlan: queues[tenant=Some(\"-\"), queue=None, entry_type=None], limit=None",
        ),
        (
            "SELECT * FROM tasks LIMIT 3",
            "SiloExecutionPlan: tasks[FullScan], limit=Some(3)",
        ),
        (
            "SELECT * FROM tenant_counts",
            "SiloExecutionPlan: tenant_counts[all], limit=None",
        ),
        (
            "SELECT * FROM queue_counts WHERE tenant = '-'",
            "SiloExecutionPlan: queue_counts[tenant=Some(\"-\")], limit=None",
        ),
    ];
    for (query, expected) in cases {
        assert_eq!(explain_line(&engine, query).await, expected, "{query}");
    }
}

#[silo::test]
async fn index_path_returns_the_same_rows_as_the_job_record_path() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 30).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' ORDER BY id";

    mark_complete(&shard, false).await;
    let from_records = id_time_rows(&engine, query).await;
    mark_complete(&shard, true).await;
    assert!(
        explain_line(&engine, query)
            .await
            .contains(ENQUEUE_INDEX_LABEL)
    );
    let from_index = id_time_rows(&engine, query).await;

    assert_eq!(from_records.len(), 30);
    assert!(
        from_records.iter().any(|(_, t)| *t == 0),
        "seed includes a job with enqueue_time_ms of zero"
    );
    assert_eq!(from_index, from_records);
}

#[silo::test]
async fn index_path_reads_only_index_keys_and_honours_the_scan_limit() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 400).await;
    mark_complete(&shard, true).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let limited = "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT 10";
    assert!(
        explain_line(&engine, limited)
            .await
            .contains(ENQUEUE_INDEX_LABEL)
    );
    let scanned_before = metrics.query_scanned_keys_value(shard.name());
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let rows = id_time_rows(&engine, limited).await;
    let scanned = metrics.query_scanned_keys_value(shard.name()) - scanned_before;
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;

    assert_eq!(rows.len(), 10);
    assert!(
        scanned <= 10.0,
        "LIMIT 10 scanned {scanned} keys; the walk must stop at the limit"
    );
    assert_eq!(lookups, 0.0, "the index path performs no point lookups");
}

#[silo::test]
async fn a_resolved_plan_keeps_its_path_after_the_flag_clears() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 20).await;
    mark_complete(&shard, true).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' ORDER BY id";
    let expected = id_time_rows(&engine, query).await;
    let plan = engine.get_physical_plan(query).await.expect("plan");
    let plan_text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert!(plan_text.contains(ENQUEUE_INDEX_LABEL), "{plan_text}");

    mark_complete(&shard, false).await;
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let batches = datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx())
        .await
        .expect("collect");
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;

    assert_eq!(rows_of(&batches), expected);
    assert_eq!(lookups, 0.0, "the plan must still execute the index path");
}
