//! The ordering the enqueue-time index path declares to DataFusion, and the
//! fetch it accepts: `ORDER BY enqueue_time_ms DESC, id ASC LIMIT n` runs as
//! a range scan of `n` index keys with no sort operator.

mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_store_shard::JobStoreShard;
use silo::query::ShardQueryEngine;
use test_helpers::*;

const ENQUEUE_INDEX_LABEL: &str = "path=EnqueueTimeIndex";
const ORDERED_LISTING: &str =
    "SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC, id ASC LIMIT 101";

/// Enqueue `count` jobs whose enqueue times collide in pairs, so the `id`
/// tiebreak matters. Writes are issued concurrently in chunks so large
/// seeds share flushes instead of awaiting one durable write per job.
async fn seed(shard: &JobStoreShard, count: usize) {
    let base = now_ms();
    for chunk in (0..count).collect::<Vec<_>>().chunks(200) {
        let writes = chunk.iter().map(|&i| {
            shard.enqueue(
                "-",
                Some(format!("job-{i}")),
                (i % 7) as u8,
                base - ((i / 2) as i64) * 1_000,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                None,
                "default",
            )
        });
        for result in futures::future::join_all(writes).await {
            result.expect("enqueue");
        }
    }
}

async fn mark_complete(shard: &JobStoreShard, complete: bool) {
    shard
        .set_enqueue_time_index_complete(complete)
        .await
        .expect("set marker");
}

async fn explain(engine: &ShardQueryEngine, query: &str) -> String {
    engine.explain(query).await.expect("explain")
}

fn plan_line(explain: &str) -> String {
    explain
        .lines()
        .find(|l| l.contains("SiloExecutionPlan:"))
        .unwrap_or_else(|| panic!("no SiloExecutionPlan line in EXPLAIN:\n{explain}"))
        .trim()
        .to_string()
}

async fn ids(engine: &ShardQueryEngine, query: &str) -> Vec<String> {
    let batches = engine
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    string_column(&batches, 0)
}

fn string_column(batches: &[RecordBatch], idx: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column");
            (0..b.num_rows())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn row_count(engine: &ShardQueryEngine, query: &str) -> usize {
    engine
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// The leaf `SiloExecutionPlan` node of a physical plan.
fn silo_leaf(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    find_silo_leaf(plan).expect("plan has a SiloExecutionPlan leaf")
}

fn find_silo_leaf(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == "SiloExecutionPlan" {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_silo_leaf)
}

fn one_line(plan: &dyn ExecutionPlan) -> String {
    displayable(plan).one_line().to_string()
}

fn ordering_text(plan: &dyn ExecutionPlan) -> String {
    plan.properties()
        .output_ordering()
        .map(|o| {
            o.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

#[silo::test]
async fn matching_order_by_runs_without_a_sort_and_pushes_the_fetch() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 300).await;
    mark_complete(&shard, true).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let plan_text = explain(&engine, ORDERED_LISTING).await;
    assert!(
        !plan_text.contains("SortExec"),
        "declared ordering must eliminate the sort:\n{plan_text}"
    );
    let line = plan_line(&plan_text);
    assert!(line.contains(ENQUEUE_INDEX_LABEL), "{line}");
    assert!(line.contains("limit=Some(101)"), "fetch pushed: {line}");

    let before = metrics.query_scanned_keys_value(shard.name());
    let got = ids(&engine, ORDERED_LISTING).await;
    let scanned = metrics.query_scanned_keys_value(shard.name()) - before;
    assert_eq!(got.len(), 101);
    assert!(
        scanned <= 101.0,
        "scanned {scanned} keys for a 101-row page"
    );

    mark_complete(&shard, false).await;
    let plan_text = explain(&engine, ORDERED_LISTING).await;
    assert!(
        plan_text.contains("SortExec"),
        "job-record path keeps its sort:\n{plan_text}"
    );
    assert_eq!(ids(&engine, ORDERED_LISTING).await, got);
}

#[silo::test]
async fn with_fetch_preserves_path_ordering_and_filters() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 10).await;
    mark_complete(&shard, true).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let plan = engine
        .get_physical_plan(ORDERED_LISTING)
        .await
        .expect("plan");
    let leaf = silo_leaf(&plan);
    assert_eq!(leaf.fetch(), Some(101));
    assert_eq!(
        ordering_text(leaf.as_ref()),
        "enqueue_time_ms@1 DESC NULLS LAST, id@0 ASC NULLS LAST"
    );
    let text = one_line(leaf.as_ref());
    assert!(text.contains(ENQUEUE_INDEX_LABEL), "{text}");
    assert!(text.contains("tenant"), "filters retained: {text}");

    let tighter = leaf
        .with_fetch(Some(50))
        .expect("index path accepts a fetch");
    assert_eq!(tighter.fetch(), Some(50));
    assert_eq!(
        ordering_text(tighter.as_ref()),
        ordering_text(leaf.as_ref())
    );
    assert_eq!(
        one_line(tighter.as_ref()),
        text.replace("limit=Some(101)", "limit=Some(50)")
    );
    assert!(
        leaf.with_fetch(Some(500)).expect("accepts").fetch() == Some(101),
        "a looser fetch keeps the tighter limit"
    );

    mark_complete(&shard, false).await;
    let plan = engine
        .get_physical_plan("SELECT id, priority FROM jobs WHERE tenant = '-'")
        .await
        .expect("plan");
    let leaf = silo_leaf(&plan);
    assert_eq!(leaf.fetch(), None);
    assert!(
        leaf.with_fetch(Some(5)).is_none(),
        "other paths refuse a fetch"
    );
    assert_eq!(
        ordering_text(leaf.as_ref()),
        "",
        "other paths declare no ordering"
    );
}

#[silo::test]
async fn offset_pages_scan_only_skip_plus_fetch_and_match_the_record_path() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 5_200).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC, id ASC LIMIT 101 OFFSET 5000";

    mark_complete(&shard, false).await;
    let from_records = ids(&engine, query).await;
    assert_eq!(from_records.len(), 101);

    mark_complete(&shard, true).await;
    let before = metrics.query_scanned_keys_value(shard.name());
    let from_index = ids(&engine, query).await;
    let scanned = metrics.query_scanned_keys_value(shard.name()) - before;
    assert_eq!(from_index, from_records);
    assert!(
        scanned <= 5_101.0,
        "OFFSET 5000 LIMIT 101 scanned {scanned} keys"
    );
}

#[silo::test]
async fn consecutive_offset_pages_are_disjoint_and_cover_the_tenant() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 250).await;
    mark_complete(&shard, true).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let mut paged = Vec::new();
    for offset in [0, 100, 200] {
        let page = ids(
            &engine,
            &format!("SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC, id ASC LIMIT 100 OFFSET {offset}"),
        )
        .await;
        assert_eq!(page.len(), if offset == 200 { 50 } else { 100 });
        paged.extend(page);
    }
    let whole = ids(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC, id ASC",
    )
    .await;
    assert_eq!(
        paged, whole,
        "pages concatenate to the full ordered listing"
    );
    let distinct: std::collections::HashSet<&String> = paged.iter().collect();
    assert_eq!(distinct.len(), 250, "pages are disjoint and complete");
}

#[silo::test]
async fn non_matching_order_by_keeps_its_sort_and_stays_correct() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let queries = [
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms ASC, id ASC LIMIT 20",
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC, id DESC LIMIT 20",
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY priority, id LIMIT 20",
    ];
    for query in queries {
        mark_complete(&shard, false).await;
        let expected = ids(&engine, query).await;
        mark_complete(&shard, true).await;
        let plan_text = explain(&engine, query).await;
        assert!(plan_text.contains("SortExec"), "{query}:\n{plan_text}");
        assert_eq!(ids(&engine, query).await, expected, "{query}");
    }
}

#[silo::test]
async fn gadget_listing_shape_takes_the_index_path_and_returns_the_true_newest() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 300).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let gadget = "SELECT id FROM (SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT 50000) ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101 OFFSET 0";

    mark_complete(&shard, false).await;
    let expected = ids(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101",
    )
    .await;

    mark_complete(&shard, true).await;
    let line = plan_line(&explain(&engine, gadget).await);
    assert!(line.contains(ENQUEUE_INDEX_LABEL), "{line}");
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let got = ids(&engine, gadget).await;
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
    assert_eq!(got, expected);
    assert_eq!(lookups, 0.0, "no job-record reads on the index path");
}

#[silo::test]
async fn limit_on_other_tables_is_unchanged() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    for (tenant, queue) in [("t1", "q1"), ("t2", "q2"), ("t3", "q3")] {
        for i in 0..2 {
            shard
                .enqueue(
                    tenant,
                    Some(format!("{tenant}-{i}")),
                    10u8,
                    now,
                    None,
                    msgpack_payload(&serde_json::json!({})),
                    vec![Limit::Concurrency(ConcurrencyLimit {
                        key: queue.to_string(),
                        max_concurrency: 1,
                    })],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
        }
    }
    shard.dequeue("w", "default", 3).await.expect("dequeue");
    shard
        .set_enqueue_time_index_complete(true)
        .await
        .expect("marker");
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    assert_eq!(
        row_count(&engine, "SELECT * FROM tenant_counts LIMIT 2").await,
        2
    );
    assert_eq!(
        row_count(&engine, "SELECT * FROM queue_counts LIMIT 2").await,
        2
    );

    let cases = [
        (
            "SELECT * FROM queues LIMIT 2",
            "SiloExecutionPlan: queues[tenant=None, queue=None, entry_type=None], limit=Some(2)",
        ),
        (
            "SELECT * FROM tasks LIMIT 2",
            "SiloExecutionPlan: tasks[FullScan], limit=Some(2)",
        ),
        (
            "SELECT * FROM tenant_counts LIMIT 2",
            "SiloExecutionPlan: tenant_counts[all], limit=Some(2)",
        ),
        (
            "SELECT * FROM queue_counts LIMIT 2",
            "SiloExecutionPlan: queue_counts[tenant=None], limit=Some(2)",
        ),
        (
            "SELECT id, priority FROM jobs WHERE tenant = 't1' LIMIT 2",
            "SiloExecutionPlan: jobs[FullScan(tenant=Some(\"t1\"))], limit=Some(2), path=FullScanJoin",
        ),
    ];
    for (query, expected) in cases {
        assert_eq!(
            plan_line(&explain(&engine, query).await),
            expected,
            "{query}"
        );
    }
}
