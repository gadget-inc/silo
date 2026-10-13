//! The jobs scanner's status-segmented enqueue-time index path: the IDE tab
//! shapes with bound parameters, served key-only in enqueue order on a
//! version-2 shard, returning exactly what the unindexed paths return.

mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::ScalarValue;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::{
    ENQUEUE_TIME_INDEX_VERSION, JobStoreShard, STATUS_ENQUEUE_TIME_INDEX_VERSION,
};
use silo::query::ShardQueryEngine;
use test_helpers::*;

const LISTING_LABEL: &str = "path=StatusEnqueueTimeIndex";
const PRIORITIES: [u8; 4] = [0, 5, 50, 99];

fn utf8(s: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(s.to_string()))
}

fn int(v: i64) -> ScalarValue {
    ScalarValue::Int64(Some(v))
}

/// Seed `count` jobs across four priorities, three jobs per enqueue
/// millisecond, with a few negative enqueue times, and spread them over
/// Succeeded, Failed, Running, Cancelled, and future-Scheduled.
async fn seed(shard: &JobStoreShard, count: usize) {
    let now = now_ms();
    let mut pending: Vec<(String, usize)> = Vec::new();
    for i in 0..count {
        let id = format!("job-{i:04}");
        let priority = PRIORITIES[i % PRIORITIES.len()];
        let enqueue_time = match i % 23 {
            0 => -5,
            1 => -1,
            _ => now - 60_000 - ((i / 3) as i64) * 1_000,
        };
        let future = i % 5 == 4;
        let start = if future {
            now + 3_600_000
        } else {
            enqueue_time
        };
        shard
            .enqueue(
                "-",
                Some(id.clone()),
                priority,
                start,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        match i % 5 {
            3 => shard.cancel_job("-", &id).await.expect("cancel"),
            4 => {}
            _ => pending.push((id, i)),
        }
    }
    let mut leased: Vec<silo::task::LeasedTask> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while leased.len() < pending.len() {
        let batch = shard
            .dequeue("worker", "default", pending.len() - leased.len())
            .await
            .expect("dequeue")
            .tasks;
        leased.extend(batch);
        assert!(
            std::time::Instant::now() < deadline,
            "timed out leasing {} tasks",
            pending.len()
        );
        if leased.len() < pending.len() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    for task in &leased {
        let (_, i) = pending
            .iter()
            .find(|(id, _)| id == task.job().id())
            .expect("leased job was pending");
        let outcome = match i % 5 {
            0 => AttemptOutcome::Success { result: vec![] },
            1 => AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
            // Running jobs stay leased.
            _ => continue,
        };
        shard
            .report_attempt_outcome(task.attempt().task_id(), outcome)
            .await
            .expect("report");
    }
}

async fn set_version(shard: &JobStoreShard, version: u8) {
    shard
        .set_listing_index_version(version)
        .await
        .expect("set marker");
}

async fn collect(
    engine: &ShardQueryEngine,
    query: &str,
    params: &[ScalarValue],
) -> Vec<RecordBatch> {
    let df = engine.sql(query).await.expect("sql");
    let df = if params.is_empty() {
        df
    } else {
        df.with_param_values(params.to_vec()).expect("bind")
    };
    df.collect().await.expect("collect")
}

/// The `SiloExecutionPlan` line of the query's EXPLAIN output.
async fn plan_line(engine: &ShardQueryEngine, query: &str, params: &[ScalarValue]) -> String {
    let df = engine.sql(query).await.expect("sql");
    let df = if params.is_empty() {
        df
    } else {
        df.with_param_values(params.to_vec()).expect("bind")
    };
    let batches = df
        .explain(false, false)
        .expect("explain")
        .collect()
        .await
        .expect("collect");
    let mut text = String::new();
    for batch in batches {
        for col in batch.columns() {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        text.push_str(arr.value(i));
                        text.push('\n');
                    }
                }
            }
        }
    }
    text.lines()
        .find(|l| l.contains("SiloExecutionPlan:"))
        .unwrap_or_else(|| panic!("no SiloExecutionPlan line in EXPLAIN:\n{text}"))
        .trim()
        .to_string()
}

/// The full EXPLAIN output, for assertions about nodes above the scan.
async fn explain_text(engine: &ShardQueryEngine, query: &str, params: &[ScalarValue]) -> String {
    let df = engine.sql(query).await.expect("sql");
    let df = if params.is_empty() {
        df
    } else {
        df.with_param_values(params.to_vec()).expect("bind")
    };
    let batches = df
        .explain(false, false)
        .expect("explain")
        .collect()
        .await
        .expect("collect");
    let mut text = String::new();
    for batch in batches {
        for col in batch.columns() {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        text.push_str(arr.value(i));
                        text.push('\n');
                    }
                }
            }
        }
    }
    text
}

fn ids_of(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        for i in 0..batch.num_rows() {
            out.push(ids.value(i).to_string());
        }
    }
    out
}

async fn ids(engine: &ShardQueryEngine, query: &str, params: &[ScalarValue]) -> Vec<String> {
    ids_of(&collect(engine, query, params).await)
}

/// Assert the query returns the same rows in the same order at version 2
/// as at version 1, and return the version-2 rows.
async fn assert_same_as_version_1(
    shard: &JobStoreShard,
    engine: &ShardQueryEngine,
    query: &str,
    params: &[ScalarValue],
) -> Vec<String> {
    set_version(shard, ENQUEUE_TIME_INDEX_VERSION).await;
    let line = plan_line(engine, query, params).await;
    assert!(
        !line.contains(LISTING_LABEL),
        "{query}: version 1 must not take the listing path: {line}"
    );
    let baseline = ids(engine, query, params).await;

    set_version(shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let indexed = ids(engine, query, params).await;
    assert_eq!(indexed, baseline, "{query} with {params:?}");
    indexed
}

/// The IDE tab shape: `$1` is the tenant, `$2` the status, then an optional
/// priority pin, then the limit and offset, every value bound.
fn tab_query(status_predicate: &str, priority: bool) -> String {
    let (pin, limit, offset) = if priority {
        (" AND priority = $3", "$4", "$5")
    } else {
        ("", "$3", "$4")
    };
    format!(
        "SELECT id FROM jobs WHERE tenant = $1 AND {status_predicate}{pin} \
         ORDER BY enqueue_time_ms DESC, id ASC LIMIT {limit} OFFSET {offset}"
    )
}

#[silo::test]
async fn stored_status_tabs_match_version_1_and_take_the_listing_path() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 120).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    for status in ["Running", "Succeeded", "Failed", "Cancelled"] {
        for priority in [None, Some(5i64), Some(50)] {
            for (limit, offset) in [(100, 0), (7, 0), (7, 5), (3, 20)] {
                let query = tab_query("status_kind = $2", priority.is_some());
                let mut params = vec![utf8("-"), utf8(status)];
                if let Some(p) = priority {
                    params.push(int(p));
                }
                params.push(int(limit));
                params.push(int(offset));
                let rows = assert_same_as_version_1(&shard, &engine, &query, &params).await;
                assert!(
                    rows.len() <= limit as usize,
                    "{query}: {} rows over limit {limit}",
                    rows.len()
                );
                let line = plan_line(&engine, &query, &params).await;
                assert!(
                    line.contains(LISTING_LABEL),
                    "{query} with {params:?}: expected the listing path, got {line}"
                );
                assert!(line.contains(&format!("segments=[{status}]")), "{line}");
                if let Some(p) = priority {
                    assert!(line.contains(&format!("priority={p}")), "{line}");
                }
            }
        }
    }
}

#[silo::test]
async fn priority_pin_is_recognized_in_every_shape_datafusion_emits() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let order = "ORDER BY enqueue_time_ms DESC, id ASC";

    let expected = ids(
        &engine,
        &format!("SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND priority = 5 {order}"),
        &[],
    )
    .await;
    assert!(!expected.is_empty());
    let shapes: Vec<(String, Vec<ScalarValue>)> = vec![
        (
            format!(
                "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND 5 = priority {order}"
            ),
            vec![],
        ),
        (
            format!(
                "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND priority = $1 {order}"
            ),
            vec![int(5)],
        ),
        (
            format!(
                "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND $1 = priority {order}"
            ),
            vec![int(5)],
        ),
        (
            format!(
                "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND CAST(priority AS BIGINT) = 5 {order}"
            ),
            vec![],
        ),
    ];
    for (query, params) in &shapes {
        let line = plan_line(&engine, query, params).await;
        assert!(
            line.contains(LISTING_LABEL) && line.contains("priority=5"),
            "{query}: {line}"
        );
        assert_eq!(ids(&engine, query, params).await, expected, "{query}");
    }

    let out_of_range = format!(
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND priority = $1 {order}"
    );
    let line = plan_line(&engine, &out_of_range, &[int(500)]).await;
    assert!(
        line.contains(LISTING_LABEL) && line.contains("priority=500"),
        "{line}"
    );
    assert!(ids(&engine, &out_of_range, &[int(500)]).await.is_empty());
}

#[silo::test]
async fn version_1_keeps_the_prior_label_and_version_2_plans_no_sort_or_filter() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 40).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let shapes = [
        (tab_query("status_kind = $2", false), vec![utf8("-"), utf8("Failed"), int(10), int(5)]),
        (
            tab_query("status_kind = $2", true),
            vec![utf8("-"), utf8("Succeeded"), int(50), int(10), int(0)],
        ),
        (
            tab_query("status_kind IN ($2, 'Running')", false),
            vec![utf8("-"), utf8("Cancelled"), int(10), int(0)],
        ),
        (
            "SELECT id FROM jobs WHERE tenant = $1 AND priority = $2 ORDER BY enqueue_time_ms DESC, id ASC LIMIT $3 OFFSET $4".to_string(),
            vec![utf8("-"), int(99), int(10), int(3)],
        ),
    ];
    for (query, params) in &shapes {
        set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
        let line = plan_line(&engine, query, params).await;
        assert!(
            line.contains("path=JobPairs")
                || line.contains("path=EnqueueTimeIndex")
                || line.contains("path=FullScanJoin"),
            "{query}: version 1 keeps the prior path, got {line}"
        );

        set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
        let text = explain_text(&engine, query, params).await;
        assert!(text.contains(LISTING_LABEL), "{query}:\n{text}");
        assert!(
            !text.contains("SortExec"),
            "{query}: sort above the scan:\n{text}"
        );
        assert!(
            !text.contains("FilterExec"),
            "{query}: filter above the scan:\n{text}"
        );
    }
}

#[silo::test]
async fn in_list_of_stored_kinds_is_merged_across_segments() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 90).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = tab_query("status_kind IN ($2, $3)", false)
        .replace("LIMIT $3 OFFSET $4", "LIMIT $4 OFFSET $5");
    let params = [utf8("-"), utf8("Failed"), utf8("Running"), int(200), int(0)];
    let rows = assert_same_as_version_1(&shard, &engine, &query, &params).await;
    assert!(rows.len() > 20, "{}", rows.len());
    let line = plan_line(&engine, &query, &params).await;
    assert!(
        line.contains(LISTING_LABEL) && line.contains("segments=[Failed,Running]"),
        "{line}"
    );
}

#[silo::test]
async fn pinned_shape_projecting_a_job_record_column_hydrates_only_matching_rows() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let query = "SELECT id, priority, payload FROM jobs WHERE tenant = $1 AND status_kind = 'Failed' AND priority = $2";
    let params = [utf8("-"), int(50)];

    let line = plan_line(&engine, query, &params).await;
    assert!(
        line.contains("path=JobPairs") && line.contains("ListingIndex("),
        "{line}"
    );
    let batches = collect(&engine, query, &params).await;
    let text = pretty_format_batches(&batches).expect("format").to_string();
    let rows = ids_of(&batches);
    assert!(!rows.is_empty());
    for batch in &batches {
        let priorities = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
            .expect("priority column");
        for i in 0..batch.num_rows() {
            assert_eq!(priorities.value(i), 50, "{text}");
        }
    }
    set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
    let mut baseline = ids(&engine, query, &params).await;
    let mut got = rows;
    baseline.sort();
    got.sort();
    assert_eq!(got, baseline);
}

#[silo::test]
async fn pinned_tenant_shapes_never_take_the_counter_or_enqueue_time_paths() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 40).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let count = "SELECT COUNT(*) FROM jobs WHERE tenant = $1 AND priority = $2";
    let listing = "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = $1 AND priority = $2";
    let params = [utf8("-"), int(5)];

    for version in [
        ENQUEUE_TIME_INDEX_VERSION,
        STATUS_ENQUEUE_TIME_INDEX_VERSION,
    ] {
        set_version(&shard, version).await;
        let line = plan_line(&engine, count, &params).await;
        assert!(
            !line.contains("path=CountFromCounters"),
            "v{version}: {line}"
        );
        let line = plan_line(&engine, listing, &params).await;
        assert!(
            !line.contains("path=EnqueueTimeIndex"),
            "v{version}: {line}"
        );
    }
    let line = plan_line(&engine, count, &params).await;
    assert!(line.contains(LISTING_LABEL), "{line}");
    let expected = ids(
        &engine,
        "SELECT id FROM jobs WHERE tenant = $1 AND priority = $2",
        &params,
    )
    .await
    .len();
    let batches = collect(&engine, count, &params).await;
    let counted = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("count")
                .values()
                .to_vec()
        })
        .sum::<i64>();
    assert_eq!(counted as usize, expected);
}

#[silo::test]
async fn priority_discovery_opens_one_arm_per_priority_present_within_the_key_budget() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let now = now_ms();
    // Failed jobs at priorities 5 and 99 only; the other priorities exist in
    // other segments, so discovery must not open arms for them.
    for i in 0..40 {
        let priority = if i % 2 == 0 { 5 } else { 99 };
        shard
            .enqueue(
                "-",
                Some(format!("f-{i:03}")),
                priority,
                now - 60_000 - i as i64,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }
    for i in 0..6 {
        shard
            .enqueue(
                "-",
                Some(format!("c-{i:03}")),
                PRIORITIES[i % 4],
                now + 60_000,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        shard
            .cancel_job("-", &format!("c-{i:03}"))
            .await
            .expect("cancel");
    }
    let task_ids = dequeue_task_ids_until(&shard, "w", "default", 40).await;
    for task_id in &task_ids {
        shard
            .report_attempt_outcome(
                task_id,
                AttemptOutcome::Error {
                    error_code: "E".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report");
    }
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let shard_name = shard.name().to_string();

    let arms_before = metrics.query_listing_index_arms_value(&shard_name);
    let keys_before = metrics.query_scanned_keys_value(&shard_name);
    let rows = ids(
        &engine,
        &tab_query("status_kind = $2", false),
        &[utf8("-"), utf8("Failed"), int(10), int(5)],
    )
    .await;
    assert_eq!(rows.len(), 10);
    let arms = metrics.query_listing_index_arms_value(&shard_name) - arms_before;
    let keys = metrics.query_scanned_keys_value(&shard_name) - keys_before;
    assert_eq!(arms, 2.0, "one arm per priority present in the segment");
    // OFFSET + LIMIT, plus per arm one final chunk capped by the limit, plus
    // one discovery seek per arm.
    let budget = (5 + 10) + 2 * 15 + 2;
    assert!(
        keys <= budget as f64,
        "scanned {keys} keys, budget {budget}"
    );
}

#[silo::test]
async fn paging_across_same_millisecond_enqueues_has_no_gaps_or_duplicates() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms() - 60_000;
    for i in 0..250 {
        shard
            .enqueue(
                "-",
                Some(format!("same-{i:04}")),
                PRIORITIES[i % 4],
                now,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }
    for task_id in dequeue_task_ids_until(&shard, "w", "default", 250).await {
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "E".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report");
    }
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = tab_query("status_kind = $2", false);

    let mut paged = Vec::new();
    for offset in [0, 100, 200] {
        let page = ids(
            &engine,
            &query,
            &[utf8("-"), utf8("Failed"), int(100), int(offset)],
        )
        .await;
        paged.extend(page);
    }
    let expected: Vec<String> = (0..250).map(|i| format!("same-{i:04}")).collect();
    assert_eq!(paged, expected);
}

#[silo::test]
async fn key_carried_projection_matches_the_unindexed_path() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT status_kind, priority, enqueue_time_ms, tenant, shard_id, id FROM jobs \
                 WHERE tenant = $1 AND status_kind IN ($2, $3) ORDER BY enqueue_time_ms DESC, id ASC";
    let params = [utf8("-"), utf8("Failed"), utf8("Cancelled")];

    set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
    let baseline = pretty_format_batches(&collect(&engine, query, &params).await)
        .expect("format")
        .to_string();
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    assert!(
        plan_line(&engine, query, &params)
            .await
            .contains(LISTING_LABEL)
    );
    let indexed = pretty_format_batches(&collect(&engine, query, &params).await)
        .expect("format")
        .to_string();
    assert_eq!(indexed, baseline);
    assert!(indexed.contains("Failed") && indexed.contains("Cancelled"));
}

#[silo::test]
async fn shapes_outside_the_listing_strategy_keep_their_strategies_on_version_2() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 30).await;
    // Two Waiting jobs: past start, never dequeued.
    for i in 0..2 {
        shard
            .enqueue(
                "-",
                Some(format!("waiting-{i}")),
                5,
                now_ms() - 1_000,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                Some(vec![("env".to_string(), "prod".to_string())]),
                "default",
            )
            .await
            .expect("enqueue");
    }
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let keeps: [(&str, &str); 6] = [
        (
            "SELECT id FROM jobs WHERE status_kind = 'Failed'",
            "Status(",
        ),
        (
            "SELECT id FROM jobs WHERE tenant = '-'",
            "path=EnqueueTimeIndex",
        ),
        (
            "SELECT id FROM jobs WHERE tenant = '-' AND id = 'job-0001' AND status_kind = 'Failed' AND priority = 5",
            "ExactId(",
        ),
        (
            "SELECT id FROM jobs WHERE tenant = '-' AND array_contains(element_at(metadata, 'env'), 'prod') AND priority = 5",
            "MetadataExact(",
        ),
        (
            "SELECT id FROM jobs WHERE tenant = '-' AND starts_with(array_any_value(element_at(metadata, 'env')), 'pr') AND status_kind = 'Waiting'",
            "MetadataPrefix(",
        ),
        (
            "SELECT id FROM jobs WHERE tenant = '-' AND (status_kind = 'Failed' OR (status_kind = 'Scheduled' AND current_attempt > 1))",
            "StatusUnion(",
        ),
    ];
    for (query, strategy) in keeps {
        let line = plan_line(&engine, query, &[]).await;
        assert!(
            line.contains(strategy) && !line.contains("ListingIndex("),
            "{query}: expected {strategy}, got {line}"
        );
    }
    let waiting = ids(
        &engine,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting' ORDER BY id",
        &[],
    )
    .await;
    assert_eq!(
        waiting,
        vec!["waiting-0".to_string(), "waiting-1".to_string()]
    );
}

#[silo::test]
async fn tenant_and_priority_shape_labels_scheduled_rows_from_the_entry_value() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 40).await;
    for i in 0..3 {
        shard
            .enqueue(
                "-",
                Some(format!("waiting-{i}")),
                99,
                now_ms() - 1_000 - i,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT id, status_kind FROM jobs WHERE tenant = $1 AND priority = $2 \
                 ORDER BY enqueue_time_ms DESC, id ASC";
    let params = [utf8("-"), int(99)];

    set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
    let baseline = pretty_format_batches(&collect(&engine, query, &params).await)
        .expect("format")
        .to_string();
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let line = plan_line(&engine, query, &params).await;
    assert!(
        line.contains(LISTING_LABEL)
            && line.contains(
                "segments=[ScheduledFirst,ScheduledRetry,Running,Succeeded,Failed,Cancelled]"
            ),
        "{line}"
    );
    let indexed = pretty_format_batches(&collect(&engine, query, &params).await)
        .expect("format")
        .to_string();
    assert_eq!(indexed, baseline);
    assert!(
        indexed.contains("Waiting") && indexed.contains("Scheduled"),
        "{indexed}"
    );
}

#[silo::test]
async fn cluster_engine_applies_the_pin_on_a_version_2_shard() {
    use silo::cluster_query::ClusterQueryEngine;
    use silo::factory::ShardFactory;
    use silo::gubernator::MockGubernatorClient;
    use silo::settings::{Backend, DatabaseTemplate};
    use silo::shard_range::ShardMap;

    let base = tempfile::tempdir().unwrap();
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: format!("{}/%shard%", base.path().to_string_lossy()),
        ..Default::default()
    };
    let factory = Arc::new(ShardFactory::new(
        template,
        MockGubernatorClient::new_arc(),
        None,
    ));
    let shard_map = ShardMap::create_initial(1).expect("shard map");
    let info = &shard_map.shards()[0];
    factory
        .open(&info.id, &info.range)
        .await
        .expect("open shard");
    let shard = factory.get(&info.id).expect("shard");
    seed(&shard, 40).await;
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let engine = ClusterQueryEngine::new(Arc::clone(&factory), None)
        .await
        .expect("cluster engine");
    let batches = engine
        .sql("SELECT id, priority FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND priority = 50 ORDER BY id")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let rows = ids_of(&batches);
    assert!(!rows.is_empty());
    for batch in &batches {
        let priorities = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
            .expect("priority column");
        for i in 0..batch.num_rows() {
            assert_eq!(priorities.value(i), 50);
        }
    }
    let local = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let expected = ids(
        &local,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Failed' AND priority = 50 ORDER BY id",
        &[],
    )
    .await;
    assert_eq!(rows, expected);
}

/// Add first attempts and retries on both sides of the clock to a seeded
/// shard: waiting and future retries (failed once, rescheduled with a zero
/// or a long backoff), then waiting first attempts that are never dequeued.
async fn seed_scheduled(shard: &JobStoreShard) {
    let now = now_ms();
    for i in 0..12 {
        let backoff = if i < 6 { 0 } else { 3_600_000 };
        shard
            .enqueue(
                "-",
                Some(format!("retry-{i:02}")),
                PRIORITIES[i % 4],
                now - 60_000 - i as i64,
                Some(silo::retry::RetryPolicy {
                    retry_count: 3,
                    initial_interval_ms: backoff,
                    max_interval_ms: backoff.max(1),
                    randomize_interval: false,
                    backoff_factor: 1.0,
                }),
                msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }
    for task_id in dequeue_task_ids_until(shard, "w", "default", 12).await {
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "E".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report");
    }
    for i in 0..6 {
        shard
            .enqueue(
                "-",
                Some(format!("waiting-{i:02}")),
                PRIORITIES[i % 4],
                now - 30_000 - i as i64,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }
}

fn scheduled_tab_shapes() -> Vec<(
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
)> {
    // (status predicate, bound status, expected split, expected attempt, expected segments)
    vec![
        (
            "status_kind = $2 AND current_attempt = 1",
            "Scheduled",
            "split=future",
            "attempt=first",
            "segments=[ScheduledFirst]",
        ),
        (
            "status_kind = $2 AND current_attempt > 1",
            "Scheduled",
            "split=future",
            "attempt=retry",
            "segments=[ScheduledRetry]",
        ),
        (
            "status_kind = $2",
            "Waiting",
            "split=waiting",
            "attempt=none",
            "segments=[ScheduledFirst,ScheduledRetry]",
        ),
    ]
}

#[silo::test]
async fn scheduled_retrying_and_waiting_tabs_match_version_1_and_take_the_listing_path() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    for (predicate, status, split, attempt, segments) in scheduled_tab_shapes() {
        for priority in [None, Some(0i64), Some(99)] {
            let query = tab_query(predicate, priority.is_some());
            let mut params = vec![utf8("-"), utf8(status)];
            if let Some(p) = priority {
                params.push(int(p));
            }
            params.push(int(100));
            params.push(int(0));
            let rows = assert_same_as_version_1(&shard, &engine, &query, &params).await;
            assert!(!rows.is_empty(), "{query} with {params:?} returned nothing");
            let line = plan_line(&engine, &query, &params).await;
            for expected in [LISTING_LABEL, split, attempt, segments] {
                assert!(
                    line.contains(expected),
                    "{query}: expected {expected} in {line}"
                );
            }
        }
    }
}

#[silo::test]
async fn waiting_scheduled_and_running_list_merges_three_segments_without_a_split() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT id FROM jobs WHERE tenant = $1 AND status_kind IN ($2, $3, $4) \
                 ORDER BY enqueue_time_ms DESC, id ASC LIMIT $5 OFFSET $6";
    let params = [
        utf8("-"),
        utf8("Waiting"),
        utf8("Scheduled"),
        utf8("Running"),
        int(500),
        int(0),
    ];

    let rows = assert_same_as_version_1(&shard, &engine, query, &params).await;
    assert!(rows.iter().any(|id| id.starts_with("waiting-")));
    assert!(rows.iter().any(|id| id.starts_with("retry-")));
    assert!(rows.iter().any(|id| id.starts_with("job-")));
    let line = plan_line(&engine, query, &params).await;
    for expected in [
        LISTING_LABEL,
        "split=none",
        "segments=[ScheduledFirst,ScheduledRetry,Running]",
    ] {
        assert!(line.contains(expected), "expected {expected} in {line}");
    }
}

#[silo::test]
async fn scheduled_rows_are_filtered_and_labeled_by_one_snapshot() {
    let (_tmp, shard) = open_temp_shard().await;
    seed_scheduled(&shard).await;
    shard
        .enqueue(
            "-",
            Some("edge".to_string()),
            7,
            now_ms() + 400,
            None,
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let labels = |batches: Vec<RecordBatch>| -> Vec<(String, String)> {
        let mut out = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id");
            let kinds = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("status_kind");
            for i in 0..batch.num_rows() {
                out.push((ids.value(i).to_string(), kinds.value(i).to_string()));
            }
        }
        out
    };
    let edge = "SELECT id, status_kind FROM jobs WHERE tenant = $1 AND priority = $2";
    assert_eq!(
        labels(collect(&engine, edge, &[utf8("-"), int(7)]).await),
        vec![("edge".to_string(), "Scheduled".to_string())]
    );
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(
        labels(collect(&engine, edge, &[utf8("-"), int(7)]).await),
        vec![("edge".to_string(), "Waiting".to_string())]
    );

    // Every row a split admits carries that split's label.
    let waiting = "SELECT id, status_kind FROM jobs WHERE tenant = $1 AND status_kind = $2";
    let rows = labels(collect(&engine, waiting, &[utf8("-"), utf8("Waiting")]).await);
    assert!(rows.len() >= 7, "{rows:?}");
    assert!(rows.iter().all(|(_, kind)| kind == "Waiting"), "{rows:?}");
    let rows = labels(collect(&engine, waiting, &[utf8("-"), utf8("Scheduled")]).await);
    assert_eq!(rows.len(), 6, "{rows:?}");
    assert!(rows.iter().all(|(_, kind)| kind == "Scheduled"), "{rows:?}");
}

#[silo::test]
async fn attempt_pins_are_exact_only_under_the_listing_strategy() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 30).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let shapes = [
        ("current_attempt = 1", "attempt=first"),
        ("current_attempt > 1", "attempt=retry"),
        ("current_attempt >= 2", "attempt=retry"),
        ("1 = current_attempt", "attempt=first"),
        ("current_attempt = $3", "attempt=first"),
    ];
    for (pin, attempt) in shapes {
        let query = format!(
            "SELECT id FROM jobs WHERE tenant = $1 AND status_kind = $2 AND {pin} \
             ORDER BY enqueue_time_ms DESC, id ASC LIMIT 50"
        );
        let mut params = vec![utf8("-"), utf8("Scheduled")];
        if pin.contains("$3") {
            params.push(int(1));
        }
        set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
        let text = explain_text(&engine, &query, &params).await;
        assert!(
            text.contains("FilterExec") && text.contains("current_attempt"),
            "{query}: version 1 must re-apply the pin:\n{text}"
        );
        let baseline = ids(&engine, &query, &params).await;

        set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
        let text = explain_text(&engine, &query, &params).await;
        assert!(
            text.contains(LISTING_LABEL) && text.contains(attempt),
            "{query}:\n{text}"
        );
        assert!(
            !text.contains("FilterExec"),
            "{query}: pin must be Exact:\n{text}"
        );
        assert_eq!(ids(&engine, &query, &params).await, baseline, "{query}");
    }
}

#[silo::test]
async fn pinned_scheduled_shape_projecting_payload_hydrates_only_the_split_and_pin() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 30).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = "SELECT id, current_attempt, payload FROM jobs WHERE tenant = $1 \
                 AND status_kind = $2 AND current_attempt > 1 AND priority = $3 ORDER BY id";
    let params = [utf8("-"), utf8("Scheduled"), int(99)];

    set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
    let baseline = pretty_format_batches(&collect(&engine, query, &params).await)
        .expect("format")
        .to_string();
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let line = plan_line(&engine, query, &params).await;
    assert!(
        line.contains("path=JobPairs")
            && line.contains("attempt=retry")
            && line.contains("split=future"),
        "{line}"
    );
    let indexed = pretty_format_batches(&collect(&engine, query, &params).await)
        .expect("format")
        .to_string();
    assert_eq!(indexed, baseline);
    assert!(indexed.contains("retry-"), "{indexed}");
}

#[silo::test]
async fn clock_split_counts_take_the_index_path_without_record_reads() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 30).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let shard_name = shard.name().to_string();
    let counts = [
        ("status_kind = $2", "Waiting"),
        ("status_kind = $2 AND current_attempt = 1", "Scheduled"),
        ("status_kind = $2 AND current_attempt > 1", "Scheduled"),
    ];
    for (predicate, status) in counts {
        let count = format!("SELECT COUNT(*) FROM jobs WHERE tenant = $1 AND {predicate}");
        let listing = format!("SELECT id FROM jobs WHERE tenant = $1 AND {predicate}");
        let params = [utf8("-"), utf8(status)];

        set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
        let walked = ids(&engine, &listing, &params).await.len();
        assert!(walked > 0, "{listing}");

        set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
        let line = plan_line(&engine, &count, &params).await;
        assert!(line.contains(LISTING_LABEL), "{count}: {line}");
        let lookups_before = metrics.query_point_lookups_value(&shard_name);
        let batches = collect(&engine, &count, &params).await;
        let counted: i64 = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .expect("count")
                    .values()
                    .to_vec()
            })
            .sum();
        assert_eq!(counted as usize, walked, "{count}");
        assert_eq!(
            metrics.query_point_lookups_value(&shard_name) - lookups_before,
            0.0,
            "{count}: no job or status records read"
        );
    }
}

#[silo::test]
async fn attempt_pin_beside_a_stored_kind_or_without_a_status_stays_a_residual() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 30).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    // Only a Scheduled status carries `current_attempt`, so the pin beside a
    // terminal kind matches nothing; it must still be applied, not dropped.
    let shapes = [
        (
            "SELECT id FROM jobs WHERE tenant = $1 AND status_kind IN ('Running', 'Scheduled') AND current_attempt = 1 ORDER BY id",
            true,
        ),
        (
            "SELECT id FROM jobs WHERE tenant = $1 AND current_attempt = 1 AND priority = 99 ORDER BY id",
            true,
        ),
        (
            "SELECT id FROM jobs WHERE tenant = $1 AND status_kind = 'Failed' AND current_attempt = 1 ORDER BY id",
            false,
        ),
    ];
    for (query, expect_rows) in shapes {
        let params = [utf8("-")];
        set_version(&shard, ENQUEUE_TIME_INDEX_VERSION).await;
        let baseline = ids(&engine, query, &params).await;
        assert_eq!(!baseline.is_empty(), expect_rows, "{query}: {baseline:?}");

        set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
        let text = explain_text(&engine, query, &params).await;
        assert!(
            text.contains("ListingIndex(") && text.contains("attempt=none"),
            "{query}:\n{text}"
        );
        assert!(
            text.contains("FilterExec") && text.contains("current_attempt"),
            "{query}: the pin must stay a residual filter:\n{text}"
        );
        assert_eq!(ids(&engine, query, &params).await, baseline, "{query}");
    }
}

#[silo::test]
async fn clock_split_scan_reads_within_the_rejected_key_budget() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 60).await;
    seed_scheduled(&shard).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let shard_name = shard.name().to_string();
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let future = ids(
        &engine,
        "SELECT id FROM jobs WHERE tenant = $1 AND status_kind = $2",
        &[utf8("-"), utf8("Scheduled")],
    )
    .await
    .len();

    let (limit, offset) = (5usize, 3usize);
    let arms_before = metrics.query_listing_index_arms_value(&shard_name);
    let keys_before = metrics.query_scanned_keys_value(&shard_name);
    let rows = ids(
        &engine,
        &tab_query("status_kind = $2", false),
        &[
            utf8("-"),
            utf8("Waiting"),
            int(limit as i64),
            int(offset as i64),
        ],
    )
    .await;
    assert_eq!(rows.len(), limit);
    let arms = (metrics.query_listing_index_arms_value(&shard_name) - arms_before) as usize;
    let keys = metrics.query_scanned_keys_value(&shard_name) - keys_before;
    assert!(arms >= 1);
    let budget = offset + limit + future + arms * (offset + limit) + arms;
    assert!(
        keys <= budget as f64,
        "scanned {keys} keys, budget {budget} with {arms} arms"
    );
}
