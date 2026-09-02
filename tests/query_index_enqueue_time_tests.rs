//! `enqueue_time_ms` served from the status/time index value by the jobs
//! scanner: path selection (EXPLAIN), correctness, the fallback for rows whose
//! index value is unknown, and the metrics that observe each.

mod test_helpers;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::query::ShardQueryEngine;
use test_helpers::*;

async fn enqueue_job(shard: &JobStoreShard, tenant: &str, id: &str, start_at_ms: i64) {
    shard
        .enqueue(
            tenant,
            Some(id.to_string()),
            10u8,
            start_at_ms,
            None,
            msgpack_payload(&serde_json::json!({"id": id})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

async fn dequeue_one(shard: &JobStoreShard) -> String {
    let ids = dequeue_task_ids_until(shard, "w", "default", 1).await;
    ids.into_iter().next().expect("one task")
}

/// Seed jobs in `tenant` with distinct enqueue times: `count` far-future
/// scheduled jobs, one waiting job, and one succeeded job. Returns
/// `job_id -> enqueue_time_ms` as `JOB_INFO` reports it.
async fn seed_jobs(shard: &JobStoreShard, tenant: &str, count: usize) -> HashMap<String, i64> {
    let now = now_ms();
    for i in 0..count {
        enqueue_job(
            shard,
            tenant,
            &format!("future{i:03}"),
            now + 3_600_000 + i as i64,
        )
        .await;
    }
    enqueue_job(shard, tenant, "done", now).await;
    let task = dequeue_one(shard).await;
    shard
        .report_attempt_outcome(&task, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("success");
    enqueue_job(shard, tenant, "waiting", now - 60_000).await;
    expected_enqueue_times(shard, tenant).await
}

async fn expected_enqueue_times(shard: &JobStoreShard, tenant: &str) -> HashMap<String, i64> {
    let pairs = shard.scan_all_jobs(None).await.expect("scan jobs");
    let mut out = HashMap::new();
    for (t, id) in pairs {
        if t != tenant {
            continue;
        }
        let view = shard.get_job(tenant, &id).await.unwrap().unwrap();
        out.insert(id, view.enqueue_time_ms());
    }
    out
}

fn engine(shard: &Arc<JobStoreShard>) -> ShardQueryEngine {
    ShardQueryEngine::new(Arc::clone(shard), "jobs").expect("engine")
}

async fn run(sql: &ShardQueryEngine, query: &str) -> Vec<RecordBatch> {
    sql.sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
}

/// `(id, enqueue_time_ms)` rows from batches projecting both columns.
fn id_enqueue_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string");
        let times = batch
            .column_by_name("enqueue_time_ms")
            .expect("enqueue_time_ms column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i).to_string(), times.value(i)));
        }
    }
    rows
}

fn string_column(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("{name} column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string");
        for i in 0..batch.num_rows() {
            out.push(col.value(i).to_string());
        }
    }
    out
}

/// The `path=` label from the SiloExecutionPlan line of EXPLAIN.
async fn explain_path(sql: &ShardQueryEngine, query: &str) -> String {
    let explain = sql.explain(query).await.expect("explain");
    let line = explain
        .lines()
        .find(|l| l.contains("SiloExecutionPlan:"))
        .unwrap_or_else(|| panic!("no SiloExecutionPlan line in EXPLAIN:\n{explain}"));
    line.split("path=")
        .nth(1)
        .unwrap_or_else(|| panic!("no path= in plan line: {line}"))
        .split(',')
        .next()
        .unwrap()
        .trim()
        .to_string()
}

fn assert_rows_match(rows: &[(String, i64)], expected: &HashMap<String, i64>, context: &str) {
    assert!(!rows.is_empty(), "{context}: no rows");
    for (id, enqueue_time_ms) in rows {
        assert_eq!(
            expected.get(id),
            Some(enqueue_time_ms),
            "{context}: enqueue_time_ms for {id}"
        );
    }
}

struct MetricSnapshot {
    point_lookups: f64,
    fallback: f64,
    scanned_keys: f64,
}

fn snapshot(metrics: &silo::metrics::Metrics, shard: &JobStoreShard) -> MetricSnapshot {
    MetricSnapshot {
        point_lookups: metrics.query_point_lookups_value(shard.name()),
        fallback: metrics.enqueue_time_fallback_hydrations_value(shard.name()),
        scanned_keys: metrics.query_scanned_keys_value(shard.name()),
    }
}

const INDEX_SERVED_SHAPES: [(&str, &str); 4] = [
    (
        "FullScan",
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-'",
    ),
    (
        "Status stored",
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Succeeded'",
    ),
    (
        "Status virtual",
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
    ),
    (
        "StatusUnion",
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND status_kind IN ('Waiting', 'Succeeded')",
    ),
];

#[silo::test]
async fn backfilled_shard_serves_enqueue_time_from_the_index() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let expected = seed_jobs(&shard, "-", 20).await;
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    let sql = engine(&shard);

    for (name, query) in INDEX_SERVED_SHAPES {
        assert_eq!(explain_path(&sql, query).await, "status-index", "{name}");
        let before = snapshot(&metrics, &shard);
        let rows = id_enqueue_rows(&run(&sql, query).await);
        let after = snapshot(&metrics, &shard);
        assert_rows_match(&rows, &expected, name);
        assert_eq!(
            after.point_lookups, before.point_lookups,
            "{name}: point lookups"
        );
        assert_eq!(
            after.fallback, before.fallback,
            "{name}: fallback hydrations"
        );
    }
}

#[silo::test]
async fn shard_without_marker_keeps_fullscan_on_the_job_record_path() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let expected = seed_jobs(&shard, "-", 5).await;
    assert!(!shard.enqueue_time_backfill_complete());
    let sql = engine(&shard);

    for (name, query) in INDEX_SERVED_SHAPES {
        let path = if name == "FullScan" {
            "fullscan-join"
        } else {
            "status-index"
        };
        assert_eq!(explain_path(&sql, query).await, path, "{name}");
        let rows = id_enqueue_rows(&run(&sql, query).await);
        assert_rows_match(&rows, &expected, name);
    }

    // Index-only shapes that do not project enqueue_time_ms are unaffected by
    // the marker: same path and zero point lookups.
    for query in [
        "SELECT COUNT(*) FROM jobs",
        "SELECT id, status_kind FROM jobs WHERE tenant = '-'",
    ] {
        assert_eq!(explain_path(&sql, query).await, "status-index", "{query}");
        let before = snapshot(&metrics, &shard);
        run(&sql, query).await;
        let after = snapshot(&metrics, &shard);
        assert_eq!(
            after.point_lookups, before.point_lookups,
            "{query}: point lookups"
        );
    }
}

#[silo::test]
async fn counters_path_only_for_unfiltered_count() {
    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    seed_jobs(&shard, "-", 3).await;
    let sql = engine(&shard);

    assert_eq!(
        explain_path(&sql, "SELECT COUNT(*) FROM jobs WHERE tenant = '-'").await,
        "status-counters"
    );
    assert_eq!(
        explain_path(&sql, "SELECT id FROM jobs WHERE tenant = '-' LIMIT 5").await,
        "status-index"
    );
}

#[silo::test]
async fn tenant_less_status_query_stays_on_the_pairs_path() {
    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    let expected = seed_jobs(&shard, "-", 3).await;
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    let sql = engine(&shard);
    let query = "SELECT id, enqueue_time_ms FROM jobs WHERE status_kind = 'Succeeded'";

    assert_eq!(explain_path(&sql, query).await, "pairs");
    let rows = id_enqueue_rows(&run(&sql, query).await);
    assert_eq!(rows.len(), 1);
    assert_rows_match(&rows, &expected, "tenant-less Status");
}

#[silo::test]
async fn other_job_info_columns_keep_the_job_record_path() {
    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    let expected = seed_jobs(&shard, "-", 5).await;
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    let sql = engine(&shard);

    let cases = [
        (
            "SELECT id, enqueue_time_ms, priority FROM jobs WHERE tenant = '-'",
            "fullscan-join",
        ),
        (
            "SELECT id, enqueue_time_ms, priority FROM jobs WHERE tenant = '-' AND status_kind = 'Succeeded'",
            "pairs",
        ),
        (
            "SELECT id, enqueue_time_ms, current_attempt FROM jobs WHERE tenant = '-'",
            "fullscan-join",
        ),
        (
            "SELECT id, enqueue_time_ms, current_attempt FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
            "pairs",
        ),
        (
            "SELECT id, enqueue_time_ms, status_changed_at_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
            "pairs",
        ),
    ];
    for (query, path) in cases {
        assert_eq!(explain_path(&sql, query).await, path, "{query}");
        let rows = id_enqueue_rows(&run(&sql, query).await);
        assert_rows_match(&rows, &expected, query);
        assert!(
            rows.iter().all(|(_, t)| *t != 0),
            "{query}: enqueue_time_ms must be populated: {rows:?}"
        );
    }
}

/// Every index-served shape hydrates exactly the rows in its range whose
/// entry lacks the value. The status shapes run without the marker, since
/// they are index-served regardless of it.
#[silo::test]
async fn rows_lacking_the_value_are_hydrated_individually() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let expected = seed_jobs(&shard, "-", 30).await;
    for i in 0..10 {
        strip_enqueue_time_from_job_rows(&shard, "-", &format!("future{i:03}")).await;
    }
    strip_enqueue_time_from_job_rows(&shard, "-", "done").await;
    strip_enqueue_time_from_job_rows(&shard, "-", "waiting").await;
    let sql = engine(&shard);

    // (shape name, marker set, stripped rows inside the shape's ranges)
    let cases = [
        ("FullScan", true, 12.0),
        ("Status stored", false, 1.0),
        ("Status virtual", false, 1.0),
        ("StatusUnion", false, 2.0),
    ];
    for (name, marker, stripped_in_range) in cases {
        let query = INDEX_SERVED_SHAPES
            .iter()
            .find(|(shape, _)| *shape == name)
            .map(|(_, query)| *query)
            .expect("known shape");
        shard
            .set_enqueue_time_backfill_complete(marker)
            .await
            .unwrap();
        assert_eq!(explain_path(&sql, query).await, "status-index", "{name}");

        let before = snapshot(&metrics, &shard);
        let rows = id_enqueue_rows(&run(&sql, query).await);
        let after = snapshot(&metrics, &shard);

        assert!(!rows.is_empty(), "{name}: rows");
        assert_rows_match(&rows, &expected, name);
        assert_eq!(
            after.fallback - before.fallback,
            stripped_in_range,
            "{name}: one hydration per stripped row in range"
        );
        assert_eq!(
            after.point_lookups - before.point_lookups,
            stripped_in_range,
            "{name}: rows carrying the value are not looked up"
        );
    }
}

#[silo::test]
async fn row_whose_job_info_is_gone_is_dropped_without_error() {
    use silo::keys::job_info_key;

    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    let mut expected = seed_jobs(&shard, "-", 5).await;
    strip_enqueue_time_from_job_rows(&shard, "-", "future000").await;
    shard
        .db()
        .delete(&job_info_key("-", "future000"))
        .await
        .expect("delete JOB_INFO");
    expected.remove("future000");
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    let sql = engine(&shard);

    let rows = id_enqueue_rows(
        &run(
            &sql,
            "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-'",
        )
        .await,
    );
    assert!(
        !rows.iter().any(|(id, _)| id == "future000"),
        "row without JOB_INFO must be absent: {rows:?}"
    );
    assert_eq!(rows.len(), expected.len());
    assert_rows_match(&rows, &expected, "after drop");

    // A dropped row does not count toward the LIMIT: the scan pulls the next
    // entry in its place.
    let limit = expected.len();
    let limited = id_enqueue_rows(
        &run(
            &sql,
            &format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT {limit}"),
        )
        .await,
    );
    assert_eq!(limited.len(), limit, "LIMIT filled past the dropped row");
    assert_rows_match(&limited, &expected, "limited after drop");
}

#[silo::test]
async fn waiting_arm_labels_rows_from_its_own_snapshot() {
    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    seed_jobs(&shard, "-", 5).await;
    let sql = engine(&shard);

    for round in 0..5 {
        let id = format!("edge{round}");
        enqueue_job(&shard, "-", &id, now_ms() + 15).await;
        let union = run(
            &sql,
            "SELECT id, status_kind, enqueue_time_ms FROM jobs WHERE tenant = '-' AND status_kind IN ('Waiting', 'Scheduled')",
        )
        .await;
        let ids = string_column(&union, "id");
        assert_eq!(
            ids.iter().filter(|x| **x == id).count(),
            1,
            "{id} must appear in exactly one arm: {ids:?}"
        );
        let waiting = run(
            &sql,
            "SELECT id, status_kind, enqueue_time_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
        )
        .await;
        let labels = string_column(&waiting, "status_kind");
        assert!(
            labels.iter().all(|label| label == "Waiting"),
            "Waiting arm must label every row Waiting: {labels:?}"
        );
    }
}

#[silo::test]
async fn listing_query_matches_the_job_record_path() {
    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    let expected = seed_jobs(&shard, "-", 50).await;
    let sql = engine(&shard);
    let query = "SELECT id FROM (SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT 50000) ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101";

    let job_record_order = string_column(&run(&sql, query).await, "id");
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    let index_order = string_column(&run(&sql, query).await, "id");

    let mut want: Vec<(String, i64)> = expected.into_iter().collect();
    want.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));
    let want: Vec<String> = want.into_iter().map(|(id, _)| id).collect();
    assert_eq!(job_record_order, want, "job-record path order");
    assert_eq!(index_order, want, "index path order");
}

#[silo::test]
async fn limit_short_circuits_the_index_scan() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let now = now_ms();
    for i in 0..400 {
        enqueue_job(&shard, "-", &format!("j{i:04}"), now + 3_600_000 + i).await;
    }
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    let sql = engine(&shard);
    let query = "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT 10";
    assert_eq!(explain_path(&sql, query).await, "status-index");

    let before = snapshot(&metrics, &shard);
    let rows = id_enqueue_rows(&run(&sql, query).await);
    let after = snapshot(&metrics, &shard);

    assert_eq!(rows.len(), 10);
    let scanned = after.scanned_keys - before.scanned_keys;
    assert!(scanned < 100.0, "LIMIT 10 scanned {scanned} keys");
}

#[silo::test]
async fn waiting_status_query_returns_the_transition_time() {
    let (_tmp, shard, _metrics) = open_temp_shard_with_metrics().await;
    seed_jobs(&shard, "-", 2).await;
    let status = shard.get_job_status("-", "waiting").await.unwrap().unwrap();
    let scheduled_start = status.next_attempt_starts_after_ms.unwrap();
    assert_ne!(status.changed_at_ms, scheduled_start);
    let sql = engine(&shard);
    let query =
        "SELECT id, status_changed_at_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'";

    for complete in [false, true] {
        shard
            .set_enqueue_time_backfill_complete(complete)
            .await
            .unwrap();
        assert_eq!(explain_path(&sql, query).await, "pairs");
        let batches = run(&sql, query).await;
        let ids = string_column(&batches, "id");
        assert_eq!(ids, vec!["waiting"]);
        let changed = batches[0]
            .column_by_name("status_changed_at_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(changed, status.changed_at_ms, "complete={complete}");
    }
}

/// The editor's listing shape on a tenant larger than production's problem
/// threshold. Imports 120k jobs, so it runs only on request:
/// `cargo test --test query_index_enqueue_time_tests -- --ignored`.
#[silo::test]
#[ignore]
async fn listing_query_on_a_100k_job_tenant_is_index_only() {
    use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};

    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let total = 120_000usize;
    let now = now_ms();
    for chunk_start in (0..total).step_by(500) {
        let params: Vec<ImportJobParams> = (chunk_start..(chunk_start + 500).min(total))
            .map(|i| {
                let enqueue_time_ms = now - total as i64 + i as i64;
                let attempts = if i % 2 == 0 {
                    vec![ImportedAttempt {
                        status: ImportedAttemptStatus::Succeeded { result: vec![] },
                        started_at_ms: enqueue_time_ms + 10,
                        finished_at_ms: enqueue_time_ms + 20,
                    }]
                } else {
                    vec![]
                };
                ImportJobParams {
                    id: format!("job-{i:07}"),
                    priority: 50,
                    enqueue_time_ms,
                    start_at_ms: now + 3_600_000,
                    retry_policy: None,
                    payload: msgpack_payload(&serde_json::json!({"i": i})),
                    limits: vec![],
                    metadata: None,
                    task_group: "default".to_string(),
                    attempts,
                }
            })
            .collect();
        let results = shard.import_jobs("-", params).await.expect("import");
        assert!(
            results.iter().all(|r| r.success),
            "import batch at {chunk_start}"
        );
    }
    let sql = engine(&shard);

    // Both paths see every row when the sample bound exceeds the tenant, so
    // their results must agree exactly.
    let whole_tenant = "SELECT id FROM (SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT 200000) ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101";
    assert_eq!(explain_path(&sql, whole_tenant).await, "fullscan-join");
    let job_record_order = string_column(&run(&sql, whole_tenant).await, "id");
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .unwrap();
    assert_eq!(explain_path(&sql, whole_tenant).await, "status-index");
    let before = snapshot(&metrics, &shard);
    let index_order = string_column(&run(&sql, whole_tenant).await, "id");
    let after = snapshot(&metrics, &shard);
    assert_eq!(index_order.len(), 101);
    assert_eq!(index_order, job_record_order, "same rows in the same order");
    assert_eq!(
        after.point_lookups, before.point_lookups,
        "zero JOB_INFO point lookups"
    );
    assert_eq!(after.fallback, before.fallback, "zero fallback hydrations");

    // The production shape (50k sample) is index-only as well.
    let production = "SELECT id FROM (SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' LIMIT 50000) ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101";
    assert_eq!(explain_path(&sql, production).await, "status-index");
    let before = snapshot(&metrics, &shard);
    let rows = string_column(&run(&sql, production).await, "id");
    let after = snapshot(&metrics, &shard);
    assert_eq!(rows.len(), 101);
    assert_eq!(after.point_lookups, before.point_lookups);
    assert_eq!(after.fallback, before.fallback);
}
