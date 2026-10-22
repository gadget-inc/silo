//! The jobs scanner's metadata index path: a tenant-scoped metadata exact or
//! prefix filter whose projection stays within the columns a metadata index
//! entry carries is served from the index alone on a version-3 shard,
//! returning exactly what the hydrating path returns.

mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use silo::job_store_shard::{
    JobStoreShard, METADATA_ENQUEUE_TIME_INDEX_VERSION, STATUS_ENQUEUE_TIME_INDEX_VERSION,
};
use silo::keys::idx_metadata_key;
use silo::query::ShardQueryEngine;
use test_helpers::*;

const METADATA_INDEX_LABEL: &str = "path=MetadataIndex";
const JOB_PAIRS_LABEL: &str = "path=JobPairs";

const EXACT: &str = "array_contains(element_at(metadata, 'env'), 'prod')";
const PREFIX: &str = "starts_with(array_any_value(element_at(metadata, 'action')), 'ship')";

/// Seed `count` jobs with an `env` of `prod` or `staging` and an `action`
/// that mostly starts with `ship`, spread over several enqueue times with
/// a few collisions, and dequeue some so statuses differ.
async fn seed(shard: &JobStoreShard, count: usize) {
    let now = now_ms();
    for i in 0..count {
        let env = if i % 3 == 0 { "staging" } else { "prod" };
        let action = match i % 4 {
            0 => "shipOrder",
            1 => "shipInvoice",
            2 => "ship",
            _ => "refund",
        };
        let enqueue_time = now - 60_000 - ((i / 2) as i64) * 1_000;
        shard
            .enqueue(
                "-",
                Some(format!("job-{i:04}")),
                10u8,
                enqueue_time,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                Some(vec![
                    ("env".to_string(), env.to_string()),
                    ("action".to_string(), action.to_string()),
                ]),
                "default",
            )
            .await
            .expect("enqueue");
    }
    let dequeued = shard
        .dequeue("worker", "default", 5)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(dequeued.len(), 5);
}

async fn set_version(shard: &JobStoreShard, version: u8) {
    shard
        .set_listing_index_version(version)
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
    let explain = engine.explain(query).await.expect("explain");
    plan_line(&explain)
}

async fn collect(engine: &ShardQueryEngine, query: &str) -> Vec<RecordBatch> {
    engine
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
}

/// `(id, enqueue_time_ms)` rows of a query projecting exactly those two
/// columns, in the order returned.
async fn id_time_rows(engine: &ShardQueryEngine, query: &str) -> Vec<(String, i64)> {
    let batches = collect(engine, query).await;
    let mut out = Vec::new();
    for batch in &batches {
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
            out.push((ids.value(i).to_string(), times.value(i)));
        }
    }
    out
}

async fn ids(engine: &ShardQueryEngine, query: &str) -> Vec<String> {
    let batches = collect(engine, query).await;
    let mut out = Vec::new();
    for batch in &batches {
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

async fn count_of(engine: &ShardQueryEngine, query: &str) -> i64 {
    let batches = collect(engine, query).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0)
}

fn sorted<T: Ord>(mut rows: Vec<T>) -> Vec<T> {
    rows.sort();
    rows
}

#[silo::test]
async fn key_served_metadata_projections_take_the_index_path_at_version_3() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 20).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let shapes = [
        format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT}"),
        format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {PREFIX}"),
        format!("SELECT id FROM jobs WHERE tenant = '-' AND {EXACT}"),
        format!("SELECT shard_id, tenant, id FROM jobs WHERE tenant = '-' AND {PREFIX}"),
        format!("SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND {EXACT}"),
        format!(
            "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 5"
        ),
    ];
    for version in [0, STATUS_ENQUEUE_TIME_INDEX_VERSION] {
        set_version(&shard, version).await;
        for query in &shapes {
            let line = explain_line(&engine, query).await;
            assert!(
                line.contains(JOB_PAIRS_LABEL) && !line.contains(METADATA_INDEX_LABEL),
                "version {version}: {query}: {line}"
            );
        }
    }
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    for query in &shapes {
        let line = explain_line(&engine, query).await;
        assert!(
            line.contains(METADATA_INDEX_LABEL),
            "version 3: {query}: {line}"
        );
        assert!(
            !line.contains("residual="),
            "the metadata and tenant filters are answered by the scan: {line}"
        );
    }

    // A residual predicate on a column the entry carries keeps the path and
    // is applied inside the scan.
    let query = format!("SELECT id FROM jobs WHERE tenant = '-' AND {EXACT} AND id LIKE 'job-00%'");
    let line = explain_line(&engine, &query).await;
    assert!(
        line.contains(METADATA_INDEX_LABEL) && line.contains("residual="),
        "{line}"
    );
    assert_eq!(
        sorted(ids(&engine, &query).await),
        sorted(
            ids(
                &engine,
                &format!("SELECT id FROM jobs WHERE tenant = '-' AND {EXACT}")
            )
            .await
            .into_iter()
            .filter(|id| id.starts_with("job-00"))
            .collect()
        )
    );
}

#[silo::test]
async fn other_projections_and_residuals_keep_the_job_pairs_path_at_version_3() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 20).await;
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let hydrating = [
        format!("SELECT id, payload FROM jobs WHERE tenant = '-' AND {EXACT}"),
        format!("SELECT id, priority FROM jobs WHERE tenant = '-' AND {PREFIX}"),
        format!("SELECT id, status_kind FROM jobs WHERE tenant = '-' AND {EXACT}"),
        format!("SELECT id, metadata FROM jobs WHERE tenant = '-' AND {EXACT}"),
        // A residual predicate on a column the entry does not carry widens
        // the scan projection past the index.
        format!("SELECT id FROM jobs WHERE tenant = '-' AND {EXACT} AND status_kind = 'Running'"),
        format!(
            "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {PREFIX} AND priority = 10"
        ),
    ];
    for query in &hydrating {
        let line = explain_line(&engine, query).await;
        assert!(
            line.contains(JOB_PAIRS_LABEL) && !line.contains(METADATA_INDEX_LABEL),
            "{query}: {line}"
        );
    }

    // Without a tenant there is no tenant-scoped index range to walk.
    let line = explain_line(
        &engine,
        &format!("SELECT id, enqueue_time_ms FROM jobs WHERE {EXACT}"),
    )
    .await;
    assert!(!line.contains(METADATA_INDEX_LABEL), "{line}");
}

#[silo::test]
async fn index_path_returns_the_same_rows_as_the_job_pairs_path() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 60).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    let shapes = [
        format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT}"),
        format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {PREFIX}"),
        format!(
            "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT} ORDER BY enqueue_time_ms DESC, id ASC"
        ),
        format!(
            "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {PREFIX} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 7 OFFSET 3"
        ),
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND array_contains(element_at(metadata, 'env'), 'nope')"
            .to_string(),
    ];
    for query in &shapes {
        set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
        assert!(explain_line(&engine, query).await.contains(JOB_PAIRS_LABEL));
        let baseline = id_time_rows(&engine, query).await;

        set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
        assert!(
            explain_line(&engine, query)
                .await
                .contains(METADATA_INDEX_LABEL)
        );
        let indexed = id_time_rows(&engine, query).await;
        if query.contains("ORDER BY") {
            assert_eq!(indexed, baseline, "{query}");
        } else {
            assert_eq!(sorted(indexed), sorted(baseline), "{query}");
        }
    }

    // The exact arm matches two thirds of the tenant, the prefix arm three
    // quarters; neither is empty.
    assert_eq!(
        ids(
            &engine,
            &format!("SELECT id FROM jobs WHERE tenant = '-' AND {EXACT}")
        )
        .await
        .len(),
        40
    );
    assert_eq!(
        ids(
            &engine,
            &format!("SELECT id FROM jobs WHERE tenant = '-' AND {PREFIX}")
        )
        .await
        .len(),
        45
    );
}

#[silo::test]
async fn counts_match_across_versions() {
    let (_tmp, shard) = open_temp_shard().await;
    seed(&shard, 30).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = format!("SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND {PREFIX}");

    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let baseline = count_of(&engine, &query).await;
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert!(
        explain_line(&engine, &query)
            .await
            .contains(METADATA_INDEX_LABEL)
    );
    assert_eq!(count_of(&engine, &query).await, baseline);
    assert_eq!(baseline, 23);
}

#[silo::test]
async fn index_path_reads_no_job_records_and_honours_the_scan_limit() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 200).await;
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");

    for (query, matches) in [
        (
            format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT}"),
            133usize,
        ),
        (
            format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {PREFIX}"),
            150,
        ),
    ] {
        assert!(
            explain_line(&engine, &query)
                .await
                .contains(METADATA_INDEX_LABEL)
        );
        let lookups_before = metrics.query_point_lookups_value(shard.name());
        let rows = id_time_rows(&engine, &query).await;
        let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
        assert_eq!(rows.len(), matches, "{query}");
        assert_eq!(
            lookups, 0.0,
            "{query}: the index path performs no point lookups"
        );
    }

    let limited =
        format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT} LIMIT 10");
    let scanned_before = metrics.query_scanned_keys_value(shard.name());
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let rows = id_time_rows(&engine, &limited).await;
    let scanned = metrics.query_scanned_keys_value(shard.name()) - scanned_before;
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
    assert_eq!(rows.len(), 10);
    assert!(
        scanned <= 10.0,
        "LIMIT 10 scanned {scanned} keys; the walk must stop at the limit"
    );
    assert_eq!(lookups, 0.0);

    // The same shape below version 3 hydrates every matching row.
    set_version(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let rows = id_time_rows(
        &engine,
        &format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT}"),
    )
    .await;
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
    assert_eq!(rows.len(), 133);
    assert_eq!(
        lookups, 133.0,
        "the job-pairs path reads every matching record"
    );
}

#[silo::test]
async fn an_entry_without_an_enqueue_time_is_hydrated_by_point_lookup() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 12).await;
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT}");
    let expected = sorted(id_time_rows(&engine, &query).await);

    // Rewrite two entries as a binary that predates version 3 would, and
    // plant one such entry for a job that does not exist.
    for id in ["job-0001", "job-0002"] {
        shard
            .db()
            .put(idx_metadata_key("-", "env", "prod", id), [])
            .await
            .expect("strip value");
    }
    shard
        .db()
        .put(idx_metadata_key("-", "env", "prod", "job-gone"), [])
        .await
        .expect("plant orphan");

    assert!(
        explain_line(&engine, &query)
            .await
            .contains(METADATA_INDEX_LABEL)
    );
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let rows = sorted(id_time_rows(&engine, &query).await);
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
    assert_eq!(
        rows, expected,
        "legacy entries are hydrated, the orphan dropped"
    );
    assert_eq!(lookups, 3.0, "one lookup per entry without an enqueue time");

    // A projection without the enqueue time needs no hydration at all, so
    // it reads the ids straight from the keys.
    let id_query = format!("SELECT id FROM jobs WHERE tenant = '-' AND {EXACT}");
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let listed = ids(&engine, &id_query).await;
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
    assert_eq!(lookups, 0.0);
    assert!(listed.contains(&"job-0001".to_string()));
}

#[silo::test]
async fn a_resolved_plan_keeps_its_path_after_the_version_drops() {
    use datafusion::execution::context::SessionContext;

    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    seed(&shard, 10).await;
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = format!("SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {EXACT}");
    let expected = sorted(id_time_rows(&engine, &query).await);
    let plan = engine.get_physical_plan(&query).await.expect("plan");
    let rendered = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert!(
        plan_line(&rendered).contains(METADATA_INDEX_LABEL),
        "{rendered}"
    );

    set_version(&shard, 0).await;
    let lookups_before = metrics.query_point_lookups_value(shard.name());
    let batches = datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx())
        .await
        .expect("collect");
    let lookups = metrics.query_point_lookups_value(shard.name()) - lookups_before;
    let mut rows = Vec::new();
    for batch in &batches {
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
    assert_eq!(sorted(rows), expected);
    assert_eq!(lookups, 0.0, "the plan must still execute the index path");
}

#[silo::test]
async fn cluster_engine_serves_the_shape_from_the_index_on_a_version_3_shard() {
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
    seed(&shard, 30).await;
    set_version(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;

    let engine = ClusterQueryEngine::new(Arc::clone(&factory), None)
        .await
        .expect("cluster engine");
    let query = format!(
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-' AND {PREFIX} ORDER BY enqueue_time_ms DESC, id ASC"
    );
    let batches = engine
        .sql(&query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
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
            assert!(!ids.is_null(i));
            rows.push((ids.value(i).to_string(), times.value(i)));
        }
    }
    let local = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    assert_eq!(rows, id_time_rows(&local, &query).await);
    assert_eq!(rows.len(), 23);
}
