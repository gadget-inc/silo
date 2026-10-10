mod grpc_integration_helpers;
mod test_helpers;

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::StringArray;
use datafusion::common::ScalarValue;
use futures::{StreamExt, stream};
use silo::job_store_shard::JobStoreShard;
use silo::query::ShardQueryEngine;
use test_helpers::*;

async fn enqueue(
    shard: &JobStoreShard,
    tenant: &str,
    id: &str,
    queue: &str,
    action: &str,
    start: i64,
) {
    shard
        .enqueue(
            tenant,
            Some(id.to_string()),
            10,
            start,
            None,
            msgpack_payload(&serde_json::json!({"test": true})),
            vec![],
            Some(vec![
                ("queue".to_string(), queue.to_string()),
                ("actionApiIdentifier".to_string(), action.to_string()),
            ]),
            "default",
        )
        .await
        .expect("enqueue");
}

fn predicate(prefix: &str) -> String {
    format!(
        "(starts_with(lower(id), '{prefix}') OR starts_with(lower(array_any_value(element_at(metadata, 'queue'))), '{prefix}') OR starts_with(lower(array_any_value(element_at(metadata, 'actionApiIdentifier'))), '{prefix}'))"
    )
}

async fn strings(engine: &ShardQueryEngine, sql: &str) -> Vec<String> {
    engine
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("query")
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column")
                .iter()
                .map(|s| s.expect("non-null").to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[silo::test]
async fn search_preserves_case_insensitive_or_semantics_filters_and_pagination() {
    let (_tmp, shard) = open_temp_shard().await;
    let future = now_ms() + 60_000;
    enqueue(&shard, "a", "id-match", "MiXeD", "mixedAction", future).await;
    enqueue(&shard, "a", "MiXeD-id", "other", "other", future).await;
    enqueue(&shard, "a", "action-match", "other", "MIXED-action", future).await;
    enqueue(&shard, "a", "waiting", "mixed", "other", 0).await;
    enqueue(&shard, "a", "cancelled", "mixed", "other", future).await;
    shard.cancel_job("a", "cancelled").await.expect("cancel");
    enqueue(&shard, "b", "foreign", "mixed", "other", future).await;
    shard
        .set_enqueue_time_index_complete(true)
        .await
        .expect("index complete");
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let sql = format!(
        "SELECT id FROM jobs WHERE tenant = 'a' AND status_kind = 'Scheduled' AND current_attempt = 1 AND priority = 10 AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 2",
        predicate("mixed")
    );
    let plan = engine.explain(&sql).await.expect("explain");
    assert!(plan.contains("SearchEnqueueTimeIndex"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
    assert_eq!(strings(&engine, &sql).await, ["MiXeD-id", "action-match"]);
    let parameterized = engine
        .sql(&sql.replace("'a'", "$1").replace("'mixed'", "$2"))
        .await
        .expect("parameterized SQL")
        .with_param_values(vec![
            ScalarValue::Utf8(Some("a".into())),
            ScalarValue::Utf8(Some("mixed".into())),
        ])
        .expect("bind parameters")
        .collect()
        .await
        .expect("query");
    let parameterized_ids: Vec<_> = parameterized
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("IDs")
                .iter()
                .flatten()
        })
        .collect();
    assert_eq!(parameterized_ids, ["MiXeD-id", "action-match"]);
    assert_eq!(
        strings(&engine, &format!("{sql} OFFSET 2")).await,
        ["id-match"]
    );
    let retry = sql.replace("current_attempt = 1", "current_attempt > 1");
    assert!(strings(&engine, &retry).await.is_empty());
    let wrong_priority = sql.replace("priority = 10", "priority = 20");
    assert!(strings(&engine, &wrong_priority).await.is_empty());
    for (status, expected) in [
        ("status_kind = 'Waiting'", vec!["waiting"]),
        ("status_kind = 'Cancelled'", vec!["cancelled"]),
        (
            "status_kind IN ('Waiting', 'Cancelled')",
            vec!["cancelled", "waiting"],
        ),
        (
            "(status_kind = 'Waiting' OR status_kind = 'Cancelled')",
            vec!["cancelled", "waiting"],
        ),
        (
            "status_kind = 'Waiting' AND status_kind = 'Cancelled'",
            vec![],
        ),
    ] {
        let filtered = format!(
            "SELECT id FROM jobs WHERE tenant = 'a' AND {status} AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 10",
            predicate("mixed")
        );
        assert_eq!(strings(&engine, &filtered).await, expected, "{filtered}");
    }
    let metadata_and_search = format!(
        "SELECT id FROM jobs WHERE tenant = 'a' AND array_contains(element_at(metadata, 'queue'), 'other') AND status_kind = 'Scheduled' AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 10",
        predicate("mixed")
    );
    assert_eq!(
        strings(&engine, &metadata_and_search).await,
        ["MiXeD-id", "action-match"]
    );
    let id_and_search = format!(
        "SELECT id FROM jobs WHERE tenant = 'a' AND id = 'waiting' AND {} LIMIT 10",
        predicate("mixed")
    );
    assert_eq!(strings(&engine, &id_and_search).await, ["waiting"]);
    assert!(
        !engine
            .explain(&id_and_search)
            .await
            .expect("plan")
            .contains("SearchEnqueueTimeIndex")
    );
    let indexed = strings(&engine, &sql.replace("LIMIT 2", "LIMIT 100")).await;
    shard
        .set_enqueue_time_index_complete(false)
        .await
        .expect("disable index");
    assert_eq!(
        strings(&engine, &sql.replace("LIMIT 2", "LIMIT 100")).await,
        indexed
    );
    shard.close().await.expect("close");
}

#[silo::test]
async fn search_page_limit_is_applied_after_residual_filters() {
    let (_tmp, shard) = open_temp_shard().await;
    for (id, priority) in [("a-rejected", 1), ("b-rejected", 1), ("c-match", 10)] {
        shard
            .enqueue(
                "a",
                Some(id.into()),
                priority,
                now_ms() + 60_000,
                None,
                msgpack_payload(&serde_json::json!({})),
                vec![],
                Some(vec![("queue".into(), "mixed".into())]),
                "default",
            )
            .await
            .expect("enqueue");
    }
    shard
        .set_enqueue_time_index_complete(true)
        .await
        .expect("index");
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let sql = format!(
        "SELECT id FROM jobs WHERE tenant = 'a' AND priority = 10 AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 1",
        predicate("mixed")
    );
    assert_eq!(strings(&engine, &sql).await, ["c-match"]);
    shard.close().await.expect("close");
}

#[silo::test]
async fn search_preserves_like_patterns_after_prefix_rewrite() {
    let (_tmp, shard) = open_temp_shard().await;
    for (id, queue) in [
        ("literal", "Q_%\\end"),
        ("other", "Q-anything"),
        ("slash", "Q\\end"),
        ("unicode", "Éclair"),
    ] {
        enqueue(&shard, "a", id, queue, "noop", now_ms() + 60_000).await;
    }
    shard
        .set_enqueue_time_index_complete(true)
        .await
        .expect("index complete");
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let sql = format!(
        "SELECT id FROM jobs WHERE tenant = 'a' AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 10",
        predicate("q_%")
    );
    assert_eq!(strings(&engine, &sql).await, ["literal"]);
    assert!(
        engine
            .explain(&sql)
            .await
            .expect("plan")
            .contains("SearchEnqueueTimeIndex")
    );
    for prefix in ["q_%", "q\\", "q", "é", "Q"] {
        let sql = format!(
            "SELECT id FROM jobs WHERE tenant = 'a' AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 10",
            predicate(prefix)
        );
        let indexed = strings(&engine, &sql).await;
        shard
            .set_enqueue_time_index_complete(false)
            .await
            .expect("disable index");
        assert_eq!(strings(&engine, &sql).await, indexed, "{sql}");
        shard
            .set_enqueue_time_index_complete(true)
            .await
            .expect("enable index");
    }
    shard.close().await.expect("close");
}

#[silo::test]
async fn pending_names_include_later_queues_and_exclude_cancelled_and_other_tenants() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    enqueue(&shard, "a", "running", "running-only", "noop", 0).await;
    assert_eq!(
        shard
            .dequeue("worker", "default", 1)
            .await
            .expect("dequeue")
            .tasks
            .len(),
        1
    );
    for i in 0..300 {
        enqueue(
            &shard,
            "a",
            &format!("first-{i}"),
            "first",
            "noop",
            now_ms() + 60_000,
        )
        .await;
    }
    enqueue(&shard, "a", "later", "Later", "noop", now_ms() + 60_000).await;
    enqueue(&shard, "a", "waiting", "later", "noop", 0).await;
    enqueue(
        &shard,
        "a",
        "cancelled",
        "cancelled-only",
        "noop",
        now_ms() + 60_000,
    )
    .await;
    shard.cancel_job("a", "cancelled").await.expect("cancel");
    enqueue(
        &shard,
        "b",
        "foreign",
        "foreign-only",
        "noop",
        now_ms() + 60_000,
    )
    .await;
    // Force discovery's status-index fallback after probing terminal history.
    for i in 0..40 {
        let id = format!("cancelled-{i}");
        enqueue(
            &shard,
            "a",
            &id,
            "cancelled-only",
            "noop",
            now_ms() + 60_000,
        )
        .await;
        shard.cancel_job("a", &id).await.expect("cancel");
    }
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let before = metrics.query_point_lookups_value(shard.name());
    assert_eq!(strings(&engine, "SELECT value FROM pending_metadata WHERE tenant = 'a' AND key = 'queue' ORDER BY value LIMIT 2").await, ["Later", "first"]);
    assert_eq!(strings(&engine, "SELECT value FROM pending_metadata WHERE tenant = 'a' AND key = 'queue' AND value > 'first' ORDER BY value LIMIT 2").await, ["later"]);
    let probes = metrics.query_point_lookups_value(shard.name()) - before;
    assert!(
        probes > 0.0 && probes <= 128.0,
        "bounded status probes: {probes}"
    );
    assert!(
        strings(
            &engine,
            "SELECT value FROM pending_metadata WHERE tenant = 'empty' AND key = 'queue'"
        )
        .await
        .is_empty()
    );
    assert!(
        strings(
            &engine,
            "SELECT value FROM pending_metadata WHERE tenant = 'a' AND key = 'missing'"
        )
        .await
        .is_empty()
    );
    shard.close().await.expect("close");
}

#[silo::test]
async fn pending_names_seek_past_previous_pages_before_reading_statuses() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let future = now_ms() + 60_000;
    for i in 0..200 {
        enqueue(
            &shard,
            "a",
            &format!("job-{i}"),
            &format!("queue-{i:04}"),
            "noop",
            future,
        )
        .await;
    }
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let before = metrics.query_point_lookups_value(shard.name());
    assert_eq!(strings(&engine, "SELECT value FROM pending_metadata WHERE tenant = 'a' AND key = 'queue' AND value > 'queue-0197' ORDER BY value LIMIT 2").await, ["queue-0198", "queue-0199"]);
    assert_eq!(
        metrics.query_point_lookups_value(shard.name()) - before,
        2.0,
        "a later page must not recheck the pending status of earlier names"
    );
    let before = metrics.query_point_lookups_value(shard.name());
    assert!(strings(&engine, "SELECT value FROM pending_metadata WHERE tenant = 'a' AND key = 'queue' AND value > 'queue-0199' ORDER BY value LIMIT 2").await.is_empty());
    assert_eq!(
        metrics.query_point_lookups_value(shard.name()) - before,
        0.0
    );
    shard.close().await.expect("close");
}

#[silo::test]
async fn pending_names_preserve_string_range_boundaries() {
    let (_tmp, shard) = open_temp_shard().await;
    let values = ["", " ", "A", "a", "a\0", "aa", "b", "é", "😀"];
    for (i, value) in values.iter().enumerate() {
        enqueue(
            &shard,
            "a",
            &format!("job-{i}"),
            value,
            "noop",
            now_ms() + 60_000,
        )
        .await;
    }
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    for (condition, expected) in [
        ("value = 'a'", vec!["a"]),
        ("value > 'a' AND value < 'b'", vec!["a\0", "aa"]),
        ("value >= 'a' AND value <= 'b'", vec!["a", "a\0", "aa", "b"]),
        ("'a' < value AND 'é' >= value", vec!["a\0", "aa", "b", "é"]),
        ("value > 'b' AND value < 'a'", vec![]),
        ("value >= 'a' AND value < 'a'", vec![]),
        ("value > 'a' AND value > 'b'", vec!["é", "😀"]),
        ("value = 'missing'", vec![]),
    ] {
        let sql = format!(
            "SELECT value FROM pending_metadata WHERE tenant = 'a' AND key = 'queue' AND {condition} ORDER BY value"
        );
        assert_eq!(strings(&engine, &sql).await, expected, "{sql}");
    }
    shard.close().await.expect("close");
}

#[silo::test]
async fn pending_names_work_through_cluster_and_grpc_query_engines() {
    use grpc_integration_helpers::{
        TEST_SHARD_ID, create_test_factory, setup_test_server, shutdown_server,
    };
    use silo::cluster_query::ClusterQueryEngine;
    use silo::pb::{
        QueryArrowRequest, QueryParameter, QueryRequest, query_parameter, serialized_bytes,
    };
    use silo::settings::AppConfig;
    use silo::shard_range::ShardId;

    let (factory, _tmp) = create_test_factory().await.expect("factory");
    let shard = factory
        .get(&ShardId::parse(TEST_SHARD_ID).expect("shard ID"))
        .expect("shard");
    for (tenant, id, queue) in [
        ("a", "a", "first"),
        ("a", "b", "second"),
        ("a", "c", "second"),
        ("b", "d", "foreign"),
    ] {
        enqueue(&shard, tenant, id, queue, "noop", now_ms() + 60_000).await;
    }
    let engine = ClusterQueryEngine::new(Arc::clone(&factory), None)
        .await
        .expect("cluster engine");
    let sql = "SELECT value AS queue_name FROM pending_metadata WHERE tenant = 'a' AND key = 'queue' AND value > 'first' ORDER BY value LIMIT 101";
    let batches = engine
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("cluster query");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("names")
            .value(0),
        "second"
    );

    let (mut client, shutdown, server, _) =
        setup_test_server(Arc::clone(&factory), AppConfig::load(None).expect("config"))
            .await
            .expect("server");
    let parameters = ["a", "queue", "first"]
        .into_iter()
        .map(|value| QueryParameter {
            value: Some(query_parameter::Value::StringValue(value.into())),
        })
        .collect();
    let result = client.query(QueryRequest {
        shard: TEST_SHARD_ID.into(),
        tenant: Some("a".into()),
        sql: "SELECT value AS queue_name FROM pending_metadata WHERE tenant = $1 AND key = $2 AND value > $3 ORDER BY value LIMIT 101".into(),
        parameters,
    }).await.expect("bound query").into_inner();
    assert_eq!(result.row_count, 1);
    let Some(serialized_bytes::Encoding::Msgpack(bytes)) = &result.rows[0].encoding else {
        panic!("msgpack row")
    };
    let row: serde_json::Value = rmp_serde::from_slice(bytes).expect("decode");
    assert_eq!(row["queue_name"], "second");

    // The cluster's remote path uses Arrow with the tenant in SQL, not the RPC.
    let mut stream = client
        .query_arrow(QueryArrowRequest {
            shard: TEST_SHARD_ID.into(),
            tenant: None,
            sql: sql.into(),
            parameters: vec![],
        })
        .await
        .expect("arrow query")
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend(chunk.expect("arrow chunk").ipc_data);
    }
    let batches: Vec<_> =
        datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .expect("arrow reader")
            .collect::<Result<_, _>>()
            .expect("arrow batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("names")
            .value(0),
        "second"
    );
    shutdown_server(shutdown, server).await.expect("shutdown");
    shard.close().await.expect("close");
}

#[silo::test]
async fn large_queue_search_bounds_job_record_hydration() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    // Set to 100000 for the 300,000-job reproduction; the regular regression uses 3000.
    let per_queue = std::env::var("QUEUE_SEARCH_TEST_JOBS_PER_QUEUE")
        .ok()
        .map(|v| v.parse::<usize>().expect("job count"))
        .unwrap_or(1000);
    let future = now_ms() + 86_400_000;
    stream::iter(0..per_queue * 3)
        .for_each_concurrent(32, |i| {
            let shard = Arc::clone(&shard);
            async move {
                let queue = format!("queue-test-{}", i / per_queue + 1);
                enqueue(
                    &shard,
                    "large",
                    &format!("job:{queue}:{:06}", i % per_queue),
                    &queue,
                    "queueTestNoop",
                    future,
                )
                .await;
            }
        })
        .await;
    shard
        .set_enqueue_time_index_complete(true)
        .await
        .expect("index complete");
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let sql = format!(
        "SELECT id FROM jobs WHERE tenant = 'large' AND status_kind = 'Scheduled' AND current_attempt = 1 AND {} ORDER BY enqueue_time_ms DESC, id ASC LIMIT 101",
        predicate("queue-test-2")
    );
    let before = metrics.query_point_lookups_value(shard.name());
    let start = Instant::now();
    let rows = tokio::time::timeout(Duration::from_secs(5), strings(&engine, &sql))
        .await
        .expect("listing under statement deadline");
    eprintln!("{} jobs: listing {:?}", per_queue * 3, start.elapsed());
    assert_eq!(rows.len(), 101);
    assert!(rows.iter().all(|id| id.contains(":queue-test-2:")));
    let point_lookups = metrics.query_point_lookups_value(shard.name()) - before;
    eprintln!("point lookups: {point_lookups}");
    let partitions = datafusion::execution::context::SessionConfig::new().target_partitions();
    assert!(
        point_lookups <= (256 * (partitions * 2 + 2)) as f64,
        "hydrate bounded batches for parallel filter prefetch, not the backlog"
    );
    let before_discovery = metrics.query_point_lookups_value(shard.name());
    let start = Instant::now();
    let names = tokio::time::timeout(Duration::from_secs(5), strings(&engine, "SELECT value FROM pending_metadata WHERE tenant = 'large' AND key = 'queue' ORDER BY value LIMIT 101")).await.expect("discovery under statement deadline");
    eprintln!("{} jobs: discovery {:?}", per_queue * 3, start.elapsed());
    assert_eq!(names, ["queue-test-1", "queue-test-2", "queue-test-3"]);
    assert_eq!(
        metrics.query_point_lookups_value(shard.name()) - before_discovery,
        3.0
    );
    shard.close().await.expect("close");
}
