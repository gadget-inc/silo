#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use silo::job_store_shard::{ENQUEUE_TIME_INDEX_VERSION, STATUS_ENQUEUE_TIME_INDEX_VERSION};
use silo::query::ShardQueryEngine;
use std::sync::Arc;
use std::time::Instant;

const WARMUP_ITERS: usize = 1;
const MEASURED_ITERS: usize = 5;

/// Rows per IDE page.
const PAGE: usize = 100;
/// Offset of the deep page each tab shape is also measured at.
const DEEP_OFFSET: usize = 2000;
/// The priority the pinned shapes select; one of the golden priorities.
const PINNED_PRIORITY: i64 = 50;

/// Run a query multiple times and collect timing stats.
async fn bench_query(engine: &ShardQueryEngine, label: &str, query: &str) -> BenchResult {
    bench_bound_query(engine, label, query, &[]).await
}

/// Execute a query with its placeholders bound, the way the IDE sends every
/// value, and collect the batches.
async fn run_bound_query(
    engine: &ShardQueryEngine,
    query: &str,
    params: &[ScalarValue],
) -> Vec<RecordBatch> {
    let df = engine.sql(query).await.expect("sql");
    let df = if params.is_empty() {
        df
    } else {
        df.with_param_values(params.to_vec()).expect("bind params")
    };
    df.collect().await.expect("collect")
}

/// Run a bound query multiple times and collect timing stats.
async fn bench_bound_query(
    engine: &ShardQueryEngine,
    label: &str,
    query: &str,
    params: &[ScalarValue],
) -> BenchResult {
    for _ in 0..WARMUP_ITERS {
        run_bound_query(engine, query, params).await;
    }

    let mut durations = Vec::with_capacity(MEASURED_ITERS);
    for _ in 0..MEASURED_ITERS {
        let start = Instant::now();
        run_bound_query(engine, query, params).await;
        durations.push(start.elapsed());
    }

    durations.sort();
    BenchResult {
        label: label.to_string(),
        durations,
    }
}

/// Pick a known job ID for exact-ID lookup benchmark.
fn known_job_id(tenant: &str) -> String {
    format!("{}-{:08}", tenant, 0)
}

#[tokio::main]
async fn main() {
    let metadata = ensure_golden_shard().await;
    let tenant_sizes = compute_tenant_sizes();
    let total_jobs = metadata.total_jobs;

    // Open shard read-only (larger flush interval since we're only reading)
    let shard = open_golden_shard_readonly(&metadata).await;

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("query engine");
    assert!(
        !shard.enqueue_time_index_complete(),
        "the read-only open must not have backfilled the cached golden shard"
    );

    // Sanity check: total count
    let df = engine
        .sql("SELECT COUNT(*) as cnt FROM jobs")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");
    let actual_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("int64")
        .value(0);

    println!("\n========================================");
    println!("Query Performance Benchmark");
    println!("========================================\n");
    println!(
        "Dataset: {} jobs across {} tenants (expected ~{}, actual {})",
        total_jobs, NUM_TENANTS, total_jobs, actual_count
    );

    let large_tenant = &tenant_sizes[0].0; // tenant_001, ~796k
    let large_count = tenant_sizes[0].1;
    let small_tenant = &tenant_sizes[249].0; // tenant_250, ~3.2k
    let small_count = tenant_sizes[249].1;

    println!("Large tenant: {} (~{} jobs)", large_tenant, large_count);
    println!("Small tenant: {} (~{} jobs)\n", small_tenant, small_count);

    // --- Cross-Tenant Queries ---
    println!("--- Cross-Tenant Queries ---");

    let r = bench_query(&engine, "total_count", "SELECT COUNT(*) FROM jobs").await;
    r.print();

    let r = bench_query(
        &engine,
        "top_20_active_tenants",
        "SELECT tenant, COUNT(*) as cnt FROM jobs WHERE status_kind NOT IN ('Succeeded','Failed','Cancelled') GROUP BY tenant ORDER BY cnt DESC LIMIT 20",
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "tenant_status_counts",
        "SELECT tenant, status_kind, cnt FROM tenant_counts",
    )
    .await;
    r.print();

    println!();

    // --- Per-Tenant Queries (Large Tenant) ---
    println!(
        "--- Large Tenant ({}, ~{} jobs) ---",
        large_tenant, large_count
    );
    run_tenant_queries(&engine, large_tenant).await;
    println!();

    // --- Per-Tenant Queries (Small Tenant) ---
    println!(
        "--- Small Tenant ({}, ~{} jobs) ---",
        small_tenant, small_count
    );
    run_tenant_queries(&engine, small_tenant).await;
    println!();

    // --- Queue Count Queries ---
    println!(
        "--- Queue Count Queries (deep-queue: {} holders + {} waiters) ---",
        DEEP_QUEUE_HOLDERS, DEEP_QUEUE_WAITERS
    );

    let r = bench_query(
        &engine,
        "count_all_holders",
        "SELECT COUNT(*) FROM queues WHERE entry_type = 'holder'",
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "count_all_requesters",
        "SELECT COUNT(*) FROM queues WHERE entry_type = 'requester'",
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "count_holders_by_tenant",
        &format!(
            "SELECT COUNT(*) FROM queues WHERE tenant = '{}' AND entry_type = 'holder'",
            BENCH_DEEP_QUEUE_TENANT
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "count_requesters_by_tenant",
        &format!(
            "SELECT COUNT(*) FROM queues WHERE tenant = '{}' AND entry_type = 'requester'",
            BENCH_DEEP_QUEUE_TENANT
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "count_holders_by_queue",
        &format!(
            "SELECT COUNT(*) FROM queues WHERE tenant = '{}' AND queue_name = '{}' AND entry_type = 'holder'",
            BENCH_DEEP_QUEUE_TENANT, DEEP_QUEUE_KEY
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "count_requesters_by_queue",
        &format!(
            "SELECT COUNT(*) FROM queues WHERE tenant = '{}' AND queue_name = '{}' AND entry_type = 'requester'",
            BENCH_DEEP_QUEUE_TENANT, DEEP_QUEUE_KEY
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "queue_breakdown",
        &format!(
            "SELECT queue_name, entry_type, COUNT(*) FROM queues WHERE tenant = '{}' GROUP BY queue_name, entry_type",
            BENCH_DEEP_QUEUE_TENANT
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "list_holders_for_queue",
        &format!(
            "SELECT * FROM queues WHERE tenant = '{}' AND queue_name = '{}' AND entry_type = 'holder'",
            BENCH_DEEP_QUEUE_TENANT, DEEP_QUEUE_KEY
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "first_page_requesters",
        &format!(
            "SELECT * FROM queues WHERE tenant = '{}' AND queue_name = '{}' AND entry_type = 'requester' LIMIT 20",
            BENCH_DEEP_QUEUE_TENANT, DEEP_QUEUE_KEY
        ),
    )
    .await;
    r.print();

    println!();

    // --- Queue Counts Queries (WebUI queue_counts table) ---
    println!("--- Queue Counts Queries (queue_counts table with max_concurrency) ---");

    let r = bench_query(&engine, "queue_counts_all", "SELECT * FROM queue_counts").await;
    r.print();

    let r = bench_query(
        &engine,
        "queue_counts_by_tenant",
        &format!(
            "SELECT * FROM queue_counts WHERE tenant = '{}'",
            BENCH_DEEP_QUEUE_TENANT
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "queue_counts_by_queue_name",
        &format!(
            "SELECT * FROM queue_counts WHERE queue_name = '{}'",
            DEEP_QUEUE_KEY
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        &engine,
        "queue_counts_limit_only",
        &format!(
            "SELECT max_concurrency, limit_type FROM queue_counts WHERE queue_name = '{}' LIMIT 1",
            DEEP_QUEUE_KEY
        ),
    )
    .await;
    r.print();

    println!();

    // The read-only open passes the default (disabled) backfill setting, so
    // no sweep can have run against the cached golden shard; check again
    // here, after every open-time task has had time to act.
    assert!(
        !shard.enqueue_time_index_complete(),
        "the cached golden shard must not have been backfilled"
    );
    shard.close().await.expect("close shard");

    run_listing_benchmarks(&metadata, large_tenant).await;
    println!();

    println!("Done.");
}

/// The editor's listing page as it should be written for the enqueue-time
/// index: newest first, `id ASC` tiebreak, one page of 101 rows.
fn ordered_listing_query(tenant: &str, offset: usize) -> String {
    format!(
        "SELECT id FROM jobs WHERE tenant = '{tenant}' ORDER BY enqueue_time_ms DESC, id ASC LIMIT 101 OFFSET {offset}"
    )
}

/// The editor's listing shape as Gadget sends it: a bounded sample of one
/// tenant ordered newest-first with an `id DESC` tiebreak, which keeps a
/// sort over the sample even on the index path.
fn gadget_listing_query(tenant: &str) -> String {
    format!(
        "SELECT id FROM (SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '{tenant}' LIMIT 50000) ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101"
    )
}

/// The first column of every batch, as strings.
fn first_column_strings(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("string column");
            (0..b.num_rows())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The first value of an `Int64` single-column result.
fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("int64 column")
                .value(0)
        })
        .expect("a row")
}

/// What one execution of a query cost, read from the clone's query counters
/// around it, with the batches it returned.
struct ScanCost {
    batches: Vec<RecordBatch>,
    scanned_keys: u64,
    point_lookups: u64,
    arms: u64,
}

async fn measure_scan(
    engine: &ShardQueryEngine,
    metrics: &silo::metrics::Metrics,
    shard_name: &str,
    query: &str,
    params: &[ScalarValue],
) -> ScanCost {
    let keys = metrics.query_scanned_keys_value(shard_name);
    let lookups = metrics.query_point_lookups_value(shard_name);
    let arms = metrics.query_listing_index_arms_value(shard_name);
    let batches = run_bound_query(engine, query, params).await;
    ScanCost {
        batches,
        scanned_keys: (metrics.query_scanned_keys_value(shard_name) - keys) as u64,
        point_lookups: (metrics.query_point_lookups_value(shard_name) - lookups) as u64,
        arms: (metrics.query_listing_index_arms_value(shard_name) - arms) as u64,
    }
}

/// Run `bench_query` and also report the index keys one execution scans, read
/// from the clone's scanned-keys counter. Returns that execution's first
/// column so callers can check the states return the same rows.
async fn bench_query_with_keys(
    engine: &ShardQueryEngine,
    metrics: &silo::metrics::Metrics,
    shard_name: &str,
    label: &str,
    query: &str,
) -> Vec<String> {
    let cost = measure_scan(engine, metrics, shard_name, query, &[]).await;
    let rows: Vec<String> = first_column_strings(&cost.batches);
    let r = bench_query(engine, label, query).await;
    r.print();
    println!(
        "  {:<30} rows={} scanned_keys={} lookups={}",
        "",
        rows.len(),
        cost.scanned_keys,
        cost.point_lookups
    );
    rows
}

fn utf8(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_string()))
}

/// One IDE tab shape: the predicates after `tenant = $1`, the values bound
/// after the tenant, whether the shape pins a priority, and, for the
/// clock-split tabs, the predicate selecting the rows the split rejects
/// inside the segments the tab walks.
struct TabShape {
    name: &'static str,
    predicate: &'static str,
    params: Vec<ScalarValue>,
    pinned: bool,
    rejected: Option<(&'static str, Vec<ScalarValue>)>,
}

/// The seven state tabs, All with a priority pin, and one state tab with a
/// priority pin.
fn tab_shapes() -> Vec<TabShape> {
    let stored = |name: &'static str, kind: &str| TabShape {
        name,
        predicate: "status_kind = $2",
        params: vec![utf8(kind)],
        pinned: false,
        rejected: None,
    };
    vec![
        TabShape {
            name: "scheduled",
            predicate: "status_kind = $2 AND current_attempt = 1",
            params: vec![utf8("Scheduled")],
            pinned: false,
            rejected: Some((
                "status_kind = $2 AND current_attempt = 1",
                vec![utf8("Waiting")],
            )),
        },
        TabShape {
            name: "retrying",
            predicate: "status_kind = $2 AND current_attempt > 1",
            params: vec![utf8("Scheduled")],
            pinned: false,
            rejected: Some((
                "status_kind = $2 AND current_attempt > 1",
                vec![utf8("Waiting")],
            )),
        },
        TabShape {
            name: "waiting",
            predicate: "status_kind = $2",
            params: vec![utf8("Waiting")],
            pinned: false,
            rejected: Some(("status_kind = $2", vec![utf8("Scheduled")])),
        },
        stored("running", "Running"),
        stored("failed", "Failed"),
        stored("cancelled", "Cancelled"),
        stored("completed", "Succeeded"),
        TabShape {
            name: "all_priority",
            predicate: "priority = $2",
            params: vec![ScalarValue::Int64(Some(PINNED_PRIORITY))],
            pinned: true,
            rejected: None,
        },
        TabShape {
            name: "failed_priority",
            predicate: "status_kind = $2 AND priority = $3",
            params: vec![utf8("Failed"), ScalarValue::Int64(Some(PINNED_PRIORITY))],
            pinned: true,
            rejected: None,
        },
    ]
}

/// A tab count: the predicates after `tenant = $1`, the bound values, and
/// whether the per-tenant status counters answer it.
struct CountShape {
    name: &'static str,
    predicate: &'static str,
    params: Vec<ScalarValue>,
    counter_served: bool,
}

fn count_shapes() -> Vec<CountShape> {
    vec![
        CountShape {
            name: "failed_counter",
            predicate: "status_kind = $2",
            params: vec![utf8("Failed")],
            counter_served: true,
        },
        CountShape {
            name: "open_counter",
            predicate: "status_kind IN ($2, $3, $4)",
            params: vec![utf8("Waiting"), utf8("Scheduled"), utf8("Running")],
            counter_served: true,
        },
        CountShape {
            name: "waiting_index",
            predicate: "status_kind = $2",
            params: vec![utf8("Waiting")],
            counter_served: false,
        },
        CountShape {
            name: "scheduled_index",
            predicate: "status_kind = $2 AND current_attempt = 1",
            params: vec![utf8("Scheduled")],
            counter_served: false,
        },
        CountShape {
            name: "retrying_index",
            predicate: "status_kind = $2 AND current_attempt > 1",
            params: vec![utf8("Scheduled")],
            counter_served: false,
        },
        CountShape {
            name: "failed_priority_index",
            predicate: "status_kind = $2 AND priority = $3",
            params: vec![utf8("Failed"), ScalarValue::Int64(Some(PINNED_PRIORITY))],
            counter_served: false,
        },
    ]
}

/// The IDE's tab page: newest first, `id ASC` tiebreak, one page at `offset`.
fn tab_listing_query(predicate: &str, offset: usize) -> String {
    format!(
        "SELECT id FROM jobs WHERE tenant = $1 AND {predicate} ORDER BY enqueue_time_ms DESC, id ASC LIMIT {PAGE} OFFSET {offset}"
    )
}

/// The IDE's tab count: no subquery, no limit.
fn tab_count_query(predicate: &str) -> String {
    format!("SELECT COUNT(*) FROM jobs WHERE tenant = $1 AND {predicate}")
}

fn bound_params(tenant: &str, params: &[ScalarValue]) -> Vec<ScalarValue> {
    let mut all = Vec::with_capacity(params.len() + 1);
    all.push(utf8(tenant));
    all.extend(params.iter().cloned());
    all
}

/// The keys a listing-index walk may read: the pushed-down fetch, one final
/// chunk per arm capped by the fetch, one discovery seek per arm when the
/// priority is not pinned, and, for a clock-split shape, the rejected rows
/// interleaved before the last emitted row.
fn listing_key_budget(fetch: usize, arms: u64, pinned: bool, rejected: u64) -> u64 {
    let fetch = fetch as u64;
    let discovery = if pinned { 0 } else { arms };
    fetch + arms * fetch + discovery + rejected
}

/// An upper bound on the rows a clock split rejected before the listing's
/// last row: the rejected-side rows of the walked segments enqueued at or
/// after that row.
async fn rejected_rows_through(
    engine: &ShardQueryEngine,
    tenant: &str,
    predicate: &str,
    params: &[ScalarValue],
    last_id: &str,
) -> u64 {
    let last_enqueue = first_i64(
        &run_bound_query(
            engine,
            "SELECT enqueue_time_ms FROM jobs WHERE tenant = $1 AND id = $2",
            &[utf8(tenant), utf8(last_id)],
        )
        .await,
    );
    let query = format!(
        "SELECT COUNT(*) FROM jobs WHERE tenant = $1 AND {predicate} AND enqueue_time_ms >= ${}",
        params.len() + 2
    );
    let mut all = bound_params(tenant, params);
    all.push(ScalarValue::Int64(Some(last_enqueue)));
    first_i64(&run_bound_query(engine, &query, &all).await) as u64
}

/// Measure one tab shape at one offset under the marker version the clone
/// currently holds: timing, rows, and scan cost. On version 2 the scan is
/// checked against the key budget and must read no job records. Returns the
/// ids the page listed.
async fn bench_tab_listing(
    engine: &ShardQueryEngine,
    metrics: &silo::metrics::Metrics,
    shard_name: &str,
    tenant: &str,
    shape: &TabShape,
    offset: usize,
    version: u8,
) -> Vec<String> {
    let query = tab_listing_query(shape.predicate, offset);
    let params = bound_params(tenant, &shape.params);
    let page = if offset == 0 { "p1" } else { "deep" };
    let label = format!("tab_{}_{page}_v{version}", shape.name);
    let cost = measure_scan(engine, metrics, shard_name, &query, &params).await;
    let rows = first_column_strings(&cost.batches);
    bench_bound_query(engine, &label, &query, &params)
        .await
        .print();
    let mut detail = format!(
        "rows={} scanned_keys={} lookups={} arms={}",
        rows.len(),
        cost.scanned_keys,
        cost.point_lookups,
        cost.arms
    );
    if version == STATUS_ENQUEUE_TIME_INDEX_VERSION {
        let rejected = match (&shape.rejected, rows.last()) {
            (Some((predicate, params)), Some(last_id)) => {
                rejected_rows_through(engine, tenant, predicate, params, last_id).await
            }
            _ => 0,
        };
        let budget = listing_key_budget(offset + PAGE, cost.arms, shape.pinned, rejected);
        detail.push_str(&format!(" rejected<={rejected} budget={budget}"));
        assert!(
            cost.scanned_keys <= budget,
            "{label}: scanned {} keys against a budget of {budget}",
            cost.scanned_keys
        );
        assert_eq!(
            cost.point_lookups, 0,
            "{label}: the index path must read no job records"
        );
    }
    println!("  {:<30} {detail}", "");
    rows
}

/// Measure one tab count under the marker version the clone currently
/// holds. On version 2 a counter-served count must walk no keys and every
/// count must read no job records. Returns the count.
async fn bench_tab_count(
    engine: &ShardQueryEngine,
    metrics: &silo::metrics::Metrics,
    shard_name: &str,
    tenant: &str,
    shape: &CountShape,
    version: u8,
) -> i64 {
    let query = tab_count_query(shape.predicate);
    let params = bound_params(tenant, &shape.params);
    let label = format!("count_{}_v{version}", shape.name);
    let cost = measure_scan(engine, metrics, shard_name, &query, &params).await;
    let count = first_i64(&cost.batches);
    bench_bound_query(engine, &label, &query, &params)
        .await
        .print();
    println!(
        "  {:<30} count={count} scanned_keys={} lookups={}",
        "", cost.scanned_keys, cost.point_lookups
    );
    if version == STATUS_ENQUEUE_TIME_INDEX_VERSION {
        if shape.counter_served {
            assert_eq!(
                cost.scanned_keys, 0,
                "{label}: a counter-served count must walk no keys"
            );
        }
        assert_eq!(
            cost.point_lookups, 0,
            "{label}: a count must read no job records"
        );
    }
    count
}

/// Every tab shape at page one and at the deep offset, then every count
/// shape, under the clone's current marker version. Returns the listed ids
/// per shape and offset, and the counts, for comparison across versions.
async fn run_tab_benchmarks(
    engine: &ShardQueryEngine,
    metrics: &silo::metrics::Metrics,
    shard_name: &str,
    tenant: &str,
    version: u8,
) -> (Vec<Vec<String>>, Vec<i64>) {
    let mut rows = Vec::new();
    for shape in &tab_shapes() {
        for offset in [0, DEEP_OFFSET] {
            rows.push(
                bench_tab_listing(engine, metrics, shard_name, tenant, shape, offset, version)
                    .await,
            );
        }
    }
    let mut counts = Vec::new();
    for shape in &count_shapes() {
        counts.push(bench_tab_count(engine, metrics, shard_name, tenant, shape, version).await);
    }
    (rows, counts)
}

/// Measure the listing shapes on a clone of the golden shard opened with
/// metrics: after the backfill sweep runs to completion on the clone (the
/// index paths), with the marker held at version 1 (the baseline for every
/// tab shape), and with the marker cleared (the job-record path for the All
/// tab), including the Gadget subquery shape with its `id DESC` tiebreak.
/// Also reports the sweep's own cost on the clone.
async fn run_listing_benchmarks(metadata: &GoldenShardMetadata, tenant: &str) {
    let metrics = silo::metrics::init().expect("init metrics");
    let (_guard, shard) =
        clone_golden_shard_with_metrics("listing", metadata, Some(metrics.clone())).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("query engine");
    let shard_name = shard.name().to_string();

    println!("--- Editor Listing Query ({}) ---", tenant);
    assert!(
        !shard.enqueue_time_index_complete(),
        "the clone must start without the completion marker"
    );
    let sweep_started = Instant::now();
    let sweep = shard
        .backfill_listing_index(
            silo::job_store_shard::STATUS_ENQUEUE_TIME_INDEX_VERSION,
            256,
            std::time::Duration::ZERO,
        )
        .await
        .expect("backfill sweep");
    println!(
        "  backfill sweep: {} rows scanned, {} entries written in {}",
        sweep.rows_scanned,
        sweep.rows_written,
        format_duration(sweep_started.elapsed())
    );
    if sweep.rows_written == 0 {
        println!("  (golden shard already carried the index; sweep cost is enumerate-only)");
    }
    assert!(sweep.complete, "sweep must complete on the clone");
    // Read the new entries from SSTs like the rest of the shard rather than
    // from the memtable the sweep just filled.
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush index entries");

    let shapes = [
        ("unfiltered", ordered_listing_query(tenant, 0)),
        ("offset5000", ordered_listing_query(tenant, 5000)),
        ("gadget", gadget_listing_query(tenant)),
    ];
    let mut index_rows = Vec::with_capacity(shapes.len());
    for (name, query) in &shapes {
        let rows = bench_query_with_keys(
            &engine,
            &metrics,
            &shard_name,
            &format!("listing_{name}_index"),
            query,
        )
        .await;
        index_rows.push(rows);
    }

    println!("--- IDE Tab Shapes ({}, marker at version 2) ---", tenant);
    let (tab_rows, tab_counts) = run_tab_benchmarks(
        &engine,
        &metrics,
        &shard_name,
        tenant,
        STATUS_ENQUEUE_TIME_INDEX_VERSION,
    )
    .await;

    println!("--- IDE Tab Shapes ({}, marker at version 1) ---", tenant);
    shard
        .set_listing_index_version(ENQUEUE_TIME_INDEX_VERSION)
        .await
        .expect("hold marker at version 1");
    let (baseline_rows, baseline_counts) = run_tab_benchmarks(
        &engine,
        &metrics,
        &shard_name,
        tenant,
        ENQUEUE_TIME_INDEX_VERSION,
    )
    .await;
    assert_eq!(
        tab_rows, baseline_rows,
        "every tab shape must list the same ids on both marker versions"
    );
    assert_eq!(
        tab_counts, baseline_counts,
        "every count shape must agree on both marker versions"
    );

    shard
        .set_listing_index_version(0)
        .await
        .expect("clear marker");
    for ((name, query), expected) in shapes.iter().zip(&index_rows) {
        let rows = bench_query_with_keys(
            &engine,
            &metrics,
            &shard_name,
            &format!("listing_{name}_marker_cleared"),
            query,
        )
        .await;
        // The Gadget shape's inner sample is the newest 50k rows on the index
        // path but the first 50k in key order on the job-record path, so its
        // truth is the unbounded newest-first query rather than the same SQL.
        if *name == "gadget" {
            let truth = engine
                .sql(&format!(
                    "SELECT id FROM jobs WHERE tenant = '{tenant}' ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101"
                ))
                .await
                .expect("sql")
                .collect()
                .await
                .expect("collect");
            let truth: Vec<String> = first_column_strings(&truth);
            assert_eq!(
                expected, &truth,
                "listing_gadget: the index path must return the true newest rows"
            );
        } else {
            assert_eq!(
                &rows, expected,
                "listing_{name}: the index path and the job-record path must return the same rows"
            );
        }
    }

    shard.close().await.expect("close listing clone");
}

async fn run_tenant_queries(engine: &ShardQueryEngine, tenant: &str) {
    let r = bench_query(
        engine,
        "count_total",
        &format!("SELECT COUNT(*) FROM jobs WHERE tenant = '{}'", tenant),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "count_waiting",
        &format!(
            "SELECT COUNT(*) FROM jobs WHERE tenant = '{}' AND status_kind = 'Waiting'",
            tenant
        ),
    )
    .await;
    r.print();

    // This is the exact query used by the tenant view page to list waiting jobs.
    // It only projects shard_id and id to avoid expensive job_info lookups.
    let r = bench_query(
        engine,
        "list_waiting_jobs_tenant_view",
        &format!(
            "SELECT shard_id, id FROM jobs WHERE tenant = '{}' AND status_kind = 'Waiting' LIMIT 100",
            tenant
        ),
    )
    .await;
    r.print();

    // SELECT * variant - what remote shards receive from cluster query engine.
    let r = bench_query(
        engine,
        "list_waiting_select_star",
        &format!(
            "SELECT * FROM jobs WHERE tenant = '{}' AND status_kind = 'Waiting' LIMIT 100",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "count_succeeded",
        &format!(
            "SELECT COUNT(*) FROM jobs WHERE tenant = '{}' AND status_kind = 'Succeeded'",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "count_scheduled",
        &format!(
            "SELECT COUNT(*) FROM jobs WHERE tenant = '{}' AND status_kind = 'Scheduled'",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "first_page_failed",
        &format!(
            "SELECT * FROM jobs WHERE tenant = '{}' AND status_kind = 'Failed' LIMIT 20",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "10th_page_failed",
        &format!(
            "SELECT * FROM jobs WHERE tenant = '{}' AND status_kind = 'Failed' LIMIT 20 OFFSET 180",
            tenant
        ),
    )
    .await;
    r.print();

    let job_id = known_job_id(tenant);
    let r = bench_query(
        engine,
        "exact_id_lookup",
        &format!(
            "SELECT * FROM jobs WHERE tenant = '{}' AND id = '{}'",
            tenant, job_id
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "metadata_status",
        &format!(
            "SELECT * FROM jobs WHERE tenant = '{}' AND array_contains(element_at(metadata, 'region'), 'us-east-1') AND status_kind = 'Waiting'",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "status_breakdown",
        &format!(
            "SELECT status_kind, COUNT(*) FROM jobs WHERE tenant = '{}' GROUP BY status_kind",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "recent_jobs_no_index",
        &format!(
            "SELECT * FROM jobs WHERE tenant = '{}' ORDER BY enqueue_time_ms DESC LIMIT 20",
            tenant
        ),
    )
    .await;
    r.print();

    let r = bench_query(
        engine,
        "metadata_count",
        &format!(
            "SELECT COUNT(*) FROM jobs WHERE tenant = '{}' AND array_contains(element_at(metadata, 'region'), 'us-east-1')",
            tenant
        ),
    )
    .await;
    r.print();
}
