#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use silo::query::ShardQueryEngine;
use std::sync::Arc;
use std::time::Instant;

const WARMUP_ITERS: usize = 1;
const MEASURED_ITERS: usize = 5;

/// Run a query multiple times and collect timing stats.
async fn bench_query(engine: &ShardQueryEngine, label: &str, query: &str) -> BenchResult {
    // Warmup
    for _ in 0..WARMUP_ITERS {
        let df = engine.sql(query).await.expect("sql");
        let _batches = df.collect().await.expect("collect");
    }

    // Measured
    let mut durations = Vec::with_capacity(MEASURED_ITERS);
    for _ in 0..MEASURED_ITERS {
        let start = Instant::now();
        let df = engine.sql(query).await.expect("sql");
        let _batches = df.collect().await.expect("collect");
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
    let before = metrics.query_scanned_keys_value(shard_name);
    let df = engine.sql(query).await.expect("sql");
    let batches = df.collect().await.expect("collect");
    let scanned = metrics.query_scanned_keys_value(shard_name) - before;
    let rows: Vec<String> = first_column_strings(&batches);
    let r = bench_query(engine, label, query).await;
    r.print();
    println!("  {:<30} rows={} scanned_keys={}", "", rows.len(), scanned);
    rows
}

/// Measure the listing shapes on a clone of the golden shard opened with
/// metrics: after the backfill sweep runs to completion on the clone (the
/// index path) and with the completion marker cleared (the job-record path),
/// including the Gadget subquery shape with its `id DESC` tiebreak. Also
/// reports the sweep's own cost on the clone.
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
        .backfill_enqueue_time_index(256, std::time::Duration::ZERO)
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

    shard
        .set_enqueue_time_index_complete(false)
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
