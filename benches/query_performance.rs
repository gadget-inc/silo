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
    // The generator writes the Zipf jobs plus the expedite and deep-queue jobs.
    let generated_jobs = total_jobs
        + metadata.expedite_job_ids.len()
        + metadata.deep_queue_waiter_ids.len()
        + metadata.deep_queue_holder_task_ids.len();
    println!(
        "Dataset: {} Zipf jobs across {} tenants (generated {}, actual {})",
        total_jobs, NUM_TENANTS, generated_jobs, actual_count
    );
    assert_eq!(
        actual_count as usize, generated_jobs,
        "golden shard job count must equal the generator's total"
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

    shard.close().await.expect("close shard");

    run_listing_benchmarks(&metadata, large_tenant).await;
    println!();

    println!("Done.");
}

/// The editor's listing shape: a bounded sample of one tenant, optionally
/// pinned to a status, ordered newest-first by enqueue time.
fn listing_query(tenant: &str, status_filter: Option<&str>) -> String {
    let status = status_filter
        .map(|s| format!(" AND status_kind = '{s}'"))
        .unwrap_or_default();
    format!(
        "SELECT id FROM (SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '{tenant}'{status} LIMIT 50000) ORDER BY enqueue_time_ms DESC, id DESC LIMIT 101"
    )
}

/// Measure the listing shape on a clone of the golden shard in three states:
/// the completion marker set with every index entry carrying its value (the
/// index path), the marker cleared (the job-record path for the unfiltered
/// shape), and the marker set with the tenant's index values stripped (every
/// row hydrated through the fallback).
async fn run_listing_benchmarks(metadata: &GoldenShardMetadata, tenant: &str) {
    let (_guard, shard) = clone_golden_shard("listing", metadata).await;
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("query engine");
    let shapes = [("unfiltered", None), ("succeeded", Some("Succeeded"))];

    println!("--- Editor Listing Query ({}) ---", tenant);
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .expect("set marker");
    for (name, filter) in shapes {
        let r = bench_query(
            &engine,
            &format!("listing_{name}_index"),
            &listing_query(tenant, filter),
        )
        .await;
        r.print();
    }

    // The marker gates only the unfiltered shape; the filtered shape is a
    // control here and should match its `_index` row within noise.
    shard
        .set_enqueue_time_backfill_complete(false)
        .await
        .expect("clear marker");
    for (name, filter) in shapes {
        let r = bench_query(
            &engine,
            &format!("listing_{name}_marker_cleared"),
            &listing_query(tenant, filter),
        )
        .await;
        r.print();
    }

    let stripped = strip_tenant_index_values(&shard, tenant).await;
    println!("  (stripped {} index values)", stripped);
    // Flush so the stripped entries are read from SSTs like the other states
    // rather than from whatever mix of memtable and L0 the writes landed in.
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush stripped entries");
    shard
        .set_enqueue_time_backfill_complete(true)
        .await
        .expect("set marker");
    for (name, filter) in shapes {
        let r = bench_query(
            &engine,
            &format!("listing_{name}_fallback"),
            &listing_query(tenant, filter),
        )
        .await;
        r.print();
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
