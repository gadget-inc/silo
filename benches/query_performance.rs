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

    shard.close().await.expect("close shard");
    println!("Done.");
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
