//! Single-iteration query runner for profiling.
//! Run with: samply record cargo bench --bench query_profile

#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use silo::query::ShardQueryEngine;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let metadata = ensure_golden_shard().await;

    let shard = open_golden_shard_readonly(&metadata).await;

    // --- Microbenchmark: how long does scan_all_jobs take? ---
    {
        let start = Instant::now();
        let pairs = shard.scan_all_jobs(None).await.expect("scan_all_jobs");
        let scan_elapsed = start.elapsed();
        println!(
            "scan_all_jobs({} pairs): {:.1}ms",
            pairs.len(),
            scan_elapsed.as_secs_f64() * 1000.0
        );

        // How long for a single get_jobs_batch of 1000?
        let sample: Vec<String> = pairs.iter().take(1000).map(|(_, id)| id.clone()).collect();
        let tenant = &pairs[0].0;
        let start2 = Instant::now();
        let _ = shard
            .get_jobs_batch(tenant, &sample)
            .await
            .expect("get_jobs_batch");
        println!(
            "get_jobs_batch(1000 from {}): {:.1}ms",
            tenant,
            start2.elapsed().as_secs_f64() * 1000.0
        );

        // And scan_jobs for just the large tenant
        let start3 = Instant::now();
        let tenant_jobs = shard
            .scan_jobs("tenant_001", None)
            .await
            .expect("scan_jobs");
        println!(
            "scan_jobs(tenant_001, {} jobs): {:.1}ms",
            tenant_jobs.len(),
            start3.elapsed().as_secs_f64() * 1000.0
        );

        // And get_jobs_status_batch for 1000 large-tenant jobs
        let sample2: Vec<String> = tenant_jobs.iter().take(1000).cloned().collect();
        let start4 = Instant::now();
        let _ = shard
            .get_jobs_status_batch("tenant_001", &sample2)
            .await
            .expect("get_jobs_status_batch");
        println!(
            "get_jobs_status_batch(1000 from tenant_001): {:.1}ms",
            start4.elapsed().as_secs_f64() * 1000.0
        );
    }
    println!();

    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("query engine");

    let queries = [
        ("total_count", "SELECT COUNT(*) FROM jobs"),
        (
            "top_20_active_tenants",
            "SELECT tenant, COUNT(*) as cnt FROM jobs WHERE status_kind NOT IN ('Succeeded','Failed','Cancelled') GROUP BY tenant ORDER BY cnt DESC LIMIT 20",
        ),
        (
            "large_tenant_count",
            "SELECT COUNT(*) FROM jobs WHERE tenant = 'tenant_001'",
        ),
        (
            "large_tenant_status_breakdown",
            "SELECT status_kind, COUNT(*) FROM jobs WHERE tenant = 'tenant_001' GROUP BY status_kind",
        ),
        (
            "large_tenant_failed_page",
            "SELECT * FROM jobs WHERE tenant = 'tenant_001' AND status_kind = 'Failed' LIMIT 20",
        ),
        (
            "large_tenant_recent",
            "SELECT * FROM jobs WHERE tenant = 'tenant_001' ORDER BY enqueue_time_ms DESC LIMIT 20",
        ),
    ];

    println!("=== Query timings ===");
    for (label, query) in &queries {
        let start = Instant::now();
        let df = engine.sql(query).await.expect("sql");
        let batches = df.collect().await.expect("collect");
        let elapsed = start.elapsed();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "{:<35} {:>8.1}ms  ({} rows)",
            label,
            elapsed.as_secs_f64() * 1000.0,
            row_count
        );
    }

    shard.close().await.expect("close");
}
