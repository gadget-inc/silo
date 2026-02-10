use silo::gubernator::NullGubernatorClient;
use silo::job_store_shard::JobStoreShard;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::query::ShardQueryEngine;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::ShardRange;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const NUM_TENANTS: usize = 300;
const TARGET_TOTAL_JOBS: usize = 500_000;
const BATCH_SIZE: usize = 500;
const WARMUP_ITERS: usize = 3;
const MEASURED_ITERS: usize = 20;
const DATA_DIR: &str = "./tmp/query-bench-data";
const MARKER_FILE: &str = ".generated";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn tenant_name(rank: usize) -> String {
    format!("tenant_{:03}", rank)
}

/// Compute tenant sizes using Zipf(s=1) distribution.
/// Returns vec of (tenant_name, job_count) sorted by rank (largest first).
fn compute_tenant_sizes() -> Vec<(String, usize)> {
    // Harmonic number H(N) for normalization
    let harmonic: f64 = (1..=NUM_TENANTS).map(|k| 1.0 / k as f64).sum();
    let mut sizes = Vec::with_capacity(NUM_TENANTS);
    let mut assigned = 0usize;

    for rank in 1..=NUM_TENANTS {
        let fraction = (1.0 / rank as f64) / harmonic;
        let count = if rank == NUM_TENANTS {
            // Last tenant gets remainder to hit exact total
            TARGET_TOTAL_JOBS - assigned
        } else {
            (fraction * TARGET_TOTAL_JOBS as f64).round() as usize
        };
        sizes.push((tenant_name(rank), count));
        assigned += count;
    }

    sizes
}

/// Deterministic metadata for a job based on its index within a tenant.
fn metadata_for_job(job_idx: usize) -> Vec<(String, String)> {
    let regions = [
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "ap-southeast-1",
        "ap-northeast-1",
    ];
    let envs = ["production", "staging", "development"];
    vec![
        (
            "region".to_string(),
            regions[job_idx % regions.len()].to_string(),
        ),
        ("env".to_string(), envs[job_idx % envs.len()].to_string()),
    ]
}

/// Build import params for a single job. Status distribution:
/// 60% Succeeded, 20% Failed, 10% Cancelled, 7% Waiting (start_at=now), 3% Scheduled (start_at=now+1hr)
fn build_import_params(job_idx: usize, job_id: &str, now: i64) -> ImportJobParams {
    let bucket = job_idx % 100;
    let enqueue_time_ms = now - 86_400_000 + (job_idx as i64 * 17); // spread over a day

    let (attempts, start_at_ms) = if bucket < 60 {
        // Succeeded
        let attempt = ImportedAttempt {
            status: ImportedAttemptStatus::Succeeded { result: Vec::new() },
            started_at_ms: enqueue_time_ms + 100,
            finished_at_ms: enqueue_time_ms + 5000,
        };
        (vec![attempt], enqueue_time_ms)
    } else if bucket < 80 {
        // Failed — no retry policy so it stays terminal
        let attempt = ImportedAttempt {
            status: ImportedAttemptStatus::Failed {
                error_code: "ERR_TIMEOUT".to_string(),
                error: Vec::new(),
            },
            started_at_ms: enqueue_time_ms + 100,
            finished_at_ms: enqueue_time_ms + 3000,
        };
        (vec![attempt], enqueue_time_ms)
    } else if bucket < 90 {
        // Cancelled
        let attempt = ImportedAttempt {
            status: ImportedAttemptStatus::Cancelled,
            started_at_ms: enqueue_time_ms + 100,
            finished_at_ms: enqueue_time_ms + 200,
        };
        (vec![attempt], enqueue_time_ms)
    } else if bucket < 97 {
        // Waiting — 0 attempts, start_at = now (becomes Scheduled with start <= now => Waiting)
        (vec![], now)
    } else {
        // Scheduled — 0 attempts, start_at = now + 1 hour (future)
        (vec![], now + 3_600_000)
    };

    ImportJobParams {
        id: job_id.to_string(),
        priority: 50,
        enqueue_time_ms,
        start_at_ms,
        retry_policy: None,
        payload: rmp_serde::to_vec(&serde_json::json!({"idx": job_idx})).unwrap(),
        limits: vec![],
        metadata: Some(metadata_for_job(job_idx)),
        task_group: String::new(),
        attempts,
    }
}

async fn open_shard(path: &str, flush_interval_ms: u64) -> Arc<JobStoreShard> {
    let cfg = DatabaseConfig {
        name: "query-bench".to_string(),
        backend: Backend::Fs,
        path: path.to_string(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(flush_interval_ms)),
            ..Default::default()
        }),
    };
    JobStoreShard::open(&cfg, NullGubernatorClient::new(), None, ShardRange::full())
        .await
        .expect("open shard")
}

async fn generate_dataset(tenant_sizes: &[(String, usize)]) {
    let total_jobs: usize = tenant_sizes.iter().map(|(_, c)| *c).sum();
    println!(
        "Generating dataset: {} jobs across {} tenants...",
        total_jobs,
        tenant_sizes.len()
    );

    let shard = open_shard(DATA_DIR, 10).await;
    let now = now_ms();

    let progress = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    // Spawn parallel importers — one per tenant
    let mut handles = vec![];
    for (tenant, count) in tenant_sizes.iter().cloned() {
        let shard = Arc::clone(&shard);
        let progress = Arc::clone(&progress);
        let handle = tokio::spawn(async move {
            let mut imported = 0usize;
            while imported < count {
                let batch_end = std::cmp::min(imported + BATCH_SIZE, count);
                let mut batch = Vec::with_capacity(batch_end - imported);

                for idx in imported..batch_end {
                    let job_id = format!("{}-{:08}", tenant, idx);
                    batch.push(build_import_params(idx, &job_id, now));
                }

                let results = shard
                    .import_jobs(&tenant, batch)
                    .await
                    .expect("import_jobs");

                let ok_count = results.iter().filter(|r| r.success).count();
                imported += ok_count;
                progress.fetch_add(ok_count, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Progress reporter
    let progress_reporter = {
        let progress = Arc::clone(&progress);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let done = progress.load(Ordering::Relaxed);
                let elapsed = start.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    done as f64 / elapsed
                } else {
                    0.0
                };
                println!(
                    "  Progress: {}/{} ({:.1}%) — {:.0} jobs/sec",
                    done,
                    total_jobs,
                    done as f64 / total_jobs as f64 * 100.0,
                    rate
                );
                if done >= total_jobs {
                    break;
                }
            }
        })
    };

    for handle in handles {
        handle.await.expect("import task");
    }
    progress_reporter.abort();

    let elapsed = start.elapsed();
    let done = progress.load(Ordering::Relaxed);
    println!(
        "  Generated {} jobs in {:.1}s ({:.0} jobs/sec)",
        done,
        elapsed.as_secs_f64(),
        done as f64 / elapsed.as_secs_f64()
    );

    println!("  Closing shard (flushing to disk)...");
    shard.close().await.expect("close shard");

    // Write marker
    let marker_path = std::path::Path::new(DATA_DIR).join(MARKER_FILE);
    std::fs::write(&marker_path, format!("{}\n", done)).expect("write marker");
    println!("  Dataset ready.");
}

fn dataset_exists() -> bool {
    std::path::Path::new(DATA_DIR).join(MARKER_FILE).exists()
}

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

struct BenchResult {
    label: String,
    durations: Vec<Duration>,
}

impl BenchResult {
    fn min(&self) -> Duration {
        self.durations[0]
    }
    fn max(&self) -> Duration {
        *self.durations.last().unwrap()
    }
    fn mean(&self) -> Duration {
        let total: Duration = self.durations.iter().sum();
        total / self.durations.len() as u32
    }
    fn p50(&self) -> Duration {
        let idx = self.durations.len() / 2;
        self.durations[idx]
    }
    fn p99(&self) -> Duration {
        let idx = (self.durations.len() as f64 * 0.99).ceil() as usize - 1;
        self.durations[idx.min(self.durations.len() - 1)]
    }
    fn print(&self) {
        println!(
            "  {:<30} p50={:<10} p99={:<10} mean={:<10} [min={:<10} max={}]",
            format!("{}:", self.label),
            format_duration(self.p50()),
            format_duration(self.p99()),
            format_duration(self.mean()),
            format_duration(self.min()),
            format_duration(self.max()),
        );
    }
}

fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else if ms >= 1.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.0}us", ms * 1000.0)
    }
}

/// Pick a known job ID for exact-ID lookup benchmark.
/// We use the first job ID for the given tenant.
fn known_job_id(tenant: &str) -> String {
    format!("{}-{:08}", tenant, 0)
}

#[tokio::main]
async fn main() {
    let tenant_sizes = compute_tenant_sizes();
    let total_jobs: usize = tenant_sizes.iter().map(|(_, c)| *c).sum();

    // Ensure dataset exists
    if !dataset_exists() {
        std::fs::create_dir_all(DATA_DIR).expect("create data dir");
        generate_dataset(&tenant_sizes).await;
    } else {
        println!("Using cached dataset from {}", DATA_DIR);
    }

    // Open shard read-only (larger flush interval since we're only reading)
    let shard = open_shard(DATA_DIR, 5000).await;

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
