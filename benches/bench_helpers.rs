//! Shared utilities for benchmarks that operate on a large "golden" shard.
//!
//! The golden shard is a ~500K-job dataset with a Zipf(s=1) tenant distribution,
//! plus special tenants for concurrency-queue and scheduled-job benchmarks.
//! It is generated once and cached on disk; subsequent benchmark runs reuse it.
//!
//! For benchmarks that mutate the shard, use `clone_golden_shard()` to get a
//! cheap copy-on-write clone via SlateDB checkpoints.

// This module is included via #[path] into multiple bench binaries, so not all
// items are used by every consumer.
#![allow(dead_code)]

use silo::gubernator::NullGubernatorClient;
use silo::job::ConcurrencyLimit;
use silo::job::Limit;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::shard_range::ShardRange;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const GOLDEN_DATA_DIR: &str = "./tmp/golden-bench-shard";
pub const NUM_TENANTS: usize = 300;
pub const TARGET_TOTAL_JOBS: usize = 500_000;
pub const BATCH_SIZE: usize = 500;

/// Tenant used for far-future scheduled jobs (expedite benchmarks).
pub const BENCH_EXPEDITE_TENANT: &str = "bench_expedite";
/// Number of far-future scheduled jobs to create for expedite benchmarks.
pub const EXPEDITE_JOB_COUNT: usize = 150;

/// Tenant used for the deep concurrency queue.
pub const BENCH_DEEP_QUEUE_TENANT: &str = "bench_deep_queue";
/// Concurrency key for the deep queue.
pub const DEEP_QUEUE_KEY: &str = "deep-queue";
/// Number of concurrency holders (filled slots) in the deep queue.
pub const DEEP_QUEUE_HOLDERS: usize = 5;
/// Number of waiters in the deep concurrency queue.
pub const DEEP_QUEUE_WAITERS: usize = 20_000;

const METADATA_FILE: &str = "golden-metadata.json";

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Persisted metadata about the golden shard, written as JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GoldenShardMetadata {
    pub total_jobs: usize,
    pub checkpoint_id: Uuid,

    /// Known (tenant, job_id) pairs for each status category from the Zipf data.
    pub waiting_jobs: Vec<(String, String)>,
    pub failed_jobs: Vec<(String, String)>,
    pub cancelled_jobs: Vec<(String, String)>,
    pub succeeded_jobs: Vec<(String, String)>,
    pub scheduled_future_jobs: Vec<(String, String)>,

    /// Job IDs for far-future scheduled jobs on the expedite tenant.
    pub expedite_job_ids: Vec<String>,

    /// Job IDs for the deep-queue waiters (concurrency requests).
    pub deep_queue_waiter_ids: Vec<String>,
    /// Task IDs for the deep-queue holders (running jobs).
    pub deep_queue_holder_task_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Tenant-size computation (Zipf)
// ---------------------------------------------------------------------------

fn tenant_name(rank: usize) -> String {
    format!("tenant_{:03}", rank)
}

pub fn compute_tenant_sizes() -> Vec<(String, usize)> {
    let harmonic: f64 = (1..=NUM_TENANTS).map(|k| 1.0 / k as f64).sum();
    let mut sizes = Vec::with_capacity(NUM_TENANTS);
    let mut assigned = 0usize;

    for rank in 1..=NUM_TENANTS {
        let fraction = (1.0 / rank as f64) / harmonic;
        let count = if rank == NUM_TENANTS {
            TARGET_TOTAL_JOBS - assigned
        } else {
            (fraction * TARGET_TOTAL_JOBS as f64).round() as usize
        };
        sizes.push((tenant_name(rank), count));
        assigned += count;
    }

    sizes
}

// ---------------------------------------------------------------------------
// Import helpers
// ---------------------------------------------------------------------------

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn metadata_for_job(job_idx: usize) -> Vec<(String, String)> {
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
pub fn build_import_params(job_idx: usize, job_id: &str, now: i64) -> ImportJobParams {
    let bucket = job_idx % 100;
    let enqueue_time_ms = now - 86_400_000 + (job_idx as i64 * 17);

    let (attempts, start_at_ms) = if bucket < 60 {
        let attempt = ImportedAttempt {
            status: ImportedAttemptStatus::Succeeded { result: Vec::new() },
            started_at_ms: enqueue_time_ms + 100,
            finished_at_ms: enqueue_time_ms + 5000,
        };
        (vec![attempt], enqueue_time_ms)
    } else if bucket < 80 {
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
        let attempt = ImportedAttempt {
            status: ImportedAttemptStatus::Cancelled,
            started_at_ms: enqueue_time_ms + 100,
            finished_at_ms: enqueue_time_ms + 200,
        };
        (vec![attempt], enqueue_time_ms)
    } else if bucket < 97 {
        (vec![], now)
    } else {
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

// ---------------------------------------------------------------------------
// Collecting known job IDs per status from the deterministic distribution
// ---------------------------------------------------------------------------

fn collect_known_job_ids(tenant_sizes: &[(String, usize)]) -> KnownJobIds {
    let mut ids = KnownJobIds::default();
    for (tenant, count) in tenant_sizes {
        for idx in 0..*count {
            let bucket = idx % 100;
            let job_id = format!("{}-{:08}", tenant, idx);
            let pair = (tenant.clone(), job_id);
            if bucket < 60 {
                ids.succeeded.push(pair);
            } else if bucket < 80 {
                ids.failed.push(pair);
            } else if bucket < 90 {
                ids.cancelled.push(pair);
            } else if bucket < 97 {
                ids.waiting.push(pair);
            } else {
                ids.scheduled_future.push(pair);
            }
        }
    }
    ids
}

#[derive(Default)]
struct KnownJobIds {
    succeeded: Vec<(String, String)>,
    failed: Vec<(String, String)>,
    cancelled: Vec<(String, String)>,
    waiting: Vec<(String, String)>,
    scheduled_future: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// Golden shard generation
// ---------------------------------------------------------------------------

fn metadata_path() -> PathBuf {
    Path::new(GOLDEN_DATA_DIR).join(METADATA_FILE)
}

/// Ensure the golden shard exists. Returns metadata for use by benchmarks.
pub async fn ensure_golden_shard() -> GoldenShardMetadata {
    if let Some(meta) = load_cached_metadata() {
        println!("Using cached golden shard from {}", GOLDEN_DATA_DIR);
        return meta;
    }

    std::fs::create_dir_all(GOLDEN_DATA_DIR).expect("create golden data dir");
    let tenant_sizes = compute_tenant_sizes();
    generate_golden_dataset(&tenant_sizes).await
}

fn load_cached_metadata() -> Option<GoldenShardMetadata> {
    let path = metadata_path();
    if !path.exists() {
        return None;
    }
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

async fn generate_golden_dataset(tenant_sizes: &[(String, usize)]) -> GoldenShardMetadata {
    let total_jobs: usize = tenant_sizes.iter().map(|(_, c)| *c).sum();
    println!(
        "Generating golden shard: {} jobs across {} tenants...",
        total_jobs,
        tenant_sizes.len()
    );

    let shard = open_shard_for_generation().await;
    let now = now_ms();

    // Phase 1: Import Zipf-distributed jobs
    import_zipf_jobs(&shard, tenant_sizes, now, total_jobs).await;

    // Phase 2: Create far-future scheduled jobs for expedite benchmarks
    let expedite_job_ids = create_expedite_jobs(&shard, now).await;

    // Phase 3: Create deep concurrency queue
    let (deep_queue_waiter_ids, deep_queue_holder_task_ids) =
        create_deep_concurrency_queue(&shard, now).await;

    // Flush memtable to SSTs — this converts all in-memory data (including WAL sorted runs)
    // into compacted SST files, which is critical for query performance. Without this, the
    // shard would have 100K+ WAL files that make DataFusion scans extremely slow.
    println!("  Flushing memtable to SSTs...");
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush memtable before checkpoint");

    let checkpoint_options = slatedb::config::CheckpointOptions {
        lifetime: None,
        ..Default::default()
    };
    let checkpoint = shard
        .db()
        .create_checkpoint(slatedb::config::CheckpointScope::All, &checkpoint_options)
        .await
        .expect("create checkpoint");

    println!("  Checkpoint created: {:?}", checkpoint.id);

    // Close shard
    println!("  Closing shard...");
    shard.close().await.expect("close golden shard");

    // Delete WAL directory — all data has been flushed to SSTs above, so the WAL
    // sorted runs are no longer needed. Removing them avoids a massive replay on
    // re-open and dramatically improves query scan performance.
    let wal_path = Path::new(GOLDEN_DATA_DIR).join("wal");
    if wal_path.exists() {
        println!(
            "  Removing WAL directory ({} files)...",
            std::fs::read_dir(&wal_path).map(|d| d.count()).unwrap_or(0)
        );
        std::fs::remove_dir_all(&wal_path).expect("remove WAL directory");
    }

    // Collect known job IDs per status
    let known = collect_known_job_ids(tenant_sizes);

    let metadata = GoldenShardMetadata {
        total_jobs,
        checkpoint_id: checkpoint.id,
        waiting_jobs: known.waiting,
        failed_jobs: known.failed,
        cancelled_jobs: known.cancelled,
        succeeded_jobs: known.succeeded,
        scheduled_future_jobs: known.scheduled_future,
        expedite_job_ids,
        deep_queue_waiter_ids,
        deep_queue_holder_task_ids,
    };

    // Write metadata marker
    let json = serde_json::to_string_pretty(&metadata).expect("serialize metadata");
    std::fs::write(metadata_path(), json).expect("write metadata");
    println!("  Golden shard ready.");

    metadata
}

async fn open_shard_for_generation() -> Arc<JobStoreShard> {
    open_shard_at_path(GOLDEN_DATA_DIR, 10).await
}

async fn open_shard_at_path(path: &str, flush_interval_ms: u64) -> Arc<JobStoreShard> {
    let cfg = silo::settings::DatabaseConfig {
        name: "golden-bench".to_string(),
        backend: silo::settings::Backend::Fs,
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

async fn import_zipf_jobs(
    shard: &Arc<JobStoreShard>,
    tenant_sizes: &[(String, usize)],
    now: i64,
    total_jobs: usize,
) {
    let progress = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let mut handles = vec![];
    for (tenant, count) in tenant_sizes.iter().cloned() {
        let shard = Arc::clone(shard);
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
        "  Imported {} Zipf jobs in {:.1}s ({:.0} jobs/sec)",
        done,
        elapsed.as_secs_f64(),
        done as f64 / elapsed.as_secs_f64()
    );
}

async fn create_expedite_jobs(shard: &Arc<JobStoreShard>, now: i64) -> Vec<String> {
    println!(
        "  Creating {} far-future scheduled jobs for expedite benchmarks...",
        EXPEDITE_JOB_COUNT
    );
    let far_future = now + 86_400_000 * 365; // ~1 year from now
    let mut ids = Vec::with_capacity(EXPEDITE_JOB_COUNT);

    for i in 0..EXPEDITE_JOB_COUNT {
        let job_id = format!("expedite-{:04}", i);
        shard
            .enqueue(
                BENCH_EXPEDITE_TENANT,
                Some(job_id.clone()),
                50,
                far_future,
                None,
                rmp_serde::to_vec(&serde_json::json!({"bench": "expedite"})).unwrap(),
                vec![],
                None,
                "",
            )
            .await
            .expect("enqueue expedite job");
        ids.push(job_id);
    }

    ids
}

async fn create_deep_concurrency_queue(
    shard: &Arc<JobStoreShard>,
    now: i64,
) -> (Vec<String>, Vec<String>) {
    println!(
        "  Creating deep concurrency queue: {} holders + {} waiters...",
        DEEP_QUEUE_HOLDERS, DEEP_QUEUE_WAITERS
    );

    let concurrency_limit = Limit::Concurrency(ConcurrencyLimit {
        key: DEEP_QUEUE_KEY.to_string(),
        max_concurrency: DEEP_QUEUE_HOLDERS as u32,
    });

    // Step 1: Enqueue holder jobs — they get immediate grants
    let mut holder_job_ids = Vec::with_capacity(DEEP_QUEUE_HOLDERS);
    for i in 0..DEEP_QUEUE_HOLDERS {
        let job_id = format!("deep-holder-{:04}", i);
        shard
            .enqueue(
                BENCH_DEEP_QUEUE_TENANT,
                Some(job_id.clone()),
                50,
                now,
                None,
                rmp_serde::to_vec(&serde_json::json!({"bench": "deep-queue-holder"})).unwrap(),
                vec![concurrency_limit.clone()],
                None,
                "",
            )
            .await
            .expect("enqueue deep-queue holder");
        holder_job_ids.push(job_id);
    }

    // Step 2: Dequeue them so they become Running (fills the concurrency slots)
    let mut holder_task_ids = Vec::with_capacity(DEEP_QUEUE_HOLDERS);
    for _ in 0..DEEP_QUEUE_HOLDERS {
        let result = shard
            .dequeue("bench-worker", "", 1)
            .await
            .expect("dequeue deep-queue holder");
        assert!(
            !result.tasks.is_empty(),
            "expected to dequeue a deep-queue holder"
        );
        holder_task_ids.push(result.tasks[0].attempt().task_id().to_string());
    }

    // Step 3: Enqueue waiters — they queue as RequestTicket tasks
    let mut waiter_ids = Vec::with_capacity(DEEP_QUEUE_WAITERS);
    for i in 0..DEEP_QUEUE_WAITERS {
        let job_id = format!("deep-waiter-{:05}", i);
        shard
            .enqueue(
                BENCH_DEEP_QUEUE_TENANT,
                Some(job_id.clone()),
                50,
                now,
                None,
                rmp_serde::to_vec(&serde_json::json!({"bench": "deep-queue-waiter"})).unwrap(),
                vec![concurrency_limit.clone()],
                None,
                "",
            )
            .await
            .expect("enqueue deep-queue waiter");
        waiter_ids.push(job_id);

        if (i + 1) % 5000 == 0 {
            println!("    Enqueued {}/{} waiters...", i + 1, DEEP_QUEUE_WAITERS);
        }
    }

    (waiter_ids, holder_task_ids)
}

// ---------------------------------------------------------------------------
// Shard access: clone and read-only open
// ---------------------------------------------------------------------------

/// RAII guard that deletes a clone directory on drop.
pub struct CloneGuard {
    path: PathBuf,
}

impl Drop for CloneGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Clone the golden shard and open it as a mutable `JobStoreShard`.
///
/// Returns a `CloneGuard` (drops the clone directory) and the shard.
pub async fn clone_golden_shard(
    clone_name: &str,
    metadata: &GoldenShardMetadata,
) -> (CloneGuard, Arc<JobStoreShard>) {
    let root = Path::new(GOLDEN_DATA_DIR)
        .parent()
        .expect("golden data dir must have a parent");
    let root_str = root.to_string_lossy().to_string();

    // Ensure the root directory exists and canonicalize it
    std::fs::create_dir_all(&root_str).expect("create root dir");
    let canonical_root = root
        .canonicalize()
        .expect("canonicalize root")
        .to_string_lossy()
        .to_string();

    let root_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(&canonical_root)
            .expect("create root LocalFileSystem"),
    );

    let golden_dir_name = Path::new(GOLDEN_DATA_DIR)
        .file_name()
        .expect("golden dir name")
        .to_string_lossy()
        .to_string();
    let clone_dir_name = format!("bench-clone-{}", clone_name);
    let clone_path = root.join(&clone_dir_name);

    // Create clone via Admin API
    let admin =
        slatedb::admin::Admin::builder(clone_dir_name.as_str(), Arc::clone(&root_store)).build();
    admin
        .create_clone(golden_dir_name.as_str(), Some(metadata.checkpoint_id))
        .await
        .expect("create clone");

    // Open the clone as a JobStoreShard
    let shard = JobStoreShard::open_with_resolved_store(
        clone_dir_name.clone(),
        &clone_dir_name,
        OpenShardOptions {
            store: root_store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(1)),
                ..Default::default()
            }),
            rate_limiter: NullGubernatorClient::new(),
            metrics: None,
        },
        ShardRange::full(),
    )
    .await
    .expect("open cloned shard");

    let guard = CloneGuard { path: clone_path };

    (guard, shard)
}

/// Open the golden shard read-only (large flush interval, no cloning).
pub async fn open_golden_shard_readonly(_metadata: &GoldenShardMetadata) -> Arc<JobStoreShard> {
    open_shard_at_path(GOLDEN_DATA_DIR, 5000).await
}

// ---------------------------------------------------------------------------
// Benchmark result formatting
// ---------------------------------------------------------------------------

pub struct BenchResult {
    pub label: String,
    pub durations: Vec<Duration>,
}

impl BenchResult {
    pub fn min(&self) -> Duration {
        self.durations[0]
    }
    pub fn max(&self) -> Duration {
        *self.durations.last().unwrap()
    }
    pub fn mean(&self) -> Duration {
        let total: Duration = self.durations.iter().sum();
        total / self.durations.len() as u32
    }
    pub fn p50(&self) -> Duration {
        let idx = self.durations.len() / 2;
        self.durations[idx]
    }
    pub fn p99(&self) -> Duration {
        let idx = (self.durations.len() as f64 * 0.99).ceil() as usize - 1;
        self.durations[idx.min(self.durations.len() - 1)]
    }
    pub fn print(&self) {
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

pub fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else if ms >= 1.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.0}us", ms * 1000.0)
    }
}

// ---------------------------------------------------------------------------
// Helpers for benchmark operation runners
// ---------------------------------------------------------------------------

/// Enqueue N jobs and dequeue them, returning (worker_id, task_ids).
/// Useful for benchmarks that need running jobs (heartbeat, report_outcome).
pub async fn enqueue_and_dequeue_jobs(
    shard: &Arc<JobStoreShard>,
    tenant: &str,
    count: usize,
) -> (String, Vec<String>) {
    let now = now_ms();
    let worker_id = format!("bench-worker-{}", Uuid::new_v4());

    for i in 0..count {
        let job_id = format!("bench-setup-{}-{:04}", tenant, i);
        shard
            .enqueue(
                tenant,
                Some(job_id),
                50,
                now,
                None,
                vec![1, 2, 3],
                vec![],
                None,
                "",
            )
            .await
            .expect("enqueue setup job");
    }

    let mut task_ids = Vec::with_capacity(count);
    let mut remaining = count;
    while remaining > 0 {
        let batch = std::cmp::min(remaining, 50);
        let result = shard
            .dequeue(&worker_id, "", batch)
            .await
            .expect("dequeue setup jobs");
        for task in &result.tasks {
            task_ids.push(task.attempt().task_id().to_string());
        }
        remaining -= result.tasks.len();
    }

    (worker_id, task_ids)
}
