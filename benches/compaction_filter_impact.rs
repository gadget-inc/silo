//! Compaction filter impact bench.
//!
//! Two scenarios run back-to-back over the same configuration matrix:
//!
//! **Scenario A — steady-state with ongoing background compaction.**
//! Spawn a fixed pool of producer + consumer + reader tasks and let them
//! run for `SILO_BENCH_DURATION_SECS` seconds (default 300, ≈5 min).
//! Background size-tiered compaction is **enabled** so the compactor
//! interleaves with the workload and the filter fires repeatedly. We
//! record per-op latencies for `enqueue`, `dequeue`, `report_outcome`,
//! point-get reads, and prefix scans, and report p50/p99 globally as
//! well as split into first-half / second-half buckets so any drift
//! caused by compaction overhead is visible. The point-get p99 is the
//! headline metric — that's where the per-row decode cost of the
//! compaction filter is expected to amplify.
//!
//! **Scenario B — latency before vs after one manual compaction.**
//! Background compaction is **suppressed** (size-tiered's
//! `min_compaction_sources` is set unreachably high). We seed
//! `SILO_BENCH_JOBS` jobs (default 50_000), sleep past the retention
//! cutoff, run a write+read probe, manually call `run_full_compaction`,
//! wait for it to settle, then run the same probe again. The
//! before/after delta isolates the cost (or benefit) of one full
//! compaction with the filter installed.
//!
//! Override the workload size with `SILO_BENCH_JOBS=N` and the
//! steady-state duration with `SILO_BENCH_DURATION_SECS=N`.
//!
//! Note: only `SizeTieredCompactionSchedulerSupplier` is currently a
//! public production scheduler in slatedb. The bench is structured so a
//! custom `Arc<dyn CompactionSchedulerSupplier>` can be plugged in
//! alongside the `scheduler_options` knob when other algorithms become
//! available.

#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::{BenchResult, format_duration};

use silo::gubernator::NullGubernatorClient;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::compaction_filter::CompletedJobCompactionFilterSupplier;
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::settings::Backend;
use silo::shard_range::ShardRange;
use silo::storage::resolve_object_store;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const TENANT: &str = "bench-tenant";
const TASK_GROUP: &str = "";
const COMPACTION_TIMEOUT: Duration = Duration::from_secs(180);

// ---- Scenario A (steady-state) tunables ----
const STEADY_PRODUCERS: usize = 4;
const STEADY_CONSUMERS: usize = 4;
const STEADY_DEQUEUE_BATCH: usize = 16;
/// Sampler tick: how often the read-probe loop runs.
const STEADY_READER_INTERVAL: Duration = Duration::from_millis(50);
/// How many point gets the read sampler issues per tick.
const STEADY_READS_PER_TICK: usize = 4;
/// How often (in sampler ticks) the read sampler also runs a prefix scan.
const STEADY_SCAN_EVERY: usize = 20;

// ---- Scenario B (one-shot) tunables ----
const ONE_SHOT_SEED_PARALLELISM: usize = 8;
const ONE_SHOT_DEQUEUE_BATCH: usize = 64;
/// Number of write+read pairs the probe phase runs (before and after the
/// manual compaction).
const ONE_SHOT_PROBE_OPS: usize = 500;
const ONE_SHOT_PROBE_WARMUP: usize = 50;

/// One row in the configuration matrix.
struct BenchConfig {
    label: &'static str,
    /// Retention to install on the `CompletedJobCompactionFilter`. The
    /// no-filter baseline uses an effectively-infinite value so nothing
    /// is ever older than the cutoff.
    filter_retention: Duration,
    /// Optional `scheduler_options` overrides merged into the size-tiered
    /// scheduler config. `None` keeps slatedb defaults.
    scheduler_options: Option<HashMap<String, String>>,
}

/// Manifest + on-disk-bytes snapshot at one point in time.
#[derive(Debug, Clone)]
struct StorageSnapshot {
    l0_ssts: usize,
    sorted_runs: usize,
    total_ssts: usize,
    /// Sum of `estimate_size()` over every live SST referenced by the
    /// manifest (L0 + every sorted run). This is the metric the compaction
    /// filter is *supposed* to reduce: dead/tombstoned rows leave the
    /// live set, even if their old SSTs are still on disk waiting on GC.
    live_bytes: u64,
    /// Total bytes on disk under the shard's filesystem root, including
    /// orphaned SSTs not yet reclaimed by GC.
    total_bytes_on_disk: u64,
}

impl StorageSnapshot {
    fn render_diff(label: &str, before: &Self, after: &Self) {
        fn fmt_bytes(b: u64) -> String {
            const KIB: f64 = 1024.0;
            const MIB: f64 = KIB * 1024.0;
            const GIB: f64 = MIB * 1024.0;
            let f = b as f64;
            if f >= GIB {
                format!("{:.2} GiB", f / GIB)
            } else if f >= MIB {
                format!("{:.2} MiB", f / MIB)
            } else if f >= KIB {
                format!("{:.2} KiB", f / KIB)
            } else {
                format!("{} B", b)
            }
        }
        fn pct_change(before: u64, after: u64) -> f64 {
            if before == 0 {
                return 0.0;
            }
            ((after as f64 - before as f64) / before as f64) * 100.0
        }
        println!("  {label}:");
        println!(
            "    l0_ssts:               {} → {}",
            before.l0_ssts, after.l0_ssts
        );
        println!(
            "    sorted_runs:           {} → {}",
            before.sorted_runs, after.sorted_runs
        );
        println!(
            "    total_ssts:            {} → {}",
            before.total_ssts, after.total_ssts
        );
        println!(
            "    live_bytes (manifest): {} → {}   ({:+.1}%)",
            fmt_bytes(before.live_bytes),
            fmt_bytes(after.live_bytes),
            pct_change(before.live_bytes, after.live_bytes)
        );
        println!(
            "    total_bytes_on_disk:   {} → {}   ({:+.1}%)",
            fmt_bytes(before.total_bytes_on_disk),
            fmt_bytes(after.total_bytes_on_disk),
            pct_change(before.total_bytes_on_disk, after.total_bytes_on_disk)
        );
    }
}

/// Read the slatedb manifest via the admin API and walk the on-disk
/// directory for total bytes.
async fn snapshot(shard: &JobStoreShard, fs_root: &std::path::Path) -> StorageSnapshot {
    use slatedb::admin::AdminBuilder;

    let admin = AdminBuilder::new(shard.db_path(), shard.store().clone()).build();
    let state = admin
        .read_compactor_state_view()
        .await
        .expect("read compactor state view");
    let manifest = state.manifest();

    let l0_ssts = manifest.l0.len();
    let sorted_runs = manifest.compacted.len();
    let total_ssts = l0_ssts
        + manifest
            .compacted
            .iter()
            .map(|sr| sr.sst_views.len())
            .sum::<usize>();
    let live_bytes: u64 = manifest
        .l0
        .iter()
        .map(|sst| sst.estimate_size())
        .sum::<u64>()
        + manifest
            .compacted
            .iter()
            .map(|sr| sr.estimate_size())
            .sum::<u64>();
    let total_bytes_on_disk = directory_size(fs_root);

    StorageSnapshot {
        l0_ssts,
        sorted_runs,
        total_ssts,
        live_bytes,
        total_bytes_on_disk,
    }
}

/// Recursively sum file sizes under `root`.
fn directory_size(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(it) => it,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => stack.push(path),
                Ok(ft) if ft.is_file() => {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
                _ => {}
            }
        }
    }
    total
}

/// Open a fresh temp shard configured for this `BenchConfig`.
///
/// `suppress_background` controls whether size-tiered's auto-scheduling
/// is disabled. Scenario A wants `false` (background compaction running);
/// Scenario B wants `true` (manual compaction is the only compaction).
async fn open_bench_shard(
    cfg: &BenchConfig,
    suppress_background: bool,
) -> (tempfile::TempDir, Arc<JobStoreShard>, std::path::PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root_str = tmp.path().to_string_lossy().to_string();
    let resolved = resolve_object_store(&Backend::Fs, &root_str).expect("resolve fs object store");
    let fs_root = std::path::PathBuf::from(&resolved.root_path);

    // Merge order: start with the config's scheduler options (if any),
    // then overlay the suppression on top when requested. This matters
    // for the aggressive config, which sets `min_compaction_sources=2`
    // — in Scenario A we want that to apply, but in Scenario B we need
    // suppression to win so manual compaction is the only compaction,
    // even for the aggressive config (where the scheduler tunables then
    // have no effect and the config is effectively equivalent to
    // filter_30s).
    let mut scheduler_options: HashMap<String, String> =
        cfg.scheduler_options.clone().unwrap_or_default();
    if suppress_background {
        // Set min_compaction_sources unreachably high so the size-tiered
        // scheduler never proposes a compaction on its own. The compactor
        // still polls the .compactions file for manually-submitted work.
        scheduler_options.insert("min_compaction_sources".to_string(), "1000000".to_string());
        // If a config also set max_compaction_sources (e.g. aggressive's
        // max=4), clear it so the compactor can merge as many sources as
        // we submit in a manual CompactionSpec.
        scheduler_options.remove("max_compaction_sources");
    }

    let compactor_options = slatedb::config::CompactorOptions {
        // Short poll so manually-submitted compactions get picked up
        // promptly in Scenario B and so size-tiered fires responsively
        // in Scenario A.
        poll_interval: Duration::from_millis(250),
        scheduler_options,
        ..slatedb::config::CompactorOptions::default()
    };

    let slatedb_settings = slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(10)),
        compactor_options: Some(compactor_options),
        // Disable background GC so the live_bytes / total_bytes
        // measurements aren't perturbed mid-bench.
        garbage_collector_options: None,
        ..Default::default()
    };

    let shard = JobStoreShard::open_with_resolved_store(
        format!("compaction-bench-{}", cfg.label),
        &resolved.canonical_path,
        OpenShardOptions {
            store: resolved.store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(slatedb_settings),
            memory_cache: None,
            rate_limiter: NullGubernatorClient::new(),
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(
                silo::settings::DEFAULT_CONCURRENCY_RECONCILE_INTERVAL_MS,
            ),
            compaction_filter_supplier: Some(Arc::new(CompletedJobCompactionFilterSupplier::new(
                cfg.filter_retention,
            ))),
        },
        ShardRange::full(),
    )
    .await
    .expect("open bench shard");

    (tmp, shard, fs_root)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Submit a full compaction and wait for it to settle.
async fn run_and_wait_for_compaction(shard: &JobStoreShard) -> Duration {
    use slatedb::admin::AdminBuilder;

    let t0 = Instant::now();
    shard
        .run_full_compaction()
        .await
        .expect("run_full_compaction");

    let admin = AdminBuilder::new(shard.db_path(), shard.store().clone()).build();
    let deadline = Instant::now() + COMPACTION_TIMEOUT;
    loop {
        let state = admin
            .read_compactor_state_view()
            .await
            .expect("read compactor state view");
        let manifest = state.manifest();
        if manifest.l0.is_empty() && manifest.compacted.len() <= 1 {
            return t0.elapsed();
        }
        if Instant::now() > deadline {
            eprintln!(
                "compaction did not finish within {:?}: l0={} compacted_runs={}",
                COMPACTION_TIMEOUT,
                manifest.l0.len(),
                manifest.compacted.len()
            );
            return t0.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// =====================================================================
// Latency sample helpers
// =====================================================================

/// A timestamped duration sample. Used by Scenario A so we can split
/// samples into first-half / second-half buckets after the fact.
#[derive(Clone, Copy, Debug)]
struct Sample {
    at: Instant,
    dur: Duration,
}

fn build_result(label: &str, mut durations: Vec<Duration>) -> BenchResult {
    durations.sort();
    BenchResult {
        label: label.to_string(),
        durations,
    }
}

/// Build two `BenchResult`s by splitting `samples` on `midpoint`.
/// Useful for showing how p99 drifts as compaction kicks in.
fn split_at_midpoint(
    label: &str,
    samples: &[Sample],
    midpoint: Instant,
) -> (BenchResult, BenchResult) {
    let first: Vec<Duration> = samples
        .iter()
        .filter(|s| s.at < midpoint)
        .map(|s| s.dur)
        .collect();
    let second: Vec<Duration> = samples
        .iter()
        .filter(|s| s.at >= midpoint)
        .map(|s| s.dur)
        .collect();
    (
        build_result(&format!("{label} [first half]"), first),
        build_result(&format!("{label} [second half]"), second),
    )
}

// =====================================================================
// Scenario A — steady-state with ongoing background compaction
// =====================================================================

async fn run_steady_state(cfg: &BenchConfig, duration: Duration) {
    println!("\n=== {} ===", cfg.label);
    println!(
        "  filter_retention: {:?}   scheduler_options: {}",
        cfg.filter_retention,
        cfg.scheduler_options
            .as_ref()
            .map(|m| format!("{m:?}"))
            .unwrap_or_else(|| "default".to_string())
    );

    let (_tmp, shard, fs_root) = open_bench_shard(cfg, /*suppress_background=*/ false).await;

    // Stop signal flipped after `duration` elapses. Workers exit at the
    // top of their next iteration.
    let stop = Arc::new(AtomicBool::new(false));
    // Shared list of job IDs the producers have created. Readers pull
    // random IDs from this list to issue point gets against. Behind a
    // Mutex but writes are batched so contention is minimal.
    let known_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::with_capacity(8192)));

    // Per-op sample buffers (one Mutex<Vec> per op type, written by all
    // workers; aggregated at the end).
    let enqueue_samples: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::with_capacity(65536)));
    let dequeue_samples: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::with_capacity(65536)));
    let complete_samples: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::with_capacity(65536)));
    let read_get_samples: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::with_capacity(65536)));
    let read_scan_samples: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::with_capacity(8192)));

    let workload_start = Instant::now();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // ---- Producers: enqueue in a tight loop ----
    for w in 0..STEADY_PRODUCERS {
        let shard = Arc::clone(&shard);
        let stop = Arc::clone(&stop);
        let known_ids = Arc::clone(&known_ids);
        let enq_samples = Arc::clone(&enqueue_samples);
        handles.push(tokio::spawn(async move {
            let mut local_ids: Vec<String> = Vec::with_capacity(256);
            let mut local_samples: Vec<Sample> = Vec::with_capacity(8192);
            while !stop.load(Ordering::Relaxed) {
                let now = now_ms();
                let t = Instant::now();
                let id = shard
                    .enqueue(
                        TENANT,
                        None,
                        50,
                        now,
                        None,
                        Vec::new(),
                        vec![],
                        None,
                        TASK_GROUP,
                    )
                    .await
                    .expect("enqueue");
                local_samples.push(Sample {
                    at: t,
                    dur: t.elapsed(),
                });
                local_ids.push(id);

                // Periodically flush to shared buffers to keep the
                // per-iteration cost flat.
                if local_ids.len() >= 64 {
                    known_ids.lock().unwrap().extend(local_ids.drain(..));
                }
                if local_samples.len() >= 256 {
                    enq_samples.lock().unwrap().extend(local_samples.drain(..));
                }
            }
            if !local_ids.is_empty() {
                known_ids.lock().unwrap().extend(local_ids);
            }
            if !local_samples.is_empty() {
                enq_samples.lock().unwrap().extend(local_samples);
            }
            let _ = w;
        }));
    }

    // ---- Consumers: dequeue + report success ----
    for c in 0..STEADY_CONSUMERS {
        let shard = Arc::clone(&shard);
        let stop = Arc::clone(&stop);
        let deq_samples = Arc::clone(&dequeue_samples);
        let comp_samples = Arc::clone(&complete_samples);
        let worker_id = format!("steady-consumer-{c}");
        handles.push(tokio::spawn(async move {
            let mut local_deq: Vec<Sample> = Vec::with_capacity(8192);
            let mut local_comp: Vec<Sample> = Vec::with_capacity(8192);
            while !stop.load(Ordering::Relaxed) {
                let t_deq = Instant::now();
                let result = shard
                    .dequeue(&worker_id, TASK_GROUP, STEADY_DEQUEUE_BATCH)
                    .await
                    .expect("dequeue");
                let deq_elapsed = t_deq.elapsed();
                local_deq.push(Sample {
                    at: t_deq,
                    dur: deq_elapsed,
                });
                if result.tasks.is_empty() {
                    // Producers may not have caught up yet — yield briefly
                    // without recording the sleep.
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }
                for t in &result.tasks {
                    let task_id = t.attempt().task_id().to_string();
                    let t_comp = Instant::now();
                    shard
                        .report_attempt_outcome(
                            &task_id,
                            AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await
                        .expect("report success");
                    local_comp.push(Sample {
                        at: t_comp,
                        dur: t_comp.elapsed(),
                    });
                }
                if local_deq.len() >= 256 {
                    deq_samples.lock().unwrap().extend(local_deq.drain(..));
                }
                if local_comp.len() >= 256 {
                    comp_samples.lock().unwrap().extend(local_comp.drain(..));
                }
            }
            if !local_deq.is_empty() {
                deq_samples.lock().unwrap().extend(local_deq);
            }
            if !local_comp.is_empty() {
                comp_samples.lock().unwrap().extend(local_comp);
            }
        }));
    }

    // ---- Reader: periodic point-get + occasional prefix scan ----
    {
        let shard = Arc::clone(&shard);
        let stop = Arc::clone(&stop);
        let known_ids = Arc::clone(&known_ids);
        let get_samples = Arc::clone(&read_get_samples);
        let scan_samples = Arc::clone(&read_scan_samples);
        handles.push(tokio::spawn(async move {
            let scan_start = silo::keys::job_status_prefix(TENANT);
            let scan_end = silo::keys::end_bound(&scan_start);
            let db = shard.db();
            let mut tick: usize = 0;
            // Tiny LCG for deterministic in-task pseudo-random index picks.
            let mut rng_state: u64 = 0x9e37_79b9_7f4a_7c15;
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(STEADY_READER_INTERVAL).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                // Snapshot the current id pool length cheaply (no clone).
                let pool_len = known_ids.lock().unwrap().len();
                if pool_len > 0 {
                    for _ in 0..STEADY_READS_PER_TICK {
                        rng_state = rng_state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let idx = (rng_state >> 33) as usize % pool_len;
                        // Re-lock just to copy out one id; cheap and avoids
                        // holding the lock across the get.
                        let id = {
                            let guard = known_ids.lock().unwrap();
                            // Pool may have grown since we sampled `pool_len`,
                            // but the index is still in-bounds for the original
                            // length, which is sufficient for a random read.
                            guard.get(idx).cloned()
                        };
                        if let Some(id) = id {
                            let key = silo::keys::job_status_key(TENANT, &id);
                            let t = Instant::now();
                            let _ = db.get(key.as_slice()).await.expect("read get");
                            get_samples.lock().unwrap().push(Sample {
                                at: t,
                                dur: t.elapsed(),
                            });
                        }
                    }
                }

                if tick % STEADY_SCAN_EVERY == 0 {
                    let t = Instant::now();
                    let mut iter = db
                        .scan::<Vec<u8>, _>(scan_start.clone()..scan_end.clone())
                        .await
                        .expect("scan");
                    let mut count = 0usize;
                    while let Some(_kv) = iter.next().await.expect("scan next") {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                    scan_samples.lock().unwrap().push(Sample {
                        at: t,
                        dur: t.elapsed(),
                    });
                }

                tick = tick.wrapping_add(1);
            }
        }));
    }

    // ---- Run for `duration` ----
    println!("  running for {} ...", format_duration(duration));
    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.await.expect("worker join");
    }
    let workload_elapsed = workload_start.elapsed();
    let midpoint = workload_start + (workload_elapsed / 2);

    // ---- Aggregate and print ----
    let enq = Arc::try_unwrap(enqueue_samples)
        .unwrap()
        .into_inner()
        .unwrap();
    let deq = Arc::try_unwrap(dequeue_samples)
        .unwrap()
        .into_inner()
        .unwrap();
    let comp = Arc::try_unwrap(complete_samples)
        .unwrap()
        .into_inner()
        .unwrap();
    let gets = Arc::try_unwrap(read_get_samples)
        .unwrap()
        .into_inner()
        .unwrap();
    let scans = Arc::try_unwrap(read_scan_samples)
        .unwrap()
        .into_inner()
        .unwrap();

    let total_completes = comp.len();
    let throughput = total_completes as f64 / workload_elapsed.as_secs_f64();
    println!(
        "  ops: enq={} deq={} complete={} read_get={} read_scan={}",
        enq.len(),
        deq.len(),
        comp.len(),
        gets.len(),
        scans.len()
    );
    println!(
        "  throughput: {:.0} completed jobs/sec over {}",
        throughput,
        format_duration(workload_elapsed)
    );

    println!("  latency:");
    build_result("enqueue", enq.iter().map(|s| s.dur).collect()).print();
    build_result("dequeue", deq.iter().map(|s| s.dur).collect()).print();
    build_result("report_complete", comp.iter().map(|s| s.dur).collect()).print();
    build_result("read_get", gets.iter().map(|s| s.dur).collect()).print();
    build_result("read_scan(<=1000)", scans.iter().map(|s| s.dur).collect()).print();

    println!("  drift (first half → second half):");
    let (a1, a2) = split_at_midpoint("read_get", &gets, midpoint);
    a1.print();
    a2.print();
    let (b1, b2) = split_at_midpoint("enqueue", &enq, midpoint);
    b1.print();
    b2.print();

    // Flush memtable so the final snapshot reflects what the workload
    // actually wrote — without this, the compactor-state-view manifest
    // shows zero because the writer's in-memory data hasn't been
    // persisted yet. Note we flush AFTER stopping workers, so this
    // doesn't perturb the latency samples.
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush memtable before final snapshot");
    let final_state = snapshot(&shard, &fs_root).await;
    println!(
        "  manifest at end: l0_ssts={} sorted_runs={} live_bytes={} bytes",
        final_state.l0_ssts, final_state.sorted_runs, final_state.live_bytes
    );

    shard.close().await.expect("close");
}

// =====================================================================
// Scenario B — one-shot, before/after manual compaction
// =====================================================================

/// Latency report for one probe phase.
struct ProbeResult {
    label: String,
    write_enqueue: BenchResult,
    write_complete: BenchResult,
    read_get: BenchResult,
}

impl ProbeResult {
    fn print(&self) {
        println!("  {}", self.label);
        self.write_enqueue.print();
        self.write_complete.print();
        self.read_get.print();
    }
}

async fn one_shot_probe(
    label: &str,
    shard: &Arc<JobStoreShard>,
    sample_keys: &[Vec<u8>],
) -> ProbeResult {
    let mut enqueue_durations = Vec::with_capacity(ONE_SHOT_PROBE_OPS);
    let mut complete_durations = Vec::with_capacity(ONE_SHOT_PROBE_OPS);
    let mut read_durations = Vec::with_capacity(ONE_SHOT_PROBE_OPS);

    let worker_id = format!("probe-{label}");

    for round in 0..(ONE_SHOT_PROBE_OPS + ONE_SHOT_PROBE_WARMUP) {
        // ---- write: enqueue ----
        let now = now_ms();
        let t_enq = Instant::now();
        let _id = shard
            .enqueue(
                TENANT,
                None,
                50,
                now,
                None,
                Vec::new(),
                vec![],
                None,
                TASK_GROUP,
            )
            .await
            .expect("probe enqueue");
        let enq_elapsed = t_enq.elapsed();

        // ---- write: dequeue + complete (we lump the complete time into
        //      `complete_durations`; the dequeue itself isn't measured
        //      for the probe since dequeue does broker work that isn't
        //      directly comparable across runs) ----
        let result = shard
            .dequeue(&worker_id, TASK_GROUP, 1)
            .await
            .expect("probe dequeue");
        if let Some(t) = result.tasks.first() {
            let task_id = t.attempt().task_id().to_string();
            let t_comp = Instant::now();
            shard
                .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: Vec::new() })
                .await
                .expect("probe report");
            let comp_elapsed = t_comp.elapsed();
            if round >= ONE_SHOT_PROBE_WARMUP {
                enqueue_durations.push(enq_elapsed);
                complete_durations.push(comp_elapsed);
            }
        }

        // ---- read: point get against a seed key ----
        if !sample_keys.is_empty() {
            let key = &sample_keys[round % sample_keys.len()];
            let t = Instant::now();
            let _ = shard.db().get(key.as_slice()).await.expect("probe get");
            if round >= ONE_SHOT_PROBE_WARMUP {
                read_durations.push(t.elapsed());
            }
        }
    }

    ProbeResult {
        label: format!("{label}:"),
        write_enqueue: build_result("enqueue", enqueue_durations),
        write_complete: build_result("report_complete", complete_durations),
        read_get: build_result("read_get", read_durations),
    }
}

async fn seed_enqueue_one_shot(shard: &Arc<JobStoreShard>, n: usize) -> Vec<String> {
    let job_ids: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(n)));
    let now = now_ms();

    let mut handles = Vec::with_capacity(ONE_SHOT_SEED_PARALLELISM);
    for w in 0..ONE_SHOT_SEED_PARALLELISM {
        let shard = Arc::clone(shard);
        let job_ids = Arc::clone(&job_ids);
        let start = (n * w) / ONE_SHOT_SEED_PARALLELISM;
        let end = (n * (w + 1)) / ONE_SHOT_SEED_PARALLELISM;
        handles.push(tokio::spawn(async move {
            let mut local = Vec::with_capacity(end - start);
            for _ in start..end {
                let id = shard
                    .enqueue(
                        TENANT,
                        None,
                        50,
                        now,
                        None,
                        Vec::new(),
                        vec![],
                        None,
                        TASK_GROUP,
                    )
                    .await
                    .expect("seed enqueue");
                local.push(id);
            }
            job_ids.lock().unwrap().extend(local);
        }));
    }
    for h in handles {
        h.await.expect("seed worker");
    }
    Arc::try_unwrap(job_ids).unwrap().into_inner().unwrap()
}

async fn drain_to_completion_one_shot(shard: &Arc<JobStoreShard>, total: usize) {
    let processed = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(ONE_SHOT_SEED_PARALLELISM);
    for w in 0..ONE_SHOT_SEED_PARALLELISM {
        let shard = Arc::clone(shard);
        let processed = Arc::clone(&processed);
        let worker_id = format!("one-shot-drain-{w}");
        handles.push(tokio::spawn(async move {
            loop {
                if processed.load(Ordering::Relaxed) >= total {
                    break;
                }
                let result = shard
                    .dequeue(&worker_id, TASK_GROUP, ONE_SHOT_DEQUEUE_BATCH)
                    .await
                    .expect("dequeue");
                if result.tasks.is_empty() {
                    if processed.load(Ordering::Relaxed) >= total {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                for t in &result.tasks {
                    let task_id = t.attempt().task_id().to_string();
                    shard
                        .report_attempt_outcome(
                            &task_id,
                            AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await
                        .expect("report success");
                }
                processed.fetch_add(result.tasks.len(), Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.await.expect("drain worker");
    }
}

async fn run_one_shot(cfg: &BenchConfig, num_jobs: usize) {
    println!("\n=== {} ===", cfg.label);
    println!(
        "  filter_retention: {:?}   scheduler_options: {}",
        cfg.filter_retention,
        cfg.scheduler_options
            .as_ref()
            .map(|m| format!("{m:?}"))
            .unwrap_or_else(|| "default".to_string())
    );

    let (_tmp, shard, fs_root) = open_bench_shard(cfg, /*suppress_background=*/ true).await;

    // ---- Seed jobs ----
    let seed_t0 = Instant::now();
    let job_ids = seed_enqueue_one_shot(&shard, num_jobs).await;
    drain_to_completion_one_shot(&shard, num_jobs).await;
    println!(
        "  seeded {} jobs (enqueue→dequeue→success) in {}",
        num_jobs,
        format_duration(seed_t0.elapsed())
    );

    // ---- Sleep past the retention cutoff (if it's reasonable) ----
    const MAX_RETENTION_SLEEP: Duration = Duration::from_secs(5 * 60);
    if cfg.filter_retention <= MAX_RETENTION_SLEEP {
        let sleep_for = cfg.filter_retention + Duration::from_secs(1);
        println!(
            "  sleeping {} for retention cutoff",
            format_duration(sleep_for)
        );
        tokio::time::sleep(sleep_for).await;
    } else {
        println!(
            "  skipping retention sleep (retention {} > {} cap)",
            format_duration(cfg.filter_retention),
            format_duration(MAX_RETENTION_SLEEP)
        );
    }

    // Build the read sample-key set from seeded job ids.
    let sample_keys: Vec<Vec<u8>> = job_ids
        .iter()
        .step_by((job_ids.len() / 256).max(1))
        .map(|id| silo::keys::job_status_key(TENANT, id))
        .collect();

    // Flush memtable so the pre-compaction snapshot reflects on-disk state.
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush memtable before snapshot");

    let before = snapshot(&shard, &fs_root).await;

    // ---- Probe BEFORE manual compaction ----
    println!("  probe before compaction:");
    let probe_before = one_shot_probe("before:", &shard, &sample_keys).await;
    probe_before.print();

    // ---- Drive manual compaction & wait ----
    let compaction_elapsed = run_and_wait_for_compaction(&shard).await;
    println!(
        "  compaction_wall_clock: {}",
        format_duration(compaction_elapsed)
    );

    let after = snapshot(&shard, &fs_root).await;
    StorageSnapshot::render_diff("storage", &before, &after);

    // ---- Probe AFTER manual compaction ----
    println!("  probe after compaction:");
    let probe_after = one_shot_probe("after:", &shard, &sample_keys).await;
    probe_after.print();

    // ---- Headline: read_get p99 delta ----
    let before_p99 = probe_before.read_get.p99();
    let after_p99 = probe_after.read_get.p99();
    let delta_pct = if before_p99.as_nanos() > 0 {
        ((after_p99.as_nanos() as f64 - before_p99.as_nanos() as f64)
            / before_p99.as_nanos() as f64)
            * 100.0
    } else {
        0.0
    };
    println!(
        "  ▶ read_get p99: {} → {}   ({:+.1}%)",
        format_duration(before_p99),
        format_duration(after_p99),
        delta_pct
    );

    shard.close().await.expect("close");
}

// =====================================================================
// main()
// =====================================================================

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("Compaction Filter Impact Benchmark");
    println!("========================================");

    let duration_secs: u64 = std::env::var("SILO_BENCH_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let num_jobs: usize = std::env::var("SILO_BENCH_JOBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    let infinite = Duration::from_secs(60 * 60 * 24 * 365 * 100);

    let mut aggressive = HashMap::new();
    aggressive.insert("min_compaction_sources".to_string(), "2".to_string());
    aggressive.insert("max_compaction_sources".to_string(), "4".to_string());

    let configs = vec![
        BenchConfig {
            label: "baseline_no_filter",
            filter_retention: infinite,
            scheduler_options: None,
        },
        BenchConfig {
            label: "filter_30s",
            filter_retention: Duration::from_secs(30),
            scheduler_options: None,
        },
        BenchConfig {
            label: "filter_30s_aggressive_size_tiered",
            filter_retention: Duration::from_secs(30),
            scheduler_options: Some(aggressive),
        },
    ];

    println!("\n--- SCENARIO A: steady-state with ongoing background compaction ---");
    println!("    duration: {duration_secs}s per config");
    for cfg in &configs {
        run_steady_state(cfg, Duration::from_secs(duration_secs)).await;
    }

    println!("\n--- SCENARIO B: latency before / after one manual compaction ---");
    println!("    workload: {num_jobs} jobs per config");
    for cfg in &configs {
        run_one_shot(cfg, num_jobs).await;
    }

    println!();
}
