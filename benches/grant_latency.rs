//! Per-grant time-to-leasable latency through the concurrency grant scanner
//! under object-store read latency.
//!
//! Models the production shape where a single tenant's gating queue is
//! saturated: every RunAttempt flows through a deferred concurrency request
//! that `process_grants` drains, and each chain-resume read pays an
//! object-store round trip (cold block cache over GCS).
//!
//! Cases:
//! 1. `grant_to_leasable` — free the gate's capacity, run one
//!    `process_grants(GRANT_COUNT)` invocation while a concurrent dequeuer
//!    timestamps each granted task as it becomes claimable. The p50/p99 of
//!    (lease time - invocation start) is the metric the production
//!    `silo_leasable_to_start_latency_ms` histogram degrades on.
//! 2. `release_to_lease` — with the grant scanner running, release one holder
//!    and measure until the next waiter is actually leased. Covers the
//!    release -> nudge -> grant -> broker-wakeup -> claim pipeline.
//! 3. Invocation-duration guard — total `process_grants` wall time from
//!    case 1 must not regress; latency fixes must not trade away throughput.
//!
//! Thresholds are asserted at the end of the run; a failing run prints all
//! measurements first.

use async_trait::async_trait;
use futures::stream::BoxStream;
use silo::gubernator::NullGubernatorClient;
use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::shard_range::ShardRange;
use slatedb::object_store::path::Path as ObjPath;
use slatedb::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Workload shape
// ---------------------------------------------------------------------------

/// Injected latency per object-store GET, approximating production GCS reads
/// (observed p50 ~23ms).
const GET_LATENCY: Duration = Duration::from_millis(20);

/// Gate queue capacity == grants per invocation. Production hot-queue
/// invocations grant ~18-75 tasks each.
const GRANT_COUNT: usize = 64;

/// Pending deferred requests behind the gate.
const WAITER_COUNT: usize = 512;

/// Releases measured in the release->lease case.
const RELEASE_ITERS: usize = 20;

/// Capacity of the small queue used by the release->lease case.
const SMALL_CAPACITY: usize = 8;
const SMALL_WAITERS: usize = 128;

const TENANT: &str = "bench-tenant";
const TASK_GROUP: &str = "grant-bench";

// ---------------------------------------------------------------------------
// Targets (asserted). Baseline recorded before the fix:
//   case 1: p50 ≈ full invocation duration (~GRANT_COUNT serial GETs)
//   case 2: p50 ≈ fixed pipeline overhead
// ---------------------------------------------------------------------------

const CASE1_P50_TARGET: Duration = Duration::from_millis(300);
const CASE1_P99_TARGET: Duration = Duration::from_millis(800);
const CASE2_P50_TARGET: Duration = Duration::from_millis(250);
/// Guard: chunked commits must not blow up total invocation time.
const CASE3_INVOCATION_TARGET: Duration = Duration::from_millis(2500);

// ---------------------------------------------------------------------------
// Latency-injecting object store
// ---------------------------------------------------------------------------

/// Wraps an object store and injects `GET_LATENCY` on read operations when
/// enabled. Writes stay fast, mirroring production (WAL and puts are cheap;
/// reads pay a GCS round trip on block-cache miss).
#[derive(Debug)]
struct SlowReadStore {
    inner: Arc<dyn ObjectStore>,
    enabled: Arc<AtomicBool>,
}

impl SlowReadStore {
    fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<AtomicBool>) {
        let enabled = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                inner,
                enabled: Arc::clone(&enabled),
            }),
            enabled,
        )
    }

    async fn maybe_delay(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            tokio::time::sleep(GET_LATENCY).await;
        }
    }
}

impl std::fmt::Display for SlowReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowReadStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SlowReadStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.maybe_delay().await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjPath,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<bytes::Bytes>> {
        self.maybe_delay().await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: slatedb::object_store::CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Task group used only for fixture staging (floating-state warm jobs).
const SETUP_TASK_GROUP: &str = "grant-bench-setup";

/// Padding appended to each floating queue key so every floating-state row
/// fills its own SST block. Production floating rows are scattered across a
/// multi-GB keyspace where each point read fetches a distinct block; without
/// padding this fixture's 64 adjacent rows would share ~3 blocks and one
/// fetch would warm them all.
const FLOATING_KEY_PAD: usize = 2048;

fn floating_limit(gate_queue: &str, index: usize) -> FloatingConcurrencyLimit {
    // Huge refresh interval keeps the refresh machinery out of the measurement.
    FloatingConcurrencyLimit {
        key: format!(
            "{}-floating-{:06}-{}",
            gate_queue,
            index,
            "p".repeat(FLOATING_KEY_PAD)
        ),
        default_max_concurrency: 1_000_000,
        refresh_interval_ms: i64::MAX / 4,
        metadata: Vec::new(),
    }
}

/// Waiter `index` is gated by the shared queue, then a per-waiter floating
/// hop. The distinct floating-state rows force one cold DB read per chain
/// resume — the production shape that makes grant passes read-bound.
fn waiter_limits(gate_queue: &str, capacity: usize, index: usize) -> Vec<Limit> {
    vec![
        Limit::Concurrency(ConcurrencyLimit {
            key: gate_queue.to_string(),
            max_concurrency: capacity as u32,
        }),
        Limit::FloatingConcurrency(floating_limit(gate_queue, index)),
    ]
}

async fn open_shard(
    dir: &str,
    store: Arc<dyn ObjectStore>,
    measured_phase: bool,
) -> Arc<JobStoreShard> {
    let mut settings = slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(1)),
        ..Default::default()
    };
    if !measured_phase {
        // Setup phase: roll memtables to L0 aggressively so fixture rows land
        // in SSTs. Otherwise the whole small dataset stays in the WAL, replays
        // into the memtable on reopen, and measured reads never touch the
        // (latency-injected) object store.
        settings.l0_sst_size_bytes = 128 * 1024;
    }
    JobStoreShard::open_with_resolved_store(
        "grant-bench".to_string(),
        dir,
        OpenShardOptions {
            store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(settings),
            memory_cache: if measured_phase {
                // Moderate block cache: the first touch of any fixture row
                // pays the injected store latency (the grant path's cost —
                // its rows are distinct-per-candidate), while re-reads of a
                // just-loaded block stay warm (the claim path's reality in
                // production, where the validate stage has already pulled the
                // job rows into a large cache).
                Some(silo::settings::MemoryCacheConfig {
                    block_cache_bytes: Some(32 * 1024 * 1024),
                    meta_cache_bytes: None,
                })
            } else {
                None
            },
            rate_limiter: NullGubernatorClient::new(),
            metrics: None,
            // Keep the periodic reconciler out of the measurement window.
            concurrency_reconcile_interval: Duration::from_secs(3600),
            counter_reconciliation_seconds: None,
            hydrate_all_at_startup: false,
            completed_job_expire_s: None,
            terminal_job_expire_s: None,
            count_from_status_counters: true,
            grant_scanner: silo::concurrency::GrantScannerConfig::default(),
            concurrency_reconcile_scan_slice:
                silo::settings::DEFAULT_CONCURRENCY_RECONCILE_SCAN_SLICE,
            holder_drift_scan_slice: silo::settings::DEFAULT_HOLDER_DRIFT_SCAN_SLICE,
        },
        ShardRange::full(),
    )
    .await
    .expect("open bench shard")
}

/// Build a saturated-queue fixture: `capacity` leased holders filling the gate
/// and `waiters` deferred requests behind it, flushed to SSTs via a
/// close/reopen so measured reads miss the block cache. Latency is off for the
/// whole setup. Returns the reopened shard and the leased holder task ids.
async fn build_saturated_fixture(
    dir_name: &str,
    gate_queue: &str,
    capacity: usize,
    waiters: usize,
    stop_scanner: bool,
) -> (Arc<JobStoreShard>, Arc<AtomicBool>, Vec<String>) {
    let root = std::path::Path::new("./tmp/grant-latency-bench");
    let shard_dir = root.join(dir_name);
    let _ = std::fs::remove_dir_all(&shard_dir);
    std::fs::create_dir_all(&shard_dir).expect("create bench dir");
    let canonical_root = root
        .canonicalize()
        .expect("canonicalize bench root")
        .to_string_lossy()
        .to_string();

    let local: Arc<dyn ObjectStore> = Arc::new(
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(&canonical_root)
            .expect("create LocalFileSystem"),
    );
    let (slow_store, latency_enabled) = SlowReadStore::new(local);
    let store: Arc<dyn ObjectStore> = slow_store;

    let start = now_ms();

    // Phase A: populate (fast, everything in memtable/WAL).
    let shard = open_shard(dir_name, Arc::clone(&store), false).await;
    shard.stop_grant_scanner();

    for i in 0..capacity {
        shard
            .enqueue(
                TENANT,
                Some(format!("{}-holder-{:06}", dir_name, i)),
                50,
                start,
                None,
                vec![1, 2, 3],
                waiter_limits(gate_queue, capacity, i),
                None,
                TASK_GROUP,
            )
            .await
            .expect("enqueue holder");
    }

    let mut holder_task_ids = Vec::with_capacity(capacity);
    while holder_task_ids.len() < capacity {
        let batch = (capacity - holder_task_ids.len()).min(50);
        let result = shard
            .dequeue("bench-setup-worker", TASK_GROUP, batch)
            .await
            .expect("dequeue holders");
        for task in &result.tasks {
            holder_task_ids.push(task.attempt().task_id().to_string());
        }
    }

    // Warm jobs: create each waiter's floating-state row now so the measured
    // chain resume performs a genuine cold read of an existing distinct row
    // (a missing row would short-circuit through the bloom filter). The warm
    // jobs run in a setup-only task group and are never dequeued; their
    // floating holders are irrelevant at 1M capacity.
    for j in 0..waiters {
        shard
            .enqueue(
                TENANT,
                Some(format!("{}-warm-{:06}", dir_name, j)),
                50,
                start,
                None,
                vec![1, 2, 3],
                vec![Limit::FloatingConcurrency(floating_limit(
                    gate_queue,
                    capacity + j,
                ))],
                None,
                SETUP_TASK_GROUP,
            )
            .await
            .expect("enqueue warm job");
    }

    shard.close().await.expect("close shard after setup");

    // Phase B: reopen for measurement. The gate is still saturated by the
    // durable holders, so nothing is grantable yet.
    let shard = open_shard(dir_name, store, true).await;

    if stop_scanner {
        // stop_grant_scanner flips the run flag but cannot cancel an
        // in-flight startup-sweep invocation; give it a moment to finish
        // idle (no requests exist yet) before staging grantable state.
        shard.stop_grant_scanner();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Waiters enqueue against the saturated gate and park as deferred
    // requests. They are written post-reopen so the request front and the
    // waiters' job rows are memtable-resident — matching production, where
    // the hot queue's front is continuously rewritten by ticket conversions
    // and stays warm. The cold rows are exactly the floating states above.
    for j in 0..waiters {
        shard
            .enqueue(
                TENANT,
                Some(format!("{}-waiter-{:06}", dir_name, j)),
                50,
                start,
                None,
                vec![1, 2, 3],
                waiter_limits(gate_queue, capacity, capacity + j),
                None,
                TASK_GROUP,
            )
            .await
            .expect("enqueue waiter");
    }

    (shard, latency_enabled, holder_task_ids)
}

// ---------------------------------------------------------------------------
// Measurement helpers
// ---------------------------------------------------------------------------

struct LatencyStats {
    label: String,
    sorted: Vec<Duration>,
}

impl LatencyStats {
    fn from(label: &str, mut samples: Vec<Duration>) -> Self {
        samples.sort();
        Self {
            label: label.to_string(),
            sorted: samples,
        }
    }

    fn p50(&self) -> Duration {
        self.sorted[self.sorted.len() / 2]
    }

    fn p99(&self) -> Duration {
        let idx = (self.sorted.len() as f64 * 0.99).ceil() as usize - 1;
        self.sorted[idx.min(self.sorted.len() - 1)]
    }

    fn print(&self) {
        println!(
            "  {:<28} n={:<4} p50={:<10?} p99={:<10?} min={:<10?} max={:?}",
            format!("{}:", self.label),
            self.sorted.len(),
            self.p50(),
            self.p99(),
            self.sorted.first().unwrap(),
            self.sorted.last().unwrap(),
        );
    }
}

/// Number of concurrent dequeue pollers during measurement. Production runs
/// hundreds of pollers against each shard; claim capacity must not be the
/// bottleneck when measuring the grant pipeline.
const COLLECTOR_WORKERS: usize = 4;

/// Poll `dequeue` from several concurrent workers until `expected` tasks have
/// been leased (or timeout), recording the instant each lease was observed.
async fn collect_leases(
    shard: Arc<JobStoreShard>,
    expected: usize,
    timeout: Duration,
    lease_instants: Arc<Mutex<Vec<Instant>>>,
) {
    let deadline = Instant::now() + timeout;
    let collected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut workers = Vec::with_capacity(COLLECTOR_WORKERS);
    for w in 0..COLLECTOR_WORKERS {
        let shard = Arc::clone(&shard);
        let lease_instants = Arc::clone(&lease_instants);
        let collected = Arc::clone(&collected);
        workers.push(tokio::spawn(async move {
            let worker_id = format!("bench-measure-worker-{}", w);
            while collected.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
                let result = shard
                    .dequeue(&worker_id, TASK_GROUP, 16)
                    .await
                    .expect("measurement dequeue");
                let observed = Instant::now();
                if result.tasks.is_empty() {
                    continue; // claim_ready_or_nudge already waited ~25ms internally
                }
                collected.fetch_add(result.tasks.len(), Ordering::Relaxed);
                let mut instants = lease_instants.lock().unwrap();
                for _ in &result.tasks {
                    instants.push(observed);
                }
            }
        }));
    }
    for worker in workers {
        worker.await.expect("collector worker");
    }
}

// ---------------------------------------------------------------------------
// Case 1 + 3: grant_to_leasable and invocation duration
// ---------------------------------------------------------------------------

async fn bench_grant_to_leasable() -> (LatencyStats, Duration) {
    println!(
        "--- case 1: grant_to_leasable ({} grants, {} waiters, {:?} GET latency) ---",
        GRANT_COUNT, WAITER_COUNT, GET_LATENCY
    );
    let gate_queue = "bench-gate";
    // Case 1 drives process_grants directly; the fixture stops the background
    // scanner (before any grantable state exists) so it cannot race the
    // measured invocation for the released capacity.
    let (shard, latency_enabled, holder_task_ids) =
        build_saturated_fixture("case1", gate_queue, GRANT_COUNT, WAITER_COUNT, true).await;

    for task_id in &holder_task_ids {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: Vec::new() })
            .await
            .expect("release holder");
    }

    // Sanity: nothing may be leasable before the measured invocation runs.
    let pre = shard
        .dequeue("bench-sanity-worker", TASK_GROUP, 1)
        .await
        .expect("sanity dequeue");
    assert!(
        pre.tasks.is_empty(),
        "unexpected leasable task before process_grants; a racing grant occurred"
    );

    latency_enabled.store(true, Ordering::Relaxed);

    let lease_instants = Arc::new(Mutex::new(Vec::with_capacity(GRANT_COUNT)));
    let collector = tokio::spawn(collect_leases(
        Arc::clone(&shard),
        GRANT_COUNT,
        Duration::from_secs(120),
        Arc::clone(&lease_instants),
    ));

    let t0 = Instant::now();
    let granted = shard
        .process_concurrency_grants(TENANT, gate_queue, GRANT_COUNT as u32)
        .await;
    let invocation_duration = t0.elapsed();
    assert_eq!(granted.len(), GRANT_COUNT, "expected all grants to land");

    collector.await.expect("collector task");
    latency_enabled.store(false, Ordering::Relaxed);

    let samples: Vec<Duration> = lease_instants
        .lock()
        .unwrap()
        .iter()
        .map(|leased_at| leased_at.duration_since(t0))
        .collect();
    assert_eq!(
        samples.len(),
        GRANT_COUNT,
        "measurement dequeuer timed out before all grants were leased"
    );

    let stats = LatencyStats::from("grant_to_leasable", samples);
    stats.print();
    println!("  invocation_duration:         {:?}", invocation_duration);

    shard.close().await.expect("close case1 shard");
    println!();
    (stats, invocation_duration)
}

// ---------------------------------------------------------------------------
// Case 2: release_to_lease (grant scanner running)
// ---------------------------------------------------------------------------

async fn bench_release_to_lease() -> LatencyStats {
    println!(
        "--- case 2: release_to_lease ({} releases, scanner running, {:?} GET latency) ---",
        RELEASE_ITERS, GET_LATENCY
    );
    let gate_queue = "bench-gate-small";
    let (shard, latency_enabled, holder_task_ids) =
        build_saturated_fixture("case2", gate_queue, SMALL_CAPACITY, SMALL_WAITERS, false).await;

    latency_enabled.store(true, Ordering::Relaxed);

    // Rolling release: complete one holder, wait for the scanner to grant and
    // the dequeuer to lease its replacement, which becomes the next holder.
    let mut current_holder = holder_task_ids[0].clone();
    let mut samples = Vec::with_capacity(RELEASE_ITERS);
    for _ in 0..RELEASE_ITERS {
        let t0 = Instant::now();
        shard
            .report_attempt_outcome(
                &current_holder,
                AttemptOutcome::Success { result: Vec::new() },
            )
            .await
            .expect("release holder");

        let deadline = Instant::now() + Duration::from_secs(30);
        let leased = loop {
            let result = shard
                .dequeue("bench-measure-worker", TASK_GROUP, 1)
                .await
                .expect("measurement dequeue");
            if let Some(task) = result.tasks.first() {
                break task.attempt().task_id().to_string();
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for a lease after release"
            );
        };
        samples.push(t0.elapsed());
        current_holder = leased;
    }

    latency_enabled.store(false, Ordering::Relaxed);
    let stats = LatencyStats::from("release_to_lease", samples);
    stats.print();

    shard.close().await.expect("close case2 shard");
    println!();
    stats
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("\n========================================");
    println!("Grant Latency Benchmark");
    println!("========================================\n");

    let (case1, invocation_duration) = bench_grant_to_leasable().await;
    let case2 = bench_release_to_lease().await;

    println!("--- thresholds ---");
    println!(
        "  case1 p50 {:?} (target {:?}), p99 {:?} (target {:?})",
        case1.p50(),
        CASE1_P50_TARGET,
        case1.p99(),
        CASE1_P99_TARGET,
    );
    println!(
        "  case2 p50 {:?} (target {:?})",
        case2.p50(),
        CASE2_P50_TARGET,
    );
    println!(
        "  case3 invocation {:?} (target {:?})",
        invocation_duration, CASE3_INVOCATION_TARGET,
    );

    let mut failures = Vec::new();
    if case1.p50() > CASE1_P50_TARGET {
        failures.push(format!(
            "case1 p50 {:?} exceeds target {:?}",
            case1.p50(),
            CASE1_P50_TARGET
        ));
    }
    if case1.p99() > CASE1_P99_TARGET {
        failures.push(format!(
            "case1 p99 {:?} exceeds target {:?}",
            case1.p99(),
            CASE1_P99_TARGET
        ));
    }
    if case2.p50() > CASE2_P50_TARGET {
        failures.push(format!(
            "case2 p50 {:?} exceeds target {:?}",
            case2.p50(),
            CASE2_P50_TARGET
        ));
    }
    if invocation_duration > CASE3_INVOCATION_TARGET {
        failures.push(format!(
            "case3 invocation {:?} exceeds target {:?}",
            invocation_duration, CASE3_INVOCATION_TARGET
        ));
    }

    if failures.is_empty() {
        println!("\nAll grant-latency targets met.");
    } else {
        for f in &failures {
            eprintln!("TARGET MISSED: {}", f);
        }
        std::process::exit(1);
    }
}
