//! Investigative benchmark: does hash-prefixing `concurrency_request` keys
//! actually speed up the per-shard reconciliation scan?
//!
//! Today, `JobStoreShard::reconcile_pending_requests` (src/concurrency.rs)
//! scans the entire `concurrency_requests_prefix()` range, parses each key,
//! and filters by `ShardRange::contains_tenant(parsed.tenant)`. The key is
//! prefixed by the raw tenant string (src/keys.rs), but shard ownership is
//! defined over `xxh64(tenant)`, so a shard's tenants are scattered across
//! the keyspace — every shard reads every key and discards `(S-1)/S`.
//!
//! This bench measures the cost of that scan, and compares it to a
//! hypothetical hash-prefixed key layout where the shard's `[start, end)`
//! hash range maps to one contiguous slice of bytes.
//!
//! Both variants are seeded into independent on-disk SlateDB instances with
//! identical logical content, then scanned 30 times each (after a 3-iter
//! warmup) for several `(tenants, requests_per_tenant, simulated_shards)`
//! cells. We report p50/p99/mean scan time and how many keys each variant
//! actually read off disk.
//!
//! The bench owns its own bench-local hash-prefixed key encoding; no
//! production key layout is touched.

use silo::instrumented_db::InstrumentedDb;
use silo::keys::{
    concurrency_request_key, concurrency_requests_prefix, end_bound, parse_concurrency_request_key,
};
use silo::scan_options;
use silo::shard_range::{ShardRange, hash_tenant};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storekey::{decode, encode_vec};
use tracing::Span;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const WARMUP_ITERS: usize = 3;
const MEASURED_ITERS: usize = 30;

const PREFIX_CONCURRENCY_REQUEST: u8 = 0x08;

/// Bench-local hash-prefixed concurrency_request key. Layout:
/// `[0x08] + encode((hash_tenant(tenant), tenant, queue, start_ms, prio, job, attempt, suffix))`,
/// using nested tuples to stay within storekey's 6-element limit.
fn hashed_concurrency_request_key(
    tenant: &str,
    queue: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
    suffix: &str,
) -> Vec<u8> {
    let hash = hash_tenant(tenant);
    let mut buf = vec![PREFIX_CONCURRENCY_REQUEST];
    let payload = encode_vec(&(
        (hash.as_str(), tenant, queue, start_time_ms.max(0) as u64),
        (priority, job_id, attempt_number, suffix),
    ))
    .expect("storekey encode");
    buf.extend(payload);
    buf
}

#[allow(clippy::type_complexity)]
fn parse_hashed_concurrency_request_key(key: &[u8]) -> Option<(String, String)> {
    if key.first() != Some(&PREFIX_CONCURRENCY_REQUEST) {
        return None;
    }
    let ((_hash, tenant, queue, _start), (_prio, _job, _attempt, _suffix)): (
        (String, String, String, u64),
        (u8, String, u32, String),
    ) = decode(&key[1..]).ok()?;
    Some((tenant, queue))
}

/// Encode the hash-prefixed start bound for a `ShardRange`'s start.
/// Empty start string means "from the beginning of the prefix".
fn hashed_range_start(range: &ShardRange) -> Vec<u8> {
    if range.start.is_empty() {
        vec![PREFIX_CONCURRENCY_REQUEST]
    } else {
        let mut buf = vec![PREFIX_CONCURRENCY_REQUEST];
        buf.extend(encode_vec(&(range.start.as_str(),)).expect("storekey encode"));
        buf
    }
}

/// Encode the hash-prefixed exclusive end bound for a `ShardRange`'s end.
/// Empty end string means "to the end of the prefix" — use `end_bound`.
fn hashed_range_end(range: &ShardRange) -> Vec<u8> {
    if range.end.is_empty() {
        end_bound(&[PREFIX_CONCURRENCY_REQUEST])
    } else {
        let mut buf = vec![PREFIX_CONCURRENCY_REQUEST];
        buf.extend(encode_vec(&(range.end.as_str(),)).expect("storekey encode"));
        buf
    }
}

// ---------------------------------------------------------------------------
// Test matrix
// ---------------------------------------------------------------------------

struct Cell {
    tenants: usize,
    requests_per_tenant: usize,
    /// Number of shards the keyspace is divided into. The "current" shard
    /// owns 1/`shards` of the hash space.
    shards: usize,
}

fn matrix() -> Vec<Cell> {
    let mut cells = Vec::new();
    for &t in &[300usize, 3000] {
        for &k in &[10usize, 100] {
            for &s in &[1usize, 4, 16] {
                cells.push(Cell {
                    tenants: t,
                    requests_per_tenant: k,
                    shards: s,
                });
            }
        }
    }
    cells
}

/// Build a `ShardRange` covering 1/`shards` of the hash keyspace, picking
/// the slice that starts at index 0 (so its start is always unbounded-ish
/// — concretely `""` for s=1, or `"0000000000000000"` for the first slot
/// of a split).
fn shard_n_of(shards: usize) -> ShardRange {
    if shards <= 1 {
        return ShardRange::full();
    }
    let slot = u64::MAX / shards as u64;
    let end = format!("{:016x}", slot);
    ShardRange::new(String::new(), end)
}

// ---------------------------------------------------------------------------
// SlateDB setup
// ---------------------------------------------------------------------------

async fn open_db(path: &std::path::Path) -> Arc<InstrumentedDb> {
    let object_store = Arc::new(
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(path)
            .expect("local fs object store"),
    );
    let db = slatedb::DbBuilder::new("bench-db", object_store)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build()
        .await
        .expect("build slatedb");
    InstrumentedDb::new(Arc::new(db), Span::current())
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Layout {
    Raw,
    Hashed,
}

fn make_key(
    layout: Layout,
    tenant: &str,
    queue: &str,
    start_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
    suffix: &str,
) -> Vec<u8> {
    match layout {
        Layout::Raw => {
            concurrency_request_key(tenant, queue, start_ms, priority, job_id, attempt, suffix)
        }
        Layout::Hashed => {
            hashed_concurrency_request_key(tenant, queue, start_ms, priority, job_id, attempt, suffix)
        }
    }
}

async fn seed(db: &InstrumentedDb, layout: Layout, cell: &Cell) {
    let now_ms: i64 = 1_700_000_000_000;
    let queue = "q";
    // Batch writes to reduce overhead.
    let mut batch = slatedb::WriteBatch::new();
    let mut staged = 0usize;
    for t in 0..cell.tenants {
        let tenant = format!("tenant_{:06}", t);
        for r in 0..cell.requests_per_tenant {
            let job_id = format!("job_{:08}", r);
            let key = make_key(
                layout,
                &tenant,
                queue,
                now_ms + r as i64,
                50,
                &job_id,
                0,
                "s",
            );
            batch.put(&key, b"".as_slice());
            staged += 1;
            if staged >= 1024 {
                let b = std::mem::replace(&mut batch, slatedb::WriteBatch::new());
                db.write(b).await.expect("write batch");
                staged = 0;
            }
        }
    }
    if staged > 0 {
        db.write(batch).await.expect("write final batch");
    }
    // Force everything to SSTs so scans hit a stable, comparable surface.
    db.flush_with_options(slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    })
    .await
    .expect("flush memtable");
}

// ---------------------------------------------------------------------------
// Scan kernels
// ---------------------------------------------------------------------------

/// Variant A: current layout — scan whole `[0x08]…` range, parse, post-filter.
async fn scan_raw(db: &InstrumentedDb, range: &ShardRange) -> ScanStats {
    let start = concurrency_requests_prefix();
    let end = end_bound(&start);
    let opts = scan_options();
    let mut iter = db
        .scan_with_options::<Vec<u8>, _>(start..end, &opts)
        .await
        .expect("scan");

    let mut stats = ScanStats::default();
    loop {
        match iter.next().await {
            Ok(Some(kv)) => {
                stats.read += 1;
                let Some(parsed) = parse_concurrency_request_key(&kv.key) else {
                    continue;
                };
                if !range.contains_tenant(&parsed.tenant) {
                    continue;
                }
                stats.kept += 1;
                stats.bump(&parsed.tenant, &parsed.queue);
            }
            Ok(None) => break,
            Err(e) => panic!("scan error: {e}"),
        }
    }
    stats
}

/// Variant B: hash-prefixed layout — scan only the shard's hash range, no filter.
async fn scan_hashed(db: &InstrumentedDb, range: &ShardRange) -> ScanStats {
    let start = hashed_range_start(range);
    let end = hashed_range_end(range);
    let opts = scan_options();
    let mut iter = db
        .scan_with_options::<Vec<u8>, _>(start..end, &opts)
        .await
        .expect("scan");

    let mut stats = ScanStats::default();
    loop {
        match iter.next().await {
            Ok(Some(kv)) => {
                stats.read += 1;
                let Some((tenant, queue)) = parse_hashed_concurrency_request_key(&kv.key) else {
                    continue;
                };
                stats.kept += 1;
                stats.bump(&tenant, &queue);
            }
            Ok(None) => break,
            Err(e) => panic!("scan error: {e}"),
        }
    }
    stats
}

#[derive(Default, Clone)]
struct ScanStats {
    read: usize,
    kept: usize,
    counts: HashMap<(String, String), u32>,
}

impl ScanStats {
    fn bump(&mut self, tenant: &str, queue: &str) {
        *self
            .counts
            .entry((tenant.to_string(), queue.to_string()))
            .or_insert(0) += 1;
    }
}

// ---------------------------------------------------------------------------
// Timing helpers (manual, matches benches/bench_helpers.rs style)
// ---------------------------------------------------------------------------

struct TimingResult {
    label: String,
    durations: Vec<Duration>,
    read_per_iter: usize,
    kept_per_iter: usize,
}

impl TimingResult {
    fn p(&self, q: f64) -> Duration {
        let idx = ((self.durations.len() as f64) * q).ceil() as usize;
        let idx = idx.saturating_sub(1).min(self.durations.len() - 1);
        self.durations[idx]
    }
    fn mean(&self) -> Duration {
        let total: Duration = self.durations.iter().sum();
        total / self.durations.len() as u32
    }
    fn discard_pct(&self) -> f64 {
        if self.read_per_iter == 0 {
            return 0.0;
        }
        100.0 * (self.read_per_iter - self.kept_per_iter) as f64 / self.read_per_iter as f64
    }
}

fn fmt_dur(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else if ms >= 1.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.0}us", ms * 1000.0)
    }
}

async fn time_scan<F, Fut>(label: &str, mut run: F) -> TimingResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ScanStats>,
{
    for _ in 0..WARMUP_ITERS {
        let _ = run().await;
    }
    let mut durations = Vec::with_capacity(MEASURED_ITERS);
    let mut last: Option<ScanStats> = None;
    for _ in 0..MEASURED_ITERS {
        let t0 = Instant::now();
        let stats = run().await;
        durations.push(t0.elapsed());
        last = Some(stats);
    }
    durations.sort();
    let stats = last.expect("at least one measured iter");
    TimingResult {
        label: label.to_string(),
        durations,
        read_per_iter: stats.read,
        kept_per_iter: stats.kept,
    }
}

// ---------------------------------------------------------------------------
// Cell runner
// ---------------------------------------------------------------------------

async fn run_cell(cell: &Cell) {
    let range = shard_n_of(cell.shards);
    let total_keys = cell.tenants * cell.requests_per_tenant;
    println!(
        "--- T={} K={} S={} (total_keys={}, shard_range={})",
        cell.tenants, cell.requests_per_tenant, cell.shards, total_keys, range,
    );

    let raw_dir = tempfile::tempdir().expect("tempdir raw");
    let hashed_dir = tempfile::tempdir().expect("tempdir hashed");

    let raw_db = open_db(raw_dir.path()).await;
    let hashed_db = open_db(hashed_dir.path()).await;

    seed(&raw_db, Layout::Raw, cell).await;
    seed(&hashed_db, Layout::Hashed, cell).await;

    let raw = time_scan("raw    (filter)", || {
        let db = Arc::clone(&raw_db);
        let range = range.clone();
        async move { scan_raw(&db, &range).await }
    })
    .await;

    let hashed = time_scan("hashed (no filter)", || {
        let db = Arc::clone(&hashed_db);
        let range = range.clone();
        async move { scan_hashed(&db, &range).await }
    })
    .await;

    // Correctness check: both variants must agree on the (tenant, queue) counts.
    let raw_counts = scan_raw(&raw_db, &range).await.counts;
    let hashed_counts = scan_hashed(&hashed_db, &range).await.counts;
    assert_eq!(
        raw_counts, hashed_counts,
        "variant counts diverged — bench key encoding is wrong"
    );

    for r in [&raw, &hashed] {
        println!(
            "  {:<22} p50={:<8} p99={:<8} mean={:<8} read={:<8} kept={:<8} discard={:>5.1}%",
            r.label,
            fmt_dur(r.p(0.5)),
            fmt_dur(r.p(0.99)),
            fmt_dur(r.mean()),
            r.read_per_iter,
            r.kept_per_iter,
            r.discard_pct(),
        );
    }

    let speedup = raw.mean().as_secs_f64() / hashed.mean().as_secs_f64().max(1e-9);
    println!("  speedup (raw_mean / hashed_mean): {:.2}x", speedup);

    // Best-effort close. Errors here aren't interesting for the benchmark.
    let _ = raw_db.close().await;
    let _ = hashed_db.close().await;
    println!();
}

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("concurrency_request scan: raw vs hash-prefixed");
    println!("========================================\n");
    println!(
        "warmup={} measured={} per (variant, cell)\n",
        WARMUP_ITERS, MEASURED_ITERS
    );

    for cell in matrix() {
        run_cell(&cell).await;
    }

    println!("Done.");
}
