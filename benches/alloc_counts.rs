//! Heap allocations per shard operation.
//!
//! Reads jemalloc's allocation counters (small and large `nmalloc`, summed
//! over all arenas) around each operation and prints allocations per op:
//! - `micro`: key encoding and parsing, flatbuffer encoding.
//! - `plain`, `md`, `bga`, `conc`: future-dated enqueue, ready enqueue
//!   (including the broker's scans), dequeue, heartbeat, and success report,
//!   for jobs with no metadata, with metadata, with background-action queue
//!   metadata, and with a concurrency limit. Each phase runs `ROUNDS` times
//!   with `IN_FLIGHT` operations in flight and reports the minimum, since
//!   background work (WAL flushes, broker scans) only adds allocations.
//!
//! Counts are exact only with jemalloc's thread cache off, so the bench reruns
//! itself with `tcache:false` appended to `_RJEM_MALLOC_CONF`.
//!
//! Run with:
//!   cargo bench --bench alloc_counts
//!
//! Run one scenario (`micro`, `plain`, `md`, `bga`, `conc`):
//!   ALLOC_SCENARIO=bga cargo bench --bench alloc_counts
use futures::StreamExt;
use silo::gubernator::NullGubernatorClient;
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::ShardRange;
use std::hint::black_box as bb;
use std::sync::Arc;
use std::time::{Duration, Instant};

const IN_FLIGHT: usize = 64;
const ROUNDS: usize = 3;
const JOBS: usize = 3000;

/// jemalloc's config variable for this build's `_rjem_` symbol prefix.
const MALLOC_CONF_ENV: &str = "_RJEM_MALLOC_CONF";
const TCACHE_OFF: &str = "tcache:false";

/// An allocation served from a thread cache never reaches the arena counters.
/// Turning the cache off per thread would miss threads the bench doesn't
/// start, so rerun this binary with it off for the whole process.
fn rerun_with_tcache_off() -> ! {
    let conf = std::env::var(MALLOC_CONF_ENV).unwrap_or_default();
    assert!(
        !conf.ends_with(TCACHE_OFF),
        "jemalloc kept its thread cache with {MALLOC_CONF_ENV}={conf}"
    );
    let conf = if conf.is_empty() {
        TCACHE_OFF.to_string()
    } else {
        format!("{conf},{TCACHE_OFF}")
    };
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(std::env::args_os().skip(1))
        .env(MALLOC_CONF_ENV, conf)
        .status()
        .unwrap();
    std::process::exit(status.code().unwrap_or(1));
}

fn nmalloc() -> u64 {
    tikv_jemalloc_ctl::epoch::advance().unwrap();
    let small: u64 =
        unsafe { tikv_jemalloc_ctl::raw::read(b"stats.arenas.4096.small.nmalloc\0") }.unwrap();
    let large: u64 =
        unsafe { tikv_jemalloc_ctl::raw::read(b"stats.arenas.4096.large.nmalloc\0") }.unwrap();
    small + large
}

struct Meter {
    start: u64,
    t: Instant,
}
impl Meter {
    fn start() -> Self {
        Self {
            start: nmalloc(),
            t: Instant::now(),
        }
    }
    fn report(self, label: &str, ops: usize) {
        let allocs = nmalloc() - self.start;
        println!(
            "{label:<52} {:>9.1} allocs/op   ({ops} ops, {:.2?})",
            allocs as f64 / ops as f64,
            self.t.elapsed()
        );
    }
}

async fn open_shard(
    metrics: Option<silo::metrics::Metrics>,
) -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "alloc-shard".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(2)),
            ..Default::default()
        }),
        // Terminal jobs' rows get a TTL, as in the example configs.
        completed_job_expire_s: Some(60),
        terminal_job_expire_s: Some(900),
        ..Default::default()
    };
    let shard = JobStoreShard::open(
        &cfg,
        NullGubernatorClient::new(),
        metrics,
        ShardRange::full(),
    )
    .await
    .unwrap();
    (tmp, shard)
}

fn now_ms() -> i64 {
    silo::job_store_shard::now_epoch_ms()
}

fn micro() {
    use silo::keys::*;
    const N: usize = 100_000;
    let tenant = "tenant-abcdef";
    let job_id = "0f8fad5b-d9cb-469f-a165-70867728950e";
    let task_id = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

    let m = Meter::start();
    for _ in 0..N {
        bb(job_status_key(bb(tenant), job_id));
    }
    m.report("micro: job_status_key", N);

    let m = Meter::start();
    for i in 0..N {
        bb(task_key(
            "default",
            1_700_000_000_000 + i as i64,
            50,
            job_id,
            1,
            1_700_000_000_000,
        ));
    }
    m.report("micro: task_key", N);

    let tk = task_key(
        "default",
        1_700_000_000_000,
        50,
        job_id,
        1,
        1_700_000_000_000,
    );
    let m = Meter::start();
    for _ in 0..N {
        bb(parse_task_key(bb(&tk)));
    }
    m.report("micro: parse_task_key", N);

    let m = Meter::start();
    for _ in 0..N {
        bb(parse_task_key_ref(bb(&tk)));
    }
    m.report("micro: parse_task_key_ref", N);

    let ck = concurrency_holder_key(tenant, "queue-key", task_id);
    let m = Meter::start();
    for _ in 0..N {
        bb(parse_concurrency_holder_key(bb(&ck)));
    }
    m.report("micro: parse_concurrency_holder_key", N);

    use silo::codec::*;
    use silo::task::{LeaseRecord, Task};
    let task = Task::RunAttempt {
        id: task_id.to_string(),
        tenant: tenant.to_string(),
        job_id: job_id.to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        held_queues: vec!["q1".to_string()],
        task_group: "default".to_string(),
    };
    let m = Meter::start();
    for _ in 0..N {
        bb(encode_task(bb(&task)));
    }
    m.report("micro: encode_task", N);
    let lease = LeaseRecord {
        worker_id: "worker-1".to_string(),
        task: task.clone(),
        expiry_ms: 1,
        started_at_ms: 1,
    };
    let m = Meter::start();
    for _ in 0..N {
        bb(encode_lease(bb(&lease)));
    }
    m.report("micro: encode_lease", N);
    let m = Meter::start();
    for _ in 0..N {
        bb(encode_job_status(bb(&silo::job::JobStatus::running(1))));
    }
    m.report("micro: encode_job_status", N);
}

async fn ops(
    metrics: Option<silo::metrics::Metrics>,
    jobs: usize,
    limits: Vec<Limit>,
    md: Option<Vec<(String, String)>>,
    label: &str,
) {
    let (_tmp, shard) = open_shard(metrics).await;
    let payload = vec![7u8; 256];

    // Warm-up cycle so lazily-built state (brokers, metric children) exists.
    shard
        .enqueue(
            "t1",
            None,
            50,
            0,
            None,
            payload.clone(),
            limits.clone(),
            md.clone(),
            "g",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    for t in shard.dequeue("w", "g", 1).await.unwrap().tasks {
        shard
            .report_attempt_outcome(
                t.attempt().task_id(),
                AttemptOutcome::Success { result: vec![] },
            )
            .await
            .unwrap();
    }

    // Each phase runs ROUNDS times; the minimum per-op count is reported,
    // since background work (WAL flush batching, broker rescans) only adds.
    let mut best = [f64::MAX; 5];
    let mut keep = |i: usize, allocs: u64, ops: usize| {
        best[i] = best[i].min(allocs as f64 / ops.max(1) as f64);
    };
    for _ in 0..ROUNDS {
        // Future-dated enqueues: no broker wakeups, so this is enqueue alone.
        let far = now_ms() + 86_400_000;
        let start = nmalloc();
        futures::stream::iter(0..jobs)
            .for_each_concurrent(IN_FLIGHT, |_| {
                let (shard, payload, limits, md) =
                    (&shard, payload.clone(), limits.clone(), md.clone());
                async move {
                    shard
                        .enqueue("t1", None, 50, far, None, payload, limits, md, "future")
                        .await
                        .unwrap();
                }
            })
            .await;
        keep(0, nmalloc() - start, jobs);

        // Ready enqueues into a backlog: includes the broker's rescans.
        let start = nmalloc();
        futures::stream::iter(0..jobs)
            .for_each_concurrent(IN_FLIGHT, |_| {
                let (shard, payload, limits, md) =
                    (&shard, payload.clone(), limits.clone(), md.clone());
                async move {
                    shard
                        .enqueue("t1", None, 50, 0, None, payload, limits, md, "g")
                        .await
                        .unwrap();
                }
            })
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        keep(1, nmalloc() - start, jobs);

        // Dequeue with several concurrent workers.
        let start = nmalloc();
        let leased = Arc::new(std::sync::Mutex::new(Vec::with_capacity(jobs)));
        let workers: Vec<_> = (0..8)
            .map(|w| {
                let (shard, leased) = (Arc::clone(&shard), Arc::clone(&leased));
                tokio::spawn(async move {
                    let worker = format!("w{w}");
                    let mut empty = 0;
                    while empty < 20 {
                        let tasks = shard.dequeue(&worker, "g", 8).await.unwrap().tasks;
                        if tasks.is_empty() {
                            empty += 1;
                        }
                        let mut l = leased.lock().unwrap();
                        for t in tasks {
                            l.push((worker.clone(), t.attempt().task_id().to_string()));
                        }
                        if l.len() >= jobs {
                            break;
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.await.unwrap();
        }
        let leased = std::mem::take(&mut *leased.lock().unwrap());
        keep(2, nmalloc() - start, leased.len());

        let start = nmalloc();
        futures::stream::iter(leased.iter())
            .for_each_concurrent(IN_FLIGHT, |(w, id)| {
                let shard = &shard;
                async move {
                    shard.heartbeat_task(w, id).await.unwrap();
                }
            })
            .await;
        keep(3, nmalloc() - start, leased.len());

        let start = nmalloc();
        futures::stream::iter(leased.iter())
            .for_each_concurrent(IN_FLIGHT, |(_, id)| {
                let shard = &shard;
                async move {
                    shard
                        .report_attempt_outcome(
                            id,
                            AttemptOutcome::Success {
                                result: vec![1; 64],
                            },
                        )
                        .await
                        .unwrap();
                }
            })
            .await;
        keep(4, nmalloc() - start, leased.len());
    }
    let names = [
        "enqueue{} (future-dated)",
        "enqueue{} (ready, + broker rescans)",
        "dequeue{} (8 workers x batch 8) per task",
        "heartbeat{}",
        "report success{}",
    ];
    for (name, allocs) in names.iter().zip(best) {
        let name = name.replace("{}", label);
        println!("{name:<52} {allocs:>9.1} allocs/op   (min of {ROUNDS} x {jobs} ops)");
    }
    shard.close().await.unwrap();
}

fn main() {
    let tcache: bool = unsafe { tikv_jemalloc_ctl::raw::read(b"opt.tcache\0") }.unwrap();
    if tcache {
        rerun_with_tcache_off();
    }
    // ALLOC_SCENARIO=<name> runs one scenario (micro, plain, md, bga, conc).
    let only = std::env::var("ALLOC_SCENARIO").ok();
    let run = |name: &str| only.as_deref().is_none_or(|o| o == name);
    if run("micro") {
        micro();
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let metrics = silo::metrics::init().ok();
        let pairs = |kv: &[(&str, &str)]| {
            Some(
                kv.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<Vec<_>>(),
            )
        };
        if run("plain") {
            ops(metrics.clone(), JOBS, vec![], None, "").await;
        }
        if run("md") {
            ops(
                metrics.clone(),
                JOBS,
                vec![],
                pairs(&[("key1", "value1"), ("key2", "value2")]),
                " +md",
            )
            .await;
        }
        if run("bga") {
            let bga = pairs(&[
                ("queue", "emails"),
                ("platformConcurrencyQueue", "env-123-queue"),
            ]);
            ops(metrics.clone(), JOBS, vec![], bga, " +bga").await;
        }
        if run("conc") {
            let cl = vec![Limit::Concurrency(ConcurrencyLimit {
                key: "ck".to_string(),
                max_concurrency: 1_000_000,
            })];
            ops(metrics.clone(), JOBS, cl, None, " +conc").await;
        }
    });
}
