use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use silo::coordination::{Coordinator, EtcdCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::job::{
    ConcurrencyLimit, FloatingConcurrencyLimit, GubernatorAlgorithm, GubernatorRateLimit, Limit,
    RateLimitRetryPolicy,
};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys;
use silo::settings::{AppConfig, Backend, DatabaseConfig, DatabaseTemplate, LogFormat};
use slatedb::WriteBatch;

#[derive(Parser, Debug)]
struct Args {
    /// Duration to run the simulation, in seconds
    #[arg(long, default_value = "180")]
    duration_secs: u64,
    /// Number of shards to open locally
    #[arg(long, default_value = "8")]
    shards: u32,
    /// Concurrency limit for q-alpha
    #[arg(long, default_value = "1")]
    alpha: u32,
    /// Concurrency limit for q-beta
    #[arg(long, default_value = "2")]
    beta: u32,
    /// Concurrency limit for q-gamma
    #[arg(long, default_value = "8")]
    gamma: u32,
    /// RNG seed (random if unset). Echoed at startup so failures are reproducible.
    #[arg(long)]
    seed: Option<u64>,
    /// Fraction of jobs enqueued with a single RateLimit limit
    #[arg(long, default_value = "0.10")]
    rate_limit_fraction: f64,
    /// Fraction of jobs enqueued with chained Concurrency + RateLimit limits
    #[arg(long, default_value = "0.15")]
    chained_fraction: f64,
    /// Fraction of jobs enqueued with a single FloatingConcurrency limit
    #[arg(long, default_value = "0.05")]
    floating_fraction: f64,
    /// Per-task probability that a worker abandons the lease without acking
    #[arg(long, default_value = "0.005")]
    worker_crash_prob: f64,
    /// Per-tick probability the injector deletes a random job_info row
    #[arg(long, default_value = "0.002")]
    job_info_delete_prob: f64,
    /// Whether to spawn a background lease reaper
    #[arg(long, default_value_t = true)]
    spawn_reaper: bool,
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sim-{}", nanos)
}

/// Count rows under a binary key prefix. Note: the previous version of this
/// helper accepted a `&str` and used `prefix.as_bytes()`, which silently
/// scanned an unrelated ASCII range (e.g., "holders/" starts with 0x68 while
/// the actual concurrency-holder prefix is 0x09). That made the simulator's
/// holder/lease/request invariants vacuous. Always pass the binary prefix from
/// `keys::*_prefix()`.
async fn count_with_binary_prefix(db: &slatedb::Db, prefix: &[u8]) -> usize {
    let start: Vec<u8> = prefix.to_vec();
    let end: Vec<u8> = keys::end_bound(prefix);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut n = 0usize;
    loop {
        let maybe = iter.next().await.expect("next");
        if maybe.is_none() {
            break;
        }
        n += 1;
    }
    n
}

async fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Build a `GubernatorRateLimit` for the simulator. With `force_deny`, the
/// limit is set to 0 so the mock returns `under_limit:false` and the
/// CheckRateLimit task exhausts retries and exercises the max-retries early
/// return path. Otherwise the limit is high enough that the mock approves.
fn rate_limit_for(q_name: &str, i: u64, force_deny: bool, max_retries: u32) -> GubernatorRateLimit {
    GubernatorRateLimit {
        name: format!("{}-rl", q_name),
        unique_key: format!("{}-{}", q_name, i),
        limit: if force_deny { 0 } else { 1_000_000 },
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 25,
            max_backoff_ms: 250,
            backoff_multiplier: 2.0,
            max_retries,
        },
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    silo::trace::init(LogFormat::Text)?;
    let args = Args::parse();

    let prefix = unique_prefix();
    let num_shards: u32 = args.shards;

    let cfg = AppConfig::load(None).expect("load default config");
    let tmpdir = tempfile::tempdir()?;

    let seed = args.seed.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    eprintln!("silo-sim: seed={}", seed);
    let rng: Arc<Mutex<StdRng>> = Arc::new(Mutex::new(StdRng::seed_from_u64(seed)));

    // Create dummy factories for the coordinators - sim manages shards itself
    let rate_limiter = MockGubernatorClient::new_arc();
    let factory1 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir
                .path()
                .join("node-a-%shard%")
                .to_string_lossy()
                .to_string(),
            wal: None,
            apply_wal_on_close: true,
            concurrency_reconcile_interval_ms: 5000,
            slatedb: None,
            memory_cache: None,
        },
        rate_limiter.clone(),
        None,
    ));
    let factory2 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir
                .path()
                .join("node-b-%shard%")
                .to_string_lossy()
                .to_string(),
            wal: None,
            apply_wal_on_close: true,
            concurrency_reconcile_interval_ms: 5000,
            slatedb: None,
            memory_cache: None,
        },
        rate_limiter.clone(),
        None,
    ));

    let (c1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-a",
        "http://127.0.0.1:7450",
        num_shards,
        10,
        factory1,
        Vec::new(),
    )
    .await?;
    let (c2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-b",
        "http://127.0.0.1:7451",
        num_shards,
        10,
        factory2,
        Vec::new(),
    )
    .await?;

    // Get the shard map from coordinator to get actual shard IDs
    let shard_map = c1.get_shard_map().await?;
    let shard_ids: Vec<silo::shard_range::ShardId> = shard_map.shard_ids();

    let mut shards: std::collections::HashMap<silo::shard_range::ShardId, Arc<JobStoreShard>> =
        std::collections::HashMap::new();
    for shard_id in &shard_ids {
        let range = shard_map
            .get_shard(shard_id)
            .map(|info| info.range.clone())
            .expect("shard should exist in shard map");
        let cfg = DatabaseConfig {
            name: shard_id.to_string(),
            backend: Backend::Fs,
            path: tmpdir
                .path()
                .join(shard_id.to_string())
                .to_string_lossy()
                .to_string(),
            wal: None,
            apply_wal_on_close: true,
            // Fast flushes for simulation
            slatedb: Some(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(10)),
                ..Default::default()
            }),
            memory_cache: None,
        };
        let shard = JobStoreShard::open(&cfg, rate_limiter.clone(), None, range).await?;
        shards.insert(*shard_id, shard);
    }

    let seen_attempt_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let processed_total = Arc::new(AtomicUsize::new(0));

    let queues = vec![
        ("q-alpha".to_string(), args.alpha),
        ("q-beta".to_string(), args.beta),
        ("q-gamma".to_string(), args.gamma),
    ];

    let shards_for_enq = Arc::new(shards);
    let shard_ids_for_enq = Arc::new(shard_ids.clone());
    let c1_owned = c1.clone();
    let c2_owned = c2.clone();
    let enq_running = Arc::new(AtomicBool::new(true));
    let queues_enq = queues.clone();
    let rate_limit_fraction = args.rate_limit_fraction;
    let chained_fraction = args.chained_fraction;
    let floating_fraction = args.floating_fraction;
    let enq_handle = {
        let shards = Arc::clone(&shards_for_enq);
        let _shard_ids = Arc::clone(&shard_ids_for_enq);
        let enq_running = enq_running.clone();
        let rng = Arc::clone(&rng);
        tokio::spawn(async move {
            let mut i: u64 = 0;
            loop {
                if !enq_running.load(Ordering::SeqCst) {
                    break;
                }
                let owned1 = c1_owned.owned_shards().await;
                let owned2 = c2_owned.owned_shards().await;
                let candidates: Vec<silo::shard_range::ShardId> =
                    owned1.into_iter().chain(owned2.into_iter()).collect();
                if candidates.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                let (q_name, q_limit) = &queues_enq[(i as usize) % queues_enq.len()];
                let shard_id = candidates[(i as usize) % candidates.len()];
                let Some(shard) = shards.get(&shard_id) else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };

                let (roll, force_deny): (f64, bool) = {
                    let mut g = rng.lock().unwrap();
                    (g.random::<f64>(), g.random::<f64>() < 0.5)
                };
                let limits: Vec<Limit> = if roll < rate_limit_fraction {
                    // Pure RateLimit job. Half configured to deny so the
                    // CheckRateLimit max-retries early return fires.
                    vec![Limit::RateLimit(rate_limit_for(
                        q_name, i, force_deny, 2,
                    ))]
                } else if roll < rate_limit_fraction + chained_fraction {
                    // Chained Concurrency -> RateLimit (Bug 2 / Bug 3 shape).
                    vec![
                        Limit::Concurrency(ConcurrencyLimit {
                            key: q_name.clone(),
                            max_concurrency: *q_limit,
                        }),
                        Limit::RateLimit(rate_limit_for(q_name, i, force_deny, 2)),
                    ]
                } else if roll < rate_limit_fraction + chained_fraction + floating_fraction {
                    vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                        key: format!("{}-float", q_name),
                        default_max_concurrency: *q_limit,
                        refresh_interval_ms: 5_000,
                        metadata: vec![],
                    })]
                } else {
                    vec![Limit::Concurrency(ConcurrencyLimit {
                        key: q_name.clone(),
                        max_concurrency: *q_limit,
                    })]
                };

                let payload = serde_json::json!({"i": i, "queue": q_name});
                let payload_bytes = rmp_serde::to_vec(&payload).unwrap();
                let _ = shard
                    .enqueue(
                        "-",
                        None,
                        (i % 100) as u8,
                        now_ms().await,
                        None,
                        payload_bytes,
                        limits,
                        None,
                        "default",
                    )
                    .await;
                i = i.wrapping_add(1);
                if i % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    };

    let workers_running = Arc::new(AtomicBool::new(true));
    let leaked_acks = Arc::new(AtomicUsize::new(0));
    let worker_crash_prob = args.worker_crash_prob;
    let mut worker_handles = Vec::new();
    for (idx, shard_id) in shard_ids.iter().enumerate() {
        let shard = shards_for_enq
            .get(shard_id)
            .expect("shard must exist")
            .clone();
        let workers_running = workers_running.clone();
        let seen = Arc::clone(&seen_attempt_ids);
        let processed = Arc::clone(&processed_total);
        let leaked_acks = Arc::clone(&leaked_acks);
        let rng = Arc::clone(&rng);
        let handle = tokio::spawn(async move {
            let wid = format!("w-{}", idx);
            loop {
                if !workers_running.load(Ordering::SeqCst) {
                    break;
                }
                let result = shard.dequeue(&wid, "default", 4).await.unwrap_or_default();
                if result.tasks.is_empty() {
                    tokio::task::yield_now().await;
                    continue;
                }
                for t in result.tasks {
                    let tid = t.attempt().task_id().to_string();
                    {
                        let mut g = seen.lock().unwrap();
                        assert!(
                            g.insert(tid.clone()),
                            "duplicate attempt id dequeued: {}",
                            tid
                        );
                    }
                    // Crash injection: with low probability, abandon the lease
                    // without acking. The reaper must recover holders.
                    let should_crash = {
                        let mut g = rng.lock().unwrap();
                        g.random::<f64>() < worker_crash_prob
                    };
                    if should_crash {
                        leaked_acks.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let _ = shard
                        .report_attempt_outcome(
                            &tid,
                            AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await;
                    processed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        worker_handles.push(handle);
    }

    let checker_running = Arc::new(AtomicBool::new(true));
    let shards_for_check = Arc::clone(&shards_for_enq);
    // Concurrency-typed jobs cap holders at the per-queue limit. RateLimit and
    // FloatingConcurrency jobs can produce holders independent of that cap, so
    // the steady-state ceiling is approximate — keep a generous slack so the
    // mid-run check still bites on egregious over-grant.
    let upper_bound_total =
        queues.iter().map(|(_, lim)| *lim as usize).sum::<usize>() + 256;
    let check_handle = {
        let checker_running = checker_running.clone();
        tokio::spawn(async move {
            while checker_running.load(Ordering::SeqCst) {
                let mut holders_total = 0usize;
                for shard in shards_for_check.values() {
                    holders_total += count_with_binary_prefix(
                        shard.db(),
                        &keys::concurrency_holders_prefix(),
                    )
                    .await;
                }
                assert!(
                    holders_total <= upper_bound_total,
                    "over-grant across cluster: {} > {}",
                    holders_total,
                    upper_bound_total
                );

                let mut requests_total = 0usize;
                for shard in shards_for_check.values() {
                    requests_total += count_with_binary_prefix(
                        shard.db(),
                        &keys::concurrency_requests_prefix(),
                    )
                    .await;
                }
                assert!(
                    requests_total < 2000,
                    "excessive queued concurrency requests: {}",
                    requests_total
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // Background lease reaper — without it, worker-crash injection has no
    // recovery path and end-of-run holder count won't drain.
    let reaper_running = Arc::new(AtomicBool::new(true));
    let reaper_handle = if args.spawn_reaper {
        let shards = Arc::clone(&shards_for_enq);
        let running = reaper_running.clone();
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            while running.load(Ordering::SeqCst) {
                tick.tick().await;
                for shard in shards.values() {
                    let _ = shard.reap_expired_leases("-").await;
                }
            }
        }))
    } else {
        None
    };

    // job_info deletion injector — exercises the missing-job_info dequeue
    // paths in handle_run_attempt and handle_check_rate_limit.
    let injector_running = Arc::new(AtomicBool::new(true));
    let injector_handle = {
        let shards = Arc::clone(&shards_for_enq);
        let running = injector_running.clone();
        let rng = Arc::clone(&rng);
        let prob = args.job_info_delete_prob;
        let job_info_deletes = Arc::new(AtomicUsize::new(0));
        let job_info_deletes_for_task = Arc::clone(&job_info_deletes);
        let handle = tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let roll: f64 = rng.lock().unwrap().random();
                if roll >= prob {
                    continue;
                }
                let shard_ids: Vec<_> = shards.keys().copied().collect();
                if shard_ids.is_empty() {
                    continue;
                }
                let pick_idx: usize =
                    rng.lock().unwrap().random_range(0..shard_ids.len());
                let Some(shard) = shards.get(&shard_ids[pick_idx]) else {
                    continue;
                };
                // Collect a small sample of job_info keys; pick one and delete.
                let prefix = keys::jobs_prefix();
                let end = keys::end_bound(&prefix);
                let mut iter = match shard.db().scan::<Vec<u8>, _>(prefix..end).await {
                    Ok(it) => it,
                    Err(_) => continue,
                };
                let mut sampled: Vec<Vec<u8>> = Vec::with_capacity(16);
                while sampled.len() < 16 {
                    match iter.next().await {
                        Ok(Some(kv)) => sampled.push(kv.key.to_vec()),
                        _ => break,
                    }
                }
                if sampled.is_empty() {
                    continue;
                }
                let pick: usize =
                    rng.lock().unwrap().random_range(0..sampled.len());
                let mut batch = WriteBatch::new();
                batch.delete(&sampled[pick]);
                if shard.db().write(batch).await.is_ok() {
                    job_info_deletes_for_task.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        (handle, job_info_deletes)
    };
    let (injector_handle, job_info_deletes) = injector_handle;

    // Add third coordinator mid-run
    tokio::time::sleep(Duration::from_secs(1)).await;
    let factory3 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir
                .path()
                .join("node-c-%shard%")
                .to_string_lossy()
                .to_string(),
            wal: None,
            apply_wal_on_close: true,
            concurrency_reconcile_interval_ms: 5000,
            slatedb: None,
            memory_cache: None,
        },
        rate_limiter.clone(),
        None,
    ));
    let (c3, h3) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "node-c",
        "http://127.0.0.1:7452",
        num_shards,
        10,
        factory3,
        Vec::new(),
    )
    .await?;

    // Run for desired duration
    tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;

    // Stop fault sources first so the reaper has a chance to drain.
    enq_running.store(false, Ordering::SeqCst);
    injector_running.store(false, Ordering::SeqCst);
    let _ = enq_handle.await;
    let _ = injector_handle.await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    workers_running.store(false, Ordering::SeqCst);
    for h in worker_handles {
        let _ = h.await;
    }
    // Give the reaper time to recover any leases abandoned by crashed workers.
    tokio::time::sleep(Duration::from_secs(2)).await;
    reaper_running.store(false, Ordering::SeqCst);
    if let Some(h) = reaper_handle {
        let _ = h.await;
    }
    checker_running.store(false, Ordering::SeqCst);
    let _ = check_handle.await;

    let leaked_acks_final = leaked_acks.load(Ordering::Relaxed);
    let job_info_deletes_final = job_info_deletes.load(Ordering::Relaxed);
    eprintln!(
        "silo-sim: leaked_acks={} job_info_deletes={}",
        leaked_acks_final, job_info_deletes_final
    );

    for (sid, shard) in shards_for_enq.iter() {
        let h = count_with_binary_prefix(shard.db(), &keys::concurrency_holders_prefix()).await;
        let l = count_with_binary_prefix(shard.db(), &keys::leases_prefix()).await;
        let r = count_with_binary_prefix(shard.db(), &keys::concurrency_requests_prefix()).await;
        assert_eq!(
            h, 0,
            "holders leak shard={} h={} l={} r={} seed={} leaked_acks={} job_info_deletes={}",
            sid, h, l, r, seed, leaked_acks_final, job_info_deletes_final
        );
        assert_eq!(
            r, 0,
            "requests leak shard={} h={} l={} r={} seed={}",
            sid, h, l, r, seed
        );
    }

    // cleanup
    c1.shutdown().await?;
    c2.shutdown().await?;
    c3.shutdown().await?;
    h1.abort();
    h2.abort();
    h3.abort();

    println!(
        "processed_total={}",
        processed_total.load(Ordering::Relaxed)
    );
    Ok(())
}
