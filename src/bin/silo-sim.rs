use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::Rng;
use rand::SeedableRng;
use silo::coordination::{Coordinator, EtcdCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{AppConfig, Backend, DatabaseConfig, DatabaseTemplate, LogFormat};

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
    /// Enable Zipf-distributed load (hot shard simulation)
    #[arg(long)]
    zipf: bool,
    /// Zipf exponent (higher = more skewed, default 1.0)
    #[arg(long, default_value = "1.0")]
    zipf_exponent: f64,
    /// Enable load tracking and output stats
    #[arg(long)]
    track_load: bool,
    /// Interval for load stats output in seconds
    #[arg(long, default_value = "10")]
    stats_interval_secs: u64,
}

/// Zipf distribution for shard selection (simulates hot shards)
struct ZipfDistribution {
    #[allow(dead_code)]
    weights: Vec<f64>,
    cumulative: Vec<f64>,
}

impl ZipfDistribution {
    fn new(n: usize, exponent: f64) -> Self {
        let mut weights = Vec::with_capacity(n);
        for i in 1..=n {
            weights.push(1.0 / (i as f64).powf(exponent));
        }
        let sum: f64 = weights.iter().sum();
        for w in &mut weights {
            *w /= sum;
        }
        
        let mut cumulative = Vec::with_capacity(n);
        let mut acc = 0.0;
        for w in &weights {
            acc += w;
            cumulative.push(acc);
        }
        
        Self { weights, cumulative }
    }
    
    fn sample(&self, rng: &mut impl Rng) -> usize {
        let p: f64 = rng.gen();
        match self.cumulative.binary_search_by(|w| w.partial_cmp(&p).unwrap()) {
            Ok(i) => i,
            Err(i) => i.min(self.cumulative.len() - 1),
        }
    }
}

/// Load tracker for monitoring shard activity
struct LoadTracker {
    shard_enqueues: Vec<AtomicU64>,
    shard_dequeues: Vec<AtomicU64>,
    last_report: Mutex<Instant>,
}

impl LoadTracker {
    fn new(num_shards: usize) -> Self {
        let mut shard_enqueues = Vec::with_capacity(num_shards);
        let mut shard_dequeues = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shard_enqueues.push(AtomicU64::new(0));
            shard_dequeues.push(AtomicU64::new(0));
        }
        Self {
            shard_enqueues,
            shard_dequeues,
            last_report: Mutex::new(Instant::now()),
        }
    }
    
    fn record_enqueue(&self, shard_id: usize) {
        if shard_id < self.shard_enqueues.len() {
            self.shard_enqueues[shard_id].fetch_add(1, Ordering::Relaxed);
        }
    }
    
    fn record_dequeue(&self, shard_id: usize) {
        if shard_id < self.shard_dequeues.len() {
            self.shard_dequeues[shard_id].fetch_add(1, Ordering::Relaxed);
        }
    }
    
    fn report_stats(&self) {
        let mut last = self.last_report.lock().unwrap();
        let elapsed = last.elapsed();
        let secs = elapsed.as_secs_f64();
        
        println!("\n=== Load Statistics (last {:.1}s) ===", secs);
        
        let mut total_enq = 0u64;
        let mut total_deq = 0u64;
        let mut enq_rates: Vec<f64> = Vec::new();
        
        for i in 0..self.shard_enqueues.len() {
            let enq = self.shard_enqueues[i].swap(0, Ordering::Relaxed);
            let deq = self.shard_dequeues[i].swap(0, Ordering::Relaxed);
            total_enq += enq;
            total_deq += deq;
            let enq_rate = enq as f64 / secs;
            enq_rates.push(enq_rate);
            println!(
                "  Shard {}: enqueue {:.1}/s, dequeue {:.1}/s",
                i,
                enq_rate,
                deq as f64 / secs
            );
        }
        
        println!("  Total: enqueue {:.1}/s, dequeue {:.1}/s", 
                 total_enq as f64 / secs, 
                 total_deq as f64 / secs);
        
        // Calculate load imbalance metrics
        if !enq_rates.is_empty() {
            let avg = enq_rates.iter().sum::<f64>() / enq_rates.len() as f64;
            let max = enq_rates.iter().cloned().fold(0.0_f64, f64::max);
            let min = enq_rates.iter().cloned().fold(f64::MAX, f64::min);
            
            if avg > 0.0 {
                println!("  Imbalance: max/avg = {:.2}x, max/min = {:.2}x", 
                         max / avg,
                         if min > 0.0 { max / min } else { f64::INFINITY });
            }
        }
        
        *last = Instant::now();
    }
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sim-{}", nanos)
}

async fn count_with_prefix(db: &slatedb::Db, prefix: &str) -> usize {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.as_bytes().to_vec();
    end.push(0xFF);
    let mut iter = db.scan::<Vec<u8>, _>(start..=end).await.expect("scan");
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    silo::trace::init(LogFormat::Text)?;
    let args = Args::parse();

    let prefix = unique_prefix();
    let num_shards: u32 = args.shards;

    let cfg = AppConfig::load(None).expect("load default config");
    let tmpdir = tempfile::tempdir()?;

    // Create dummy factories for the coordinators - sim manages shards itself
    let rate_limiter = MockGubernatorClient::new_arc();
    let factory1 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.path().join("node-a-%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        rate_limiter.clone(),
    ));
    let factory2 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.path().join("node-b-%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        rate_limiter.clone(),
    ));

    let (c1, h1) = EtcdCoordinator::start(&cfg.coordination.etcd_endpoints, &prefix, "node-a", "http://127.0.0.1:50051", num_shards, 10, factory1).await?;
    let (c2, h2) = EtcdCoordinator::start(&cfg.coordination.etcd_endpoints, &prefix, "node-b", "http://127.0.0.1:50052", num_shards, 10, factory2).await?;

    let mut shards: Vec<Arc<JobStoreShard>> = Vec::new();
    for s in 0..num_shards as usize {
        let cfg = DatabaseConfig {
            name: s.to_string(),
            backend: Backend::Fs,
            path: tmpdir
                .path()
                .join(format!("{}", s))
                .to_string_lossy()
                .to_string(),
            flush_interval_ms: Some(10), // Fast flushes for simulation
            wal: None,
            apply_wal_on_close: true,
        };
        let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter.clone()).await?;
        shards.push(shard);
    }

    let seen_attempt_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let processed_total = Arc::new(AtomicUsize::new(0));

    let queues = vec![
        ("q-alpha".to_string(), args.alpha),
        ("q-beta".to_string(), args.beta),
        ("q-gamma".to_string(), args.gamma),
    ];

    // Initialize Zipf distribution if enabled
    let zipf_dist = if args.zipf {
        println!("Zipf distribution enabled with exponent {}", args.zipf_exponent);
        Some(Arc::new(ZipfDistribution::new(num_shards as usize, args.zipf_exponent)))
    } else {
        None
    };
    
    // Initialize load tracker if enabled
    let load_tracker = if args.track_load {
        println!("Load tracking enabled, stats every {}s", args.stats_interval_secs);
        Some(Arc::new(LoadTracker::new(num_shards as usize)))
    } else {
        None
    };

    let shards_for_enq = Arc::new(shards);
    let c1_owned = c1.clone();
    let c2_owned = c2.clone();
    let enq_running = Arc::new(AtomicBool::new(true));
    let queues_enq = queues.clone();
    let zipf_for_enq = zipf_dist.clone();
    let tracker_for_enq = load_tracker.clone();
    let enq_handle = {
        let shards = Arc::clone(&shards_for_enq);
        let enq_running = enq_running.clone();
        tokio::spawn(async move {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            let mut i: u64 = 0;
            loop {
                if !enq_running.load(Ordering::SeqCst) {
                    break;
                }
                let owned1 = c1_owned.owned_shards().await;
                let owned2 = c2_owned.owned_shards().await;
                let candidates: Vec<u32> = owned1.into_iter().chain(owned2.into_iter()).collect();
                if candidates.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                
                // Select shard - use Zipf if enabled, otherwise round-robin
                let shard_id = if let Some(ref zipf) = zipf_for_enq {
                    // Zipf selects from all shards (creates hot shards)
                    zipf.sample(&mut rng)
                } else {
                    // Round-robin across candidates
                    candidates[(i as usize) % candidates.len()] as usize
                };
                
                let (q_name, q_limit) = &queues_enq[(i as usize) % queues_enq.len()];
                let shard = &shards[shard_id];
                let payload = serde_json::json!({"i": i, "queue": q_name});
                let payload_bytes = rmp_serde::to_vec(&payload).unwrap();
                let result = shard
                    .enqueue(
                        "-",
                        None,
                        (i % 100) as u8,
                        now_ms().await,
                        None,
                        payload_bytes,
                        vec![Limit::Concurrency(ConcurrencyLimit {
                            key: q_name.clone(),
                            max_concurrency: *q_limit,
                        })],
                        None,
                    )
                    .await;
                
                // Track load if enabled
                if result.is_ok() {
                    if let Some(ref tracker) = tracker_for_enq {
                        tracker.record_enqueue(shard_id);
                    }
                }
                
                i = i.wrapping_add(1);
                if i % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    };

    let workers_running = Arc::new(AtomicBool::new(true));
    let mut worker_handles = Vec::new();
    for s in 0..num_shards as usize {
        let shard = shards_for_enq[s].clone();
        let workers_running = workers_running.clone();
        let seen = Arc::clone(&seen_attempt_ids);
        let processed = Arc::clone(&processed_total);
        let tracker_for_worker = load_tracker.clone();
        let handle = tokio::spawn(async move {
            let wid = format!("w-{}", s);
            loop {
                if !workers_running.load(Ordering::SeqCst) {
                    break;
                }
                let result = shard.dequeue(&wid, 4).await.unwrap_or_default();
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
                    let _ = shard
                        .report_attempt_outcome(
                            "-",
                            &tid,
                            AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await;
                    processed.fetch_add(1, Ordering::Relaxed);
                    
                    // Track dequeue if enabled
                    if let Some(ref tracker) = tracker_for_worker {
                        tracker.record_dequeue(s);
                    }
                }
            }
        });
        worker_handles.push(handle);
    }
    
    // Stats reporting task if load tracking is enabled
    let stats_running = Arc::new(AtomicBool::new(true));
    let stats_handle = if let Some(tracker) = load_tracker.clone() {
        let stats_running = stats_running.clone();
        let interval = Duration::from_secs(args.stats_interval_secs);
        Some(tokio::spawn(async move {
            while stats_running.load(Ordering::SeqCst) {
                tokio::time::sleep(interval).await;
                tracker.report_stats();
            }
        }))
    } else {
        None
    };

    let checker_running = Arc::new(AtomicBool::new(true));
    let shards_for_check = Arc::clone(&shards_for_enq);
    let upper_bound_total = queues.iter().map(|(_, lim)| *lim as usize).sum::<usize>();
    let check_handle = {
        let checker_running = checker_running.clone();
        tokio::spawn(async move {
            while checker_running.load(Ordering::SeqCst) {
                let mut holders_total = 0usize;
                for shard in shards_for_check.iter() {
                    holders_total += count_with_prefix(shard.db(), "holders/").await;
                }
                assert!(
                    holders_total <= upper_bound_total,
                    "over-grant across cluster: {} > {}",
                    holders_total,
                    upper_bound_total
                );

                let mut requests_total = 0usize;
                for shard in shards_for_check.iter() {
                    requests_total += count_with_prefix(shard.db(), "requests/").await;
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

    // Add third coordinator mid-run
    tokio::time::sleep(Duration::from_secs(1)).await;
    let factory3 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.path().join("node-c-%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        rate_limiter.clone(),
    ));
    let (c3, h3) = EtcdCoordinator::start(&cfg.coordination.etcd_endpoints, &prefix, "node-c", "http://127.0.0.1:50053", num_shards, 10, factory3).await?;

    // Run for desired duration
    tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;

    // Stop producers/consumers and invariant checker
    enq_running.store(false, Ordering::SeqCst);
    let _ = enq_handle.await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    workers_running.store(false, Ordering::SeqCst);
    for h in worker_handles {
        let _ = h.await;
    }
    checker_running.store(false, Ordering::SeqCst);
    let _ = check_handle.await;
    
    // Stop stats reporter if running
    stats_running.store(false, Ordering::SeqCst);
    if let Some(h) = stats_handle {
        let _ = h.await;
    }
    
    // Final load stats report
    if let Some(ref tracker) = load_tracker {
        tracker.report_stats();
    }

    for shard in shards_for_enq.iter() {
        assert_eq!(
            count_with_prefix(shard.db(), "holders/").await,
            0,
            "holders must be empty at end"
        );
        assert_eq!(
            count_with_prefix(shard.db(), "requests/").await,
            0,
            "requests must be empty at end"
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
