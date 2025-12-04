use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use silo::coordination::{Coordinator, EtcdCoordinator};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{AppConfig, Backend, DatabaseConfig, DatabaseTemplate};

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
    silo::trace::init_from_env()?;
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
        },
        rate_limiter.clone(),
    ));
    let factory2 = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.path().join("node-b-%shard%").to_string_lossy().to_string(),
            wal: None,
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

    let shards_for_enq = Arc::new(shards);
    let c1_owned = c1.clone();
    let c2_owned = c2.clone();
    let enq_running = Arc::new(AtomicBool::new(true));
    let queues_enq = queues.clone();
    let enq_handle = {
        let shards = Arc::clone(&shards_for_enq);
        let enq_running = enq_running.clone();
        tokio::spawn(async move {
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
                let (q_name, q_limit) = &queues_enq[(i as usize) % queues_enq.len()];
                let shard_id = candidates[(i as usize) % candidates.len()] as usize;
                let shard = &shards[shard_id];
                let payload = serde_json::json!({"i": i, "queue": q_name});
                let _ = shard
                    .enqueue(
                        "-",
                        None,
                        (i % 100) as u8,
                        now_ms().await,
                        None,
                        payload,
                        vec![Limit::Concurrency(ConcurrencyLimit {
                            key: q_name.clone(),
                            max_concurrency: *q_limit,
                        })],
                        None,
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
    let mut worker_handles = Vec::new();
    for s in 0..num_shards as usize {
        let shard = shards_for_enq[s].clone();
        let workers_running = workers_running.clone();
        let seen = Arc::clone(&seen_attempt_ids);
        let processed = Arc::clone(&processed_total);
        let handle = tokio::spawn(async move {
            let wid = format!("w-{}", s);
            loop {
                if !workers_running.load(Ordering::SeqCst) {
                    break;
                }
                let result = shard.dequeue("-", &wid, 4).await.unwrap_or_default();
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
                }
            }
        });
        worker_handles.push(handle);
    }

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
