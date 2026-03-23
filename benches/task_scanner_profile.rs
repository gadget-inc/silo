//! Profile the task scanner hot path.
//!
//! Creates a shard with a large number of pending tasks and repeatedly scans
//! the task key range, measuring per-scan cost. This reproduces the production
//! hot path where `TaskBroker::scan_tasks` iterates through SlateDB SSTs.
//!
//! Run with:
//!   cargo bench --bench task_scanner_profile
//!
//! Profile with:
//!   samply record cargo bench --bench task_scanner_profile

#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use silo::codec::decode_task_validated;
use silo::keys::{end_bound, parse_task_key, task_group_prefix, tasks_prefix};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Number of additional tasks to enqueue beyond what the golden shard has.
const EXTRA_TASKS: usize = 100_000;
/// How many scan iterations to run for timing.
const SCAN_ITERATIONS: usize = 200;
/// Batch size per scan (matches TaskBroker::scan_batch).
const SCAN_BATCH: usize = 1024;

/// Enqueue a large number of ready tasks into a shard for benchmarking.
async fn enqueue_many_tasks(shard: &Arc<silo::job_store_shard::JobStoreShard>, count: usize) {
    let now = now_ms();
    let batch_size = 500;
    let mut enqueued = 0;
    let start = Instant::now();

    while enqueued < count {
        let batch_end = std::cmp::min(enqueued + batch_size, count);
        let mut batch = Vec::with_capacity(batch_end - enqueued);
        for idx in enqueued..batch_end {
            let job_id = format!("scan-bench-{:08}", idx);
            batch.push(silo::job_store_shard::import::ImportJobParams {
                id: job_id,
                priority: 50,
                enqueue_time_ms: now - 86_400_000 + (idx as i64 * 17),
                start_at_ms: now, // ready immediately
                retry_policy: None,
                payload: rmp_serde::to_vec(&serde_json::json!({"bench": "scanner"})).unwrap(),
                limits: vec![],
                metadata: None,
                task_group: "scanner-bench".to_string(),
                attempts: vec![], // no attempts = Waiting status, creates a task entry
            });
        }

        shard
            .import_jobs("scanner-bench-tenant", batch)
            .await
            .expect("import scan bench jobs");

        enqueued = batch_end;
        if enqueued % 10_000 == 0 {
            let rate = enqueued as f64 / start.elapsed().as_secs_f64();
            println!(
                "  Enqueued {}/{} tasks ({:.0} jobs/sec)",
                enqueued, count, rate
            );
        }
    }

    // Flush to SSTs so the scan path is realistic
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush memtable");

    println!(
        "  Enqueued {} tasks in {:.1}s",
        count,
        start.elapsed().as_secs_f64()
    );
}

/// Count all task keys in the DB under the tasks prefix.
async fn count_all_tasks(db: &slatedb::Db) -> usize {
    let start = tasks_prefix();
    let end = end_bound(&start);
    let Ok(mut iter) = db.scan::<Vec<u8>, _>(start..end).await else {
        return 0;
    };
    let mut count = 0;
    while let Ok(Some(_)) = iter.next().await {
        count += 1;
    }
    count
}

/// Simulate one scan_tasks pass: open iterator, read up to SCAN_BATCH entries,
/// parse keys and decode values. Returns (entries_read, duration).
async fn single_scan_pass(db: &slatedb::Db, task_group: &str, now: i64) -> (usize, Duration) {
    let start_key = task_group_prefix(task_group);
    let end_key = end_bound(&start_key);

    let scan_start = Instant::now();

    let Ok(mut iter) = db.scan::<Vec<u8>, _>(start_key..end_key).await else {
        return (0, scan_start.elapsed());
    };

    let mut read = 0;
    while read < SCAN_BATCH {
        let Ok(Some(kv)) = iter.next().await else {
            break;
        };

        // Parse key (same as TaskBroker::scan_tasks)
        let Some(parsed_key) = parse_task_key(&kv.key) else {
            continue;
        };

        // Time filter check
        if parsed_key.start_time_ms > now as u64 {
            continue;
        }

        // Decode task value (same as TaskBroker::scan_tasks)
        let _decoded = decode_task_validated(kv.value.clone());

        read += 1;
    }

    (read, scan_start.elapsed())
}

/// Detailed breakdown of scan costs: iterator creation, reads, parsing, decoding.
struct ScanBreakdown {
    iter_creation: Duration,
    total_read: Duration,
    total_key_parse: Duration,
    total_value_decode: Duration,
    total_wall: Duration,
    entries: usize,
}

async fn single_scan_pass_breakdown(db: &slatedb::Db, task_group: &str, now: i64) -> ScanBreakdown {
    let start_key = task_group_prefix(task_group);
    let end_key = end_bound(&start_key);

    let wall_start = Instant::now();

    let t = Instant::now();
    let Ok(mut iter) = db.scan::<Vec<u8>, _>(start_key..end_key).await else {
        return ScanBreakdown {
            iter_creation: t.elapsed(),
            total_read: Duration::ZERO,
            total_key_parse: Duration::ZERO,
            total_value_decode: Duration::ZERO,
            total_wall: wall_start.elapsed(),
            entries: 0,
        };
    };
    let iter_creation = t.elapsed();

    let mut total_read = Duration::ZERO;
    let mut total_key_parse = Duration::ZERO;
    let mut total_value_decode = Duration::ZERO;
    let mut read = 0;

    while read < SCAN_BATCH {
        let t = Instant::now();
        let Ok(Some(kv)) = iter.next().await else {
            total_read += t.elapsed();
            break;
        };
        total_read += t.elapsed();

        let t = Instant::now();
        let Some(parsed_key) = parse_task_key(&kv.key) else {
            total_key_parse += t.elapsed();
            continue;
        };
        total_key_parse += t.elapsed();

        if parsed_key.start_time_ms > now as u64 {
            continue;
        }

        let t = Instant::now();
        let _decoded = decode_task_validated(kv.value.clone());
        total_value_decode += t.elapsed();

        read += 1;
    }

    ScanBreakdown {
        iter_creation,
        total_read,
        total_key_parse,
        total_value_decode,
        total_wall: wall_start.elapsed(),
        entries: read,
    }
}

/// Benchmark just the iterator creation cost (no reads).
async fn bench_iterator_creation(
    db: &slatedb::Db,
    task_group: &str,
    iterations: usize,
) -> Vec<Duration> {
    let start_key = task_group_prefix(task_group);
    let end_key = end_bound(&start_key);

    let mut durations = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        let _iter = db
            .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
            .await;
        durations.push(t.elapsed());
    }
    durations.sort();
    durations
}

/// Benchmark full scan passes (iterator creation + reading SCAN_BATCH entries).
async fn bench_full_scan(
    db: &slatedb::Db,
    task_group: &str,
    iterations: usize,
) -> (Vec<Duration>, usize) {
    let now = now_ms();
    let mut durations = Vec::with_capacity(iterations);
    let mut total_entries = 0;

    for _ in 0..iterations {
        let (entries, elapsed) = single_scan_pass(db, task_group, now).await;
        durations.push(elapsed);
        total_entries += entries;
    }
    durations.sort();
    (durations, total_entries)
}

/// Benchmark scanning the ENTIRE task group (all entries, not just SCAN_BATCH).
async fn bench_full_group_scan(
    db: &slatedb::Db,
    task_group: &str,
    iterations: usize,
) -> (Vec<Duration>, usize) {
    let now = now_ms();
    let start_key = task_group_prefix(task_group);
    let end_key = end_bound(&start_key);

    let mut durations = Vec::with_capacity(iterations);
    let mut last_count = 0;

    for _ in 0..iterations {
        let scan_start = Instant::now();

        let Ok(mut iter) = db
            .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
            .await
        else {
            durations.push(scan_start.elapsed());
            continue;
        };

        let mut read = 0;
        loop {
            let Ok(Some(kv)) = iter.next().await else {
                break;
            };
            let Some(parsed_key) = parse_task_key(&kv.key) else {
                continue;
            };
            if parsed_key.start_time_ms > now as u64 {
                continue;
            }
            let _decoded = decode_task_validated(kv.value.clone());
            read += 1;
        }

        durations.push(scan_start.elapsed());
        last_count = read;
    }

    durations.sort();
    (durations, last_count)
}

/// Benchmark just iterator reads with NO key parsing or value decoding.
async fn bench_raw_reads(
    db: &slatedb::Db,
    task_group: &str,
    iterations: usize,
) -> (Vec<Duration>, usize) {
    let start_key = task_group_prefix(task_group);
    let end_key = end_bound(&start_key);

    let mut durations = Vec::with_capacity(iterations);
    let mut total = 0;

    for _ in 0..iterations {
        let Ok(mut iter) = db
            .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
            .await
        else {
            continue;
        };
        let t = Instant::now();
        let mut read = 0;
        while read < SCAN_BATCH {
            let Ok(Some(_kv)) = iter.next().await else {
                break;
            };
            read += 1;
        }
        durations.push(t.elapsed());
        total += read;
    }
    durations.sort();
    (durations, total)
}

fn print_stats(label: &str, durations: &[Duration]) {
    if durations.is_empty() {
        println!("  {:<40} (no data)", label);
        return;
    }
    let p50 = durations[durations.len() / 2];
    let p99 = durations[(durations.len() as f64 * 0.99).ceil() as usize - 1];
    let mean: Duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    let min = durations[0];
    let max = durations[durations.len() - 1];
    println!(
        "  {:<40} p50={:<10} p99={:<10} mean={:<10} [min={:<10} max={}]",
        format!("{}:", label),
        format_duration(p50),
        format_duration(p99),
        format_duration(mean),
        format_duration(min),
        format_duration(max),
    );
}

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("Task Scanner Profile Benchmark");
    println!("========================================\n");

    let metadata = ensure_golden_shard().await;

    println!(
        "Cloning golden shard and adding {} extra tasks...",
        EXTRA_TASKS
    );
    let (_guard, shard) = clone_golden_shard("scanner-profile", &metadata).await;

    // Count existing tasks
    let existing_tasks = count_all_tasks(shard.db()).await;
    println!("  Existing task keys in golden shard: {}", existing_tasks);

    // Add more tasks to the "scanner-bench" task group
    enqueue_many_tasks(&shard, EXTRA_TASKS).await;

    let total_tasks = count_all_tasks(shard.db()).await;
    println!("  Total task keys after additions: {}", total_tasks);

    // Count tasks in each group
    {
        let start_key = task_group_prefix("scanner-bench");
        let end_key = end_bound(&start_key);
        let Ok(mut iter) = shard.db().scan::<Vec<u8>, _>(start_key..end_key).await else {
            panic!("failed to scan scanner-bench group");
        };
        let mut count = 0;
        while let Ok(Some(_)) = iter.next().await {
            count += 1;
        }
        println!("  Tasks in 'scanner-bench' group: {}", count);

        let start_key = task_group_prefix("");
        let end_key = end_bound(&start_key);
        let Ok(mut iter) = shard.db().scan::<Vec<u8>, _>(start_key..end_key).await else {
            panic!("failed to scan default group");
        };
        let mut count = 0;
        while let Ok(Some(_)) = iter.next().await {
            count += 1;
        }
        println!("  Tasks in default '' group: {}", count);
    }

    println!(
        "\n--- Iterator creation cost ({} iterations) ---",
        SCAN_ITERATIONS
    );

    let durations = bench_iterator_creation(shard.db(), "scanner-bench", SCAN_ITERATIONS).await;
    print_stats("iter_create(scanner-bench, 100K)", &durations);

    let durations = bench_iterator_creation(shard.db(), "", SCAN_ITERATIONS).await;
    print_stats("iter_create(default group)", &durations);

    {
        let start = tasks_prefix();
        let end = end_bound(&start);
        let mut durations = Vec::with_capacity(SCAN_ITERATIONS);
        for _ in 0..SCAN_ITERATIONS {
            let t = Instant::now();
            let _iter = shard
                .db()
                .scan::<Vec<u8>, _>(start.clone()..end.clone())
                .await;
            durations.push(t.elapsed());
        }
        durations.sort();
        print_stats("iter_create(all tasks)", &durations);
    }

    println!(
        "\n--- Raw reads (no parse/decode) {} entries ({} iters) ---",
        SCAN_BATCH, SCAN_ITERATIONS
    );

    let (durations, _) = bench_raw_reads(shard.db(), "scanner-bench", SCAN_ITERATIONS).await;
    print_stats(
        &format!("raw_read(scanner-bench, {})", SCAN_BATCH),
        &durations,
    );

    let (durations, _) = bench_raw_reads(shard.db(), "", SCAN_ITERATIONS).await;
    print_stats(&format!("raw_read(default, {})", SCAN_BATCH), &durations);

    println!(
        "\n--- Full scan {} entries per pass ({} iterations) ---",
        SCAN_BATCH, SCAN_ITERATIONS
    );

    let (durations, total) = bench_full_scan(shard.db(), "scanner-bench", SCAN_ITERATIONS).await;
    print_stats(
        &format!("scan_batch(scanner-bench, {})", SCAN_BATCH),
        &durations,
    );
    println!("    total entries read across all iterations: {}", total);

    let (durations, total) = bench_full_scan(shard.db(), "", SCAN_ITERATIONS).await;
    print_stats(&format!("scan_batch(default, {})", SCAN_BATCH), &durations);
    println!("    total entries read across all iterations: {}", total);

    println!("\n--- CPU time breakdown (100 iterations, scanner-bench group) ---");
    {
        let now = now_ms();
        let iters: u32 = 100;
        let mut iter_creation = Duration::ZERO;
        let mut total_read = Duration::ZERO;
        let mut total_key_parse = Duration::ZERO;
        let mut total_value_decode = Duration::ZERO;
        let mut total_wall = Duration::ZERO;
        let mut total_entries: usize = 0;

        for _ in 0..iters {
            let bd = single_scan_pass_breakdown(shard.db(), "scanner-bench", now).await;
            iter_creation += bd.iter_creation;
            total_read += bd.total_read;
            total_key_parse += bd.total_key_parse;
            total_value_decode += bd.total_value_decode;
            total_wall += bd.total_wall;
            total_entries += bd.entries;
        }

        let avg_wall = total_wall / iters;
        let avg_iter = iter_creation / iters;
        let avg_read = total_read / iters;
        let avg_parse = total_key_parse / iters;
        let avg_decode = total_value_decode / iters;

        println!(
            "  Average per-scan breakdown ({} entries/scan):",
            total_entries / iters as usize
        );
        println!("    wall time:         {}", format_duration(avg_wall));
        println!(
            "    iter creation:     {} ({:.1}%)",
            format_duration(avg_iter),
            avg_iter.as_secs_f64() / avg_wall.as_secs_f64() * 100.0
        );
        println!(
            "    iter.next() reads: {} ({:.1}%)",
            format_duration(avg_read),
            avg_read.as_secs_f64() / avg_wall.as_secs_f64() * 100.0
        );
        println!(
            "    key parsing:       {} ({:.1}%)",
            format_duration(avg_parse),
            avg_parse.as_secs_f64() / avg_wall.as_secs_f64() * 100.0
        );
        println!(
            "    value decoding:    {} ({:.1}%)",
            format_duration(avg_decode),
            avg_decode.as_secs_f64() / avg_wall.as_secs_f64() * 100.0
        );
        let accounted = avg_iter + avg_read + avg_parse + avg_decode;
        let overhead = avg_wall.saturating_sub(accounted);
        println!(
            "    other overhead:    {} ({:.1}%)",
            format_duration(overhead),
            overhead.as_secs_f64() / avg_wall.as_secs_f64() * 100.0
        );
    }

    // Simulate production pattern: scan with a pre-populated buffer.
    // The scanner must read through all buffered entries before finding new ones.
    println!("\n--- Production pattern: scan with pre-populated buffer ---");
    {
        use std::collections::HashSet;
        let now = now_ms();

        // Pre-populate a "buffer" by scanning first N entries
        for buffer_size in [0usize, 2048, 4096, 8192, 16384, 32768] {
            let start_key = task_group_prefix("scanner-bench");
            let end_key = end_bound(&start_key);

            // Collect the first buffer_size keys to simulate what's already in the buffer
            let mut buffered_keys: HashSet<Vec<u8>> = HashSet::new();
            if buffer_size > 0 {
                let Ok(mut iter) = shard
                    .db()
                    .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
                    .await
                else {
                    continue;
                };
                let mut collected = 0;
                while collected < buffer_size {
                    let Ok(Some(kv)) = iter.next().await else {
                        break;
                    };
                    buffered_keys.insert(kv.key.to_vec());
                    collected += 1;
                }
            }

            // Now simulate scan_tasks: scan from beginning, skip buffered, insert 1024 new
            let iters = 50;
            let mut durations = Vec::with_capacity(iters);
            let mut total_iter_next_calls = 0usize;

            for _ in 0..iters {
                let Ok(mut iter) = shard
                    .db()
                    .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
                    .await
                else {
                    continue;
                };

                let scan_start = Instant::now();
                let mut inserted = 0;
                let mut reads = 0;
                while inserted < SCAN_BATCH {
                    let Ok(Some(kv)) = iter.next().await else {
                        break;
                    };
                    reads += 1;

                    let Some(parsed_key) = parse_task_key(&kv.key) else {
                        continue;
                    };
                    if parsed_key.start_time_ms > now as u64 {
                        continue;
                    }
                    let _decoded = decode_task_validated(kv.value.clone());

                    // Skip if already in "buffer"
                    if buffered_keys.contains(&kv.key.to_vec()) {
                        continue;
                    }
                    inserted += 1;
                }
                durations.push(scan_start.elapsed());
                total_iter_next_calls += reads;
            }

            durations.sort();
            let avg_reads = total_iter_next_calls / iters;
            print_stats(
                &format!("buffer={}, +{} new", buffer_size, SCAN_BATCH),
                &durations,
            );
            println!(
                "    avg iter.next() calls: {} ({}x scan_batch)",
                avg_reads,
                avg_reads as f64 / SCAN_BATCH as f64
            );
        }
    }

    // Simulate a full refill cycle: buffer drains from target_buffer to low_watermark,
    // then the scanner refills it. Measure total CPU cost of the refill with different
    // scan_batch sizes.
    println!("\n--- Refill cycle cost: buffer 4096->8192 with varying scan_batch ---");
    {
        use std::collections::HashSet;
        let now = now_ms();
        let target_buffer = 8192usize;
        let low_watermark = target_buffer / 2; // 4096

        // Pre-populate buffer with 4096 entries (simulates post-drain state)
        let start_key = task_group_prefix("scanner-bench");
        let end_key = end_bound(&start_key);
        let mut buffered_keys: HashSet<Vec<u8>> = HashSet::new();
        {
            let Ok(mut iter) = shard
                .db()
                .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
                .await
            else {
                panic!("scan failed");
            };
            let mut collected = 0;
            while collected < low_watermark {
                let Ok(Some(kv)) = iter.next().await else {
                    break;
                };
                buffered_keys.insert(kv.key.to_vec());
                collected += 1;
            }
        }

        for scan_batch in [1024usize, 2048, 4096, 8192] {
            let iters = 20;
            let mut durations = Vec::with_capacity(iters);
            let mut total_scans = 0usize;
            let mut total_reads = 0usize;

            for _ in 0..iters {
                let cycle_start = Instant::now();
                let mut buf_len = low_watermark;
                let mut current_buffered = buffered_keys.clone();
                let mut cycle_scans = 0;
                let mut cycle_reads = 0;

                // Simulate multiple scan passes to fill buffer from low_watermark to target_buffer
                while buf_len < target_buffer {
                    let Ok(mut iter) = shard
                        .db()
                        .scan::<Vec<u8>, _>(start_key.clone()..end_key.clone())
                        .await
                    else {
                        break;
                    };

                    let mut inserted = 0;
                    while inserted < scan_batch && buf_len < target_buffer {
                        let Ok(Some(kv)) = iter.next().await else {
                            break;
                        };
                        cycle_reads += 1;

                        let Some(parsed_key) = parse_task_key(&kv.key) else {
                            continue;
                        };
                        if parsed_key.start_time_ms > now as u64 {
                            continue;
                        }
                        let _decoded = decode_task_validated(kv.value.clone());

                        let key_bytes = kv.key.to_vec();
                        if !current_buffered.contains(&key_bytes) {
                            current_buffered.insert(key_bytes);
                            buf_len += 1;
                            inserted += 1;
                        }
                    }
                    cycle_scans += 1;
                }

                durations.push(cycle_start.elapsed());
                total_scans += cycle_scans;
                total_reads += cycle_reads;
            }

            durations.sort();
            let avg_scans = total_scans as f64 / iters as f64;
            let avg_reads = total_reads as f64 / iters as f64;
            print_stats(&format!("scan_batch={}", scan_batch), &durations);
            println!(
                "    avg scans/refill: {:.1}, avg reads/refill: {:.0}, refills_at_50scan/s: {:.1}/s, est CPU: {:.2} vCPU",
                avg_scans,
                avg_reads,
                50.0 / avg_scans,
                durations[durations.len() / 2].as_secs_f64() * 50.0 / avg_scans,
            );
        }
    }

    println!("\n--- Full group scan ({} iterations) ---", 10);

    let (durations, count) = bench_full_group_scan(shard.db(), "scanner-bench", 10).await;
    print_stats(
        &format!("full_scan(scanner-bench, {} tasks)", count),
        &durations,
    );

    let (durations, count) = bench_full_group_scan(shard.db(), "", 10).await;
    print_stats(&format!("full_scan(default, {} tasks)", count), &durations);

    // Throughput estimate
    println!("\n--- Throughput estimate ---");
    let now = now_ms();
    let sustained_iters = 500;
    let sustained_start = Instant::now();
    let mut total_entries = 0;
    for _ in 0..sustained_iters {
        let (entries, _) = single_scan_pass(shard.db(), "scanner-bench", now).await;
        total_entries += entries;
    }
    let sustained_elapsed = sustained_start.elapsed();
    let scans_per_sec = sustained_iters as f64 / sustained_elapsed.as_secs_f64();
    let entries_per_sec = total_entries as f64 / sustained_elapsed.as_secs_f64();
    println!(
        "  {} scans in {:.1}s = {:.1} scans/sec ({:.0} entries/sec)",
        sustained_iters,
        sustained_elapsed.as_secs_f64(),
        scans_per_sec,
        entries_per_sec,
    );
    println!(
        "  Average {:.1}ms per scan ({} entries/scan)",
        sustained_elapsed.as_secs_f64() * 1000.0 / sustained_iters as f64,
        total_entries / sustained_iters
    );

    // Extrapolate to production: if scanning at 50 scans/sec
    let ms_per_scan = sustained_elapsed.as_secs_f64() * 1000.0 / sustained_iters as f64;
    let cpu_at_50_scans = ms_per_scan * 50.0 / 1000.0;
    println!(
        "\n  At 50 scans/sec: estimated {:.2} vCPU ({:.1}ms/scan * 50)",
        cpu_at_50_scans, ms_per_scan
    );

    shard.close().await.expect("close");
    println!("\nDone.");
}
