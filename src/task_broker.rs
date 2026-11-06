use std::collections::{HashMap, HashSet};
use std::str;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use slatedb::WriteBatch;
use slatedb::bytes::Bytes;

use crate::instrumented_db::InstrumentedDb;
use tokio::sync::Notify;

use crate::codec::{DecodedTask, decode_task_validated};
use crate::keys::{end_bound, parse_task_key_ref, task_group_prefix, task_group_time_prefix};
use crate::metrics::Metrics;
use crate::shard_range::ShardRange;
use tracing::debug;

/// A task entry stored in the in-memory broker buffer.
///
/// Stores a pre-validated `DecodedTask` (wraps `Bytes`) instead of raw bytes.
/// Validation happens once during scan; callers can access zero-copy
/// FlatBuffer accessors without re-parsing. The key is the `Bytes` the scan
/// read, so the buffer, the in-flight set, and the ack tombstones share one
/// copy of it.
#[derive(Debug, Clone)]
pub struct BrokerTask {
    pub key: Bytes,
    pub decoded: DecodedTask,
}

/// Suppression record for a durably deleted task key.
///
/// Expiry is keyed to `last_seen` so persistent stale observations extend
/// suppression; the revive bound is keyed to `inserted_at` so a key that a
/// scan keeps yielding is eventually point-read rather than hidden forever.
#[derive(Debug, Clone, Copy)]
struct AckTombstone {
    inserted_at: u64,
    last_seen: u64,
}

/// A single per-task-group broker that scans only its own key range.
///
/// - Maintains a sorted buffer of ready tasks using a skiplist keyed by the task key bytes.
/// - Populates from SlateDB in the background with exponential backoff when no work is found.
/// - Ensures tasks claimed but not yet durably leased are tracked as in-flight and not reinserted.
/// - Scans only when a scan can find something: after a write that may have
///   readied a task, when the earliest future-dated row becomes due, or when
///   the last scan stopped with ready rows remaining. An empty poll on its own
///   never triggers a scan.
/// - Starts each scan at the scan floor: the start time below which every
///   row has already been dealt with, so the SlateDB tombstones of dequeued
///   and cancelled tasks at the head of the range are not walked again.
///   Every writer of a task row lowers the floor to the row's start time, and
///   a periodic full-range scan bounds the cost of a writer that did not.
pub struct TaskBroker {
    // The task group this broker is responsible for
    task_group: String,
    // The SlateDB database to read tasks from
    db: Arc<InstrumentedDb>,
    // The buffer of tasks read out of the DB and ready to be claimed by a dequeue-ing worker
    buffer: Arc<SkipMap<Bytes, BrokerTask>>,
    // The set of tasks already read out of the DB and claimed by a worker but not yet durably leased. Required so that the scanner doesn't re-add tasks that are in the middle of being dequeued to the buffer.
    inflight: Arc<Mutex<HashSet<Bytes>>>,
    // Tombstones for task keys that were durably acked, so a stale scan cannot
    // re-buffer a row the broker has already deleted.
    ack_tombstones: Arc<Mutex<HashMap<Bytes, AckTombstone>>>,
    // Scan generations a tombstone may keep suppressing a re-observed key
    // before the scanner point-reads the row to check the delete landed.
    tombstone_revive_after_generations: u64,
    // Monotonic generation numbers for scanner iterations.
    scan_generation_started: Arc<AtomicU64>,
    scan_generation_completed: Arc<AtomicU64>,
    // Whether the background scanner is running
    running: Arc<AtomicBool>,
    // A notify object to wake up the background scanner when a task is claimed
    notify: Arc<Notify>,
    // Whether the background scanner should be woken up
    scan_requested: Arc<AtomicBool>,
    // Set by a write that may have made a task ready; cleared when a scan
    // begins, so a write that lands mid-scan forces one more pass.
    dirty: AtomicBool,
    // Where and when the next scan reads. One lock covers the floor, the
    // next-ready time and the note generation so a note and a scan's result
    // never interleave.
    cursor: Mutex<ScanCursor>,
    // How often a scan ignores the floor and reads the whole range.
    full_rescan_interval: Duration,
    // The target buffer size
    target_buffer: usize,
    // The batch size for the background scanner to read out of the DB
    scan_batch: usize,
    // The shard name for metrics labeling
    shard_name: String,
    // Optional metrics for recording broker stats
    metrics: Option<Metrics>,
    /// The shard's tenant range for filtering defunct tasks.
    range: ShardRange,
}

impl TaskBroker {
    // Keep ack tombstones for a bounded number of completed scan generations and
    // refresh `last_seen` whenever a stale key is observed again.
    const ACK_TOMBSTONE_RETAIN_GENERATIONS: u64 = 64;

    #[allow(clippy::too_many_arguments)]
    fn new(
        task_group: String,
        db: Arc<InstrumentedDb>,
        shard_name: String,
        metrics: Option<Metrics>,
        range: ShardRange,
        tombstone_revive_after_generations: u64,
        full_rescan_interval: Duration,
    ) -> Arc<Self> {
        // scan_batch must be >= target_buffer / 2 (the low watermark) so that
        // a single scan pass can refill from low-watermark to target. With a
        // smaller batch the scanner needs multiple passes and each pass
        // re-reads every already-buffered entry from the start of the key
        // range, wasting O(buffer_size) reads/pass.
        Self::new_with_limits(
            task_group,
            db,
            shard_name,
            metrics,
            range,
            tombstone_revive_after_generations,
            full_rescan_interval,
            8192,
            4096,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_limits(
        task_group: String,
        db: Arc<InstrumentedDb>,
        shard_name: String,
        metrics: Option<Metrics>,
        range: ShardRange,
        tombstone_revive_after_generations: u64,
        full_rescan_interval: Duration,
        target_buffer: usize,
        scan_batch: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            task_group,
            db,
            buffer: Arc::new(SkipMap::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            ack_tombstones: Arc::new(Mutex::new(HashMap::new())),
            scan_generation_started: Arc::new(AtomicU64::new(0)),
            scan_generation_completed: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            scan_requested: Arc::new(AtomicBool::new(false)),
            dirty: AtomicBool::new(false),
            cursor: Mutex::new(ScanCursor::new()),
            full_rescan_interval,
            target_buffer,
            scan_batch,
            shard_name,
            metrics,
            range,
            tombstone_revive_after_generations,
        })
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn inflight_len(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    fn begin_scan_generation(&self) -> u64 {
        self.scan_generation_started.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn complete_scan_generation(&self, generation: u64) {
        self.scan_generation_completed
            .store(generation, Ordering::SeqCst);
        let mut tombstones = self.ack_tombstones.lock().unwrap();
        tombstones.retain(|_, tombstone| {
            generation.saturating_sub(tombstone.last_seen) <= Self::ACK_TOMBSTONE_RETAIN_GENERATIONS
        });
    }

    /// Scan tasks from DB and insert into buffer, skipping future tasks and inflight ones.
    ///
    /// Reads from the scan floor unless a full rescan is due, and records in
    /// the cursor why the scan stopped (which gates the nudges that empty
    /// polls send, see `nudge`) and the new floor.
    async fn scan_tasks(&self, now_ms: i64, generation: u64) -> usize {
        // Clear `dirty` and read the cursor before the iterator is created: a
        // write or note that lands after this point may describe a row the
        // iterator's view does not include, so it must set `dirty` again
        // (forcing one more pass) or survive this scan's result.
        self.dirty.store(false, Ordering::SeqCst);
        let ScanStart {
            floor_ms,
            notes_before,
            full_rescan,
            cleared_floor_ms,
        } = self.begin_scan_cursor();

        // [SILO-SCAN-1] Scan only this task group's key range, from the floor
        let prefix = task_group_prefix(&self.task_group);
        let end = end_bound(&prefix);
        let start = match floor_ms {
            Some(floor_ms) => task_group_time_prefix(&self.task_group, floor_ms),
            None => prefix,
        };

        let Ok(mut iter) = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await
        else {
            self.record_scan_stop(ScanStop::MoreReady, 0, notes_before, floor_ms);
            return 0;
        };

        // Collect keys to delete for defunct tasks (outside shard range)
        let mut defunct_keys: Vec<Bytes> = Vec::new();

        let mut inserted = 0;
        let mut total_read = 0u64;
        let mut skipped_future = 0u64;
        let mut skipped_inflight = 0u64;
        let mut skipped_tombstone = 0u64;
        let mut skipped_already_buffered = 0u64;
        let mut skipped_defunct = 0u64;
        let mut revived = 0u64;
        // Rows a full rescan buffered from below the floor it cleared: rows
        // a floor-bounded scan would not have read, so a writer did not tell
        // the broker about them.
        let mut inserted_below_floor = 0u64;
        // Start time of the last row this scan dealt with. Rows sort by start
        // time, so no row below it is left for a later scan to find.
        let mut consumed_through_ms: Option<i64> = None;
        // Leaving the loop through its condition means the batch or buffer
        // limit was hit with ready rows possibly remaining.
        let mut stop = ScanStop::MoreReady;
        while inserted < self.scan_batch && self.buffer.len() < self.target_buffer {
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => {
                    stop = ScanStop::EndOfRange;
                    break;
                }
                Err(_) => break,
            };
            total_read += 1;

            // Parse the task key to extract timestamp
            let Some(parsed_key) = parse_task_key_ref(&kv.key) else {
                continue;
            };

            // Filter out future tasks. Task keys sort ascending by start_time_ms
            // within a task group (see keys.rs::task_key — timestamp is stored as a
            // plain u64, not inverted), so the first future-dated key implies every
            // remaining key in the range is also future-dated.
            if parsed_key.start_time_ms > now_ms as u64 {
                skipped_future += 1;
                stop = ScanStop::Future {
                    start_time_ms: parsed_key.start_time_ms as i64,
                };
                break;
            }

            // Every path from here deals with the row for good: it is
            // buffered, in flight, suppressed by an ack tombstone, deleted as
            // defunct, or malformed. A later scan need not read it again.
            consumed_through_ms = Some(parsed_key.start_time_ms as i64);

            // Most rows a scan reads are already buffered or in flight, so
            // every check below borrows the key; it is only cloned (a
            // refcount bump) when the row is buffered or marked defunct.
            let key: &[u8] = &kv.key;

            // [SILO-SCAN-3] Skip inflight tasks
            if self.inflight.lock().unwrap().contains(key) {
                skipped_inflight += 1;
                continue;
            }

            // Skip keys that were recently durably acked to avoid stale scan re-inserts.
            // Refresh last_seen so persistent stale observations extend suppression.
            let tombstone_age = {
                let mut tombstones = self.ack_tombstones.lock().unwrap();
                tombstones.get_mut(key).map(|tombstone| {
                    tombstone.last_seen = generation;
                    generation.saturating_sub(tombstone.inserted_at)
                })
            };
            let mut value = kv.value;
            if let Some(age) = tombstone_age {
                if age <= self.tombstone_revive_after_generations {
                    skipped_tombstone += 1;
                    continue;
                }
                // A key re-observed past the bound is either a stale scan or a
                // row rewritten under the same key after the delete. A point get
                // sees a fresher view than the range scan that yielded the key;
                // the guard is not held across it.
                match self.db.get(key).await {
                    Ok(Some(durable)) => {
                        self.ack_tombstones.lock().unwrap().remove(key);
                        revived += 1;
                        value = durable;
                    }
                    Ok(None) => {
                        // The delete is confirmed. Re-arm the bound so a stale
                        // scan costs one point read per bound window, not one
                        // per generation.
                        if let Some(tombstone) = self.ack_tombstones.lock().unwrap().get_mut(key) {
                            tombstone.inserted_at = generation;
                        }
                        skipped_tombstone += 1;
                        continue;
                    }
                    Err(e) => {
                        debug!(error = %e, "tombstone revive point read failed; keeping suppression");
                        skipped_tombstone += 1;
                        continue;
                    }
                }
            }

            let decoded = match decode_task_validated(value) {
                Ok(t) => t,
                Err(_) => continue, // Skip malformed tasks
            };

            // Check if task's tenant is within shard range
            let task_tenant = decoded.tenant();

            if !self.range.contains_tenant(task_tenant) {
                // Task is for a tenant outside our range - mark for deletion
                defunct_keys.push(kv.key.clone());
                skipped_defunct += 1;
                debug!(
                    task_group = %parsed_key.task_group,
                    job_id = %parsed_key.job_id,
                    tenant = %task_tenant,
                    range = %self.range,
                    "skipping defunct task (tenant outside shard range)"
                );
                continue;
            }

            // [SILO-SCAN-2] Insert into buffer if not already present
            if !self.buffer.contains_key(key) {
                let entry = BrokerTask {
                    key: kv.key.clone(),
                    decoded,
                };
                self.buffer.insert(kv.key.clone(), entry);
                inserted += 1;
                if cleared_floor_ms
                    .is_some_and(|cleared_ms| (parsed_key.start_time_ms as i64) < cleared_ms)
                {
                    inserted_below_floor += 1;
                }

                // Yield periodically to avoid starving other tasks
                if inserted % 16 == 0 {
                    tokio::task::yield_now().await;
                }
            } else {
                skipped_already_buffered += 1;
            }
        }

        // Delete defunct tasks from the database. Until they are gone they
        // still have to be read, so a failed delete leaves the floor alone.
        let mut defunct_deleted = true;
        if !defunct_keys.is_empty() {
            let mut batch = WriteBatch::new();
            for key in &defunct_keys {
                batch.delete(key);
            }
            if let Err(e) = self.db.write(batch).await {
                debug!(error = %e, count = defunct_keys.len(), "failed to delete defunct tasks");
                defunct_deleted = false;
            } else {
                debug!(
                    count = defunct_keys.len(),
                    "deleted defunct tasks outside shard range"
                );
            }
        }

        // The new floor. Reading the whole remaining range (to its end or to
        // the first future-dated row) proves no row at or before `now_ms` is
        // left, so the floor moves to `now_ms` even when nothing was read;
        // stopping early moves it only as far as the rows actually read.
        let new_floor_ms = if !defunct_deleted {
            floor_ms
        } else {
            match stop {
                ScanStop::EndOfRange | ScanStop::Future { .. } => Some(now_ms),
                ScanStop::MoreReady => consumed_through_ms.or(floor_ms),
            }
        };
        self.record_scan_stop(stop, skipped_tombstone, notes_before, new_floor_ms);

        if let Some(ref m) = self.metrics {
            if total_read > 0 {
                m.record_broker_scan_tasks(
                    &self.shard_name,
                    &self.task_group,
                    inserted as u64,
                    skipped_future,
                    skipped_inflight,
                    skipped_tombstone,
                    skipped_already_buffered,
                    skipped_defunct,
                    revived,
                );
                m.set_broker_tombstone_count(
                    &self.shard_name,
                    &self.task_group,
                    self.ack_tombstones.lock().unwrap().len() as u64,
                );
            }
            if full_rescan {
                m.record_broker_full_rescan(
                    &self.shard_name,
                    &self.task_group,
                    inserted_below_floor,
                );
            }
        }

        inserted
    }

    /// Take the cursor for a scan that is about to create its iterator. A
    /// periodic full rescan clears the floor, so a row that a writer failed
    /// to note is found within one interval.
    fn begin_scan_cursor(&self) -> ScanStart {
        let mut cursor = self.cursor.lock().unwrap();
        let full_rescan = cursor.last_full_scan.elapsed() >= self.full_rescan_interval;
        let mut cleared_floor_ms = None;
        if full_rescan {
            cleared_floor_ms = cursor.floor_ms.take();
            cursor.last_full_scan = Instant::now();
        }
        ScanStart {
            floor_ms: cursor.floor_ms,
            notes_before: cursor.note_generation,
            full_rescan,
            cleared_floor_ms,
        }
    }

    /// Start the background scanning loop.
    fn start(self: &Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }

        let broker = Arc::clone(self);
        tokio::spawn(async move {
            let min_sleep_ms = 50;
            let max_sleep_ms = 2000;
            let mut sleep_ms = min_sleep_ms;
            let scan_low_watermark = broker.target_buffer / 2;
            let mut scanning = false;

            loop {
                if !broker.running.load(Ordering::SeqCst) {
                    break;
                }

                // Hysteresis: start scanning when buffer drops below low
                // watermark, keep scanning until it reaches target_buffer.
                let buf_len = broker.buffer.len();
                if buf_len < scan_low_watermark {
                    scanning = true;
                } else if buf_len >= broker.target_buffer {
                    scanning = false;
                }

                if !scanning {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                // Scan for ready tasks
                let now_ms = crate::job_store_shard::now_epoch_ms();
                let scan_start = std::time::Instant::now();
                let generation = broker.begin_scan_generation();
                let inserted = broker.scan_tasks(now_ms, generation).await;
                broker.complete_scan_generation(generation);
                if let Some(ref m) = broker.metrics {
                    m.record_broker_scan_duration(
                        &broker.shard_name,
                        scan_start.elapsed().as_secs_f64(),
                    );
                    m.set_broker_buffer_size(
                        &broker.shard_name,
                        &broker.task_group,
                        broker.buffer.len() as u64,
                    );
                    m.set_broker_inflight_size(
                        &broker.shard_name,
                        &broker.task_group,
                        broker.inflight.lock().unwrap().len() as u64,
                    );
                }

                // Adjust backoff: stay aggressive when buffer needs filling
                // AND the scan actually found tasks to insert. If the scan
                // found nothing, back off even if the buffer is low — the DB
                // is empty and there's no point hammering it at 200 scans/s.
                if inserted == 0 {
                    sleep_ms = (sleep_ms * 2).min(max_sleep_ms);
                } else {
                    sleep_ms = min_sleep_ms;
                }

                // Handle explicit scan requests. When scan_requested is set
                // (wakeup fired), use minimal sleep if we found tasks, or
                // reset to min_sleep if we didn't. This prevents continuous
                // enqueues from keeping the scanner spinning at 100+ scans/s
                // (old behavior: always 1ms) while still keeping it responsive
                // (won't exponentially back off to 2s during active workloads).
                if broker.scan_requested.swap(false, Ordering::SeqCst) {
                    if inserted > 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                    // Wakeup arrived but scan found nothing new. Sleep a
                    // short duration instead of the 1ms fast-path (which
                    // causes spinning at 100+/s) or the full min_sleep_ms
                    // (50ms, too slow for the nudge loop's 25ms window).
                    // 5ms matches the nudge poll interval and caps the
                    // scan rate at ~200/s during active workloads.
                    // We must `continue` here to skip the `tokio::select!`
                    // below — wakeup() also calls notify_one(), so the
                    // stored permit would resolve notified() immediately,
                    // defeating the sleep entirely.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }

                // Sleep with early wakeup support. Wake no later than the
                // earliest future-dated row becomes due, so a scheduled task
                // is picked up on time even when no poll nudges the scanner.
                let delay_ms = broker.delay_until_next_ready_ms(sleep_ms);
                let delay = tokio::time::sleep(Duration::from_millis(delay_ms));
                tokio::pin!(delay);
                tokio::select! {
                    biased;
                    _ = broker.notify.notified() => {
                        // Halve backoff on explicit wakeup instead of
                        // resetting to min — keeps scan rate reasonable
                        // while still responding to demand.
                        sleep_ms = (sleep_ms / 2).max(min_sleep_ms);
                        debug!(task_group = %broker.task_group, "broker woken by notification");
                    }
                    _ = &mut delay => {},
                }
            }
        });
    }

    /// Stop the background loop.
    fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Claim up to `max` ready tasks from the buffer.
    ///
    /// Since each broker is scoped to a single task group, no prefix filtering is needed.
    fn claim_ready(&self, max: usize) -> Vec<BrokerTask> {
        // Idle polls land here repeatedly; skip the lock and the allocations.
        if max == 0 || self.buffer.is_empty() {
            return Vec::new();
        }
        let candidate_keys: Vec<Bytes> = {
            let mut inflight = self.inflight.lock().unwrap();
            let mut keys = Vec::with_capacity(max.min(self.buffer.len()));
            for entry in self.buffer.iter() {
                if keys.len() >= max {
                    break;
                }
                let key = entry.key();
                if inflight.contains(key) {
                    continue;
                }
                inflight.insert(key.clone());
                keys.push(key.clone());
            }
            keys
        };

        // Remove claimed entries from buffer
        let mut claimed = Vec::with_capacity(candidate_keys.len());
        for key in candidate_keys {
            if let Some(entry) = self.buffer.remove(&key) {
                claimed.push(entry.value().clone());
            } else {
                // Entry was removed between scan and removal, undo inflight reservation
                self.inflight.lock().unwrap().remove(&key);
            }
        }

        claimed
    }

    /// Try to claim tasks. If none available, wait briefly for the buffer to
    /// fill, waking the scanner only when a scan could find something.
    async fn claim_ready_or_nudge(&self, max: usize) -> Vec<BrokerTask> {
        // Try fast path first
        let claimed = self.claim_ready(max);
        if !claimed.is_empty() {
            return claimed;
        }

        // An empty poll on a quiet broker never scans: the scanner is woken
        // only when a write has landed since the last scan or a scheduled row
        // has become due (see `nudge`). The wait is kept either way, and
        // re-checked each tick, so a write that lands during it is picked up
        // by this poll instead of the next.
        let mut woken = self.nudge(crate::job_store_shard::now_epoch_ms());
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if !woken {
                woken = self.nudge(crate::job_store_shard::now_epoch_ms());
            }
            let claimed = self.claim_ready(max);
            if !claimed.is_empty() {
                return claimed;
            }
        }

        Vec::new()
    }

    /// Requeue tasks back into the buffer after a failed durable write.
    fn requeue(&self, tasks: Vec<BrokerTask>) {
        let mut inflight = self.inflight.lock().unwrap();
        let mut tombstones = self.ack_tombstones.lock().unwrap();
        for entry in tasks {
            inflight.remove(&entry.key);
            tombstones.remove(&entry.key);
            self.buffer.insert(entry.key.clone(), entry);
        }
    }

    /// Release claimed keys from in-flight tracking *without* re-buffering them.
    ///
    /// Used by the dequeue drop-guard when the dequeue future is cancelled or
    /// panics after claiming but before it could `ack_durable`/`requeue`. At
    /// that point we cannot know whether the durable commit landed.
    ///
    /// Unlike `requeue`, this does not re-insert into the buffer and installs no
    /// tombstone — re-pickup is driven by the DB scanner, which only re-adds
    /// keys still present in the DB. This matters because the claimed batch can
    /// include internal chain tasks (`RequestTicket`/`CheckRateLimit`) whose
    /// processing has non-idempotent side effects (reserving a concurrency
    /// holder, charging a Gubernator rate limit, writing the next chain task).
    /// Blindly re-buffering one whose commit *did* land would re-run those
    /// effects (double-reserved holders, double-charged limits, duplicate
    /// follow-ups; for a RunAttempt, a second lease on the same task_id).
    /// Routing through the scanner avoids that: a committed task had its key
    /// deleted in the same batch (not re-added → no double-processing), while an
    /// uncommitted task still has its key (re-scanned → processed exactly once,
    /// un-stranded).
    fn release_inflight<K: AsRef<[u8]>>(&self, keys: &[K]) {
        {
            let mut inflight = self.inflight.lock().unwrap();
            for k in keys {
                inflight.remove(k.as_ref());
            }
        }
        // The rows may still be durable and were skipped as in-flight by any
        // scan that ran meanwhile, which may have moved the floor past them,
        // so the next nudge must scan again from no later than their start
        // times.
        for k in keys {
            if let Some(parsed) = parse_task_key_ref(k.as_ref()) {
                self.note_task_written(parsed.start_time_ms as i64);
            }
        }
        self.dirty.store(true, Ordering::SeqCst);
    }

    /// Acknowledge durable dequeue outcomes.
    /// - `release_keys`: keys to release from in-flight tracking.
    /// - `tombstone_keys`: subset of keys that were durably deleted and should be
    ///   protected from stale-scan re-inserts.
    fn ack_durable(&self, release_keys: &[Bytes], tombstone_keys: &[Bytes]) {
        let mut inflight = self.inflight.lock().unwrap();
        let mut tombstones = self.ack_tombstones.lock().unwrap();
        let generation = self.scan_generation_started.load(Ordering::SeqCst);
        for k in release_keys {
            inflight.remove(k);
        }
        for k in tombstone_keys {
            tombstones.insert(
                k.clone(),
                AckTombstone {
                    inserted_at: generation,
                    last_seen: generation,
                },
            );
        }
    }

    /// Remove any buffered entries that match the provided keys.
    fn evict_keys<K: AsRef<[u8]>>(&self, keys: &[K]) {
        for k in keys {
            self.buffer.remove(k.as_ref());
        }
    }

    /// Wake the scanner to refill promptly. Called after a write that may
    /// have made a task ready (enqueue of a due task, lease release, restart,
    /// expedite, grant, limit-chain advance).
    fn wakeup(&self) {
        self.dirty.store(true, Ordering::SeqCst);
        self.scan_requested.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Wake the scanner from an empty poll, but only when a scan could find
    /// something: a write landed since the last scan began, the last scan
    /// stopped with ready rows remaining, or the earliest future-dated row
    /// it stopped at is now due. Returns whether the scanner was woken.
    fn nudge(&self, now_ms: i64) -> bool {
        if self.dirty.load(Ordering::SeqCst) || now_ms >= self.next_ready_at_ms() {
            self.wakeup();
            true
        } else {
            false
        }
    }

    /// Record that a durable task row with `start_time_ms` exists, without
    /// waking the scanner: the next scan reads from no later than that time.
    fn note_task_written(&self, start_time_ms: i64) {
        self.cursor.lock().unwrap().note_task_written(start_time_ms);
    }

    /// Record that a future-dated task row with `start_time_ms` was written,
    /// without waking the scanner. Lowers the floor and the recorded
    /// next-ready time so a nudge or the scanner's own timer fires when the
    /// row becomes due even if it sorts before the row the last scan stopped
    /// at.
    fn note_scheduled(&self, start_time_ms: i64) {
        let mut cursor = self.cursor.lock().unwrap();
        cursor.note_task_written(start_time_ms);
        cursor.at_ms = cursor.at_ms.min(start_time_ms);
    }

    /// Store why a scan stopped and the floor it established. A scan that
    /// overlapped a note may have missed the noted row, so it only lowers the
    /// recorded next-ready time. The floor is capped at the lowest time noted
    /// since the last scan that no note overlapped, because a noted row may
    /// not have been visible to this scan either (see
    /// [`ScanCursor::pending_floor_ms`]).
    fn record_scan_stop(
        &self,
        stop: ScanStop,
        skipped_tombstone: u64,
        notes_before: u64,
        floor_ms: Option<i64>,
    ) {
        let next_ready_at_ms = match stop {
            // A key suppressed by an ack tombstone is point-read once its
            // tombstone ages past the revive bound, which takes further scan
            // generations: keep nudges scanning until the key stops showing.
            _ if skipped_tombstone > 0 => 0,
            ScanStop::MoreReady => 0,
            ScanStop::EndOfRange => i64::MAX,
            ScanStop::Future { start_time_ms } => start_time_ms,
        };
        let mut cursor = self.cursor.lock().unwrap();
        cursor.floor_ms = match (floor_ms, cursor.pending_floor_ms) {
            (Some(floor_ms), Some(pending_ms)) => Some(floor_ms.min(pending_ms)),
            (floor_ms, None) => floor_ms,
            (None, Some(_)) => None,
        };
        if cursor.note_generation == notes_before {
            cursor.at_ms = next_ready_at_ms;
            // Every row noted before this scan began was either read by it
            // or is capped into the floor just set.
            cursor.pending_floor_ms = None;
        } else {
            cursor.at_ms = cursor.at_ms.min(next_ready_at_ms);
        }
    }

    /// The start time the next scan reads from, or `None` when it reads the
    /// whole range. See [`ScanCursor::floor_ms`].
    fn scan_floor_ms(&self) -> Option<i64> {
        self.cursor.lock().unwrap().floor_ms
    }

    /// The scanner's idle delay: the backoff, shortened to the time until the
    /// earliest future-dated row becomes due. A recorded time that is already
    /// due never shortens the backoff, which exists for exactly that case.
    fn delay_until_next_ready_ms(&self, backoff_ms: u64) -> u64 {
        let next_ready_at_ms = self.next_ready_at_ms();
        let now_ms = crate::job_store_shard::now_epoch_ms();
        if next_ready_at_ms == i64::MAX || next_ready_at_ms <= now_ms {
            return backoff_ms;
        }
        backoff_ms.min((next_ready_at_ms - now_ms) as u64)
    }

    /// The earliest start time at which a scan could find a task to buffer:
    /// `i64::MAX` once a scan has read the whole range, `0` when the last scan
    /// stopped with ready rows possibly remaining, otherwise the start time of
    /// the first future-dated row the last scan stopped at.
    fn next_ready_at_ms(&self) -> i64 {
        self.cursor.lock().unwrap().at_ms
    }
}

/// Where and when the next scan reads. A new cursor (`at_ms` of 0, no floor)
/// knows nothing about the range, so the first nudge scans all of it.
#[derive(Debug, Clone, Copy)]
struct ScanCursor {
    /// When a scan could next find a task to buffer. See
    /// [`TaskBroker::next_ready_at_ms`].
    at_ms: i64,
    /// Bumped by every note. A scan that overlapped a note keeps the lower of
    /// its own result and the noted time instead of overwriting.
    note_generation: u64,
    /// Start time below which no task row is waiting to be buffered: a scan
    /// dealt with every row there (buffered it, saw it in flight or under an
    /// ack tombstone, deleted it as defunct, or found it malformed) and every
    /// row written since was noted. Scans read from here, inclusive, so rows
    /// that share the floor's millisecond are read again. `None` reads the
    /// whole range.
    floor_ms: Option<i64>,
    /// Lowest start time noted since the last scan that no note overlapped
    /// ended. A row is noted before its write is issued and again once it is
    /// durable, and becomes visible to scans somewhere in between, so a scan
    /// that began after a note may still not have seen the row: the floor a
    /// scan establishes is capped at this time.
    pending_floor_ms: Option<i64>,
    /// When the floor was last cleared for a full-range scan.
    last_full_scan: Instant,
}

/// What a scan starts from, taken from the cursor as its iterator is created.
struct ScanStart {
    /// The start time to read from; `None` reads the whole range.
    floor_ms: Option<i64>,
    /// The note generation to compare against when the scan records its
    /// result.
    notes_before: u64,
    /// Whether this scan is a periodic full rescan.
    full_rescan: bool,
    /// The floor a full rescan cleared, if one was set: a row it buffers
    /// from below that time is one a floor-bounded scan would have missed.
    cleared_floor_ms: Option<i64>,
}

impl ScanCursor {
    fn new() -> Self {
        Self {
            at_ms: 0,
            note_generation: 0,
            floor_ms: None,
            pending_floor_ms: None,
            last_full_scan: Instant::now(),
        }
    }

    /// A task row with `start_time_ms` exists or is about to: the floor can
    /// be no higher than that.
    fn note_task_written(&mut self, start_time_ms: i64) {
        self.note_generation += 1;
        self.floor_ms = self.floor_ms.map(|floor_ms| floor_ms.min(start_time_ms));
        self.pending_floor_ms = Some(
            self.pending_floor_ms
                .map_or(start_time_ms, |pending_ms| pending_ms.min(start_time_ms)),
        );
    }
}

/// Why a scan stopped reading the task line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanStop {
    /// Every key in the range was read.
    EndOfRange,
    /// The first future-dated key, which sorts before every later one.
    Future { start_time_ms: i64 },
    /// The batch or buffer limit was hit, or the iterator failed, with ready
    /// rows possibly remaining.
    MoreReady,
}

impl Drop for TaskBroker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Registry of per-task-group brokers. Lazily creates a broker for each task group
/// on first access, scoping each broker's DB scan to its task group's key range.
pub struct TaskBrokerRegistry {
    db: Arc<InstrumentedDb>,
    shard_name: String,
    metrics: Option<Metrics>,
    range: ShardRange,
    tombstone_revive_after_generations: u64,
    full_rescan_interval: Duration,
    brokers: DashMap<String, Arc<TaskBroker>>,
}

impl TaskBrokerRegistry {
    pub fn new(
        db: Arc<InstrumentedDb>,
        shard_name: String,
        metrics: Option<Metrics>,
        range: ShardRange,
        tombstone_revive_after_generations: u64,
        full_rescan_interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            shard_name,
            metrics,
            range,
            tombstone_revive_after_generations,
            full_rescan_interval,
            brokers: DashMap::new(),
        })
    }

    /// Get or create a broker for the given task group.
    fn get_or_create(&self, task_group: &str) -> Arc<TaskBroker> {
        if let Some(broker) = self.brokers.get(task_group) {
            return Arc::clone(broker.value());
        }

        // Use entry API to avoid race between check and insert
        let broker = self
            .brokers
            .entry(task_group.to_string())
            .or_insert_with(|| {
                let b = TaskBroker::new(
                    task_group.to_string(),
                    Arc::clone(&self.db),
                    self.shard_name.clone(),
                    self.metrics.clone(),
                    self.range.clone(),
                    self.tombstone_revive_after_generations,
                    self.full_rescan_interval,
                );
                b.start();
                b
            });
        Arc::clone(broker.value())
    }

    /// Claim up to `max` ready tasks for a task group.
    pub fn claim_ready(&self, task_group: &str, max: usize) -> Vec<BrokerTask> {
        self.get_or_create(task_group).claim_ready(max)
    }

    /// Try to claim tasks for a task group. If none available, wait briefly for scanner to populate.
    pub async fn claim_ready_or_nudge(&self, task_group: &str, max: usize) -> Vec<BrokerTask> {
        self.get_or_create(task_group)
            .claim_ready_or_nudge(max)
            .await
    }

    /// Wake the scanner for a specific task group.
    /// Creates the broker if it doesn't exist, since a wakeup implies tasks were just written.
    pub fn wakeup(&self, task_group: &str) {
        self.get_or_create(task_group).wakeup();
    }

    /// Record a committed task row with `start_time_ms`: lower the broker's
    /// scan floor to it, then wake the scanner when the row is due now or
    /// lower the broker's recorded next-ready time so the row is scanned for
    /// once it becomes due.
    ///
    /// Call after the write is durable, from every path that writes a task
    /// row. A broker created later starts with nothing known about its range
    /// and scans all of it on its first nudge, so a future-dated row only
    /// needs noting on a broker that already exists.
    pub fn notify_task_written(&self, task_group: &str, start_time_ms: i64) {
        if start_time_ms <= crate::job_store_shard::now_epoch_ms() {
            let broker = self.get_or_create(task_group);
            broker.note_task_written(start_time_ms);
            broker.wakeup();
        } else if let Some(broker) = self.brokers.get(task_group) {
            broker.note_scheduled(start_time_ms);
        }
    }

    /// [`Self::notify_task_written`] for several task groups at once, each
    /// with the earliest start time written under it.
    pub fn notify_tasks_written(&self, written: &[(String, i64)]) {
        for (task_group, start_time_ms) in written {
            self.notify_task_written(task_group, *start_time_ms);
        }
    }

    /// Record that a task row with `start_time_ms` is about to be written
    /// under `task_group`: lower the broker's scan floor to it, so a scan that
    /// sees the row while its write awaits durability buffers it instead of
    /// starting above it.
    ///
    /// Call before the write is issued; [`Self::notify_task_written`] still
    /// follows once the write is durable. A write that fails leaves the floor
    /// lowered, which costs one scan a few extra reads. A broker that does
    /// not exist yet has no floor to lower.
    pub fn expect_task_written(&self, task_group: &str, start_time_ms: i64) {
        if let Some(broker) = self.brokers.get(task_group) {
            broker.note_task_written(start_time_ms);
        }
    }

    /// [`Self::expect_task_written`] for several task groups at once, each
    /// with the earliest start time about to be written under it.
    pub fn expect_tasks_written(&self, written: &[(String, i64)]) {
        for (task_group, start_time_ms) in written {
            self.expect_task_written(task_group, *start_time_ms);
        }
    }

    /// The scan floor of a task group's broker, if one exists: the start time
    /// its next scan reads from, or `None` for the whole range.
    pub fn group_scan_floor_ms(&self, task_group: &str) -> Option<Option<i64>> {
        self.brokers
            .get(task_group)
            .map(|e| e.value().scan_floor_ms())
    }

    /// The next-ready time recorded by a task group's broker, if one exists:
    /// `i64::MAX` when its last scan read the whole range, `0` when a scan
    /// may find ready rows, otherwise the start time of the earliest
    /// future-dated row.
    pub fn group_next_ready_at_ms(&self, task_group: &str) -> Option<i64> {
        self.brokers
            .get(task_group)
            .map(|e| e.value().next_ready_at_ms())
    }

    /// Requeue tasks back into the appropriate broker's buffer after a failed durable write.
    pub fn requeue(&self, tasks: Vec<BrokerTask>) {
        // Group tasks by task group to batch requeue into the right broker
        let mut by_group: HashMap<String, Vec<BrokerTask>> = HashMap::new();
        for task in tasks {
            let group = task.decoded.task_group().to_string();
            by_group.entry(group).or_default().push(task);
        }
        for (group, group_tasks) in by_group {
            self.get_or_create(&group).requeue(group_tasks);
        }
    }

    /// Acknowledge durable dequeue outcomes for a specific task group.
    pub fn ack_durable(&self, task_group: &str, release_keys: &[Bytes], tombstone_keys: &[Bytes]) {
        self.get_or_create(task_group)
            .ack_durable(release_keys, tombstone_keys);
    }

    /// Remove any buffered entries that match the provided keys.
    /// Keys may belong to different task groups; only evicts from brokers that already exist.
    pub fn evict_keys<K: AsRef<[u8]>>(&self, keys: &[K]) {
        for k in keys {
            if let Some(parsed) = parse_task_key_ref(k.as_ref())
                && let Some(broker) = self.brokers.get(parsed.task_group.as_ref())
            {
                broker.evict_keys(std::slice::from_ref(k));
            }
        }
    }

    /// Release claimed keys from in-flight tracking without re-buffering them.
    /// Keys may belong to different task groups; only touches brokers that
    /// already exist. See [`TaskBroker::release_inflight`].
    pub fn release_inflight<K: AsRef<[u8]>>(&self, keys: &[K]) {
        for k in keys {
            if let Some(parsed) = parse_task_key_ref(k.as_ref())
                && let Some(broker) = self.brokers.get(parsed.task_group.as_ref())
            {
                broker.release_inflight(std::slice::from_ref(k));
            }
        }
    }

    /// Get the total buffer length across all brokers.
    pub fn buffer_len(&self) -> usize {
        self.brokers.iter().map(|e| e.value().buffer_len()).sum()
    }

    /// Get the total inflight count across all brokers.
    pub fn inflight_len(&self) -> usize {
        self.brokers.iter().map(|e| e.value().inflight_len()).sum()
    }

    /// Get the buffer length for a specific task group.
    pub fn group_buffer_len(&self, task_group: &str) -> usize {
        self.brokers
            .get(task_group)
            .map(|e| e.value().buffer_len())
            .unwrap_or(0)
    }

    /// Get the inflight count for a specific task group.
    pub fn group_inflight_len(&self, task_group: &str) -> usize {
        self.brokers
            .get(task_group)
            .map(|e| e.value().inflight_len())
            .unwrap_or(0)
    }

    /// Stop all brokers.
    pub fn stop(&self) {
        for entry in self.brokers.iter() {
            entry.value().stop();
        }
    }

    /// Test-only: ensure a broker exists for `task_group` and directly inject
    /// `keys` into its in-flight set. Used to exercise drop-guard / release
    /// paths without standing up a scanner + DB write round-trip just to drive
    /// the same state.
    #[cfg(test)]
    pub(crate) fn seed_inflight_for_test(&self, task_group: &str, keys: Vec<Bytes>) {
        let broker = self.get_or_create(task_group);
        let mut inflight = broker.inflight.lock().unwrap();
        for k in keys {
            inflight.insert(k);
        }
    }
}

#[cfg(test)]
mod inflight_release_tests {
    use super::*;
    use crate::codec::encode_task;
    use crate::instrumented_db::InstrumentedDb;
    use crate::shard_range::ShardRange;
    use crate::task::Task;

    async fn empty_broker() -> Arc<TaskBroker> {
        broker_with_revive_bound(TaskBroker::ACK_TOMBSTONE_RETAIN_GENERATIONS).await
    }

    /// Every scan reads the whole range (a zero full-rescan interval): the
    /// tombstone tests leave a row durable under an acked key, which the scan
    /// floor would otherwise hide until the next full rescan.
    async fn broker_with_revive_bound(bound: u64) -> Arc<TaskBroker> {
        let store = Arc::new(slatedb::object_store::memory::InMemory::new());
        let db = slatedb::DbBuilder::new("test", store)
            .build()
            .await
            .expect("open in-memory db");
        let db = InstrumentedDb::new(Arc::new(db), tracing::Span::none());
        TaskBroker::new(
            "tg".to_string(),
            db,
            "shard".to_string(),
            None,
            ShardRange::full(),
            bound,
            Duration::ZERO,
        )
    }

    /// A tombstoned key that a scan keeps yielding is suppressed while its
    /// age (generations since insertion) is within the bound and point-read
    /// once past it; a row still present is revived and buffered.
    #[tokio::test]
    async fn tombstoned_key_is_revived_only_past_the_bound() {
        let broker = broker_with_revive_bound(1).await;
        let entry = sample_broker_task("revive-boundary");
        let row = encode_task(&entry.decoded.to_task().expect("task"));
        broker.db.put(&entry.key, &row).await.expect("put row");
        let now_ms = crate::job_store_shard::now_epoch_ms();

        let generation = broker.begin_scan_generation();
        assert_eq!(broker.scan_tasks(now_ms, generation).await, 1);
        assert_eq!(broker.claim_ready(1).len(), 1);
        broker.ack_durable(
            std::slice::from_ref(&entry.key),
            std::slice::from_ref(&entry.key),
        );

        // Age 1: within the bound, suppressed without a point read.
        let generation = broker.begin_scan_generation();
        assert_eq!(broker.scan_tasks(now_ms, generation).await, 0);
        assert_eq!(broker.buffer_len(), 0);
        assert!(
            broker
                .ack_tombstones
                .lock()
                .unwrap()
                .contains_key(&entry.key)
        );

        // Age 2: past the bound, point-read, present, revived.
        let generation = broker.begin_scan_generation();
        assert_eq!(broker.scan_tasks(now_ms, generation).await, 1);
        assert_eq!(broker.buffer_len(), 1);
        assert!(
            !broker
                .ack_tombstones
                .lock()
                .unwrap()
                .contains_key(&entry.key)
        );
    }

    fn sample_broker_task(job_id: &str) -> BrokerTask {
        let task = Task::RunAttempt {
            id: format!("task-{job_id}"),
            tenant: "-".to_string(),
            job_id: job_id.to_string(),
            attempt_number: 1,
            relative_attempt_number: 1,
            held_queues: vec![],
            task_group: "tg".to_string(),
        };
        let key = crate::keys::task_key("tg", 1, 0, job_id, 1, 1).into();
        let decoded = decode_task_validated(encode_task(&task)).expect("decode task");
        BrokerTask { key, decoded }
    }

    /// The drop-guard uses `release_inflight`: it must free the in-flight
    /// reservation WITHOUT re-buffering, so re-pickup is driven by the DB
    /// scanner (which won't re-add a key whose commit deleted it → no double
    /// dispatch).
    #[tokio::test]
    async fn release_inflight_frees_without_rebuffering() {
        let broker = empty_broker().await;
        let task = sample_broker_task("j1");
        let key = task.key.clone();
        broker.buffer.insert(key.clone(), task);

        // Claim moves it from the buffer into in-flight.
        let claimed = broker.claim_ready(10);
        assert_eq!(claimed.len(), 1);
        assert_eq!(broker.buffer_len(), 0);
        assert_eq!(broker.inflight_len(), 1);

        broker.release_inflight(&[key]);
        assert_eq!(broker.inflight_len(), 0, "inflight should be drained");
        assert_eq!(
            broker.buffer_len(),
            0,
            "release_inflight must not re-buffer"
        );
    }

    /// Contrast: the explicit error paths use `requeue`, which frees in-flight
    /// AND re-buffers for immediate re-pickup. Guards the distinction the fix
    /// relies on.
    #[tokio::test]
    async fn requeue_frees_and_rebuffers() {
        let broker = empty_broker().await;
        let task = sample_broker_task("j2");
        let key = task.key.clone();
        broker.buffer.insert(key, task);

        let claimed = broker.claim_ready(10);
        assert_eq!(claimed.len(), 1);
        assert_eq!(broker.inflight_len(), 1);

        broker.requeue(claimed);
        assert_eq!(broker.inflight_len(), 0);
        assert_eq!(
            broker.buffer_len(),
            1,
            "requeue should re-buffer for immediate re-pickup"
        );
    }
}

#[cfg(test)]
mod scan_gating_tests {
    use super::*;
    use crate::codec::encode_task;
    use crate::instrumented_db::InstrumentedDb;
    use crate::job_store_shard::now_epoch_ms;
    use crate::shard_range::ShardRange;
    use crate::task::Task;

    async fn broker_with_scan_batch(scan_batch: usize) -> Arc<TaskBroker> {
        let store = Arc::new(slatedb::object_store::memory::InMemory::new());
        let db = slatedb::DbBuilder::new("test", store)
            .build()
            .await
            .expect("open in-memory db");
        let db = InstrumentedDb::new(Arc::new(db), tracing::Span::none());
        TaskBroker::new_with_limits(
            "tg".to_string(),
            db,
            "shard".to_string(),
            None,
            ShardRange::full(),
            TaskBroker::ACK_TOMBSTONE_RETAIN_GENERATIONS,
            Duration::from_secs(60),
            8192,
            scan_batch,
        )
    }

    async fn empty_broker() -> Arc<TaskBroker> {
        broker_with_scan_batch(4096).await
    }

    async fn put_run_attempt(broker: &TaskBroker, job_id: &str, start_time_ms: i64) -> Vec<u8> {
        let task = Task::RunAttempt {
            id: format!("task-{job_id}"),
            tenant: "-".to_string(),
            job_id: job_id.to_string(),
            attempt_number: 1,
            relative_attempt_number: 1,
            held_queues: vec![],
            task_group: "tg".to_string(),
        };
        let key = crate::keys::task_key("tg", start_time_ms, 0, job_id, 1, start_time_ms);
        broker
            .db
            .put(&key, &encode_task(&task))
            .await
            .expect("put row");
        key
    }

    async fn scan(broker: &TaskBroker, now_ms: i64) -> usize {
        let generation = broker.begin_scan_generation();
        let inserted = broker.scan_tasks(now_ms, generation).await;
        broker.complete_scan_generation(generation);
        inserted
    }

    /// A broker that knows nothing about its range scans on its first nudge;
    /// once a scan has read the whole range, nudges are skipped until a write
    /// marks the broker dirty.
    #[tokio::test]
    async fn nudges_are_skipped_once_the_range_is_known_empty() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert!(broker.nudge(now_ms), "first nudge must scan");
        assert!(broker.scan_requested.swap(false, Ordering::SeqCst));

        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.next_ready_at_ms(), i64::MAX);
        for _ in 0..100 {
            assert!(!broker.nudge(now_ms));
        }
        assert!(!broker.scan_requested.load(Ordering::SeqCst));

        broker.wakeup();
        assert!(broker.nudge(now_ms), "a write re-arms the nudge");
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert!(!broker.nudge(now_ms), "the scan clears dirty");
    }

    /// A scan that stops at a future-dated row records its start time, and a
    /// nudge scans again only once that time has arrived.
    #[tokio::test]
    async fn scan_stopping_at_a_future_row_records_when_it_is_due() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        let due_ms = now_ms + 5_000;
        put_run_attempt(&broker, "future", due_ms).await;

        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.next_ready_at_ms(), due_ms);
        assert!(!broker.nudge(now_ms));
        assert!(!broker.nudge(due_ms - 1));
        assert!(broker.nudge(due_ms));

        assert_eq!(scan(&broker, due_ms).await, 1);
        assert_eq!(broker.next_ready_at_ms(), i64::MAX);
        assert_eq!(broker.claim_ready(10).len(), 1);
    }

    /// Noting a scheduled row lowers the recorded next-ready time and never
    /// raises it.
    #[tokio::test]
    async fn note_scheduled_only_lowers_the_recorded_time() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.next_ready_at_ms(), i64::MAX);

        broker.note_scheduled(now_ms + 1_000);
        assert_eq!(broker.next_ready_at_ms(), now_ms + 1_000);
        broker.note_scheduled(now_ms + 2_000);
        assert_eq!(broker.next_ready_at_ms(), now_ms + 1_000);
        broker.note_scheduled(now_ms + 500);
        assert_eq!(broker.next_ready_at_ms(), now_ms + 500);
        assert!(!broker.nudge(now_ms + 499));
        assert!(broker.nudge(now_ms + 500));
    }

    /// A scan that overlaps a note keeps the noted time rather than replacing
    /// it with its own result, since its view may predate the noted row.
    #[tokio::test]
    async fn note_during_a_scan_survives_the_scan_result() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.next_ready_at_ms(), i64::MAX);

        let notes_before = broker.cursor.lock().unwrap().note_generation;

        broker.note_scheduled(now_ms + 700);
        broker.record_scan_stop(ScanStop::EndOfRange, 0, notes_before, None);
        assert_eq!(broker.next_ready_at_ms(), now_ms + 700);

        let notes_before = broker.cursor.lock().unwrap().note_generation;
        broker.record_scan_stop(ScanStop::EndOfRange, 0, notes_before, None);
        assert_eq!(broker.next_ready_at_ms(), i64::MAX);
    }

    /// A scan that hits its batch limit with ready rows remaining leaves the
    /// gate open so the next nudge scans again.
    #[tokio::test]
    async fn scan_stopping_at_the_batch_limit_leaves_nudges_armed() {
        let broker = broker_with_scan_batch(2).await;
        let now_ms = now_epoch_ms();
        for job in ["a", "b", "c"] {
            put_run_attempt(&broker, job, now_ms - 10).await;
        }

        assert_eq!(scan(&broker, now_ms).await, 2);
        assert_eq!(broker.next_ready_at_ms(), 0);
        assert!(broker.nudge(now_ms));

        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.next_ready_at_ms(), i64::MAX);
        assert_eq!(broker.claim_ready(10).len(), 3);
    }

    /// Releasing in-flight keys without re-buffering leaves rows that may
    /// still be durable, so the next nudge must scan.
    #[tokio::test]
    async fn release_inflight_re_arms_the_nudge() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        let key = put_run_attempt(&broker, "stranded", now_ms - 10).await;
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.claim_ready(1).len(), 1);
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert!(!broker.nudge(now_ms));

        broker.release_inflight(std::slice::from_ref(&key));
        assert!(broker.nudge(now_ms));
        assert_eq!(scan(&broker, now_ms).await, 1);
    }

    /// An empty poll on a broker with nothing to find waits out its window
    /// without ever asking the scanner to run.
    #[tokio::test]
    async fn empty_poll_does_not_request_a_scan_when_nothing_can_be_ready() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(scan(&broker, now_ms).await, 0);

        for _ in 0..4 {
            assert!(broker.claim_ready_or_nudge(10).await.is_empty());
        }
        assert!(!broker.scan_requested.load(Ordering::SeqCst));
        assert!(!broker.dirty.load(Ordering::SeqCst));
    }

    /// A write that lands while an empty poll is waiting wakes the scanner
    /// from inside the wait, so that poll's window is not wasted.
    #[tokio::test]
    async fn write_during_an_empty_poll_requests_a_scan() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(scan(&broker, now_ms).await, 0);

        let waiter = {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move { broker.claim_ready_or_nudge(10).await })
        };
        tokio::time::sleep(Duration::from_millis(8)).await;
        assert!(!broker.scan_requested.load(Ordering::SeqCst));
        broker.wakeup();
        assert!(waiter.await.expect("poll task").is_empty());
        assert!(broker.scan_requested.load(Ordering::SeqCst));
    }

    /// The scanner's idle delay shortens to the time until the earliest
    /// future-dated row is due, and is otherwise the backoff.
    #[tokio::test]
    async fn idle_delay_wakes_for_the_next_due_row() {
        let broker = empty_broker().await;
        assert_eq!(broker.delay_until_next_ready_ms(2_000), 2_000);

        let now_ms = now_epoch_ms();
        put_run_attempt(&broker, "soon", now_ms + 60_000).await;
        assert_eq!(scan(&broker, now_ms).await, 0);
        let delay = broker.delay_until_next_ready_ms(2_000);
        assert!(delay <= 2_000, "delay {delay} never exceeds the backoff");

        broker.note_scheduled(now_ms + 300);
        let delay = broker.delay_until_next_ready_ms(2_000);
        assert!(delay <= 300, "delay {delay} shortens to the due row");

        broker.note_scheduled(now_ms - 1);
        assert_eq!(
            broker.delay_until_next_ready_ms(2_000),
            2_000,
            "an already-due time keeps the backoff"
        );
    }

    async fn broker_with_full_rescan_interval(interval: Duration) -> Arc<TaskBroker> {
        let store = Arc::new(slatedb::object_store::memory::InMemory::new());
        let db = slatedb::DbBuilder::new("test", store)
            .build()
            .await
            .expect("open in-memory db");
        let db = InstrumentedDb::new(Arc::new(db), tracing::Span::none());
        TaskBroker::new(
            "tg".to_string(),
            db,
            "shard".to_string(),
            None,
            ShardRange::full(),
            TaskBroker::ACK_TOMBSTONE_RETAIN_GENERATIONS,
            interval,
        )
    }

    /// A scan that reads to the end of the range moves the floor to its
    /// `now_ms`; a later scan starts there, so a row written below the floor
    /// behind the broker's back is not read until the row is noted.
    #[tokio::test]
    async fn scan_starts_at_the_floor_and_a_note_lowers_it() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(
            broker.scan_floor_ms(),
            None,
            "a new broker reads the whole range"
        );

        put_run_attempt(&broker, "head", now_ms - 10).await;
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));

        put_run_attempt(&broker, "below", now_ms - 20).await;
        assert_eq!(
            scan(&broker, now_ms).await,
            0,
            "a row below the floor is not read"
        );
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));

        broker.note_task_written(now_ms - 20);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 20));
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(
            broker.scan_floor_ms(),
            Some(now_ms - 20),
            "the note caps the floor of the scan that follows it"
        );
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));
        assert_eq!(broker.claim_ready(10).len(), 2);
    }

    /// A note never raises the floor.
    #[tokio::test]
    async fn note_only_lowers_the_floor() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));

        broker.note_task_written(now_ms + 1_000);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));
        broker.note_task_written(now_ms - 1_000);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 1_000));
        broker.note_scheduled(now_ms - 2_000);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 2_000));
    }

    /// A scan that stops at its batch limit moves the floor only through the
    /// rows it read; the next scan continues from there.
    #[tokio::test]
    async fn scan_stopping_at_the_batch_limit_moves_the_floor_through_the_rows_read() {
        let broker = broker_with_scan_batch(2).await;
        let now_ms = now_epoch_ms();
        put_run_attempt(&broker, "a", now_ms - 30).await;
        put_run_attempt(&broker, "b", now_ms - 20).await;
        put_run_attempt(&broker, "c", now_ms - 10).await;

        assert_eq!(scan(&broker, now_ms).await, 2);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 20));

        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));
        assert_eq!(broker.claim_ready(10).len(), 3);
    }

    /// The floor is inclusive: rows that share the floor's millisecond but
    /// were not read by the scan that set it are read by the next one.
    #[tokio::test]
    async fn rows_sharing_the_floor_millisecond_are_read_again() {
        let broker = broker_with_scan_batch(1).await;
        let now_ms = now_epoch_ms();
        put_run_attempt(&broker, "a", now_ms - 10).await;
        put_run_attempt(&broker, "b", now_ms - 10).await;

        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 10));
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.claim_ready(10).len(), 2);
    }

    /// A scan that stops at a future-dated row moves the floor to its
    /// `now_ms`, below that row, so the row is read once it is due.
    #[tokio::test]
    async fn scan_stopping_at_a_future_row_moves_the_floor_to_now() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        let due_ms = now_ms + 5_000;
        put_run_attempt(&broker, "future", due_ms).await;

        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));
        assert_eq!(scan(&broker, due_ms).await, 1);
        assert_eq!(broker.scan_floor_ms(), Some(due_ms));
    }

    /// A scan that overlaps a note caps its floor at the noted time, since
    /// its view may predate the noted row. The cap persists through the next
    /// scan too (the row may still not have been visible to it) and is
    /// dropped by a scan that no note overlapped.
    #[tokio::test]
    async fn note_during_a_scan_caps_the_floor() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        assert_eq!(scan(&broker, now_ms).await, 0);

        let notes_before = broker.begin_scan_cursor().notes_before;
        broker.note_task_written(now_ms - 500);
        broker.record_scan_stop(ScanStop::EndOfRange, 0, notes_before, Some(now_ms));
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 500));

        let notes_before = broker.begin_scan_cursor().notes_before;
        broker.record_scan_stop(ScanStop::EndOfRange, 0, notes_before, Some(now_ms));
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 500));

        let notes_before = broker.begin_scan_cursor().notes_before;
        broker.record_scan_stop(ScanStop::EndOfRange, 0, notes_before, Some(now_ms));
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));
    }

    /// A row noted before a broker's first scan (its write may still be in
    /// flight) caps that scan's floor, so the row is read once it lands.
    #[tokio::test]
    async fn note_before_the_first_scan_caps_its_floor() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        broker.note_task_written(now_ms - 500);
        assert_eq!(broker.scan_floor_ms(), None);

        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 500));

        put_run_attempt(&broker, "landed", now_ms - 500).await;
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));
    }

    /// Releasing in-flight keys lowers the floor to their start times, so a
    /// scan that moved past them while they were in flight is not final.
    #[tokio::test]
    async fn release_inflight_lowers_the_floor() {
        let broker = empty_broker().await;
        let now_ms = now_epoch_ms();
        let key = put_run_attempt(&broker, "stranded", now_ms - 10).await;
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.claim_ready(1).len(), 1);
        assert_eq!(scan(&broker, now_ms).await, 0);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));

        broker.release_inflight(std::slice::from_ref(&key));
        assert_eq!(broker.scan_floor_ms(), Some(now_ms - 10));
        assert_eq!(scan(&broker, now_ms).await, 1);
    }

    /// Once the full-rescan interval has passed, a scan ignores the floor and
    /// reads the whole range, finding a row written below the floor behind
    /// the broker's back.
    #[tokio::test]
    async fn full_rescan_clears_the_floor_on_schedule() {
        let broker = broker_with_full_rescan_interval(Duration::from_millis(50)).await;
        let now_ms = now_epoch_ms();
        put_run_attempt(&broker, "head", now_ms - 10).await;
        assert_eq!(scan(&broker, now_ms).await, 1);
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));

        put_run_attempt(&broker, "below", now_ms - 20).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            scan(&broker, now_ms).await,
            1,
            "the full rescan reads below the floor"
        );
        assert_eq!(broker.scan_floor_ms(), Some(now_ms));

        put_run_attempt(&broker, "below-again", now_ms - 30).await;
        assert_eq!(
            scan(&broker, now_ms).await,
            0,
            "within the interval the floor is honoured"
        );
    }
}
