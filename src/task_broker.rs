use std::collections::{HashMap, HashSet};
use std::str;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use slatedb::{Db, WriteBatch};
use tokio::sync::Notify;

use crate::codec::{DecodedTask, decode_task_validated};
use crate::keys::{end_bound, parse_task_key, task_group_prefix};
use crate::metrics::Metrics;
use crate::shard_range::ShardRange;
use tracing::debug;

/// A task entry stored in the in-memory broker buffer.
///
/// Stores a pre-validated `DecodedTask` (wraps `Bytes`) instead of raw bytes.
/// Validation happens once during scan; callers can access zero-copy
/// FlatBuffer accessors without re-parsing.
#[derive(Debug, Clone)]
pub struct BrokerTask {
    pub key: Vec<u8>,
    pub decoded: DecodedTask,
}

/// A single per-task-group broker that scans only its own key range.
///
/// - Maintains a sorted buffer of ready tasks using a skiplist keyed by the task key bytes.
/// - Populates from SlateDB in the background with exponential backoff when no work is found.
/// - Ensures tasks claimed but not yet durably leased are tracked as in-flight and not reinserted.
pub struct TaskBroker {
    // The task group this broker is responsible for
    task_group: String,
    // The SlateDB database to read tasks from
    db: Arc<Db>,
    // The buffer of tasks read out of the DB and ready to be claimed by a dequeue-ing worker
    buffer: Arc<SkipMap<Vec<u8>, BrokerTask>>,
    // The set of tasks already read out of the DB and claimed by a worker but not yet durably leased. Required so that the scanner doesn't re-add tasks that are in the middle of being dequeued to the buffer.
    inflight: Arc<Mutex<HashSet<Vec<u8>>>>,
    // Tombstones for task keys that were durably acked, keyed by the most recent
    // scan generation where that key should be suppressed.
    ack_tombstones: Arc<Mutex<HashMap<Vec<u8>, u64>>>,
    // Monotonic generation numbers for scanner iterations.
    scan_generation_started: Arc<AtomicU64>,
    scan_generation_completed: Arc<AtomicU64>,
    // Whether the background scanner is running
    running: Arc<AtomicBool>,
    // A notify object to wake up the background scanner when a task is claimed
    notify: Arc<Notify>,
    // Whether the background scanner should be woken up
    scan_requested: Arc<AtomicBool>,
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
    // refresh the tombstone generation whenever a stale key is observed again.
    const ACK_TOMBSTONE_RETAIN_GENERATIONS: u64 = 64;

    fn new(
        task_group: String,
        db: Arc<Db>,
        shard_name: String,
        metrics: Option<Metrics>,
        range: ShardRange,
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
            target_buffer: 8192,
            // Must be >= target_buffer / 2 (the low watermark) so that a
            // single scan pass can refill from low-watermark to target.
            // With a smaller batch the scanner needs multiple passes and
            // each pass re-reads every already-buffered entry from the
            // start of the key range, wasting O(buffer_size) reads/pass.
            scan_batch: 4096,
            shard_name,
            metrics,
            range,
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
        tombstones.retain(|_, last_seen_generation| {
            generation.saturating_sub(*last_seen_generation)
                <= Self::ACK_TOMBSTONE_RETAIN_GENERATIONS
        });
    }

    /// Scan tasks from DB and insert into buffer, skipping future tasks and inflight ones.
    async fn scan_tasks(&self, now_ms: i64, generation: u64) -> usize {
        // [SILO-SCAN-1] Scan only this task group's key range
        let start = task_group_prefix(&self.task_group);
        let end = end_bound(&start);

        let Ok(mut iter) = self.db.scan::<Vec<u8>, _>(start..end).await else {
            return 0;
        };

        // Collect keys to delete for defunct tasks (outside shard range)
        let mut defunct_keys: Vec<Vec<u8>> = Vec::new();

        let mut inserted = 0;
        let mut total_read = 0u64;
        let mut skipped_future = 0u64;
        let mut skipped_inflight = 0u64;
        let mut skipped_tombstone = 0u64;
        let mut skipped_already_buffered = 0u64;
        let mut skipped_defunct = 0u64;
        while inserted < self.scan_batch && self.buffer.len() < self.target_buffer {
            let Ok(Some(kv)) = iter.next().await else {
                break;
            };
            total_read += 1;

            // Parse the task key to extract timestamp
            let Some(parsed_key) = parse_task_key(&kv.key) else {
                continue;
            };

            // Filter out future tasks
            if parsed_key.start_time_ms > now_ms as u64 {
                skipped_future += 1;
                continue;
            }

            let key_bytes = kv.key.to_vec();

            // [SILO-SCAN-3] Skip inflight tasks
            if self.inflight.lock().unwrap().contains(&key_bytes) {
                skipped_inflight += 1;
                continue;
            }

            // Skip keys that were recently durably acked to avoid stale scan re-inserts.
            // Refresh generation so persistent stale observations extend suppression.
            let suppress_due_to_tombstone = {
                let mut tombstones = self.ack_tombstones.lock().unwrap();
                if let Some(last_seen_generation) = tombstones.get_mut(&key_bytes) {
                    *last_seen_generation = generation;
                    true
                } else {
                    false
                }
            };
            if suppress_due_to_tombstone {
                skipped_tombstone += 1;
                continue;
            }

            let decoded = match decode_task_validated(kv.value.clone()) {
                Ok(t) => t,
                Err(_) => continue, // Skip malformed tasks
            };

            // Check if task's tenant is within shard range
            let task_tenant = decoded.tenant();

            if !self.range.contains_tenant(task_tenant) {
                // Task is for a tenant outside our range - mark for deletion
                defunct_keys.push(kv.key.to_vec());
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

            let entry = BrokerTask {
                key: key_bytes.clone(),
                decoded,
            };

            // [SILO-SCAN-2] Insert into buffer if not already present
            if self.buffer.get(&key_bytes).is_none() {
                self.buffer.insert(key_bytes, entry);
                inserted += 1;

                // Yield periodically to avoid starving other tasks
                if inserted % 16 == 0 {
                    tokio::task::yield_now().await;
                }
            } else {
                skipped_already_buffered += 1;
            }
        }

        if total_read > 0 {
            if let Some(ref m) = self.metrics {
                m.record_broker_scan_tasks(
                    &self.shard_name,
                    &self.task_group,
                    inserted as u64,
                    skipped_future,
                    skipped_inflight,
                    skipped_tombstone,
                    skipped_already_buffered,
                    skipped_defunct,
                );
                m.set_broker_tombstone_count(
                    &self.shard_name,
                    &self.task_group,
                    self.ack_tombstones.lock().unwrap().len() as u64,
                );
            }
        }

        // Delete defunct tasks from the database
        if !defunct_keys.is_empty() {
            let mut batch = WriteBatch::new();
            for key in &defunct_keys {
                batch.delete(key);
            }
            if let Err(e) = self.db.write(batch).await {
                debug!(error = %e, count = defunct_keys.len(), "failed to delete defunct tasks");
            } else {
                debug!(
                    count = defunct_keys.len(),
                    "deleted defunct tasks outside shard range"
                );
            }
        }

        inserted
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

                // Sleep with early wakeup support
                let delay = tokio::time::sleep(Duration::from_millis(sleep_ms));
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
        let candidate_keys: Vec<Vec<u8>> = {
            let mut inflight = self.inflight.lock().unwrap();
            let mut keys = Vec::with_capacity(max);
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

    /// Try to claim tasks. If none available, wait briefly for scanner to populate.
    async fn claim_ready_or_nudge(&self, max: usize) -> Vec<BrokerTask> {
        // Try fast path first
        let claimed = self.claim_ready(max);
        if !claimed.is_empty() {
            return claimed;
        }

        // Wake scanner and wait briefly
        self.wakeup();
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(5)).await;
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

    /// Acknowledge durable dequeue outcomes.
    /// - `release_keys`: keys to release from in-flight tracking.
    /// - `tombstone_keys`: subset of keys that were durably deleted and should be
    ///   protected from stale-scan re-inserts.
    fn ack_durable(&self, release_keys: &[Vec<u8>], tombstone_keys: &[Vec<u8>]) {
        let mut inflight = self.inflight.lock().unwrap();
        let mut tombstones = self.ack_tombstones.lock().unwrap();
        let generation = self.scan_generation_started.load(Ordering::SeqCst);
        for k in release_keys {
            inflight.remove(k);
        }
        for k in tombstone_keys {
            tombstones.insert(k.clone(), generation);
        }
    }

    /// Remove any buffered entries that match the provided keys.
    fn evict_keys(&self, keys: &[Vec<u8>]) {
        for k in keys {
            self.buffer.remove(k);
        }
    }

    /// Wake the scanner to refill promptly.
    fn wakeup(&self) {
        self.scan_requested.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }
}

impl Drop for TaskBroker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Registry of per-task-group brokers. Lazily creates a broker for each task group
/// on first access, scoping each broker's DB scan to its task group's key range.
pub struct TaskBrokerRegistry {
    db: Arc<Db>,
    shard_name: String,
    metrics: Option<Metrics>,
    range: ShardRange,
    brokers: DashMap<String, Arc<TaskBroker>>,
}

impl TaskBrokerRegistry {
    pub fn new(
        db: Arc<Db>,
        shard_name: String,
        metrics: Option<Metrics>,
        range: ShardRange,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            shard_name,
            metrics,
            range,
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

    /// Wake specific task group brokers that already exist.
    /// Does not create brokers — only wakes groups that have an active scanner.
    pub fn wakeup_groups(&self, task_groups: &[String]) {
        for group in task_groups {
            if let Some(broker) = self.brokers.get(group.as_str()) {
                broker.wakeup();
            }
        }
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
    pub fn ack_durable(
        &self,
        task_group: &str,
        release_keys: &[Vec<u8>],
        tombstone_keys: &[Vec<u8>],
    ) {
        self.get_or_create(task_group)
            .ack_durable(release_keys, tombstone_keys);
    }

    /// Remove any buffered entries that match the provided keys.
    /// Keys may belong to different task groups; only evicts from brokers that already exist.
    pub fn evict_keys(&self, keys: &[Vec<u8>]) {
        for k in keys {
            if let Some(parsed) = parse_task_key(k)
                && let Some(broker) = self.brokers.get(&parsed.task_group)
            {
                broker.evict_keys(std::slice::from_ref(k));
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
}
