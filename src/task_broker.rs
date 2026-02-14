use std::collections::{HashMap, HashSet};
use std::str;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use slatedb::{Db, WriteBatch};
use tokio::sync::Notify;

use crate::codec::decode_task_validated;
use crate::keys::{end_bound, parse_task_key, tasks_prefix};
use crate::metrics::Metrics;
use crate::shard_range::ShardRange;
use tracing::debug;

/// A task entry stored in the in-memory broker buffer.
///
/// Stores raw FlatBuffer bytes instead of an owned Task struct.
/// `Bytes::clone()` is an Arc increment (zero-copy), avoiding
/// per-field string allocations on claim.
#[derive(Debug, Clone)]
pub struct BrokerTask {
    pub key: Vec<u8>,
    pub task_bytes: Bytes,
}

/// Lock-free in-memory task broker backed by SlateDB.
///
/// - Maintains a sorted buffer of ready tasks using a skiplist keyed by the task key bytes.
/// - Populates from SlateDB in the background with exponential backoff when no work is found.
/// - Ensures tasks claimed but not yet durably leased are tracked as in-flight and not reinserted.
pub struct TaskBroker {
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

    pub fn new(
        db: Arc<Db>,
        shard_name: String,
        metrics: Option<Metrics>,
        range: ShardRange,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            buffer: Arc::new(SkipMap::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            ack_tombstones: Arc::new(Mutex::new(HashMap::new())),
            scan_generation_started: Arc::new(AtomicU64::new(0)),
            scan_generation_completed: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            scan_requested: Arc::new(AtomicBool::new(false)),
            target_buffer: 4096,
            scan_batch: 1024,
            shard_name,
            metrics,
            range,
        })
    }

    /// Get the shard's tenant range.
    pub fn get_range(&self) -> ShardRange {
        self.range.clone()
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
        // [SILO-SCAN-1] Tasks use binary storekey encoding with prefix byte
        let start = tasks_prefix();
        let end = end_bound(&start);

        let Ok(mut iter) = self.db.scan::<Vec<u8>, _>(start..end).await else {
            return 0;
        };

        // Collect keys to delete for defunct tasks (outside shard range)
        let mut defunct_keys: Vec<Vec<u8>> = Vec::new();

        let mut inserted = 0;
        while inserted < self.scan_batch && self.buffer.len() < self.target_buffer {
            let Ok(Some(kv)) = iter.next().await else {
                break;
            };

            // Parse the task key to extract timestamp
            let Some(parsed_key) = parse_task_key(&kv.key) else {
                continue;
            };

            // Filter out future tasks
            if parsed_key.start_time_ms > now_ms as u64 {
                continue;
            }

            let key_bytes = kv.key.to_vec();

            // [SILO-SCAN-3] Skip inflight tasks
            if self.inflight.lock().unwrap().contains(&key_bytes) {
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
                continue;
            }

            let value_bytes: Bytes = kv.value.clone();
            let decoded = match decode_task_validated(value_bytes.clone()) {
                Ok(t) => t,
                Err(_) => continue, // Skip malformed tasks
            };

            // Check if task's tenant is within shard range
            let task_tenant = decoded.tenant();

            if !self.range.contains(task_tenant) {
                // Task is for a tenant outside our range - mark for deletion
                defunct_keys.push(kv.key.to_vec());
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
                task_bytes: value_bytes,
            };

            // [SILO-SCAN-2] Insert into buffer if not already present
            if self.buffer.get(&key_bytes).is_none() {
                self.buffer.insert(key_bytes, entry);
                inserted += 1;

                // Yield periodically to avoid starving other tasks
                if inserted % 16 == 0 {
                    tokio::task::yield_now().await;
                }
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
    pub fn start(self: &Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }

        let broker = Arc::clone(self);
        tokio::spawn(async move {
            // Note: Concurrency holders are hydrated synchronously in
            // JobStoreShard::open_with_resolved_store() before this broker starts.
            // This ensures concurrent limits are enforced correctly from the first request.

            let min_sleep_ms = 5;
            let max_sleep_ms = 1000;
            let mut sleep_ms = min_sleep_ms;

            loop {
                if !broker.running.load(Ordering::SeqCst) {
                    break;
                }

                // Wait if buffer is full
                if broker.buffer.len() >= broker.target_buffer {
                    tokio::time::sleep(Duration::from_millis(50)).await;
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
                }

                // Adjust backoff: stay aggressive when buffer needs filling
                if broker.buffer.len() < broker.target_buffer / 2 {
                    sleep_ms = min_sleep_ms;
                } else if inserted == 0 {
                    sleep_ms = (sleep_ms * 2).min(max_sleep_ms);
                } else {
                    sleep_ms = min_sleep_ms;
                }

                // Handle explicit scan requests with minimal sleep
                if broker.scan_requested.swap(false, Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }

                // Sleep with early wakeup support
                let delay = tokio::time::sleep(Duration::from_millis(sleep_ms));
                tokio::pin!(delay);
                tokio::select! {
                    _ = &mut delay => {},
                    _ = broker.notify.notified() => {
                        debug!("broker woken by notification");
                    }
                }
            }
        });
    }

    /// Stop the background loop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Claim up to `max` ready tasks from the head of the buffer for a specific task_group.
    pub fn claim_ready(&self, task_group: &str, max: usize) -> Vec<BrokerTask> {
        use crate::keys::task_group_prefix;
        let prefix = task_group_prefix(task_group);

        // Collect candidate keys in a single scan while holding the inflight lock once.
        // This avoids O(N*M) repeated full scans from the beginning and reduces mutex acquisitions.
        let candidate_keys: Vec<Vec<u8>> = {
            let mut inflight = self.inflight.lock().unwrap();
            let mut keys = Vec::with_capacity(max);
            for entry in self.buffer.iter() {
                if keys.len() >= max {
                    break;
                }
                let key = entry.key();
                if !key.starts_with(&prefix) {
                    continue;
                }
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

    /// Try to claim tasks for a task_group. If none available, wait briefly for scanner to populate.
    pub async fn claim_ready_or_nudge(&self, task_group: &str, max: usize) -> Vec<BrokerTask> {
        // Try fast path first
        let claimed = self.claim_ready(task_group, max);
        if !claimed.is_empty() {
            return claimed;
        }

        // Wake scanner and wait briefly
        self.wakeup();
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let claimed = self.claim_ready(task_group, max);
            if !claimed.is_empty() {
                return claimed;
            }
        }

        Vec::new()
    }

    /// Requeue tasks back into the buffer after a failed durable write.
    pub fn requeue(&self, tasks: Vec<BrokerTask>) {
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
    pub fn ack_durable(&self, release_keys: &[Vec<u8>], tombstone_keys: &[Vec<u8>]) {
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
    pub fn evict_keys(&self, keys: &[Vec<u8>]) {
        for k in keys {
            self.buffer.remove(k);
        }
    }

    /// Wake the scanner to refill promptly.
    pub fn wakeup(&self) {
        self.scan_requested.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }
}

impl Drop for TaskBroker {
    fn drop(&mut self) {
        self.stop();
    }
}
