use std::collections::HashSet;
use std::str;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use crossbeam_skiplist::SkipMap;
use slatedb::Db;
use tokio::sync::Notify;

use crate::codec::decode_task;
use crate::concurrency::ConcurrencyManager;
use crate::job_store_shard::Task;
use tracing::debug;

/// A task entry stored in the in-memory broker buffer
#[derive(Debug, Clone)]
pub struct BrokerTask {
    pub key: String,
    pub task: Task,
}

/// Lock-free in-memory task broker backed by SlateDB.
///
/// - Maintains a sorted buffer of ready tasks using a skiplist keyed by the task key string.
/// - Populates from SlateDB in the background with exponential backoff when no work is found.
/// - Ensures tasks claimed but not yet durably leased are tracked as in-flight and not reinserted.
pub struct TaskBroker {
    db: Arc<Db>,
    buffer: Arc<SkipMap<String, BrokerTask>>,
    inflight: Arc<Mutex<HashSet<String>>>,
    running: Arc<AtomicBool>,
    notify: Arc<Notify>,
    scan_requested: Arc<AtomicBool>,
    concurrency: Arc<ConcurrencyManager>,
    target_buffer: usize,
    scan_batch: usize,
}

impl TaskBroker {
    pub fn new(db: Arc<Db>, concurrency: Arc<ConcurrencyManager>) -> Arc<Self> {
        Arc::new(Self {
            db,
            buffer: Arc::new(SkipMap::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            running: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            scan_requested: Arc::new(AtomicBool::new(false)),
            concurrency,
            target_buffer: 4096,
            scan_batch: 1024,
        })
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn inflight_len(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    /// Scan tasks from DB and insert into buffer, skipping future tasks and inflight ones.
    async fn scan_tasks(&self, now_ms: i64) -> usize {
        // [SILO-SCAN-1] Tasks live under tasks/<ts>/<pri>/<job_id>/<attempt>
        let start: Vec<u8> = b"tasks/".to_vec();
        let mut end: Vec<u8> = b"tasks/".to_vec();
        end.push(0xFF);

        let Ok(mut iter) = self.db.scan::<Vec<u8>, _>(start..=end).await else {
            return 0;
        };

        let mut inserted = 0;
        while inserted < self.scan_batch && self.buffer.len() < self.target_buffer {
            let Ok(Some(kv)) = iter.next().await else {
                break;
            };

            let Ok(key_str) = str::from_utf8(&kv.key) else {
                continue;
            };

            // Filter out future tasks by parsing timestamp from key
            // Format: tasks/<ts>/<pri>/<job_id>/<attempt>
            let mut parts = key_str.split('/');
            if parts.next() != Some("tasks") {
                continue;
            }
            let ts_part = match parts.next() {
                Some(x) => x,
                None => continue,
            };
            if let Ok(ts_val) = ts_part.parse::<u64>() {
                if ts_val > now_ms as u64 {
                    continue;
                }
            }

            // [SILO-SCAN-3] Skip inflight tasks
            if self.inflight.lock().unwrap().contains(key_str) {
                continue;
            }

            let task = match decode_task(&kv.value) {
                Ok(t) => t,
                Err(_) => continue, // Skip malformed tasks
            };
            let entry = BrokerTask {
                key: key_str.to_string(),
                task,
            };

            // [SILO-SCAN-2] Insert into buffer if not already present
            if self.buffer.get(&entry.key).is_none() {
                self.buffer.insert(entry.key.clone(), entry);
                inserted += 1;

                // Yield periodically to avoid starving other tasks
                if inserted % 16 == 0 {
                    tokio::task::yield_now().await;
                }
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
            // Hydrate concurrency holders from durable state on startup
            let _ = broker.concurrency.counts().hydrate(&broker.db).await;
            tokio::time::sleep(Duration::from_millis(10)).await;

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
                let inserted = broker.scan_tasks(now_ms).await;

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

    /// Claim up to `max` ready tasks from the head of the buffer.
    pub fn claim_ready(&self, max: usize) -> Vec<BrokerTask> {
        let mut claimed = Vec::with_capacity(max);

        while claimed.len() < max {
            // Find the first claimable entry
            let candidate_key = self.buffer.iter().find_map(|entry| {
                let key = entry.key();

                // Skip if inflight
                if self.inflight.lock().unwrap().contains(key) {
                    return None;
                }

                // Let RequestTickets through to dequeue where max_concurrency will be checked properly
                // (we can't check here because we don't know the max_concurrency without loading the job)

                Some(key.clone())
            });

            let Some(key) = candidate_key else { break };

            // Reserve as inflight
            if !self.inflight.lock().unwrap().insert(key.clone()) {
                continue; // Lost race, try again
            }

            // Remove from buffer
            if let Some(entry) = self.buffer.remove(&key) {
                claimed.push(entry.value().clone());
            } else {
                // Removal failed, clear inflight reservation
                self.inflight.lock().unwrap().remove(&key);
            }
        }

        claimed
    }

    /// Try to claim tasks. If none available, wait briefly for scanner to populate.
    pub async fn claim_ready_or_nudge(&self, max: usize) -> Vec<BrokerTask> {
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

    /// Try to claim up to `max` ready tasks for a specific tenant. If none, nudge scanner.
    pub async fn claim_ready_for_tenant_or_nudge(
        &self,
        tenant: &str,
        max: usize,
    ) -> Vec<BrokerTask> {
        let _ = tenant; // tasks are tenant-agnostic; claim globally
        self.claim_ready_or_nudge(max).await
    }

    /// Requeue tasks back into the buffer after a failed durable write.
    pub fn requeue(&self, tasks: Vec<BrokerTask>) {
        let mut inflight = self.inflight.lock().unwrap();
        for entry in tasks {
            inflight.remove(&entry.key);
            self.buffer.insert(entry.key.clone(), entry);
        }
    }

    /// Acknowledge that these tasks are durably leased and can be removed from in-flight tracking.
    pub fn ack_durable(&self, keys: &[String]) {
        let mut inflight = self.inflight.lock().unwrap();
        for k in keys {
            inflight.remove(k);
        }
    }

    /// Remove any buffered entries that match the provided keys.
    pub fn evict_keys(&self, keys: &[String]) {
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
