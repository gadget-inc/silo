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
use crate::concurrency::ConcurrencyCounts;
use crate::job_store_shard::Task;
use crate::keys::leased_task_key;
use tracing::{debug, info_span};

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
    buffer: Arc<SkipMap<String, BrokerTask>>, // key is the full task key
    inflight: Arc<Mutex<HashSet<String>>>,
    running: Arc<AtomicBool>,
    notify: Arc<Notify>,
    scan_requested: Arc<AtomicBool>,
    // Concurrency: in-memory counts for tickets
    concurrency: Arc<ConcurrencyCounts>,
    target_buffer: usize,
    scan_batch: usize,
}

impl TaskBroker {
    pub fn new(db: Arc<Db>) -> Arc<Self> {
        Arc::new(Self {
            db,
            buffer: Arc::new(SkipMap::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            running: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            scan_requested: Arc::new(AtomicBool::new(false)),
            concurrency: Arc::new(ConcurrencyCounts::new()),
            target_buffer: 4096,
            scan_batch: 1024,
        })
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Start the background scanning loop.
    pub fn start(self: &Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }

        let broker = Arc::clone(self);
        tokio::spawn(async move {
            // Hydrate concurrency holders from durable state on startup (best effort)
            let _ = broker.concurrency.hydrate(&broker.db).await;
            let mut sleep_ms: u64 = 20;
            let min_sleep_ms: u64 = 20;
            let max_sleep_ms: u64 = 1000;
            let _tombstone_ttl = Duration::from_millis(2_000);

            loop {
                if !broker.running.load(Ordering::SeqCst) {
                    break;
                }

                // Avoid overfilling the buffer
                if broker.buffer.len() >= broker.target_buffer {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                // Scan ready tasks up to now
                let now_ms = crate::job_store_shard::now_epoch_ms();
                let start: Vec<u8> = b"tasks/".to_vec();
                let end_prefix = crate::keys::task_key(now_ms, 99, "~", u32::MAX);
                let mut end: Vec<u8> = end_prefix.into_bytes();
                end.push(0xFF);

                let mut inserted: usize = 0;

                // Each scan call creates an iterator snapshot
                let scan_res = broker.db.scan::<Vec<u8>, _>(start..=end).await;
                let mut iter = match scan_res {
                    Ok(it) => it,
                    Err(_) => {
                        // On scan error, backoff briefly
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                while inserted < broker.scan_batch && broker.buffer.len() < broker.target_buffer {
                    let maybe_kv = match iter.next().await {
                        Ok(v) => v,
                        Err(_) => None,
                    };
                    let Some(kv) = maybe_kv else { break };

                    // Convert key to &str
                    let key_str = match str::from_utf8(&kv.key) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    // If key is inflight, ensure it's not in the buffer and skip
                    if broker.inflight.lock().unwrap().contains(key_str) {
                        let _ = broker.buffer.remove(key_str);
                        continue;
                    }

                    // Decode the task from value bytes
                    let task = decode_task(&kv.value);
                    let task_id = match &task {
                        Task::RunAttempt { id, .. } => id.clone(),
                    };

                    // If a lease exists for this task id, skip to avoid double-brokering
                    let lkey = leased_task_key(&task_id);
                    if let Ok(Some(_lease)) = broker.db.get(lkey.as_bytes()).await {
                        continue;
                    }

                    let entry = BrokerTask {
                        key: key_str.to_string(),
                        task,
                    };

                    // Insert into buffer if absent (idempotent)
                    if broker.buffer.get(&entry.key).is_none() {
                        let _ = broker.buffer.insert(entry.key.clone(), entry.clone());
                        // lightweight event for scan insert
                        let span = info_span!("broker.scan_insert", key = %entry.key);
                        let _g = span.enter();
                        debug!("inserted ready task into buffer");
                        inserted += 1;
                    }
                }

                // Adjust backoff based on whether we found anything
                if inserted == 0 {
                    sleep_ms = (sleep_ms.saturating_mul(2)).min(max_sleep_ms);
                } else {
                    sleep_ms = min_sleep_ms;
                }
                // If a scan was explicitly requested, skip sleeping and loop immediately
                if broker.scan_requested.swap(false, Ordering::SeqCst) {
                    sleep_ms = min_sleep_ms;
                    continue;
                }
                let delay = tokio::time::sleep(Duration::from_millis(sleep_ms));
                tokio::pin!(delay);
                tokio::select! {
                    _ = &mut delay => {},
                    _ = broker.notify.notified() => {
                        // Wake early and reset backoff for prompt refill
                        sleep_ms = min_sleep_ms;
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
        let mut claimed: Vec<BrokerTask> = Vec::with_capacity(max);
        for _ in 0..max {
            let Some(front) = self.buffer.front() else {
                break;
            };
            let key = front.key().clone();
            // Reserve in inflight first to close scanner race
            {
                let mut inflight = self.inflight.lock().unwrap();
                if !inflight.insert(key.clone()) {
                    // already inflight; remove stray buffer entry and continue
                    let _ = self.buffer.remove(&key);
                    continue;
                }
            }
            match self.buffer.remove(&key) {
                Some(entry) => {
                    claimed.push(entry.value().clone());
                }
                None => {
                    // couldn't remove; clear inflight reservation and continue
                    self.inflight.lock().unwrap().remove(&key);
                    continue;
                }
            }
        }
        claimed
    }

    /// Try to claim up to `max` tasks. If none available, nudge scanner and
    /// yield once before retrying the claim. Avoids DB hits in dequeuers.
    pub async fn claim_ready_or_nudge(&self, max: usize) -> Vec<BrokerTask> {
        let mut claimed = self.claim_ready(max);
        if claimed.is_empty() {
            self.wakeup();
            tokio::task::yield_now().await;
            claimed = self.claim_ready(max);
        }
        claimed
    }

    /// Requeue tasks back into the buffer after a failed durable write.
    pub fn requeue(&self, tasks: Vec<BrokerTask>) {
        let mut inflight = self.inflight.lock().unwrap();
        for entry in tasks.into_iter() {
            inflight.remove(&entry.key);
            let _ = self.buffer.insert(entry.key.clone(), entry);
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
            let _ = self.buffer.remove(k);
        }
    }

    /// Wake the scanner to refill promptly (e.g., after enqueuing a ready task).
    pub fn wakeup(&self) {
        self.scan_requested.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    // Concurrency helpers (in-memory decisions)
    pub fn concurrency_can_grant(&self, queue: &str, limit: usize) -> bool {
        self.concurrency.can_grant(queue, limit)
    }
    pub fn concurrency_record_grant(&self, queue: &str, task_id: &str) {
        self.concurrency.record_grant(queue, task_id)
    }
    pub fn concurrency_record_release(&self, queue: &str, task_id: &str) {
        self.concurrency.record_release(queue, task_id)
    }
}

impl Drop for TaskBroker {
    fn drop(&mut self) {
        self.stop();
    }
}
