use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use rkyv::AlignedVec;
use slatedb::{Db, DbIterator, WriteBatch};

use crate::codec::encode_task;
use crate::job_store_shard::{HolderRecord, Task};
use crate::keys::{concurrency_holder_key, concurrency_request_key, task_key};

#[derive(Debug, Clone)]
pub enum MemoryEvent {
    Granted { queue: String, task_id: String },
    Released { queue: String, task_id: String },
}

#[derive(Debug, Clone)]
pub struct DurableEdits {
    pub batch: WriteBatch,
    pub events: Vec<MemoryEvent>,
}

impl DurableEdits {
    pub fn new() -> Self {
        Self {
            batch: WriteBatch::new(),
            events: Vec::new(),
        }
    }
}

/// In-memory counts for concurrency holders
pub struct ConcurrencyCounts {
    holders: Mutex<HashMap<String, HashSet<String>>>,
}

impl ConcurrencyCounts {
    pub fn new() -> Self {
        Self {
            holders: Mutex::new(HashMap::new()),
        }
    }

    pub async fn hydrate(&self, db: &Db) -> Result<(), slatedb::Error> {
        let start: Vec<u8> = b"holders/".to_vec();
        let mut end: Vec<u8> = b"holders/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await?;
        loop {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };
            if let Ok(s) = std::str::from_utf8(&kv.key) {
                let mut parts = s.splitn(3, '/');
                let _ = parts.next();
                let q = parts.next().unwrap_or("");
                let tid = parts.next().unwrap_or("");
                if !q.is_empty() && !tid.is_empty() {
                    let mut h = self.holders.lock().unwrap();
                    let set = h.entry(q.to_string()).or_insert_with(HashSet::new);
                    set.insert(tid.to_string());
                }
            }
        }
        Ok(())
    }

    pub fn can_grant(&self, queue: &str, limit: usize) -> bool {
        self.holders
            .lock()
            .unwrap()
            .get(queue)
            .map(|s| s.len())
            .unwrap_or(0)
            < limit
    }

    pub fn record_grant(&self, queue: &str, task_id: &str) {
        let mut h = self.holders.lock().unwrap();
        let set = h.entry(queue.to_string()).or_insert_with(HashSet::new);
        set.insert(task_id.to_string());
    }

    pub fn record_release(&self, queue: &str, task_id: &str) {
        let mut h = self.holders.lock().unwrap();
        if let Some(set) = h.get_mut(queue) {
            set.remove(task_id);
        }
    }
}

/// Build edits to grant immediately (holder + task)
pub fn build_grant_edits(
    now_ms: i64,
    queue: &str,
    task_id: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
) -> Result<DurableEdits, String> {
    let mut out = DurableEdits::new();
    let holder = crate::job_store_shard::HolderRecord {
        granted_at_ms: now_ms,
    };
    let holder_val: AlignedVec =
        rkyv::to_bytes::<HolderRecord, 256>(&holder).map_err(|e| e.to_string())?;
    out.batch.put(
        concurrency_holder_key(queue, task_id).as_bytes(),
        &holder_val,
    );

    let task = Task::RunAttempt {
        id: task_id.to_string(),
        job_id: job_id.to_string(),
        attempt_number,
        held_queues: vec![queue.to_string()],
    };
    let task_value = encode_task(&task)?;
    out.batch.put(
        task_key(start_time_ms, priority, job_id, attempt_number).as_bytes(),
        &task_value,
    );
    out.events.push(MemoryEvent::Granted {
        queue: queue.to_string(),
        task_id: task_id.to_string(),
    });
    Ok(out)
}

/// Build edits to enqueue a concurrency request
pub fn build_request_edits(
    queue: &str,
    now_ms: i64,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
) -> Result<DurableEdits, String> {
    let mut out = DurableEdits::new();
    let action = crate::job_store_shard::ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority,
        job_id: job_id.to_string(),
        attempt_number,
    };
    let action_val: AlignedVec =
        rkyv::to_bytes::<crate::job_store_shard::ConcurrencyAction, 256>(&action)
            .map_err(|e| e.to_string())?;
    let req_key = concurrency_request_key(queue, now_ms, &uuid::Uuid::new_v4().to_string());
    out.batch.put(req_key.as_bytes(), &action_val);
    Ok(out)
}

/// Build edits to release holders for finished task id and maybe grant one request per queue
pub async fn build_release_and_grant_edits(
    db: &Db,
    queues: &[String],
    finished_task_id: &str,
    now_ms: i64,
) -> Result<DurableEdits, String> {
    let mut out = DurableEdits::new();
    for queue in queues {
        // delete holder
        out.batch
            .delete(concurrency_holder_key(queue, finished_task_id).as_bytes());
        out.events.push(MemoryEvent::Released {
            queue: queue.clone(),
            task_id: finished_task_id.to_string(),
        });

        // grant next request if any
        let start = format!("requests/{}/", queue).into_bytes();
        let mut end: Vec<u8> = format!("requests/{}/", queue).into_bytes();
        end.push(0xFF);
        let mut iter: DbIterator = db
            .scan::<Vec<u8>, _>(start..=end)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(kv) = iter.next().await.map_err(|e| e.to_string())? {
            type ArchivedAction =
                <crate::job_store_shard::ConcurrencyAction as rkyv::Archive>::Archived;
            let a: &ArchivedAction = unsafe {
                rkyv::archived_root::<crate::job_store_shard::ConcurrencyAction>(&kv.value)
            };
            match a {
                ArchivedAction::EnqueueTask {
                    start_time_ms,
                    priority,
                    job_id,
                    attempt_number,
                } => {
                    let req_key_str = String::from_utf8_lossy(&kv.key).to_string();
                    let request_id = req_key_str.split('/').last().unwrap_or("").to_string();
                    // Only grant if ready to run
                    if *start_time_ms > now_ms {
                        // not ready yet; leave request in place
                    } else {
                        let holder = HolderRecord {
                            granted_at_ms: now_ms,
                        };
                        let holder_val: AlignedVec = rkyv::to_bytes::<HolderRecord, 256>(&holder)
                            .map_err(|e| e.to_string())?;
                        // Holder identity is the new task id (request id)
                        out.batch.put(
                            concurrency_holder_key(queue, &request_id).as_bytes(),
                            &holder_val,
                        );

                        let task = Task::RunAttempt {
                            id: request_id.clone(),
                            job_id: job_id.as_str().to_string(),
                            attempt_number: *attempt_number,
                            held_queues: vec![queue.clone()],
                        };
                        let tval = encode_task(&task)?;
                        out.batch.put(
                            task_key(*start_time_ms, *priority, job_id.as_str(), *attempt_number)
                                .as_bytes(),
                            &tval,
                        );

                        out.batch.delete(&kv.key);
                        // Memory event carries holder identity (task id / request id)
                        out.events.push(MemoryEvent::Granted {
                            queue: queue.clone(),
                            task_id: request_id,
                        });
                    }
                }
            }
        }
    }
    Ok(out)
}

// Append-style helpers for composing single durable batch
pub fn append_grant_edits(
    batch: &mut WriteBatch,
    now_ms: i64,
    queue: &str,
    task_id: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
) -> Result<Vec<MemoryEvent>, String> {
    let holder = crate::job_store_shard::HolderRecord {
        granted_at_ms: now_ms,
    };
    let holder_val: AlignedVec =
        rkyv::to_bytes::<HolderRecord, 256>(&holder).map_err(|e| e.to_string())?;
    // Use per-attempt task id as holder identity
    batch.put(
        concurrency_holder_key(queue, task_id).as_bytes(),
        &holder_val,
    );

    let task = Task::RunAttempt {
        id: task_id.to_string(),
        job_id: job_id.to_string(),
        attempt_number,
        held_queues: vec![queue.to_string()],
    };
    let task_value = encode_task(&task)?;
    batch.put(
        task_key(start_time_ms, priority, job_id, attempt_number).as_bytes(),
        &task_value,
    );
    // Memory event carries holder identity (task_id)
    Ok(vec![MemoryEvent::Granted {
        queue: queue.to_string(),
        task_id: task_id.to_string(),
    }])
}

pub fn append_request_edits(
    batch: &mut WriteBatch,
    queue: &str,
    now_ms: i64,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
) -> Result<(), String> {
    let action = crate::job_store_shard::ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority,
        job_id: job_id.to_string(),
        attempt_number,
    };
    let action_val: AlignedVec =
        rkyv::to_bytes::<crate::job_store_shard::ConcurrencyAction, 256>(&action)
            .map_err(|e| e.to_string())?;
    let req_key = concurrency_request_key(queue, now_ms, &uuid::Uuid::new_v4().to_string());
    batch.put(req_key.as_bytes(), &action_val);
    Ok(())
}

pub async fn append_release_and_grant_next(
    db: &Db,
    batch: &mut WriteBatch,
    queues: &[String],
    finished_task_id: &str,
    now_ms: i64,
) -> Result<Vec<MemoryEvent>, String> {
    let mut events: Vec<MemoryEvent> = Vec::new();
    for queue in queues {
        batch.delete(concurrency_holder_key(queue, finished_task_id).as_bytes());
        events.push(MemoryEvent::Released {
            queue: queue.clone(),
            task_id: finished_task_id.to_string(),
        });

        // grant next
        let start = format!("requests/{}/", queue).into_bytes();
        let mut end: Vec<u8> = format!("requests/{}/", queue).into_bytes();
        end.push(0xFF);
        let mut iter: DbIterator = db
            .scan::<Vec<u8>, _>(start..=end)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(kv) = iter.next().await.map_err(|e| e.to_string())? {
            type ArchivedAction =
                <crate::job_store_shard::ConcurrencyAction as rkyv::Archive>::Archived;
            let a: &ArchivedAction = unsafe {
                rkyv::archived_root::<crate::job_store_shard::ConcurrencyAction>(&kv.value)
            };
            match a {
                ArchivedAction::EnqueueTask {
                    start_time_ms,
                    priority,
                    job_id,
                    attempt_number,
                } => {
                    let req_key_str = String::from_utf8_lossy(&kv.key).to_string();
                    let request_id = req_key_str.split('/').last().unwrap_or("").to_string();
                    if *start_time_ms > now_ms {
                        // Not ready yet; leave request for later
                    } else {
                        let holder = HolderRecord {
                            granted_at_ms: now_ms,
                        };
                        let holder_val: AlignedVec = rkyv::to_bytes::<HolderRecord, 256>(&holder)
                            .map_err(|e| e.to_string())?;
                        // Holder identity is the new task id (request id)
                        batch.put(
                            concurrency_holder_key(queue, &request_id).as_bytes(),
                            &holder_val,
                        );

                        let task = Task::RunAttempt {
                            id: request_id.clone(),
                            job_id: job_id.as_str().to_string(),
                            attempt_number: *attempt_number,
                            held_queues: vec![queue.clone()],
                        };
                        let tval = encode_task(&task)?;
                        batch.put(
                            task_key(*start_time_ms, *priority, job_id.as_str(), *attempt_number)
                                .as_bytes(),
                            &tval,
                        );
                        batch.delete(&kv.key);
                        events.push(MemoryEvent::Granted {
                            queue: queue.clone(),
                            task_id: request_id,
                        });
                    }
                }
            }
        }
    }
    Ok(events)
}
