use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use rkyv::AlignedVec;
use slatedb::{Db, DbIterator, WriteBatch};

use crate::codec::encode_task;
use crate::job::{ConcurrencyLimit, JobView};
use crate::job_store_shard::{HolderRecord, Task};
use crate::keys::{concurrency_holder_key, concurrency_request_key, task_key};

#[derive(Debug, Clone)]
pub enum MemoryEvent {
    Granted { queue: String, task_id: String },
    Released { queue: String, task_id: String },
}

/// Result of attempting to enqueue a job with concurrency limits
#[derive(Debug)]
pub enum RequestTicketOutcome {
    /// Concurrency tocket granted immediately - RunAttempt task created
    GrantedImmediately {
        task_id: String,
        queue: String,
        events: Vec<MemoryEvent>,
    },
    /// Ticket queued as a request record (for immediate start time but no capacity)
    TicketRequested { queue: String },
    /// Job queued as a RequestTicket task (for future start time)
    FutureRequestTaskWritten { queue: String, task_id: String },
}

/// Result of processing a RequestTicket task
#[derive(Debug)]
pub enum RequestTicketTaskOutcome {
    /// Ticket granted - RunAttempt lease created
    Granted { request_id: String, queue: String },
    /// Ticket not available right now, but request has been durably stored
    Requested,
    /// Job missing
    JobMissing,
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

/// High-level concurrency manager
pub struct ConcurrencyManager {
    counts: ConcurrencyCounts,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        Self {
            counts: ConcurrencyCounts::new(),
        }
    }

    pub fn counts(&self) -> &ConcurrencyCounts {
        &self.counts
    }

    /// Handle concurrency for a new job enqueue
    pub fn handle_enqueue(
        &self,
        batch: &mut WriteBatch,
        task_id: &str,
        job_id: &str,
        priority: u8,
        start_at_ms: i64,
        now_ms: i64,
        limits: &[ConcurrencyLimit],
    ) -> Result<Option<RequestTicketOutcome>, String> {
        // Only gate on the first limit (if any)
        let Some(limit) = limits.get(0) else {
            return Ok(None); // No limits
        };

        let queue = &limit.key;
        let max_allowed = limit.max_concurrency as usize;

        if self.counts.can_grant(queue, max_allowed) {
            // Grant immediately
            let events = append_grant_edits(
                batch,
                now_ms,
                queue,
                task_id,
                start_at_ms,
                priority,
                job_id,
                1,
            )?;
            Ok(Some(RequestTicketOutcome::GrantedImmediately {
                task_id: task_id.to_string(),
                queue: queue.clone(),
                events,
            }))
        } else if start_at_ms <= now_ms {
            // Job should start now but no capacity: queue as request
            append_request_edits(batch, queue, now_ms, start_at_ms, priority, job_id, 1)?;
            Ok(Some(RequestTicketOutcome::TicketRequested {
                queue: queue.clone(),
            }))
        } else {
            // Job scheduled for future: queue as RequestTicket task
            let request_id = uuid::Uuid::new_v4().to_string();
            let ticket = Task::RequestTicket {
                queue: queue.clone(),
                start_time_ms: start_at_ms,
                priority,
                job_id: job_id.to_string(),
                attempt_number: 1,
                request_id: request_id.clone(),
            };
            let ticket_value: AlignedVec =
                rkyv::to_bytes::<Task, 256>(&ticket).map_err(|e| e.to_string())?;
            batch.put(
                task_key(start_at_ms, priority, job_id, 1).as_bytes(),
                &ticket_value,
            );
            Ok(Some(RequestTicketOutcome::FutureRequestTaskWritten {
                queue: queue.clone(),
                task_id: request_id,
            }))
        }
    }

    /// Process a RequestTicket task during dequeue
    pub fn process_ticket_request_task(
        &self,
        batch: &mut WriteBatch,
        task_key: &str,
        queue: &str,
        request_id: &str,
        _job_id: &str,
        _attempt_number: u32,
        now_ms: i64,
        job_view: Option<&JobView>,
    ) -> Result<RequestTicketTaskOutcome, String> {
        // Check if job exists
        let Some(view) = job_view else {
            batch.delete(task_key.as_bytes());
            return Ok(RequestTicketTaskOutcome::JobMissing);
        };

        // Determine max concurrency for this queue
        let mut max_allowed: usize = 1;
        for lim in view.concurrency_limits() {
            if lim.key == queue {
                max_allowed = lim.max_concurrency as usize;
                break;
            }
        }

        // Check if can grant
        if !self.counts.can_grant(queue, max_allowed) {
            return Ok(RequestTicketTaskOutcome::Requested);
        }

        // Grant: create holder, delete ticket
        let holder = HolderRecord {
            granted_at_ms: now_ms,
        };
        let hval: AlignedVec =
            rkyv::to_bytes::<HolderRecord, 256>(&holder).map_err(|e| e.to_string())?;
        batch.put(concurrency_holder_key(queue, request_id).as_bytes(), &hval);
        batch.delete(task_key.as_bytes());

        Ok(RequestTicketTaskOutcome::Granted {
            request_id: request_id.to_string(),
            queue: queue.to_string(),
        })
    }

    /// Release holders and grant next requests
    pub async fn release_and_grant_next(
        &self,
        db: &Db,
        batch: &mut WriteBatch,
        queues: &[String],
        finished_task_id: &str,
        now_ms: i64,
    ) -> Result<Vec<MemoryEvent>, String> {
        append_release_and_grant_next(db, batch, queues, finished_task_id, now_ms).await
    }
}

// Internal helper functions

fn append_grant_edits(
    batch: &mut WriteBatch,
    now_ms: i64,
    queue: &str,
    task_id: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
) -> Result<Vec<MemoryEvent>, String> {
    let holder = HolderRecord {
        granted_at_ms: now_ms,
    };
    let holder_val: AlignedVec =
        rkyv::to_bytes::<HolderRecord, 256>(&holder).map_err(|e| e.to_string())?;
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

    Ok(vec![MemoryEvent::Granted {
        queue: queue.to_string(),
        task_id: task_id.to_string(),
    }])
}

fn append_request_edits(
    batch: &mut WriteBatch,
    queue: &str,
    _now_ms: i64,
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
    let req_key = concurrency_request_key(
        queue,
        start_time_ms,
        priority,
        &uuid::Uuid::new_v4().to_string(),
    );
    batch.put(req_key.as_bytes(), &action_val);
    Ok(())
}

async fn append_release_and_grant_next(
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
