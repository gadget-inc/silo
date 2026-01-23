//! Concurrency ticket management for limiting parallel job execution.
//!
//! This module implements a ticket-based concurrency control system where jobs must acquire
//! tickets from named queues before they can proceed to execution.
//!
//! # Key Invariants (see specs/job_shard.als for formal specification)
//!
//! - Queue limit is enforced: at most N holders per queue at any time.
//!   Enforced by `try_reserve` which atomically checks capacity and reserves a slot.
//!
//! - Holders only exist for tasks that are active (in DB queue, buffer, or leased).
//!   Holders are created at enqueue (granted) or grant_next, and released when task completes,
//!   lease expires, or cancelled task is cleaned up at dequeue.
//!
//! # TOCTOU Prevention
//!
//! To prevent time-of-check-time-of-use races, in-memory concurrency counts are updated
//! BEFORE the DB write. If the DB write fails, callers must use rollback methods to revert
//! the in-memory state. This ensures no window exists where capacity appears available
//! between check and grant.
//!
//! # Cancellation Semantics
//!
//! - [SILO-GRANT-CXL] Requests for cancelled jobs are skipped during grant_next.
//!   When release_and_grant_next scans for the next request to grant, it checks if the job
//!   is cancelled and deletes the request without granting.
//!
//! - [SILO-DEQ-CXL-REL] Holders for cancelled tasks are released at dequeue cleanup.
//!   When dequeue encounters a cancelled job's task, it releases any held tickets.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use slatedb::{Db, DbIterator, WriteBatch};

use crate::dst_events::{self, DstEvent};

use crate::codec::{
    decode_concurrency_action, encode_concurrency_action, encode_holder, encode_task,
};
use crate::job::{ConcurrencyLimit, JobView};
use crate::keys::{concurrency_holder_key, concurrency_request_key, job_cancelled_key, task_key};
use crate::task::{ConcurrencyAction, HolderRecord, Task};

/// Information needed to rollback a release_and_grant operation if DB write fails.
#[derive(Debug, Clone)]
pub struct ReleaseGrantRollback {
    pub tenant: String,
    pub queue: String,
    pub released_task_id: String,
    /// If Some, a grant was made to this task_id
    pub granted_task_id: Option<String>,
}

/// Result of attempting to enqueue a job with concurrency limits
#[derive(Debug)]
pub enum RequestTicketOutcome {
    /// Concurrency ticket granted immediately - RunAttempt task created.
    /// Note: In-memory slot is already reserved by try_reserve before this is returned.
    GrantedImmediately { task_id: String, queue: String },
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
    // Composite key: "<tenant>|<queue>" -> set of task ids holding tickets
    holders: Mutex<HashMap<String, HashSet<String>>>,
}

impl Default for ConcurrencyCounts {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrencyCounts {
    pub fn new() -> Self {
        Self {
            holders: Mutex::new(HashMap::new()),
        }
    }

    pub async fn hydrate(&self, db: &Db) -> Result<(), slatedb::Error> {
        // Scan holders under holders/<tenant>/<queue>/<task-id>
        let start: Vec<u8> = b"holders/".to_vec();
        let mut end: Vec<u8> = b"holders/".to_vec();
        end.push(0xFF);
        let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await?;
        loop {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };
            if let Ok(s) = std::str::from_utf8(&kv.key) {
                // Expect: holders/<tenant>/<queue>/<task-id>
                let mut parts = s.split('/');
                if parts.next() != Some("holders") {
                    continue;
                }
                let tenant = match parts.next() {
                    Some(t) => t,
                    None => continue,
                };
                let queue = match parts.next() {
                    Some(q) => q,
                    None => continue,
                };
                let task = match parts.next() {
                    Some(x) => x,
                    None => continue,
                };
                let key = format!("{}|{}", tenant, queue);
                let mut h = self.holders.lock().unwrap();
                let set = h.entry(key).or_default();
                set.insert(task.to_string());
            }
        }
        Ok(())
    }

    /// Atomically try to reserve a concurrency slot.
    /// Returns true if the slot was reserved, false if at capacity.
    /// This MUST be called before writing to the DB to prevent TOCTOU races.
    /// If the DB write fails, call `release_reservation` to roll back.
    pub fn try_reserve(
        &self,
        tenant: &str,
        queue: &str,
        task_id: &str,
        limit: usize,
        job_id: &str,
    ) -> bool {
        let mut h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        let set = h.entry(key).or_default();

        // Check if we're at capacity
        if set.len() >= limit {
            return false;
        }

        // Reserve the slot atomically
        set.insert(task_id.to_string());

        // Emit DST event for instrumentation
        dst_events::emit(DstEvent::ConcurrencyTicketGranted {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            task_id: task_id.to_string(),
            job_id: job_id.to_string(),
        });

        true
    }

    /// Release a reservation made by `try_reserve` if the DB write fails.
    pub fn release_reservation(&self, tenant: &str, queue: &str, task_id: &str) {
        let mut h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        if let Some(set) = h.get_mut(&key) {
            set.remove(task_id);
        }
    }

    /// Atomically release one task and reserve another in a single mutex acquisition.
    /// This prevents a race window where capacity appears available between release and grant.
    /// Used by release_and_grant_next to keep in-memory counts consistent.
    pub fn atomic_release_and_reserve(
        &self,
        tenant: &str,
        queue: &str,
        release_task_id: &str,
        reserve_task_id: &str,
        reserve_job_id: &str,
    ) {
        let mut h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        let set = h.entry(key).or_default();
        set.remove(release_task_id);
        set.insert(reserve_task_id.to_string());

        // Emit DST events for instrumentation
        dst_events::emit(DstEvent::ConcurrencyTicketReleased {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            task_id: release_task_id.to_string(),
        });
        dst_events::emit(DstEvent::ConcurrencyTicketGranted {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            task_id: reserve_task_id.to_string(),
            job_id: reserve_job_id.to_string(),
        });
    }

    /// Atomically release a task without granting to another.
    /// Used when there are no pending requests to grant to.
    pub fn atomic_release(&self, tenant: &str, queue: &str, task_id: &str) {
        let mut h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        if let Some(set) = h.get_mut(&key) {
            set.remove(task_id);
        }

        // Emit DST event for instrumentation
        dst_events::emit(DstEvent::ConcurrencyTicketReleased {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            task_id: task_id.to_string(),
        });
    }

    /// Rollback a release_and_reserve operation if DB write fails.
    /// Re-adds the released task and removes the reserved task.
    pub fn rollback_release_and_reserve(
        &self,
        tenant: &str,
        queue: &str,
        released_task_id: &str,
        reserved_task_id: &str,
    ) {
        let mut h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        let set = h.entry(key).or_default();
        set.remove(reserved_task_id);
        set.insert(released_task_id.to_string());
    }

    /// Rollback a release operation if DB write fails.
    /// Re-adds the released task.
    pub fn rollback_release(&self, tenant: &str, queue: &str, task_id: &str) {
        let mut h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        let set = h.entry(key).or_default();
        set.insert(task_id.to_string());
    }

    /// Get the current holder count for a queue.
    /// Useful for testing and debugging.
    pub fn holder_count(&self, tenant: &str, queue: &str) -> usize {
        let h = self.holders.lock().unwrap();
        let key = format!("{}|{}", tenant, queue);
        h.get(&key).map(|s| s.len()).unwrap_or(0)
    }
}

/// High-level concurrency manager
pub struct ConcurrencyManager {
    counts: ConcurrencyCounts,
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
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

    /// Handle concurrency for a new job enqueue.
    ///
    /// IMPORTANT: This method atomically reserves in-memory concurrency slots BEFORE returning.
    /// If the DB write fails after calling this, you MUST call `rollback_grant` to release
    /// the reservation.
    #[allow(clippy::too_many_arguments)]
    pub fn handle_enqueue(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        priority: u8,
        start_at_ms: i64,
        now_ms: i64,
        limits: &[ConcurrencyLimit],
        task_group: &str,
        attempt_number: u32,
    ) -> Result<Option<RequestTicketOutcome>, String> {
        // Only gate on the first limit (if any)
        let Some(limit) = limits.first() else {
            return Ok(None); // No limits
        };

        let queue = &limit.key;
        let max_allowed = limit.max_concurrency as usize;

        // [SILO-ENQ-CONC-1] Atomically check and reserve if queue has capacity
        // This prevents TOCTOU races by reserving the slot before writing to DB
        if self
            .counts
            .try_reserve(tenant, queue, task_id, max_allowed, job_id)
        {
            // Grant immediately: [SILO-ENQ-CONC-2] create holder, [SILO-ENQ-CONC-3] create task
            // Note: in-memory slot is already reserved by try_reserve
            append_grant_edits(
                batch,
                now_ms,
                tenant,
                queue,
                task_id,
                start_at_ms,
                priority,
                job_id,
                attempt_number,
                task_group,
            )?;
            Ok(Some(RequestTicketOutcome::GrantedImmediately {
                task_id: task_id.to_string(),
                queue: queue.clone(),
            }))
        } else if start_at_ms <= now_ms {
            // [SILO-ENQ-CONC-4] Queue is at capacity
            // [SILO-ENQ-CONC-5] No task in DB queue
            // [SILO-ENQ-CONC-6] Create request record instead
            append_request_edits(
                batch,
                tenant,
                queue,
                now_ms,
                start_at_ms,
                priority,
                job_id,
                attempt_number,
                task_group,
            )?;
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
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                request_id: request_id.clone(),
                task_group: task_group.to_string(),
            };
            let ticket_value = encode_task(&ticket)?;
            batch.put(
                task_key(task_group, start_at_ms, priority, job_id, attempt_number).as_bytes(),
                &ticket_value,
            );
            Ok(Some(RequestTicketOutcome::FutureRequestTaskWritten {
                queue: queue.clone(),
                task_id: request_id,
            }))
        }
    }

    /// Rollback a grant that was made by `handle_enqueue` if the DB write fails.
    /// Call this with the queue and task_id from the GrantedImmediately outcome.
    pub fn rollback_grant(&self, tenant: &str, queue: &str, task_id: &str) {
        self.counts.release_reservation(tenant, queue, task_id);
    }

    /// Process a RequestTicket task during dequeue.
    ///
    /// IMPORTANT: This method atomically reserves in-memory concurrency slots when granting.
    /// If the DB write fails after calling this with a Granted outcome, you MUST call
    /// `rollback_grant` to release the reservation.
    #[allow(clippy::too_many_arguments)]
    pub fn process_ticket_request_task(
        &self,
        batch: &mut WriteBatch,
        task_key: &str,
        tenant: &str,
        queue: &str,
        request_id: &str,
        job_id: &str,
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

        // Atomically check and reserve the slot to prevent TOCTOU races
        if !self
            .counts
            .try_reserve(tenant, queue, request_id, max_allowed, job_id)
        {
            return Ok(RequestTicketTaskOutcome::Requested);
        }

        // Grant: create holder in DB, delete ticket
        let holder = HolderRecord {
            granted_at_ms: now_ms,
        };
        let hval = encode_holder(&holder)?;
        batch.put(
            concurrency_holder_key(tenant, queue, request_id).as_bytes(),
            &hval,
        );
        batch.delete(task_key.as_bytes());

        Ok(RequestTicketTaskOutcome::Granted {
            request_id: request_id.to_string(),
            queue: queue.to_string(),
        })
    }

    /// Release holders and grant next requests.
    ///
    /// IMPORTANT: This method updates in-memory counts BEFORE returning (atomically).
    /// If the DB write fails, you MUST call `rollback_release_grants` with the returned
    /// rollback info to revert the in-memory changes.
    ///
    /// Returns a tuple of (rollback_info, queues_to_wakeup).
    /// Call broker.wakeup() for each queue after successful DB write.
    pub async fn release_and_grant_next(
        &self,
        db: &Db,
        batch: &mut WriteBatch,
        tenant: &str,
        queues: &[String],
        finished_task_id: &str,
        now_ms: i64,
    ) -> Result<Vec<ReleaseGrantRollback>, String> {
        let mut rollbacks: Vec<ReleaseGrantRollback> = Vec::new();

        for queue in queues {
            // [SILO-REL-1] Remove holder for this task/queue from DB
            batch.delete(concurrency_holder_key(tenant, queue, finished_task_id).as_bytes());

            // [SILO-GRANT-1] Queue now has capacity (we just released)
            // [SILO-GRANT-2] Find pending requests for this queue
            let start = format!("requests/{}/{}/", tenant, queue).into_bytes();
            let mut end: Vec<u8> = format!("requests/{}/{}/", tenant, queue).into_bytes();
            end.push(0xFF);
            let mut iter: DbIterator = db
                .scan::<Vec<u8>, _>(start..=end)
                .await
                .map_err(|e| e.to_string())?;

            let mut granted_task_id: Option<String> = None;
            let mut granted_job_id: Option<String> = None;

            while let Some(kv) = iter.next().await.map_err(|e| e.to_string())? {
                type ArchivedAction = <ConcurrencyAction as rkyv::Archive>::Archived;
                let decoded = decode_concurrency_action(&kv.value)?;
                let a: &ArchivedAction = decoded.archived();
                match a {
                    ArchivedAction::EnqueueTask {
                        start_time_ms,
                        priority,
                        job_id,
                        attempt_number,
                        task_group,
                    } => {
                        let job_id_str = job_id.as_str();
                        let task_group_str = task_group.as_str();

                        // [SILO-GRANT-CXL] Check if job is cancelled - if so, delete request and continue
                        let cancelled_key = job_cancelled_key(tenant, job_id_str);
                        let is_cancelled = db
                            .get(cancelled_key.as_bytes())
                            .await
                            .map_err(|e| e.to_string())?
                            .is_some();

                        if is_cancelled {
                            // [SILO-GRANT-CXL-2] Delete the cancelled request without granting
                            batch.delete(&kv.key);
                            tracing::debug!(
                                job_id = %job_id_str,
                                queue = %queue,
                                "grant_next: skipping cancelled job request"
                            );
                            continue;
                        }

                        let req_key_str = String::from_utf8_lossy(&kv.key).to_string();
                        let request_id =
                            req_key_str.split('/').next_back().unwrap_or("").to_string();

                        if *start_time_ms > now_ms {
                            // Not ready yet; leave request for later and stop searching
                            // (requests are ordered by time, so subsequent ones are also not ready)
                            break;
                        }

                        // [SILO-GRANT-3] Create holder for this task/queue in DB
                        let holder = HolderRecord {
                            granted_at_ms: now_ms,
                        };
                        let holder_val = encode_holder(&holder)?;
                        batch.put(
                            concurrency_holder_key(tenant, queue, &request_id).as_bytes(),
                            &holder_val,
                        );

                        // [SILO-GRANT-4] Create RunAttempt task in DB queue
                        let task = Task::RunAttempt {
                            id: request_id.clone(),
                            tenant: tenant.to_string(),
                            job_id: job_id_str.to_string(),
                            attempt_number: *attempt_number,
                            held_queues: vec![queue.clone()],
                            task_group: task_group_str.to_string(),
                        };
                        let tval = encode_task(&task)?;
                        batch.put(
                            task_key(
                                task_group_str,
                                *start_time_ms,
                                *priority,
                                job_id_str,
                                *attempt_number,
                            )
                            .as_bytes(),
                            &tval,
                        );
                        batch.delete(&kv.key);

                        granted_task_id = Some(request_id);
                        granted_job_id = Some(job_id_str.to_string());

                        // Only grant one request per release
                        break;
                    }
                }
            }

            // Update in-memory counts ATOMICALLY before returning
            // This prevents a race window where capacity appears available
            if let Some(ref new_task_id) = granted_task_id {
                // Release old + grant new in one atomic operation
                let job_id = granted_job_id.as_deref().unwrap_or("");
                self.counts.atomic_release_and_reserve(
                    tenant,
                    queue,
                    finished_task_id,
                    new_task_id,
                    job_id,
                );
            } else {
                // Just release, no grant
                self.counts.atomic_release(tenant, queue, finished_task_id);
            }

            rollbacks.push(ReleaseGrantRollback {
                tenant: tenant.to_string(),
                queue: queue.clone(),
                released_task_id: finished_task_id.to_string(),
                granted_task_id,
            });
        }

        Ok(rollbacks)
    }

    /// Rollback release_and_grant operations if DB write fails.
    pub fn rollback_release_grants(&self, rollbacks: &[ReleaseGrantRollback]) {
        for rb in rollbacks {
            if let Some(ref granted) = rb.granted_task_id {
                self.counts.rollback_release_and_reserve(
                    &rb.tenant,
                    &rb.queue,
                    &rb.released_task_id,
                    granted,
                );
            } else {
                self.counts
                    .rollback_release(&rb.tenant, &rb.queue, &rb.released_task_id);
            }
        }
    }
}

// Internal helper functions

/// Append DB edits to grant a concurrency slot: creates holder record and RunAttempt task.
/// Note: In-memory reservation should already be done via try_reserve before calling this.
#[allow(clippy::too_many_arguments)]
fn append_grant_edits(
    batch: &mut WriteBatch,
    now_ms: i64,
    tenant: &str,
    queue: &str,
    task_id: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
    task_group: &str,
) -> Result<(), String> {
    let holder = HolderRecord {
        granted_at_ms: now_ms,
    };
    let holder_val = encode_holder(&holder)?;
    batch.put(
        concurrency_holder_key(tenant, queue, task_id).as_bytes(),
        &holder_val,
    );

    let task = Task::RunAttempt {
        id: task_id.to_string(),
        tenant: tenant.to_string(),
        job_id: job_id.to_string(),
        attempt_number,
        held_queues: vec![queue.to_string()],
        task_group: task_group.to_string(),
    };
    let task_value = encode_task(&task)?;
    batch.put(
        task_key(task_group, start_time_ms, priority, job_id, attempt_number).as_bytes(),
        &task_value,
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_request_edits(
    batch: &mut WriteBatch,
    tenant: &str,
    queue: &str,
    _now_ms: i64,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
    task_group: &str,
) -> Result<(), String> {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority,
        job_id: job_id.to_string(),
        attempt_number,
        task_group: task_group.to_string(),
    };
    let action_val = encode_concurrency_action(&action)?;
    let req_key = concurrency_request_key(
        tenant,
        queue,
        start_time_ms,
        priority,
        &uuid::Uuid::new_v4().to_string(),
    );
    batch.put(req_key.as_bytes(), &action_val);
    Ok(())
}
