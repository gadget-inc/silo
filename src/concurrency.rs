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
//!   Holders are created at enqueue (granted) or grant_next, and released when task completes, lease expires, or cancelled task is cleaned up at dequeue.
//!
//! # TOCTOU Prevention
//!
//! To prevent time-of-check-time-of-use races, in-memory concurrency counts are updated BEFORE the DB write. If the DB write fails, callers must use rollback methods to revert the in-memory state. This ensures no window exists where capacity appears available between check and grant.
//!
//! # Grant Broker
//!
//! Granting pending requests is decoupled from releasing holders. When a holder is released
//! (via report_attempt_outcome or cancel_job), the caller deletes the holder in its own batch,
//! updates in-memory counts, and signals the grant scanner via `request_grant`. A single
//! background scanner task processes all pending grants, eliminating the race condition where
//! concurrent release_and_grant_next calls would both scan and grant the same pending request.
//!
//! This matches the Alloy model which defines `releaseHolder` and `grantNextRequest` as
//! separate, independent transitions.
//!
//! # Cancellation Semantics
//!
//! - When a job is cancelled, cancel_job eagerly deletes any pending requests and
//!   releases holders for tasks in the queue. This avoids relying on lazy cleanup.
//!
//! - Requests for terminal jobs (Succeeded/Failed/Cancelled) are skipped during grant_next.
//!   This handles the case where a job succeeds via one attempt while a pending concurrency
//!   request from a restart or retry is still in the DB. Since Cancelled is terminal,
//!   this also catches any stale cancelled requests.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;

use slatedb::config::WriteOptions;
use slatedb::{Db, DbIterator, WriteBatch};

use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::helpers::WriteBatcher;

use crate::codec::{
    decode_concurrency_action, decode_floating_limit_state, encode_concurrency_action,
    encode_holder, encode_task,
};
use crate::dst_events::{self, DstEvent};
use crate::job::{ConcurrencyLimit, JobStatusKind, JobView, Limit};
use crate::job_store_shard::helpers::decode_job_status_owned;
use crate::keys::{
    concurrency_counts_key, concurrency_holder_key, concurrency_holders_queue_prefix,
    concurrency_request_key, concurrency_request_prefix, concurrency_requester_counter_key,
    concurrency_requests_prefix, end_bound, floating_limit_state_key, job_status_key,
    parse_concurrency_holder_key, parse_concurrency_request_key, task_key,
};
use crate::shard_range::ShardRange;
use crate::task::{ConcurrencyAction, HolderRecord, Task};
use crate::task_broker::TaskBrokerRegistry;

/// Error type for concurrency operations that can fail due to storage or encoding errors.
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error(transparent)]
    Slate(#[from] slatedb::Error),
    #[error("encoding error: {0}")]
    Encoding(String),
}

impl From<String> for ConcurrencyError {
    fn from(s: String) -> Self {
        ConcurrencyError::Encoding(s)
    }
}

impl From<crate::codec::CodecError> for ConcurrencyError {
    fn from(e: crate::codec::CodecError) -> Self {
        ConcurrencyError::Encoding(e.to_string())
    }
}

/// Default cap on in-flight `db.get(job_status_key)` lookups during a single
/// `process_grants` pass. Caps slatedb-side block-fetch fan-out so the scanner
/// can't pin unbounded `Bytes` while validating a large pending backlog.
/// Mirrors the `CONCURRENCY = 64` pattern used in
/// `job_store_shard::get_jobs_status_batch` for the same reason.
///
/// Operators can override this per shard via the
/// `database.concurrency_status_lookup_concurrency` TOML setting.
pub const DEFAULT_STATUS_LOOKUP_CONCURRENCY: usize = 64;

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
    // Composite key: storekey-encoded (tenant, queue) -> set of task ids holding tickets
    holders: Mutex<HashMap<Vec<u8>, HashSet<String>>>,
    // Track which (tenant, queue) keys have been hydrated from durable storage
    hydrated_queues: Mutex<HashSet<Vec<u8>>>,
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
            hydrated_queues: Mutex::new(HashSet::new()),
        }
    }

    /// Hydrate a specific queue's concurrency holder state from durable storage.
    ///
    /// Uses the per-queue prefix for efficient scanning of only the relevant holders. The `range` parameter filters holders to only load those for tenants within the shard's range. This is critical after shard splits - both child shards clone the same holder records, and without filtering, both would think they own the same concurrency tickets, leading to limit violations.
    pub async fn hydrate_queue(
        &self,
        db: &Db,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
    ) -> Result<(), slatedb::Error> {
        let key = concurrency_counts_key(tenant, queue);

        // Scan holders for this specific tenant/queue using the queue prefix
        let start = concurrency_holders_queue_prefix(tenant, queue);
        let end = end_bound(&start);
        let mut iter: DbIterator = db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await?;

        let mut task_ids = Vec::new();
        loop {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };

            // Parse holder key to extract tenant, queue, task_id
            let Some(parsed) = parse_concurrency_holder_key(&kv.key) else {
                continue;
            };

            // Filter by shard range - only hydrate holders for tenants in this shard
            if !range.contains_tenant(&parsed.tenant) {
                tracing::debug!(
                    tenant = %parsed.tenant,
                    queue = %parsed.queue,
                    task = %parsed.task_id,
                    range = %range,
                    "skipping holder outside shard range during queue hydration"
                );
                continue;
            }

            task_ids.push(parsed.task_id);
        }

        // Update holders map
        {
            let mut h = self.holders.lock().unwrap();
            let set = h.entry(key.clone()).or_default();
            for task_id in task_ids {
                set.insert(task_id);
            }
        }

        // Mark queue as hydrated
        {
            let mut hydrated = self.hydrated_queues.lock().unwrap();
            hydrated.insert(key);
        }

        Ok(())
    }

    /// Ensure a queue is hydrated before checking capacity.
    /// Called by try_reserve on first access to each queue.
    /// Fast path: if already hydrated, return immediately.
    pub async fn ensure_hydrated(
        &self,
        db: &Db,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
    ) -> Result<(), slatedb::Error> {
        let key = concurrency_counts_key(tenant, queue);

        // Fast path: check if already hydrated
        {
            let hydrated = self.hydrated_queues.lock().unwrap();
            if hydrated.contains(&key) {
                return Ok(());
            }
        }

        // Slow path: hydrate the queue
        self.hydrate_queue(db, range, tenant, queue).await
    }

    /// Atomically try to reserve a concurrency slot.
    /// Returns true if the slot was reserved, false if at capacity.
    /// This MUST be called before writing to the DB to prevent TOCTOU races.
    /// If the DB write fails, call `release_reservation` to roll back.
    ///
    /// This method lazily hydrates the queue from durable storage on first access.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_reserve(
        &self,
        db: &Db,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        task_id: &str,
        limit: usize,
        job_id: &str,
    ) -> Result<bool, slatedb::Error> {
        // Ensure the queue is hydrated before checking capacity
        self.ensure_hydrated(db, range, tenant, queue).await?;

        Ok(self.try_reserve_internal(tenant, queue, task_id, limit, job_id))
    }

    /// Internal method that performs the atomic reservation without hydration check.
    /// Used by try_reserve after hydration, and for testing the in-memory logic.
    pub(crate) fn try_reserve_internal(
        &self,
        tenant: &str,
        queue: &str,
        task_id: &str,
        limit: usize,
        job_id: &str,
    ) -> bool {
        let key = concurrency_counts_key(tenant, queue);
        let reserved = {
            let mut h = self.holders.lock().unwrap();
            let set = h.entry(key).or_default();

            // Check if we're at capacity
            if set.len() >= limit {
                return false;
            }

            // Reserve the slot atomically
            set.insert(task_id.to_string());
            true
        };

        if reserved {
            dst_events::emit(DstEvent::ConcurrencyTicketGranted {
                tenant: tenant.to_string(),
                queue: queue.to_string(),
                task_id: task_id.to_string(),
                job_id: job_id.to_string(),
            });
        }

        reserved
    }

    /// Mark a queue as hydrated without actually scanning storage.
    /// Useful for tests that want to use try_reserve_internal directly.
    #[doc(hidden)]
    pub fn mark_hydrated(&self, tenant: &str, queue: &str) {
        let key = concurrency_counts_key(tenant, queue);
        let mut hydrated = self.hydrated_queues.lock().unwrap();
        hydrated.insert(key);
    }

    /// Synchronous try_reserve for testing when the queue is known to be hydrated or when testing in-memory reservation logic without DB.
    ///
    /// This method is exposed for testing purposes only. Production code should use `try_reserve` which performs lazy hydration.
    #[doc(hidden)]
    pub fn try_reserve_sync(
        &self,
        tenant: &str,
        queue: &str,
        task_id: &str,
        limit: usize,
        job_id: &str,
    ) -> bool {
        self.try_reserve_internal(tenant, queue, task_id, limit, job_id)
    }

    /// Release a reservation made by `try_reserve` if the DB write fails.
    pub fn release_reservation(&self, tenant: &str, queue: &str, task_id: &str) {
        let key = concurrency_counts_key(tenant, queue);
        let mut h = self.holders.lock().unwrap();
        if let Some(set) = h.get_mut(&key) {
            set.remove(task_id);
        }
    }

    /// Atomically release a task without granting to another.
    /// Used by callers post-commit after deleting a holder from the DB batch.
    pub fn atomic_release(&self, tenant: &str, queue: &str, task_id: &str) {
        let key = concurrency_counts_key(tenant, queue);
        {
            let mut h = self.holders.lock().unwrap();
            if let Some(set) = h.get_mut(&key) {
                set.remove(task_id);
            }
        }

        // Emit DST event for instrumentation
        dst_events::emit(DstEvent::ConcurrencyTicketReleased {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            task_id: task_id.to_string(),
        });
    }

    /// Get the current holder count for a queue.
    /// Useful for testing and debugging.
    pub fn holder_count(&self, tenant: &str, queue: &str) -> usize {
        let h = self.holders.lock().unwrap();
        let key = concurrency_counts_key(tenant, queue);
        h.get(&key).map(|s| s.len()).unwrap_or(0)
    }
}

/// The type of concurrency limit for a queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyLimitType {
    Fixed,
    Floating,
}

/// Cached concurrency limit info for a queue.
#[derive(Debug, Clone)]
pub struct CachedQueueLimit {
    pub tenant: String,
    pub queue: String,
    pub max_concurrency: u32,
    pub limit_type: ConcurrencyLimitType,
}

/// High-level concurrency manager with a background grant scanner.
///
/// The grant scanner is a single background task that processes all pending grants.
/// This eliminates the race condition where concurrent `release_and_grant_next` calls
/// would both scan and grant the same pending request.
pub struct ConcurrencyManager {
    counts: ConcurrencyCounts,
    /// Pending grant counts per queue. Key: concurrency_counts_key(tenant, queue).
    /// Value: (tenant, queue, count). Lock held only briefly for increment/drain.
    /// BTreeMap ensures deterministic iteration order for DST reproducibility.
    pending_grants: Mutex<BTreeMap<Vec<u8>, (String, String, u32)>>,
    grant_notify: tokio::sync::Notify,
    grant_running: AtomicBool,
    /// In-memory cache of resolved concurrency limits per queue.
    /// Key: concurrency_counts_key(tenant, queue). Populated during enqueue and grant_next.
    limit_cache: Mutex<HashMap<Vec<u8>, CachedQueueLimit>>,
    /// Cap on in-flight `db.get(job_status_key)` lookups during a single
    /// `process_grants` pass. Configured at construction time and held for the
    /// life of the manager. Always `>= 1`.
    status_lookup_concurrency: usize,
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        Self::with_status_lookup_concurrency(DEFAULT_STATUS_LOOKUP_CONCURRENCY)
    }

    /// Construct a `ConcurrencyManager` with a custom cap on in-flight
    /// `db.get(job_status_key)` lookups in `process_grants`. Values < 1 are
    /// clamped to 1.
    pub fn with_status_lookup_concurrency(status_lookup_concurrency: usize) -> Self {
        Self {
            counts: ConcurrencyCounts::new(),
            pending_grants: Mutex::new(BTreeMap::new()),
            grant_notify: tokio::sync::Notify::new(),
            grant_running: AtomicBool::new(false),
            limit_cache: Mutex::new(HashMap::new()),
            status_lookup_concurrency: status_lookup_concurrency.max(1),
        }
    }

    /// The configured cap on in-flight status lookups in `process_grants`.
    pub fn status_lookup_concurrency(&self) -> usize {
        self.status_lookup_concurrency
    }

    pub fn counts(&self) -> &ConcurrencyCounts {
        &self.counts
    }

    /// Cache the resolved concurrency limit for a queue. Called during enqueue and grant_next
    /// so that the query system can read limits without scanning the DB.
    pub fn cache_queue_limit(
        &self,
        tenant: &str,
        queue: &str,
        max_concurrency: u32,
        limit_type: ConcurrencyLimitType,
    ) {
        let key = concurrency_counts_key(tenant, queue);
        let mut cache = self.limit_cache.lock().unwrap();
        cache.insert(
            key,
            CachedQueueLimit {
                tenant: tenant.to_string(),
                queue: queue.to_string(),
                max_concurrency,
                limit_type,
            },
        );
    }

    /// Snapshot the current limit cache for use by the query system.
    /// Returns a vec of all cached queue limits.
    pub fn snapshot_queue_limits(&self) -> Vec<CachedQueueLimit> {
        let cache = self.limit_cache.lock().unwrap();
        cache.values().cloned().collect()
    }

    async fn resolve_queue_capacity(
        db: &Db,
        tenant: &str,
        queue: &str,
        view: &JobView,
    ) -> (usize, ConcurrencyLimitType) {
        for limit in view.limits() {
            match limit {
                Limit::Concurrency(cl) if cl.key == queue => {
                    return (cl.max_concurrency as usize, ConcurrencyLimitType::Fixed);
                }
                Limit::FloatingConcurrency(fl) if fl.key == queue => {
                    let state_key = floating_limit_state_key(tenant, queue);
                    let state_capacity = match db.get(&state_key).await {
                        Ok(Some(raw)) => match decode_floating_limit_state(raw) {
                            Ok(state) => Some(state.current_max_concurrency() as usize),
                            Err(_) => None,
                        },
                        _ => None,
                    };
                    return (
                        state_capacity.unwrap_or(fl.default_max_concurrency as usize),
                        ConcurrencyLimitType::Floating,
                    );
                }
                _ => {}
            }
        }
        (1, ConcurrencyLimitType::Fixed)
    }

    /// Handle concurrency for a new job enqueue.
    ///
    /// IMPORTANT: This method atomically reserves in-memory concurrency slots BEFORE returning.
    /// If the DB write fails after calling this, you MUST call `rollback_grant` to release the reservation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_enqueue<W: WriteBatcher>(
        &self,
        db: &Db,
        range: &ShardRange,
        writer: &mut W,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        priority: u8,
        start_at_ms: i64,
        now_ms: i64,
        limits: &[ConcurrencyLimit],
        task_group: &str,
        attempt_number: u32,
        relative_attempt_number: u32,
        skip_try_reserve: bool,
    ) -> Result<Option<RequestTicketOutcome>, ConcurrencyError> {
        // Only gate on the first limit (if any)
        let Some(limit) = limits.first() else {
            return Ok(None); // No limits
        };

        let queue = &limit.key;
        let max_allowed = limit.max_concurrency as usize;

        // [SILO-ENQ-CONC-1] [SILO-IMP-CONC-1] [SILO-REIMP-CONC-1] Atomically check and reserve if queue has capacity
        // This prevents TOCTOU races by reserving the slot before writing to DB
        // [SILO-RETRY-5-CONC] Skip try_reserve for retries: the old holder is still in-memory
        // (released post-commit), so the retry must go through the request queue. This matches
        // the Alloy model's completeFailureRetryReleaseTicket which creates a TicketRequest.
        if !skip_try_reserve
            && self
                .counts
                .try_reserve(db, range, tenant, queue, task_id, max_allowed, job_id)
                .await?
        {
            // Grant immediately: [SILO-ENQ-CONC-2] [SILO-ENQ-CONC-3] [SILO-IMP-CONC-2] [SILO-REIMP-CONC-2] create holder + task in DB queue
            // Note: in-memory slot is already reserved by try_reserve
            append_grant_edits(
                writer,
                now_ms,
                tenant,
                queue,
                task_id,
                start_at_ms,
                priority,
                job_id,
                attempt_number,
                relative_attempt_number,
                task_group,
            )?;
            Ok(Some(RequestTicketOutcome::GrantedImmediately {
                task_id: task_id.to_string(),
                queue: queue.clone(),
            }))
        } else if start_at_ms <= now_ms {
            // [SILO-ENQ-CONC-4] [SILO-IMP-CONC-3] [SILO-REIMP-CONC-3] Queue is at capacity
            // [SILO-ENQ-CONC-5] [SILO-IMP-CONC-4] [SILO-REIMP-CONC-4] No task in DB queue, request created
            // [SILO-ENQ-CONC-6] Create request record instead
            append_request_edits(
                writer,
                tenant,
                queue,
                start_at_ms,
                priority,
                job_id,
                attempt_number,
                relative_attempt_number,
                task_group,
            )?;
            Ok(Some(RequestTicketOutcome::TicketRequested {
                queue: queue.clone(),
            }))
        } else {
            // Job scheduled for future: queue as RequestTicket task
            let suffix = format!("{:08x}", rand::random::<u32>());
            let request_id = format!("{job_id}:{attempt_number}:{suffix}");
            let ticket = Task::RequestTicket {
                queue: queue.clone(),
                start_time_ms: start_at_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                relative_attempt_number,
                request_id: request_id.clone(),
                task_group: task_group.to_string(),
            };
            let ticket_value = encode_task(&ticket);
            writer.put(
                task_key(task_group, start_at_ms, priority, job_id, attempt_number),
                &ticket_value,
            )?;
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
    /// IMPORTANT: This method atomically reserves in-memory concurrency slots when granting.  If the DB write fails after calling this with a Granted outcome, you MUST call `rollback_grant` to release the reservation.
    #[allow(clippy::too_many_arguments)]
    pub async fn process_ticket_request_task(
        &self,
        db: &Db,
        range: &ShardRange,
        batch: &mut WriteBatch,
        task_key: &[u8],
        tenant: &str,
        queue: &str,
        request_id: &str,
        job_id: &str,
        _attempt_number: u32,
        now_ms: i64,
        job_view: Option<&JobView>,
    ) -> Result<RequestTicketTaskOutcome, ConcurrencyError> {
        // Check if job exists
        let Some(view) = job_view else {
            batch.delete(task_key);
            return Ok(RequestTicketTaskOutcome::JobMissing);
        };

        let (max_allowed, limit_type) = Self::resolve_queue_capacity(db, tenant, queue, view).await;

        // Cache the resolved limit for the query system
        self.cache_queue_limit(tenant, queue, max_allowed as u32, limit_type);

        // Atomically check and reserve the slot to prevent TOCTOU races
        if !self
            .counts
            .try_reserve(db, range, tenant, queue, request_id, max_allowed, job_id)
            .await?
        {
            return Ok(RequestTicketTaskOutcome::Requested);
        }

        // Grant: create holder in DB, delete ticket
        let holder = HolderRecord {
            granted_at_ms: now_ms,
        };
        let hval = encode_holder(&holder);
        batch.put(concurrency_holder_key(tenant, queue, request_id), &hval);
        batch.delete(task_key);

        Ok(RequestTicketTaskOutcome::Granted {
            request_id: request_id.to_string(),
            queue: queue.to_string(),
        })
    }

    /// Signal that a concurrency slot was freed for the given queue.
    /// Called by callers after releasing a holder (post-commit).
    /// Wakes the grant scanner to process pending requests.
    pub fn request_grant(&self, tenant: &str, queue: &str) {
        self.request_grant_count(tenant, queue, 1);
    }

    fn request_grant_count(&self, tenant: &str, queue: &str, count: u32) {
        let key = concurrency_counts_key(tenant, queue);
        {
            let mut pending = self.pending_grants.lock().unwrap();
            let entry = pending
                .entry(key)
                .or_insert_with(|| (tenant.to_string(), queue.to_string(), 0));
            entry.2 += count;
        }
        self.grant_notify.notify_one();
    }

    /// Start the background grant scanner task.
    /// On startup, scans for existing pending requests and processes them.
    /// Then enters an event-driven loop, woken by `request_grant` calls.
    pub fn start_grant_scanner(
        self: &Arc<Self>,
        db: Arc<Db>,
        brokers: Arc<TaskBrokerRegistry>,
        range: ShardRange,
    ) {
        self.grant_running.store(true, Ordering::SeqCst);
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            // Startup: scan for existing pending requests
            mgr.reconcile_pending_requests(&db, &range).await;

            loop {
                mgr.grant_notify.notified().await;
                if !mgr.grant_running.load(Ordering::SeqCst) {
                    break;
                }

                // Inner loop: drain and process until no more pending work
                loop {
                    let work = {
                        let mut pending = mgr.pending_grants.lock().unwrap();
                        std::mem::take(&mut *pending)
                    };
                    if work.is_empty() {
                        break;
                    }

                    let mut granted_groups: Vec<String> = Vec::new();
                    for (_key, (tenant, queue, count)) in work {
                        let groups = mgr
                            .process_grants(&db, &range, &tenant, &queue, count)
                            .await;
                        granted_groups.extend(groups);
                    }

                    // Wake only the brokers whose task groups received grants
                    if !granted_groups.is_empty() {
                        brokers.wakeup_groups(&granted_groups);
                    }
                }
            }
        });
    }

    /// Stop the background grant scanner.
    pub fn stop_grant_scanner(&self) {
        self.grant_running.store(false, Ordering::SeqCst);
        self.grant_notify.notify_one();
    }

    /// Scan all pending concurrency requests and trigger grants for each queue.
    ///
    /// This is used at grant-scanner startup and by the shard's periodic
    /// reconciliation task to self-heal from missed notifications.
    pub async fn reconcile_pending_requests(&self, db: &Db, range: &ShardRange) {
        let start = concurrency_requests_prefix();
        let end = end_bound(&start);
        let mut iter = match db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await
        {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "grant scanner: failed to scan requests during reconciliation"
                );
                return;
            }
        };

        // Count pending requests per (tenant, queue)
        let mut queue_counts: BTreeMap<Vec<u8>, (String, String, u32)> = BTreeMap::new();
        loop {
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "grant scanner: reconciliation scan iteration error"
                    );
                    break;
                }
            };

            let Some(parsed) = parse_concurrency_request_key(&kv.key) else {
                continue;
            };

            // Only process requests for tenants in our range
            if !range.contains_tenant(&parsed.tenant) {
                continue;
            }

            let key = concurrency_counts_key(&parsed.tenant, &parsed.queue);
            let entry = queue_counts
                .entry(key)
                .or_insert_with(|| (parsed.tenant.clone(), parsed.queue.clone(), 0));
            entry.2 += 1;
        }

        // Trigger grants for each queue with pending requests
        for (_key, (tenant, queue, count)) in queue_counts {
            self.request_grant_count(&tenant, &queue, count);
        }
    }

    /// Process pending grant requests for a single (tenant, queue).
    /// Scans up to `count` pending requests and grants those for which capacity exists.
    /// Returns the task groups that received grants.
    ///
    /// Optimized for throughput: scans candidates in batches, batch-validates their
    /// status concurrently, reserves in-memory slots, then writes all grants plus
    /// stale deletions in a single durable batch. If stale/corrupt requests reduce
    /// the yield, scanning continues until `count` valid grants are found or the
    /// request queue is exhausted.
    pub(crate) async fn process_grants(
        &self,
        db: &Db,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        count: u32,
    ) -> Vec<String> {
        if let Err(e) = self.counts.ensure_hydrated(db, range, tenant, queue).await {
            tracing::warn!(
                error = %e,
                tenant = %tenant,
                queue = %queue,
                "grant scanner: failed to hydrate queue"
            );
            return Vec::new();
        }

        let now_ms = crate::job_store_shard::now_epoch_ms();

        // [SILO-GRANT-2] Pre: Scan for pending requests for this queue
        let start = concurrency_request_prefix(tenant, queue);
        let end_key = end_bound(&start);
        let mut iter = match db
            .scan_with_options::<Vec<u8>, _>(start..end_key, &crate::scan_options())
            .await
        {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(error = %e, "grant scanner: failed to scan requests");
                return Vec::new();
            }
        };

        struct ScannedRequest {
            request_key: Vec<u8>,
            request_id: String,
            job_id: String,
            attempt_number: u32,
            relative_attempt_number: u32,
            start_time_ms: i64,
            priority: u8,
            task_group: String,
        }

        let mut max_concurrency: Option<(usize, ConcurrencyLimitType)> = None;
        let mut batch = WriteBatch::new();
        let mut grants: Vec<(String, String)> = Vec::new();
        let mut stale_and_corrupt_count: usize = 0;
        let mut iter_exhausted = false;
        let mut capacity_exhausted = false;

        // Scan→validate→grant loop: keeps pulling from the iterator until we've
        // granted `count` requests, or hit the end / capacity limit. Each iteration
        // scans a batch of candidates, validates them concurrently, then reserves
        // slots for valid ones. Stale/corrupt entries are cleaned up along the way.
        while grants.len() < count as usize && !iter_exhausted && !capacity_exhausted {
            let needed = count as usize - grants.len();

            // --- Scan batch of candidates ---
            let mut scanned: Vec<ScannedRequest> = Vec::new();
            while scanned.len() < needed {
                let kv = match iter.next().await {
                    Ok(Some(kv)) => kv,
                    Ok(None) => {
                        iter_exhausted = true;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "grant scanner: iteration error");
                        iter_exhausted = true;
                        break;
                    }
                };

                let parsed_req = parse_concurrency_request_key(&kv.key);

                let decoded = match decode_concurrency_action(kv.value.clone()) {
                    Ok(d) => d,
                    Err(_) => {
                        tracing::warn!(queue = %queue, "grant scanner: failed to decode request, deleting");
                        batch.delete(&kv.key);
                        stale_and_corrupt_count += 1;
                        continue;
                    }
                };
                let a = decoded.fb();
                let Some(et) = a.variant_as_enqueue_task() else {
                    tracing::warn!(queue = %queue, "grant scanner: unknown concurrency action variant, deleting");
                    batch.delete(&kv.key);
                    stale_and_corrupt_count += 1;
                    continue;
                };

                let start_time_ms = et.start_time_ms();
                let job_id_str = et.job_id().unwrap_or_default();

                if start_time_ms > now_ms {
                    iter_exhausted = true;
                    break;
                }

                let Some(parsed_req) = parsed_req else {
                    continue;
                };
                let request_id = parsed_req.request_id();

                // Resolve max_concurrency from the first request whose job info is readable.
                // Don't cache failures — a stale request's missing job shouldn't lock
                // the limit to 1 for the rest of the scan.
                if max_concurrency.is_none() {
                    let job_key = crate::keys::job_info_key(tenant, job_id_str);
                    match db.get(&job_key).await {
                        Ok(Some(bytes)) => match JobView::new(bytes) {
                            Ok(view) => {
                                let (l, lt) =
                                    Self::resolve_queue_capacity(db, tenant, queue, &view).await;
                                self.cache_queue_limit(tenant, queue, l as u32, lt);
                                max_concurrency = Some((l, lt));
                            }
                            Err(_) => {
                                tracing::warn!(
                                    job_id = %job_id_str,
                                    queue = %queue,
                                    "grant scanner: undecodable job info, will retry with next request"
                                );
                            }
                        },
                        Ok(None) => {
                            tracing::debug!(
                                job_id = %job_id_str,
                                queue = %queue,
                                "grant scanner: job info missing, will retry with next request"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                job_id = %job_id_str,
                                queue = %queue,
                                "grant scanner: failed to read job info, will retry with next request"
                            );
                        }
                    };
                }

                scanned.push(ScannedRequest {
                    request_key: kv.key.to_vec(),
                    request_id,
                    job_id: job_id_str.to_string(),
                    attempt_number: et.attempt_number(),
                    relative_attempt_number: et.relative_attempt_number(),
                    start_time_ms,
                    priority: et.priority(),
                    task_group: et.task_group().unwrap_or_default().to_string(),
                });
            }

            if scanned.is_empty() {
                break;
            }

            // --- Batch validate status ---
            // Use `buffered` (order-preserving) rather than `join_all` so that
            // we cap in-flight slatedb reads at `self.status_lookup_concurrency`.
            // Each db.get() can fan out to many SST block fetches; with thousands
            // of pending requests, an unbounded join_all pinned multi-GB of Bytes.
            // `buffered` (not `buffer_unordered`) keeps results in scanned-order
            // for the zip below. Keys are materialized owned to avoid borrowing
            // `scanned` across await points (needed for HRTB on the async closure).
            let status_keys: Vec<_> = scanned
                .iter()
                .map(|c| job_status_key(tenant, &c.job_id))
                .collect();
            let status_results: Vec<_> = futures::stream::iter(
                status_keys
                    .into_iter()
                    .map(|key| async move { db.get(&key).await }),
            )
            .buffered(self.status_lookup_concurrency)
            .collect()
            .await;

            let mut valid_requests: Vec<ScannedRequest> = Vec::new();
            for (req, status_result) in scanned.into_iter().zip(status_results.into_iter()) {
                let is_valid = match status_result {
                    Ok(Some(status_raw)) => match decode_job_status_owned(&status_raw) {
                        // [SILO-GRANT-5] Request records are only valid for the currently
                        // scheduled attempt.
                        Ok(status)
                            if status.kind != JobStatusKind::Scheduled
                                || status.current_attempt != Some(req.attempt_number) =>
                        {
                            // [SILO-GRANT-6] Drop stale request key without granting work.
                            tracing::debug!(
                                job_id = %req.job_id,
                                queue = %queue,
                                request_attempt = req.attempt_number,
                                status_kind = ?status.kind,
                                status_attempt = ?status.current_attempt,
                                "grant scanner: skipping stale request for non-current attempt"
                            );
                            false
                        }
                        Ok(_) => true,
                        Err(_) => {
                            tracing::warn!(
                                job_id = %req.job_id,
                                queue = %queue,
                                "grant scanner: dropping request with unreadable job status"
                            );
                            false
                        }
                    },
                    Ok(None) => {
                        tracing::debug!(
                            job_id = %req.job_id,
                            queue = %queue,
                            "grant scanner: dropping request for missing job status"
                        );
                        false
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            job_id = %req.job_id,
                            queue = %queue,
                            "grant scanner: failed to load job status; skipping request this pass"
                        );
                        continue;
                    }
                };

                if is_valid {
                    valid_requests.push(req);
                } else {
                    batch.delete(&req.request_key);
                    stale_and_corrupt_count += 1;
                }
            }

            // --- Reserve in-memory slots and accumulate grant edits ---
            let limit = max_concurrency.map(|(l, _)| l).unwrap_or(1);
            for req in &valid_requests {
                if grants.len() >= count as usize {
                    break;
                }

                // [SILO-GRANT-1] Pre: Queue has capacity — try to atomically reserve a slot
                if !self.counts.try_reserve_internal(
                    tenant,
                    queue,
                    &req.request_id,
                    limit,
                    &req.job_id,
                ) {
                    capacity_exhausted = true;
                    break;
                }

                // [SILO-GRANT-3] Create holder
                let holder_val = encode_holder(&HolderRecord {
                    granted_at_ms: now_ms,
                });
                batch.put(
                    concurrency_holder_key(tenant, queue, &req.request_id),
                    &holder_val,
                );

                // [SILO-GRANT-4] Create RunAttempt task
                let tval = encode_task(&Task::RunAttempt {
                    id: req.request_id.clone(),
                    tenant: tenant.to_string(),
                    job_id: req.job_id.clone(),
                    attempt_number: req.attempt_number,
                    relative_attempt_number: req.relative_attempt_number,
                    held_queues: vec![queue.to_string()],
                    task_group: req.task_group.clone(),
                });
                batch.put(
                    task_key(
                        &req.task_group,
                        req.start_time_ms,
                        req.priority,
                        &req.job_id,
                        req.attempt_number,
                    ),
                    &tval,
                );
                batch.delete(&req.request_key);

                grants.push((req.request_id.clone(), req.task_group.clone()));
            }
        }

        // Single combined counter decrement for all grants + stale/corrupt deletions
        let total_counter_decrement = grants.len() + stale_and_corrupt_count;
        if total_counter_decrement > 0 {
            batch.merge(
                concurrency_requester_counter_key(tenant, queue),
                encode_counter(-(total_counter_decrement as i64)),
            );
        }

        if grants.is_empty() && stale_and_corrupt_count == 0 {
            return Vec::new();
        }

        // --- Single durable write ---
        if let Err(e) = db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                },
            )
            .await
        {
            for (request_id, _) in &grants {
                self.counts.release_reservation(tenant, queue, request_id);
            }
            tracing::warn!(
                error = %e,
                count = grants.len(),
                "grant scanner: batch write failed, rolled back all reservations"
            );
            return Vec::new();
        }

        for (request_id, task_group) in &grants {
            tracing::debug!(
                queue = %queue,
                request_id = %request_id,
                task_group = %task_group,
                "grant scanner: granted concurrency ticket"
            );
        }

        grants.into_iter().map(|(_, tg)| tg).collect()
    }
}

/// Append DB edits to grant a concurrency slot: creates holder record and RunAttempt task.
/// Note: In-memory reservation should already be done via try_reserve before calling this.
#[allow(clippy::too_many_arguments)]
fn append_grant_edits<W: WriteBatcher>(
    writer: &mut W,
    now_ms: i64,
    tenant: &str,
    queue: &str,
    task_id: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
    relative_attempt_number: u32,
    task_group: &str,
) -> Result<(), ConcurrencyError> {
    let holder = HolderRecord {
        granted_at_ms: now_ms,
    };
    let holder_val = encode_holder(&holder);
    writer.put(concurrency_holder_key(tenant, queue, task_id), &holder_val)?;

    let task = Task::RunAttempt {
        id: task_id.to_string(),
        tenant: tenant.to_string(),
        job_id: job_id.to_string(),
        attempt_number,
        relative_attempt_number,
        held_queues: vec![queue.to_string()],
        task_group: task_group.to_string(),
    };
    let task_value = encode_task(&task);
    writer.put(
        task_key(task_group, start_time_ms, priority, job_id, attempt_number),
        &task_value,
    )?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_request_edits<W: WriteBatcher>(
    writer: &mut W,
    tenant: &str,
    queue: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt_number: u32,
    relative_attempt_number: u32,
    task_group: &str,
) -> Result<(), ConcurrencyError> {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority,
        job_id: job_id.to_string(),
        attempt_number,
        relative_attempt_number,
        task_group: task_group.to_string(),
    };
    let action_val = encode_concurrency_action(&action);
    let suffix = format!("{:08x}", rand::random::<u32>());
    let req_key = concurrency_request_key(
        tenant,
        queue,
        start_time_ms,
        priority,
        job_id,
        attempt_number,
        &suffix,
    );
    writer.put(&req_key, &action_val)?;

    // Increment the per-queue requester counter
    let counter_key = concurrency_requester_counter_key(tenant, queue);
    writer.merge(&counter_key, encode_counter(1))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_status_lookup_concurrency_matches_constant() {
        let mgr = ConcurrencyManager::new();
        assert_eq!(
            mgr.status_lookup_concurrency(),
            DEFAULT_STATUS_LOOKUP_CONCURRENCY,
        );
    }

    #[test]
    fn with_status_lookup_concurrency_stores_configured_value() {
        let mgr = ConcurrencyManager::with_status_lookup_concurrency(8);
        assert_eq!(mgr.status_lookup_concurrency(), 8);

        let mgr = ConcurrencyManager::with_status_lookup_concurrency(256);
        assert_eq!(mgr.status_lookup_concurrency(), 256);
    }

    #[test]
    fn status_lookup_concurrency_is_clamped_to_at_least_one() {
        // `buffered(0)` would deadlock the grant scanner — clamp invalid input.
        let mgr = ConcurrencyManager::with_status_lookup_concurrency(0);
        assert_eq!(mgr.status_lookup_concurrency(), 1);
    }
}
