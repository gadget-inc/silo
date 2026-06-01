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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::{BoxFuture, Shared};

use slatedb::WriteBatch;
use slatedb::config::WriteOptions;

use crate::instrumented_db::InstrumentedDb;

use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::helpers::WriteBatcher;

use crate::codec::{
    decode_concurrency_action, decode_floating_limit_state, encode_concurrency_action,
    encode_holder, encode_task,
};
use crate::dst_events::{self, DstEvent};
use crate::job::{ConcurrencyLimit, JobStatusKind, JobView, Limit};
use crate::job_store_shard::helpers::decode_job_status_owned;
use crate::job_store_shard::limit_processing_order;
use crate::keys::{
    concurrency_counts_key, concurrency_holder_key, concurrency_holders_prefix,
    concurrency_holders_queue_prefix, concurrency_request_key, concurrency_request_prefix,
    concurrency_requester_counter_key, concurrency_requests_prefix, end_bound,
    floating_limit_state_key, job_info_key, job_status_key, parse_concurrency_holder_key,
    parse_concurrency_request_key, task_key,
};
use crate::metrics::Metrics;
use crate::shard_range::ShardRange;
use crate::task::{ConcurrencyAction, HolderRecord, Task};
use crate::task_broker::TaskBrokerRegistry;

/// Error type for concurrency operations that can fail due to storage or encoding errors.
///
/// `Slate` holds `Arc<slatedb::Error>` because `slatedb::Error` is not `Clone`,
/// and singleflighted hydration must hand the same error to multiple awaiters via
/// `Shared<...>::Output`. Holding the `Arc` directly preserves `kind()` so downstream
/// retry logic (e.g. `retry_on_txn_conflict`) keeps working.
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("{0}")]
    Slate(Arc<slatedb::Error>),
    #[error("encoding error: {0}")]
    Encoding(String),
    #[error("shard shutting down, aborting chain resume")]
    ShardShuttingDown,
    #[error("limit chain resume failed: {0}")]
    ChainResume(String),
}

/// Parameters passed to a [`LimitChainResumer`] to continue a job's limit chain
/// after a deferred concurrency grant has been awarded.
///
/// The just-won queue is already in `held_queues` (the caller appends it
/// before invoking the resumer) and `limit_index` already points at the
/// *next* limit to evaluate. The resumer simply walks the remaining limits.
///
/// `limits` is the full limits list, supplied here so the resumer can walk
/// the chain without a separate JobInfo fetch — the deferred request/ticket
/// carries this list on the wire.
#[derive(Debug, Clone)]
pub struct ResumeChainParams {
    pub tenant: String,
    pub task_id: String,
    pub job_id: String,
    pub attempt_number: u32,
    pub relative_attempt_number: u32,
    /// Index into `limit_processing_order(limits)` for the next limit to
    /// evaluate. May equal `limits.len()` (in canonical order) — meaning the
    /// chain is complete and the resumer should write a `RunAttempt`.
    pub limit_index: u32,
    pub priority: u8,
    pub start_at_ms: i64,
    pub held_queues: Vec<String>,
    pub task_group: String,
    pub limits: Vec<Limit>,
    /// The scanner's `now_ms`, threaded through verbatim so the resumer's
    /// `task_key(now_ms, …)` writes can't disagree with the `granted_at_ms`
    /// the scanner just wrote into the same batch.
    pub now_ms: i64,
}

/// Bridge between the grant scanner / `process_grants` and the shard-level
/// limit-chain walker (`JobStoreShard::enqueue_limit_task_at_index`).
///
/// Implemented on the shard side and installed via
/// [`ConcurrencyManager::set_chain_resumer`] after the shard `Arc<Self>` is
/// created. The implementation holds a `Weak<JobStoreShard>` so this trait
/// object does not form a strong reference cycle with the manager.
#[async_trait::async_trait]
pub trait LimitChainResumer: Send + Sync {
    /// Append edits to `batch` that continue the limit chain from
    /// `params.limit_index`. Returns the `(queue, task_id)` pairs of any
    /// additional in-memory concurrency reservations the chain made (so the
    /// caller can roll them back if the batch write fails).
    async fn resume_chain(
        &self,
        batch: &mut slatedb::WriteBatch,
        params: ResumeChainParams,
    ) -> Result<Vec<(String, String)>, ConcurrencyError>;
}

impl From<slatedb::Error> for ConcurrencyError {
    fn from(e: slatedb::Error) -> Self {
        ConcurrencyError::Slate(Arc::new(e))
    }
}

impl From<Arc<slatedb::Error>> for ConcurrencyError {
    fn from(e: Arc<slatedb::Error>) -> Self {
        ConcurrencyError::Slate(e)
    }
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

/// Max in-flight `db.get(job_status_key)` lookups during a single
/// `process_grants` pass. Caps slatedb-side block-fetch fan-out so the
/// scanner can't pin unbounded `Bytes` while validating a large pending
/// backlog. Mirrors the `CONCURRENCY = 64` pattern used in
/// `job_store_shard::get_jobs_status_batch` for the same reason.
const STATUS_LOOKUP_CONCURRENCY: usize = 64;

/// Max grants written in a single `process_grants` pass. A single
/// `request_grant_count` accumulation can be arbitrarily large (every
/// release between scanner wakeups adds to it), and without a per-pass
/// cap the scanner would materialize `count` `ScannedRequest`s, issue
/// `count` buffered status gets, and accumulate `count` edits in a
/// single `WriteBatch` before committing. Capping per pass bounds peak
/// memory; the outer loop iterates as many passes as needed to drain
/// the requested count.
const MAX_GRANTS_PER_PASS: usize = 256;

fn concurrency_queue_for_limit(limit: &Limit) -> Option<&str> {
    match limit {
        Limit::Concurrency(cl) => Some(&cl.key),
        Limit::FloatingConcurrency(fl) => Some(&fl.key),
        Limit::RateLimit(_) => None,
    }
}

fn limit_index_matches_queue(limits: &[Limit], limit_index: u32, queue: &str) -> bool {
    let order = limit_processing_order(limits);
    order
        .get(limit_index as usize)
        .and_then(|&idx| concurrency_queue_for_limit(&limits[idx]))
        == Some(queue)
}

fn canonical_limit_index_for_queue(limits: &[Limit], queue: &str) -> Option<u32> {
    let order = limit_processing_order(limits);
    order
        .iter()
        .position(|&idx| concurrency_queue_for_limit(&limits[idx]) == Some(queue))
        .map(|idx| idx as u32)
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

/// Shared future driving a one-shot hydration scan for a queue.
///
/// Concurrent first-touch callers all observe `Hydrating(_)` and `.await` the same
/// `Shared` clone, so the underlying scan runs exactly once. The output is wrapped
/// in `Arc` because `slatedb::Error` is not `Clone`, and `Shared::Output` must be.
type HydrateFut = Shared<BoxFuture<'static, Result<(), Arc<slatedb::Error>>>>;

enum HydrationState {
    /// Never scanned (or last scan errored).
    NotHydrated,
    /// A scan is in flight; awaiters get the result from this shared future.
    Hydrating(HydrateFut),
    /// Holders set reflects the latest durable state for this shard's range.
    Hydrated,
}

/// Per-queue state: hydration status and in-memory holder set.
///
/// The hydration future writes into `holders` (via the same map lock) before
/// flipping `state` to `Hydrated`. Concurrent `try_reserve_internal` callers may
/// also write to `holders` while the hydration future is still running; both
/// paths take the same map lock so mutations are serialized, and the
/// hydration `insert` is idempotent for any task_id.
struct QueueEntry {
    state: HydrationState,
    holders: HashSet<String>,
}

impl QueueEntry {
    fn new() -> Self {
        Self {
            state: HydrationState::NotHydrated,
            holders: HashSet::new(),
        }
    }
}

/// In-memory key for `ConcurrencyCounts.queues`. Two `Arc<str>` (tenant, queue)
/// instead of a storekey-encoded `Vec<u8>` so per-call lookups avoid the
/// storekey encode and the in-map keys are cheap to clone for spawned futures.
type QueueKey = (Arc<str>, Arc<str>);

fn queue_key(tenant: &str, queue: &str) -> QueueKey {
    (Arc::from(tenant), Arc::from(queue))
}

/// In-memory counts for concurrency holders.
///
/// Backed by a `DashMap` so each call serializes only on its shard's lock
/// rather than a single global mutex. Hydration is singleflighted: concurrent
/// first-touch callers share a single `Shared<BoxFuture>` rather than each
/// running its own scan.
///
/// **Locking discipline:** every DashMap guard (`entry`, `get`, `get_mut`)
/// MUST be dropped before any `.await`. Holding a guard across an await on
/// the same executor risks deadlocking the worker thread on its own shard
/// lock. All such scopes in this file are explicit `{ ... }` blocks.
pub struct ConcurrencyCounts {
    queues: Arc<DashMap<QueueKey, QueueEntry>>,
    /// When `true`, `ensure_hydrated` treats `NotHydrated` misses as
    /// already-hydrated empty queues — startup `hydrate_all` is expected to
    /// have already scanned every non-empty queue. When `false`, the
    /// singleflighted per-queue scan runs on miss.
    hydrate_all_at_startup: bool,
    /// (tenant, queue, task_id) tuples submitted by RAII guards on drop of a
    /// dequeue future, awaiting per-task_id reconciliation against the durable
    /// holder row. BTreeSet for deterministic iteration order (DST) and
    /// natural deduplication of repeated tuples.
    ///
    /// Mutex is held only for synchronous `insert` and `mem::take` — never
    /// across `.await`, matching the locking discipline at the top of this
    /// module.
    pending_reconciliations: Mutex<BTreeSet<(String, String, String)>>,
}

impl Default for ConcurrencyCounts {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrencyCounts {
    /// JIT-hydration ConcurrencyCounts. Used by unit tests that don't go
    /// through a real shard open; production code goes through
    /// [`ConcurrencyCounts::with_config`].
    pub fn new() -> Self {
        Self::with_config(false)
    }

    pub fn with_config(hydrate_all_at_startup: bool) -> Self {
        Self {
            queues: Arc::new(DashMap::new()),
            hydrate_all_at_startup,
            pending_reconciliations: Mutex::new(BTreeSet::new()),
        }
    }

    /// Eagerly hydrate every (tenant, queue) pair that has at least one
    /// in-range holder in durable storage. Called once at shard startup so
    /// that `try_reserve` and the grant scanner observe accurate capacity
    /// from the very first operation.
    ///
    /// Queues with zero holders are deliberately skipped — they don't appear
    /// in the scan, so they're never inserted into `holders` or
    /// `hydrated_queues`. The lazy `ensure_hydrated` path remains responsible
    /// for them: when such a queue is first encountered after startup, the
    /// per-queue scan correctly finds zero durable holders (see the
    /// `omittedQueuesAreSafe` assertion in specs/job_shard.als).
    ///
    /// The `range` parameter filters holders to only those for tenants within
    /// the shard's range. This is critical after shard splits — both child
    /// shards may see the same holder records in durable storage but only one
    /// of them owns each tenant.
    pub async fn hydrate_all(
        &self,
        db: &InstrumentedDb,
        range: &ShardRange,
    ) -> Result<(), slatedb::Error> {
        let started_at = std::time::Instant::now();
        let start = concurrency_holders_prefix();
        let end = end_bound(&start);
        let mut iter = db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await?;

        // Group task_ids by composite key. BTreeMap gives deterministic
        // iteration order for DST reproducibility.
        let mut grouped: BTreeMap<QueueKey, HashSet<String>> = BTreeMap::new();
        loop {
            let Some(kv) = iter.next().await? else { break };
            let Some(parsed) = parse_concurrency_holder_key(&kv.key) else {
                continue;
            };
            if !range.contains_tenant(&parsed.tenant) {
                tracing::debug!(
                    tenant = %parsed.tenant,
                    queue = %parsed.queue,
                    task = %parsed.task_id,
                    range = %range,
                    "skipping holder outside shard range during startup hydration"
                );
                continue;
            }
            let key = queue_key(&parsed.tenant, &parsed.queue);
            grouped.entry(key).or_default().insert(parsed.task_id);
        }

        let queue_count = grouped.len();
        let holder_count: usize = grouped.values().map(|s| s.len()).sum();

        // DashMap shards are per-key, so we acquire each entry's guard
        // individually and drop it at the end of the iteration. Called once
        // at startup before workers run, so no concurrent `ensure_hydrated`
        // can race here.
        for (key, set) in grouped {
            let mut entry = self.queues.entry(key).or_insert_with(QueueEntry::new);
            for task_id in set {
                entry.holders.insert(task_id);
            }
            entry.state = HydrationState::Hydrated;
        }

        tracing::info!(
            queues = queue_count,
            holders = holder_count,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "hydrated concurrency holders cache at startup"
        );
        Ok(())
    }

    /// Ensure a queue is hydrated before checking capacity.
    /// Called by try_reserve on first access to each queue.
    ///
    /// Fast path: if already hydrated, return immediately.
    /// Slow path: if a scan is in flight, await it; otherwise build a new
    /// scan future, store it on the entry, spawn a detached driver to keep
    /// it making progress regardless of caller cancellation, and await.
    ///
    /// When `hydrate_all_at_startup` is set, `hydrate_all` has already loaded every
    /// non-empty queue at startup, so a `NotHydrated` miss here is a queue
    /// with zero durable holders. We flip it to `Hydrated` in place rather
    /// than scanning durable storage again.
    pub async fn ensure_hydrated(
        &self,
        db: &Arc<InstrumentedDb>,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
    ) -> Result<(), Arc<slatedb::Error>> {
        let key = queue_key(tenant, queue);

        // Pick an action under the shard guard, then drop it before awaiting.
        let action: HydrateAction = {
            let mut entry = self
                .queues
                .entry(key.clone())
                .or_insert_with(QueueEntry::new);
            match &entry.state {
                HydrationState::Hydrated => HydrateAction::Done,
                HydrationState::Hydrating(fut) => HydrateAction::Wait(fut.clone()),
                HydrationState::NotHydrated if self.hydrate_all_at_startup => {
                    // Startup `hydrate_all` already scanned every non-empty
                    // queue, so a miss is an empty queue. Skip the per-queue
                    // scan and treat as hydrated.
                    entry.state = HydrationState::Hydrated;
                    HydrateAction::Done
                }
                HydrationState::NotHydrated => {
                    let fut = build_hydrate_future(
                        Arc::clone(&self.queues),
                        Arc::clone(db),
                        range.clone(),
                        tenant.to_string(),
                        queue.to_string(),
                        key.clone(),
                    )
                    .boxed()
                    .shared();
                    entry.state = HydrationState::Hydrating(fut.clone());
                    // Detach a driver so the scan completes even if every awaiting
                    // caller is cancelled. Dropping the JoinHandle is intentional.
                    let driver = fut.clone();
                    tokio::spawn(async move {
                        let _ = driver.await;
                    });
                    HydrateAction::Wait(fut)
                }
            }
        }; // shard guard dropped here

        match action {
            HydrateAction::Done => Ok(()),
            // The future itself resets state on error before resolving, so
            // awaiters don't touch state here — if multiple awaiters did,
            // a late one could clobber a *new* Hydrating(fut2) installed by
            // a concurrent caller after a peer awaiter reset to NotHydrated.
            HydrateAction::Wait(fut) => fut.await,
        }
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
        db: &Arc<InstrumentedDb>,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        task_id: &str,
        limit: usize,
        job_id: &str,
    ) -> Result<bool, ConcurrencyError> {
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
        let key = queue_key(tenant, queue);
        let (reserved, holders_len_after) = {
            let mut entry = self.queues.entry(key).or_insert_with(QueueEntry::new);

            // Check if we're at capacity
            if entry.holders.len() >= limit {
                return false;
            }

            // Reserve the slot atomically
            entry.holders.insert(task_id.to_string());
            (true, entry.holders.len())
        }; // shard guard dropped here

        if reserved {
            // Bench-debug: warn if a successful reserve lands above the limit.
            // The check inside the lock guarantees this can't happen normally;
            // a triggered warning indicates a logic bug (e.g. limit param
            // mismatch, hydration race).
            if holders_len_after > limit {
                tracing::warn!(
                    tenant = %tenant,
                    queue = %queue,
                    task_id = %task_id,
                    job_id = %job_id,
                    limit = limit,
                    holders_after = holders_len_after,
                    "try_reserve_internal: reserved past limit (potential bug)"
                );
            }
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
        let key = queue_key(tenant, queue);
        self.queues.entry(key).or_insert_with(QueueEntry::new).state = HydrationState::Hydrated;
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
        let key = queue_key(tenant, queue);
        if let Some(mut entry) = self.queues.get_mut(&key) {
            entry.holders.remove(task_id);
        } // shard guard dropped here
    }

    /// Atomically release a task without granting to another.
    /// Used by callers post-commit after deleting a holder from the DB batch.
    pub fn atomic_release(&self, tenant: &str, queue: &str, task_id: &str) {
        let key = queue_key(tenant, queue);
        {
            if let Some(mut entry) = self.queues.get_mut(&key) {
                entry.holders.remove(task_id);
            }
        } // shard guard dropped here

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
        let key = queue_key(tenant, queue);
        self.queues
            .get(&key)
            .map(|entry| entry.holders.len())
            .unwrap_or(0)
    }

    /// Whether this task_id is currently recorded as an in-memory holder for
    /// `(tenant, queue)`. Used by the reconciler to compare against the
    /// durable holder row.
    pub fn contains_holder(&self, tenant: &str, queue: &str, task_id: &str) -> bool {
        let key = queue_key(tenant, queue);
        self.queues
            .get(&key)
            .map(|entry| entry.holders.contains(task_id))
            .unwrap_or(false)
    }

    /// Insert an in-memory holder without going through `try_reserve` (no
    /// capacity check). Used by the reconciler when it observes a durable
    /// holder row with no matching in-memory entry — a possible outcome of
    /// the ambiguous "commit landed during cancellation" case on the
    /// grant-side guard, which the reconciler self-heals.
    pub fn insert_holder(&self, tenant: &str, queue: &str, task_id: &str) {
        let key = queue_key(tenant, queue);
        let mut entry = self.queues.entry(key).or_insert_with(QueueEntry::new);
        entry.holders.insert(task_id.to_string());
    }

    /// Push a `(tenant, queue, task_id)` tuple onto the per-task_id
    /// reconciliation queue. Idempotent: BTreeSet dedupes repeats.
    ///
    /// Called by `PendingGrantGuard::drop` and `PendingHolderReleaseGuard::drop`
    /// in `dequeue.rs` when a dequeue future is cancelled around its commit.
    /// The actual reconciliation runs on the periodic
    /// `spawn_concurrency_reconcile_task`, woken sub-tick via
    /// `ConcurrencyManager::request_reconciliation`.
    pub fn enqueue_reconciliation(&self, tenant: String, queue: String, task_id: String) {
        let mut p = self.pending_reconciliations.lock().unwrap();
        p.insert((tenant, queue, task_id));
    }

    /// Drain the pending reconciliation set. Synchronous — does not touch the
    /// `await_durable` path.
    fn drain_pending_reconciliations(&self) -> BTreeSet<(String, String, String)> {
        let mut p = self.pending_reconciliations.lock().unwrap();
        std::mem::take(&mut *p)
    }

    /// Count of pending reconciliation tuples. Used by metrics and a stuck-
    /// reconciler warn.
    pub fn pending_reconciliations_len(&self) -> usize {
        self.pending_reconciliations.lock().unwrap().len()
    }

    /// Snapshot every hydrated (tenant, queue) and its current in-memory
    /// holder count. Used by the reconciler tick to compute drift against
    /// the durable holder rows for each queue.
    ///
    /// `NotHydrated` and `Hydrating` queues are intentionally skipped: we
    /// haven't loaded their durable rows yet, so comparing in-memory (empty)
    /// against durable (unknown) is meaningless.
    pub fn hydrated_queue_holders_snapshot(&self) -> Vec<(String, String, usize)> {
        let mut out = Vec::new();
        for entry in self.queues.iter() {
            if !matches!(entry.state, HydrationState::Hydrated) {
                continue;
            }
            let (tenant_arc, queue_arc) = entry.key();
            out.push((
                tenant_arc.to_string(),
                queue_arc.to_string(),
                entry.holders.len(),
            ));
        }
        out
    }

    /// Snapshot the sizes of the in-memory holders cache for metrics reporting.
    pub fn cache_stats(&self) -> ConcurrencyCacheStats {
        let mut total_holders = 0usize;
        let mut hydrated_queue_count = 0usize;
        for entry in self.queues.iter() {
            total_holders += entry.holders.len();
            if matches!(entry.state, HydrationState::Hydrated) {
                hydrated_queue_count += 1;
            }
        }
        ConcurrencyCacheStats {
            total_holders,
            queue_count: self.queues.len(),
            hydrated_queue_count,
        }
    }
}

/// Snapshot of the in-memory concurrency holders cache for metrics reporting.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConcurrencyCacheStats {
    /// Total number of holder entries across all tracked queues.
    pub total_holders: usize,
    /// Number of (tenant, queue) pairs currently tracked in the holders map.
    pub queue_count: usize,
    /// Number of (tenant, queue) pairs that have been hydrated from durable storage.
    pub hydrated_queue_count: usize,
}

/// Local decision computed under the `queues` lock in `ensure_hydrated`,
/// returned out of the locked scope so the caller can `.await` without
/// holding the lock.
enum HydrateAction {
    Done,
    Wait(HydrateFut),
}

/// Build the future that scans `concurrency_holders_queue_prefix(tenant, queue)`
/// and, on success, merges results into the entry and flips state to `Hydrated`.
/// On error, resets the entry's state to `NotHydrated` so the next caller starts
/// a fresh scan; the future does this itself rather than letting awaiters do it,
/// so that we can't clobber a `Hydrating(fut2)` installed by a concurrent caller
/// after a peer awaiter reset to `NotHydrated`.
///
/// `'static` because it must be `.shared()` and spawned as a detached driver.
async fn build_hydrate_future(
    queues: Arc<DashMap<QueueKey, QueueEntry>>,
    db: Arc<InstrumentedDb>,
    range: ShardRange,
    tenant: String,
    queue: String,
    key: QueueKey,
) -> Result<(), Arc<slatedb::Error>> {
    let scan_result: Result<Vec<String>, Arc<slatedb::Error>> = async {
        let start = concurrency_holders_queue_prefix(&tenant, &queue);
        let end = end_bound(&start);
        let mut iter = db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await
            .map_err(Arc::new)?;

        let mut task_ids = Vec::new();
        loop {
            let maybe = iter.next().await.map_err(Arc::new)?;
            let Some(kv) = maybe else { break };

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
        Ok(task_ids)
    }
    .await;

    match scan_result {
        Ok(task_ids) => {
            // Merge scanned ids and flip state to Hydrated under a single shard guard.
            // Idempotent w.r.t. concurrent try_reserve_internal inserts.
            let mut entry = queues.entry(key).or_insert_with(QueueEntry::new);
            for tid in task_ids {
                entry.holders.insert(tid);
            }
            entry.state = HydrationState::Hydrated;
            Ok(())
        }
        Err(arc_err) => {
            // Reset state to NotHydrated so the next caller starts a fresh
            // future. Only this future can be the `Hydrating(_)` we installed:
            // success/error are the only state transitions, and `ensure_hydrated`
            // only installs a new future when state is `NotHydrated`. So if state
            // is still `Hydrating(_)` here, it is ours.
            if let Some(mut entry) = queues.get_mut(&key)
                && matches!(entry.state, HydrationState::Hydrating(_))
            {
                entry.state = HydrationState::NotHydrated;
            }
            Err(arc_err)
        }
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
    /// Name of the shard this manager belongs to. Used as the `shard` label
    /// on per-shard metrics emitted from the grant paths.
    shard: String,
    counts: ConcurrencyCounts,
    /// Pending grant counts per queue. Key: concurrency_counts_key(tenant, queue).
    /// Value: (tenant, queue, count). Lock held only briefly for increment/drain.
    /// BTreeMap ensures deterministic iteration order for DST reproducibility.
    pending_grants: Mutex<BTreeMap<Vec<u8>, (String, String, u32)>>,
    grant_notify: tokio::sync::Notify,
    grant_running: AtomicBool,
    /// Sub-tick wake for the periodic holder reconciler. Fired by
    /// `request_reconciliation` after a dequeue future is cancelled around
    /// its commit (via `PendingGrantGuard::drop` / `PendingHolderReleaseGuard::drop`).
    /// Sibling to `grant_notify` — separate notify because the grant scanner
    /// and the holder reconciler are distinct concerns (granting vs
    /// reconciling); the periodic-reconcile task selects on this directly.
    reconcile_notify: Arc<tokio::sync::Notify>,
    /// In-memory cache of resolved concurrency limits per queue.
    /// Key: concurrency_counts_key(tenant, queue). Populated during enqueue and grant_next.
    limit_cache: Mutex<HashMap<Vec<u8>, CachedQueueLimit>>,
    metrics: Option<Metrics>,
    /// Callback for resuming a job's limit chain after a deferred concurrency
    /// grant is awarded. Wired up by `JobStoreShard::open` after the shard
    /// `Arc<Self>` exists (so the impl can hold a `Weak<JobStoreShard>` without
    /// creating a strong cycle). The grant scanner and `process_grants` both
    /// invoke this so chain continuation logic lives in exactly one place
    /// (`JobStoreShard::enqueue_limit_task_at_index`).
    /// Chain resumer is installed exactly once at shard construction. Stored
    /// behind a `Mutex<Option<…>>` rather than a `OnceLock` so tests can
    /// `take_chain_resumer_for_test` to exercise the not-installed bailout
    /// path; production code calls `set_chain_resumer` once and never clears.
    chain_resumer: Mutex<Option<Arc<dyn LimitChainResumer>>>,
}

impl ConcurrencyManager {
    pub fn new(
        shard: impl Into<String>,
        metrics: Option<Metrics>,
        hydrate_all_at_startup: bool,
    ) -> Self {
        Self {
            shard: shard.into(),
            counts: ConcurrencyCounts::with_config(hydrate_all_at_startup),
            pending_grants: Mutex::new(BTreeMap::new()),
            grant_notify: tokio::sync::Notify::new(),
            grant_running: AtomicBool::new(false),
            reconcile_notify: Arc::new(tokio::sync::Notify::new()),
            limit_cache: Mutex::new(HashMap::new()),
            metrics,
            chain_resumer: Mutex::new(None),
        }
    }

    /// Install the limit-chain resumer. Must be called exactly once, after the
    /// owning `Arc<JobStoreShard>` exists, before the grant scanner can fire.
    pub fn set_chain_resumer(&self, resumer: Arc<dyn LimitChainResumer>) {
        *self.chain_resumer.lock().unwrap() = Some(resumer);
    }

    /// Test-only: drop the installed chain resumer to exercise the
    /// scanner's release-and-bail branch.
    pub fn take_chain_resumer_for_test(&self) -> Option<Arc<dyn LimitChainResumer>> {
        self.chain_resumer.lock().unwrap().take()
    }

    fn chain_resumer(&self) -> Option<Arc<dyn LimitChainResumer>> {
        self.chain_resumer.lock().unwrap().clone()
    }

    pub fn counts(&self) -> &ConcurrencyCounts {
        &self.counts
    }

    /// Snapshot the sizes of the in-memory concurrency caches for metrics reporting.
    pub fn cache_stats(&self) -> ConcurrencyCacheStats {
        self.counts.cache_stats()
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

    /// Resolve the gating queue's capacity from the persisted limits list.
    /// Public so dequeue and other shard paths can compute capacity without
    /// fetching JobInfo when the limits are already in hand (e.g., off a
    /// `RequestTicket` or `EnqueueTask` value).
    pub(crate) async fn capacity_for_queue(
        db: &InstrumentedDb,
        tenant: &str,
        queue: &str,
        limits: &[Limit],
    ) -> (usize, ConcurrencyLimitType) {
        Self::resolve_queue_capacity_from_limits(db, tenant, queue, limits).await
    }

    async fn resolve_queue_capacity_from_limits(
        db: &InstrumentedDb,
        tenant: &str,
        queue: &str,
        limits: &[Limit],
    ) -> (usize, ConcurrencyLimitType) {
        for limit in limits {
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
        db: &Arc<InstrumentedDb>,
        range: &ShardRange,
        writer: &mut W,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        priority: u8,
        // The job's user-requested start time. Drives the future-scheduling
        // decision (`scheduled_at_ms > now_ms` ⇒ write a `Task::RequestTicket`
        // at that future time) and is persisted as `start_time_ms` on any
        // request record / RequestTicket task so `process_grants` can later
        // skip future-dated requests.
        scheduled_at_ms: i64,
        // Time component for every `task_key` / `concurrency_request_key` this
        // call writes. For a fresh enqueue this equals `scheduled_at_ms`. For
        // a chain *resume* this is `now_ms`, so the resumed task can't
        // collide with a broker tombstone pinned by an earlier ack-delete at
        // `task_key(scheduled_at_ms, …)`. See
        // `project_broker_tombstone_chain_continuation`.
        task_key_start_ms: i64,
        now_ms: i64,
        limits: &[ConcurrencyLimit],
        task_group: &str,
        attempt_number: u32,
        relative_attempt_number: u32,
        skip_try_reserve: bool,
        // Position of this limit in the job's canonical limit order. Persisted
        // on the deferred request/ticket so `process_grants` (or the dequeue
        // path for future-scheduled tickets) can resume the chain at
        // `limit_index + 1` after this slot is granted.
        limit_index: u32,
        // Concurrency queues already held by earlier chain steps. Persisted
        // alongside `limit_index` so the resumed chain knows which holders
        // are already in place — without this, prior grants leak when this
        // request is later granted.
        held_queues: &[String],
        // Full limits list for the job. Persisted on the deferred request so
        // the chain can resume without re-fetching JobInfo.
        all_limits: &[Limit],
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
            if scheduled_at_ms > now_ms {
                // Future-scheduled: don't hold the slot until the job runs.
                // Release the in-memory reservation and fall through to the
                // FutureRequestTaskWritten branch, which writes a
                // `Task::RequestTicket` at `scheduled_at_ms`.
                // `handle_request_ticket` calls `try_reserve` again at
                // dequeue time, so the slot is taken when it's actually
                // about to be used. Without this rollback, a burst of
                // future-scheduled jobs into a free queue grabs every
                // holder and starves present-time work.
                self.rollback_grant(tenant, queue, task_id);
            } else {
                // Grant immediately: [SILO-ENQ-CONC-2] [SILO-ENQ-CONC-3] [SILO-IMP-CONC-2] [SILO-REIMP-CONC-2] create holder in DB queue.
                // The RunAttempt task itself is written exactly once by the chain
                // walker's terminal branch, carrying the full accumulated
                // `held_queues`.
                // Note: in-memory slot is already reserved by try_reserve
                append_grant_edits(writer, now_ms, tenant, queue, task_id)?;
                if let Some(ref m) = self.metrics {
                    m.record_concurrency_tickets_granted(
                        &self.shard,
                        crate::metrics::GrantPath::Immediate,
                        1,
                    );
                }
                return Ok(Some(RequestTicketOutcome::GrantedImmediately {
                    task_id: task_id.to_string(),
                    queue: queue.clone(),
                }));
            }
        }
        if scheduled_at_ms <= now_ms {
            // [SILO-ENQ-CONC-4] [SILO-IMP-CONC-3] [SILO-REIMP-CONC-3] Queue is at capacity
            // [SILO-ENQ-CONC-5] [SILO-IMP-CONC-4] [SILO-REIMP-CONC-4] No task in DB queue, request created
            // [SILO-ENQ-CONC-6] Create request record instead
            append_request_edits(
                writer,
                tenant,
                queue,
                scheduled_at_ms,
                priority,
                job_id,
                attempt_number,
                relative_attempt_number,
                task_group,
                limit_index,
                held_queues,
                task_id,
                all_limits,
            )?;
            Ok(Some(RequestTicketOutcome::TicketRequested {
                queue: queue.clone(),
            }))
        } else {
            // Job scheduled for future: queue as RequestTicket task. The
            // chain's `task_id` is persisted so when the ticket is granted
            // at dequeue time, the holder it creates joins the same task_id
            // as any prior holders accumulated by the chain (avoiding the
            // task_id mismatch that used to orphan earlier grants).
            //
            // For a future-scheduled enqueue the broker must see this task
            // at the user-requested time, so `task_key_start_ms ==
            // scheduled_at_ms` on this branch. (No call site sets them apart
            // here — resume goes through the request branch above.) If a
            // future call site ever passes them apart, the persisted
            // `start_time_ms` and the `task_key` would disagree and the
            // broker would notice.
            debug_assert_eq!(
                scheduled_at_ms, task_key_start_ms,
                "future RequestTicket: task_key_start_ms must equal scheduled_at_ms"
            );
            let ticket = Task::RequestTicket {
                queue: queue.clone(),
                start_time_ms: scheduled_at_ms,
                priority,
                tenant: tenant.to_string(),
                job_id: job_id.to_string(),
                attempt_number,
                relative_attempt_number,
                task_id: task_id.to_string(),
                task_group: task_group.to_string(),
                limit_index,
                held_queues: held_queues.to_vec(),
                limits: all_limits.to_vec(),
            };
            let ticket_value = encode_task(&ticket);
            writer.put(
                task_key(
                    task_group,
                    task_key_start_ms,
                    priority,
                    job_id,
                    attempt_number,
                ),
                &ticket_value,
            )?;
            Ok(Some(RequestTicketOutcome::FutureRequestTaskWritten {
                queue: queue.clone(),
                task_id: task_id.to_string(),
            }))
        }
    }

    /// Rollback a grant that was made by `handle_enqueue` if the DB write fails.
    /// Call this with the queue and task_id from the GrantedImmediately outcome.
    pub fn rollback_grant(&self, tenant: &str, queue: &str, task_id: &str) {
        self.counts.release_reservation(tenant, queue, task_id);
    }

    /// Signal that a concurrency slot was freed for the given queue.
    /// Called by callers after releasing a holder (post-commit).
    /// Wakes the grant scanner to process pending requests.
    pub fn request_grant(&self, tenant: &str, queue: &str) {
        self.request_grant_count(tenant, queue, 1);
    }

    /// Clone-able handle to the reconcile-notify so the periodic reconcile
    /// task can `select!` on it without holding the manager.
    pub fn reconcile_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.reconcile_notify)
    }

    /// Queue a `(tenant, queue, task_id)` for the periodic reconciler to
    /// verify against the durable holder row. Wakes the reconciler sub-tick.
    ///
    /// Called by `PendingGrantGuard::drop` and `PendingHolderReleaseGuard::drop`
    /// in `dequeue.rs` after a dequeue future is cancelled around its commit
    /// — the only place where in-memory and durable holder state can drift.
    pub fn request_reconciliation(&self, tenant: String, queue: String, task_id: String) {
        self.counts.enqueue_reconciliation(tenant, queue, task_id);
        self.reconcile_notify.notify_one();
    }

    /// Drain the pending reconciliation set and reconcile each
    /// `(tenant, queue, task_id)` against the durable holder row.
    ///
    /// Per-task_id, idempotent. Resolves the ambiguity of "cancellation mid
    /// commit-await may or may not have applied to the WAL" deterministically:
    ///
    /// | durable | in-memory | action                                   |
    /// |---------|-----------|------------------------------------------|
    /// | Y       | Y         | no-op (consistent)                       |
    /// | N       | N         | no-op (consistent)                       |
    /// | Y       | N         | `insert_holder` (mirror case self-heal)  |
    /// | N       | Y         | `atomic_release` + `request_grant` (ghost) |
    ///
    /// On a transient DB error, the failing tuple is re-queued so the next
    /// tick (or sub-tick wake) retries it.
    pub async fn reconcile_pending_holders(&self, db: &Arc<InstrumentedDb>, range: &ShardRange) {
        let pending = self.counts.drain_pending_reconciliations();
        if pending.is_empty() {
            return;
        }

        // Group by (tenant, queue) so each queue is hydrated at most once
        // per pass.
        let mut by_queue: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for (tenant, queue, task_id) in pending {
            by_queue.entry((tenant, queue)).or_default().push(task_id);
        }

        for ((tenant, queue), task_ids) in by_queue {
            // Skip tenants outside this shard's range — they may have been
            // queued before a shard split.
            if !range.contains_tenant(&tenant) {
                continue;
            }

            if let Err(e) = self
                .counts
                .ensure_hydrated(db, range, &tenant, &queue)
                .await
            {
                tracing::warn!(
                    error = %e,
                    tenant = %tenant,
                    queue = %queue,
                    "reconciler: hydrate failed; re-queueing for next tick"
                );
                for tid in task_ids {
                    self.counts
                        .enqueue_reconciliation(tenant.clone(), queue.clone(), tid);
                }
                continue;
            }

            let mut any_released = false;
            for task_id in task_ids {
                let key = concurrency_holder_key(&tenant, &queue, &task_id);
                let durable_present = match db.get(&key).await {
                    Ok(opt) => opt.is_some(),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            tenant = %tenant,
                            queue = %queue,
                            task_id = %task_id,
                            "reconciler: durable lookup failed; re-queueing"
                        );
                        self.counts
                            .enqueue_reconciliation(tenant.clone(), queue.clone(), task_id);
                        continue;
                    }
                };
                let in_mem_present = self.counts.contains_holder(&tenant, &queue, &task_id);

                match (durable_present, in_mem_present) {
                    (true, true) | (false, false) => {
                        // Consistent — nothing to do.
                    }
                    (true, false) => {
                        // Durable holder exists with no in-memory entry.
                        // Reachable if a grant-side guard's Drop fired after
                        // the commit landed but the rollback never ran (the
                        // mirror case of the ghost). Re-insert to match.
                        self.counts.insert_holder(&tenant, &queue, &task_id);
                        tracing::info!(
                            tenant = %tenant,
                            queue = %queue,
                            task_id = %task_id,
                            "reconciler: re-inserted in-memory holder to match durable"
                        );
                    }
                    (false, true) => {
                        // Ghost: durable holder is gone but the in-memory
                        // reservation survives. This is the wedge bug.
                        // Release and wake the grant scanner.
                        self.counts.atomic_release(&tenant, &queue, &task_id);
                        any_released = true;
                        tracing::info!(
                            tenant = %tenant,
                            queue = %queue,
                            task_id = %task_id,
                            "reconciler: released ghost in-memory holder (no durable row)"
                        );
                    }
                }
            }
            if any_released {
                // One grant kick per queue that saw a release.
                self.request_grant(&tenant, &queue);
            }
        }

        // Stuck-reconciler signal: if the pending set keeps growing across
        // ticks, something is wrong (slatedb unavailable, etc.).
        let remaining = self.counts.pending_reconciliations_len();
        if remaining > 10_000 {
            tracing::warn!(
                pending = remaining,
                "reconciler: pending_reconciliations growing — backlog suggests stuck reconciler"
            );
        }
    }

    /// Walk every hydrated (tenant, queue) in this shard, compare the
    /// in-memory holder count to the durable holder count (one ranged scan
    /// per queue), and publish:
    ///
    /// * `silo_concurrency_holder_drift` — Σ |in_mem - durable| across hydrated queues
    /// * `silo_concurrency_reconciliation_pending` — current size of the
    ///   per-task_id reconciliation queue (stuck-reconciler signal)
    ///
    /// Called from the periodic reconciler's tick (NOT from the reactive
    /// sub-tick wake) — keeps the cost amortized at the deployment-configured
    /// `concurrency_reconcile_interval` rather than firing on every dropped
    /// dequeue future. Noop if metrics are disabled.
    pub async fn report_holder_drift(&self, db: &Arc<InstrumentedDb>, range: &ShardRange) {
        let Some(metrics) = self.metrics.as_ref() else {
            // Always publish the pending gauge if metrics are present; if not, skip everything.
            return;
        };

        let mut drift_total: u64 = 0;
        for (tenant, queue, in_mem_count) in self.counts.hydrated_queue_holders_snapshot() {
            if !range.contains_tenant(&tenant) {
                continue;
            }
            // Per-queue ranged scan. Cost dominated by holder count; capped
            // by the deployment's reconcile interval.
            let start = crate::keys::concurrency_holders_queue_prefix(&tenant, &queue);
            let end = crate::keys::end_bound(&start);
            let mut iter = match db
                .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
                .await
            {
                Ok(i) => i,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        tenant = %tenant,
                        queue = %queue,
                        "report_holder_drift: scan failed; skipping queue"
                    );
                    continue;
                }
            };
            let mut durable_count: usize = 0;
            loop {
                match iter.next().await {
                    Ok(Some(_)) => durable_count += 1,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            tenant = %tenant,
                            queue = %queue,
                            "report_holder_drift: iter error; partial count"
                        );
                        break;
                    }
                }
            }
            drift_total += (in_mem_count as i64 - durable_count as i64).unsigned_abs();
        }

        let pending = self.counts.pending_reconciliations_len() as u64;
        metrics.set_concurrency_reconciler_signals(&self.shard, drift_total, pending);
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
        db: Arc<InstrumentedDb>,
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
    pub async fn reconcile_pending_requests(&self, db: &InstrumentedDb, range: &ShardRange) {
        let start = concurrency_requests_prefix();
        let end = end_bound(&start);
        let mut iter = match db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options_uncached())
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
        db: &Arc<InstrumentedDb>,
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
            /// The chain's task identifier (from the stored EnqueueTask value,
            /// NOT the random suffix in the request key). Used as the holder
            /// key and carried forward as the resumed chain's task_id so prior
            /// holders accumulated under the same id stay reachable.
            task_id: String,
            job_id: String,
            attempt_number: u32,
            relative_attempt_number: u32,
            start_time_ms: i64,
            priority: u8,
            task_group: String,
            /// Position of the gating limit (this queue) in the job's canonical
            /// order. Resuming the chain starts at `limit_index + 1`.
            limit_index: u32,
            /// Holders already in place from earlier chain steps. The resumed
            /// chain appends this queue and continues from there.
            held_queues: Vec<String>,
            /// Full limits list, copied from the EnqueueTask value. Saves a
            /// JobInfo fetch when computing max_concurrency and resuming the
            /// chain.
            limits: Vec<Limit>,
        }

        let mut max_concurrency: Option<(usize, ConcurrencyLimitType)> = None;
        let mut all_granted_groups: Vec<String> = Vec::new();
        let mut total_granted: usize = 0;
        let mut iter_exhausted = false;
        let mut capacity_exhausted = false;
        let chain_resumer = self.chain_resumer();

        // Scan→validate→grant loop: keeps pulling from the iterator until we've
        // granted `count` requests, or hit the end / capacity limit. Each pass
        // scans up to `MAX_GRANTS_PER_PASS` candidates, validates them concurrently,
        // reserves slots for valid ones, and commits the batch. Stale/corrupt entries
        // are cleaned up along the way. Bounding per-pass scan size caps peak memory
        // (the scanned Vec, buffered status results, and WriteBatch all scale with it),
        // so a large accumulated `count` is drained over multiple bounded passes
        // rather than one unbounded one.
        while total_granted < count as usize && !iter_exhausted && !capacity_exhausted {
            let needed = (count as usize - total_granted).min(MAX_GRANTS_PER_PASS);

            let mut batch = WriteBatch::new();
            let mut grants: Vec<(String, String)> = Vec::new();
            let mut stale_and_corrupt_count: usize = 0;

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

                let Some(parsed_req) = parse_concurrency_request_key(&kv.key) else {
                    tracing::warn!(queue = %queue, "grant scanner: malformed request key, deleting");
                    batch.delete(&kv.key);
                    stale_and_corrupt_count += 1;
                    continue;
                };

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

                // The request key is authoritative for scheduler ordering and
                // storage identity. Legacy values written before task_id/limits
                // were added still have these fields in the key.
                let start_time_ms = parsed_req.start_time_ms as i64;
                let job_id_str = parsed_req.job_id.as_str();
                let attempt_number = parsed_req.attempt_number;
                let priority = parsed_req.priority;

                if start_time_ms > now_ms {
                    // Concurrency request keys encode `start_time_ms` ahead of
                    // the per-job suffix, so the scan iterates in ascending
                    // start-time order. The first entry whose start_time_ms
                    // exceeds now_ms guarantees every subsequent entry in the
                    // scan also exceeds now_ms — breaking here is safe and
                    // cannot skip an already-ready request.
                    iter_exhausted = true;
                    break;
                }

                let task_id_str = match et.task_id() {
                    Some(task_id) if !task_id.is_empty() => task_id.to_string(),
                    _ => parsed_req.request_id(),
                };
                let mut held_queues: Vec<String> = et
                    .held_queues()
                    .map(|v| v.iter().map(|s| s.to_string()).collect())
                    .unwrap_or_default();
                let mut limits = crate::codec::limit_entries_to_owned(et.limits());
                let mut task_group = et.task_group().unwrap_or_default().to_string();

                if limits.is_empty() || task_group.is_empty() {
                    match db.get(&job_info_key(tenant, job_id_str)).await {
                        Ok(Some(job_raw)) => match JobView::new(job_raw) {
                            Ok(job_view) => {
                                if limits.is_empty() {
                                    limits = job_view.limits();
                                }
                                if task_group.is_empty() {
                                    task_group = job_view.task_group().to_string();
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    queue = %queue,
                                    job_id = %job_id_str,
                                    error = %e,
                                    "grant scanner: request needs JobInfo fallback but JobInfo is unreadable; deleting"
                                );
                                batch.delete(&kv.key);
                                stale_and_corrupt_count += 1;
                                continue;
                            }
                        },
                        Ok(None) => {
                            tracing::warn!(
                                queue = %queue,
                                job_id = %job_id_str,
                                "grant scanner: request needs JobInfo fallback but JobInfo is missing; deleting"
                            );
                            batch.delete(&kv.key);
                            stale_and_corrupt_count += 1;
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(
                                queue = %queue,
                                job_id = %job_id_str,
                                error = %e,
                                "grant scanner: failed to load JobInfo fallback; skipping request this pass"
                            );
                            continue;
                        }
                    }
                }

                if limits.is_empty() {
                    tracing::warn!(
                        queue = %queue,
                        job_id = %job_id_str,
                        "grant scanner: request has no decoded limits; deleting"
                    );
                    batch.delete(&kv.key);
                    stale_and_corrupt_count += 1;
                    continue;
                }
                if task_group.is_empty() {
                    tracing::warn!(
                        queue = %queue,
                        job_id = %job_id_str,
                        "grant scanner: request has no task group after fallback; deleting"
                    );
                    batch.delete(&kv.key);
                    stale_and_corrupt_count += 1;
                    continue;
                }

                let raw_limit_index = et.limit_index();
                let limit_index = if limit_index_matches_queue(&limits, raw_limit_index, queue) {
                    raw_limit_index
                } else if let Some(idx) = canonical_limit_index_for_queue(&limits, queue) {
                    idx
                } else {
                    tracing::warn!(
                        queue = %queue,
                        job_id = %job_id_str,
                        "grant scanner: request queue is not in job limits; deleting"
                    );
                    batch.delete(&kv.key);
                    stale_and_corrupt_count += 1;
                    continue;
                };

                // If the value omitted `held_queues` (pre-unify-chain
                // schema) but the chain is gated at a later limit, upstream
                // concurrency slots may have been acquired under this
                // task_id. Probe each earlier concurrency queue and add any
                // matching holder to `held_queues` so completion / cancel
                // releases them. Pre-unify chains used a UUID task_id
                // distinct from `parsed_req.request_id()`, so the probe
                // won't reach those — they stay orphaned regardless. This
                // matters for any storage state where task_id consistency
                // was preserved but held_queues was dropped.
                if held_queues.is_empty() && limit_index > 0 {
                    let order = limit_processing_order(&limits);
                    let upstream_end = (limit_index as usize).min(order.len());
                    for &lim_idx in &order[..upstream_end] {
                        let Some(upstream_queue) = concurrency_queue_for_limit(&limits[lim_idx])
                        else {
                            continue;
                        };
                        let holder_key =
                            concurrency_holder_key(tenant, upstream_queue, &task_id_str);
                        match db.get(&holder_key).await {
                            Ok(Some(_)) => held_queues.push(upstream_queue.to_string()),
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(
                                    queue = %queue,
                                    upstream_queue = %upstream_queue,
                                    job_id = %job_id_str,
                                    error = %e,
                                    "grant scanner: upstream holder probe failed; held_queues may be incomplete"
                                );
                            }
                        }
                    }
                }

                // Resolve max_concurrency from the first request's limits
                // (always populated, per the check above).
                if max_concurrency.is_none() {
                    let (l, lt) =
                        Self::resolve_queue_capacity_from_limits(db, tenant, queue, &limits).await;
                    self.cache_queue_limit(tenant, queue, l as u32, lt);
                    max_concurrency = Some((l, lt));
                }

                scanned.push(ScannedRequest {
                    request_key: kv.key.to_vec(),
                    task_id: task_id_str,
                    job_id: job_id_str.to_string(),
                    attempt_number,
                    relative_attempt_number: {
                        let rel = et.relative_attempt_number();
                        if rel == 0 { attempt_number } else { rel }
                    },
                    start_time_ms,
                    priority,
                    task_group,
                    limit_index,
                    held_queues,
                    limits,
                });
            }

            // If the scan yielded no candidates, fall through to the per-pass
            // commit so any stale-delete edits accumulated above (malformed
            // entries) still get written, then let the outer while guard
            // (which checks `iter_exhausted`) terminate the loop.
            //
            // --- Batch validate status ---
            // Use `buffered` (order-preserving) rather than `join_all` so that
            // we cap in-flight slatedb reads at STATUS_LOOKUP_CONCURRENCY. Each
            // db.get() can fan out to many SST block fetches; with thousands of
            // pending requests, an unbounded join_all pinned multi-GB of Bytes.
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
            .buffered(STATUS_LOOKUP_CONCURRENCY)
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
            //
            // For each valid request we: reserve the in-memory slot for this
            // queue, write the holder + delete the request, then hand control
            // to the chain resumer to write the next limit step. The resumer
            // may make *additional* in-memory reservations (when a downstream
            // FloatingConcurrency/Concurrency limit grants immediately), so we
            // collect all per-pass reservations into `pass_reservations` for
            // rollback if the batch write fails.
            let limit = max_concurrency.map(|(l, _)| l).unwrap_or(1);
            // `(queue, task_id)` reservations to release if the per-pass batch
            // write fails. Includes both the just-won queue and any further
            // grants the chain resumer made.
            let mut pass_reservations: Vec<(String, String)> = Vec::new();
            for req in &valid_requests {
                if total_granted + grants.len() >= count as usize {
                    break;
                }

                // [SILO-GRANT-1] Pre: Queue has capacity — try to atomically reserve a slot
                if !self.counts.try_reserve_internal(
                    tenant,
                    queue,
                    &req.task_id,
                    limit,
                    &req.job_id,
                ) {
                    capacity_exhausted = true;
                    break;
                }
                pass_reservations.push((queue.to_string(), req.task_id.clone()));

                // [SILO-GRANT-3] Create holder for the just-won queue
                let holder_val = encode_holder(&HolderRecord {
                    granted_at_ms: now_ms,
                });
                batch.put(
                    concurrency_holder_key(tenant, queue, &req.task_id),
                    &holder_val,
                );
                batch.delete(&req.request_key);

                // [SILO-GRANT-4] Resume the limit chain. The chain walker writes
                // either the terminal `Task::RunAttempt` (if no more limits),
                // a `Task::CheckRateLimit` (if next is a rate limit), or a
                // fresh deferred concurrency request (if next concurrency
                // limit is full). It also accumulates further immediate
                // grants on downstream Concurrency/FloatingConcurrency limits
                // — those reservations are added to `pass_reservations`.
                //
                // If the chain resumer is unavailable (shard shutdown), abort
                // this request: release the just-acquired slot and break out
                // of the pass. The durable request key was already added to
                // `batch` as a delete; restore it by NOT committing this
                // pass's grants.
                let Some(ref resumer) = chain_resumer else {
                    tracing::warn!(
                        queue = %queue,
                        task_id = %req.task_id,
                        "grant scanner: chain resumer not installed; aborting pass"
                    );
                    // Release everything we reserved this pass and bail.
                    for (q, tid) in &pass_reservations {
                        self.counts.release_reservation(tenant, q, tid);
                    }
                    return all_granted_groups;
                };

                let mut new_held = req.held_queues.clone();
                new_held.push(queue.to_string());
                let resume_params = ResumeChainParams {
                    tenant: tenant.to_string(),
                    task_id: req.task_id.clone(),
                    job_id: req.job_id.clone(),
                    attempt_number: req.attempt_number,
                    relative_attempt_number: req.relative_attempt_number,
                    limit_index: req.limit_index + 1,
                    priority: req.priority,
                    start_at_ms: req.start_time_ms,
                    held_queues: new_held,
                    task_group: req.task_group.clone(),
                    limits: req.limits.clone(),
                    now_ms,
                };

                match resumer.resume_chain(&mut batch, resume_params).await {
                    Ok(chain_grants) => {
                        // Each (queue, task_id) the chain reserved must roll back
                        // alongside ours if the batch write fails.
                        pass_reservations.extend(chain_grants);
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            queue = %queue,
                            task_id = %req.task_id,
                            "grant scanner: chain resume failed; rolling back pass"
                        );
                        for (q, tid) in &pass_reservations {
                            self.counts.release_reservation(tenant, q, tid);
                        }
                        return all_granted_groups;
                    }
                }

                grants.push((req.task_id.clone(), req.task_group.clone()));
            }

            // --- Per-pass commit ---
            // Combined counter decrement for this pass's grants + stale/corrupt deletions.
            let pass_decrement = grants.len() + stale_and_corrupt_count;
            if pass_decrement > 0 {
                batch.merge(
                    concurrency_requester_counter_key(tenant, queue),
                    encode_counter(-(pass_decrement as i64)),
                );
            }

            // Skip the durable write if this pass produced no edits (e.g. every
            // scanned status lookup returned an error). The iterator has still
            // advanced, so the outer loop continues until iter_exhausted.
            if grants.is_empty() && stale_and_corrupt_count == 0 {
                continue;
            }

            if let Err(e) = db
                .write_with_options(
                    batch,
                    &WriteOptions {
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
            {
                // Roll back ALL reservations made this pass — both the gating
                // queue grants and any further reservations the chain resumer
                // made on downstream limits. Prior committed passes stand.
                for (q, tid) in &pass_reservations {
                    self.counts.release_reservation(tenant, q, tid);
                }
                tracing::warn!(
                    error = %e,
                    pass_grants = grants.len(),
                    pass_reservations = pass_reservations.len(),
                    prior_grants = total_granted,
                    "grant scanner: batch write failed, rolled back this pass's reservations"
                );
                return all_granted_groups;
            }

            for (request_id, task_group) in &grants {
                tracing::debug!(
                    queue = %queue,
                    request_id = %request_id,
                    task_group = %task_group,
                    "grant scanner: granted concurrency ticket"
                );
            }

            if let Some(ref m) = self.metrics {
                m.record_concurrency_tickets_granted(
                    &self.shard,
                    crate::metrics::GrantPath::Scanned,
                    grants.len() as u64,
                );
            }

            total_granted += grants.len();
            all_granted_groups.extend(grants.into_iter().map(|(_, tg)| tg));
        }

        all_granted_groups
    }
}

/// Append DB edits to grant a concurrency slot: creates only the holder
/// record. The `RunAttempt` `task_key` is written exactly once, by the chain
/// walker's terminal branch, with the *full* accumulated `held_queues`.
///
/// Writing an interim per-queue `RunAttempt` here would be redundant (it gets
/// overwritten by the terminal write, deleted by the Queued-branch cleanup,
/// or overwritten by `CheckRateLimit`), and worse, gives the broker scan a
/// chance to observe a `RunAttempt` whose `held_queues` is missing every
/// later-granted queue — which would under-release on completion.
///
/// Note: In-memory reservation should already be done via `try_reserve`
/// before calling this.
fn append_grant_edits<W: WriteBatcher>(
    writer: &mut W,
    now_ms: i64,
    tenant: &str,
    queue: &str,
    task_id: &str,
) -> Result<(), ConcurrencyError> {
    let holder = HolderRecord {
        granted_at_ms: now_ms,
    };
    let holder_val = encode_holder(&holder);
    writer.put(concurrency_holder_key(tenant, queue, task_id), &holder_val)?;
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
    limit_index: u32,
    held_queues: &[String],
    task_id: &str,
    all_limits: &[Limit],
) -> Result<(), ConcurrencyError> {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority,
        job_id: job_id.to_string(),
        attempt_number,
        relative_attempt_number,
        task_group: task_group.to_string(),
        limit_index,
        held_queues: held_queues.to_vec(),
        task_id: task_id.to_string(),
        limits: all_limits.to_vec(),
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
