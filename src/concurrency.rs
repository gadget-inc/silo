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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::{BoxFuture, Shared};

use slatedb::WriteBatch;
use slatedb::config::WriteOptions;

use crate::instrumented_db::InstrumentedDb;

use crate::job_store_shard::counters::{decode_counter, encode_counter};
use crate::job_store_shard::helpers::WriteBatcher;

use crate::codec::{
    decode_concurrency_action, decode_floating_limit_state, encode_concurrency_action,
    encode_holder, encode_task,
};
use crate::dst_events::{self, DstEvent};
use crate::job::{ConcurrencyLimit, JobStatusKind, JobView, Limit};
use crate::job_store_shard::helpers::decode_job_status_owned;
use crate::keys::{
    concurrency_counts_key, concurrency_holder_key, concurrency_holders_prefix,
    concurrency_holders_queue_prefix, concurrency_request_key, concurrency_request_prefix,
    concurrency_requester_counter_key, end_bound, floating_limit_state_key, job_info_key,
    job_status_key, parse_concurrency_holder_key, parse_concurrency_request_key,
    parse_concurrency_requester_counter_key, task_key,
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
    /// Index into the client-provided `limits` slice for the next limit to
    /// evaluate. May equal `limits.len()` — meaning the chain is complete and
    /// the resumer should write a `RunAttempt`.
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
    /// Prefetched point reads (key → value as read from the DB) for rows the
    /// chain walk is expected to read, e.g. downstream floating-limit state.
    /// The resumer's writer serves `get`s from this overlay, letting the
    /// scanner fetch many candidates' rows concurrently instead of paying one
    /// object-store round trip per grant inside the serial reserve loop.
    pub read_cache: Option<Arc<HashMap<Vec<u8>, slatedb::bytes::Bytes>>>,
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

    /// Wake the task brokers for `groups` after a durable grant commit, so
    /// freshly written tasks become claimable without waiting for the grant
    /// scanner's drain iteration to finish every other queue's pass. Default
    /// is a no-op for resumers with no broker access.
    fn wakeup_task_groups(&self, _groups: &[String]) {}
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

fn concurrency_queue_for_limit(limit: &Limit) -> Option<&str> {
    match limit {
        Limit::Concurrency(cl) => Some(&cl.key),
        Limit::FloatingConcurrency(fl) => Some(&fl.key),
        Limit::RateLimit(_) => None,
    }
}

fn limit_index_matches_queue(limits: &[Limit], limit_index: u32, queue: &str) -> bool {
    limits
        .get(limit_index as usize)
        .and_then(concurrency_queue_for_limit)
        == Some(queue)
}

fn canonical_limit_index_for_queue(limits: &[Limit], queue: &str) -> Option<u32> {
    limits
        .iter()
        .position(|limit| concurrency_queue_for_limit(limit) == Some(queue))
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

    /// Snapshot every hydrated (tenant, queue) key, SORTED. Used by the
    /// self-healing drift pass to build its per-pass work list. Sorted
    /// because DashMap iteration order is nondeterministic and must never
    /// reach DST-observable behavior (the drift pass's visit order). Unlike
    /// the old whole-cache snapshot this does NOT clone any holder sets —
    /// per-queue holders are fetched lazily via `holders_if_hydrated` as the
    /// sliced pass visits each queue.
    pub fn hydrated_queue_keys(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for entry in self.queues.iter() {
            if !matches!(entry.state, HydrationState::Hydrated) {
                continue;
            }
            let (tenant_arc, queue_arc) = entry.key();
            out.push((tenant_arc.to_string(), queue_arc.to_string()));
        }
        out.sort_unstable();
        out
    }

    /// Owned clone of one queue's in-memory holder task_ids, or `None` when
    /// the queue is absent or not (fully) hydrated — a queue that left the
    /// `Hydrated` state between the pass snapshot and its visit is skipped,
    /// matching the old whole-cache snapshot's filter: comparing in-memory
    /// (empty/partial) against durable (unknown) is meaningless. The DashMap
    /// guard is dropped before the caller does any `.await`.
    pub fn holders_if_hydrated(&self, tenant: &str, queue: &str) -> Option<HashSet<String>> {
        let key = queue_key(tenant, queue);
        let entry = self.queues.get(&key)?;
        if !matches!(entry.state, HydrationState::Hydrated) {
            return None;
        }
        Some(entry.holders.clone())
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

/// Tuning knobs for the background grant scanner, carried as one struct
/// through `OpenShardOptions` so call sites stay readable as knobs
/// accumulate. Raw values straight from settings; `ConcurrencyManager::new`
/// applies the clamps (`>= 1` where a zero would stall or panic, and the
/// `[0.0, 0.9]` fraction clamp).
#[derive(Debug, Clone)]
pub struct GrantScannerConfig {
    /// Max grants materialized, status-checked, and committed per
    /// `process_grants` pass.
    pub batch_size: usize,
    /// Max in-flight job-status lookups buffered within a single pass.
    pub buffer_size: usize,
    /// Max per-queue `process_grants` passes run concurrently per drain
    /// iteration.
    pub concurrency: usize,
    /// Max reconciler-sourced (cold-lane) queue entries merged into a single
    /// drain iteration alongside all event-driven (hot-lane) entries.
    pub cold_batch_size: usize,
    /// Minimum requester backlog on a candidate's next concurrency hop before
    /// the scanner defers granting the candidate while that hop is saturated.
    /// 0 disables the skip.
    pub next_hop_skip_min_backlog: u64,
    /// Fraction of each queue's capacity the scanner leaves unfilled for
    /// immediate (live) grants.
    pub live_headroom_fraction: f64,
    /// Max grants accumulated in a pass before their edits are committed and
    /// their task groups' brokers woken. Smaller chunks stream grants out of a
    /// long pass earlier (lower leasable latency); larger chunks amortize
    /// commit overhead.
    pub commit_chunk_size: usize,
}

impl Default for GrantScannerConfig {
    fn default() -> Self {
        Self {
            batch_size: crate::settings::DEFAULT_GRANT_SCANNER_BATCH_SIZE,
            buffer_size: crate::settings::DEFAULT_GRANT_SCANNER_BUFFER_SIZE,
            concurrency: crate::settings::DEFAULT_GRANT_SCANNER_CONCURRENCY,
            cold_batch_size: crate::settings::DEFAULT_GRANT_SCANNER_COLD_BATCH_SIZE,
            next_hop_skip_min_backlog:
                crate::settings::DEFAULT_GRANT_SCANNER_NEXT_HOP_SKIP_MIN_BACKLOG,
            live_headroom_fraction: crate::settings::DEFAULT_GRANT_SCANNER_LIVE_HEADROOM_FRACTION,
            commit_chunk_size: crate::settings::DEFAULT_GRANT_SCANNER_COMMIT_CHUNK_SIZE,
        }
    }
}

/// The grant scanner's cold pending lane: (tenant, queue) entries seeded by
/// the periodic `reconcile_pending_requests` sweep, drained in bounded
/// batches so a shard-wide sweep backlog can never delay servicing the hot
/// lane's event-driven nudges. `pop_cursor` implements round-robin draining —
/// see `take_pending_grant_batch` for why a plain `pop_first` would starve
/// the lexicographic tail.
#[derive(Default)]
struct ColdLane {
    /// Key: `concurrency_counts_key(tenant, queue)`; value: (tenant, queue,
    /// pending count). BTreeMap for deterministic iteration order (DST).
    entries: BTreeMap<Vec<u8>, (String, String, u32)>,
    /// The last key handed out by `take_pending_grant_batch`; the next batch
    /// resumes strictly after it and wraps to the front when the tail is
    /// exhausted.
    pop_cursor: Option<Vec<u8>>,
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
    /// HOT pending lane: grant counts per queue from event-driven callers
    /// (releases from dequeue/lease/cancel/import/drop-tenant, at-capacity
    /// ticket conversions, floating-limit raises, ghost releases). Drained in
    /// FULL every scanner iteration so a freed slot is refilled within one
    /// iteration rather than one shard-wide sweep. Key:
    /// concurrency_counts_key(tenant, queue). Value: (tenant, queue, count).
    /// Lock held only briefly for increment/drain. BTreeMap ensures
    /// deterministic iteration order for DST reproducibility.
    pending_grants: Mutex<BTreeMap<Vec<u8>, (String, String, u32)>>,
    /// COLD pending lane: entries seeded by the periodic
    /// `reconcile_pending_requests` counter sweep, drained at most
    /// `grant_scanner_cold_batch_size` per iteration behind the hot lane. On
    /// a queue-heavy shard the sweep re-seeds hundreds of thousands of
    /// entries; letting them share the hot lane meant a release nudge waited
    /// an entire shard-wide cycle (~40 min observed) to be serviced.
    pending_grants_cold: Mutex<ColdLane>,
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
    /// Resume cursor for sliced `reconcile_pending_requests` sweeps: the
    /// counter key the next sliced sweep starts scanning from. `None` starts
    /// at the front of the counter keyspace.
    reconcile_cursor: Mutex<Option<Vec<u8>>>,
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
    /// Max grants written in a single `process_grants` pass. A single
    /// `request_grant_count` accumulation can be arbitrarily large (every
    /// release between scanner wakeups adds to it), and without a per-pass
    /// cap the scanner would materialize `count` `ScannedRequest`s, issue
    /// `count` buffered status gets, and accumulate `count` edits in a
    /// single `WriteBatch` before committing. Capping per pass bounds peak
    /// memory; the outer loop iterates as many passes as needed to drain
    /// the requested count. Configurable via `grant_scanner_batch_size`
    /// (default 256); stored already clamped to `>= 1`.
    grant_scanner_batch_size: usize,
    /// Max in-flight `db.get(job_status_key)` lookups during a single
    /// `process_grants` pass. Caps slatedb-side block-fetch fan-out so the
    /// scanner can't pin unbounded `Bytes` while validating a large pending
    /// backlog. Mirrors the `CONCURRENCY = 64` pattern used in
    /// `job_store_shard::get_jobs_status_batch` for the same reason.
    /// Configurable via `grant_scanner_buffer_size` (default 64); stored
    /// already clamped to `>= 1`.
    grant_scanner_buffer_size: usize,
    /// Max number of per-queue `process_grants` passes run concurrently when
    /// draining `pending_grants`. Each entry in `work` is a unique
    /// `(tenant, queue)`, so no two in-flight passes share a gating queue; this
    /// bounds the `.buffer_unordered(...)` fan-out over those passes
    /// (completion-order, so one slow queue can't head-of-line block the rest)
    /// so peak memory stays `N × per-pass`. Configurable via
    /// `grant_scanner_concurrency` (default 8); stored already clamped to
    /// `>= 1` (`.buffer_unordered(0)` is invalid).
    grant_scanner_concurrency: usize,
    /// Max number of pending-request keys a single `process_grants` invocation
    /// will walk across all of its passes before committing what it found and
    /// returning — even if neither capacity nor the iterator is exhausted. This
    /// bounds the cost one queue can impose per scanner cycle so a single queue
    /// with a huge backlog of non-grantable keys (holder-orphan / stale-request
    /// shape) can't pin a `.buffer_unordered` slot and starve every other
    /// tenant's queue on the shard. The queue is re-driven on the next cycle (its requester
    /// counter is still non-zero), making bounded forward progress from the
    /// front each time. Derived from `grant_scanner_batch_size` (a small
    /// multiple) in `new`; always `>= grant_scanner_batch_size` so every
    /// invocation can run at least one full pass.
    grant_scanner_max_scan_per_invocation: usize,
    /// Max cold-lane (reconciler-sourced) queue entries merged into a single
    /// drain iteration alongside all hot-lane (event-driven) entries. Bounds
    /// how long a reconcile-sweep backlog can delay servicing a release
    /// nudge. Configurable via `grant_scanner_cold_batch_size` (default 64);
    /// stored already clamped to `>= 1`.
    grant_scanner_cold_batch_size: usize,
    /// Minimum requester backlog on a candidate's next concurrency hop before
    /// `process_grants` defers granting the candidate while that hop is
    /// saturated. 0 disables the next-hop skip. Configurable via
    /// `grant_scanner_next_hop_skip_min_backlog` (default 1024).
    grant_scanner_next_hop_skip_min_backlog: u64,
    /// Fraction of each queue's capacity the scanner leaves unfilled for
    /// immediate (live) grants. Configurable via
    /// `grant_scanner_live_headroom_fraction` (default 0.0); stored already
    /// clamped to `[0.0, 0.9]` with non-finite values treated as 0.0.
    grant_scanner_live_headroom_fraction: f64,
    /// Max grants accumulated in a `process_grants` pass before their edits
    /// are committed and their task groups' brokers woken. Configurable via
    /// `grant_scanner_commit_chunk_size` (default 16); stored already clamped
    /// to `>= 1`.
    grant_scanner_commit_chunk_size: usize,
    /// State of the (possibly multi-tick) holder-drift pass. Maintained only
    /// by `report_holder_drift` (a single periodic task). See `DriftPass`.
    drift_pass: Mutex<DriftPass>,
}

/// Memoized per-hop durable reads for `next_hop_should_skip`, keyed by the
/// next hop's queue name for the duration of one `process_grants` invocation.
/// The two point reads (floating state row, requester counter) are per-hop and
/// stable across candidates, so they are cached; but the capacity a Fixed hop
/// is COMPARED against is the candidate's OWN embedded limit, resolved per
/// candidate — so a stale low limit on an old parked row can no longer veto a
/// newer row whose embedded (raised) limit shows the hop has real headroom.
#[derive(Default)]
struct NextHopReads {
    /// Durable floating-state capacity (Floating hops only); filled on first read.
    floating_capacity: Option<usize>,
    /// Durable requester-counter depth; filled the first time a candidate
    /// finds this hop saturated.
    depth: Option<i64>,
}

/// A drift-detected ghost must be observed as a ghost in this many
/// *consecutive* `report_holder_drift` passes before it is enqueued for
/// release. A true ghost (a dropped-future leak) is permanent and survives
/// every pass; an in-flight reservation (`try_reserve_internal` inserts into
/// the in-memory holder set before its durable write commits) clears within
/// one pass — far shorter than a full sliced pass over all hydrated queues —
/// so a value of 2 excludes those false positives. Bump to debounce harder.
const GHOST_CONFIRM_PASSES: u32 = 2;

/// Accumulators for the holder-drift pass. A full pass visits every hydrated
/// queue in one sorted snapshot; with per-tick slicing
/// (`holder_drift_scan_slice`) the pass spans multiple reconcile ticks, so
/// its state lives here between ticks. The drift gauge and the
/// ghost-candidate promotion both advance only at FULL-pass boundaries,
/// preserving `GHOST_CONFIRM_PASSES` = two complete passes.
#[derive(Default)]
struct DriftPass {
    /// Queues still to visit this pass, in sorted order. Refilled from a
    /// fresh sorted snapshot when empty (= a new pass begins).
    work: VecDeque<(String, String)>,
    /// Ghost-candidate observation counts as of the last COMPLETED pass,
    /// keyed by (tenant, queue, task_id). Bounded by the ghost +
    /// in-flight-reservation count; self-prunes when a candidate gains a
    /// durable row or is released (absent from the next pass's set).
    prev_candidates: HashMap<(String, String, String), u32>,
    /// Ghost-candidate observation counts accumulated by the CURRENT pass.
    next_candidates: HashMap<(String, String, String), u32>,
    /// Σ |in_memory - durable| accumulated by the current pass; published to
    /// the drift gauge when the pass completes.
    drift_total: u64,
}

impl ConcurrencyManager {
    pub fn new(
        shard: impl Into<String>,
        metrics: Option<Metrics>,
        hydrate_all_at_startup: bool,
        scanner: GrantScannerConfig,
    ) -> Self {
        Self {
            shard: shard.into(),
            counts: ConcurrencyCounts::with_config(hydrate_all_at_startup),
            pending_grants: Mutex::new(BTreeMap::new()),
            pending_grants_cold: Mutex::new(ColdLane::default()),
            grant_notify: tokio::sync::Notify::new(),
            grant_running: AtomicBool::new(false),
            reconcile_notify: Arc::new(tokio::sync::Notify::new()),
            limit_cache: Mutex::new(HashMap::new()),
            reconcile_cursor: Mutex::new(None),
            metrics,
            chain_resumer: Mutex::new(None),
            // Clamp to >= 1: a 0 batch would stall the drain loop and
            // `.buffered(0)` / `.buffer_unordered(0)` are invalid.
            grant_scanner_batch_size: scanner.batch_size.max(1),
            grant_scanner_buffer_size: scanner.buffer_size.max(1),
            grant_scanner_concurrency: scanner.concurrency.max(1),
            // Cap total keys walked per invocation at a small multiple of the
            // per-pass batch so a giant non-grantable backlog yields after a
            // bounded walk and round-robins with other queues, while still
            // allowing several full passes of real draining per cycle. `×4` is
            // inherently `>= batch_size`, so at least one full pass always runs.
            grant_scanner_max_scan_per_invocation: scanner.batch_size.max(1).saturating_mul(4),
            grant_scanner_cold_batch_size: scanner.cold_batch_size.max(1),
            grant_scanner_next_hop_skip_min_backlog: scanner.next_hop_skip_min_backlog,
            // Non-finite fractions (NaN/inf from a hand-edited TOML) are
            // meaningless as a headroom share — treat them as disabled.
            grant_scanner_live_headroom_fraction: if scanner.live_headroom_fraction.is_finite() {
                scanner.live_headroom_fraction.clamp(0.0, 0.9)
            } else {
                0.0
            },
            grant_scanner_commit_chunk_size: scanner.commit_chunk_size.max(1),
            drift_pass: Mutex::new(DriftPass::default()),
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

    /// Look up the cached limit for a single (tenant, queue). Returns None if
    /// no enqueue/dequeue/refresh/grant pass has cached this queue's limit yet
    /// (e.g., freshly restarted shard).
    pub(crate) fn cached_queue_limit(&self, tenant: &str, queue: &str) -> Option<CachedQueueLimit> {
        let key = concurrency_counts_key(tenant, queue);
        self.limit_cache.lock().unwrap().get(&key).cloned()
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

    /// Point-read the durable floating-limit state row for (tenant, queue) and
    /// return its current max concurrency. None when the row is missing or
    /// unreadable — callers choose their own fallback (the scan path falls back
    /// to the limit's default_max_concurrency; the pre-scan gate treats it as
    /// unknown and lets the scan run).
    async fn floating_state_capacity(
        db: &InstrumentedDb,
        tenant: &str,
        queue: &str,
    ) -> Option<usize> {
        let state_key = floating_limit_state_key(tenant, queue);
        match db.get(&state_key).await {
            Ok(Some(raw)) => match decode_floating_limit_state(raw) {
                Ok(state) => Some(state.current_max_concurrency() as usize),
                Err(_) => None,
            },
            _ => None,
        }
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
                    let state_capacity = Self::floating_state_capacity(db, tenant, queue).await;
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

    /// Resolve the queue's current capacity WITHOUT scanning requests.
    /// Fixed limits come straight from the in-memory cache; Floating limits do
    /// one point read of the durable state row (the cached snapshot may be
    /// stale, and the durable row is what the scan path would gate on).
    /// Returns None when capacity cannot be cheaply determined — cache miss,
    /// or a Floating limit whose state row is missing/unreadable — in which
    /// case the caller must fall back to the scan, which resolves capacity
    /// from the request's embedded limits and re-populates the cache.
    async fn known_queue_capacity(
        &self,
        db: &InstrumentedDb,
        tenant: &str,
        queue: &str,
    ) -> Option<usize> {
        let cached = self.cached_queue_limit(tenant, queue)?;
        match cached.limit_type {
            ConcurrencyLimitType::Fixed => Some(cached.max_concurrency as usize),
            ConcurrencyLimitType::Floating => {
                Self::floating_state_capacity(db, tenant, queue).await
            }
        }
    }

    /// Capacity the SCANNER may fill for a queue: full capacity minus the
    /// configured live-headroom share, never reduced below 1 so a backlog
    /// always drains. Immediate paths (enqueue-time grants, chain-resume
    /// downstream grants, dequeue ticket conversion) keep the full capacity,
    /// so the reserved share stays available for fresh work while the
    /// scanner digs out a deferred backlog. With the default fraction of 0.0
    /// this is the identity. A zero capacity stays zero — the headroom knob
    /// must never grant slots a limit of 0 forbids.
    fn scanner_effective_capacity(&self, capacity: usize) -> usize {
        let fraction = self.grant_scanner_live_headroom_fraction;
        if fraction <= 0.0 || capacity == 0 {
            return capacity;
        }
        capacity
            .saturating_sub((capacity as f64 * fraction).ceil() as usize)
            .max(1)
    }

    /// Decide whether the grant scanner should defer a candidate because its
    /// NEXT concurrency hop is saturated with a deep requester backlog.
    ///
    /// The gate is the next CAPACITY-HOLDING hop after `limit_index`, found by
    /// walking forward THROUGH any RateLimit entries: a rate limit holds and
    /// releases no concurrency slot (the chain writes a `CheckRateLimit` task
    /// and proceeds still holding this queue's slot), so the hop that actually
    /// decides whether granting merely parks a holder is the next Concurrency /
    /// FloatingConcurrency limit, not the rate limit. A terminal candidate (no
    /// further concurrency hop) never skips (granting emits the RunAttempt). A
    /// Concurrency / FloatingConcurrency gate skips iff its holders >= this
    /// candidate's capacity AND its durable requester counter >=
    /// `grant_scanner_next_hop_skip_min_backlog` (0 disables the check).
    ///
    /// Inputs degrade safely: `holder_count` on an unhydrated next hop reads
    /// 0 and an unreadable counter reads 0, so missing data can only make the
    /// skip NOT fire — behavior falls back to granting as before. The two
    /// durable point reads (floating state, requester counter) are memoized in
    /// `cache` per next-hop queue for the invocation, but the holders-vs-capacity
    /// comparison is re-evaluated per candidate against that candidate's own
    /// embedded capacity (see `NextHopReads`).
    async fn next_hop_should_skip(
        &self,
        db: &Arc<InstrumentedDb>,
        tenant: &str,
        limits: &[Limit],
        limit_index: u32,
        cache: &mut HashMap<String, NextHopReads>,
    ) -> bool {
        if self.grant_scanner_next_hop_skip_min_backlog == 0 {
            return false;
        }
        // Look through rate limits to the next capacity-holding hop; a terminal
        // candidate (nothing, or only rate limits, after `limit_index`) never skips.
        let Some(next) = Self::next_capacity_hop(limits, limit_index) else {
            return false;
        };
        let next_queue = match next {
            Limit::Concurrency(cl) => cl.key.as_str(),
            Limit::FloatingConcurrency(fl) => fl.key.as_str(),
            // Unreachable: RateLimit is looked through above.
            Limit::RateLimit(_) => return false,
        };
        let reads = cache.entry(next_queue.to_string()).or_default();

        // This candidate's view of the hop capacity. Fixed: the embedded
        // per-request limit, resolved per candidate. Floating: the durable
        // state row (a per-hop value, memoized), falling back to the embedded
        // default when the row is missing/unreadable — mirroring
        // resolve_queue_capacity_from_limits.
        let capacity = match next {
            Limit::Concurrency(cl) => cl.max_concurrency as usize,
            Limit::FloatingConcurrency(fl) => {
                if reads.floating_capacity.is_none() {
                    reads.floating_capacity = Some(
                        Self::floating_state_capacity(db, tenant, next_queue)
                            .await
                            .unwrap_or(fl.default_max_concurrency as usize),
                    );
                }
                reads.floating_capacity.unwrap()
            }
            Limit::RateLimit(_) => return false,
        };
        if self.counts.holder_count(tenant, next_queue) < capacity {
            return false;
        }
        // Saturated for this candidate — consult the durable requester counter
        // (memoized per hop for the invocation).
        if reads.depth.is_none() {
            reads.depth = Some(
                match db
                    .get(&concurrency_requester_counter_key(tenant, next_queue))
                    .await
                {
                    Ok(Some(raw)) => decode_counter(&raw),
                    _ => 0,
                },
            );
        }
        // Saturating conversion: a config value above i64::MAX must not wrap to
        // a negative threshold (which would make the skip fire on any saturated
        // hop regardless of backlog depth).
        let min_backlog =
            i64::try_from(self.grant_scanner_next_hop_skip_min_backlog).unwrap_or(i64::MAX);
        reads.depth.unwrap() >= min_backlog
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
        // Write-time disambiguator for the trailing `epoch_ms` of any `task_key`
        // this call writes (only the future-`RequestTicket` branch writes one).
        // It does not affect broker ordering; it only keeps a rewritten task_key
        // distinct from the broker tombstone of a just-deleted predecessor. See
        // `project_broker_tombstone_chain_continuation`.
        task_key_epoch_ms: i64,
        now_ms: i64,
        limits: &[ConcurrencyLimit],
        task_group: &str,
        attempt_number: u32,
        relative_attempt_number: u32,
        skip_try_reserve: bool,
        // Index of this limit in the job's `limits` list. Persisted on the
        // deferred request/ticket so `process_grants` (or the dequeue path for
        // future-scheduled tickets) can resume the chain at `limit_index + 1`
        // after this slot is granted.
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
            // For a future-scheduled enqueue the broker must see this task at
            // the user-requested time: the `task_key` start component is
            // `scheduled_at_ms`, matching the persisted `start_time_ms` below.
            // `task_key_epoch_ms` is only the trailing disambiguator.
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
                    scheduled_at_ms,
                    priority,
                    job_id,
                    attempt_number,
                    task_key_epoch_ms,
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
    /// Reconcile each pending `(tenant, queue, task_id)` against its durable
    /// holder row. Returns `true` if any tuple had to be re-queued (hydrate
    /// or per-task `db.get` returned an error), so the caller knows to
    /// schedule a retry — see `spawn_holder_reconcile_task` for the
    /// backoff-and-renotify loop that consumes this signal.
    pub async fn reconcile_pending_holders(
        &self,
        db: &Arc<InstrumentedDb>,
        range: &ShardRange,
    ) -> bool {
        let pending = self.counts.drain_pending_reconciliations();
        if pending.is_empty() {
            return false;
        }
        let mut requeued = false;

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
                requeued = true;
                continue;
            }

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
                        requeued = true;
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
                        // Release and kick the grant scanner *per release*
                        // (mirroring `dequeue.rs` post-commit holder loop)
                        // so a reconcile pass that frees N ghosts grants N
                        // pending requests, not just 1 — `request_grant_count`
                        // accumulates in a single map entry and the grant
                        // scanner drains it in one pass with count = N.
                        self.counts.atomic_release(&tenant, &queue, &task_id);
                        self.request_grant(&tenant, &queue);
                        tracing::info!(
                            tenant = %tenant,
                            queue = %queue,
                            task_id = %task_id,
                            "reconciler: released ghost in-memory holder (no durable row)"
                        );
                    }
                }
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

        requeued
    }

    /// Walk every hydrated (tenant, queue) in this shard, compare the
    /// in-memory holder set to the durable holder set (one ranged scan per
    /// queue), and:
    ///
    /// 1. **Self-heal ghosts.** An in-memory holder with no durable row is a
    ///    ghost candidate (the wedge bug — an in-memory reservation that
    ///    survived after its durable holder was deleted). Candidates must be
    ///    observed in `GHOST_CONFIRM_PASSES` *consecutive* passes (tracked in
    ///    `ghost_candidates`) before being enqueued via `request_reconciliation`
    ///    / `reconcile_pending_holders`. This recovers ghosts regardless of how
    ///    they formed (e.g. a handler that releases in-memory state outside a
    ///    `PendingHolderReleaseGuard`).
    ///
    ///    **Two-stage false-positive guard.** The per-`task_id` durable
    ///    re-check in `reconcile_pending_holders` guards against the snapshot
    ///    racing a holder *delete* (the original ghost case). But it cannot
    ///    distinguish a true ghost from an in-flight reservation:
    ///    `process_grants` calls `try_reserve_internal` (which inserts into the
    ///    in-memory holder set) *before* its durable holder write commits, so a
    ///    just-reserved-not-yet-durable slot looks identical to a ghost to a
    ///    point-in-time `db.get`. Releasing it would over-grant the queue. The
    ///    consecutive-pass requirement closes that gap: an in-flight reservation
    ///    commits within milliseconds — far inside one `concurrency_reconcile_interval`
    ///    — so it never survives to a second pass, whereas a real ghost is
    ///    permanent and survives every pass.
    /// 2. **Publish metrics** (only when metrics are enabled):
    ///    * `silo_concurrency_holder_drift` — Σ |in_mem - durable| across hydrated queues
    ///    * `silo_concurrency_reconciliation_pending` — current size of the
    ///      per-task_id reconciliation queue (stuck-reconciler signal)
    ///
    /// Called from the periodic reconciler's tick (NOT from the reactive
    /// sub-tick wake) — keeps the cost amortized at the deployment-configured
    /// `concurrency_reconcile_interval` rather than firing on every dropped
    /// dequeue future. Self-healing runs even when metrics are disabled.
    ///
    /// `slice` bounds the queues scanned per call: a full pass over all
    /// hydrated queues spans `ceil(hydrated / slice)` ticks, with work,
    /// ghost-candidate counts, and the drift accumulator carried between
    /// ticks in `drift_pass`. On a queue-heavy shard this replaces the old
    /// behavior of issuing one durable ranged scan per hydrated queue —
    /// hundreds of thousands of scans — EVERY tick. The trade-off is drift
    /// gauge freshness and drift-detected ghost latency of ~one full pass;
    /// the reactive `request_reconciliation` path (sub-tick) is unaffected
    /// and remains the primary ghost-release path. Tests pass `usize::MAX`
    /// so one call is one full pass.
    pub async fn report_holder_drift(
        &self,
        db: &Arc<InstrumentedDb>,
        range: &ShardRange,
        slice: usize,
    ) {
        // Clamp to >= 1: a slice of 0 would `drain(..0)` nothing each tick, so
        // `pass.work` never empties, `pass_completes` is never true, and the
        // drift gauge / ghost confirmation would stall forever (never
        // self-healing a leaked holder). Honors the settings doc "values below
        // 1 are treated as 1".
        let slice = slice.max(1);
        // Pop this tick's batch under the lock (never held across awaits).
        // An empty work list means a new pass begins: take a fresh sorted
        // snapshot of the hydrated queues.
        let (batch, pass_completes) = {
            let mut pass = self.drift_pass.lock().unwrap();
            if pass.work.is_empty() {
                pass.work = self.counts.hydrated_queue_keys().into();
            }
            let take = slice.min(pass.work.len());
            let batch: Vec<(String, String)> = pass.work.drain(..take).collect();
            (batch, pass.work.is_empty())
        };

        let mut batch_drift: u64 = 0;
        let mut batch_ghosts: Vec<(String, String, String)> = Vec::new();
        for (tenant, queue) in &batch {
            if !range.contains_tenant(tenant) {
                continue;
            }
            // A queue that left the Hydrated state since the pass snapshot is
            // skipped — same filter the whole-cache snapshot applied.
            let Some(in_mem_holders) = self.counts.holders_if_hydrated(tenant, queue) else {
                continue;
            };
            // Per-queue ranged scan. Cost dominated by holder count; total
            // per tick bounded by `slice`.
            let start = crate::keys::concurrency_holders_queue_prefix(tenant, queue);
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
            let mut durable_ids: HashSet<String> = HashSet::new();
            loop {
                match iter.next().await {
                    Ok(Some(kv)) => {
                        if let Some(parsed) = parse_concurrency_holder_key(&kv.key) {
                            durable_ids.insert(parsed.task_id);
                        }
                    }
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

            batch_drift += (in_mem_holders.len() as i64 - durable_ids.len() as i64).unsigned_abs();

            // Ghost candidates: in-memory holders with no durable row.
            for task_id in in_mem_holders.difference(&durable_ids) {
                batch_ghosts.push((tenant.clone(), queue.clone(), task_id.clone()));
            }
        }

        // Fold the batch into the pass accumulators and promote confirmed
        // ghosts. Only enqueue a candidate for reconciliation once it has
        // survived `GHOST_CONFIRM_PASSES` consecutive FULL passes — an
        // in-flight reservation (durable write not yet committed) clears
        // long before the next pass revisits its queue, while a real ghost
        // is permanent. Candidates absent from a pass (durable row appeared,
        // or released) drop out at the pass-boundary swap. Confirmations are
        // collected under the lock and enqueued after it drops.
        let (to_reconcile, completed_drift) = {
            let mut pass = self.drift_pass.lock().unwrap();
            pass.drift_total = pass.drift_total.saturating_add(batch_drift);
            let mut to_reconcile = Vec::new();
            for key in batch_ghosts {
                let seen = pass
                    .prev_candidates
                    .get(&key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1);
                if seen >= GHOST_CONFIRM_PASSES {
                    to_reconcile.push(key.clone());
                }
                // Saturate at the threshold so a long-lived ghost the
                // reconciler keeps re-queueing doesn't grow the count
                // unboundedly; re-enqueuing each pass is harmless
                // (`pending_reconciliations` dedups and release is idempotent).
                pass.next_candidates
                    .insert(key, seen.min(GHOST_CONFIRM_PASSES));
            }
            let completed_drift = if pass_completes {
                let drift = pass.drift_total;
                pass.drift_total = 0;
                pass.prev_candidates = std::mem::take(&mut pass.next_candidates);
                Some(drift)
            } else {
                None
            };
            (to_reconcile, completed_drift)
        };
        for (tenant, queue, task_id) in to_reconcile {
            self.request_reconciliation(tenant, queue, task_id);
        }

        if let Some(metrics) = self.metrics.as_ref() {
            // Pending is a live queue size: publish every tick. Drift is a
            // pass-level aggregate: publish only when a full pass completes,
            // so the gauge never shows a partial sweep as "drift shrank".
            let pending = self.counts.pending_reconciliations_len() as u64;
            metrics.set_concurrency_reconciliation_pending(&self.shard, pending);
            if let Some(drift) = completed_drift {
                metrics.set_concurrency_holder_drift(&self.shard, drift);
            }
        }
    }

    pub(crate) fn request_grant_count(&self, tenant: &str, queue: &str, count: u32) {
        let key = concurrency_counts_key(tenant, queue);
        {
            let mut pending = self.pending_grants.lock().unwrap();
            let entry = pending
                .entry(key)
                .or_insert_with(|| (tenant.to_string(), queue.to_string(), 0));
            entry.2 = entry.2.saturating_add(count);
        }
        self.grant_notify.notify_one();
    }

    /// Cold-lane variant of `request_grant_count`, used only by the
    /// `reconcile_pending_requests` counter sweep.
    ///
    /// SET/max semantics rather than accumulation: the sweep re-reads the
    /// durable requester counter on every visit, so the counter value IS the
    /// outstanding demand. Accumulating it (the old shared-lane behavior)
    /// grew the pending count without bound while a queue went unserviced —
    /// u32 overflow in minutes for a queue with tens of millions of
    /// requesters.
    pub(crate) fn request_grant_count_cold(&self, tenant: &str, queue: &str, count: u32) {
        let key = concurrency_counts_key(tenant, queue);
        {
            let mut cold = self.pending_grants_cold.lock().unwrap();
            let entry = cold
                .entries
                .entry(key)
                .or_insert_with(|| (tenant.to_string(), queue.to_string(), 0));
            entry.2 = entry.2.max(count);
        }
        self.grant_notify.notify_one();
    }

    /// Compute one drain iteration's work set: ALL hot-lane entries plus at
    /// most `grant_scanner_cold_batch_size` cold-lane entries, merged with
    /// saturating counts on key collision.
    ///
    /// Cold entries are popped ROUND-ROBIN from `pop_cursor` (next key
    /// strictly after it, wrapping to the front) — NOT `pop_first`. The
    /// reconcile sweep re-seeds the cold lane continuously while pops proceed
    /// at the drain rate; whenever re-seeding outpaces popping, a
    /// global-minimum pop stays confined forever to the lexicographically
    /// smallest resident keys and the tail is never visited. The cursor
    /// bounds every resident entry's wait to one pop-rotation, independent of
    /// the insert pattern, and is deterministic for DST.
    ///
    /// Fully synchronous; never holds both lane locks at once.
    pub fn take_pending_grant_batch(&self) -> BTreeMap<Vec<u8>, (String, String, u32)> {
        let mut work = {
            let mut pending = self.pending_grants.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        let cold = &mut *self.pending_grants_cold.lock().unwrap();
        for _ in 0..self.grant_scanner_cold_batch_size {
            let next_key = cold
                .pop_cursor
                .as_deref()
                .and_then(|cursor| {
                    cold.entries
                        .range::<[u8], _>((
                            std::ops::Bound::Excluded(cursor),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                        .map(|(k, _)| k.clone())
                })
                // Cursor unset or tail exhausted: wrap to the front.
                .or_else(|| cold.entries.keys().next().cloned());
            let Some(key) = next_key else { break };
            let Some((tenant, queue, count)) = cold.entries.remove(&key) else {
                break;
            };
            match work.entry(key.clone()) {
                std::collections::btree_map::Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    entry.2 = entry.2.saturating_add(count);
                }
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    vacant.insert((tenant, queue, count));
                }
            }
            cold.pop_cursor = Some(key);
        }
        work
    }

    /// Test-only: peek the current pending-grant count for a queue, summed
    /// across the hot and cold lanes. Used by
    /// `reconcile_pending_holders_kicks_grant_per_release` to assert that
    /// the reconciler kicked the scanner once per ghost release rather than
    /// once per queue.
    pub fn pending_grant_count_for_test(&self, tenant: &str, queue: &str) -> u32 {
        self.hot_pending_grant_count_for_test(tenant, queue)
            .saturating_add(self.cold_pending_grant_count_for_test(tenant, queue))
    }

    /// Test-only: peek the hot-lane pending-grant count for a queue.
    #[doc(hidden)]
    pub fn hot_pending_grant_count_for_test(&self, tenant: &str, queue: &str) -> u32 {
        let key = concurrency_counts_key(tenant, queue);
        let pending = self.pending_grants.lock().unwrap();
        pending.get(&key).map(|(_, _, n)| *n).unwrap_or(0)
    }

    /// Test-only: peek the cold-lane pending-grant count for a queue.
    #[doc(hidden)]
    pub fn cold_pending_grant_count_for_test(&self, tenant: &str, queue: &str) -> u32 {
        let key = concurrency_counts_key(tenant, queue);
        let cold = self.pending_grants_cold.lock().unwrap();
        cold.entries.get(&key).map(|(_, _, n)| *n).unwrap_or(0)
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
            // Startup: one full sweep for existing pending requests, seeding
            // the cold lane so recovery work drains behind live nudges.
            mgr.reconcile_pending_requests(&db, &range, None).await;

            loop {
                mgr.grant_notify.notified().await;
                if !mgr.grant_running.load(Ordering::SeqCst) {
                    break;
                }

                // Inner loop: drain and process until no more pending work.
                // Each iteration takes ALL hot-lane entries plus a bounded
                // cold-lane batch, so a shard-wide reconcile-sweep backlog
                // (potentially hundreds of thousands of cold entries) can
                // delay a release-driven hot nudge by at most one iteration
                // instead of one full cycle over the whole backlog.
                loop {
                    // Re-check the stop flag per iteration: with a large cold
                    // backlog the inner loop can run for a long time, and
                    // `close()` must not leave the scanner granting against a
                    // closing DB. The AtomicBool (not select!/abort) is the
                    // cancellation boundary because `process_grants` is not
                    // cancellation-safe mid-pass — in-memory reservations are
                    // only rolled back on its explicit error paths.
                    if !mgr.grant_running.load(Ordering::SeqCst) {
                        break;
                    }
                    let work = mgr.take_pending_grant_batch();
                    if work.is_empty() {
                        break;
                    }
                    // Lane sizes AFTER the take: what remains queued. Hot is
                    // ~0 in steady state (drained fully every iteration);
                    // cold sawtooths with the periodic reconcile sweep.
                    if let Some(ref m) = mgr.metrics {
                        let hot = mgr.pending_grants.lock().unwrap().len();
                        let cold = mgr.pending_grants_cold.lock().unwrap().entries.len();
                        m.set_concurrency_pending_grants(&mgr.shard, hot, cold);
                    }

                    // Fan out across queues with bounded concurrency. Each
                    // (tenant, queue) is unique in `work`, so no two in-flight
                    // passes share a gating queue; downstream chain-resumer
                    // reservations are gated atomically by the per-key DashMap
                    // guard. `buffer_unordered` (completion-order) rather than
                    // `buffered` (submission-order): with `buffered`, one slow
                    // pass (a queue with a deep backlog) blocks yielding of
                    // every already-finished pass behind it, so completed
                    // futures pile up in the window and no new pass can start
                    // until the slow one finishes — head-of-line blocking that
                    // serializes other queues behind the busiest one. With
                    // `buffer_unordered`, a slow pass occupies exactly one slot
                    // while the remaining slots keep draining the other queues.
                    let granted: Vec<Vec<String>> = futures::stream::iter(work.into_values())
                        .map(|(tenant, queue, count)| {
                            let mgr = Arc::clone(&mgr);
                            let db = Arc::clone(&db);
                            let range = range.clone();
                            async move {
                                mgr.process_grants(&db, &range, &tenant, &queue, count)
                                    .await
                            }
                        })
                        .buffer_unordered(mgr.grant_scanner_concurrency)
                        .collect()
                        .await;
                    // Canonicalize: `buffer_unordered` collects in completion
                    // order, which depends on scheduling. Sorting (and deduping
                    // groups granted via multiple queues) makes the broker wake
                    // sequence a pure function of the granted *set*, keeping
                    // downstream behavior independent of interleaving for DST.
                    let mut granted_groups: Vec<String> = granted.into_iter().flatten().collect();
                    granted_groups.sort_unstable();
                    granted_groups.dedup();

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

    /// Scan the per-queue requester counters and trigger grants for each queue
    /// that has pending concurrency requests.
    ///
    /// This is used at grant-scanner startup and by the shard's periodic
    /// reconciliation task to self-heal from missed notifications.
    ///
    /// Rather than scanning the entire `CONCURRENCY_REQUEST` keyspace (tens of
    /// millions of keys on a busy shard) we iterate the
    /// `COUNTER_CONCURRENCY_REQUESTERS` counters, which are maintained
    /// co-committed with request creation (`append_request_edits`) and
    /// decremented on grant/cancel — a bounded set of ~one row per active
    /// (tenant, queue). The counter is a safe over-estimate: `process_grants`
    /// re-validates the real pending requests, so a stale-positive counter just
    /// wakes the scanner for a queue that grants nothing. It will not
    /// under-report newly created work because the increment lands in the same
    /// write batch as the request row.
    ///
    /// Discovered queues are seeded into the scanner's COLD lane so a
    /// shard-wide sweep can never delay event-driven (hot-lane) nudges.
    ///
    /// `max_keys` bounds the counter keys WALKED this call — grantable or
    /// not, including zero-valued and out-of-range rows, since the walk cost
    /// is what a tick must bound. `None` sweeps the whole keyspace in one
    /// call (scanner startup, diagnostics). `Some(n)` walks at most `n` keys
    /// resuming from the persistent cursor and saves the new resume point, so
    /// a shard with hundreds of thousands of counter rows spreads full
    /// coverage across ticks instead of re-scanning everything every 5s.
    /// Reaching the end of the keyspace resets the cursor to the front.
    pub async fn reconcile_pending_requests(
        &self,
        db: &InstrumentedDb,
        range: &ShardRange,
        max_keys: Option<usize>,
    ) {
        // Clamp a sliced budget to >= 1 (a full sweep stays `None`): a `Some(0)`
        // budget would break before walking any key every tick — seeding
        // nothing into the cold lane and resetting the cursor forever — so
        // deferred requests not carried by a hot nudge would never be
        // rediscovered. Honors the settings doc "values below 1 are treated as 1".
        let max_keys = max_keys.map(|budget| budget.max(1));
        let sweep_started = std::time::Instant::now();
        let prefix_start = vec![crate::keys::prefix::COUNTER_CONCURRENCY_REQUESTERS];
        let end = end_bound(&prefix_start);
        // Sliced sweeps resume from the saved cursor; full sweeps always
        // cover the whole keyspace from the front.
        let saved_cursor = self.reconcile_cursor.lock().unwrap().clone();
        let start = match saved_cursor {
            Some(cursor) if max_keys.is_some() => cursor,
            _ => prefix_start,
        };
        let mut iter = match db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options_uncached())
            .await
        {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "grant scanner: failed to scan requester counters during reconciliation"
                );
                return;
            }
        };

        let mut walked: usize = 0;
        let mut last_key: Option<Vec<u8>> = None;
        // Set when the sweep stops before cleanly reaching the end of the
        // keyspace — either the per-tick budget was hit or a scan error
        // interrupted iteration. Both resume the next sweep AFTER the last good
        // key rather than restarting the rotation at the front; only a clean
        // `Ok(None)` end resets to the front for a fresh rotation.
        let mut stopped_early = false;
        loop {
            if max_keys.is_some_and(|budget| walked >= budget) {
                stopped_early = true;
                break;
            }
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "grant scanner: reconciliation scan iteration error"
                    );
                    // Resume forward from the last good key instead of resetting
                    // to the front: a transient object-store error clears and
                    // the next sweep continues past it, so keys already covered
                    // this rotation aren't re-walked. A persistent error is no
                    // worse than the pre-slicing full sweep, which also
                    // refaulted here every tick.
                    stopped_early = true;
                    break;
                }
            };
            walked += 1;
            last_key = Some(kv.key.to_vec());

            let Some(parsed) = parse_concurrency_requester_counter_key(&kv.key) else {
                continue;
            };

            // Only process requests for tenants in our range
            if !range.contains_tenant(&parsed.tenant) {
                continue;
            }

            let count = decode_counter(&kv.value);
            if count > 0 {
                // Clamp rather than cast: a drifted counter beyond u32::MAX
                // must not wrap into a tiny pending count.
                let count = count.clamp(0, u32::MAX as i64) as u32;
                self.request_grant_count_cold(&parsed.tenant, &parsed.queue, count);
            }
        }

        // Persist the resume point. Stopping early mid-keyspace (budget hit or
        // scan error) resumes at the successor of the last walked key — append
        // 0x00, the immediate successor in bytewise order (never increment
        // bytes: 0xFF would overflow). Cleanly reaching the end resets to the
        // front so the next sliced sweep starts a fresh rotation.
        *self.reconcile_cursor.lock().unwrap() = match (stopped_early, last_key) {
            (true, Some(mut key)) => {
                key.push(0x00);
                Some(key)
            }
            _ => None,
        };

        if let Some(ref m) = self.metrics {
            m.record_concurrency_reconcile_sweep(
                &self.shard,
                sweep_started.elapsed().as_secs_f64(),
            );
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
        let invocation_started = std::time::Instant::now();
        // Invocation-level accounting, recorded once at every exit path
        // (including precheck skips) so invocation rate and per-invocation
        // walk cost are observable regardless of how the invocation ends.
        let record_invocation = |keys_scanned: usize, stale_deleted: usize| {
            if let Some(ref m) = self.metrics {
                m.record_concurrency_process_grants(
                    &self.shard,
                    invocation_started.elapsed().as_secs_f64(),
                    keys_scanned as u64,
                    stale_deleted as u64,
                );
            }
        };

        if let Err(e) = self.counts.ensure_hydrated(db, range, tenant, queue).await {
            tracing::warn!(
                error = %e,
                tenant = %tenant,
                queue = %queue,
                "grant scanner: failed to hydrate queue"
            );
            record_invocation(0, 0);
            return Vec::new();
        }

        // [SILO-GRANT-1] Pre: Queue has capacity. Fast pre-scan gate: when the
        // queue's capacity is known (cached limit; durable state row for
        // floating) and every SCANNER-fillable slot is occupied, the
        // try_reserve_internal guard below cannot succeed for any candidate —
        // the transition cannot fire, so skip building the request iterator
        // and the per-candidate status reads entirely. The gate uses the same
        // headroom-reduced capacity the reserve loop grants against (see
        // `scanner_effective_capacity`): gating on full capacity while
        // reserving against the reduced one would re-scan a full batch every
        // cycle just to fail the first reserve whenever holders sit inside
        // the headroom band. `saturating_sub` is required: holders can
        // legitimately exceed capacity after a floating-limit shrink
        // (try_reserve_internal gates with `>=` and never evicts). Skipping
        // leaves the requester counter untouched, so the periodic reconciler
        // re-drives this queue on its next sliced-sweep cursor rotation, and
        // every release / floating-raise path (plus the scanner's own
        // write-failure rollback) nudges the scanner the moment capacity frees.
        // Unknown capacity (cache miss after restart, unreadable floating
        // state) falls through to the scan, which resolves the limit from the
        // first request and re-warms the cache. Accepted trade-offs: stale
        // request cleanup for a saturated queue pauses until a slot frees, and
        // a lowered fixed limit gates old requests that embed a higher one.
        if let Some(capacity) = self.known_queue_capacity(db, tenant, queue).await {
            let effective_capacity = self.scanner_effective_capacity(capacity);
            let holders = self.counts.holder_count(tenant, queue);
            if effective_capacity.saturating_sub(holders) == 0 {
                if let Some(ref m) = self.metrics {
                    m.record_concurrency_grant_precheck_skip(&self.shard);
                }
                tracing::debug!(
                    tenant = %tenant,
                    queue = %queue,
                    requested = count,
                    capacity,
                    effective_capacity,
                    holders,
                    "grant scanner: queue at capacity, skipping request scan"
                );
                record_invocation(0, 0);
                return Vec::new();
            }
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
                record_invocation(0, 0);
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
            /// Index of the gating limit (this queue) in the job's `limits`
            /// list. Resuming the chain starts at `limit_index + 1`.
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
        // Next-hop saturation skip state: per-hop durable reads memoized for
        // the whole invocation (most candidates in one queue share the same
        // next hop, e.g. the tenant's platform queue). A skipped candidate
        // still consumes the per-invocation scan budget below, so a front
        // dominated by saturated-next-hop candidates yields at the budget like
        // any other non-grantable front — no separate early-bail counter.
        let mut next_hop_skip_cache: HashMap<String, NextHopReads> = HashMap::new();
        // Per-invocation scan budget. Total request keys walked
        // across all passes (valid, stale, and corrupt — the walk cost is what
        // we bound). When it reaches `grant_scanner_max_scan_per_invocation` the
        // call commits what it found and returns even if capacity/iterator are
        // not exhausted, so a single queue with a huge non-grantable front
        // cannot starve other tenants' queues sharing the `.buffer_unordered`
        // fan-out.
        // The queue is re-driven next cycle (its requester counter is still
        // non-zero), making bounded forward progress from the front each time.
        let mut scanned_this_invocation: usize = 0;
        let mut total_stale_deleted: usize = 0;
        let mut budget_exhausted = false;
        let chain_resumer = self.chain_resumer();

        // Scan→validate→grant loop: keeps pulling from the iterator until we've
        // granted `count` requests, or hit the end / capacity limit. Each pass
        // scans up to `grant_scanner_batch_size` candidates, validates them concurrently,
        // reserves slots for valid ones, and commits the batch. Stale/corrupt entries
        // are cleaned up along the way. Bounding per-pass scan size caps peak memory
        // (the scanned Vec, buffered status results, and WriteBatch all scale with it),
        // so a large accumulated `count` is drained over multiple bounded passes
        // rather than one unbounded one. The per-invocation scan budget
        // (`budget_exhausted`) additionally bounds total keys walked across all
        // passes, so a queue whose front is millions of non-grantable keys yields
        // after bounded work instead of walking the whole backlog in one call.
        while total_granted < count as usize
            && !iter_exhausted
            && !capacity_exhausted
            && !budget_exhausted
        {
            let needed = (count as usize - total_granted).min(self.grant_scanner_batch_size);

            let mut batch = WriteBatch::new();
            let mut stale_and_corrupt_count: usize = 0;

            // --- Scan batch of candidates ---
            let mut scanned: Vec<ScannedRequest> = Vec::new();
            while scanned.len() < needed {
                if scanned_this_invocation >= self.grant_scanner_max_scan_per_invocation {
                    // Yield mid-scan: commit this pass's accumulated edits below
                    // and let the outer guard end the invocation.
                    budget_exhausted = true;
                    break;
                }
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
                scanned_this_invocation += 1;

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
                let held_queues: Vec<String> = et
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

            // --- Next-hop skip filter ---
            // Defer candidates whose NEXT concurrency hop is saturated
            // with a deep requester backlog: granting them cannot make
            // the job runnable — the chain immediately parks a deferred
            // request at that hop — so it only converts "waiting here"
            // into a parked holder plus one more line entry (pure state
            // inflation), while occupying this queue's slots with jobs
            // that cannot run (the shared-queue wedge shape). The row is
            // left in place — no grant, no delete, no counter decrement —
            // and keeps its original start_time, so when it is eventually
            // granted it parks at its age-correct position in the next
            // hop's FIFO.
            //
            // A skipped row still consumed the per-invocation scan budget
            // (`scanned_this_invocation` was incremented during the scan), so
            // a front of >budget saturated candidates yields at the budget
            // like any other non-grantable front and a grantable candidate
            // anywhere within the budget window is still reached this
            // invocation. Liveness for candidates past the window: the
            // requester counter stays non-zero, so the periodic reconcile
            // sweep re-drives this queue on its next cursor rotation. The
            // min-backlog threshold is a heuristic that the hop stays
            // saturated across that rotation — sound for a deep, slowly
            // draining hop (the wedge shape this targets), but a small-cap
            // hop that drains its whole line within a rotation is re-driven
            // a rotation later than ideal (bounded staleness, no lost work;
            // no reverse-edge nudge wakes upstream queues on desaturation).
            //
            // The per-hop durable reads are warmed for every distinct hop in
            // this pass concurrently; the filter itself then runs against the
            // warmed cache without further I/O in the common case.
            if self.grant_scanner_next_hop_skip_min_backlog > 0 {
                let mut new_hops: Vec<(String, Limit)> = Vec::new();
                let mut seen_hops: HashSet<&str> = HashSet::new();
                for req in &scanned {
                    if let Some(hop) = Self::next_capacity_hop(&req.limits, req.limit_index) {
                        let hop_queue = match hop {
                            Limit::Concurrency(cl) => cl.key.as_str(),
                            Limit::FloatingConcurrency(fl) => fl.key.as_str(),
                            Limit::RateLimit(_) => continue,
                        };
                        if !next_hop_skip_cache.contains_key(hop_queue)
                            && seen_hops.insert(hop_queue)
                        {
                            new_hops.push((hop_queue.to_string(), hop.clone()));
                        }
                    }
                }
                if !new_hops.is_empty() {
                    self.prefetch_next_hop_reads(db, tenant, new_hops, &mut next_hop_skip_cache)
                        .await;
                }

                let mut kept: Vec<ScannedRequest> = Vec::with_capacity(scanned.len());
                for req in scanned {
                    if self
                        .next_hop_should_skip(
                            db,
                            tenant,
                            &req.limits,
                            req.limit_index,
                            &mut next_hop_skip_cache,
                        )
                        .await
                    {
                        if let Some(ref m) = self.metrics {
                            m.record_concurrency_grant_next_hop_skip(&self.shard);
                        }
                        continue;
                    }
                    kept.push(req);
                }
                scanned = kept;
            }

            // If the scan yielded no candidates, fall through to the per-pass
            // commit so any stale-delete edits accumulated above (malformed
            // entries) still get written, then let the outer while guard
            // (which checks `iter_exhausted`) terminate the loop.
            //
            // --- Batch validate status ---
            // Use `buffered` (order-preserving) rather than `join_all` so that
            // we cap in-flight slatedb reads at grant_scanner_buffer_size. Each
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
            .buffered(self.grant_scanner_buffer_size)
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

            // --- Prefetch chain-resume reads ---
            // The reserve loop below walks each grant's remaining limit chain
            // serially. Rows that walk reads — downstream floating-limit
            // state — and the downstream queue hydration each try_reserve
            // consults are fetched here concurrently, so the serial loop runs
            // against warm state instead of paying one object-store round
            // trip per grant. The prefetched values are point-in-time DB
            // reads served through the writer's read overlay, matching the
            // writer's `get` semantics (batch edits are never visible to it).
            let read_cache: Option<Arc<HashMap<Vec<u8>, slatedb::bytes::Bytes>>> =
                if valid_requests.is_empty() {
                    None
                } else {
                    // Owned keys/queues: the async closures below capture by
                    // move, and borrowing across the stream's await points
                    // trips HRTB inference.
                    let mut floating_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
                    let mut downstream_queues: BTreeSet<String> = BTreeSet::new();
                    for req in &valid_requests {
                        for l in req.limits.iter().skip(req.limit_index as usize + 1) {
                            match l {
                                Limit::FloatingConcurrency(fl) => {
                                    floating_keys.insert(floating_limit_state_key(tenant, &fl.key));
                                    downstream_queues.insert(fl.key.clone());
                                }
                                Limit::Concurrency(cl) => {
                                    downstream_queues.insert(cl.key.clone());
                                }
                                Limit::RateLimit(_) => {}
                            }
                        }
                    }
                    futures::stream::iter(downstream_queues.into_iter().map(|q| async move {
                        if let Err(e) = self.counts.ensure_hydrated(db, range, tenant, &q).await {
                            tracing::debug!(
                                error = %e,
                                queue = %q,
                                "grant scanner: downstream hydration prefetch failed"
                            );
                        }
                    }))
                    .buffer_unordered(self.grant_scanner_buffer_size)
                    .collect::<Vec<()>>()
                    .await;
                    let fetched: Vec<(Vec<u8>, Option<slatedb::bytes::Bytes>)> =
                        futures::stream::iter(floating_keys.into_iter().map(|key| async move {
                            let value = db.get(&key).await.ok().flatten();
                            (key, value)
                        }))
                        .buffer_unordered(self.grant_scanner_buffer_size)
                        .collect()
                        .await;
                    let cache: HashMap<Vec<u8>, slatedb::bytes::Bytes> = fetched
                        .into_iter()
                        .filter_map(|(key, value)| value.map(|v| (key, v)))
                        .collect();
                    (!cache.is_empty()).then(|| Arc::new(cache))
                };

            // --- Reserve in-memory slots and accumulate grant edits ---
            //
            // For each valid request we: reserve the in-memory slot for this
            // queue, write the holder + delete the request, then hand control
            // to the chain resumer to write the next limit step. The resumer
            // may make *additional* in-memory reservations (when a downstream
            // FloatingConcurrency/Concurrency limit grants immediately), so we
            // collect all reservations for rollback if a batch write fails.
            //
            // The scanner reserves against the headroom-reduced capacity so
            // the configured live share of slots stays reachable by immediate
            // grants (which use full capacity) while a backlog drains. Note
            // the resumer's downstream immediate grants also use full
            // capacity — headroom applies only to the queue being scanned.
            let limit =
                self.scanner_effective_capacity(max_concurrency.map(|(l, _)| l).unwrap_or(1));
            // Grants stream out of the pass in chunks: every
            // `grant_scanner_commit_chunk_size` grants, the accumulated batch
            // is committed durably and the granted task groups' brokers are
            // woken, so early grants in a long read-bound pass become
            // leasable while later candidates are still being walked. Each
            // chunk stamps a fresh `now_ms` so holder `granted_at_ms` and the
            // resumed `task_key` epoch reflect commit-time reality rather
            // than invocation start.
            //
            // `chunk_reservations` holds the `(queue, task_id)` in-memory
            // reservations of the CURRENT (uncommitted) chunk only — both the
            // just-won queue and any further grants the chain resumer made —
            // for rollback if that chunk's write fails. Committed chunks
            // stand.
            let mut chunk_grants: Vec<(String, String)> = Vec::new();
            let mut chunk_reservations: Vec<(String, String)> = Vec::new();
            let mut chunk_stale = stale_and_corrupt_count;
            let mut chunk_now = crate::job_store_shard::now_epoch_ms();
            for req in &valid_requests {
                if total_granted + chunk_grants.len() >= count as usize {
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
                chunk_reservations.push((queue.to_string(), req.task_id.clone()));

                // [SILO-GRANT-3] Create holder for the just-won queue
                let holder_val = encode_holder(&HolderRecord {
                    granted_at_ms: chunk_now,
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
                // — those reservations are added to `chunk_reservations`.
                //
                // If the chain resumer is unavailable (shard shutdown), abort
                // this request: release the current chunk's reservations and
                // bail. The uncommitted chunk's request deletes die with the
                // uncommitted batch; committed chunks stand.
                let Some(ref resumer) = chain_resumer else {
                    tracing::warn!(
                        queue = %queue,
                        task_id = %req.task_id,
                        "grant scanner: chain resumer not installed; aborting pass"
                    );
                    for (q, tid) in &chunk_reservations {
                        self.counts.release_reservation(tenant, q, tid);
                    }
                    record_invocation(scanned_this_invocation, total_stale_deleted);
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
                    now_ms: chunk_now,
                    read_cache: read_cache.clone(),
                };

                match resumer.resume_chain(&mut batch, resume_params).await {
                    Ok(chain_grants) => {
                        // Each (queue, task_id) the chain reserved must roll back
                        // alongside ours if this chunk's write fails.
                        chunk_reservations.extend(chain_grants);
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            queue = %queue,
                            task_id = %req.task_id,
                            "grant scanner: chain resume failed; rolling back chunk"
                        );
                        for (q, tid) in &chunk_reservations {
                            self.counts.release_reservation(tenant, q, tid);
                        }
                        record_invocation(scanned_this_invocation, total_stale_deleted);
                        return all_granted_groups;
                    }
                }

                chunk_grants.push((req.task_id.clone(), req.task_group.clone()));

                if chunk_grants.len() >= self.grant_scanner_commit_chunk_size {
                    let commit_batch = std::mem::replace(&mut batch, WriteBatch::new());
                    let stale_deletes = std::mem::take(&mut chunk_stale);
                    if self
                        .commit_grant_chunk(
                            db,
                            tenant,
                            queue,
                            commit_batch,
                            &chunk_grants,
                            &chunk_reservations,
                            stale_deletes,
                            total_granted,
                            &chain_resumer,
                        )
                        .await
                        .is_err()
                    {
                        record_invocation(scanned_this_invocation, total_stale_deleted);
                        return all_granted_groups;
                    }
                    total_granted += chunk_grants.len();
                    total_stale_deleted += stale_deletes;
                    all_granted_groups.extend(chunk_grants.drain(..).map(|(_, tg)| tg));
                    chunk_reservations.clear();
                    chunk_now = crate::job_store_shard::now_epoch_ms();
                }
            }

            // --- Final chunk commit ---
            // Skip the durable write if nothing is pending (e.g. every scanned
            // status lookup returned an error). The iterator has still
            // advanced, so the outer loop continues until iter_exhausted.
            if chunk_grants.is_empty() && chunk_stale == 0 {
                continue;
            }
            let commit_batch = std::mem::replace(&mut batch, WriteBatch::new());
            let stale_deletes = std::mem::take(&mut chunk_stale);
            if self
                .commit_grant_chunk(
                    db,
                    tenant,
                    queue,
                    commit_batch,
                    &chunk_grants,
                    &chunk_reservations,
                    stale_deletes,
                    total_granted,
                    &chain_resumer,
                )
                .await
                .is_err()
            {
                record_invocation(scanned_this_invocation, total_stale_deleted);
                return all_granted_groups;
            }
            total_granted += chunk_grants.len();
            total_stale_deleted += stale_deletes;
            all_granted_groups.extend(chunk_grants.into_iter().map(|(_, tg)| tg));
        }

        // No silent cap: surface invocations that yielded on the scan budget so
        // a queue needing many cycles to drain a huge non-grantable front is
        // visible rather than looking fully drained.
        if budget_exhausted {
            tracing::debug!(
                tenant = %tenant,
                queue = %queue,
                requested = count,
                scanned = scanned_this_invocation,
                stale_deleted = total_stale_deleted,
                total_granted,
                "grant scanner: hit per-invocation scan budget; yielding, will re-drive next cycle"
            );
        }

        record_invocation(scanned_this_invocation, total_stale_deleted);
        all_granted_groups
    }

    /// Durably commit one chunk of scanner grants: folds the requester-counter
    /// decrement for the chunk's grants and carried stale deletes into
    /// `batch`, writes it with `await_durable`, and wakes the granted task
    /// groups' brokers so the tasks become claimable immediately.
    ///
    /// On write failure, rolls back the chunk's in-memory reservations and
    /// re-nudges the queue's hot lane; previously committed chunks stand.
    #[allow(clippy::too_many_arguments)]
    async fn commit_grant_chunk(
        &self,
        db: &Arc<InstrumentedDb>,
        tenant: &str,
        queue: &str,
        mut batch: WriteBatch,
        chunk_grants: &[(String, String)],
        chunk_reservations: &[(String, String)],
        stale_deletes: usize,
        prior_grants: usize,
        chain_resumer: &Option<Arc<dyn LimitChainResumer>>,
    ) -> Result<(), ()> {
        let decrement = chunk_grants.len() + stale_deletes;
        if decrement > 0 {
            batch.merge(
                concurrency_requester_counter_key(tenant, queue),
                encode_counter(-(decrement as i64)),
            );
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
            for (q, tid) in chunk_reservations {
                self.counts.release_reservation(tenant, q, tid);
            }
            // The batch did not commit, so this chunk's durable request rows
            // and counter decrement are untouched — the freed slots are
            // grantable again immediately. Re-nudge the hot lane so the retry
            // happens on the next drain iteration rather than waiting for the
            // sliced reconcile sweep to rotate back to this queue
            // (release_reservation alone does not wake the scanner).
            self.request_grant(tenant, queue);
            tracing::warn!(
                error = %e,
                chunk_grants = chunk_grants.len(),
                chunk_reservations = chunk_reservations.len(),
                prior_grants,
                "grant scanner: chunk write failed, rolled back this chunk's reservations"
            );
            return Err(());
        }

        for (request_id, task_group) in chunk_grants {
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
                chunk_grants.len() as u64,
            );
        }

        // Wake the brokers for this chunk's task groups now that the tasks
        // are durable; without this the tasks wait for a broker scan or for
        // the drain iteration's post-fan-out wakeup.
        if !chunk_grants.is_empty()
            && let Some(resumer) = chain_resumer
        {
            let mut groups: Vec<String> = chunk_grants.iter().map(|(_, tg)| tg.clone()).collect();
            groups.sort_unstable();
            groups.dedup();
            resumer.wakeup_task_groups(&groups);
        }

        Ok(())
    }

    /// The next capacity-holding hop after `limit_index`, looking through
    /// rate limits. `None` when the chain is terminal from there.
    fn next_capacity_hop(limits: &[Limit], limit_index: u32) -> Option<&Limit> {
        let mut probe = limit_index as usize + 1;
        loop {
            match limits.get(probe) {
                None => return None,
                Some(Limit::RateLimit(_)) => probe += 1,
                Some(other) => return Some(other),
            }
        }
    }

    /// Concurrently warm `next_hop_should_skip`'s per-hop durable reads for
    /// every hop in `hops` (each not yet present in `cache`): the floating
    /// capacity read, and — when the hop is saturated per that read — the
    /// requester-counter depth read. Fixed hops compute saturation against
    /// the supplied limit's embedded capacity; a candidate carrying a
    /// different embedded value falls back to the skip check's own read.
    async fn prefetch_next_hop_reads(
        &self,
        db: &Arc<InstrumentedDb>,
        tenant: &str,
        hops: Vec<(String, Limit)>,
        cache: &mut HashMap<String, NextHopReads>,
    ) {
        let fetched: Vec<(String, NextHopReads)> =
            futures::stream::iter(hops.into_iter().map(|(hop_queue, hop)| async move {
                let mut reads = NextHopReads::default();
                let capacity = match &hop {
                    Limit::Concurrency(cl) => Some(cl.max_concurrency as usize),
                    Limit::FloatingConcurrency(fl) => {
                        let capacity = Self::floating_state_capacity(db, tenant, &hop_queue)
                            .await
                            .unwrap_or(fl.default_max_concurrency as usize);
                        reads.floating_capacity = Some(capacity);
                        Some(capacity)
                    }
                    Limit::RateLimit(_) => None,
                };
                if let Some(capacity) = capacity
                    && self.counts.holder_count(tenant, &hop_queue) >= capacity
                {
                    reads.depth = Some(
                        match db
                            .get(&concurrency_requester_counter_key(tenant, &hop_queue))
                            .await
                        {
                            Ok(Some(raw)) => decode_counter(&raw),
                            _ => 0,
                        },
                    );
                }
                (hop_queue, reads)
            }))
            .buffer_unordered(self.grant_scanner_buffer_size)
            .collect()
            .await;
        for (hop_queue, reads) in fetched {
            cache.entry(hop_queue).or_insert(reads);
        }
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

/// Append a deferred concurrency request record (plus the +1 requester
/// counter merge) for a chain parked at an at-capacity queue.
///
/// Two callers: the at-capacity immediate-enqueue path ([SILO-ENQ-CONC-6])
/// and the dequeue-time ticket-conversion path in `job_store_shard::dequeue`
/// (`handle_request_ticket`). Callers must pass the chain's ORIGINAL
/// `start_time_ms` and `priority`: cancel/reimport locate request records by
/// the narrow `(tenant, queue, start_time_ms, priority, job_id, attempt)`
/// prefix derived from job status, and `process_grants` grants in
/// start-time order, so a rewritten time would orphan the record from
/// cleanup and forfeit the job's queue position.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_request_edits<W: WriteBatcher>(
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
