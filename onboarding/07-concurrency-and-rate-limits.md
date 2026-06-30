# 07 — Concurrency & Rate Limiting

A job can be gated behind an **ordered list of limits** it must clear before its attempt becomes a leasable `RunAttempt` task. This is silo's most subtle subsystem — read file 04 (lifecycle) first.

---

## 1. The `Limit` type (`src/job.rs:144`)

```rust
pub enum Limit {
    Concurrency(ConcurrencyLimit),                 // fixed slot count
    RateLimit(GubernatorRateLimit),                // rate window, checked via gubernator
    FloatingConcurrency(FloatingConcurrencyLimit), // slot count refreshed dynamically by a worker
}
```

A job carries `limits: Vec<Limit>` on its `JobInfo` (`src/job.rs:275`) — **ordered**; the client decides the order.

- **`ConcurrencyLimit`** (`job.rs:18`) — `{ key, max_concurrency }`. `key` is the queue name; `max_concurrency` is fixed.
- **`FloatingConcurrencyLimit`** (`job.rs:26`) — `{ key, default_max_concurrency, refresh_interval_ms, metadata }`. Max is dynamic, refreshed by a worker.
- **`GubernatorRateLimit`** (`job.rs:103`) — `{ name, unique_key, limit, duration_ms, hits, algorithm (TokenBucket|LeakyBucket), behavior, retry_policy }`.

### "High cardinality"
There is **no pre-registered set of queues.** A queue exists implicitly the moment a job references its `key`. This works because:
- Capacity is resolved **on demand** from the job's limits list (`resolve_queue_capacity_from_limits`, `concurrency.rs:929`; default 1 if not found), never from a config table.
- In-memory per-queue state is a **sparse `DashMap`** keyed by `(tenant, queue)`, lazily hydrated on first touch. Millions of distinct queues cost nothing until used.
- Holders/requests are individual SlateDB rows keyed by `(tenant, queue, …)`; the keyspace scales with *active work*, not declared queues.

---

## 2. Concurrency limits: the holder/grant model

### Storage (key prefixes from `src/keys.rs`)
- **`0x09` CONCURRENCY_HOLDER** `(tenant, queue, task_id)` → `HolderRecord { granted_at_ms }`. **Presence = `task_id` holds a slot.**
- **`0x08` CONCURRENCY_REQUEST** `(tenant, queue, start_time, priority, job_id, attempt, suffix)` → an encoded action to resume the chain. **A job waiting for a slot.** Ordered by start time then priority.
- **`0x0B` FLOATING_LIMIT** `(tenant, queue)` → `FloatingLimitState`.
- **`0xF7` COUNTER_CONCURRENCY_REQUESTERS** `(tenant, queue)` → LE i64 count of pending requests (§7).

### In-memory authority
The "is there capacity?" decision is **in-memory**, not the DB. `ConcurrencyCounts` (`concurrency.rs:260`) is a `DashMap<(tenant,queue), QueueEntry>`; each `QueueEntry` holds a `holders: HashSet<String>` of task_ids. Slots-in-use = `holders.len()`.

> **The cardinal invariant** (`concurrency.rs:14`): update in-memory counts **before** the DB write (to prevent time-of-check/time-of-use races), and **roll back** the in-memory change if the write fails.

### Acquiring a slot (`try_reserve`, `concurrency.rs:451`)
Atomic check-and-reserve under the DashMap shard guard:
```rust
if entry.holders.len() >= limit { return false; }  // at capacity
entry.holders.insert(task_id);                      // reserve
```
If the subsequent DB write fails, `release_reservation` rolls back the insert.

### Enqueue-time handling (`handle_enqueue`, `concurrency.rs:965`)
Processes **one** concurrency limit, returning a `RequestTicketOutcome`:
- **`GrantedImmediately`** — capacity exists (and not future-scheduled): reserve in-memory, write the holder (`0x09`). (The `RunAttempt` task is *not* written here — only once, at the end of the chain.)
- **`TicketRequested`** — present-time, no capacity: write a `0x08` request row.
- **`FutureRequestTaskWritten`** — `scheduled_at_ms > now`: roll back any reservation (so a burst of future jobs can't starve present work) and write a future `RequestTicket` task.

`skip_try_reserve` is set on **retries** (the old holder is still in-memory, released only post-commit, so the retry must queue rather than re-grab a slot).

### The grant scanner: admitting queued requests
Release and grant are **decoupled**. When a holder is released (completion, expiry, cancel, dropped task), the releaser deletes the holder row, calls `atomic_release` in-memory, and signals via `request_grant`:
```rust
for (tenant, queue, task_id) in holder_guard.take_all() {
    self.concurrency.counts().atomic_release(&tenant, &queue, &task_id);
    self.concurrency.request_grant(&tenant, &queue);
}
```
A **single** background task (`start_grant_scanner`, `concurrency.rs:1378`) drains pending grants and runs `process_grants` per queue. *One* scanner eliminates the race where two releasers both try to grant the same request.

`process_grants` (`concurrency.rs:1496`):
1. Scan `0x08` request rows in ascending `(start_time, priority)` order; stop early at the first request with `start_time > now`.
2. Work in bounded passes (`grant_scanner_batch_size`, default 256), validating job status concurrently (`grant_scanner_buffer_size`, default 64). A request is valid only if the job is still `Scheduled` for that exact attempt; stale/terminal ones are deleted.
3. For each valid request: `try_reserve` the gating slot; if full, stop. On success write the holder, delete the request, and **resume the chain** at `limit_index + 1`.
4. Commit the pass durably; on failure roll back *all* in-memory reservations made this pass.
5. Wake only the brokers whose task groups got grants.

### Startup & reconciliation
`hydrate_all` (optional, `hydrate_all_at_startup`) pre-populates the cache from durable holders. `reconcile_pending_requests` re-scans at scanner startup to self-heal missed notifications. A periodic `reconcile_pending_holders` resolves drift between in-memory and durable holder state per task_id (the "ghost" case: in-memory reservation survives but durable holder is gone → release + kick scanner).

---

## 3. The limit chain

A job's limits are processed **one at a time** by the chain walker `walk_limit_chain` / `enqueue_limit_task_at_index` (`src/job_store_shard/enqueue.rs:520`).

### The walk (`LimitTaskParams`, `enqueue.rs:31`)
Carries `limit_index` (where to start), the `limits` slice, `held_queues` (queues already held by earlier steps), the chain's `task_id`, and `skip_try_reserve`. Loop body:
- `index >= limits.len()` → write the terminal **`RunAttempt`** task carrying all accumulated `held_queues`, return.
- `Concurrency`/`FloatingConcurrency` → `handle_enqueue`; if granted, append the queue to `held_queues`, advance; if queued, return (chain parks).
- `RateLimit` → write a `CheckRateLimit` task at this index, return (never grants immediately).

> **Critical invariant** (`enqueue.rs:112`): the chain's `task_id` is **never cycled** across limits. Every holder accumulated across the chain shares one `task_id`, because cleanup releases holders by `concurrency_holder_key(tenant, queue, <chain task_id>)`. Cycling the id would orphan earlier holders and leak slots. (This is why retries and rate-limit retries carefully *reuse* the task_id.)

### Resuming a parked chain
When a gate is full, the chain parks (as a `0x08` request, present-time, or a `RequestTicket` task, future-time). When the slot frees, two resume paths both land back at `limit_index + 1`:
1. **Future `RequestTicket` fired by the broker** → `handle_request_ticket` (`dequeue.rs:651`): re-check status, `try_reserve`, on success write holder + delete ticket + resume chain.
2. **Durable `0x08` request fired by the grant scanner** → via the `LimitChainResumer` trait. The bridge `ShardChainResumer` (`src/job_store_shard/limit_chain.rs:25`) holds a `Weak<JobStoreShard>` (to avoid a reference cycle) and forwards into `enqueue_limit_task_at_index`.

**Start-time subtlety**: a resumed chain keeps the job's **original** `start_at_ms` for broker ordering (a job that already waited mustn't be shoved to the back of the start-time-ordered scan). The parent task's broker tombstone is dodged via the trailing `epoch_ms` (set to `now` or `parent_epoch + 1`), not by changing the start time.

---

## 4. Floating limits

A `FloatingConcurrency` limit's max is **refreshed by an external worker**, not fixed. Durable state at `0x0B` (`FloatingLimitState`: `current_max_concurrency`, `last_refreshed_at_ms`, `refresh_task_scheduled`, `refresh_interval_ms`, `default_max_concurrency`, retry fields, metadata).

### Treated like a concurrency limit
During the walk, the floating branch reads/creates the state, builds a **temporary fixed** `ConcurrencyLimit` from `state.current_max_concurrency()`, and runs the same `handle_enqueue`. So at any instant it gates exactly like a fixed limit — just with a dynamic cap.

### Refresh (lazy, demand-driven) — `src/job_store_shard/floating.rs`
Gated by `floating_limit_refresh_ready` (not already scheduled, past `last_refreshed + interval`, not in backoff) **and only when there are waiters**. `maybe_schedule_floating_limit_refresh` writes a `RefreshFloatingLimit` task (highest priority, synthetic job_id `floating_refresh:<key>`) and flips `refresh_task_scheduled = true`.

The task is **leased to a worker** at dequeue (`handle_refresh_floating_limit`) as a `LeasedRefreshTask` (with `queue_key`, `current_max_concurrency`, metadata). The worker computes a new max and reports back via `ReportRefreshOutcome`:
- **Success** → write the new `current_max_concurrency`, stamp `last_refreshed_at_ms`, clear flags, delete the lease, update the in-memory limit cache.
- **Failure** → exponential backoff (1s base, 60s cap, ×2); reschedule only if waiters remain.

A raised cap frees slots the grant scanner then distributes; a lowered cap just lowers the ceiling for future `try_reserve` (existing holders aren't evicted).

---

## 5. Rate limits via gubernator

**Gubernator** is an external, distributed rate-limiting service (separate process). Silo defers all rate-limit accounting to it over gRPC (`proto/gubernator.proto`: `GetRateLimits` batched, `HealthCheck`; `Algorithm` = TOKEN_BUCKET/LEAKY_BUCKET; status UNDER_LIMIT/OVER_LIMIT).

### The client (`src/gubernator.rs`)
The `RateLimitClient` trait has three impls: `GubernatorClient` (real), `NullGubernatorClient` (returns `NotConfigured` — when no address configured), `MockGubernatorClient` (in-memory sim for tests). The real client **coalesces** requests: individual checks push onto an mpsc channel; a background loop batches within `coalesce_interval_ms` (default 5ms, up to 100) into a single `GetRateLimits` RPC and fans replies back.

### The CheckRateLimit flow (`handle_check_rate_limit`, `dequeue.rs:861`)
1. Delete + tombstone the task.
2. If the job is terminal/missing → drop and **release upstream holders** (else they leak).
3. Call gubernator.
4. **Under limit** → resume the chain at `limit_index + 1`.
5. **Over limit** → schedule a retry (backoff prefers gubernator's `reset_time_ms`), unless `max_retries` exhausted (then drop + release holders).
6. **RPC error** → retry with initial backoff.

Retries reuse the chain's `task_id` (`src/job_store_shard/rate_limit.rs:9`) and dodge the parent tombstone via `epoch_ms = parent_epoch + 1`.

---

## 6. Limit-change support (high cardinality)

Silo supports changing a queue's effective limit with **no registration/migration step**:
- **Capacity is resolved on demand, never cached as truth** (`resolve_queue_capacity_from_limits`). A job enqueued with a different `max_concurrency`, or a refreshed floating state, immediately changes the effective cap at the next `try_reserve`/`process_grants`.
- **Floating limits are the explicit dynamic path** — a worker raises/lowers `current_max_concurrency` at any refresh. Raising frees capacity the grant scanner distributes; lowering drains naturally.
- The in-memory `limit_cache` is only an advisory snapshot for observability/queries — never the source of truth for admission.

---

## 7. The requesters counter (`0xF7`)

`COUNTER_CONCURRENCY_REQUESTERS` is a per-`(tenant, queue)` merge counter of **pending requests**. Incremented in `append_request_edits` when a request is written; decremented by the grant scanner per pass (granted + stale-deleted). Because both directions use the SlateDB merge operator, it's crash-safe without read-modify-write — an O(1) "how many jobs are waiting on this queue?" readout. Distinct from the in-memory holder count (granted slots).

---

## 8. Mental model

- **Holders (`0x09`) = granted slots; requests (`0x08`) = the waiting line.** In-memory `holders` set is the source of truth for capacity; durable rows are for recovery.
- **Update in-memory first, write durably second, roll back on failure.**
- **One grant scanner** decouples release from admission and avoids double-grant races.
- **The chain** processes ordered limits, accumulating holders under **one shared task_id**, parking when a gate is full and resuming at the next index when the slot frees.
- **Floating** = dynamic concurrency cap via worker refresh; **rate** = delegated to gubernator.

Next: [`08-server-query-webui-observability.md`](./08-server-query-webui-observability.md).
