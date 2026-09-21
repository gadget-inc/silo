# 04 — The End-to-End Job Lifecycle

> This is the headline file. It traces a job from `Enqueue` through completion, retry, cancellation, and crash-recovery, naming the exact code and storage keys touched at each step. If you only deeply understand one file, make it this one.

All file:line citations are "look near here." The key prefixes (`0x01`…`0x0B`, `0xF0+`) are explained in [file 05](./05-storage-and-keys.md); the limit chain in [file 07](./07-concurrency-and-rate-limits.md).

---

## 0. The four nouns (recap)

- **Job** = the durable user-enqueued thing. Has immutable *info* (`0x01`) + mutable *status* (`0x02`).
- **Task** = a unit of pending work in a task group's queue (`0x05`). Variants: `RunAttempt`, `RequestTicket`, `CheckRateLimit`, `RefreshFloatingLimit`. Ordered by `(start_time, priority)`.
- **Lease** = a `RunAttempt` task claimed by a worker (`0x06`), with an expiry; renewed by heartbeats.
- **Attempt** = one execution try (`0x07`), holding the outcome and (on success) the **result**.

---

## 1. Job states & the state machine

The status enum (`src/job.rs:155`):

```rust
pub enum JobStatusKind {
    Scheduled,   // waiting to run (initial, future-scheduled, or between retries)
    Running,     // leased to a worker
    Failed,      // terminal: retries exhausted (a kind of dead-letter)
    Cancelled,   // terminal-ish: cancelled (can be restarted)
    Succeeded,   // terminal: done with a result
}
```

Two distinct "doneness" predicates — don't conflate them:
- **`is_final()`** (`src/job.rs:167`) = `Succeeded | Failed`. Truly irreversible; gates cancel/expedite/lease.
- **`is_terminal()`** (`src/job.rs:242`) = `Succeeded | Failed | Cancelled`. Used by `GetJobResult` to decide a result is available.

So **Cancelled is terminal but not final** — a cancelled job can be restarted; `RestartJob` also allows `Failed`.

```
                  enqueue
                     │
                     ▼
   expedite     ┌───────────┐   report Error + retries remain
  (future→now)  │ Scheduled │◄──────────────────────────┐
       ────────▶│ waiting/  │                            │
                │ future/   │                            │
                │ retry     │                            │
                └─────┬─────┘                            │
            dequeue → task leased                        │
                     ▼                                   │
                ┌───────────┐  report Error (retry left) ┘
                │  Running  │
                │  (leased) │  report Error (no retries) ──▶ Failed (terminal)
                └─────┬─────┘
        report Success│  report Cancelled / lease-expiry-while-cancelled
                      ▼            ▼
                  Succeeded     Cancelled  ──(RestartJob)──▶ Scheduled
                  (terminal)                Failed ─(RestartJob)─▶ Scheduled
```

The `JobStatus` struct (`src/job.rs:184`) carries `kind`, `changed_at_ms`, `next_attempt_starts_after_ms` (when the next attempt starts — present for Scheduled), and `current_attempt` (the pending task's attempt number — lets cancel/expedite/lease rebuild the task key in O(1)).

---

## 2. ENQUEUE

**Entry:** `JobStoreShard::enqueue` (`src/job_store_shard/enqueue.rs:149`). gRPC `Enqueue` (`proto/silo.proto:655`).

### Idempotency
Branches on whether the caller supplied a job ID (`enqueue.rs:161`):
- **Caller-supplied ID** → `enqueue_with_dedup` → `enqueue_txn` (`enqueue.rs:269`): opens a SlateDB transaction with **Serializable Snapshot Isolation**, checks `job_info_key` doesn't already exist (`JobAlreadyExists` if it does — `[SILO-ENQ-1]` at `enqueue.rs:316`), wrapped in `retry_on_txn_conflict`.
- **No ID** → generates a UUIDv4 and takes the faster `enqueue_batch` path with a plain `WriteBatch` (no dedup needed).

### What gets written (`write_enqueue_data`, `enqueue.rs:380`)
The `JobInfo` (`src/job.rs:267`) bundles `id, priority (0=highest..99), enqueue_time_ms, payload (opaque MessagePack), retry_policy, metadata, limits (ordered), task_group`. Writes, all in one durable batch:

1. **`0x01` JobInfo** — the immutable definition.
2. **`0x04` Metadata index** — one empty-valued entry per metadata pair (so you can later find jobs by metadata).
3. **`0x02` JobStatus = Scheduled** + **`0x03` status/time index** + **`0xF8` per-(tenant,status) counter**. Initial status is `Scheduled(now, effective_start_at_ms, attempt=1)`.
   - `effective_start_at_ms = if start_at_ms <= 0 { now } else { start_at_ms }`.
4. **The limit chain / first task** — `enqueue_limit_task_at_index` walks the job's `limits` in order (`walk_limit_chain`, `enqueue.rs:551`):
   - **No limits** → write a `RunAttempt` **task** (`0x05`) at `scheduled_at_ms`.
   - **Concurrency / Floating limit** → try an immediate in-memory grant; if granted write a holder (`0x09`) and continue to the next limit; if full, write a request (`0x08`) or future `RequestTicket` task and stop.
   - **Rate limit** → write a `CheckRateLimit` task (`0x05`); never granted immediately.
5. **`0xF0` total-jobs counter** — incremented.

### Future scheduling (delayed jobs)
`start_at_ms` flows into the task key's `start_time_ms`. The broker scanner skips any task with `start_time_ms > now` and, because task keys sort by start time, **stops at the first future task** (`task_broker.rs:168`). After commit, `finish_enqueue` only wakes the broker if the job is due now (`enqueue.rs:485`); future jobs are picked up when their time arrives.

### Durability & rollback
Both paths commit with `await_durable: true` (blocks until the WAL is flushed to object storage). On write failure, in-memory concurrency grants are rolled back. A two-phase DST `JobEnqueued` event brackets the write (file 09 §3.4).

---

## 3. DEQUEUE (a worker asks for work)

gRPC `LeaseTasks` (`proto/silo.proto:694`) — requires `worker_id`, `max_tasks`, `task_group`. Core: `JobStoreShard::dequeue` (`src/job_store_shard/dequeue.rs:292`).

### The TaskBroker long-poll (`src/task_broker.rs`)
**One broker per task group** (`TaskBrokerRegistry::get_or_create`, `task_broker.rs:548`). Each broker holds:
- an in-memory sorted `buffer` (a `SkipMap`) of ready tasks,
- an `inflight` set of claimed-but-not-yet-leased keys,
- `ack_tombstones` to suppress re-inserting a task that was just deleted.

A **background scanner** (`task_broker.rs:278`) range-scans only this task group's `0x05` keys, skipping future-dated (breaks on first), inflight, tombstoned, and already-buffered tasks; it deletes "defunct" tasks whose tenant falls outside the shard range. It refills when the buffer drops below half its target (8192), batch size 4096, with exponential backoff (50ms→2s) when idle and early wakeup via `wakeup()` on enqueue/retry/expedite/restart.

### Selecting & leasing (`dequeue`)
`dequeue` loops up to `MAX_INTERNAL_ITERATIONS = 10` because internal chain tasks must be processed transparently before returning real work:

1. **Claim** up to N tasks via `claim_ready_or_nudge` (moves keys `buffer → inflight`; if empty, wakes the scanner and polls briefly).
2. Each claimed task is wrapped in a **`ClaimedInflightGuard`** (RAII) so a cancelled/panicked dequeue releases the reservation instead of leaking it.
3. Dispatch by task variant:
   - **`RunAttempt`** → `handle_run_attempt` (`dequeue.rs:1140`): load `JobInfo` (if gone, delete task + release held concurrency), delete the task, then `write_lease_and_attempt`.
   - **`RequestTicket`** → `handle_request_ticket`: try to reserve the gating concurrency slot; on grant write a holder, delete the ticket, continue the chain. (Internal — loops again.)
   - **`CheckRateLimit`** → `handle_check_rate_limit`: call gubernator; on pass continue the chain, on over-limit reschedule. (Internal.)
   - **`RefreshFloatingLimit`** → `handle_refresh_floating_limit`: turn into a lease returned to the worker as a *refresh task*.
4. Tasks outside the shard range are deleted & skipped.
5. Commit the whole iteration durably; on failure, roll back grants, requeue claimed tasks, cancel DST events.
6. After commit, release inflight + install tombstones for deleted keys. If only internal tasks were processed, loop to pick up the freshly-written follow-ups.

### What a lease costs (`write_lease_and_attempt`, `dequeue.rs:577`)
For each `RunAttempt`:
- **`0x06` Lease** — `LeaseRecord { worker_id, task, expiry_ms = now + DEFAULT_LEASE_MS, started_at_ms }`. `DEFAULT_LEASE_MS = 10_000` (`src/task.rs:10`).
- **`0x02` JobStatus = Running** (+ `0x03` index update).
- **`0x07` Attempt** — `JobAttempt { status: Running, started_at_ms, ... }`.

The worker receives a proto `Task` carrying `lease_ms`, `is_last_attempt`, `payload`, `limits` (`proto/silo.proto:253`).

---

## 4. LEASE: heartbeat, expiry & reclaim

### Heartbeat (`heartbeat_task`, `src/job_store_shard/lease.rs:31`)
gRPC `Heartbeat` (`proto/silo.proto:707`). Reads the lease (`0x06`), verifies `worker_id` matches (`LeaseOwnerMismatch` otherwise), and **always renews** the expiry — even for a cancelled job, so the worker keeps the lease alive during graceful shutdown. Returns `{ cancelled, cancelled_at_ms }` by checking the `0x0A` cancellation flag. **This is how a worker learns it was cancelled.**

### Expiry & reclaim (`reap_expired_leases`, `lease.rs:493`)
A background process (run every 1s from the server, file 08 §2.3) scans all `0x06` leases:
- Deletes leases for tenants outside the shard range (defunct).
- Skips leases not yet expired (`expiry_ms > now`).
- For expired **RunAttempt** leases: if the job's `0x0A` cancel flag is set → report `Cancelled`; otherwise → report `Error{ error_code: "WORKER_CRASHED" }`. Both route through `report_attempt_outcome`, which fails/retries the job and releases held concurrency.
- For expired **RefreshFloatingLimit** leases → reset the floating-limit state so a new refresh can be scheduled.

`purge_orphaned_holders_for_task` (`lease.rs:670`) is a defensive self-heal: if an outcome arrives for a task whose lease was already reaped, it scans and deletes any concurrency holders tied to that `task_id`.

---

## 5. ATTEMPTS

`JobAttempt` (`src/job_attempt.rs:43`):

```rust
pub struct JobAttempt {
    pub job_id: String,
    pub attempt_number: u32,           // monotonic across restarts (used in the 0x07 key)
    pub relative_attempt_number: u32,  // resets to 1 on restart; drives retry backoff
    pub task_id: String,
    pub started_at_ms: i64,
    pub status: AttemptStatus,
}
```

`AttemptStatus` (`src/job_attempt.rs:25`): `Running`, `Succeeded { finished_at_ms, result }`, `Failed { finished_at_ms, error_code, error }`, `Cancelled { finished_at_ms }`.

Workers report via `AttemptOutcome` (`src/job_attempt.rs:9`): `Success { result }`, `Error { error_code, error }`, or `Cancelled`.

Each attempt is its own `0x07` key `(tenant, job_id, attempt)`, so history is recoverable by scanning `attempt_prefix(tenant, job_id)`. **Why two attempt numbers?** `attempt_number` is monotonic (history never overwritten); `relative_attempt_number` resets on restart so a restarted job gets a fresh backoff schedule.

---

## 6. COMPLETION (success)

gRPC `ReportOutcome` (`proto/silo.proto:698`). Core: `report_attempt_outcome` (`src/job_store_shard/lease.rs:94`), Success branch (`lease.rs:176`):

1. Load the lease by `task_id` (`LeaseNotFound` if missing — e.g. lease already reaped).
2. Build the terminal `JobAttempt { Succeeded { result } }`.
3. Delete the lease (`0x06`).
4. Set `JobStatus = Succeeded` **with a row TTL** (`compute_terminal_expire_ts`).
5. Increment `0xF1` completed-jobs counter.
6. `expire_terminal_job_records` re-writes `0x01/0x04/0x07/0x0A` with a TTL so they age out (ordering subtlety: runs *before* writing the new terminal attempt, since WriteBatch is last-write-wins).
7. Write the terminal `0x07` attempt (with TTL) carrying the **result**.
8. Delete concurrency holders (`0x09`) for all `held_queues`.
9. Commit durably; emit DST events.
10. Post-commit: release each held slot in-memory and `request_grant` to wake the grant scanner so queued requesters can be admitted.

### How the enqueuer gets the result
The result is **stored on the terminal attempt, not pushed.** The enqueuer polls `GetJobResult` (`proto/silo.proto:663`). Server impl `get_job_result` (`src/server.rs:964`): requires the job to be terminal (`is_terminal()`, else `FAILED_PRECONDITION`), fetches the latest attempt, and maps `Succeeded → success_data`, `Failed → failure{error_code, error_data}`, `Cancelled → cancelled{cancelled_at_ms}`. Results are wrapped in `SerializedBytes { msgpack }`.

---

## 7. FAILURE & RETRY

### Retry policy (`src/job.rs:319`) & backoff (`src/retry.rs`)
```rust
pub struct RetryPolicy {
    pub retry_count: u32,          // max retries AFTER the initial attempt
    pub initial_interval_ms: i64,  // default 1000
    pub max_interval_ms: i64,      // caps backoff
    pub randomize_interval: bool,  // deterministic jitter
    pub backoff_factor: f64,       // default 2.0
}
```
- `retries_exhausted(failures_so_far, policy)` = `failures_so_far > policy.retry_count`.
- `next_retry_time_ms`: `delay = initial * factor^failures_so_far`, optional seeded jitter in `[1,2)`, clamped to `max_interval_ms`; returns `failure_time + delay` or `None` if exhausted.

### The decision (`report_attempt_outcome`, Error branch, `lease.rs:241`)
1. Load `JobInfo` for priority/task_group/limits/retry policy.
2. `failures_so_far = relative_attempt_number` (so restart resets the schedule).
3. If a retry policy exists **and** `next_retry_time_ms` is `Some(next)`:
   - Enqueue a fresh limit chain at `limit_index: 0`, `attempt_number+1`, `relative_attempt_number+1`, new `task_id`, **empty `held_queues`**, `scheduled_at_ms = next`, `skip_try_reserve: true` (the old holder is still in-memory; the retry must go through the request queue so other jobs can run during the backoff window).
   - Set `JobStatus = Scheduled(now, next, attempt_number+1)`.
4. **Else** → dead-letter: `JobStatus = Failed` (with TTL), bump `0xF1`, `expire_terminal_job_records`, write terminal `Failed` attempt.
5. Either way, the prior attempt's concurrency holders are deleted & released post-commit.

There is **no separate dead-letter queue** — an exhausted job simply lands in `Failed` (recoverable only via `RestartJob`).

---

## 8. CANCELLATION

`cancel_job` (`src/job_store_shard/cancel.rs:33`). gRPC `CancelJob` (`proto/silo.proto:671`). Runs in an SSI transaction. Cancellation is stored **separately** in the `0x0A` flag so dequeue can blindly write `Running` without clobbering it.

- **Preconditions**: job exists, not already cancelled, not *final* (`is_final()`). Monotonic: once cancelled, always cancelled.
- Always writes the `0x0A` cancellation record.
- **Scheduled job** → eager cleanup: set `JobStatus = Cancelled` (with TTL), find & delete the pending task (`0x05`), release any chain-accumulated holders (`0x09`), and if it was waiting in the request queue, delete its `0x08` requests and release upstream holders. Bump `0xF1`; `expire_terminal_job_records`.
- **Running job** → status stays `Running`; only the flag is set. The worker discovers `cancelled: true` on its next heartbeat and reports `Cancelled`, which finalizes the job to `Cancelled`. (The reaper also converts an expired lease of a cancelled job into a `Cancelled` outcome.)
- Post-commit: evict the deleted task from the broker buffer and release in-memory slots + wake the grant scanner.

Note: dequeue deliberately does **not** check cancellation for a stale task — it leases it normally and the worker learns via heartbeat.

---

## 9. EXPEDITE

`expedite_job` (`src/job_store_shard/expedite.rs:49`). gRPC `ExpediteJob` (`proto/silo.proto:683`). Drags a future-scheduled task (or a retry-waiting job) forward to run now.
- **Preconditions**: exists; not final; not cancelled; not `Running`; has a pending task that is future-scheduled (`start_time_ms > now`); not a RefreshFloatingLimit task.
- **Action**: delete the old `0x05` task key, write a new one at `now` (raw byte passthrough — only the key's time component moves), set `JobStatus = Scheduled(now, now, attempt)`, commit, wake the broker.

### Priority
`priority` (0=highest..99=lowest) is the second sort field in the task key after `start_time_ms`, so within a start time, higher-priority tasks lease first. There's no "bump priority" RPC — priority is fixed at enqueue; expedite changes time.

---

## 10. RESTART

`restart_job` (`src/job_store_shard/restart.rs:85`). gRPC `RestartJob` (`proto/silo.proto:676`). Restarts a `Cancelled` or `Failed` job (else `FAILED_PRECONDITION`).
- Computes the next attempt number by scanning existing attempts for the max.
- Resets `relative_attempt_number` to 1 (fresh retry budget).
- Sets `JobStatus = Scheduled`, re-enqueues the limit chain.
- Decrements `0xF1` (reverses the terminal accounting).

---

## 11. IMPORT (bulk, with history)

gRPC `ImportJobs` (`proto/silo.proto:750`). Bulk-imports jobs *with* their historical (terminal) attempts from another system — e.g. migrating from another queue. Implemented in `src/job_store_shard/import.rs`, batched and grouped by (shard, tenant) on the server (`src/server.rs:1444`). Has its own family of Alloy invariants (`import*`/`reimport*`).

---

## 12. Terminal-record TTL (cross-cutting)

When a job goes terminal, its records are re-written with a SlateDB **row TTL** so they're physically dropped during compaction. `compute_terminal_expire_ts` (`src/job_store_shard/mod.rs:130`): `Succeeded → completed_job_expire_s`, `Failed/Cancelled → terminal_job_expire_s`, `Scheduled/Running → None` (never expire). Applied to `0x01/0x02/0x03/0x04/0x07/0x0A`. `RestartJob` decrements the completed counter to keep accounting straight.

> ⚠️ If you set `completed_job_expire_s`/`terminal_job_expire_s`, also set `counter_reconciliation_seconds` — the standalone compactor drops expired rows without a writable DB handle, so counters drift and must be periodically reconciled from ground truth (`src/job_store_shard/mod.rs:581`). See file 05 §6.

---

## 13. The complete RPC list for jobs (`proto/silo.proto`)

| RPC | Purpose |
|-----|---------|
| `Enqueue` | Create a job; scheduled per `start_at_ms`, gated by limits. |
| `GetJob` | Full details incl. status; optionally all attempts. |
| `GetJobResult` | Terminal result; `NOT_FOUND` if absent, `FAILED_PRECONDITION` if not terminal. |
| `DeleteJob` | Permanently delete a job and all its data. |
| `CancelJob` | Cancel any non-final job; running workers notified via heartbeat. |
| `RestartJob` | Restart a Cancelled/Failed job with a fresh schedule. |
| `ExpediteJob` | Move a future/retry-waiting job to run now. |
| `LeaseTask` | Test helper: directly lease one specific job's task. |
| `LeaseTasks` | Primary worker poll: tasks + floating-limit refresh tasks for a task group. |
| `ReportOutcome` | Report success/failure/cancelled; must beat lease expiry. |
| `ReportRefreshOutcome` | Report a new max_concurrency for a floating-limit refresh. |
| `Heartbeat` | Extend lease + learn if the job was cancelled. |
| `ImportJobs` | Bulk-import jobs with historical terminal attempts. |

---

## 14. Two subtleties worth remembering

1. **`JobInfo.enqueue_time_ms`** is set from the raw `start_at_ms`, not the literal enqueue wall-clock; for an immediate job the *status* uses `effective_start_at_ms = now` while info stores the raw value (`enqueue.rs:395`).
2. **The retry-vs-fail boundary is `failures_so_far > retry_count` using `relative_attempt_number`** — a restart genuinely resets the retry budget.

Next: [`05-storage-and-keys.md`](./05-storage-and-keys.md) for what all these keys look like on disk.
