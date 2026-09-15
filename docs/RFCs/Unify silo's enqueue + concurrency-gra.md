# Unify silo's enqueue + concurrency-grant via the intent model

## Context

Silo has two parallel paths that admit a job through its multi-limit chain, and they share no state about the chain:

- **Immediate path** — `JobStoreShard::enqueue_limit_task_at_index` (`src/job_store_shard/enqueue.rs:452`) walks limits in canonical order. For each `Concurrency`/`FloatingConcurrency`, it calls `ConcurrencyManager::handle_enqueue` (`src/concurrency.rs:746`), which on success writes a holder via `append_grant_edits` plus an interim `Task::RunAttempt` at the chain's `task_key`. The walker continues, and each successful limit overwrites the same `task_key` with a fresh interim `Task::RunAttempt` until the terminal branch emits the final one with the full accumulated `held_queues`.
- **Deferred path** — when `try_reserve` fails, `handle_enqueue` writes a `ConcurrencyAction::EnqueueTask` intent record via `append_request_edits` keyed at `concurrency_request_key(tenant, queue, …, suffix)`. The background scanner `ConcurrencyManager::process_grants` (`src/concurrency.rs:1051`) later promotes that request — but the intent carries no chain context, so the scanner fabricates `request_id = "{job_id}:{attempt_number}:{suffix}"` (`src/keys.rs:419-422`), keys the new holder under that synthetic id (`src/concurrency.rs:1324`), and writes a *terminal* `RunAttempt` with `held_queues: vec![queue]` only (`src/concurrency.rs:1335`).

When a chain mixes the two paths (e.g. limits `[Concurrency_A, FloatingConcurrency_B]` where A grants immediately and B has to queue), three bugs fall out as special cases of the same structural problem — the durable intent doesn't carry chain context, and resurrection from the request key forces the scanner to invent identity and forget remaining limits.

*A chain holding queues mid-pause is fine — FIFO at each queue plus eventual resolution (grant or cancel) ensures progress.* The bugs below are not about the mid-pause state; they're about what happens at the **terminal** moment (chain completion or cancel), where identity mismatches make holders un-releasable or limits silently un-enforced.

1. **Permanent holder leak at completion** — A's holder is keyed by `first_task_id` (Uuid generated at `enqueue.rs:371`, also written as the immediate-path `RunAttempt.id`). When B queues, the scanner later promotes it: the new holder is keyed by `request_id = "{job_id}:{attempt}:{suffix}"` from `ParsedConcurrencyRequestKey::request_id()` (`keys.rs:419-422`), and the terminal `Task::RunAttempt` the scanner writes has `id = request_id` and `held_queues = [B]` only (`concurrency.rs:1330, 1335`). When the job runs and `report_attempt_outcome` (`lease.rs:286-288`) releases by `(held_queues × task_id)`, it deletes `(B, request_id)` — A's holder under `first_task_id` is in neither the `held_queues` list nor the matching `task_id` slot and is never reached. Even after the chain completes, A's slot is permanently consumed. Verified by code inspection: no cross-holder release path keys on chain-wide context (job_id, attempt_number).
2. **Limit bypass (silent under-enforcement)** — if the chain were `[A, B, C]` and B queues, the scanner has no view of C; it writes the terminal `RunAttempt` after granting B, silently skipping C. Not a leak — a correctness violation, since C's quota is never enforced for this chain.
3. **`Task::RequestTicket` (future-scheduled)** — same shape as bugs 1 + 2. For `start_at_ms > now_ms`, `handle_enqueue`'s third branch (`concurrency.rs:826-849`) writes only a `RequestTicket` for the first concurrency queue; `process_ticket_request_task` (`concurrency.rs:863`) on grant creates a holder under the synthetic id + a single-queue terminal `RunAttempt`, dropping the rest of the chain (limit bypass) and orphaning any earlier holders that might exist (leak — though structurally `RequestTicket` is the *first* concurrency limit, so usually there are no earlier holders to orphan).

The `specs/job_shard.als` `TicketHolder.th_task: one TaskId` invariant (line 158-162) encodes the right structural property — every holder belongs to one task identity. But the `holdersRequireActiveTask` assertion (line 2115) currently only counts `RunAttempt`, `CheckRateLimit`, and `RequestTicket` as "active tasks" — not `EnqueueTask` durable intents, which today carry no `task_id` to bind anything to. After this refactor, the durable `EnqueueTask` *is* the active marker for a partially-walked chain, so the spec must be updated to count it. The Alloy model changes are listed in the Critical files section; the implementation goal is then to comply with the (updated) assertion.

The recently-merged PR #317 (`b868f2c`: "Delete stale interim RunAttempt when later limit returns TicketRequested") patches the dequeue-races-the-pause symptom (see `record_grant_outcome`'s `queued_with_request && !grants.is_empty()` task-key delete at `enqueue.rs:552-560` and again at `:640-648`); the regression test is `second_limit_request_must_not_leave_runnable_task` (`tests/job_store_shard_concurrency_tests.rs:2420`). This plan goes further and eliminates the interim-RunAttempt write entirely so the dance becomes unnecessary.

## Design: `LimitWalkState` + `WalkOutcome` as the single walker API

Every entry point — initial enqueue, retry, import, rate-limit continuation, scanner resumption, future-ticket grant — constructs the same in-memory `LimitWalkState` value and calls the walker. The walker advances it through limits in canonical order and returns a `WalkOutcome` describing where it stopped. Callers stage durable edits into the same `WriteBatch` and act on the outcome (notify task group, set up immediate lease, roll back reservations).

```rust
struct LimitWalkState {
    tenant: Tenant,
    task_id: Uuid,              // chain identity
    job_id, attempt_number, relative_attempt_number, task_group, priority,
    start_time_ms: i64,         // unchanged across pauses; drives FIFO at every queue
    held_queues: Vec<String>,   // queues already acquired, in canonical order
    next_limit_index: u32,      // resume position into limits[]
    limits: Vec<LimitEntry>,    // *already canonicalized* at construction
}

enum WalkOutcome {
    Resolved,                   // walker wrote terminal Task::RunAttempt
    PausedOnRateLimit,          // walker wrote Task::CheckRateLimit
    PausedOnConcurrency {       // walker wrote a durable EnqueueTask intent
        queue: String,          //   (need queue for task-group wake; counter handling)
    },
    Future {                    // walker wrote Task::RequestTicket; scheduler will wake
        queue: String,
        start_time_ms: i64,
    },
}
```

The walker returns a `WalkResult` carrying everything callers need without introspecting WriteBatch internals:

```rust
struct WalkResult {
    outcome: WalkOutcome,
    reservations: Vec<(String /*queue*/, Uuid /*task_id*/)>,  // in-memory rollback set
    request_record_ops: Vec<RequestRecordOp>,                  // counter-delta source of truth
}
enum RequestRecordOp { Put { queue: String }, Delete { queue: String } }
```

Reservations are *in addition to* the WriteBatch holder edits — they are the side-effect on `ConcurrencyManager`'s in-memory ticket map that must be undone if the batch fails. `request_record_ops` is the walker's authoritative record of every `concurrency_request_key` Put/Delete it appended this call; the scanner driver derives `concurrency_requester_counter_key` deltas from this list (one increment per `Put.queue`, one decrement per `Delete.queue`) rather than parsing WriteBatch mutations.

Walker invariants (encoded as `debug_assert!` at emit sites):
- `WalkOutcome::Resolved` ⇒ exactly one terminal `Task::RunAttempt` is in the WriteBatch at this chain's `task_key`, with `held_queues` equal to the union acquired across all walker calls for this chain.
- `held_queues` ordering is the canonical-acquisition order (queue pushed at success of `process_concurrency_limit`); deterministic across runs, useful for test assertions and log readability.
- `Task::RequestTicket` is emitted only when `next_limit_index` points at the first concurrency limit; its `held_queues` is structurally empty.

```
                    ┌──────────────────────────────────────────────────────────┐
                    │ walk(LimitWalkState, now_ms, skip_first_try_reserve)     │
                    │ → (WalkOutcome, Vec<reservation>)                        │
                    │                                                          │
                    │ loop over limits[state.next_limit_index..]:              │
                    │   Concurrency / FloatingConcurrency:                     │
                    │     - if first concurrency seen AND start_time > now:    │
                    │         emit Task::RequestTicket(full state),            │
                    │         return Future.                                   │
                    │     - if skip_first_try_reserve AND this is first        │
                    │       concurrency limit seen this call: skip reserve →   │
                    │       emit EnqueueTask(full state), return Paused.       │
                    │     - process_concurrency_limit(state, limit_idx, batch) │
                    │       does: floating state setup, refresh scheduling,    │
                    │       queue-limit cache update, try_reserve, holder edit │
                    │       on success.                                        │
                    │     - on success: state.held_queues.push(queue);         │
                    │                   state.next_limit_index += 1;           │
                    │     - on capacity-exhausted: emit EnqueueTask(state) at  │
                    │       concurrency_request_key, return Paused.            │
                    │   RateLimit:                                             │
                    │     - emit Task::CheckRateLimit(state), return.          │
                    │   end of limits:                                         │
                    │     - emit Task::RunAttempt(state.task_id,               │
                    │       held_queues=state.held_queues), return Resolved.   │
                    └──────────────────────────────────────────────────────────┘
                       ▲             ▲             ▲             ▲
   initial enqueue     retry         scanner       RequestTicket CheckRateLimit
   build state         build state   resume:       grant:        re-entry: build
   from job,           skip_first_   decode        decode        state from
   index=0,            try_reserve   EnqueueTask   RequestTicket CheckRateLimit
   held_queues=[]      = true        payload →     payload →     payload (already
                                     walk          walk          carries everything)
                                                                 → walk
```

### `process_concurrency_limit` shared helper

The bulk of today's `handle_enqueue` body (`concurrency.rs:746-849`) factors into `process_concurrency_limit(state, limit_idx, batch) -> ProcessLimit { Granted | CapacityExhausted | RetryDeferred }`. This helper owns: floating-concurrency state resolution + `current_max` derivation (today's `concurrency.rs:769-799`); refresh scheduling for floating windows; queue-limit cache updates; `try_reserve_internal` call; holder edit append on success. The walker calls it; the scanner does not — scanner just calls the walker, which calls the helper. This addresses **review issue #3**: scanner does not pre-grant outside the walker; floating-concurrency setup runs identically for scanner-resumed and initial-path executions.

### Canonical ordering

`LimitWalkState.limits` is stored *already in canonical order*. The walker indexes directly: `state.limits[state.next_limit_index]`. Canonicalization runs once at construction time (initial enqueue, retry, import, rate-limit continuation re-entry). The scanner and ticket-processor decode payloads that were already canonical when written, so no re-canonicalization on resume. This addresses **review issue #6** (no `order[next_limit_index]` indirection).

When the scanner picks up an `EnqueueTask` from `concurrency_request_key(tenant, queue_k, …)`, it validates `state.limits[state.next_limit_index].queue == queue_k` before calling the walker. Mismatch → log + skip + delete the orphaned record (defensive; should be unreachable).

### Future scheduling

Future scheduling stays on `Task::RequestTicket`, driven by the existing scheduler's `start_time_ms` wake-up. The walker, when called with `start_time_ms > now_ms` and arriving at the *first concurrency limit*, emits `Task::RequestTicket` (carrying the full `LimitWalkState` payload) and returns `Future`. The scheduler wakes the task at `start_time_ms`; `process_ticket_request_task` decodes the payload → calls the walker → walker treats `start_time_ms <= now_ms` and proceeds normally. This addresses **review issue #2**: one mechanism for time waits (scheduler-driven `RequestTicket`), one mechanism for capacity waits (scanner-driven `EnqueueTask`), walker chooses based on `start_time_ms` vs `now_ms`.

`RequestTicket.held_queues` is structurally empty (`start_time_ms` is per-job and gates the *first* concurrency limit; no holders are taken before that). Cancel still must handle it for safety — see Cancel path below.

### WalkOutcome consumers

- **`write_enqueue_data`** (initial enqueue): doesn't branch on outcome — caller proceeds with whatever the WriteBatch contains.
- **Scanner**: on `Resolved` or `PausedOnRateLimit`, the task group whose work was admitted must be woken (notify worker pool). On `PausedOnConcurrency` at a *different* queue than the one that was resolved, that queue's request counter is incremented by the walker's request-edit append; on the resolved queue, the request counter is decremented (mirroring today's `concurrency.rs:1316-1322`). The driver collects these counter deltas from the walker's `WriteBatch` mutations.
- **`process_ticket_request_task`**: on `Resolved`, immediately create a lease for the just-promoted task (today's `dequeue.rs:412-446` path). On other outcomes, commit and return; scanner or scheduler will wake the next stage.
- **`handle_check_rate_limit`**: outcome is informational; caller doesn't act on it directly (rate-limit continuation either resolves or pauses again, both committed via the WriteBatch).

### Counter management on scanner resumption

Today's grant pass decrements the request counter for each promoted request (`concurrency.rs:1316-1322`) under the assumption: one request resolved → one counter decrement on its queue. After this refactor, scanner-resumed walks can:

1. Resolve the request (most common) → decrement counter on the resolved queue.
2. Pause again on the *same* queue (e.g. floating-concurrency window shrinks mid-walk) → no net counter change; existing request key stays.
3. Pause on a *different, later* queue → decrement counter on resolved queue, increment counter on new pause queue. Both the request-record delete and the new request-record write happen in the walker's WriteBatch; the scanner driver derives per-queue counter deltas from `WalkResult.request_record_ops` (each `Put.queue` → counter +1, each `Delete.queue` → counter −1) and appends counter mutations to the same batch before commit. This addresses the **counter management gap** without batch introspection.

There is no `Task::ContinueLimits` and no broker round-trip for scanner-driven resumption — the walker runs to its next pause inside the scanner's same `WriteBatch`.

### Identity unification

The chain's UUIDv4 `task_id` (minted at `enqueue.rs:371` initial, `lease.rs:222` retry, `import.rs:258/634` import) is the single identity throughout:

- Every holder for every queue the chain acquires is `concurrency_holder_key(tenant, queue, task_id)`.
- `EnqueueTask.task_id` (new field) carries it through the durable intent.
- `Task::RunAttempt.id`, `Task::CheckRateLimit.task_id`, `Task::RequestTicket.task_id` (renamed from `request_id`) all equal it.

The `suffix` in `concurrency_request_key` stays purely as a key-uniqueness tiebreaker and never escapes the key. `ParsedConcurrencyRequestKey::request_id()` (`src/keys.rs:419-422`) loses its current role as identity source — consumers needing a task identity read `task_id` from the decoded `EnqueueTask` payload instead. (Audit map below.)

### Payload extensions

All three durable types that carry walker state — `ConcurrencyAction::EnqueueTask`, `Task::CheckRateLimit`, `Task::RequestTicket` — encode the same `LimitWalkState` shape so the walker can resume from any of them. Today `CheckRateLimit` already carries most of it; this refactor brings `EnqueueTask` and `RequestTicket` into alignment.

**`ConcurrencyAction::EnqueueTask`** (`src/task.rs:141-153`, schema `schema/internal_storage.fbs:233-240`, codec `src/codec.rs:467`) gains:

```
table EnqueueTask {
  start_time_ms, priority, job_id, attempt_number, relative_attempt_number, task_group;  // existing
  task_id: string;                  // chain identity
  held_queues: [string];            // queues already acquired
  next_limit_index: uint32;         // walker resume position
  limits: [LimitEntry];             // *canonical order at construction*; walker indexes directly
}
```

`LimitEntry` already exists for `JobInfo.limits`; reuse it. Canonicalization runs once at the construction site (initial enqueue, retry, import); decoded values are already canonical and the walker does not reorder.

**`Task::RequestTicket`** (`task.rs:28-40`, schema `internal_storage.fbs:81-91`) gains `task_id` (renamed from `request_id`), `next_limit_index`, and `limits`. It does *not* gain `held_queues` — `start_time_ms` gates the first concurrency limit, before any holders are taken, so a `RequestTicket` mid-flight always has `held_queues = []`. (If a future change ever lets `RequestTicket` exist mid-chain, add the field then.)

The rename `request_id` → `task_id` is a semantic change, not an append-only field addition. The new field's slot may be wire-compatible with the old, but consumers of the accessor change. Pre-launch DB nuke covers the data side; the code change is mechanical (audit map in Critical files).

### Shrinkages from the walker becoming the single entry

- `handle_enqueue` (`concurrency.rs:746`) is dissolved: its concurrency-limit body becomes `process_concurrency_limit` (called by the walker); its `Task::RunAttempt`-writing branch is removed entirely; its `Task::RequestTicket` branch moves into the walker's `Future` outcome.
- `append_grant_edits` (`concurrency.rs:1421`) shrinks to a holder-only put; drop lines 1440-1453 (the `Task::RunAttempt` construction and `put_task`).
- `append_request_edits` (`concurrency.rs:1459`) is extended to encode the new payload fields.
- `record_grant_outcome` + `GrantResult` (`enqueue.rs:69-117`) are removed entirely; the walker mutates `LimitWalkState` directly instead of routing through the outcome enum.
- `LimitTaskParams` (`enqueue.rs:28-46`) is replaced by `LimitWalkState` + the two runtime args (`now_ms`, `skip_first_try_reserve`). Tenant lives inside `LimitWalkState` (in the in-memory shape only; the durable wire format derives tenant from the request/task key).
- PR #317's stale-task-key delete dance at `enqueue.rs:552-560` and `:640-648` is removed — no interim `task_key` is ever written, so nothing to clean up on pause.

### Initial enqueue (`write_enqueue_data`)

`write_enqueue_data` (`enqueue.rs:344`) builds an in-memory `LimitWalkState`:

```rust
let state = LimitWalkState {
    tenant, task_id: first_task_id, job_id, attempt_number: 1,
    relative_attempt_number: 1, priority, start_time_ms: start_at_ms, task_group,
    held_queues: vec![], next_limit_index: 0,
    limits: canonicalize(job.limits.clone()),
};
self.walk(writer, state, now_ms, /* skip_first_try_reserve */ false).await
```

Walker advances. Full grant → batch contains holders + `Task::RunAttempt`. Partial grant → batch contains holders for granted limits + `EnqueueTask` durable record at the paused queue's `concurrency_request_key`. Future-scheduled → batch contains a single `Task::RequestTicket` at the first concurrency queue, no holders. No interim task ever exists.

### Scanner resumption (`process_grants`)

Today's `process_grants` (`concurrency.rs:1051`) lives on `ConcurrencyManager`, which doesn't own the walker. Restructuring:

- Keep `ConcurrencyManager` owning `pending_grants`, `grant_notify`, `try_reserve_internal`, hydration. The in-memory primitives stay there. Expose `drain_pending() -> Vec<(tenant, queue, count)>` if needed.
- Move the grant-scanner driver out of `ConcurrencyManager::start_grant_scanner` into `JobStoreShard`. `JobStoreShard::open_internal` (`src/job_store_shard/mod.rs:457`) spawns it, holding a `Weak<JobStoreShard>` so it can call the walker. Pattern mirrors `spawn_concurrency_reconcile_task` (`mod.rs:479`). Shutdown wiring goes through `JobStoreShard::close` (`mod.rs:520`), replacing `stop_grant_scanner` (`concurrency.rs:979`).

New per-pass shape (one `WriteBatch` per pass):

1. Scan up to `MAX_GRANTS_PER_PASS` candidates from the request prefix (unchanged).
2. Validate via the existing `[SILO-GRANT-5]` status gate (unchanged).
3. For each valid candidate: decode the (extended) `EnqueueTask` payload into a `LimitWalkState`. Validate consistency: payload `tenant`, `job_id`, `attempt_number`, `priority`, `start_time_ms` match the request key's parsed fields, and `state.limits[state.next_limit_index].queue` matches the request key's queue. Mismatch → log + delete the orphan record + skip.
4. Call the walker against the same `WriteBatch`. The walker — not the scanner — handles the paused limit via `process_concurrency_limit`, which runs the full floating-concurrency logic (state setup, refresh scheduling, queue-limit cache, `try_reserve_internal`). On success it pushes the queue to `held_queues`, increments the index, and continues; on capacity-exhausted it emits the request-record edit and returns. (This addresses **review issue #3**.)
5. Apply per-queue counter deltas from each candidate's `WalkResult.request_record_ops` (Put → +1, Delete → −1) as `concurrency_requester_counter_key` mutations in the same batch.
6. Commit. On batch-write failure, roll back *every* in-memory reservation accumulated this pass — concatenate each candidate's `WalkResult.reservations` into a per-pass set and release all of them. Extend the existing rollback at `concurrency.rs:1381-1383` accordingly.
7. **Post-commit only** — for each candidate whose outcome was `Resolved` or `PausedOnRateLimit`, wake the corresponding task group. Wake-ups never fire before commit (avoids workers picking up a row that the failing batch would have rolled back). Today's pending-grant decrement at `concurrency.rs:1316-1322` is subsumed into step 5.

### Future-scheduled path (`process_ticket_request_task`)

`process_ticket_request_task` (`concurrency.rs:863`) on scheduler wake-up: decode the (extended) `RequestTicket` payload into `LimitWalkState` (with `held_queues: vec![]`, `next_limit_index: 0`), call the walker with `now_ms >= start_time_ms`. Walker runs `process_concurrency_limit` for the first concurrency limit (the one the ticket was waiting on) and continues. `handle_request_ticket` in `dequeue.rs:388-476` inspects `WalkOutcome`: on `Resolved`, immediately create a lease (today's `:412-446` path); on any other outcome, just commit and let the scanner/scheduler handle further wake-ups.

### Rate-limit continuation (`handle_check_rate_limit`)

`handle_check_rate_limit` (`src/job_store_shard/dequeue.rs:482-661`) already builds the walker-input shape today (`LimitTaskParams` at `:565-579`). Replace with `LimitWalkState` constructed directly from the `CheckRateLimit` fields (which already carry `task_id`, `held_queues`, `limit_index`), pulling `limits` from `job_info` and canonicalizing once. Walker re-entry is otherwise unchanged.

### Retry path

`lease.rs:228`'s call sets `skip_first_try_reserve: true`. When the walker hits the first concurrency limit in a retry chain, `skip_first_try_reserve` short-circuits straight to the pause branch (write `EnqueueTask` intent, no `try_reserve`). This protects `[SILO-RETRY-5-CONC]`: the old holder is still in-memory (released post-commit). The flag only suppresses the *first* `try_reserve` the walker reaches; subsequent limits in the same chain reserve normally — but those don't happen synchronously here because the chain has already paused at limit 0. (If the future schedules retries to reserve later limits eagerly, that's a separate design decision; preserve today's "pause-and-defer" semantics.)

### Cancel and reimport cleanup

Today `cancel_job` (`src/job_store_shard/cancel.rs:130-176`) finds the chain's task in the queue and:
- `Task::RunAttempt`: deletes its holders.
- `Task::RequestTicket`: deletes only the task. (Currently safe because today's `RequestTicket` carries no holders; will stay safe after the refactor for the same reason.)
- `Task::CheckRateLimit`: ⚠️ **pre-existing bug** — today's cancel does *not* release the `held_queues` a `CheckRateLimit` carries. Worth fixing in this refactor since the cleanup path is being touched anyway.
- No-task-found (`TicketRequested`): calls `delete_concurrency_requests_for_job` which scans by `(tenant, queue, job_id, attempt_number)` prefix and deletes request records — *but* today's records carry no holders, so there's nothing to release beyond the records themselves.

After the refactor, the durable records that carry `held_queues` (and therefore require holder release on cancel) are:
- `Task::RunAttempt` — full chain (already handled).
- `Task::CheckRateLimit` — partial chain (fix the pre-existing bug here).
- `ConcurrencyAction::EnqueueTask` — partial chain (new with this refactor).

`delete_concurrency_requests_for_job` (today in `cancel.rs:219`, also called from reimport at `import.rs:600`) is upgraded to *read* each `EnqueueTask` record before deleting, decode `task_id` + `held_queues`, and *return* the decoded chain references to the caller (so cancel and reimport both apply holder deletes + post-commit in-memory release for those queues, deduplicated against any already released for the same `task_id`). Reimport uses the same helper, so this lines up automatically. (This addresses **review issue #5**.)

Cancel of `Task::CheckRateLimit` follows the same pattern: read the task, decode `held_queues` + `task_id`, append holder deletes, then delete the task. Cancel of `Task::RequestTicket` continues to delete only the task (held_queues is structurally empty).

### FIFO at pause points

A chain that pauses at limit_index `k` writes its intent at `concurrency_request_key(tenant, queue_k, original_start_time_ms, priority, job_id, attempt_number, suffix)`. The walker threads `start_time_ms` unchanged through the `EnqueueTask` payload, so a chain that took real wallclock time to walk earlier limits still races FIFO by job age — paused chains aren't penalized for chain progress.

### Stale limits in durable payloads

`EnqueueTask` / `RequestTicket` / `CheckRateLimit` all duplicate the chain's `limits` so the walker doesn't have to re-read `job_info`. Risk: if the job's limits change between when the record was written and when the walker resumes, the walker uses stale data. Mitigation:
- Job status is the validity gate (`[SILO-GRANT-5]`). A cancelled or completed job's records are skipped + deleted regardless of payload.
- Reimport / job mutation paths aggressively delete all durable chain records for a given `(job_id, attempt_number)` via the upgraded `delete_concurrency_requests_for_job`. Limits-changed-mid-flight is reduced to "record exists with old limits AND status still gates it" — a small window relative to today, where the same data is read indirectly via the same `job_info` field.

If this proves too risky, the alternative is "look up `limits` from `job_info` on resume" — but that re-introduces the inconsistency `CheckRateLimit` already accepts today, so duplication is the simpler choice.

### Schema baseline (no migration)

Silo is pre-production and this change ships on net-new shards — there are no existing durable records to migrate or nuke. The schema additions (`EnqueueTask` gaining `task_id` / `held_queues` / `next_limit_index` / `limits` / `limits_fingerprint`; `RequestTicket` renaming `request_id` → `task_id` and gaining chain fields) become the baseline for every shard from first boot.

For *future* (post-launch) schema evolution, the additions are still done append-only by Flatbuffer convention so later upgrades remain backwards-compatible. The `RequestTicket` rename is irreversible in code (consumers switch accessors), so any later wire-level concern would be a separate forward-only decision; not relevant now.

`concurrency_holder_key`'s on-disk *value* (`HolderRecord { granted_at_ms }`) is unchanged; only the key's `task_id` slot is now uniformly the chain's UUID. `concurrency_requester_counter_key` semantics are unchanged.

## Hardening invariants & observability

These guards must ship with the implementation rather than be retrofitted. Listed once here so they aren't lost between design and code review.

### Payload validation at every decode boundary

`EnqueueTask`, `RequestTicket`, and `CheckRateLimit` are all walker-resumption inputs; each decode site validates and on failure: structured warn log + delete the orphan record + increment `silo_walker_payload_malformed_total{kind, reason}` + skip.

- `next_limit_index <= limits.len()` (equality means terminal-pending; strictly less means a queue must match below).
- If `next_limit_index < limits.len()`: for `EnqueueTask`, `limits[next_limit_index].queue == parsed_request_key.queue`; for `RequestTicket`, `limits[next_limit_index]` is the first concurrency limit.
- `task_id` non-empty and parses as a Uuid.
- Payload key-fields (`tenant`, `job_id`, `attempt_number`, `priority`, `start_time_ms`) match the parsed request/task key.
- `RequestTicket.held_queues` is empty (debug-assert; structural invariant per the design section).

### Limits fingerprint

Add `limits_fingerprint: u64` (xxhash of the canonical `limits` payload bytes) to `EnqueueTask`, `RequestTicket`, and `CheckRateLimit`. At decode sites that already read `job_info` (cancel, reimport, scanner's status-gate), compare against `hash(canonicalize(job_info.limits))`. Mismatch → warn + `silo_walker_limits_fingerprint_mismatch_total{kind}`; policy: prefer the durable payload's `limits` (already canonical, already what the chain was walking) and rely on status-gate cleanup to retire the record. The fingerprint is informational; hot paths (scanner per-candidate) do not pay an extra `job_info` read for it.

### Idempotency contract for scanner reprocessing

Scanner reprocessing is idempotent by construction:
- Walker actions are keyed on durable identity (`concurrency_holder_key(tenant, queue, task_id)`, `concurrency_request_key(...)`, `task_key`). A re-run after a partial pass either finds the request record already deleted (status-gate skips trivially) or re-runs the walk producing a bitwise-equivalent WriteBatch.
- `try_reserve_internal`'s existing pending-grant dedupe prevents double in-memory reservation on the same `(queue, task_id)` within the same `ConcurrencyManager` instance.
- After restart, in-memory reservations are gone; `reconcile_pending_requests` (`concurrency.rs:988`) rebuilds counters from durable records.

Stated invariant (encoded in `test_scanner_idempotent_replay`, not a debug-assert because it spans passes): the per-queue durable request-record set after committing a pass equals the pre-pass set minus all `Delete` op queues plus all `Put` op queues across all candidates that pass committed. Replaying the same pass on the post-commit state is a no-op.

### Cancel-path dedupe

Cancel collects holders to release as a `HashMap<(queue, task_id), ()>` across `Task::RunAttempt.held_queues`, `Task::CheckRateLimit.held_queues`, and every chain `EnqueueTask.held_queues`. Multiple records referencing the same `(queue, task_id)` coalesce into one holder delete + one in-memory release. Without this, cancel of a chain holding both a paused `EnqueueTask` and a stale `CheckRateLimit` with overlapping `held_queues` would double-release. The `(queue, task_id)` tuple is the explicit dedupe key.

### Scanner driver lifecycle

The relocated driver (now on `JobStoreShard`) has explicit lifecycle:
- **Startup ordering** — spawned by `open_internal` *after* `reconcile_pending_requests` finishes hydration and *after* the existing reconcile task is spawned, so the first walker pass sees consistent counters.
- **Stop signal** — `close` flips an `AtomicBool` and notifies via a `Notify` (mirrors today's `stop_grant_scanner` at `concurrency.rs:979`). The driver checks the signal *between candidates within a pass*, not just between passes, to bound shutdown latency under high candidate counts.
- **Join timeout** — `close` awaits the driver's `JoinHandle` with a 5s deadline. On timeout: structured warn + force-abort. The in-flight pass's WriteBatch is dropped (commit is atomic, so no half-applied state).
- **No-double-scanner invariant** — `JobStoreShard` holds a single `Option<JoinHandle>` taken on close; any second spawn attempt panics in debug, logs + skips in release.

### Observability counters

All `silo_*`-namespaced:
- `silo_walker_payload_malformed_total{kind, reason}` — guardrail trip at decode.
- `silo_walker_limits_fingerprint_mismatch_total{kind}` — limits changed mid-flight.
- `silo_scanner_reservation_rollback_total{reason}` — batch write failure → in-memory release.
- `silo_counter_drift_corrected_total{queue}` — `reconcile_pending_requests` adjustments at restart.
- `silo_holder_release_miss_total{reason}` — `report_attempt_outcome`'s debug-assert site (logs + counts even when the assert is compiled out, so production sees mismatches).
- `silo_scanner_pass_walker_calls_total{outcome}` — `WalkOutcome` variant distribution.

### `request_id` → `task_id` log audit

The rename is a semantic change, not a wire-only field swap. Before merging, audit `src/` and `tests/` for every log site emitting `request_id` (grep both `request_id =` style and `tracing::field("request_id"…)`/`%request_id`/`?request_id` interpolations) and rename to `task_id`. Do not alias — mixed naming during rollout causes confusion in production triage.

## Critical files

- `src/concurrency.rs` — shrink `handle_enqueue` (drop task writes), shrink `append_grant_edits` (holder only), extend `append_request_edits` (4 new fields), restructure `process_grants` to construct `EnqueueTask` from durable payload and call walker, restructure `process_ticket_request_task` similarly, extract scanner driver out to `JobStoreShard`, expose `drain_pending`.
- `src/job_store_shard/enqueue.rs` — `enqueue_limit_task_at_index` takes an `EnqueueTask` value; `write_enqueue_data` constructs the initial value; remove `LimitTaskParams` / `record_grant_outcome` / `GrantResult` (`:28-117`), remove PR #317's task-key-delete dance (`:552-560`, `:640-648`).
- `src/job_store_shard/mod.rs` — spawn grant-scanner driver here (replace `:457`); `Weak<JobStoreShard>` drives walker calls; wire shutdown into `close` (`:520`).
- `src/job_store_shard/dequeue.rs` — `handle_request_ticket` (`:351`) consumes extended `RequestTicket` and calls walker; `handle_check_rate_limit`'s walker re-entry (`:563`) constructs `EnqueueTask` from the `CheckRateLimit` fields.
- `src/job_store_shard/lease.rs` — retry call site (`:228`) updated to construct `EnqueueTask`; add debug-assert in release loop (`:286-288`) that each holder key existed.
- `src/job_store_shard/cancel.rs` — `delete_concurrency_requests_for_job` (`:219`) decodes `EnqueueTask` payloads, returns decoded `(task_id, held_queues)` to callers, and queues holder deletes for `held_queues`. `cancel_job` (`:130`) also fixes the pre-existing `Task::CheckRateLimit` holder-leak by decoding `held_queues` from the task before deletion and releasing them.
- `src/job_store_shard/import.rs` — both walker call sites (`:261`, `:641`) construct `EnqueueTask` values; no logic change.
- `src/task.rs` — extend `ConcurrencyAction::EnqueueTask` (4 fields + tenant), extend `Task::RequestTicket` (rename `request_id` → `task_id`, add chain fields).
- `src/codec.rs` — encode/decode for both extended types; existing pattern around `:467` and `RequestTicket` codec.
- `src/keys.rs` — leave `parse_concurrency_request_key` intact; consumers that read identity must instead decode the `EnqueueTask` value:
  - `query.rs:1630` — column comes from the request record; decode `EnqueueTask` and use its `task_id`. (Requires reading the value, not just the key.)
  - `concurrency.rs:1163` — replace with payload's `task_id`.
  - `dequeue.rs:371` — uses `RequestTicket.request_id()`; switch to renamed `task_id` accessor.
  - `codec.rs:592` — accessor on the `RequestTicket` codec follows the schema rename.
  - `cleanup.rs:154` — uses only `parsed.tenant`; no change.
- `schema/internal_storage.fbs` — extend `EnqueueTask` and `RequestTicket` tables.
- `specs/job_shard.als` — model update: amend the predicate(s) underlying `holdersRequireActiveTask` (line 2115) so a durable `EnqueueTask` request record with a non-empty `task_id` counts as an "active task" capable of holding the chain's holders. Concretely, extend whatever today enumerates active tasks (the union over `RunAttempt`, `CheckRateLimit`, `RequestTicket`) to also include `EnqueueTask` request records bound to a `task_id`. Then re-check the assertion. Without this, the assertion fails on legitimate post-refactor states (chain paused mid-walk with holders + EnqueueTask).

## Risks

- **Per-pass scanner rollback spans multiple queues' reservations.** Today rollback (`concurrency.rs:1381-1383`) iterates `grants` for the single queue the pass was promoting. With walker-driven resumption a pass may reserve across multiple queues (scanner promotes FC1 candidate; walker advances into FC2; FC2 reserves in-memory; batch write fails). The pass must track every reservation accumulated across all candidates and release all of them.
- **`Task::RequestTicket` semantic change.** Future-scheduled multi-limit chains will now correctly walk all limits at grant time. Today they silently bypass; tests that assert the bypass will need updates.
- **Scanner driver relocation.** Moving the spawn from `ConcurrencyManager::start_grant_scanner` to `JobStoreShard` changes shutdown ordering. `stop_grant_scanner` (`concurrency.rs:979`) flips an `AtomicBool` + notifies; the relocated driver needs equivalent shutdown wiring through `JobStoreShard::close` (`mod.rs:520`).
- **Counter accuracy across multi-queue pauses.** Counter deltas now derive from the walker's explicit `request_record_ops` artifact, not WriteBatch introspection (see Walker output contract). A miscount would mean the walker emitted inconsistent ops vs WriteBatch writes — caught by a debug-assert that pairs each `Put`/`Delete` op with the matching batch mutation, plus the `silo_counter_drift_corrected_total` metric exposing reconciler corrections at restart.
- **Stale-limits window** on durable payloads carrying `limits` (discussed in design). Status-gate is the validity guard.

## Verification

1. **New regression cases** in `tests/holder_leak_tests.rs` and `tests/job_store_shard_concurrency_tests.rs`:
   - `[C immediate, FC queued]` → both holders share `task_id`; both released on terminal outcome.
   - `[C immediate, FC1 queued → granted, FC2 queued → granted, terminal]` → three holders all keyed by chain's `task_id`.
   - `[C immediate, FC queued, R rate-limit]` → granted concurrency holders survive the rate-limit pause; `CheckRateLimit` carries them; cancel during rate-limit pause releases all of them (also exercises the pre-existing CheckRateLimit cleanup fix).
   - Mid-pause crash + restart: `reconcile_pending_requests` (`concurrency.rs:988`) re-emits grant signals; the next scanner pass resumes from the durable `EnqueueTask` payload.
   - Future-scheduled `[C, FC]` multi-limit job: `RequestTicket` fires at `start_time_ms`; walker walks both limits at grant time.
   - Scanner-resumed walk that pauses on a *different* queue (FC1 resolves → walker tries FC2 → FC2 capacity-exhausted): old request record at FC1 deleted, new at FC2, counter deltas correct on both queues.
   - Cancel during mid-pause: all chain holders torn down by `task_id`, all durable chain records deleted, counters decremented on every queue with a request.
   - Reimport during mid-pause: same as cancel cleanup, then new chain enqueued.
2. **Strengthen `second_limit_request_must_not_leave_runnable_task`** (`tests/job_store_shard_concurrency_tests.rs:2420`): assert "no `task_key` record ever exists between chain-pause and terminal-walker write" — true by construction after this refactor.
3. **Run full concurrency suites**: `concurrency_tests`, `floating_concurrency_tests`, `concurrent_grant_race_tests`, `concurrency_counts_tests`, `concurrency_requester_counter_tests`, `holder_leak_tests`, `job_store_shard_concurrency_tests`, `job_store_shard_retry_tests`, `cancel_tests` (or equivalent), `import_tests`. All should pass.
4. **Re-run `specs/job_shard.als`** with the updated `holdersRequireActiveTask` predicate (EnqueueTask counted as active). Assertion should pass; verify by deliberately breaking the implementation (skip holder release on cancel) and confirming the model catches it.
5. **Codec roundtrip tests** in `tests/codec_tests.rs` for both extended payloads, including: empty `held_queues`, populated `held_queues`, `next_limit_index` at boundaries (0, len, len-1), canonical-order `limits`, fingerprint stability across encode/decode.
6. **Malformed-payload guardrails** (`tests/walker_guardrails_tests.rs`, new): exercise each decode-site validation:
   - `next_limit_index > limits.len()` → record deleted, malformed counter incremented, no panic.
   - `EnqueueTask` with `limits[next_limit_index].queue` ≠ parsed key's queue → same.
   - Empty `task_id` → same.
   - `RequestTicket` with non-empty `held_queues` → debug-assert fires (test runs in debug build).
   - `limits_fingerprint` mismatch at cancel-time → warn metric, payload's `limits` used.
7. **Idempotent scanner replay** (`test_scanner_idempotent_replay`): drive a pass to completion, snapshot durable state, replay the same pass from the post-commit state, assert WriteBatch is empty + no in-memory side effects + counters unchanged.
8. **Batch-fail rollback across multiple queues**: inject a write-failure after the walker reserved on two queues in the same candidate; assert both in-memory reservations released, no holders persisted, request record at the resumed queue unchanged, `silo_scanner_reservation_rollback_total` incremented.
9. **Shutdown race**: `JobStoreShard::close` while a scanner pass has reserved on multiple queues. Assert (a) no leaked in-memory reservations after close returns, (b) durable state consistent (either fully-committed pass or fully-rolled-back), (c) join completes within the 5s timeout.
10. **Manual smoke** at high contention with `enqueue_with_two_concurrency_limits_grants_both` to verify capacity reclaim across the immediate/scanner path mix (no leaks in `snapshot_holders` metrics).
11. **Counter audit**: after each scanner pass test, snapshot `concurrency_requester_counter_key` values per queue and verify they equal the count of durable `EnqueueTask` records at that queue.