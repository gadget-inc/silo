# Lifecycle of a job in silo: `[Concurrency, FloatingConcurrency]`, not immediately grantable

A naming correction up front, since it changes how you read everything below: there is **no `GrantOutcome` enum**. The concurrency layer returns `RequestTicketOutcome` (`src/concurrency.rs:210`); the shard chain-walker has a tiny internal `GrantResult { Granted, Queued }` (`src/job_store_shard/enqueue.rs:75`). Your instinct ("it does `GrantOutcome::Queued`") maps to the chain-walker's `GrantResult::Queued`, produced when the concurrency layer returns `RequestTicketOutcome::TicketRequested`.

Also key to the whole trace: **limits are walked in client order, one at a time, and the walk stops at the first limit that can't be granted.** Each granted limit writes its own holder; only when *all* limits are satisfied does a terminal `RunAttempt` task get written. So with `[Concurrency, FloatingConcurrency]` and the concurrency limit full, the walk stops at index 0 and the floating limit is never even looked at yet.

---

## 1. The enqueue RPC — what gets written, and the `Queued` outcome

The gRPC handler is `src/server.rs:812`. It converts proto limits into domain `Limit`s **preserving client order**, then calls `shard.enqueue(...)`:

```rust
// src/server.rs:812
let limits: Vec<crate::job::Limit> = r.limits.into_iter()
    .filter_map(proto_limit_to_job_limit).collect();
let id = shard.enqueue(&tenant, /*id*/, r.priority as u8, r.start_at_ms,
                       retry, payload_bytes, limits, metadata, &r.task_group).await?;
```

`shard.enqueue` (`src/job_store_shard/enqueue.rs:144`) writes the **durable job facts** via `write_enqueue_data` (`enqueue.rs:359`), in one `WriteBatch` (auto-UUID) or `SerializableSnapshot` txn (caller-supplied id, with a dedup check):

- `job_info_key(tenant, job_id)` → encoded `JobInfo`, **including the full `limits` list** (`enqueue.rs:396`). This is the record everything later reads back from.
- metadata secondary-index rows (`enqueue.rs:400`).
- `JobStatus::scheduled(...)` plus its status-time index (`enqueue.rs:404`).
- a total-jobs counter bump, and a two-phase `DstEvent::JobEnqueued`.

Then, in the *same* batch, it begins the **limit chain walk** at `limit_index: 0` — this is where tickets/holders get written.

**The chain walker** is `walk_limit_chain` (`enqueue.rs:514`). The control structure is the spine of the whole system:

```rust
// src/job_store_shard/enqueue.rs:561
loop {
    if current_index >= limits.len() {
        // No more limits - enqueue RunAttempt
        let run_task = Task::RunAttempt { id: current_task_id, tenant, job_id,
            attempt_number, relative_attempt_number,
            held_queues: current_held_queues, task_group };
        put_task(writer, task_group, task_key_start_ms, priority, job_id, attempt_number, &run_task)?;
        return Ok(Some(task_key_start_ms));
    }
    match &limits[current_index] {
        Limit::Concurrency(cl) => { /* try immediate grant */ }
        Limit::FloatingConcurrency(fl) => { /* ... */ }
        Limit::RateLimit(rl) => { /* write CheckRateLimit task, return */ }
    }
}
```

- The `if current_index >= limits.len()` branch is the **terminal exit**: only reached when every limit has been granted; it writes the `RunAttempt` and stops. This is the single place a `RunAttempt` is born.
- Otherwise it dispatches on the limit *at the current index* — the walk is strictly ordered and resumable, because `current_index` is a direct index into the persisted `limits` array.

For your scenario, `current_index = 0` hits `Limit::Concurrency(cl)` (`enqueue.rs:586`). It calls `handle_enqueue` with **a single-element slice** `std::slice::from_ref(cl)` (`enqueue.rs:601`) — that's why `handle_enqueue` only ever gates on `limits.first()`.

Inside `handle_enqueue` (`src/concurrency.rs:958`), the not-grantable decision:

```rust
// src/concurrency.rs:1013
if !skip_try_reserve
    && self.counts.try_reserve(db, range, tenant, queue, task_id, max_allowed, job_id).await?
{
    if scheduled_at_ms > now_ms { self.rollback_grant(tenant, queue, task_id); }
    else { /* GrantedImmediately: append_grant_edits + return */ }
}
if scheduled_at_ms <= now_ms {
    // src/concurrency.rs:1054  Queue is at capacity → create request record
    append_request_edits(writer, tenant, queue, scheduled_at_ms, priority, job_id,
        attempt_number, relative_attempt_number, task_group,
        limit_index, held_queues, task_id, all_limits)?;
    Ok(Some(RequestTicketOutcome::TicketRequested { queue: queue.clone() }))
}
```

- `try_reserve` is the atomic capacity gate. Since the queue is full, it returns `false`, so the whole `if` is skipped.
- `scheduled_at_ms <= now_ms` (present-time job at capacity) → `append_request_edits` writes a **request record** and returns `TicketRequested`. **This is your "Queued" path.** (If the job were future-scheduled, the `else` branch writes a `Task::RequestTicket` into the task queue instead, returning `FutureRequestTaskWritten`.)

**Where the request ticket is written** — `append_request_edits` (`src/concurrency.rs:2044`):

```rust
// src/concurrency.rs:2059
let action = ConcurrencyAction::EnqueueTask {
    start_time_ms, priority, job_id, attempt_number, relative_attempt_number,
    task_group, limit_index, held_queues, task_id, limits: all_limits };
let req_key = concurrency_request_key(tenant, queue, start_time_ms, priority, job_id, attempt_number, &suffix);
writer.put(&req_key, &encode_concurrency_action(&action))?;
writer.merge(&concurrency_requester_counter_key(tenant, queue), encode_counter(1))?;
```

- The **keyspace is `CONCURRENCY_REQUEST`, prefix byte `0x08`** (`src/keys.rs:23`). Key ordering is `(tenant, queue, start_time_ms, priority, job_id, attempt_number, suffix)` (`concurrency_request_key`, `keys.rs:350`) — so requests in a queue sort by start-time then priority, which is the grant order.
- Critically, the value `ConcurrencyAction::EnqueueTask` **carries `limit_index`, `held_queues`, `task_id`, and the full `limits` list**. That's the entire state needed to *resume the chain* later without re-reading `JobInfo`.
- A per-queue requester counter (`COUNTER_CONCURRENCY_REQUESTERS`, `0xF7`) is bumped so the system knows the queue has waiters.

So to answer Q1 directly: yes — `RequestTicketOutcome::TicketRequested` → `GrantResult::Queued`, and a request record is written to the **`CONCURRENCY_REQUEST` (0x08) keyspace**, *not* to the task broker. The chain-walker then returns early (`enqueue.rs:630–637`); the floating limit is untouched.

---

## 2. What dequeues that request ticket and re-attempts the grant

The present-time request record (0x08) is **not** picked up by the broker. It is drained by a dedicated **grant scanner** that wakes when a holder frees up. The loop is `start_grant_scanner` (`src/concurrency.rs:1378`):

```rust
// src/concurrency.rs:1390
loop {
    mgr.grant_notify.notified().await;            // woken by request_grant()
    ...
    for (_key, (tenant, queue, count)) in work {
        let groups = mgr.process_grants(&db, &range, &tenant, &queue, count).await;
        granted_groups.extend(groups);
    }
    if !granted_groups.is_empty() { brokers.wakeup_groups(&granted_groups); }
}
```

- The scanner blocks on `grant_notify`. It's nudged by `request_grant` → `request_grant_count` (`concurrency.rs:1353`), which is called whenever a holder is released (more on that in Q9). `count` is how many slots opened up.
- `process_grants` (`concurrency.rs:1496`) scans the `concurrency_request_prefix(tenant, queue)` range and grants up to `count` of the oldest requests. For a **plain concurrency limit**, "granting" means: `try_reserve` a slot, write the holder, delete the request record, and **resume the chain at `limit_index + 1`** so the next limit (or the terminal RunAttempt) gets processed. After granting, it wakes the affected task-group brokers.

(There's a second dequeue path for *future-scheduled* `Task::RequestTicket`s — those go through the broker's `handle_request_ticket`, `src/job_store_shard/dequeue.rs:639`. Same shape: `capacity_for_queue` → `try_reserve` → holder → chain continuation. I cover its continuation mechanics in Q7 because that's where the tombstone subtlety lives.)

---

## 3. Where the holder is written, and how it's reserved in memory

**Two-layer reservation.** A grant is *first* reserved in memory (atomically, to win the race), *then* persisted as a durable holder row in the same batch.

**In-memory reservation** lives in `ConcurrencyCounts.queues`, a `DashMap<(tenant, queue), QueueEntry>` where each `QueueEntry` holds a `HashSet<String>` of `task_id`s:

```rust
// src/concurrency.rs:243
struct QueueEntry { state: HydrationState, holders: HashSet<String> }
// src/concurrency.rs:277
pub struct ConcurrencyCounts { queues: Arc<DashMap<QueueKey, QueueEntry>>, ... }
```

The reservation itself is `try_reserve_internal` (`concurrency.rs:486`), executed under the DashMap shard lock so check-and-insert is atomic:

```rust
// src/concurrency.rs:486
let mut entry = self.queues.entry(key).or_insert_with(QueueEntry::new);
if entry.holders.len() >= limit { return false; }   // at capacity → caller queues
entry.holders.insert(task_id.to_string());          // reserve the slot
```

- This is the TOCTOU-safe gate: capacity check and slot insert happen as one critical section. A `true` means *you own a slot*; the caller must now make it durable or roll it back.

**Durable holder row** — written to the **`CONCURRENCY_HOLDER` keyspace, prefix `0x09`** (`keys.rs:24`), keyed `(tenant, queue, task_id)` (`concurrency_holder_key`, `keys.rs:449`). Value is a `HolderRecord { granted_at_ms }` (`task.rs:144`). The grant-scanner / dequeue write site:

```rust
// src/job_store_shard/dequeue.rs:751
let holder_val = encode_holder(&HolderRecord { granted_at_ms: now_ms });
state.batch.put(concurrency_holder_key(&tenant, &queue, &task_id), &holder_val);
```

- The `task_id` keying matters: **all holders in a chain share the one `task_id`**, so when the job finishes, every held queue's holder can be deleted by that single id (and the chain's earlier grants never orphan when a later limit is granted in a different code path).
- The pairing of in-memory `holders.insert` + durable `0x09` put is what "reserved in memory" means: the `HashSet` is the source of truth for the live count; the `0x09` row is the durable mirror for crash recovery.

---

## 4. Floating concurrency — is a RefreshTask enqueued?

Once the concurrency slot frees and the chain resumes to `current_index = 1`, the walker hits `Limit::FloatingConcurrency(fl)` (`enqueue.rs:641`). The important thing: **the floating limit grants synchronously against its *current* cached max — it never blocks on a refresh.** The refresh is a *separate, throttled, side-channel* to keep that max fresh.

```rust
// src/job_store_shard/enqueue.rs:643
let state = self.get_or_create_floating_limit_state(writer, tenant, fl).await?;
let refresh_ready = JobStoreShard::floating_limit_refresh_ready(&state, now_ms);
let current_max = state.current_max_concurrency();
let temp_cl = ConcurrencyLimit { key: fl.key.clone(), max_concurrency: current_max };
let outcome = self.concurrency.handle_enqueue(/* ... */ std::slice::from_ref(&temp_cl), /* ... */).await?;
```

- `get_or_create_floating_limit_state` loads (or seeds) the floating state row. On cold creation it seeds `current_max_concurrency = fl.default_max_concurrency` (`src/job_store_shard/floating.rs:68`) — **this is the default you asked about in Q5: the floating limit always has a usable value on the immediate path, even before any refresh runs.**
- It builds a *temporary* fixed `ConcurrencyLimit` from `current_max` and grants through the exact same `handle_enqueue` machinery as a normal concurrency limit. So floating concurrency is "ordinary concurrency with a periodically-updated ceiling."

Then, conditionally, the refresh is scheduled:

```rust
// src/job_store_shard/enqueue.rs:687
let mut has_waiters = matches!(outcome, Some(RequestTicketOutcome::TicketRequested { .. }));
if refresh_ready && !has_waiters {
    has_waiters = self.has_waiting_concurrency_requests(tenant, &fl.key).await?;
}
if refresh_ready {
    self.maybe_schedule_floating_limit_refresh(writer, tenant, fl, &state, now_ms, task_group, has_waiters)?;
}
```

- `refresh_ready` (`floating.rs:19`) gates on: no refresh already in flight, the refresh interval has elapsed, and not in backoff.
- `maybe_schedule_floating_limit_refresh` (`floating.rs:89`) writes a **`Task::RefreshFloatingLimit` broker task** and flips `refresh_task_scheduled = true` on the state row so only one is ever outstanding:

```rust
// src/job_store_shard/floating.rs:119
let refresh_task = Task::RefreshFloatingLimit { task_id, tenant, queue_key: fl.key.clone(),
    current_max_concurrency: state.current_max_concurrency(),
    last_refreshed_at_ms: state.last_refreshed_at_ms(), metadata: state.metadata(), task_group };
let synthetic_job_id = format!("floating_refresh:{}", fl.key);
let task_key_bytes = crate::keys::task_key(task_group, now_ms, 0 /*highest priority*/, &synthetic_job_id, 0);
writer.put(&task_key_bytes, &task_value)?;
let new_state = FloatingLimitState { refresh_task_scheduled: true, ..state.to_owned() };
```

So to answer Q4 directly: **yes, a `Task::RefreshFloatingLimit` is enqueued into the task broker** (prefix `0x05`, highest priority, synthetic job id) — but it is *orthogonal* to granting this job. This job grants against the existing `current_max`; the refresh just updates that number for *future* grants, and only when the interval has elapsed and there are waiters.

---

## 5. How the RefreshTask is leased; the "second RequestTicket" question

The broker dequeue loop dispatches `RefreshFloatingLimit` → `handle_refresh_floating_limit` (`dequeue.rs:1072`). Leasing means: write a **lease record** keyed by the refresh task's id and delete the broker task, atomically:

```rust
// src/job_store_shard/dequeue.rs:1086
let lease_key = leased_task_key(task_id);                     // LEASE keyspace, 0x06
let record = LeaseRecord { worker_id, task, expiry_ms, started_at_ms: 0 };
state.batch.put(&lease_key, &encode_lease(&record));
state.batch.delete(task_key);
...
refresh_out.push(LeasedRefreshTask { task_id, tenant_id, queue_key,
    current_max_concurrency, last_refreshed_at_ms, metadata, task_group });
state.ack_deleted(task_key);
```

- The lease lives in the **`LEASE` keyspace, prefix `0x06`** (`keys.rs:21`), keyed by `task_id` (`leased_task_key`, `keys.rs:288`). Same leasing primitive used for run attempts.
- The task is then surfaced to the worker over gRPC as a `RefreshFloatingLimitTask` (`server.rs:1212`). The worker computes the new ceiling and calls back `report_refresh_success` (`floating.rs:163`) or `report_refresh_failure` (`floating.rs:229`).
- On success (`floating.rs:194`): the lease is deleted, the state row is updated (`current_max_concurrency = new_max`, `last_refreshed_at_ms = now`, `refresh_task_scheduled = false`), and the in-memory limit cache is updated. On failure: an exponential-backoff retry is scheduled, *only if there are still waiters*.

**Your two sub-questions for Q5:**

- *Is there another RequestTicket written after the floating limit is found?* **No.** The refresh round-trip only updates the floating *state row* + in-memory cache — it writes no request/ticket and does not re-enqueue the waiting job. Previously-queued requesters are admitted the normal way: when a holder frees, `request_grant` wakes the scanner, which re-reads the now-larger `current_max` via `capacity_for_queue` and grants.
- *Is there a default floating value for the immediate path?* **Yes** — `fl.default_max_concurrency`, used in two places: it seeds a cold state row (`floating.rs:68`), and it's the fallback in `capacity_for_queue` if the state row is missing/undecodable (`concurrency.rs:933`):

```rust
// src/concurrency.rs:933
Limit::FloatingConcurrency(fl) if fl.key == queue => {
    let state_capacity = /* decode floating_limit_state_key(tenant, queue) */;
    return (state_capacity.unwrap_or(fl.default_max_concurrency as usize),
            ConcurrencyLimitType::Floating);
}
```

---

## 6. After the floating limit is granted — how the RunAttempt is written

When the floating limit grants, `record_grant_outcome` pushes its `(queue, task_id)` into `grants`, appends the queue to `current_held_queues`, and **increments `current_index` to 2**. The `loop` re-enters, `current_index (2) >= limits.len() (2)` is now true, and the terminal branch fires — the **same** code shown in Q1:

```rust
// src/job_store_shard/enqueue.rs:564
let run_task = Task::RunAttempt { id: current_task_id, tenant, job_id,
    attempt_number, relative_attempt_number,
    held_queues: current_held_queues, task_group };
put_task(writer, task_group, task_key_start_ms, priority, job_id, attempt_number, &run_task)?;
return Ok(Some(task_key_start_ms));
```

- `RunAttempt` is **not a struct — it's a `Task` enum variant** (`task.rs:14`) persisted as an ordinary task row. It carries `held_queues` = the accumulated list of every queue this chain holds (the concurrency queue *and* the floating queue), so cleanup later knows exactly which holders to release.
- `put_task` (`helpers.rs:242`) encodes it and writes it under the **`TASK` keyspace, prefix `0x05`**, at `task_key(task_group, task_key_start_ms, priority, job_id, attempt)` (`keys.rs:232`). The time component is a plain ascending `u64`, so within a task group, tasks sort by ready-time.
- Note `task_key_start_ms` here is `now_ms` on a resumed chain (not the original `scheduled_at_ms`) — that +1-style bump is exactly the tombstone-dodge described in Q7.

The `RunAttempt` row is the handoff from the *limit/concurrency* subsystem to the *execution/broker* subsystem.

---

## 7. What dequeues the RunAttempt into the task broker — and why tombstones

The **task broker** (`src/task_broker.rs:38`, one per task group) is an in-memory buffer fed by a background scanner that prefix-scans the `TASK` keyspace. Writing the `RunAttempt` row does **not** push it to the broker directly; the scanner discovers it.

**Buffer + scan:**

```rust
// src/task_broker.rs:44
buffer: Arc<SkipMap<Vec<u8>, BrokerTask>>,
```

```rust
// src/task_broker.rs:130 (scan_tasks)
let start = task_group_prefix(&self.task_group);
let mut iter = self.db.scan_with_options(start..end_bound(&start), ...).await?;
...
let Some(parsed_key) = parse_task_key(&kv.key) else { continue; };
if parsed_key.start_time_ms > now_ms as u64 { break; }    // skip not-yet-ready
if self.buffer.get(&key_bytes).is_none() { self.buffer.insert(key_bytes, entry); }
```

- The scanner walks `TASK/<task_group>/…` in key order, drops future-dated rows, and inserts ready ones into the sorted skiplist `buffer`. It runs with hysteresis (refill below 4096, stop at 8192) and is nudged early via `wakeup()` after an enqueue. So: `put_task` writes the row → scanner finds it → it lands in the buffer as a `BrokerTask`.

**Why tombstones are needed.** SlateDB is an LSM store. When a worker claims a task and durably **deletes** its key, a concurrent or slightly-stale broker scan can still observe the just-deleted key (the delete may not have compacted over the old value yet). Without protection, the scanner would re-insert an already-leased task and it would be dispatched **twice**. The broker keeps **in-memory tombstones** (not DB tombstones) to suppress re-insert for a bounded window:

```rust
// src/task_broker.rs:47
ack_tombstones: Arc<Mutex<HashMap<Vec<u8>, u64>>>,
```

```rust
// src/task_broker.rs:183 (inside scan)
let suppress_due_to_tombstone = {
    let mut tombstones = self.ack_tombstones.lock().unwrap();
    if let Some(last_seen_generation) = tombstones.get_mut(&key_bytes) {
        *last_seen_generation = generation; true     // refresh → extend suppression
    } else { false }
};
if suppress_due_to_tombstone { skipped_tombstone += 1; continue; }
```

- A tombstone is installed only on a **durable delete**, recorded via `ack_deleted` (which appends to both `release_keys` and `tombstone_keys`), versus `ack_release` (release-only, no tombstone — used when a ticket is *left in place* to be re-scanned). See `dequeue.rs:55`.

**The chain-continuation gotcha.** A `task_key` is `(task_group, start_time_ms, priority, job_id, attempt_number)`. A chain continuation keeps *every* component constant except `start_time_ms`. So a follow-up task written at the **same** `start_time_ms` as the just-tombstoned parent would be silently suppressed — stranding the holders just granted. The fix bumps the follow-up's time past the parent (`dequeue.rs:778`):

```rust
// src/job_store_shard/dequeue.rs:778
let parent_start_time_ms = parse_task_key(task_key).map(|p| p.start_time_ms as i64).unwrap_or(now_ms);
let new_task_key_start_ms = now_ms.max(parent_start_time_ms + 1);
```

- `now_ms` *usually* already exceeds the parent's `start_time_ms`, but a worker processing a future-scheduled ticket in the same millisecond it becomes ready could collide — hence `max(parent + 1)` makes the dodge unconditional. (This is the "trailing task_key `epoch_ms`"; the field is named `start_time_ms` in `task_key`.)

---

## 8. How a job is leased from the broker buffer

Two stages: an **in-memory claim** from the skiplist, then a **durable lease write**.

**Claim from buffer** — `claim_ready` (`task_broker.rs:395`): walk the sorted skiplist, mark each key `inflight`, remove it from the buffer:

```rust
// src/task_broker.rs:395
for entry in self.buffer.iter() {
    if keys.len() >= max { break; }
    if inflight.contains(entry.key()) { continue; }
    inflight.insert(entry.key().clone());
    keys.push(entry.key().clone());
}
...
for key in candidate_keys { if let Some(entry) = self.buffer.remove(&key) { claimed.push(entry.value().clone()); } }
```

- The `inflight` set prevents the scanner from re-buffering a task that's mid-dequeue (complementing the tombstone, which covers the post-delete window).

**Drive loop + durable lease** — `dequeue` (`dequeue.rs:289`) claims a batch (`claim_ready_or_nudge`, `dequeue.rs:331`), wraps it in a cancellation-safe `ClaimedInflightGuard`, dispatches `RunAttempt → handle_run_attempt`, commits durably (`await_durable: true`), then `ack_durable` installs tombstones + evicts. `handle_run_attempt` deletes the task key and writes three durable records (`write_lease_and_attempt`, `dequeue.rs:582`):

```rust
// src/job_store_shard/dequeue.rs:582
let lease_key = leased_task_key(task_id);                              // LEASE 0x06
batch.put(&lease_key, &encode_lease(&LeaseRecord { worker_id, task, expiry_ms, started_at_ms: now_ms }));
self.set_job_status_with_index(..., JobStatus::running(now_ms)).await?; // JOB_STATUS
batch.put(&attempt_key(tenant, job_id, attempt_number), &encode_attempt(&attempt)); // ATTEMPT 0x07
```

- The lease (`0x06`, keyed by `task_id`) is what a worker crash recovery and `report_outcome` later look up. Default lease is `DEFAULT_LEASE_MS = 10_000` (`task.rs:10`); expiry `now_ms + 10s`. The job status flips to `running`, and an `ATTEMPT` row (`0x07`) records this attempt. After commit, the task becomes a `LeasedTask` (tenant + `JobView` + `JobAttemptView`) returned to the worker.

A lease reaper (`reap_expired_leases`) sweeps leases past expiry and synthesizes a `WORKER_CRASHED` error outcome — feeding the same failure path as Q9.

---

## 9. `report_outcome` — success and failure

The RPC is `report_outcome` (`server.rs:1249`), mapping proto outcome → `AttemptOutcome::{Success, Error, Cancelled}` (`job_attempt.rs:11`) and delegating to `report_attempt_outcome` (`src/job_store_shard/lease.rs:93`). It loads the lease (errors `LeaseNotFound` if gone — which triggers a self-heal of orphaned holders), and **always** starts the batch by deleting the lease:

```rust
// src/job_store_shard/lease.rs:161
batch.delete(&leased_task_key);
```

**Success** (`lease.rs:176`):

```rust
AttemptOutcome::Success { .. } => {
    let job_status = JobStatus::succeeded(now_ms);
    terminal_expire_ts = self.terminal_expire_ts(JobStatusKind::Succeeded, now_ms);
    self.set_job_status_with_index_opts(..., job_status, terminal_expire_ts).await?;
    self.increment_completed_jobs_counter(...)?;
    reached_terminal = true;
}
```

- Lease deleted, job status → `Succeeded` (with a terminal TTL `completed_job_expire_s`), completed-jobs counter bumped, and a terminal `Succeeded` `ATTEMPT` row written with `Ttl::ExpireAt`. `reached_terminal` triggers `expire_terminal_job_records` to stamp the TTL on `JobInfo`/metadata/prior attempts. (Cancelled is structurally identical.)

**Failure** (`lease.rs:216`) — the "bump an attempt, maybe make a new RunAttempt" path:

```rust
// src/job_store_shard/lease.rs:239
let next_attempt_number = attempt_number + 1;
let next_relative_attempt_number = relative_attempt_number + 1;
let next_task_id = Uuid::new_v4().to_string();
retry_grants = self.enqueue_limit_task_at_index(..., LimitTaskParams {
    attempt_number: next_attempt_number,
    relative_attempt_number: next_relative_attempt_number,
    limit_index: 0,
    limits: &limits,
    scheduled_at_ms: next_time, task_key_start_ms: next_time,
    held_queues: Vec::new(),       // retry re-acquires all tickets from scratch
    skip_try_reserve: true,        // old holder still in memory → must queue
}).await?.grants;
let job_status = JobStatus::scheduled(now_ms, next_time, next_attempt_number);
```

- If the retry policy yields a `next_time` (`retry::next_retry_time_ms`), the attempt counter is bumped (`+1`), a **fresh `task_id`** is minted, and the **whole limit chain is re-walked from `limit_index: 0`** — so the retry must re-acquire both the concurrency and floating holders from scratch (`held_queues: Vec::new()`). Job status goes back to `Scheduled(n+1)`; the job is *not* terminal.
- `skip_try_reserve: true` is the subtle invariant: the *current* attempt's holders are still in memory (released only post-commit, below), so the retry can't grab an immediate grant or it would double-count the slot. It is forced through the request queue. This is why `walk_limit_chain` consumes the flag with `std::mem::replace(&mut skip_try_reserve, false)` after the first limit — a compile-checked guard so no downstream limit silently bypasses its reservation.
- If retries are exhausted (or no policy), job status → `Failed` (terminal, with TTL), completed counter bumped.

**Holder release — identical for all outcomes** (outside the match). Durable delete in the batch, then in-memory release post-commit:

```rust
// src/job_store_shard/lease.rs:361
for queue in &held_queues_local { batch.delete(concurrency_holder_key(&tenant, queue, task_id)); }
// ... commit batch durably ...
// src/job_store_shard/lease.rs:412 (post-commit)
for queue in &held_queues_local {
    self.concurrency.counts().atomic_release(&tenant, queue, task_id);   // free in-memory slot
    self.concurrency.request_grant(&tenant, queue);                      // wake grant scanner
}
```

- Every queue in `held_queues` (the concurrency queue **and** the floating queue) has its `0x09` holder row deleted and its in-memory `HashSet` slot freed via `atomic_release` (`concurrency.rs:568`).
- Then `request_grant` (`concurrency.rs:1131`) wakes the grant scanner from Q2 — closing the loop, since the freed slot is exactly what lets the *next* queued requester be granted. This is the linkage between one job finishing and the next starting.

---

## The loop, in one breath

`enqueue` writes `JobInfo` + walks limits → concurrency full ⇒ **request record** in `0x08`, walk stops → a holder frees, `request_grant` wakes the **grant scanner** → it grants the concurrency slot (in-memory `HashSet` + `0x09` holder), resumes the chain → **floating limit** grants against `current_max` (refreshed out-of-band by a throttled `RefreshFloatingLimit` broker task) → all limits held ⇒ terminal **`RunAttempt`** task written to `0x05` → broker **scanner** buffers it (tombstones guard against LSM re-reads; chain continuations bump `start_time_ms` to dodge them) → worker **claims + leases** it (`0x06` lease, `0x07` attempt, status→running) → **`report_outcome`**: success → terminal + holders released; failure → attempt bumped, new chain re-walked from index 0, holders released → release wakes the grant scanner for the next waiter.

## Keyspace prefix reference

| Prefix | Name | Key | Holds |
|--------|------|-----|-------|
| `0x05` | `TASK` | `(task_group, start_time_ms, priority, job_id, attempt)` | broker tasks (RunAttempt, RequestTicket, CheckRateLimit, RefreshFloatingLimit) |
| `0x06` | `LEASE` | `(task_id)` | active leases |
| `0x07` | `ATTEMPT` | `(tenant, job_id, attempt)` | per-attempt records |
| `0x08` | `CONCURRENCY_REQUEST` | `(tenant, queue, start_time_ms, priority, job_id, attempt, suffix)` | queued request records |
| `0x09` | `CONCURRENCY_HOLDER` | `(tenant, queue, task_id)` | durable holder rows |
| `0x0B` | `FLOATING_LIMIT` | `(tenant, queue_key)` | floating-limit state |
| `0xF7` | `COUNTER_CONCURRENCY_REQUESTERS` | `(tenant, queue)` | per-queue waiter counter |
