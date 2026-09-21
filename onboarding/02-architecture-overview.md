# 02 — Architecture Overview

This is the bird's-eye view. Each subsystem named here has its own deep-dive file; cross-references are linked inline.

---

## 1. The four planes

Silo has four conceptual planes. Keep them separate in your head:

1. **Data plane** — enqueue/dequeue/report jobs. gRPC in, SlateDB read/write, gRPC out. The hot path. (Files [04](./04-job-lifecycle.md), [05](./05-storage-and-keys.md), [07](./07-concurrency-and-rate-limits.md))
2. **Control/coordination plane** — which node owns which shard, membership, splits. Backed by etcd or k8s. (File [06](./06-coordination-and-sharding.md))
3. **Query/observability plane** — SQL over shard data (DataFusion), the operator web UI, Prometheus metrics, tracing, profiling. (File [08](./08-server-query-webui-observability.md))
4. **Supporting plane** — out-of-process compactor, k8s autoscaler, CLI, client libraries, dev tooling. (File [10](./10-supporting-crates-and-dev-env.md))

---

## 2. Request flow (the 60-second version)

```
client/worker gRPC request (carries an explicit `shard` UUID + `tenant`)
        │
        ▼
src/server.rs  ── SiloService (tonic) ──────────────────────────────────┐
        │  tower layers: GrpcTraceLayer → GrpcMetricsLayer → Auth        │
        │                                                                │
        ├─ Is this shard open locally (ShardFactory.get)?                │
        │     ├─ yes, but paused for split → Status::UNAVAILABLE         │
        │     └─ yes → hand to the JobStoreShard ──────────────┐         │
        │                                                       │         │
        ├─ no, but rendezvous says *I* should own it,           │         │
        │   acquisition still in flight → Status::UNAVAILABLE   │         │
        │                                                       │         │
        └─ no → Status::NOT_FOUND + redirect metadata           │         │
              (owner addr + node id; client retries there)      │         │
                                                                ▼         │
                                       src/job_store_shard/*  (the core)  │
                                       reads/writes SlateDB keys ◄────────┘
                                                │
                                                ▼
                                  SlateDB (one instance per shard)
                                                │
                                                ▼
                                  Object storage (GCS/S3/FS)
```

The routing decision lives in `shard_with_redirect` (`src/server.rs:446`). The client side that follows redirects and refreshes topology is `RoutingClient` (`src/routing_client.rs`); the node-to-node version that never needs redirects (it has the coordinator) is `ClusterClient` (`src/cluster_client.rs`). See file 06 §5.

---

## 3. Sharding & routing in one diagram

```
tenant_id ──xxhash64──▶ "00..ff" (16 hex chars)        [src/shard_range.rs: hash_tenant]
                              │
            binary-search the sorted ShardMap of half-open ranges
                              │
                              ▼
                          ShardId (a UUID)                [src/shard_range.rs: shard_for_tenant]
                              │
        bounded-load rendezvous hashing over current members
                              │
                              ▼
                        owning Node (grpc addr)           [src/coordination/mod.rs]
```

- **Tenants → shards**: range partitioning over the *hash space* (not raw tenant strings), so even tenants with shared prefixes (`env-...`) spread evenly. `ShardMap` is the authoritative sorted list of `(ShardId, ShardRange)`.
- **Shards → nodes**: every node and client independently computes the same assignment via rendezvous hashing — no central assigner. Bounded-load means shard counts differ by at most 1 per placement ring.
- **Splits**: a shard whose data grows can split into two children at a hash midpoint, online, via a pause→clone→atomic-map-update state machine. Once children are in the map, the split is committed forever.

Full detail in [file 06](./06-coordination-and-sharding.md).

---

## 4. Storage model

Each shard is one **SlateDB** instance (an LSM tree over a bucket prefix). Inside it, every fact about every job is a binary key/value row:

- **Keys** are `[1-byte prefix][storekey-encoded tuple]`. The prefix namespaces record types; storekey makes byte-order match logical order so everything is a **range scan**.
- The key-space (from `src/keys.rs`):

  | Prefix | Record |
  |--------|--------|
  | `0x01` | Job info (immutable definition) |
  | `0x02` | Job status (current state) |
  | `0x03` | Status/time index (find jobs by status, newest-first) |
  | `0x04` | Metadata index (find jobs by metadata key/value) |
  | `0x05` | Tasks (the execution queue) |
  | `0x06` | Leases (in-flight tasks) |
  | `0x07` | Attempts (per-attempt history + results) |
  | `0x08` | Concurrency requests (queued for a slot) |
  | `0x09` | Concurrency holders (holding a slot) |
  | `0x0A` | Cancellation flag |
  | `0x0B` | Floating-limit state |
  | `0xF0+` | Counters & cleanup checkpoints |

- **Values** are FlatBuffers (`src/codec.rs`), except counters which are little-endian i64 deltas summed by a SlateDB **merge operator**.
- Terminal jobs are written with a **row TTL** so they age out during compaction.

Full detail in [file 05](./05-storage-and-keys.md).

---

## 5. The core domain: jobs, tasks, leases, attempts

These four nouns are the heart of silo. Distinguish them carefully:

- **Job** — the durable thing the user enqueued. Has an immutable *info* record and a mutable *status* (`Scheduled → Running → Succeeded/Failed/Cancelled`).
- **Task** — a *unit of pending work* in a task group's queue (`0x05`). A job produces tasks. There are several variants: `RunAttempt` (actually run the job), and internal chain tasks `RequestTicket` / `CheckRateLimit` / `RefreshFloatingLimit` used to enforce limits. Tasks are ordered by `(start_time, priority)`.
- **Lease** — when a worker dequeues a `RunAttempt` task, it becomes a **lease** (`0x06`) owned by that worker with an expiry. The worker heartbeats to extend it; if it expires, the lease is reclaimed (worker presumed crashed).
- **Attempt** — a single execution try (`0x07`), recording start time and outcome (Running/Succeeded{result}/Failed{error}/Cancelled). A job accumulates attempts across retries; the **result** lives on the terminal attempt.

The job lifecycle ties them together; that's [file 04](./04-job-lifecycle.md), the most important file.

---

## 6. The task broker (dequeue scheduling)

There is **one `TaskBroker` per task group** (`src/task_broker.rs`). It keeps an in-memory sorted buffer of ready tasks, fed by a background scanner that range-scans that task group's `0x05` keys, skipping future-dated and in-flight tasks. Workers long-poll via `LeaseTasks`; the broker hands out claimed tasks, which the dequeue path turns into durable leases. Future-scheduled jobs are simply skipped until their time arrives (keys sort by start time, so the scanner stops at the first future task).

---

## 7. Concurrency & rate limits

A job can carry an **ordered list of limits** it must clear before running:

- **Concurrency limit** — a fixed number of slots per queue key. Acquiring a slot writes a *holder* (`0x09`); when full, a *request* (`0x08`) is queued. A single background **grant scanner** admits queued requests as slots free up. High-cardinality: queues are implicit and lazily materialized — millions of distinct queue keys cost nothing until used.
- **Floating concurrency limit** — like above but the max is refreshed dynamically by a worker (via a `RefreshFloatingLimit` task and the `0x0B` state row).
- **Rate limit** — delegated to the external **gubernator** service via a `CheckRateLimit` task.

Limits are processed as a **chain**: clear limit 1, then limit 2, …, then write the final `RunAttempt`. Full detail in [file 07](./07-concurrency-and-rate-limits.md).

---

## 8. Cluster coordination backends

Pluggable via the `Coordinator` trait (`src/coordination/mod.rs`):

- **`none`** — single-node dev mode; opens all shards locally.
- **`etcd`** — membership = etcd lease w/ keepalive; shard ownership = a *permanent* leaseless KV key; shard map = one JSON KV.
- **`k8s`** — membership = a per-node `Lease`; shard ownership = a per-shard `Lease`; shard map = a `ConfigMap`.

Key invariant: **shard ownership leases are permanent** (survive crashes) so a crashed node's unflushed WAL can be recovered when it restarts — they're *not* tied to the membership lease. Both etcd and k8s share the `CoordinatorBase`, `ShardGuardState` state machine, and rendezvous logic. [File 06](./06-coordination-and-sharding.md).

---

## 9. Where the binaries live

- `silo` (`src/main.rs`) — the daemon. Boots config → tracing → metrics → rate limiter → `ShardFactory` → `Coordinator` → gRPC server (+ optional web UI, metrics server, runtime/jemalloc scrapers). Graceful shutdown drains gRPC then closes shards then releases leases. (File 08 §1.)
- `silo-bench`, `silo-sim` (`src/bin/`) — load generators (one over gRPC, one in-process).
- `silo-compactor`, `siloctl`, `silo-autoscaler` — separate workspace crates (File 10).

---

## 10. How correctness is defended

Silo is a distributed system holding durable state, so correctness is taken seriously on three layers (File 09):

1. **~70 integration test files** in `tests/` covering every operation.
2. **Deterministic Simulation Testing (DST)** — runs the real server in a simulated network + clock, reproducible by seed, replaying an event log against invariant trackers.
3. **Alloy formal models** (`specs/*.als`) — machine-checked proofs of the algorithms, linked to the code by `[SILO-...]` sigils that CI keeps in sync.

---

## 11. Putting it together: the life of one enqueue→complete

1. Client hashes tenant → shard, dials the owning node, calls `Enqueue`.
2. Server validates shard/tenant, hands to the local `JobStoreShard`.
3. Enqueue writes `JobInfo` (0x01), `JobStatus=Scheduled` (0x02), indexes (0x03/0x04), walks the limit chain, and (if clear) writes a `RunAttempt` **task** (0x05) — all durably in one batch. Bumps the total-jobs counter.
4. The task group's broker scanner picks up the task into its buffer.
5. A worker long-polls `LeaseTasks`; the broker hands out the task; dequeue deletes the task, writes a **lease** (0x06), sets `JobStatus=Running`, writes a Running **attempt** (0x07).
6. Worker runs the job, heartbeating (which also tells it if the job was cancelled). On success it calls `ReportOutcome`.
7. Report deletes the lease, sets `JobStatus=Succeeded` (with TTL), writes the terminal **attempt** carrying the **result**, releases any concurrency holders, bumps the completed counter, and wakes the grant scanner so queued jobs get slots.
8. The enqueuer polls `GetJobResult`, which reads the result off the terminal attempt.

If the worker fails instead, the retry logic (file 04 §7) either schedules a new attempt with backoff or dead-letters the job to `Failed`. If the worker crashes, the lease expires and the reaper reclaims it. Every step is a durable SlateDB write; nothing is lost across restarts.

Next: [`03-codebase-map.md`](./03-codebase-map.md) to learn where each piece lives.
