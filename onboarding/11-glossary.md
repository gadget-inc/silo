# 11 — Glossary

Every silo-specific (and a few Rust/infra) term in one place. Alphabetical.

---

**Alloy** — A formal modeling language + bounded model checker. Silo's `specs/*.als` prove algorithm invariants; CI runs them when changed. See file 09 §4.

**Attempt** — One execution try of a job (key `0x07`). Records start time + outcome (`Running`/`Succeeded{result}`/`Failed{error}`/`Cancelled`). The **result** lives on the terminal attempt. Has `attempt_number` (monotonic, in the key) and `relative_attempt_number` (resets on restart, drives backoff).

**`AttemptOutcome`** — What a worker reports: `Success{result}`, `Error{error_code, error}`, or `Cancelled`.

**Backend (object store)** — Where SlateDB files live: `fs`, `s3`, `gcs`, `memory`, `url`, or `turmoilfs` (DST). Set in `[database]` config.

**Bounded-load rendezvous hashing** — How shards are assigned to nodes: rendezvous (highest-score-wins) but capped so per-node shard counts differ by ≤1 within a placement ring. `src/coordination/mod.rs`.

**ClusterClient** — The **node-to-node** routing client. Has the coordinator, so serves local shards directly and dials owners for remote ones; never needs redirects. `src/cluster_client.rs`.

**ClusterQueryEngine** — Cross-cluster SQL: one DataFusion partition per shard (local scan or `QueryArrow` RPC). `src/cluster_query.rs`.

**Compactor (silo-compactor)** — A standalone out-of-process fleet that runs SlateDB compaction, isolating it from job-serving nodes. Must register the same counter merge operator. File 10 §3.

**Concurrency limit** — A fixed slot count per queue key. Acquiring writes a **holder** (`0x09`); when full, a **request** (`0x08`) queues. File 07.

**Coordinator** — The cluster control-plane trait (`none`/`etcd`/`k8s` backends) managing membership + shard ownership. `src/coordination/`.

**DataFusion** — Apache Arrow-based embeddable SQL engine. Silo exposes shard data as SQL tables. File 08 §3.

**Dead-letter** — Silo has *no separate dead-letter queue*; a job that exhausts retries simply becomes `Failed` (recoverable only via `RestartJob`).

**Dequeue** — A worker leasing tasks via `LeaseTasks`. Turns a `RunAttempt` task into a lease + Running attempt. File 04 §3.

**DST (Deterministic Simulation Testing)** — Running the real server in a simulated network + clock + RNG, reproducible by seed. turmoil + mad-turmoil. File 09 §3.

**`dst_events`** — Structured event log (no-op without the `dst` feature) replayed against invariant trackers. Two-phase emission around durable writes. File 09 §3.4.

**Enqueue** — Creating a job (`Enqueue` RPC). Writes job info/status/indexes + walks the limit chain + first task. File 04 §2.

**Expedite** — Drag a future/retry-waiting job forward to run now (`ExpediteJob`). Changes the task key's time component. File 04 §9.

**FlatBuffers** — The on-disk serialization for record **values** (zero-copy reads). Schema `schema/internal_storage.fbs`, codec `src/codec.rs`.

**Floating (concurrency) limit** — A concurrency limit whose max is refreshed dynamically by a worker via a `RefreshFloatingLimit` task and the `0x0B` state row. File 07 §4.

**Grant scanner** — The single background task that admits queued concurrency requests as slots free up (`process_grants`). Decouples release from admission. File 07 §2.

**gRPC** — The RPC protocol silo speaks (over HTTP/2, via tonic). Service in `proto/silo.proto`.

**Gubernator** — External distributed rate-limiting service. Silo calls it for `RateLimit` checks. `src/gubernator.rs`, `proto/gubernator.proto`.

**Heartbeat** — A worker extending its lease (and learning if the job was cancelled). `Heartbeat` RPC. File 04 §4.

**Holder** — A held concurrency slot (key `0x09`, `(tenant, queue, task_id)`). Presence = the task holds a slot.

**Idempotency / dedup** — Caller-supplied job IDs are deduplicated in an SSI transaction (`JobAlreadyExists`). No ID → UUIDv4, no dedup.

**Import** — Bulk-loading jobs *with* historical terminal attempts (`ImportJobs`). `src/job_store_shard/import.rs`.

**InstrumentedDb** — A wrapper around SlateDB that attaches the shard's **tracing span** to every call (not a metrics wrapper). `src/instrumented_db.rs`.

**Job** — The durable user-enqueued unit. Immutable **info** (`0x01`) + mutable **status** (`0x02`).

**JobStoreShard** — The core per-shard storage engine; owns one SlateDB. `src/job_store_shard/`.

**`JobStatusKind`** — `Scheduled`/`Running`/`Failed`/`Cancelled`/`Succeeded`. `is_final()` = Succeeded|Failed; `is_terminal()` = +Cancelled. File 04 §1.

**Key prefix** — The 1-byte namespace at the start of every SlateDB key (`0x01`…`0x0B`, `0xF0+`). File 05 §2.

**Lease** — A `RunAttempt` task claimed by a worker (key `0x06`), with an expiry; renewed by heartbeats; reclaimed if it expires (worker presumed crashed). `DEFAULT_LEASE_MS = 10s`.

**Limit / Limit chain** — A job's ordered list of `Concurrency`/`FloatingConcurrency`/`RateLimit` gates, processed one at a time; holders accumulate under one shared `task_id`. File 07 §3.

**LSM tree** — Log-Structured Merge tree; SlateDB's storage structure (memtable + WAL → SSTs → compaction).

**mad-turmoil** — libc-level interception (clock + RNG) that makes DST fully deterministic. File 09 §3.

**Membership lease** — A *transient* TTL/keepalive lease proving a node is alive (etcd lease / k8s Lease object). Distinct from shard ownership.

**Merge operator** — SlateDB feature summing i64 counter deltas at read/compaction time (avoids read-modify-write). Both shard and compactor must register it. File 05 §6.

**MessagePack (rmp-serde)** — Encoding for job **payloads** (opaque to silo) and some query rows.

**Node** — A silo process/pod in the cluster. Identified by `node_id`; owns a set of shards.

**Placement ring** — A label pinning shards to a subset of nodes. Default ring = `None`. Moved via `ConfigureShard`.

**Priority** — `0` (highest) .. `99` (lowest). Second sort field in the task key (after start time). Fixed at enqueue.

**Rendezvous hashing** — See *bounded-load rendezvous hashing*.

**Request (concurrency)** — A job waiting for a concurrency slot (key `0x08`). Admitted by the grant scanner.

**Restart** — Re-run a `Cancelled`/`Failed` job with a fresh retry budget (`RestartJob`). File 04 §10.

**Result** — A job's success output, stored on the terminal attempt, fetched by polling `GetJobResult`.

**Retry policy** — `retry_count`, `initial_interval_ms`, `max_interval_ms`, `randomize_interval`, `backoff_factor` (default ×2). Math in `src/retry.rs`. File 04 §7.

**RoutingClient** — The **outsider** client (benches/workers, no coordinator): follows redirects, refreshes topology, classifies gRPC errors. `src/routing_client.rs`.

**`RunAttempt`** — The task variant that actually runs a job (vs internal chain tasks). Becomes a lease on dequeue.

**Shard** — A partition of tenants = one SlateDB instance = one object-store prefix. Identified by a `ShardId` (UUID).

**ShardFactory** — Opens/closes SlateDB instances as ownership changes; clones a closed parent for splits. `src/factory.rs`.

**ShardMap** — The authoritative sorted list of `(ShardId, ShardRange)` + a version. Stored in etcd/ConfigMap.

**ShardRange** — A half-open `[start, end)` interval over the *hash space* (hex strings). `src/shard_range.rs`.

**Sigil** — A tag like `[SILO-ENQ-1]` linking an Alloy invariant to its Rust enforcement site. CI keeps them in sync. File 09 §4.

**SlateDB** — The embedded LSM key/value store over object storage; one instance per shard. File 05.

**Split** — Dividing one shard into two children at a hash midpoint, online, via pause → clone → atomic-map-update. Committed at the map write. File 06 §4.

**SSI (Serializable Snapshot Isolation)** — The transaction isolation level used for read-modify-write paths (enqueue-dedup, cancel, expedite). Retries on conflict.

**storekey** — Order-preserving binary key encoding (byte-order == logical order), enabling range scans. `src/keys.rs`.

**Task** — A unit of pending work in a task group's queue (key `0x05`). Variants: `RunAttempt`, `RequestTicket`, `CheckRateLimit`, `RefreshFloatingLimit`. Ordered by `(start_time, priority)`.

**TaskBroker** — Per-task-group in-memory buffer + background scanner + long-poll that feeds dequeue. `src/task_broker.rs`.

**Task group** — A named queue of tasks that workers poll by name (`LeaseTasks(task_group)`). Immutable on a job after enqueue. One broker per task group.

**Tenant** — A multi-tenancy key. Hashed (XXH64 → 16 hex) to pick a shard. Synthetic `-` when tenancy is disabled.

**Terminal / final** — *Terminal* = Succeeded|Failed|Cancelled (result available). *Final* = Succeeded|Failed (irreversible).

**Terminal TTL** — Row expiry written on terminal records so they age out during compaction. Needs `counter_reconciliation_seconds` if used. File 05 §6.

**tonic / prost** — The gRPC server/client library (tonic) and protobuf codegen (prost).

**Tower layer** — Middleware wrapping the gRPC service. Order: `GrpcTraceLayer` → `GrpcMetricsLayer` → auth interceptor.

**turmoil** — Simulated network + time for DST. File 09 §3.

**WAL (Write-Ahead Log)** — SlateDB's durability log; can live in a separate object store (`[database.wal]`). A crashed node's unflushed WAL is recovered because shard leases are permanent.

**WriteBatcher** — Trait abstracting `WriteBatch` (fast, no conflict tracking) vs transaction (SSI). `src/job_store_shard/helpers.rs`.

**xxhash (XXH64)** — The tenant hash (seed 0). Same in Rust, siloctl, and the TS client.
