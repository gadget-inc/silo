# 05 — Storage Layer & Key Encoding

How silo persists everything. Mental model first, then the full key-space, then serialization, then the lifecycle plumbing (factory, counters, compaction).

---

## 1. SlateDB, as silo uses it

**SlateDB** is an embedded LSM-tree key/value store whose data files (SSTs, WAL, manifest) live in **object storage** rather than local disk. LSM = writes land in an in-memory memtable + a write-ahead log (WAL), get flushed to immutable sorted files (SSTs), and a *compactor* later merges those. Reads check memtable → WAL → SSTs.

### One DB per shard
A `JobStoreShard` (`src/job_store_shard/mod.rs:181`) "owns the SlateDB instance" as `db: Arc<InstrumentedDb>`. It's built in `open_with_resolved_store` (`mod.rs:421`):

```rust
let mut db_builder = slatedb::DbBuilder::new(db_path, store.clone())
    .with_merge_operator(counters::counter_merge_operator())   // for i64 counters
    .with_metrics_recorder(slatedb_metrics_recorder.clone());
```

Optional extras: a separate WAL object store (`with_wal_object_store`), custom SlateDB `Settings` (deep-merged onto defaults), and an in-memory `FoyerCache` (block + meta cache wrapped in a `SplitCache`).

### Object-store backends (`src/storage.rs`)
`resolve_object_store(backend, path)` (`storage.rs:30`) maps a `Backend` to an `ObjectStore`:
- **`Fs`** — local filesystem (creates+canonicalizes the dir). Used in most tests as a stand-in for a bucket.
- **`Memory`** — in-memory (tests).
- **`S3 | Gcs | Url`** — delegates to SlateDB's resolver, interpreting `path` as a URL (`gs://bucket/prefix`, `s3://bucket/prefix`).
- **`TurmoilFs`** (`dst` feature only) — the deterministic simulated FS (file 09 §3.3).

### Reads / writes / scans (all via `InstrumentedDb`)
- Point read: `get` → `Option<Bytes>`, `get_key_value` → `Option<KeyValue>`.
- Writes: `put`, `delete`, `merge`, and batched `write(WriteBatch)`.
- Range scans: `scan(range)` / `scan_with_options(range, &ScanOptions)` → an iterator yielding `KeyValue`s in **byte order**.
- Transactions: `begin(isolation_level)`.
- Durability: `flush` / `flush_with_options`.

Silo writes in **two modes**, abstracted by the `WriteBatcher` trait (`src/job_store_shard/helpers.rs:23`):
- **`WriteBatch`** — fast, no conflict tracking. For paths that don't need read-modify-write.
- **Transaction** (Serializable Snapshot Isolation) — conflict-tracked, supports read-modify-write, retries on conflict (`JobStoreShardError::TransactionConflict`). Used by enqueue-with-dedup, cancel, expedite, etc.

The trait also exposes `put_with_expire` (row TTL via `PutOptions { ttl: Ttl::ExpireAt(...) }`) — how terminal-job expiration works.

---

## 2. Key encoding (`src/keys.rs`)

### Why order-preserving keys
Every query in silo is a **range scan over an LSM tree**, which iterates keys in byte order. So keys are built with **`storekey`**, an order-preserving binary encoding: comparing two encoded keys byte-by-byte gives the same result as comparing the original typed tuples. That's what makes "all jobs for tenant T", "Scheduled jobs newest-first", "tasks in a group by start time" each a contiguous byte range.

Every key is `[1-byte prefix] ++ storekey::encode_vec(tuple)` (`encode_with_prefix`, `keys.rs:39`). The prefix namespaces record types. `end_bound(prefix)` (`keys.rs:690`) computes the exclusive upper bound for a prefix scan (increment the rightmost non-`0xFF` byte). Callers form ranges as `prefix..end_bound(prefix)`.

### The prefix table (`keys.rs:15`)

| Prefix | Const | Key tuple | Value | Notes |
|--------|-------|-----------|-------|-------|
| `0x01` | `JOB_INFO` | `(tenant, job_id)` | FB `JobInfo` | Immutable job definition |
| `0x02` | `JOB_STATUS` | `(tenant, job_id)` | FB `JobStatus` | Current state |
| `0x03` | `IDX_STATUS_TIME` | `(tenant, status, inv_ts, job_id)` | marker | **Inverted** ts ⇒ newest-first; for Scheduled uses `next_attempt_starts_after_ms` |
| `0x04` | `IDX_METADATA` | `(tenant, key, value, job_id)` | marker | Find jobs by metadata |
| `0x05` | `TASK` | `(task_group, start_time_ms, priority, job_id, attempt, epoch_ms)` | FB `Task` union | The execution queue |
| `0x06` | `LEASE` | `(task_id)` | FB `LeaseRecord` | In-flight task |
| `0x07` | `ATTEMPT` | `(tenant, job_id, attempt)` | FB `JobAttempt` | Per-attempt record + result |
| `0x08` | `CONCURRENCY_REQUEST` | `((tenant,queue,start,priority),(job_id,attempt,suffix))` | request payload | Queued for a slot (nested tuple — storekey caps at 6 fields) |
| `0x09` | `CONCURRENCY_HOLDER` | `(tenant, queue, task_id)` | FB `HolderRecord` | Holds a slot |
| `0x0A` | `JOB_CANCELLED` | `(tenant, job_id)` | FB `JobCancellation` | Cancel flag, **separate from status** |
| `0x0B` | `FLOATING_LIMIT` | `(tenant, queue_key)` | FB `FloatingLimitState` | Dynamic-max state |
| `0xF0` | `COUNTER_TOTAL_JOBS` | singleton | LE i64 | All non-deleted jobs |
| `0xF1` | `COUNTER_COMPLETED_JOBS` | singleton | LE i64 | Terminal jobs |
| `0xF2`–`0xF6` | cleanup keys | singletons | various | Split-cleanup checkpoints/markers, shard-created-at |
| `0xF7` | `COUNTER_CONCURRENCY_REQUESTERS` | `(tenant, queue)` | LE i64 | Pending requesters per queue |
| `0xF8` | `COUNTER_TENANT_STATUS` | `(tenant, status_kind)` | LE i64 | Per-(tenant,status) count |

The `0xF0+` range sorts *after* all per-record data, so counters/cleanup keys never interleave with job rows.

### Two subtleties to know
- **The `TASK` key's trailing `epoch_ms`** is a *write-time disambiguator*, not part of task identity (`keys.rs:233`). It lets a chain rewrite re-emit a task at the same logical `start_time_ms` without colliding with the broker's ack-tombstone from the just-deleted predecessor. Readers that know identity-but-not-epoch use `task_key_lookup_prefix` and prefix-scan.
- **`0x08` uses a nested tuple** `((tenant,queue,start,priority),(job_id,attempt,suffix))` because storekey tuples max out at 6 elements; since storekey encodes sequentially, the nested form is byte-identical to a flat 7-field encoding.

---

## 3. Value serialization

Three formats, by purpose:

1. **FlatBuffers** — all durable record *values* (`src/codec.rs`, schema `schema/internal_storage.fbs`, compiled by `flatc` in `build.rs`). Every `encode_*` builds a `FlatBufferBuilder`. Decoding prefers **zero-copy validated wrappers** (`DecodedJobInfo`, `DecodedTask`, `DecodedLease`, …) generated by the `decoded_wrapper!`/`decode_fn!` macros — validate once, then read fields without copying. Owned decoders exist where mutation is needed. Forward-compat: unknown limit/union variants are silently dropped.
2. **LE i64** — counter values (not FlatBuffers); summed by a merge operator (§6).
3. **MessagePack** (`rmp-serde`) — job **payloads** (opaque to silo) and some query rows. Protobuf (`prost`) is the *wire/API* format, not used for on-disk values.

> Rule: **keys = storekey, values = FlatBuffers, counters = LE i64, payloads = MessagePack, API = protobuf.**

`codec.rs` is the FlatBuffers layer (encode + zero-copy decode). `arrow_ipc.rs` is unrelated to storage — it's the **query-result** transport (Arrow IPC stream over gRPC); see §7.

---

## 4. `instrumented_db.rs` — what it actually does

`InstrumentedDb` is **not a metrics wrapper** — it's a **tracing-span** wrapper. It holds `inner: Arc<Db>` + a per-shard `span`, and `.instrument(self.span.clone())`s every async DB call. Why: SlateDB logs via the `log` crate, which `tracing-subscriber` bridges into tracing; awaiting SlateDB under the shard span tags SlateDB's own log lines with the shard. It deliberately does **not** implement `Deref<Target = Db>` (that caused past coverage gaps) — if a method is missing, add a wrapper, don't reach for `inner()`. Actual SlateDB performance metrics come separately via a `DefaultMetricsRecorder` + a status watcher.

---

## 5. `factory.rs` — ShardFactory (open/close lifecycle)

`ShardFactory` (`src/factory.rs:56`) holds `instances: DashMap<ShardId, Arc<ShardEntry>>` and opens/closes SlateDB instances as shard ownership moves between nodes.

- **Atomic open** — each `ShardEntry` wraps a `tokio::sync::OnceCell`, so concurrent opens of the *same* shard are serialized (only one actually opens) while *different* shards open in parallel.
- **Path templating** — the configured path must contain a `%shard%`/`{shard}` placeholder at a directory boundary; resolution differs by backend (local FS resolves at the root before the placeholder; object stores substitute it).
- **Close with timeout + re-insert on failure** (`factory.rs:319`) — `close` removes the entry then `timeout(30s, shard.close())`. The timeout exists because SlateDB's retrying object store retries transient errors indefinitely. On error/timeout it **re-inserts** the shard so close can be retried — this prevents the "release-lease-then-lose-unflushed-data" hazard. (A `Closed` error is treated as success.)
- **Clone for splits** (`clone_closed_shard`, `factory.rs:545`) — operates on a *fully closed* parent: reopens a raw SlateDB (with the counter merge operator), flushes, writes+deletes a sentinel to advance the WAL, creates one `CheckpointScope::All` checkpoint, and uses SlateDB's `Admin::create_clone_builder` to clone that checkpoint into both child DB paths.

The coordinator's per-shard guard loops call `factory.open(...)` **before** marking a shard held, and `factory.close(...)` **before** releasing the lease (file 06 §3/§6).

---

## 6. Counters & compaction

### Counters via merge operator (`src/job_store_shard/counters.rs`)
Counters (`0xF0/0xF1/0xF7/0xF8`) avoid read-modify-write by using SlateDB's **MergeOperator** to sum i64 deltas at read/compaction time. The operator decodes existing + operand, `saturating_add`s, re-encodes (`counters.rs:132`). Writes go through `WriteBatcher::merge`. For transactions, `merge` also calls `unmark_write` to keep these shard-global keys *out* of SSI conflict detection (otherwise they'd cause constant transaction conflicts under load).

`ShardCounters { total_jobs, completed_jobs }` with `open_jobs() = total - completed`.

### Reconciliation
Because the **standalone compactor** drops expired terminal rows *without a writable DB handle*, it can't decrement counters at drop-time. So if you enable row TTLs (`completed_job_expire_s` / `terminal_job_expire_s`), enable `counter_reconciliation_seconds` too — `spawn_counter_reconcile_task` periodically rebuilds counters from `JobInfo`/`JobStatus` ground truth.

### Compaction
- **Row TTL** drops terminal job rows during compaction.
- **`silo-compactor`** (a separate crate, file 10 §3) runs SlateDB compaction out-of-process. It **must register the same `counter_merge_operator`** so counter merges sum correctly during compaction. The coordinator assigns shards to compactor pods.
- LSM observability: `JobStoreShard` exposes `LsmState` (L0 SSTs + sorted runs with sizes), surfaced via `GetShardStorageInfo`.

### Cleanup keys (`0xF2`–`0xF6`)
Track post-split cleanup state (progress checkpoint for crash resumption, complete marker, authoritative status, created-at, completed-at). Logic in `src/job_store_shard/cleanup.rs`.

---

## 7. `arrow_ipc.rs` (not storage — query transport)

Serializes DataFusion/Arrow `RecordBatch`es to/from **Arrow IPC stream** format so query results can stream over gRPC (`schema_to_ipc`, `batch_to_ipc`, `ipc_to_batches`). Part of the SQL query feature (file 08 §3), gated behind the `server` feature. Distinct from the FlatBuffers used for durable records.

---

## 8. Mental model recap

- **One shard = one SlateDB = one object-store prefix.** `ShardFactory` opens/closes them; splits clone a closed parent into two children via a shared checkpoint.
- **Keys** are `[prefix][storekey tuple]`, order-preserving so every read is a range scan. `0x01`–`0x0B` = data/index, `0xF0+` = counters/cleanup.
- **Values** = FlatBuffers (records) or LE-i64 (counters, summed by a merge operator both the shard *and* the standalone compactor must install).
- **`InstrumentedDb`** = tracing span wrapper (not metrics).

Next: [`06-coordination-and-sharding.md`](./06-coordination-and-sharding.md).
