# 01 — Rust & Ecosystem Primer (for the silo newcomer)

You said you don't know Rust. This file gives you *just enough* to read silo's source confidently. It's not a Rust course — it's a "here's what you'll see in these files and what it means" cheat sheet. Skim it once, then refer back when a symbol confuses you.

---

## 1. The absolute basics of reading Rust

### Variables, mutability, ownership

```rust
let x = 5;              // immutable by default
let mut y = 10;         // `mut` = you can reassign / mutate it
y += 1;
```

Rust's headline feature is **ownership**: every value has exactly one owner; when the owner goes out of scope, the value is freed (no garbage collector). You'll constantly see:

- `&x` — a **shared/immutable borrow** (a read-only reference). Many allowed at once.
- `&mut x` — a **mutable borrow**. Only one at a time, and not while shared borrows exist. This is the "borrow checker" enforcing no data races at compile time.
- `x.clone()` — make an owned copy (used when you can't or don't want to share a reference).
- A function taking `x` (no `&`) **moves** ownership in; the caller can't use `x` afterward unless the type is `Copy` (small things like integers).

When you see lots of `.clone()` in silo, it's usually cloning an `Arc` (cheap — see below), not deep-copying data.

### Functions, returns, the `?` operator

```rust
fn parse(s: &str) -> Result<i64, MyError> {
    let n = s.parse::<i64>()?;   // `?` = if Err, return it early; if Ok, unwrap the value
    Ok(n * 2)                    // last expression with no `;` is the return value
}
```

- **No `return` needed** for the final expression — the last line without a semicolon *is* the return.
- **`?`** is the workhorse for error handling: "do this fallible thing; on failure, bubble the error up to my caller." Silo uses it everywhere instead of try/catch.

### `Result` and `Option` — Rust has no `null` and no exceptions

- `Option<T>` is `Some(value)` or `None`. This replaces null. `current_attempt: Option<u32>` means "maybe a number, maybe absent."
- `Result<T, E>` is `Ok(value)` or `Err(error)`. This replaces exceptions. Functions that can fail return `Result`.
- You "unwrap" these with pattern matching, `?`, `.unwrap()` (panics on the bad case — **banned in src/**, fine in tests), or helpers like `.unwrap_or(default)`, `.map(...)`, `.ok_or(err)`.

### Pattern matching with `match`

```rust
match status.kind {
    JobStatusKind::Succeeded => { /* ... */ }
    JobStatusKind::Failed | JobStatusKind::Cancelled => { /* ... */ }
    _ => { /* the catch-all */ }
}
```

`match` is exhaustive — the compiler forces you to handle every case (or use `_`). This is why adding a new enum variant is safe: the compiler shows you every place that must change. You'll also see `if let Some(x) = maybe { ... }` as shorthand for matching one case.

### Structs, enums, impl blocks

```rust
struct JobStatus {            // a record / "class with only data"
    kind: JobStatusKind,
    changed_at_ms: i64,
}

enum JobStatusKind {          // a closed set of variants ("tagged union")
    Scheduled, Running, Failed, Cancelled, Succeeded,
}

impl JobStatus {              // methods go in `impl` blocks, separate from the data
    fn is_terminal(&self) -> bool { /* &self = a method that borrows the value */ }
    fn running(changed: i64) -> JobStatus { /* no self = an associated/static fn, like a constructor */ }
}
```

Rust enums are powerful — variants can carry data: `Succeeded { finished_at_ms: i64, result: Vec<u8> }`. That's how silo models "an attempt that succeeded *with* a result payload" in one type.

### Traits = interfaces

```rust
trait RateLimitClient {
    async fn check_rate_limit(&self, ...) -> Result<RateLimitResult, Error>;
}
```

A **trait** is like an interface in Java/TS. Types `impl SomeTrait for SomeType` to satisfy it. Silo uses traits heavily to swap implementations: `Coordinator` (etcd vs k8s vs none), `RateLimitClient` (real vs null vs mock), `Scan` (each SQL table). `Box<dyn Coordinator>` / `Arc<dyn Coordinator>` means "some value implementing `Coordinator`, decided at runtime" (dynamic dispatch).

### Generics and lifetimes (don't panic)

- `Vec<u8>` = a growable array of bytes; `HashMap<K, V>`, `Option<T>` — angle brackets are generic type parameters, same idea as TS `Array<T>`.
- `<'a>` and `&'a Foo` are **lifetimes** — annotations telling the compiler how long a reference is valid. You rarely need to *write* them to read silo; just mentally skip `'a` as "this reference."

### Macros end with `!`

`println!`, `vec![1,2,3]`, `info_span!(...)`, `assert_eq!(...)`. A `!` means it's a macro (code that generates code), not a normal function call. `#[derive(Debug, Clone)]` above a struct is an *attribute* macro that auto-generates the `Debug` (printable) and `Clone` (copyable) implementations.

---

## 2. Async Rust & Tokio (critical for silo)

Silo is an async network server. Almost everything is `async`.

```rust
async fn enqueue(&self, ...) -> Result<...> {
    let info = self.db.get(key).await?;   // `.await` yields until the I/O completes
    ...
}
```

- **`async fn`** returns a *future* — a lazy computation that does nothing until `.await`ed.
- **`.await`** is the suspension point: "pause here, let other tasks run, resume when ready." It is *cooperative* — a task only yields at `.await` points.
- **[Tokio](https://tokio.rs)** is the async runtime that schedules these futures across a thread pool. `#[tokio::main]` on `main()` and `tokio::spawn(future)` (launch a concurrent task) are everywhere.
- `tokio::select! { ... }` waits on several futures and proceeds with whichever finishes first (used for "do work OR shut down").
- `tokio::time::{sleep, interval, timeout}` for time; in tests/DST this time can be *simulated* (see file 09).

**Why this matters for correctness:** because a task can be paused at any `.await`, another task can run in between. Silo is full of comments about ordering operations carefully around `.await` points (e.g. update in-memory state *before* the durable write, roll back if the write fails). The clippy lint `await_holding_lock` and silo's `significant_drop_in_scrutinee` guard against holding a lock across an `.await`, which would be a deadlock/contention hazard.

---

## 3. Shared state: `Arc`, `Mutex`, `DashMap`, `RwLock`

Concurrency needs shared, thread-safe state. The toolkit:

- **`Arc<T>`** = "Atomically Reference-Counted" smart pointer. It's how you share one value across many tasks/threads. `.clone()`ing an `Arc` just bumps a counter — cheap. You'll see `Arc<JobStoreShard>`, `Arc<dyn Coordinator>`, etc. everywhere.
- **`Mutex<T>` / `RwLock<T>`** = locks. `Mutex` = one accessor at a time; `RwLock` = many readers or one writer. There are two flavors: `std::sync`/`parking_lot` (sync, must not be held across `.await`) and `tokio::sync` (async, can be held across `.await`). The combo `Arc<Mutex<HashSet<ShardId>>>` (shared, lockable set of owned shards) is idiomatic.
- **`DashMap<K, V>`** = a concurrent hash map (sharded internally) you can read/write from many tasks without wrapping it in a `Mutex`. Silo uses it for per-shard instances and per-queue concurrency counts.
- **`watch` / `broadcast` / `mpsc` / `oneshot`** = tokio channels for passing messages between tasks. `watch` = "latest value" (used for shutdown signals), `mpsc` = multi-producer queue, `oneshot` = single reply.
- **`Notify`** = a lightweight "wake up whoever's waiting" primitive (used by the task broker and grant scanner).
- **`OnceCell`** = a slot initialized exactly once (used so concurrent shard-opens don't double-open).
- **`Weak<T>`** = a non-owning `Arc` reference that won't keep the value alive — used to break reference cycles (e.g. the limit-chain resumer holds a `Weak<JobStoreShard>`).

---

## 4. Error handling crates

- **`thiserror`** — derive macro for *library* error enums. You'll see:
  ```rust
  #[derive(thiserror::Error, Debug)]
  pub enum JobStoreShardError {
      #[error("job not found")]
      JobNotFound,
      #[error(transparent)]
      Codec(#[from] CodecError),   // #[from] auto-converts CodecError into this via `?`
  }
  ```
  The `#[from]` is what lets `?` automatically convert one error type into another.
- **`anyhow`** — a catch-all dynamic error type (`anyhow::Result<T>`), used in binaries/tooling where you don't need typed errors. `bail!("msg")` early-returns an error.

Rule of thumb in silo: **`thiserror` for library code with typed errors, `anyhow` for top-level binaries.**

---

## 5. The networking & serialization stack

Silo speaks gRPC and serializes data three different ways. Know which is which:

| Crate / format | What it's for | Where you see it |
|----------------|---------------|------------------|
| **`tonic`** | gRPC server & client (over HTTP/2) | `src/server.rs`, `cluster_client.rs` |
| **`prost`** | Protobuf code-gen for the **wire/API** types | generated into `crate::pb` from `proto/silo.proto` |
| **FlatBuffers** (`flatbuffers`) | **On-disk** record values in SlateDB (zero-copy reads) | `src/codec.rs`, schema in `schema/internal_storage.fbs` |
| **`rmp-serde`** (MessagePack) | Job **payloads** (opaque to silo) and some query rows | client payloads, webui row decode |
| **`storekey`** | **Order-preserving** binary encoding of KV **keys** | `src/keys.rs` |
| **Arrow IPC** (`arrow`/`datafusion`) | Streaming **query results** | `src/arrow_ipc.rs`, `QueryArrow` RPC |

Key insight: **keys** are storekey-encoded (so byte-order == logical order, enabling range scans), **values** are FlatBuffers (fast validated zero-copy), **payloads** are opaque MessagePack blobs silo never looks inside, and the **API** is protobuf/gRPC.

### gRPC vocabulary you'll need

- A **service** (`Silo`) is a set of **RPCs** (methods). Defined in `proto/silo.proto`, the service starts around line 645.
- `tonic` generates a `SiloServer<T>` (you implement the trait) and a `SiloClient` (you call it).
- `Status` is gRPC's error type with a **code** (`NotFound`, `Unavailable`, `FailedPrecondition`, `Unauthenticated`, …). Silo overloads these codes for routing: `NotFound` + metadata = "redirect to another node," `Unavailable` = "shard busy/acquiring," `FailedPrecondition` = "your topology is stale."
- **Streaming RPCs** return a stream of messages (`QueryArrow` streams Arrow batches).
- **Interceptors** wrap requests (silo injects auth tokens & trace context, checks the Bearer token).

---

## 6. The big external dependencies, in plain terms

- **SlateDB** — an embedded LSM-tree key/value store whose data files (SSTs, WAL, manifest) live in **object storage**. Silo opens one SlateDB per shard. "LSM" = writes go to an in-memory memtable + a write-ahead log, periodically flushed to immutable sorted files (SSTs), which a *compactor* later merges. Reads check memtable → WAL → SSTs. (See file 05.)
- **DataFusion** — an embeddable SQL query engine (Apache Arrow-based). Silo exposes its shard data as SQL tables (`jobs`, `queues`, `tasks`, …) and lets DataFusion plan/execute `SELECT`s, pushing filters down into SlateDB range scans. (See file 08.)
- **etcd** — a distributed key/value store used as one coordination backend (membership leases + shard ownership). (See file 06.)
- **kube** — the Kubernetes client library; the other coordination backend uses k8s `Lease`/`ConfigMap` objects, and the autoscaler is a k8s controller. (See files 06 & 10.)
- **gubernator** — an external, distributed **rate-limiting** service (separate process). Silo calls it over gRPC for rate-limit checks. (See file 07.)
- **turmoil / mad-turmoil** — deterministic simulation testing: fake network + fake clock + seeded randomness so distributed bugs are reproducible from an integer seed. (See file 09.)
- **Alloy** — a formal modeling language; silo proves its algorithms' invariants in `specs/*.als` and links them to code via `[SILO-...]` sigils. (See file 09.)
- **jemalloc** — the memory allocator (on unix), with heap profiling compiled in. (See file 08.)
- **tracing** — structured, span-based logging/observability (think structured logs + distributed traces). `info_span!`, `#[instrument]`, `.instrument(span)`.

---

## 7. Cargo & the workspace

- **`cargo`** is Rust's build tool + package manager. `Cargo.toml` = manifest (deps, metadata); `Cargo.lock` = pinned versions.
- A **workspace** is multiple crates (packages) built together. Silo's workspace members: the root `silo` crate, plus `silo-macros`, `silo-autoscaler`, `silo-compactor`, `siloctl` (see `Cargo.toml:1-2`).
- A **crate** is a compilation unit: either a library (`src/lib.rs`) or a binary (`src/main.rs`, `src/bin/*.rs`).
- **Features** are compile-time flags: `server` (pulls in DataFusion + web UI), `k8s`, `dst`, `tokio-console`. `default = ["k8s", "server"]`. Some crates build silo with `default-features = false` to skip the heavy DataFusion compile.
- Common commands: `cargo build`, `cargo test`, `cargo test <name>`, `cargo test --test <file>`, `cargo clippy` (linter), `just fmt` (clippy --fix + rustfmt).

---

## 8. A worked example: reading one real silo function

Here's a (lightly simplified) snippet shape you'll meet in `src/job_store_shard/`:

```rust
pub async fn cancel_job(
    self: &Arc<Self>,            // method on an Arc<JobStoreShard>
    tenant: &str,
    job_id: &str,
) -> Result<(), JobStoreShardError> {
    // run inside a transaction, retrying if another writer conflicts
    retry_on_txn_conflict(|| async {
        let mut txn = self.db.begin(SerializableSnapshot);
        let status = txn.get(job_status_key(tenant, job_id)).await?   // ? bubbles errors
            .ok_or(JobStoreShardError::JobNotFound)?;                  // None -> error
        if status.kind.is_final() {
            return Err(JobStoreShardError::JobAlreadyTerminal);
        }
        txn.put(job_cancelled_key(tenant, job_id), encode(...))?;
        txn.commit().await?;                                          // durable write
        Ok(())
    }).await
}
```

Reading it: it's an `async` method on a shared `Arc<JobStoreShard>`; it returns `Result<(), Error>` (where `()` is "unit" / void). It opens a transaction, reads a key (`?` propagates I/O errors, `.ok_or(...)?` turns a missing row into a typed error), guards a precondition, writes a key, and commits. No try/catch, no nulls, no exceptions — just `Result`, `?`, `Option`, and explicit ownership. Once this shape clicks, most of silo reads the same way.

---

### TL;DR survival kit

- `?` = propagate error; `Option`/`Result` = no null/no exceptions; `match` = exhaustive switch.
- `.await` = async pause point; `tokio` = the runtime; `Arc` = cheap shared pointer.
- traits = interfaces; `dyn` = runtime-chosen implementation.
- keys = storekey, values = FlatBuffers, payloads = MessagePack, API = protobuf/gRPC.
- No panics in `src/`. Errors are typed with `thiserror` and bubbled with `?`.

Now go read [`02-architecture-overview.md`](./02-architecture-overview.md).
