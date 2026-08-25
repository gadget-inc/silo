# Silo Onboarding — Start Here

> These notes were written to get a newcomer (who may not know Rust) productive on the **silo** codebase. They were produced by reading the actual source on branch `mill/sortableTableHeaders`. Where the existing `docs/` package (a Starlight site) disagrees with the code, **the code wins** — this is also the project's stated policy (`.ai/AGENTS.md:47`).

## What is Silo, in one paragraph

Silo is a **durable, horizontally-scalable background job broker** written in Rust. Think Sidekiq / BullMQ / Temporal / Restate, but with three twists:

1. **The source of truth is object storage** (GCS / S3 / local FS), not Redis or Postgres. Silo stores everything in [SlateDB](https://slatedb.io), an LSM-tree database whose files live in a bucket. This makes it cheap at scale and gives compute/storage separation.
2. **It's a broker, not an executor.** Silo never runs your job code. *Workers* (in any language) connect over gRPC, lease tasks, run them, and report outcomes back. Silo just durably tracks state and brokers work.
3. **It's multi-tenant and sharded.** Tenants are hashed and partitioned across shards; shards are spread across cluster nodes and can split as they grow. A worker or client can hit any node and get transparently routed to the owner.

## The mental model (read this first)

```
   Enqueuer ─┐                                   ┌─ Worker (your code, any language)
             │   gRPC                       gRPC  │
             ▼                                    ▼
        ┌─────────────────── Silo cluster ──────────────────┐
        │  Node A            Node B            Node C        │
        │  ├ Shard 1 (SlateDB)   ├ Shard 3        ├ Shard 5 │
        │  └ Shard 2             └ Shard 4        └ Shard 6 │
        └────────────────────────┬──────────────────────────┘
                                  │ reads/writes SSTs + WAL
                                  ▼
                       Object storage (GCS / S3 / FS)
                          one prefix per shard
```

- **Tenant → Shard**: hash the tenant ID (xxhash64 → 16 hex chars), find which shard's range covers that hash.
- **Shard → Node**: rendezvous hashing (bounded-load) decides which node owns each shard, recomputed identically by every node and client.
- **One shard = one SlateDB instance = one object-storage prefix.**
- A job's whole life — its definition, status, attempts, results, queue position, leases, concurrency tickets — is a set of **binary key/value rows** inside its shard's SlateDB, namespaced by a 1-byte key prefix.

## Suggested reading order

| # | File | What you'll learn | Read if you want to… |
|---|------|-------------------|----------------------|
| 1 | [`01-rust-and-ecosystem-primer.md`](./01-rust-and-ecosystem-primer.md) | Just enough Rust + the crates silo uses (tokio, async, traits, `Arc`, tonic, prost, flatbuffers, DataFusion) | Read any Rust file without getting lost |
| 2 | [`02-architecture-overview.md`](./02-architecture-overview.md) | The big picture: request flow, sharding, storage, coordination | Get oriented fast |
| 3 | [`03-codebase-map.md`](./03-codebase-map.md) | Every important file & module, annotated | Find where things live |
| 4 | [`04-job-lifecycle.md`](./04-job-lifecycle.md) | **The end-to-end life of a job** (the headline) | Understand the core domain |
| 5 | [`05-storage-and-keys.md`](./05-storage-and-keys.md) | SlateDB, the key-space, codecs, counters | Understand what's on disk |
| 6 | [`06-coordination-and-sharding.md`](./06-coordination-and-sharding.md) | Cluster coordination, ownership, routing, splits | Work on clustering |
| 7 | [`07-concurrency-and-rate-limits.md`](./07-concurrency-and-rate-limits.md) | Concurrency limits, the grant scanner, gubernator rate limits | Work on limits |
| 8 | [`08-server-query-webui-observability.md`](./08-server-query-webui-observability.md) | gRPC server, SQL query engine, web UI, metrics/tracing | Work on the surface/ops |
| 9 | [`09-testing-and-verification.md`](./09-testing-and-verification.md) | Tests, DST (deterministic simulation), Alloy formal specs, CI | Write or fix tests |
| 10 | [`10-supporting-crates-and-dev-env.md`](./10-supporting-crates-and-dev-env.md) | autoscaler, compactor, siloctl, TS client, nix/just/process-compose | Run it locally or touch the tooling |
| 11 | [`11-glossary.md`](./11-glossary.md) | Every silo-specific term in one place | Look up a word |

If you only read two files, read **04 (job lifecycle)** and **02 (architecture)**.

## Project conventions you should internalize early

From `.ai/AGENTS.md` (which is the repo's `CLAUDE.md`):

- **No panics in `src/`.** Don't use `.unwrap()`, `.expect()`, `panic!`, array indexing that can panic, etc. in production code — handle errors and propagate with `?`. In **tests**, unwraps/panics are fine.
- **Tests live in `tests/`, never inline in `src/`.** Each file in `tests/` compiles to its own test binary.
- **Use `#[silo::test]`, not `#[tokio::test]`** — it sets up per-test tracing.
- **Prefer `crate::` over `super::`** for imports.
- **Strong types over strings** — newtypes and enums for closed/validated domains.
- **Typed errors with `thiserror`** (`JobStoreShardError`, `ClusterClientError`, …); propagate with `?`.
- **No global state** (`lazy_static!`, `Once`, …) — pass explicit context structs.
- **Alloy "sigils"**: comments like `[SILO-ENQ-1]` in Rust link to invariants in the formal models (`specs/*.als`). CI checks they stay in sync. Don't invent new ones without a matching Alloy assertion.

## The environment

Silo uses **nix + flakes + direnv**. If a tool like `protoc` or `flatc` is missing, run `direnv reload`. Proto/flatbuffer code is generated at build time by `build.rs` — just `cargo build`, no separate codegen step. See file 10 for the full dev-environment rundown.

## A note on accuracy

Every claim in these docs is grounded in a specific file and (usually) line number, e.g. `src/job_store_shard/enqueue.rs:316`. Line numbers drift as code changes — treat them as "look near here," and trust the function/type names over the exact line. Two known doc-vs-code drifts to be aware of are flagged in file 09 (the DST runner is now a directory, not `tests/turmoil_runner.rs`).
