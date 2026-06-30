# 03 — Codebase Map (annotated)

A directory-and-file tour so you can find things. Grouped by purpose, not alphabetically. Deep-dive files are linked.

---

## Top-level layout

```
silo/
├── src/                  ← the main `silo` crate (library + daemon)
├── silo-macros/          ← #[silo::test] proc-macro
├── silo-autoscaler/      ← k8s controller for safe scaling
├── silo-compactor/       ← out-of-process SlateDB compactor
├── siloctl/              ← operator CLI
├── typescript_client/    ← TS client + worker library
├── proto/                ← gRPC schema (silo.proto, gubernator.proto)
├── schema/               ← FlatBuffers on-disk schema (internal_storage.fbs)
├── specs/                ← Alloy formal models (job_shard.als, coordination.als)
├── tests/                ← ALL Rust tests (~70 files) + DST runner
├── benches/              ← performance benchmarks
├── example_configs/      ← TOML config examples
├── deploy/               ← raw k8s manifests (local-test, autoscaler-int-test)
├── docs/                 ← Starlight docs site (may be stale; code wins)
├── nix/ flake.nix .envrc ← nix dev environment
├── justfile process-compose.yml ← dev recipes & local cluster
├── build.rs              ← compiles proto + flatbuffers at build time
└── Cargo.toml            ← workspace + main crate manifest
```

---

## `src/` — the main crate

### Entry points & wiring
| File | Role |
|------|------|
| `main.rs` | The `silo` daemon. Boot sequence + graceful shutdown. (File 08 §1) |
| `lib.rs` | Library root: module declarations, generated-code includes (`pb`, `fb`), jemalloc global allocator, scan-option helpers. |
| `bin/silo-bench.rs` | Load generator over gRPC (uses `RoutingClient`). |
| `bin/silo-sim.rs` | In-process workload driver against `JobStoreShard` directly. |
| `settings.rs` | TOML config model (`AppConfig` and all sections). (File 08 §5) |

### gRPC server & routing
| File | Role |
|------|------|
| `server.rs` | The tonic `SiloService`, all RPC handlers, `shard_with_redirect` routing, background tasks (lease reaper, metrics scrape). (File 08 §2) |
| `cluster_client.rs` | **Node-to-node** client: local-or-remote routing, no redirects (has the coordinator). Also `ClientConfig`, `AuthInterceptor`. (File 06 §5) |
| `routing_client.rs` | **Outsider** client (benches/workers): follows redirects, refreshes topology, classifies errors. (File 06 §5) |
| `pb_convert.rs` | Conversions between protobuf wire types and internal types; redirect metadata key constants. |
| `grpc_trace.rs` | Tower layer: extracts W3C trace context from inbound gRPC headers → per-request span. (File 08 §6.2) |

### The core: job store shard
`src/job_store_shard/` is the heart. One `JobStoreShard` owns one SlateDB. (Files [04](./04-job-lifecycle.md), [05](./05-storage-and-keys.md), [07](./07-concurrency-and-rate-limits.md))

| File | Role |
|------|------|
| `mod.rs` | `JobStoreShard` struct, `open_with_resolved_store`, terminal-TTL helpers, `LsmState`, error enum. |
| `enqueue.rs` | Enqueue path; `walk_limit_chain` / `enqueue_limit_task_at_index` (the limit chain). |
| `dequeue.rs` | Dequeue path; lease creation; the internal task handlers (`handle_run_attempt`, `handle_request_ticket`, `handle_check_rate_limit`, `handle_refresh_floating_limit`). |
| `lease.rs` | Heartbeat, `report_attempt_outcome` (success/fail/cancel/retry), lease expiry & reclaim (`reap_expired_leases`). |
| `cancel.rs` | Cancellation (scheduled vs running jobs). |
| `expedite.rs` | Drag a future/retry-waiting job forward to run now. |
| `restart.rs` | Restart a Cancelled/Failed job with a fresh retry budget. |
| `import.rs` | Bulk import of jobs with historical (terminal) attempts. |
| `floating.rs` | Floating-limit scheduling, refresh success/failure. |
| `rate_limit.rs` | Gubernator check + rate-limit retry scheduling. |
| `limit_chain.rs` | `ShardChainResumer` bridge that lets the grant scanner resume a parked chain. |
| `counters.rs` | The SlateDB merge-operator counters (total/completed jobs, per-tenant-status, requesters). |
| `helpers.rs` | `WriteBatcher` trait abstracting `WriteBatch` vs transaction; `put_with_expire`. |
| `cleanup.rs` | Post-split cleanup: purge keys now outside a child's range. |
| `lease_task.rs` | Test helper RPC: directly lease one job's task. |
| `scan.rs` | Range-scan helpers used by the query engine and ops. |

### Storage & encoding
| File | Role |
|------|------|
| `keys.rs` | **The key-space**: every prefix constant + key-build/parse function. storekey encoding. (File 05 §2) |
| `codec.rs` | FlatBuffers encode/decode for every on-disk record value. (File 05 §3) |
| `storage.rs` | `resolve_object_store` — maps a `Backend` (fs/s3/gcs/memory/url/turmoilfs) + path to an `ObjectStore`. |
| `instrumented_db.rs` | Thin wrapper around SlateDB that attaches the shard's tracing span to every call. (File 05 §4) |
| `factory.rs` | `ShardFactory`: opens/closes SlateDB instances as ownership changes; `clone_closed_shard` for splits. (File 05 §5) |
| `arrow_ipc.rs` | Arrow IPC (de)serialization for streaming query results over gRPC. (File 05 §7) |
| `turmoil_object_store.rs` | DST-only in-memory simulated object store. (File 09 §3.3) |

### Domain types
| File | Role |
|------|------|
| `job.rs` | `JobInfo`, `JobStatus`, `JobStatusKind`, `Limit` and its variants, `RetryPolicy`. |
| `job_attempt.rs` | `JobAttempt`, `AttemptStatus`, `AttemptOutcome`. |
| `task.rs` | `Task` variants, `LeaseRecord`, `HolderRecord`, `LeasedTask`, `DEFAULT_LEASE_MS`. |
| `retry.rs` | Backoff math (`next_retry_time_ms`, `retries_exhausted`). |
| `shard_range.rs` | `ShardId`, `ShardRange`, `ShardMap`, `hash_tenant`, split logic. (File 06 §1) |

### Concurrency, rate limiting, brokering
| File | Role |
|------|------|
| `concurrency.rs` | In-memory concurrency counts, `try_reserve`, `handle_enqueue`, the **grant scanner** / `process_grants`, `LimitChainResumer` trait. (File 07) |
| `gubernator.rs` | Rate-limit client (real/null/mock) with request coalescing. (File 07 §5) |
| `task_broker.rs` | Per-task-group broker: in-memory buffer + background scanner + long-poll. (File 04 §3) |

### Coordination
`src/coordination/` — cluster control plane. (File [06](./06-coordination-and-sharding.md))
| File | Role |
|------|------|
| `mod.rs` | `Coordinator` trait, `CoordinatorBase`, `ShardGuardState`, rendezvous hashing, reconcile logic, coordination key names. |
| `none.rs` | Single-node dev backend. |
| `etcd.rs` | etcd backend (membership lease + KV shard ownership). |
| `k8s.rs` | k8s backend (Lease objects + ConfigMap shard map). |
| `k8s_backend.rs` | The k8s API abstraction (so tests can mock it). |
| `split.rs` | `ShardSplitter` — the online shard-split state machine. |

### Query & web UI
| File | Role |
|------|------|
| `query.rs` | Single-shard DataFusion engine: `Scan` trait, `SiloTableProvider`, scan strategies, the 5 SQL tables. (File 08 §3) |
| `cluster_query.rs` | Cross-cluster fan-out: one DataFusion partition per shard, local or via `QueryArrow` RPC. (File 08 §3.3) |
| `webui.rs` | axum + askama operator UI; `query_shards_partial` cross-shard pattern. (File 08 §4) |

### Observability
| File | Role |
|------|------|
| `metrics.rs` | Prometheus registry + instruments; `/metrics` endpoint; `GrpcMetricsLayer`. (File 08 §6.1) |
| `trace.rs` | Tracing init (text/JSON/Perfetto/OTLP), W3C propagator, test tracing. (File 08 §6.2) |
| `heap_profile.rs` | jemalloc heap profiling (backs the `HeapProfile` RPC). |
| `jemalloc_metrics.rs` | jemalloc allocator gauges. |
| `tokio_runtime_metrics.rs` | Per-worker tokio runtime metrics (needs `tokio_unstable`). |
| `dst_events.rs` | DST-only structured event log for invariant checking (no-op without `dst`). (File 09 §3.4) |

---

## `proto/` & `schema/` — the contracts
- `proto/silo.proto` — the **`Silo` gRPC service** (~30 RPCs, starts ~line 645) and all wire messages. Compiled by `build.rs` for Rust and by the TS `proto:generate` script — **one schema, both clients**.
- `proto/gubernator.proto` — the gubernator rate-limit service contract (client-only).
- `schema/internal_storage.fbs` — FlatBuffers schema for **on-disk** record values; compiled by `flatc` in `build.rs`.

---

## `tests/`, `benches/`, `specs/` — verification
- `tests/*.rs` — ~70 integration test files (one binary each). Key helpers: `tests/test_helpers.rs` (shard fixtures), `tests/grpc_integration_helpers.rs` (server fixtures). The DST runner is `tests/turmoil_runner/` (a directory: `main.rs`, `helpers.rs`, `mock_k8s.rs`, `scenarios/`). (File 09)
- `benches/*.rs` — `job_shard_throughput`, `big_shard_operations`, `query_performance`, `query_profile`, `task_scanner_profile`; shared `bench_helpers.rs` (golden shard). (File 09 §6)
- `specs/job_shard.als`, `specs/coordination.als` — Alloy formal models. (File 09 §4)

---

## Supporting crates (separate workspace members) — File 10
- `silo-macros/` — `#[silo::test]`.
- `silo-autoscaler/` — k8s controller; safe StatefulSet scale-down via a finalizer + lease-orphan detection.
- `silo-compactor/` — standalone compactor fleet; assigns shards by contiguous-range partitioning; must register the same counter merge operator.
- `siloctl/` — operator CLI mapping 1:1 to gRPC RPCs.
- `typescript_client/` — `@silo/client`: `SiloGRPCClient`, `SiloWorker`, `JobHandle`; client-side shard routing matching the Rust hashing.

---

## Quick "where do I go to change X?"

| I want to change… | Go to |
|-------------------|-------|
| What happens on enqueue | `src/job_store_shard/enqueue.rs` |
| How workers get work | `src/job_store_shard/dequeue.rs` + `src/task_broker.rs` |
| Retry/backoff behavior | `src/job_store_shard/lease.rs` (decision) + `src/retry.rs` (math) |
| The key/value layout | `src/keys.rs` + `src/codec.rs` + `schema/internal_storage.fbs` |
| Concurrency/rate limits | `src/concurrency.rs`, `src/gubernator.rs`, `src/job_store_shard/{floating,rate_limit,limit_chain}.rs` |
| Cluster ownership / splits | `src/coordination/` |
| A gRPC method | `proto/silo.proto` → `src/server.rs` → `src/cluster_client.rs` (+ TS client) |
| SQL tables / queries | `src/query.rs`, `src/cluster_query.rs` |
| The web UI | `src/webui.rs` + `templates/` |
| Config options | `src/settings.rs` + `example_configs/` |
| Metrics/tracing | `src/metrics.rs`, `src/trace.rs` |
