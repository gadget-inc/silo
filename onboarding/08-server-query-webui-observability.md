# 08 — Server, Query Engine, Web UI & Observability

The "surface" of a silo node: how it boots, serves gRPC, answers SQL queries, renders the operator UI, and exposes metrics/traces.

---

## 1. Server startup (`src/main.rs`)

`main` is `#[tokio::main]`. CLI via `clap`: `-v` (verbose), `-c/--config <PATH>`. Boot order:

1. **Config first** — `AppConfig::load(path)`; a config error → stderr + `exit(1)`. (Loaded before tracing so log format can be set.)
2. **Tracing** — `trace::init(cfg.logging.format)`.
3. **Metrics** — `metrics::init()` if enabled; failure is non-fatal.
4. **Rate limiter** — `GubernatorClient` if an address is configured, else `NullGubernatorClient`. Typed `Arc<dyn RateLimitClient>`.
5. **Factory** — `Arc<ShardFactory>` (the coordinator manages shard lifecycle through it).
6. **Coordinator** — `create_coordinator(...)` per `cfg.coordination.backend` (none/etcd/k8s). `node_id` = config or random UUID.
7. **Re-hydrate paused shards** — `ShardSplitter::rehydrate_paused_shards()` so a shard that was mid-split before a restart keeps rejecting traffic.
8. **Bind gRPC early** — `TcpListener::bind` to fail fast on port conflict.
9. **Spawn gRPC server** — `run_server(...)`.
10. **Optional**: web UI, metrics HTTP server, tokio-runtime metrics scraper (1s), jemalloc metrics scraper (1s, unix).

**Shutdown** — `select!` on SIGINT/SIGTERM → broadcast shutdown. Drains gRPC with a hard 5s timeout (tonic has no built-in drain timeout), awaits web UI/metrics, then `coordinator.shutdown()` (orders **close → release-lease**, critical for permanent leases), then `factory.close_all()` as a safety net, then `trace::shutdown()` to flush spans.

### Binaries
- `silo` (`src/main.rs`) — the daemon (needs `server` feature).
- `silo-bench` (`src/bin/silo-bench.rs`) — gRPC load generator using `RoutingClient`.
- `silo-sim` (`src/bin/silo-sim.rs`) — in-process workload against `JobStoreShard` (no gRPC).
- (`silo-compactor` is a *separate crate*, not in `src/bin/` — file 10 §3.)

---

## 2. The gRPC server (`src/server.rs`)

### `SiloService` (`server.rs:215`)
`#[derive(Clone)]`, holds `factory`, `coordinator`, `cfg`, `metrics`, and a `cluster_info_cache`/`inflight` (to singleflight `GetClusterInfo` and serve stale on timeout). Implements `#[tonic::async_trait] impl Silo for SiloService`.

### Wiring tonic (`run_server_with_incoming`, `server.rs:2112`)
Generic over the incoming stream so simulations can inject a custom accept loop. The build:
1. `SiloServer::new(svc)` with 32 MiB max encode/decode message size.
2. Wrapped in an **auth interceptor** (`make_auth_interceptor`): if `auth_token` is set, requires `authorization: Bearer <token>`, else passthrough.
3. **Health** (`tonic_health`) + **Reflection** (`tonic_reflection`, enables `grpcurl`).
4. Tuned HTTP/2 flow control (4 MiB connection / 2 MiB stream windows; `http2_max_pending_accept_reset_streams(1000)`).
5. **Two tower layers, order matters**: `GrpcTraceLayer` *then* `GrpcMetricsLayer` (so spans/logs carry the caller's trace, and metrics see the final status).
6. `serve_with_incoming_shutdown`.

### Background tasks (co-located, tied to shutdown)
- **Lease reaper** — every 1s, per owned shard: `reap_expired_leases` (file 04 §4), update open-shards gauge.
- **SlateDB metrics scrape** — every 1s, decoupled so a backpressured write can't stall observability.
- **Optional periodic full-compaction ticker** — if `periodic_full_compaction_s` set, `submit_full_compaction()` per shard (the spec is consumed by the standalone compactor).

### The RPC surface
~30 RPCs (proto service starts ~`silo.proto:645`). Job RPCs are in file 04 §13; coordination RPCs in file 06 §7. Plus: `Query`/`QueryArrow` (§3), `CpuProfile`/`HeapProfile`/`DumpTasks` (profiling), `ImportJobs`, `ResetShards` (dev_mode only). Routing semantics (NOT_FOUND+redirect / UNAVAILABLE / FAILED_PRECONDITION) via `shard_with_redirect` — file 06 §5.

`execute_shard_query` (`server.rs:376`) runs a per-shard query under `statement_timeout` (default 5s); dropping the DataFusion stream aborts execution, so the timeout really stops the query.

---

## 3. The SQL query engine

### Single-shard (`src/query.rs`)
`ShardQueryEngine` wraps a DataFusion `SessionContext` and registers **five tables**, each a `SiloTableProvider` over a scanner implementing the custom `Scan` trait:

| Table | Backing data | Key columns |
|-------|--------------|-------------|
| `jobs` | `0x01`+`0x02`+indexes | shard_id, tenant, id, priority, status_kind, task_group, current_attempt, metadata (Map) |
| `queues` | `0x08`/`0x09` | tenant, queue_name, entry_type (holder/requester), task_id, job_id, priority |
| `tenant_counts` | `0xF8` | tenant, status_kind, cnt |
| `queue_counts` | `0xF7`/`0x09` | tenant, queue_name, holders, requesters, max_concurrency, limit_type |
| `tasks` | `0x05` | task_group, start_time_ms, priority, job_id, attempt, variant_type, task_id, held_queues |

**Architecture:** `Scan` trait → `SiloTableProvider` (a DataFusion `TableProvider`) → `SiloExecutionPlan` (a custom `ExecutionPlan`). The `Scan` trait's `classify_filters()` returns `Exact`/`Inexact` per filter — returning `Exact` suppresses DataFusion's post-filter and **enables LIMIT pushdown into the scan**.

**How a SELECT becomes range scans (jobs path):** filters are collapsed into a `JobsScanStrategy` enum — `ExactId{tenant,id}` (single lookup), `MetadataExact/Prefix` (metadata-index scan, `0x04`), `Status{tenant,status}` (status/time index, `0x03`), or `FullScan{tenant}`. Each strategy bottoms out in a SlateDB `scan_with_options(prefix..end_bound(prefix))`. A fast path can serve a query purely from the status/time index with **zero point-lookups**. Everything streams over an mpsc channel into a `RecordBatchStreamAdapter` (incremental results).

### Cross-cluster (`src/cluster_query.rs`)
`ClusterQueryEngine` registers the same five tables, but each `ExecutionPlan` creates **one DataFusion partition per shard**. `build_shard_configs` asks the coordinator for all shards: `Local` if open here, else `Remote{addr}`. On execute, a Local partition runs the single-shard scanner directly; a Remote partition calls the **`QueryArrow`** streaming RPC (rebuilding SQL from pushed-down filters via DataFusion's `Unparser`), decoding Arrow IPC back to batches — with bounded retry on `Unavailable` and redirect-following on `NotFound`. DataFusion's aggregation/sort operators sit above the union of partitions.

So a cluster query plans once, opens N partitions, each scans a local shard or makes a `QueryArrow` gRPC call to the owner, and DataFusion aggregates.

---

## 4. The web UI (`src/webui.rs`)

**Stack**: server-rendered HTML — **axum** routing + **askama** templating, with **htmx + Tailwind from CDN** for interactivity. No SPA/JS build step. Templates live in `templates/`.

**Routes** (`create_router`): `/` (cluster index), `/job` (+ `/job/cancel`), `/queues`, `/queue`, `/tenants`, `/tenant`, `/cluster`, `/shard` (+ `/shard/:id/split`, `/shard/:id/compact`), `/sql` (+ `/sql/execute`), `/config`. The UI surfaces topology/members, per-shard status (incl. split phase, cleanup status, parent shard, ring), tenants, queues (holders/requesters), individual jobs (with cancel), split/compact actions, a config viewer, and an ad-hoc SQL console.

### The cross-shard RPC pattern (important constraint)
The web UI **must work regardless of which node serves the HTTP request** — **never read shard data directly from `ShardFactory`** (which only has locally-owned shards). Two correct approaches:
1. **`ClusterQueryEngine`** for aggregate reads (jobs, counts, SQL console) — auto fans out local+remote and aggregates.
2. **`query_shards_partial<T>`** for partial-tolerant views — fans out `cluster_client.query_shard(shard, sql)` concurrently and **degrades gracefully**: a down shard is reported as unavailable rather than failing the whole page. It detects the "owned-but-not-open" case (owner is local but factory doesn't have it) and reports it instead of querying.

> **If you add new shard-specific data to the UI, follow this pattern** (`.ai/AGENTS.md:160`): add a gRPC RPC → implement the server handler reading from the local factory → add a `ClusterClient` routing method (local-first, remote fallback) → call it from the webui. Examples already doing this: `compact_shard`, `request_split`, `get_shard_storage_info`.

---

## 5. Config (`src/settings.rs`)

`AppConfig` sections (all `#[serde(default)]` except `database`, which is required):

| Section | Highlights |
|---------|-----------|
| `[server]` | `grpc_addr` (default `127.0.0.1:7450`), `dev_mode` (gates destructive RPCs), `statement_timeout_ms` (5000; 0 disables), `auth_token` (`${VAR}` expansion) |
| `[coordination]` | `backend` (none/etcd/k8s), `cluster_prefix` ("silo"), `lease_ttl_secs` (10), `initial_shard_count` (8, bootstrap only), `advertised_grpc_addr`, `etcd_endpoints`, `k8s_namespace`, `placement_rings`, `node_id` |
| `[tenancy]` | `enabled` (when off, synthetic tenant `-`) |
| `[gubernator]` | `address` (absence disables rate limiting), coalesce/batch/timeout knobs |
| `[webui]` | `enabled` (true), `addr` (`127.0.0.1:8080`) |
| `[logging]` | `format` (Text/Json) |
| `[metrics]` | `enabled` (true), `addr` (`127.0.0.1:9090`) |
| `[database]` | The richest: `backend` (fs/s3/gcs/memory/url/turmoilfs), `path` (with `%shard%`), optional `[database.wal]`, expiry TTLs, grant-scanner sizes, `counter_reconciliation_seconds`, `periodic_full_compaction_s`, and optional SlateDB `slatedb`/`memory_cache` tuning |

Loading supports **env expansion** (`${VAR}`, `${VAR:-default}`). SlateDB settings are deep-merged onto complete defaults (slatedb's `Settings` lacks per-field serde defaults); silo's one divergence is the embedded compactor is **off** unless you provide `compactor_options`.

Example configs in `example_configs/` (file 10 §9): `local-dev.toml`, `dev-node1/2.toml`, `compactor-dev.toml`, `k8s-gcs.toml`.

---

## 6. Observability

### 6.1 Metrics (`src/metrics.rs`) — Prometheus
The `prometheus` crate. `Metrics` owns an `Arc<Registry>` + dozens of HistogramVec/CounterVec/GaugeVec (e.g. `job_wait_time`, `grpc_request_duration`, `broker_scan_duration`, `ready_to_start_latency_ms`, `lease_reaper_duration`) plus nested `tokio_runtime` and `jemalloc` sub-structs. `init()` registers the process collector + all instruments. The HTTP endpoint (`serve_registry`) is an axum app exposing `GET /metrics` (`TextEncoder`). `GrpcMetricsLayer` is the second tower layer — records request count + duration labeled by method and gRPC status code (no-op when metrics disabled).

### 6.2 Tracing & propagation (`src/trace.rs`, `src/grpc_trace.rs`)
`trace::init` **always installs the W3C `TraceContextPropagator`** (so the gRPC layer can link silo spans to a caller's trace). Layers depend on env:
- `SILO_DEBUG_LOG_FILE` → also write debug logs to a file.
- `SILO_PERFETTO` → add a Perfetto export layer.
- `OTEL_EXPORTER_OTLP_ENDPOINT` → OpenTelemetry OTLP (metrics + traces, `service.name = "silo"`).
- else fmt-only (text or JSON per config).

`grpc_trace.rs` (`GrpcTraceLayer`) is the inbound side — extracts the parent context from HTTP headers, derives the method name, creates `grpc_request` span with `set_parent`, so all downstream spans/logs carry the worker's `trace_id`. It's the *first* tower layer.

### 6.3 Heap profiling (`src/heap_profile.rs`)
jemalloc mallctl wrappers. Heap profiling is **compiled in but inactive** (`prof:true,prof_active:false` in `lib.rs`). `ProfilingActivationGuard` turns it on (RAII, off on drop). Backs the `HeapProfile` RPC (activate → sleep → dump → return bytes). The `CpuProfile` RPC uses `pprof`. Both serialize through a global `PROFILE_MUTEX`.

### 6.4 jemalloc metrics (`src/jemalloc_metrics.rs`)
Five gauges (`allocated`/`active`/`resident`/`retained`/`mapped` bytes) to distinguish purgeable retained pages from live allocations. Must `epoch::advance()` before reading (jemalloc caches per-epoch). Scraped every 1s.

### 6.5 tokio runtime metrics (`src/tokio_runtime_metrics.rs`)
Per-worker busy time + starvation signals (long mean poll time, deep queues, forced yields, noop wakeups). Most fields need `--cfg tokio_unstable` (set in `.cargo/config.toml`). Scraped every 1s.

---

## 7. Mental model

- **main.rs** orchestrates boot/shutdown; **server.rs** wires tonic + routes RPCs to shards (or redirects).
- **Two tower layers** (trace → metrics) + an auth interceptor wrap every request.
- **query.rs** exposes a shard's keys as 5 SQL tables, pushing filters into range scans; **cluster_query.rs** fans out one partition per shard.
- **webui.rs** is server-rendered (axum+askama+htmx) and *never touches the local factory directly* — always via the query engine or `cluster_client`.
- Observability: Prometheus metrics, W3C-propagated tracing (OTLP/Perfetto), jemalloc + tokio runtime gauges, on-demand CPU/heap profiles.

Next: [`09-testing-and-verification.md`](./09-testing-and-verification.md).
