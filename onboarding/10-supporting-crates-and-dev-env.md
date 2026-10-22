# 10 — Supporting Crates, Client Libraries & Dev Environment

Everything outside the main `silo` crate: the workspace members, the TypeScript client, and how to actually run silo locally.

---

## 1. Workspace layout (`Cargo.toml:1`)

```toml
[workspace]
members = [".", "silo-macros", "silo-autoscaler", "silo-compactor", "siloctl"]
```

| Crate | Role |
|-------|------|
| **silo** (root) | The daemon + library. Binary `silo` needs the `server` feature. |
| **silo-macros** | The `#[silo::test]` proc-macro. |
| **silo-autoscaler** | k8s controller for safe StatefulSet scaling. |
| **silo-compactor** | Out-of-process SlateDB compactor. |
| **siloctl** | Operator gRPC CLI. |

**Feature flags on `silo`**: `default = ["k8s", "server"]`. `server` pulls in DataFusion (SQL/Arrow/web UI); `k8s` pulls in `kube`; `dst` pulls in turmoil; `tokio-console` adds the console subscriber. `siloctl` and `silo-compactor` build silo with `default-features = false` to skip the heavy DataFusion compile, reusing only the genuinely-shared pieces (`counter_merge_operator`, `metrics`, `AuthInterceptor`, `shard_range`).

---

## 2. silo-autoscaler

A Kubernetes controller (`kube`-runtime) that **safely scales a silo StatefulSet up/down without losing shard leases.** It is *not* a metrics-driven autoscaler — it sits **between an HPA and the StatefulSet** and guarantees safe drain.

- **CRD** (`crd.rs`) — a `SiloAutoscaler` resource (group `silo.dev`, `v1alpha1`) with `replicas`, `targetStatefulSet`, `clusterPrefix`. It exposes the `scale` subresource so an HPA's `scaleTargetRef` can point at it.
- **Controller** (`controller.rs`) — watches `SiloAutoscaler` + Pods. On reconcile:
  1. `handle_terminating_pods`: for a pod being deleted that still carries the finalizer `silo.dev/safe-scaledown`, wait until the container terminates, then check if it still holds shard leases. **No leases held** → clean shutdown, remove finalizer. **Leases still held** (pod was SIGKILL'd) → **recovery**: scale the STS back up to `ordinal+1` so the pod is recreated with the same ordinal/PVC, letting silo reclaim leases and flush its WAL, *then* remove the finalizer.
  2. Only when no termination is in flight does it patch the STS toward the desired replica count, waiting for all pods Ready before scaling down.
- **Orphan detection** (`orphan.rs`) — lists k8s shard `Lease`s labeled for this cluster and finds those whose `holderIdentity` is the pod name.
- **Validation** (`validation.rs`) — mirrors the CEL rules in `deploy/.../admission-policy.yaml`.

The whole point: when k8s scales a StatefulSet down, a naive delete could SIGKILL a pod mid-write and strand unflushed WAL behind a permanent lease. The finalizer + recovery dance prevents that.

---

## 3. silo-compactor

A **standalone, out-of-process SlateDB compactor fleet.** SlateDB compaction is decoupled from writing: silo nodes write SSTs/WAL, and a separate set of compactor pods merges sorted runs in object storage — isolating CPU/IO-heavy compaction from latency-sensitive job serving.

- **Entry** (`main.rs`) — loads TOML config, optional Prometheus endpoint (reuses `silo::metrics::serve_registry`), builds a coordinator, runs until SIGINT/SIGTERM.
- **Coordinator** (`coordinator.rs`) — wires **pod discovery** (k8s pod listing or static `single` mode) + a **shard-map loader** (etcd KV or k8s ConfigMap — the *same* shard map silo writes). On a `rebalance_interval` timer (default 30s) or pod-set change, it computes this pod's shard assignment, stops workers no longer assigned, spawns workers for new ones.
- **Assignment** (`assignment.rs`) — deterministic **contiguous-range partitioning**: sort pods + shards, give this pod the contiguous slice `[my_idx*n/total, (my_idx+1)*n/total)`, capped at `max_shards_per_pod`. Every compactor sorts the same inputs → disjoint partition with no inter-pod coordination.
- **Workers** (`worker.rs`) — one supervised task per shard. Builds a `slatedb::CompactorBuilder` with **`silo::job_store_shard::counter_merge_operator()`** (critical — so compaction preserves silo's counter semantics) and runs `compactor.run()`.

The dev silo nodes set `periodic_full_compaction_s = 60` (file 08 §2.3) to submit specs this fleet consumes.

---

## 4. siloctl — operator CLI

`siloctl/src/main.rs` (clap CLI) + `siloctl/src/lib.rs` (implementations, shared with tests). Global flags: `--address/-a` (default `http://localhost:7450`), `--tenant/-t`, `--json`, `--auth-token` (env `SILO_AUTH_TOKEN`).

**Connection model**: `connect` builds a tonic channel with `AuthInterceptor`; `connect_to_shard_owner` first calls `GetClusterInfo`, finds the node owning the target shard, and connects there directly — siloctl is topology-aware.

**Commands** (each maps ~1:1 to a gRPC RPC):
- `cluster info` — topology + shard ownership.
- `job get|result|cancel|restart|expedite|delete`.
- `shard split [--at <tenant>|--auto] [--wait] | split-status | configure --ring | force-release | compact | flush`.
- `tenant locate <id> | hash <id>` — which shard owns a tenant / its hash (same `hash_tenant` as server + TS client).
- `query <shard> <sql>`.
- `profile` (CPU pprof), `heap-profile` (jemalloc), `dump-tasks` (in-flight async backtraces).
- `validate-config -c <file>`.

---

## 5. silo-macros

One proc-macro, `#[silo::test]` (`silo-macros/src/lib.rs:13`). See file 09 §1.

---

## 6. typescript_client — `@silo/client`

A TS client + worker library (CommonJS, Node ≥18, pnpm). Source in `typescript_client/src/`.

### Transport & codegen
Built on **`@protobuf-ts`** + **`@grpc/grpc-js`**. `pnpm proto:generate` runs `protoc` over **the same `proto/silo.proto`** the Rust server uses → `src/pb/silo.ts` + `silo.client.ts`. **One schema, both clients.** Lint/format via `oxlint`/`oxfmt`; tests via `vitest`.

### `SiloGRPCClient` (`src/client.ts`)
- **Client-side shard routing**: `hashTenant` XXH64-hashes the tenant to 16 hex chars — explicitly documented to **match the Rust `hash_tenant`** (via `xxhash-wasm`, seed 0). `shardForTenant` binary-searches a range-sorted shard list. Each request routes to the owning node.
- **Topology discovery**: `refreshTopology` calls `GetClusterInfo`, rebuilds the shard→server map, opens connections to owners, and closes stale connections (handles pod-IP churn / scale-down).
- **Auth**: Bearer token via channel metadata (TLS) or an interceptor (insecure).
- **Payloads**: MessagePack via `msgpackr` (matches siloctl's `--format msgpack`).
- **Methods**: `enqueue` (→ a `JobHandle`), `importJobs`, `getJob`/`getJobStatus`/`getJobResult`, `cancel/restart/expedite/deleteJob`, `leaseTask(s)`, `reportOutcome`/`reportRefreshOutcome`, `heartbeat`, `query`, topology helpers.
- **Error hierarchy**: domain errors (`JobNotFoundError`, `JobAlreadyExistsError`, …) + gRPC-status errors, all extending `SiloGrpcError` with stable `code` strings.

### `SiloWorker` (`src/worker.ts`)
A polling worker: runs `concurrentPollers` loops against `LeaseTasks`, executes a `handler` under a `PQueue` (bounded by `maxConcurrentTasks`), **auto-heartbeats** running tasks, surfaces server-side cancellation via an `AbortSignal`, and exposes an optional `refreshHandler` for floating-limit refresh tasks. Lifecycle: `start()` / `stop(timeoutMs)`. Emits OpenTelemetry worker metrics. `JobHandle` (`src/JobHandle.ts`) provides `getStatus`/`cancel`/`awaitResult({ timeoutMs })`.

### Tests
Unit tests need no backend; integration tests (`*-integration.test.ts`, `toxiproxy-integration.test.ts`, `shard-routing.test.ts`) auto-skip unless `RUN_INTEGRATION=true` with a server up.

---

## 7. Dev environment

### Nix + flakes + direnv
- `flake.nix` (flake-parts) auto-imports `nix/modules/*`. `.envrc` does `use flake` and watches `rust-toolchain.toml` + modules. If a tool is missing, **`direnv reload`**.
- `rust-toolchain.toml` pins Rust **1.89.0** + rust-analyzer/rust-src/rustfmt/clippy/llvm-tools.
- **`nix/modules/devshell.nix`** — the dev shell: rust toolchain, `git`, `just`, `bacon`, `etcd`, `protobuf` (protoc), `flatbuffers` (flatc), `alloy6`, `process-compose`, `gubernator` (built from source), `nodejs`, `pnpm`, `zx`, `kubectl`, `grpcurl`, `toxiproxy`, `sccache`. Sets `RUSTFLAGS="--cfg tokio_unstable -C force-frame-pointers=yes"` and a toxiproxy config exposing proxies for silo-1 (`17450→7450`), silo-2 (`17451→7451`), and **etcd (`12379→2379`)** so tests can simulate a slow coordination layer.
- **`nix/modules/rust.nix`** — crane builds for every binary (deps built separately for caching). Exposed packages: `silo` (default), `silo-debug`, `silo-autoscaler`, `silo-compactor`, `siloctl`.
- **`nix/modules/docker.nix`** — layered images: `silo-docker` (prod), `silo-docker-dev` (+ debugging toolbox), and autoscaler/compactor images.

> Per `.ai/AGENTS.md`: don't use `nix develop -c` (it mishandles signals); assume direnv set up your shell, and use `direnv exec` for one-off commands in fresh shells.

### justfile recipes
`just` (list), `just fmt` (clippy --fix + rustfmt edition 2024), `just run`/`watch` (bacon), `just etcd` (run etcd alone), and coverage targets (`coverage`, `coverage-full`, `coverage-open`, …) via `cargo-llvm-cov`.

### process-compose.yml — the local cluster
`dev` (or `process-compose up`) starts six processes:
1. **etcd** — coordination (`127.0.0.1:2379`).
2. **gubernator** — rate limiting (gRPC `:9992`).
3. **toxiproxy** — fault injection (`:8474`).
4. **silo-1** — `cargo run --bin silo -c example_configs/dev-node1.toml`, watchexec-reloaded, tokio-console on `:6669`.
5. **silo-2** — same with `dev-node2.toml`, console `:6670`. **Shares the same data path** as silo-1 to simulate object-storage compute/storage separation.
6. **silo-compactor** — `-c example_configs/compactor-dev.toml`; depends on etcd + silo-1 (which bootstraps the shard map the compactor reads).

> Per `.ai/AGENTS.md`: when running tests, **assume these services are already running** and don't start `dev`/`process-compose` yourself unless you're a cloud agent. Don't run tests inside a restricted sandbox (false SSL/network failures).

### build.rs (codegen)
`build.rs` runs at every `cargo build`:
1. Vendored `protoc` → compile `proto/silo.proto` via `tonic-build` (client + server + a **reflection descriptor set** + serde derives on retry policies).
2. Compile `proto/gubernator.proto` (client-only).
3. Run **`flatc --rust`** on `schema/internal_storage.fbs`.
It also declares the custom cfgs `tokio_unstable`/`tokio_taskdump` and only reruns when proto/fbs files change. **So: modify a proto or fbs, just `cargo build` — no separate codegen step.**

---

## 8. Deploy (`deploy/`)

Raw k8s YAML (no Helm/Kustomize), two dirs:
- **`deploy/local-test/`** — a full local stack (namespace `silo-test`): a `silo-config` ConfigMap + headless Service + 3-replica StatefulSet (pods carry the `silo.dev/safe-scaledown` finalizer, gRPC probes, Downward-API `POD_IP`/`POD_NAME`), the `SiloAutoscaler` CRD, the autoscaler Deployment, a 2-replica compactor Deployment, an **HPA** targeting the `SiloAutoscaler` (demonstrating HPA→autoscaler→STS), a `ValidatingAdmissionPolicy`, and RBAC.
- **`deploy/autoscaler-integration-test/`** — a slimmer namespace used by the autoscaler integration test.

---

## 9. example_configs/

| File | Purpose |
|------|---------|
| `local-dev.toml` | Single-node, `backend = "none"`, fs storage, webui+metrics on. |
| `dev-node1.toml` | process-compose node 1: gRPC `:7450`, `dev_mode`, **etcd** via toxiproxy `:12379`, tenancy on, gubernator `:9992`, `periodic_full_compaction_s=60`, expiry TTLs, `counter_reconciliation_seconds=30`. |
| `dev-node2.toml` | Node 2: gRPC `:7451`; **same data path** as node 1. |
| `compactor-dev.toml` | The dev compactor: fs storage, etcd shard-discovery, `mode="single"`, `compactor_options`. |
| `k8s-gcs.toml` | Production-style: `backend="k8s"`, **GCS** main store + local-fs WAL, `${POD_IP}`/`${POD_NAME}` Downward-API substitution, optional Bearer `auth_token`. |

All use the `%shard%` path placeholder and explicit SlateDB GC settings (GC is off by default in SlateDB, so files would accumulate forever otherwise).

---

## 10. Cross-cutting notes

- **One proto, all clients**: `proto/silo.proto` is compiled by `build.rs` (Rust) and `pnpm proto:generate` (TS). The `Silo` gRPC service is the entire surface; every siloctl command and TS method maps to one of its ~30 RPCs.
- **Consistent tenant hashing** across server (`shard_range::hash_tenant`), siloctl (`tenant hash`), and TS (`hashTenant`) — XXH64 seed 0, cross-referenced in code comments.
- **Supporting crates mirror, not import, heavy silo internals** where pulling the full crate is too costly (the compactor re-implements `Backend` resolution and env-var expansion with comments noting what they mirror), while importing the genuinely-shared pieces.

That's the tour. Back to [`00-START-HERE.md`](./00-START-HERE.md), or see [`11-glossary.md`](./11-glossary.md).
