# Generating Sample Data for Silo WebUI Development

A practical guide to populating a local dev cluster with realistic, varied data so
you can iterate on the WebUI dashboard (`src/webui.rs` + `templates/*.html`) against
panels that actually have something to show.

---

## TL;DR

```bash
# 1. Start the dev cluster (etcd + gubernator + 2 silo nodes + compactor)
dev

# 2. In a second terminal, generate live load
cargo run --bin silo-bench -- \
  --address http://localhost:7450 \
  --workers 8 --enqueuers 4 \
  --duration-secs 300 \
  --tenant-count 6 --imbalance-factor 2.5 \
  --max-concurrency 20 --max-floating-limits 2

# 3. Watch the dashboard *while the bench runs*
open http://localhost:8081      # node 1
open http://localhost:8082      # node 2
```

The single most important thing: **view the dashboard while `silo-bench` is running.**
The bench both enqueues *and* processes jobs, so once it stops, most jobs settle into
terminal states and the live panels (running jobs, queue holders/waiters, throughput)
go quiet. See [Shaping the data](#shaping-the-data-for-specific-panels) below.

---

## The dev cluster

- Launch with the `dev` command (defined in `nix/modules/devshell.nix:48-59`; it runs
  `process-compose up` against `process-compose.yml`).
- Brings up: `etcd`, `gubernator`, `silo-1`, `silo-2`, `silo-compactor`.
- Both nodes auto-rebuild on `src/` changes via `watchexec`, so editing a template or
  `webui.rs` triggers a recompile — give it a moment after saving.

Key endpoints (from `example_configs/dev-node1.toml` / `dev-node2.toml`):

| Thing | Node 1 | Node 2 |
|-------|--------|--------|
| WebUI dashboard | http://127.0.0.1:8081 | http://127.0.0.1:8082 |
| gRPC (enqueue target) | 127.0.0.1:7450 | 127.0.0.1:7451 |
| Tokio console | 127.0.0.1:6669 | 127.0.0.1:6670 |

The dev cluster has **8 shards** and **tenancy enabled** (`dev-node1.toml:11,14-15`),
so tenant- and shard-oriented panels have real structure to render.

---

## Method 1 — `silo-bench` (recommended)

`src/bin/silo-bench.rs` is a load generator that runs enqueuers and workers
concurrently against a live cluster. It's the fastest way to fill every dashboard
panel with realistic, multi-tenant, multi-queue data. (It's auto-discovered by Cargo
from `src/bin/`, so `cargo run --bin silo-bench` just works — no `[[bin]]` entry
needed.)

What it exercises:
- Multiple **tenants** (`--tenant-count`), optionally **imbalanced** so some tenants
  are hot — good for testing sorting/skew in tenant panels.
- **Static concurrency limits** (per-tenant `{tenant}-queue`, or a shared
  `--concurrency-key`) → queue holders & waiters.
- **Floating concurrency limits** (`--max-floating-limits`) with a refresh loop and an
  injected `--floating-failure-rate` → exercises the refresh/backoff machinery.
- Jobs across states: running, waiting, scheduled, succeeded, failed.
- **Priorities** (0–99) and msgpack payloads, generated automatically.

### Flags worth knowing

(Full list: `src/bin/silo-bench.rs:27-110`, or `cargo run --bin silo-bench -- --help`.)

| Flag | Default | Effect |
|------|---------|--------|
| `--address`, `-a` | `http://localhost:7450` | Target node's gRPC addr |
| `--workers`, `-w` | `8` | Concurrent workers leasing/completing jobs |
| `--enqueuers`, `-e` | `4` | Concurrent job producers |
| `--duration-secs`, `-d` | `30` | How long to run |
| `--tenant-count`, `-t` | `4` | Number of distinct tenants |
| `--tenant-prefix` | `bench` | Tenant naming (`bench-0`, `bench-1`, …) |
| `--imbalance-factor` | `1.0` | `>1.0` makes early tenants hotter |
| `--max-concurrency` | `100` | Static limit per queue |
| `--concurrency-key` | (per-tenant) | Set to force all tenants onto one shared queue |
| `--max-floating-limits` | `2` | Floating limits per job (`0` to disable) |
| `--floating-default-max` | `50` | Floating limit's initial max |
| `--floating-refresh-interval-ms` | `2000` | How often workers get refresh tasks |
| `--floating-failure-rate` | `0.1` | Probability a refresh reports failure |

---

## Shaping the data for specific panels

`silo-bench`'s default settings tend toward a healthy, draining queue. To make
particular panels interesting, deliberately unbalance it:

### Deep queues with waiters (concurrency contention)
Make demand exceed capacity so jobs pile up as **waiters** behind a small set of
**holders**:

```bash
cargo run --bin silo-bench -- -a http://localhost:7450 \
  --enqueuers 8 --workers 2 \
  --max-concurrency 3 --concurrency-key hot-queue \
  --max-floating-limits 0 --duration-secs 600
```

Low `--max-concurrency` + a shared `--concurrency-key` + few workers = a single hot
queue with visible holders and a long waiter line.

### Backlog of waiting/scheduled jobs (let work accumulate, don't drain it)
Run with **zero or very few workers** so enqueued jobs stay un-leased:

```bash
cargo run --bin silo-bench -- -a http://localhost:7450 \
  --workers 1 --enqueuers 6 --duration-secs 120
```

Now the cluster/shard/tenant job counts stay high after the run, which is handy for
developing static panels without needing live traffic.

### Tenant skew (for sorting / "top tenants" views)
```bash
cargo run --bin silo-bench -- -a http://localhost:7450 \
  --tenant-count 10 --imbalance-factor 5.0 --duration-secs 300
```

### Floating-limit churn (refresh/backoff signals)
```bash
cargo run --bin silo-bench -- -a http://localhost:7450 \
  --max-floating-limits 3 --floating-refresh-interval-ms 500 \
  --floating-failure-rate 0.3 --duration-secs 300
```

### Spread load across both nodes / shards
Run two benches at once, one per node, so shard ownership and distribution panels show
cross-node activity:

```bash
cargo run --bin silo-bench -- -a http://localhost:7450 --tenant-prefix east &
cargo run --bin silo-bench -- -a http://localhost:7451 --tenant-prefix west &
```

---

## Method 2 — direct gRPC enqueue (precise control)

gRPC reflection is enabled (`src/server.rs:2251-2255`), so you can hand-craft jobs with
`grpcurl` for exact, reproducible states (a single high-priority job, a future-scheduled
job, a specific limit config, etc.). Proto: `proto/silo.proto` (`EnqueueRequest` ~L90).

```bash
# Discover the service
grpcurl -plaintext localhost:7450 list

# Enqueue one job onto a named queue
grpcurl -plaintext -d @ localhost:7450 silo.v1.Silo/Enqueue <<'EOF'
{
  "shard": "00000000-0000-0000-0000-000000000000",
  "id": "demo-job-1",
  "priority": 5,
  "start_at_ms": 0,
  "payload": { "msgpack": "" },
  "limits": [ { "concurrency": { "key": "demo-queue", "max_concurrency": 10 } } ],
  "tenant": "demo-tenant",
  "task_group": "default"
}
EOF
```

For looping/scripted seeding in Rust, the integration test helpers show the full
enqueue→lease→report lifecycle and are easy to adapt:
- `tests/grpc_lease_task_tests.rs` (full cycle, ~L9-97)
- `tests/grpc_integration_helpers.rs` (multi-shard setup, ~L86-131)

---

## Method 3 — large pre-generated dataset (`benches/bench_helpers.rs`)

For scale testing (pagination, big tables, deep queues), `benches/bench_helpers.rs` has
`ensure_golden_shard()` which deterministically builds ~500K jobs across 300 tenants
(Zipf-distributed) with a realistic state mix (60% succeeded / 20% failed / 10%
cancelled / 7% waiting / 3% scheduled) and a 20K-deep concurrency queue
(`bench_helpers.rs:30-47,85-102`). It's heavier to wire into a live cluster than the
options above, but it's the reference for what "realistic at scale" looks like.

---

## Inspecting what you generated — the `/sql` console

The WebUI's `/sql` page (`src/webui.rs`, ~L1648) runs read-only SQL across all shards
via the cluster query engine. Use it to confirm your seed produced the distribution you
expect (and as a quick reference for what data the templates can pull from):

```sql
SELECT tenant, COUNT(*) FROM jobs GROUP BY tenant ORDER BY 2 DESC;
SELECT status, COUNT(*) FROM jobs GROUP BY status;
```

---

## Resetting between iterations

`silo-bench` accumulates state. To start clean:

```bash
# Stop the dev cluster first (Ctrl-C in the `dev` terminal), then:
scripts/reset-dev-cluster.sh    # wipes tmp/etcd-data and tmp/silo-data
dev                             # restart fresh
```

---

## Reference

| Purpose | File |
|---------|------|
| Load generator | `src/bin/silo-bench.rs` |
| WebUI handlers & routes | `src/webui.rs` |
| Dashboard templates | `templates/*.html` |
| gRPC API / EnqueueRequest | `proto/silo.proto` |
| gRPC reflection | `src/server.rs:2251-2255` |
| Enqueue example (test) | `tests/grpc_lease_task_tests.rs` |
| Multi-shard test setup | `tests/grpc_integration_helpers.rs` |
| Large dataset generator | `benches/bench_helpers.rs` |
| Dev configs (ports, tenancy) | `example_configs/dev-node1.toml`, `dev-node2.toml` |
| Dev launch command | `nix/modules/devshell.nix:48-59` |
| Reset script | `scripts/reset-dev-cluster.sh` |
| Observability backlog / panel ideas | `DASHBOARD_OBSERVABILITY_IDEAS.md` |
