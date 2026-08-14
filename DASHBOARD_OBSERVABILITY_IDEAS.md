# Dashboard Observability Ideas

Brainstorm of additional observability that would be valuable to surface in the Silo
WebUI (`src/webui.rs` + `templates/*.html`).

## What the dashboard shows today

- **Cluster** (`cluster.html`) — total/owned shards, node count, total jobs, active
  splits, shard distribution grid, shard table (owner/ring/jobs/status), members
  (node id, gRPC addr, rings, shard count, uptime).
- **Shard** (`shard.html`) — range, owner, job count, LSM state (L0 SSTs, sorted runs,
  sizes), split/compaction controls.
- **Queues** (`queues.html` / `queue.html`) — concurrency limit, utilization, current
  holders, waiting requesters.
- **Tenants** (`tenants.html` / `tenant.html`) — per-tenant job counts and queues.
- **Job** (`job.html`) — status, priority, timestamps, metadata, limits, payload.
- **SQL** console, **Config** view.

The gap: the dashboard shows mostly **current structural state**. Nearly everything
about **flow, health, and history** lives only in Prometheus (`/metrics`) or in-memory
structs that are never rendered. The system already collects almost all of the data
below — the work is plumbing it into handlers/templates, not new instrumentation.

---

## Priority 1 — Operational health signals (catch incidents early)

These are the signals that most directly predict or explain production problems.

### 1. Concurrency "wedge" early warning
The known failure mode where the concurrency reconciler drifts/stalls. Two gauges
already track it but are invisible in the UI:
- `silo_concurrency_holder_drift` — sum of `|in_memory − durable|` holder mismatches
  per shard. Sustained non-zero = drifting toward a wedge.
- `silo_concurrency_reconciliation_pending` — queued reconciliation tuples per shard.
  Sustained `> 0` = reconciler wedged.

**Idea:** a cluster-level health banner + per-shard column that turns amber/red when
either is non-zero. This is arguably the single highest-value addition.

### 2. Task broker health
The broker feeds work into execution; if it lags, jobs sit idle even with capacity.
- `silo_broker_buffer_size` vs `silo_broker_inflight_size` per shard/task_group
- `silo_broker_scan_duration_seconds`, `silo_broker_scans_total`
- `silo_broker_scan_tasks_read_total` broken down by outcome (`inserted`,
  `skipped_future`, `skipped_inflight`, `skipped_tombstone`, `skipped_defunct`, …)
- `silo_broker_tombstone_count`

**Idea:** a "Broker" panel on the shard page showing buffer/inflight, scan latency,
and the scan-outcome breakdown (a stacked bar reads at a glance).

### 3. Lease reaper health
- `sile_lease_reaper_leases_reaped_total`, `silo_lease_reaper_errors_total`,
  `silo_lease_reaper_duration_seconds`

**Idea:** surface reaper error count and reap rate; non-zero errors should be loud.

### 4. Runtime starvation (tokio)
Blocking the async runtime causes mysterious latency across everything.
- Per-worker `silo_tokio_worker_local_queue_depth`, `..._noop_count_total`,
  `..._mean_poll_time_seconds` (long mean poll = a task is blocking)
- Global `silo_tokio_global_queue_depth`, `silo_tokio_alive_tasks`,
  `silo_tokio_budget_forced_yield_total`

**Idea:** a small "Runtime" card on the cluster/node page — worker queue depths and
mean poll time, flagged when poll time spikes.

### 5. Memory / leak detection (jemalloc)
- `silo_jemalloc_allocated_bytes`, `..._resident_bytes`, `..._retained_bytes`
- A climbing `retained / allocated` ratio is a leak signal.

**Idea:** memory card per node; optionally wire `src/heap_profile.rs` to show
profiling status and trigger/download a heap profile from the UI.

---

## Priority 2 — Flow & latency (is work moving?)

The dashboard shows counts but never **rates** or **latency distributions**.

### 6. Job throughput & latency
- Rates from `silo_jobs_enqueued_total`, `silo_jobs_dequeued_total`,
  `silo_jobs_completed_total` (by status: success/failed/cancelled)
- `silo_job_wait_time_seconds` (enqueue→dequeue) p50/p95/p99
- `silo_ready_to_start_latency_ms` (ready→first lease) percentiles

**Idea:** a "Throughput" strip on the cluster page (enqueue/complete/min, success vs
failure split) and wait-time percentiles. Even sparklines from short in-process
ring buffers would help without a full TSDB.

### 7. Retry & failure visibility
- `silo_job_attempts_total` split by `is_retry`
- Per-job **attempt history** (`src/job_attempt.rs`): each attempt's status
  (`Running` / `Succeeded` / `Failed{error_code,error}` / `Cancelled`), durations,
  and `attempt_number` / `relative_attempt_number`.
- Retry policy + `next_retry_time_ms` (backoff) per job.

**Idea:** an **attempt timeline** on the job page (currently it shows only the latest
status) with error codes; a cluster-level "top error codes" list.

### 8. gRPC surface health
- `silo_grpc_requests_total` by method/status_code, `silo_grpc_request_duration_seconds`

**Idea:** a per-method table (call rate, error %, p99) — quickly answers "which RPC is
failing/slow."

---

## Priority 3 — Storage & compaction

The shard page shows a static LSM snapshot; the dynamics are missing.

### 9. Compaction lag & progress
- `silo_slatedb_manifest_l0_count`, `..._compacted_count`, `..._checkpoints_count`
- `slatedb_running_compactions`, `slatedb_last_compaction_ts_seconds`,
  `slatedb_bytes_compacted_total`, `slatedb_backpressure_count_total` (write stalls)

**Idea:** extend the shard LSM panel with compaction activity (running, last run,
bytes compacted) and a write-stall counter. The template already hints at "compaction
lag" from L0 count — make it data-driven across the manifest signals.

### 10. Cache & read amplification
- `slatedb_cache_{data_block,index,filter}_{hit,miss}_total` → hit-rate panel
- `slatedb_sst_filter_false_positives_total` → bloom-filter read amplification

### 11. Object store health
- `slatedb_object_store_requests_total` / `..._errors_total` /
  `..._request_duration_seconds` by op/api

**Idea:** an "Object Store" panel — S3/GCS op latency and error rate, since this is a
common source of tail latency that's otherwise invisible.

---

## Priority 4 — Coordination & cluster lifecycle

### 12. Shard ownership / guard phases
- `ShardGuardState` phase lifecycle (`Idle → Acquiring → Held → Releasing →
  ShuttingDown → ShutDown`) and `desired` vs actual ownership.
- `silo_shards_owned` vs `silo_coordination_shards_open` disparity.

**Idea:** show each shard's guard phase and "desired≠held" mismatches — explains
shards stuck mid-handoff.

### 13. Split cleanup progress
The cluster page shows active splits, but **post-split cleanup** of defunct keys is
hidden:
- `SplitCleanupStatus` (`CleanupPending → CleanupRunning → CleanupDone`) per child
  shard; defunct-key cleanup progress (`skipped_defunct` broker outcome).

**Idea:** add a cleanup-status column/badge for child shards on the cluster + shard
pages.

### 14. Autoscaler activity (`silo-autoscaler/`)
- Terminating pods, **orphaned lease** detection (pod killed with leases still held),
  shard-lease recovery, reconcile conditions (`Ready`, `OrphanedLeases`, `Scaling`).

**Idea:** an "Autoscaler" page/panel — current scaling intent, orphaned-lease count,
recent recovery actions. (Cross-process: would need the autoscaler to expose this.)

### 15. Compactor service health (`silo-compactor/`)
- `silo_compactor_shards_owned`, `..._workers_running`,
  `..._worker_restarts_total`, `..._run_errors_total` per shard.

**Idea:** a "Compactor" panel showing per-shard worker health and restart churn.

---

## Priority 5 — Rate limiting & advanced queue state

### 16. Gubernator / rate-limit state (`src/gubernator.rs`)
- `RateLimitResult`: `remaining`, `reset_time_ms`, `under_limit`, errors.
- Coalescing batch size / interval.

**Idea:** per-queue rate-limit panel — remaining capacity, window reset, recent
throttle/error counts.

### 17. Floating concurrency limits (`FloatingLimitState`)
- `current_max_concurrency`, `last_refreshed_at_ms`, `refresh_task_scheduled`,
  `retry_count`, `next_retry_at_ms`.

**Idea:** on the queue page, show the live max-concurrency, last refresh time, and
refresh backoff state (currently the limit is shown as a static number).

### 18. Concurrency grant internals
- `silo_concurrency_tickets_granted_total` by path (`immediate` vs `scanned`)
- Per-queue hydration state (`NotHydrated/Hydrating/Hydrated`)
- `RequestTicketOutcome` distribution (granted-immediately vs deferred vs future-dated)

**Idea:** grant-path ratio and hydration state on the queue page — explains latency to
acquire a slot.

---

## Cross-cutting UX ideas

- **Auto-refresh:** htmx is already loaded — add `hx-trigger="every 5s"` polling to the
  live panels (broker, throughput, runtime) instead of manual reload. The footer
  already runs a JS clock, so the page is "live" in spirit.
- **Lightweight sparklines without a TSDB:** keep small in-process ring buffers of key
  gauges/rates and render inline SVG/Unicode sparklines — enough for trend direction
  without standing up Prometheus+Grafana.
- **A "Health" landing strip** at the top of the cluster page aggregating the P1
  signals (wedge drift, reaper errors, compaction lag, object-store errors, memory) into
  green/amber/red chips that deep-link to detail.
- **Per-shard drill-down consistency:** broker, reconciliation, compaction, and guard
  phase all key off shard — group them as tabs/sections on the existing shard page
  rather than new top-level pages.
- **Metric provenance:** every panel maps to an existing `/metrics` series — link each
  to its raw metric name so operators can build their own alerts.

---

## Notes / caveats

- Most P1–P3 data is already collected (Prometheus or in-memory) — these are rendering
  tasks, not instrumentation tasks.
- Autoscaler (#14) and Compactor (#15) run as **separate processes**; surfacing them in
  the main WebUI needs a data path (scrape their `/metrics`, or a small RPC) — more than
  a template change.
- Histogram percentiles (wait time, poll latency) are exported as Prometheus histograms;
  rendering p95/p99 in-process means either reading back from the registry or keeping a
  summary alongside.
