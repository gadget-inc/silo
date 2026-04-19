# Compaction Test Harness

The compaction test harness runs a silo writer with in-process compaction disabled alongside a separate `silo-compactor` process. This lets you A/B test different compaction strategies, filters, and tuning parameters while measuring object-store I/O, throughput, and filter behavior.

## Quick start

```sh
# Build all required binaries
cargo build --bin silo --bin silo-bench --bin silo-compactor --bin siloctl

# Run with the default compactor config (leveled compaction + completed_jobs filter)
node scripts/compaction-harness.mts --duration-secs 120

# Run with a specific compactor config
node scripts/compaction-harness.mts --duration-secs 120 \
  --compactor-config example_configs/compaction-harness-compactor-size-tiered.toml
```

Each run produces artifacts in `tmp/compaction-runs/<run-id>/` including logs, metrics snapshots, and a `summary.json`.

## Compactor config files

The harness ships with two compactor configs:

| Config | Scheduler | Description |
|--------|-----------|-------------|
| `example_configs/compaction-harness-compactor.toml` | Leveled | Default. Leveled compaction with completed_jobs filter. |
| `example_configs/compaction-harness-compactor-size-tiered.toml` | Size-tiered | STCS with completed_jobs filter. |

Both point at the same database path (`./tmp/compaction-harness/%shard%`) as the writer config (`compaction-harness-writer.toml`). Copy and modify these to test different configurations.

## Configuring the compaction filter

Set `[database.compaction.filter]` in the compactor TOML:

### No filter (baseline)

```toml
[database.compaction.filter]
kind = "none"
```

### Noop counting filter (validates plumbing)

Keeps all entries but logs a summary of what was seen (entries, tombstones, merges, bytes).

```toml
[database.compaction.filter]
kind = "noop_counting"
```

### Completed jobs filter (production filter)

Drops completed job data (Succeeded, Failed, Cancelled) older than `retention_secs`. Deletes all associated keys (JOB_INFO, JOB_STATUS, IDX_STATUS_TIME, IDX_METADATA, ATTEMPT, JOB_CANCELLED) using point reads via a DbReader.

```toml
[database.compaction.filter]
kind = "completed_jobs"
retention_secs = 604800          # 7 days (default)
```

The `retention_secs` value controls how long completed job data is kept before the filter is allowed to purge it. Tune this based on your test goals:

| retention_secs | Duration | Use case |
|---------------|----------|----------|
| `604800` | 7 days | Production default. No jobs are dropped during a short test run. |
| `86400` | 1 day | Moderate retention for longer test runs. |
| `15` | 15 seconds | Aggressive. The filter drops data almost immediately, useful for observing filter behavior and measuring its impact on write amplification. |
| `0` | instant | Drops all completed jobs on every compaction pass. Maximum filter activity. |

For local testing, use a short retention to see the filter actively dropping data:

```toml
[database.compaction.filter]
kind = "completed_jobs"
retention_secs = 15              # 15 seconds — useful for local testing
```

With a 7-day retention and a test run under a few minutes, no jobs will be old enough to drop — the filter runs but keeps everything. Use `retention_secs = 15` or lower to actually exercise the filter's drop/tombstone logic during short harness runs.

## Configuring the compaction algorithm

Set `[database.compaction.scheduler]` in the compactor TOML:

### Size-tiered compaction (STCS)

Merges similarly-sized SSTs together. Lower write amplification in theory, but rewrites the entire sorted run on each merge.

```toml
[database.compaction.scheduler]
kind = "size_tiered"
min_compaction_sources = 4       # minimum SSTs to trigger compaction
max_compaction_sources = 8       # maximum SSTs per compaction job
include_size_threshold = 4.0     # size ratio threshold for including SSTs
```

### Leveled compaction

Organizes data into levels with exponentially increasing size targets. Reduces read and space amplification. Better write efficiency when a compaction filter is actively dropping data, since expired data is purged before it cascades to deeper levels.

```toml
[database.compaction.scheduler]
kind = "leveled"
level0_file_num_compaction_trigger = 4   # L0 files before compacting into L1
max_bytes_for_level_base = 268435456     # 256 MiB target for L1
max_bytes_for_level_multiplier = 10.0    # each level is 10x the previous
num_levels = 7                           # total levels including L0
```

## Controlling harness parameters

Parameters can be set in three places (highest priority first):

1. **CLI flags** on `compaction-harness.mts`
2. **`[harness]` section** in the compactor TOML
3. **Built-in defaults**

### Duration

How long the `silo-bench` writer runs. The bench generates continuous enqueue + complete traffic for this period.

```sh
node scripts/compaction-harness.mts --duration-secs 300
```

### Compaction passes

How many times the harness invokes `silo-compactor --mode once`. Each invocation submits a full compaction spec and waits for completion.

```sh
# Via CLI (overrides TOML)
node scripts/compaction-harness.mts --compactions 25

# Via TOML
[harness]
compactions = 25
```

### Compaction interval

Milliseconds to sleep before each compactor invocation. The first sleep gives the writer time to accumulate L0 SSTs before the first compaction.

```sh
# Via CLI
node scripts/compaction-harness.mts --compaction-interval-ms 10000

# Via TOML
[harness]
compaction_interval_ms = 10000
```

### Writer workload tuning

```sh
node scripts/compaction-harness.mts \
  --duration-secs 300 \
  --enqueuers 4 \
  --workers 8
```

### Full example: A/B comparison

```sh
# Run A: size-tiered, 300s, 25 compactions
node scripts/compaction-harness.mts --duration-secs 300 \
  --compactor-config example_configs/compaction-harness-compactor-size-tiered.toml

# Clean data between runs
rm -rf tmp/compaction-harness

# Run B: leveled, 300s, 25 compactions
node scripts/compaction-harness.mts --duration-secs 300 \
  --compactor-config example_configs/compaction-harness-compactor.toml
```

## Reading the results

### Terminal summary

Each run prints a summary table to stdout:

```
================ compaction harness summary ================
  Run ID                       2026-04-19T22-55-00-702Z
  Bench duration               300s
  Compactor passes             25 x --mode once (interval 10000ms)
  Bench enqueue                11884 tasks (39.5/s)
  Bench complete               11884 tasks (39.5/s)

  Bytes compacted (sum)        14.61 MiB
  Last compaction ts           2026-04-19T22:59:56.000Z

  Object-store API calls       5317 total across 25 compaction passes
    put                        710
    get (incl. head)           3905
    list                       702
  Object-store put bytes       15.92 MiB
============================================================
```

### summary.json

Each run writes a structured `summary.json` to `tmp/compaction-runs/<run-id>/`. Key fields:

```
summary.json
├── bench
│   ├── totalEnqueued          # total tasks enqueued during the run
│   ├── totalCompleted         # total tasks completed
│   ├── avgRate                # completed tasks/sec
│   └── enqueueRate            # enqueued tasks/sec
├── compactor
│   ├── bytesCompactedTotal    # sum of bytes_compacted across all passes
│   └── lastCompactionTs       # epoch seconds of last compaction
├── objectStore
│   ├── totalOps               # total API calls across all passes
│   ├── totals                 # { put, get, list, delete, ... }
│   └── perIteration[]         # per-pass breakdown
│       ├── iteration
│       └── ops                # { put, get, list, bytes, ... }
└── slatedb
    ├── before                 # writer metrics before bench
    ├── afterWrites            # writer metrics after bench completes
    └── afterCompact           # writer metrics after all compactions
```

Use `jq` to extract specific fields:

```sh
# Compare bytes compacted across two runs
jq '.compactor.bytesCompactedTotal' tmp/compaction-runs/*/summary.json

# Per-iteration object-store ops
jq '.objectStore.perIteration[] | "\(.iteration): \(.ops.put) puts, \(.ops.get) gets"' \
  tmp/compaction-runs/<run-id>/summary.json
```

### Run artifacts

Each run directory also contains:

| File | Contents |
|------|----------|
| `writer.log` | Silo server logs |
| `bench.log` | silo-bench output (throughput stats) |
| `compactor.log` | All compactor iterations (filter summaries, compaction progress) |
| `compactor-metrics-iter-N.txt` | Prometheus snapshot from compactor iteration N |
| `writer-metrics-before.txt` | Writer Prometheus metrics before bench |
| `writer-metrics-after-writes.txt` | Writer metrics after bench completes |
| `writer-metrics-after-compact.txt` | Writer metrics after all compactions |

### Checking compaction filter activity

The `completed_jobs` filter logs to `compactor.log`. Look for:

```sh
# How many jobs were dropped per compaction pass
grep "jobs_dropped" tmp/compaction-runs/<run-id>/compactor.log

# DbReader activity (one per compaction job)
grep "opened DbReader" tmp/compaction-runs/<run-id>/compactor.log | wc -l
```

### Key metrics to compare

When A/B testing compaction strategies:

| Metric | What it tells you |
|--------|-------------------|
| **Bytes compacted** | Total data rewritten by the compactor (write amplification) |
| **Object-store put bytes** | Actual bytes uploaded to storage (cost signal) |
| **Object-store ops** | API call count (cloud billing signal) |
| **GET count** | Read amplification — how much existing data must be re-read to compact |
| **PUT count** | Number of new SSTs written |
| **Jobs dropped** | How many expired jobs the filter purged |
| **Bench throughput** | Whether compaction impacts write performance |
