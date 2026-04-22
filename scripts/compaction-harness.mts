#!/usr/bin/env -S node --experimental-strip-types
/**
 * Compaction A/B test harness driver.
 *
 * Spins up:
 *   1. A single-shard `silo` server with its in-process compactor DISABLED
 *      (see example_configs/compaction-harness-writer.toml).
 *   2. `silo-bench` to push write traffic at that server.
 *   3. A separate `silo-compactor --mode loop` process that compacts the
 *      same object store the writer is writing to, running continuously
 *      for the duration of the bench.
 *
 * The compactor's Prometheus metrics are polled periodically (default every
 * 5 s) and saved as timestamped snapshots. The final snapshot provides
 * cumulative counters; the full time series is included in summary.json.
 *
 * Results (logs, Prometheus snapshots, filter summaries) are collected into
 * `tmp/compaction-runs/<run-id>/` so two runs with different compactor
 * configs (e.g. 64 MiB vs 256 MiB `max_sst_size`) can be compared.
 *
 * Usage:
 *   node scripts/compaction-harness.mts --writer-config <path> \
 *                                       --compactor-config <path> \
 *                                       [--duration-secs 20] \
 *                                       [--metrics-interval-ms 5000] \
 *                                       [--shard <uuid>]
 *
 * The shard UUID is auto-discovered from the writer's /shards endpoint if
 * not supplied — for the single-shard harness there is only ever one.
 */

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  mkdirSync,
  writeFileSync,
  createWriteStream,
  readdirSync,
  readFileSync,
  existsSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

type Args = {
  writerConfig: string;
  compactorConfig: string;
  durationSecs: number;
  enqueuers: number;
  workers: number;
  metricsIntervalMs: number;
  shard: string | null;
  dataRoot: string;
  writerGrpc: string;
  writerMetrics: string;
  compactorMetrics: string;
};

/** Read `metrics_interval_ms` from the compactor TOML's `[harness]` section.
 * Returns null for keys not present. We parse with a tiny regex scanner
 * rather than pulling in a TOML dep — the only key we care about here is
 * one integer. */
function readHarnessDefaults(tomlPath: string): {
  metricsIntervalMs: number | null;
} {
  if (!existsSync(tomlPath)) return { metricsIntervalMs: null };
  const txt = readFileSync(tomlPath, "utf8");
  // Find the [harness] section body — everything between the `[harness]`
  // header and the next TOML table header (or end of file).
  const header = /^\[harness\][ \t]*\r?\n/m;
  const headerMatch = header.exec(txt);
  let body = "";
  if (headerMatch) {
    const start = headerMatch.index + headerMatch[0].length;
    const rest = txt.slice(start);
    const nextHeader = rest.match(/^\[/m);
    body = nextHeader ? rest.slice(0, nextHeader.index!) : rest;
  }
  const pick = (key: string): number | null => {
    const m = body.match(new RegExp(`^\\s*${key}\\s*=\\s*(\\d+)`, "m"));
    return m ? Number(m[1]) : null;
  };
  return {
    metricsIntervalMs: pick("metrics_interval_ms"),
  };
}

function parseArgs(argv: string[]): Args {
  const flag = (name: string, def?: string) => {
    const i = argv.indexOf(`--${name}`);
    if (i < 0) return def;
    return argv[i + 1];
  };
  const writerConfig = flag(
    "writer-config",
    "example_configs/compaction-harness-writer.toml",
  )!;
  const compactorConfig = flag(
    "compactor-config",
    "example_configs/compaction-harness-compactor.toml",
  )!;
  // `--writes` is accepted as a compat alias and converted to a duration
  // (silo-bench is duration-driven, not request-count driven).
  const writesArg = flag("writes");
  const writes = writesArg !== undefined ? Number(writesArg) : undefined;
  const durationSecs = Number(
    flag("duration-secs", writes ? String(Math.max(10, Math.ceil(writes / 500))) : "20"),
  );
  const enqueuers = Number(flag("enqueuers", "4"));
  const workers = Number(flag("workers", "8"));
  // Harness settings: CLI > [harness] section in compactor TOML > built-in.
  const harnessDefaults = readHarnessDefaults(resolve(root, compactorConfig));
  const metricsIntervalMs = Number(
    flag("metrics-interval-ms") ??
      harnessDefaults.metricsIntervalMs ??
      "5000",
  );
  const shard = flag("shard") ?? null;
  const dataRoot = flag("data-root", resolve(root, "tmp/compaction-harness"))!;
  const writerGrpc = flag("writer-grpc", "http://127.0.0.1:7460")!;
  const writerMetrics = flag("writer-metrics", "http://127.0.0.1:9190")!;
  const compactorMetrics = flag("compactor-metrics", "http://127.0.0.1:9191")!;
  return {
    writerConfig,
    compactorConfig,
    durationSecs,
    enqueuers,
    workers,
    metricsIntervalMs,
    shard,
    dataRoot,
    writerGrpc,
    writerMetrics,
    compactorMetrics,
  };
}

function run(cmd: string, args: string[]): void {
  const result = spawnSync(cmd, args, { stdio: "inherit", cwd: root });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

async function waitForGrpc(addr: string, timeoutMs = 30_000): Promise<void> {
  const host = addr.replace(/^https?:\/\//, "");
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const [h, p] = host.split(":");
    const probe = spawnSync("nc", ["-z", h, p], { stdio: "ignore" });
    if (probe.status === 0) return;
    await sleep(250);
  }
  throw new Error(`gRPC ${addr} did not come up within ${timeoutMs}ms`);
}

async function fetchText(url: string): Promise<string> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url}: ${res.status}`);
  return await res.text();
}

async function discoverShardIdViaCli(writerGrpc: string): Promise<string> {
  // `siloctl cluster info --json` queries the server's GetClusterInfo RPC.
  const result = spawnSync(
    resolve(root, "target/debug/siloctl"),
    ["cluster", "info", "--json", "--address", writerGrpc],
    { cwd: root, encoding: "utf8" },
  );
  if (result.status !== 0) {
    throw new Error(`siloctl cluster info failed:\n${result.stderr}`);
  }
  try {
    const parsed = JSON.parse(result.stdout);
    const owners = parsed?.shard_owners ?? parsed?.shards ?? [];
    const first = owners[0]?.shard_id ?? owners[0]?.id;
    if (typeof first === "string" && first.length > 0) return first;
  } catch {
    // fall through to regex
  }
  const match = result.stdout.match(
    /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i,
  );
  if (!match) {
    throw new Error(`could not parse shard from siloctl output:\n${result.stdout}`);
  }
  return match[0];
}

function discoverShardIdViaFs(dataRoot: string): string {
  const entries = readdirSync(dataRoot, { withFileTypes: true })
    .filter((e) => e.isDirectory() && /^[0-9a-f-]{36}$/i.test(e.name))
    .map((e) => e.name);
  if (entries.length === 0) {
    throw new Error(`no shard directories under ${dataRoot}`);
  }
  if (entries.length > 1) {
    console.warn(
      `[harness] multiple shard dirs found (${entries.join(", ")}), using first`,
    );
  }
  return entries[0];
}

async function snapshotMetrics(url: string, file: string): Promise<void> {
  try {
    const text = await fetchText(`${url}/metrics`);
    writeFileSync(file, text);
  } catch (e) {
    console.warn(`metrics snapshot from ${url} failed: ${e}`);
  }
}

type BenchTotals = {
  totalEnqueued: number | null;
  totalCompleted: number | null;
  avgRate: number | null;
  enqueueRate: number | null;
};

type NoopFilterStats = {
  invocations: number;
  entriesSeen: number;
  valueEntries: number;
  tombstoneEntries: number;
  mergeEntries: number;
  bytesSeen: number;
};

type SlatedbGauges = {
  bytesCompactedTotal: number | null;
  l0SstCount: number | null;
  lastCompactionTs: number | null;
  runningCompactions: number | null;
  totalMemBytes: number | null;
};

type ObjectStoreOps = {
  put: number;
  put_multipart: number;
  get: number;
  delete: number;
  list: number;
  list_with_delimiter: number;
  copy: number;
  copy_if_not_exists: number;
  bytes: number;
};

const ZERO_OBJECT_STORE_OPS: ObjectStoreOps = {
  put: 0,
  put_multipart: 0,
  get: 0,
  delete: 0,
  list: 0,
  list_with_delimiter: 0,
  copy: 0,
  copy_if_not_exists: 0,
  bytes: 0,
};

function totalOps(o: ObjectStoreOps): number {
  return (
    o.put +
    o.put_multipart +
    o.get +
    o.delete +
    o.list +
    o.list_with_delimiter +
    o.copy +
    o.copy_if_not_exists
  );
}

function parseObjectStoreOps(file: string): ObjectStoreOps {
  const out: ObjectStoreOps = { ...ZERO_OBJECT_STORE_OPS };
  if (!existsSync(file)) return out;
  const txt = readFileSync(file, "utf8");
  const opRe =
    /^silo_object_store_ops_total\{[^}]*op="([a-z_]+)"[^}]*\}\s+([\d.eE+-]+)/gm;
  let m: RegExpExecArray | null;
  while ((m = opRe.exec(txt)) !== null) {
    const op = m[1];
    const val = Number(m[2]);
    if (op in out) {
      // @ts-expect-error — indexed access guarded by `in` check above.
      out[op] = (out[op] ?? 0) + val;
    }
  }
  const bytesM = txt.match(
    /^silo_object_store_bytes_total(?:\{[^}]*\})?\s+([\d.eE+-]+)/m,
  );
  if (bytesM) out.bytes = Number(bytesM[1]);
  return out;
}

function parseBenchTotals(file: string): BenchTotals {
  const defaults: BenchTotals = {
    totalEnqueued: null,
    totalCompleted: null,
    avgRate: null,
    enqueueRate: null,
  };
  if (!existsSync(file)) return defaults;
  const txt = readFileSync(file, "utf8");
  // Strip ANSI escapes so regexes line up.
  const clean = txt.replace(/\u001b\[[0-9;]*m/g, "");
  const num = (re: RegExp): number | null => {
    const m = clean.match(re);
    return m ? Number(m[1]) : null;
  };
  return {
    totalEnqueued: num(/Total enqueued:\s*([\d.]+)/),
    totalCompleted: num(/Total completed:\s*([\d.]+)/),
    avgRate: num(/Average rate:\s*([\d.]+)\s*tasks\/sec/),
    enqueueRate: num(/Average enqueue rate:\s*([\d.]+)\s*tasks\/sec/),
  };
}

function parseNoopFilter(file: string): NoopFilterStats {
  const out: NoopFilterStats = {
    invocations: 0,
    entriesSeen: 0,
    valueEntries: 0,
    tombstoneEntries: 0,
    mergeEntries: 0,
    bytesSeen: 0,
  };
  if (!existsSync(file)) return out;
  const clean = readFileSync(file, "utf8").replace(/\u001b\[[0-9;]*m/g, "");
  for (const line of clean.split("\n")) {
    if (!line.includes("noop_counting compaction filter summary")) continue;
    out.invocations += 1;
    const pick = (key: string): number => {
      const m = line.match(new RegExp(`${key}=([\\d]+)`));
      return m ? Number(m[1]) : 0;
    };
    out.entriesSeen += pick("entries_seen");
    out.valueEntries += pick("value_entries");
    out.tombstoneEntries += pick("tombstone_entries");
    out.mergeEntries += pick("merge_entries");
    out.bytesSeen += pick("bytes_seen");
  }
  return out;
}

type CompactorNewMetrics = {
  // manifest gauges (last observed value)
  sortedRunCount: number | null;
  totalSrSizeBytes: number | null;
  avgSstSizeBytes: number | null;
  outputTombstoneRatio: number | null;
  // scheduler counters (cumulative)
  compactionsProposedL0: number | null;
  compactionsProposedSr: number | null;
  backpressureRejections: number | null;
  conflictRejections: number | null;
  destLastRunCompactions: number | null;
  // per-job counters
  entriesWrittenPuts: number | null;
  entriesWrittenTombstones: number | null;
  entriesWrittenMerges: number | null;
  // filter decisions
  filterKeptLastRun: number | null;
  filterDroppedLastRun: number | null;
  filterKeptNonLastRun: number | null;
  filterDroppedNonLastRun: number | null;
};

const ZERO_COMPACTOR_METRICS: CompactorNewMetrics = {
  sortedRunCount: null,
  totalSrSizeBytes: null,
  avgSstSizeBytes: null,
  outputTombstoneRatio: null,
  compactionsProposedL0: null,
  compactionsProposedSr: null,
  backpressureRejections: null,
  conflictRejections: null,
  destLastRunCompactions: null,
  entriesWrittenPuts: null,
  entriesWrittenTombstones: null,
  entriesWrittenMerges: null,
  filterKeptLastRun: null,
  filterDroppedLastRun: null,
  filterKeptNonLastRun: null,
  filterDroppedNonLastRun: null,
};

function pickMetric(txt: string, name: string, extraLabels?: string): number | null {
  // Match: name{...shard="..."[,...extraLabel="value"...]} <number>
  // or name{shard="..."} <number> when no extra labels.
  // We use a flexible regex that picks the first matching line.
  const escaped = name.replace(/\./g, "\\.");
  const pattern = extraLabels
    ? new RegExp(`^${escaped}\\{[^}]*${extraLabels}[^}]*\\}\\s+([\\d.eE+-]+)`, "m")
    : new RegExp(`^${escaped}(?:\\{[^}]*\\})?\\s+([\\d.eE+-]+)`, "m");
  const m = txt.match(pattern);
  return m ? Number(m[1]) : null;
}

function sumMetric(txt: string, name: string, extraLabels?: string): number | null {
  // Sum all series matching the name (and optional extra label substring).
  const escaped = name.replace(/\./g, "\\.");
  const pattern = extraLabels
    ? new RegExp(`^${escaped}\\{[^}]*${extraLabels}[^}]*\\}\\s+([\\d.eE+-]+)`, "gm")
    : new RegExp(`^${escaped}(?:\\{[^}]*\\})?\\s+([\\d.eE+-]+)`, "gm");
  let sum = 0;
  let any = false;
  let m: RegExpExecArray | null;
  while ((m = pattern.exec(txt)) !== null) {
    sum += Number(m[1]);
    any = true;
  }
  return any ? sum : null;
}

function parseCompactorNewMetrics(file: string): CompactorNewMetrics {
  if (!existsSync(file)) return { ...ZERO_COMPACTOR_METRICS };
  const txt = readFileSync(file, "utf8");
  const p = (name: string, extra?: string) => pickMetric(txt, name, extra);
  const s = (name: string, extra?: string) => sumMetric(txt, name, extra);
  return {
    sortedRunCount: p("silo_slatedb_sorted_run_count"),
    totalSrSizeBytes: p("silo_slatedb_total_sr_size_bytes"),
    avgSstSizeBytes: p("silo_slatedb_avg_sst_size_bytes"),
    outputTombstoneRatio: p("silo_slatedb_output_tombstone_ratio"),
    compactionsProposedL0: s(`silo_slatedb_compactions_proposed_total`, `source_type="l0"`),
    compactionsProposedSr: s(`silo_slatedb_compactions_proposed_total`, `source_type="sr"`),
    backpressureRejections: s("silo_slatedb_backpressure_rejections_total"),
    conflictRejections: s("silo_slatedb_conflict_rejections_total"),
    destLastRunCompactions: s("silo_slatedb_dest_last_run_compactions_total"),
    entriesWrittenPuts: s(`silo_slatedb_entries_written_total`, `value_type="put"`),
    entriesWrittenTombstones: s(`silo_slatedb_entries_written_total`, `value_type="tombstone"`),
    entriesWrittenMerges: s(`silo_slatedb_entries_written_total`, `value_type="merge"`),
    filterKeptLastRun: s(`silo_slatedb_filter_decisions_total`, `decision="kept",is_last_run="true"`),
    filterDroppedLastRun: s(`silo_slatedb_filter_decisions_total`, `decision="dropped",is_last_run="true"`),
    filterKeptNonLastRun: s(`silo_slatedb_filter_decisions_total`, `decision="kept",is_last_run="false"`),
    filterDroppedNonLastRun: s(`silo_slatedb_filter_decisions_total`, `decision="dropped",is_last_run="false"`),
  };
}

function parseSlatedbGauges(file: string): SlatedbGauges {
  const out: SlatedbGauges = {
    bytesCompactedTotal: null,
    l0SstCount: null,
    lastCompactionTs: null,
    runningCompactions: null,
    totalMemBytes: null,
  };
  if (!existsSync(file)) return out;
  const txt = readFileSync(file, "utf8");
  // Prometheus exposition: <name>{labels} <value>. There can be multiple lines
  // for the same metric across shards; for this 1-shard harness we just take
  // the first numeric occurrence.
  const pick = (metric: string): number | null => {
    const re = new RegExp(`^${metric}(?:\\{[^}]*\\})?\\s+([\\d.eE+-]+)`, "m");
    const m = txt.match(re);
    return m ? Number(m[1]) : null;
  };
  out.bytesCompactedTotal = pick("silo_slatedb_bytes_compacted_total");
  out.l0SstCount = pick("silo_slatedb_l0_sst_count");
  out.lastCompactionTs = pick("silo_slatedb_last_compaction_ts_seconds");
  out.runningCompactions = pick("silo_slatedb_running_compactions");
  out.totalMemBytes = pick("silo_slatedb_total_mem_size_bytes");
  return out;
}

function formatBytes(n: number | null): string {
  if (n === null || Number.isNaN(n)) return "n/a";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}

type MetricsSnapshot = { index: number; timestampMs: number; file: string };

function buildSummary(opts: {
  runId: string;
  shard: string;
  args: Args;
  runDir: string;
  metricsSnapshots: MetricsSnapshot[];
}) {
  const bench = parseBenchTotals(resolve(opts.runDir, "bench.log"));
  const noop = parseNoopFilter(resolve(opts.runDir, "compactor.log"));
  const before = parseSlatedbGauges(
    resolve(opts.runDir, "writer-metrics-before.txt"),
  );
  const afterWrites = parseSlatedbGauges(
    resolve(opts.runDir, "writer-metrics-after-writes.txt"),
  );
  const afterCompact = parseSlatedbGauges(
    resolve(opts.runDir, "writer-metrics-after-compact.txt"),
  );
  const bytesCompactedDelta =
    afterCompact.bytesCompactedTotal !== null
      ? afterCompact.bytesCompactedTotal - (before.bytesCompactedTotal ?? 0)
      : null;

  // With a single long-lived compactor process, the final metrics snapshot
  // has the cumulative counters directly — no cross-iteration summation.
  const finalSnapshot = opts.metricsSnapshots.length > 0
    ? opts.metricsSnapshots[opts.metricsSnapshots.length - 1]
    : null;
  const finalFile = finalSnapshot?.file ?? "";
  const finalGauges = parseSlatedbGauges(finalFile);
  const finalObjectStore = parseObjectStoreOps(finalFile);
  const finalCompactorMetrics = parseCompactorNewMetrics(finalFile);

  // Build per-snapshot time series for the JSON artifact. Each entry records
  // the timestamp and gauge/counter values at that point so downstream tools
  // can plot metrics over time.
  const timeSeries = opts.metricsSnapshots.map((snap) => {
    const gauges = parseSlatedbGauges(snap.file);
    const ops = parseObjectStoreOps(snap.file);
    const cm = parseCompactorNewMetrics(snap.file);
    return {
      index: snap.index,
      timestampMs: snap.timestampMs,
      slatedbGauges: gauges,
      objectStoreOps: ops,
      compactorMetrics: cm,
    };
  });

  return {
    runId: opts.runId,
    shard: opts.shard,
    writerConfig: opts.args.writerConfig,
    compactorConfig: opts.args.compactorConfig,
    durationSecs: opts.args.durationSecs,
    enqueuers: opts.args.enqueuers,
    workers: opts.args.workers,
    metricsIntervalMs: opts.args.metricsIntervalMs,
    metricsSnapshots: opts.metricsSnapshots.length,
    completedAt: new Date().toISOString(),
    bench,
    noopFilter: noop,
    slatedb: {
      before,
      afterWrites,
      afterCompact,
      bytesCompactedDelta,
    },
    compactor: {
      bytesCompactedTotal: finalGauges.bytesCompactedTotal,
      lastCompactionTs:
        finalGauges.lastCompactionTs !== null && finalGauges.lastCompactionTs > 0
          ? finalGauges.lastCompactionTs
          : null,
    },
    compactorMetrics: finalCompactorMetrics,
    objectStore: {
      totals: finalObjectStore,
      totalOps: totalOps(finalObjectStore),
    },
    timeSeries,
  };
}

function printSummary(summary: ReturnType<typeof buildSummary>, runDir: string): void {
  const rows: [string, string][] = [
    ["Run ID", summary.runId],
    ["Shard", summary.shard],
    ["Writer config", summary.writerConfig],
    ["Compactor config", summary.compactorConfig],
    ["", ""],
    ["Bench duration", `${summary.durationSecs}s`],
    [
      "Compactor mode",
      `--mode loop (metrics every ${summary.metricsIntervalMs}ms, ${summary.metricsSnapshots} snapshots)`,
    ],
    [
      "Bench enqueue",
      summary.bench.totalEnqueued !== null
        ? `${summary.bench.totalEnqueued} tasks (${summary.bench.enqueueRate ?? "?"}/s)`
        : "n/a",
    ],
    [
      "Bench complete",
      summary.bench.totalCompleted !== null
        ? `${summary.bench.totalCompleted} tasks (${summary.bench.avgRate ?? "?"}/s)`
        : "n/a",
    ],
    ["", ""],
    ["L0 SSTs after writes", String(summary.slatedb.afterWrites.l0SstCount ?? "n/a")],
    ["L0 SSTs after compact", String(summary.slatedb.afterCompact.l0SstCount ?? "n/a")],
    [
      "Bytes compacted (total)",
      formatBytes(summary.compactor.bytesCompactedTotal),
    ],
    [
      "Last compaction ts",
      summary.compactor.lastCompactionTs !== null
        ? new Date(summary.compactor.lastCompactionTs * 1000).toISOString()
        : "n/a",
    ],
    ["", ""],
    ["Filter invocations", String(summary.noopFilter.invocations)],
    ["Filter entries seen", String(summary.noopFilter.entriesSeen)],
    [
      "  value / tombstone / merge",
      `${summary.noopFilter.valueEntries} / ${summary.noopFilter.tombstoneEntries} / ${summary.noopFilter.mergeEntries}`,
    ],
    ["Filter bytes seen", formatBytes(summary.noopFilter.bytesSeen)],
    ["", ""],
    [
      "Object-store API calls",
      `${summary.objectStore.totalOps} total`,
    ],
    ["  put", String(summary.objectStore.totals.put)],
    ["  put_multipart", String(summary.objectStore.totals.put_multipart)],
    ["  get (incl. head)", String(summary.objectStore.totals.get)],
    ["  delete", String(summary.objectStore.totals.delete)],
    ["  list", String(summary.objectStore.totals.list)],
    [
      "  list_with_delimiter",
      String(summary.objectStore.totals.list_with_delimiter),
    ],
    ["  copy", String(summary.objectStore.totals.copy)],
    [
      "  copy_if_not_exists",
      String(summary.objectStore.totals.copy_if_not_exists),
    ],
    ["Object-store put bytes", formatBytes(summary.objectStore.totals.bytes)],
    ["", ""],
    ["── Compactor scheduler ──", ""],
    ["Sorted run count (final)", String(summary.compactorMetrics.sortedRunCount ?? "n/a")],
    ["Total SR size (final)", formatBytes(summary.compactorMetrics.totalSrSizeBytes)],
    ["Avg SST size (final)", formatBytes(summary.compactorMetrics.avgSstSizeBytes)],
    ["Compactions proposed L0", String(summary.compactorMetrics.compactionsProposedL0 ?? "n/a")],
    ["Compactions proposed SR", String(summary.compactorMetrics.compactionsProposedSr ?? "n/a")],
    ["Backpressure rejections", String(summary.compactorMetrics.backpressureRejections ?? "n/a")],
    ["Conflict rejections", String(summary.compactorMetrics.conflictRejections ?? "n/a")],
    ["Dest=last-run compactions", String(summary.compactorMetrics.destLastRunCompactions ?? "n/a")],
    ["── Entries written ──", ""],
    ["  puts", String(summary.compactorMetrics.entriesWrittenPuts ?? "n/a")],
    ["  tombstones", String(summary.compactorMetrics.entriesWrittenTombstones ?? "n/a")],
    ["  merges", String(summary.compactorMetrics.entriesWrittenMerges ?? "n/a")],
    ["Output tombstone ratio", String(summary.compactorMetrics.outputTombstoneRatio ?? "n/a")],
    ["── Filter decisions ──", ""],
    ["  kept (last run)", String(summary.compactorMetrics.filterKeptLastRun ?? "n/a")],
    ["  dropped (last run)", String(summary.compactorMetrics.filterDroppedLastRun ?? "n/a")],
    ["  kept (non-last run)", String(summary.compactorMetrics.filterKeptNonLastRun ?? "n/a")],
    ["  dropped (non-last run)", String(summary.compactorMetrics.filterDroppedNonLastRun ?? "n/a")],
  ];
  const w = Math.max(...rows.map(([k]) => k.length));
  console.log("");
  console.log("================ compaction harness summary ================");
  for (const [k, v] of rows) {
    if (!k && !v) {
      console.log("");
    } else {
      console.log(`  ${k.padEnd(w)}  ${v}`);
    }
  }
  console.log("============================================================");
  console.log(`  artifacts: ${runDir}`);
  console.log(
    "  diff writer-metrics-after-compact.txt across two runs to A/B compaction configs",
  );
  console.log("");
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const runId = new Date().toISOString().replace(/[:.]/g, "-");
  const runDir = resolve(root, "tmp/compaction-runs", runId);
  mkdirSync(runDir, { recursive: true });
  console.log(`[harness] run dir: ${runDir}`);

  console.log("[harness] building silo, silo-bench, silo-compactor...");
  run("cargo", ["build", "--bin", "silo", "--bin", "silo-bench", "--bin", "silo-compactor", "--bin", "siloctl"]);

  // Start writer.
  const writerLog = createWriteStream(resolve(runDir, "writer.log"));
  const writer: ChildProcess = spawn(
    resolve(root, "target/debug/silo"),
    ["-c", args.writerConfig],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
  );
  writer.stdout!.pipe(writerLog);
  writer.stderr!.pipe(writerLog);
  writer.on("exit", (code) => console.log(`[harness] writer exited code=${code}`));

  try {
    await waitForGrpc(args.writerGrpc);
    console.log("[harness] writer up");

    let shard: string;
    if (args.shard) {
      shard = args.shard;
    } else {
      // Prefer siloctl (canonical source). If the RPC path fails or returns a
      // shard that doesn't exist on disk yet, fall back to filesystem
      // enumeration of the data root — for a single-shard harness this is
      // unambiguous.
      try {
        shard = await discoverShardIdViaCli(args.writerGrpc);
      } catch (e) {
        console.warn(`[harness] siloctl lookup failed: ${e}; falling back to fs`);
        shard = discoverShardIdViaFs(args.dataRoot);
      }
    }
    console.log(`[harness] shard: ${shard}`);

    // Pre-bench metrics snapshot.
    await snapshotMetrics(args.writerMetrics, resolve(runDir, "writer-metrics-before.txt"));

    // Launch silo-bench in the background so the compactor can run
    // concurrently with the write workload. This mirrors the realistic case
    // where compaction happens while writes are still arriving.
    console.log(
      `[harness] running silo-bench for ${args.durationSecs}s (enqueuers=${args.enqueuers} workers=${args.workers})...`,
    );
    const benchLog = createWriteStream(resolve(runDir, "bench.log"));
    const bench = spawn(
      resolve(root, "target/debug/silo-bench"),
      [
        "--address",
        args.writerGrpc,
        "--mode",
        "throughput",
        "--duration-secs",
        args.durationSecs.toString(),
        "--enqueuers",
        args.enqueuers.toString(),
        "--workers",
        args.workers.toString(),
      ],
      { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
    );
    bench.stdout!.pipe(benchLog);
    bench.stderr!.pipe(benchLog);
    const benchDone = new Promise<number>((res) => {
      bench.on("exit", (code) => res(code ?? -1));
    });

    // Start silo-compactor in loop mode alongside the bench. The compactor
    // runs continuously until we SIGTERM it after the bench finishes, which
    // mirrors production where compaction runs alongside live writes.
    console.log(
      `[harness] starting silo-compactor --mode loop (metrics every ${args.metricsIntervalMs}ms)`,
    );
    const compactorLog = createWriteStream(resolve(runDir, "compactor.log"));
    const compactor = spawn(
      resolve(root, "target/debug/silo-compactor"),
      ["-c", args.compactorConfig, "--shard", shard, "--mode", "loop"],
      { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
    );
    compactor.stdout!.pipe(compactorLog, { end: false });
    compactor.stderr!.pipe(compactorLog, { end: false });
    const compactorDone = new Promise<number>((res) => {
      compactor.on("exit", (code) => res(code ?? -1));
    });

    // Periodically snapshot the compactor's /metrics endpoint. Each snapshot
    // is saved with a monotonic index so we can reconstruct a time series.
    const metricsSnapshots: Array<{ index: number; timestampMs: number; file: string }> = [];
    const compactorRunning = { done: false };
    let snapshotIndex = 0;
    const metricsPoller = (async () => {
      // Give the compactor a moment to start its metrics server.
      await sleep(500);
      while (!compactorRunning.done) {
        snapshotIndex += 1;
        const file = resolve(runDir, `compactor-metrics-${String(snapshotIndex).padStart(4, "0")}.txt`);
        try {
          const text = await fetchText(`${args.compactorMetrics}/metrics`);
          writeFileSync(file, text);
          metricsSnapshots.push({ index: snapshotIndex, timestampMs: Date.now(), file });
        } catch {
          /* compactor metrics server not up yet — keep trying */
        }
        await sleep(args.metricsIntervalMs);
      }
    })();

    // Helper to stop the compactor and its metrics poller cleanly.
    const stopCompactor = async () => {
      // Take a final compactor metrics snapshot before shutting it down.
      snapshotIndex += 1;
      const finalFile = resolve(runDir, `compactor-metrics-${String(snapshotIndex).padStart(4, "0")}.txt`);
      try {
        const text = await fetchText(`${args.compactorMetrics}/metrics`);
        writeFileSync(finalFile, text);
        metricsSnapshots.push({ index: snapshotIndex, timestampMs: Date.now(), file: finalFile });
      } catch {
        console.warn("[harness] final compactor metrics snapshot failed");
      }
      compactor.kill("SIGTERM");
      const exitCode = await compactorDone;
      compactorRunning.done = true;
      await metricsPoller;
      compactorLog.end();
      console.log(`[harness] compactor exited code=${exitCode}`);
    };

    try {
      // Wait for the bench to finish.
      const benchExitCode = await benchDone;
      if (benchExitCode !== 0) {
        throw new Error(`bench exited ${benchExitCode}`);
      }

      await snapshotMetrics(args.writerMetrics, resolve(runDir, "writer-metrics-after-writes.txt"));
      await stopCompactor();
      await snapshotMetrics(args.writerMetrics, resolve(runDir, "writer-metrics-after-compact.txt"));

      const summary = buildSummary({
        runId,
        shard,
        args,
        runDir,
        metricsSnapshots,
      });
      writeFileSync(resolve(runDir, "summary.json"), JSON.stringify(summary, null, 2));
      printSummary(summary, runDir);
    } finally {
      // Ensure the compactor is stopped even if the bench fails.
      if (!compactorRunning.done) {
        await stopCompactor();
      }
    }
  } finally {
    writer.kill("SIGTERM");
    await new Promise<void>((res) => writer.on("exit", () => res()));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
