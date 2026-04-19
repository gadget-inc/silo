#!/usr/bin/env -S node --experimental-strip-types
/**
 * Compaction A/B test harness driver.
 *
 * Spins up:
 *   1. A single-shard `silo` server with its in-process compactor DISABLED
 *      (see example_configs/compaction-harness-writer.toml).
 *   2. `silo-bench` to push write traffic at that server.
 *   3. A separate `silo-compactor` process that compacts the same object
 *      store the writer is writing to.
 *
 * Results (logs, Prometheus snapshots, filter summaries) are collected into
 * `tmp/compaction-runs/<run-id>/` so two runs with different compactor
 * configs (e.g. 64 MiB vs 256 MiB `max_sst_size`) can be compared.
 *
 * Usage:
 *   node scripts/compaction-harness.mts --writer-config <path> \
 *                                       --compactor-config <path> \
 *                                       [--writes 10000] \
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
  shard: string | null;
  dataRoot: string;
  writerGrpc: string;
  writerMetrics: string;
  compactorMetrics: string;
};

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
};

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

function parseSlatedbGauges(file: string): SlatedbGauges {
  const out: SlatedbGauges = {
    bytesCompactedTotal: null,
    l0SstCount: null,
    lastCompactionTs: null,
    runningCompactions: null,
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
  return out;
}

function formatBytes(n: number | null): string {
  if (n === null || Number.isNaN(n)) return "n/a";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}

function buildSummary(opts: {
  runId: string;
  shard: string;
  args: Args;
  runDir: string;
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
  return {
    runId: opts.runId,
    shard: opts.shard,
    writerConfig: opts.args.writerConfig,
    compactorConfig: opts.args.compactorConfig,
    durationSecs: opts.args.durationSecs,
    enqueuers: opts.args.enqueuers,
    workers: opts.args.workers,
    completedAt: new Date().toISOString(),
    bench,
    noopFilter: noop,
    slatedb: {
      before,
      afterWrites,
      afterCompact,
      bytesCompactedDelta,
    },
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
      "Bytes compacted (delta)",
      formatBytes(summary.slatedb.bytesCompactedDelta),
    ],
    [
      "Last compaction ts",
      summary.slatedb.afterCompact.lastCompactionTs !== null
        ? new Date(
            summary.slatedb.afterCompact.lastCompactionTs * 1000,
          ).toISOString()
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

    // Run the write burst.
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
    await new Promise<void>((res, rej) => {
      bench.on("exit", (code) => (code === 0 ? res() : rej(new Error(`bench exited ${code}`))));
    });

    await snapshotMetrics(args.writerMetrics, resolve(runDir, "writer-metrics-after-writes.txt"));

    // Run the standalone compactor once and wait for it to drain.
    console.log("[harness] running silo-compactor --mode once...");
    const compactorLog = createWriteStream(resolve(runDir, "compactor.log"));
    const compactor = spawn(
      resolve(root, "target/debug/silo-compactor"),
      ["-c", args.compactorConfig, "--shard", shard, "--mode", "once"],
      { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
    );
    compactor.stdout!.pipe(compactorLog);
    compactor.stderr!.pipe(compactorLog);
    await new Promise<void>((res, rej) => {
      compactor.on("exit", (code) =>
        code === 0 ? res() : rej(new Error(`compactor exited ${code}`)),
      );
    });

    await snapshotMetrics(args.writerMetrics, resolve(runDir, "writer-metrics-after-compact.txt"));
    await snapshotMetrics(args.compactorMetrics, resolve(runDir, "compactor-metrics-after.txt"));

    const summary = buildSummary({
      runId,
      shard,
      args,
      runDir,
    });
    writeFileSync(resolve(runDir, "summary.json"), JSON.stringify(summary, null, 2));
    printSummary(summary, runDir);
  } finally {
    writer.kill("SIGTERM");
    await new Promise<void>((res) => writer.on("exit", () => res()));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
