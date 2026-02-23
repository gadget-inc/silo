#!/usr/bin/env node
/**
 * Run deterministic simulation tests with multiple random seeds.
 *
 * Uses mad-turmoil for deterministic time and randomness, running each scenario
 * in a separate subprocess to ensure clean state.
 */

import { spawn, spawnSync } from "node:child_process";
import { parseArgs } from "node:util";
import { randomInt } from "node:crypto";
import { cpus } from "node:os";
import { mkdirSync, unlinkSync, rmSync, createWriteStream, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

// Directory for storing test output logs
const LOGS_DIR = "dst-logs";

// Auto-discover scenarios from the filesystem
const __dirname = dirname(fileURLToPath(import.meta.url));
const SCENARIOS_DIR = join(__dirname, "..", "tests", "turmoil_runner", "scenarios");

/**
 * Discover all DST scenarios by scanning the scenarios directory.
 * Scenario names are derived from filenames (e.g., chaos.rs -> chaos).
 * Excludes mod.rs as it's the module declaration file.
 */
function discoverScenarios() {
  const files = readdirSync(SCENARIOS_DIR);
  return files
    .filter((f) => f.endsWith(".rs") && f !== "mod.rs")
    .map((f) => f.replace(/\.rs$/, ""))
    .sort();
}

const SCENARIOS = discoverScenarios();

// Determine default parallelism: all cores in CI, 3 otherwise
const isCI = process.env.CI === "true" || process.env.CI === "1";
const defaultParallelism = isCI ? cpus().length : 3;

const { values: args } = parseArgs({
  options: {
    help: {
      type: "boolean",
      short: "h",
      default: false,
    },
    seeds: {
      type: "string",
      short: "n",
      default: "100",
    },
    seed: {
      type: "string",
      short: "s",
    },
    scenario: {
      type: "string",
      short: "t",
    },
    "random-seed": {
      type: "string",
      short: "r",
    },
    parallelism: {
      type: "string",
      short: "j",
      default: String(defaultParallelism),
    },
  },
  allowPositionals: true,
  strict: true,
});

function printHelp() {
  console.log(`
Usage: run-simulation-tests.mjs [options]

Fuzz the simulation tests with many random seeds to find failures.

Each seed runs exactly one scenario, deterministically selected based on the seed.
Each scenario runs in a separate subprocess to ensure clean state and true determinism.

Failed test output is written to ${LOGS_DIR}/ to avoid memory issues when running many seeds.

Options:
  -h, --help              Show this help message
  -n, --seeds <count>     Number of random seeds to test (default: 100)
  -s, --seed <seed>       Run a specific seed (for reproduction)
  -t, --scenario <name>   Run only a specific scenario (overrides seed-based selection)
  -r, --random-seed <n>   Base seed for generating test seeds (for reproducibility)
  -j, --parallelism <n>   Number of tests to run in parallel (default: ${defaultParallelism}, all cores in CI, 3 otherwise)

Available scenarios:
  ${SCENARIOS.join(", ")}

Examples:
  # Run 100 random seeds (each with one deterministic scenario)
  ./scripts/run-simulation-tests.mjs

  # Run 50 random seeds
  ./scripts/run-simulation-tests.mjs -n 50

  # Run a specific seed with its deterministic scenario
  ./scripts/run-simulation-tests.mjs -s 12345

  # Run a specific seed for a specific scenario (for debugging)
  ./scripts/run-simulation-tests.mjs -s 12345 -t chaos

  # Run with a reproducible set of random seeds
  ./scripts/run-simulation-tests.mjs -r 999 -n 50

  # Run with 8 parallel workers
  ./scripts/run-simulation-tests.mjs -n 100 -j 8
`);
}

if (args.help) {
  printHelp();
  process.exit(0);
}

const NUM_SEEDS = parseInt(args.seeds, 10);
const SPECIFIC_SEED = args.seed ? parseInt(args.seed, 10) : null;
const SPECIFIC_SCENARIO = args.scenario || null;
const RANDOM_SEED = args["random-seed"]
  ? parseInt(args["random-seed"], 10)
  : randomInt(0, 2 ** 32);
const PARALLELISM = parseInt(args.parallelism, 10);

// Validate scenario if specified
if (SPECIFIC_SCENARIO && !SCENARIOS.includes(SPECIFIC_SCENARIO)) {
  console.error(`Unknown scenario: ${SPECIFIC_SCENARIO}`);
  console.error(`Available scenarios: ${SCENARIOS.join(", ")}`);
  process.exit(1);
}

/**
 * Simple seeded PRNG (mulberry32)
 */
function createRng(seed) {
  let state = seed >>> 0;
  return function () {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return (t ^ (t >>> 14)) >>> 0;
  };
}

/**
 * Scenario weights for biased selection.
 * Higher weights mean the scenario runs more often. Based on historical CI
 * failure frequency — scenarios that fail more are more valuable to fuzz.
 * Scenarios not listed here get DEFAULT_WEIGHT.
 */
const SCENARIO_WEIGHTS = {
  // High value: frequently failing in CI
  high_message_loss: 4,
  high_latency: 4,
  chaos: 4,
  k8s_shard_splits: 4,
  // Medium value: occasional failures or related to high-value scenarios
  k8s_coordination: 2,
  k8s_permanent_leases: 2,
};
const DEFAULT_WEIGHT = 1;

/**
 * Deterministically select a scenario based on a seed, with weighted bias.
 * Scenarios with higher weights are selected more frequently.
 */
function selectScenarioFromSeed(seed, scenarios) {
  const totalWeight = scenarios.reduce(
    (sum, s) => sum + (SCENARIO_WEIGHTS[s] || DEFAULT_WEIGHT),
    0
  );
  let pick = seed % totalWeight;
  for (const scenario of scenarios) {
    const weight = SCENARIO_WEIGHTS[scenario] || DEFAULT_WEIGHT;
    if (pick < weight) return scenario;
    pick -= weight;
  }
  return scenarios[scenarios.length - 1];
}

/**
 * Get the log file path for a test run.
 */
function getLogFilePath(scenario, seed) {
  return join(LOGS_DIR, `${scenario}_seed_${seed}.log`);
}

/**
 * Build the test binary and return its path.
 * Uses cargo's JSON output to discover the binary location.
 */
function buildAndGetBinaryPath() {
  console.log("Building test binary (opt-level=1)...");
  // Use opt-level=1 for DST tests: debug builds (opt-level=0) are ~6x slower,
  // which dominates CI runtime. opt-level=1 adds basic optimizations (inlining,
  // dead code elimination) with negligible extra compile time.
  const buildEnv = { ...process.env, CARGO_PROFILE_TEST_OPT_LEVEL: "1" };
  const buildResult = spawnSync(
    "cargo",
    ["test", "--features", "dst", "--test", "turmoil_runner", "--no-run", "--message-format=json"],
    { stdio: ["ignore", "pipe", "inherit"], env: buildEnv }
  );
  if (buildResult.status !== 0) {
    console.error("Failed to build test binary");
    process.exit(1);
  }

  const lines = buildResult.stdout.toString().split("\n").filter(Boolean);
  let binaryPath = null;
  for (const line of lines) {
    try {
      const msg = JSON.parse(line);
      if (msg.reason === "compiler-artifact" && msg.executable && msg.target?.name === "turmoil_runner") {
        binaryPath = msg.executable;
      }
    } catch {
      // Skip non-JSON lines
    }
  }

  if (!binaryPath) {
    console.error("Could not find test binary path from cargo output");
    process.exit(1);
  }

  console.log(`Test binary: ${binaryPath}`);
  return binaryPath;
}

/**
 * Run a single scenario with a specific seed in fuzz mode.
 * Invokes the pre-built test binary directly (no cargo overhead).
 * Writes output directly to a log file to avoid OOM when running many seeds.
 */
function runScenarioWithSeed(binaryPath, scenario, seed) {
  return new Promise((resolve) => {
    const logFile = getLogFilePath(scenario, seed);
    const logStream = createWriteStream(logFile);
    const startTime = Date.now();

    const proc = spawn(
      binaryPath,
      [scenario, "--exact", "--nocapture"],
      {
        env: { ...process.env, DST_SEED: String(seed), DST_FUZZ: "1" },
        stdio: ["ignore", "pipe", "pipe"],
      }
    );

    // Stream output directly to file instead of buffering in memory
    proc.stdout.pipe(logStream, { end: false });
    proc.stderr.pipe(logStream, { end: false });

    proc.on("close", (code) => {
      // End the log stream and clean up
      const durationSec = (Date.now() - startTime) / 1000;
      logStream.end(() => {
        const passed = code === 0;

        // Delete log file for passing tests to save disk space
        if (passed) {
          try {
            unlinkSync(logFile);
          } catch {
            // Ignore deletion errors
          }
          resolve({ passed, logFile: null, exitCode: code, scenario, seed, durationSec });
        } else {
          resolve({ passed, logFile, exitCode: code, scenario, seed, durationSec });
        }
      });
    });

    proc.on("error", (err) => {
      logStream.end();
      const durationSec = (Date.now() - startTime) / 1000;
      resolve({ passed: false, logFile, exitCode: -1, scenario, seed, durationSec, error: err.message });
    });
  });
}

/**
 * Run tests in parallel using a worker pool pattern.
 * Processes results via callback to avoid accumulating them in memory.
 */
async function runTestsInParallel(binaryPath, tasks, parallelism, onResult) {
  const pending = [...tasks];
  const running = new Map(); // Map<Promise, {task, promise}>
  let completed = 0;
  let promiseId = 0;

  while (pending.length > 0 || running.size > 0) {
    // Start new tasks up to parallelism limit
    while (running.size < parallelism && pending.length > 0) {
      const task = pending.shift();
      const id = promiseId++;
      const promise = runScenarioWithSeed(binaryPath, task.scenario, task.seed).then((result) => ({
        id,
        result,
      }));
      running.set(id, promise);
    }

    if (running.size === 0) break;

    // Wait for any task to complete - Promise.race returns the value, not the promise
    const { id, result } = await Promise.race(running.values());
    running.delete(id);

    completed++;
    onResult(result, completed, tasks.length);
  }
}

async function main() {
  const scenariosToRun = SPECIFIC_SCENARIO ? [SPECIFIC_SCENARIO] : SCENARIOS;

  // If a specific seed is set, run that seed with output visible
  if (SPECIFIC_SEED !== null) {
    const binaryPath = buildAndGetBinaryPath();
    // Use specific scenario if provided, otherwise deterministically select based on seed
    const scenario = SPECIFIC_SCENARIO || selectScenarioFromSeed(SPECIFIC_SEED, SCENARIOS);
    console.log(`\nRunning scenario '${scenario}' with seed: ${SPECIFIC_SEED}\n`);
    const result = spawnSync(
      binaryPath,
      [scenario, "--exact", "--nocapture"],
      {
        env: { ...process.env, DST_SEED: String(SPECIFIC_SEED), DST_FUZZ: "1" },
        stdio: "inherit",
      }
    );
    process.exit(result.status ?? 1);
  }

  console.log("==============================================");
  console.log("Simulation Test Fuzzing");
  console.log("==============================================");
  console.log(`Seeds to test: ${NUM_SEEDS}`);
  console.log(`Parallelism: ${PARALLELISM} workers${isCI ? " (CI mode)" : ""}`);
  if (SPECIFIC_SCENARIO) {
    console.log(`Scenario: ${SPECIFIC_SCENARIO} (fixed)`);
  } else {
    console.log(`Scenarios: ${scenariosToRun.length} (selected per-seed from: ${scenariosToRun.join(", ")})`);
  }
  console.log(`Base random seed: ${RANDOM_SEED}`);
  console.log("");
  console.log("To reproduce this exact run:");
  console.log(
    `  ./scripts/run-simulation-tests.mjs --random-seed ${RANDOM_SEED} --seeds ${NUM_SEEDS}`
  );
  console.log("");
  console.log("----------------------------------------------");

  // Build test binary and get its path for direct invocation
  const binaryPath = buildAndGetBinaryPath();
  console.log("");

  // Clean and create logs directory
  rmSync(LOGS_DIR, { recursive: true, force: true });
  mkdirSync(LOGS_DIR, { recursive: true });

  // Generate all tasks upfront
  const rng = createRng(RANDOM_SEED);
  const tasks = [];
  for (let i = 0; i < NUM_SEEDS; i++) {
    const seed = rng();
    const scenario = SPECIFIC_SCENARIO || selectScenarioFromSeed(seed, SCENARIOS);
    tasks.push({ seed, scenario, index: i + 1 });
  }

  const failures = [];
  let totalPassed = 0;
  let totalFailed = 0;
  const startTime = Date.now();

  // Run tests in parallel, invoking the binary directly (no cargo overhead)
  const scenarioDurations = {};
  await runTestsInParallel(binaryPath, tasks, PARALLELISM, (result, completed, total) => {
    const { passed, scenario, seed, durationSec } = result;
    const padSeed = String(seed).padEnd(10);
    const progress = `(${String(completed).padStart(3)}/${total})`;
    const durStr = `${durationSec.toFixed(1)}s`;

    if (!scenarioDurations[scenario]) scenarioDurations[scenario] = [];
    scenarioDurations[scenario].push(durationSec);

    if (passed) {
      console.log(`Seed ${padSeed} ${progress} [${scenario}]: ✓ (${durStr})`);
      totalPassed += 1;
    } else {
      console.log(`Seed ${padSeed} ${progress} [${scenario}]: FAILED (${durStr})`);
      totalFailed += 1;
      failures.push(result);
    }
  });

  const elapsedSecs = ((Date.now() - startTime) / 1000).toFixed(1);
  const perSeedSecs = ((Date.now() - startTime) / 1000 / NUM_SEEDS).toFixed(2);

  console.log("");
  console.log("==============================================");
  console.log(`Results: ${totalPassed} passed, ${totalFailed} failed`);
  console.log(`Elapsed: ${elapsedSecs}s total, ${perSeedSecs}s per seed (wall clock / seeds)`);
  console.log("");
  console.log("Per-scenario timing:");
  const scenarioStats = Object.entries(scenarioDurations)
    .map(([name, times]) => ({
      name,
      avg: times.reduce((a, b) => a + b, 0) / times.length,
      max: Math.max(...times),
      count: times.length,
    }))
    .sort((a, b) => b.avg - a.avg);
  for (const s of scenarioStats) {
    console.log(`  ${s.name.padEnd(28)} avg=${s.avg.toFixed(1).padStart(6)}s  max=${s.max.toFixed(1).padStart(6)}s  (n=${s.count})`);
  }
  console.log("==============================================");

  if (failures.length > 0) {
    console.log("");
    console.log("Failures:");
    for (const { seed, scenario, logFile } of failures) {
      console.log(`  - seed=${seed} scenario=${scenario}`);
      console.log(`    log: ${logFile}`);
    }
    console.log("");
    console.log("To reproduce a failure:");
    console.log("  ./scripts/run-simulation-tests.mjs --seed <seed>");
    console.log("  (scenario is automatically selected based on seed)");
    console.log("");
    console.log("To verify determinism for a seed (runs twice, compares output):");
    console.log("  DST_SEED=<seed> cargo test --test turmoil_runner <scenario> -- --exact");
    console.log("");
    console.log(`Failure logs written to: ${LOGS_DIR}/`);

    process.exit(1);
  }

  console.log("");
  console.log(`✓ All ${NUM_SEEDS} seeds passed`);
}


main().catch((err) => {
  console.error(err);
  process.exit(1);
});
