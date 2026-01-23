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

// All available DST scenarios - must match test function names in main.rs
const SCENARIOS = [
  "chaos",
  "concurrency_limits",
  "fault_injection_partition",
  "grpc_end_to_end",
  "high_message_loss",
  "lease_expiry",
  "multiple_workers",
];

// Determine default parallelism: all cores in CI, 3 otherwise
const isCI = process.env.CI === "true" || process.env.CI === "1";
const defaultParallelism = isCI ? cpus().length : 3;

// Parse command line arguments
const { values: args, positionals } = parseArgs({
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
    verbose: {
      type: "boolean",
      short: "v",
      default: false,
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

Options:
  -h, --help              Show this help message
  -n, --seeds <count>     Number of random seeds to test (default: 100)
  -s, --seed <seed>       Run a specific seed (for reproduction)
  -t, --scenario <name>   Run only a specific scenario (overrides seed-based selection)
  -r, --random-seed <n>   Base seed for generating test seeds (for reproducibility)
  -j, --parallelism <n>   Number of tests to run in parallel (default: ${defaultParallelism}, all cores in CI, 3 otherwise)
  -v, --verbose           Show full output on failure

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

  # Run in CI with verbose output on failure
  ./scripts/run-simulation-tests.mjs -n 100 -v
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
const VERBOSE = args.verbose;

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
 * Deterministically select a scenario based on a seed.
 * Uses the seed to pick one of the available scenarios.
 */
function selectScenarioFromSeed(seed, scenarios) {
  const index = seed % scenarios.length;
  return scenarios[index];
}

/**
 * Run a single scenario with a specific seed in fuzz mode.
 * Each scenario runs in its own subprocess for determinism.
 */
function runScenarioWithSeed(scenario, seed) {
  return new Promise((resolve) => {
    const proc = spawn(
      "cargo",
      ["test", "--test", "turmoil_runner", scenario, "--", "--exact", "--nocapture"],
      {
        env: { ...process.env, DST_SEED: String(seed), DST_FUZZ: "1" },
        stdio: ["ignore", "pipe", "pipe"],
      }
    );

    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (data) => (stdout += data));
    proc.stderr.on("data", (data) => (stderr += data));

    proc.on("close", (code) => {
      const output = stdout + stderr;
      const passed = code === 0 && output.includes("test result: ok");
      resolve({ passed, output, exitCode: code, scenario, seed });
    });
  });
}

/**
 * Run tests in parallel using a worker pool pattern.
 * Returns results in the order tests complete.
 */
async function runTestsInParallel(tasks, parallelism, onResult) {
  const pending = [...tasks];
  const running = new Map(); // Map<Promise, {task, promise}>
  const results = [];
  let promiseId = 0;

  while (pending.length > 0 || running.size > 0) {
    // Start new tasks up to parallelism limit
    while (running.size < parallelism && pending.length > 0) {
      const task = pending.shift();
      const id = promiseId++;
      const promise = runScenarioWithSeed(task.scenario, task.seed).then((result) => ({
        id,
        result,
      }));
      running.set(id, promise);
    }

    if (running.size === 0) break;

    // Wait for any task to complete - Promise.race returns the value, not the promise
    const { id, result } = await Promise.race(running.values());
    running.delete(id);

    results.push(result);
    onResult(result, results.length, tasks.length);
  }

  return results;
}

async function main() {
  const scenariosToRun = SPECIFIC_SCENARIO ? [SPECIFIC_SCENARIO] : SCENARIOS;

  // If a specific seed is set, run that seed with output visible
  if (SPECIFIC_SEED !== null) {
    // Use specific scenario if provided, otherwise deterministically select based on seed
    const scenario = SPECIFIC_SCENARIO || selectScenarioFromSeed(SPECIFIC_SEED, SCENARIOS);
    console.log(`Running scenario '${scenario}' with seed: ${SPECIFIC_SEED}\n`);
    const result = spawnSync(
      "cargo",
      ["test", "--test", "turmoil_runner", scenario, "--", "--exact", "--nocapture"],
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

  // Build test binary first
  console.log("Building test binary...");
  const buildResult = spawnSync("cargo", ["test", "--test", "turmoil_runner", "--no-run"], {
    stdio: "inherit",
  });
  if (buildResult.status !== 0) {
    console.error("Failed to build test binary");
    process.exit(1);
  }
  console.log("");

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

  // Run tests in parallel
  await runTestsInParallel(tasks, PARALLELISM, (result, completed, total) => {
    const { passed, scenario, seed } = result;
    const padSeed = String(seed).padEnd(10);
    const progress = `(${String(completed).padStart(3)}/${total})`;

    if (passed) {
      console.log(`Seed ${padSeed} ${progress} [${scenario}]: ✓`);
      totalPassed += 1;
    } else {
      console.log(`Seed ${padSeed} ${progress} [${scenario}]: FAILED`);
      totalFailed += 1;
      failures.push(result);
    }
  });

  console.log("");
  console.log("==============================================");
  console.log(`Results: ${totalPassed} passed, ${totalFailed} failed`);
  console.log("==============================================");

  if (failures.length > 0) {
    console.log("");
    console.log("Failures:");
    for (const { seed, scenario } of failures) {
      console.log(`  - seed=${seed} scenario=${scenario}`);
    }
    console.log("");
    console.log("To reproduce a failure:");
    console.log("  ./scripts/run-simulation-tests.mjs --seed <seed>");
    console.log("  (scenario is automatically selected based on seed)");
    console.log("");
    console.log("To verify determinism for a seed (runs twice, compares output):");
    console.log("  DST_SEED=<seed> cargo test --test turmoil_runner <scenario> -- --exact");

    if (VERBOSE && failures.length > 0) {
      console.log("");
      console.log("==============================================");
      console.log(`First failure output (${failures[0].scenario} seed=${failures[0].seed}):`);
      console.log("==============================================");
      console.log(failures[0].output);
    }

    process.exit(1);
  }

  console.log("");
  console.log(`✓ All ${NUM_SEEDS} seeds passed`);
}


main().catch((err) => {
  console.error(err);
  process.exit(1);
});
