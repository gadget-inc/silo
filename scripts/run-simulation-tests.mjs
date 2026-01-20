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

Each seed runs all scenarios, with each scenario in a separate subprocess
to ensure clean state and true determinism.

Options:
  -h, --help              Show this help message
  -n, --seeds <count>     Number of random seeds to test (default: 100)
  -s, --seed <seed>       Run a specific seed (for reproduction)
  -t, --scenario <name>   Run only a specific scenario (default: all)
  -r, --random-seed <n>   Base seed for generating test seeds (for reproducibility)
  -v, --verbose           Show full output on failure

Available scenarios:
  ${SCENARIOS.join(", ")}

Examples:
  # Run 100 random seeds across all scenarios
  ./scripts/run-simulation-tests.mjs

  # Run 50 random seeds
  ./scripts/run-simulation-tests.mjs -n 50

  # Run a specific seed across all scenarios
  ./scripts/run-simulation-tests.mjs -s 12345

  # Run a specific seed for a specific scenario (for debugging)
  ./scripts/run-simulation-tests.mjs -s 12345 -t chaos

  # Run with a reproducible set of random seeds
  ./scripts/run-simulation-tests.mjs -r 999 -n 50

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
 * Run all scenarios with a specific seed, each in a separate subprocess.
 * Returns results for each scenario.
 */
async function runAllScenariosWithSeed(seed, scenarios) {
  const results = [];
  for (const scenario of scenarios) {
    const result = await runScenarioWithSeed(scenario, seed);
    results.push(result);
  }
  return results;
}

async function main() {
  const scenariosToRun = SPECIFIC_SCENARIO ? [SPECIFIC_SCENARIO] : SCENARIOS;

  // If a specific seed is set, run that seed with output visible
  if (SPECIFIC_SEED !== null) {
    if (SPECIFIC_SCENARIO) {
      console.log(`Running scenario '${SPECIFIC_SCENARIO}' with seed: ${SPECIFIC_SEED}\n`);
      const result = spawnSync(
        "cargo",
        ["test", "--test", "turmoil_runner", SPECIFIC_SCENARIO, "--", "--exact", "--nocapture"],
        {
          env: { ...process.env, DST_SEED: String(SPECIFIC_SEED), DST_FUZZ: "1" },
          stdio: "inherit",
        }
      );
      process.exit(result.status ?? 1);
    } else {
      console.log(`Running all scenarios with seed: ${SPECIFIC_SEED}\n`);
      let allPassed = true;
      for (const scenario of scenariosToRun) {
        console.log(`\n--- ${scenario} ---`);
        const result = spawnSync(
          "cargo",
          ["test", "--test", "turmoil_runner", scenario, "--", "--exact", "--nocapture"],
          {
            env: { ...process.env, DST_SEED: String(SPECIFIC_SEED), DST_FUZZ: "1" },
            stdio: "inherit",
          }
        );
        if (result.status !== 0) {
          allPassed = false;
        }
      }
      process.exit(allPassed ? 0 : 1);
    }
  }

  console.log("==============================================");
  console.log("Simulation Test Fuzzing");
  console.log("==============================================");
  console.log(`Seeds to test: ${NUM_SEEDS}`);
  console.log(`Scenarios: ${scenariosToRun.length} (${scenariosToRun.join(", ")})`);
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

  const rng = createRng(RANDOM_SEED);
  const failures = [];
  let totalPassed = 0;
  let totalFailed = 0;

  for (let i = 1; i <= NUM_SEEDS; i++) {
    const seed = rng();
    const padSeed = String(seed).padEnd(10);
    const progress = `(${String(i).padStart(3)}/${NUM_SEEDS})`;

    process.stdout.write(`Seed ${padSeed} ${progress}: `);

    const results = await runAllScenariosWithSeed(seed, scenariosToRun);
    const failedScenarios = results.filter((r) => !r.passed);

    if (failedScenarios.length === 0) {
      console.log("✓");
      totalPassed += scenariosToRun.length;
    } else {
      const failedNames = failedScenarios.map((r) => r.scenario).join(", ");
      console.log(`FAILED (${failedNames})`);
      totalFailed += failedScenarios.length;
      totalPassed += scenariosToRun.length - failedScenarios.length;
      for (const f of failedScenarios) {
        failures.push(f);
      }
    }
  }

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
    console.log("  ./scripts/run-simulation-tests.mjs --seed <seed> --scenario <scenario>");
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
  console.log(`✓ All ${NUM_SEEDS} seeds × ${scenariosToRun.length} scenarios passed`);
}


main().catch((err) => {
  console.error(err);
  process.exit(1);
});
