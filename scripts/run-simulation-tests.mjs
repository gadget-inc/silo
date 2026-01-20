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

// Parse command line arguments
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

Each seed runs all scenarios once (fuzz mode). This maximizes seed coverage
to find bugs. Determinism verification (running twice per seed) is done
separately in CI via 'cargo test'.

Options:
  -h, --help              Show this help message
  -n, --seeds <count>     Number of random seeds to test (default: 100)
  -s, --seed <seed>       Run a specific seed (for reproduction)
  -r, --random-seed <n>   Base seed for generating test seeds (for reproducibility)
  -v, --verbose           Show full output on failure

Examples:
  # Run 100 random seeds
  ./scripts/run-simulation-tests.mjs

  # Run 50 random seeds
  ./scripts/run-simulation-tests.mjs -n 50

  # Run a specific seed (for reproducing a failure)
  ./scripts/run-simulation-tests.mjs -s 12345

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
const RANDOM_SEED = args["random-seed"]
  ? parseInt(args["random-seed"], 10)
  : randomInt(0, 2 ** 32);
const VERBOSE = args.verbose;

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
 * Run cargo test with a specific seed in fuzz mode (single run, no determinism verification)
 */
function runTestWithSeed(seed) {
  return new Promise((resolve) => {
    const proc = spawn(
      "cargo",
      ["test", "--test", "turmoil_runner", "--", "--test-threads=1"],
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
      resolve({ passed, output, exitCode: code });
    });
  });
}

async function main() {
  // If a specific seed is set, just run that single seed with inherited stdio
  if (SPECIFIC_SEED !== null) {
    console.log(`Running simulation tests with specific seed: ${SPECIFIC_SEED}\n`);
    const result = spawnSync(
      "cargo",
      ["test", "--test", "turmoil_runner", "--", "--test-threads=1"],
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
  const failedSeeds = [];
  let passed = 0;
  let failed = 0;

  for (let i = 1; i <= NUM_SEEDS; i++) {
    const seed = rng();
    const padSeed = String(seed).padEnd(10);
    const progress = `(${String(i).padStart(3)}/${NUM_SEEDS})`;

    process.stdout.write(`Seed ${padSeed} ${progress}: `);

    const result = await runTestWithSeed(seed);
    if (result.passed) {
      console.log("✓");
      passed++;
    } else {
      console.log("FAILED");
      failed++;
      failedSeeds.push({ seed, output: result.output });
    }
  }

  console.log("");
  console.log("==============================================");
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log("==============================================");

  if (failedSeeds.length > 0) {
    console.log("");
    console.log("Failed seeds:");
    for (const { seed } of failedSeeds) {
      console.log(`  - ${seed}`);
    }
    console.log("");
    console.log("To reproduce a failure (fuzz mode, single run):");
    console.log("  ./scripts/run-simulation-tests.mjs --seed <seed>");
    console.log("");
    console.log("To verify determinism for a seed (runs twice, compares output):");
    console.log("  DST_SEED=<seed> cargo test --test turmoil_runner -- --test-threads=1");

    if (VERBOSE && failedSeeds.length > 0) {
      console.log("");
      console.log("==============================================");
      console.log("First failure output:");
      console.log("==============================================");
      console.log(failedSeeds[0].output);
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
