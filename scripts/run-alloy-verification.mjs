#!/usr/bin/env node
/**
 * Run Alloy model verification on a spec file.
 *
 * Verifies that:
 * - All 'check' commands return UNSAT (no counterexamples found)
 * - All 'run' commands return SAT (examples are satisfiable)
 * - At least one 'check' command was executed
 */

import { spawn } from "node:child_process";
import { parseArgs } from "node:util";
import { basename, dirname, join } from "node:path";
import { mkdirSync } from "node:fs";

/**
 * Run a command, streaming output to stdout/stderr while also capturing it.
 * Returns a promise that resolves with { code, output }.
 */
function runAndCapture(command, args) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, { stdio: ["inherit", "pipe", "pipe"] });
    const chunks = [];

    proc.stdout.on("data", (chunk) => {
      process.stdout.write(chunk);
      chunks.push(chunk);
    });

    proc.stderr.on("data", (chunk) => {
      process.stderr.write(chunk);
      chunks.push(chunk);
    });

    proc.on("error", reject);
    proc.on("close", (code) => {
      resolve({ code, output: Buffer.concat(chunks).toString("utf-8") });
    });
  });
}

const { values: args, positionals } = parseArgs({
  options: {
    help: {
      type: "boolean",
      short: "h",
      default: false,
    },
    solver: {
      type: "string",
      short: "s",
      default: "glucose",
    },
  },
  allowPositionals: true,
  strict: true,
});

function printHelp() {
  console.log(`
Usage: run-alloy-verification.mjs [options] <spec-file>

Run Alloy model verification on a spec file and verify results.

Options:
  -h, --help            Show this help message
  -s, --solver <name>   SAT solver to use (default: glucose)

Examples:
  ./scripts/run-alloy-verification.mjs specs/job_shard.als
  ./scripts/run-alloy-verification.mjs specs/coordination.als
  ./scripts/run-alloy-verification.mjs -s sat4j specs/job_shard.als
`);
}

if (args.help) {
  printHelp();
  process.exit(0);
}

if (positionals.length === 0) {
  console.error("Error: No spec file provided");
  printHelp();
  process.exit(1);
}

if (positionals.length > 1) {
  console.error("Error: Only one spec file can be provided at a time");
  process.exit(1);
}

const specFile = positionals[0];
const solver = args.solver;
const specName = basename(specFile);
const specsDir = dirname(specFile);
const outputDir = join(specsDir, "output");

console.log(`Alloy Model Verification: ${specName}`);
console.log(`Spec file: ${specFile}`);
console.log(`Solver: ${solver}`);
console.log("");

// Run Alloy
console.log("Running Alloy model checker...");
console.log("");

// Ensure output directory exists
mkdirSync(outputDir, { recursive: true });

let output;
try {
  const result = await runAndCapture("alloy6", [
    "exec",
    "-f",
    "-s", solver,
    "-o", outputDir,
    specFile,
  ]);
  output = result.output;

  if (result.code !== 0 && result.code !== null) {
    console.error(`Alloy exited with status ${result.code}`);
    process.exit(1);
  }
} catch (err) {
  console.error(`Failed to run Alloy: ${err.message}`);
  process.exit(1);
}

// Parse and verify results
console.log("");
console.log("Verifying results...");
console.log("");

let hasErrors = false;

// Check that all 'check' commands are UNSAT (no counterexamples found)
const satChecks = output.match(/^\d+\. check .* SAT$/gm) || [];
if (satChecks.length > 0) {
  console.error("ERROR: Alloy found counterexamples for the following assertions:");
  for (const line of satChecks) {
    console.error(`  ${line}`);
  }
  console.error("");
  console.error("A 'check' command returning SAT means the assertion is violated.");
  hasErrors = true;
}

// Check that all 'run' commands are SAT (examples are satisfiable)
const unsatRuns = output.match(/^\d+\. run .* UNSAT$/gm) || [];
if (unsatRuns.length > 0) {
  console.error("ERROR: Alloy could not find instances for the following examples:");
  for (const line of unsatRuns) {
    console.error(`  ${line}`);
  }
  console.error("");
  console.error("A 'run' command returning UNSAT means the scenario is impossible.");
  hasErrors = true;
}

// Count checks and runs
const checkMatches = output.match(/^\d+\. check/gm) || [];
const runMatches = output.match(/^\d+\. run/gm) || [];
const checkCount = checkMatches.length;
const runCount = runMatches.length;

if (checkCount === 0) {
  console.error("ERROR: No 'check' commands found in Alloy output!");
  hasErrors = true;
}

if (hasErrors) {
  console.log("");
  console.log("==============================================");
  console.log(`FAILED: ${specName}`);
  console.log("==============================================");
  process.exit(1);
}

console.log("==============================================");
console.log(`PASSED: ${specName}`);
console.log("==============================================");
console.log(`✓ All ${checkCount} assertions verified (no counterexamples found)`);
if (runCount > 0) {
  console.log(`✓ All ${runCount} example scenarios are satisfiable`);
}
console.log("");
