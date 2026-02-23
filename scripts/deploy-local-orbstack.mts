#!/usr/bin/env -S node --experimental-strip-types
/**
 * Build the silo Docker image and deploy it to the local OrbStack Kubernetes cluster.
 */

import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function run(cmd: string, args: string[]): void {
  const result = spawnSync(cmd, args, { stdio: "inherit", cwd: root });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function capture(cmd: string, args: string[]): string {
  const result = spawnSync(cmd, args, { encoding: "utf8", cwd: root });
  if (result.status !== 0) {
    process.stderr.write(result.stderr ?? "");
    process.exit(result.status ?? 1);
  }
  return result.stdout.trim();
}

console.log("Building Docker image with Nix...");
const imagePath = capture("nix", ["build", ".#silo-docker", "--print-out-paths"]);

console.log("\nLoading image into Docker...");
run("docker", ["load", "--input", imagePath]);

console.log("\nApplying Kubernetes manifests to OrbStack...");
run("kubectl", ["--context", "orbstack", "apply", "-f", "deploy/local-test"]);

console.log("\nDone. Pods:");
run("kubectl", ["--context", "orbstack", "-n", "silo-test", "get", "pods"]);
