#!/usr/bin/env -S node --experimental-strip-types
/**
 * Build the silo Docker image inside OrbStack (native aarch64-linux) and
 * deploy it to the local OrbStack Kubernetes cluster.
 *
 * The Nix build runs inside a nixos/nix container in OrbStack so the build
 * is native aarch64-linux — no cross-compilation required on macOS.
 * The resulting image tarball is piped directly to `docker load` to avoid
 * writing a temporary file.
 * A named Docker volume (silo-nix-store) caches the Nix store between builds.
 */

import { spawn, spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function run(cmd: string, args: string[]): void {
  const result = spawnSync(cmd, args, { stdio: "inherit", cwd: root });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

async function buildAndLoad(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const build = spawn(
      "docker",
      [
        "--context", "orbstack",
        "run", "--rm",
        "--volume", `${root}:/workspace:ro`,
        "--volume", "silo-nix-store:/nix",
        "--workdir", "/workspace",
        "nixos/nix",
        "sh", "-c",
        "echo 'experimental-features = nix-command flakes' >> /etc/nix/nix.conf && IMAGE=$(nix build .#silo-docker --no-link --print-out-paths) && cat \"$IMAGE\"",
      ],
      { cwd: root, stdio: ["ignore", "pipe", "inherit"] }
    );

    const load = spawn(
      "docker",
      ["--context", "orbstack", "load"],
      { cwd: root, stdio: [build.stdout, "inherit", "inherit"] }
    );

    build.on("error", reject);
    load.on("error", reject);

    load.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`docker load failed with exit code ${code}`));
    });
  });
}

console.log("Building silo image inside OrbStack (aarch64-linux)...");
await buildAndLoad();

console.log("\nApplying Kubernetes manifests to OrbStack...");
run("kubectl", ["--context", "orbstack", "apply", "-f", "deploy/local-test"]);

console.log("\nRestarting StatefulSet...");
run("kubectl", ["--context", "orbstack", "-n", "silo-test", "rollout", "restart", "statefulset/silo"]);

console.log("\nDone. Pods:");
run("kubectl", ["--context", "orbstack", "-n", "silo-test", "get", "pods"]);
