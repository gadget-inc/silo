#!/usr/bin/env bash
#
# Seed the running dev cluster with varied sample data for WebUI dashboard
# development. Requires the dev cluster to be running (`dev` in another terminal).
#
# It runs `silo-bench` in a few passes, each shaped to populate different parts
# of the dashboard:
#   1. Balanced multi-tenant history  -> tenant distribution, job states, throughput
#   2. Tenant skew                     -> "hot tenant" / sorting views
#   3. Enqueue-only backlog (leftover) -> pending jobs that persist after exit
#
# Passes 1 and 2 let workers keep up so silo-bench drains cleanly and its
# post-run holder audit passes. Pass 3 runs with `--workers 0`: jobs are only
# enqueued, never leased, so no concurrency holders are taken (clean exit) and
# the enqueued jobs remain as a visible backlog after the script finishes.
#
# Note: silo-bench is a correctness/throughput harness. It always drains and
# then audits for leaked concurrency holders, FAILING if any remain. That is
# why we don't try to leave a "stuck holders" queue behind here — to watch live
# holders/waiters, run the foreground command printed at the end and watch the
# dashboard while it runs.
#
# Usage:
#   scripts/seed-sample-data.sh                          # node 1 (localhost:7450)
#   scripts/seed-sample-data.sh http://localhost:7451    # node 2
#
set -euo pipefail

ADDRESS="${1:-http://localhost:7450}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Extract host:port from the address for a reachability check.
HOSTPORT="${ADDRESS#http://}"
HOSTPORT="${HOSTPORT#https://}"
HOST="${HOSTPORT%%:*}"
PORT="${HOSTPORT##*:}"

echo "==> Target Silo gRPC: $ADDRESS"

if ! nc -z "$HOST" "$PORT" 2>/dev/null; then
  echo "ERROR: nothing listening on $HOST:$PORT."
  echo "       Start the dev cluster first: run 'dev' in another terminal."
  exit 1
fi
echo "    Cluster is reachable."

# Build once up front so the timed passes below aren't skewed by compilation.
echo "==> Building silo-bench..."
cargo build --bin silo-bench >/dev/null 2>&1
BENCH="target/debug/silo-bench"
echo "    Using $BENCH"

# silo-bench is a correctness harness: after draining it audits for leaked
# concurrency holders and exits non-zero if any remain (its floating-failure
# path deliberately exercises that). For *seeding* we only care that jobs were
# created, so a non-zero audit verdict is not a failure here — we report it and
# keep going rather than letting `set -e` abort the run.
run_bench() {
  local label="$1"; shift
  echo
  echo "==> Pass: $label"
  echo "    silo-bench $*"
  if "$BENCH" --address "$ADDRESS" "$@"; then
    echo "    Pass OK."
  else
    echo "    Pass exited non-zero (likely silo-bench's post-drain holder audit) — data was still seeded; continuing."
  fi
}

# Pass 1 — balanced multi-tenant history.
# Workers keep up with enqueuers, so this produces a healthy mix of completed /
# failed jobs across several tenants, plus floating-limit refresh activity.
run_bench "balanced multi-tenant history" \
  --workers 8 --enqueuers 4 --duration-secs 20 \
  --tenant-count 6 --tenant-prefix demo \
  --max-concurrency 25 --max-floating-limits 2 --floating-failure-rate 0.15

# Pass 2 — tenant skew. Early tenants get far more work than later ones, so
# tenant-ranking / distribution panels show meaningful skew.
run_bench "tenant skew" \
  --workers 8 --enqueuers 6 --duration-secs 15 \
  --tenant-count 10 --tenant-prefix skew \
  --imbalance-factor 5.0 --max-concurrency 40 --max-floating-limits 1

# Pass 3 — enqueue-only backlog, left behind on purpose.
# `--workers 0` means jobs are enqueued but never leased: no holders are taken
# (so the audit passes and we exit 0), and the jobs persist as pending work
# spread across tenants/shards for the cluster/tenant/shard/job panels.
run_bench "enqueue-only backlog (persists after exit)" \
  --workers 0 --enqueuers 6 --duration-secs 10 \
  --tenant-count 5 --tenant-prefix backlog \
  --concurrency-key backlog-queue --max-concurrency 5 --max-floating-limits 0

echo
echo "==> Done seeding sample data."
echo "    Open the dashboard:  http://127.0.0.1:8081  (node 1)"
echo "                         http://127.0.0.1:8082  (node 2)"
echo
echo "    To watch LIVE concurrency holders + waiters, run this and refresh the"
echo "    dashboard while it runs (it deliberately can't drain, so it ends with"
echo "    an *expected* holder-audit error — that's fine, just Ctrl-C it):"
echo
echo "      $BENCH --address $ADDRESS \\"
echo "        --workers 1 --enqueuers 8 --duration-secs 600 \\"
echo "        --concurrency-key hot-queue --max-concurrency 3 --max-floating-limits 0"
echo
echo "    To start clean later: stop the cluster, run scripts/reset-dev-cluster.sh, then 'dev'."
