#!/usr/bin/env bash
#
# Reset the dev cluster state by removing etcd and slatedb data directories.
# Run this script from the repository root when the dev cluster is stopped.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ETCD_DATA_DIR="$REPO_ROOT/tmp/etcd-data"
SILO_DATA_DIR="$REPO_ROOT/tmp/silo-data"

echo "Resetting dev cluster state..."

if [ -d "$ETCD_DATA_DIR" ]; then
    echo "Removing etcd data: $ETCD_DATA_DIR"
    rm -rf "$ETCD_DATA_DIR"
else
    echo "No etcd data found at $ETCD_DATA_DIR"
fi

if [ -d "$SILO_DATA_DIR" ]; then
    echo "Removing slatedb data: $SILO_DATA_DIR"
    rm -rf "$SILO_DATA_DIR"
else
    echo "No slatedb data found at $SILO_DATA_DIR"
fi

echo "Done. Dev cluster state has been reset."
