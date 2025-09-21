# Justfile for silo

set shell := ["/bin/bash", "-cu"]

# default prints available recipes
default:
	@just --list

# Spawn a 3-node cluster with temporary configs and show leader election
# Usage: just cluster <seconds>
cluster seconds:
	set -euo pipefail
	rm -rf ./tmp/cluster || true
	mkdir -p ./tmp/cluster
	# Build once
	RUST_LOG=info cargo build --quiet
	# Start three nodes using configs in tests/cluster
	RUST_LOG=info target/debug/silo -c tests/cluster/n1.toml & echo $! > ./tmp/cluster/p1
	RUST_LOG=info target/debug/silo -c tests/cluster/n2.toml & echo $! > ./tmp/cluster/p2
	RUST_LOG=info target/debug/silo -c tests/cluster/n3.toml & echo $! > ./tmp/cluster/p3
	# Let the cluster run for a bit
	sleep {{seconds}}
	# Cleanup
	kill $(cat ./tmp/cluster/p1 ./tmp/cluster/p2 ./tmp/cluster/p3) >/dev/null 2>&1 || true
	wait >/dev/null 2>&1 || true

cluster-default:
	just cluster 10

fmt:
	rustfmt --all

run *ARGS:
	cargo run {{ARGS}}

watch *ARGS:
	bacon --job run -- -- {{ ARGS }}
