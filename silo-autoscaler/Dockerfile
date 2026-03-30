FROM rust:1.89.0 AS builder
WORKDIR /app

# Copy workspace Cargo files for dependency resolution
COPY Cargo.toml Cargo.lock ./
COPY silo-macros/Cargo.toml silo-macros/Cargo.toml
COPY silo-autoscaler/Cargo.toml silo-autoscaler/Cargo.toml

# Create minimal dummy sources so cargo can resolve the workspace.
# The main silo crate needs dummy files (including benches/tests referenced in Cargo.toml)
# but won't actually be built — only silo-autoscaler is compiled.
RUN mkdir -p src silo-macros/src silo-autoscaler/src benches tests/turmoil_runner && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    touch silo-macros/src/lib.rs && \
    echo "fn main() {}" > silo-autoscaler/src/main.rs && \
    echo "fn main() {}" > benches/job_shard_throughput.rs && \
    echo "fn main() {}" > benches/query_performance.rs && \
    echo "fn main() {}" > benches/big_shard_operations.rs && \
    echo "fn main() {}" > benches/query_profile.rs && \
    echo "fn main() {}" > benches/task_scanner_profile.rs && \
    echo "fn main() {}" > tests/turmoil_runner/main.rs

# Pre-build dependencies (cached layer)
RUN cargo build --release --package silo-autoscaler 2>/dev/null || true

# Copy actual autoscaler source
COPY silo-autoscaler/src silo-autoscaler/src

# Touch to invalidate cache for the autoscaler source only
RUN touch silo-autoscaler/src/main.rs

# Build the real binary
RUN cargo build --release --package silo-autoscaler

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/silo-autoscaler /
ENTRYPOINT ["/silo-autoscaler"]
