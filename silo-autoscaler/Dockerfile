FROM rust:1.89.0 AS builder
WORKDIR /app

# Copy only the autoscaler crate and the lockfile.
# Create a minimal single-member workspace so cargo doesn't need
# to resolve the root silo crate or its bench/test targets.
COPY Cargo.lock ./
COPY silo-autoscaler/Cargo.toml silo-autoscaler/Cargo.toml
RUN echo '[workspace]\nmembers = ["silo-autoscaler"]' > Cargo.toml

# Dummy source for dependency caching
RUN mkdir -p silo-autoscaler/src && \
    echo "fn main() {}" > silo-autoscaler/src/main.rs
RUN cargo build --release --package silo-autoscaler 2>/dev/null || true

# Copy actual source and rebuild
COPY silo-autoscaler/src silo-autoscaler/src
RUN touch silo-autoscaler/src/main.rs && \
    cargo build --release --package silo-autoscaler

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/silo-autoscaler /
ENTRYPOINT ["/silo-autoscaler"]
