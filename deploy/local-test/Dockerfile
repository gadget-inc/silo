FROM rust:1.89-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY silo-macros ./silo-macros
COPY proto ./proto
COPY build.rs ./
COPY src ./src
COPY templates ./templates
COPY benches ./benches
COPY tests ./tests
RUN cargo build --release --locked --bin silo --bin siloctl

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/silo /usr/local/bin/silo
COPY --from=builder /app/target/release/siloctl /usr/local/bin/siloctl
EXPOSE 50051 50052 9090
ENTRYPOINT ["/usr/local/bin/silo"]
