# silo development

## Nix + flakes

Silo uses nix flakes to set up a local development environment. Ensure you have nix installed, with flakes support enabled, and `nix-direnv` in your profile. The flake will be automatically loaded by the `.envrc` file

## Running background services

You can run silo locally to make requests to a test instance. Running locally requires running `etcd` and `gubernator` in the background. You can run these with:

```shell
dev
```

## Running tests

### Rust tests

After starting the background services, you can run Rust tests with:

```shell
cargo test
```

### TypeScript client tests

The TypeScript client has unit tests and integration tests. Run from the `typescript_client/` directory:

```shell
cd typescript_client
pnpm install
pnpm test
```

Unit tests use mocks and run without any backend services. Integration tests automatically skip when no server is available.

To run integration tests, start etcd and a silo server, then run with `RUN_INTEGRATION=true`:

```shell
# Terminal 1: Start etcd
just etcd

# Terminal 2: Start silo with the test config
cargo run --bin silo -- -c typescript_client/test/silo-test-config.toml

# Terminal 3: Run all tests including integration tests
cd typescript_client
RUN_INTEGRATION=true pnpm test
```

## Running a local instance

You can run a local server instance by using cargo to build and run the `silo` binary:

```shell
cargo run --bin silo
```

## Running Alloy system models

We use [Alloy](https://alloytools.org/) for software modelling to help us prove that Silo's algorithms are sound. The `specs/` directory contains the Alloy models, and the Rust implementation has some [SILO-XYZ-1] sigils in comments referring back to specific Alloy predicates we know we need for correctness.

Run the alloy verifier with:

```shell
alloy6 exec -f -s glucose -o specs/output specs/job_shard.als
```

A pass is indicated by these two things being true:

- all the `check` commands are `UNSAT`, ie, no counter examples are found for any checks
- all the `run` commands are `SAT` such that one trace matches the examples we require

### Perfetto traces for debugging

We support exporting `tracing` spans to a Perfetto trace you can open in the Perfetto UI.

- **Enable in the binary**: set `SILO_PERFETTO` to a file path. Example:

```bash
SILO_PERFETTO=/tmp/silo-run.pftrace cargo run --bin silo
```

- **Enable per test case**: use the test attribute `#[silo::test]` instead of `#[tokio::test]`. This automatically installs a per-test subscriber and, if `SILO_PERFETTO_DIR` is set, writes a unique Perfetto file per test named `<testname>-<timestamp>.pftrace`. You can still pass tokio args through, e.g. `#[silo::test(flavor = "multi_thread", worker_threads = 2)]`.
  - Set `SILO_PERFETTO_DIR` to a directory to capture each test into its own timestamped file
  - Or set `SILO_PERFETTO` to a specific file path to force a single-file capture for the test

Examples:

```bash
# Capture a single test into a dedicated file
SILO_PERFETTO=/tmp/coord_unique.pftrace cargo test --test coordination_tests -- multiple_nodes_own_unique_shards --exact --nocapture

# Capture separate files per test into a directory
mkdir -p /tmp/silo-traces
SILO_PERFETTO_DIR=/tmp/silo-traces cargo test --test coordination_tests -- adding_a_node_rebalances_shards --exact --nocapture
```

- **Viewing traces**: open the generated `.pftrace` file at the Perfetto UI: [`https://ui.perfetto.dev`](https://ui.perfetto.dev)

- **Notes**:
  - Tracing also respects `RUST_LOG` via `tracing-subscriber`'s `EnvFilter` (defaults to `info`).
  - If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the binary exports OpenTelemetry spans instead; set `SILO_PERFETTO` to prefer Perfetto in the binary.
  - Etcd is required for tests and local runs; ensure it is running (e.g., `just etcd`).

References:

- Perfetto layer crate docs: [`tracing-perfetto`](https://docs.rs/tracing-perfetto/latest/tracing_perfetto/)
