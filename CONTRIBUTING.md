# silo development

## Nix + flakes

Silo uses nix flakes to set up a local development environment. Ensure you have nix installed, with flakes support enabled, and `nix-direnv` in your profile. The flake will be automatically loaded by the `.envrc` file

## Running background services

You can run silo locally to make requests to a test instance. Running locally requires running `etcd` and `gubernator` in the background. You can run these with:

```shell
dev
```

## Setting up for agentic development

Silo uses `lnai` to link all the configs for various agent development tools locally.

Run

```shell
pnpm install -g lnai
lnai sync
```

to setup the repo's rules and commands for your local editor

## Formatting code

You can autoformat all the Rust code with

```shell
just fmt
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

## Code coverage

Coverage uses [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov). It will be installed automatically if missing.

### Quick coverage (main tests only)

Runs all tests except etcd, k8s, and DST tests. No background services required beyond what normal tests need.

```shell
# HTML report at target/coverage/html/index.html
just coverage

# HTML report, then open in browser
just coverage-open

# Summary printed to terminal
just coverage-summary
```

### Full coverage (all test groups)

Runs main tests, etcd coordination tests, coordination split tests, and k8s coordination tests with merged coverage. Requires etcd running and a k8s cluster available (e.g. orbstack). DST tests are excluded (they require `--features dst` and are focused on determinism verification, not coverage).

```shell
# Run all test groups and generate HTML report
just coverage-full

# After running coverage-full, print summary to terminal
just coverage-full-summary
```

The `coverage-full` target handles the multi-step process automatically:
1. Cleans previous coverage data
2. Runs main tests (skipping etcd/k8s tests)
3. Runs etcd coordination tests single-threaded
4. Runs coordination split tests single-threaded
5. Runs k8s coordination tests single-threaded
6. Generates a merged HTML report

### Other formats

```shell
# LCOV format (for IDE integration)
just coverage-lcov

# JSON format (for programmatic analysis)
just coverage-json
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

## Benchmark history

You can view throughput benchmark trends over time by fetching results from CI:

```shell
# Last 30 days (default)
node scripts/benchmark-history.mjs

# Last 90 days
node scripts/benchmark-history.mjs --days 90
```

This fetches the `job_shard_throughput` benchmark output from each successful main branch CI run, and opens an interactive HTML report with charts and a table showing enqueue/dequeue throughput over time.

## Deterministic Simulation Testing (DST)

Silo uses deterministic simulation testing via [turmoil](https://github.com/tokio-rs/turmoil) and [mad-turmoil](https://crates.io/crates/mad-turmoil) to test distributed scenarios with controlled randomness and simulated time. This allows us to:

- Reproduce bugs reliably by re-running with the same seed
- Test scenarios that would be flaky with real time (e.g., lease expiry, timeouts)
- Inject network faults (partitions, message loss, latency) deterministically

### How it works

The DST tests live in `tests/turmoil_runner.rs`. Each test scenario runs in an isolated subprocess with:

- **Simulated time**: `mad-turmoil` intercepts `clock_gettime` at the libc level, so `SystemTime::now()` returns turmoil's simulated time
- **Deterministic randomness**: `mad-turmoil` intercepts `getrandom`/`getentropy`, so all random number generation (including UUIDs) is seeded deterministically
- **Simulated network**: turmoil simulates TCP connections between hosts, allowing fault injection

### Running DST tests

```shell
# Run the fuzzer to test for one seed, which will choose the scenario automatically based on the seed
scripts/run-simulation-tests.mjs --seed 123

# Run the fuzzer to test many random seeds
scripts/run-simulation-tests.mjs --seeds 100
```

### Two modes

1. **Verification mode** (default for `cargo test`): Runs each scenario twice with the same seed and compares trace output byte-for-byte. This proves the simulation is truly deterministic.

2. **Fuzz mode** (via the script or `DST_FUZZ=1`): Runs each scenario once per seed to maximize coverage. Use this to find bugs by testing many seeds quickly.

#### Running verification tests

```bash
# Run all DST tests with determinism verification (runs each scenario twice)
cargo test --test turmoil_runner

# Run determinism verifier with a specific seed
DST_SEED=12345 cargo test --test turmoil_runner
```

### Adding a new scenario

1. **Create the scenario function** in `tests/turmoil_runner.rs`:

```rust
fn scenario_your_test_name() {
    let seed = get_seed();
    run_scenario_impl("your_test_name", seed, 30, |sim| {
        // Set up turmoil simulation
        sim.host("server", || async move {
            setup_server(9900).await
        });

        sim.client("client", async move {
            // Your test logic here
            // Use tracing::trace!() to emit deterministic events
            tracing::trace!(key = "value", "event description");
            Ok(())
        });

        Ok(())
    });
}
```

2. **Add the test function**:

```rust
#[silo::test]
fn your_test_name() {
    if is_subprocess() || is_fuzz_mode() {
        scenario_your_test_name();
    } else {
        verify_determinism("your_test_name", get_seed());
    }
}
```

3. **Tips for deterministic scenarios**:
   - Use `tracing::trace!()` to log important events for determinism verification
   - Use turmoil's fault injection (`sim.partition()`, `sim.set_message_loss_rate()`) to test failure scenarios
   - The `run_scenario_impl` helper sets up tracing and mad-turmoil automatically
