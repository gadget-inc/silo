---
paths:
  - "tests/turmoil/**"
---

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
