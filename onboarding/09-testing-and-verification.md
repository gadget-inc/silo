# 09 — Testing & Verification

Silo is a distributed system holding durable state, so correctness rests on **four** layers. Understand all four — you'll touch them constantly.

1. **Integration tests** (`tests/*.rs`) — fast, exhaustive per-operation coverage.
2. **Deterministic Simulation Testing (DST)** — distributed chaos with reproducible seeds.
3. **Alloy formal models** (`specs/*.als`) — machine-checked proofs, linked to code by sigils.
4. **Benchmarks** (`benches/`) — performance tracking.

House rule: **all Rust tests live in `tests/`** (each file = its own test binary), never inline in `src/`. A handful of `#[cfg(test)]` exceptions exist for self-contained modules, but for job-broker logic the rule holds.

---

## 1. Test organization

### `#[silo::test]` vs `#[tokio::test]`
Use **`#[silo::test]`** (defined in the `silo-macros` crate). For an `async` fn it expands to `#[tokio::test]` + wraps the body in `silo::trace::with_test_tracing(name, ...)`; for a sync fn it expands to `#[test]` + `with_test_tracing_sync`. It forwards tokio args (`#[silo::test(flavor = "multi_thread", worker_threads = 2)]`). Benefit: per-test tracing, and per-test Perfetto files if `SILO_PERFETTO_DIR` is set. (The DST runner is the one place that uses plain `#[test]` — it installs its own deterministic subscriber.)

### Major test files by area (~70 total)
- **Single-shard engine**: `job_store_shard_{enqueue,dequeue,cancel,retry,expedite,restart,restart_attempt,lease_task,edge_case}_tests.rs`, `..._import_tests.rs`, `..._concurrency_tests.rs` (the largest).
- **Concurrency limits**: `concurrency_tests.rs`, `floating_concurrency_tests.rs`, `concurrency_counts_tests.rs`, `concurrency_requester_counter_tests.rs`, `concurrent_grant_race_tests.rs`, `holder_leak_tests.rs`, `counter_reconcile_tests.rs`, `task_scanner_saturation_tests.rs`.
- **Coordination/clustering**: `etcd_coordination_tests.rs`, `k8s_coordination_tests.rs` (largest file overall), `coordination_split_tests.rs`, `coordination_mod_tests.rs`, `none_coordinator_tests.rs`, plus `shard_split_tests.rs`, `shard_range_tests.rs`, `shard_cleanup_tests.rs`, `startup_hydration_tests.rs`, etc.
- **gRPC API**: `grpc_job_tests.rs`, `grpc_misc_tests.rs`, `grpc_query_tests.rs`, `grpc_enqueue_tests.rs`, `grpc_auth_tests.rs`, …
- **Query engine**: `query_tests.rs`, `cluster_query_tests.rs`, `cluster_query_integration_tests.rs`, `query_waiting_tests.rs`.
- **Routing**: `cluster_client_tests.rs`, `cluster_info_cache_tests.rs`, `routing_client_tests.rs`, `bench_routing_tests.rs`.
- **Cross-cutting**: `metrics_tests.rs`, `rate_limit_tests.rs`, `gubernator_tests.rs`, `codec_tests.rs`, `keys_tests.rs`, `factory_tests.rs`, `retry_tests.rs`, `webui_tests.rs`, `siloctl_tests.rs`, …

---

## 2. Unit/integration fixtures

### Spinning up a shard (`tests/test_helpers.rs`)
`open_temp_shard()` — a `tempdir` + `Backend::Fs` (local FS as a stand-in for object storage) + a `JobStoreShard`, returns `(TempDir, Arc<JobStoreShard>)`. Uses `fast_flush_slatedb_settings()` (10ms flush) and a `MockGubernatorClient`. Variants exist for metrics, terminal/split expiry, reconcile interval, custom range, custom rate limiter, local WAL. Inspection helpers read the binary keyspace directly: `count_task_keys`, `count_lease_keys`, `count_concurrency_holders`, etc. Plus `poll_until()` and a `with_timeout!` macro.

### Spinning up a gRPC server (`tests/grpc_integration_helpers.rs`)
`setup_test_server(factory, config)` — binds `127.0.0.1:0`, uses a `NoneCoordinator` (single-node), spawns `run_server`, returns a connected `SiloClient` + shutdown handle. `setup_multi_shard_server(n, config)` for N shards.

### Flake-avoidance conventions
- **Use separate tenants per test** to avoid cross-test interference.
- etcd needs adequate lease timeouts; k8s tests interfere → run single-threaded.

---

## 3. Deterministic Simulation Testing (DST)

### What & why
DST runs the **real** silo server inside a simulated world where **time, randomness, and the network are all controlled and reproducible**. For a distributed broker, this is the only sane way to test lease expiry, partitions, message loss, and split-brain — and a failure is reproducible byte-for-byte from one integer **seed**.

Two libraries (gated behind the `dst` feature):
- **turmoil** — simulates TCP + time across multiple "hosts" in one process, with fault injection (`partition()`, latency/loss).
- **mad-turmoil** — intercepts at the **libc level**: `clock_gettime` (so `SystemTime::now()` is simulated) and `getrandom`/`getentropy` (so all RNG, incl. UUIDs, is seeded).

The runner is a dedicated test binary at **`tests/turmoil_runner/`** (a *directory*: `main.rs`, `helpers.rs`, `mock_k8s.rs`, `scenarios/`), `required-features = ["dst"]`.

### Determinism plumbing (`tests/turmoil_runner/helpers.rs`)
`run_scenario_impl(name, seed, duration, setup)` prints markers, clears shared storage, `init_deterministic_sim(seed)` (seeds mad-turmoil RNG + `fastrand` + simulated clocks), builds the turmoil `Sim` (1ms tick, `rng_seed(seed)`), runs, emits result markers. Deterministic tracing disables ANSI/thread-ids/file-line and enforces **one scenario per process**.

`setup_server(port)` is the DST gRPC fixture: in-memory `mem://shard-{shard}` DB with **SlateDB compaction disabled** (the compactor uses `spawn_blocking`/`block_on`, incompatible with turmoil's single-threaded runtime), turmoil `TcpListener`, `NoneCoordinator`.

### Simulated object store (`src/turmoil_object_store.rs`)
`TurmoilObjectStore` over a process-global `BTreeMap` (deterministic iteration order), shared across hosts (models a shared bucket), timestamps from `sim_elapsed()`, seeded `simulate_latency`. `dst`-feature-only. `clear_shared_storage()` between runs.

### Event instrumentation & invariant checking (`src/dst_events.rs` + `helpers.rs`)
The server emits structured `DstEvent`s (`JobEnqueued`, `JobStatusChanged`, `TaskLeased`, `TaskReleased`, `ConcurrencyTicketGranted/Released`, `ShardAcquired/Closed/Released`) — **zero-cost no-ops without the `dst` feature**. Because turmoil's scheduler interleaves around `.await`, events use **two-phase emission**: `emit_pending(event, write_op)` records at the correct causal position, then `confirm_write`/`cancel_write` based on the durable write's outcome. At sim end, the confirmed event stream is replayed into trackers that enforce **the same invariants Alloy proves**:
- `GlobalConcurrencyTracker` — `queueLimitEnforced`, `noDoubleLease`, `oneLeasePerJob`.
- `ShardOwnershipTracker` — `noSplitBrain`, `closeBeforeRelease`.
- `JobStateTracker` — `validTransitions`, `noLeasesForTerminal`, `noZombieAttempts`.

These trackers explicitly state they're "derived from the Alloy specification in specs/job_shard.als" — the live link between formal model and running system.

### Scenarios (`tests/turmoil_runner/scenarios/`)
Each is one file with `pub fn run()`: `cancel_releases_ticket`, `chaos`, `concurrency_limits`, `concurrent_grant_race`, `expedite_concurrency`, `fault_injection_partition`, `floating_concurrency`, `grpc_end_to_end`, `high_latency`, `high_message_loss`, `lease_expiry`, `multiple_workers`, `rate_limits`, `retry_releases_ticket`, plus k8s-gated `k8s_coordination`, `k8s_permanent_leases`, `k8s_shard_splits` (which run the **real** `K8sCoordinator` against `MockK8sBackend`).

### Two modes & how to run
- **Verification mode** (default): runs each scenario **twice in separate subprocesses** with the same seed and asserts **byte-identical** trace output (catches non-determinism). Each subprocess has a 180s timeout.
- **Fuzz mode** (`DST_FUZZ=1` / the script): runs each scenario once per seed, panics on failure.

```bash
cargo test --test turmoil_runner                 # verify determinism (each scenario ×2)
DST_SEED=12345 cargo test --test turmoil_runner  # verify a specific seed
scripts/run-simulation-tests.mjs --seed 123      # reproduce one seed
scripts/run-simulation-tests.mjs --seeds 200     # fuzz 200 random seeds
```
The fuzzer auto-discovers scenarios, builds once, runs seeds in a parallel pool, **weights** seed→scenario by historical CI failure frequency, and writes failing-seed logs to `dst-logs/`. Skills `.ai/skills/fix-dst/SKILL.md` and `.ai/skills/fix-ci/SKILL.md` drive these workflows.

---

## 4. Alloy formal specs (`specs/`)

### What Alloy is
[Alloy](https://alloytools.org/) is a declarative formal-modeling language with a **bounded model checker**: describe state + transitions, write `assert`ions and `run` examples, and it exhaustively searches all states up to a finite bound. A `check` finding **no** counterexample is `UNSAT` (good); a `run` finding an instance is `SAT` (good). Silo uses it to prove its algorithms sound *independent of* the Rust.

Two models:
- `specs/job_shard.als` — the job/task/lease/concurrency state machine. 26 assertions (`noDoubleLease`, `oneLeasePerJob`, `validTransitions`, `queueLimitEnforced`, `retryReleasesTicket`, the expedite/import families, …).
- `specs/coordination.als` — shard ownership, leases, ranges, splits. 16 assertions (`noSplitBrain`, `noOverlappingRanges`, `splitChildrenContiguous`, `flapPreservesLeases`, `splitPhaseMonotonic`, …).

### The sigil system
Each Alloy predicate the implementation must honor carries a tag like `[SILO-ENQ-1]`, `[SILO-GRANT-1]`, `[SILO-COORD-INV-1]`, and the **same tag** is placed in a comment at the corresponding Rust site — bidirectional traceability between proof and code. Examples:
- `src/job_store_shard/enqueue.rs:316` — `// [SILO-ENQ-1] If caller provided an id, ensure it doesn't already exist`
- `src/concurrency.rs:1859` — `// [SILO-GRANT-1] ... try to atomically reserve a slot`
- `src/coordination/k8s.rs:1965` — `/// [SILO-COORD-INV-1] Try to acquire the lease using compare-and-swap`

### CI enforcement
- **Sigil sync** (`scripts/validate-silo-sigils.js`) runs in the `fmt` job: fails if any `[SILO-...]` tag exists in Alloy but not Rust, or vice versa. **So don't invent a Rust sigil without a matching Alloy assertion, and don't orphan one.**
- **Alloy verification** (`scripts/run-alloy-verification.mjs`) runs `alloy6` and asserts no `check` is SAT and no `run` is UNSAT — but **only when the relevant spec changed** (paths-filter gated).

---

## 5. CI (`.github/workflows/`)

Main pipeline `ci.yml`:

| Job | Runs |
|-----|------|
| `fmt` | `cargo fmt --check` + sigil validation |
| `clippy` | `clippy --lib --bins --all-features -D warnings` (prod warnings fail); `clippy --tests` warn-only |
| `test` | starts gubernator+etcd, then `cargo test -- --skip k8s_ --skip etcd_ --skip coordination_split_tests --skip cluster_two_nodes` (the parallel pass) |
| `test-etcd` | `etcd_coordination_tests` + `coordination_split_tests` **`--test-threads=1`**, then `cluster_query_integration_tests` |
| `test-k8s` | **kind** cluster + `k8s_coordination_tests --test-threads=1` |
| `test-autoscaler` | kind + nix images + autoscaler integration, single-threaded |
| `benchmark` | `cargo bench` for a few benches (caches a golden shard) |
| `alloy-*` | conditional Alloy verification |
| `typescript-client-*` | TS lint/typecheck/tests (against a real 2-node silo via toxiproxy) |
| `build-docker` | multi-arch nix-built images |

### Why three single-threaded groups
**k8s, etcd, and `coordination_split_tests`** share external global state (a kind cluster, etcd keyspace, coordination state) and otherwise stomp each other → they run in their own jobs with `--test-threads=1`. (Mock-k8s and DST k8s scenarios use mocks, not the cluster.)

Other workflows: `simulation-testing.yml` (the DST fuzzer, manual dispatch with `num_seeds`), `grind.yml` (flake hunting — loops the main/etcd/split groups).

Coverage (`cargo-llvm-cov` via `just coverage` / `just coverage-full`) — DST tests are excluded (they exist for determinism, not line coverage).

---

## 6. Benchmarks (`benches/`)

`harness = false` (custom `main()`). Shared `bench_helpers.rs` maintains a cached **"golden shard"** (~500K jobs, Zipf tenant distribution), with cheap copy-on-write clones via SlateDB checkpoints for mutating benches. The benches:
- `job_shard_throughput.rs` — enqueue/dequeue throughput at 1/4/8 producers/consumers (the headline metric; tracked over time via `scripts/benchmark-history.mjs`).
- `big_shard_operations.rs` — common ops against the golden dataset.
- `query_performance.rs` / `query_profile.rs` — DataFusion query latency (needs `server` feature).
- `task_scanner_profile.rs` — sustained dequeue exercising the broker scanner hot path.
- `k8s-bench.mjs` — separate JS-driven k8s benchmark.

---

## 7. Coordination-backend test fidelity

| Backend | Fidelity | Where |
|---------|----------|-------|
| `none` | single-node | nearly all integration + DST fixtures |
| `etcd` | **real** etcd (process-compose / CI), single-threaded | `etcd_coordination_tests.rs` |
| `k8s` (real) | **real** kind cluster (CI) / orbstack (local), single-threaded | `k8s_coordination_tests.rs` |
| `k8s` (mocked) | real `K8sCoordinator` vs `MockK8sBackend` (CAS, watches, injectable latency/failures), deterministic | DST `k8s_*` scenarios |

---

## 8. Two known doc-vs-code drifts to be aware of

- `CONTRIBUTING.md` and `.ai/rules/dst.md` say DST lives in `tests/turmoil_runner.rs`, but it was refactored into a **directory** `tests/turmoil_runner/`. Trust the directory.
- The Starlight `docs/` site can lag the code; per project policy, **the code is the source of truth.**

---

## Quick command reference

```bash
cargo test                                                   # main suite
cargo test -- --skip k8s_ --skip etcd_ --skip coordination_split_tests  # exactly what CI's main job runs
cargo test some_test_name                                    # one test by name
cargo test --test job_store_shard_enqueue_tests             # one test file
cargo test --test etcd_coordination_tests -- --test-threads=1
cargo test --test turmoil_runner                            # DST determinism verification
DST_SEED=12345 cargo test --test turmoil_runner
node scripts/validate-silo-sigils.js                        # Alloy↔Rust sigil sync
node scripts/run-alloy-verification.mjs specs/job_shard.als
just fmt                                                     # clippy --fix + rustfmt
just coverage                                               # coverage report
cargo bench --bench job_shard_throughput
```

Next: [`10-supporting-crates-and-dev-env.md`](./10-supporting-crates-and-dev-env.md).
