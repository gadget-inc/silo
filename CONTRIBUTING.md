# silo development

You can run silo locally to make requests to a test instance. Running locally requires a running `etcd` instance. You can run one with:

```shell
just etcd
```

Then you can run tests with:

```shell
cargo test
```

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
