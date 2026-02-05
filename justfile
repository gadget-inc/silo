default:
    @just --list

fmt:
    cargo clippy --fix --allow-dirty && rustfmt --edition=2024 **/*.rs

run *ARGS:
    cargo run {{ARGS}}

watch *ARGS:
	bacon --job run -- -- {{ ARGS }}

etcd:
    etcd

# Install cargo-llvm-cov if not already installed
_ensure-llvm-cov:
    @command -v cargo-llvm-cov >/dev/null 2>&1 || cargo install cargo-llvm-cov

# Quick coverage: run main tests only (skips etcd/k8s/DST) and generate HTML report
coverage: _ensure-llvm-cov
    cargo llvm-cov --html --output-dir target/coverage -- --skip k8s_ --skip etcd_ --skip coordination_split_tests

# Quick coverage: generate and open HTML report in browser
coverage-open: _ensure-llvm-cov
    cargo llvm-cov --html --output-dir target/coverage --open -- --skip k8s_ --skip etcd_ --skip coordination_split_tests

# Quick coverage: print summary to terminal
coverage-summary: _ensure-llvm-cov
    cargo llvm-cov -- --skip k8s_ --skip etcd_ --skip coordination_split_tests

# Full coverage: run ALL test groups (main + etcd + k8s) and generate HTML report.
# Requires etcd running and a k8s cluster available (e.g. orbstack).
# Excludes turmoil DST tests (require --features dst).
coverage-full: _ensure-llvm-cov
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report -- --skip k8s_ --skip etcd_ --skip coordination_split_tests
    cargo llvm-cov --no-report --test etcd_coordination_tests -- --test-threads=1
    cargo llvm-cov --no-report --test coordination_split_tests -- --test-threads=1
    cargo llvm-cov --no-report --test k8s_coordination_tests -- --test-threads=1
    cargo llvm-cov report --html --output-dir target/coverage
    @echo "Coverage report: target/coverage/html/index.html"

# Full coverage: print summary to terminal (after running coverage-full)
coverage-full-summary: _ensure-llvm-cov
    cargo llvm-cov report

# Generate lcov format for CI/IDE integration
coverage-lcov: _ensure-llvm-cov
    cargo llvm-cov --lcov --output-path target/coverage/lcov.info -- --skip k8s_ --skip etcd_ --skip coordination_split_tests

# Generate JSON coverage report for programmatic analysis
coverage-json: _ensure-llvm-cov
    cargo llvm-cov --json --output-path target/coverage/coverage.json -- --skip k8s_ --skip etcd_ --skip coordination_split_tests
