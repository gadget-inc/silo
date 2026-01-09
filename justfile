default:
    @just --list

fmt:
    cargo clippy --fix --allow-dirty && rustfmt --edition=2021 **/*.rs

run *ARGS:
    cargo run {{ARGS}}

watch *ARGS:
	bacon --job run -- -- {{ ARGS }}

etcd:
    etcd

# Install cargo-llvm-cov if not already installed
_ensure-llvm-cov:
    @command -v cargo-llvm-cov >/dev/null 2>&1 || cargo install cargo-llvm-cov

# Code coverage commands - generates HTML report
coverage: _ensure-llvm-cov
    cargo llvm-cov --html --output-dir target/coverage

# Generate and open HTML coverage report in browser
coverage-open: _ensure-llvm-cov
    cargo llvm-cov --html --output-dir target/coverage --open

# Print coverage summary to terminal
coverage-summary: _ensure-llvm-cov
    cargo llvm-cov

# Generate lcov format for CI/IDE integration
coverage-lcov: _ensure-llvm-cov
    cargo llvm-cov --lcov --output-path target/coverage/lcov.info

# Generate JSON coverage report for programmatic analysis
coverage-json: _ensure-llvm-cov
    cargo llvm-cov --json --output-path target/coverage/coverage.json

# Run coverage including integration tests
coverage-all: _ensure-llvm-cov
    cargo llvm-cov --html --output-dir target/coverage --doctests
