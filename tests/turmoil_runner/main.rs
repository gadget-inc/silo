//! Turmoil DST Test Runner
//!
//! This is a test binary that runs turmoil simulation scenarios in isolation.
//! Each scenario runs in its own process to ensure clean state and true determinism.
//!
//! # Usage
//!
//! Run a specific scenario:
//! ```bash
//! DST_SEED=12345 cargo test --test turmoil_runner -- scenario_name --nocapture
//! ```
//!
//! The test captures all TRACE-level logs and validates they are byte-for-byte identical
//! across runs with the same seed. This ensures true determinism of the simulation.
//!
//! # Adding New Scenarios
//!
//! 1. Create a new file in `scenarios/` with a `pub fn run()` function
//! 2. Add it to `scenarios/mod.rs`
//! 3. Add a `#[test] fn your_name()` in this file that calls the scenario

mod helpers;
mod scenarios;

use helpers::{get_seed, is_fuzz_mode, is_subprocess, verify_determinism};

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn grpc_end_to_end() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::grpc_end_to_end::run();
    } else {
        verify_determinism("grpc_end_to_end", get_seed());
    }
}

#[test]
fn fault_injection_partition() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::fault_injection_partition::run();
    } else {
        verify_determinism("fault_injection_partition", get_seed());
    }
}

#[test]
fn multiple_workers() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::multiple_workers::run();
    } else {
        verify_determinism("multiple_workers", get_seed());
    }
}

#[test]
fn lease_expiry() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::lease_expiry::run();
    } else {
        verify_determinism("lease_expiry", get_seed());
    }
}

#[test]
fn high_message_loss() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::high_message_loss::run();
    } else {
        verify_determinism("high_message_loss", get_seed());
    }
}

#[test]
fn concurrency_limits() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::concurrency_limits::run();
    } else {
        verify_determinism("concurrency_limits", get_seed());
    }
}

#[test]
fn chaos() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::chaos::run();
    } else {
        verify_determinism("chaos", get_seed());
    }
}
