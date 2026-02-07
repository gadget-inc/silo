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
//! 3. Add a `#[silo::test] fn your_name()` in this file that calls the scenario

mod helpers;
#[cfg(feature = "k8s")]
mod mock_k8s;
mod scenarios;

use helpers::{get_seed, is_fuzz_mode, is_subprocess, verify_determinism};

// DST tests use plain #[test] instead of #[silo::test] because they set up
// their own deterministic tracing subscriber in init_deterministic_tracing().
// Using #[silo::test] would set up a global subscriber first, causing a conflict.

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

#[test]
fn retry_releases_ticket() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::retry_releases_ticket::run();
    } else {
        verify_determinism("retry_releases_ticket", get_seed());
    }
}

#[test]
fn cancel_releases_ticket() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::cancel_releases_ticket::run();
    } else {
        verify_determinism("cancel_releases_ticket", get_seed());
    }
}

#[test]
fn expedite_concurrency() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::expedite_concurrency::run();
    } else {
        verify_determinism("expedite_concurrency", get_seed());
    }
}

#[test]
fn rate_limits() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::rate_limits::run();
    } else {
        verify_determinism("rate_limits", get_seed());
    }
}

#[test]
fn floating_concurrency() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::floating_concurrency::run();
    } else {
        verify_determinism("floating_concurrency", get_seed());
    }
}

#[test]
fn high_latency() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::high_latency::run();
    } else {
        verify_determinism("high_latency", get_seed());
    }
}

#[test]
#[cfg(feature = "k8s")]
fn k8s_coordination() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::k8s_coordination::run();
    } else {
        verify_determinism("k8s_coordination", get_seed());
    }
}

#[test]
#[cfg(feature = "k8s")]
fn k8s_shard_splits() {
    if is_subprocess() || is_fuzz_mode() {
        scenarios::k8s_shard_splits::run();
    } else {
        verify_determinism("k8s_shard_splits", get_seed());
    }
}
