//! Unit tests for the simulation's shard ownership tracker.
//!
//! The tracker lives with the simulation crate; its tests run here because the
//! `turmoil_runner` binary links mad-turmoil, whose process clock stands still
//! outside a simulation.

#[path = "turmoil_runner/shard_ownership_tracker.rs"]
mod shard_ownership_tracker;
