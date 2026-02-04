//! DST Scenarios
//!
//! Each scenario tests a specific aspect of the system under deterministic simulation.

pub mod cancel_releases_ticket;
pub mod chaos;
pub mod concurrency_limits;
pub mod expedite_concurrency;
pub mod fault_injection_partition;
pub mod floating_concurrency;
pub mod grpc_end_to_end;
pub mod high_latency;
pub mod high_message_loss;
#[cfg(feature = "k8s")]
pub mod k8s_coordination;
#[cfg(feature = "k8s")]
pub mod k8s_shard_splits;
pub mod lease_expiry;
pub mod multiple_workers;
pub mod rate_limits;
pub mod retry_releases_ticket;
