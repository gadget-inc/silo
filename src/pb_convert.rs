//! Pure-Rust conversion helpers between internal types and generated protobuf
//! types, plus shared gRPC metadata keys. Lives outside the `server` module so
//! it remains compileable without the heavy `server` feature (datafusion).

use crate::job_attempt::AttemptStatus as JobAttemptStatus;
use crate::pb::{AttemptStatus, ConcurrencyLimit, Limit, SerializedBytes, limit, serialized_bytes};

/// gRPC metadata key for shard owner address on redirect
pub const SHARD_OWNER_ADDR_METADATA_KEY: &str = "x-silo-shard-owner-addr";
/// gRPC metadata key for shard owner node ID on redirect
pub const SHARD_OWNER_NODE_METADATA_KEY: &str = "x-silo-shard-owner-node";

/// Convert a job::Limit to a proto Limit
pub fn job_limit_to_proto_limit(job_limit: crate::job::Limit) -> Limit {
    match job_limit {
        crate::job::Limit::Concurrency(c) => Limit {
            limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                key: c.key,
                max_concurrency: c.max_concurrency,
            })),
        },
        crate::job::Limit::RateLimit(r) => Limit {
            limit: Some(limit::Limit::RateLimit(crate::pb::GubernatorRateLimit {
                name: r.name,
                unique_key: r.unique_key,
                limit: r.limit,
                duration_ms: r.duration_ms,
                hits: r.hits,
                algorithm: r.algorithm.as_u8() as i32,
                behavior: r.behavior,
                retry_policy: Some(crate::pb::RateLimitRetryPolicy {
                    initial_backoff_ms: r.retry_policy.initial_backoff_ms,
                    max_backoff_ms: r.retry_policy.max_backoff_ms,
                    backoff_multiplier: r.retry_policy.backoff_multiplier,
                    max_retries: r.retry_policy.max_retries,
                }),
            })),
        },
        crate::job::Limit::FloatingConcurrency(f) => Limit {
            limit: Some(limit::Limit::FloatingConcurrency(
                crate::pb::FloatingConcurrencyLimit {
                    key: f.key,
                    default_max_concurrency: f.default_max_concurrency,
                    refresh_interval_ms: f.refresh_interval_ms,
                    metadata: f.metadata.into_iter().collect(),
                },
            )),
        },
    }
}

/// Convert a JobAttemptView to a proto JobAttempt
pub fn job_attempt_view_to_proto(
    attempt: &crate::job_attempt::JobAttemptView,
) -> crate::pb::JobAttempt {
    let state = attempt.state();
    let (status, finished_at_ms, result, error_code, error_data) = match state {
        JobAttemptStatus::Running => (AttemptStatus::Running, None, None, None, None),
        JobAttemptStatus::Succeeded {
            finished_at_ms,
            result,
        } => (
            AttemptStatus::Succeeded,
            Some(finished_at_ms),
            Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(result)),
            }),
            None,
            None,
        ),
        JobAttemptStatus::Failed {
            finished_at_ms,
            error_code,
            error,
        } => (
            AttemptStatus::Failed,
            Some(finished_at_ms),
            None,
            Some(error_code),
            Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(error)),
            }),
        ),
        JobAttemptStatus::Cancelled { finished_at_ms } => (
            AttemptStatus::Cancelled,
            Some(finished_at_ms),
            None,
            None,
            None,
        ),
    };

    crate::pb::JobAttempt {
        job_id: attempt.job_id().to_string(),
        attempt_number: attempt.attempt_number(),
        task_id: attempt.task_id().to_string(),
        status: status.into(),
        started_at_ms: attempt.started_at_ms(),
        finished_at_ms,
        result,
        error_code,
        error_data,
    }
}

/// Extract the result from a list of proto JobAttempts.
/// Returns the result from the last succeeded attempt, if any.
pub fn result_from_proto_attempts(attempts: &[crate::pb::JobAttempt]) -> Option<SerializedBytes> {
    attempts
        .iter()
        .rev()
        .find(|a| a.status == i32::from(AttemptStatus::Succeeded))
        .and_then(|a| a.result.clone())
}
