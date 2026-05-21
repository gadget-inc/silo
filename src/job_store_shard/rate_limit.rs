//! Rate limit checking operations via Gubernator.

use crate::gubernator::{GubernatorError, RateLimitResult};
use crate::job_store_shard::helpers::{WriteBatcher, put_task};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::task::{GubernatorRateLimitData, Task};

impl JobStoreShard {
    /// Schedule a rate limit check retry task.
    ///
    /// `task_id` is the chain's continuing task_id (carried from the
    /// CheckRateLimit being retried). It MUST be reused — not regenerated —
    /// because every prior holder this chain accumulated is keyed by that
    /// task_id. Allocating a fresh UUID here orphans those holders: the
    /// terminal RunAttempt later writes under the new id, the worker's
    /// completion releases holders under the new id, and the original
    /// holders are never reachable again.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn schedule_rate_limit_retry<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        task_id: &str,
        job_id: &str,
        attempt_number: u32,
        relative_attempt_number: u32,
        limit_index: u32,
        rate_limit: &GubernatorRateLimitData,
        retry_count: u32,
        started_at_ms: i64,
        priority: u8,
        held_queues: &[String],
        retry_at_ms: i64,
        parent_start_time_ms: i64,
        task_group: &str,
    ) -> Result<(), JobStoreShardError> {
        let task_key_start_ms =
            rate_limit_retry_task_key_start_ms(retry_at_ms, parent_start_time_ms);
        let retry_task = Task::CheckRateLimit {
            task_id: task_id.to_string(),
            tenant: tenant.to_string(),
            job_id: job_id.to_string(),
            attempt_number,
            relative_attempt_number,
            limit_index,
            rate_limit: rate_limit.clone(),
            retry_count: retry_count + 1,
            started_at_ms,
            priority,
            held_queues: held_queues.to_vec(),
            task_group: task_group.to_string(),
        };
        put_task(
            writer,
            task_group,
            task_key_start_ms,
            priority,
            job_id,
            attempt_number,
            &retry_task,
        )
    }

    /// Check a rate limit via the rate limit client
    pub(crate) async fn check_gubernator_rate_limit(
        &self,
        rate_limit: &GubernatorRateLimitData,
    ) -> Result<RateLimitResult, GubernatorError> {
        use crate::pb::gubernator::Algorithm;
        let algorithm = match rate_limit.algorithm {
            0 => Algorithm::TokenBucket,
            1 => Algorithm::LeakyBucket,
            _ => Algorithm::TokenBucket,
        };

        self.rate_limiter
            .check_rate_limit(
                &rate_limit.name,
                &rate_limit.unique_key,
                rate_limit.hits as i64,
                rate_limit.limit,
                rate_limit.duration_ms,
                algorithm,
                rate_limit.behavior,
            )
            .await
    }

    /// Calculate the backoff time for a rate limit retry
    pub(crate) fn calculate_rate_limit_backoff(
        &self,
        rate_limit: &GubernatorRateLimitData,
        retry_count: u32,
        reset_time_ms: i64,
        now_ms: i64,
    ) -> i64 {
        // If we have a reset time from Gubernator, use it if it's reasonable
        if reset_time_ms > now_ms && reset_time_ms < now_ms + rate_limit.retry_max_backoff_ms {
            return reset_time_ms;
        }

        // Otherwise use exponential backoff
        let base = rate_limit.retry_initial_backoff_ms as f64;
        let multiplier = rate_limit.retry_backoff_multiplier;
        let backoff = (base * multiplier.powi(retry_count as i32)).round() as i64;
        let capped = backoff.min(rate_limit.retry_max_backoff_ms);

        now_ms + capped
    }
}

/// Compute the `task_key_start_ms` for a rate-limit retry given the
/// caller's requested `retry_at_ms` and the parent CheckRateLimit's
/// `start_time_ms` (the value baked into the just-tombstoned task_key).
///
/// `handle_check_rate_limit` ack-deletes the parent CheckRateLimit's
/// task_key, which installs a broker tombstone keyed by
/// `(task_group, parent_start_time_ms, priority, job_id, attempt_number)`.
/// All other components of the retry's task_key are identical to the
/// parent's; the only differentiator is `start_time_ms`. If
/// `retry_at_ms == parent_start_time_ms` — possible with zero/near-zero
/// backoff, or with `reset_time_ms == parent.start_time_ms` — the retry
/// write would land on the tombstoned key and be silently suppressed,
/// stalling the chain and stranding every `held_queues` entry. Bumping
/// past the parent by one millisecond costs nothing on the broker's
/// dispatch ordering and makes the dodge unconditional.
///
/// Extracted as a free function so the invariant can be unit-tested
/// without spinning up a `JobStoreShard`.
pub(crate) fn rate_limit_retry_task_key_start_ms(
    retry_at_ms: i64,
    parent_start_time_ms: i64,
) -> i64 {
    retry_at_ms.max(parent_start_time_ms + 1)
}

#[cfg(test)]
mod tests {
    use super::rate_limit_retry_task_key_start_ms;

    #[test]
    fn equal_retry_and_parent_bumps_by_one() {
        // The exact collision case: zero backoff with the dequeue happening
        // in the same millisecond as the parent's start_time_ms.
        assert_eq!(rate_limit_retry_task_key_start_ms(100, 100), 101);
    }

    #[test]
    fn retry_before_parent_jumps_to_parent_plus_one() {
        // Clock skew or a Gubernator reset_time pointing at an earlier
        // millisecond than the parent's start. Still must dodge the
        // tombstone.
        assert_eq!(rate_limit_retry_task_key_start_ms(50, 100), 101);
    }

    #[test]
    fn retry_strictly_after_parent_is_unchanged() {
        // The common case: backoff > 0 carries retry_at_ms past the parent.
        // No bump needed.
        assert_eq!(rate_limit_retry_task_key_start_ms(200, 100), 200);
    }

    #[test]
    fn retry_one_past_parent_is_unchanged() {
        // Boundary: retry_at_ms is exactly parent + 1. The `max` returns
        // retry_at_ms unchanged; no double-bump.
        assert_eq!(rate_limit_retry_task_key_start_ms(101, 100), 101);
    }
}
