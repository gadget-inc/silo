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
        parent_epoch_ms: i64,
        task_group: &str,
    ) -> Result<(), JobStoreShardError> {
        // The retry's task_key keeps `retry_at_ms` as its start time (the broker
        // must dispatch it after the backoff). `handle_check_rate_limit`
        // ack-deleted the parent CheckRateLimit, installing a broker tombstone at
        // its task_key; every other key component is identical to the parent's,
        // so we dodge it with an `epoch_ms` strictly past the parent's. This
        // holds even for zero backoff (`retry_at_ms == parent_start_time_ms`).
        let task_key_epoch_ms = rate_limit_retry_epoch_ms(parent_epoch_ms);
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
            retry_at_ms,
            priority,
            job_id,
            attempt_number,
            task_key_epoch_ms,
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

/// Compute the trailing `epoch_ms` for a rate-limit retry's task_key.
///
/// `handle_check_rate_limit` ack-deletes the parent CheckRateLimit's task_key,
/// installing a broker tombstone keyed by the FULL key bytes
/// `(task_group, start_time_ms, priority, job_id, attempt_number, epoch_ms)`.
/// The retry keeps `retry_at_ms` as its start time (so the broker dispatches it
/// after the backoff) and reuses the same task_group/priority/job_id/attempt, so
/// the only field free to disambiguate from the tombstone is `epoch_ms`. One
/// past the parent's epoch makes the dodge unconditional — even for zero backoff
/// where `retry_at_ms == parent_start_time_ms`.
///
/// Extracted as a free function so the invariant can be unit-tested without
/// spinning up a `JobStoreShard`.
pub(crate) fn rate_limit_retry_epoch_ms(parent_epoch_ms: i64) -> i64 {
    parent_epoch_ms + 1
}

#[cfg(test)]
mod tests {
    use super::rate_limit_retry_epoch_ms;

    #[test]
    fn retry_epoch_is_one_past_parent() {
        // The retry's task_key reuses the parent's start_time (and every other
        // component); a strictly-greater epoch dodges the parent's tombstone.
        assert_eq!(rate_limit_retry_epoch_ms(100), 101);
        assert_eq!(rate_limit_retry_epoch_ms(0), 1);
    }

    #[test]
    fn retry_epoch_strictly_increases_across_a_multi_hop_chain() {
        // A rate-limited job can retry many times. Each hop feeds the prior
        // hop's epoch back through this function (handle_check_rate_limit parses
        // the parent CheckRateLimit's key and passes its `epoch_ms`), so the
        // chain's epochs must form a strictly increasing sequence. Under zero
        // backoff every hop reuses the same `start_time`, so this epoch growth is
        // the *only* thing keeping each retry clear of the previous hop's broker
        // ack-tombstone — a single non-increasing step would silently suppress a
        // retry and strand the chain's held_queues.
        let mut epoch = 7; // arbitrary starting epoch from the initial CheckRateLimit
        for _ in 0..10 {
            let next = rate_limit_retry_epoch_ms(epoch);
            assert!(
                next > epoch,
                "retry epoch must strictly exceed the parent's: {next} !> {epoch}",
            );
            epoch = next;
        }
        assert_eq!(epoch, 17, "10 hops from epoch 7 should land at 17");
    }
}
