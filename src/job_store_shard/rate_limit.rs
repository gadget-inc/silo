//! Rate limit checking operations via Gubernator.

use slatedb::WriteBatch;
use uuid::Uuid;

use crate::gubernator::{GubernatorError, RateLimitResult};
use crate::job_store_shard::helpers::put_task;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::task::{GubernatorRateLimitData, Task};

impl JobStoreShard {
    /// Schedule a rate limit check retry task
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn schedule_rate_limit_retry(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
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
        task_group: &str,
    ) -> Result<(), JobStoreShardError> {
        let retry_task = Task::CheckRateLimit {
            task_id: Uuid::new_v4().to_string(),
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
            batch,
            task_group,
            retry_at_ms,
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
