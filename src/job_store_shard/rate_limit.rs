//! Rate limit checking operations via Gubernator.

use uuid::Uuid;

use crate::codec::encode_check_rate_limit_retry_from_task;
use crate::flatbuf::silo_internal as fb;
use crate::gubernator::{GubernatorError, RateLimitResult};
use crate::job_store_shard::helpers::WriteBatcher;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::task_key;
use crate::task::GubernatorRateLimitData;

impl JobStoreShard {
    /// Schedule a rate-limit retry from a decoded task without materializing `Task::CheckRateLimit`.
    pub(crate) fn schedule_rate_limit_retry_from_task<W: WriteBatcher>(
        &self,
        writer: &mut W,
        task: fb::TaskCheckRateLimit<'_>,
        retry_at_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let retry_task_id = Uuid::new_v4().to_string();
        let value =
            encode_check_rate_limit_retry_from_task(task, &retry_task_id, task.retry_count() + 1)?;
        writer.put(
            task_key(
                task.task_group().unwrap_or_default(),
                retry_at_ms,
                task.priority(),
                task.job_id().unwrap_or_default(),
                task.attempt_number(),
            ),
            &value,
        )?;
        Ok(())
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
