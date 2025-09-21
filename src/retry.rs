use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Retry policy for a job's attempts
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct RetryPolicy {
    pub retry_count: Option<u32>,
    pub initial_interval_ms: Option<i64>,
    pub max_interval_ms: Option<i64>,
    pub randomize_interval: Option<bool>,
    pub backoff_factor: Option<f64>,
}

impl RetryPolicy {
    pub fn default_initial_interval_ms() -> i64 {
        1_000
    }
    pub fn default_backoff_factor() -> f64 {
        2.0
    }
    pub fn default_randomize() -> bool {
        false
    }
}

/// Compute the time (epoch ms) for the next retry attempt, if any.
/// - `failure_time_ms`: time the last attempt failed
/// - `failures_so_far`: number of failed attempts so far (0 for first failure)
/// - `policy`: retry configuration
/// Returns Some(next_time_ms) or None if no more retries should be attempted.
pub fn next_retry_time_ms(
    failure_time_ms: i64,
    failures_so_far: u32,
    policy: &RetryPolicy,
) -> Option<i64> {
    let retries_allowed = policy.retry_count.unwrap_or(0);
    if failures_so_far >= retries_allowed {
        return None;
    }

    let initial = policy
        .initial_interval_ms
        .unwrap_or_else(RetryPolicy::default_initial_interval_ms);
    let factor = policy
        .backoff_factor
        .unwrap_or_else(RetryPolicy::default_backoff_factor);
    let max_interval = policy.max_interval_ms.unwrap_or(i64::MAX);
    let randomize = policy
        .randomize_interval
        .unwrap_or_else(RetryPolicy::default_randomize);

    // Exponential backoff similar to npm `retry`: delay_n = initial * factor^n
    // where n = failures_so_far (0-based)
    let mut delay = (initial as f64 * factor.powi(failures_so_far as i32)).round() as i64;
    if randomize {
        // Deterministic pseudo-random multiplier in [1.0, 2.0)
        let mut seed = (failure_time_ms as u64) ^ ((failures_so_far as u64) << 32);
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let frac = ((seed >> 32) as f64) / (u32::MAX as f64); // [0,1)
        let multiplier = 1.0 + frac; // [1,2)
        delay = (delay as f64 * multiplier).round() as i64;
    }
    if delay > max_interval {
        delay = max_interval;
    }
    Some(failure_time_ms.saturating_add(delay))
}
