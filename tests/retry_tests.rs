use silo::retry::{RetryPolicy, next_retry_time_ms};

#[silo::test]
fn next_retry_no_retries_returns_none() {
    let policy = RetryPolicy {
        retry_count: 0,
        initial_interval_ms: 1000,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 2.0,
    };
    assert_eq!(next_retry_time_ms(1_000_000, 1, &policy), None);
}

#[silo::test]
fn next_retry_basic_exponential() {
    let policy = RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 1000,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 2.0,
    };
    // first failure -> +1000ms
    assert_eq!(next_retry_time_ms(1_000_000, 1, &policy), Some(1_001_000));
    // second failure -> +2000ms
    assert_eq!(next_retry_time_ms(1_000_000, 2, &policy), Some(1_002_000));
    // third failure -> +4000ms
    assert_eq!(next_retry_time_ms(1_000_000, 3, &policy), Some(1_004_000));
}

#[silo::test]
fn next_retry_first_failure_waits_initial_interval() {
    let policy = RetryPolicy {
        retry_count: 2,
        initial_interval_ms: 1000,
        max_interval_ms: i64::MAX,
        randomize_interval: false,
        backoff_factor: 3.0,
    };
    assert_eq!(next_retry_time_ms(1_000_000, 1, &policy), Some(1_001_000));
    assert_eq!(next_retry_time_ms(1_000_000, 2, &policy), Some(1_003_000));
    // 2 retries means 3 attempts: the third failure schedules nothing
    assert_eq!(next_retry_time_ms(1_000_000, 3, &policy), None);
}

#[silo::test]
fn next_retry_caps_at_max_interval() {
    let policy = RetryPolicy {
        retry_count: 10,
        initial_interval_ms: 1_000,
        max_interval_ms: 2_000,
        randomize_interval: false,
        backoff_factor: 10.0,
    };
    // the second failure would wait 10_000ms but is capped at 2_000ms
    assert_eq!(next_retry_time_ms(100, 2, &policy), Some(2_100));
}

#[silo::test]
fn next_retry_randomized_is_deterministic_for_given_inputs() {
    let policy = RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 10,
        max_interval_ms: 1_000_000,
        randomize_interval: true,
        backoff_factor: 2.0,
    };
    let a = next_retry_time_ms(1_234_567, 1, &policy);
    let b = next_retry_time_ms(1_234_567, 1, &policy);
    assert_eq!(
        a, b,
        "randomized next time should be deterministic for same inputs"
    );
}
