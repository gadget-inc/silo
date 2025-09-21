use silo::retry::{RetryPolicy, next_retry_time_ms};

#[test]
fn next_retry_no_retries_returns_none() {
    let policy = RetryPolicy {
        retry_count: Some(0),
        initial_interval_ms: Some(1000),
        max_interval_ms: None,
        randomize_interval: None,
        backoff_factor: None,
    };
    assert_eq!(next_retry_time_ms(1_000_000, 0, &policy), None);
}

#[test]
fn next_retry_basic_exponential() {
    let policy = RetryPolicy {
        retry_count: Some(5),
        initial_interval_ms: Some(1000),
        max_interval_ms: None,
        randomize_interval: Some(false),
        backoff_factor: Some(2.0),
    };
    // first failure (n=0) -> +1000ms
    assert_eq!(next_retry_time_ms(1_000_000, 0, &policy), Some(1_001_000));
    // second failure (n=1) -> +2000ms
    assert_eq!(next_retry_time_ms(1_000_000, 1, &policy), Some(1_002_000));
    // third failure (n=2) -> +4000ms
    assert_eq!(next_retry_time_ms(1_000_000, 2, &policy), Some(1_004_000));
}

#[test]
fn next_retry_caps_at_max_interval() {
    let policy = RetryPolicy {
        retry_count: Some(10),
        initial_interval_ms: Some(1_000),
        max_interval_ms: Some(2_000),
        randomize_interval: Some(false),
        backoff_factor: Some(10.0),
    };
    // n=1 would be 10_000ms but capped at 2_000ms
    assert_eq!(next_retry_time_ms(100, 1, &policy), Some(2_100));
}


