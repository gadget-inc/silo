use silo::gubernator::{GubernatorError, MockGubernatorClient, NullGubernatorClient, RateLimitClient};
use silo::pb::gubernator::Algorithm;

#[silo::test]
async fn test_mock_rate_limit_under_limit() {
    let client = MockGubernatorClient::new();

    let result = client
        .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(result.under_limit);
    assert_eq!(result.limit, 10);
    assert_eq!(result.remaining, 9);
}

#[silo::test]
async fn test_mock_rate_limit_over_limit() {
    let client = MockGubernatorClient::new();

    // Exhaust the limit
    for _ in 0..10 {
        client
            .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
            .await
            .unwrap();
    }

    // This should be over limit
    let result = client
        .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(!result.under_limit);
    assert_eq!(result.remaining, 0);
}

#[silo::test]
async fn test_mock_rate_limit_reset() {
    let client = MockGubernatorClient::new();

    // Use some of the limit
    for _ in 0..5 {
        client
            .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
            .await
            .unwrap();
    }

    // Reset
    client.reset().await;

    // Should be back to full
    let result = client
        .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(result.under_limit);
    assert_eq!(result.remaining, 9);
}

#[silo::test]
async fn test_mock_different_keys_independent() {
    let client = MockGubernatorClient::new();

    // Exhaust limit for user1
    for _ in 0..10 {
        client
            .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
            .await
            .unwrap();
    }

    // user2 should still have full quota
    let result = client
        .check_rate_limit("test", "user2", 1, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(result.under_limit);
    assert_eq!(result.remaining, 9);
}

#[silo::test]
async fn test_null_client_returns_error() {
    let client = NullGubernatorClient::new();

    let result = client
        .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
        .await;

    assert!(matches!(result, Err(GubernatorError::NotConfigured)));
}

