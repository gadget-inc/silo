use silo::gubernator::{
    GubernatorClient, GubernatorConfig, GubernatorError, MockGubernatorClient,
    NullGubernatorClient, RateLimitClient,
};
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

#[silo::test]
async fn test_null_client_health_check_returns_not_configured() {
    let client = NullGubernatorClient::new();

    let result = client.health_check().await;

    assert!(matches!(result, Err(GubernatorError::NotConfigured)));
}

#[silo::test]
async fn test_mock_reset_key() {
    let client = MockGubernatorClient::new();

    // Use some of the quota for user1
    for _ in 0..5 {
        client
            .check_rate_limit(
                "test-limit",
                "user1",
                1,
                10,
                1000,
                Algorithm::TokenBucket,
                0,
            )
            .await
            .unwrap();
    }

    // Verify user1 has used some quota
    let result = client
        .check_rate_limit(
            "test-limit",
            "user1",
            1,
            10,
            1000,
            Algorithm::TokenBucket,
            0,
        )
        .await
        .unwrap();
    assert_eq!(result.remaining, 4); // 10 - 6 = 4

    // Reset just user1's quota for this limit
    client.reset_key("test-limit", "user1").await;

    // user1 should have full quota again
    let result = client
        .check_rate_limit(
            "test-limit",
            "user1",
            1,
            10,
            1000,
            Algorithm::TokenBucket,
            0,
        )
        .await
        .unwrap();
    assert_eq!(result.remaining, 9); // Full quota minus 1 hit
}

#[silo::test]
async fn test_mock_reset_key_preserves_other_keys() {
    let client = MockGubernatorClient::new();

    // Use quota for both user1 and user2
    for _ in 0..5 {
        client
            .check_rate_limit(
                "test-limit",
                "user1",
                1,
                10,
                1000,
                Algorithm::TokenBucket,
                0,
            )
            .await
            .unwrap();
        client
            .check_rate_limit(
                "test-limit",
                "user2",
                1,
                10,
                1000,
                Algorithm::TokenBucket,
                0,
            )
            .await
            .unwrap();
    }

    // Reset only user1
    client.reset_key("test-limit", "user1").await;

    // user1 should have full quota
    let result = client
        .check_rate_limit(
            "test-limit",
            "user1",
            1,
            10,
            1000,
            Algorithm::TokenBucket,
            0,
        )
        .await
        .unwrap();
    assert_eq!(result.remaining, 9);

    // user2 should still have reduced quota
    let result = client
        .check_rate_limit(
            "test-limit",
            "user2",
            1,
            10,
            1000,
            Algorithm::TokenBucket,
            0,
        )
        .await
        .unwrap();
    assert_eq!(result.remaining, 4); // 10 - 6 = 4
}

#[silo::test]
async fn test_mock_health_check() {
    let client = MockGubernatorClient::new();

    let result = client.health_check().await.unwrap();

    assert_eq!(result.0, "healthy");
    assert_eq!(result.1, 1);
}

#[silo::test]
async fn test_mock_multiple_hits() {
    let client = MockGubernatorClient::new();

    // Single request consuming 5 hits at once
    let result = client
        .check_rate_limit("test", "user1", 5, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(result.under_limit);
    assert_eq!(result.remaining, 5); // 10 - 5 = 5

    // Another request consuming 5 more hits
    let result = client
        .check_rate_limit("test", "user1", 5, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(result.under_limit);
    assert_eq!(result.remaining, 0); // 10 - 10 = 0

    // Any more should be over limit
    let result = client
        .check_rate_limit("test", "user1", 1, 10, 1000, Algorithm::TokenBucket, 0)
        .await
        .unwrap();

    assert!(!result.under_limit);
}

#[silo::test]
async fn test_gubernator_client_not_connected_error() {
    // Create client but don't connect
    let config = GubernatorConfig {
        address: "http://localhost:99999".to_string(), // Invalid port
        coalesce_interval_ms: 5,
        max_batch_size: 100,
        connect_timeout_ms: 100, // Short timeout
        request_timeout_ms: 100,
    };

    let client = GubernatorClient::new(config);

    // Trying to use it without connecting should fail
    let result = client.is_connected().await;
    assert!(!result);
}
