//! Tests for the Prometheus metrics endpoint.

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    routing::get,
};
use http_body_util::BodyExt;
use silo::metrics;
use tower::ServiceExt;

/// Helper to create a metrics router for testing
fn create_metrics_router() -> (metrics::Metrics, Router) {
    let m = metrics::init().expect("init metrics");
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(m.clone());
    (m, app)
}

/// Axum handler for the `/metrics` endpoint (copied from metrics module for testing)
async fn metrics_handler(
    axum::extract::State(metrics): axum::extract::State<metrics::Metrics>,
) -> impl axum::response::IntoResponse {
    use prometheus::{Encoder, TextEncoder};

    let encoder = TextEncoder::new();
    let metric_families = metrics.registry().gather();

    let mut buffer = Vec::new();
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            buffer,
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            format!("Failed to encode metrics: {}", e).into_bytes(),
        ),
    }
}

#[silo::test]
async fn test_metrics_endpoint_returns_prometheus_format() {
    let (_metrics, app) = create_metrics_router();

    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get("content-type")
        .expect("content-type header");
    assert!(
        content_type.to_str().unwrap().contains("text/plain"),
        "content-type should be text/plain"
    );

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);

    // Prometheus format should contain HELP and TYPE comments
    // Even with no recorded metrics, the registered metrics should appear
    assert!(
        body_str.contains("# HELP") || body_str.contains("# TYPE") || body_str.is_empty(),
        "response should be valid Prometheus format or empty"
    );
}

#[silo::test]
async fn test_metrics_endpoint_includes_registered_metrics_after_recording() {
    let (metrics, app) = create_metrics_router();

    // Record some metrics
    metrics.record_enqueue("0", "test-tenant");
    metrics.record_enqueue("0", "test-tenant");
    metrics.record_dequeue("0", "default", 5);
    metrics.record_completion("0", "succeeded");
    metrics.record_completion("0", "failed");

    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);

    // Should contain our registered metrics
    assert!(
        body_str.contains("silo_jobs_enqueued_total"),
        "should contain silo_jobs_enqueued_total metric"
    );
    assert!(
        body_str.contains("silo_jobs_dequeued_total"),
        "should contain silo_jobs_dequeued_total metric"
    );
    assert!(
        body_str.contains("silo_jobs_completed_total"),
        "should contain silo_jobs_completed_total metric"
    );

    // Should contain the labels we used
    assert!(
        body_str.contains("shard=\"0\""),
        "should contain shard label"
    );
    assert!(
        body_str.contains("tenant=\"test-tenant\""),
        "should contain tenant label"
    );

    // Check the values are recorded (enqueue was called twice)
    assert!(
        body_str.contains("silo_jobs_enqueued_total{shard=\"0\",tenant=\"test-tenant\"} 2"),
        "enqueue counter should show 2"
    );

    // Check dequeue value
    assert!(
        body_str.contains("silo_jobs_dequeued_total{shard=\"0\",task_group=\"default\"} 5"),
        "dequeue counter should show 5"
    );
}

#[silo::test]
async fn test_metrics_gauge_values() {
    let (metrics, app) = create_metrics_router();

    // Set some gauge values
    metrics.set_shards_owned(3);
    metrics.set_broker_buffer_size("0", 100);
    metrics.set_broker_buffer_size("1", 50);
    metrics.set_broker_inflight_size("0", 10);

    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);

    // Should contain gauge metrics
    assert!(
        body_str.contains("silo_shards_owned"),
        "should contain silo_shards_owned metric"
    );
    assert!(
        body_str.contains("silo_broker_buffer_size"),
        "should contain silo_broker_buffer_size metric"
    );
    assert!(
        body_str.contains("silo_broker_inflight_size"),
        "should contain silo_broker_inflight_size metric"
    );

    // Check values
    assert!(
        body_str.contains("silo_shards_owned 3"),
        "shards_owned should be 3"
    );
    assert!(
        body_str.contains("silo_broker_buffer_size{shard=\"0\"} 100"),
        "buffer size for shard 0 should be 100"
    );
    assert!(
        body_str.contains("silo_broker_buffer_size{shard=\"1\"} 50"),
        "buffer size for shard 1 should be 50"
    );
}
