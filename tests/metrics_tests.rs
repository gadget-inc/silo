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
    metrics.set_broker_buffer_size("0", "default", 100);
    metrics.set_broker_buffer_size("1", "default", 50);
    metrics.set_broker_inflight_size("0", "default", 10);

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
    // Use find_line helper since label order may vary
    let find_line = |substrings: &[&str]| -> Option<String> {
        body_str
            .lines()
            .find(|l| substrings.iter().all(|s| l.contains(s)))
            .map(|s| s.to_string())
    };

    let buf0 = find_line(&[
        "silo_broker_buffer_size",
        "shard=\"0\"",
        "task_group=\"default\"",
    ]);
    assert!(
        buf0.as_ref().map_or(false, |l| l.ends_with(" 100")),
        "buffer size for shard 0 should be 100, found: {:?}",
        buf0
    );
    let buf1 = find_line(&[
        "silo_broker_buffer_size",
        "shard=\"1\"",
        "task_group=\"default\"",
    ]);
    assert!(
        buf1.as_ref().map_or(false, |l| l.ends_with(" 50")),
        "buffer size for shard 1 should be 50, found: {:?}",
        buf1
    );
}

#[silo::test]
async fn test_metrics_all_recording_methods() {
    let (metrics, app) = create_metrics_router();

    // record_attempt with retry and non-retry
    metrics.record_attempt("0", "default", false);
    metrics.record_attempt("0", "default", true);
    metrics.record_attempt("0", "default", true);

    // record_grpc_request
    metrics.record_grpc_request("Enqueue", "OK");
    metrics.record_grpc_request("Dequeue", "INTERNAL");

    // record_grpc_duration
    metrics.record_grpc_duration("Enqueue", 0.05);

    // record_job_wait_time
    metrics.record_job_wait_time("0", "default", 1.5);

    // record_ready_to_start_latency_ms
    metrics.record_ready_to_start_latency_ms("0", "default", 42.0);

    // record_broker_scan_duration
    metrics.record_broker_scan_duration("0", 0.01);

    // inc/dec_task_leases_active
    metrics.inc_task_leases_active("0", "default");
    metrics.inc_task_leases_active("0", "default");
    metrics.dec_task_leases_active("0", "default");

    // record_concurrency_ticket_granted
    metrics.record_concurrency_ticket_granted();

    // set_coordination_shards_open
    metrics.set_coordination_shards_open(5);

    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);

    // Helper: find a metric line that contains all given substrings
    let find_line = |substrings: &[&str]| -> Option<String> {
        body_str
            .lines()
            .find(|l| substrings.iter().all(|s| l.contains(s)))
            .map(|s| s.to_string())
    };

    // Verify attempt counter with is_retry labels
    let non_retry_line = find_line(&[
        "silo_job_attempts_total",
        "is_retry=\"false\"",
        "shard=\"0\"",
    ]);
    assert!(
        non_retry_line.as_ref().map_or(false, |l| l.ends_with(" 1")),
        "should have 1 non-retry attempt, found: {:?}",
        non_retry_line
    );
    let retry_line = find_line(&[
        "silo_job_attempts_total",
        "is_retry=\"true\"",
        "shard=\"0\"",
    ]);
    assert!(
        retry_line.as_ref().map_or(false, |l| l.ends_with(" 2")),
        "should have 2 retry attempts, found: {:?}",
        retry_line
    );

    // Verify gRPC request counter
    let grpc_enqueue = find_line(&[
        "silo_grpc_requests_total",
        "method=\"Enqueue\"",
        "status=\"OK\"",
    ]);
    assert!(
        grpc_enqueue.as_ref().map_or(false, |l| l.ends_with(" 1")),
        "should have 1 Enqueue/OK request, found: {:?}",
        grpc_enqueue
    );
    let grpc_dequeue = find_line(&[
        "silo_grpc_requests_total",
        "method=\"Dequeue\"",
        "status=\"INTERNAL\"",
    ]);
    assert!(
        grpc_dequeue.as_ref().map_or(false, |l| l.ends_with(" 1")),
        "should have 1 Dequeue/INTERNAL request, found: {:?}",
        grpc_dequeue
    );

    // Verify gRPC duration histogram was recorded
    assert!(
        body_str.contains("silo_grpc_request_duration_seconds"),
        "should contain gRPC duration metric"
    );

    // Verify job wait time histogram was recorded
    assert!(
        body_str.contains("silo_job_wait_time_seconds"),
        "should contain job wait time metric"
    );

    // Verify ready-to-start latency histogram was recorded
    assert!(
        body_str.contains("silo_ready_to_start_latency_ms"),
        "should contain ready-to-start latency metric"
    );

    // Verify broker scan duration
    assert!(
        body_str.contains("silo_broker_scan_duration_seconds"),
        "should contain broker scan duration metric"
    );

    // Verify task leases active (inc twice, dec once = 1)
    let leases_line = find_line(&[
        "silo_task_leases_active",
        "shard=\"0\"",
        "task_group=\"default\"",
    ]);
    assert!(
        leases_line.as_ref().map_or(false, |l| l.ends_with(" 1")),
        "task leases active should be 1 after 2 inc and 1 dec, found: {:?}",
        leases_line
    );

    // Verify concurrency tickets granted
    assert!(
        body_str.contains("silo_concurrency_tickets_granted_total 1"),
        "concurrency tickets granted should be 1"
    );

    // Verify coordination shards open
    assert!(
        body_str.contains("silo_coordination_shards_open 5"),
        "coordination shards open should be 5"
    );
}

#[silo::test]
async fn test_metrics_slatedb_stats() {
    let (metrics, _app) = create_metrics_router();

    // Create a real SlateDB to get a StatRegistry with actual stats
    let tmpdir = tempfile::tempdir().unwrap();
    let object_store = std::sync::Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(tmpdir.path()).unwrap(),
    );
    let db = slatedb::Db::open(object_store::path::Path::from("test-db"), object_store)
        .await
        .unwrap();

    // Do some writes to populate stats
    db.put(b"key1", b"value1").await.unwrap();
    db.put(b"key2", b"value2").await.unwrap();
    let _ = db.get(b"key1").await;

    let stat_registry = db.metrics();
    metrics.update_slatedb_stats("0", &stat_registry);

    let body_str = gather_metrics_text(&metrics);

    // Counter-type metrics should be emitted as TYPE counter
    assert!(
        body_str.contains("# TYPE silo_slatedb_write_ops_total counter"),
        "write_ops should be a counter type"
    );
    assert!(
        body_str.contains("# TYPE silo_slatedb_get_requests_total counter"),
        "get_requests should be a counter type"
    );
    assert!(
        body_str.contains("# TYPE silo_slatedb_write_batch_count_total counter"),
        "write_batch_count should be a counter type"
    );

    // Gauge-type metrics should be emitted as TYPE gauge
    assert!(
        body_str.contains("# TYPE silo_slatedb_wal_buffer_estimated_bytes gauge"),
        "wal_buffer_estimated_bytes should be a gauge type"
    );
    assert!(
        body_str.contains("# TYPE silo_slatedb_running_compactions gauge"),
        "running_compactions should be a gauge type"
    );

    // Verify counter values are non-zero after writes and reads
    let find_line = |substrings: &[&str]| -> Option<String> {
        body_str
            .lines()
            .find(|l| substrings.iter().all(|s| l.contains(s)))
            .map(|s| s.to_string())
    };

    let write_ops_line = find_line(&["silo_slatedb_write_ops_total", "shard=\"0\""]);
    assert!(
        write_ops_line
            .as_ref()
            .map_or(false, |l| !l.ends_with(" 0")),
        "write_ops counter should be > 0 after puts, found: {:?}",
        write_ops_line
    );

    let get_requests_line = find_line(&["silo_slatedb_get_requests_total", "shard=\"0\""]);
    assert!(
        get_requests_line
            .as_ref()
            .map_or(false, |l| !l.ends_with(" 0")),
        "get_requests counter should be > 0 after get, found: {:?}",
        get_requests_line
    );

    db.close().await.unwrap();
}

#[silo::test]
async fn test_metrics_slatedb_counter_delta_tracking() {
    let (metrics, _app) = create_metrics_router();

    let tmpdir = tempfile::tempdir().unwrap();
    let object_store = std::sync::Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(tmpdir.path()).unwrap(),
    );
    let db = slatedb::Db::open(object_store::path::Path::from("test-db"), object_store)
        .await
        .unwrap();

    // First batch of writes
    db.put(b"key1", b"value1").await.unwrap();
    let stat_registry = db.metrics();
    metrics.update_slatedb_stats("0", &stat_registry);

    let body1 = gather_metrics_text(&metrics);
    let write_ops_1 =
        extract_metric_value(&body1, &["silo_slatedb_write_ops_total", "shard=\"0\""]);
    assert!(write_ops_1 > 0.0, "write_ops should be > 0 after first put");

    // Second batch of writes — delta tracking should accumulate, not reset
    db.put(b"key2", b"value2").await.unwrap();
    db.put(b"key3", b"value3").await.unwrap();
    metrics.update_slatedb_stats("0", &stat_registry);

    let body2 = gather_metrics_text(&metrics);
    let write_ops_2 =
        extract_metric_value(&body2, &["silo_slatedb_write_ops_total", "shard=\"0\""]);
    assert!(
        write_ops_2 > write_ops_1,
        "write_ops should increase after more puts: {} -> {}",
        write_ops_1,
        write_ops_2
    );

    // Calling update again without new writes should not change the counter
    metrics.update_slatedb_stats("0", &stat_registry);
    let body3 = gather_metrics_text(&metrics);
    let write_ops_3 =
        extract_metric_value(&body3, &["silo_slatedb_write_ops_total", "shard=\"0\""]);
    assert_eq!(
        write_ops_2, write_ops_3,
        "write_ops should not change when no new writes occurred"
    );

    db.close().await.unwrap();
}

#[silo::test]
async fn test_metrics_slatedb_multiple_shards() {
    let (metrics, _app) = create_metrics_router();

    let tmpdir1 = tempfile::tempdir().unwrap();
    let object_store1 = std::sync::Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(tmpdir1.path()).unwrap(),
    );
    let db1 = slatedb::Db::open(object_store::path::Path::from("test-db"), object_store1)
        .await
        .unwrap();

    let tmpdir2 = tempfile::tempdir().unwrap();
    let object_store2 = std::sync::Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(tmpdir2.path()).unwrap(),
    );
    let db2 = slatedb::Db::open(object_store::path::Path::from("test-db"), object_store2)
        .await
        .unwrap();

    // Write different amounts to each shard
    db1.put(b"a", b"1").await.unwrap();
    db2.put(b"b", b"2").await.unwrap();
    db2.put(b"c", b"3").await.unwrap();

    metrics.update_slatedb_stats("shard-a", &db1.metrics());
    metrics.update_slatedb_stats("shard-b", &db2.metrics());

    let body = gather_metrics_text(&metrics);
    let shard_a_writes = extract_metric_value(
        &body,
        &["silo_slatedb_write_ops_total", "shard=\"shard-a\""],
    );
    let shard_b_writes = extract_metric_value(
        &body,
        &["silo_slatedb_write_ops_total", "shard=\"shard-b\""],
    );

    assert!(shard_a_writes > 0.0, "shard-a should have writes");
    assert!(shard_b_writes > 0.0, "shard-b should have writes");
    assert!(
        shard_b_writes > shard_a_writes,
        "shard-b should have more writes than shard-a: {} vs {}",
        shard_b_writes,
        shard_a_writes
    );

    db1.close().await.unwrap();
    db2.close().await.unwrap();
}

#[silo::test]
async fn test_metrics_ready_to_start_latency() {
    let (metrics, _app) = create_metrics_router();

    // Record latency observations for different shards and task groups
    metrics.record_ready_to_start_latency_ms("0", "default", 5.0);
    metrics.record_ready_to_start_latency_ms("0", "default", 150.0);
    metrics.record_ready_to_start_latency_ms("0", "high-priority", 2.0);
    metrics.record_ready_to_start_latency_ms("1", "default", 1000.0);

    let body_str = gather_metrics_text(&metrics);

    // Should contain TYPE and HELP metadata
    assert!(
        body_str.contains("# TYPE silo_ready_to_start_latency_ms histogram"),
        "should be a histogram type"
    );
    assert!(
        body_str.contains("# HELP silo_ready_to_start_latency_ms"),
        "should have a HELP description"
    );

    let find_line = |substrings: &[&str]| -> Option<String> {
        body_str
            .lines()
            .find(|l| substrings.iter().all(|s| l.contains(s)))
            .map(|s| s.to_string())
    };

    // Verify count for shard 0, default task group (2 observations)
    let count_line = find_line(&[
        "silo_ready_to_start_latency_ms_count",
        "shard=\"0\"",
        "task_group=\"default\"",
    ]);
    assert!(
        count_line.as_ref().map_or(false, |l| l.ends_with(" 2")),
        "should have 2 observations for shard 0 default, found: {:?}",
        count_line
    );

    // Verify sum for shard 0, default task group (5.0 + 150.0 = 155.0)
    let sum_line = find_line(&[
        "silo_ready_to_start_latency_ms_sum",
        "shard=\"0\"",
        "task_group=\"default\"",
    ]);
    assert!(
        sum_line.as_ref().map_or(false, |l| l.ends_with(" 155")),
        "sum should be 155 for shard 0 default, found: {:?}",
        sum_line
    );

    // Verify count for shard 0, high-priority task group (1 observation)
    let hp_count_line = find_line(&[
        "silo_ready_to_start_latency_ms_count",
        "shard=\"0\"",
        "task_group=\"high-priority\"",
    ]);
    assert!(
        hp_count_line.as_ref().map_or(false, |l| l.ends_with(" 1")),
        "should have 1 observation for shard 0 high-priority, found: {:?}",
        hp_count_line
    );

    // Verify shard 1 is tracked separately
    let shard1_count = find_line(&[
        "silo_ready_to_start_latency_ms_count",
        "shard=\"1\"",
        "task_group=\"default\"",
    ]);
    assert!(
        shard1_count.as_ref().map_or(false, |l| l.ends_with(" 1")),
        "should have 1 observation for shard 1 default, found: {:?}",
        shard1_count
    );
}

/// Gather metrics text from the Prometheus registry.
fn gather_metrics_text(metrics: &metrics::Metrics) -> String {
    use prometheus::{Encoder, TextEncoder};
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry().gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Extract a numeric metric value from Prometheus text output, finding the line
/// that contains all given substrings.
fn extract_metric_value(body: &str, substrings: &[&str]) -> f64 {
    let line = body
        .lines()
        .find(|l| !l.starts_with('#') && substrings.iter().all(|s| l.contains(s)))
        .unwrap_or_else(|| panic!("metric line not found for substrings {:?}", substrings));
    line.rsplit_once(' ')
        .unwrap_or_else(|| panic!("no space-separated value in line: {}", line))
        .1
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("failed to parse metric value from line: {}", line))
}
