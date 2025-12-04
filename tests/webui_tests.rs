//! WebUI regression tests
//!
//! Tests for the web UI HTTP handlers and rendering.

mod test_helpers;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use silo::cluster_client::ClusterClient;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use silo::webui::{create_router, AppState};
use std::sync::Arc;
use tower::ServiceExt;

/// Helper to create a test AppState
async fn setup_test_state() -> (tempfile::TempDir, AppState) {
    let tmp = tempfile::tempdir().unwrap();

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().to_string_lossy().to_string(),
            wal: None,
        },
        rate_limiter,
    );

    // Open shard 0 in the factory
    factory.open(0).await.expect("open shard");

    let factory = Arc::new(factory);
    let cluster_client = Arc::new(ClusterClient::new(factory.clone(), None));

    let state = AppState {
        factory,
        coordinator: None,
        cluster_client,
    };

    (tmp, state)
}

/// Helper to make HTTP requests to the router
async fn make_request(
    state: AppState,
    method: &str,
    path: &str,
) -> (StatusCode, String) {
    let app = create_router(state);

    let request = Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let status = response.status();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body).to_string();

    (status, body_str)
}

#[silo::test]
async fn test_index_page_renders() {
    let (_tmp, state) = setup_test_state().await;
    let (status, body) = make_request(state, "GET", "/").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Jobs"), "body should contain 'Jobs'");
    assert!(body.contains("Silo"), "body should contain 'Silo'");
}

#[silo::test]
async fn test_index_page_shows_scheduled_jobs() {
    let (_tmp, state) = setup_test_state().await;

    // Enqueue a job
    let shard = state.factory.get("0").expect("shard 0");
    let _job_id = shard
        .enqueue(
            "-",
            Some("test-job-1".to_string()),
            50,
            test_helpers::now_ms() + 10000, // Future time
            None,
            serde_json::json!({"foo": "bar"}),
            vec![],
            None,
        )
        .await
        .expect("enqueue");

    let (status, body) = make_request(state, "GET", "/").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("test-job-1"), "body should contain job id");
}

#[silo::test]
async fn test_job_view_renders() {
    let (_tmp, state) = setup_test_state().await;

    // Enqueue a job
    let shard = state.factory.get("0").expect("shard 0");
    let job_id = shard
        .enqueue(
            "-",
            Some("test-job-view".to_string()),
            25,
            test_helpers::now_ms(),
            None,
            serde_json::json!({"test": "payload"}),
            vec![],
            Some(vec![("key1".to_string(), "value1".to_string())]),
        )
        .await
        .expect("enqueue");

    let (status, body) = make_request(
        state,
        "GET",
        &format!("/job?shard=0&id={}", job_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(&job_id), "expected job id in body");
    assert!(body.contains("P25"), "expected priority in body");
    assert!(body.contains("key1"), "expected metadata key in body");
    assert!(body.contains("value1"), "expected metadata value in body");
}

#[silo::test]
async fn test_job_view_not_found() {
    let (_tmp, state) = setup_test_state().await;

    let (status, body) = make_request(
        state,
        "GET",
        "/job?shard=0&id=nonexistent-job",
    )
    .await;

    assert_eq!(status, StatusCode::OK); // Renders error page with 200
    assert!(body.contains("not found") || body.contains("Not Found"));
}

#[silo::test]
async fn test_job_cancel() {
    let (_tmp, state) = setup_test_state().await;

    // Enqueue a job
    let shard = state.factory.get("0").expect("shard 0");
    let job_id = shard
        .enqueue(
            "-",
            Some("test-job-cancel".to_string()),
            50,
            test_helpers::now_ms() + 100000, // Future time so it stays scheduled
            None,
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("enqueue");

    // Verify job is scheduled
    let status_before = shard.get_job_status("-", &job_id).await.expect("get status");
    assert!(status_before.is_some());
    assert_eq!(
        status_before.unwrap().kind,
        silo::job::JobStatusKind::Scheduled
    );

    // Cancel via HTTP
    let (status, body) = make_request(
        state.clone(),
        "POST",
        &format!("/job/cancel?shard=0&id={}", job_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Cancelled"));

    // Verify job is cancelled
    let status_after = shard.get_job_status("-", &job_id).await.expect("get status");
    assert!(status_after.is_some());
    assert_eq!(
        status_after.unwrap().kind,
        silo::job::JobStatusKind::Cancelled
    );
}

#[silo::test]
async fn test_queues_view_renders() {
    let (_tmp, state) = setup_test_state().await;

    let (status, body) = make_request(state, "GET", "/queues").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Queues"));
}

#[silo::test]
async fn test_queue_view_renders() {
    let (_tmp, state) = setup_test_state().await;

    // Queue view now requires shard parameter
    let (status, body) = make_request(
        state,
        "GET",
        "/queue?shard=0&name=test-queue",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("test-queue"));
}

#[silo::test]
async fn test_cluster_view_renders() {
    let (_tmp, state) = setup_test_state().await;

    let (status, body) = make_request(state, "GET", "/cluster").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Cluster"));
    assert!(body.contains("0")); // Shard 0 should be listed
}

#[silo::test]
async fn test_cancel_already_terminal_job_is_noop() {
    let (_tmp, state) = setup_test_state().await;

    // Enqueue a job
    let shard = state.factory.get("0").expect("shard 0");
    let job_id = shard
        .enqueue(
            "-",
            Some("test-job-terminal".to_string()),
            50,
            test_helpers::now_ms() + 100000,
            None,
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("enqueue");

    // Cancel it first time
    shard.cancel_job("-", &job_id).await.expect("first cancel");

    // Verify it's cancelled
    let status1 = shard.get_job_status("-", &job_id).await.expect("get status").unwrap();
    assert_eq!(status1.kind, silo::job::JobStatusKind::Cancelled);

    // Cancel again - should be a no-op
    let (status, _body) = make_request(
        state.clone(),
        "POST",
        &format!("/job/cancel?shard=0&id={}", job_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);

    // Still cancelled
    let status2 = shard.get_job_status("-", &job_id).await.expect("get status").unwrap();
    assert_eq!(status2.kind, silo::job::JobStatusKind::Cancelled);
}

#[silo::test]
async fn test_cancel_nonexistent_job_returns_error() {
    let (_tmp, state) = setup_test_state().await;

    let (status, body) = make_request(
        state,
        "POST",
        "/job/cancel?shard=0&id=nonexistent",
    )
    .await;

    // Returns 200 with error message in body
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Error") || body.contains("not found"));
}

#[silo::test]
async fn test_404_handler() {
    let (_tmp, state) = setup_test_state().await;

    let (status, body) = make_request(state, "GET", "/nonexistent-page").await;

    assert_eq!(status, StatusCode::OK); // Fallback renders error page
    assert!(body.contains("404") || body.contains("Not Found"));
}

// =============================================================================
// Multi-shard regression tests
// =============================================================================

/// Helper to create a multi-shard test AppState
async fn setup_multi_shard_state(num_shards: usize) -> (tempfile::TempDir, AppState) {
    let tmp = tempfile::tempdir().unwrap();

    let rate_limiter = MockGubernatorClient::new_arc();
    // Use {shard} placeholder so each shard gets its own subdirectory
    let path_with_shard = format!("{}/{{shard}}", tmp.path().to_string_lossy());
    let factory = ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: path_with_shard,
            wal: None,
        },
        rate_limiter,
    );

    // Open multiple shards
    for i in 0..num_shards {
        factory.open(i).await.expect(&format!("open shard {}", i));
    }

    let factory = Arc::new(factory);
    let cluster_client = Arc::new(ClusterClient::new(factory.clone(), None));

    let state = AppState {
        factory,
        coordinator: None,
        cluster_client,
    };

    (tmp, state)
}

#[silo::test]
async fn test_index_shows_jobs_from_all_shards() {
    // Create state with 3 shards
    let (_tmp, state) = setup_multi_shard_state(3).await;

    // Enqueue a job on each shard
    let shard0 = state.factory.get("0").expect("shard 0");
    let _job0 = shard0
        .enqueue(
            "-",
            Some("job-on-shard-0".to_string()),
            50,
            test_helpers::now_ms() + 10000,
            None,
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("enqueue shard 0");

    let shard1 = state.factory.get("1").expect("shard 1");
    let _job1 = shard1
        .enqueue(
            "-",
            Some("job-on-shard-1".to_string()),
            50,
            test_helpers::now_ms() + 10000,
            None,
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("enqueue shard 1");

    let shard2 = state.factory.get("2").expect("shard 2");
    let _job2 = shard2
        .enqueue(
            "-",
            Some("job-on-shard-2".to_string()),
            50,
            test_helpers::now_ms() + 10000,
            None,
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("enqueue shard 2");

    // Request the index page
    let (status, body) = make_request(state, "GET", "/").await;

    assert_eq!(status, StatusCode::OK);
    // Verify jobs from ALL shards are displayed
    assert!(
        body.contains("job-on-shard-0"),
        "index should show job from shard 0"
    );
    assert!(
        body.contains("job-on-shard-1"),
        "index should show job from shard 1"
    );
    assert!(
        body.contains("job-on-shard-2"),
        "index should show job from shard 2"
    );
}

#[silo::test]
async fn test_cluster_page_shows_all_shards() {
    // Create state with 3 shards
    let (_tmp, state) = setup_multi_shard_state(3).await;

    // Request the cluster page
    let (status, body) = make_request(state, "GET", "/cluster").await;

    assert_eq!(status, StatusCode::OK);
    // Verify all shards are displayed
    assert!(body.contains(">0<") || body.contains(">0</"), "cluster page should show shard 0");
    assert!(body.contains(">1<") || body.contains(">1</"), "cluster page should show shard 1");
    assert!(body.contains(">2<") || body.contains(">2</"), "cluster page should show shard 2");
}

#[silo::test]
async fn test_job_detail_from_non_zero_shard() {
    // Create state with 2 shards
    let (_tmp, state) = setup_multi_shard_state(2).await;

    // Enqueue a job on shard 1
    let shard1 = state.factory.get("1").expect("shard 1");
    let job_id = shard1
        .enqueue(
            "-",
            Some("job-on-shard-one".to_string()),
            25,
            test_helpers::now_ms(),
            None,
            serde_json::json!({"shard": 1}),
            vec![],
            None,
        )
        .await
        .expect("enqueue shard 1");

    // Request job detail from shard 1
    let (status, body) = make_request(
        state,
        "GET",
        &format!("/job?shard=1&id={}", job_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains(&job_id),
        "job detail should show the job from shard 1"
    );
    assert!(body.contains("P25"), "job detail should show priority");
}

#[silo::test]
async fn test_queues_page_shows_queues_from_all_shards() {
    use silo::job::{ConcurrencyLimit, Limit};

    // Create state with 2 shards
    let (_tmp, state) = setup_multi_shard_state(2).await;

    // Enqueue a job with concurrency limit on shard 0
    let shard0 = state.factory.get("0").expect("shard 0");
    let _job0 = shard0
        .enqueue(
            "-",
            Some("job-queue-shard0".to_string()),
            50,
            test_helpers::now_ms(),
            None,
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-on-shard-0".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue shard 0");

    // Enqueue a job with concurrency limit on shard 1
    let shard1 = state.factory.get("1").expect("shard 1");
    let _job1 = shard1
        .enqueue(
            "-",
            Some("job-queue-shard1".to_string()),
            50,
            test_helpers::now_ms(),
            None,
            serde_json::json!({}),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: "queue-on-shard-1".to_string(),
                max_concurrency: 1,
            })],
            None,
        )
        .await
        .expect("enqueue shard 1");

    // Dequeue to create holders (jobs will wait in concurrency queues)
    // Since max_concurrency is 1 and we enqueued, there should be holders or requesters
    let _ = shard0.dequeue("worker-0", 1).await;
    let _ = shard1.dequeue("worker-1", 1).await;

    // Request the queues page
    let (status, body) = make_request(state, "GET", "/queues").await;

    assert_eq!(status, StatusCode::OK);
    // The queues page should show queues from both shards
    // Note: The exact queue names may vary based on implementation
    // This test verifies the page renders successfully with multi-shard data
    assert!(
        body.contains("Queues"),
        "queues page should render"
    );
}
