use axum::{Json, Router, routing::post};
use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview};
use kube::core::DynamicObject;
use tracing::{info, warn};

use crate::crd::SiloAutoscaler;

/// Build the webhook router.
pub fn webhook_router() -> Router {
    Router::new().route("/validate-silo-autoscaler", post(validate_handler))
}

async fn validate_handler(
    Json(review): Json<AdmissionReview<SiloAutoscaler>>,
) -> Json<AdmissionReview<DynamicObject>> {
    let req: AdmissionRequest<SiloAutoscaler> = match review.try_into() {
        Ok(req) => req,
        Err(e) => {
            warn!(error = %e, "failed to parse admission request");
            let response = AdmissionResponse::invalid(format!("failed to parse request: {e}"));
            return Json(response.into_review());
        }
    };

    let uid = req.uid.clone();
    let response = match validate(&req) {
        Ok(()) => {
            info!(uid = %uid, "admission request accepted");
            AdmissionResponse::from(&req)
        }
        Err(errors) => {
            let msg = errors.join("; ");
            warn!(uid = %uid, errors = %msg, "admission request rejected");
            AdmissionResponse::from(&req).deny(msg)
        }
    };

    Json(response.into_review())
}

/// Validate a SiloAutoscaler admission request.
/// Returns Ok(()) if valid, or Err with a list of validation errors.
pub(crate) fn validate(req: &AdmissionRequest<SiloAutoscaler>) -> Result<(), Vec<String>> {
    let obj = req
        .object
        .as_ref()
        .ok_or_else(|| vec!["missing object in request".to_string()])?;

    let spec = &obj.spec;
    let mut errors = Vec::new();

    if spec.replicas < 0 {
        errors.push("spec.replicas must be >= 0".to_string());
    }

    if spec.target_stateful_set.is_empty() {
        errors.push("spec.targetStatefulSet must be non-empty".to_string());
    }

    if spec.cluster_prefix.is_empty() {
        errors.push("spec.clusterPrefix must be non-empty".to_string());
    }

    if spec.orphaned_lease_grace_period_seconds <= 0 {
        errors.push("spec.orphanedLeaseGracePeriodSeconds must be positive".to_string());
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use crate::crd::SiloAutoscaler;
    use tower::ServiceExt;

    /// Build a minimal AdmissionReview JSON for testing.
    fn make_review(spec: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "test-uid-12345",
                "kind": {"group": "silo.dev", "version": "v1alpha1", "kind": "SiloAutoscaler"},
                "resource": {"group": "silo.dev", "version": "v1alpha1", "resource": "siloautoscalers"},
                "operation": "CREATE",
                "userInfo": {"username": "test-user", "groups": []},
                "object": {
                    "apiVersion": "silo.dev/v1alpha1",
                    "kind": "SiloAutoscaler",
                    "metadata": {"name": "test", "namespace": "silo-test"},
                    "spec": spec
                }
            }
        })
    }

    /// Send a request to the webhook handler and return the response body.
    async fn send_webhook_request(body: serde_json::Value) -> (StatusCode, serde_json::Value) {
        let app = webhook_router();
        let request = Request::builder()
            .method("POST")
            .uri("/validate-silo-autoscaler")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(&body_bytes);
        let body_json: serde_json::Value = serde_json::from_str(&body_str)
            .unwrap_or_else(|e| panic!("failed to parse response (status={status}): {e}\nbody: {body_str}"));
        (status, body_json)
    }

    #[tokio::test]
    async fn test_webhook_rejects_negative_replicas() {
        let review = make_review(serde_json::json!({
            "replicas": -1,
            "targetStatefulSet": "silo",
            "clusterPrefix": "test-cluster",
            "orphanedLeaseGracePeriodSeconds": 120
        }));

        let (status, body) = send_webhook_request(review).await;
        assert_eq!(status, StatusCode::OK);

        let allowed = body["response"]["allowed"].as_bool().unwrap();
        assert!(!allowed, "negative replicas should be rejected");

        let message = body["response"]["status"]["message"].as_str().unwrap();
        assert!(message.contains("spec.replicas must be >= 0"), "message: {message}");
    }

    #[tokio::test]
    async fn test_webhook_rejects_empty_target_statefulset() {
        let review = make_review(serde_json::json!({
            "replicas": 1,
            "targetStatefulSet": "",
            "clusterPrefix": "test-cluster",
            "orphanedLeaseGracePeriodSeconds": 120
        }));

        let (status, body) = send_webhook_request(review).await;
        assert_eq!(status, StatusCode::OK);

        let allowed = body["response"]["allowed"].as_bool().unwrap();
        assert!(!allowed, "empty targetStatefulSet should be rejected");

        let message = body["response"]["status"]["message"].as_str().unwrap();
        assert!(message.contains("spec.targetStatefulSet must be non-empty"), "message: {message}");
    }

    #[tokio::test]
    async fn test_webhook_rejects_empty_cluster_prefix() {
        let review = make_review(serde_json::json!({
            "replicas": 1,
            "targetStatefulSet": "silo",
            "clusterPrefix": "",
            "orphanedLeaseGracePeriodSeconds": 120
        }));

        let (status, body) = send_webhook_request(review).await;
        assert_eq!(status, StatusCode::OK);

        let allowed = body["response"]["allowed"].as_bool().unwrap();
        assert!(!allowed, "empty clusterPrefix should be rejected");

        let message = body["response"]["status"]["message"].as_str().unwrap();
        assert!(message.contains("spec.clusterPrefix must be non-empty"), "message: {message}");
    }

    #[tokio::test]
    async fn test_webhook_rejects_negative_grace_period() {
        let review = make_review(serde_json::json!({
            "replicas": 1,
            "targetStatefulSet": "silo",
            "clusterPrefix": "test-cluster",
            "orphanedLeaseGracePeriodSeconds": -10
        }));

        let (status, body) = send_webhook_request(review).await;
        assert_eq!(status, StatusCode::OK);

        let allowed = body["response"]["allowed"].as_bool().unwrap();
        assert!(!allowed, "negative grace period should be rejected");

        let message = body["response"]["status"]["message"].as_str().unwrap();
        assert!(
            message.contains("spec.orphanedLeaseGracePeriodSeconds must be positive"),
            "message: {message}"
        );
    }

    #[tokio::test]
    async fn test_webhook_accepts_valid_resource() {
        let review = make_review(serde_json::json!({
            "replicas": 3,
            "targetStatefulSet": "silo",
            "clusterPrefix": "silo-local-test",
            "orphanedLeaseGracePeriodSeconds": 120
        }));

        let (status, body) = send_webhook_request(review).await;
        assert_eq!(status, StatusCode::OK);

        let allowed = body["response"]["allowed"].as_bool().unwrap();
        assert!(allowed, "valid resource should be accepted");
    }
}
