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

fn validate(req: &AdmissionRequest<SiloAutoscaler>) -> Result<(), Vec<String>> {
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
