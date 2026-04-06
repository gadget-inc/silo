use crate::crd::SiloAutoscalerSpec;

/// Validate a SiloAutoscalerSpec.
///
/// These rules mirror the CEL expressions in deploy/admission-policy.yaml.
/// They are enforced at the API server level via ValidatingAdmissionPolicy,
/// and also checked here so that the controller can detect invalid specs
/// during reconciliation and unit tests can verify the rules.
pub fn validate_spec(spec: &SiloAutoscalerSpec) -> Result<(), Vec<String>> {
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

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_spec() -> SiloAutoscalerSpec {
        SiloAutoscalerSpec {
            replicas: 3,
            target_stateful_set: "silo".to_string(),
            cluster_prefix: "silo-local-test".to_string(),
        }
    }

    #[test]
    fn test_rejects_negative_replicas() {
        let spec = SiloAutoscalerSpec {
            replicas: -1,
            ..valid_spec()
        };
        let err = validate_spec(&spec).unwrap_err();
        assert!(err.iter().any(|e| e.contains("spec.replicas must be >= 0")));
    }

    #[test]
    fn test_rejects_empty_target_statefulset() {
        let spec = SiloAutoscalerSpec {
            target_stateful_set: "".to_string(),
            ..valid_spec()
        };
        let err = validate_spec(&spec).unwrap_err();
        assert!(
            err.iter()
                .any(|e| e.contains("spec.targetStatefulSet must be non-empty"))
        );
    }

    #[test]
    fn test_rejects_empty_cluster_prefix() {
        let spec = SiloAutoscalerSpec {
            cluster_prefix: "".to_string(),
            ..valid_spec()
        };
        let err = validate_spec(&spec).unwrap_err();
        assert!(
            err.iter()
                .any(|e| e.contains("spec.clusterPrefix must be non-empty"))
        );
    }

    #[test]
    fn test_accepts_valid_spec() {
        assert!(validate_spec(&valid_spec()).is_ok());
    }

    #[test]
    fn test_collects_multiple_errors() {
        let spec = SiloAutoscalerSpec {
            replicas: -1,
            target_stateful_set: "".to_string(),
            cluster_prefix: "".to_string(),
        };
        let err = validate_spec(&spec).unwrap_err();
        assert_eq!(err.len(), 3, "should report all 3 validation errors");
    }
}
