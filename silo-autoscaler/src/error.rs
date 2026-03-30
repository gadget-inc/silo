use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Missing object key: {0}")]
    MissingObjectKey(&'static str),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("StatefulSet {0} not found in namespace {1}")]
    StatefulSetNotFound(String, String),

    #[error("Invalid spec: {0}")]
    InvalidSpec(String),
}

/// Wrapper for controller reconciler error reporting.
/// kube::runtime::Controller requires the error type to be clone-friendly for metrics,
/// so we wrap it in an Arc-based type.
#[derive(Debug, Clone)]
pub struct ReconcileError(pub String);

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReconcileError {}

impl From<Error> for ReconcileError {
    fn from(e: Error) -> Self {
        ReconcileError(e.to_string())
    }
}

impl From<kube::Error> for ReconcileError {
    fn from(e: kube::Error) -> Self {
        ReconcileError(e.to_string())
    }
}
