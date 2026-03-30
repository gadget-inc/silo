use kube::CustomResource;
use kube::CustomResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec for the SiloAutoscaler CRD.
///
/// The SiloAutoscaler manages safe scaling of a Silo StatefulSet by monitoring
/// shard lease ownership and recovering from failed scale-down operations where
/// pods were SIGKILL'd before flushing their WAL.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "silo.dev",
    version = "v1alpha1",
    kind = "SiloAutoscaler",
    namespaced,
    status = "SiloAutoscalerStatus",
    scale = r#"{"specReplicasPath":".spec.replicas","statusReplicasPath":".status.replicas"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Current","type":"integer","jsonPath":".status.replicas"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SiloAutoscalerSpec {
    /// Target replica count. An HPA can modify this via the /scale subresource.
    pub replicas: i32,

    /// Name of the StatefulSet to manage, in the same namespace.
    pub target_stateful_set: String,

    /// Cluster prefix used by silo coordination for lease labels (silo.dev/cluster={prefix}).
    pub cluster_prefix: String,

    /// Seconds to wait after a scale-down before checking for orphaned leases.
    /// Default: 120 seconds.
    #[serde(default = "default_grace_period")]
    pub orphaned_lease_grace_period_seconds: i64,
}

fn default_grace_period() -> i64 {
    120
}

/// Status of the SiloAutoscaler.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SiloAutoscalerStatus {
    /// Current replica count read from the StatefulSet.
    #[serde(default)]
    pub replicas: i32,

    /// Current phase: Idle, ScalingUp, ScalingDown, RecoveringOrphans.
    #[serde(default)]
    pub phase: String,

    /// Standard Kubernetes-style conditions.
    #[serde(default)]
    pub conditions: Vec<AutoscalerCondition>,

    /// Orphaned shard leases currently detected.
    #[serde(default)]
    pub orphaned_leases: Vec<OrphanedLeaseInfo>,

    /// State tracking for in-progress scale-down operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_down_state: Option<ScaleDownState>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AutoscalerCondition {
    pub r#type: String,
    pub status: String,
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrphanedLeaseInfo {
    pub lease_name: String,
    pub shard_id: String,
    pub holder_identity: String,
    pub detected_at: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleDownState {
    /// The replica count before the scale-down started.
    pub original_replicas: i32,
    /// The target replica count for the scale-down.
    pub target_replicas: i32,
    /// Number of scale-back-up recovery attempts so far.
    pub recovery_attempts: i32,
    /// Maximum recovery attempts before giving up.
    pub max_recovery_attempts: i32,
    /// When the scale-down was initiated (RFC 3339).
    pub started_at: String,
}

/// Generate the CRD definition for installation.
pub fn crd_definition() -> k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition {
    SiloAutoscaler::crd()
}
