use kube::CustomResource;
use kube::CustomResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec for the SiloAutoscaler CRD.
///
/// The SiloAutoscaler manages safe scaling of a Silo StatefulSet by adding
/// finalizers to pods and ensuring shard leases are cleanly released before
/// allowing pod deletion to complete.
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
    printcolumn = r#"{"name":"Orphans","type":"integer","jsonPath":".status.orphanedLeaseCount"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SiloAutoscalerSpec {
    /// Target replica count. An HPA can modify this via the /scale subresource.
    pub replicas: i32,

    /// Name of the StatefulSet to manage, in the same namespace.
    pub target_stateful_set: String,

    /// Cluster prefix used by silo coordination for lease labels (silo.dev/cluster={prefix}).
    pub cluster_prefix: String,
}

/// Status of the SiloAutoscaler.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SiloAutoscalerStatus {
    /// Current replica count read from the StatefulSet.
    #[serde(default)]
    pub replicas: i32,

    /// Number of orphaned leases currently detected.
    #[serde(default)]
    pub orphaned_lease_count: i32,

    /// Standard Kubernetes-style conditions.
    #[serde(default)]
    pub conditions: Vec<AutoscalerCondition>,

    /// Orphaned shard leases currently detected.
    #[serde(default)]
    pub orphaned_leases: Vec<OrphanedLeaseInfo>,
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

/// Generate the CRD definition for installation.
pub fn crd_definition() -> k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition {
    SiloAutoscaler::crd()
}
