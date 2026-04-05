use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use kube::Client;
use tracing::{debug, error, info, warn};

use crate::crd::{AutoscalerCondition, OrphanedLeaseInfo, SiloAutoscaler, SiloAutoscalerStatus};
use crate::error::ReconcileError;
use crate::orphan;

const FINALIZER_NAME: &str = "silo.dev/safe-scaledown";
const SA_NAMESPACE_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

pub struct Context {
    pub client: Client,
}

/// Read the namespace the controller is running in from the service account mount.
/// Falls back to the NAMESPACE env var, then "default".
fn detect_namespace() -> String {
    std::fs::read_to_string(SA_NAMESPACE_PATH)
        .or_else(|_| std::env::var("NAMESPACE"))
        .unwrap_or_else(|_| "default".to_string())
}

/// Outcome of processing terminating pods during reconciliation.
enum TerminationStatus {
    /// No pods are terminating — safe to adjust replicas.
    Clear,
    /// At least one pod is still terminating (container running or awaiting finalizer removal).
    Pending,
    /// Recovery was triggered: orphaned leases were found on a SIGKILL'd pod.
    Recovery { orphaned_leases: Vec<OrphanedLeaseInfo> },
}

impl TerminationStatus {
    fn is_active(&self) -> bool {
        !matches!(self, TerminationStatus::Clear)
    }

    fn orphaned_leases(self) -> Vec<OrphanedLeaseInfo> {
        match self {
            TerminationStatus::Recovery { orphaned_leases } => orphaned_leases,
            _ => vec![],
        }
    }
}

/// Run the SiloAutoscaler controller loop.
///
/// The controller is scoped to its own namespace, detected from the
/// service account mount at `/var/run/secrets/kubernetes.io/serviceaccount/namespace`,
/// the `NAMESPACE` env var, or falling back to `default`.
pub async fn run_controller(client: Client) -> anyhow::Result<()> {
    let namespace = detect_namespace();
    let autoscalers: Api<SiloAutoscaler> = Api::namespaced(client.clone(), &namespace);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);

    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    info!(namespace = %namespace, "starting SiloAutoscaler controller");

    Controller::new(autoscalers, watcher::Config::default())
        .watches(
            pods,
            watcher::Config::default(),
            // Pod changes don't directly map to a SiloAutoscaler name, so we return empty.
            // The namespaced watch ensures we only track pods in our namespace, and the
            // requeue interval (5s during active operations) drives convergence.
            |_pod| std::iter::empty(),
        )
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => {
                    debug!(name = %obj.name, namespace = ?obj.namespace, "reconciled");
                }
                Err(e) => {
                    error!(error = %e, "reconcile failed");
                }
            }
        })
        .await;

    Ok(())
}

fn error_policy(
    _obj: Arc<SiloAutoscaler>,
    error: &ReconcileError,
    _ctx: Arc<Context>,
) -> Action {
    warn!(error = %error, "reconcile error, retrying in 15s");
    Action::requeue(Duration::from_secs(15))
}

async fn reconcile(
    obj: Arc<SiloAutoscaler>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let client = &ctx.client;
    let namespace = obj
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| ReconcileError("missing metadata.namespace".into()))?;
    let name = obj
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| ReconcileError("missing metadata.name".into()))?;
    let spec = &obj.spec;
    let desired = spec.replicas;

    debug!(
        name = %name,
        namespace = %namespace,
        desired_replicas = desired,
        target_sts = %spec.target_stateful_set,
        "reconciling SiloAutoscaler"
    );

    // Read target StatefulSet
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let sts = match sts_api.get(&spec.target_stateful_set).await {
        Ok(sts) => sts,
        Err(kube::Error::Api(e)) if e.code == 404 => {
            warn!(
                name = %name,
                target_sts = %spec.target_stateful_set,
                "target StatefulSet not found"
            );
            update_status(
                client, namespace, name, 0, vec![], vec![
                    make_condition("Ready", "False", "StatefulSetNotFound",
                        &format!("StatefulSet {} not found", spec.target_stateful_set)),
                ],
                "",
            ).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
        Err(e) => return Err(e.into()),
    };

    let sts_replicas = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);

    // List pods for this StatefulSet
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let label_selector = label_selector_from_sts(&sts);
    let pods = pod_api
        .list(&ListParams::default().labels(&label_selector))
        .await
        .map_err(|e| ReconcileError(format!("failed to list pods: {e}")))?;

    // Process any terminating pods, handling finalizer removal and recovery
    let termination_status = handle_terminating_pods(
        client, namespace, &pod_api, &sts_api, &pods.items, spec, sts_replicas,
    ).await?;

    // Only adjust replicas when no pods are mid-termination
    let termination_active = termination_status.is_active();
    if !termination_active {
        apply_desired_replicas(name, &sts_api, spec, sts_replicas, desired, &pods.items).await?;
    }

    // Update status
    let orphaned_leases = termination_status.orphaned_leases();
    let conditions = if !orphaned_leases.is_empty() {
        vec![make_condition(
            "OrphanedLeases", "True", "Recovering",
            &format!("{} orphaned leases, scaling up for recovery", orphaned_leases.len()),
        )]
    } else if sts_replicas != desired {
        vec![make_condition(
            "Ready", "False", "Scaling",
            &format!("replicas: {} desired: {}", sts_replicas, desired),
        )]
    } else {
        vec![make_condition("Ready", "True", "Idle", "replicas match desired count")]
    };
    update_status(client, namespace, name, sts_replicas, orphaned_leases, conditions, &label_selector).await?;

    // Requeue interval
    let interval = requeue_interval(sts_replicas, desired, termination_active);
    Ok(Action::requeue(Duration::from_secs(interval)))
}

/// Process pods that are terminating and have our finalizer.
///
/// For each terminating pod whose container has exited:
/// - If its shard leases were released: remove the finalizer (clean shutdown).
/// - If leases are still held (SIGKILL'd): scale the StatefulSet up so the pod
///   gets recreated, then remove the finalizer to allow the recreation.
async fn handle_terminating_pods(
    client: &Client,
    namespace: &str,
    pod_api: &Api<Pod>,
    sts_api: &Api<StatefulSet>,
    pods: &[Pod],
    spec: &crate::crd::SiloAutoscalerSpec,
    sts_replicas: i32,
) -> Result<TerminationStatus, ReconcileError> {
    let mut orphaned_leases: Vec<OrphanedLeaseInfo> = vec![];
    let mut has_terminating = false;

    for pod in pods {
        if pod.metadata.deletion_timestamp.is_none() || !has_finalizer(pod) {
            continue;
        }
        has_terminating = true;

        let pod_name = match &pod.metadata.name {
            Some(n) => n.as_str(),
            None => continue,
        };

        if !is_container_terminated(pod) {
            debug!(pod = %pod_name, "container still running, waiting for flush");
            continue;
        }

        // Container is dead — check if leases were released
        let held_leases = orphan::leases_held_by_pod(
            client, namespace, &spec.cluster_prefix, pod_name,
        ).await?;

        if held_leases.is_empty() {
            info!(pod = %pod_name, "leases released, removing finalizer");
            remove_finalizer(pod_api, pod_name).await?;
        } else {
            recover_orphaned_pod(
                pod_api, sts_api, pod_name, &held_leases, spec, sts_replicas,
            ).await?;
            orphaned_leases.extend(held_leases);
        }
    }

    if !orphaned_leases.is_empty() {
        Ok(TerminationStatus::Recovery { orphaned_leases })
    } else if has_terminating {
        Ok(TerminationStatus::Pending)
    } else {
        Ok(TerminationStatus::Clear)
    }
}

/// Scale the StatefulSet up to cover a pod that was SIGKILL'd with leases still held,
/// then remove its finalizer so it can be fully deleted and recreated.
async fn recover_orphaned_pod(
    pod_api: &Api<Pod>,
    sts_api: &Api<StatefulSet>,
    pod_name: &str,
    held_leases: &[OrphanedLeaseInfo],
    spec: &crate::crd::SiloAutoscalerSpec,
    sts_replicas: i32,
) -> Result<(), ReconcileError> {
    let ordinal = orphan::extract_pod_ordinal(pod_name).unwrap_or(0);
    let recovery_replicas = ordinal + 1;

    info!(
        pod = %pod_name,
        orphaned_leases = held_leases.len(),
        recovery_replicas = recovery_replicas,
        "orphaned leases detected, scaling up for recovery"
    );

    if sts_replicas < recovery_replicas {
        patch_statefulset_replicas(sts_api, &spec.target_stateful_set, recovery_replicas).await?;
    }

    // Remove the finalizer so the old terminating pod can be fully deleted.
    // The StatefulSet (now scaled up) will recreate the pod with the same
    // ordinal and PVC, allowing silo to reclaim the leases and flush the WAL.
    // The shard is already down at this point (container was SIGKILL'd), so
    // removing the finalizer doesn't cause additional downtime.
    remove_finalizer(pod_api, pod_name).await?;
    Ok(())
}

/// Adjust StatefulSet replicas toward the desired count when no terminations are in progress.
async fn apply_desired_replicas(
    name: &str,
    sts_api: &Api<StatefulSet>,
    spec: &crate::crd::SiloAutoscalerSpec,
    sts_replicas: i32,
    desired: i32,
    pods: &[Pod],
) -> Result<(), ReconcileError> {
    if sts_replicas > desired {
        // Recovery just completed — wait for all pods to be ready before scaling back down
        let all_ready = pods.iter().all(|p| {
            p.metadata.deletion_timestamp.is_none() && is_pod_ready(p)
        });
        if all_ready {
            info!(name = %name, current = sts_replicas, desired = desired, "recovery complete, scaling back down");
            patch_statefulset_replicas(sts_api, &spec.target_stateful_set, desired).await?;
        } else {
            debug!(name = %name, "waiting for recovery pods to become ready");
        }
    } else if sts_replicas < desired {
        info!(name = %name, current = sts_replicas, desired = desired, "scaling up");
        patch_statefulset_replicas(sts_api, &spec.target_stateful_set, desired).await?;
    }
    Ok(())
}

fn requeue_interval(sts_replicas: i32, desired: i32, termination_active: bool) -> u64 {
    if termination_active {
        5
    } else if sts_replicas != desired {
        10
    } else {
        60
    }
}

fn has_finalizer(pod: &Pod) -> bool {
    pod.metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|fin| fin == FINALIZER_NAME))
}

fn is_container_terminated(pod: &Pod) -> bool {
    let statuses = match pod.status.as_ref().and_then(|s| s.container_statuses.as_ref()) {
        Some(s) => s,
        None => return true, // No container statuses means containers never ran or were evicted
    };
    statuses.iter().all(|cs| {
        cs.state
            .as_ref()
            .is_some_and(|s| s.terminated.is_some())
    })
}

fn is_pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True")
        })
}

async fn remove_finalizer(pod_api: &Api<Pod>, pod_name: &str) -> Result<(), ReconcileError> {
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": []
        }
    });
    pod_api
        .patch(pod_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(|e| ReconcileError(format!("failed to remove finalizer from pod {pod_name}: {e}")))?;
    debug!(pod = %pod_name, "removed finalizer");
    Ok(())
}

async fn patch_statefulset_replicas(
    sts_api: &Api<StatefulSet>,
    name: &str,
    replicas: i32,
) -> Result<(), ReconcileError> {
    let patch = serde_json::json!({
        "spec": {
            "replicas": replicas
        }
    });
    sts_api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(|e| ReconcileError(format!("failed to patch StatefulSet replicas: {e}")))?;
    Ok(())
}

async fn update_status(
    client: &Client,
    namespace: &str,
    name: &str,
    replicas: i32,
    orphaned_leases: Vec<OrphanedLeaseInfo>,
    conditions: Vec<AutoscalerCondition>,
    label_selector: &str,
) -> Result<(), ReconcileError> {
    let autoscaler_api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), namespace);

    let orphaned_lease_count = orphaned_leases.len() as i32;
    let selector = if label_selector.is_empty() {
        None
    } else {
        Some(label_selector.to_string())
    };
    let status = SiloAutoscalerStatus {
        replicas,
        orphaned_lease_count,
        conditions,
        orphaned_leases,
        selector,
    };

    let patch = serde_json::json!({
        "apiVersion": "silo.dev/v1alpha1",
        "kind": "SiloAutoscaler",
        "status": status
    });

    autoscaler_api
        .patch_status(
            name,
            &PatchParams::apply("silo-autoscaler").force(),
            &Patch::Apply(&patch),
        )
        .await
        .map_err(|e| ReconcileError(format!("failed to update status: {e}")))?;

    Ok(())
}

fn label_selector_from_sts(sts: &StatefulSet) -> String {
    sts.spec
        .as_ref()
        .and_then(|s| s.selector.match_labels.as_ref())
        .map(|labels| {
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

fn make_condition(
    condition_type: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> AutoscalerCondition {
    AutoscalerCondition {
        r#type: condition_type.to_string(),
        status: status.to_string(),
        last_transition_time: chrono::Utc::now().to_rfc3339(),
        reason: reason.to_string(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateTerminated, ContainerStatus, PodCondition, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn make_pod(name: &str, finalizers: Vec<&str>, terminated: bool, ready: bool) -> Pod {
        let container_state = if terminated {
            Some(ContainerState {
                terminated: Some(ContainerStateTerminated::default()),
                ..Default::default()
            })
        } else {
            Some(ContainerState {
                running: Some(Default::default()),
                ..Default::default()
            })
        };

        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                finalizers: if finalizers.is_empty() {
                    None
                } else {
                    Some(finalizers.into_iter().map(String::from).collect())
                },
                ..Default::default()
            },
            status: Some(PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    name: "silo".to_string(),
                    state: container_state,
                    ..Default::default()
                }]),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_string(),
                    status: if ready { "True" } else { "False" }.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_has_finalizer() {
        let pod_with = make_pod("silo-0", vec![FINALIZER_NAME], false, true);
        let pod_without = make_pod("silo-1", vec![], false, true);
        let pod_other = make_pod("silo-2", vec!["other.io/finalizer"], false, true);

        assert!(has_finalizer(&pod_with));
        assert!(!has_finalizer(&pod_without));
        assert!(!has_finalizer(&pod_other));
    }

    #[test]
    fn test_is_container_terminated() {
        let running = make_pod("silo-0", vec![], false, true);
        let terminated = make_pod("silo-0", vec![], true, false);
        let no_status = Pod {
            metadata: ObjectMeta { name: Some("silo-0".into()), ..Default::default() },
            ..Default::default()
        };

        assert!(!is_container_terminated(&running));
        assert!(is_container_terminated(&terminated));
        assert!(is_container_terminated(&no_status)); // no statuses = evicted
    }

    #[test]
    fn test_is_pod_ready() {
        let ready = make_pod("silo-0", vec![], false, true);
        let not_ready = make_pod("silo-0", vec![], false, false);
        let no_status = Pod {
            metadata: ObjectMeta { name: Some("silo-0".into()), ..Default::default() },
            ..Default::default()
        };

        assert!(is_pod_ready(&ready));
        assert!(!is_pod_ready(&not_ready));
        assert!(!is_pod_ready(&no_status));
    }

    #[test]
    fn test_requeue_interval() {
        assert_eq!(requeue_interval(3, 3, false), 60); // steady state
        assert_eq!(requeue_interval(3, 5, false), 10); // scaling
        assert_eq!(requeue_interval(3, 3, true), 5);   // termination active
        assert_eq!(requeue_interval(5, 3, true), 5);   // termination active overrides scaling
    }

    fn make_sts(match_labels: Option<std::collections::BTreeMap<String, String>>) -> StatefulSet {
        use k8s_openapi::api::apps::v1::StatefulSetSpec;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

        StatefulSet {
            spec: Some(StatefulSetSpec {
                selector: LabelSelector {
                    match_labels,
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_label_selector_from_sts() {
        let labels: std::collections::BTreeMap<String, String> =
            [("app".to_string(), "silo-staging".to_string())].into();
        let sts = make_sts(Some(labels));
        assert_eq!(label_selector_from_sts(&sts), "app=silo-staging");
    }

    #[test]
    fn test_label_selector_from_sts_empty() {
        let sts = make_sts(None);
        assert_eq!(label_selector_from_sts(&sts), "");
    }

    #[test]
    fn test_label_selector_from_sts_multiple_labels() {
        let labels: std::collections::BTreeMap<String, String> = [
            ("app".to_string(), "silo".to_string()),
            ("env".to_string(), "staging".to_string()),
        ].into();
        let sts = make_sts(Some(labels));
        // BTreeMap is sorted by key
        assert_eq!(label_selector_from_sts(&sts), "app=silo,env=staging");
    }
}
