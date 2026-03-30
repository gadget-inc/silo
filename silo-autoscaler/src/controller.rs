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

pub struct Context {
    pub client: Client,
}

/// Run the SiloAutoscaler controller loop.
pub async fn run_controller(client: Client) -> anyhow::Result<()> {
    let autoscalers: Api<SiloAutoscaler> = Api::all(client.clone());
    let pods: Api<Pod> = Api::all(client.clone());

    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    info!("starting SiloAutoscaler controller");

    Controller::new(autoscalers, watcher::Config::default())
        .watches(
            pods,
            watcher::Config::default(),
            |pod| {
                // Trigger reconciliation when pods with our finalizer change.
                // We return empty because we can't easily map pod → SiloAutoscaler.
                // The short requeue interval (5s) during active operations handles this.
                if has_finalizer(&pod) {
                    let ns = pod.metadata.namespace.clone().unwrap_or_default();
                    let name = pod.metadata.name.clone().unwrap_or_default();
                    debug!(namespace = %ns, pod = %name, "finalizer pod changed");
                }
                std::iter::empty()
            },
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

    // 1. Read target StatefulSet
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
            ).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
        Err(e) => return Err(e.into()),
    };

    let sts_replicas = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);

    // 2. List pods for this StatefulSet
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let label_selector = sts
        .spec
        .as_ref()
        .and_then(|s| s.selector.match_labels.as_ref())
        .map(|labels| {
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let pods = pod_api
        .list(&ListParams::default().labels(&label_selector))
        .await
        .map_err(|e| ReconcileError(format!("failed to list pods: {e}")))?;

    // 3. Handle terminating pods with our finalizer
    let mut all_orphaned_leases: Vec<OrphanedLeaseInfo> = vec![];
    let mut recovery_needed = false;
    let mut has_terminating_pods = false;

    for pod in &pods.items {
        if pod.metadata.deletion_timestamp.is_none() || !has_finalizer(pod) {
            continue;
        }
        has_terminating_pods = true;

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
            // Clean shutdown: leases released, remove finalizer
            info!(pod = %pod_name, "leases released, removing finalizer");
            remove_finalizer(&pod_api, pod_name).await?;
        } else {
            // SIGKILL'd: leases still held, need recovery
            let ordinal = orphan::extract_pod_ordinal(pod_name).unwrap_or(0);
            let recovery_replicas = ordinal + 1;

            info!(
                pod = %pod_name,
                orphaned_leases = held_leases.len(),
                recovery_replicas = recovery_replicas,
                "orphaned leases detected, scaling up for recovery"
            );

            // Scale up to cover this pod's ordinal
            if sts_replicas < recovery_replicas {
                patch_statefulset_replicas(&sts_api, &spec.target_stateful_set, recovery_replicas).await?;
            }

            // Remove finalizer so the terminating pod can be fully deleted
            // and the StatefulSet can recreate it
            remove_finalizer(&pod_api, pod_name).await?;

            all_orphaned_leases.extend(held_leases);
            recovery_needed = true;
        }
    }

    // 5. Apply desired replica count when safe
    if !recovery_needed && !has_terminating_pods {
        if sts_replicas > desired {
            // Recovery just completed (sts has more replicas than desired).
            // Check if all pods are ready before scaling back down.
            let all_ready = pods.items.iter().all(|p| {
                p.metadata.deletion_timestamp.is_none() && is_pod_ready(p)
            });
            if all_ready {
                info!(
                    name = %name,
                    current = sts_replicas,
                    desired = desired,
                    "recovery complete, scaling back down"
                );
                patch_statefulset_replicas(&sts_api, &spec.target_stateful_set, desired).await?;
            } else {
                debug!(name = %name, "waiting for recovery pods to become ready");
            }
        } else if sts_replicas < desired {
            info!(
                name = %name,
                current = sts_replicas,
                desired = desired,
                "scaling up"
            );
            patch_statefulset_replicas(&sts_api, &spec.target_stateful_set, desired).await?;
        }
    }

    // 6. Update status
    let conditions = if !all_orphaned_leases.is_empty() {
        vec![make_condition(
            "OrphanedLeases", "True", "Recovering",
            &format!("{} orphaned leases, scaling up for recovery", all_orphaned_leases.len()),
        )]
    } else if sts_replicas != desired {
        vec![make_condition(
            "Ready", "False", "Scaling",
            &format!("replicas: {} desired: {}", sts_replicas, desired),
        )]
    } else {
        vec![make_condition("Ready", "True", "Idle", "replicas match desired count")]
    };

    update_status(client, namespace, name, sts_replicas, all_orphaned_leases, conditions).await?;

    // 7. Requeue interval
    let interval = if recovery_needed || has_terminating_pods {
        5 // Fast poll during active operations
    } else if sts_replicas != desired {
        10
    } else {
        60
    };
    Ok(Action::requeue(Duration::from_secs(interval)))
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
) -> Result<(), ReconcileError> {
    let autoscaler_api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), namespace);

    let orphaned_lease_count = orphaned_leases.len() as i32;
    let status = SiloAutoscalerStatus {
        replicas,
        orphaned_lease_count,
        conditions,
        orphaned_leases,
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
