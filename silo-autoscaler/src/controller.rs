use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::coordination::v1::Lease;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use kube::Client;
use tracing::{debug, error, info, warn};

use crate::crd::{
    AutoscalerCondition, ScaleDownState, SiloAutoscaler, SiloAutoscalerStatus,
};
use crate::error::{Error, ReconcileError};
use crate::orphan;

pub struct Context {
    pub client: Client,
}

/// Run the SiloAutoscaler controller loop.
pub async fn run_controller(client: Client) -> anyhow::Result<()> {
    let autoscalers: Api<SiloAutoscaler> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let leases: Api<Lease> = Api::all(client.clone());

    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    info!("starting SiloAutoscaler controller");

    Controller::new(autoscalers, watcher::Config::default())
        .watches(
            statefulsets,
            watcher::Config::default(),
            |sts| {
                // Trigger reconcile for any SiloAutoscaler that might target this StatefulSet.
                // We return the StatefulSet name as the object ref — the controller framework
                // will match it against SiloAutoscaler objects. Since we can't easily do a
                // reverse lookup here, we rely on periodic requeue as the primary mechanism
                // and this watch as best-effort notification.
                let ns = sts.metadata.namespace.clone().unwrap_or_default();
                let name = sts.metadata.name.clone().unwrap_or_default();
                debug!(
                    namespace = %ns,
                    statefulset = %name,
                    "StatefulSet changed, will reconcile matching autoscalers"
                );
                // Return empty — periodic requeue handles reconciliation
                std::iter::empty()
            },
        )
        .watches(
            leases,
            watcher::Config::default().labels("silo.dev/type=shard"),
            |lease| {
                let ns = lease.metadata.namespace.clone().unwrap_or_default();
                debug!(namespace = %ns, "shard lease changed");
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
    warn!(error = %error, "reconcile error, retrying in 30s");
    Action::requeue(Duration::from_secs(30))
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
        .ok_or_else(|| Error::MissingObjectKey("metadata.namespace"))?;
    let name = obj
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| Error::MissingObjectKey("metadata.name"))?;
    let spec = &obj.spec;

    info!(
        name = %name,
        namespace = %namespace,
        desired_replicas = spec.replicas,
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
                client,
                namespace,
                name,
                0,
                "Idle",
                vec![],
                None,
                vec![make_condition(
                    "Ready",
                    "False",
                    "StatefulSetNotFound",
                    &format!("StatefulSet {} not found", spec.target_stateful_set),
                )],
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
        Err(e) => return Err(e.into()),
    };

    let current_replicas = sts
        .spec
        .as_ref()
        .and_then(|s| s.replicas)
        .unwrap_or(0);

    let ready_replicas = sts
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);

    // 2. Detect orphaned leases
    let orphans = orphan::detect_orphaned_leases(
        client,
        namespace,
        &spec.cluster_prefix,
        &spec.target_stateful_set,
        current_replicas,
    )
    .await?;

    if !orphans.is_empty() {
        info!(
            name = %name,
            orphan_count = orphans.len(),
            "detected orphaned shard leases"
        );
    }

    let desired = spec.replicas;

    // 3. Determine scaling action
    if desired == current_replicas && orphans.is_empty() {
        // Steady state
        debug!(name = %name, replicas = current_replicas, "steady state, no action needed");
        update_status(
            client,
            namespace,
            name,
            current_replicas,
            "Idle",
            vec![],
            None,
            vec![make_condition("Ready", "True", "Idle", "replicas match desired count")],
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    if desired > current_replicas {
        // Scale up: simple patch
        info!(
            name = %name,
            current = current_replicas,
            desired = desired,
            "scaling up StatefulSet"
        );
        patch_statefulset_replicas(&sts_api, &spec.target_stateful_set, desired).await?;
        update_status(
            client,
            namespace,
            name,
            current_replicas,
            "ScalingUp",
            orphans,
            None,
            vec![make_condition(
                "Scaling",
                "True",
                "ScalingUp",
                &format!("scaling from {} to {}", current_replicas, desired),
            )],
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }

    if desired < current_replicas {
        // Scale down: complex flow with orphan detection
        let status = obj.status.as_ref();
        let scale_down_state = status.and_then(|s| s.scale_down_state.as_ref());

        return handle_scale_down(
            client,
            namespace,
            name,
            spec,
            &sts_api,
            current_replicas,
            ready_replicas,
            desired,
            &orphans,
            scale_down_state,
        )
        .await;
    }

    // desired == current_replicas but orphans exist — need recovery
    if !orphans.is_empty() {
        let status = obj.status.as_ref();
        let scale_down_state = status.and_then(|s| s.scale_down_state.as_ref());
        let recovery_attempts = scale_down_state.map(|s| s.recovery_attempts).unwrap_or(0);
        let max_attempts = scale_down_state
            .map(|s| s.max_recovery_attempts)
            .unwrap_or(3);

        if recovery_attempts >= max_attempts {
            warn!(
                name = %name,
                attempts = recovery_attempts,
                "max recovery attempts exceeded, orphaned leases remain"
            );
            update_status(
                client,
                namespace,
                name,
                current_replicas,
                "Idle",
                orphans,
                None,
                vec![make_condition(
                    "OrphanedLeases",
                    "True",
                    "RecoveryFailed",
                    "max recovery attempts exceeded, manual intervention required",
                )],
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(60)));
        }

        let recovery_replicas = orphan::compute_recovery_replicas(&orphans, desired);
        info!(
            name = %name,
            recovery_replicas = recovery_replicas,
            orphan_count = orphans.len(),
            "scaling up to recover orphaned shards"
        );
        patch_statefulset_replicas(&sts_api, &spec.target_stateful_set, recovery_replicas).await?;

        let sds = ScaleDownState {
            original_replicas: current_replicas,
            target_replicas: desired,
            recovery_attempts: recovery_attempts + 1,
            max_recovery_attempts: max_attempts,
            started_at: scale_down_state
                .map(|s| s.started_at.clone())
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        };
        update_status(
            client,
            namespace,
            name,
            current_replicas,
            "RecoveringOrphans",
            orphans,
            Some(sds),
            vec![make_condition(
                "OrphanedLeases",
                "True",
                "Recovering",
                "scaling up to recover orphaned shards",
            )],
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn handle_scale_down(
    client: &Client,
    namespace: &str,
    name: &str,
    spec: &crate::crd::SiloAutoscalerSpec,
    sts_api: &Api<StatefulSet>,
    current_replicas: i32,
    _ready_replicas: i32,
    desired: i32,
    orphans: &[crate::crd::OrphanedLeaseInfo],
    scale_down_state: Option<&ScaleDownState>,
) -> Result<Action, ReconcileError> {
    match scale_down_state {
        None => {
            // First time: initiate scale-down
            info!(
                name = %name,
                current = current_replicas,
                desired = desired,
                "initiating scale-down"
            );
            patch_statefulset_replicas(sts_api, &spec.target_stateful_set, desired).await?;

            let sds = ScaleDownState {
                original_replicas: current_replicas,
                target_replicas: desired,
                recovery_attempts: 0,
                max_recovery_attempts: 3,
                started_at: chrono::Utc::now().to_rfc3339(),
            };
            update_status(
                client,
                namespace,
                name,
                current_replicas,
                "ScalingDown",
                orphans.to_vec(),
                Some(sds),
                vec![make_condition(
                    "Scaling",
                    "True",
                    "ScalingDown",
                    &format!("scaling from {} to {}", current_replicas, desired),
                )],
            )
            .await?;

            // Requeue after grace period to check for orphans
            let grace = Duration::from_secs(spec.orphaned_lease_grace_period_seconds as u64);
            Ok(Action::requeue(grace))
        }

        Some(sds) if !orphans.is_empty() && sds.recovery_attempts < sds.max_recovery_attempts => {
            // Orphaned leases detected: scale back up to recover
            let recovery_replicas = orphan::compute_recovery_replicas(orphans, desired);
            info!(
                name = %name,
                recovery_replicas = recovery_replicas,
                orphan_count = orphans.len(),
                attempt = sds.recovery_attempts + 1,
                "scaling back up to recover orphaned shards"
            );
            patch_statefulset_replicas(sts_api, &spec.target_stateful_set, recovery_replicas)
                .await?;

            let new_sds = ScaleDownState {
                original_replicas: sds.original_replicas,
                target_replicas: sds.target_replicas,
                recovery_attempts: sds.recovery_attempts + 1,
                max_recovery_attempts: sds.max_recovery_attempts,
                started_at: sds.started_at.clone(),
            };
            update_status(
                client,
                namespace,
                name,
                current_replicas,
                "RecoveringOrphans",
                orphans.to_vec(),
                Some(new_sds),
                vec![make_condition(
                    "OrphanedLeases",
                    "True",
                    "Recovering",
                    &format!(
                        "recovery attempt {}, scaling to {} replicas",
                        sds.recovery_attempts + 1,
                        recovery_replicas
                    ),
                )],
            )
            .await?;

            Ok(Action::requeue(Duration::from_secs(30)))
        }

        Some(sds) if sds.recovery_attempts >= sds.max_recovery_attempts => {
            // Max retries exceeded
            warn!(
                name = %name,
                attempts = sds.recovery_attempts,
                "max recovery attempts exceeded during scale-down"
            );
            update_status(
                client,
                namespace,
                name,
                current_replicas,
                "Idle",
                orphans.to_vec(),
                None,
                vec![make_condition(
                    "OrphanedLeases",
                    "True",
                    "RecoveryFailed",
                    "max recovery attempts exceeded, manual intervention required",
                )],
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }

        Some(_) if orphans.is_empty() => {
            // Recovery succeeded: scale-down completed
            info!(
                name = %name,
                desired = desired,
                "scale-down recovery complete, no orphaned leases"
            );

            // If the StatefulSet is still at the recovery replica count, scale down again
            if current_replicas > desired {
                info!(
                    name = %name,
                    current = current_replicas,
                    desired = desired,
                    "retrying scale-down after successful recovery"
                );
                patch_statefulset_replicas(sts_api, &spec.target_stateful_set, desired).await?;
                // Keep the scale_down_state but requeue after grace period
                let grace = Duration::from_secs(spec.orphaned_lease_grace_period_seconds as u64);
                update_status(
                    client,
                    namespace,
                    name,
                    current_replicas,
                    "ScalingDown",
                    vec![],
                    None,
                    vec![make_condition(
                        "Scaling",
                        "True",
                        "ScalingDown",
                        &format!("retrying scale-down from {} to {}", current_replicas, desired),
                    )],
                )
                .await?;
                return Ok(Action::requeue(grace));
            }

            update_status(
                client,
                namespace,
                name,
                current_replicas,
                "Idle",
                vec![],
                None,
                vec![make_condition("Ready", "True", "Idle", "scale-down complete")],
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }

        _ => {
            // Waiting for state to converge
            debug!(name = %name, "waiting for scale-down to converge");
            Ok(Action::requeue(Duration::from_secs(15)))
        }
    }
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
        .patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await
        .map_err(|e| ReconcileError(format!("failed to patch StatefulSet replicas: {e}")))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_status(
    client: &Client,
    namespace: &str,
    name: &str,
    replicas: i32,
    phase: &str,
    orphaned_leases: Vec<crate::crd::OrphanedLeaseInfo>,
    scale_down_state: Option<ScaleDownState>,
    conditions: Vec<AutoscalerCondition>,
) -> Result<(), ReconcileError> {
    let autoscaler_api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), namespace);

    let status = SiloAutoscalerStatus {
        replicas,
        phase: phase.to_string(),
        conditions,
        orphaned_leases,
        scale_down_state,
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
