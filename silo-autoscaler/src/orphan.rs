use k8s_openapi::api::coordination::v1::Lease;
use kube::{Api, api::ListParams};

use crate::crd::OrphanedLeaseInfo;
use crate::error::Error;

/// Detect orphaned shard leases: leases held by pods that no longer exist
/// because the StatefulSet was scaled down.
///
/// A shard lease is orphaned when its `holderIdentity` refers to a StatefulSet
/// pod ordinal that is >= the current replica count. StatefulSet pods are named
/// `{name}-{ordinal}` and scale-down removes the highest ordinals first.
pub async fn detect_orphaned_leases(
    client: &kube::Client,
    namespace: &str,
    cluster_prefix: &str,
    target_sts_name: &str,
    current_replicas: i32,
) -> Result<Vec<OrphanedLeaseInfo>, Error> {
    let lease_api: Api<Lease> = Api::namespaced(client.clone(), namespace);
    let label_selector = format!(
        "silo.dev/type=shard,silo.dev/cluster={}",
        cluster_prefix
    );
    let leases = lease_api
        .list(&ListParams::default().labels(&label_selector))
        .await?;

    let sts_prefix = format!("{}-", target_sts_name);
    let mut orphaned = Vec::new();

    for lease in leases.items {
        let holder = lease
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref())
            .unwrap_or("");

        if holder.is_empty() {
            continue;
        }

        // Only consider leases held by pods of our target StatefulSet
        if !holder.starts_with(&sts_prefix) {
            continue;
        }

        if let Some(ordinal) = extract_pod_ordinal(holder) {
            if ordinal >= current_replicas {
                let shard_id = lease
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("silo.dev/shard"))
                    .cloned()
                    .unwrap_or_default();

                orphaned.push(OrphanedLeaseInfo {
                    lease_name: lease.metadata.name.unwrap_or_default(),
                    shard_id,
                    holder_identity: holder.to_string(),
                    detected_at: chrono::Utc::now().to_rfc3339(),
                });
            }
        }
    }

    Ok(orphaned)
}

/// Compute the minimum replica count needed to cover all orphaned pods.
/// Returns at least `desired` replicas, but enough to include the highest
/// orphaned pod ordinal.
pub fn compute_recovery_replicas(orphans: &[OrphanedLeaseInfo], desired: i32) -> i32 {
    let max_orphan_ordinal = orphans
        .iter()
        .filter_map(|o| extract_pod_ordinal(&o.holder_identity))
        .max()
        .unwrap_or(desired - 1);

    std::cmp::max(desired, max_orphan_ordinal + 1)
}

/// Extract the pod ordinal from a StatefulSet pod name.
/// StatefulSet pods are named `{name}-{ordinal}`, e.g. `silo-0`, `silo-2`.
fn extract_pod_ordinal(pod_name: &str) -> Option<i32> {
    pod_name.rsplit('-').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_pod_ordinal() {
        assert_eq!(extract_pod_ordinal("silo-0"), Some(0));
        assert_eq!(extract_pod_ordinal("silo-1"), Some(1));
        assert_eq!(extract_pod_ordinal("silo-42"), Some(42));
        assert_eq!(extract_pod_ordinal("my-app-statefulset-3"), Some(3));
        assert_eq!(extract_pod_ordinal("no-number-here-abc"), None);
        assert_eq!(extract_pod_ordinal(""), None);
    }

    #[test]
    fn test_compute_recovery_replicas() {
        let orphans = vec![
            OrphanedLeaseInfo {
                lease_name: "lease-1".into(),
                shard_id: "shard-1".into(),
                holder_identity: "silo-3".into(),
                detected_at: "2024-01-01T00:00:00Z".into(),
            },
            OrphanedLeaseInfo {
                lease_name: "lease-2".into(),
                shard_id: "shard-2".into(),
                holder_identity: "silo-5".into(),
                detected_at: "2024-01-01T00:00:00Z".into(),
            },
        ];

        // desired=2, but orphan at ordinal 5 means we need at least 6 replicas
        assert_eq!(compute_recovery_replicas(&orphans, 2), 6);

        // desired=10, higher than any orphan, so keep 10
        assert_eq!(compute_recovery_replicas(&orphans, 10), 10);

        // empty orphans
        assert_eq!(compute_recovery_replicas(&[], 3), 3);
    }
}
