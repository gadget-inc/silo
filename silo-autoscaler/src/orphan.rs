use k8s_openapi::api::coordination::v1::Lease;
use kube::{Api, api::ListParams};

use crate::crd::OrphanedLeaseInfo;
use crate::error::Error;

/// List shard leases currently held by a specific pod.
///
/// Queries leases with labels `silo.dev/type=shard,silo.dev/cluster={cluster_prefix}`
/// and returns those whose `holderIdentity` matches the given pod name.
pub async fn leases_held_by_pod(
    client: &kube::Client,
    namespace: &str,
    cluster_prefix: &str,
    pod_name: &str,
) -> Result<Vec<OrphanedLeaseInfo>, Error> {
    let lease_api: Api<Lease> = Api::namespaced(client.clone(), namespace);
    let label_selector = format!(
        "silo.dev/type=shard,silo.dev/cluster={}",
        cluster_prefix
    );
    let leases = lease_api
        .list(&ListParams::default().labels(&label_selector))
        .await?;

    let mut held = Vec::new();

    for lease in leases.items {
        let holder = lease
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref())
            .unwrap_or("");

        if holder == pod_name {
            let shard_id = lease
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("silo.dev/shard"))
                .cloned()
                .unwrap_or_default();

            held.push(OrphanedLeaseInfo {
                lease_name: lease.metadata.name.unwrap_or_default(),
                shard_id,
                holder_identity: holder.to_string(),
                detected_at: chrono::Utc::now().to_rfc3339(),
            });
        }
    }

    Ok(held)
}

/// Extract the pod ordinal from a StatefulSet pod name.
/// StatefulSet pods are named `{name}-{ordinal}`, e.g. `silo-0`, `silo-2`.
pub fn extract_pod_ordinal(pod_name: &str) -> Option<i32> {
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
}
