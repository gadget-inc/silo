//! Integration tests for SiloAutoscaler controller.
//!
//! These tests require:
//! - A running Kubernetes cluster with the SiloAutoscaler CRD installed
//! - A silo StatefulSet deployed in the test namespace
//! - The silo-autoscaler controller running
//!
//! Environment variables:
//!   AUTOSCALER_TEST_NAMESPACE - namespace to use (default: silo-autoscaler-test)
//!   AUTOSCALER_TEST_STS       - StatefulSet name (default: silo)
//!   AUTOSCALER_TEST_PREFIX    - cluster prefix for leases (default: silo-inttest)
//!
//! Local development:
//!   kubectl config use-context orbstack
//!   kubectl apply -f deploy/autoscaler-integration-test
//!   cargo test --package silo-autoscaler --test autoscaler_integration_tests -- --test-threads=1
//!
//! CI:
//!   Uses kind — see .github/workflows/ci.yml

use std::time::{Duration, Instant};

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use kube::Client;

use silo_autoscaler::crd::{SiloAutoscaler, SiloAutoscalerSpec};

fn namespace() -> String {
    std::env::var("AUTOSCALER_TEST_NAMESPACE").unwrap_or_else(|_| "silo-autoscaler-test".into())
}

fn sts_name() -> String {
    std::env::var("AUTOSCALER_TEST_STS").unwrap_or_else(|_| "silo".into())
}

fn cluster_prefix() -> String {
    std::env::var("AUTOSCALER_TEST_PREFIX").unwrap_or_else(|_| "silo-inttest".into())
}

const AUTOSCALER_NAME: &str = "silo-autoscaler-inttest";

async fn get_sts_replicas(client: &Client) -> i32 {
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace());
    let sts = sts_api.get(&sts_name()).await.unwrap();
    sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0)
}

async fn get_ready_pod_count(client: &Client) -> i32 {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace());
    let pods = pod_api.list(&ListParams::default().labels("app=silo")).await.unwrap();
    pods.items.iter().filter(|p| {
        p.metadata.deletion_timestamp.is_none()
            && p.status.as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
    }).count() as i32
}

async fn get_autoscaler_status(client: &Client) -> (i32, i32) {
    let api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), &namespace());
    match api.get(AUTOSCALER_NAME).await {
        Ok(sa) => {
            let status = sa.status.as_ref();
            let replicas = status.map(|s| s.replicas).unwrap_or(0);
            let orphans = status.map(|s| s.orphaned_lease_count).unwrap_or(0);
            (replicas, orphans)
        }
        Err(_) => (0, 0),
    }
}

/// Delete ALL SiloAutoscalers in the namespace to prevent conflicts,
/// clear stuck finalizers on terminating pods, then wait for the
/// StatefulSet to stabilize at the given replica count.
async fn setup(client: &Client, expected_replicas: i32) {
    let ns = namespace();
    let api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), &ns);
    let list = api.list(&ListParams::default()).await.unwrap();
    for sa in list.items {
        if let Some(name) = sa.metadata.name {
            let _ = api.delete(&name, &Default::default()).await;
        }
    }

    // Clear finalizers on any stuck terminating pods
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let pods = pod_api.list(&ListParams::default().labels("app=silo")).await.unwrap();
    for pod in &pods.items {
        if pod.metadata.deletion_timestamp.is_some() {
            if let Some(name) = &pod.metadata.name {
                let patch = serde_json::json!({"metadata": {"finalizers": []}});
                let _ = pod_api.patch(name, &PatchParams::default(), &Patch::Merge(&patch)).await;
            }
        }
    }

    // Restore StatefulSet to expected replicas
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns);
    let patch = serde_json::json!({"spec": {"replicas": expected_replicas}});
    let _ = sts_api.patch(&sts_name(), &PatchParams::default(), &Patch::Merge(&patch)).await;

    assert!(
        wait_for_replicas(client, expected_replicas, Duration::from_secs(120)).await,
        "setup: StatefulSet did not stabilize at {} replicas", expected_replicas
    );
}

async fn ensure_autoscaler(client: &Client, replicas: i32) {
    let ns = namespace();
    let api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), &ns);

    let sa = SiloAutoscaler::new(AUTOSCALER_NAME, SiloAutoscalerSpec {
        replicas,
        target_stateful_set: sts_name(),
        cluster_prefix: cluster_prefix(),
    });

    match api.get(AUTOSCALER_NAME).await {
        Ok(_) => {
            let patch = serde_json::json!({"spec": {"replicas": replicas}});
            api.patch(AUTOSCALER_NAME, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .unwrap();
        }
        Err(_) => {
            api.create(&PostParams::default(), &sa).await.unwrap();
        }
    }
}

async fn wait_for_sts_replicas_gte(client: &Client, min: i32, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if get_sts_replicas(client).await >= min {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    false
}

async fn wait_for_replicas(client: &Client, expected: i32, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let ready = get_ready_pod_count(client).await;
        let sts = get_sts_replicas(client).await;
        if ready == expected && sts == expected {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    false
}

async fn cleanup(client: &Client, restore_replicas: i32) {
    let ns = namespace();
    let api: Api<SiloAutoscaler> = Api::namespaced(client.clone(), &ns);
    let _ = api.delete(AUTOSCALER_NAME, &Default::default()).await;

    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns);
    let patch = serde_json::json!({"spec": {"replicas": restore_replicas}});
    let _ = sts_api.patch(&sts_name(), &PatchParams::default(), &Patch::Merge(&patch)).await;
}

#[tokio::test]
async fn test_scale_up() {
    let client = Client::try_default().await.unwrap();
    setup(&client, 3).await;

    ensure_autoscaler(&client, 4).await;

    assert!(
        wait_for_replicas(&client, 4, Duration::from_secs(120)).await,
        "StatefulSet did not scale up to 4 within timeout"
    );

    let (status_replicas, orphans) = get_autoscaler_status(&client).await;
    assert_eq!(status_replicas, 4);
    assert_eq!(orphans, 0);

    cleanup(&client, 3).await;
}

#[tokio::test]
async fn test_scale_down() {
    let client = Client::try_default().await.unwrap();
    setup(&client, 3).await;

    ensure_autoscaler(&client, 2).await;

    assert!(
        wait_for_replicas(&client, 2, Duration::from_secs(120)).await,
        "StatefulSet did not scale down to 2 within timeout"
    );

    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace());
    let pods = pod_api.list(&ListParams::default().labels("app=silo")).await.unwrap();
    let running: Vec<_> = pods.items.iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .collect();
    assert_eq!(running.len(), 2);

    cleanup(&client, 3).await;
}

#[tokio::test]
async fn test_scale_down_with_orphaned_lease_triggers_recovery() {
    let client = Client::try_default().await.unwrap();
    let ns = namespace();
    let prefix = cluster_prefix();
    let sts = sts_name();
    setup(&client, 3).await;

    let highest_pod = format!("{}-2", sts);
    let fake_lease_name = format!("{}-shard-fake-inttest-orphan", prefix);

    // Create a fake orphaned shard lease held by the highest-ordinal pod
    let lease_api: Api<Lease> = Api::namespaced(client.clone(), &ns);
    let lease: Lease = serde_json::from_value(serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": fake_lease_name,
            "namespace": ns,
            "labels": {
                "silo.dev/type": "shard",
                "silo.dev/cluster": prefix,
                "silo.dev/shard": "fake-inttest-orphan-shard"
            }
        },
        "spec": {
            "holderIdentity": highest_pod,
            "leaseDurationSeconds": 99999
        }
    })).unwrap();

    let _ = lease_api.delete(&fake_lease_name, &Default::default()).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    lease_api.create(&PostParams::default(), &lease).await.unwrap();

    // Scale down — controller should detect orphaned lease and scale back up
    ensure_autoscaler(&client, 2).await;

    // Wait for the controller to detect the orphan and scale back up
    let recovered = wait_for_sts_replicas_gte(&client, 3, Duration::from_secs(60)).await;
    assert!(recovered, "expected StatefulSet to scale back up for recovery");

    // Clean up fake lease — allows recovery to complete
    lease_api.delete(&fake_lease_name, &Default::default()).await.unwrap();

    // Wait for scale-down to complete
    assert!(
        wait_for_replicas(&client, 2, Duration::from_secs(120)).await,
        "StatefulSet did not reach target 2 after orphan lease cleanup"
    );

    cleanup(&client, 3).await;
}
