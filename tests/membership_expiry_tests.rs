//! When a Kubernetes coordinator drops a member whose lease has gone stale,
//! run against the in-memory Kubernetes API with paused time.
//!
//! The observer is a real coordinator with one shard. Its peer is a lease
//! the test writes directly, with whatever renewTime the scenario needs: a
//! peer's lease "goes stale" when the test rewrites it with a renewTime a
//! TTL or more in the past, and "renews" when the test rewrites it with a
//! current one. Lease staleness is judged on the wall clock, which the
//! paused runtime does not move, so a renewTime stays fresh or stale for the
//! whole test; the observer's own listings and renewals run on the paused
//! clock, so a stall in them costs the test no wall-clock time.

#![cfg(feature = "k8s")]

#[path = "turmoil_runner/mock_k8s.rs"]
#[allow(dead_code)]
mod mock_k8s;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use mock_k8s::{K8sCall, MockK8sBackend, MockK8sState};
use silo::coordination::k8s::K8sCoordinator;
use silo::coordination::{Coordinator, K8sCoordinatorConfig, MemberInfo, keys};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::DatabaseTemplate;
use silo::shard_range::ShardId;

const NAMESPACE: &str = "default";
const OBSERVER: &str = "observer";
const PEER: &str = "peer";
const LEASE_TTL_SECS: i64 = 5;
const LEASE_TTL: Duration = Duration::from_secs(LEASE_TTL_SECS as u64);

/// How long the observer's listings hang. Longer than the TTL, so a peer
/// that stopped renewing when the stall began is a full TTL stale by the
/// time the listings recover.
const STALL: Duration = Duration::from_secs(20);

/// Longest any wait may take before the test fails naming the step.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// A coordinator with one shard on an in-memory store, observing the member
/// leases on an in-memory API server the test writes peer leases to.
struct Observer {
    /// Unique per observer, so the log lines of tests sharing the process
    /// can be told apart.
    cluster_prefix: String,
    state: Arc<MockK8sState>,
    backend: MockK8sBackend,
    coordinator: K8sCoordinator<MockK8sBackend>,
    _background: tokio::task::JoinHandle<()>,
}

impl Observer {
    async fn started() -> Self {
        let cluster_prefix = format!("expiry-{}", ShardId::new());
        let state = MockK8sState::new();
        let backend = MockK8sBackend::new(state.clone(), NAMESPACE);
        let (coordinator, background) = K8sCoordinator::start_with_backend(
            backend.clone(),
            K8sCoordinatorConfig {
                namespace: NAMESPACE.to_string(),
                cluster_prefix: cluster_prefix.clone(),
                node_id: OBSERVER.to_string(),
                grpc_addr: "http://127.0.0.1:0".to_string(),
                initial_shard_count: 1,
                lease_duration_secs: LEASE_TTL_SECS,
                placement_rings: Vec::new(),
            },
            Arc::new(ShardFactory::new(
                DatabaseTemplate {
                    // Memory stores are shared per root, so each observer
                    // gets a root of its own.
                    path: format!("membership-expiry-{}/%shard%", ShardId::new()),
                    ..Default::default()
                },
                MockGubernatorClient::new_arc(),
                None,
            )),
        )
        .await
        .expect("start the observer");
        Self {
            cluster_prefix,
            state,
            backend,
            coordinator,
            _background: background,
        }
    }

    /// The observer, with the peer listed as a live member and the
    /// observer's own calls healthy for longer than a TTL.
    async fn with_a_live_peer() -> Self {
        let observer = Self::started().await;
        observer.write_peer_lease(Utc::now()).await;
        observer
            .wait_until("the peer joins", || observer.sees_peer())
            .await;
        tokio::time::sleep(LEASE_TTL + Duration::from_secs(1)).await;
        assert!(observer.sees_peer().await);
        observer
    }

    async fn sees_peer(&self) -> bool {
        self.coordinator
            .get_members()
            .await
            .expect("the members cache")
            .iter()
            .any(|member| member.node_id == PEER)
    }

    fn reconciles(&self) -> u64 {
        self.coordinator.membership_notify_count()
    }

    fn peer_lease_name(&self) -> String {
        keys::k8s_member_lease_name(&self.cluster_prefix, PEER)
    }

    /// Write the peer's lease as the peer's own renewer would, renewed at
    /// `renew_time`.
    async fn write_peer_lease(&self, renew_time: DateTime<Utc>) {
        let member = MemberInfo {
            node_id: PEER.to_string(),
            grpc_addr: "http://127.0.0.1:1".to_string(),
            startup_time_ms: Some(1),
            hostname: None,
            placement_rings: Vec::new(),
        };
        let lease = Lease {
            metadata: kube::api::ObjectMeta {
                name: Some(self.peer_lease_name()),
                namespace: Some(NAMESPACE.to_string()),
                labels: Some(
                    [
                        ("silo.dev/type".to_string(), "member".to_string()),
                        ("silo.dev/cluster".to_string(), self.cluster_prefix.clone()),
                        ("silo.dev/node".to_string(), PEER.to_string()),
                    ]
                    .into(),
                ),
                annotations: Some(
                    [(
                        "silo.dev/member-info".to_string(),
                        serde_json::to_string(&member).expect("member info"),
                    )]
                    .into(),
                ),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(PEER.to_string()),
                lease_duration_seconds: Some(LEASE_TTL_SECS as i32),
                acquire_time: Some(MicroTime(renew_time)),
                renew_time: Some(MicroTime(renew_time)),
                ..Default::default()
            }),
        };
        self.state
            .apply_lease(NAMESPACE, &self.peer_lease_name(), lease)
            .await
            .expect("write the peer lease");
    }

    /// The peer stops renewing: its lease's renewTime falls two TTLs behind.
    async fn peer_goes_stale(&self) {
        self.write_peer_lease(Utc::now() - 2 * LEASE_TTL).await;
    }

    async fn peer_renews(&self) {
        self.write_peer_lease(Utc::now()).await;
    }

    async fn delete_peer_lease(&self) {
        self.state
            .delete_lease(NAMESPACE, &self.peer_lease_name())
            .await
            .expect("delete the peer lease");
    }

    /// Make every listing the observer starts in the next `STALL` hang, and
    /// return when the first one has. The observer's own renewals trigger a
    /// listing every second, so this is quick.
    async fn listings_start_hanging(&self) -> tokio::time::Instant {
        let before = self.backend.stalled_call_count();
        self.backend
            .stall_calls(K8sCall::ListLeases, "silo.dev/type=member", STALL, None);
        self.wait_until("a listing hangs", || async {
            self.backend.stalled_call_count() > before
        })
        .await;
        tokio::time::Instant::now()
    }

    async fn wait_until<F, Fut>(&self, step: &str, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + STEP_DEADLINE;
        while !condition().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{step}: not within {STEP_DEADLINE:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Check `condition` every 100ms for `duration`.
    async fn assert_throughout<F, Fut>(&self, what: &str, duration: Duration, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let started = tokio::time::Instant::now();
        while started.elapsed() < duration {
            assert!(
                condition().await,
                "{what}: not so {:?} in",
                started.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The observer's own calls have been answered all along, so a peer whose
/// lease goes a TTL without a renewal is gone: the first listing to see the
/// stale lease drops the peer and reconciles.
#[silo::test(start_paused = true)]
async fn a_stale_lease_is_expired_while_the_observers_calls_stay_healthy() {
    let observer = Observer::with_a_live_peer().await;
    let reconciles = observer.reconciles();

    observer.peer_goes_stale().await;

    observer
        .wait_until("the peer is dropped", || async {
            !observer.sees_peer().await
        })
        .await;
    assert_eq!(observer.reconciles(), reconciles + 1, "the drop reconciled");
}

/// The control plane stopped answering the observer: its listings hung past
/// their timeout. The peer, whose renewals the same control plane serves,
/// stopped renewing meanwhile. The first listing after the control plane
/// answers again sees the stale lease, and keeps the peer: the observer's
/// own blindness explains the staleness. The peer renews shortly after, as
/// a live peer does, and is never dropped. Nothing reconciles.
#[silo::test(start_paused = true)]
async fn a_stale_lease_seen_right_after_the_observers_calls_recover_is_kept() {
    let observer = Observer::with_a_live_peer().await;
    let reconciles = observer.reconciles();

    let stall_began = observer.listings_start_hanging().await;
    observer.peer_goes_stale().await;
    let recovered_at = stall_began + STALL;
    tokio::time::sleep_until(recovered_at + Duration::from_secs(1)).await;
    assert!(
        observer.backend.stalled_call_count() > 1,
        "listings kept hanging through the stall"
    );

    observer
        .assert_throughout(
            "the peer is kept while the observer's calls recover",
            Duration::from_secs(2),
            || observer.sees_peer(),
        )
        .await;
    observer.peer_renews().await;
    observer
        .assert_throughout(
            "the peer stays a member once it has renewed",
            2 * LEASE_TTL,
            || observer.sees_peer(),
        )
        .await;
    assert_eq!(observer.reconciles(), reconciles, "nothing reconciled");
}

/// As above, but the peer never renews: it really is gone. Once the
/// observer's calls have been answered for a full TTL, the stale lease is
/// evidence again, and the peer is dropped.
#[silo::test(start_paused = true)]
async fn a_lease_still_stale_a_ttl_after_the_observers_calls_recover_is_expired() {
    let observer = Observer::with_a_live_peer().await;
    let reconciles = observer.reconciles();

    let stall_began = observer.listings_start_hanging().await;
    observer.peer_goes_stale().await;
    let recovered_at = stall_began + STALL;

    tokio::time::sleep_until(recovered_at + Duration::from_secs(1)).await;
    observer
        .assert_throughout(
            "the peer is kept for a TTL after the observer's calls recover",
            LEASE_TTL - Duration::from_secs(2),
            || observer.sees_peer(),
        )
        .await;
    observer
        .wait_until("the peer is dropped", || async {
            !observer.sees_peer().await
        })
        .await;
    assert!(
        tokio::time::Instant::now() < recovered_at + LEASE_TTL + Duration::from_secs(2),
        "the peer was dropped within a renewal of the TTL elapsing"
    );
    assert_eq!(observer.reconciles(), reconciles + 1, "the drop reconciled");
}

/// A lease that is deleted (a planned shutdown, or an operator's delete) is
/// the strongest evidence there is: its member is gone at once, grace or
/// not.
#[silo::test(start_paused = true)]
async fn a_deleted_lease_removes_the_member_at_once_during_the_grace_window() {
    let observer = Observer::with_a_live_peer().await;

    let stall_began = observer.listings_start_hanging().await;
    observer.peer_goes_stale().await;
    tokio::time::sleep_until(stall_began + STALL + Duration::from_secs(1)).await;
    assert!(observer.sees_peer().await, "the stale peer is kept");

    observer.delete_peer_lease().await;

    let deleted_at = tokio::time::Instant::now();
    observer
        .wait_until("the peer is dropped", || async {
            !observer.sees_peer().await
        })
        .await;
    assert!(
        deleted_at.elapsed() < Duration::from_secs(1),
        "the peer was dropped at once, not after a TTL: {:?}",
        deleted_at.elapsed()
    );
}

/// Keeping a stale lease is logged with the member, how stale its lease is
/// and how long the observer's own calls have been answered, so the next
/// investigation can see the rule acting.
#[cfg(debug_assertions)]
#[silo::test(start_paused = true)]
async fn keeping_a_stale_lease_is_logged() {
    let observer = Observer::with_a_live_peer().await;

    let stall_began = observer.listings_start_hanging().await;
    observer.peer_goes_stale().await;
    silo::trace::start_log_capture();
    tokio::time::sleep_until(stall_began + STALL + Duration::from_secs(1)).await;
    let logs = silo::trace::stop_log_capture();

    assert!(observer.sees_peer().await, "the stale peer is kept");
    // Other tests in this process keep stale leases of their own meanwhile.
    let lines: Vec<&str> = logs
        .lines()
        .filter(|line| {
            line.contains("keeping a stale member lease") && line.contains(&observer.cluster_prefix)
        })
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "one line about the kept lease, however many listings keep it:\n{logs}"
    );
    let line = lines[0];
    for field in [
        format!("member={}", observer.peer_lease_name()),
        format!("lease_ttl_secs={LEASE_TTL_SECS}"),
        "stale_for_ms=".to_string(),
        "calls_healthy_for_ms=".to_string(),
    ] {
        assert!(line.contains(&field), "{field} missing from: {line}");
    }
}
