//! The Kubernetes membership lease renewer, run against the in-memory
//! Kubernetes API from the DST crate so that hung and conflicting calls can
//! be planted deterministically.
//!
//! Time is paused: a planted hang is skipped over by the runtime, so a call
//! timeout costs the test no wall-clock time.

#![cfg(feature = "k8s")]

#[path = "turmoil_runner/mock_k8s.rs"]
#[allow(dead_code)]
mod mock_k8s;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::coordination::v1::Lease;
use mock_k8s::{MockK8sBackend, MockK8sState, StalledCall};
use silo::coordination::MemberInfo;
use silo::coordination::k8s::MembershipLeaseRenewer;

const NAMESPACE: &str = "default";
const CLUSTER_PREFIX: &str = "renewal-test";
const NODE_ID: &str = "node-1";
const LEASE_TTL_SECS: i32 = 5;

const CALL_TIMEOUT: Duration = Duration::from_secs(1);
/// Room for one call to time out and one retry to run to its own limit.
const BUDGET: Duration = Duration::from_secs(2);
/// Longer than any budget, so a stalled call can only end in a timeout.
const HANG: Duration = Duration::from_secs(30);

fn member(node_id: &str) -> MemberInfo {
    MemberInfo {
        node_id: node_id.to_string(),
        grpc_addr: "http://127.0.0.1:0".to_string(),
        startup_time_ms: Some(1),
        hostname: None,
        placement_rings: Vec::new(),
    }
}

/// One node's renewer, with its lease already on the API server.
struct Member {
    state: Arc<MockK8sState>,
    backend: MockK8sBackend,
    renewer: MembershipLeaseRenewer<MockK8sBackend>,
}

impl Member {
    async fn registered() -> Self {
        let state = MockK8sState::new();
        let backend = MockK8sBackend::new(state.clone(), NAMESPACE);
        let mut renewer = MembershipLeaseRenewer::new(
            backend.clone(),
            NAMESPACE.to_string(),
            CLUSTER_PREFIX,
            member(NODE_ID),
            LEASE_TTL_SECS,
        );
        renewer.create().await.expect("create the membership lease");
        Self {
            state,
            backend,
            renewer,
        }
    }

    async fn stored_lease(&self) -> Lease {
        self.state
            .get_lease(NAMESPACE, self.renewer.lease_name())
            .await
            .expect("read the lease")
            .expect("the lease exists")
            .lease
    }

    /// Write the lease back as another client would, with `change` applied.
    /// Its resourceVersion moves, so the renewer's copy is stale.
    async fn overwrite_lease(&self, change: impl FnOnce(&mut Lease)) {
        let mut lease = self.stored_lease().await;
        change(&mut lease);
        self.state
            .replace_lease(NAMESPACE, self.renewer.lease_name(), lease)
            .await
            .expect("overwrite the lease");
    }
}

fn renew_time(lease: &Lease) -> DateTime<Utc> {
    lease
        .spec
        .as_ref()
        .and_then(|spec| spec.renew_time.as_ref())
        .expect("the lease has a renew time")
        .0
}

fn holder(lease: &Lease) -> Option<&str> {
    lease
        .spec
        .as_ref()
        .and_then(|spec| spec.holder_identity.as_deref())
}

/// The steady-state renewal is one write. A read of the lease would hang
/// past the budget, so the renewal can only succeed by writing straight away
/// with the resourceVersion it already holds.
#[silo::test(start_paused = true)]
async fn steady_state_renewal_writes_without_reading() {
    let mut node = Member::registered().await;
    let before = node.stored_lease().await;
    node.backend
        .stall_calls(StalledCall::GetLease, node.renewer.lease_name(), HANG, None);

    let renewal = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;

    renewal.result.expect("the renewal succeeds");
    assert_eq!(renewal.attempts, 1);
    assert_eq!(node.backend.stalled_call_count(), 0, "no read was made");
    let after = node.stored_lease().await;
    assert_ne!(
        after.metadata.resource_version, before.metadata.resource_version,
        "the write landed"
    );
    assert!(
        renew_time(&after) > renew_time(&before),
        "renewTime moved from {} to {}",
        renew_time(&before),
        renew_time(&after)
    );
}

/// A renewer that has not written the lease itself, as after a restart that
/// finds the lease already there, reads it before writing.
#[silo::test(start_paused = true)]
async fn first_renewal_reads_a_lease_it_has_not_written() {
    let node = Member::registered().await;
    let before = node.stored_lease().await;
    let mut restarted = MembershipLeaseRenewer::new(
        node.backend.clone(),
        NAMESPACE.to_string(),
        CLUSTER_PREFIX,
        member(NODE_ID),
        LEASE_TTL_SECS,
    );

    let renewal = restarted.renew(CALL_TIMEOUT, BUDGET).await;

    renewal.result.expect("the renewal succeeds");
    assert_eq!(renewal.attempts, 1);
    let after = node.stored_lease().await;
    assert!(renew_time(&after) > renew_time(&before));
    assert_eq!(holder(&after), Some(NODE_ID));
}

/// Another client rewrote the lease without changing its holder, so the
/// renewer's copy is stale. The conflicting write makes the renewal read the
/// lease back and write again against its current resourceVersion.
#[silo::test(start_paused = true)]
async fn a_conflicting_write_is_recovered_by_reading_the_lease() {
    let mut node = Member::registered().await;
    node.overwrite_lease(|_| {}).await;
    let overwritten = node.stored_lease().await;

    let renewal = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;

    renewal.result.expect("the renewal succeeds");
    assert_eq!(renewal.attempts, 1);
    let after = node.stored_lease().await;
    assert_ne!(
        after.metadata.resource_version, overwritten.metadata.resource_version,
        "the renewal wrote over the other client's version"
    );
    assert!(renew_time(&after) > renew_time(&overwritten));
}

/// The lease was handed to another holder behind this node's back. The
/// conflicting write makes the renewal read the lease, which reports the
/// other holder and leaves their lease alone.
#[silo::test(start_paused = true)]
async fn a_conflicting_write_reveals_a_changed_holder() {
    let mut node = Member::registered().await;
    node.overwrite_lease(|lease| {
        lease
            .spec
            .as_mut()
            .expect("the lease has a spec")
            .holder_identity = Some("node-2".to_string());
    })
    .await;
    let theirs = node.stored_lease().await;

    let renewal = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;

    let error = renewal.result.expect_err("the renewal fails");
    assert!(
        error
            .to_string()
            .contains("we are no longer the membership lease holder"),
        "unexpected error: {error}"
    );
    let after = node.stored_lease().await;
    assert_eq!(holder(&after), Some("node-2"));
    assert_eq!(
        after.metadata.resource_version,
        theirs.metadata.resource_version
    );
}

/// A lease that is gone cannot be renewed into existence: the write is
/// refused as not found, and the read that follows reports the deletion.
#[silo::test(start_paused = true)]
async fn a_deleted_lease_is_reported_as_deleted() {
    let mut node = Member::registered().await;
    node.state
        .delete_lease(NAMESPACE, node.renewer.lease_name())
        .await
        .expect("delete the lease");

    let renewal = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;

    let error = renewal.result.expect_err("the renewal fails");
    assert!(
        error.to_string().contains("membership lease was deleted"),
        "unexpected error: {error}"
    );
    assert!(
        node.state
            .get_lease(NAMESPACE, node.renewer.lease_name())
            .await
            .expect("read the lease")
            .is_none(),
        "the renewal did not recreate the lease"
    );
}

/// One write hangs past its timeout. The renewal has budget left, so it
/// writes again and the second write lands.
#[silo::test(start_paused = true)]
async fn a_timed_out_write_is_retried_once_within_the_budget() {
    let mut node = Member::registered().await;
    let before = node.stored_lease().await;
    node.backend.stall_calls(
        StalledCall::ReplaceLease,
        node.renewer.lease_name(),
        HANG,
        Some(1),
    );

    let started = tokio::time::Instant::now();
    let renewal = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;

    renewal.result.expect("the retry succeeds");
    assert_eq!(renewal.attempts, 2);
    assert_eq!(node.backend.stalled_call_count(), 1);
    let took = started.elapsed();
    assert!(
        took >= CALL_TIMEOUT && took < BUDGET,
        "the renewal waited out one call timeout and no more: {took:?}"
    );
    assert!(renew_time(&node.stored_lease().await) > renew_time(&before));
}

/// Both writes hang. The renewal gives up after the retry, inside its budget,
/// and the next renewal (whose calls are answered) reads the lease and
/// succeeds.
#[silo::test(start_paused = true)]
async fn two_timed_out_writes_fail_the_renewal() {
    let mut node = Member::registered().await;
    let before = node.stored_lease().await;
    node.backend.stall_calls(
        StalledCall::ReplaceLease,
        node.renewer.lease_name(),
        HANG,
        Some(2),
    );

    let started = tokio::time::Instant::now();
    let renewal = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;

    let error = renewal.result.expect_err("the renewal fails");
    assert!(
        error
            .to_string()
            .contains("replace membership lease timed out"),
        "unexpected error: {error}"
    );
    assert_eq!(renewal.attempts, 2);
    assert_eq!(node.backend.stalled_call_count(), 2);
    let took = started.elapsed();
    assert!(
        took >= BUDGET && took < BUDGET + Duration::from_millis(100),
        "the renewal used its budget and no more: {took:?}"
    );
    assert_eq!(
        node.stored_lease().await.metadata.resource_version,
        before.metadata.resource_version,
        "neither write landed"
    );

    let next = node.renewer.renew(CALL_TIMEOUT, BUDGET).await;
    next.result.expect("the next renewal succeeds");
    assert_eq!(next.attempts, 1);
    assert!(renew_time(&node.stored_lease().await) > renew_time(&before));
}

/// A timed-out write is retried only while enough budget remains for the
/// retry to be worth making: at least half a call timeout.
#[silo::test(start_paused = true)]
async fn a_timeout_with_little_budget_left_is_not_retried() {
    let mut node = Member::registered().await;
    node.backend.stall_calls(
        StalledCall::ReplaceLease,
        node.renewer.lease_name(),
        HANG,
        Some(1),
    );
    let tight_budget = CALL_TIMEOUT + CALL_TIMEOUT / 4;

    let renewal = node.renewer.renew(CALL_TIMEOUT, tight_budget).await;

    renewal.result.expect_err("the renewal fails");
    assert_eq!(renewal.attempts, 1);
    assert_eq!(node.backend.stalled_call_count(), 1);
}

/// The renewal cadence, budget and call timeout all follow from the lease
/// TTL: renew a third of the way through it, give a renewal until the next
/// tick, and give each call half of that so one retry fits.
#[test]
fn renewal_timing_follows_the_lease_ttl() {
    use silo::coordination::k8s::{
        membership_call_timeout, membership_renew_interval, membership_renewal_budget,
    };

    let cases = [
        // (lease TTL secs, renew interval, budget, call timeout), in ms
        (3, 1_000, 1_000, 500),
        (5, 1_000, 1_000, 500),
        (15, 5_000, 5_000, 2_500),
        (30, 10_000, 10_000, 5_000),
        (60, 20_000, 20_000, 5_000),
    ];
    for (ttl_secs, interval_ms, budget_ms, timeout_ms) in cases {
        let interval = membership_renew_interval(ttl_secs);
        let budget = membership_renewal_budget(interval);
        let timeout = membership_call_timeout(budget);
        assert_eq!(
            (
                interval.as_millis(),
                budget.as_millis(),
                timeout.as_millis()
            ),
            (interval_ms, budget_ms, timeout_ms),
            "lease TTL {ttl_secs}s: (interval, budget, call timeout)"
        );
    }
}
