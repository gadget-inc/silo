//! Tests for coordination module types: ShardGuardState, ShardGuardContext,
//! CoordinatorBase, and ShardOwnerMap.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use silo::coordination::{
    CloseRetryBackoff, CoordinationError, CoordinatorBase, MemberInfo, ShardGuardState,
    ShardOwnerMap, ShardPhase,
};
use silo::factory::ShardFactory;
use silo::shard_range::{ShardId, ShardMap};

// --- ShardGuardState tests ---

#[silo::test]
fn compute_transition_idle_to_acquiring() {
    let mut state: ShardGuardState<Vec<u8>> = ShardGuardState::new();
    assert_eq!(state.phase, ShardPhase::Idle);
    assert!(!state.desired);
    assert!(!state.has_token());

    // Set desired=true, no token → should transition to Acquiring
    state.desired = true;
    let transition = state.compute_transition();
    assert_eq!(transition, Some(ShardPhase::Acquiring));
}

#[silo::test]
fn compute_transition_held_to_releasing() {
    let mut state: ShardGuardState<String> = ShardGuardState::new();
    state.phase = ShardPhase::Held;
    state.desired = true;
    state.ownership_token = Some("resource-v1".to_string());

    // Now set desired=false while holding token → should transition to Releasing
    state.desired = false;
    let transition = state.compute_transition();
    assert_eq!(transition, Some(ShardPhase::Releasing));
}

#[silo::test]
fn compute_transition_no_change_cases() {
    // ShutDown - no transition regardless of desired/token
    let mut state: ShardGuardState<u64> = ShardGuardState::new();
    state.phase = ShardPhase::ShutDown;
    state.desired = true;
    assert_eq!(state.compute_transition(), None);

    // ShuttingDown - no transition
    state.phase = ShardPhase::ShuttingDown;
    state.desired = false;
    assert_eq!(state.compute_transition(), None);

    // Idle + desired=false → stable, no transition
    state.phase = ShardPhase::Idle;
    state.desired = false;
    assert_eq!(state.compute_transition(), None);

    // Held + desired=true → stable, no transition
    state.phase = ShardPhase::Held;
    state.desired = true;
    state.ownership_token = Some(42);
    assert_eq!(state.compute_transition(), None);

    // Acquiring + desired=true → in-progress, no transition
    state.phase = ShardPhase::Acquiring;
    state.desired = true;
    state.ownership_token = None;
    assert_eq!(state.compute_transition(), None);

    // Releasing + desired=false → in-progress, no transition
    state.phase = ShardPhase::Releasing;
    state.desired = false;
    state.ownership_token = Some(99);
    assert_eq!(state.compute_transition(), None);
}

#[silo::test]
fn maybe_transition_applies_and_stops() {
    let mut state: ShardGuardState<Vec<u8>> = ShardGuardState::new();
    state.desired = true;

    // First call should transition Idle → Acquiring
    assert!(state.maybe_transition());
    assert_eq!(state.phase, ShardPhase::Acquiring);

    // Second call should not transition (Acquiring + desired=true is stable)
    assert!(!state.maybe_transition());
    assert_eq!(state.phase, ShardPhase::Acquiring);
}

#[silo::test]
fn shard_guard_state_default() {
    let state: ShardGuardState<String> = ShardGuardState::default();
    assert_eq!(state.phase, ShardPhase::Idle);
    assert!(!state.desired);
    assert!(!state.has_token());
    assert!(state.ownership_token.is_none());
}

// --- ShardOwnerMap tests ---

#[silo::test]
fn shard_owner_map_shard_for_tenant() {
    let shard_map = ShardMap::create_initial(2).expect("create shard map");
    let shard_ids = shard_map.shard_ids();

    let mut shard_to_addr = HashMap::new();
    let mut shard_to_node = HashMap::new();
    for id in &shard_ids {
        shard_to_addr.insert(*id, format!("http://node-{}", id));
        shard_to_node.insert(*id, format!("node-{}", id));
    }

    let owner_map = ShardOwnerMap {
        shard_map: shard_map.clone(),
        shard_to_addr,
        shard_to_node,
    };

    // With 2 shards, any tenant should map to one of them
    let result = owner_map.shard_for_tenant("some-tenant");
    assert!(result.is_some());
    assert!(shard_ids.contains(&result.unwrap()));

    // num_shards and shard_ids should match
    assert_eq!(owner_map.num_shards(), 2);
    assert_eq!(owner_map.shard_ids().len(), 2);
}

#[silo::test]
fn shard_owner_map_get_addr_and_node() {
    let shard_map = ShardMap::create_initial(2).expect("create shard map");
    let shard_ids = shard_map.shard_ids();
    let shard0 = shard_ids[0];
    let shard1 = shard_ids[1];

    let mut shard_to_addr = HashMap::new();
    let mut shard_to_node = HashMap::new();
    shard_to_addr.insert(shard0, "http://node-a:9910".to_string());
    shard_to_node.insert(shard0, "node-a".to_string());
    // shard1 intentionally not in the maps

    let owner_map = ShardOwnerMap {
        shard_map,
        shard_to_addr,
        shard_to_node,
    };

    // Known shard
    assert_eq!(
        owner_map.get_addr(&shard0),
        Some(&"http://node-a:9910".to_string())
    );
    assert_eq!(owner_map.get_node(&shard0), Some(&"node-a".to_string()));

    // Unknown shard (not in ownership maps)
    assert_eq!(owner_map.get_addr(&shard1), None);
    assert_eq!(owner_map.get_node(&shard1), None);

    // Completely unknown shard ID
    let unknown = ShardId::new();
    assert_eq!(owner_map.get_addr(&unknown), None);
    assert_eq!(owner_map.get_node(&unknown), None);
}

/// Build an owner map over `rings.len()` shards, pinning shard `i` to
/// `rings[i]` (`None` = default ring) and assigning an owner to every shard
/// except those listed in `unowned`.
fn owner_map_with_rings(rings: &[Option<&str>], unowned: &[usize]) -> ShardOwnerMap {
    let mut shard_map = ShardMap::create_initial(rings.len() as u32).expect("create shard map");
    let shard_ids = shard_map.shard_ids();
    for (i, ring) in rings.iter().enumerate() {
        shard_map
            .get_shard_mut(&shard_ids[i])
            .expect("shard exists")
            .set_placement_ring(ring.map(str::to_string));
    }

    let mut shard_to_addr = HashMap::new();
    let mut shard_to_node = HashMap::new();
    for (i, id) in shard_ids.iter().enumerate() {
        if unowned.contains(&i) {
            continue;
        }
        shard_to_addr.insert(*id, format!("http://node-{}", id));
        shard_to_node.insert(*id, format!("node-{}", id));
    }

    ShardOwnerMap {
        shard_map,
        shard_to_addr,
        shard_to_node,
    }
}

#[silo::test]
fn unassigned_shards_reports_named_ring_shard_absent_from_assignments() {
    let owner_map = owner_map_with_rings(&[Some("heavy"), None], &[0]);
    let stranded = owner_map.shard_ids()[0];

    let unassigned = owner_map.unassigned_shards();

    assert_eq!(
        unassigned,
        BTreeMap::from([(stranded, "heavy".to_string())]),
        "only the heavy-ring shard lacks an owner"
    );
}

#[silo::test]
fn unassigned_shards_reports_default_ring_shard_under_default_label() {
    let owner_map = owner_map_with_rings(&[Some("heavy"), None], &[1]);
    let stranded = owner_map.shard_ids()[1];

    let unassigned = owner_map.unassigned_shards();

    assert_eq!(
        unassigned,
        BTreeMap::from([(stranded, "default".to_string())]),
        "a default-ring shard is reported under the `default` label"
    );
}

#[silo::test]
fn unassigned_shards_is_empty_when_every_shard_is_assigned() {
    let owner_map = owner_map_with_rings(&[Some("heavy"), None, None], &[]);

    assert!(
        owner_map.unassigned_shards().is_empty(),
        "every shard has an owner"
    );
}

// --- ShardGuardContext tests ---

#[silo::test(flavor = "multi_thread")]
async fn wait_for_change_notify() {
    use silo::coordination::ShardGuardContext;

    let (_, shutdown_rx) = tokio::sync::watch::channel(false);
    let shard_id = ShardId::new();
    let ctx = Arc::new(ShardGuardContext::<()>::new(shard_id, shutdown_rx));

    let ctx_clone = ctx.clone();
    let handle = tokio::spawn(async move {
        ctx_clone.wait_for_change().await;
    });

    // Give the spawned task a moment to start waiting
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Notify should unblock wait_for_change
    ctx.notify.notify_one();

    // Should complete within a reasonable timeout
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("timed out waiting for notify")
        .expect("task panicked");
}

#[silo::test(flavor = "multi_thread")]
async fn wait_for_change_shutdown() {
    use silo::coordination::ShardGuardContext;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shard_id = ShardId::new();
    let ctx = Arc::new(ShardGuardContext::<()>::new(shard_id, shutdown_rx));

    let ctx_clone = ctx.clone();
    let handle = tokio::spawn(async move {
        ctx_clone.wait_for_change().await;
    });

    // Give the spawned task a moment to start waiting
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Shutdown signal should unblock wait_for_change
    shutdown_tx.send(true).expect("send shutdown");

    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("timed out waiting for shutdown")
        .expect("task panicked");
}

#[silo::test]
fn is_shutdown_reflects_channel() {
    use silo::coordination::ShardGuardContext;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shard_id = ShardId::new();
    let ctx = ShardGuardContext::<()>::new(shard_id, shutdown_rx);

    assert!(!ctx.is_shutdown());

    shutdown_tx.send(true).expect("send shutdown");

    assert!(ctx.is_shutdown());
}

// --- CoordinatorBase tests ---

fn make_coordinator_base(node_id: &str, num_shards: u32) -> CoordinatorBase {
    let shard_map = ShardMap::create_initial(num_shards).expect("create shard map");
    let factory = Arc::new(ShardFactory::new_noop());
    CoordinatorBase::new(
        node_id,
        "http://localhost:9910",
        shard_map,
        factory,
        Vec::new(),
    )
}

#[silo::test]
async fn wait_converged_timeout() {
    let base = make_coordinator_base("node-1", 2);
    // owned is empty, desired will have shards → should not converge

    let get_members = || async {
        Ok::<Vec<MemberInfo>, CoordinationError>(vec![MemberInfo {
            node_id: "node-1".to_string(),
            grpc_addr: "http://localhost:9910".to_string(),
            startup_time_ms: None,
            hostname: None,
            placement_rings: Vec::new(),
        }])
    };

    let converged = base
        .wait_converged(Duration::from_millis(200), get_members)
        .await;
    assert!(!converged, "should not converge when owned != desired");
}

#[silo::test]
async fn wait_converged_success() {
    let base = make_coordinator_base("node-1", 2);

    // Pre-populate owned with all shards so it matches desired
    {
        let shard_map = base.shard_map.lock().await;
        let mut owned = base.owned.lock().await;
        for shard_info in shard_map.shards() {
            owned.insert(shard_info.id);
        }
    }

    let get_members = || async {
        Ok::<Vec<MemberInfo>, CoordinationError>(vec![MemberInfo {
            node_id: "node-1".to_string(),
            grpc_addr: "http://localhost:9910".to_string(),
            startup_time_ms: None,
            hostname: None,
            placement_rings: Vec::new(),
        }])
    };

    let converged = base
        .wait_converged(Duration::from_secs(2), get_members)
        .await;
    assert!(converged, "should converge when owned == desired");
}

#[silo::test]
async fn coordinator_base_owned_shards_sorted() {
    let base = make_coordinator_base("node-1", 3);

    let shard_ids = base.shard_ids().await;
    // Insert in reverse order
    {
        let mut owned = base.owned.lock().await;
        for id in shard_ids.iter().rev() {
            owned.insert(*id);
        }
    }

    let owned = base.owned_shards().await;
    // Should be sorted by string representation
    let strs: Vec<String> = owned.iter().map(|id| id.to_string()).collect();
    let mut sorted = strs.clone();
    sorted.sort();
    assert_eq!(strs, sorted, "owned_shards should be sorted");
}

#[silo::test]
async fn coordinator_base_signal_shutdown() {
    let base = make_coordinator_base("node-1", 1);

    assert!(!*base.shutdown_rx.borrow());
    base.signal_shutdown();
    assert!(*base.shutdown_rx.borrow());
}

// --- ShardPhase Display tests ---

#[silo::test]
fn shard_phase_display() {
    assert_eq!(format!("{}", ShardPhase::Idle), "Idle");
    assert_eq!(format!("{}", ShardPhase::Acquiring), "Acquiring");
    assert_eq!(format!("{}", ShardPhase::Held), "Held");
    assert_eq!(format!("{}", ShardPhase::Releasing), "Releasing");
    assert_eq!(format!("{}", ShardPhase::ShuttingDown), "ShuttingDown");
    assert_eq!(format!("{}", ShardPhase::ShutDown), "ShutDown");
}

// --- compute_shard_owner_map test ---

#[silo::test]
async fn compute_shard_owner_map_basic() {
    let base = make_coordinator_base("node-1", 2);

    let members = vec![MemberInfo {
        node_id: "node-1".to_string(),
        grpc_addr: "http://localhost:9910".to_string(),
        startup_time_ms: None,
        hostname: None,
        placement_rings: Vec::new(),
    }];

    let owner_map = base.compute_shard_owner_map(&members).await;

    // With a single member, all shards should be owned by that member
    assert_eq!(owner_map.num_shards(), 2);
    for shard_id in owner_map.shard_ids() {
        assert_eq!(
            owner_map.get_addr(&shard_id),
            Some(&"http://localhost:9910".to_string())
        );
        assert_eq!(owner_map.get_node(&shard_id), Some(&"node-1".to_string()));
    }
}

// --- CloseRetryBackoff ---

#[silo::test]
fn close_retry_backoff_doubles_from_one_second_to_a_thirty_second_cap() {
    let mut backoff = CloseRetryBackoff::new();

    let delays: Vec<u64> = (0..7).map(|_| backoff.next_delay().as_secs()).collect();

    assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30]);
    assert_eq!(
        backoff.attempts(),
        7,
        "each delay records one failed attempt"
    );
}

#[silo::test]
fn close_retry_backoff_returns_to_one_second_after_reset() {
    let mut backoff = CloseRetryBackoff::new();
    for _ in 0..6 {
        backoff.next_delay();
    }

    backoff.reset();

    assert_eq!(backoff.attempts(), 0);
    assert_eq!(backoff.next_delay().as_secs(), 1);
}

// --- ShardGuardContext::wait_close_backoff ---

fn spawn_close_backoff_wait(
    ctx: &Arc<silo::coordination::ShardGuardContext<()>>,
    delay: Duration,
) -> tokio::task::JoinHandle<bool> {
    let ctx = Arc::clone(ctx);
    tokio::spawn(async move { ctx.wait_close_backoff(delay).await })
}

#[silo::test(flavor = "multi_thread")]
async fn close_backoff_wait_ignores_notify_and_returns_on_shutdown_signal() {
    use silo::coordination::ShardGuardContext;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ctx = Arc::new(ShardGuardContext::<()>::new(ShardId::new(), shutdown_rx));
    let wait = spawn_close_backoff_wait(&ctx, Duration::from_secs(30));
    tokio::time::sleep(Duration::from_millis(10)).await;

    // A `desired` flap notifies the guard; it must not shorten the backoff.
    ctx.notify.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !wait.is_finished(),
        "a notify alone should not end the close backoff"
    );

    shutdown_tx.send(true).expect("send shutdown");
    let shutdown_observed = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("close backoff should end promptly once shutdown is signalled")
        .expect("task panicked");
    assert!(shutdown_observed);
}

/// Coordinator shutdown reaches a guard as `trigger_shutdown` plus a notify,
/// before the shutdown channel fires.
#[silo::test(flavor = "multi_thread")]
async fn close_backoff_wait_returns_when_guard_shutdown_is_triggered() {
    use silo::coordination::ShardGuardContext;

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ctx = Arc::new(ShardGuardContext::<()>::new(ShardId::new(), shutdown_rx));
    let wait = spawn_close_backoff_wait(&ctx, Duration::from_secs(30));
    tokio::time::sleep(Duration::from_millis(10)).await;

    ctx.trigger_shutdown().await;
    ctx.notify.notify_one();

    let shutdown_observed = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("close backoff should end promptly once the guard is shutting down")
        .expect("task panicked");
    assert!(shutdown_observed);
}

#[silo::test(flavor = "multi_thread")]
async fn close_backoff_wait_elapses_without_shutdown() {
    use silo::coordination::ShardGuardContext;

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ctx = Arc::new(ShardGuardContext::<()>::new(ShardId::new(), shutdown_rx));

    let shutdown_observed = ctx.wait_close_backoff(Duration::from_millis(20)).await;

    assert!(!shutdown_observed);
}

/// `desired` can flap many times during one backoff. The notifies must leave
/// the deadline where it was: neither ending the wait nor restarting it.
#[silo::test(flavor = "multi_thread")]
async fn close_backoff_wait_keeps_its_deadline_across_notifies() {
    use silo::coordination::ShardGuardContext;

    const DELAY: Duration = Duration::from_secs(1);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ctx = Arc::new(ShardGuardContext::<()>::new(ShardId::new(), shutdown_rx));
    let started = std::time::Instant::now();
    let wait = spawn_close_backoff_wait(&ctx, DELAY);

    // Notifies spread across most of the delay, the last at 900ms. A wait that
    // restarted its timer on each one would run to at least 1.9s.
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        ctx.notify.notify_one();
    }

    let shutdown_observed = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the close backoff should elapse")
        .expect("task panicked");
    let elapsed = started.elapsed();
    assert!(!shutdown_observed, "no shutdown was signalled");
    assert!(
        elapsed >= DELAY,
        "notifies should not end the backoff early: elapsed {elapsed:?}, delay {DELAY:?}"
    );
    assert!(
        elapsed < DELAY + Duration::from_millis(700),
        "notifies should not restart the backoff: elapsed {elapsed:?}, delay {DELAY:?}"
    );
}

/// A dropped shutdown sender means the coordinator is gone.
#[silo::test(flavor = "multi_thread")]
async fn close_backoff_wait_returns_when_the_shutdown_sender_is_dropped() {
    use silo::coordination::ShardGuardContext;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ctx = Arc::new(ShardGuardContext::<()>::new(ShardId::new(), shutdown_rx));
    let wait = spawn_close_backoff_wait(&ctx, Duration::from_secs(30));
    tokio::time::sleep(Duration::from_millis(10)).await;

    drop(shutdown_tx);

    let shutdown_observed = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("close backoff should end promptly once the shutdown sender is gone")
        .expect("task panicked");
    assert!(shutdown_observed);
}

#[silo::test]
fn guard_state_exposes_failed_close_attempts() {
    let mut state: ShardGuardState<()> = ShardGuardState::new();
    assert_eq!(state.failed_close_attempts(), 0);

    state.close_backoff.next_delay();
    state.close_backoff.next_delay();

    assert_eq!(state.failed_close_attempts(), 2);
}
