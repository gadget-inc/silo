//! Tests for coordination module types: ShardGuardState, ShardGuardContext,
//! CoordinatorBase, and ShardOwnerMap.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use silo::coordination::{
    CoordinationError, CoordinatorBase, MemberInfo, ShardGuardState, ShardOwnerMap, ShardPhase,
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

// --- ShardGuardContext tests ---

#[silo::test(flavor = "multi_thread")]
async fn wait_for_change_notify() {
    use silo::coordination::ShardGuardContext;

    let (_, shutdown_rx) = tokio::sync::watch::channel(false);
    let shard_id = ShardId::new();
    let ctx = Arc::new(ShardGuardContext::new(shard_id, shutdown_rx));

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
    let ctx = Arc::new(ShardGuardContext::new(shard_id, shutdown_rx));

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
    let ctx = ShardGuardContext::new(shard_id, shutdown_rx);

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
