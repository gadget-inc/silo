//! Tests for the cached `GetClusterInfo` fallback in `SiloService`.
//!
//! When the coordination layer (k8s API server, etcd, ...) is slow or
//! unreachable, `GetClusterInfo` should:
//!  - return the last successfully observed response from an in-memory cache,
//!  - within the configured timeout (so routing clients aren't blocked),
//!  - and fall back to `Status::unavailable` only if no cached response exists.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use silo::coordination::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap, SplitStorageBackend,
};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::silo_server::Silo;
use silo::pb::*;
use silo::server::SiloService;
use silo::settings::{Backend, DatabaseTemplate};
use silo::shard_range::{ShardId, ShardMap, SplitInProgress};
use tonic::Request;

/// Mock coordinator that can be told to hang on `get_shard_owner_map` /
/// `get_members`, simulating a stuck k8s API server.
struct HangingCoordinator {
    base: CoordinatorBase,
    hang: Arc<AtomicBool>,
    /// Per-call artificial latency for `get_shard_owner_map` / `get_members`
    /// (milliseconds). Used by the dedup test to create a window where many
    /// callers can race into the in-flight slot.
    delay_ms: Arc<AtomicU64>,
    owner_map_calls: Arc<AtomicUsize>,
    member_calls: Arc<AtomicUsize>,
}

impl HangingCoordinator {
    async fn new(node_id: &str, grpc_addr: &str, num_shards: u32) -> Self {
        let shard_map = ShardMap::create_initial(num_shards).expect("create shard map");
        let owned: HashSet<ShardId> = shard_map.shard_ids().into_iter().collect();
        let factory = Arc::new(ShardFactory::new_noop());
        let base = CoordinatorBase::new(node_id, grpc_addr, shard_map, factory, Vec::new());
        *base.owned.lock().await = owned;
        Self {
            base,
            hang: Arc::new(AtomicBool::new(false)),
            delay_ms: Arc::new(AtomicU64::new(0)),
            owner_map_calls: Arc::new(AtomicUsize::new(0)),
            member_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn set_hang(&self, hang: bool) {
        self.hang.store(hang, Ordering::SeqCst);
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    fn owner_map_call_count(&self) -> usize {
        self.owner_map_calls.load(Ordering::SeqCst)
    }

    fn member_call_count(&self) -> usize {
        self.member_calls.load(Ordering::SeqCst)
    }

    async fn maybe_hang(&self) {
        if self.hang.load(Ordering::SeqCst) {
            // Sleep longer than the server's 5s GetClusterInfo timeout so the
            // server is forced into the cache-fallback path.
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }
}

#[tonic::async_trait]
impl Coordinator for HangingCoordinator {
    fn base(&self) -> &CoordinatorBase {
        &self.base
    }

    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        self.member_calls.fetch_add(1, Ordering::SeqCst);
        self.maybe_hang().await;
        Ok(vec![MemberInfo {
            node_id: self.base.node_id.clone(),
            grpc_addr: self.base.grpc_addr.clone(),
            hostname: Some("test-host".to_string()),
            startup_time_ms: Some(0),
            placement_rings: self.base.placement_rings.clone(),
        }])
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
        self.owner_map_calls.fetch_add(1, Ordering::SeqCst);
        self.maybe_hang().await;
        let shard_map = self.base.shard_map.lock().await;
        let mut shard_to_node = HashMap::new();
        let mut shard_to_addr = HashMap::new();
        for shard in shard_map.shards() {
            shard_to_node.insert(shard.id, self.base.node_id.clone());
            shard_to_addr.insert(shard.id, self.base.grpc_addr.clone());
        }
        Ok(ShardOwnerMap {
            shard_map: shard_map.clone(),
            shard_to_node,
            shard_to_addr,
        })
    }

    async fn wait_converged(&self, _timeout: Duration) -> bool {
        true
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        Ok(())
    }

    async fn update_shard_placement_ring(
        &self,
        shard_id: &ShardId,
        ring: Option<&str>,
    ) -> Result<(Option<String>, Option<String>), CoordinationError> {
        let mut shard_map = self.base.shard_map.lock().await;
        let shard = shard_map
            .get_shard_mut(shard_id)
            .ok_or_else(|| CoordinationError::ShardNotFound(*shard_id))?;
        let previous = shard.placement_ring.clone();
        let current = ring.map(|s| s.to_string());
        shard.placement_ring = current.clone();
        Ok((previous, current))
    }

    async fn force_release_shard_lease(
        &self,
        _shard_id: &ShardId,
    ) -> Result<(), CoordinationError> {
        Ok(())
    }

    async fn reclaim_existing_leases(&self) -> Result<Vec<ShardId>, CoordinationError> {
        Ok(vec![])
    }
}

#[tonic::async_trait]
impl SplitStorageBackend for HangingCoordinator {
    async fn load_split(
        &self,
        _parent_shard_id: &ShardId,
    ) -> Result<Option<SplitInProgress>, CoordinationError> {
        Ok(None)
    }

    async fn store_split(&self, _split: &SplitInProgress) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn delete_split(&self, _parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn update_shard_map_for_split(
        &self,
        _split: &SplitInProgress,
    ) -> Result<(), CoordinationError> {
        Err(CoordinationError::NotSupported)
    }

    async fn reload_shard_map(&self) -> Result<(), CoordinationError> {
        Ok(())
    }

    async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError> {
        Ok(vec![])
    }
}

async fn create_test_service() -> (SiloService, Arc<HangingCoordinator>) {
    let coord =
        Arc::new(HangingCoordinator::new("cache-test-node", "http://localhost:7450", 4).await);

    let tmpdir = std::env::temp_dir().join(format!(
        "silo-cache-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let factory = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            ..Default::default()
        },
        MockGubernatorClient::new_arc(),
        None,
    ));

    let cfg = silo::settings::AppConfig::load(None).expect("load config");
    let svc = SiloService::new(factory, coord.clone(), cfg, None);
    (svc, coord)
}

/// Server returns the cached response within the timeout when the coordinator
/// hangs, and the cached payload matches what the live call returned earlier.
#[silo::test(flavor = "multi_thread")]
async fn get_cluster_info_serves_cache_when_coordinator_hangs() {
    let (svc, coord) = create_test_service().await;

    let live = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect("baseline GetClusterInfo should succeed")
        .into_inner();
    assert_eq!(live.num_shards, 4);
    assert_eq!(live.shard_owners.len(), 4);

    // Now make the coordinator hang. The next call must not block forever and
    // must return exactly what the cache holds from the baseline call.
    coord.set_hang(true);

    let started = Instant::now();
    let cached = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect("cached GetClusterInfo should succeed")
        .into_inner();
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(8),
        "cached response should arrive within the server timeout window, took {:?}",
        elapsed
    );
    assert!(
        elapsed >= Duration::from_secs(5),
        "cached response should only be returned after the timeout fires, took {:?}",
        elapsed
    );
    assert_eq!(cached.num_shards, live.num_shards);
    assert_eq!(cached.this_node_id, live.this_node_id);
    assert_eq!(cached.this_grpc_addr, live.this_grpc_addr);
    assert_eq!(cached.shard_owners.len(), live.shard_owners.len());
    let mut cached_ids: Vec<_> = cached.shard_owners.iter().map(|s| &s.shard_id).collect();
    let mut live_ids: Vec<_> = live.shard_owners.iter().map(|s| &s.shard_id).collect();
    cached_ids.sort();
    live_ids.sort();
    assert_eq!(cached_ids, live_ids);
}

/// With no successful prior call to populate the cache, the server returns
/// `Unavailable` rather than hanging or returning empty data.
#[silo::test(flavor = "multi_thread")]
async fn get_cluster_info_errors_when_no_cache_and_coordinator_hangs() {
    let (svc, coord) = create_test_service().await;
    coord.set_hang(true);

    let started = Instant::now();
    let err = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect_err("should fail when no cache and coordinator is unreachable");
    let elapsed = started.elapsed();

    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(
        elapsed < Duration::from_secs(8),
        "should fail within the timeout window, took {:?}",
        elapsed
    );
}

/// After the coordinator recovers, the next successful call refreshes the cache.
#[silo::test(flavor = "multi_thread")]
async fn get_cluster_info_refreshes_cache_after_recovery() {
    let (svc, coord) = create_test_service().await;

    let _ = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect("baseline call");

    coord.set_hang(true);
    let cached = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect("cached fallback")
        .into_inner();

    coord.set_hang(false);
    let started = Instant::now();
    let fresh = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect("call after recovery should succeed")
        .into_inner();
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "post-recovery call should be fast, took {:?}",
        elapsed
    );
    assert_eq!(fresh.num_shards, cached.num_shards);
    assert_eq!(fresh.shard_owners.len(), cached.shard_owners.len());
}

/// Concurrent callers should collapse onto a single in-flight coordinator
/// fetch rather than each issuing their own (thundering-herd defense). With
/// the coordinator artificially slow, all N callers initiated within the
/// fetch window must share the same underlying request.
#[silo::test(flavor = "multi_thread")]
async fn get_cluster_info_dedupes_concurrent_callers() {
    let (svc, coord) = create_test_service().await;
    // Slow enough that all callers will pile in before the first fetch resolves.
    coord.set_delay(Duration::from_millis(300));

    let svc = Arc::new(svc);
    let n = 50;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let svc = svc.clone();
        handles.push(tokio::spawn(async move {
            svc.get_cluster_info(Request::new(GetClusterInfoRequest {}))
                .await
                .expect("concurrent caller should succeed")
                .into_inner()
        }));
    }

    let mut responses = Vec::with_capacity(n);
    for h in handles {
        responses.push(h.await.expect("task joins cleanly"));
    }

    // Exactly one coordinator fetch should have occurred — every other
    // caller must have awaited the shared in-flight future.
    assert_eq!(
        coord.owner_map_call_count(),
        1,
        "thundering herd should collapse to one get_shard_owner_map call, got {}",
        coord.owner_map_call_count()
    );
    assert_eq!(
        coord.member_call_count(),
        1,
        "thundering herd should collapse to one get_members call, got {}",
        coord.member_call_count()
    );

    // All callers must observe the same response.
    let first = &responses[0];
    for (i, r) in responses.iter().enumerate() {
        assert_eq!(
            r.num_shards, first.num_shards,
            "response {i} disagrees with the first on num_shards"
        );
        assert_eq!(
            r.this_node_id, first.this_node_id,
            "response {i} disagrees with the first on this_node_id"
        );
        assert_eq!(
            r.shard_owners.len(),
            first.shard_owners.len(),
            "response {i} has a different shard_owners length"
        );
    }

    // After the herd resolves, the in-flight slot is cleared so a fresh
    // call hits the coordinator again.
    coord.set_delay(Duration::ZERO);
    let _ = svc
        .get_cluster_info(Request::new(GetClusterInfoRequest {}))
        .await
        .expect("post-herd call should succeed");
    assert_eq!(
        coord.owner_map_call_count(),
        2,
        "a fresh call after the herd should produce a new fetch, got {}",
        coord.owner_map_call_count()
    );
}
