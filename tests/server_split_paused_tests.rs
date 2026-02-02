//! Tests for server-side split-paused behavior.
//!
//! These tests verify [SILO-ROUTE-PAUSED-1]: when a shard is paused for splitting,
//! the gRPC server should return UNAVAILABLE to tell clients to retry.
//!
//! We use a mock coordinator to simulate the paused state without requiring
//! an actual split operation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use silo::coordination::{
    CoordinationError, Coordinator, CoordinatorBase, MemberInfo, ShardOwnerMap, SplitStorageBackend,
};
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::server::SiloService;
use silo::settings::{Backend, DatabaseTemplate};
use silo::shard_range::{ShardId, ShardMap, ShardRange, SplitInProgress};

use silo::pb::silo_server::Silo;
use silo::pb::*;
use tonic::Request;

mod test_helpers;
use test_helpers::msgpack_payload;

/// A mock coordinator that allows controlling whether a shard is paused
struct MockPausedCoordinator {
    base: CoordinatorBase,
    shard_paused: Arc<AtomicBool>,
}

impl MockPausedCoordinator {
    async fn new(
        node_id: impl Into<String>,
        grpc_addr: impl Into<String>,
        num_shards: u32,
    ) -> Self {
        let shard_map = ShardMap::create_initial(num_shards).expect("create shard map");
        let owned_shards: HashSet<ShardId> = shard_map.shard_ids().into_iter().collect();
        let factory = Arc::new(ShardFactory::new_noop());
        let base = CoordinatorBase::new(node_id, grpc_addr, shard_map, factory);
        // Pre-populate owned shards
        *base.owned.lock().await = owned_shards;
        Self {
            base,
            shard_paused: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_paused(&self, paused: bool) {
        self.shard_paused.store(paused, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl Coordinator for MockPausedCoordinator {
    fn base(&self) -> &CoordinatorBase {
        &self.base
    }

    async fn get_members(&self) -> Result<Vec<MemberInfo>, CoordinationError> {
        Ok(vec![MemberInfo {
            node_id: self.base.node_id.clone(),
            grpc_addr: self.base.grpc_addr.clone(),
            hostname: Some("test-host".to_string()),
            startup_time_ms: Some(0),
        }])
    }

    async fn get_shard_owner_map(&self) -> Result<ShardOwnerMap, CoordinationError> {
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

    async fn wait_converged(&self, _timeout: std::time::Duration) -> bool {
        true
    }

    async fn shutdown(&self) -> Result<(), CoordinationError> {
        Ok(())
    }

    /// [SILO-ROUTE-PAUSED-1] Returns the paused state as controlled by the test.
    /// Overrides the default trait implementation to use our test flag.
    async fn is_shard_paused(&self, _shard_id: ShardId) -> bool {
        self.shard_paused.load(Ordering::SeqCst)
    }
}

/// Mock does not support split operations - returns errors or empty results.
#[tonic::async_trait]
impl SplitStorageBackend for MockPausedCoordinator {
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

/// Helper to create a test service with mock coordinator
async fn create_test_service(paused: bool) -> (SiloService, Arc<MockPausedCoordinator>, ShardId) {
    let coord =
        Arc::new(MockPausedCoordinator::new("test-node", "http://localhost:50051", 1).await);
    coord.set_paused(paused);

    let shard_ids: Vec<ShardId> = coord.owned_shards().await;
    let shard_id = shard_ids[0];

    let tmpdir = std::env::temp_dir().join(format!(
        "silo-pause-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let factory = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    ));

    // Open the shard so we have a local shard
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let cfg = silo::settings::AppConfig::load(None).expect("load config");
    let svc = SiloService::new(factory, coord.clone(), cfg, None);

    (svc, coord, shard_id)
}

/// [SILO-ROUTE-PAUSED-1] Enqueue should return UNAVAILABLE when shard is paused
#[silo::test]
async fn enqueue_returns_unavailable_when_shard_paused() {
    let (svc, coord, shard_id) = create_test_service(false).await;

    // First request should succeed when not paused
    let req = Request::new(EnqueueRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "job-1".to_string(),
        priority: 0,
        start_at_ms: 0,
        retry_policy: None,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_payload(
                &serde_json::json!({"data": "test"}),
            ))),
        }),
        limits: vec![],
        metadata: HashMap::new(),
        task_group: "default".to_string(),
    });

    let result = svc.enqueue(req).await;
    assert!(result.is_ok(), "enqueue should succeed when not paused");

    // Now pause the shard
    coord.set_paused(true);

    // Second request should fail with UNAVAILABLE
    let req = Request::new(EnqueueRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "job-2".to_string(),
        priority: 0,
        start_at_ms: 0,
        retry_policy: None,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_payload(
                &serde_json::json!({"data": "test"}),
            ))),
        }),
        limits: vec![],
        metadata: HashMap::new(),
        task_group: "default".to_string(),
    });

    let result = svc.enqueue(req).await;
    assert!(result.is_err(), "enqueue should fail when paused");

    let status = result.err().unwrap();
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "should return UNAVAILABLE when shard is paused"
    );
    assert!(
        status.message().contains("split in progress"),
        "error message should mention split"
    );
}

/// [SILO-ROUTE-PAUSED-1] GetJob should return UNAVAILABLE when shard is paused
#[silo::test]
async fn get_job_returns_unavailable_when_shard_paused() {
    let (svc, coord, shard_id) = create_test_service(false).await;

    // First, create a job while not paused
    let enqueue_req = Request::new(EnqueueRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "test-job".to_string(),
        priority: 0,
        start_at_ms: 0,
        retry_policy: None,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_payload(
                &serde_json::json!({"data": "test"}),
            ))),
        }),
        limits: vec![],
        metadata: HashMap::new(),
        task_group: "default".to_string(),
    });
    svc.enqueue(enqueue_req).await.expect("enqueue should work");

    // GetJob should succeed when not paused
    let req = Request::new(GetJobRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "test-job".to_string(),
        include_attempts: false,
    });
    let result = svc.get_job(req).await;
    assert!(result.is_ok(), "get_job should succeed when not paused");

    // Now pause the shard
    coord.set_paused(true);

    // GetJob should fail with UNAVAILABLE
    let req = Request::new(GetJobRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "test-job".to_string(),
        include_attempts: false,
    });
    let result = svc.get_job(req).await;
    assert!(result.is_err(), "get_job should fail when paused");

    let status = result.err().unwrap();
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "should return UNAVAILABLE"
    );
}

/// [SILO-ROUTE-PAUSED-1] CancelJob should return UNAVAILABLE when shard is paused
#[silo::test]
async fn cancel_job_returns_unavailable_when_shard_paused() {
    let (svc, coord, shard_id) = create_test_service(false).await;

    // Create a job while not paused
    let enqueue_req = Request::new(EnqueueRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "cancel-test-job".to_string(),
        priority: 0,
        start_at_ms: 0,
        retry_policy: None,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_payload(
                &serde_json::json!({"data": "test"}),
            ))),
        }),
        limits: vec![],
        metadata: HashMap::new(),
        task_group: "default".to_string(),
    });
    svc.enqueue(enqueue_req).await.expect("enqueue should work");

    // Pause the shard
    coord.set_paused(true);

    // CancelJob should fail with UNAVAILABLE
    let req = Request::new(CancelJobRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "cancel-test-job".to_string(),
    });
    let result = svc.cancel_job(req).await;
    assert!(result.is_err(), "cancel_job should fail when paused");

    let status = result.err().unwrap();
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "should return UNAVAILABLE"
    );
}

/// [SILO-ROUTE-PAUSED-1] Query should return UNAVAILABLE when shard is paused
#[silo::test]
async fn query_returns_unavailable_when_shard_paused() {
    let (svc, coord, shard_id) = create_test_service(true).await;

    // Query should fail when shard is paused
    let req = Request::new(QueryRequest {
        shard: shard_id.to_string(),
        tenant: None,
        sql: "SELECT * FROM jobs LIMIT 10".to_string(),
    });

    let result = svc.query(req).await;
    assert!(result.is_err(), "query should fail when paused");

    let status = result.err().unwrap();
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "should return UNAVAILABLE"
    );

    // Unpause and verify query works
    coord.set_paused(false);

    let req = Request::new(QueryRequest {
        shard: shard_id.to_string(),
        tenant: None,
        sql: "SELECT * FROM jobs LIMIT 10".to_string(),
    });

    let result = svc.query(req).await;
    assert!(result.is_ok(), "query should succeed when not paused");
}

/// After split completes (unpaused), requests should succeed again
#[silo::test]
async fn requests_succeed_after_split_completes() {
    let (svc, coord, shard_id) = create_test_service(true).await;

    // While paused, request should fail
    let req = Request::new(EnqueueRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "job-during-split".to_string(),
        priority: 0,
        start_at_ms: 0,
        retry_policy: None,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_payload(
                &serde_json::json!({"data": "test"}),
            ))),
        }),
        limits: vec![],
        metadata: HashMap::new(),
        task_group: "default".to_string(),
    });
    let result = svc.enqueue(req).await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), tonic::Code::Unavailable);

    // Simulate split completion by unpausing
    coord.set_paused(false);

    // Now request should succeed
    let req = Request::new(EnqueueRequest {
        shard: shard_id.to_string(),
        tenant: None,
        id: "job-after-split".to_string(),
        priority: 0,
        start_at_ms: 0,
        retry_policy: None,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(msgpack_payload(
                &serde_json::json!({"data": "test"}),
            ))),
        }),
        limits: vec![],
        metadata: HashMap::new(),
        task_group: "default".to_string(),
    });
    let result = svc.enqueue(req).await;
    assert!(result.is_ok(), "should succeed after split completes");
}
