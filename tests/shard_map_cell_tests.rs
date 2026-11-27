//! Tests for `ShardMapCell`, the coordinator's lock-free copy of the shard map.
//!
//! Every tenant-scoped RPC reads the shard map to validate the tenant's range.
//! When that read awaited a fair tokio mutex, a saturated runtime turned it
//! into a convoy: each lock handoff waited behind the whole run queue, and
//! GetJob and ReportOutcome stopped completing. These tests pin the property
//! that fixes it: a read returns immediately even while a write is in
//! progress, and writers still don't lose each other's updates.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use futures::FutureExt;
use silo::coordination::{Coordinator, NoneCoordinator, ShardMapCell};
use silo::pb::silo_server::Silo;
use silo::pb::*;
use silo::server::SiloService;
use silo::shard_range::ShardMap;
use tonic::Request;

mod grpc_integration_helpers;
mod test_helpers;
use grpc_integration_helpers::{TEST_SHARD_ID, create_test_factory};
use test_helpers::msgpack_payload;

/// A writer parked inside `ShardMapCell::update` on its own OS thread, holding
/// the write side until released.
struct ParkedWriter {
    release: mpsc::Sender<()>,
    thread: JoinHandle<()>,
}

impl ParkedWriter {
    /// Start an update that bumps the map's version, and return once the
    /// writer holds the write side.
    fn start(cell: Arc<ShardMapCell>) -> Self {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            cell.update(|map| {
                entered_tx.send(()).expect("signal entered");
                // Also returns if the test drops `release` by panicking.
                let _ = release_rx.recv();
                map.version += 1;
                Ok::<_, ()>(())
            })
            .expect("update");
        });
        entered_rx.recv().expect("writer entered update");
        Self { release, thread }
    }

    fn finish(self) {
        self.release.send(()).expect("release writer");
        self.thread.join().expect("writer thread");
    }
}

#[silo::test]
fn load_returns_the_current_map_while_a_write_is_in_progress() {
    let cell = Arc::new(ShardMapCell::new(ShardMap::create_initial(4).unwrap()));
    let before = cell.load().version;
    let writer = ParkedWriter::start(cell.clone());

    // Read from a third thread so a read that waited on the writer fails the
    // test instead of hanging it.
    let (read_tx, read_rx) = mpsc::channel();
    let reader_cell = cell.clone();
    std::thread::spawn(move || {
        let _ = read_tx.send(reader_cell.load().version);
    });
    let seen = read_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("load returned while a write was in progress");
    assert_eq!(seen, before, "a read during the write sees the old map");

    writer.finish();
    assert_eq!(
        cell.load().version,
        before + 1,
        "the write is visible once it completes"
    );
}

#[silo::test]
fn snapshot_is_unchanged_by_a_later_store() {
    let cell = ShardMapCell::new(ShardMap::create_initial(4).unwrap());
    let snapshot = cell.load();

    cell.store(ShardMap::create_initial(8).unwrap());

    assert_eq!(snapshot.len(), 4, "a snapshot already taken keeps its map");
    assert_eq!(cell.load().len(), 8, "new reads see the stored map");
}

#[silo::test]
fn update_that_fails_leaves_the_map_unchanged() {
    let cell = ShardMapCell::new(ShardMap::create_initial(4).unwrap());
    let before = cell.load().version;

    let result = cell.update(|map| {
        map.version += 100;
        Err::<(), _>("rejected")
    });

    assert_eq!(result, Err("rejected"));
    assert_eq!(
        cell.load().version,
        before,
        "the failed update's copy is dropped"
    );
}

#[silo::test]
fn concurrent_updates_are_not_lost() {
    const THREADS: u64 = 8;
    const UPDATES_PER_THREAD: u64 = 500;
    let cell = Arc::new(ShardMapCell::new(ShardMap::create_initial(4).unwrap()));
    let before = cell.load().version;

    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let cell = cell.clone();
            std::thread::spawn(move || {
                for _ in 0..UPDATES_PER_THREAD {
                    cell.update(|map| {
                        map.version += 1;
                        Ok::<_, ()>(())
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }

    // Each update copies the current map and swaps the copy in. Without the
    // writer lock, two updates could copy the same map and one would be lost.
    assert_eq!(
        cell.load().version,
        before + THREADS * UPDATES_PER_THREAD,
        "every update is applied"
    );
}

fn enqueue_request(tenant: &str, id: &str) -> EnqueueRequest {
    EnqueueRequest {
        shard: TEST_SHARD_ID.to_string(),
        tenant: Some(tenant.to_string()),
        id: id.to_string(),
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
    }
}

/// With tenancy on, enqueue and get_job validate the tenant against the shard
/// map. On the old mutex, the same RPCs waited for as long as the map was held
/// (an enqueue timed out after 2 s against a held lock).
#[silo::test]
async fn tenant_rpcs_complete_while_a_shard_map_write_is_in_progress() {
    let (factory, _tmp) = create_test_factory().await.expect("factory");
    let coordinator = Arc::new(
        NoneCoordinator::from_factory("test-node", "http://127.0.0.1:0", factory.clone()).await,
    );
    let mut cfg = silo::settings::AppConfig::load(None).expect("load config");
    cfg.tenancy.enabled = true;
    let svc = SiloService::new(factory, coordinator.clone(), cfg, None);
    let shard_map = coordinator.base().shard_map.clone();
    let before = shard_map.load().version;

    let writer = ParkedWriter::start(shard_map.clone());

    let read = coordinator
        .get_shard_map()
        .now_or_never()
        .expect("get_shard_map completes without yielding during a write")
        .expect("get_shard_map");
    assert_eq!(
        read.version, before,
        "the read sees the map before the write"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        svc.enqueue(Request::new(enqueue_request("tenant-a", "job-1"))),
    )
    .await
    .expect("enqueue completed while the shard map write was in progress")
    .expect("enqueue");

    let job = tokio::time::timeout(
        Duration::from_secs(5),
        svc.get_job(Request::new(GetJobRequest {
            shard: TEST_SHARD_ID.to_string(),
            tenant: Some("tenant-a".to_string()),
            id: "job-1".to_string(),
            include_attempts: false,
        })),
    )
    .await
    .expect("get_job completed while the shard map write was in progress")
    .expect("get_job");
    assert_eq!(job.into_inner().id, "job-1");

    writer.finish();
    assert_eq!(shard_map.load().version, before + 1);
}
