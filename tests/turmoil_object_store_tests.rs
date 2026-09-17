//! Tests for `TurmoilObjectStore`'s arm-able write stall.
//!
//! These live outside the `turmoil_runner` crate because that binary links
//! mad-turmoil, which takes over the process clock: outside a simulation no
//! timer there ever fires.

use bytes::Bytes;
use silo::turmoil_object_store::{
    TurmoilObjectStore, clear_shared_storage, release_write_stalls, stall_writes_under,
};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use std::time::Duration;

/// The stall state is process-global, so these tests run one at a time.
static STALL_STATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How long a write is given before it counts as stalled.
const STALL_OBSERVATION: Duration = Duration::from_millis(200);

/// Serialize on the global stall state and start from a clean store.
async fn exclusive_clean_store() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = STALL_STATE.lock().await;
    clear_shared_storage();
    guard
}

async fn put_completes(store: &TurmoilObjectStore, location: &str) -> bool {
    let path = Path::from(location);
    let put = store.put(&path, Bytes::from("data").into());
    tokio::time::timeout(STALL_OBSERVATION, put).await.is_ok()
}

#[silo::test(flavor = "multi_thread", worker_threads = 2)]
async fn armed_stall_hangs_writes_under_its_prefix_until_released() {
    let _exclusive = exclusive_clean_store().await;
    {
        let store = TurmoilObjectStore::new("/stall-test").unwrap();
        stall_writes_under("/stall-test/shard-a");

        assert!(
            !put_completes(&store, "shard-a/wal/1.sst").await,
            "a put under the stalled prefix should hang"
        );
        assert!(
            put_completes(&store, "shard-b/wal/1.sst").await,
            "a put outside the stalled prefix should complete"
        );

        let stalled = {
            let store = TurmoilObjectStore::new("/stall-test").unwrap();
            tokio::spawn(async move {
                store
                    .put(&Path::from("shard-a/manifest/1"), Bytes::from("m").into())
                    .await
            })
        };
        tokio::time::sleep(STALL_OBSERVATION).await;
        assert!(!stalled.is_finished(), "the put should still be stalled");

        release_write_stalls();
        tokio::time::timeout(Duration::from_secs(5), stalled)
            .await
            .expect("a released put should complete")
            .expect("put task panicked")
            .expect("put failed");
        let stored = store.get(&Path::from("shard-a/manifest/1")).await.unwrap();
        assert_eq!(stored.bytes().await.unwrap(), Bytes::from("m"));
    }
    clear_shared_storage();
}

#[silo::test]
async fn armed_stall_hangs_multipart_completion() {
    let _exclusive = exclusive_clean_store().await;
    {
        let store = TurmoilObjectStore::new("/stall-multipart").unwrap();
        stall_writes_under("/stall-multipart/shard-a");

        let mut upload = store
            .put_multipart(&Path::from("shard-a/compacted/1.sst"))
            .await
            .unwrap();
        upload.put_part(Bytes::from("part").into()).await.unwrap();
        let completed = tokio::time::timeout(STALL_OBSERVATION, upload.complete()).await;
        assert!(
            completed.is_err(),
            "completing a multipart upload under the stalled prefix should hang"
        );
    }
    clear_shared_storage();
}

#[silo::test]
async fn clear_shared_storage_clears_an_armed_stall() {
    let _exclusive = exclusive_clean_store().await;
    {
        let store = TurmoilObjectStore::new("/stall-cleared").unwrap();
        stall_writes_under("/stall-cleared/shard-a");

        clear_shared_storage();

        assert!(
            put_completes(&store, "shard-a/wal/1.sst").await,
            "a put should complete once clear_shared_storage cleared the stall"
        );
    }
    clear_shared_storage();
}

/// SlateDB fences stale writers through put-if-absent on manifest versions, so
/// the simulated store has to refuse a `Create` of an existing object.
#[silo::test]
async fn create_mode_put_refuses_an_existing_object() {
    use slatedb::object_store::{Error as ObjectStoreError, PutMode, PutOptions};

    let _exclusive = exclusive_clean_store().await;
    let store = TurmoilObjectStore::new("/create-mode").unwrap();
    let path = Path::from("shard-a/manifest/00000000000000000002.manifest");
    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };

    store
        .put_opts(&path, Bytes::from("first writer").into(), create())
        .await
        .expect("creating a new object should succeed");
    let second = store
        .put_opts(&path, Bytes::from("stale writer").into(), create())
        .await;
    assert!(
        matches!(second, Err(ObjectStoreError::AlreadyExists { .. })),
        "a Create put of an existing object should fail with AlreadyExists, got {second:?}"
    );

    let stored = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(
        stored,
        Bytes::from("first writer"),
        "the first write must survive"
    );

    store
        .put_opts(
            &path,
            Bytes::from("overwrite").into(),
            PutOptions::default(),
        )
        .await
        .expect("an Overwrite put of an existing object should succeed");
    clear_shared_storage();
}
