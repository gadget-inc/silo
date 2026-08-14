//! The standalone compactor must read clone-inherited (external) SSTs from
//! the DB path recorded in the manifest's external-DB entries, the same way
//! `slatedb::Db`'s open path does. A split child's manifest delegates the
//! parent's SSTs; resolving them against the child's own root yields
//! `NotFound` on every compaction, so the child's L0s never drain.

use std::sync::Arc;
use std::time::Duration;

use slatedb::admin::Admin;
use slatedb::compactor::{CompactionSpec, CompactionStatus, Compactor, SourceId};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use uuid::Uuid;

use silo_compactor::external_sst::build_shard_compactor;
use silo_compactor::shard_map::ShardId;
use silo_compactor::storage::{Backend, resolve_object_store_at_root};

const PARENT_PATH: &str = "shards/parent";
const CHILD_PATH: &str = "shards/child";

/// Seed a parent DB with data flushed into compacted/ SSTs, close it, and
/// shallow-clone it to `child_path` on the same store -- the way a split
/// creates its children. Returns an admin handle for the child.
async fn seed_parent_and_clone(
    store: &Arc<dyn ObjectStore>,
    parent_path: &str,
    child_path: &str,
) -> Admin {
    let parent = slatedb::Db::builder(parent_path, Arc::clone(store))
        .build()
        .await
        .expect("open parent db");
    for i in 0..500u32 {
        parent
            .put(
                format!("key-{i:05}").as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .await
            .expect("put");
    }
    parent
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("flush parent memtable");
    parent.close().await.expect("close parent");

    let admin = Admin::builder(child_path.to_string(), Arc::clone(store)).build();
    admin
        .create_clone_builder(parent_path, None)
        .build()
        .await
        .expect("clone parent into child");
    admin
}

/// Start `compactor`, submit a compaction over the child's clone-inherited L0
/// views, wait for a terminal status (or time out), stop the compactor, and
/// return the final status.
async fn run_compaction_of_inherited_l0s(admin: &Admin, compactor: Compactor) -> CompactionStatus {
    let manifest = admin
        .read_manifest(None)
        .await
        .expect("read child manifest")
        .expect("child manifest exists");
    assert!(
        !manifest.external_dbs().is_empty(),
        "clone should delegate parent SSTs via external_dbs"
    );
    let sources: Vec<SourceId> = manifest
        .l0()
        .iter()
        .map(|view| SourceId::SstView(view.id))
        .collect();
    assert!(
        !sources.is_empty(),
        "cloned child should surface the parent's L0 views"
    );

    let run_task = tokio::spawn({
        let compactor = compactor.clone();
        async move { compactor.run().await }
    });

    // Submission is retried until the compactor's startup has created the
    // compactions file.
    let submit_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let compaction = loop {
        match admin
            .submit_compaction(CompactionSpec::new(sources.clone(), 0))
            .await
        {
            Ok(compaction) => break compaction,
            Err(e) if tokio::time::Instant::now() > submit_deadline => {
                panic!("submit compaction: {e}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let final_status = loop {
        let current = admin
            .read_compaction(compaction.id(), None)
            .await
            .expect("read compaction")
            .expect("compaction record exists");
        let status = current.status();
        if matches!(
            status,
            CompactionStatus::Completed | CompactionStatus::Failed
        ) || tokio::time::Instant::now() > deadline
        {
            break status;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    compactor.stop().await.expect("stop compactor");
    let _ = run_task.await;
    final_status
}

#[tokio::test]
async fn compactor_reads_clone_inherited_ssts_from_parent_path() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let admin = seed_parent_and_clone(&store, PARENT_PATH, CHILD_PATH).await;

    let compactor = build_shard_compactor(CHILD_PATH.to_string(), Arc::clone(&store), None, None)
        .await
        .expect("build compactor");
    let final_status = run_compaction_of_inherited_l0s(&admin, compactor).await;

    assert_eq!(
        final_status,
        CompactionStatus::Completed,
        "compaction of clone-inherited SSTs should complete; Failed means the \
         compactor resolved external SSTs against the child root instead of \
         the parent path recorded in the manifest"
    );

    // The compaction re-localized the inherited data: fully readable from
    // the child's own SSTs.
    let child = slatedb::Db::builder(CHILD_PATH, Arc::clone(&store))
        .build()
        .await
        .expect("open child db");
    let got = child.get(b"key-00042").await.expect("get");
    assert_eq!(got.as_deref(), Some(b"value-42".as_ref()));
    child.close().await.expect("close child");
}

/// The deployed shape: each shard's template expands to its own prefix, so a
/// store resolved at the shard's full path cannot address the parent's
/// objects at all. The worker must resolve the store at the shared template
/// root (as the serving factory does) for the external-SST redirect to have
/// any effect.
#[tokio::test]
async fn worker_resolution_reaches_parent_ssts_under_shared_root() {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let root = root_dir.path().to_str().expect("utf-8 tempdir path");
    let parent_id = ShardId(Uuid::new_v4());
    let child_id = ShardId(Uuid::new_v4());

    // Seed parent and clone through a root-scoped store with per-shard db
    // paths -- the layout the serving factory produces for Fs backends.
    let seed_store: Arc<dyn ObjectStore> = Arc::new(
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(root)
            .expect("root-scoped local store"),
    );
    seed_parent_and_clone(&seed_store, &parent_id.to_string(), &child_id.to_string()).await;

    // Resolve the child's store the way the worker does, from the deployed
    // template shape, and run the standalone compactor on it.
    let template = format!("{root}/%shard%");
    let resolved = resolve_object_store_at_root(&Backend::Fs, &template, &child_id)
        .expect("resolve child store at shared root");
    let admin =
        Admin::builder(resolved.canonical_path.clone(), Arc::clone(&resolved.store)).build();
    let compactor = build_shard_compactor(
        resolved.canonical_path.clone(),
        Arc::clone(&resolved.store),
        None,
        None,
    )
    .await
    .expect("build compactor");
    let final_status = run_compaction_of_inherited_l0s(&admin, compactor).await;

    assert_eq!(
        final_status,
        CompactionStatus::Completed,
        "compaction must complete through the worker's store resolution; \
         Failed means the store is scoped to the child's own prefix and the \
         clone-inherited parent SSTs are unreachable no matter how their \
         paths are rewritten"
    );
}
