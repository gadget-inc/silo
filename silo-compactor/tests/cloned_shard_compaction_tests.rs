//! The standalone compactor must read clone-inherited (external) SSTs from
//! the DB path recorded in the manifest's external-DB entries, the same way
//! `slatedb::Db`'s open path does. A split child's manifest delegates the
//! parent's SSTs; resolving them against the child's own root yields
//! `NotFound` on every compaction, so the child's L0s never drain.

use std::sync::Arc;
use std::time::Duration;

use slatedb::admin::Admin;
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

use silo_compactor::external_sst::build_shard_compactor;

const PARENT_PATH: &str = "shards/parent";
const CHILD_PATH: &str = "shards/child";

#[tokio::test]
async fn compactor_reads_clone_inherited_ssts_from_parent_path() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Parent DB with data flushed into compacted/ SSTs, then closed.
    let parent = slatedb::Db::builder(PARENT_PATH, Arc::clone(&store))
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

    // Shallow clone: the child's manifest delegates the parent's SSTs as
    // external, exactly how a split creates its children.
    let admin = Admin::builder(CHILD_PATH, Arc::clone(&store)).build();
    admin
        .create_clone_builder(PARENT_PATH, None)
        .build()
        .await
        .expect("clone parent into child");

    let manifest = admin
        .read_manifest(None)
        .await
        .expect("read child manifest")
        .expect("child manifest exists");
    assert!(
        !manifest.external_dbs().is_empty(),
        "clone should delegate parent SSTs via external_dbs"
    );

    // Standalone compactor, assembled exactly as the worker does. Started
    // before submission: its startup creates the compactions file.
    let compactor = build_shard_compactor(CHILD_PATH.to_string(), Arc::clone(&store), None, None)
        .await
        .expect("build compactor");
    let run_task = tokio::spawn({
        let compactor = compactor.clone();
        async move { compactor.run().await }
    });

    // Compact the child's (externally-owned) L0 views into sorted run 0.
    // Submission is retried until the compactor's startup has created the
    // compactions file.
    let sources: Vec<SourceId> = manifest
        .l0()
        .iter()
        .map(|view| SourceId::SstView(view.id))
        .collect();
    assert!(
        !sources.is_empty(),
        "cloned child should surface the parent's L0 views"
    );
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
