//! Compactor construction for a resolved shard store.
//!
//! `slatedb::Db`'s open path reads the manifest and resolves SSTs delegated
//! to an external DB (a split child's clone parent) against that DB's
//! recorded path. `CompactorBuilder::build` resolves every SST against the
//! shard's own root, so a standalone compactor reading clone-inherited SSTs
//! gets `NotFound` on every compaction and the shard never drains its L0s.
//! Until slatedb's compactor is manifest-aware, the store handed to
//! `CompactorBuilder` is wrapped so reads of externally-owned SST paths are
//! redirected to the owning DB's path.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use slatedb::admin::Admin;
use slatedb::compactor::Compactor;
use slatedb::manifest::SsTableId;
use slatedb_common::metrics::DefaultMetricsRecorder;

use crate::error::CompactorError;

/// Build the slatedb compactor for a shard whose object store and canonical
/// path are already resolved. When the shard's manifest delegates SSTs to an
/// external DB (the shard is a split child that has not yet re-localized its
/// inherited data), the store is wrapped so reads of those SSTs go to the
/// external DB's path.
pub async fn build_shard_compactor(
    canonical_path: String,
    store: Arc<dyn ObjectStore>,
    compactor_options: Option<slatedb::config::CompactorOptions>,
    recorder: Option<Arc<DefaultMetricsRecorder>>,
) -> Result<Compactor, CompactorError> {
    let redirects = external_sst_redirects(&canonical_path, Arc::clone(&store)).await?;
    let store: Arc<dyn ObjectStore> = if redirects.is_empty() {
        store
    } else {
        tracing::info!(
            path = %canonical_path,
            external_ssts = redirects.len(),
            "shard manifest delegates SSTs to an external DB; redirecting compactor reads"
        );
        Arc::new(ExternalSstReadRedirect {
            inner: store,
            redirects,
        })
    };

    let mut builder = slatedb::CompactorBuilder::new(canonical_path, store)
        .with_merge_operator(silo::job_store_shard::counter_merge_operator());
    if let Some(opts) = compactor_options {
        builder = builder.with_options(opts);
    }
    if let Some(rec) = recorder {
        builder =
            builder.with_metrics_recorder(rec as Arc<dyn slatedb_common::metrics::MetricsRecorder>);
    }
    Ok(builder.build())
}

/// Read the shard's latest manifest and map each externally-owned SST from
/// the path the compactor will resolve (under the shard's own root) to the
/// path recorded for its owning DB. Empty when the manifest is absent or
/// references no external DBs.
async fn external_sst_redirects(
    canonical_path: &str,
    store: Arc<dyn ObjectStore>,
) -> Result<HashMap<Path, Path>, CompactorError> {
    let admin = Admin::builder(canonical_path.to_string(), store).build();
    let manifest = admin
        .read_manifest(None)
        .await
        .map_err(|e| CompactorError::Slatedb(e.to_string()))?;
    let Some(manifest) = manifest else {
        return Ok(HashMap::new());
    };

    let own_root = Path::from(canonical_path);
    let mut redirects = HashMap::new();
    for external_db in manifest.external_dbs() {
        let external_root = Path::from(external_db.path.as_str());
        for sst_id in &external_db.sst_ids {
            redirects.insert(
                table_path(&own_root, sst_id),
                table_path(&external_root, sst_id),
            );
        }
    }
    Ok(redirects)
}

/// SST object path under `root`, matching slatedb's `PathResolver` layout.
fn table_path(root: &Path, id: &SsTableId) -> Path {
    match id {
        SsTableId::Wal(id) => Path::from(format!("{root}/wal/{id:020}.sst")),
        SsTableId::Compacted(ulid) => Path::from(format!("{root}/compacted/{ulid}.sst")),
    }
}

/// Object store wrapper that redirects reads of externally-owned SST paths to
/// the owning DB's path. Writes, deletes, copies, and listings are never
/// redirected: external objects are shared with the parent DB and the sibling
/// child, so only reads may cross DB roots.
struct ExternalSstReadRedirect {
    inner: Arc<dyn ObjectStore>,
    redirects: HashMap<Path, Path>,
}

impl ExternalSstReadRedirect {
    fn resolve<'a>(&'a self, location: &'a Path) -> &'a Path {
        self.redirects.get(location).unwrap_or(location)
    }
}

impl fmt::Debug for ExternalSstReadRedirect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalSstReadRedirect")
            .field("inner", &self.inner)
            .field("redirects", &self.redirects.len())
            .finish()
    }
}

impl fmt::Display for ExternalSstReadRedirect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExternalSstReadRedirect({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ExternalSstReadRedirect {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(self.resolve(location), options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
