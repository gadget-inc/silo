use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, Error as SlateError};
use thiserror::Error;
use url::Url;

use crate::shard_map::ShardId;

/// Storage backends supported by the standalone compactor.
///
/// Mirrors the variants in `silo::settings::Backend` (sans `TurmoilFs`, which
/// only exists when silo is compiled with the `dst` feature for simulation).
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Fs,
    S3,
    Gcs,
    Memory,
    Url,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("slatedb error: {0}")]
    Slate(#[from] SlateError),
    #[error("invalid object store url: {0}")]
    InvalidUrl(String),
}

/// Resolved object store with the canonical path to hand to slatedb's builders.
pub struct ResolvedStore {
    pub store: Arc<dyn ObjectStore>,
    /// Path to use with `CompactorBuilder` — empty for `LocalFileSystem` since
    /// the root is already baked into the store.
    pub canonical_path: String,
    /// Actual filesystem root or URL — useful for diagnostics.
    pub root_path: String,
}

/// Resolve an object store for the given backend and path, mirroring silo's
/// `silo::storage::resolve_object_store` so the compactor sees data at the same
/// location silo writes it.
pub fn resolve_object_store(backend: &Backend, path: &str) -> Result<ResolvedStore, StorageError> {
    match backend {
        Backend::Fs => {
            let root = Path::new(path);
            if !root.exists() {
                fs::create_dir_all(root).map_err(|e| {
                    StorageError::InvalidUrl(format!("failed to create fs root {}: {}", path, e))
                })?;
            }
            let canonical_path = root.canonicalize().map_err(|e| {
                StorageError::InvalidUrl(format!("failed to canonicalize path {}: {}", path, e))
            })?;
            let canonical_str = canonical_path.to_string_lossy().to_string();
            let fs = slatedb::object_store::local::LocalFileSystem::new_with_prefix(&canonical_str)
                .map_err(|e| StorageError::InvalidUrl(format!("{}", e)))?;
            Ok(ResolvedStore {
                store: Arc::new(fs),
                canonical_path: String::new(),
                root_path: canonical_str,
            })
        }
        Backend::Memory => Ok(ResolvedStore {
            store: Arc::new(slatedb::object_store::memory::InMemory::new()),
            canonical_path: path.to_string(),
            root_path: path.to_string(),
        }),
        Backend::S3 | Backend::Gcs | Backend::Url => {
            let store = Db::resolve_object_store(path)?;
            let canonical_path = match Url::parse(path) {
                Ok(url) => {
                    let url_path = url.path();
                    url_path.strip_prefix('/').unwrap_or(url_path).to_string()
                }
                Err(e) => {
                    return Err(StorageError::InvalidUrl(format!(
                        "failed to parse object store URL '{}': {}. Expected gs://bucket/path or s3://bucket/path",
                        path, e
                    )));
                }
            };
            Ok(ResolvedStore {
                store,
                canonical_path: canonical_path.clone(),
                root_path: canonical_path,
            })
        }
    }
}

/// Substitute the `%shard%` placeholder in a path template with a shard's UUID.
pub fn path_for_shard(template: &str, shard_id: &ShardId) -> String {
    template.replace("%shard%", &shard_id.to_string())
}

/// Resolve a shard's object store at the shared template root -- the store
/// rooted at the template prefix before `%shard%`, with the shard's database
/// path expressed relative to that root -- mirroring the serving factory's
/// `ShardFactory::resolve_at_root`. A store scoped to the shard's own prefix
/// cannot address the external-DB paths a clone's manifest records, so the
/// compactor could never read clone-inherited SSTs regardless of how those
/// paths are rewritten. Absolute object keys are unchanged by the re-rooting.
pub fn resolve_object_store_at_root(
    backend: &Backend,
    path_template: &str,
    shard_id: &ShardId,
) -> Result<ResolvedStore, StorageError> {
    let shard_name = shard_id.to_string();
    match backend {
        Backend::Fs => {
            let Some(pos) = path_template.find("%shard%") else {
                // Placeholder-free template: no shared root to resolve at.
                return resolve_object_store(backend, path_template);
            };
            let root = path_template[..pos].trim_end_matches('/');
            let root = if root.is_empty() { "/" } else { root };
            let resolved = resolve_object_store(backend, root)?;
            Ok(ResolvedStore {
                store: resolved.store,
                canonical_path: shard_name,
                root_path: resolved.root_path,
            })
        }
        // Memory stores are per-resolve sentinels used in tests; there is no
        // shared root to re-root at.
        Backend::Memory => resolve_object_store(backend, &path_for_shard(path_template, shard_id)),
        Backend::S3 | Backend::Gcs | Backend::Url => {
            let (root, db_path) = silo::factory::ShardFactory::object_store_layout(
                &to_silo_backend(backend),
                path_template,
                &shard_name,
            )
            .map_err(|e| {
                StorageError::InvalidUrl(format!(
                    "shared-root layout for template '{path_template}': {e}"
                ))
            })?;
            let store = Db::resolve_object_store(&root)?;
            Ok(ResolvedStore {
                store,
                canonical_path: db_path,
                root_path: root,
            })
        }
    }
}

/// Map the compactor's backend enum onto silo's, which drives the shared-root
/// layout arithmetic.
fn to_silo_backend(backend: &Backend) -> silo::settings::Backend {
    match backend {
        Backend::Fs => silo::settings::Backend::Fs,
        Backend::S3 => silo::settings::Backend::S3,
        Backend::Gcs => silo::settings::Backend::Gcs,
        Backend::Memory => silo::settings::Backend::Memory,
        Backend::Url => silo::settings::Backend::Url,
    }
}
