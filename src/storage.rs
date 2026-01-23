use std::sync::Arc;

use slatedb::object_store::ObjectStore;
use slatedb::{Db, Error as SlateError};
use std::fs;
use std::path::Path;
use thiserror::Error;
use url::Url;

use crate::settings::Backend;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("slatedb error: {0}")]
    Slate(#[from] SlateError),
    #[error("invalid object store url: {0}")]
    InvalidUrl(String),
}

/// Result of resolving an object store, includes the canonical path used
pub struct ResolvedStore {
    pub store: Arc<dyn ObjectStore>,
    pub canonical_path: String,
}

pub fn resolve_object_store(backend: &Backend, path: &str) -> Result<ResolvedStore, StorageError> {
    match backend {
        Backend::Fs => {
            // Ensure the directory exists before creating the LocalFileSystem root
            let root = Path::new(path);
            if !root.exists() {
                fs::create_dir_all(root).map_err(|e| {
                    StorageError::InvalidUrl(format!("failed to create fs root {}: {}", path, e))
                })?;
            }
            // Canonicalize the path to avoid URL-encoding issues with relative paths
            // (e.g., "./tmp" being encoded as "%2E/tmp" or "%252E/tmp" inconsistently)
            let canonical_path = root.canonicalize().map_err(|e| {
                StorageError::InvalidUrl(format!("failed to canonicalize path {}: {}", path, e))
            })?;
            let canonical_str = canonical_path.to_string_lossy().to_string();
            // Use slatedb's re-exported object_store to ensure trait compatibility
            let fs = slatedb::object_store::local::LocalFileSystem::new_with_prefix(&canonical_str)
                .map_err(|e| StorageError::InvalidUrl(format!("{}", e)))?;
            Ok(ResolvedStore {
                store: Arc::new(fs),
                canonical_path: canonical_str,
            })
        }
        Backend::Memory => Ok(ResolvedStore {
            store: Arc::new(slatedb::object_store::memory::InMemory::new()),
            canonical_path: path.to_string(),
        }),
        Backend::S3 | Backend::Gcs | Backend::Url => {
            // Interpret path as a URL understood by SlateDB's resolver, e.g. s3://bucket/prefix or gs://bucket/prefix
            let store = Db::resolve_object_store(path)?;

            // Extract just the path component from the URL for use with DbBuilder.
            // When DbBuilder receives both a path and an object store, the path should be
            // relative to the object store's root (the bucket), not the full URL.
            // e.g., "gs://bucket/silo/data-0" -> "silo/data-0"
            let canonical_path = match Url::parse(path) {
                Ok(url) => {
                    // url.path() returns the path component, e.g., "/silo/data-0"
                    // We strip the leading "/" to get a relative path within the bucket
                    let url_path = url.path();
                    url_path.strip_prefix('/').unwrap_or(url_path).to_string()
                }
                Err(e) => {
                    return Err(StorageError::InvalidUrl(format!(
                        "failed to parse object store URL '{}': {}. Expected format: gs://bucket/path or s3://bucket/path",
                        path, e
                    )));
                }
            };

            Ok(ResolvedStore {
                store,
                canonical_path,
            })
        }
    }
}
