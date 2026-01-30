//! TurmoilObjectStore: A deterministic ObjectStore implementation using shared in-memory storage.
//!
//! This module provides an ObjectStore implementation that simulates a shared object store
//! (like S3 or GCS) for DST testing. All file I/O goes through a global in-memory HashMap
//! that is shared across all turmoil hosts, ensuring that data written by one host can be
//! read by another (simulating cloud object storage behavior).
//!
//! The storage is deterministic - all randomness (including simulated latency) uses turmoil's
//! seeded RNG via mad-turmoil's `getrandom` interception. Time stamps and sleeps use turmoil's
//! simulated time for consistency.
//!
//! This module is only available when the `dst` feature is enabled.

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use rand::Rng;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Attributes, Error as ObjectStoreError, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult, UploadPart,
};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

/// Entry in the shared storage - contains file data and metadata.
#[derive(Clone, Debug)]
struct StorageEntry {
    data: Bytes,
    last_modified: SystemTime,
}

/// Global shared storage for all TurmoilObjectStore instances.
/// Using BTreeMap for deterministic iteration order.
/// The key is the full path (root + location).
static SHARED_STORAGE: OnceLock<Mutex<BTreeMap<PathBuf, StorageEntry>>> = OnceLock::new();

fn get_shared_storage() -> &'static Mutex<BTreeMap<PathBuf, StorageEntry>> {
    SHARED_STORAGE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Clear all data from the shared storage.
/// This should be called between test runs to ensure isolation.
pub fn clear_shared_storage() {
    if let Some(storage) = SHARED_STORAGE.get() {
        storage.lock().unwrap().clear();
    }
}

/// Get the current simulated time from turmoil, or fall back to UNIX_EPOCH.
fn current_time() -> SystemTime {
    turmoil::sim_elapsed()
        .map(|d| SystemTime::UNIX_EPOCH + d)
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

/// Simulate object store latency using turmoil's simulated time.
///
/// The latency is randomly generated using `rand::rng()` which is
/// intercepted by mad-turmoil to use a seeded RNG, ensuring determinism.
/// Latency range simulates real-world object store behavior:
/// - min_ms: minimum latency (e.g., best-case network round trip)
/// - max_ms: maximum latency (e.g., slow requests, retries)
async fn simulate_latency(min_ms: u64, max_ms: u64) {
    let latency_ms = rand::rng().random_range(min_ms..=max_ms);
    tokio::time::sleep(Duration::from_millis(latency_ms)).await;
}

/// An ObjectStore implementation backed by global shared in-memory storage.
///
/// This simulates a shared object store (like S3 or GCS) for DST testing.
/// All TurmoilObjectStore instances share the same underlying storage,
/// regardless of which turmoil host they're running on.
pub struct TurmoilObjectStore {
    /// Root path prefix for this store instance
    root: PathBuf,
}

impl TurmoilObjectStore {
    /// Create a new TurmoilObjectStore rooted at the given path.
    ///
    /// The root path acts as a prefix for all operations. Multiple stores
    /// can be created with different roots to simulate separate buckets.
    pub fn new(root: impl Into<PathBuf>) -> ObjectStoreResult<Self> {
        let root = root.into();
        Ok(Self { root })
    }

    /// Convert an object_store Path to a full filesystem PathBuf (root + location).
    fn full_path(&self, location: &Path) -> PathBuf {
        self.root.join(location.as_ref())
    }
}

impl Debug for TurmoilObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurmoilObjectStore")
            .field("root", &self.root)
            .finish()
    }
}

impl Display for TurmoilObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TurmoilObjectStore({})", self.root.display())
    }
}

#[async_trait]
impl ObjectStore for TurmoilObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        // Simulate network latency for write operations (1-5ms)
        simulate_latency(1, 5).await;

        let full_path = self.full_path(location);
        let bytes: Bytes = payload.into();

        let entry = StorageEntry {
            data: bytes,
            last_modified: current_time(),
        };

        get_shared_storage()
            .lock()
            .unwrap()
            .insert(full_path, entry);

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let full_path = self.full_path(location);

        Ok(Box::new(TurmoilMultipartUpload {
            path: full_path,
            parts: Arc::new(std::sync::Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // Simulate network latency for read operations (1-5ms)
        simulate_latency(1, 5).await;

        let full_path = self.full_path(location);

        let (data, last_modified) = {
            let storage = get_shared_storage().lock().unwrap();
            let entry = storage
                .get(&full_path)
                .ok_or_else(|| ObjectStoreError::NotFound {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "file not found",
                    )),
                })?;
            (entry.data.clone(), entry.last_modified)
        };

        let data_len = data.len();

        // Handle range requests
        let (range, bytes) = match options.range {
            Some(GetRange::Bounded(range)) => {
                let start = range.start as usize;
                let end = (range.end as usize).min(data_len);
                let slice = if start < data_len {
                    data.slice(start..end)
                } else {
                    Bytes::new()
                };
                ((start as u64)..(end as u64), slice)
            }
            Some(GetRange::Offset(offset)) => {
                let start = offset as usize;
                let slice = if start < data_len {
                    data.slice(start..)
                } else {
                    Bytes::new()
                };
                (offset..(data_len as u64), slice)
            }
            Some(GetRange::Suffix(suffix)) => {
                let suffix_usize = suffix as usize;
                let start = data_len.saturating_sub(suffix_usize);
                let slice = data.slice(start..);
                ((start as u64)..(data_len as u64), slice)
            }
            None => {
                let len = data_len as u64;
                (0..len, data)
            }
        };

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: last_modified.into(),
            size: data_len as u64,
            e_tag: None,
            version: None,
        };

        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::once(async { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::new(),
        })
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        // Simulate network latency for delete operations (1-3ms)
        simulate_latency(1, 3).await;

        let full_path = self.full_path(location);

        let removed = get_shared_storage().lock().unwrap().remove(&full_path);

        if removed.is_none() {
            return Err(ObjectStoreError::NotFound {
                path: location.to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "file not found",
                )),
            });
        }

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let root = self.root.clone();
        let prefix_path = prefix
            .map(|p| root.join(p.as_ref()))
            .unwrap_or(root.clone());

        let entries: Vec<ObjectMeta> = {
            let storage = get_shared_storage().lock().unwrap();

            storage
                .iter()
                .filter(|entry| {
                    let (path, _) = entry;
                    path.starts_with(&prefix_path)
                })
                .filter_map(|(path, entry)| {
                    path.strip_prefix(&root)
                        .ok()
                        .and_then(|rel| Path::parse(rel.to_string_lossy()).ok())
                        .map(|location| ObjectMeta {
                            location,
                            last_modified: entry.last_modified.into(),
                            size: entry.data.len() as u64,
                            e_tag: None,
                            version: None,
                        })
                })
                .collect()
        };

        // Already sorted due to BTreeMap
        futures::stream::iter(entries.into_iter().map(Ok)).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        // Simulate network latency for list operations (2-8ms, slightly higher than single-object ops)
        simulate_latency(2, 8).await;

        let root = self.root.clone();
        let prefix_path = prefix
            .map(|p| root.join(p.as_ref()))
            .unwrap_or(root.clone());
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();

        let mut objects = Vec::new();
        let mut common_prefixes_set = std::collections::BTreeSet::new();

        {
            let storage = get_shared_storage().lock().unwrap();

            for (path, entry) in storage.iter() {
                // Only consider paths under our prefix
                if !path.starts_with(&prefix_path) {
                    continue;
                }

                // Get relative path from root
                let Some(rel_path) = path.strip_prefix(&root).ok() else {
                    continue;
                };

                let rel_str = rel_path.to_string_lossy();

                // Skip if this is not under the prefix (for object store path)
                if !prefix_str.is_empty() && !rel_str.starts_with(&prefix_str) {
                    continue;
                }

                // Get the part after the prefix
                let after_prefix = if prefix_str.is_empty() {
                    rel_str.to_string()
                } else {
                    rel_str
                        .strip_prefix(&prefix_str)
                        .map(|s| s.trim_start_matches('/').to_string())
                        .unwrap_or_default()
                };

                if after_prefix.is_empty() {
                    continue;
                }

                // Check if there's a delimiter (/) in the remaining path
                if let Some(slash_pos) = after_prefix.find('/') {
                    // This is a "directory" - add the common prefix
                    let dir_name = &after_prefix[..slash_pos];
                    let common_prefix = if prefix_str.is_empty() {
                        dir_name.to_string()
                    } else {
                        format!("{}/{}", prefix_str.trim_end_matches('/'), dir_name)
                    };
                    if let Ok(p) = Path::parse(&common_prefix) {
                        common_prefixes_set.insert(p);
                    }
                } else {
                    // This is a file directly under the prefix
                    if let Ok(location) = Path::parse(rel_str.as_ref()) {
                        objects.push(ObjectMeta {
                            location,
                            last_modified: entry.last_modified.into(),
                            size: entry.data.len() as u64,
                            e_tag: None,
                            version: None,
                        });
                    }
                }
            }
        }

        // Sort objects for deterministic output
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        let common_prefixes: Vec<Path> = common_prefixes_set.into_iter().collect();

        Ok(ListResult {
            objects,
            common_prefixes,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        // Simulate network latency for copy operations (1-5ms)
        simulate_latency(1, 5).await;

        let from_path = self.full_path(from);
        let to_path = self.full_path(to);

        let entry = {
            let storage = get_shared_storage().lock().unwrap();
            storage
                .get(&from_path)
                .cloned()
                .ok_or_else(|| ObjectStoreError::NotFound {
                    path: from.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "source file not found",
                    )),
                })?
        };

        // Update timestamp for the copy
        let new_entry = StorageEntry {
            data: entry.data,
            last_modified: current_time(),
        };

        get_shared_storage()
            .lock()
            .unwrap()
            .insert(to_path, new_entry);

        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let to_path = self.full_path(to);

        // Check if destination exists
        {
            let storage = get_shared_storage().lock().unwrap();
            if storage.contains_key(&to_path) {
                return Err(ObjectStoreError::AlreadyExists {
                    path: to.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "destination already exists",
                    )),
                });
            }
        }

        self.copy(from, to).await
    }
}

/// Multipart upload implementation for TurmoilObjectStore.
#[derive(Debug)]
struct TurmoilMultipartUpload {
    path: PathBuf,
    parts: Arc<std::sync::Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for TurmoilMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let parts = Arc::clone(&self.parts);
        Box::pin(async move {
            let bytes: Bytes = data.into();
            parts.lock().unwrap().push(bytes);
            Ok(())
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        // Simulate network latency for multipart complete (2-8ms)
        simulate_latency(2, 8).await;

        let parts = self.parts.lock().unwrap();
        let mut data = Vec::new();
        for part in parts.iter() {
            data.extend_from_slice(part);
        }

        let entry = StorageEntry {
            data: Bytes::from(data),
            last_modified: current_time(),
        };

        get_shared_storage()
            .lock()
            .unwrap()
            .insert(self.path.clone(), entry);

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        // Just clear the parts - nothing to clean up
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_put_get() {
        clear_shared_storage();

        let store = TurmoilObjectStore::new("/test").unwrap();
        let path = Path::from("test.txt");
        let data = Bytes::from("hello world");

        store.put(&path, data.clone().into()).await.unwrap();

        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, data);
    }

    #[tokio::test]
    async fn test_shared_across_instances() {
        clear_shared_storage();

        let store1 = TurmoilObjectStore::new("/shared").unwrap();
        let store2 = TurmoilObjectStore::new("/shared").unwrap();
        let path = Path::from("shared.txt");
        let data = Bytes::from("shared data");

        // Write from store1
        store1.put(&path, data.clone().into()).await.unwrap();

        // Read from store2
        let result = store2.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, data);
    }

    #[tokio::test]
    async fn test_list() {
        clear_shared_storage();

        let store = TurmoilObjectStore::new("/list-test").unwrap();

        store
            .put(&Path::from("a.txt"), Bytes::from("a").into())
            .await
            .unwrap();
        store
            .put(&Path::from("b.txt"), Bytes::from("b").into())
            .await
            .unwrap();
        store
            .put(&Path::from("dir/c.txt"), Bytes::from("c").into())
            .await
            .unwrap();

        let entries: Vec<_> = store.list(None).collect().await;
        assert_eq!(entries.len(), 3);

        let paths: Vec<_> = entries
            .into_iter()
            .map(|r| r.unwrap().location.to_string())
            .collect();
        assert!(paths.contains(&"a.txt".to_string()));
        assert!(paths.contains(&"b.txt".to_string()));
        assert!(paths.contains(&"dir/c.txt".to_string()));
    }
}
