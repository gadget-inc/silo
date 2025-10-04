use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig};
use slatedb::{Db, DbIterator};

// Helper: enforce a tight timeout for async tests likely to hang
#[macro_export]
macro_rules! with_timeout {
    ($ms:expr, $body:block) => {{
        tokio::time::timeout(std::time::Duration::from_millis($ms), async move { $body })
            .await
            .expect("test timed out")
    }};
}

pub fn parse_time_from_task_key(key: &str) -> Option<u64> {
    // Accept both legacy and tenant-aware formats:
    // - tasks/{:020}/{:02}/{job_id}/{attempt}
    // - t/<tenant>/tasks/{:020}/{:02}/{job_id}/{attempt}
    if let Some(pos) = key.find("tasks/") {
        let after = &key[pos + "tasks/".len()..];
        let ts_str = after.split('/').next().unwrap_or("");
        return ts_str.parse::<u64>().ok();
    }
    None
}

pub async fn open_temp_shard() -> (tempfile::TempDir, std::sync::Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        // Use fast flush interval for tests to speed them up
        flush_interval_ms: Some(10),
    };
    let shard = JobStoreShard::open(&cfg).await.expect("open shard");
    (tmp, shard)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub async fn first_kv_with_prefix(db: &Db, prefix: &str) -> Option<(String, bytes::Bytes)> {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.ok()?;
    let first = iter.next().await.ok()?;
    first.map(|kv| (String::from_utf8_lossy(&kv.key).to_string(), kv.value))
}

pub async fn count_tasks_before(db: &Db, cutoff_ms: i64) -> usize {
    let start: Vec<u8> = b"tasks/".to_vec();
    let mut end: Vec<u8> = b"tasks/".to_vec();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        let Some(kv) = maybe else { break };
        let key_str = String::from_utf8_lossy(&kv.key).to_string();
        if let Some(ts) = parse_time_from_task_key(&key_str) {
            if (ts as i64) < cutoff_ms {
                count += 1;
            }
        }
    }
    count
}

pub async fn count_with_prefix(db: &Db, prefix: &str) -> usize {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        if maybe.is_none() {
            break;
        }
        count += 1;
    }
    count
}
