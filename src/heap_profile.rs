use std::ffi::{CString, c_char};
use std::path::Path;
use std::sync::Mutex;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

#[cfg(unix)]
const OPT_PROF: &[u8] = b"opt.prof\0";
#[cfg(unix)]
const PROF_ACTIVE: &[u8] = b"prof.active\0";
#[cfg(unix)]
const PROF_DUMP: &[u8] = b"prof.dump\0";

#[cfg(unix)]
const STATS_ALLOCATED: &[u8] = b"stats.allocated\0";
#[cfg(unix)]
const STATS_ACTIVE: &[u8] = b"stats.active\0";
#[cfg(unix)]
const STATS_RESIDENT: &[u8] = b"stats.resident\0";
#[cfg(unix)]
const STATS_MAPPED: &[u8] = b"stats.mapped\0";
#[cfg(unix)]
const STATS_METADATA: &[u8] = b"stats.metadata\0";
#[cfg(unix)]
const STATS_RETAINED: &[u8] = b"stats.retained\0";

/// Snapshot of jemalloc-reported memory stats.
///
/// All values are bytes. `allocated` is application-requested live bytes;
/// `resident` is the RSS-relevant total (active + metadata + retained pages
/// the kernel has not reclaimed).
#[derive(Debug, Default, Clone, Copy)]
pub struct JemallocStats {
    pub allocated: u64,
    pub active: u64,
    pub resident: u64,
    pub mapped: u64,
    pub metadata: u64,
    pub retained: u64,
}

#[cfg(unix)]
pub fn read_stats() -> anyhow::Result<JemallocStats> {
    // jemalloc requires advancing the epoch before reading stats so that the
    // per-thread caches flush into the global counters.
    let epoch: u64 = 1;
    // SAFETY: `epoch` is the documented mallctl key and `u64` is the
    // expected value type.
    unsafe { tikv_jemalloc_ctl::raw::write(b"epoch\0", epoch) }
        .map_err(|e| anyhow::anyhow!("mallctl epoch advance failed: {e}"))?;

    // SAFETY: each `stats.*` mallctl key returns a `usize`. We read each
    // independently and widen to `u64` for transport.
    Ok(JemallocStats {
        allocated: unsafe { tikv_jemalloc_ctl::raw::read::<usize>(STATS_ALLOCATED) }
            .map_err(|e| anyhow::anyhow!("mallctl stats.allocated failed: {e}"))?
            as u64,
        active: unsafe { tikv_jemalloc_ctl::raw::read::<usize>(STATS_ACTIVE) }
            .map_err(|e| anyhow::anyhow!("mallctl stats.active failed: {e}"))?
            as u64,
        resident: unsafe { tikv_jemalloc_ctl::raw::read::<usize>(STATS_RESIDENT) }
            .map_err(|e| anyhow::anyhow!("mallctl stats.resident failed: {e}"))?
            as u64,
        mapped: unsafe { tikv_jemalloc_ctl::raw::read::<usize>(STATS_MAPPED) }
            .map_err(|e| anyhow::anyhow!("mallctl stats.mapped failed: {e}"))?
            as u64,
        metadata: unsafe { tikv_jemalloc_ctl::raw::read::<usize>(STATS_METADATA) }
            .map_err(|e| anyhow::anyhow!("mallctl stats.metadata failed: {e}"))?
            as u64,
        retained: unsafe { tikv_jemalloc_ctl::raw::read::<usize>(STATS_RETAINED) }
            .map_err(|e| anyhow::anyhow!("mallctl stats.retained failed: {e}"))?
            as u64,
    })
}

#[cfg(not(unix))]
pub fn read_stats() -> anyhow::Result<JemallocStats> {
    Ok(JemallocStats::default())
}

/// Registered object-storage (SlateDB block/meta) cache capacity record.
///
/// SlateDB's `FoyerCache` wrapper does not expose live byte usage, so we
/// register the configured capacities at construction. The configured
/// capacity is the upper bound; the actual RAM footprint of cache contents
/// is reflected in `JemallocStats::allocated`.
#[derive(Debug, Clone)]
pub struct ObjectCacheStat {
    pub shard: String,
    pub kind: ObjectCacheKind,
    pub capacity_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectCacheKind {
    Block,
    Meta,
}

impl ObjectCacheKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectCacheKind::Block => "block",
            ObjectCacheKind::Meta => "meta",
        }
    }
}

static OBJECT_CACHES: Mutex<Vec<ObjectCacheStat>> = Mutex::new(Vec::new());

pub fn register_object_cache(shard: String, kind: ObjectCacheKind, capacity_bytes: u64) {
    let mut guard = OBJECT_CACHES
        .lock()
        .expect("object cache registry poisoned");
    guard.push(ObjectCacheStat {
        shard,
        kind,
        capacity_bytes,
    });
}

pub fn read_object_cache_stats() -> Vec<ObjectCacheStat> {
    OBJECT_CACHES
        .lock()
        .expect("object cache registry poisoned")
        .clone()
}

#[cfg(unix)]
pub fn profiling_enabled() -> anyhow::Result<bool> {
    // SAFETY: The mallctl key and expected return type match jemalloc's
    // `opt.prof` boolean runtime option.
    unsafe { tikv_jemalloc_ctl::raw::read(OPT_PROF) }
        .map_err(|e| anyhow::anyhow!("mallctl opt.prof failed: {e}"))
}

#[cfg(not(unix))]
pub fn profiling_enabled() -> anyhow::Result<bool> {
    Ok(false)
}

#[cfg(unix)]
pub fn profiling_active() -> anyhow::Result<bool> {
    // SAFETY: The mallctl key and expected return type match jemalloc's
    // `prof.active` boolean runtime option.
    unsafe { tikv_jemalloc_ctl::raw::read(PROF_ACTIVE) }
        .map_err(|e| anyhow::anyhow!("mallctl prof.active read failed: {e}"))
}

#[cfg(not(unix))]
pub fn profiling_active() -> anyhow::Result<bool> {
    Ok(false)
}

#[cfg(unix)]
pub fn set_profiling_active(active: bool) -> anyhow::Result<()> {
    // SAFETY: The mallctl key and written value type match jemalloc's
    // `prof.active` boolean runtime option.
    unsafe { tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, active) }
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("mallctl prof.active write failed: {e}"))
}

#[cfg(not(unix))]
pub fn set_profiling_active(_active: bool) -> anyhow::Result<()> {
    anyhow::bail!("heap profiling is only supported on unix targets")
}

pub struct ProfilingActivationGuard {
    deactivate_on_drop: bool,
}

impl ProfilingActivationGuard {
    pub fn activate_if_needed() -> anyhow::Result<Self> {
        let was_active = profiling_active()?;
        if !was_active {
            set_profiling_active(true)?;
        }

        Ok(Self {
            deactivate_on_drop: !was_active,
        })
    }

    pub fn activated_profiling(&self) -> bool {
        self.deactivate_on_drop
    }
}

impl Drop for ProfilingActivationGuard {
    fn drop(&mut self) {
        if self.deactivate_on_drop {
            let _ = set_profiling_active(false);
        }
    }
}

#[cfg(unix)]
pub fn dump_profile(path: &Path) -> anyhow::Result<()> {
    let mut path_bytes = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("invalid heap profile path: {e}"))?
        .into_bytes_with_nul();
    let path_ptr = path_bytes.as_mut_ptr().cast::<c_char>();

    // SAFETY: `prof.dump` expects a mutable pointer to a NUL-terminated path.
    // `path_bytes` is kept alive for the duration of the call.
    unsafe { tikv_jemalloc_ctl::raw::write(PROF_DUMP, path_ptr) }
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("mallctl prof.dump failed: {e}"))
}

#[cfg(not(unix))]
pub fn dump_profile(_path: &Path) -> anyhow::Result<()> {
    anyhow::bail!("heap profiling is only supported on unix targets")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    static HEAP_PROFILE_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn read_stats_returns_nonzero_allocated() -> anyhow::Result<()> {
        let stats = read_stats()?;
        assert!(stats.allocated > 0, "allocated should be > 0");
        assert!(stats.resident >= stats.allocated, "resident >= allocated");
        Ok(())
    }

    #[test]
    fn profiling_guard_deactivates_on_drop() -> anyhow::Result<()> {
        let _lock = HEAP_PROFILE_TEST_MUTEX.lock().unwrap();

        if !profiling_enabled()? {
            anyhow::bail!("heap profiling is not enabled in the allocator")
        }

        set_profiling_active(false)?;
        {
            let guard = ProfilingActivationGuard::activate_if_needed()?;
            assert!(guard.activated_profiling());
            assert!(profiling_active()?);
        }
        assert!(!profiling_active()?);
        Ok(())
    }
}
