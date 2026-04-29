use std::ffi::{CString, c_char};
use std::path::Path;
#[cfg(test)]
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
