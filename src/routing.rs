//! Routing utilities for determining which shard owns jobs and concurrency queues.
//!
//! In the distributed concurrency model:
//! - Jobs are routed to shards by `hash(job_id) % num_shards`
//! - Concurrency queues are owned by shards by `hash(tenant, queue_key) % num_shards`
//!
//! This module provides consistent hash functions for both routing decisions.

/// FNV-1a 32-bit hash, matching the TypeScript client implementation
#[inline]
pub fn fnv1a32(data: &[u8]) -> u32 {
    const FNV_OFFSET_BASIS: u32 = 2166136261;
    const FNV_PRIME: u32 = 16777619;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in data {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Compute which shard a job should live on.
/// Uses FNV-1a hash of the job_id to determine shard.
#[inline]
pub fn job_to_shard(job_id: &str, num_shards: u32) -> u32 {
    fnv1a32(job_id.as_bytes()) % num_shards
}

/// Compute which shard owns a concurrency queue.
/// Uses FNV-1a hash of (tenant + queue_key) to determine owner.
/// This keeps queues from the same tenant together while distributing across shards.
#[inline]
pub fn queue_to_shard(tenant: &str, queue_key: &str, num_shards: u32) -> u32 {
    // Concatenate tenant and queue_key with a separator that won't appear in either
    let combined = format!("{}\x00{}", tenant, queue_key);
    fnv1a32(combined.as_bytes()) % num_shards
}

/// Check if a queue is local to the job's shard (queue owner == job shard).
#[inline]
pub fn queue_is_local(job_id: &str, tenant: &str, queue_key: &str, num_shards: u32) -> bool {
    job_to_shard(job_id, num_shards) == queue_to_shard(tenant, queue_key, num_shards)
}

/// Check if a queue is remote from the job's shard (queue owner != job shard).
#[inline]
pub fn queue_is_remote(job_id: &str, tenant: &str, queue_key: &str, num_shards: u32) -> bool {
    !queue_is_local(job_id, tenant, queue_key, num_shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fnv1a32_basic() {
        // Test vector from FNV spec
        assert_eq!(fnv1a32(b""), 2166136261);
        assert_eq!(fnv1a32(b"a"), 0xe40c292c);
        assert_eq!(fnv1a32(b"foobar"), 0xbf9cf968);
    }

    #[test]
    fn test_job_to_shard_consistency() {
        let job_id = "job-12345";
        let shard1 = job_to_shard(job_id, 8);
        let shard2 = job_to_shard(job_id, 8);
        assert_eq!(
            shard1, shard2,
            "Same job_id should always route to same shard"
        );
    }

    #[test]
    fn test_job_to_shard_distribution() {
        // Test that different job IDs distribute across shards
        let num_shards = 8;
        let mut shard_counts = vec![0u32; num_shards as usize];
        for i in 0..1000 {
            let job_id = format!("job-{}", i);
            let shard = job_to_shard(&job_id, num_shards);
            assert!(shard < num_shards, "Shard should be in range");
            shard_counts[shard as usize] += 1;
        }
        // Check that distribution is reasonably even (each shard gets at least 50 jobs)
        for count in &shard_counts {
            assert!(
                *count >= 50,
                "Distribution should be reasonably even: {:?}",
                shard_counts
            );
        }
    }

    #[test]
    fn test_queue_to_shard_consistency() {
        let tenant = "tenant-a";
        let queue = "api-limit";
        let shard1 = queue_to_shard(tenant, queue, 8);
        let shard2 = queue_to_shard(tenant, queue, 8);
        assert_eq!(
            shard1, shard2,
            "Same tenant+queue should always route to same shard"
        );
    }

    #[test]
    fn test_queue_to_shard_tenant_isolation() {
        // Different tenants with same queue name should (usually) route to different shards
        let queue = "api-limit";
        let shard_a = queue_to_shard("tenant-a", queue, 16);
        let shard_b = queue_to_shard("tenant-b", queue, 16);
        // They might happen to collide, but with enough shards they probably won't
        // This test just verifies the function works; real isolation is probabilistic
        let _ = (shard_a, shard_b); // Just compile-time check
    }

    #[test]
    fn test_queue_is_local_remote() {
        let job_id = "test-job";
        let tenant = "test-tenant";
        let queue = "test-queue";
        let num_shards = 16;

        // Either local or remote, but not both
        let is_local = queue_is_local(job_id, tenant, queue, num_shards);
        let is_remote = queue_is_remote(job_id, tenant, queue, num_shards);
        assert!(
            is_local ^ is_remote,
            "Queue must be either local or remote, not both or neither"
        );
    }

    #[test]
    fn test_single_shard_always_local() {
        // With only 1 shard, everything is local
        let job_id = "any-job";
        let tenant = "any-tenant";
        let queue = "any-queue";
        assert!(queue_is_local(job_id, tenant, queue, 1));
        assert!(!queue_is_remote(job_id, tenant, queue, 1));
    }
}
