use anyhow::Result;
use std::hash::{Hash, Hasher};

fn hash_u64<T: Hash>(t: &T) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn compute_shards_for_node(voters: &[u64], shard_count: u32, my_node_id: u32) -> Vec<u32> {
    if voters.is_empty() || shard_count == 0 {
        return Vec::new();
    }
    // Sort voters for stability
    let mut voters: Vec<u64> = voters.to_vec();
    voters.sort_unstable();

    // Build continuum: for each voter create virtual nodes for better balance
    let virtual_nodes = 100u32;
    let mut ring: Vec<(u64, u64)> = Vec::with_capacity(voters.len() * virtual_nodes as usize);
    for v in &voters {
        for n in 0..virtual_nodes {
            let key = (*v, n);
            ring.push((hash_u64(&key), *v));
        }
    }
    ring.sort_unstable_by_key(|x| x.0);

    // Assign each shard to the first node clockwise from its hash position
    let mut mine = Vec::new();
    for shard_id in 0..shard_count {
        let shard_hash = hash_u64(&shard_id);
        let idx = match ring.binary_search_by_key(&shard_hash, |x| x.0) {
            Ok(i) => i,
            Err(i) => {
                if i >= ring.len() {
                    0
                } else {
                    i
                }
            }
        };
        let owner = ring[idx].1 as u32;
        if owner == my_node_id {
            mine.push(shard_id);
        }
    }
    mine
}

/// Join the cluster via OpenRaft admin API.
pub async fn join_cluster(
    _bootstrap_endpoints: Vec<String>,
    node_id: u32,
    address: String,
) -> Result<()> {
    // TODO: implement via real Raft admin operations
    let _ = (_bootstrap_endpoints, node_id, address);
    Ok(())
}

/// Remove a node from the cluster via OpenRaft membership change.
pub async fn remove_node(_bootstrap_endpoints: &[String], _node_id: u32) -> Result<()> {
    // TODO: implement via real Raft admin operations
    let _ = (_bootstrap_endpoints, _node_id);
    Ok(())
}

// Helper functions for OpenRaft admin operations will be added here when needed

/// Compute and commit a new shard map to the OpenRaft state machine.
pub async fn commit_shard_map(
    _raft_leader_endpoint: &str,
    _shard_count: u32,
    _active_nodes: &[u32],
) -> Result<()> {
    // TODO: persist to Raft state machine via client-write
    Ok(())
}

/// Helper to recompute shard map based on current membership
async fn recompute_and_commit_shard_map() -> Result<()> {
    Ok(())
}
