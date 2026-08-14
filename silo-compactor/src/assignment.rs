use crate::shard_map::ShardId;

/// Compute the slice of shards this pod is responsible for.
///
/// Sorts pods lexicographically and shards by UUID, then assigns the contiguous
/// range `[my_idx * n / total, (my_idx + 1) * n / total)`. Returns at most
/// `max_shards` items.
///
/// Returns an empty vec if `self_name` isn't in `pods` (this pod is not yet a
/// member — wait for the next reconcile tick).
pub fn compute_assignment(
    self_name: &str,
    mut pods: Vec<String>,
    mut shards: Vec<ShardId>,
    max_shards: usize,
) -> Vec<ShardId> {
    if shards.is_empty() || pods.is_empty() {
        return Vec::new();
    }

    pods.sort();
    pods.dedup();
    shards.sort_by_key(|s| s.0);
    shards.dedup();

    // Counts must be taken after dedup: the range arithmetic below indexes
    // into the deduped vecs, and a duplicated pod count would shrink (or,
    // for duplicated shards, overrun) every pod's slice.
    let total_pods = pods.len();
    let n = shards.len();

    let Some(my_idx) = pods.iter().position(|n| n == self_name) else {
        return Vec::new();
    };

    let start = (my_idx * n) / total_pods;
    let end = ((my_idx + 1) * n) / total_pods;
    shards[start..end]
        .iter()
        .take(max_shards)
        .copied()
        .collect()
}
