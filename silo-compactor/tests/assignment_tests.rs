use silo_compactor::assignment::compute_assignment;
use silo_compactor::shard_map::ShardId;
use uuid::Uuid;

fn shards(n: usize) -> Vec<ShardId> {
    (0..n)
        .map(|i| {
            let mut bytes = [0u8; 16];
            bytes[15] = i as u8;
            bytes[14] = (i >> 8) as u8;
            ShardId(Uuid::from_bytes(bytes))
        })
        .collect()
}

fn names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("pod-{:02}", i)).collect()
}

#[test]
fn even_split_three_pods_twelve_shards() {
    let pods = names(3);
    let s = shards(12);
    let mut total_assigned = 0;
    let mut seen = std::collections::HashSet::new();
    for p in &pods {
        let a = compute_assignment(p, pods.clone(), s.clone(), 12);
        assert_eq!(a.len(), 4, "pod {p} should get exactly 4 shards");
        for sh in &a {
            assert!(seen.insert(*sh), "shard {sh} assigned to multiple pods");
        }
        total_assigned += a.len();
    }
    assert_eq!(total_assigned, 12);
}

#[test]
fn uneven_split_five_pods_twelve_shards_sums_to_twelve() {
    let pods = names(5);
    let s = shards(12);
    let mut total = 0;
    for p in &pods {
        total += compute_assignment(p, pods.clone(), s.clone(), 12).len();
    }
    assert_eq!(total, 12);
}

#[test]
fn cap_respected_one_pod_many_shards() {
    let s = shards(100);
    let a = compute_assignment("pod-00", vec!["pod-00".into()], s, 12);
    assert_eq!(a.len(), 12);
}

#[test]
fn missing_self_returns_empty() {
    let a = compute_assignment("pod-99", names(3), shards(6), 12);
    assert!(a.is_empty());
}

#[test]
fn empty_shards_returns_empty() {
    let a = compute_assignment("pod-00", names(3), Vec::new(), 12);
    assert!(a.is_empty());
}

#[test]
fn permutation_invariant_in_pod_order() {
    let s = shards(8);
    let mut pods_a = vec!["pod-c".to_string(), "pod-a".into(), "pod-b".into()];
    let mut pods_b = vec!["pod-b".to_string(), "pod-c".into(), "pod-a".into()];
    let a = compute_assignment("pod-a", pods_a.clone(), s.clone(), 12);
    let b = compute_assignment("pod-a", pods_b.clone(), s.clone(), 12);
    assert_eq!(a, b);
    pods_a.sort();
    pods_b.sort();
    assert_eq!(pods_a, pods_b);
}

#[test]
fn permutation_invariant_in_shard_order() {
    let pods = names(2);
    let mut s1 = shards(6);
    let mut s2 = s1.clone();
    s2.reverse();
    let a = compute_assignment("pod-00", pods.clone(), s1.clone(), 12);
    let b = compute_assignment("pod-00", pods.clone(), s2, 12);
    assert_eq!(a, b);
    s1.sort_by_key(|x| x.0);
    assert_eq!(a, s1[..3].to_vec());
}

#[test]
fn duplicate_pods_collapsed() {
    let pods = vec!["pod-00".to_string(), "pod-00".into(), "pod-01".into()];
    let s = shards(4);
    let a = compute_assignment("pod-00", pods, s, 12);
    assert_eq!(a.len(), 2);
}
