use anyhow::Result;
use once_cell::sync::Lazy;
use std::sync::RwLock;
use tonic::Request;

#[derive(Clone, Debug, Default)]
pub struct ShardAssignment {
    pub shard_id: u32,
    pub node_id: u32,
}

#[derive(Clone, Debug, Default)]
pub struct ShardMap {
    pub shard_count: u32,
    pub assignments: Vec<ShardAssignment>,
}

pub async fn get_shard_map() -> Result<Option<ShardMap>> {
    Ok(GLOBAL_SHARD_MAP.read().unwrap().clone())
}

pub async fn join_cluster(endpoints: Vec<String>, node_id: u32, rpc_addr: String) -> Result<()> {
    use crate::membershippb as pb;
    use crate::membershippb::membership_service_client::MembershipServiceClient;

    for ep in endpoints {
        if let Ok(mut client) = MembershipServiceClient::connect(ep.clone()).await {
            let _ = client
                .add_learner(Request::new(pb::AddLearnerRequest {
                    node: Some(pb::Node {
                        node_id: node_id as u64,
                        rpc_addr: rpc_addr.clone(),
                    }),
                }))
                .await;
            if client
                .change_membership(Request::new(pb::ChangeMembershipRequest {
                    members: vec![node_id as u64],
                    retain: true,
                }))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
    }
    anyhow::bail!("failed to contact any membership endpoint for join")
}

pub async fn remove_node(endpoints: &[String], node_id: u32) -> Result<()> {
    use crate::membershippb as pb;
    use crate::membershippb::membership_service_client::MembershipServiceClient;

    let remaining: Vec<u64> = endpoints
        .iter()
        .filter_map(|ep| infer_node_id_from_endpoint(ep))
        .filter(|id| *id != node_id as u64)
        .collect();

    for ep in endpoints {
        if let Ok(mut client) = MembershipServiceClient::connect(ep.clone()).await {
            if client
                .change_membership(Request::new(pb::ChangeMembershipRequest {
                    members: remaining.clone(),
                    retain: false,
                }))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
    }
    anyhow::bail!("failed to contact any membership endpoint for remove")
}

pub async fn seed_if_empty(initial_nodes: &[u32], shard_count: u32) {
    let mut guard = GLOBAL_SHARD_MAP.write().unwrap();
    if guard.is_none() {
        let nodes: Vec<u32> = {
            let mut v = initial_nodes.to_vec();
            v.sort_unstable();
            v.dedup();
            v
        };
        *guard = Some(ShardMap {
            shard_count,
            assignments: compute_assignments(&nodes, shard_count),
        });
    }
}

fn compute_assignments(nodes: &[u32], shard_count: u32) -> Vec<ShardAssignment> {
    if nodes.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(shard_count as usize);
    for shard_id in 0..shard_count {
        let idx = (shard_id as usize) % nodes.len();
        out.push(ShardAssignment {
            shard_id,
            node_id: nodes[idx],
        });
    }
    out
}

static GLOBAL_SHARD_MAP: Lazy<RwLock<Option<ShardMap>>> = Lazy::new(|| RwLock::new(None));

fn infer_node_id_from_endpoint(endpoint: &str) -> Option<u64> {
    let port = endpoint.rsplit(':').next()?;
    let num: u64 = port.parse().ok()?;
    if num >= 19080 { Some(num - 19080) } else { None }
}
