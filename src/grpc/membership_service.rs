use openraft::Snapshot;
use std::collections::BTreeMap;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;

use crate::membership::*;
use crate::membershippb as pb;
use crate::membershippb::membership_service_server::MembershipService;
use crate::membershippb::VoteRequest;
use crate::membershippb::VoteResponse;

/// Internal gRPC service implementation for Raft protocol communications that manage cluster membership.
/// This service handles the core Raft consensus protocol operations between cluster nodes.
///
/// # Responsibilities
/// - Vote requests/responses during leader election
/// - Log replication between nodes
/// - Snapshot installation for state synchronization
///
/// # Protocol Safety
/// This service implements critical consensus protocol operations and should only be
/// exposed to other trusted Raft cluster nodes, never to external clients.
pub struct MembershipServiceImpl {
    /// The local Raft node instance that this service operates on
    raft_node: Raft,
}

impl MembershipServiceImpl {
    /// Creates a new instance of the internal service
    ///
    /// # Arguments
    /// * `raft_node` - The Raft node instance this service will operate on
    pub fn new(raft_node: Raft) -> Self {
        MembershipServiceImpl { raft_node }
    }
}

#[tonic::async_trait]
impl MembershipService for MembershipServiceImpl {
    /// Initializes a new Raft cluster with the specified nodes
    ///
    /// # Arguments
    /// * `request` - Contains the initial set of nodes for the cluster
    ///
    /// # Returns
    /// * Success response with initialization details
    /// * Error if initialization fails
    async fn init(&self, request: Request<pb::InitRequest>) -> Result<Response<()>, Status> {
        debug!("Initializing Raft cluster");
        let req = request.into_inner();

        // Convert nodes into required format
        let nodes_map: BTreeMap<u64, pb::Node> = req
            .nodes
            .into_iter()
            .map(|node| {
                (
                    node.node_id,
                    pb::Node {
                        rpc_addr: node.rpc_addr,
                        node_id: node.node_id,
                    },
                )
            })
            .collect();

        // Initialize the cluster
        let result = self
            .raft_node
            .initialize(nodes_map)
            .await
            .map_err(|e| Status::internal(format!("Failed to initialize cluster: {}", e)))?;

        debug!("Cluster initialization successful");
        Ok(Response::new(result))
    }

    /// Adds a learner node to the Raft cluster
    ///
    /// # Arguments
    /// * `request` - Contains the node information and blocking preference
    ///
    /// # Returns
    /// * Success response with learner addition details
    /// * Error if the operation fails
    async fn add_learner(
        &self,
        request: Request<pb::AddLearnerRequest>,
    ) -> Result<Response<pb::ClientWriteResponse>, Status> {
        let req = request.into_inner();

        let node = req
            .node
            .ok_or_else(|| Status::internal("Node information is required"))?;

        debug!("Adding learner node {}", node.node_id);

        let raft_node = Node {
            rpc_addr: node.rpc_addr.clone(),
            node_id: node.node_id,
        };

        let result = self
            .raft_node
            .add_learner(node.node_id, raft_node, true)
            .await
            .map_err(|e| Status::internal(format!("Failed to add learner node: {}", e)))?;

        debug!("Successfully added learner node {}", node.node_id);
        Ok(Response::new(result.into()))
    }

    /// Changes the membership of the Raft cluster
    ///
    /// # Arguments
    /// * `request` - Contains the new member set and retention policy
    ///
    /// # Returns
    /// * Success response with membership change details
    /// * Error if the operation fails
    async fn change_membership(
        &self,
        request: Request<pb::ChangeMembershipRequest>,
    ) -> Result<Response<pb::ClientWriteResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Changing membership. Members: {:?}, Retain: {}",
            req.members, req.retain
        );

        let result = self
            .raft_node
            .change_membership(req.members, req.retain)
            .await
            .map_err(|e| Status::internal(format!("Failed to change membership: {}", e)))?;

        debug!("Successfully changed cluster membership");
        Ok(Response::new(result.into()))
    }

    /// Handles vote requests during leader election.
    ///
    /// # Arguments
    /// * `request` - The vote request containing candidate information
    ///
    /// # Returns
    /// * `Ok(Response)` - Vote response indicating whether the vote was granted
    /// * `Err(Status)` - Error status if the vote operation fails
    ///
    /// # Protocol Details
    /// This implements the RequestVote RPC from the Raft protocol.
    /// Nodes vote for candidates based on log completeness and term numbers.
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        debug!("Processing vote request");

        let vote_resp = self
            .raft_node
            .vote(request.into_inner().into())
            .await
            .map_err(|e| Status::internal(format!("Vote operation failed: {}", e)))?;

        debug!("Vote request processed successfully");
        Ok(Response::new(vote_resp.into()))
    }

    /// Handles append entries requests for log replication.
    ///
    /// # Arguments
    /// * `request` - The append entries request containing log entries to replicate
    ///
    /// # Returns
    /// * `Ok(Response)` - Response indicating success/failure of the append operation
    /// * `Err(Status)` - Error status if the append operation fails
    ///
    /// # Protocol Details
    /// This implements the AppendEntries RPC from the Raft protocol.
    /// Used for both log replication and as heartbeat mechanism.
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        debug!("Processing append entries request");

        let append_resp = self
            .raft_node
            .append_entries(request.into_inner().into())
            .await
            .map_err(|e| Status::internal(format!("Append entries operation failed: {}", e)))?;

        debug!("Append entries request processed successfully");
        Ok(Response::new(append_resp.into()))
    }

    /// Handles snapshot installation requests for state transfer using streaming.
    ///
    /// # Arguments
    /// * `request` - Stream of snapshot chunks with metadata
    ///
    /// # Returns
    /// * `Ok(Response)` - Response indicating success/failure of snapshot installation
    /// * `Err(Status)` - Error status if the snapshot operation fails
    async fn snapshot(
        &self,
        request: Request<Streaming<pb::SnapshotRequest>>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        debug!("Processing streaming snapshot installation request");
        let mut stream = request.into_inner();

        // Get the first chunk which contains metadata
        let first_chunk = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty snapshot stream"))??;

        let vote;
        let snapshot_meta;
        {
            let meta = first_chunk
                .into_meta()
                .ok_or_else(|| Status::invalid_argument("First snapshot chunk must be metadata"))?;

            debug!("Received snapshot metadata chunk: {:?}", meta);

            vote = meta.vote.unwrap();

            snapshot_meta = SnapshotMeta {
                last_log_id: meta.last_log_id.map(|log_id| log_id.into()),
                last_membership: StoredMembership::new(
                    meta.last_membership_log_id.map(|x| x.into()),
                    meta.last_membership.unwrap().into(),
                ),
                snapshot_id: meta.snapshot_id,
            };
        }

        // Collect snapshot data
        let mut snapshot_data_bytes = Vec::new();

        while let Some(chunk) = stream.next().await {
            let data = chunk?
                .into_data_chunk()
                .ok_or_else(|| Status::invalid_argument("Snapshot chunk must be data"))?;
            snapshot_data_bytes.extend_from_slice(&data);
        }

        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: snapshot_data_bytes,
        };

        // Install the full snapshot
        let snapshot_resp = self
            .raft_node
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("Snapshot installation failed: {}", e)))?;

        debug!("Streaming snapshot installation request processed successfully");
        Ok(Response::new(pb::SnapshotResponse {
            vote: Some(snapshot_resp.vote),
        }))
    }
}
