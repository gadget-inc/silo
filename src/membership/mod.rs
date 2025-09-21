use crate::membershippb;

pub mod admin;
pub mod impl_append_entries_request;
pub mod impl_append_entries_response;
pub mod impl_client_write_response;
pub mod impl_entry;
pub mod impl_leader_id;
pub mod impl_log_id;
pub mod impl_membership;
pub mod impl_snapshot_request;
pub mod impl_vote;
pub mod impl_vote_request;
pub mod impl_vote_response;
pub mod mem_log_store;
pub mod network;
pub mod store;

// Re-export admin helpers at module root for external callers/tests
pub use admin::{get_shard_map, join_cluster, remove_node, seed_if_empty};

// Re-export the main types from submodules
pub use network::Network;
pub use store::{LogStore, StateMachineStore};

#[derive(Debug, Clone, Copy, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = membershippb::SetRequest;
    type R = membershippb::Response;
    type NodeId = u64;
    type Node = membershippb::Node;
    type Entry = membershippb::Entry;
    type SnapshotData = Vec<u8>;
    type AsyncRuntime = openraft::TokioRuntime;

    // Required types in the main branch
    type Term = u64;
    type LeaderId = membershippb::LeaderId;
    type Vote = membershippb::Vote;
    type Responder = openraft::impls::OneshotResponder<TypeConfig>;
}

pub type Raft = openraft::Raft<TypeConfig>;
pub type Vote = <TypeConfig as openraft::RaftTypeConfig>::Vote;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
pub type LogId = openraft::LogId<TypeConfig>;
pub type Entry = <TypeConfig as openraft::RaftTypeConfig>::Entry;
pub type EntryPayload = openraft::EntryPayload<TypeConfig>;
pub type Membership = openraft::membership::Membership<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;

pub type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;

pub type LogState = openraft::storage::LogState<TypeConfig>;

pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type SnapshotData = <TypeConfig as openraft::RaftTypeConfig>::SnapshotData;

pub type IOFlushed = openraft::storage::IOFlushed<TypeConfig>;

pub type Infallible = openraft::error::Infallible;
pub type Fatal = openraft::error::Fatal<TypeConfig>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TypeConfig, E>;
pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TypeConfig, E>;

pub type ErrorSubject = openraft::ErrorSubject<TypeConfig>;
pub type StorageError = openraft::StorageError<TypeConfig>;
pub type StreamingError = openraft::error::StreamingError<TypeConfig>;

pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;

pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TypeConfig>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type InitializeError = openraft::error::InitializeError<TypeConfig>;

pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;

pub type NodeId = u64;
