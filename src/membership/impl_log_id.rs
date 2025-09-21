use crate::membershippb as pb;
use crate::membership::TypeConfig;
use openraft::LogId;

impl From<LogId<TypeConfig>> for pb::LogId {
    fn from(log_id: LogId<TypeConfig>) -> Self {
        pb::LogId {
            term: *log_id.committed_leader_id(),
            index: log_id.index(),
        }
    }
}

impl From<pb::LogId> for LogId<TypeConfig> {
    fn from(proto_log_id: pb::LogId) -> Self {
        LogId::new(proto_log_id.term, proto_log_id.index)
    }
}
