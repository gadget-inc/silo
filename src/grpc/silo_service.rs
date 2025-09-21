use crate::factory::ShardFactory;
use crate::job_store_shard::{AttemptOutcome, Shard, ShardError, DEFAULT_LEASE_MS};
use crate::membership::Raft;
use crate::pb::silo_server::Silo;
use crate::pb::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

fn map_err(e: ShardError) -> Status {
    Status::internal(e.to_string())
}

/// gRPC service implementation backed by a `ShardFactory`.
#[derive(Clone)]
pub struct SiloService {
    factory: Arc<ShardFactory>,
    my_node_id: u32,
    shard_count: u32,
    raft: Option<Raft>,
    initial_voters: Vec<u64>,
}

impl SiloService {
    pub fn new(
        factory: Arc<ShardFactory>,
        my_node_id: u32,
        shard_count: u32,
        raft: Option<Raft>,
        initial_voters: Vec<u64>,
    ) -> Self {
        Self {
            factory,
            my_node_id,
            shard_count,
            raft,
            initial_voters,
        }
    }

    fn shard(&self, name: &str) -> Result<&Shard, Status> {
        self.factory
            .get(name)
            .ok_or_else(|| Status::not_found("shard not found"))
    }

    async fn shard_ownership_from_raft(&self) -> (Vec<u32>, u32, u32) {
        if let Some(raft) = &self.raft {
            let rx = raft.metrics();
            let metrics = rx.borrow().clone();
            // Extract voter node ids from membership in metrics
            let sm = metrics.membership_config.clone();
            let mut voters: Vec<u64> = sm.membership().voter_ids().collect();
            if voters.is_empty() {
                voters = self.initial_voters.clone();
            }
            let shards = crate::ring::compute_shards_for_node(
                &voters,
                self.shard_count,
                self.my_node_id,
            );
            (shards, self.shard_count, self.my_node_id)
        } else {
            let shards = crate::ring::compute_shards_for_node(
                &self.initial_voters,
                self.shard_count,
                self.my_node_id,
            );
            (shards, self.shard_count, self.my_node_id)
        }
    }
}

#[tonic::async_trait]
impl Silo for SiloService {
    async fn enqueue(
        &self,
        req: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let payload_bytes = r
            .payload
            .as_ref()
            .map(|p| p.data.clone())
            .unwrap_or_default();
        let payload = serde_json::from_slice::<serde_json::Value>(&payload_bytes)
            .unwrap_or(serde_json::Value::Null);
        let retry = r.retry_policy.map(|rp| crate::retry::RetryPolicy {
            retry_count: rp.retry_count as u32,
            initial_interval_ms: rp.initial_interval_ms,
            max_interval_ms: rp.max_interval_ms,
            randomize_interval: rp.randomize_interval,
            backoff_factor: rp.backoff_factor,
        });
        let id = shard
            .enqueue(
                if r.id.is_empty() { None } else { Some(r.id) },
                r.priority as u8,
                r.start_at_ms,
                retry,
                payload,
            )
            .await
            .map_err(map_err)?;
        Ok(Response::new(EnqueueResponse { id }))
    }

    async fn get_job(
        &self,
        req: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let Some(view) = shard.get_job(&r.id).await.map_err(map_err)? else {
            return Err(Status::not_found("job not found"));
        };
        let retry = view.retry_policy();
        let retry_policy = retry.map(|p| RetryPolicy {
            retry_count: p.retry_count,
            initial_interval_ms: p.initial_interval_ms,
            max_interval_ms: p.max_interval_ms,
            randomize_interval: p.randomize_interval,
            backoff_factor: p.backoff_factor,
        });
        let resp = GetJobResponse {
            id: view.id().to_string(),
            priority: view.priority() as u32,
            enqueue_time_ms: view.enqueue_time_ms(),
            payload: Some(JsonValueBytes {
                data: view.payload_bytes().to_vec(),
            }),
            retry_policy,
        };
        Ok(Response::new(resp))
    }

    async fn delete_job(
        &self,
        req: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        shard.delete_job(&r.id).await.map_err(map_err)?;
        Ok(Response::new(DeleteJobResponse {}))
    }

    async fn lease_tasks(
        &self,
        req: Request<LeaseTasksRequest>,
    ) -> Result<Response<LeaseTasksResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let tasks = shard
            .dequeue(&r.worker_id, r.max_tasks as usize)
            .await
            .map_err(map_err)?;
        let mut out = Vec::with_capacity(tasks.len());
        for lt in tasks {
            let job = lt.job();
            let attempt = lt.attempt();
            out.push(Task {
                id: attempt.task_id().to_string(),
                job_id: job.id().to_string(),
                attempt_number: attempt.attempt_number(),
                lease_ms: DEFAULT_LEASE_MS,
                payload: Some(JsonValueBytes {
                    data: job.payload_bytes().to_vec(),
                }),
                priority: job.priority() as u32,
            });
        }
        Ok(Response::new(LeaseTasksResponse { tasks: out }))
    }

    async fn report_outcome(
        &self,
        req: Request<ReportOutcomeRequest>,
    ) -> Result<Response<ReportOutcomeResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let outcome = match r
            .outcome
            .ok_or_else(|| Status::invalid_argument("missing outcome"))?
        {
            report_outcome_request::Outcome::Success(s) => {
                AttemptOutcome::Success { result: s.data }
            }
            report_outcome_request::Outcome::Failure(f) => AttemptOutcome::Error {
                error_code: f.code,
                error: f.data,
            },
        };
        shard
            .report_attempt_outcome(&r.task_id, outcome)
            .await
            .map_err(map_err)?;
        Ok(Response::new(ReportOutcomeResponse {}))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        shard
            .heartbeat_task(&r.worker_id, &r.task_id)
            .await
            .map_err(map_err)?;
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn get_shard_ownership(
        &self,
        _req: Request<GetShardOwnershipRequest>,
    ) -> Result<Response<GetShardOwnershipResponse>, Status> {
        let (shards, shard_count, node_id) = self.shard_ownership_from_raft().await;
        Ok(Response::new(GetShardOwnershipResponse {
            shards,
            shard_count,
            node_id,
        }))
    }
}
