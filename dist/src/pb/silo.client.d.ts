import type { RpcTransport } from "@protobuf-ts/runtime-rpc";
import type { ServiceInfo } from "@protobuf-ts/runtime-rpc";
import type { CpuProfileResponse } from "./silo";
import type { CpuProfileRequest } from "./silo";
import type { ResetShardsResponse } from "./silo";
import type { ResetShardsRequest } from "./silo";
import type { GetShardCountersResponse } from "./silo";
import type { GetShardCountersRequest } from "./silo";
import type { ArrowIpcMessage } from "./silo";
import type { QueryArrowRequest } from "./silo";
import type { ServerStreamingCall } from "@protobuf-ts/runtime-rpc";
import type { QueryResponse } from "./silo";
import type { QueryRequest } from "./silo";
import type { HeartbeatResponse } from "./silo";
import type { HeartbeatRequest } from "./silo";
import type { ReportRefreshOutcomeResponse } from "./silo";
import type { ReportRefreshOutcomeRequest } from "./silo";
import type { ReportOutcomeResponse } from "./silo";
import type { ReportOutcomeRequest } from "./silo";
import type { LeaseTasksResponse } from "./silo";
import type { LeaseTasksRequest } from "./silo";
import type { ExpediteJobResponse } from "./silo";
import type { ExpediteJobRequest } from "./silo";
import type { RestartJobResponse } from "./silo";
import type { RestartJobRequest } from "./silo";
import type { CancelJobResponse } from "./silo";
import type { CancelJobRequest } from "./silo";
import type { DeleteJobResponse } from "./silo";
import type { DeleteJobRequest } from "./silo";
import type { GetJobResultResponse } from "./silo";
import type { GetJobResultRequest } from "./silo";
import type { GetJobResponse } from "./silo";
import type { GetJobRequest } from "./silo";
import type { EnqueueResponse } from "./silo";
import type { EnqueueRequest } from "./silo";
import type { GetClusterInfoResponse } from "./silo";
import type { GetClusterInfoRequest } from "./silo";
import type { UnaryCall } from "@protobuf-ts/runtime-rpc";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
/**
 * The Silo job queue service.
 *
 * @generated from protobuf service silo.v1.Silo
 */
export interface ISiloClient {
    /**
     * Get cluster topology for client-side routing.
     * Returns shard ownership information so clients can route requests to the correct node.
     *
     * @generated from protobuf rpc: GetClusterInfo
     */
    getClusterInfo(input: GetClusterInfoRequest, options?: RpcOptions): UnaryCall<GetClusterInfoRequest, GetClusterInfoResponse>;
    /**
     * Enqueue a new job for processing.
     * The job will be scheduled according to its start_at_ms and processed when limits allow.
     *
     * @generated from protobuf rpc: Enqueue
     */
    enqueue(input: EnqueueRequest, options?: RpcOptions): UnaryCall<EnqueueRequest, EnqueueResponse>;
    /**
     * Get full details of a job including its current status.
     *
     * @generated from protobuf rpc: GetJob
     */
    getJob(input: GetJobRequest, options?: RpcOptions): UnaryCall<GetJobRequest, GetJobResponse>;
    /**
     * Get the result of a completed job.
     * Returns NOT_FOUND if the job doesn't exist.
     * Returns FAILED_PRECONDITION if the job hasn't reached a terminal state.
     *
     * @generated from protobuf rpc: GetJobResult
     */
    getJobResult(input: GetJobResultRequest, options?: RpcOptions): UnaryCall<GetJobResultRequest, GetJobResultResponse>;
    /**
     * Permanently delete a job and all its data.
     * Running jobs should be cancelled first.
     *
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input: DeleteJobRequest, options?: RpcOptions): UnaryCall<DeleteJobRequest, DeleteJobResponse>;
    /**
     * Cancel a job. Running jobs will be notified via heartbeat.
     * Jobs in any state can be cancelled.
     *
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input: CancelJobRequest, options?: RpcOptions): UnaryCall<CancelJobRequest, CancelJobResponse>;
    /**
     * Restart a cancelled or failed job for another attempt.
     * Returns FAILED_PRECONDITION if job is not in a restartable state.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: RestartJob
     */
    restartJob(input: RestartJobRequest, options?: RpcOptions): UnaryCall<RestartJobRequest, RestartJobResponse>;
    /**
     * Expedite a future-scheduled job to run immediately.
     * Useful for dragging forward scheduled jobs or skipping retry backoff delays.
     * Returns FAILED_PRECONDITION if job is not in an expeditable state
     * (already running, terminal, cancelled, or task already ready to run).
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: ExpediteJob
     */
    expediteJob(input: ExpediteJobRequest, options?: RpcOptions): UnaryCall<ExpediteJobRequest, ExpediteJobResponse>;
    /**
     * Lease tasks for a worker to process.
     * Workers should call this periodically to get work.
     * Returns both job tasks and floating limit refresh tasks.
     *
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input: LeaseTasksRequest, options?: RpcOptions): UnaryCall<LeaseTasksRequest, LeaseTasksResponse>;
    /**
     * Report the outcome of a completed job task.
     * Must be called before the task lease expires.
     *
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input: ReportOutcomeRequest, options?: RpcOptions): UnaryCall<ReportOutcomeRequest, ReportOutcomeResponse>;
    /**
     * Report the outcome of a floating limit refresh task.
     * Workers compute new max_concurrency and report here.
     *
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input: ReportRefreshOutcomeRequest, options?: RpcOptions): UnaryCall<ReportRefreshOutcomeRequest, ReportRefreshOutcomeResponse>;
    /**
     * Extend a task lease and check for cancellation.
     * Workers must heartbeat before lease expires to keep tasks.
     * Returns cancelled=true if the job was cancelled.
     *
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input: HeartbeatRequest, options?: RpcOptions): UnaryCall<HeartbeatRequest, HeartbeatResponse>;
    /**
     * Execute an SQL query against shard data.
     * Returns results as JSON rows.
     *
     * @generated from protobuf rpc: Query
     */
    query(input: QueryRequest, options?: RpcOptions): UnaryCall<QueryRequest, QueryResponse>;
    /**
     * Execute an SQL query with Arrow IPC streaming response.
     * More efficient for large result sets.
     * First message contains schema, subsequent messages contain record batches.
     *
     * @generated from protobuf rpc: QueryArrow
     */
    queryArrow(input: QueryArrowRequest, options?: RpcOptions): ServerStreamingCall<QueryArrowRequest, ArrowIpcMessage>;
    /**
     * Get job counters for a shard.
     * Returns total jobs and completed jobs without scanning all job data.
     *
     * @generated from protobuf rpc: GetShardCounters
     */
    getShardCounters(input: GetShardCountersRequest, options?: RpcOptions): UnaryCall<GetShardCountersRequest, GetShardCountersResponse>;
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
    /**
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input: CpuProfileRequest, options?: RpcOptions): UnaryCall<CpuProfileRequest, CpuProfileResponse>;
}
/**
 * The Silo job queue service.
 *
 * @generated from protobuf service silo.v1.Silo
 */
export declare class SiloClient implements ISiloClient, ServiceInfo {
    private readonly _transport;
    typeName: string;
    methods: import("@protobuf-ts/runtime-rpc").MethodInfo<any, any>[];
    options: {
        [extensionName: string]: import("@protobuf-ts/runtime").JsonValue;
    };
    constructor(_transport: RpcTransport);
    /**
     * Get cluster topology for client-side routing.
     * Returns shard ownership information so clients can route requests to the correct node.
     *
     * @generated from protobuf rpc: GetClusterInfo
     */
    getClusterInfo(input: GetClusterInfoRequest, options?: RpcOptions): UnaryCall<GetClusterInfoRequest, GetClusterInfoResponse>;
    /**
     * Enqueue a new job for processing.
     * The job will be scheduled according to its start_at_ms and processed when limits allow.
     *
     * @generated from protobuf rpc: Enqueue
     */
    enqueue(input: EnqueueRequest, options?: RpcOptions): UnaryCall<EnqueueRequest, EnqueueResponse>;
    /**
     * Get full details of a job including its current status.
     *
     * @generated from protobuf rpc: GetJob
     */
    getJob(input: GetJobRequest, options?: RpcOptions): UnaryCall<GetJobRequest, GetJobResponse>;
    /**
     * Get the result of a completed job.
     * Returns NOT_FOUND if the job doesn't exist.
     * Returns FAILED_PRECONDITION if the job hasn't reached a terminal state.
     *
     * @generated from protobuf rpc: GetJobResult
     */
    getJobResult(input: GetJobResultRequest, options?: RpcOptions): UnaryCall<GetJobResultRequest, GetJobResultResponse>;
    /**
     * Permanently delete a job and all its data.
     * Running jobs should be cancelled first.
     *
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input: DeleteJobRequest, options?: RpcOptions): UnaryCall<DeleteJobRequest, DeleteJobResponse>;
    /**
     * Cancel a job. Running jobs will be notified via heartbeat.
     * Jobs in any state can be cancelled.
     *
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input: CancelJobRequest, options?: RpcOptions): UnaryCall<CancelJobRequest, CancelJobResponse>;
    /**
     * Restart a cancelled or failed job for another attempt.
     * Returns FAILED_PRECONDITION if job is not in a restartable state.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: RestartJob
     */
    restartJob(input: RestartJobRequest, options?: RpcOptions): UnaryCall<RestartJobRequest, RestartJobResponse>;
    /**
     * Expedite a future-scheduled job to run immediately.
     * Useful for dragging forward scheduled jobs or skipping retry backoff delays.
     * Returns FAILED_PRECONDITION if job is not in an expeditable state
     * (already running, terminal, cancelled, or task already ready to run).
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: ExpediteJob
     */
    expediteJob(input: ExpediteJobRequest, options?: RpcOptions): UnaryCall<ExpediteJobRequest, ExpediteJobResponse>;
    /**
     * Lease tasks for a worker to process.
     * Workers should call this periodically to get work.
     * Returns both job tasks and floating limit refresh tasks.
     *
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input: LeaseTasksRequest, options?: RpcOptions): UnaryCall<LeaseTasksRequest, LeaseTasksResponse>;
    /**
     * Report the outcome of a completed job task.
     * Must be called before the task lease expires.
     *
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input: ReportOutcomeRequest, options?: RpcOptions): UnaryCall<ReportOutcomeRequest, ReportOutcomeResponse>;
    /**
     * Report the outcome of a floating limit refresh task.
     * Workers compute new max_concurrency and report here.
     *
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input: ReportRefreshOutcomeRequest, options?: RpcOptions): UnaryCall<ReportRefreshOutcomeRequest, ReportRefreshOutcomeResponse>;
    /**
     * Extend a task lease and check for cancellation.
     * Workers must heartbeat before lease expires to keep tasks.
     * Returns cancelled=true if the job was cancelled.
     *
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input: HeartbeatRequest, options?: RpcOptions): UnaryCall<HeartbeatRequest, HeartbeatResponse>;
    /**
     * Execute an SQL query against shard data.
     * Returns results as JSON rows.
     *
     * @generated from protobuf rpc: Query
     */
    query(input: QueryRequest, options?: RpcOptions): UnaryCall<QueryRequest, QueryResponse>;
    /**
     * Execute an SQL query with Arrow IPC streaming response.
     * More efficient for large result sets.
     * First message contains schema, subsequent messages contain record batches.
     *
     * @generated from protobuf rpc: QueryArrow
     */
    queryArrow(input: QueryArrowRequest, options?: RpcOptions): ServerStreamingCall<QueryArrowRequest, ArrowIpcMessage>;
    /**
     * Get job counters for a shard.
     * Returns total jobs and completed jobs without scanning all job data.
     *
     * @generated from protobuf rpc: GetShardCounters
     */
    getShardCounters(input: GetShardCountersRequest, options?: RpcOptions): UnaryCall<GetShardCountersRequest, GetShardCountersResponse>;
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
    /**
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input: CpuProfileRequest, options?: RpcOptions): UnaryCall<CpuProfileRequest, CpuProfileResponse>;
}
//# sourceMappingURL=silo.client.d.ts.map