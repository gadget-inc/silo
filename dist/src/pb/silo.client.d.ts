import type { RpcTransport } from "@protobuf-ts/runtime-rpc";
import type { ServiceInfo } from "@protobuf-ts/runtime-rpc";
import type { ForceReleaseShardResponse } from "./silo";
import type { ForceReleaseShardRequest } from "./silo";
import type { ResetShardsResponse } from "./silo";
import type { ResetShardsRequest } from "./silo";
import type { ImportJobsResponse } from "./silo";
import type { ImportJobsRequest } from "./silo";
import type { ConfigureShardResponse } from "./silo";
import type { ConfigureShardRequest } from "./silo";
import type { GetSplitStatusResponse } from "./silo";
import type { GetSplitStatusRequest } from "./silo";
import type { RequestSplitResponse } from "./silo";
import type { RequestSplitRequest } from "./silo";
import type { CpuProfileResponse } from "./silo";
import type { CpuProfileRequest } from "./silo";
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
import type { LeaseTaskResponse } from "./silo";
import type { LeaseTaskRequest } from "./silo";
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
import type { GetNodeInfoResponse } from "./silo";
import type { GetNodeInfoRequest } from "./silo";
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
     * Get information about this node and all the shards it owns.
     *
     * @generated from protobuf rpc: GetNodeInfo
     */
    getNodeInfo(input: GetNodeInfoRequest, options?: RpcOptions): UnaryCall<GetNodeInfoRequest, GetNodeInfoResponse>;
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
     * Lease a specific job's task directly, putting it into Running state.
     * Test-oriented helper: workers should use LeaseTasks for normal processing.
     * Returns FAILED_PRECONDITION if the job is running, terminal, or cancelled.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: LeaseTask
     */
    leaseTask(input: LeaseTaskRequest, options?: RpcOptions): UnaryCall<LeaseTaskRequest, LeaseTaskResponse>;
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
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input: CpuProfileRequest, options?: RpcOptions): UnaryCall<CpuProfileRequest, CpuProfileResponse>;
    /**
     * Request a shard split operation.
     * Initiates splitting a shard into two child shards at the specified split point.
     * Returns FAILED_PRECONDITION if a split is already in progress.
     * Returns NOT_FOUND if the shard doesn't exist on this node.
     * Returns INVALID_ARGUMENT if the split point is outside the shard's range.
     *
     * @generated from protobuf rpc: RequestSplit
     */
    requestSplit(input: RequestSplitRequest, options?: RpcOptions): UnaryCall<RequestSplitRequest, RequestSplitResponse>;
    /**
     * Get the status of a shard split operation.
     * Returns the current phase and child shard IDs if a split is in progress.
     * If no split is in progress, returns with in_progress=false.
     *
     * @generated from protobuf rpc: GetSplitStatus
     */
    getSplitStatus(input: GetSplitStatusRequest, options?: RpcOptions): UnaryCall<GetSplitStatusRequest, GetSplitStatusResponse>;
    /**
     * Configure a shard's placement ring.
     * Changes which placement ring the shard belongs to, affecting which nodes can own it.
     * The shard will be handed off to a node that participates in the new ring.
     * Returns the previous and current ring assignments.
     *
     * @generated from protobuf rpc: ConfigureShard
     */
    configureShard(input: ConfigureShardRequest, options?: RpcOptions): UnaryCall<ConfigureShardRequest, ConfigureShardResponse>;
    /**
     * Import jobs from another system with historical attempts.
     * Unlike Enqueue, ImportJobs accepts completed attempt records and lets Silo
     * take ownership going forward. Used for migrating workloads from other job queues.
     * Each job is imported independently; per-job errors are returned in the response.
     *
     * @generated from protobuf rpc: ImportJobs
     */
    importJobs(input: ImportJobsRequest, options?: RpcOptions): UnaryCall<ImportJobsRequest, ImportJobsResponse>;
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
    /**
     * Force-release a shard lease regardless of the current holder.
     * Operator escape hatch for recovering from permanently lost nodes.
     * After force-release, any live node that desires the shard can acquire it.
     *
     * @generated from protobuf rpc: ForceReleaseShard
     */
    forceReleaseShard(input: ForceReleaseShardRequest, options?: RpcOptions): UnaryCall<ForceReleaseShardRequest, ForceReleaseShardResponse>;
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
     * Get information about this node and all the shards it owns.
     *
     * @generated from protobuf rpc: GetNodeInfo
     */
    getNodeInfo(input: GetNodeInfoRequest, options?: RpcOptions): UnaryCall<GetNodeInfoRequest, GetNodeInfoResponse>;
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
     * Lease a specific job's task directly, putting it into Running state.
     * Test-oriented helper: workers should use LeaseTasks for normal processing.
     * Returns FAILED_PRECONDITION if the job is running, terminal, or cancelled.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: LeaseTask
     */
    leaseTask(input: LeaseTaskRequest, options?: RpcOptions): UnaryCall<LeaseTaskRequest, LeaseTaskResponse>;
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
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input: CpuProfileRequest, options?: RpcOptions): UnaryCall<CpuProfileRequest, CpuProfileResponse>;
    /**
     * Request a shard split operation.
     * Initiates splitting a shard into two child shards at the specified split point.
     * Returns FAILED_PRECONDITION if a split is already in progress.
     * Returns NOT_FOUND if the shard doesn't exist on this node.
     * Returns INVALID_ARGUMENT if the split point is outside the shard's range.
     *
     * @generated from protobuf rpc: RequestSplit
     */
    requestSplit(input: RequestSplitRequest, options?: RpcOptions): UnaryCall<RequestSplitRequest, RequestSplitResponse>;
    /**
     * Get the status of a shard split operation.
     * Returns the current phase and child shard IDs if a split is in progress.
     * If no split is in progress, returns with in_progress=false.
     *
     * @generated from protobuf rpc: GetSplitStatus
     */
    getSplitStatus(input: GetSplitStatusRequest, options?: RpcOptions): UnaryCall<GetSplitStatusRequest, GetSplitStatusResponse>;
    /**
     * Configure a shard's placement ring.
     * Changes which placement ring the shard belongs to, affecting which nodes can own it.
     * The shard will be handed off to a node that participates in the new ring.
     * Returns the previous and current ring assignments.
     *
     * @generated from protobuf rpc: ConfigureShard
     */
    configureShard(input: ConfigureShardRequest, options?: RpcOptions): UnaryCall<ConfigureShardRequest, ConfigureShardResponse>;
    /**
     * Import jobs from another system with historical attempts.
     * Unlike Enqueue, ImportJobs accepts completed attempt records and lets Silo
     * take ownership going forward. Used for migrating workloads from other job queues.
     * Each job is imported independently; per-job errors are returned in the response.
     *
     * @generated from protobuf rpc: ImportJobs
     */
    importJobs(input: ImportJobsRequest, options?: RpcOptions): UnaryCall<ImportJobsRequest, ImportJobsResponse>;
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
    /**
     * Force-release a shard lease regardless of the current holder.
     * Operator escape hatch for recovering from permanently lost nodes.
     * After force-release, any live node that desires the shard can acquire it.
     *
     * @generated from protobuf rpc: ForceReleaseShard
     */
    forceReleaseShard(input: ForceReleaseShardRequest, options?: RpcOptions): UnaryCall<ForceReleaseShardRequest, ForceReleaseShardResponse>;
}
//# sourceMappingURL=silo.client.d.ts.map