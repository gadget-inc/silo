import type { RpcTransport } from "@protobuf-ts/runtime-rpc";
import type { ServiceInfo } from "@protobuf-ts/runtime-rpc";
import type { ResetShardsResponse } from "./silo";
import type { ResetShardsRequest } from "./silo";
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
 * @generated from protobuf service silo.v1.Silo
 */
export interface ISiloClient {
    /**
     * Get cluster topology for client-side routing
     *
     * @generated from protobuf rpc: GetClusterInfo
     */
    getClusterInfo(input: GetClusterInfoRequest, options?: RpcOptions): UnaryCall<GetClusterInfoRequest, GetClusterInfoResponse>;
    /**
     * @generated from protobuf rpc: Enqueue
     */
    enqueue(input: EnqueueRequest, options?: RpcOptions): UnaryCall<EnqueueRequest, EnqueueResponse>;
    /**
     * @generated from protobuf rpc: GetJob
     */
    getJob(input: GetJobRequest, options?: RpcOptions): UnaryCall<GetJobRequest, GetJobResponse>;
    /**
     * Get the result of a completed job.
     * Returns NOT_FOUND if the job doesn't exist.
     * Returns FAILED_PRECONDITION if the job is not yet in a terminal state (still scheduled or running).
     * Returns the result/error/cancellation info if the job has completed.
     *
     * @generated from protobuf rpc: GetJobResult
     */
    getJobResult(input: GetJobResultRequest, options?: RpcOptions): UnaryCall<GetJobResultRequest, GetJobResultResponse>;
    /**
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input: DeleteJobRequest, options?: RpcOptions): UnaryCall<DeleteJobRequest, DeleteJobResponse>;
    /**
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input: CancelJobRequest, options?: RpcOptions): UnaryCall<CancelJobRequest, CancelJobResponse>;
    /**
     * Restart a cancelled or failed job, allowing it to be processed again.
     * Returns FAILED_PRECONDITION if the job is not in a restartable state.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: RestartJob
     */
    restartJob(input: RestartJobRequest, options?: RpcOptions): UnaryCall<RestartJobRequest, RestartJobResponse>;
    /**
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input: LeaseTasksRequest, options?: RpcOptions): UnaryCall<LeaseTasksRequest, LeaseTasksResponse>;
    /**
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input: ReportOutcomeRequest, options?: RpcOptions): UnaryCall<ReportOutcomeRequest, ReportOutcomeResponse>;
    /**
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input: ReportRefreshOutcomeRequest, options?: RpcOptions): UnaryCall<ReportRefreshOutcomeRequest, ReportRefreshOutcomeResponse>;
    /**
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input: HeartbeatRequest, options?: RpcOptions): UnaryCall<HeartbeatRequest, HeartbeatResponse>;
    /**
     * @generated from protobuf rpc: Query
     */
    query(input: QueryRequest, options?: RpcOptions): UnaryCall<QueryRequest, QueryResponse>;
    /**
     * Streaming query that returns Arrow IPC encoded record batches
     * First message contains the schema, subsequent messages contain record batches
     *
     * @generated from protobuf rpc: QueryArrow
     */
    queryArrow(input: QueryArrowRequest, options?: RpcOptions): ServerStreamingCall<QueryArrowRequest, ArrowIpcMessage>;
    /**
     * Admin: Reset all shards owned by this server (dev mode only)
     * Clears all data - jobs, tasks, queues, etc.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
}
/**
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
     * Get cluster topology for client-side routing
     *
     * @generated from protobuf rpc: GetClusterInfo
     */
    getClusterInfo(input: GetClusterInfoRequest, options?: RpcOptions): UnaryCall<GetClusterInfoRequest, GetClusterInfoResponse>;
    /**
     * @generated from protobuf rpc: Enqueue
     */
    enqueue(input: EnqueueRequest, options?: RpcOptions): UnaryCall<EnqueueRequest, EnqueueResponse>;
    /**
     * @generated from protobuf rpc: GetJob
     */
    getJob(input: GetJobRequest, options?: RpcOptions): UnaryCall<GetJobRequest, GetJobResponse>;
    /**
     * Get the result of a completed job.
     * Returns NOT_FOUND if the job doesn't exist.
     * Returns FAILED_PRECONDITION if the job is not yet in a terminal state (still scheduled or running).
     * Returns the result/error/cancellation info if the job has completed.
     *
     * @generated from protobuf rpc: GetJobResult
     */
    getJobResult(input: GetJobResultRequest, options?: RpcOptions): UnaryCall<GetJobResultRequest, GetJobResultResponse>;
    /**
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input: DeleteJobRequest, options?: RpcOptions): UnaryCall<DeleteJobRequest, DeleteJobResponse>;
    /**
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input: CancelJobRequest, options?: RpcOptions): UnaryCall<CancelJobRequest, CancelJobResponse>;
    /**
     * Restart a cancelled or failed job, allowing it to be processed again.
     * Returns FAILED_PRECONDITION if the job is not in a restartable state.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: RestartJob
     */
    restartJob(input: RestartJobRequest, options?: RpcOptions): UnaryCall<RestartJobRequest, RestartJobResponse>;
    /**
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input: LeaseTasksRequest, options?: RpcOptions): UnaryCall<LeaseTasksRequest, LeaseTasksResponse>;
    /**
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input: ReportOutcomeRequest, options?: RpcOptions): UnaryCall<ReportOutcomeRequest, ReportOutcomeResponse>;
    /**
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input: ReportRefreshOutcomeRequest, options?: RpcOptions): UnaryCall<ReportRefreshOutcomeRequest, ReportRefreshOutcomeResponse>;
    /**
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input: HeartbeatRequest, options?: RpcOptions): UnaryCall<HeartbeatRequest, HeartbeatResponse>;
    /**
     * @generated from protobuf rpc: Query
     */
    query(input: QueryRequest, options?: RpcOptions): UnaryCall<QueryRequest, QueryResponse>;
    /**
     * Streaming query that returns Arrow IPC encoded record batches
     * First message contains the schema, subsequent messages contain record batches
     *
     * @generated from protobuf rpc: QueryArrow
     */
    queryArrow(input: QueryArrowRequest, options?: RpcOptions): ServerStreamingCall<QueryArrowRequest, ArrowIpcMessage>;
    /**
     * Admin: Reset all shards owned by this server (dev mode only)
     * Clears all data - jobs, tasks, queues, etc.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
}
//# sourceMappingURL=silo.client.d.ts.map