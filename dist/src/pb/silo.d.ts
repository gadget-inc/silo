import { ServiceType } from "@protobuf-ts/runtime-rpc";
import type { BinaryWriteOptions } from "@protobuf-ts/runtime";
import type { IBinaryWriter } from "@protobuf-ts/runtime";
import type { BinaryReadOptions } from "@protobuf-ts/runtime";
import type { IBinaryReader } from "@protobuf-ts/runtime";
import type { PartialMessage } from "@protobuf-ts/runtime";
import { MessageType } from "@protobuf-ts/runtime";
/**
 * Core job messages
 *
 * @generated from protobuf message silo.v1.JsonValueBytes
 */
export interface JsonValueBytes {
    /**
     * @generated from protobuf field: bytes data = 1
     */
    data: Uint8Array;
}
/**
 * @generated from protobuf message silo.v1.RetryPolicy
 */
export interface RetryPolicy {
    /**
     * @generated from protobuf field: uint32 retry_count = 1
     */
    retryCount: number;
    /**
     * @generated from protobuf field: int64 initial_interval_ms = 2
     */
    initialIntervalMs: bigint;
    /**
     * @generated from protobuf field: int64 max_interval_ms = 3
     */
    maxIntervalMs: bigint;
    /**
     * @generated from protobuf field: bool randomize_interval = 4
     */
    randomizeInterval: boolean;
    /**
     * @generated from protobuf field: double backoff_factor = 5
     */
    backoffFactor: number;
}
/**
 * Per-job concurrency limit declaration
 *
 * @generated from protobuf message silo.v1.ConcurrencyLimit
 */
export interface ConcurrencyLimit {
    /**
     * @generated from protobuf field: string key = 1
     */
    key: string;
    /**
     * @generated from protobuf field: uint32 max_concurrency = 2
     */
    maxConcurrency: number;
}
/**
 * Floating concurrency limit - max concurrency is dynamic and refreshed by workers
 *
 * @generated from protobuf message silo.v1.FloatingConcurrencyLimit
 */
export interface FloatingConcurrencyLimit {
    /**
     * @generated from protobuf field: string key = 1
     */
    key: string;
    /**
     * @generated from protobuf field: uint32 default_max_concurrency = 2
     */
    defaultMaxConcurrency: number;
    /**
     * @generated from protobuf field: int64 refresh_interval_ms = 3
     */
    refreshIntervalMs: bigint;
    /**
     * @generated from protobuf field: map<string, string> metadata = 4
     */
    metadata: {
        [key: string]: string;
    };
}
/**
 * Retry policy specifically for rate limit check retries (when rate limit is exceeded)
 *
 * @generated from protobuf message silo.v1.RateLimitRetryPolicy
 */
export interface RateLimitRetryPolicy {
    /**
     * @generated from protobuf field: int64 initial_backoff_ms = 1
     */
    initialBackoffMs: bigint;
    /**
     * @generated from protobuf field: int64 max_backoff_ms = 2
     */
    maxBackoffMs: bigint;
    /**
     * @generated from protobuf field: double backoff_multiplier = 3
     */
    backoffMultiplier: number;
    /**
     * @generated from protobuf field: uint32 max_retries = 4
     */
    maxRetries: number;
}
/**
 * Gubernator-based rate limit declaration
 *
 * @generated from protobuf message silo.v1.GubernatorRateLimit
 */
export interface GubernatorRateLimit {
    /**
     * @generated from protobuf field: string name = 1
     */
    name: string;
    /**
     * @generated from protobuf field: string unique_key = 2
     */
    uniqueKey: string;
    /**
     * @generated from protobuf field: int64 limit = 3
     */
    limit: bigint;
    /**
     * @generated from protobuf field: int64 duration_ms = 4
     */
    durationMs: bigint;
    /**
     * @generated from protobuf field: int32 hits = 5
     */
    hits: number;
    /**
     * @generated from protobuf field: silo.v1.GubernatorAlgorithm algorithm = 6
     */
    algorithm: GubernatorAlgorithm;
    /**
     * @generated from protobuf field: int32 behavior = 7
     */
    behavior: number;
    /**
     * @generated from protobuf field: silo.v1.RateLimitRetryPolicy retry_policy = 8
     */
    retryPolicy?: RateLimitRetryPolicy;
}
/**
 * A single limit that can be either a concurrency limit, rate limit, or floating concurrency limit
 *
 * @generated from protobuf message silo.v1.Limit
 */
export interface Limit {
    /**
     * @generated from protobuf oneof: limit
     */
    limit: {
        oneofKind: "concurrency";
        /**
         * @generated from protobuf field: silo.v1.ConcurrencyLimit concurrency = 1
         */
        concurrency: ConcurrencyLimit;
    } | {
        oneofKind: "rateLimit";
        /**
         * @generated from protobuf field: silo.v1.GubernatorRateLimit rate_limit = 2
         */
        rateLimit: GubernatorRateLimit;
    } | {
        oneofKind: "floatingConcurrency";
        /**
         * @generated from protobuf field: silo.v1.FloatingConcurrencyLimit floating_concurrency = 3
         */
        floatingConcurrency: FloatingConcurrencyLimit;
    } | {
        oneofKind: undefined;
    };
}
/**
 * @generated from protobuf message silo.v1.EnqueueRequest
 */
export interface EnqueueRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string id = 2
     */
    id: string;
    /**
     * @generated from protobuf field: uint32 priority = 3
     */
    priority: number;
    /**
     * @generated from protobuf field: int64 start_at_ms = 4
     */
    startAtMs: bigint;
    /**
     * @generated from protobuf field: optional silo.v1.RetryPolicy retry_policy = 5
     */
    retryPolicy?: RetryPolicy;
    /**
     * @generated from protobuf field: silo.v1.JsonValueBytes payload = 6
     */
    payload?: JsonValueBytes;
    /**
     * @generated from protobuf field: repeated silo.v1.Limit limits = 7
     */
    limits: Limit[];
    /**
     * @generated from protobuf field: optional string tenant = 8
     */
    tenant?: string;
    /**
     * @generated from protobuf field: map<string, string> metadata = 9
     */
    metadata: {
        [key: string]: string;
    };
}
/**
 * @generated from protobuf message silo.v1.EnqueueResponse
 */
export interface EnqueueResponse {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
}
/**
 * @generated from protobuf message silo.v1.GetJobRequest
 */
export interface GetJobRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string id = 2
     */
    id: string;
    /**
     * @generated from protobuf field: optional string tenant = 3
     */
    tenant?: string;
}
/**
 * @generated from protobuf message silo.v1.GetJobResponse
 */
export interface GetJobResponse {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
    /**
     * @generated from protobuf field: uint32 priority = 2
     */
    priority: number;
    /**
     * @generated from protobuf field: int64 enqueue_time_ms = 3
     */
    enqueueTimeMs: bigint;
    /**
     * @generated from protobuf field: silo.v1.JsonValueBytes payload = 4
     */
    payload?: JsonValueBytes;
    /**
     * @generated from protobuf field: optional silo.v1.RetryPolicy retry_policy = 5
     */
    retryPolicy?: RetryPolicy;
    /**
     * @generated from protobuf field: repeated silo.v1.Limit limits = 6
     */
    limits: Limit[];
    /**
     * @generated from protobuf field: map<string, string> metadata = 7
     */
    metadata: {
        [key: string]: string;
    };
}
/**
 * @generated from protobuf message silo.v1.DeleteJobRequest
 */
export interface DeleteJobRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string id = 2
     */
    id: string;
    /**
     * @generated from protobuf field: optional string tenant = 3
     */
    tenant?: string;
}
/**
 * @generated from protobuf message silo.v1.DeleteJobResponse
 */
export interface DeleteJobResponse {
}
/**
 * @generated from protobuf message silo.v1.CancelJobRequest
 */
export interface CancelJobRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string id = 2
     */
    id: string;
    /**
     * @generated from protobuf field: optional string tenant = 3
     */
    tenant?: string;
}
/**
 * @generated from protobuf message silo.v1.CancelJobResponse
 */
export interface CancelJobResponse {
}
/**
 * Lease tasks for processing from this server.
 * By default, leases from all shards this server owns (fair distribution).
 * If shard is specified, filters to only that shard.
 *
 * @generated from protobuf message silo.v1.LeaseTasksRequest
 */
export interface LeaseTasksRequest {
    /**
     * @generated from protobuf field: optional uint32 shard = 1
     */
    shard?: number;
    /**
     * @generated from protobuf field: string worker_id = 2
     */
    workerId: string;
    /**
     * @generated from protobuf field: uint32 max_tasks = 3
     */
    maxTasks: number;
}
/**
 * @generated from protobuf message silo.v1.Task
 */
export interface Task {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
    /**
     * @generated from protobuf field: string job_id = 2
     */
    jobId: string;
    /**
     * @generated from protobuf field: uint32 attempt_number = 3
     */
    attemptNumber: number;
    /**
     * @generated from protobuf field: int64 lease_ms = 4
     */
    leaseMs: bigint;
    /**
     * @generated from protobuf field: silo.v1.JsonValueBytes payload = 5
     */
    payload?: JsonValueBytes;
    /**
     * @generated from protobuf field: uint32 priority = 6
     */
    priority: number;
    /**
     * @generated from protobuf field: uint32 shard = 7
     */
    shard: number;
}
/**
 * Task for refreshing a floating concurrency limit - workers compute new max concurrency
 *
 * @generated from protobuf message silo.v1.RefreshFloatingLimitTask
 */
export interface RefreshFloatingLimitTask {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
    /**
     * @generated from protobuf field: string queue_key = 2
     */
    queueKey: string;
    /**
     * @generated from protobuf field: uint32 current_max_concurrency = 3
     */
    currentMaxConcurrency: number;
    /**
     * @generated from protobuf field: int64 last_refreshed_at_ms = 4
     */
    lastRefreshedAtMs: bigint;
    /**
     * @generated from protobuf field: map<string, string> metadata = 5
     */
    metadata: {
        [key: string]: string;
    };
    /**
     * @generated from protobuf field: int64 lease_ms = 6
     */
    leaseMs: bigint;
}
/**
 * @generated from protobuf message silo.v1.LeaseTasksResponse
 */
export interface LeaseTasksResponse {
    /**
     * @generated from protobuf field: repeated silo.v1.Task tasks = 1
     */
    tasks: Task[];
    /**
     * @generated from protobuf field: repeated silo.v1.RefreshFloatingLimitTask refresh_tasks = 2
     */
    refreshTasks: RefreshFloatingLimitTask[];
}
/**
 * Report the outcome of a job attempt task from a worker back to the server
 *
 * @generated from protobuf message silo.v1.ReportOutcomeRequest
 */
export interface ReportOutcomeRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string task_id = 2
     */
    taskId: string;
    /**
     * @generated from protobuf field: optional string tenant = 5
     */
    tenant?: string;
    /**
     * @generated from protobuf oneof: outcome
     */
    outcome: {
        oneofKind: "success";
        /**
         * @generated from protobuf field: silo.v1.JsonValueBytes success = 3
         */
        success: JsonValueBytes;
    } | {
        oneofKind: "failure";
        /**
         * @generated from protobuf field: silo.v1.Failure failure = 4
         */
        failure: Failure;
    } | {
        oneofKind: "cancelled";
        /**
         * @generated from protobuf field: silo.v1.Cancelled cancelled = 6
         */
        cancelled: Cancelled;
    } | {
        oneofKind: undefined;
    };
}
/**
 * @generated from protobuf message silo.v1.Failure
 */
export interface Failure {
    /**
     * @generated from protobuf field: string code = 1
     */
    code: string;
    /**
     * @generated from protobuf field: bytes data = 2
     */
    data: Uint8Array;
}
/**
 * @generated from protobuf message silo.v1.Cancelled
 */
export interface Cancelled {
}
/**
 * @generated from protobuf message silo.v1.ReportOutcomeResponse
 */
export interface ReportOutcomeResponse {
}
/**
 * Report the outcome of a floating limit refresh task from a worker back to the server
 *
 * @generated from protobuf message silo.v1.ReportRefreshOutcomeRequest
 */
export interface ReportRefreshOutcomeRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string task_id = 2
     */
    taskId: string;
    /**
     * @generated from protobuf field: optional string tenant = 3
     */
    tenant?: string;
    /**
     * @generated from protobuf oneof: outcome
     */
    outcome: {
        oneofKind: "success";
        /**
         * @generated from protobuf field: silo.v1.RefreshSuccess success = 4
         */
        success: RefreshSuccess;
    } | {
        oneofKind: "failure";
        /**
         * @generated from protobuf field: silo.v1.RefreshFailure failure = 5
         */
        failure: RefreshFailure;
    } | {
        oneofKind: undefined;
    };
}
/**
 * @generated from protobuf message silo.v1.RefreshSuccess
 */
export interface RefreshSuccess {
    /**
     * @generated from protobuf field: uint32 new_max_concurrency = 1
     */
    newMaxConcurrency: number;
}
/**
 * @generated from protobuf message silo.v1.RefreshFailure
 */
export interface RefreshFailure {
    /**
     * @generated from protobuf field: string code = 1
     */
    code: string;
    /**
     * @generated from protobuf field: string message = 2
     */
    message: string;
}
/**
 * @generated from protobuf message silo.v1.ReportRefreshOutcomeResponse
 */
export interface ReportRefreshOutcomeResponse {
}
/**
 * @generated from protobuf message silo.v1.HeartbeatRequest
 */
export interface HeartbeatRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string worker_id = 2
     */
    workerId: string;
    /**
     * @generated from protobuf field: string task_id = 3
     */
    taskId: string;
    /**
     * @generated from protobuf field: optional string tenant = 4
     */
    tenant?: string;
}
/**
 * @generated from protobuf message silo.v1.HeartbeatResponse
 */
export interface HeartbeatResponse {
    /**
     * True if the job has been cancelled. Worker should stop work and report Cancelled outcome.
     *
     * @generated from protobuf field: bool cancelled = 1
     */
    cancelled: boolean;
    /**
     * Timestamp (epoch ms) when cancellation was requested, if cancelled
     *
     * @generated from protobuf field: optional int64 cancelled_at_ms = 2
     */
    cancelledAtMs?: bigint;
}
/**
 * Execute SQL query against shard data
 *
 * @generated from protobuf message silo.v1.QueryRequest
 */
export interface QueryRequest {
    /**
     * @generated from protobuf field: uint32 shard = 1
     */
    shard: number;
    /**
     * @generated from protobuf field: string sql = 2
     */
    sql: string;
    /**
     * @generated from protobuf field: optional string tenant = 3
     */
    tenant?: string;
}
/**
 * Column metadata for the result schema
 *
 * @generated from protobuf message silo.v1.ColumnInfo
 */
export interface ColumnInfo {
    /**
     * @generated from protobuf field: string name = 1
     */
    name: string;
    /**
     * @generated from protobuf field: string data_type = 2
     */
    dataType: string;
}
/**
 * Query result row as JSON object
 *
 * @generated from protobuf message silo.v1.QueryResponse
 */
export interface QueryResponse {
    /**
     * @generated from protobuf field: repeated silo.v1.ColumnInfo columns = 1
     */
    columns: ColumnInfo[];
    /**
     * @generated from protobuf field: repeated silo.v1.JsonValueBytes rows = 2
     */
    rows: JsonValueBytes[];
    /**
     * @generated from protobuf field: int32 row_count = 3
     */
    rowCount: number;
}
/**
 * Cluster topology information for client-side routing
 *
 * @generated from protobuf message silo.v1.GetClusterInfoRequest
 */
export interface GetClusterInfoRequest {
}
/**
 * @generated from protobuf message silo.v1.ShardOwner
 */
export interface ShardOwner {
    /**
     * @generated from protobuf field: uint32 shard_id = 1
     */
    shardId: number;
    /**
     * @generated from protobuf field: string grpc_addr = 2
     */
    grpcAddr: string;
    /**
     * @generated from protobuf field: string node_id = 3
     */
    nodeId: string;
}
/**
 * @generated from protobuf message silo.v1.GetClusterInfoResponse
 */
export interface GetClusterInfoResponse {
    /**
     * @generated from protobuf field: uint32 num_shards = 1
     */
    numShards: number;
    /**
     * @generated from protobuf field: repeated silo.v1.ShardOwner shard_owners = 2
     */
    shardOwners: ShardOwner[];
    /**
     * @generated from protobuf field: string this_node_id = 3
     */
    thisNodeId: string;
    /**
     * @generated from protobuf field: string this_grpc_addr = 4
     */
    thisGrpcAddr: string;
}
/**
 * Admin request to reset all shards (dev mode only)
 * This clears all data from all shards owned by this server.
 * WARNING: This is a destructive operation intended only for testing.
 *
 * @generated from protobuf message silo.v1.ResetShardsRequest
 */
export interface ResetShardsRequest {
}
/**
 * @generated from protobuf message silo.v1.ResetShardsResponse
 */
export interface ResetShardsResponse {
    /**
     * @generated from protobuf field: uint32 shards_reset = 1
     */
    shardsReset: number;
}
/**
 * Gubernator rate limiting algorithm
 *
 * @generated from protobuf enum silo.v1.GubernatorAlgorithm
 */
export declare enum GubernatorAlgorithm {
    /**
     * Token bucket algorithm
     *
     * @generated from protobuf enum value: GUBERNATOR_ALGORITHM_TOKEN_BUCKET = 0;
     */
    TOKEN_BUCKET = 0,
    /**
     * Leaky bucket algorithm
     *
     * @generated from protobuf enum value: GUBERNATOR_ALGORITHM_LEAKY_BUCKET = 1;
     */
    LEAKY_BUCKET = 1
}
/**
 * Gubernator behavior flags (can be combined via bitwise OR)
 *
 * @generated from protobuf enum silo.v1.GubernatorBehavior
 */
export declare enum GubernatorBehavior {
    /**
     * Default: batch requests to peers
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_BATCHING = 0;
     */
    BATCHING = 0,
    /**
     * Disable batching
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_NO_BATCHING = 1;
     */
    NO_BATCHING = 1,
    /**
     * Global rate limit across all peers
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_GLOBAL = 2;
     */
    GLOBAL = 2,
    /**
     * Duration resets on calendar boundaries
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_DURATION_IS_GREGORIAN = 4;
     */
    DURATION_IS_GREGORIAN = 4,
    /**
     * Reset the rate limit on this request
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_RESET_REMAINING = 8;
     */
    RESET_REMAINING = 8,
    /**
     * Drain remaining counter on over limit
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_DRAIN_OVER_LIMIT = 16;
     */
    DRAIN_OVER_LIMIT = 16
}
declare class JsonValueBytes$Type extends MessageType<JsonValueBytes> {
    constructor();
    create(value?: PartialMessage<JsonValueBytes>): JsonValueBytes;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: JsonValueBytes): JsonValueBytes;
    internalBinaryWrite(message: JsonValueBytes, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.JsonValueBytes
 */
export declare const JsonValueBytes: JsonValueBytes$Type;
declare class RetryPolicy$Type extends MessageType<RetryPolicy> {
    constructor();
    create(value?: PartialMessage<RetryPolicy>): RetryPolicy;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RetryPolicy): RetryPolicy;
    internalBinaryWrite(message: RetryPolicy, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RetryPolicy
 */
export declare const RetryPolicy: RetryPolicy$Type;
declare class ConcurrencyLimit$Type extends MessageType<ConcurrencyLimit> {
    constructor();
    create(value?: PartialMessage<ConcurrencyLimit>): ConcurrencyLimit;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ConcurrencyLimit): ConcurrencyLimit;
    internalBinaryWrite(message: ConcurrencyLimit, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ConcurrencyLimit
 */
export declare const ConcurrencyLimit: ConcurrencyLimit$Type;
declare class FloatingConcurrencyLimit$Type extends MessageType<FloatingConcurrencyLimit> {
    constructor();
    create(value?: PartialMessage<FloatingConcurrencyLimit>): FloatingConcurrencyLimit;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: FloatingConcurrencyLimit): FloatingConcurrencyLimit;
    private binaryReadMap4;
    internalBinaryWrite(message: FloatingConcurrencyLimit, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.FloatingConcurrencyLimit
 */
export declare const FloatingConcurrencyLimit: FloatingConcurrencyLimit$Type;
declare class RateLimitRetryPolicy$Type extends MessageType<RateLimitRetryPolicy> {
    constructor();
    create(value?: PartialMessage<RateLimitRetryPolicy>): RateLimitRetryPolicy;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RateLimitRetryPolicy): RateLimitRetryPolicy;
    internalBinaryWrite(message: RateLimitRetryPolicy, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RateLimitRetryPolicy
 */
export declare const RateLimitRetryPolicy: RateLimitRetryPolicy$Type;
declare class GubernatorRateLimit$Type extends MessageType<GubernatorRateLimit> {
    constructor();
    create(value?: PartialMessage<GubernatorRateLimit>): GubernatorRateLimit;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GubernatorRateLimit): GubernatorRateLimit;
    internalBinaryWrite(message: GubernatorRateLimit, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GubernatorRateLimit
 */
export declare const GubernatorRateLimit: GubernatorRateLimit$Type;
declare class Limit$Type extends MessageType<Limit> {
    constructor();
    create(value?: PartialMessage<Limit>): Limit;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: Limit): Limit;
    internalBinaryWrite(message: Limit, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.Limit
 */
export declare const Limit: Limit$Type;
declare class EnqueueRequest$Type extends MessageType<EnqueueRequest> {
    constructor();
    create(value?: PartialMessage<EnqueueRequest>): EnqueueRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: EnqueueRequest): EnqueueRequest;
    private binaryReadMap9;
    internalBinaryWrite(message: EnqueueRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.EnqueueRequest
 */
export declare const EnqueueRequest: EnqueueRequest$Type;
declare class EnqueueResponse$Type extends MessageType<EnqueueResponse> {
    constructor();
    create(value?: PartialMessage<EnqueueResponse>): EnqueueResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: EnqueueResponse): EnqueueResponse;
    internalBinaryWrite(message: EnqueueResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.EnqueueResponse
 */
export declare const EnqueueResponse: EnqueueResponse$Type;
declare class GetJobRequest$Type extends MessageType<GetJobRequest> {
    constructor();
    create(value?: PartialMessage<GetJobRequest>): GetJobRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetJobRequest): GetJobRequest;
    internalBinaryWrite(message: GetJobRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetJobRequest
 */
export declare const GetJobRequest: GetJobRequest$Type;
declare class GetJobResponse$Type extends MessageType<GetJobResponse> {
    constructor();
    create(value?: PartialMessage<GetJobResponse>): GetJobResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetJobResponse): GetJobResponse;
    private binaryReadMap7;
    internalBinaryWrite(message: GetJobResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetJobResponse
 */
export declare const GetJobResponse: GetJobResponse$Type;
declare class DeleteJobRequest$Type extends MessageType<DeleteJobRequest> {
    constructor();
    create(value?: PartialMessage<DeleteJobRequest>): DeleteJobRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: DeleteJobRequest): DeleteJobRequest;
    internalBinaryWrite(message: DeleteJobRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.DeleteJobRequest
 */
export declare const DeleteJobRequest: DeleteJobRequest$Type;
declare class DeleteJobResponse$Type extends MessageType<DeleteJobResponse> {
    constructor();
    create(value?: PartialMessage<DeleteJobResponse>): DeleteJobResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: DeleteJobResponse): DeleteJobResponse;
    internalBinaryWrite(message: DeleteJobResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.DeleteJobResponse
 */
export declare const DeleteJobResponse: DeleteJobResponse$Type;
declare class CancelJobRequest$Type extends MessageType<CancelJobRequest> {
    constructor();
    create(value?: PartialMessage<CancelJobRequest>): CancelJobRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CancelJobRequest): CancelJobRequest;
    internalBinaryWrite(message: CancelJobRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CancelJobRequest
 */
export declare const CancelJobRequest: CancelJobRequest$Type;
declare class CancelJobResponse$Type extends MessageType<CancelJobResponse> {
    constructor();
    create(value?: PartialMessage<CancelJobResponse>): CancelJobResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CancelJobResponse): CancelJobResponse;
    internalBinaryWrite(message: CancelJobResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CancelJobResponse
 */
export declare const CancelJobResponse: CancelJobResponse$Type;
declare class LeaseTasksRequest$Type extends MessageType<LeaseTasksRequest> {
    constructor();
    create(value?: PartialMessage<LeaseTasksRequest>): LeaseTasksRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: LeaseTasksRequest): LeaseTasksRequest;
    internalBinaryWrite(message: LeaseTasksRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.LeaseTasksRequest
 */
export declare const LeaseTasksRequest: LeaseTasksRequest$Type;
declare class Task$Type extends MessageType<Task> {
    constructor();
    create(value?: PartialMessage<Task>): Task;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: Task): Task;
    internalBinaryWrite(message: Task, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.Task
 */
export declare const Task: Task$Type;
declare class RefreshFloatingLimitTask$Type extends MessageType<RefreshFloatingLimitTask> {
    constructor();
    create(value?: PartialMessage<RefreshFloatingLimitTask>): RefreshFloatingLimitTask;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RefreshFloatingLimitTask): RefreshFloatingLimitTask;
    private binaryReadMap5;
    internalBinaryWrite(message: RefreshFloatingLimitTask, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RefreshFloatingLimitTask
 */
export declare const RefreshFloatingLimitTask: RefreshFloatingLimitTask$Type;
declare class LeaseTasksResponse$Type extends MessageType<LeaseTasksResponse> {
    constructor();
    create(value?: PartialMessage<LeaseTasksResponse>): LeaseTasksResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: LeaseTasksResponse): LeaseTasksResponse;
    internalBinaryWrite(message: LeaseTasksResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.LeaseTasksResponse
 */
export declare const LeaseTasksResponse: LeaseTasksResponse$Type;
declare class ReportOutcomeRequest$Type extends MessageType<ReportOutcomeRequest> {
    constructor();
    create(value?: PartialMessage<ReportOutcomeRequest>): ReportOutcomeRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ReportOutcomeRequest): ReportOutcomeRequest;
    internalBinaryWrite(message: ReportOutcomeRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ReportOutcomeRequest
 */
export declare const ReportOutcomeRequest: ReportOutcomeRequest$Type;
declare class Failure$Type extends MessageType<Failure> {
    constructor();
    create(value?: PartialMessage<Failure>): Failure;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: Failure): Failure;
    internalBinaryWrite(message: Failure, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.Failure
 */
export declare const Failure: Failure$Type;
declare class Cancelled$Type extends MessageType<Cancelled> {
    constructor();
    create(value?: PartialMessage<Cancelled>): Cancelled;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: Cancelled): Cancelled;
    internalBinaryWrite(message: Cancelled, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.Cancelled
 */
export declare const Cancelled: Cancelled$Type;
declare class ReportOutcomeResponse$Type extends MessageType<ReportOutcomeResponse> {
    constructor();
    create(value?: PartialMessage<ReportOutcomeResponse>): ReportOutcomeResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ReportOutcomeResponse): ReportOutcomeResponse;
    internalBinaryWrite(message: ReportOutcomeResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ReportOutcomeResponse
 */
export declare const ReportOutcomeResponse: ReportOutcomeResponse$Type;
declare class ReportRefreshOutcomeRequest$Type extends MessageType<ReportRefreshOutcomeRequest> {
    constructor();
    create(value?: PartialMessage<ReportRefreshOutcomeRequest>): ReportRefreshOutcomeRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ReportRefreshOutcomeRequest): ReportRefreshOutcomeRequest;
    internalBinaryWrite(message: ReportRefreshOutcomeRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ReportRefreshOutcomeRequest
 */
export declare const ReportRefreshOutcomeRequest: ReportRefreshOutcomeRequest$Type;
declare class RefreshSuccess$Type extends MessageType<RefreshSuccess> {
    constructor();
    create(value?: PartialMessage<RefreshSuccess>): RefreshSuccess;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RefreshSuccess): RefreshSuccess;
    internalBinaryWrite(message: RefreshSuccess, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RefreshSuccess
 */
export declare const RefreshSuccess: RefreshSuccess$Type;
declare class RefreshFailure$Type extends MessageType<RefreshFailure> {
    constructor();
    create(value?: PartialMessage<RefreshFailure>): RefreshFailure;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RefreshFailure): RefreshFailure;
    internalBinaryWrite(message: RefreshFailure, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RefreshFailure
 */
export declare const RefreshFailure: RefreshFailure$Type;
declare class ReportRefreshOutcomeResponse$Type extends MessageType<ReportRefreshOutcomeResponse> {
    constructor();
    create(value?: PartialMessage<ReportRefreshOutcomeResponse>): ReportRefreshOutcomeResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ReportRefreshOutcomeResponse): ReportRefreshOutcomeResponse;
    internalBinaryWrite(message: ReportRefreshOutcomeResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ReportRefreshOutcomeResponse
 */
export declare const ReportRefreshOutcomeResponse: ReportRefreshOutcomeResponse$Type;
declare class HeartbeatRequest$Type extends MessageType<HeartbeatRequest> {
    constructor();
    create(value?: PartialMessage<HeartbeatRequest>): HeartbeatRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: HeartbeatRequest): HeartbeatRequest;
    internalBinaryWrite(message: HeartbeatRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.HeartbeatRequest
 */
export declare const HeartbeatRequest: HeartbeatRequest$Type;
declare class HeartbeatResponse$Type extends MessageType<HeartbeatResponse> {
    constructor();
    create(value?: PartialMessage<HeartbeatResponse>): HeartbeatResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: HeartbeatResponse): HeartbeatResponse;
    internalBinaryWrite(message: HeartbeatResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.HeartbeatResponse
 */
export declare const HeartbeatResponse: HeartbeatResponse$Type;
declare class QueryRequest$Type extends MessageType<QueryRequest> {
    constructor();
    create(value?: PartialMessage<QueryRequest>): QueryRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: QueryRequest): QueryRequest;
    internalBinaryWrite(message: QueryRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.QueryRequest
 */
export declare const QueryRequest: QueryRequest$Type;
declare class ColumnInfo$Type extends MessageType<ColumnInfo> {
    constructor();
    create(value?: PartialMessage<ColumnInfo>): ColumnInfo;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ColumnInfo): ColumnInfo;
    internalBinaryWrite(message: ColumnInfo, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ColumnInfo
 */
export declare const ColumnInfo: ColumnInfo$Type;
declare class QueryResponse$Type extends MessageType<QueryResponse> {
    constructor();
    create(value?: PartialMessage<QueryResponse>): QueryResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: QueryResponse): QueryResponse;
    internalBinaryWrite(message: QueryResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.QueryResponse
 */
export declare const QueryResponse: QueryResponse$Type;
declare class GetClusterInfoRequest$Type extends MessageType<GetClusterInfoRequest> {
    constructor();
    create(value?: PartialMessage<GetClusterInfoRequest>): GetClusterInfoRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetClusterInfoRequest): GetClusterInfoRequest;
    internalBinaryWrite(message: GetClusterInfoRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetClusterInfoRequest
 */
export declare const GetClusterInfoRequest: GetClusterInfoRequest$Type;
declare class ShardOwner$Type extends MessageType<ShardOwner> {
    constructor();
    create(value?: PartialMessage<ShardOwner>): ShardOwner;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ShardOwner): ShardOwner;
    internalBinaryWrite(message: ShardOwner, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ShardOwner
 */
export declare const ShardOwner: ShardOwner$Type;
declare class GetClusterInfoResponse$Type extends MessageType<GetClusterInfoResponse> {
    constructor();
    create(value?: PartialMessage<GetClusterInfoResponse>): GetClusterInfoResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetClusterInfoResponse): GetClusterInfoResponse;
    internalBinaryWrite(message: GetClusterInfoResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetClusterInfoResponse
 */
export declare const GetClusterInfoResponse: GetClusterInfoResponse$Type;
declare class ResetShardsRequest$Type extends MessageType<ResetShardsRequest> {
    constructor();
    create(value?: PartialMessage<ResetShardsRequest>): ResetShardsRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ResetShardsRequest): ResetShardsRequest;
    internalBinaryWrite(message: ResetShardsRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ResetShardsRequest
 */
export declare const ResetShardsRequest: ResetShardsRequest$Type;
declare class ResetShardsResponse$Type extends MessageType<ResetShardsResponse> {
    constructor();
    create(value?: PartialMessage<ResetShardsResponse>): ResetShardsResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ResetShardsResponse): ResetShardsResponse;
    internalBinaryWrite(message: ResetShardsResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ResetShardsResponse
 */
export declare const ResetShardsResponse: ResetShardsResponse$Type;
/**
 * @generated ServiceType for protobuf service silo.v1.Silo
 */
export declare const Silo: ServiceType;
export {};
//# sourceMappingURL=silo.d.ts.map