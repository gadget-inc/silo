import { ServiceType } from "@protobuf-ts/runtime-rpc";
import type { BinaryWriteOptions } from "@protobuf-ts/runtime";
import type { IBinaryWriter } from "@protobuf-ts/runtime";
import type { BinaryReadOptions } from "@protobuf-ts/runtime";
import type { IBinaryReader } from "@protobuf-ts/runtime";
import type { PartialMessage } from "@protobuf-ts/runtime";
import { MessageType } from "@protobuf-ts/runtime";
/**
 * Container for arbitrary serialized data with support for multiple encoding formats.
 * Currently only MessagePack is supported, but structured as a union for forward compatibility
 * to add new serialization formats in the future (e.g., JSON, Protobuf).
 * Used for job payloads, results, error data, and query response rows.
 *
 * @generated from protobuf message silo.v1.SerializedBytes
 */
export interface SerializedBytes {
    /**
     * @generated from protobuf oneof: encoding
     */
    encoding: {
        oneofKind: "msgpack";
        /**
         * @generated from protobuf field: bytes msgpack = 1
         */
        msgpack: Uint8Array;
    } | {
        oneofKind: undefined;
    };
}
/**
 * Configuration for automatic job retry on failure.
 * When a job attempt fails, Silo will automatically retry according to this policy.
 *
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
 * Static concurrency limit that restricts how many jobs with the same key can run simultaneously.
 * Jobs sharing the same key will queue up if max_concurrency is reached.
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
 * Dynamic concurrency limit where the max_concurrency value is computed by workers.
 * Useful when concurrency should be based on external factors like API rate limits.
 * Workers periodically receive refresh tasks to update the limit.
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
 * Retry policy for when a job is blocked by a rate limit.
 * Configures how long workers should wait before retrying the rate limit check.
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
 * Rate limit backed by the Gubernator distributed rate limiting service.
 * Allows controlling job throughput based on request rates.
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
 * Union type representing any kind of limit that can be applied to a job.
 * Jobs can have multiple limits; all must be satisfied before execution.
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
 * Request to enqueue a new job for processing.
 *
 * @generated from protobuf message silo.v1.EnqueueRequest
 */
export interface EnqueueRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
     * @generated from protobuf field: silo.v1.SerializedBytes payload = 6
     */
    payload?: SerializedBytes;
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
    /**
     * @generated from protobuf field: string task_group = 10
     */
    taskGroup: string;
}
/**
 * Response after successfully enqueueing a job.
 *
 * @generated from protobuf message silo.v1.EnqueueResponse
 */
export interface EnqueueResponse {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
}
/**
 * Request to retrieve details about a specific job.
 *
 * @generated from protobuf message silo.v1.GetJobRequest
 */
export interface GetJobRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: string id = 2
     */
    id: string;
    /**
     * @generated from protobuf field: optional string tenant = 3
     */
    tenant?: string;
    /**
     * @generated from protobuf field: bool include_attempts = 4
     */
    includeAttempts: boolean;
}
/**
 * A single execution attempt of a job.
 *
 * @generated from protobuf message silo.v1.JobAttempt
 */
export interface JobAttempt {
    /**
     * @generated from protobuf field: string job_id = 1
     */
    jobId: string;
    /**
     * @generated from protobuf field: uint32 attempt_number = 2
     */
    attemptNumber: number;
    /**
     * @generated from protobuf field: string task_id = 3
     */
    taskId: string;
    /**
     * @generated from protobuf field: silo.v1.AttemptStatus status = 4
     */
    status: AttemptStatus;
    /**
     * @generated from protobuf field: optional int64 started_at_ms = 5
     */
    startedAtMs?: bigint;
    /**
     * @generated from protobuf field: optional int64 finished_at_ms = 6
     */
    finishedAtMs?: bigint;
    /**
     * @generated from protobuf field: optional silo.v1.SerializedBytes result = 7
     */
    result?: SerializedBytes;
    /**
     * @generated from protobuf field: optional string error_code = 8
     */
    errorCode?: string;
    /**
     * @generated from protobuf field: optional silo.v1.SerializedBytes error_data = 9
     */
    errorData?: SerializedBytes;
}
/**
 * Full details of a job including its current state.
 *
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
     * @generated from protobuf field: silo.v1.SerializedBytes payload = 4
     */
    payload?: SerializedBytes;
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
    /**
     * @generated from protobuf field: silo.v1.JobStatus status = 8
     */
    status: JobStatus;
    /**
     * @generated from protobuf field: int64 status_changed_at_ms = 9
     */
    statusChangedAtMs: bigint;
    /**
     * @generated from protobuf field: repeated silo.v1.JobAttempt attempts = 10
     */
    attempts: JobAttempt[];
    /**
     * @generated from protobuf field: optional int64 next_attempt_starts_after_ms = 11
     */
    nextAttemptStartsAfterMs?: bigint;
    /**
     * @generated from protobuf field: string task_group = 12
     */
    taskGroup: string;
}
/**
 * Request to get the result of a completed job.
 * Only succeeds if the job has reached a terminal state (succeeded, failed, or cancelled).
 *
 * @generated from protobuf message silo.v1.GetJobResultRequest
 */
export interface GetJobResultRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * Result of a completed job. Check status to determine which result field is populated.
 *
 * @generated from protobuf message silo.v1.GetJobResultResponse
 */
export interface GetJobResultResponse {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
    /**
     * @generated from protobuf field: silo.v1.JobStatus status = 2
     */
    status: JobStatus;
    /**
     * @generated from protobuf field: int64 finished_at_ms = 3
     */
    finishedAtMs: bigint;
    /**
     * The result depends on the terminal status.
     *
     * @generated from protobuf oneof: result
     */
    result: {
        oneofKind: "successData";
        /**
         * @generated from protobuf field: silo.v1.SerializedBytes success_data = 4
         */
        successData: SerializedBytes;
    } | {
        oneofKind: "failure";
        /**
         * @generated from protobuf field: silo.v1.JobFailure failure = 5
         */
        failure: JobFailure;
    } | {
        oneofKind: "cancelled";
        /**
         * @generated from protobuf field: silo.v1.JobCancelled cancelled = 6
         */
        cancelled: JobCancelled;
    } | {
        oneofKind: undefined;
    };
}
/**
 * Error information for a failed job.
 *
 * @generated from protobuf message silo.v1.JobFailure
 */
export interface JobFailure {
    /**
     * @generated from protobuf field: string error_code = 1
     */
    errorCode: string;
    /**
     * @generated from protobuf field: silo.v1.SerializedBytes error_data = 2
     */
    errorData?: SerializedBytes;
}
/**
 * Information about a cancelled job.
 *
 * @generated from protobuf message silo.v1.JobCancelled
 */
export interface JobCancelled {
    /**
     * @generated from protobuf field: int64 cancelled_at_ms = 1
     */
    cancelledAtMs: bigint;
}
/**
 * Request to permanently delete a job and all its data.
 *
 * @generated from protobuf message silo.v1.DeleteJobRequest
 */
export interface DeleteJobRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * Response confirming job deletion.
 *
 * @generated from protobuf message silo.v1.DeleteJobResponse
 */
export interface DeleteJobResponse {
}
/**
 * Request to cancel a job. Running jobs will be notified via heartbeat response.
 *
 * @generated from protobuf message silo.v1.CancelJobRequest
 */
export interface CancelJobRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * Response confirming cancellation was requested.
 *
 * @generated from protobuf message silo.v1.CancelJobResponse
 */
export interface CancelJobResponse {
}
/**
 * Restart a cancelled or failed job, allowing it to be processed again.
 * The job will get a fresh set of retries according to its retry policy.
 *
 * @generated from protobuf message silo.v1.RestartJobRequest
 */
export interface RestartJobRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * @generated from protobuf message silo.v1.RestartJobResponse
 */
export interface RestartJobResponse {
}
/**
 * Expedite a future-scheduled job to run immediately.
 * This is useful for dragging forward a job that was scheduled for the future,
 * or for skipping retry backoff delays on a mid-retry job.
 *
 * @generated from protobuf message silo.v1.ExpediteJobRequest
 */
export interface ExpediteJobRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * @generated from protobuf message silo.v1.ExpediteJobResponse
 */
export interface ExpediteJobResponse {
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
     * @generated from protobuf field: optional string shard = 1
     */
    shard?: string;
    /**
     * @generated from protobuf field: string worker_id = 2
     */
    workerId: string;
    /**
     * @generated from protobuf field: uint32 max_tasks = 3
     */
    maxTasks: number;
    /**
     * @generated from protobuf field: string task_group = 4
     */
    taskGroup: string;
}
/**
 * A task representing a single job attempt leased to a worker.
 *
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
     * @generated from protobuf field: optional string tenant_id = 3
     */
    tenantId?: string;
    /**
     * @generated from protobuf field: uint32 attempt_number = 4
     */
    attemptNumber: number;
    /**
     * @generated from protobuf field: uint32 relative_attempt_number = 5
     */
    relativeAttemptNumber: number;
    /**
     * @generated from protobuf field: bool is_last_attempt = 6
     */
    isLastAttempt: boolean;
    /**
     * @generated from protobuf field: map<string, string> metadata = 7
     */
    metadata: {
        [key: string]: string;
    };
    /**
     * @generated from protobuf field: repeated silo.v1.Limit limits = 8
     */
    limits: Limit[];
    /**
     * @generated from protobuf field: silo.v1.SerializedBytes payload = 9
     */
    payload?: SerializedBytes;
    /**
     * @generated from protobuf field: uint32 priority = 10
     */
    priority: number;
    /**
     * @generated from protobuf field: string shard = 11
     */
    shard: string;
    /**
     * @generated from protobuf field: string task_group = 12
     */
    taskGroup: string;
    /**
     * @generated from protobuf field: int64 lease_ms = 13
     */
    leaseMs: bigint;
}
/**
 * Task for refreshing a floating concurrency limit.
 * Workers compute the new max_concurrency and report back.
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
    /**
     * @generated from protobuf field: string shard = 7
     */
    shard: string;
    /**
     * @generated from protobuf field: string task_group = 8
     */
    taskGroup: string;
    /**
     * @generated from protobuf field: optional string tenant_id = 9
     */
    tenantId?: string;
}
/**
 * Response containing tasks leased to a worker.
 *
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
 * Request to report the outcome of a completed task.
 * Note: tenant is determined from the task lease, not from the request.
 *
 * @generated from protobuf message silo.v1.ReportOutcomeRequest
 */
export interface ReportOutcomeRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: string task_id = 2
     */
    taskId: string;
    /**
     * @generated from protobuf oneof: outcome
     */
    outcome: {
        oneofKind: "success";
        /**
         * @generated from protobuf field: silo.v1.SerializedBytes success = 3
         */
        success: SerializedBytes;
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
 * Error details for a failed task.
 *
 * @generated from protobuf message silo.v1.Failure
 */
export interface Failure {
    /**
     * @generated from protobuf field: string code = 1
     */
    code: string;
    /**
     * @generated from protobuf field: silo.v1.SerializedBytes data = 2
     */
    data?: SerializedBytes;
}
/**
 * Marker indicating the worker acknowledges the job was cancelled.
 *
 * @generated from protobuf message silo.v1.Cancelled
 */
export interface Cancelled {
}
/**
 * Response confirming outcome was recorded.
 *
 * @generated from protobuf message silo.v1.ReportOutcomeResponse
 */
export interface ReportOutcomeResponse {
}
/**
 * Request to report the outcome of a floating limit refresh task.
 * Note: tenant is determined from the task lease, not from the request.
 *
 * @generated from protobuf message silo.v1.ReportRefreshOutcomeRequest
 */
export interface ReportRefreshOutcomeRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: string task_id = 2
     */
    taskId: string;
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
 * Successful floating limit refresh with the new computed value.
 *
 * @generated from protobuf message silo.v1.RefreshSuccess
 */
export interface RefreshSuccess {
    /**
     * @generated from protobuf field: uint32 new_max_concurrency = 1
     */
    newMaxConcurrency: number;
}
/**
 * Error during floating limit refresh.
 *
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
 * Response confirming refresh outcome was recorded.
 *
 * @generated from protobuf message silo.v1.ReportRefreshOutcomeResponse
 */
export interface ReportRefreshOutcomeResponse {
}
/**
 * Request to extend a task lease and check for cancellation.
 * Workers must heartbeat before lease_ms expires to keep the task.
 * Note: tenant is determined from the task lease, not from the request.
 *
 * @generated from protobuf message silo.v1.HeartbeatRequest
 */
export interface HeartbeatRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: string worker_id = 2
     */
    workerId: string;
    /**
     * @generated from protobuf field: string task_id = 3
     */
    taskId: string;
}
/**
 * Response indicating if the lease was extended and if the job was cancelled.
 *
 * @generated from protobuf message silo.v1.HeartbeatResponse
 */
export interface HeartbeatResponse {
    /**
     * @generated from protobuf field: bool cancelled = 1
     */
    cancelled: boolean;
    /**
     * @generated from protobuf field: optional int64 cancelled_at_ms = 2
     */
    cancelledAtMs?: bigint;
}
/**
 * Request to execute an arbitrary SQL query against shard data.
 * Useful for ad-hoc inspection and debugging.
 *
 * @generated from protobuf message silo.v1.QueryRequest
 */
export interface QueryRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * Metadata about a column in query results.
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
 * Query results with rows as serialized objects.
 *
 * @generated from protobuf message silo.v1.QueryResponse
 */
export interface QueryResponse {
    /**
     * @generated from protobuf field: repeated silo.v1.ColumnInfo columns = 1
     */
    columns: ColumnInfo[];
    /**
     * @generated from protobuf field: repeated silo.v1.SerializedBytes rows = 2
     */
    rows: SerializedBytes[];
    /**
     * @generated from protobuf field: int32 row_count = 3
     */
    rowCount: number;
}
/**
 * Request to execute SQL query with Arrow IPC streaming response.
 * More efficient than QueryResponse for large result sets.
 *
 * @generated from protobuf message silo.v1.QueryArrowRequest
 */
export interface QueryArrowRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
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
 * Arrow IPC encoded message. Part of a streaming response.
 *
 * @generated from protobuf message silo.v1.ArrowIpcMessage
 */
export interface ArrowIpcMessage {
    /**
     * @generated from protobuf field: bytes ipc_data = 1
     */
    ipcData: Uint8Array;
}
/**
 * Request to get cluster topology for client-side routing.
 *
 * @generated from protobuf message silo.v1.GetClusterInfoRequest
 */
export interface GetClusterInfoRequest {
}
/**
 * Information about which node owns a specific shard.
 *
 * @generated from protobuf message silo.v1.ShardOwner
 */
export interface ShardOwner {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
    /**
     * @generated from protobuf field: string grpc_addr = 2
     */
    grpcAddr: string;
    /**
     * @generated from protobuf field: string node_id = 3
     */
    nodeId: string;
    /**
     * @generated from protobuf field: string range_start = 4
     */
    rangeStart: string;
    /**
     * @generated from protobuf field: string range_end = 5
     */
    rangeEnd: string;
    /**
     * @generated from protobuf field: optional string placement_ring = 6
     */
    placementRing?: string;
}
/**
 * Information about a cluster member node.
 *
 * @generated from protobuf message silo.v1.ClusterMember
 */
export interface ClusterMember {
    /**
     * @generated from protobuf field: string node_id = 1
     */
    nodeId: string;
    /**
     * @generated from protobuf field: string grpc_addr = 2
     */
    grpcAddr: string;
    /**
     * @generated from protobuf field: repeated string placement_rings = 3
     */
    placementRings: string[];
}
/**
 * Cluster topology information.
 *
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
    /**
     * @generated from protobuf field: repeated silo.v1.ClusterMember members = 5
     */
    members: ClusterMember[];
}
/**
 * Request to reset all shards owned by this server.
 * WARNING: Destructive operation. Only available in dev mode.
 *
 * @generated from protobuf message silo.v1.ResetShardsRequest
 */
export interface ResetShardsRequest {
}
/**
 * Response confirming shards were reset.
 *
 * @generated from protobuf message silo.v1.ResetShardsResponse
 */
export interface ResetShardsResponse {
    /**
     * @generated from protobuf field: uint32 shards_reset = 1
     */
    shardsReset: number;
}
/**
 * Request to capture a CPU profile from this node.
 * Used for production debugging and performance analysis.
 *
 * @generated from protobuf message silo.v1.CpuProfileRequest
 */
export interface CpuProfileRequest {
    /**
     * @generated from protobuf field: uint32 duration_seconds = 1
     */
    durationSeconds: number;
    /**
     * @generated from protobuf field: uint32 frequency = 2
     */
    frequency: number;
}
/**
 * CPU profile data in pprof protobuf format.
 * Can be analyzed with `pprof` or `go tool pprof`.
 *
 * @generated from protobuf message silo.v1.CpuProfileResponse
 */
export interface CpuProfileResponse {
    /**
     * @generated from protobuf field: bytes profile_data = 1
     */
    profileData: Uint8Array;
    /**
     * @generated from protobuf field: uint32 duration_seconds = 2
     */
    durationSeconds: number;
    /**
     * @generated from protobuf field: uint64 samples = 3
     */
    samples: bigint;
}
/**
 * Request to initiate a shard split operation.
 *
 * @generated from protobuf message silo.v1.RequestSplitRequest
 */
export interface RequestSplitRequest {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
    /**
     * @generated from protobuf field: string split_point = 2
     */
    splitPoint: string;
}
/**
 * Response after initiating a shard split.
 *
 * @generated from protobuf message silo.v1.RequestSplitResponse
 */
export interface RequestSplitResponse {
    /**
     * @generated from protobuf field: string left_child_id = 1
     */
    leftChildId: string;
    /**
     * @generated from protobuf field: string right_child_id = 2
     */
    rightChildId: string;
    /**
     * @generated from protobuf field: string phase = 3
     */
    phase: string;
}
/**
 * Request to get the status of a shard split operation.
 *
 * @generated from protobuf message silo.v1.GetSplitStatusRequest
 */
export interface GetSplitStatusRequest {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
}
/**
 * Response with the current split status.
 * Returns empty if no split is in progress for the shard.
 *
 * @generated from protobuf message silo.v1.GetSplitStatusResponse
 */
export interface GetSplitStatusResponse {
    /**
     * @generated from protobuf field: bool in_progress = 1
     */
    inProgress: boolean;
    /**
     * @generated from protobuf field: string phase = 2
     */
    phase: string;
    /**
     * @generated from protobuf field: string left_child_id = 3
     */
    leftChildId: string;
    /**
     * @generated from protobuf field: string right_child_id = 4
     */
    rightChildId: string;
    /**
     * @generated from protobuf field: string split_point = 5
     */
    splitPoint: string;
    /**
     * @generated from protobuf field: string initiator_node_id = 6
     */
    initiatorNodeId: string;
    /**
     * @generated from protobuf field: int64 requested_at_ms = 7
     */
    requestedAtMs: bigint;
}
/**
 * Information about a shard owned by a node, including counters and cleanup status.
 *
 * @generated from protobuf message silo.v1.OwnedShardInfo
 */
export interface OwnedShardInfo {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
    /**
     * @generated from protobuf field: int64 total_jobs = 2
     */
    totalJobs: bigint;
    /**
     * @generated from protobuf field: int64 completed_jobs = 3
     */
    completedJobs: bigint;
    /**
     * @generated from protobuf field: string cleanup_status = 4
     */
    cleanupStatus: string;
    /**
     * @generated from protobuf field: int64 created_at_ms = 5
     */
    createdAtMs: bigint;
    /**
     * @generated from protobuf field: int64 cleanup_completed_at_ms = 6
     */
    cleanupCompletedAtMs: bigint;
}
/**
 * Request to get node information including owned shards with their counters and cleanup status.
 *
 * @generated from protobuf message silo.v1.GetNodeInfoRequest
 */
export interface GetNodeInfoRequest {
}
/**
 * Response with node information and details for all shards owned by this node.
 *
 * @generated from protobuf message silo.v1.GetNodeInfoResponse
 */
export interface GetNodeInfoResponse {
    /**
     * @generated from protobuf field: string node_id = 1
     */
    nodeId: string;
    /**
     * @generated from protobuf field: repeated silo.v1.OwnedShardInfo owned_shards = 2
     */
    ownedShards: OwnedShardInfo[];
    /**
     * @generated from protobuf field: repeated string placement_rings = 3
     */
    placementRings: string[];
}
/**
 * Request to configure a shard's placement ring.
 *
 * @generated from protobuf message silo.v1.ConfigureShardRequest
 */
export interface ConfigureShardRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: optional string placement_ring = 2
     */
    placementRing?: string;
    /**
     * @generated from protobuf field: optional string tenant = 100
     */
    tenant?: string;
}
/**
 * Response after configuring a shard's placement ring.
 *
 * @generated from protobuf message silo.v1.ConfigureShardResponse
 */
export interface ConfigureShardResponse {
    /**
     * @generated from protobuf field: string previous_ring = 1
     */
    previousRing: string;
    /**
     * @generated from protobuf field: string current_ring = 2
     */
    currentRing: string;
}
/**
 * Rate limiting algorithm for Gubernator-based limits.
 *
 * @generated from protobuf enum silo.v1.GubernatorAlgorithm
 */
export declare enum GubernatorAlgorithm {
    /**
     * Token bucket: tokens refill at steady rate, requests consume tokens.
     *
     * @generated from protobuf enum value: GUBERNATOR_ALGORITHM_TOKEN_BUCKET = 0;
     */
    TOKEN_BUCKET = 0,
    /**
     * Leaky bucket: requests processed at fixed rate, excess queued.
     *
     * @generated from protobuf enum value: GUBERNATOR_ALGORITHM_LEAKY_BUCKET = 1;
     */
    LEAKY_BUCKET = 1
}
/**
 * Behavior flags for Gubernator rate limits. Can be combined via bitwise OR.
 *
 * @generated from protobuf enum silo.v1.GubernatorBehavior
 */
export declare enum GubernatorBehavior {
    /**
     * Default: batch rate limit checks to peers for efficiency.
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_BATCHING = 0;
     */
    BATCHING = 0,
    /**
     * Send each rate limit check immediately (lower latency, higher load).
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_NO_BATCHING = 1;
     */
    NO_BATCHING = 1,
    /**
     * Synchronize rate limit globally across all Gubernator peers.
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_GLOBAL = 2;
     */
    GLOBAL = 2,
    /**
     * Reset duration on calendar boundaries (minute, hour, day).
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_DURATION_IS_GREGORIAN = 4;
     */
    DURATION_IS_GREGORIAN = 4,
    /**
     * Force reset the remaining counter on this request.
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_RESET_REMAINING = 8;
     */
    RESET_REMAINING = 8,
    /**
     * Set remaining to zero on first over-limit event.
     *
     * @generated from protobuf enum value: GUBERNATOR_BEHAVIOR_DRAIN_OVER_LIMIT = 16;
     */
    DRAIN_OVER_LIMIT = 16
}
/**
 * Current state of a job in its lifecycle.
 *
 * @generated from protobuf enum silo.v1.JobStatus
 */
export declare enum JobStatus {
    /**
     * Job is waiting to be executed (queued or scheduled for future).
     *
     * @generated from protobuf enum value: JOB_STATUS_SCHEDULED = 0;
     */
    SCHEDULED = 0,
    /**
     * Job is currently being processed by a worker.
     *
     * @generated from protobuf enum value: JOB_STATUS_RUNNING = 1;
     */
    RUNNING = 1,
    /**
     * Job completed successfully.
     *
     * @generated from protobuf enum value: JOB_STATUS_SUCCEEDED = 2;
     */
    SUCCEEDED = 2,
    /**
     * Job failed after exhausting all retry attempts.
     *
     * @generated from protobuf enum value: JOB_STATUS_FAILED = 3;
     */
    FAILED = 3,
    /**
     * Job was cancelled before completion.
     *
     * @generated from protobuf enum value: JOB_STATUS_CANCELLED = 4;
     */
    CANCELLED = 4
}
/**
 * Status of a job attempt in its lifecycle.
 *
 * @generated from protobuf enum silo.v1.AttemptStatus
 */
export declare enum AttemptStatus {
    /**
     * Attempt is currently running.
     *
     * @generated from protobuf enum value: ATTEMPT_STATUS_RUNNING = 0;
     */
    RUNNING = 0,
    /**
     * Attempt completed successfully.
     *
     * @generated from protobuf enum value: ATTEMPT_STATUS_SUCCEEDED = 1;
     */
    SUCCEEDED = 1,
    /**
     * Attempt failed.
     *
     * @generated from protobuf enum value: ATTEMPT_STATUS_FAILED = 2;
     */
    FAILED = 2,
    /**
     * Attempt was cancelled.
     *
     * @generated from protobuf enum value: ATTEMPT_STATUS_CANCELLED = 3;
     */
    CANCELLED = 3
}
declare class SerializedBytes$Type extends MessageType<SerializedBytes> {
    constructor();
    create(value?: PartialMessage<SerializedBytes>): SerializedBytes;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: SerializedBytes): SerializedBytes;
    internalBinaryWrite(message: SerializedBytes, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.SerializedBytes
 */
export declare const SerializedBytes: SerializedBytes$Type;
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
declare class JobAttempt$Type extends MessageType<JobAttempt> {
    constructor();
    create(value?: PartialMessage<JobAttempt>): JobAttempt;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: JobAttempt): JobAttempt;
    internalBinaryWrite(message: JobAttempt, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.JobAttempt
 */
export declare const JobAttempt: JobAttempt$Type;
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
declare class GetJobResultRequest$Type extends MessageType<GetJobResultRequest> {
    constructor();
    create(value?: PartialMessage<GetJobResultRequest>): GetJobResultRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetJobResultRequest): GetJobResultRequest;
    internalBinaryWrite(message: GetJobResultRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetJobResultRequest
 */
export declare const GetJobResultRequest: GetJobResultRequest$Type;
declare class GetJobResultResponse$Type extends MessageType<GetJobResultResponse> {
    constructor();
    create(value?: PartialMessage<GetJobResultResponse>): GetJobResultResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetJobResultResponse): GetJobResultResponse;
    internalBinaryWrite(message: GetJobResultResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetJobResultResponse
 */
export declare const GetJobResultResponse: GetJobResultResponse$Type;
declare class JobFailure$Type extends MessageType<JobFailure> {
    constructor();
    create(value?: PartialMessage<JobFailure>): JobFailure;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: JobFailure): JobFailure;
    internalBinaryWrite(message: JobFailure, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.JobFailure
 */
export declare const JobFailure: JobFailure$Type;
declare class JobCancelled$Type extends MessageType<JobCancelled> {
    constructor();
    create(value?: PartialMessage<JobCancelled>): JobCancelled;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: JobCancelled): JobCancelled;
    internalBinaryWrite(message: JobCancelled, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.JobCancelled
 */
export declare const JobCancelled: JobCancelled$Type;
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
declare class RestartJobRequest$Type extends MessageType<RestartJobRequest> {
    constructor();
    create(value?: PartialMessage<RestartJobRequest>): RestartJobRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RestartJobRequest): RestartJobRequest;
    internalBinaryWrite(message: RestartJobRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RestartJobRequest
 */
export declare const RestartJobRequest: RestartJobRequest$Type;
declare class RestartJobResponse$Type extends MessageType<RestartJobResponse> {
    constructor();
    create(value?: PartialMessage<RestartJobResponse>): RestartJobResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RestartJobResponse): RestartJobResponse;
    internalBinaryWrite(message: RestartJobResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RestartJobResponse
 */
export declare const RestartJobResponse: RestartJobResponse$Type;
declare class ExpediteJobRequest$Type extends MessageType<ExpediteJobRequest> {
    constructor();
    create(value?: PartialMessage<ExpediteJobRequest>): ExpediteJobRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ExpediteJobRequest): ExpediteJobRequest;
    internalBinaryWrite(message: ExpediteJobRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ExpediteJobRequest
 */
export declare const ExpediteJobRequest: ExpediteJobRequest$Type;
declare class ExpediteJobResponse$Type extends MessageType<ExpediteJobResponse> {
    constructor();
    create(value?: PartialMessage<ExpediteJobResponse>): ExpediteJobResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ExpediteJobResponse): ExpediteJobResponse;
    internalBinaryWrite(message: ExpediteJobResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ExpediteJobResponse
 */
export declare const ExpediteJobResponse: ExpediteJobResponse$Type;
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
    private binaryReadMap7;
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
declare class QueryArrowRequest$Type extends MessageType<QueryArrowRequest> {
    constructor();
    create(value?: PartialMessage<QueryArrowRequest>): QueryArrowRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: QueryArrowRequest): QueryArrowRequest;
    internalBinaryWrite(message: QueryArrowRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.QueryArrowRequest
 */
export declare const QueryArrowRequest: QueryArrowRequest$Type;
declare class ArrowIpcMessage$Type extends MessageType<ArrowIpcMessage> {
    constructor();
    create(value?: PartialMessage<ArrowIpcMessage>): ArrowIpcMessage;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ArrowIpcMessage): ArrowIpcMessage;
    internalBinaryWrite(message: ArrowIpcMessage, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ArrowIpcMessage
 */
export declare const ArrowIpcMessage: ArrowIpcMessage$Type;
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
declare class ClusterMember$Type extends MessageType<ClusterMember> {
    constructor();
    create(value?: PartialMessage<ClusterMember>): ClusterMember;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ClusterMember): ClusterMember;
    internalBinaryWrite(message: ClusterMember, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ClusterMember
 */
export declare const ClusterMember: ClusterMember$Type;
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
declare class CpuProfileRequest$Type extends MessageType<CpuProfileRequest> {
    constructor();
    create(value?: PartialMessage<CpuProfileRequest>): CpuProfileRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CpuProfileRequest): CpuProfileRequest;
    internalBinaryWrite(message: CpuProfileRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CpuProfileRequest
 */
export declare const CpuProfileRequest: CpuProfileRequest$Type;
declare class CpuProfileResponse$Type extends MessageType<CpuProfileResponse> {
    constructor();
    create(value?: PartialMessage<CpuProfileResponse>): CpuProfileResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CpuProfileResponse): CpuProfileResponse;
    internalBinaryWrite(message: CpuProfileResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CpuProfileResponse
 */
export declare const CpuProfileResponse: CpuProfileResponse$Type;
declare class RequestSplitRequest$Type extends MessageType<RequestSplitRequest> {
    constructor();
    create(value?: PartialMessage<RequestSplitRequest>): RequestSplitRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RequestSplitRequest): RequestSplitRequest;
    internalBinaryWrite(message: RequestSplitRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RequestSplitRequest
 */
export declare const RequestSplitRequest: RequestSplitRequest$Type;
declare class RequestSplitResponse$Type extends MessageType<RequestSplitResponse> {
    constructor();
    create(value?: PartialMessage<RequestSplitResponse>): RequestSplitResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RequestSplitResponse): RequestSplitResponse;
    internalBinaryWrite(message: RequestSplitResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RequestSplitResponse
 */
export declare const RequestSplitResponse: RequestSplitResponse$Type;
declare class GetSplitStatusRequest$Type extends MessageType<GetSplitStatusRequest> {
    constructor();
    create(value?: PartialMessage<GetSplitStatusRequest>): GetSplitStatusRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetSplitStatusRequest): GetSplitStatusRequest;
    internalBinaryWrite(message: GetSplitStatusRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetSplitStatusRequest
 */
export declare const GetSplitStatusRequest: GetSplitStatusRequest$Type;
declare class GetSplitStatusResponse$Type extends MessageType<GetSplitStatusResponse> {
    constructor();
    create(value?: PartialMessage<GetSplitStatusResponse>): GetSplitStatusResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetSplitStatusResponse): GetSplitStatusResponse;
    internalBinaryWrite(message: GetSplitStatusResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetSplitStatusResponse
 */
export declare const GetSplitStatusResponse: GetSplitStatusResponse$Type;
declare class OwnedShardInfo$Type extends MessageType<OwnedShardInfo> {
    constructor();
    create(value?: PartialMessage<OwnedShardInfo>): OwnedShardInfo;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: OwnedShardInfo): OwnedShardInfo;
    internalBinaryWrite(message: OwnedShardInfo, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.OwnedShardInfo
 */
export declare const OwnedShardInfo: OwnedShardInfo$Type;
declare class GetNodeInfoRequest$Type extends MessageType<GetNodeInfoRequest> {
    constructor();
    create(value?: PartialMessage<GetNodeInfoRequest>): GetNodeInfoRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetNodeInfoRequest): GetNodeInfoRequest;
    internalBinaryWrite(message: GetNodeInfoRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetNodeInfoRequest
 */
export declare const GetNodeInfoRequest: GetNodeInfoRequest$Type;
declare class GetNodeInfoResponse$Type extends MessageType<GetNodeInfoResponse> {
    constructor();
    create(value?: PartialMessage<GetNodeInfoResponse>): GetNodeInfoResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetNodeInfoResponse): GetNodeInfoResponse;
    internalBinaryWrite(message: GetNodeInfoResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetNodeInfoResponse
 */
export declare const GetNodeInfoResponse: GetNodeInfoResponse$Type;
declare class ConfigureShardRequest$Type extends MessageType<ConfigureShardRequest> {
    constructor();
    create(value?: PartialMessage<ConfigureShardRequest>): ConfigureShardRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ConfigureShardRequest): ConfigureShardRequest;
    internalBinaryWrite(message: ConfigureShardRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ConfigureShardRequest
 */
export declare const ConfigureShardRequest: ConfigureShardRequest$Type;
declare class ConfigureShardResponse$Type extends MessageType<ConfigureShardResponse> {
    constructor();
    create(value?: PartialMessage<ConfigureShardResponse>): ConfigureShardResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ConfigureShardResponse): ConfigureShardResponse;
    internalBinaryWrite(message: ConfigureShardResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ConfigureShardResponse
 */
export declare const ConfigureShardResponse: ConfigureShardResponse$Type;
/**
 * @generated ServiceType for protobuf service silo.v1.Silo
 */
export declare const Silo: ServiceType;
export {};
//# sourceMappingURL=silo.d.ts.map