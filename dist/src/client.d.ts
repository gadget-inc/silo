import type { ClientOptions } from "@grpc/grpc-js";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
import type { JsonValueBytes, QueryResponse, RetryPolicy, Task, ShardOwner } from "./pb/silo";
import { GubernatorAlgorithm, GubernatorBehavior } from "./pb/silo";
import { JobHandle } from "./JobHandle";
export type { QueryResponse, RetryPolicy, Task, ShardOwner };
export { GubernatorAlgorithm, GubernatorBehavior };
/**
 * Error thrown when a job is not found.
 */
export declare class JobNotFoundError extends Error {
    code: string;
    /** The job ID that was not found */
    readonly jobId: string;
    /** The tenant ID used for the lookup */
    readonly tenant?: string;
    constructor(jobId: string, tenant?: string);
}
/**
 * Error thrown when trying to get a job's result before it has reached a terminal state.
 */
export declare class JobNotTerminalError extends Error {
    code: string;
    /** The job ID */
    readonly jobId: string;
    /** The tenant ID */
    readonly tenant?: string;
    /** The current status of the job */
    readonly currentStatus?: JobStatus;
    constructor(jobId: string, tenant?: string, currentStatus?: JobStatus);
}
/**
 * Error thrown when a task (lease) is not found.
 */
export declare class TaskNotFoundError extends Error {
    code: string;
    /** The task ID that was not found */
    readonly taskId: string;
    constructor(taskId: string);
}
/**
 * Compute FNV-1a 32-bit hash of a string.
 * This is a fast, simple hash function suitable for shard distribution.
 * @param str The string to hash
 * @returns A 32-bit unsigned integer hash
 */
export declare function fnv1a32(str: string): number;
/**
 * Default function to map a tenant ID to a shard ID.
 * Uses FNV-1a hash for fast, well-distributed results.
 * @param tenantId The tenant identifier
 * @param numShards Total number of shards in the system
 * @returns The shard ID (0 to numShards-1)
 */
export declare function defaultTenantToShard(tenantId: string, numShards: number): number;
/**
 * Function type for mapping tenant IDs to shard IDs.
 */
export type TenantToShardFn = (tenantId: string, numShards: number) => number;
/**
 * Configuration for shard routing in a multi-shard cluster.
 */
export interface ShardRoutingConfig {
    /**
     * Total number of shards in the cluster.
     * This is discovered automatically from the server but can be provided as a hint.
     */
    numShards?: number;
    /**
     * Custom function to map tenant IDs to shard IDs.
     * If not provided, uses FNV-1a hash (see {@link defaultTenantToShard}).
     */
    tenantToShard?: TenantToShardFn;
    /**
     * Maximum number of retries when receiving a "wrong shard" error.
     * This can happen during cluster rebalancing.
     * @default 5
     */
    maxWrongShardRetries?: number;
    /**
     * Initial delay in ms before retrying after a wrong shard error.
     * Uses exponential backoff.
     * @default 100
     */
    wrongShardRetryDelayMs?: number;
    /**
     * How often to refresh the cluster topology (in ms).
     * Set to 0 to disable automatic refresh.
     * @default 30000
     */
    topologyRefreshIntervalMs?: number;
}
/**
 * Options for {@link SiloGRPCClient}.
 */
export interface SiloGRPCClientOptions {
    /**
     * Initial server addresses to connect to.
     * Can be a single address or multiple addresses for a cluster.
     * The client will discover additional servers via GetClusterInfo.
     */
    servers: string | string[] | {
        host: string;
        port: number;
    } | Array<{
        host: string;
        port: number;
    }>;
    /**
     * The token that will be sent as authorization metadata to the silo server.
     * If not provided, no authorization header will be sent.
     */
    token?: string | (() => Promise<string>);
    /**
     * Whether to use TLS for the connection. Defaults to true.
     */
    useTls?: boolean;
    /**
     * Options that will be passed to the underlying grpc client constructor.
     * @see ClientOptions
     */
    grpcClientOptions?: ClientOptions;
    /**
     * Options that will be passed to every remote procedure call.
     * @see RpcOptions
     */
    rpcOptions?: RpcOptions | (() => RpcOptions | undefined);
    /**
     * Configuration for shard routing.
     */
    shardRouting?: ShardRoutingConfig;
}
/** Concurrency limit configuration */
export interface ConcurrencyLimitConfig {
    type: "concurrency";
    /** Grouping key; jobs with the same key share the limit */
    key: string;
    /** Maximum concurrent running jobs for this key */
    maxConcurrency: number;
}
/** Retry policy for rate limit retries */
export interface RateLimitRetryPolicyConfig {
    /** Initial backoff time in ms when rate limited */
    initialBackoffMs: bigint;
    /** Maximum backoff time in ms */
    maxBackoffMs: bigint;
    /** Multiplier for exponential backoff (default 2.0) */
    backoffMultiplier?: number;
    /** Maximum number of retries (0 = infinite until reset_time) */
    maxRetries?: number;
}
/** Rate limit configuration using the Gubernator algorithm */
export interface RateLimitConfig {
    type: "rateLimit";
    /** Name identifying this rate limit (for debugging/metrics) */
    name: string;
    /** Unique key for this specific rate limit instance */
    uniqueKey: string;
    /** Maximum requests allowed in the duration */
    limit: bigint;
    /** Duration window in milliseconds */
    durationMs: bigint;
    /** Number of hits to consume (default 1) */
    hits?: number;
    /** Rate limiting algorithm (default TOKEN_BUCKET) */
    algorithm?: GubernatorAlgorithm;
    /** Behavior flags (bitwise OR of GubernatorBehavior values) */
    behavior?: number;
    /** How to retry when rate limited */
    retryPolicy?: RateLimitRetryPolicyConfig;
}
/**
 * Floating concurrency limit configuration.
 *
 * Unlike regular concurrency limits, floating limits have a dynamic max concurrency
 * that is periodically refreshed by workers. This is useful for adaptive rate limiting
 * based on external factors like API quotas or system load.
 *
 * When the limit becomes stale (based on refreshIntervalMs), the server schedules
 * a refresh task that workers can handle to compute a new max concurrency value.
 */
export interface FloatingConcurrencyLimitConfig {
    type: "floatingConcurrency";
    /** Grouping key; jobs with the same key share the limit */
    key: string;
    /**
     * Initial max concurrency value used until the first refresh completes.
     * Also used as fallback if refreshes fail.
     */
    defaultMaxConcurrency: number;
    /** How often to refresh the max concurrency value (in milliseconds) */
    refreshIntervalMs: bigint;
    /**
     * Arbitrary key/value metadata passed to workers during refresh.
     * Use this to provide context needed to compute the new max concurrency,
     * such as organization IDs, API keys, or resource identifiers.
     */
    metadata?: Record<string, string>;
}
/** A limit that can be a concurrency limit, rate limit, or floating concurrency limit */
export type JobLimit = ConcurrencyLimitConfig | RateLimitConfig | FloatingConcurrencyLimitConfig;
/** Job details returned from getJob */
export interface Job {
    /** The job ID */
    id: string;
    /** Priority 0-99, where 0 is highest priority */
    priority: number;
    /** Timestamp when the job was enqueued (epoch ms) */
    enqueueTimeMs: bigint;
    /** The job payload as raw bytes */
    payload?: JsonValueBytes;
    /** Retry policy if configured */
    retryPolicy?: RetryPolicy;
    /** Limits (concurrency limits and/or rate limits) */
    limits: JobLimit[];
    /** Key/value metadata stored with the job */
    metadata: {
        [key: string]: string;
    };
    /** Current status of the job */
    status: JobStatus;
    /** Timestamp when the status last changed (epoch ms) */
    statusChangedAtMs: bigint;
    /** All attempts for this job. Only populated if includeAttempts was true in the request to fetch the job. */
    attempts?: JobAttempt[];
}
/** Possible job statuses */
export declare enum JobStatus {
    Scheduled = "Scheduled",
    Running = "Running",
    Succeeded = "Succeeded",
    Failed = "Failed",
    Cancelled = "Cancelled"
}
/** Possible attempt statuses */
export declare enum AttemptStatus {
    Running = "Running",
    Succeeded = "Succeeded",
    Failed = "Failed",
    Cancelled = "Cancelled"
}
/** A single execution attempt of a job */
export interface JobAttempt {
    /** The job ID */
    jobId: string;
    /** Which attempt this is (1 = first attempt) */
    attemptNumber: number;
    /** Unique task ID for this attempt */
    taskId: string;
    /** Current status of the attempt */
    status: AttemptStatus;
    /** Timestamp when attempt started (epoch ms). Present if running. */
    startedAtMs?: bigint;
    /** Timestamp when attempt finished (epoch ms). Present if completed. */
    finishedAtMs?: bigint;
    /** Result data if attempt succeeded */
    result?: JsonValueBytes;
    /** Error code if attempt failed */
    errorCode?: string;
    /** Error data if attempt failed */
    errorData?: Uint8Array;
}
/** Options for getJob */
export interface GetJobOptions {
    /** If true, include all attempts in the response. Defaults to false. */
    includeAttempts?: boolean;
}
/** Result of awaiting a job completion */
export interface JobResult<T = unknown> {
    /** Final status of the job */
    status: JobStatus.Succeeded | JobStatus.Failed | JobStatus.Cancelled;
    /** Result data if the job succeeded */
    result?: T;
    /** Error code if the job failed */
    errorCode?: string;
    /** Error data if the job failed */
    errorData?: unknown;
}
/** Options for awaiting a job result */
export interface AwaitJobOptions {
    /** Polling interval in ms. Defaults to 500ms. */
    pollIntervalMs?: number;
    /** Timeout in ms. If not set, waits indefinitely. */
    timeoutMs?: number;
}
/** Options for enqueueing a job */
export interface EnqueueJobOptions {
    /** The JSON-serializable payload for the job */
    payload: unknown;
    /** Optional job ID. If not provided, the server will generate one. */
    id?: string;
    /** Priority 0-99, where 0 is highest priority. Defaults to 50. */
    priority?: number;
    /** Unix timestamp in milliseconds when the job should start. Defaults to now. */
    startAtMs?: bigint;
    /** Optional retry policy */
    retryPolicy?: RetryPolicy;
    /** Optional limits (concurrency limits and/or rate limits) */
    limits?: JobLimit[];
    /** Tenant ID for routing to the correct shard. Uses default tenant if not provided. */
    tenant?: string;
    /** Optional key/value metadata stored with the job */
    metadata?: Record<string, string>;
}
/** Options for leasing tasks */
export interface LeaseTasksOptions {
    /** The worker ID claiming the tasks */
    workerId: string;
    /** Maximum number of tasks to lease */
    maxTasks: number;
    /**
     * Optional shard filter. If specified, only leases from this shard.
     * If not specified (the default), the server leases from all its shards.
     */
    shard?: number;
}
/** Outcome for reporting task success */
export interface SuccessOutcome {
    type: "success";
    /** The JSON-serializable result data */
    result: unknown;
}
/** Outcome for reporting task failure */
export interface FailureOutcome {
    type: "failure";
    /** Error code */
    code: string;
    /** Optional error data as JSON-serializable object or raw bytes (Uint8Array) */
    data?: unknown;
}
/** Outcome for reporting task cancellation */
export interface CancelledOutcome {
    type: "cancelled";
}
export type TaskOutcome = SuccessOutcome | FailureOutcome | CancelledOutcome;
/** Options for reporting task outcome */
export interface ReportOutcomeOptions {
    /** The task ID to report outcome for */
    taskId: string;
    /** The shard the task came from (from Task.shard) */
    shard: number;
    /** The outcome of the task */
    outcome: TaskOutcome;
    /** Tenant ID. Required when server has tenancy enabled. */
    tenant?: string;
}
/**
 * A floating limit refresh task.
 *
 * Workers receive these tasks when a floating concurrency limit needs its
 * max concurrency value refreshed. The worker should compute the new value
 * based on external factors (API quotas, system load, etc.) and report the result.
 */
export interface RefreshTask {
    /** Unique task ID */
    id: string;
    /** The floating limit queue key to refresh */
    queueKey: string;
    /** Current max concurrency value */
    currentMaxConcurrency: number;
    /** When the value was last refreshed (epoch ms) */
    lastRefreshedAtMs: bigint;
    /** Metadata from the floating limit definition */
    metadata: Record<string, string>;
    /** How long to heartbeat in ms */
    leaseMs: bigint;
    /** Which shard this task came from (for reporting outcomes) */
    shard: number;
}
/** Outcome for reporting refresh task success */
export interface RefreshSuccessOutcome {
    type: "success";
    /** The new max concurrency value computed by the worker */
    newMaxConcurrency: number;
}
/** Outcome for reporting refresh task failure */
export interface RefreshFailureOutcome {
    type: "failure";
    /** Error code */
    code: string;
    /** Error message */
    message: string;
}
export type RefreshOutcome = RefreshSuccessOutcome | RefreshFailureOutcome;
/** Options for reporting refresh task outcome */
export interface ReportRefreshOutcomeOptions {
    /** The refresh task ID to report outcome for */
    taskId: string;
    /** The shard the task came from (from RefreshTask.shard) */
    shard: number;
    /** The outcome of the refresh */
    outcome: RefreshOutcome;
    /** Tenant ID. Required when server has tenancy enabled. */
    tenant?: string;
}
/** Result from a heartbeat request */
export interface HeartbeatResult {
    /** True if the job has been cancelled. Worker should stop work and report Cancelled outcome. */
    cancelled: boolean;
    /** Timestamp (epoch ms) when cancellation was requested, if cancelled. */
    cancelledAtMs?: bigint;
}
/** Result from leasing tasks - includes both job tasks and refresh tasks */
export interface LeaseTasksResult {
    /** Regular job execution tasks */
    tasks: Task[];
    /** Floating limit refresh tasks */
    refreshTasks: RefreshTask[];
}
/**
 * Sleep for a given number of milliseconds.
 * @internal
 */
export declare function sleep(ms: number): Promise<void>;
/**
 * A client class for interacting with Silo's GRPC API.
 *
 * The Silo API provides job queue operations including enqueueing jobs,
 * leasing tasks, reporting outcomes, and querying job state.
 *
 * This client automatically discovers cluster topology and routes requests
 * to the correct server based on tenant → shard → server mapping.
 */
export declare class SiloGRPCClient {
    /** @internal */
    private readonly _connections;
    /** @internal */
    private readonly _rpcOptions;
    /** @internal */
    private readonly _channelCredentials;
    /** @internal */
    private readonly _grpcClientOptions;
    /** @internal */
    private readonly _tenantToShard;
    /** @internal */
    private readonly _maxWrongShardRetries;
    /** @internal */
    private readonly _wrongShardRetryDelayMs;
    /** @internal Shard ID → server address */
    private _shardToServer;
    /** @internal */
    private _numShards;
    /** @internal */
    private _topologyRefreshInterval;
    /** Counter for round-robin server selection */
    private _anyClientCounter;
    /** @internal */
    private _initialServers;
    /**
     * Create a new Silo gRPC client.
     *
     * The client will automatically discover the cluster topology by calling
     * GetClusterInfo on the provided servers.
     *
     * @param options Client options including server addresses.
     */
    constructor(options: SiloGRPCClientOptions);
    /**
     * Parse server addresses into normalized strings.
     * @internal
     */
    private _parseServers;
    /**
     * Get or create a connection to a server.
     * @internal
     */
    private _getOrCreateConnection;
    /**
     * Compute the shard ID from tenant.
     * @internal
     */
    private _resolveShard;
    /**
     * Get the client for a specific shard.
     * @internal
     */
    private _getClientForShard;
    /**
     * Get any available client (for operations that work with any server).
     * Uses round-robin to distribute load across all connected servers.
     * @internal
     */
    private _getAnyClient;
    /**
     * Get the client for a tenant.
     * @internal
     */
    private _getClientForTenant;
    /**
     * Execute an operation with retry logic for wrong shard errors.
     * @internal
     */
    private _withWrongShardRetry;
    /**
     * Refresh the cluster topology by calling GetClusterInfo.
     * This updates the shard → server mapping.
     */
    refreshTopology(): Promise<void>;
    /**
     * Close the underlying GRPC connections.
     */
    close(): void;
    /**
     * Enqueue a job for processing.
     * @param options The options for the enqueue request.
     * @returns A JobHandle for the enqueued job.
     */
    enqueue(options: EnqueueJobOptions): Promise<JobHandle>;
    /**
     * Get a job by ID.
     * @param id      The job ID.
     * @param tenant  Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @param options Optional settings for the request.
     * @returns The job details.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    getJob(id: string, tenant?: string, options?: GetJobOptions): Promise<Job>;
    /**
     * Delete a job by ID.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    deleteJob(id: string, tenant?: string): Promise<void>;
    /**
     * Cancel a job by ID.
     * This marks the job for cancellation. Workers will be notified via heartbeat
     * and should stop processing and report a cancelled outcome.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    cancelJob(id: string, tenant?: string): Promise<void>;
    /**
     * Restart a cancelled or failed job, allowing it to be processed again.
     * The job will get a fresh set of retries according to its retry policy.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @throws JobNotFoundError if the job doesn't exist.
     * @throws Error with FAILED_PRECONDITION if the job is not in a restartable state.
     */
    restartJob(id: string, tenant?: string): Promise<void>;
    /**
     * Expedite a future-scheduled job to run immediately.
     * This is useful for dragging forward a job that was scheduled for the future,
     * or for skipping retry backoff delays on a mid-retry job.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @throws JobNotFoundError if the job doesn't exist.
     * @throws Error with FAILED_PRECONDITION if the job is not in an expeditable state
     *         (already running, terminal, cancelled, or task already ready to run).
     */
    expediteJob(id: string, tenant?: string): Promise<void>;
    /**
     * Get the current status of a job.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @returns The job status.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    getJobStatus(id: string, tenant?: string): Promise<JobStatus>;
    /**
     * Get the result of a completed job.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @returns The job result.
     * @throws JobNotFoundError if the job doesn't exist.
     * @throws JobNotTerminalError if the job is not yet in a terminal state.
     * @internal
     */
    getJobResult<T = unknown>(id: string, tenant?: string): Promise<JobResult<T>>;
    /**
     * Create a job handle for an existing job ID.
     * This allows you to interact with a job that was previously enqueued.
     * @param id     The job ID.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @returns A JobHandle for the specified job.
     */
    handle(id: string, tenant?: string): JobHandle;
    /**
     * Lease tasks for processing.
     *
     * By default, leases tasks from all shards the contacted server owns.
     * If `shard` is specified, filters to only that shard.
     *
     * Returns both regular job tasks and floating limit refresh tasks.
     * Each returned task includes a `shard` field indicating which shard it came from,
     * which must be used when reporting outcomes or sending heartbeats.
     *
     * @param options The options for the lease request.
     * @returns Object containing both job tasks and refresh tasks.
     */
    leaseTasks(options: LeaseTasksOptions): Promise<LeaseTasksResult>;
    /**
     * Report the outcome of a task.
     * @param options The options for reporting the outcome.
     * @throws TaskNotFoundError if the task (lease) doesn't exist.
     */
    reportOutcome(options: ReportOutcomeOptions): Promise<void>;
    /**
     * Report the outcome of a floating limit refresh task.
     *
     * Workers that handle refresh tasks should call this to report either:
     * - A new max concurrency value (success)
     * - An error explaining why the refresh failed (failure)
     *
     * On success, the floating limit's max concurrency is updated immediately.
     * On failure, the server schedules a retry with exponential backoff.
     *
     * @param options The options for reporting the refresh outcome.
     * @throws TaskNotFoundError if the refresh task (lease) doesn't exist.
     */
    reportRefreshOutcome(options: ReportRefreshOutcomeOptions): Promise<void>;
    /**
     * Send a heartbeat for a task to extend its lease.
     * @param workerId The worker ID that owns the task.
     * @param taskId   The task ID.
     * @param shard    The shard the task came from (from Task.shard).
     * @param tenant   The tenant ID (required when tenancy is enabled).
     * @returns HeartbeatResult indicating if the job was cancelled.
     * @throws TaskNotFoundError if the task (lease) doesn't exist.
     */
    heartbeat(workerId: string, taskId: string, shard: number, tenant?: string): Promise<HeartbeatResult>;
    /**
     * Execute a SQL query against the shard data.
     * @param sql    The SQL query to execute.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @returns The query results.
     */
    query(sql: string, tenant?: string): Promise<QueryResponse>;
    /**
     * Compute the shard ID for a given tenant.
     * Useful for debugging or when you need to know which shard a tenant maps to.
     * @param tenant The tenant ID
     * @returns The shard ID
     */
    getShardForTenant(tenant: string): number;
    /**
     * Get the current cluster topology.
     * @returns Map of shard ID to server address
     */
    getTopology(): {
        numShards: number;
        shardToServer: Map<number, string>;
    };
    /**
     * Reset all shards on all servers in the cluster.
     * This is a destructive operation that clears all data and is only available in dev mode.
     * WARNING: This will delete all jobs, tasks, and queues!
     *
     * @throws Error if the server is not in dev mode
     */
    resetShards(): Promise<void>;
}
/**
 * Encode a JSON-serializable value as an array of bytes.
 * @param value The value to encode.
 * @returns The encoded value as an array of bytes.
 */
export declare function encodePayload(value: unknown): Uint8Array;
/**
 * Decode an array of bytes as a JSON value.
 * @param bytes The array of bytes to decode.
 * @returns The bytes decoded and parsed as JSON.
 */
export declare function decodePayload<T = unknown>(bytes: Uint8Array | undefined): T;
//# sourceMappingURL=client.d.ts.map