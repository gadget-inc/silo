import type { ClientOptions } from "@grpc/grpc-js";
import { ChannelCredentials, credentials, Metadata } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { pack, unpack } from "msgpackr";
import { SiloClient } from "./pb/silo.client";
import type { Limit, SerializedBytes, RetryPolicy, Task, ShardOwner } from "./pb/silo";
import {
  GubernatorAlgorithm,
  GubernatorBehavior,
  JobStatus as ProtoJobStatus,
  AttemptStatus as ProtoAttemptStatus,
} from "./pb/silo";
import type { JobAttempt as ProtoJobAttempt } from "./pb/silo";
import { JobHandle } from "./JobHandle";
export type { RetryPolicy, ShardOwner };
export { GubernatorAlgorithm, GubernatorBehavior };

/**
 * Error thrown when a job is not found.
 */
export class JobNotFoundError extends Error {
  code = "SILO_JOB_NOT_FOUND";

  /** The job ID that was not found */
  public readonly jobId: string;
  /** The tenant ID used for the lookup */
  public readonly tenant?: string;

  constructor(jobId: string, tenant?: string) {
    const tenantMsg = tenant ? ` in tenant "${tenant}"` : "";
    super(`Job "${jobId}" not found${tenantMsg}`);
    this.name = "JobNotFoundError";
    this.jobId = jobId;
    this.tenant = tenant;
  }
}

/**
 * Error thrown when trying to get a job's result before it has reached a terminal state.
 */
export class JobNotTerminalError extends Error {
  code = "SILO_JOB_NOT_TERMINAL";

  /** The job ID */
  public readonly jobId: string;
  /** The tenant ID */
  public readonly tenant?: string;
  /** The current status of the job */
  public readonly currentStatus?: JobStatus;

  constructor(jobId: string, tenant?: string, currentStatus?: JobStatus) {
    const statusMsg = currentStatus ? ` (current status: ${currentStatus})` : "";
    const tenantMsg = tenant ? ` in tenant "${tenant}"` : "";
    super(`Job "${jobId}"${tenantMsg} is not in a terminal state${statusMsg}`);
    this.name = "JobNotTerminalError";
    this.jobId = jobId;
    this.tenant = tenant;
    this.currentStatus = currentStatus;
  }
}

/**
 * Error thrown when a task (lease) is not found.
 */
export class TaskNotFoundError extends Error {
  code = "SILO_TASK_NOT_FOUND";

  /** The task ID that was not found */
  public readonly taskId: string;

  constructor(taskId: string) {
    super(`Task "${taskId}" not found`);
    this.name = "TaskNotFoundError";
    this.taskId = taskId;
  }
}

/**
 * Base class for errors translated from gRPC status codes.
 */
export class SiloGrpcError extends Error {
  code = "SILO_GRPC_ERROR";

  /** The original gRPC status code. */
  public readonly grpcCode: string;
  /** The original gRPC status message. */
  public readonly grpcMessage: string;

  constructor(message: string, grpcCode: string, grpcMessage: string) {
    super(message);
    this.name = "SiloGrpcError";
    this.grpcCode = grpcCode;
    this.grpcMessage = grpcMessage;
  }
}

/**
 * Error thrown when the requested resource already exists.
 */
export class SiloAlreadyExistsError extends SiloGrpcError {
  code = "SILO_ALREADY_EXISTS";

  constructor(grpcMessage: string) {
    super(grpcMessage, "ALREADY_EXISTS", grpcMessage);
    this.name = "SiloAlreadyExistsError";
  }
}

/**
 * Error thrown when the requested resource is not found.
 */
export class SiloNotFoundError extends SiloGrpcError {
  code = "SILO_NOT_FOUND";

  constructor(grpcMessage: string) {
    super(grpcMessage, "NOT_FOUND", grpcMessage);
    this.name = "SiloNotFoundError";
  }
}

/**
 * Error thrown when enqueueing a job with an ID that already exists.
 */
export class JobAlreadyExistsError extends SiloAlreadyExistsError {
  code = "SILO_JOB_ALREADY_EXISTS";

  /** The conflicting job ID, if known. */
  public readonly jobId?: string;
  /** The tenant ID, if provided. */
  public readonly tenant?: string;

  constructor(jobId?: string, tenant?: string, grpcMessage?: string) {
    const tenantMsg = tenant ? ` in tenant "${tenant}"` : "";
    const message = jobId
      ? `Job "${jobId}" already exists${tenantMsg}`
      : `Job already exists${tenantMsg}`;
    super(grpcMessage ?? message);
    this.name = "JobAlreadyExistsError";
    this.jobId = jobId;
    this.tenant = tenant;
  }
}

/**
 * Error thrown when a request argument is invalid.
 */
export class SiloInvalidArgumentError extends SiloGrpcError {
  code = "SILO_INVALID_ARGUMENT";

  constructor(grpcMessage: string) {
    super(grpcMessage, "INVALID_ARGUMENT", grpcMessage);
    this.name = "SiloInvalidArgumentError";
  }
}

/**
 * Error thrown when a request violates server-side preconditions.
 */
export class SiloFailedPreconditionError extends SiloGrpcError {
  code = "SILO_FAILED_PRECONDITION";

  constructor(grpcMessage: string) {
    super(grpcMessage, "FAILED_PRECONDITION", grpcMessage);
    this.name = "SiloFailedPreconditionError";
  }
}

/**
 * Error thrown when authentication is required or invalid.
 */
export class SiloUnauthenticatedError extends SiloGrpcError {
  code = "SILO_UNAUTHENTICATED";

  constructor(grpcMessage: string) {
    super(grpcMessage, "UNAUTHENTICATED", grpcMessage);
    this.name = "SiloUnauthenticatedError";
  }
}

/**
 * Error thrown when the caller is not authorized to perform the operation.
 */
export class SiloPermissionDeniedError extends SiloGrpcError {
  code = "SILO_PERMISSION_DENIED";

  constructor(grpcMessage: string) {
    super(grpcMessage, "PERMISSION_DENIED", grpcMessage);
    this.name = "SiloPermissionDeniedError";
  }
}

/**
 * Error thrown when the server is overloaded or quota-constrained.
 */
export class SiloResourceExhaustedError extends SiloGrpcError {
  code = "SILO_RESOURCE_EXHAUSTED";

  constructor(grpcMessage: string) {
    super(grpcMessage, "RESOURCE_EXHAUSTED", grpcMessage);
    this.name = "SiloResourceExhaustedError";
  }
}

/**
 * Error thrown when the server is currently unavailable.
 */
export class SiloUnavailableError extends SiloGrpcError {
  code = "SILO_UNAVAILABLE";

  constructor(grpcMessage: string) {
    super(grpcMessage, "UNAVAILABLE", grpcMessage);
    this.name = "SiloUnavailableError";
  }
}

/**
 * Error thrown when an RPC exceeded the configured deadline.
 */
export class SiloDeadlineExceededError extends SiloGrpcError {
  code = "SILO_DEADLINE_EXCEEDED";

  constructor(grpcMessage: string) {
    super(grpcMessage, "DEADLINE_EXCEEDED", grpcMessage);
    this.name = "SiloDeadlineExceededError";
  }
}

/**
 * Represents a shard with its range and server address.
 * Shards own lexicographical ranges of tenant_ids.
 */
export interface ShardInfoWithRange {
  /** Unique shard identifier (UUID string) */
  shardId: string;
  /** Server address owning this shard */
  serverAddr: string;
  /** Inclusive start of the tenant_id range (empty string means no lower bound) */
  rangeStart: string;
  /** Exclusive end of the tenant_id range (empty string means no upper bound) */
  rangeEnd: string;
}

/**
 * Find the shard that owns a given tenant ID using range-based lookup.
 * Shards should be sorted by rangeStart for efficient binary search.
 * @param tenantId The tenant identifier
 * @param shards Array of shards sorted by rangeStart
 * @returns The shard info, or undefined if no shard found
 */
export function shardForTenant(
  tenantId: string,
  shards: ShardInfoWithRange[],
): ShardInfoWithRange | undefined {
  if (shards.length === 0) {
    return undefined;
  }

  // Binary search to find the shard whose range contains the tenant
  // We're looking for the last shard where rangeStart <= tenantId
  let left = 0;
  let right = shards.length - 1;
  let result: ShardInfoWithRange | undefined = undefined;

  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const shard = shards[mid];

    // Check if tenantId >= rangeStart (empty rangeStart means -infinity)
    const afterStart = shard.rangeStart === "" || tenantId >= shard.rangeStart;

    if (afterStart) {
      // This shard's rangeStart is <= tenantId, could be a candidate
      result = shard;
      left = mid + 1; // Look for a shard with a later rangeStart
    } else {
      right = mid - 1; // rangeStart > tenantId, look earlier
    }
  }

  // Verify the tenant is within the range (before rangeEnd)
  if (result) {
    const beforeEnd = result.rangeEnd === "" || tenantId < result.rangeEnd;
    if (!beforeEnd) {
      // The tenant is >= rangeEnd, so it's not in this shard's range
      // This shouldn't happen if shards cover the full keyspace
      return undefined;
    }
  }

  return result;
}

/** Column metadata from a query result */
export interface QueryColumnInfo {
  /** Column name */
  name: string;
  /** Arrow/DataFusion type as string (e.g., "Utf8", "Int64") */
  dataType: string;
}

/** Deserialized query result */
export interface QueryResult<Row = Record<string, unknown>> {
  /** Schema information for the result columns */
  columns: QueryColumnInfo[];
  /** Deserialized rows */
  rows: Row[];
  /** Total number of rows returned */
  rowCount: number;
}

/** gRPC metadata key for shard owner address on redirect */
const SHARD_OWNER_ADDR_METADATA_KEY = "x-silo-shard-owner-addr";

/**
 * Configuration for shard routing in a multi-shard cluster.
 */
export interface ShardRoutingConfig {
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
  servers:
    | string
    | string[]
    | {
        host: string;
        port: number;
      }
    | Array<{ host: string; port: number }>;

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

  /**
   * A map of server addresses to replace during topology discovery.
   * Keys are the addresses returned by the cluster (e.g. "127.0.0.1:7450"),
   * values are the addresses the client should connect to instead (e.g. "127.0.0.1:17450").
   *
   * This is useful for routing client traffic through a proxy (e.g. toxiproxy)
   * without modifying the server's advertised address, which would also affect
   * inter-node communication.
   */
  addressMap?: Record<string, string>;
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
export interface Job<Payload = unknown, Result = unknown> {
  /** The job ID */
  id: string;
  /** Priority 0-99, where 0 is highest priority */
  priority: number;
  /** Timestamp when the job was enqueued (epoch ms) */
  enqueueTimeMs: bigint;
  /** The job payload as MessagePack bytes */
  payload: Payload;
  /** Retry policy if configured */
  retryPolicy?: RetryPolicy;
  /** Limits (concurrency limits and/or rate limits) */
  limits: JobLimit[];
  /** Key/value metadata stored with the job */
  metadata: { [key: string]: string };
  /** Current status of the job */
  status: JobStatus;
  /** Timestamp when the status last changed (epoch ms) */
  statusChangedAtMs: bigint;
  /** All attempts for this job. Only populated if includeAttempts was true in the request to fetch the job. */
  attempts?: JobAttempt<Result>[];
  /**
   * Timestamp (epoch ms) when the next attempt will start.
   * Present for scheduled jobs (initial or after retry), absent for running or terminal jobs.
   */
  nextAttemptStartsAfterMs?: bigint;
  /** Task group the job belongs to */
  taskGroup: string;
}

/** Possible job statuses */
export enum JobStatus {
  Scheduled = "Scheduled",
  Running = "Running",
  Succeeded = "Succeeded",
  Failed = "Failed",
  Cancelled = "Cancelled",
}

/** Possible attempt statuses */
export enum AttemptStatus {
  Running = "Running",
  Succeeded = "Succeeded",
  Failed = "Failed",
  Cancelled = "Cancelled",
}

/** A single execution attempt of a job */
export interface JobAttempt<Result = unknown> {
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
  result?: Result;
  /** Error code if attempt failed */
  errorCode?: string;
  /** Error data if attempt failed */
  errorData?: Record<string, unknown>;
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

/**
 * Convert a TypeScript-native JobLimit to the protobuf Limit format.
 * @internal
 */
function toProtoLimit(limit: JobLimit): Limit {
  if (limit.type === "concurrency") {
    return {
      limit: {
        oneofKind: "concurrency",
        concurrency: {
          key: limit.key,
          maxConcurrency: limit.maxConcurrency,
        },
      },
    };
  } else if (limit.type === "floatingConcurrency") {
    return {
      limit: {
        oneofKind: "floatingConcurrency",
        floatingConcurrency: {
          key: limit.key,
          defaultMaxConcurrency: limit.defaultMaxConcurrency,
          refreshIntervalMs: limit.refreshIntervalMs,
          metadata: limit.metadata ?? {},
        },
      },
    };
  } else {
    return {
      limit: {
        oneofKind: "rateLimit",
        rateLimit: {
          name: limit.name,
          uniqueKey: limit.uniqueKey,
          limit: limit.limit,
          durationMs: limit.durationMs,
          hits: limit.hits ?? 1,
          algorithm: limit.algorithm ?? GubernatorAlgorithm.TOKEN_BUCKET,
          behavior: limit.behavior ?? 0,
          retryPolicy: limit.retryPolicy
            ? {
                initialBackoffMs: limit.retryPolicy.initialBackoffMs,
                maxBackoffMs: limit.retryPolicy.maxBackoffMs,
                backoffMultiplier: limit.retryPolicy.backoffMultiplier ?? 2.0,
                maxRetries: limit.retryPolicy.maxRetries ?? 0,
              }
            : undefined,
        },
      },
    };
  }
}

/**
 * Convert a protobuf Limit to a TypeScript-native JobLimit.
 * @internal
 */
function fromProtoLimit(limit: Limit): JobLimit | undefined {
  if (limit.limit.oneofKind === "concurrency") {
    return {
      type: "concurrency",
      key: limit.limit.concurrency.key,
      maxConcurrency: limit.limit.concurrency.maxConcurrency,
    };
  } else if (limit.limit.oneofKind === "floatingConcurrency") {
    const fl = limit.limit.floatingConcurrency;
    return {
      type: "floatingConcurrency",
      key: fl.key,
      defaultMaxConcurrency: fl.defaultMaxConcurrency,
      refreshIntervalMs: fl.refreshIntervalMs,
      metadata: Object.keys(fl.metadata).length > 0 ? { ...fl.metadata } : undefined,
    };
  } else if (limit.limit.oneofKind === "rateLimit") {
    const rl = limit.limit.rateLimit;
    return {
      type: "rateLimit",
      name: rl.name,
      uniqueKey: rl.uniqueKey,
      limit: rl.limit,
      durationMs: rl.durationMs,
      hits: rl.hits,
      algorithm: rl.algorithm,
      behavior: rl.behavior,
      retryPolicy: rl.retryPolicy
        ? {
            initialBackoffMs: rl.retryPolicy.initialBackoffMs,
            maxBackoffMs: rl.retryPolicy.maxBackoffMs,
            backoffMultiplier: rl.retryPolicy.backoffMultiplier,
            maxRetries: rl.retryPolicy.maxRetries,
          }
        : undefined,
    };
  }
  return undefined;
}

/** Options for enqueueing a job */
export interface EnqueueJobOptions {
  /** The JSON-serializable payload for the job */
  payload: unknown;
  /**
   * Task group for organizing tasks.
   * Workers must specify which task group to poll for tasks.
   * Required.
   */
  taskGroup: string;
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

/** Options for leasing a specific job's task directly */
export interface LeaseTaskOptions {
  /** The job ID to lease */
  id: string;
  /** The worker ID claiming the task */
  workerId: string;
  /** Tenant ID for routing to the correct shard. Uses default tenant if not provided. */
  tenant?: string;
}

/** Options for leasing tasks */
export interface LeaseTasksOptions {
  /** The worker ID claiming the tasks */
  workerId: string;
  /** Maximum number of tasks to lease */
  maxTasks: number;
  /**
   * Task group to poll for tasks.
   * Workers can only receive tasks from the specified task group.
   * Required.
   */
  taskGroup: string;
  /**
   * Optional shard ID (UUID) filter. If specified, only leases from this shard.
   * If not specified (the default), the server leases from all its shards.
   */
  shard?: string;
}

/** Outcome for reporting task success */
export interface SuccessOutcome<Result = unknown> {
  type: "success";
  /** The JSON-serializable result data */
  result: Result;
}

/** Outcome for reporting task failure */
export interface FailureOutcome {
  type: "failure";
  /** Error code */
  code: string;
  /** Optional error data as JSON-serializable object or raw bytes (Uint8Array) */
  data?: Record<string, unknown>;
}

/** Outcome for reporting task cancellation */
export interface CancelledOutcome {
  type: "cancelled";
}

export type TaskOutcome<Result> = SuccessOutcome<Result> | FailureOutcome | CancelledOutcome;

/** Options for reporting task outcome */
export interface ReportOutcomeOptions<Result = unknown> {
  /** The task ID to report outcome for */
  taskId: string;
  /** The shard ID (UUID) the task came from (from Task.shard) */
  shard: string;
  /** The outcome of the task */
  outcome: TaskOutcome<Result>;
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
  /** Which shard ID (UUID) this task came from (for reporting outcomes) */
  shard: string;
  /** Task group this task belongs to */
  taskGroup: string;
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
  /** The shard ID (UUID) the task came from (from RefreshTask.shard) */
  shard: string;
  /** The outcome of the refresh */
  outcome: RefreshOutcome;
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
 * Check if an error is a "shard not found" / wrong shard error.
 * @internal
 */
function isWrongShardError(error: unknown): boolean {
  if (error instanceof RpcError) {
    // gRPC NOT_FOUND status with "shard not found" message
    return error.code === "NOT_FOUND" && error.message.includes("shard not found");
  }
  return false;
}

/**
 * Extract redirect address from error metadata.
 * @internal
 */
function extractRedirectAddress(error: unknown): string | undefined {
  if (error instanceof RpcError) {
    // Check for redirect metadata
    const metadata = error.meta;
    if (metadata && SHARD_OWNER_ADDR_METADATA_KEY in metadata) {
      return metadata[SHARD_OWNER_ADDR_METADATA_KEY] as string;
    }
  }
  return undefined;
}

interface RpcErrorContext {
  operation: string;
  jobId?: string;
  taskId?: string;
  tenant?: string;
}

function mapRpcError(error: unknown, context: RpcErrorContext): Error {
  if (!(error instanceof RpcError)) {
    return error instanceof Error
      ? error
      : new Error(`${context.operation} failed: ${String(error)}`);
  }

  switch (error.code) {
    case "NOT_FOUND":
      if (context.taskId) {
        return new TaskNotFoundError(context.taskId);
      }
      if (context.jobId) {
        return new JobNotFoundError(context.jobId, context.tenant);
      }
      return new SiloNotFoundError(error.message);
    case "ALREADY_EXISTS":
      if (context.jobId) {
        return new JobAlreadyExistsError(context.jobId, context.tenant, error.message);
      }
      return new SiloAlreadyExistsError(error.message);
    case "INVALID_ARGUMENT":
      return new SiloInvalidArgumentError(error.message);
    case "FAILED_PRECONDITION":
      return new SiloFailedPreconditionError(error.message);
    case "UNAUTHENTICATED":
      return new SiloUnauthenticatedError(error.message);
    case "PERMISSION_DENIED":
      return new SiloPermissionDeniedError(error.message);
    case "RESOURCE_EXHAUSTED":
      return new SiloResourceExhaustedError(error.message);
    case "UNAVAILABLE":
      return new SiloUnavailableError(error.message);
    case "DEADLINE_EXCEEDED":
      return new SiloDeadlineExceededError(error.message);
    default:
      return new SiloGrpcError(error.message, error.code, error.message);
  }
}

function throwMappedRpcError(error: unknown, context: RpcErrorContext): never {
  throw mapRpcError(error, context);
}

/**
 * Sleep for a given number of milliseconds.
 * @internal
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Default tenant used when tenancy is not enabled */
const DEFAULT_TENANT = "-";

/**
 * Convert proto JobStatus enum to public JobStatus enum.
 * @internal
 */
function protoJobStatusToPublic(status: ProtoJobStatus): JobStatus {
  switch (status) {
    case ProtoJobStatus.SCHEDULED:
      return JobStatus.Scheduled;
    case ProtoJobStatus.RUNNING:
      return JobStatus.Running;
    case ProtoJobStatus.SUCCEEDED:
      return JobStatus.Succeeded;
    case ProtoJobStatus.FAILED:
      return JobStatus.Failed;
    case ProtoJobStatus.CANCELLED:
      return JobStatus.Cancelled;
    default: {
      const _exhaustive: never = status;
      throw new Error(`Unknown job status: ${String(_exhaustive)}`);
    }
  }
}

/**
 * Convert proto AttemptStatus enum to public AttemptStatus enum.
 * @internal
 */
function protoAttemptStatusToPublic(status: ProtoAttemptStatus): AttemptStatus {
  switch (status) {
    case ProtoAttemptStatus.RUNNING:
      return AttemptStatus.Running;
    case ProtoAttemptStatus.SUCCEEDED:
      return AttemptStatus.Succeeded;
    case ProtoAttemptStatus.FAILED:
      return AttemptStatus.Failed;
    case ProtoAttemptStatus.CANCELLED:
      return AttemptStatus.Cancelled;
    default: {
      const _exhaustive: never = status;
      throw new Error(`Unknown attempt status: ${String(_exhaustive)}`);
    }
  }
}

/**
 * Convert proto JobAttempt to public JobAttempt.
 * @internal
 */
function protoAttemptToPublic(attempt: ProtoJobAttempt): JobAttempt {
  return {
    jobId: attempt.jobId,
    attemptNumber: attempt.attemptNumber,
    taskId: attempt.taskId,
    status: protoAttemptStatusToPublic(attempt.status),
    startedAtMs: attempt.startedAtMs,
    finishedAtMs: attempt.finishedAtMs,
    result:
      attempt.result?.encoding.oneofKind === "msgpack"
        ? decodeBytes(attempt.result.encoding.msgpack, "result")
        : undefined,
    errorCode: attempt.errorCode,
    errorData:
      attempt.errorData?.encoding.oneofKind === "msgpack"
        ? decodeBytes(attempt.errorData.encoding.msgpack, "errorData")
        : undefined,
  };
}

/** Connection to a single server */
interface ServerConnection {
  address: string;
  transport: GrpcTransport;
  client: SiloClient;
}

/**
 * A client class for interacting with Silo's GRPC API.
 *
 * The Silo API provides job queue operations including enqueueing jobs,
 * leasing tasks, reporting outcomes, and querying job state.
 *
 * This client automatically discovers cluster topology and routes requests
 * to the correct server based on tenant → shard → server mapping.
 */
export class SiloGRPCClient {
  /** @internal */
  private readonly _connections: Map<string, ServerConnection> = new Map();

  /** @internal */
  private readonly _rpcOptions: () => RpcOptions | undefined;

  /** @internal */
  private readonly _channelCredentials: ChannelCredentials;

  /** @internal */
  private readonly _grpcClientOptions: ClientOptions;

  /** @internal */
  private readonly _maxWrongShardRetries: number;

  /** @internal */
  private readonly _wrongShardRetryDelayMs: number;

  /** @internal Shard ID (string UUID) → server address */
  private _shardToServer: Map<string, string> = new Map();

  /** @internal Array of shards sorted by rangeStart for efficient lookup */
  private _shards: ShardInfoWithRange[] = [];

  /** @internal */
  private _topologyRefreshInterval: ReturnType<typeof setInterval> | null = null;
  /** Counter for round-robin server selection */
  private _anyClientCounter: number = 0;

  /** @internal */
  private _initialServers: string[];

  /** @internal Address remapping for proxy routing */
  private readonly _addressMap: Record<string, string>;

  /**
   * Create a new Silo gRPC client.
   *
   * The client will automatically discover the cluster topology by calling
   * GetClusterInfo on the provided servers.
   *
   * @param options Client options including server addresses.
   */
  public constructor(options: SiloGRPCClientOptions) {
    const tokenFn = options.token
      ? typeof options.token === "string"
        ? () => Promise.resolve(options.token as string)
        : options.token
      : null;

    const useTls = options.useTls ?? true;

    this._channelCredentials = useTls
      ? tokenFn
        ? credentials.combineChannelCredentials(
            ChannelCredentials.createSsl(),
            credentials.createFromMetadataGenerator((_, callback) => {
              tokenFn()
                .then((token) => {
                  const meta = new Metadata();
                  meta.add("authorization", `Bearer ${token}`);
                  callback(null, meta);
                })
                .catch(callback);
            }),
          )
        : ChannelCredentials.createSsl()
      : ChannelCredentials.createInsecure();

    // Default gRPC service config with retry policy for transient failures
    const defaultServiceConfig = {
      methodConfig: [
        {
          name: [{ service: "silo.v1.Silo" }],
          retryPolicy: {
            maxAttempts: 5,
            initialBackoff: "0.1s",
            maxBackoff: "5s",
            backoffMultiplier: 2,
            retryableStatusCodes: ["UNAVAILABLE", "RESOURCE_EXHAUSTED"],
          },
        },
      ],
    };

    this._grpcClientOptions = {
      "grpc.keepalive_time_ms": 5_000,
      "grpc.keepalive_timeout_ms": 1_000,
      "grpc.keepalive_permit_without_calls": 1,
      "grpc.max_send_message_length": 128 * 1024 * 1024,
      "grpc.max_receive_message_length": 128 * 1024 * 1024,
      "grpc.service_config": JSON.stringify(defaultServiceConfig),
      // Enable retries (required for service config retry policy)
      "grpc.enable_retries": 1,
      ...options.grpcClientOptions,
    };

    this._rpcOptions =
      options.rpcOptions instanceof Function
        ? options.rpcOptions
        : () => options.rpcOptions as RpcOptions | undefined;

    // Shard routing configuration

    this._maxWrongShardRetries = options.shardRouting?.maxWrongShardRetries ?? 5;
    this._wrongShardRetryDelayMs = options.shardRouting?.wrongShardRetryDelayMs ?? 100;

    // Parse initial servers
    this._initialServers = this._parseServers(options.servers);

    // Address remapping (e.g. for routing through toxiproxy)
    this._addressMap = options.addressMap ?? {};

    // Create initial connections
    for (const address of this._initialServers) {
      this._getOrCreateConnection(address);
    }

    // Start topology refresh if configured
    const refreshInterval = options.shardRouting?.topologyRefreshIntervalMs ?? 30000;
    if (refreshInterval > 0) {
      this._topologyRefreshInterval = setInterval(() => {
        this.refreshTopology().catch((err) => {
          console.error("[SiloGRPCClient] Failed to refresh topology:", err);
        });
      }, refreshInterval);
    }
  }

  /**
   * Parse server addresses into normalized strings.
   * @internal
   */
  private _parseServers(
    servers:
      | string
      | string[]
      | { host: string; port: number }
      | Array<{ host: string; port: number }>,
  ): string[] {
    if (typeof servers === "string") {
      return [servers];
    }
    if (Array.isArray(servers)) {
      return servers.map((s) => (typeof s === "string" ? s : `${s.host}:${s.port}`));
    }
    return [`${servers.host}:${servers.port}`];
  }

  /**
   * Remap a server address using the addressMap, or return it unchanged.
   * @internal
   */
  private _mapAddress(address: string): string {
    return this._addressMap[address] ?? address;
  }

  /**
   * Get or create a connection to a server.
   * @internal
   */
  private _getOrCreateConnection(address: string): ServerConnection {
    let conn = this._connections.get(address);
    if (!conn) {
      const transport = new GrpcTransport({
        host: address,
        channelCredentials: this._channelCredentials,
        clientOptions: this._grpcClientOptions,
      });
      const client = new SiloClient(transport);
      conn = { address, transport, client };
      this._connections.set(address, conn);
    }
    return conn;
  }

  /**
   * Resolve the shard ID (string UUID) from tenant using range-based lookup.
   * @internal
   */
  private _resolveShard(tenant: string | undefined): string {
    const tenantId = tenant ?? DEFAULT_TENANT;
    if (this._shards.length === 0) {
      throw new Error("Cluster topology not discovered yet. Call refreshTopology() first.");
    }
    const shardInfo = shardForTenant(tenantId, this._shards);
    if (!shardInfo) {
      throw new Error(`No shard found for tenant "${tenantId}". This indicates a topology error.`);
    }
    return shardInfo.shardId;
  }

  /**
   * Get the client for a specific shard.
   * @internal
   */
  private _getClientForShard(shardId: string): SiloClient {
    const serverAddr = this._shardToServer.get(shardId);
    if (serverAddr) {
      const conn = this._connections.get(serverAddr);
      if (conn) {
        return conn.client;
      }
    }

    // Fallback to any available connection
    return this._getAnyClient();
  }

  /**
   * Get any available client (for operations that work with any server).
   * Uses round-robin to distribute load across all connected servers.
   * @internal
   */
  private _getAnyClient(): SiloClient {
    const connections = Array.from(this._connections.values());
    if (connections.length === 0) {
      throw new Error("No server connections available");
    }
    const index = this._anyClientCounter % connections.length;
    this._anyClientCounter++;
    return connections[index].client;
  }

  /**
   * Get a client at a specific index (modulo number of connections).
   * Used by workers to implement per-worker round-robin polling.
   * @internal
   */
  public _getClientAtIndex(index: number): SiloClient {
    const connections = Array.from(this._connections.values());
    if (connections.length === 0) {
      throw new Error("No server connections available");
    }
    return connections[index % connections.length].client;
  }

  /**
   * Get the client for a tenant.
   * @internal
   */
  private _getClientForTenant(tenant: string | undefined): {
    client: SiloClient;
    shard: string;
  } {
    const shardId = this._resolveShard(tenant);
    return {
      client: this._getClientForShard(shardId),
      shard: shardId,
    };
  }

  /**
   * Execute an operation with retry logic for wrong shard errors.
   * @internal
   */
  private async _withWrongShardRetry<T>(
    tenant: string | undefined,
    operation: (client: SiloClient, shard: string) => Promise<T>,
  ): Promise<T> {
    let lastError: unknown;
    let delay = this._wrongShardRetryDelayMs;
    let { client, shard } = this._getClientForTenant(tenant);

    for (let attempt = 0; attempt <= this._maxWrongShardRetries; attempt++) {
      try {
        return await operation(client, shard);
      } catch (error) {
        if (isWrongShardError(error) && attempt < this._maxWrongShardRetries) {
          lastError = error;

          // Check for redirect address in metadata
          const redirectAddr = extractRedirectAddress(error);
          if (redirectAddr) {
            // Update routing and create connection to new server
            const mappedAddr = this._mapAddress(redirectAddr);
            this._shardToServer.set(shard, mappedAddr);
            const conn = this._getOrCreateConnection(mappedAddr);
            client = conn.client;
          } else {
            // No redirect info, try refreshing topology
            await this.refreshTopology();
            const result = this._getClientForTenant(tenant);
            client = result.client;
            shard = result.shard;
          }

          // Wait with exponential backoff before retrying
          await sleep(delay);
          delay = Math.min(delay * 2, 5000); // Cap at 5 seconds
          continue;
        }
        throw error;
      }
    }
    // Should not reach here, but throw last error if we do
    throw lastError;
  }

  /**
   * Refresh the cluster topology by calling GetClusterInfo.
   * This updates the shard → server mapping and range info.
   */
  public async refreshTopology(): Promise<void> {
    // Try each known server until one responds
    const servers = [
      ...this._connections.keys(),
      ...this._initialServers.filter((s) => !this._connections.has(s)),
    ];

    for (const serverAddr of servers) {
      try {
        const conn = this._getOrCreateConnection(serverAddr);
        const call = conn.client.getClusterInfo({}, this._rpcOptions());
        const response = await call.response;

        // Update shard → server mapping and build shards array
        // Skip shards without a valid address (cluster may not have fully converged)
        this._shardToServer.clear();
        const shards: ShardInfoWithRange[] = [];
        for (const owner of response.shardOwners) {
          // Skip shards that don't have an owner yet (empty grpcAddr)
          // This can happen during cluster startup before all shards are acquired
          if (!owner.grpcAddr) {
            continue;
          }
          const addr = this._mapAddress(owner.grpcAddr);
          this._shardToServer.set(owner.shardId, addr);
          shards.push({
            shardId: owner.shardId,
            serverAddr: addr,
            rangeStart: owner.rangeStart,
            rangeEnd: owner.rangeEnd,
          });
          // Ensure we have a connection to this server
          this._getOrCreateConnection(addr);
        }

        // Sort shards by rangeStart for efficient binary search lookup
        shards.sort((a, b) => a.rangeStart.localeCompare(b.rangeStart));
        this._shards = shards;

        return; // Success
      } catch {
        // Try next server
        continue;
      }
    }

    throw new Error("Failed to refresh cluster topology from any server");
  }

  /**
   * Close the underlying GRPC connections.
   */
  public close(): void {
    if (this._topologyRefreshInterval) {
      clearInterval(this._topologyRefreshInterval);
      this._topologyRefreshInterval = null;
    }

    for (const conn of this._connections.values()) {
      conn.transport.close();
    }
    this._connections.clear();
  }

  /**
   * Enqueue a job for processing.
   * @param options The options for the enqueue request.
   * @returns A JobHandle for the enqueued job.
   * @throws JobAlreadyExistsError if enqueue conflicts with an existing job ID.
   */
  public async enqueue(options: EnqueueJobOptions): Promise<JobHandle> {
    try {
      const id = await this._withWrongShardRetry(options.tenant, async (client, shard) => {
        const call = client.enqueue(
          {
            shard,
            id: options.id ?? "",
            priority: options.priority ?? 50,
            startAtMs: options.startAtMs ?? BigInt(Date.now()),
            retryPolicy: options.retryPolicy,
            payload: {
              encoding: {
                oneofKind: "msgpack",
                msgpack: encodeBytes(options.payload),
              },
            },
            limits: options.limits?.map(toProtoLimit) ?? [],
            tenant: options.tenant,
            metadata: options.metadata ?? {},
            taskGroup: options.taskGroup,
          },
          this._rpcOptions(),
        );

        const response = await call.response;
        return response.id;
      });
      return new JobHandle(this, id, options.tenant);
    } catch (error) {
      throwMappedRpcError(error, {
        operation: "enqueue",
        jobId: options.id,
        tenant: options.tenant,
      });
    }
  }

  /**
   * Get a job by ID.
   * @param id      The job ID.
   * @param tenant  Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @param options Optional settings for the request.
   * @returns The job details.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async getJob(id: string, tenant?: string, options?: GetJobOptions): Promise<Job> {
    try {
      return await this._withWrongShardRetry(tenant, async (client, shard) => {
        const call = client.getJob(
          {
            shard,
            id,
            tenant,
            includeAttempts: options?.includeAttempts ?? false,
          },
          this._rpcOptions(),
        );

        const response = await call.response;

        return {
          id: response.id,
          priority: response.priority,
          enqueueTimeMs: response.enqueueTimeMs,
          payload: decodeBytes(
            response.payload?.encoding.oneofKind === "msgpack"
              ? response.payload.encoding.msgpack
              : undefined,
            "payload",
          ),
          retryPolicy: response.retryPolicy,
          limits: response.limits.map(fromProtoLimit).filter((l): l is JobLimit => l !== undefined),
          metadata: response.metadata,
          status: protoJobStatusToPublic(response.status),
          statusChangedAtMs: response.statusChangedAtMs,
          attempts:
            response.attempts.length > 0 ? response.attempts.map(protoAttemptToPublic) : undefined,
          nextAttemptStartsAfterMs: response.nextAttemptStartsAfterMs,
          taskGroup: response.taskGroup,
        };
      });
    } catch (error) {
      throwMappedRpcError(error, { operation: "getJob", jobId: id, tenant });
    }
  }

  /**
   * Delete a job by ID.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async deleteJob(id: string, tenant?: string): Promise<void> {
    try {
      await this._withWrongShardRetry(tenant, async (client, shard) => {
        await client.deleteJob(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions(),
        );
      });
    } catch (error) {
      throwMappedRpcError(error, { operation: "deleteJob", jobId: id, tenant });
    }
  }

  /**
   * Cancel a job by ID.
   * This marks the job for cancellation. Workers will be notified via heartbeat
   * and should stop processing and report a cancelled outcome.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async cancelJob(id: string, tenant?: string): Promise<void> {
    try {
      await this._withWrongShardRetry(tenant, async (client, shard) => {
        await client.cancelJob(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions(),
        );
      });
    } catch (error) {
      throwMappedRpcError(error, { operation: "cancelJob", jobId: id, tenant });
    }
  }

  /**
   * Restart a cancelled or failed job, allowing it to be processed again.
   * The job will get a fresh set of retries according to its retry policy.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @throws JobNotFoundError if the job doesn't exist.
   * @throws SiloFailedPreconditionError if the job is not in a restartable state.
   */
  public async restartJob(id: string, tenant?: string): Promise<void> {
    try {
      await this._withWrongShardRetry(tenant, async (client, shard) => {
        await client.restartJob(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions(),
        );
      });
    } catch (error) {
      throwMappedRpcError(error, { operation: "restartJob", jobId: id, tenant });
    }
  }

  /**
   * Expedite a future-scheduled job to run immediately.
   * This is useful for dragging forward a job that was scheduled for the future,
   * or for skipping retry backoff delays on a mid-retry job.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @throws JobNotFoundError if the job doesn't exist.
   * @throws SiloFailedPreconditionError if the job is not in an expeditable state
   *         (already running, terminal, cancelled, or task already ready to run).
   */
  public async expediteJob(id: string, tenant?: string): Promise<void> {
    try {
      await this._withWrongShardRetry(tenant, async (client, shard) => {
        await client.expediteJob(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions(),
        );
      });
    } catch (error) {
      throwMappedRpcError(error, { operation: "expediteJob", jobId: id, tenant });
    }
  }

  /**
   * Lease a specific job's task directly, putting it into Running state.
   *
   * This is a test-oriented helper that bypasses concurrency/rate-limit
   * processing. For normal task processing, use `leaseTasks` instead.
   *
   * @param options The options for the lease request.
   * @returns The leased task.
   * @throws JobNotFoundError if the job doesn't exist.
   * @throws SiloFailedPreconditionError if the job is not in a leaseable state
   *         (already running, terminal, or cancelled).
   */
  public async leaseTask(options: LeaseTaskOptions): Promise<Task> {
    try {
      return await this._withWrongShardRetry(options.tenant, async (client, shard) => {
        const call = client.leaseTask(
          {
            shard,
            id: options.id,
            tenant: options.tenant,
            workerId: options.workerId,
          },
          this._rpcOptions(),
        );

        const response = await call.response;
        if (!response.task) {
          throw new Error("leaseTask response missing task");
        }
        return response.task;
      });
    } catch (error) {
      throwMappedRpcError(error, {
        operation: "leaseTask",
        jobId: options.id,
        tenant: options.tenant,
      });
    }
  }

  /**
   * Get the current status of a job.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @returns The job status.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async getJobStatus(id: string, tenant?: string): Promise<JobStatus> {
    const job = await this.getJob(id, tenant);
    return job.status;
  }

  /**
   * Get the result of a completed job.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @returns The job result.
   * @throws JobNotFoundError if the job doesn't exist.
   * @throws JobNotTerminalError if the job is not yet in a terminal state.
   * @internal
   */
  public async getJobResult<T = unknown>(id: string, tenant?: string): Promise<JobResult<T>> {
    try {
      return await this._withWrongShardRetry(tenant, async (client, shard) => {
        const call = client.getJobResult(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions(),
        );

        const response = await call.response;
        const status = protoJobStatusToPublic(response.status);

        if (status === JobStatus.Cancelled) {
          return { status: JobStatus.Cancelled };
        }

        if (status === JobStatus.Succeeded) {
          let result: T | undefined;
          if (
            response.result.oneofKind === "successData" &&
            response.result.successData.encoding.oneofKind === "msgpack" &&
            response.result.successData.encoding.msgpack.length > 0
          ) {
            result = decodeBytes<T>(response.result.successData.encoding.msgpack, "successData");
          }
          return { status: JobStatus.Succeeded, result };
        }

        if (status === JobStatus.Failed) {
          let errorData: unknown;
          if (
            response.result.oneofKind === "failure" &&
            response.result.failure.errorData?.encoding.oneofKind === "msgpack" &&
            response.result.failure.errorData.encoding.msgpack.length > 0
          ) {
            errorData = decodeBytes(response.result.failure.errorData.encoding.msgpack, "failure");
          }
          return {
            status: JobStatus.Failed,
            errorCode:
              response.result.oneofKind === "failure"
                ? response.result.failure.errorCode
                : undefined,
            errorData,
          };
        }

        // Should not reach here if server is behaving correctly
        throw new JobNotTerminalError(id, tenant);
      });
    } catch (error) {
      // FAILED_PRECONDITION means job is not in a terminal state yet
      if (error instanceof RpcError && error.code === "FAILED_PRECONDITION") {
        // Try to get current status for better error message
        try {
          const job = await this.getJob(id, tenant);
          throw new JobNotTerminalError(id, tenant, job.status);
        } catch (e) {
          if (e instanceof JobNotTerminalError) throw e;
          throw new JobNotTerminalError(id, tenant);
        }
      }
      throwMappedRpcError(error, { operation: "getJobResult", jobId: id, tenant });
    }
  }

  /**
   * Create a job handle for an existing job ID.
   * This allows you to interact with a job that was previously enqueued.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @returns A JobHandle for the specified job.
   */
  public handle(id: string, tenant?: string): JobHandle {
    return new JobHandle(this, id, tenant);
  }

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
   * @param serverIndex Optional index for server selection (for per-worker round-robin).
   *                    If provided, uses that index modulo number of servers.
   *                    If not provided, uses the shared round-robin counter.
   * @returns Object containing both job tasks and refresh tasks.
   */
  public async leaseTasks(
    options: LeaseTasksOptions,
    serverIndex?: number,
  ): Promise<LeaseTasksResult> {
    try {
      // If shard is specified, route to that shard's server
      // Otherwise, use round-robin server selection. If serverIndex is provided,
      // use it for per-worker round-robin (avoids lock-step with shared counter).
      const client =
        options.shard !== undefined
          ? this._getClientForShard(options.shard)
          : serverIndex !== undefined
            ? this._getClientAtIndex(serverIndex)
            : this._getAnyClient();

      const call = client.leaseTasks(
        {
          shard: options.shard,
          workerId: options.workerId,
          maxTasks: options.maxTasks,
          taskGroup: options.taskGroup,
        },
        this._rpcOptions(),
      );

      const response = await call.response;

      // Convert proto refresh tasks to our RefreshTask type
      const refreshTasks: RefreshTask[] = response.refreshTasks.map((rt) => ({
        id: rt.id,
        queueKey: rt.queueKey,
        currentMaxConcurrency: rt.currentMaxConcurrency,
        lastRefreshedAtMs: rt.lastRefreshedAtMs,
        metadata: { ...rt.metadata },
        leaseMs: rt.leaseMs,
        shard: rt.shard,
        taskGroup: rt.taskGroup,
      }));

      return {
        tasks: response.tasks,
        refreshTasks,
      };
    } catch (error) {
      throwMappedRpcError(error, { operation: "leaseTasks" });
    }
  }

  /**
   * Report the outcome of a task.
   * @param options The options for reporting the outcome.
   * @throws TaskNotFoundError if the task (lease) doesn't exist.
   */
  public async reportOutcome(options: ReportOutcomeOptions): Promise<void> {
    let outcome:
      | { oneofKind: "success"; success: SerializedBytes }
      | { oneofKind: "failure"; failure: { code: string; data?: SerializedBytes } }
      | { oneofKind: "cancelled"; cancelled: Record<string, never> };

    if (options.outcome.type === "success") {
      outcome = {
        oneofKind: "success" as const,
        success: {
          encoding: { oneofKind: "msgpack", msgpack: encodeBytes(options.outcome.result) },
        },
      };
    } else if (options.outcome.type === "failure") {
      const errorData =
        options.outcome.data instanceof Uint8Array
          ? options.outcome.data
          : encodeBytes(options.outcome.data);
      outcome = {
        oneofKind: "failure" as const,
        failure: {
          code: options.outcome.code,
          data: {
            encoding: { oneofKind: "msgpack", msgpack: errorData },
          },
        },
      };
    } else {
      // cancelled
      outcome = {
        oneofKind: "cancelled" as const,
        cancelled: {},
      };
    }

    try {
      // Route to the correct shard (from Task.shard)
      const client = this._getClientForShard(options.shard);
      await client.reportOutcome(
        {
          shard: options.shard,
          taskId: options.taskId,
          outcome,
        },
        this._rpcOptions(),
      );
    } catch (error) {
      throwMappedRpcError(error, {
        operation: "reportOutcome",
        taskId: options.taskId,
      });
    }
  }

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
  public async reportRefreshOutcome(options: ReportRefreshOutcomeOptions): Promise<void> {
    const outcome =
      options.outcome.type === "success"
        ? {
            oneofKind: "success" as const,
            success: { newMaxConcurrency: options.outcome.newMaxConcurrency },
          }
        : {
            oneofKind: "failure" as const,
            failure: {
              code: options.outcome.code,
              message: options.outcome.message,
            },
          };

    try {
      const client = this._getClientForShard(options.shard);
      await client.reportRefreshOutcome(
        {
          shard: options.shard,
          taskId: options.taskId,
          outcome,
        },
        this._rpcOptions(),
      );
    } catch (error) {
      throwMappedRpcError(error, {
        operation: "reportRefreshOutcome",
        taskId: options.taskId,
      });
    }
  }

  /**
   * Send a heartbeat for a task to extend its lease.
   * @param workerId The worker ID that owns the task.
   * @param taskId   The task ID.
   * @param shard    The shard the task came from (from Task.shard).
   * @returns HeartbeatResult indicating if the job was cancelled.
   * @throws TaskNotFoundError if the task (lease) doesn't exist.
   */
  public async heartbeat(
    workerId: string,
    taskId: string,
    shard: string,
  ): Promise<HeartbeatResult> {
    try {
      const client = this._getClientForShard(shard);
      const call = client.heartbeat(
        {
          shard,
          workerId,
          taskId,
        },
        this._rpcOptions(),
      );
      const response = await call.response;
      return {
        cancelled: response.cancelled,
        cancelledAtMs: response.cancelledAtMs,
      };
    } catch (error) {
      throwMappedRpcError(error, { operation: "heartbeat", taskId });
    }
  }

  /**
   * Execute a SQL query against the shard data.
   * @param sql    The SQL query to execute.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @returns The query results with deserialized rows.
   */
  public async query<Row = Record<string, unknown>>(
    sql: string,
    tenant?: string,
  ): Promise<QueryResult<Row>> {
    try {
      return await this._withWrongShardRetry(tenant, async (client, shard) => {
        const call = client.query(
          {
            shard,
            sql,
            tenant,
          },
          this._rpcOptions(),
        );

        const response = await call.response;

        const columns: QueryColumnInfo[] = response.columns.map((c) => ({
          name: c.name,
          dataType: c.dataType,
        }));

        const rows: Row[] = response.rows.map((row, index) => {
          if (row.encoding.oneofKind === "msgpack") {
            return decodeBytes<Row>(row.encoding.msgpack, `row[${index}]`);
          }
          throw new Error(`Unsupported encoding for row[${index}]: ${row.encoding.oneofKind}`);
        });

        return { columns, rows, rowCount: response.rowCount };
      });
    } catch (error) {
      throwMappedRpcError(error, { operation: "query", tenant });
    }
  }

  /**
   * Compute the shard ID for a given tenant using range-based lookup.
   * Useful for debugging or when you need to know which shard a tenant maps to.
   * @param tenant The tenant ID
   * @returns The shard ID (UUID string) or undefined if no shard found
   */
  public getShardForTenant(tenant: string): string {
    return this._resolveShard(tenant);
  }

  /**
   * Get the current cluster topology.
   * @returns Information about shards including their ranges
   */
  public getTopology(): {
    shardToServer: Map<string, string>;
    shards: ShardInfoWithRange[];
  } {
    return {
      shardToServer: new Map(this._shardToServer),
      shards: [...this._shards],
    };
  }

  /**
   * Reset all shards on all servers in the cluster.
   * This is a destructive operation that clears all data and is only available in dev mode.
   * WARNING: This will delete all jobs, tasks, and queues!
   *
   * @throws SiloPermissionDeniedError if the server is not in dev mode.
   */
  public async resetShards(): Promise<void> {
    try {
      // Reset shards on all known servers
      const servers = new Set<string>();
      for (const addr of this._shardToServer.values()) {
        servers.add(addr);
      }

      // If no shards known, try all configured servers
      if (servers.size === 0) {
        for (const conn of this._connections.values()) {
          const call = conn.client.resetShards({}, this._rpcOptions());
          await call.response;
        }
        return;
      }

      // Reset on each unique server sequentially to avoid SlateDB conflicts
      // when nodes share the same underlying object storage
      for (const addr of servers) {
        const conn = this._getOrCreateConnection(addr);
        const call = conn.client.resetShards({}, this._rpcOptions());
        await call.response;
      }
    } catch (error) {
      throwMappedRpcError(error, { operation: "resetShards" });
    }
  }
}

/**
 * Encode a value as MessagePack bytes.
 * @param value The value to encode.
 * @returns The encoded value as MessagePack bytes.
 */
export function encodeBytes(value: unknown): Uint8Array {
  return pack(value);
}

/**
 * Decode MessagePack bytes as a value.
 * @param bytes The MessagePack bytes to decode.
 * @returns The decoded value.
 */
export function decodeBytes<T = unknown>(bytes: Uint8Array | undefined, field: string): T {
  if (!bytes || bytes.length === 0) {
    throw new Error(`No bytes to decode for field ${field}`);
  }
  return unpack(bytes) as T;
}
