import type { ClientOptions } from "@grpc/grpc-js";
import { ChannelCredentials, credentials, Metadata } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { pack, unpack } from "msgpackr";
import { SiloClient } from "./pb/silo.client";
import type {
  Limit,
  SerializedBytes,
  QueryResponse,
  RetryPolicy,
  Task,
  ShardOwner,
} from "./pb/silo";
import {
  GubernatorAlgorithm,
  GubernatorBehavior,
  JobStatus as ProtoJobStatus,
  AttemptStatus as ProtoAttemptStatus,
} from "./pb/silo";
import type { JobAttempt as ProtoJobAttempt } from "./pb/silo";
import { JobHandle } from "./JobHandle";
export type { QueryResponse, RetryPolicy, ShardOwner };
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
    const statusMsg = currentStatus
      ? ` (current status: ${currentStatus})`
      : "";
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
 * FNV-1a hash constants for 32-bit hash.
 * Using 32-bit for simplicity and JS number safety.
 */
const FNV_OFFSET_32 = 0x811c9dc5;
const FNV_PRIME_32 = 0x01000193;

/**
 * Compute FNV-1a 32-bit hash of a string.
 * This is a fast, simple hash function suitable for shard distribution.
 * @param str The string to hash
 * @returns A 32-bit unsigned integer hash
 */
export function fnv1a32(str: string): number {
  let hash = FNV_OFFSET_32;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    // Multiply by prime, keeping 32-bit result using unsigned right shift
    hash = Math.imul(hash, FNV_PRIME_32) >>> 0;
  }
  return hash >>> 0;
}

/**
 * Default function to map a tenant ID to a shard ID.
 * Uses FNV-1a hash for fast, well-distributed results.
 * @param tenantId The tenant identifier
 * @param numShards Total number of shards in the system
 * @returns The shard ID (0 to numShards-1)
 */
export function defaultTenantToShard(
  tenantId: string,
  numShards: number
): number {
  if (numShards <= 0) {
    throw new Error("numShards must be positive");
  }
  const hash = fnv1a32(tenantId);
  return hash % numShards;
}

/**
 * Function type for mapping tenant IDs to shard IDs.
 */
export type TenantToShardFn = (tenantId: string, numShards: number) => number;

/** gRPC metadata key for shard owner address on redirect */
const SHARD_OWNER_ADDR_METADATA_KEY = "x-silo-shard-owner-addr";

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
export type JobLimit =
  | ConcurrencyLimitConfig
  | RateLimitConfig
  | FloatingConcurrencyLimitConfig;

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
      metadata:
        Object.keys(fl.metadata).length > 0 ? { ...fl.metadata } : undefined,
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
   * Optional shard filter. If specified, only leases from this shard.
   * If not specified (the default), the server leases from all its shards.
   */
  shard?: number;
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

export type TaskOutcome<Result, > = SuccessOutcome<Result> | FailureOutcome | CancelledOutcome;

/** Options for reporting task outcome */
export interface ReportOutcomeOptions<Result = unknown> {
  /** The task ID to report outcome for */
  taskId: string;
  /** The shard the task came from (from Task.shard) */
  shard: number;
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
  /** Which shard this task came from (for reporting outcomes) */
  shard: number;
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
  /** The shard the task came from (from RefreshTask.shard) */
  shard: number;
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
    return (
      error.code === "NOT_FOUND" && error.message.includes("shard not found")
    );
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
  private readonly _tenantToShard: TenantToShardFn;

  /** @internal */
  private readonly _maxWrongShardRetries: number;

  /** @internal */
  private readonly _wrongShardRetryDelayMs: number;

  /** @internal Shard ID → server address */
  private _shardToServer: Map<number, string> = new Map();

  /** @internal */
  private _numShards: number = 0;

  /** @internal */
  private _topologyRefreshInterval: ReturnType<typeof setInterval> | null =
    null;
  /** Counter for round-robin server selection */
  private _anyClientCounter: number = 0;

  /** @internal */
  private _initialServers: string[];

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
            })
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
    this._tenantToShard =
      options.shardRouting?.tenantToShard ?? defaultTenantToShard;
    this._maxWrongShardRetries =
      options.shardRouting?.maxWrongShardRetries ?? 5;
    this._wrongShardRetryDelayMs =
      options.shardRouting?.wrongShardRetryDelayMs ?? 100;

    if (options.shardRouting?.numShards) {
      this._numShards = options.shardRouting.numShards;
    }

    // Parse initial servers
    this._initialServers = this._parseServers(options.servers);

    // Create initial connections
    for (const address of this._initialServers) {
      this._getOrCreateConnection(address);
    }

    // Start topology refresh if configured
    const refreshInterval =
      options.shardRouting?.topologyRefreshIntervalMs ?? 30000;
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
      | Array<{ host: string; port: number }>
  ): string[] {
    if (typeof servers === "string") {
      return [servers];
    }
    if (Array.isArray(servers)) {
      return servers.map((s) =>
        typeof s === "string" ? s : `${s.host}:${s.port}`
      );
    }
    return [`${servers.host}:${servers.port}`];
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
   * Compute the shard ID from tenant.
   * @internal
   */
  private _resolveShard(tenant: string | undefined): number {
    const tenantId = tenant ?? DEFAULT_TENANT;
    if (this._numShards <= 0) {
      throw new Error(
        "Cluster topology not discovered yet. Call refreshTopology() first."
      );
    }
    return this._tenantToShard(tenantId, this._numShards);
  }

  /**
   * Get the client for a specific shard.
   * @internal
   */
  private _getClientForShard(shardId: number): SiloClient {
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
    shard: number;
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
    operation: (client: SiloClient, shard: number) => Promise<T>
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
            this._shardToServer.set(shard, redirectAddr);
            const conn = this._getOrCreateConnection(redirectAddr);
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
   * This updates the shard → server mapping.
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

        this._numShards = response.numShards;

        // Update shard → server mapping
        this._shardToServer.clear();
        for (const owner of response.shardOwners) {
          this._shardToServer.set(owner.shardId, owner.grpcAddr);
          // Ensure we have a connection to this server
          this._getOrCreateConnection(owner.grpcAddr);
        }

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
   */
  public async enqueue(options: EnqueueJobOptions): Promise<JobHandle> {
    const id = await this._withWrongShardRetry(
      options.tenant,
      async (client, shard) => {
        const call = client.enqueue(
          {
            shard,
            id: options.id ?? "",
            priority: options.priority ?? 50,
            startAtMs: options.startAtMs ?? BigInt(Date.now()),
            retryPolicy: options.retryPolicy,
            payload: {
              encoding: { oneofKind: "msgpack", msgpack: encodeBytes(options.payload) },
            },
            limits: options.limits?.map(toProtoLimit) ?? [],
            tenant: options.tenant,
            metadata: options.metadata ?? {},
            taskGroup: options.taskGroup,
          },
          this._rpcOptions()
        );

        const response = await call.response;
        return response.id;
      }
    );
    return new JobHandle(this, id, options.tenant);
  }

  /**
   * Get a job by ID.
   * @param id      The job ID.
   * @param tenant  Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @param options Optional settings for the request.
   * @returns The job details.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async getJob(
    id: string,
    tenant?: string,
    options?: GetJobOptions
  ): Promise<Job> {
    try {
      return await this._withWrongShardRetry(tenant, async (client, shard) => {
        const call = client.getJob(
          {
            shard,
            id,
            tenant,
            includeAttempts: options?.includeAttempts ?? false,
          },
          this._rpcOptions()
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
            "payload"
          ),
          retryPolicy: response.retryPolicy,
          limits: response.limits
            .map(fromProtoLimit)
            .filter((l): l is JobLimit => l !== undefined),
          metadata: response.metadata,
          status: protoJobStatusToPublic(response.status),
          statusChangedAtMs: response.statusChangedAtMs,
          attempts:
            response.attempts.length > 0
              ? response.attempts.map(protoAttemptToPublic)
              : undefined,
          nextAttemptStartsAfterMs: response.nextAttemptStartsAfterMs,
          taskGroup: response.taskGroup,
        };
      });
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new JobNotFoundError(id, tenant);
      }
      throw error;
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
          this._rpcOptions()
        );
      });
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new JobNotFoundError(id, tenant);
      }
      throw error;
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
          this._rpcOptions()
        );
      });
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new JobNotFoundError(id, tenant);
      }
      throw error;
    }
  }

  /**
   * Restart a cancelled or failed job, allowing it to be processed again.
   * The job will get a fresh set of retries according to its retry policy.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @throws JobNotFoundError if the job doesn't exist.
   * @throws Error with FAILED_PRECONDITION if the job is not in a restartable state.
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
          this._rpcOptions()
        );
      });
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new JobNotFoundError(id, tenant);
      }
      throw error;
    }
  }

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
  public async expediteJob(id: string, tenant?: string): Promise<void> {
    try {
      await this._withWrongShardRetry(tenant, async (client, shard) => {
        await client.expediteJob(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions()
        );
      });
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new JobNotFoundError(id, tenant);
      }
      throw error;
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
  public async getJobResult<T = unknown>(
    id: string,
    tenant?: string
  ): Promise<JobResult<T>> {
    try {
      return await this._withWrongShardRetry(tenant, async (client, shard) => {
        const call = client.getJobResult(
          {
            shard,
            id,
            tenant,
          },
          this._rpcOptions()
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
            result = decodeBytes<T>(
              response.result.successData.encoding.msgpack,
              "successData"
            );
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
            errorData = decodeBytes(
              response.result.failure.errorData.encoding.msgpack,
              "failure"
            );
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
      // NOT_FOUND means job doesn't exist
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new JobNotFoundError(id, tenant);
      }
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
      throw error;
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
    serverIndex?: number
  ): Promise<LeaseTasksResult> {
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
      this._rpcOptions()
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
        this._rpcOptions()
      );
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new TaskNotFoundError(options.taskId);
      }
      throw error;
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
  public async reportRefreshOutcome(
    options: ReportRefreshOutcomeOptions
  ): Promise<void> {
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
        this._rpcOptions()
      );
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new TaskNotFoundError(options.taskId);
      }
      throw error;
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
    shard: number
  ): Promise<HeartbeatResult> {
    try {
      const client = this._getClientForShard(shard);
      const call = client.heartbeat(
        {
          shard,
          workerId,
          taskId,
        },
        this._rpcOptions()
      );
      const response = await call.response;
      return {
        cancelled: response.cancelled,
        cancelledAtMs: response.cancelledAtMs,
      };
    } catch (error) {
      if (error instanceof RpcError && error.code === "NOT_FOUND") {
        throw new TaskNotFoundError(taskId);
      }
      throw error;
    }
  }

  /**
   * Execute a SQL query against the shard data.
   * @param sql    The SQL query to execute.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @returns The query results.
   */
  public async query(sql: string, tenant?: string): Promise<QueryResponse> {
    return this._withWrongShardRetry(tenant, async (client, shard) => {
      const call = client.query(
        {
          shard,
          sql,
          tenant,
        },
        this._rpcOptions()
      );

      return await call.response;
    });
  }

  /**
   * Compute the shard ID for a given tenant.
   * Useful for debugging or when you need to know which shard a tenant maps to.
   * @param tenant The tenant ID
   * @returns The shard ID
   */
  public getShardForTenant(tenant: string): number {
    return this._resolveShard(tenant);
  }

  /**
   * Get the current cluster topology.
   * @returns Map of shard ID to server address
   */
  public getTopology(): {
    numShards: number;
    shardToServer: Map<number, string>;
  } {
    return {
      numShards: this._numShards,
      shardToServer: new Map(this._shardToServer),
    };
  }

  /**
   * Reset all shards on all servers in the cluster.
   * This is a destructive operation that clears all data and is only available in dev mode.
   * WARNING: This will delete all jobs, tasks, and queues!
   *
   * @throws Error if the server is not in dev mode
   */
  public async resetShards(): Promise<void> {
    // Reset shards on all known servers
    const servers = new Set<string>();
    for (const addr of this._shardToServer.values()) {
      servers.add(addr);
    }

    // If no shards known, try all configured servers
    if (servers.size === 0) {
      for (const conn of this._connections.values()) {
        await conn.client.resetShards({}, this._rpcOptions());
      }
      return;
    }

    // Reset on each unique server
    for (const addr of servers) {
      const conn = this._getOrCreateConnection(addr);
      await conn.client.resetShards({}, this._rpcOptions());
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
