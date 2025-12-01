import type { ClientOptions } from "@grpc/grpc-js";
import { ChannelCredentials, credentials, Metadata } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
import { TextDecoder, TextEncoder } from "util";
import { SiloClient } from "./pb/silo.client";
import type {
  JsonValueBytes,
  Limit,
  QueryResponse,
  RetryPolicy,
  Task,
} from "./pb/silo";
import { GubernatorAlgorithm, GubernatorBehavior } from "./pb/silo";

export type { QueryResponse, RetryPolicy, Task };
export { GubernatorAlgorithm, GubernatorBehavior };

/**
 * Options for {@link SiloGrpcClient}.
 */
export interface SiloGrpcClientOptions {
  /**
   * The address of the silo server.
   */
  server:
    | string
    | {
        /**
         * The host of the silo server.
         */
        host: string;

        /**
         * The port of the silo server.
         */
        port: number;
      };

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

/** A limit that can be either a concurrency limit or a rate limit */
export type JobLimit = ConcurrencyLimitConfig | RateLimitConfig;

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
  metadata: { [key: string]: string };
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
  /** The shard to enqueue the job on */
  shard: string;
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
  /** Optional tenant ID when tenancy is enabled */
  tenant?: string;
  /** Optional key/value metadata stored with the job */
  metadata?: Record<string, string>;
}

/** Options for leasing tasks */
export interface LeaseTasksOptions {
  /** The shard to lease tasks from */
  shard: string;
  /** The worker ID claiming the tasks */
  workerId: string;
  /** Maximum number of tasks to lease */
  maxTasks: number;
  /** Optional tenant ID when tenancy is enabled */
  tenant?: string;
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

export type TaskOutcome = SuccessOutcome | FailureOutcome;

/** Options for reporting task outcome */
export interface ReportOutcomeOptions {
  /** The shard the task is on */
  shard: string;
  /** The task ID to report outcome for */
  taskId: string;
  /** The outcome of the task */
  outcome: TaskOutcome;
  /** Optional tenant ID when tenancy is enabled */
  tenant?: string;
}

/**
 * A client class for interacting with Silo's GRPC API.
 *
 * The Silo API provides job queue operations including enqueueing jobs,
 * leasing tasks, reporting outcomes, and querying job state.
 */
export class SiloGrpcClient {
  /** @internal */
  private readonly _client: SiloClient;

  /** @internal */
  private readonly _transport: GrpcTransport;

  /** @internal */
  private readonly _rpcOptions: () => RpcOptions | undefined;

  /**
   * The library used to interact with GRPC creates connections lazily, this constructor will not
   * raise an error even if there is no service running at {@link SiloGrpcClientOptions.server server}.
   * @param options Grpc client options.
   */
  public constructor(options: SiloGrpcClientOptions) {
    const tokenFn = options.token
      ? typeof options.token === "string"
        ? () => Promise.resolve(options.token as string)
        : options.token
      : null;

    const useTls = options.useTls ?? true;

    const channelCredentials = useTls
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

    this._transport = new GrpcTransport({
      host:
        typeof options.server === "string"
          ? options.server
          : `${options.server.host}:${options.server.port}`,
      channelCredentials,
      clientOptions: {
        "grpc.keepalive_time_ms": 5_000,
        "grpc.keepalive_timeout_ms": 1_000,
        "grpc.keepalive_permit_without_calls": 1,
        ...options.grpcClientOptions,
      },
    });

    this._client = new SiloClient(this._transport);

    this._rpcOptions =
      options.rpcOptions instanceof Function
        ? options.rpcOptions
        : () => options.rpcOptions as RpcOptions | undefined;
  }

  /**
   * Close the underlying GRPC client.
   */
  public close(): void {
    this._transport.close();
  }

  /**
   * Enqueue a job for processing.
   * @param options The options for the enqueue request.
   * @returns The ID of the enqueued job.
   */
  public async enqueue(options: EnqueueJobOptions): Promise<string> {
    const call = this._client.enqueue(
      {
        shard: options.shard,
        id: options.id ?? "",
        priority: options.priority ?? 50,
        startAtMs: options.startAtMs ?? BigInt(Date.now()),
        retryPolicy: options.retryPolicy,
        payload: { data: encodePayload(options.payload) },
        limits: options.limits?.map(toProtoLimit) ?? [],
        tenant: options.tenant,
        metadata: options.metadata ?? {},
      },
      this._rpcOptions()
    );

    const response = await call.response;
    return response.id;
  }

  /**
   * Get a job by ID.
   * @param shard  The shard the job is on.
   * @param id     The job ID.
   * @param tenant Optional tenant ID when tenancy is enabled.
   * @returns The job details, or undefined if not found.
   */
  public async getJob(
    shard: string,
    id: string,
    tenant?: string
  ): Promise<Job | undefined> {
    const call = this._client.getJob(
      {
        shard,
        id,
        tenant,
      },
      this._rpcOptions()
    );

    const response = await call.response;
    if (!response.id) {
      return undefined;
    }

    return {
      id: response.id,
      priority: response.priority,
      enqueueTimeMs: response.enqueueTimeMs,
      payload: response.payload,
      retryPolicy: response.retryPolicy,
      limits: response.limits
        .map(fromProtoLimit)
        .filter((l): l is JobLimit => l !== undefined),
      metadata: response.metadata,
    };
  }

  /**
   * Delete a job by ID.
   * @param shard  The shard the job is on.
   * @param id     The job ID.
   * @param tenant Optional tenant ID when tenancy is enabled.
   */
  public async deleteJob(
    shard: string,
    id: string,
    tenant?: string
  ): Promise<void> {
    await this._client.deleteJob(
      {
        shard,
        id,
        tenant,
      },
      this._rpcOptions()
    );
  }

  /**
   * Lease tasks for processing.
   * @param options The options for the lease request.
   * @returns The leased tasks.
   */
  public async leaseTasks(options: LeaseTasksOptions): Promise<Task[]> {
    const call = this._client.leaseTasks(
      {
        shard: options.shard,
        workerId: options.workerId,
        maxTasks: options.maxTasks,
        tenant: options.tenant,
      },
      this._rpcOptions()
    );

    const response = await call.response;
    return response.tasks;
  }

  /**
   * Report the outcome of a task.
   * @param options The options for reporting the outcome.
   */
  public async reportOutcome(options: ReportOutcomeOptions): Promise<void> {
    const outcome =
      options.outcome.type === "success"
        ? {
            oneofKind: "success" as const,
            success: { data: encodePayload(options.outcome.result) },
          }
        : {
            oneofKind: "failure" as const,
            failure: {
              code: options.outcome.code,
              data:
                options.outcome.data instanceof Uint8Array
                  ? options.outcome.data
                  : encodePayload(options.outcome.data),
            },
          };

    await this._client.reportOutcome(
      {
        shard: options.shard,
        taskId: options.taskId,
        tenant: options.tenant,
        outcome,
      },
      this._rpcOptions()
    );
  }

  /**
   * Send a heartbeat for a task to extend its lease.
   * @param shard    The shard the task is on.
   * @param workerId The worker ID that owns the task.
   * @param taskId   The task ID.
   */
  public async heartbeat(
    shard: string,
    workerId: string,
    taskId: string
  ): Promise<void> {
    await this._client.heartbeat(
      {
        shard,
        workerId,
        taskId,
      },
      this._rpcOptions()
    );
  }

  /**
   * Execute a SQL query against the shard data.
   * @param shard  The shard to query.
   * @param sql    The SQL query to execute.
   * @param tenant Optional tenant ID when tenancy is enabled.
   * @returns The query results.
   */
  public async query(
    shard: string,
    sql: string,
    tenant?: string
  ): Promise<QueryResponse> {
    const call = this._client.query(
      {
        shard,
        sql,
        tenant,
      },
      this._rpcOptions()
    );

    return await call.response;
  }
}

const encoder = new TextEncoder();

/**
 * Encode a JSON-serializable value as an array of bytes.
 * @param value The value to encode.
 * @returns The encoded value as an array of bytes.
 */
export function encodePayload(value: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(value));
}

const decoder = new TextDecoder();

/**
 * Decode an array of bytes as a JSON value.
 * @param bytes The array of bytes to decode.
 * @returns The bytes decoded and parsed as JSON.
 */
export function decodePayload<T = unknown>(bytes: Uint8Array | undefined): T {
  if (!bytes || bytes.length === 0) {
    return undefined as T;
  }
  return JSON.parse(decoder.decode(bytes)) as T;
}
