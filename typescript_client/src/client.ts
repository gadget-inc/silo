import type { ClientOptions } from "@grpc/grpc-js";
import { ChannelCredentials, credentials, Metadata } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { TextDecoder, TextEncoder } from "util";
import { SiloClient } from "./pb/silo.client";
import type {
  JsonValueBytes,
  Limit,
  QueryResponse,
  RetryPolicy,
  Task,
  ShardOwner,
} from "./pb/silo";
import { GubernatorAlgorithm, GubernatorBehavior } from "./pb/silo";

export type { QueryResponse, RetryPolicy, Task, ShardOwner };
export { GubernatorAlgorithm, GubernatorBehavior };

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
  /** Tenant ID. Required when server has tenancy enabled. */
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
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Default tenant used when tenancy is not enabled */
const DEFAULT_TENANT = "-";

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
   * @internal
   */
  private _getAnyClient(): SiloClient {
    const firstConn = this._connections.values().next().value;
    if (!firstConn) {
      throw new Error("No server connections available");
    }
    return firstConn.client;
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
   * @returns The ID of the enqueued job.
   */
  public async enqueue(options: EnqueueJobOptions): Promise<string> {
    return this._withWrongShardRetry(options.tenant, async (client, shard) => {
      const call = client.enqueue(
        {
          shard,
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
    });
  }

  /**
   * Get a job by ID.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   * @returns The job details, or undefined if not found.
   */
  public async getJob(id: string, tenant?: string): Promise<Job | undefined> {
    return this._withWrongShardRetry(tenant, async (client, shard) => {
      const call = client.getJob(
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
    });
  }

  /**
   * Delete a job by ID.
   * @param id     The job ID.
   * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
   */
  public async deleteJob(id: string, tenant?: string): Promise<void> {
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
  }

  /**
   * Lease tasks for processing.
   *
   * By default, leases tasks from all shards the contacted server owns.
   * If `shard` is specified, filters to only that shard.
   *
   * Each returned Task includes a `shard` field indicating which shard it came from,
   * which must be used when reporting outcomes or sending heartbeats.
   *
   * @param options The options for the lease request.
   * @returns The leased tasks (each with a `shard` field).
   */
  public async leaseTasks(options: LeaseTasksOptions): Promise<Task[]> {
    // Get a client - if shard filter is specified, route to that shard's server
    // Otherwise, use any available server (it will poll all its local shards)
    const client =
      options.shard !== undefined
        ? this._getClientForShard(options.shard)
        : this._getAnyClient();

    const call = client.leaseTasks(
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

    // Route to the correct shard (from Task.shard)
    const client = this._getClientForShard(options.shard);
    await client.reportOutcome(
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
   * @param workerId The worker ID that owns the task.
   * @param taskId   The task ID.
   * @param shard    The shard the task came from (from Task.shard).
   * @param tenant   The tenant ID (required when tenancy is enabled).
   */
  public async heartbeat(
    workerId: string,
    taskId: string,
    shard: number,
    tenant?: string
  ): Promise<void> {
    const client = this._getClientForShard(shard);
    await client.heartbeat(
      {
        shard,
        workerId,
        taskId,
        tenant,
      },
      this._rpcOptions()
    );
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
