"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloGRPCClient = exports.AttemptStatus = exports.JobStatus = exports.TaskNotFoundError = exports.JobNotTerminalError = exports.JobNotFoundError = exports.GubernatorBehavior = exports.GubernatorAlgorithm = void 0;
exports.fnv1a32 = fnv1a32;
exports.defaultTenantToShard = defaultTenantToShard;
exports.sleep = sleep;
exports.encodePayload = encodePayload;
exports.decodePayload = decodePayload;
const grpc_js_1 = require("@grpc/grpc-js");
const grpc_transport_1 = require("@protobuf-ts/grpc-transport");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
const msgpackr_1 = require("msgpackr");
const silo_client_1 = require("./pb/silo.client");
const silo_1 = require("./pb/silo");
Object.defineProperty(exports, "GubernatorAlgorithm", { enumerable: true, get: function () { return silo_1.GubernatorAlgorithm; } });
Object.defineProperty(exports, "GubernatorBehavior", { enumerable: true, get: function () { return silo_1.GubernatorBehavior; } });
const JobHandle_1 = require("./JobHandle");
/**
 * Error thrown when a job is not found.
 */
class JobNotFoundError extends Error {
    code = "SILO_JOB_NOT_FOUND";
    /** The job ID that was not found */
    jobId;
    /** The tenant ID used for the lookup */
    tenant;
    constructor(jobId, tenant) {
        const tenantMsg = tenant ? ` in tenant "${tenant}"` : "";
        super(`Job "${jobId}" not found${tenantMsg}`);
        this.name = "JobNotFoundError";
        this.jobId = jobId;
        this.tenant = tenant;
    }
}
exports.JobNotFoundError = JobNotFoundError;
/**
 * Error thrown when trying to get a job's result before it has reached a terminal state.
 */
class JobNotTerminalError extends Error {
    code = "SILO_JOB_NOT_TERMINAL";
    /** The job ID */
    jobId;
    /** The tenant ID */
    tenant;
    /** The current status of the job */
    currentStatus;
    constructor(jobId, tenant, currentStatus) {
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
exports.JobNotTerminalError = JobNotTerminalError;
/**
 * Error thrown when a task (lease) is not found.
 */
class TaskNotFoundError extends Error {
    code = "SILO_TASK_NOT_FOUND";
    /** The task ID that was not found */
    taskId;
    constructor(taskId) {
        super(`Task "${taskId}" not found`);
        this.name = "TaskNotFoundError";
        this.taskId = taskId;
    }
}
exports.TaskNotFoundError = TaskNotFoundError;
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
function fnv1a32(str) {
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
function defaultTenantToShard(tenantId, numShards) {
    if (numShards <= 0) {
        throw new Error("numShards must be positive");
    }
    const hash = fnv1a32(tenantId);
    return hash % numShards;
}
/** gRPC metadata key for shard owner address on redirect */
const SHARD_OWNER_ADDR_METADATA_KEY = "x-silo-shard-owner-addr";
/** Possible job statuses */
var JobStatus;
(function (JobStatus) {
    JobStatus["Scheduled"] = "Scheduled";
    JobStatus["Running"] = "Running";
    JobStatus["Succeeded"] = "Succeeded";
    JobStatus["Failed"] = "Failed";
    JobStatus["Cancelled"] = "Cancelled";
})(JobStatus || (exports.JobStatus = JobStatus = {}));
/** Possible attempt statuses */
var AttemptStatus;
(function (AttemptStatus) {
    AttemptStatus["Running"] = "Running";
    AttemptStatus["Succeeded"] = "Succeeded";
    AttemptStatus["Failed"] = "Failed";
    AttemptStatus["Cancelled"] = "Cancelled";
})(AttemptStatus || (exports.AttemptStatus = AttemptStatus = {}));
/**
 * Convert a TypeScript-native JobLimit to the protobuf Limit format.
 * @internal
 */
function toProtoLimit(limit) {
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
    }
    else if (limit.type === "floatingConcurrency") {
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
    }
    else {
        return {
            limit: {
                oneofKind: "rateLimit",
                rateLimit: {
                    name: limit.name,
                    uniqueKey: limit.uniqueKey,
                    limit: limit.limit,
                    durationMs: limit.durationMs,
                    hits: limit.hits ?? 1,
                    algorithm: limit.algorithm ?? silo_1.GubernatorAlgorithm.TOKEN_BUCKET,
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
function fromProtoLimit(limit) {
    if (limit.limit.oneofKind === "concurrency") {
        return {
            type: "concurrency",
            key: limit.limit.concurrency.key,
            maxConcurrency: limit.limit.concurrency.maxConcurrency,
        };
    }
    else if (limit.limit.oneofKind === "floatingConcurrency") {
        const fl = limit.limit.floatingConcurrency;
        return {
            type: "floatingConcurrency",
            key: fl.key,
            defaultMaxConcurrency: fl.defaultMaxConcurrency,
            refreshIntervalMs: fl.refreshIntervalMs,
            metadata: Object.keys(fl.metadata).length > 0 ? { ...fl.metadata } : undefined,
        };
    }
    else if (limit.limit.oneofKind === "rateLimit") {
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
/**
 * Check if an error is a "shard not found" / wrong shard error.
 * @internal
 */
function isWrongShardError(error) {
    if (error instanceof runtime_rpc_1.RpcError) {
        // gRPC NOT_FOUND status with "shard not found" message
        return (error.code === "NOT_FOUND" && error.message.includes("shard not found"));
    }
    return false;
}
/**
 * Extract redirect address from error metadata.
 * @internal
 */
function extractRedirectAddress(error) {
    if (error instanceof runtime_rpc_1.RpcError) {
        // Check for redirect metadata
        const metadata = error.meta;
        if (metadata && SHARD_OWNER_ADDR_METADATA_KEY in metadata) {
            return metadata[SHARD_OWNER_ADDR_METADATA_KEY];
        }
    }
    return undefined;
}
/**
 * Sleep for a given number of milliseconds.
 * @internal
 */
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
/** Default tenant used when tenancy is not enabled */
const DEFAULT_TENANT = "-";
/**
 * Convert proto JobStatus enum to public JobStatus enum.
 * @internal
 */
function protoJobStatusToPublic(status) {
    switch (status) {
        case silo_1.JobStatus.SCHEDULED:
            return JobStatus.Scheduled;
        case silo_1.JobStatus.RUNNING:
            return JobStatus.Running;
        case silo_1.JobStatus.SUCCEEDED:
            return JobStatus.Succeeded;
        case silo_1.JobStatus.FAILED:
            return JobStatus.Failed;
        case silo_1.JobStatus.CANCELLED:
            return JobStatus.Cancelled;
        default:
            throw new Error(`Unknown job status: ${status}`);
    }
}
/**
 * Convert proto AttemptStatus enum to public AttemptStatus enum.
 * @internal
 */
function protoAttemptStatusToPublic(status) {
    switch (status) {
        case silo_1.AttemptStatus.RUNNING:
            return AttemptStatus.Running;
        case silo_1.AttemptStatus.SUCCEEDED:
            return AttemptStatus.Succeeded;
        case silo_1.AttemptStatus.FAILED:
            return AttemptStatus.Failed;
        case silo_1.AttemptStatus.CANCELLED:
            return AttemptStatus.Cancelled;
        default:
            throw new Error(`Unknown attempt status: ${status}`);
    }
}
/**
 * Convert proto JobAttempt to public JobAttempt.
 * @internal
 */
function protoAttemptToPublic(attempt) {
    return {
        jobId: attempt.jobId,
        attemptNumber: attempt.attemptNumber,
        taskId: attempt.taskId,
        status: protoAttemptStatusToPublic(attempt.status),
        startedAtMs: attempt.startedAtMs,
        finishedAtMs: attempt.finishedAtMs,
        result: attempt.result,
        errorCode: attempt.errorCode,
        errorData: attempt.errorData,
    };
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
class SiloGRPCClient {
    /** @internal */
    _connections = new Map();
    /** @internal */
    _rpcOptions;
    /** @internal */
    _channelCredentials;
    /** @internal */
    _grpcClientOptions;
    /** @internal */
    _tenantToShard;
    /** @internal */
    _maxWrongShardRetries;
    /** @internal */
    _wrongShardRetryDelayMs;
    /** @internal Shard ID → server address */
    _shardToServer = new Map();
    /** @internal */
    _numShards = 0;
    /** @internal */
    _topologyRefreshInterval = null;
    /** Counter for round-robin server selection */
    _anyClientCounter = 0;
    /** @internal */
    _initialServers;
    /**
     * Create a new Silo gRPC client.
     *
     * The client will automatically discover the cluster topology by calling
     * GetClusterInfo on the provided servers.
     *
     * @param options Client options including server addresses.
     */
    constructor(options) {
        const tokenFn = options.token
            ? typeof options.token === "string"
                ? () => Promise.resolve(options.token)
                : options.token
            : null;
        const useTls = options.useTls ?? true;
        this._channelCredentials = useTls
            ? tokenFn
                ? grpc_js_1.credentials.combineChannelCredentials(grpc_js_1.ChannelCredentials.createSsl(), grpc_js_1.credentials.createFromMetadataGenerator((_, callback) => {
                    tokenFn()
                        .then((token) => {
                        const meta = new grpc_js_1.Metadata();
                        meta.add("authorization", `Bearer ${token}`);
                        callback(null, meta);
                    })
                        .catch(callback);
                }))
                : grpc_js_1.ChannelCredentials.createSsl()
            : grpc_js_1.ChannelCredentials.createInsecure();
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
                : () => options.rpcOptions;
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
    _parseServers(servers) {
        if (typeof servers === "string") {
            return [servers];
        }
        if (Array.isArray(servers)) {
            return servers.map((s) => typeof s === "string" ? s : `${s.host}:${s.port}`);
        }
        return [`${servers.host}:${servers.port}`];
    }
    /**
     * Get or create a connection to a server.
     * @internal
     */
    _getOrCreateConnection(address) {
        let conn = this._connections.get(address);
        if (!conn) {
            const transport = new grpc_transport_1.GrpcTransport({
                host: address,
                channelCredentials: this._channelCredentials,
                clientOptions: this._grpcClientOptions,
            });
            const client = new silo_client_1.SiloClient(transport);
            conn = { address, transport, client };
            this._connections.set(address, conn);
        }
        return conn;
    }
    /**
     * Compute the shard ID from tenant.
     * @internal
     */
    _resolveShard(tenant) {
        const tenantId = tenant ?? DEFAULT_TENANT;
        if (this._numShards <= 0) {
            throw new Error("Cluster topology not discovered yet. Call refreshTopology() first.");
        }
        return this._tenantToShard(tenantId, this._numShards);
    }
    /**
     * Get the client for a specific shard.
     * @internal
     */
    _getClientForShard(shardId) {
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
    _getAnyClient() {
        const connections = Array.from(this._connections.values());
        if (connections.length === 0) {
            throw new Error("No server connections available");
        }
        const index = this._anyClientCounter % connections.length;
        this._anyClientCounter++;
        return connections[index].client;
    }
    /**
     * Get the client for a tenant.
     * @internal
     */
    _getClientForTenant(tenant) {
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
    async _withWrongShardRetry(tenant, operation) {
        let lastError;
        let delay = this._wrongShardRetryDelayMs;
        let { client, shard } = this._getClientForTenant(tenant);
        for (let attempt = 0; attempt <= this._maxWrongShardRetries; attempt++) {
            try {
                return await operation(client, shard);
            }
            catch (error) {
                if (isWrongShardError(error) && attempt < this._maxWrongShardRetries) {
                    lastError = error;
                    // Check for redirect address in metadata
                    const redirectAddr = extractRedirectAddress(error);
                    if (redirectAddr) {
                        // Update routing and create connection to new server
                        this._shardToServer.set(shard, redirectAddr);
                        const conn = this._getOrCreateConnection(redirectAddr);
                        client = conn.client;
                    }
                    else {
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
    async refreshTopology() {
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
            }
            catch {
                // Try next server
                continue;
            }
        }
        throw new Error("Failed to refresh cluster topology from any server");
    }
    /**
     * Close the underlying GRPC connections.
     */
    close() {
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
    async enqueue(options) {
        const id = await this._withWrongShardRetry(options.tenant, async (client, shard) => {
            const call = client.enqueue({
                shard,
                id: options.id ?? "",
                priority: options.priority ?? 50,
                startAtMs: options.startAtMs ?? BigInt(Date.now()),
                retryPolicy: options.retryPolicy,
                payload: { data: encodePayload(options.payload) },
                limits: options.limits?.map(toProtoLimit) ?? [],
                tenant: options.tenant,
                metadata: options.metadata ?? {},
            }, this._rpcOptions());
            const response = await call.response;
            return response.id;
        });
        return new JobHandle_1.JobHandle(this, id, options.tenant);
    }
    /**
     * Get a job by ID.
     * @param id      The job ID.
     * @param tenant  Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @param options Optional settings for the request.
     * @returns The job details.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    async getJob(id, tenant, options) {
        try {
            return await this._withWrongShardRetry(tenant, async (client, shard) => {
                const call = client.getJob({
                    shard,
                    id,
                    tenant,
                    includeAttempts: options?.includeAttempts ?? false,
                }, this._rpcOptions());
                const response = await call.response;
                return {
                    id: response.id,
                    priority: response.priority,
                    enqueueTimeMs: response.enqueueTimeMs,
                    payload: response.payload,
                    retryPolicy: response.retryPolicy,
                    limits: response.limits
                        .map(fromProtoLimit)
                        .filter((l) => l !== undefined),
                    metadata: response.metadata,
                    status: protoJobStatusToPublic(response.status),
                    statusChangedAtMs: response.statusChangedAtMs,
                    attempts: response.attempts.length > 0
                        ? response.attempts.map(protoAttemptToPublic)
                        : undefined,
                    nextAttemptStartsAfterMs: response.nextAttemptStartsAfterMs,
                };
            });
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async deleteJob(id, tenant) {
        try {
            await this._withWrongShardRetry(tenant, async (client, shard) => {
                await client.deleteJob({
                    shard,
                    id,
                    tenant,
                }, this._rpcOptions());
            });
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async cancelJob(id, tenant) {
        try {
            await this._withWrongShardRetry(tenant, async (client, shard) => {
                await client.cancelJob({
                    shard,
                    id,
                    tenant,
                }, this._rpcOptions());
            });
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async restartJob(id, tenant) {
        try {
            await this._withWrongShardRetry(tenant, async (client, shard) => {
                await client.restartJob({
                    shard,
                    id,
                    tenant,
                }, this._rpcOptions());
            });
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async expediteJob(id, tenant) {
        try {
            await this._withWrongShardRetry(tenant, async (client, shard) => {
                await client.expediteJob({
                    shard,
                    id,
                    tenant,
                }, this._rpcOptions());
            });
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async getJobStatus(id, tenant) {
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
    async getJobResult(id, tenant) {
        try {
            return await this._withWrongShardRetry(tenant, async (client, shard) => {
                const call = client.getJobResult({
                    shard,
                    id,
                    tenant,
                }, this._rpcOptions());
                const response = await call.response;
                const status = protoJobStatusToPublic(response.status);
                if (status === JobStatus.Cancelled) {
                    return { status: JobStatus.Cancelled };
                }
                if (status === JobStatus.Succeeded) {
                    let result;
                    if (response.result.oneofKind === "successData" &&
                        response.result.successData.data.length > 0) {
                        result = decodePayload(response.result.successData.data);
                    }
                    return { status: JobStatus.Succeeded, result };
                }
                if (status === JobStatus.Failed) {
                    let errorData;
                    if (response.result.oneofKind === "failure" &&
                        response.result.failure.errorData.length > 0) {
                        errorData = decodePayload(response.result.failure.errorData);
                    }
                    return {
                        status: JobStatus.Failed,
                        errorCode: response.result.oneofKind === "failure"
                            ? response.result.failure.errorCode
                            : undefined,
                        errorData,
                    };
                }
                // Should not reach here if server is behaving correctly
                throw new JobNotTerminalError(id, tenant);
            });
        }
        catch (error) {
            // NOT_FOUND means job doesn't exist
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
                throw new JobNotFoundError(id, tenant);
            }
            // FAILED_PRECONDITION means job is not in a terminal state yet
            if (error instanceof runtime_rpc_1.RpcError && error.code === "FAILED_PRECONDITION") {
                // Try to get current status for better error message
                try {
                    const job = await this.getJob(id, tenant);
                    throw new JobNotTerminalError(id, tenant, job.status);
                }
                catch (e) {
                    if (e instanceof JobNotTerminalError)
                        throw e;
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
    handle(id, tenant) {
        return new JobHandle_1.JobHandle(this, id, tenant);
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
     * @returns Object containing both job tasks and refresh tasks.
     */
    async leaseTasks(options) {
        // If shard is specified, route to that shard's server
        // Otherwise, use any available server (it will poll all its local shards)
        const client = options.shard !== undefined
            ? this._getClientForShard(options.shard)
            : this._getAnyClient();
        const call = client.leaseTasks({
            shard: options.shard,
            workerId: options.workerId,
            maxTasks: options.maxTasks,
        }, this._rpcOptions());
        const response = await call.response;
        // Convert proto refresh tasks to our RefreshTask type
        const refreshTasks = response.refreshTasks.map((rt) => ({
            id: rt.id,
            queueKey: rt.queueKey,
            currentMaxConcurrency: rt.currentMaxConcurrency,
            lastRefreshedAtMs: rt.lastRefreshedAtMs,
            metadata: { ...rt.metadata },
            leaseMs: rt.leaseMs,
            shard: rt.shard,
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
    async reportOutcome(options) {
        let outcome;
        if (options.outcome.type === "success") {
            outcome = {
                oneofKind: "success",
                success: { data: encodePayload(options.outcome.result) },
            };
        }
        else if (options.outcome.type === "failure") {
            outcome = {
                oneofKind: "failure",
                failure: {
                    code: options.outcome.code,
                    data: options.outcome.data instanceof Uint8Array
                        ? options.outcome.data
                        : encodePayload(options.outcome.data),
                },
            };
        }
        else {
            // cancelled
            outcome = {
                oneofKind: "cancelled",
                cancelled: {},
            };
        }
        try {
            // Route to the correct shard (from Task.shard)
            const client = this._getClientForShard(options.shard);
            await client.reportOutcome({
                shard: options.shard,
                taskId: options.taskId,
                tenant: options.tenant,
                outcome,
            }, this._rpcOptions());
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async reportRefreshOutcome(options) {
        const outcome = options.outcome.type === "success"
            ? {
                oneofKind: "success",
                success: { newMaxConcurrency: options.outcome.newMaxConcurrency },
            }
            : {
                oneofKind: "failure",
                failure: {
                    code: options.outcome.code,
                    message: options.outcome.message,
                },
            };
        try {
            const client = this._getClientForShard(options.shard);
            await client.reportRefreshOutcome({
                shard: options.shard,
                taskId: options.taskId,
                tenant: options.tenant,
                outcome,
            }, this._rpcOptions());
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
     * @param tenant   The tenant ID (required when tenancy is enabled).
     * @returns HeartbeatResult indicating if the job was cancelled.
     * @throws TaskNotFoundError if the task (lease) doesn't exist.
     */
    async heartbeat(workerId, taskId, shard, tenant) {
        try {
            const client = this._getClientForShard(shard);
            const call = client.heartbeat({
                shard,
                workerId,
                taskId,
                tenant,
            }, this._rpcOptions());
            const response = await call.response;
            return {
                cancelled: response.cancelled,
                cancelledAtMs: response.cancelledAtMs,
            };
        }
        catch (error) {
            if (error instanceof runtime_rpc_1.RpcError && error.code === "NOT_FOUND") {
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
    async query(sql, tenant) {
        return this._withWrongShardRetry(tenant, async (client, shard) => {
            const call = client.query({
                shard,
                sql,
                tenant,
            }, this._rpcOptions());
            return await call.response;
        });
    }
    /**
     * Compute the shard ID for a given tenant.
     * Useful for debugging or when you need to know which shard a tenant maps to.
     * @param tenant The tenant ID
     * @returns The shard ID
     */
    getShardForTenant(tenant) {
        return this._resolveShard(tenant);
    }
    /**
     * Get the current cluster topology.
     * @returns Map of shard ID to server address
     */
    getTopology() {
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
    async resetShards() {
        // Reset shards on all known servers
        const servers = new Set();
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
exports.SiloGRPCClient = SiloGRPCClient;
/**
 * Encode a value as MessagePack bytes.
 * @param value The value to encode.
 * @returns The encoded value as MessagePack bytes.
 */
function encodePayload(value) {
    return (0, msgpackr_1.pack)(value);
}
/**
 * Decode MessagePack bytes as a value.
 * @param bytes The MessagePack bytes to decode.
 * @returns The decoded value.
 */
function decodePayload(bytes) {
    if (!bytes || bytes.length === 0) {
        return undefined;
    }
    return (0, msgpackr_1.unpack)(bytes);
}
//# sourceMappingURL=client.js.map