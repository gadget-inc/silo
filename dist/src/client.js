"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloGRPCClient = exports.AttemptStatus = exports.JobStatus = exports.SiloDeadlineExceededError = exports.SiloUnavailableError = exports.SiloResourceExhaustedError = exports.SiloPermissionDeniedError = exports.SiloUnauthenticatedError = exports.SiloFailedPreconditionError = exports.SiloInvalidArgumentError = exports.JobAlreadyExistsError = exports.SiloNotFoundError = exports.SiloAlreadyExistsError = exports.SiloGrpcError = exports.TaskNotFoundError = exports.JobNotTerminalError = exports.JobNotFoundError = exports.GubernatorBehavior = exports.GubernatorAlgorithm = void 0;
exports.shardForTenant = shardForTenant;
exports.sleep = sleep;
exports.encodeBytes = encodeBytes;
exports.decodeBytes = decodeBytes;
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
        const statusMsg = currentStatus ? ` (current status: ${currentStatus})` : "";
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
 * Base class for errors translated from gRPC status codes.
 */
class SiloGrpcError extends Error {
    code = "SILO_GRPC_ERROR";
    /** The original gRPC status code. */
    grpcCode;
    /** The original gRPC status message. */
    grpcMessage;
    constructor(message, grpcCode, grpcMessage) {
        super(message);
        this.name = "SiloGrpcError";
        this.grpcCode = grpcCode;
        this.grpcMessage = grpcMessage;
    }
}
exports.SiloGrpcError = SiloGrpcError;
/**
 * Error thrown when the requested resource already exists.
 */
class SiloAlreadyExistsError extends SiloGrpcError {
    code = "SILO_ALREADY_EXISTS";
    constructor(grpcMessage) {
        super(grpcMessage, "ALREADY_EXISTS", grpcMessage);
        this.name = "SiloAlreadyExistsError";
    }
}
exports.SiloAlreadyExistsError = SiloAlreadyExistsError;
/**
 * Error thrown when the requested resource is not found.
 */
class SiloNotFoundError extends SiloGrpcError {
    code = "SILO_NOT_FOUND";
    constructor(grpcMessage) {
        super(grpcMessage, "NOT_FOUND", grpcMessage);
        this.name = "SiloNotFoundError";
    }
}
exports.SiloNotFoundError = SiloNotFoundError;
/**
 * Error thrown when enqueueing a job with an ID that already exists.
 */
class JobAlreadyExistsError extends SiloAlreadyExistsError {
    code = "SILO_JOB_ALREADY_EXISTS";
    /** The conflicting job ID, if known. */
    jobId;
    /** The tenant ID, if provided. */
    tenant;
    constructor(jobId, tenant, grpcMessage) {
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
exports.JobAlreadyExistsError = JobAlreadyExistsError;
/**
 * Error thrown when a request argument is invalid.
 */
class SiloInvalidArgumentError extends SiloGrpcError {
    code = "SILO_INVALID_ARGUMENT";
    constructor(grpcMessage) {
        super(grpcMessage, "INVALID_ARGUMENT", grpcMessage);
        this.name = "SiloInvalidArgumentError";
    }
}
exports.SiloInvalidArgumentError = SiloInvalidArgumentError;
/**
 * Error thrown when a request violates server-side preconditions.
 */
class SiloFailedPreconditionError extends SiloGrpcError {
    code = "SILO_FAILED_PRECONDITION";
    constructor(grpcMessage) {
        super(grpcMessage, "FAILED_PRECONDITION", grpcMessage);
        this.name = "SiloFailedPreconditionError";
    }
}
exports.SiloFailedPreconditionError = SiloFailedPreconditionError;
/**
 * Error thrown when authentication is required or invalid.
 */
class SiloUnauthenticatedError extends SiloGrpcError {
    code = "SILO_UNAUTHENTICATED";
    constructor(grpcMessage) {
        super(grpcMessage, "UNAUTHENTICATED", grpcMessage);
        this.name = "SiloUnauthenticatedError";
    }
}
exports.SiloUnauthenticatedError = SiloUnauthenticatedError;
/**
 * Error thrown when the caller is not authorized to perform the operation.
 */
class SiloPermissionDeniedError extends SiloGrpcError {
    code = "SILO_PERMISSION_DENIED";
    constructor(grpcMessage) {
        super(grpcMessage, "PERMISSION_DENIED", grpcMessage);
        this.name = "SiloPermissionDeniedError";
    }
}
exports.SiloPermissionDeniedError = SiloPermissionDeniedError;
/**
 * Error thrown when the server is overloaded or quota-constrained.
 */
class SiloResourceExhaustedError extends SiloGrpcError {
    code = "SILO_RESOURCE_EXHAUSTED";
    constructor(grpcMessage) {
        super(grpcMessage, "RESOURCE_EXHAUSTED", grpcMessage);
        this.name = "SiloResourceExhaustedError";
    }
}
exports.SiloResourceExhaustedError = SiloResourceExhaustedError;
/**
 * Error thrown when the server is currently unavailable.
 */
class SiloUnavailableError extends SiloGrpcError {
    code = "SILO_UNAVAILABLE";
    constructor(grpcMessage) {
        super(grpcMessage, "UNAVAILABLE", grpcMessage);
        this.name = "SiloUnavailableError";
    }
}
exports.SiloUnavailableError = SiloUnavailableError;
/**
 * Error thrown when an RPC exceeded the configured deadline.
 */
class SiloDeadlineExceededError extends SiloGrpcError {
    code = "SILO_DEADLINE_EXCEEDED";
    constructor(grpcMessage) {
        super(grpcMessage, "DEADLINE_EXCEEDED", grpcMessage);
        this.name = "SiloDeadlineExceededError";
    }
}
exports.SiloDeadlineExceededError = SiloDeadlineExceededError;
/**
 * Find the shard that owns a given tenant ID using range-based lookup.
 * Shards should be sorted by rangeStart for efficient binary search.
 * @param tenantId The tenant identifier
 * @param shards Array of shards sorted by rangeStart
 * @returns The shard info, or undefined if no shard found
 */
function shardForTenant(tenantId, shards) {
    if (shards.length === 0) {
        return undefined;
    }
    // Binary search to find the shard whose range contains the tenant
    // We're looking for the last shard where rangeStart <= tenantId
    let left = 0;
    let right = shards.length - 1;
    let result = undefined;
    while (left <= right) {
        const mid = Math.floor((left + right) / 2);
        const shard = shards[mid];
        // Check if tenantId >= rangeStart (empty rangeStart means -infinity)
        const afterStart = shard.rangeStart === "" || tenantId >= shard.rangeStart;
        if (afterStart) {
            // This shard's rangeStart is <= tenantId, could be a candidate
            result = shard;
            left = mid + 1; // Look for a shard with a later rangeStart
        }
        else {
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
        return error.code === "NOT_FOUND" && error.message.includes("shard not found");
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
function mapRpcError(error, context) {
    if (!(error instanceof runtime_rpc_1.RpcError)) {
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
function throwMappedRpcError(error, context) {
    throw mapRpcError(error, context);
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
        default: {
            const _exhaustive = status;
            throw new Error(`Unknown job status: ${String(_exhaustive)}`);
        }
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
        default: {
            const _exhaustive = status;
            throw new Error(`Unknown attempt status: ${String(_exhaustive)}`);
        }
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
        result: attempt.result?.encoding.oneofKind === "msgpack"
            ? decodeBytes(attempt.result.encoding.msgpack, "result")
            : undefined,
        errorCode: attempt.errorCode,
        errorData: attempt.errorData?.encoding.oneofKind === "msgpack"
            ? decodeBytes(attempt.errorData.encoding.msgpack, "errorData")
            : undefined,
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
    _maxWrongShardRetries;
    /** @internal */
    _wrongShardRetryDelayMs;
    /** @internal Shard ID (string UUID) → server address */
    _shardToServer = new Map();
    /** @internal Array of shards sorted by rangeStart for efficient lookup */
    _shards = [];
    /** @internal */
    _topologyRefreshInterval = null;
    /** Counter for round-robin server selection */
    _anyClientCounter = 0;
    /** @internal */
    _initialServers;
    /** @internal Address remapping for proxy routing */
    _addressMap;
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
                : () => options.rpcOptions;
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
    _parseServers(servers) {
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
    _mapAddress(address) {
        return this._addressMap[address] ?? address;
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
     * Resolve the shard ID (string UUID) from tenant using range-based lookup.
     * @internal
     */
    _resolveShard(tenant) {
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
     * Get a client at a specific index (modulo number of connections).
     * Used by workers to implement per-worker round-robin polling.
     * @internal
     */
    _getClientAtIndex(index) {
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
                        const mappedAddr = this._mapAddress(redirectAddr);
                        this._shardToServer.set(shard, mappedAddr);
                        const conn = this._getOrCreateConnection(mappedAddr);
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
     * This updates the shard → server mapping and range info.
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
                // Update shard → server mapping and build shards array
                // Skip shards without a valid address (cluster may not have fully converged)
                this._shardToServer.clear();
                const shards = [];
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
     * @throws JobAlreadyExistsError if enqueue conflicts with an existing job ID.
     */
    async enqueue(options) {
        try {
            const id = await this._withWrongShardRetry(options.tenant, async (client, shard) => {
                const call = client.enqueue({
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
                }, this._rpcOptions());
                const response = await call.response;
                return response.id;
            });
            return new JobHandle_1.JobHandle(this, id, options.tenant);
        }
        catch (error) {
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
                    payload: decodeBytes(response.payload?.encoding.oneofKind === "msgpack"
                        ? response.payload.encoding.msgpack
                        : undefined, "payload"),
                    retryPolicy: response.retryPolicy,
                    limits: response.limits.map(fromProtoLimit).filter((l) => l !== undefined),
                    metadata: response.metadata,
                    status: protoJobStatusToPublic(response.status),
                    statusChangedAtMs: response.statusChangedAtMs,
                    attempts: response.attempts.length > 0 ? response.attempts.map(protoAttemptToPublic) : undefined,
                    nextAttemptStartsAfterMs: response.nextAttemptStartsAfterMs,
                    taskGroup: response.taskGroup,
                };
            });
        }
        catch (error) {
            throwMappedRpcError(error, { operation: "getJob", jobId: id, tenant });
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
    async leaseTask(options) {
        try {
            return await this._withWrongShardRetry(options.tenant, async (client, shard) => {
                const call = client.leaseTask({
                    shard,
                    id: options.id,
                    tenant: options.tenant,
                    workerId: options.workerId,
                }, this._rpcOptions());
                const response = await call.response;
                if (!response.task) {
                    throw new Error("leaseTask response missing task");
                }
                return response.task;
            });
        }
        catch (error) {
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
                        response.result.successData.encoding.oneofKind === "msgpack" &&
                        response.result.successData.encoding.msgpack.length > 0) {
                        result = decodeBytes(response.result.successData.encoding.msgpack, "successData");
                    }
                    return { status: JobStatus.Succeeded, result };
                }
                if (status === JobStatus.Failed) {
                    let errorData;
                    if (response.result.oneofKind === "failure" &&
                        response.result.failure.errorData?.encoding.oneofKind === "msgpack" &&
                        response.result.failure.errorData.encoding.msgpack.length > 0) {
                        errorData = decodeBytes(response.result.failure.errorData.encoding.msgpack, "failure");
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
     * @param serverIndex Optional index for server selection (for per-worker round-robin).
     *                    If provided, uses that index modulo number of servers.
     *                    If not provided, uses the shared round-robin counter.
     * @returns Object containing both job tasks and refresh tasks.
     */
    async leaseTasks(options, serverIndex) {
        try {
            // If shard is specified, route to that shard's server
            // Otherwise, use round-robin server selection. If serverIndex is provided,
            // use it for per-worker round-robin (avoids lock-step with shared counter).
            const client = options.shard !== undefined
                ? this._getClientForShard(options.shard)
                : serverIndex !== undefined
                    ? this._getClientAtIndex(serverIndex)
                    : this._getAnyClient();
            const call = client.leaseTasks({
                shard: options.shard,
                workerId: options.workerId,
                maxTasks: options.maxTasks,
                taskGroup: options.taskGroup,
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
                taskGroup: rt.taskGroup,
            }));
            return {
                tasks: response.tasks,
                refreshTasks,
            };
        }
        catch (error) {
            throwMappedRpcError(error, { operation: "leaseTasks" });
        }
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
                success: {
                    encoding: { oneofKind: "msgpack", msgpack: encodeBytes(options.outcome.result) },
                },
            };
        }
        else if (options.outcome.type === "failure") {
            const errorData = options.outcome.data instanceof Uint8Array
                ? options.outcome.data
                : encodeBytes(options.outcome.data);
            outcome = {
                oneofKind: "failure",
                failure: {
                    code: options.outcome.code,
                    data: {
                        encoding: { oneofKind: "msgpack", msgpack: errorData },
                    },
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
                outcome,
            }, this._rpcOptions());
        }
        catch (error) {
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
                outcome,
            }, this._rpcOptions());
        }
        catch (error) {
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
    async heartbeat(workerId, taskId, shard) {
        try {
            const client = this._getClientForShard(shard);
            const call = client.heartbeat({
                shard,
                workerId,
                taskId,
            }, this._rpcOptions());
            const response = await call.response;
            return {
                cancelled: response.cancelled,
                cancelledAtMs: response.cancelledAtMs,
            };
        }
        catch (error) {
            throwMappedRpcError(error, { operation: "heartbeat", taskId });
        }
    }
    /**
     * Execute a SQL query against the shard data.
     * @param sql    The SQL query to execute.
     * @param tenant Tenant ID for routing to the correct shard. Uses default tenant if not provided.
     * @returns The query results with deserialized rows.
     */
    async query(sql, tenant) {
        try {
            return await this._withWrongShardRetry(tenant, async (client, shard) => {
                const call = client.query({
                    shard,
                    sql,
                    tenant,
                }, this._rpcOptions());
                const response = await call.response;
                const columns = response.columns.map((c) => ({
                    name: c.name,
                    dataType: c.dataType,
                }));
                const rows = response.rows.map((row, index) => {
                    if (row.encoding.oneofKind === "msgpack") {
                        return decodeBytes(row.encoding.msgpack, `row[${index}]`);
                    }
                    throw new Error(`Unsupported encoding for row[${index}]: ${row.encoding.oneofKind}`);
                });
                return { columns, rows, rowCount: response.rowCount };
            });
        }
        catch (error) {
            throwMappedRpcError(error, { operation: "query", tenant });
        }
    }
    /**
     * Compute the shard ID for a given tenant using range-based lookup.
     * Useful for debugging or when you need to know which shard a tenant maps to.
     * @param tenant The tenant ID
     * @returns The shard ID (UUID string) or undefined if no shard found
     */
    getShardForTenant(tenant) {
        return this._resolveShard(tenant);
    }
    /**
     * Get the current cluster topology.
     * @returns Information about shards including their ranges
     */
    getTopology() {
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
    async resetShards() {
        try {
            // Reset shards on all known servers
            const servers = new Set();
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
        }
        catch (error) {
            throwMappedRpcError(error, { operation: "resetShards" });
        }
    }
}
exports.SiloGRPCClient = SiloGRPCClient;
/**
 * Encode a value as MessagePack bytes.
 * @param value The value to encode.
 * @returns The encoded value as MessagePack bytes.
 */
function encodeBytes(value) {
    return (0, msgpackr_1.pack)(value);
}
/**
 * Decode MessagePack bytes as a value.
 * @param bytes The MessagePack bytes to decode.
 * @returns The decoded value.
 */
function decodeBytes(bytes, field) {
    if (!bytes || bytes.length === 0) {
        throw new Error(`No bytes to decode for field ${field}`);
    }
    return (0, msgpackr_1.unpack)(bytes);
}
//# sourceMappingURL=client.js.map