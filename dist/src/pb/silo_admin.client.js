"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloAdminClient = void 0;
const silo_admin_1 = require("./silo_admin");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
/**
 * Admin service for operator/management operations.
 * Used by the web UI, siloctl CLI, and monitoring tools.
 * These RPCs are not part of the worker-facing API.
 *
 * @generated from protobuf service silo.v1.SiloAdmin
 */
class SiloAdminClient {
    _transport;
    typeName = silo_admin_1.SiloAdmin.typeName;
    methods = silo_admin_1.SiloAdmin.methods;
    options = silo_admin_1.SiloAdmin.options;
    constructor(_transport) {
        this._transport = _transport;
    }
    /**
     * Get information about this node and all the shards it owns.
     *
     * @generated from protobuf rpc: GetNodeInfo
     */
    getNodeInfo(input, options) {
        const method = this.methods[0], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input, options) {
        const method = this.methods[1], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Request a shard split operation.
     * Initiates splitting a shard into two child shards at the specified split point.
     * Returns FAILED_PRECONDITION if a split is already in progress.
     * Returns NOT_FOUND if the shard doesn't exist on this node.
     * Returns INVALID_ARGUMENT if the split point is outside the shard's range.
     *
     * @generated from protobuf rpc: RequestSplit
     */
    requestSplit(input, options) {
        const method = this.methods[2], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Get the status of a shard split operation.
     * Returns the current phase and child shard IDs if a split is in progress.
     * If no split is in progress, returns with in_progress=false.
     *
     * @generated from protobuf rpc: GetSplitStatus
     */
    getSplitStatus(input, options) {
        const method = this.methods[3], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Configure a shard's placement ring.
     * Changes which placement ring the shard belongs to, affecting which nodes can own it.
     * The shard will be handed off to a node that participates in the new ring.
     * Returns the previous and current ring assignments.
     *
     * @generated from protobuf rpc: ConfigureShard
     */
    configureShard(input, options) {
        const method = this.methods[4], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Import jobs from another system with historical attempts.
     * Unlike Enqueue, ImportJobs accepts completed attempt records and lets Silo
     * take ownership going forward. Used for migrating workloads from other job queues.
     * Each job is imported independently; per-job errors are returned in the response.
     *
     * @generated from protobuf rpc: ImportJobs
     */
    importJobs(input, options) {
        const method = this.methods[5], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input, options) {
        const method = this.methods[6], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Force-release a shard lease regardless of the current holder.
     * Operator escape hatch for recovering from permanently lost nodes.
     * After force-release, any live node that desires the shard can acquire it.
     *
     * @generated from protobuf rpc: ForceReleaseShard
     */
    forceReleaseShard(input, options) {
        const method = this.methods[7], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Trigger a full compaction on a shard's SlateDB instance.
     * Merges all L0 SSTs and sorted runs into a single sorted run, removing tombstones.
     * This is useful for reclaiming space after bulk deletes or high job throughput.
     *
     * @generated from protobuf rpc: CompactShard
     */
    compactShard(input, options) {
        const method = this.methods[8], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Read the LSM tree state of a shard's SlateDB instance.
     * Returns information about L0 SSTs and sorted runs to help operators
     * understand storage health and whether compaction is needed.
     *
     * @generated from protobuf rpc: ReadLsmState
     */
    readLsmState(input, options) {
        const method = this.methods[9], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
}
exports.SiloAdminClient = SiloAdminClient;
//# sourceMappingURL=silo_admin.client.js.map