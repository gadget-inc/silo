"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloClient = void 0;
const silo_1 = require("./silo");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
/**
 * The Silo job queue service.
 *
 * @generated from protobuf service silo.v1.Silo
 */
class SiloClient {
    _transport;
    typeName = silo_1.Silo.typeName;
    methods = silo_1.Silo.methods;
    options = silo_1.Silo.options;
    constructor(_transport) {
        this._transport = _transport;
    }
    /**
     * Get cluster topology for client-side routing.
     * Returns shard ownership information so clients can route requests to the correct node.
     *
     * @generated from protobuf rpc: GetClusterInfo
     */
    getClusterInfo(input, options) {
        const method = this.methods[0], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Get information about this node and all the shards it owns.
     *
     * @generated from protobuf rpc: GetNodeInfo
     */
    getNodeInfo(input, options) {
        const method = this.methods[1], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Enqueue a new job for processing.
     * The job will be scheduled according to its start_at_ms and processed when limits allow.
     *
     * @generated from protobuf rpc: Enqueue
     */
    enqueue(input, options) {
        const method = this.methods[2], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Get full details of a job including its current status.
     *
     * @generated from protobuf rpc: GetJob
     */
    getJob(input, options) {
        const method = this.methods[3], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Get the result of a completed job.
     * Returns NOT_FOUND if the job doesn't exist.
     * Returns FAILED_PRECONDITION if the job hasn't reached a terminal state.
     *
     * @generated from protobuf rpc: GetJobResult
     */
    getJobResult(input, options) {
        const method = this.methods[4], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Permanently delete a job and all its data.
     * Running jobs should be cancelled first.
     *
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input, options) {
        const method = this.methods[5], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Cancel a job. Running jobs will be notified via heartbeat.
     * Jobs in any state can be cancelled.
     *
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input, options) {
        const method = this.methods[6], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Restart a cancelled or failed job for another attempt.
     * Returns FAILED_PRECONDITION if job is not in a restartable state.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: RestartJob
     */
    restartJob(input, options) {
        const method = this.methods[7], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Expedite a future-scheduled job to run immediately.
     * Useful for dragging forward scheduled jobs or skipping retry backoff delays.
     * Returns FAILED_PRECONDITION if job is not in an expeditable state
     * (already running, terminal, cancelled, or task already ready to run).
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: ExpediteJob
     */
    expediteJob(input, options) {
        const method = this.methods[8], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Lease a specific job's task directly, putting it into Running state.
     * Test-oriented helper: workers should use LeaseTasks for normal processing.
     * Returns FAILED_PRECONDITION if the job is running, terminal, or cancelled.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: LeaseTask
     */
    leaseTask(input, options) {
        const method = this.methods[9], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Lease tasks for a worker to process.
     * Workers should call this periodically to get work.
     * Returns both job tasks and floating limit refresh tasks.
     *
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input, options) {
        const method = this.methods[10], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Report the outcome of a completed job task.
     * Must be called before the task lease expires.
     *
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input, options) {
        const method = this.methods[11], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Report the outcome of a floating limit refresh task.
     * Workers compute new max_concurrency and report here.
     *
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input, options) {
        const method = this.methods[12], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Extend a task lease and check for cancellation.
     * Workers must heartbeat before lease expires to keep tasks.
     * Returns cancelled=true if the job was cancelled.
     *
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input, options) {
        const method = this.methods[13], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Execute an SQL query against shard data.
     * Returns results as JSON rows.
     *
     * @generated from protobuf rpc: Query
     */
    query(input, options) {
        const method = this.methods[14], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Execute an SQL query with Arrow IPC streaming response.
     * More efficient for large result sets.
     * First message contains schema, subsequent messages contain record batches.
     *
     * @generated from protobuf rpc: QueryArrow
     */
    queryArrow(input, options) {
        const method = this.methods[15], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("serverStreaming", this._transport, method, opt, input);
    }
    /**
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input, options) {
        const method = this.methods[16], opt = this._transport.mergeOptions(options);
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
        const method = this.methods[17], opt = this._transport.mergeOptions(options);
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
        const method = this.methods[18], opt = this._transport.mergeOptions(options);
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
        const method = this.methods[19], opt = this._transport.mergeOptions(options);
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
        const method = this.methods[20], opt = this._transport.mergeOptions(options);
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
        const method = this.methods[21], opt = this._transport.mergeOptions(options);
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
        const method = this.methods[22], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
}
exports.SiloClient = SiloClient;
//# sourceMappingURL=silo.client.js.map