"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloClient = void 0;
const silo_1 = require("./silo");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
/**
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
     * Get cluster topology for client-side routing
     *
     * @generated from protobuf rpc: GetClusterInfo
     */
    getClusterInfo(input, options) {
        const method = this.methods[0], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: Enqueue
     */
    enqueue(input, options) {
        const method = this.methods[1], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: GetJob
     */
    getJob(input, options) {
        const method = this.methods[2], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Get the result of a completed job.
     * Returns NOT_FOUND if the job doesn't exist.
     * Returns FAILED_PRECONDITION if the job is not yet in a terminal state (still scheduled or running).
     * Returns the result/error/cancellation info if the job has completed.
     *
     * @generated from protobuf rpc: GetJobResult
     */
    getJobResult(input, options) {
        const method = this.methods[3], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input, options) {
        const method = this.methods[4], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input, options) {
        const method = this.methods[5], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Restart a cancelled or failed job, allowing it to be processed again.
     * Returns FAILED_PRECONDITION if the job is not in a restartable state.
     * Returns NOT_FOUND if the job doesn't exist.
     *
     * @generated from protobuf rpc: RestartJob
     */
    restartJob(input, options) {
        const method = this.methods[6], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input, options) {
        const method = this.methods[7], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input, options) {
        const method = this.methods[8], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input, options) {
        const method = this.methods[9], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input, options) {
        const method = this.methods[10], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: Query
     */
    query(input, options) {
        const method = this.methods[11], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Streaming query that returns Arrow IPC encoded record batches
     * First message contains the schema, subsequent messages contain record batches
     *
     * @generated from protobuf rpc: QueryArrow
     */
    queryArrow(input, options) {
        const method = this.methods[12], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("serverStreaming", this._transport, method, opt, input);
    }
    /**
     * Admin: Reset all shards owned by this server (dev mode only)
     * Clears all data - jobs, tasks, queues, etc.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input, options) {
        const method = this.methods[13], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
}
exports.SiloClient = SiloClient;
//# sourceMappingURL=silo.client.js.map