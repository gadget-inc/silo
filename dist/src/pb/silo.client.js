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
     * @generated from protobuf rpc: DeleteJob
     */
    deleteJob(input, options) {
        const method = this.methods[3], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: CancelJob
     */
    cancelJob(input, options) {
        const method = this.methods[4], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: LeaseTasks
     */
    leaseTasks(input, options) {
        const method = this.methods[5], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: ReportOutcome
     */
    reportOutcome(input, options) {
        const method = this.methods[6], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: ReportRefreshOutcome
     */
    reportRefreshOutcome(input, options) {
        const method = this.methods[7], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: Heartbeat
     */
    heartbeat(input, options) {
        const method = this.methods[8], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * @generated from protobuf rpc: Query
     */
    query(input, options) {
        const method = this.methods[9], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
    /**
     * Admin: Reset all shards owned by this server (dev mode only)
     * Clears all data - jobs, tasks, queues, etc.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input, options) {
        const method = this.methods[10], opt = this._transport.mergeOptions(options);
        return (0, runtime_rpc_1.stackIntercept)("unary", this._transport, method, opt, input);
    }
}
exports.SiloClient = SiloClient;
//# sourceMappingURL=silo.client.js.map