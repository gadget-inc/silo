"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskExecution = void 0;
exports.transformTask = transformTask;
const client_1 = require("./client");
/**
 * Transform a raw protobuf Task into a userland Task with decoded payload.
 */
function transformTask(protoTask) {
    return {
        id: protoTask.id,
        jobId: protoTask.jobId,
        attemptNumber: protoTask.attemptNumber,
        relativeAttemptNumber: protoTask.relativeAttemptNumber,
        leaseMs: protoTask.leaseMs,
        payload: (0, client_1.decodeBytes)(protoTask.payload?.encoding.oneofKind === "msgpack"
            ? protoTask.payload.encoding.msgpack
            : undefined, "payload"),
        priority: protoTask.priority,
        shard: protoTask.shard,
        taskGroup: protoTask.taskGroup,
        tenantId: protoTask.tenantId,
        isLastAttempt: protoTask.isLastAttempt,
        metadata: protoTask.metadata,
        limits: protoTask.limits,
    };
}
/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
class TaskExecution {
    /** The task being executed (raw proto format) */
    task;
    /** The worker ID */
    workerId;
    /** Abort controller for this specific task's cancellation signal */
    _taskAbortController;
    /** Whether the task has been cancelled (by server or client) */
    _cancelled = false;
    /** Whether the worker discovered mid-execution that it no longer holds the lease */
    _leaseLost = false;
    /** Whether the handler has settled (outcome reporting is underway or done) */
    _settled = false;
    /** The reason for cancellation if cancelled */
    _cancellationReason;
    /** Promise resolving when cancel RPC completes (if initiated by client) */
    _cancelPromise;
    /** Reference to the client for cancel RPC */
    _client;
    constructor(task, workerId, client) {
        this.task = task;
        this.workerId = workerId;
        this._client = client;
        this._taskAbortController = new AbortController();
    }
    /**
     * The cancellation signal for this task.
     * Only aborts when the task is explicitly cancelled (by server or client),
     * NOT when the worker shuts down.
     */
    get signal() {
        return this._taskAbortController.signal;
    }
    /** Whether this task has been cancelled */
    get isCancelled() {
        return this._cancelled;
    }
    /** The reason for cancellation, if cancelled */
    get cancellationReason() {
        return this._cancellationReason;
    }
    /** Whether the task was cancelled and should report Cancelled outcome */
    get shouldReportCancelled() {
        return this._cancelled;
    }
    /** Whether the lease for this task is known to be gone (no outcome may be reported) */
    get isLeaseLost() {
        return this._leaseLost;
    }
    /** Whether the handler has settled (outcome reporting is underway or done) */
    get isSettled() {
        return this._settled;
    }
    /**
     * Mark the handler as settled. Heartbeat responses that land after this
     * point are meaningless: outcome reporting releases the lease, so a late
     * lease-gone rejection is the routine completion race, not a lost lease.
     */
    markSettled() {
        this._settled = true;
    }
    /**
     * Mark the lease as lost and cancel the execution.
     * One-shot: returns true only for the call that performs the transition,
     * so concurrent in-flight heartbeat failures surface the event once.
     */
    markLeaseLost() {
        if (this._leaseLost)
            return false;
        this._leaseLost = true;
        if (!this._cancelled) {
            this._cancelled = true;
            this._cancellationReason = "lease-lost";
            this._taskAbortController.abort();
        }
        return true;
    }
    /**
     * Called when heartbeat detects server-side cancellation.
     * Aborts the task signal immediately.
     */
    markCancelledByServer() {
        if (this._cancelled)
            return;
        this._cancelled = true;
        this._cancellationReason = "server";
        this._taskAbortController.abort();
    }
    /**
     * Cancel this task from the client side.
     * Calls the server to cancel the job and aborts the task signal.
     */
    async cancelFromClient() {
        if (this._cancelled)
            return;
        this._cancelled = true;
        this._cancellationReason = "client";
        // Abort the signal immediately so the handler can stop work
        this._taskAbortController.abort();
        // Call the server to persist the cancellation
        // We store the promise so we can await it if needed
        this._cancelPromise = this._client.cancelJob(this.task.jobId, this.task.tenantId).catch(() => {
            // Ignore errors - the job may already be cancelled or completed
        });
        await this._cancelPromise;
    }
}
exports.TaskExecution = TaskExecution;
//# sourceMappingURL=TaskExecution.js.map