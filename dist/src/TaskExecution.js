"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskExecution = void 0;
const utils_1 = require("./utils");
/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
class TaskExecution {
    /** The task being executed */
    task;
    /** The shard this task came from */
    shard;
    /** The worker ID */
    workerId;
    /** Tenant for this task */
    tenant;
    /** Abort controller for this specific task */
    _taskAbortController;
    /** Combined signal that aborts on task cancel OR worker shutdown */
    _combinedSignal;
    /** Whether the task has been cancelled (by server or client) */
    _cancelled = false;
    /** The reason for cancellation if cancelled */
    _cancellationReason;
    /** Promise resolving when cancel RPC completes (if initiated by client) */
    _cancelPromise;
    /** Reference to the client for cancel RPC */
    _client;
    constructor(task, shard, workerId, workerAbortSignal, client) {
        this.task = task;
        this.shard = shard;
        this.workerId = workerId;
        this._client = client;
        this._taskAbortController = new AbortController();
        // Create a combined signal that aborts when EITHER the task or worker is aborted
        this._combinedSignal = (0, utils_1.combineAbortSignals)(workerAbortSignal, this._taskAbortController.signal);
        // Track when worker shutdown causes abort
        workerAbortSignal.addEventListener("abort", () => {
            if (!this._cancelled) {
                this._cancelled = true;
                this._cancellationReason = "worker_shutdown";
            }
        }, { once: true });
    }
    /** The combined abort signal for this task */
    get signal() {
        return this._combinedSignal;
    }
    /** Whether this task has been cancelled */
    get isCancelled() {
        return this._cancelled;
    }
    /** The reason for cancellation, if cancelled */
    get cancellationReason() {
        return this._cancellationReason;
    }
    /** Whether the cancellation was due to server-side cancel (should report Cancelled outcome) */
    get shouldReportCancelled() {
        return (this._cancelled &&
            (this._cancellationReason === "server" ||
                this._cancellationReason === "client"));
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
        this._cancelPromise = this._client
            .cancelJob(this.task.jobId, this.tenant)
            .catch(() => {
            // Ignore errors - the job may already be cancelled or completed
        });
        await this._cancelPromise;
    }
}
exports.TaskExecution = TaskExecution;
//# sourceMappingURL=TaskExecution.js.map