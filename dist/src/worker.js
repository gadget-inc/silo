"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloWorker = void 0;
const p_queue_1 = __importDefault(require("p-queue"));
const serialize_error_1 = require("serialize-error");
/**
 * A worker that continuously polls for tasks and executes them.
 *
 * The worker polls the server for tasks from all shards the server owns.
 * Each task includes a `shard` field that is automatically used for
 * heartbeats and reporting outcomes.
 *
 * @example
 * ```typescript
 * const worker = new SiloWorker({
 *   client,
 *   workerId: "worker-1",
 *   concurrentPollers: 2,
 *   maxConcurrentTasks: 10,
 *   handler: async (ctx) => {
 *     const payload = decodePayload(ctx.task.payload?.data);
 *     // Process the task...
 *     return { type: "success", result: { processed: true } };
 *   },
 *   // Optional: handle floating limit refreshes
 *   refreshHandler: async (ctx) => {
 *     // Compute new max concurrency based on external factors
 *     const quota = await checkApiQuota(ctx.task.metadata.orgId);
 *     return quota.rateLimit;
 *   },
 * });
 *
 * worker.start();
 * // ... later
 * await worker.stop();
 * ```
 */
class SiloWorker {
    _client;
    _workerId;
    _handler;
    _refreshHandler;
    _tenant;
    _concurrentPollers;
    _maxConcurrentTasks;
    _tasksPerPoll;
    _pollIntervalMs;
    _heartbeatIntervalMs;
    _onError;
    _running = false;
    _abortController = null;
    _pollerPromises = [];
    _taskQueue;
    _heartbeatIntervals = new Map();
    constructor(options) {
        this._client = options.client;
        this._workerId = options.workerId;
        this._handler = options.handler;
        this._refreshHandler = options.refreshHandler;
        this._tenant = options.tenant;
        this._concurrentPollers = options.concurrentPollers ?? 1;
        this._maxConcurrentTasks = options.maxConcurrentTasks ?? 10;
        this._tasksPerPoll =
            options.tasksPerPoll ?? Math.ceil(this._maxConcurrentTasks / 2);
        this._pollIntervalMs = options.pollIntervalMs ?? 1000;
        this._heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;
        this._onError =
            options.onError ??
                ((error, ctx) => {
                    console.error(`[SiloWorker] Error${ctx?.taskId ? ` (task ${ctx.taskId})` : ""}:`, error);
                });
        // Initialize the task queue with concurrency limit
        this._taskQueue = new p_queue_1.default({ concurrency: this._maxConcurrentTasks });
    }
    /**
     * Whether the worker is currently running.
     */
    get isRunning() {
        return this._running;
    }
    /**
     * The number of tasks currently being executed.
     */
    get activeTasks() {
        return this._taskQueue.pending;
    }
    /**
     * The number of tasks waiting in the queue.
     */
    get queuedTasks() {
        return this._taskQueue.size;
    }
    /**
     * Start the worker. This will begin polling for tasks and executing them.
     * Returns immediately after starting the polling loops.
     */
    start() {
        if (this._running) {
            return;
        }
        this._running = true;
        this._abortController = new AbortController();
        // Reset the queue in case we're restarting
        this._taskQueue = new p_queue_1.default({ concurrency: this._maxConcurrentTasks });
        // Start the configured number of pollers
        for (let i = 0; i < this._concurrentPollers; i++) {
            this._pollerPromises.push(this._pollLoop());
        }
    }
    /**
     * Stop the worker gracefully.
     * Waits for all currently executing tasks to complete.
     * @param timeoutMs Maximum time to wait for tasks to complete. Default: 30000ms.
     */
    async stop(timeoutMs = 30000) {
        if (!this._running) {
            return;
        }
        this._running = false;
        this._abortController?.abort();
        // Wait for all pollers to stop
        await Promise.all(this._pollerPromises);
        this._pollerPromises = [];
        // Wait for all queued and active tasks to complete with timeout
        const queueIdlePromise = this._taskQueue.onIdle();
        const timeoutPromise = new Promise((resolve) => setTimeout(resolve, timeoutMs));
        await Promise.race([queueIdlePromise, timeoutPromise]);
        // Clear the queue (any remaining tasks will be abandoned)
        this._taskQueue.clear();
        // Clear all heartbeat intervals
        for (const interval of this._heartbeatIntervals.values()) {
            clearInterval(interval);
        }
        this._heartbeatIntervals.clear();
        this._abortController = null;
    }
    /**
     * The main polling loop for a single poller.
     */
    async _pollLoop() {
        while (this._running) {
            try {
                await this._poll();
            }
            catch (error) {
                if (this._running) {
                    this._onError(error instanceof Error ? error : new Error(String(error)));
                }
            }
            // Wait before polling again if we're still running
            if (this._running) {
                await this._sleep(this._pollIntervalMs);
            }
        }
    }
    /**
     * Poll for tasks and add them to the execution queue.
     */
    async _poll() {
        // Wait if the queue is too full
        while (this._running && this._taskQueue.size >= this._maxConcurrentTasks) {
            await this._sleep(this._pollIntervalMs / 2);
        }
        if (!this._running) {
            return;
        }
        // Calculate how many tasks we can accept
        const availableQueueSlots = this._maxConcurrentTasks - this._taskQueue.size;
        if (availableQueueSlots <= 0) {
            return;
        }
        const tasksToRequest = Math.min(availableQueueSlots, this._tasksPerPoll);
        // Lease tasks from the server (server handles multi-shard polling)
        const result = await this._client.leaseTasks({
            workerId: this._workerId,
            maxTasks: tasksToRequest,
        });
        // Add regular job tasks to the queue
        for (const task of result.tasks) {
            this._enqueueTask(task);
        }
        // Handle refresh tasks
        if (result.refreshTasks.length > 0) {
            if (!this._refreshHandler) {
                throw new Error(`Worker received ${result.refreshTasks.length} floating limit refresh task(s) but no refreshHandler is configured. ` +
                    `Configure a refreshHandler in SiloWorkerOptions to handle floating concurrency limits.`);
            }
            for (const refreshTask of result.refreshTasks) {
                this._enqueueRefreshTask(refreshTask);
            }
        }
    }
    /**
     * Add a task to the execution queue.
     */
    _enqueueTask(task) {
        // Shard is now a number from the proto
        const shard = task.shard;
        // Start heartbeat for this task immediately
        const heartbeatInterval = setInterval(() => {
            this._sendHeartbeat(task.id, shard).catch((error) => {
                this._onError(error instanceof Error ? error : new Error(String(error)), {
                    taskId: task.id,
                });
            });
        }, this._heartbeatIntervalMs);
        this._heartbeatIntervals.set(task.id, heartbeatInterval);
        // Add task to the queue for execution
        this._taskQueue
            .add(async () => {
            await this._executeTask(task, shard);
        })
            .catch((error) => {
            this._onError(error instanceof Error ? error : new Error(String(error)), {
                taskId: task.id,
            });
        })
            .finally(() => {
            // Stop heartbeat for this task
            const interval = this._heartbeatIntervals.get(task.id);
            if (interval) {
                clearInterval(interval);
                this._heartbeatIntervals.delete(task.id);
            }
        });
    }
    /**
     * Add a refresh task to the execution queue.
     */
    _enqueueRefreshTask(task) {
        const shard = task.shard;
        // Start heartbeat for this refresh task
        const heartbeatInterval = setInterval(() => {
            this._sendHeartbeat(task.id, shard).catch((error) => {
                this._onError(error instanceof Error ? error : new Error(String(error)), {
                    taskId: task.id,
                });
            });
        }, this._heartbeatIntervalMs);
        this._heartbeatIntervals.set(task.id, heartbeatInterval);
        // Add to the queue for execution
        this._taskQueue
            .add(async () => {
            await this._executeRefreshTask(task, shard);
        })
            .catch((error) => {
            this._onError(error instanceof Error ? error : new Error(String(error)), {
                taskId: task.id,
            });
        })
            .finally(() => {
            // Stop heartbeat
            const interval = this._heartbeatIntervals.get(task.id);
            if (interval) {
                clearInterval(interval);
                this._heartbeatIntervals.delete(task.id);
            }
        });
    }
    /**
     * Execute a single task and report its outcome.
     */
    async _executeTask(task, shard) {
        const context = {
            task,
            shard,
            workerId: this._workerId,
            signal: this._abortController?.signal ?? new AbortController().signal,
        };
        let outcome;
        try {
            outcome = await this._handler(context);
        }
        catch (error) {
            // Handler threw an error, report as failure
            outcome = {
                type: "failure",
                code: "HANDLER_ERROR",
                data: (0, serialize_error_1.serializeError)(error),
            };
        }
        // Report the outcome to the correct shard
        await this._client.reportOutcome({
            taskId: task.id,
            shard,
            outcome,
            tenant: this._tenant,
        });
    }
    /**
     * Execute a refresh task and report its outcome.
     */
    async _executeRefreshTask(task, shard) {
        const context = {
            task,
            shard,
            workerId: this._workerId,
            signal: this._abortController?.signal ?? new AbortController().signal,
        };
        let outcome;
        try {
            // The refresh handler should return the new max concurrency
            const newMaxConcurrency = await this._refreshHandler(context);
            outcome = {
                type: "success",
                newMaxConcurrency,
            };
        }
        catch (error) {
            // Handler threw an error, report as failure
            const errorObj = error instanceof Error ? error : new Error(String(error));
            outcome = {
                type: "failure",
                code: "REFRESH_HANDLER_ERROR",
                message: errorObj.message,
            };
        }
        // Report the refresh outcome
        await this._client.reportRefreshOutcome({
            taskId: task.id,
            shard,
            outcome,
            tenant: this._tenant,
        });
    }
    /**
     * Send a heartbeat for a task.
     */
    async _sendHeartbeat(taskId, shard) {
        await this._client.heartbeat(this._workerId, taskId, shard, this._tenant);
    }
    /**
     * Sleep for the specified duration.
     */
    _sleep(ms) {
        return new Promise((resolve) => {
            const timeout = setTimeout(resolve, ms);
            // If we're stopping, resolve immediately
            this._abortController?.signal.addEventListener("abort", () => {
                clearTimeout(timeout);
                resolve();
            }, { once: true });
        });
    }
}
exports.SiloWorker = SiloWorker;
//# sourceMappingURL=worker.js.map