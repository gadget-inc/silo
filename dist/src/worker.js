"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SiloWorker = void 0;
const api_1 = require("@opentelemetry/api");
const p_queue_1 = __importDefault(require("p-queue"));
const serialize_error_1 = require("serialize-error");
const TaskExecution_1 = require("./TaskExecution");
const metrics_1 = require("./metrics");
/**
 * A worker that continuously polls for tasks and executes them.
 *
 * The worker polls the server for tasks from all shards the server owns.
 * Each task includes a `shard` field that is automatically used for
 * heartbeats and reporting outcomes.
 *
 * The payload is automatically decoded from MessagePack bytes. You can
 * specify the payload type as a generic parameter for type safety.
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 *
 * @example
 * ```typescript
 * interface MyPayload {
 *   userId: string;
 *   action: string;
 * }
 *
 * const worker = new SiloWorker<MyPayload>({
 *   client,
 *   workerId: "worker-1",
 *   concurrentPollers: 2,
 *   maxConcurrentTasks: 10,
 *   handler: async (ctx) => {
 *     // ctx.task.payload is typed as MyPayload
 *     console.log(ctx.task.payload.userId);
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
    _taskGroup;
    _handler;
    _refreshHandler;
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
    /** Active task executions, keyed by task ID */
    _activeExecutions = new Map();
    /** Heartbeat intervals for active tasks (regular tasks and refresh tasks) */
    _heartbeatIntervals = new Map();
    /** Counter for per-worker round-robin server selection */
    _pollCounter = 0;
    /** OpenTelemetry metrics */
    _metrics;
    /** OpenTelemetry tracer for per-task lifecycle spans */
    _tracer;
    constructor(options) {
        this._client = options.client;
        this._workerId = options.workerId;
        this._taskGroup = options.taskGroup;
        this._handler = options.handler;
        this._refreshHandler = options.refreshHandler;
        this._concurrentPollers = options.concurrentPollers ?? 1;
        this._maxConcurrentTasks = options.maxConcurrentTasks ?? 10;
        this._tasksPerPoll = options.tasksPerPoll ?? Math.ceil(this._maxConcurrentTasks / 2);
        this._pollIntervalMs = options.pollIntervalMs ?? 1000;
        this._heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;
        this._onError =
            options.onError ??
                ((error, ctx) => {
                    console.error(`[SiloWorker] Error${ctx?.taskId ? ` (task ${ctx.taskId})` : ""}:`, error);
                });
        // Initialize the task queue with concurrency limit
        this._taskQueue = new p_queue_1.default({ concurrency: this._maxConcurrentTasks });
        // Initialize metrics
        const meter = (0, metrics_1.getWorkerMeter)(options.meter);
        this._metrics = new metrics_1.WorkerMetrics(meter, this._taskGroup, () => this.availableTaskSlots);
        // Tracer for per-task lifecycle spans. Uses the global TracerProvider; when
        // none is configured this resolves to a noop tracer and all the span calls
        // below are zero-cost.
        this._tracer = api_1.trace.getTracer("silo-worker");
    }
    /**
     * The unique identifier for this worker.
     */
    get workerId() {
        return this._workerId;
    }
    /**
     * The task group this worker polls for tasks from.
     */
    get taskGroup() {
        return this._taskGroup;
    }
    /**
     * The number of concurrent poll calls configured.
     */
    get concurrentPollers() {
        return this._concurrentPollers;
    }
    /**
     * The maximum number of tasks that can execute simultaneously.
     */
    get maxConcurrentTasks() {
        return this._maxConcurrentTasks;
    }
    /**
     * The number of tasks requested per poll call.
     */
    get tasksPerPoll() {
        return this._tasksPerPoll;
    }
    /**
     * The interval in ms between poll attempts.
     */
    get pollIntervalMs() {
        return this._pollIntervalMs;
    }
    /**
     * The interval in ms between heartbeats for running tasks.
     */
    get heartbeatIntervalMs() {
        return this._heartbeatIntervalMs;
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
     * The number of task slots currently available (not active or queued).
     */
    get availableTaskSlots() {
        return this._maxConcurrentTasks - this._taskQueue.pending - this._taskQueue.size;
    }
    /**
     * The OpenTelemetry metrics for this worker.
     */
    get workerMetrics() {
        return this._metrics;
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
        // Clear active executions
        this._activeExecutions.clear();
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
        // Use per-worker round-robin counter to cycle through all servers,
        // ensuring each worker independently polls all servers in order.
        const serverIndex = this._pollCounter++;
        const result = await this._client.leaseTasks({
            workerId: this._workerId,
            maxTasks: tasksToRequest,
            taskGroup: this._taskGroup,
        }, serverIndex);
        // Record poll metrics
        this._metrics.pollCounter.add(1, this._metrics.defaultAttributes);
        const totalTasksReturned = result.tasks.length + result.refreshTasks.length;
        if (totalTasksReturned === 0) {
            this._metrics.emptyPollCounter.add(1, this._metrics.defaultAttributes);
        }
        else {
            this._metrics.pollTasksReturnedCounter.add(totalTasksReturned, this._metrics.defaultAttributes);
        }
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
    _enqueueTask(protoTask) {
        const task = (0, TaskExecution_1.transformTask)(protoTask);
        // Open a per-task lifecycle span that covers heartbeats, the user
        // handler, and the eventual reportOutcome. LeaseTasks intentionally sits
        // outside this span — a single batch lease can produce N tasks and they
        // shouldn't share a span. The gRPC client interceptor reads
        // otelContext.active() on each outbound RPC, so any silo call made while
        // taskContext is active will carry traceparent for this span.
        const span = this._tracer.startSpan("silo.task.lifecycle", {
            attributes: {
                "silo.task_id": task.id,
                "silo.shard": task.shard,
                "silo.tenant": task.tenantId ?? "",
                "silo.worker_id": this._workerId,
                "silo.job_id": task.jobId,
                "silo.task_group": this._taskGroup,
                "silo.attempt_number": task.attemptNumber,
            },
        });
        const taskContext = api_1.trace.setSpan(api_1.context.active(), span);
        // Create TaskExecution to manage this task's state
        const execution = new TaskExecution_1.TaskExecution(task, this._workerId, this._client);
        this._activeExecutions.set(task.id, execution);
        // Start heartbeat for this task immediately. setInterval callbacks fire at
        // the top of the event loop and don't inherit any context that was active
        // when setInterval was called, so we explicitly bind the heartbeat fn to
        // taskContext — without this every Heartbeat RPC would be its own root
        // trace instead of a child of the lifecycle span.
        const heartbeatFn = api_1.context.bind(taskContext, () => {
            this._sendHeartbeatForTask(execution).catch((error) => {
                this._onError(error instanceof Error ? error : new Error(String(error)), {
                    taskId: task.id,
                });
            });
        });
        const heartbeatInterval = setInterval(heartbeatFn, this._heartbeatIntervalMs);
        this._heartbeatIntervals.set(task.id, heartbeatInterval);
        // Add task to the queue for execution. Bind the queued callback so the
        // await chain inside (handler invocation + reportOutcome) inherits
        // taskContext even though p-queue defers execution to a future microtask.
        const queuedFn = api_1.context.bind(taskContext, async () => {
            await this._executeTaskWithExecution(execution);
        });
        this._taskQueue
            .add(queuedFn)
            .catch((error) => {
            const e = error instanceof Error ? error : new Error(String(error));
            span.recordException(e);
            span.setStatus({ code: api_1.SpanStatusCode.ERROR, message: e.message });
            this._onError(e, { taskId: task.id });
        })
            .finally(() => {
            // Stop heartbeat for this task
            const interval = this._heartbeatIntervals.get(task.id);
            if (interval) {
                clearInterval(interval);
                this._heartbeatIntervals.delete(task.id);
            }
            // Remove from active executions
            this._activeExecutions.delete(task.id);
            span.end();
        });
    }
    /**
     * Add a refresh task to the execution queue.
     */
    _enqueueRefreshTask(task) {
        const shard = task.shard;
        // Start heartbeat for this refresh task (no TaskExecution needed, refresh tasks can't be cancelled)
        const heartbeatInterval = setInterval(() => {
            this._sendHeartbeat(task.id, shard, task.tenant).catch((error) => {
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
     * Execute a single task with its TaskExecution and report its outcome.
     */
    async _executeTaskWithExecution(execution) {
        const context = {
            task: execution.task,
            cancellationSignal: execution.signal,
            cancel: () => execution.cancelFromClient(),
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
        // If the task was cancelled (by server or client), report Cancelled outcome instead
        if (execution.shouldReportCancelled) {
            await this._client.reportOutcome({
                taskId: execution.task.id,
                shard: execution.task.shard,
                tenant: execution.task.tenantId,
                outcome: { type: "cancelled" },
            });
            return;
        }
        // Report the outcome to the correct shard
        await this._client.reportOutcome({
            taskId: execution.task.id,
            shard: execution.task.shard,
            tenant: execution.task.tenantId,
            outcome,
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
            tenant: task.tenant,
            outcome,
        });
    }
    /**
     * Send a heartbeat for a task execution and handle cancellation if detected.
     */
    async _sendHeartbeatForTask(execution) {
        // Don't send heartbeats for already-cancelled tasks
        if (execution.isCancelled) {
            return;
        }
        const result = await this._client.heartbeat(this._workerId, execution.task.id, execution.task.shard, execution.task.tenantId);
        // If the server reports cancellation, mark the execution as cancelled
        if (result.cancelled) {
            execution.markCancelledByServer();
        }
    }
    /**
     * Send a heartbeat for a task (for refresh tasks that don't need TaskExecution).
     */
    async _sendHeartbeat(taskId, shard, tenant) {
        await this._client.heartbeat(this._workerId, taskId, shard, tenant);
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