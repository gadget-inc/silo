import type { Task } from "./pb/silo";
import type { SiloGRPCClient, TaskOutcome } from "./client";
export { Task };
/**
 * Context passed to task handlers with utilities for the current task.
 */
export interface TaskContext {
    /** The task being executed */
    task: Task;
    /** The shard the task is from (for reference) */
    shard: number;
    /** The worker ID processing this task */
    workerId: string;
    /** Signal that is aborted when the worker is stopping */
    signal: AbortSignal;
}
/**
 * Function that handles a task and returns its outcome.
 */
export type TaskHandler = (context: TaskContext) => Promise<TaskOutcome>;
/**
 * Options for configuring a {@link SiloWorker}.
 */
export interface SiloWorkerOptions {
    /** The silo client to use for communication */
    client: SiloGRPCClient;
    /** Unique identifier for this worker */
    workerId: string;
    /** The function that will handle each task */
    handler: TaskHandler;
    /**
     * Tenant ID for this worker. Required when the server has tenancy enabled.
     * The worker will only process tasks for this tenant.
     */
    tenant?: string;
    /**
     * Number of concurrent poll calls to make.
     * More pollers can help ensure work is always available.
     * @default 1
     */
    concurrentPollers?: number;
    /**
     * Maximum number of tasks to execute simultaneously.
     * @default 10
     */
    maxConcurrentTasks?: number;
    /**
     * Number of tasks to request per poll call.
     * @default maxConcurrentTasks
     */
    tasksPerPoll?: number;
    /**
     * Interval in ms between poll attempts when no tasks are available.
     * @default 1000
     */
    pollIntervalMs?: number;
    /**
     * Interval in ms between heartbeats for running tasks.
     * Should be less than the server's lease timeout.
     * @default 5000
     */
    heartbeatIntervalMs?: number;
    /**
     * Called when an error occurs during polling or task execution.
     * If not provided, errors are logged to console.error.
     */
    onError?: (error: Error, context?: {
        taskId?: string;
    }) => void;
}
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
 * });
 *
 * worker.start();
 * // ... later
 * await worker.stop();
 * ```
 */
export declare class SiloWorker {
    private readonly _client;
    private readonly _workerId;
    private readonly _handler;
    private readonly _tenant;
    private readonly _concurrentPollers;
    private readonly _maxConcurrentTasks;
    private readonly _tasksPerPoll;
    private readonly _pollIntervalMs;
    private readonly _heartbeatIntervalMs;
    private readonly _onError;
    private _running;
    private _abortController;
    private _pollerPromises;
    private _taskQueue;
    private _heartbeatIntervals;
    constructor(options: SiloWorkerOptions);
    /**
     * Whether the worker is currently running.
     */
    get isRunning(): boolean;
    /**
     * The number of tasks currently being executed.
     */
    get activeTasks(): number;
    /**
     * The number of tasks waiting in the queue.
     */
    get queuedTasks(): number;
    /**
     * Start the worker. This will begin polling for tasks and executing them.
     * Returns immediately after starting the polling loops.
     */
    start(): void;
    /**
     * Stop the worker gracefully.
     * Waits for all currently executing tasks to complete.
     * @param timeoutMs Maximum time to wait for tasks to complete. Default: 30000ms.
     */
    stop(timeoutMs?: number): Promise<void>;
    /**
     * The main polling loop for a single poller.
     */
    private _pollLoop;
    /**
     * Poll for tasks and add them to the execution queue.
     */
    private _poll;
    /**
     * Add a task to the execution queue.
     */
    private _enqueueTask;
    /**
     * Execute a single task and report its outcome.
     */
    private _executeTask;
    /**
     * Send a heartbeat for a task.
     */
    private _sendHeartbeat;
    /**
     * Sleep for the specified duration.
     */
    private _sleep;
}
//# sourceMappingURL=worker.d.ts.map