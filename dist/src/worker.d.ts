import type { SiloGRPCClient, TaskOutcome, RefreshTask } from "./client";
import { Task } from "./TaskExecution";
/**
 * Context passed to task handlers with utilities for the current task.
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 */
export interface TaskContext<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>> {
    /** The task being executed, with decoded payload */
    task: Task<Payload, Metadata>;
    /**
     * Signal that is aborted when the task is cancelled.
     * This signal is triggered when:
     * - The server reports the job as cancelled (via heartbeat)
     * - The handler explicitly calls cancel()
     */
    cancellationSignal: AbortSignal;
    /**
     * Cancel this task. This will:
     * 1. Abort the task's signal immediately
     * 2. Notify the server that the job is cancelled
     *
     * The handler should stop work when the signal is aborted.
     * The worker will automatically report a Cancelled outcome.
     *
     * @example
     * ```typescript
     * const handler: TaskHandler = async (ctx) => {
     *   // Start processing...
     *   if (someConditionToCancel) {
     *     await ctx.cancel();
     *     // Signal is now aborted, handler should return
     *     return { type: "success", result: {} }; // This result is ignored
     *   }
     *   // ... continue processing
     * };
     * ```
     */
    cancel(): Promise<void>;
}
/**
 * Function that handles a task and returns its outcome.
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 */
export type TaskHandler<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>, Result = unknown> = (context: TaskContext<Payload, Metadata>) => Promise<TaskOutcome<Result>>;
/**
 * Context passed to refresh handlers for floating concurrency limit refreshes.
 */
export interface RefreshTaskContext {
    /** The refresh task being executed */
    task: RefreshTask;
    /** The shard ID (UUID) the task is from (for reference) */
    shard: string;
    /** The worker ID processing this task */
    workerId: string;
    /** Signal that is aborted when the worker is stopping */
    signal: AbortSignal;
}
/**
 * Function that handles a floating limit refresh task.
 *
 * The handler receives context about the floating limit that needs refreshing,
 * including the current max concurrency, when it was last refreshed, and any
 * metadata from the limit definition.
 *
 * The handler should compute and return the new max concurrency value.
 * If the handler throws an error, it will be reported as a refresh failure
 * and the server will schedule a retry with exponential backoff.
 *
 * @example
 * ```typescript
 * const refreshHandler: RefreshHandler = async (ctx) => {
 *   // Use metadata to determine which API to query
 *   const orgId = ctx.task.metadata.orgId;
 *   const quota = await getApiQuota(orgId);
 *   return quota.maxConcurrentRequests;
 * };
 * ```
 */
export type RefreshHandler = (context: RefreshTaskContext) => Promise<number>;
/**
 * Options for configuring a {@link SiloWorker}.
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 */
export interface SiloWorkerOptions<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>, Result = unknown> {
    /** The silo client to use for communication */
    client: SiloGRPCClient;
    /** Unique identifier for this worker */
    workerId: string;
    /**
     * Task group to poll for tasks.
     * Workers can only receive tasks from the specified task group.
     * Required.
     */
    taskGroup: string;
    /** The function that will handle each task */
    handler: TaskHandler<Payload, Metadata, Result>;
    /**
     * Optional handler for floating limit refresh tasks.
     *
     * If your jobs use floating concurrency limits, you should provide this handler
     * to compute updated max concurrency values. The handler receives context about
     * the limit and should return the new max concurrency value.
     *
     * If not provided, refresh tasks will be ignored (and will eventually expire,
     * causing the server to retry with exponential backoff).
     *
     * @example
     * ```typescript
     * refreshHandler: async (ctx) => {
     *   const orgId = ctx.task.metadata.orgId;
     *   const quota = await getApiQuota(orgId);
     *   return quota.maxConcurrentRequests;
     * }
     * ```
     */
    refreshHandler?: RefreshHandler;
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
export declare class SiloWorker<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>, Result = unknown> {
    private readonly _client;
    private readonly _workerId;
    private readonly _taskGroup;
    private readonly _handler;
    private readonly _refreshHandler;
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
    /** Active task executions, keyed by task ID */
    private _activeExecutions;
    /** Heartbeat intervals for active tasks (regular tasks and refresh tasks) */
    private _heartbeatIntervals;
    /** Counter for per-worker round-robin server selection */
    private _pollCounter;
    constructor(options: SiloWorkerOptions<Payload, Metadata, Result>);
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
     * Add a refresh task to the execution queue.
     */
    private _enqueueRefreshTask;
    /**
     * Execute a single task with its TaskExecution and report its outcome.
     */
    private _executeTaskWithExecution;
    /**
     * Execute a refresh task and report its outcome.
     */
    private _executeRefreshTask;
    /**
     * Send a heartbeat for a task execution and handle cancellation if detected.
     */
    private _sendHeartbeatForTask;
    /**
     * Send a heartbeat for a task (for refresh tasks that don't need TaskExecution).
     */
    private _sendHeartbeat;
    /**
     * Sleep for the specified duration.
     */
    private _sleep;
}
//# sourceMappingURL=worker.d.ts.map