import PQueue from "p-queue";
import { serializeError } from "serialize-error";
import type { Task as ProtoTask } from "./pb/silo";
import type {
  SiloGRPCClient,
  TaskOutcome,
  RefreshTask,
  RefreshOutcome,
  HeartbeatResult,
} from "./client";
import { Task, TaskExecution, transformTask } from "./TaskExecution";

/**
 * Context passed to task handlers with utilities for the current task.
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 */
export interface TaskContext<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
> {
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
export type TaskHandler<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
  Result = unknown,
> = (context: TaskContext<Payload, Metadata>) => Promise<TaskOutcome<Result>>;

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
export interface SiloWorkerOptions<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
  Result = unknown,
> {
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
  onError?: (error: Error, context?: { taskId?: string }) => void;
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
export class SiloWorker<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
  Result = unknown,
> {
  private readonly _client: SiloGRPCClient;
  private readonly _workerId: string;
  private readonly _taskGroup: string;
  private readonly _handler: TaskHandler<Payload, Metadata, Result>;
  private readonly _refreshHandler: RefreshHandler | undefined;
  private readonly _concurrentPollers: number;
  private readonly _maxConcurrentTasks: number;
  private readonly _tasksPerPoll: number;
  private readonly _pollIntervalMs: number;
  private readonly _heartbeatIntervalMs: number;
  private readonly _onError: (
    error: Error,
    context?: { taskId?: string },
  ) => void;

  private _running = false;
  private _abortController: AbortController | null = null;
  private _pollerPromises: Promise<void>[] = [];
  private _taskQueue: PQueue;
  /** Active task executions, keyed by task ID */
  private _activeExecutions: Map<string, TaskExecution> = new Map();
  /** Heartbeat intervals for active tasks (regular tasks and refresh tasks) */
  private _heartbeatIntervals: Map<string, ReturnType<typeof setInterval>> =
    new Map();
  /** Counter for per-worker round-robin server selection */
  private _pollCounter: number = 0;

  public constructor(options: SiloWorkerOptions<Payload, Metadata, Result>) {
    this._client = options.client;
    this._workerId = options.workerId;
    this._taskGroup = options.taskGroup;
    this._handler = options.handler;
    this._refreshHandler = options.refreshHandler;
    this._concurrentPollers = options.concurrentPollers ?? 1;
    this._maxConcurrentTasks = options.maxConcurrentTasks ?? 10;
    this._tasksPerPoll =
      options.tasksPerPoll ?? Math.ceil(this._maxConcurrentTasks / 2);
    this._pollIntervalMs = options.pollIntervalMs ?? 1000;
    this._heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;
    this._onError =
      options.onError ??
      ((error, ctx) => {
        console.error(
          `[SiloWorker] Error${ctx?.taskId ? ` (task ${ctx.taskId})` : ""}:`,
          error,
        );
      });

    // Initialize the task queue with concurrency limit
    this._taskQueue = new PQueue({ concurrency: this._maxConcurrentTasks });
  }

  /**
   * Whether the worker is currently running.
   */
  public get isRunning(): boolean {
    return this._running;
  }

  /**
   * The number of tasks currently being executed.
   */
  public get activeTasks(): number {
    return this._taskQueue.pending;
  }

  /**
   * The number of tasks waiting in the queue.
   */
  public get queuedTasks(): number {
    return this._taskQueue.size;
  }

  /**
   * Start the worker. This will begin polling for tasks and executing them.
   * Returns immediately after starting the polling loops.
   */
  public start(): void {
    if (this._running) {
      return;
    }

    this._running = true;
    this._abortController = new AbortController();

    // Reset the queue in case we're restarting
    this._taskQueue = new PQueue({ concurrency: this._maxConcurrentTasks });

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
  public async stop(timeoutMs = 30000): Promise<void> {
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
    const timeoutPromise = new Promise<void>((resolve) =>
      setTimeout(resolve, timeoutMs),
    );
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
  private async _pollLoop(): Promise<void> {
    while (this._running) {
      try {
        await this._poll();
      } catch (error) {
        if (this._running) {
          this._onError(
            error instanceof Error ? error : new Error(String(error)),
          );
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
  private async _poll(): Promise<void> {
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
    const result = await this._client.leaseTasks(
      {
        workerId: this._workerId,
        maxTasks: tasksToRequest,
        taskGroup: this._taskGroup,
      },
      serverIndex,
    );

    // Add regular job tasks to the queue
    for (const task of result.tasks) {
      this._enqueueTask(task);
    }

    // Handle refresh tasks
    if (result.refreshTasks.length > 0) {
      if (!this._refreshHandler) {
        throw new Error(
          `Worker received ${result.refreshTasks.length} floating limit refresh task(s) but no refreshHandler is configured. ` +
            `Configure a refreshHandler in SiloWorkerOptions to handle floating concurrency limits.`,
        );
      }
      for (const refreshTask of result.refreshTasks) {
        this._enqueueRefreshTask(refreshTask);
      }
    }
  }

  /**
   * Add a task to the execution queue.
   */
  private _enqueueTask(protoTask: ProtoTask): void {
    const task = transformTask<Payload, Metadata>(protoTask);

    // Create TaskExecution to manage this task's state
    const execution = new TaskExecution(task, this._workerId, this._client);
    this._activeExecutions.set(task.id, execution);

    // Start heartbeat for this task immediately
    const heartbeatInterval = setInterval(() => {
      this._sendHeartbeatForTask(execution).catch((error) => {
        this._onError(
          error instanceof Error ? error : new Error(String(error)),
          {
            taskId: task.id,
          },
        );
      });
    }, this._heartbeatIntervalMs);
    this._heartbeatIntervals.set(task.id, heartbeatInterval);

    // Add task to the queue for execution
    this._taskQueue
      .add(async () => {
        await this._executeTaskWithExecution(execution);
      })
      .catch((error) => {
        this._onError(
          error instanceof Error ? error : new Error(String(error)),
          {
            taskId: task.id,
          },
        );
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
      });
  }

  /**
   * Add a refresh task to the execution queue.
   */
  private _enqueueRefreshTask(task: RefreshTask): void {
    const shard = task.shard;

    // Start heartbeat for this refresh task (no TaskExecution needed, refresh tasks can't be cancelled)
    const heartbeatInterval = setInterval(() => {
      this._sendHeartbeat(task.id, shard).catch((error) => {
        this._onError(
          error instanceof Error ? error : new Error(String(error)),
          {
            taskId: task.id,
          },
        );
      });
    }, this._heartbeatIntervalMs);
    this._heartbeatIntervals.set(task.id, heartbeatInterval);

    // Add to the queue for execution
    this._taskQueue
      .add(async () => {
        await this._executeRefreshTask(task, shard);
      })
      .catch((error) => {
        this._onError(
          error instanceof Error ? error : new Error(String(error)),
          {
            taskId: task.id,
          },
        );
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
  private async _executeTaskWithExecution(
    execution: TaskExecution<Payload, Metadata>,
  ): Promise<void> {
    const context: TaskContext<Payload, Metadata> = {
      task: execution.task,
      cancellationSignal: execution.signal,
      cancel: () => execution.cancelFromClient(),
    };

    let outcome: TaskOutcome<Result>;
    try {
      outcome = await this._handler(context);
    } catch (error) {
      // Handler threw an error, report as failure
      outcome = {
        type: "failure",
        code: "HANDLER_ERROR",
        data: serializeError(error),
      };
    }

    // If the task was cancelled (by server or client), report Cancelled outcome instead
    if (execution.shouldReportCancelled) {
      await this._client.reportOutcome({
        taskId: execution.task.id,
        shard: execution.task.shard,
        outcome: { type: "cancelled" },
      });
      return;
    }

    // Report the outcome to the correct shard
    await this._client.reportOutcome({
      taskId: execution.task.id,
      shard: execution.task.shard,
      outcome,
    });
  }

  /**
   * Execute a refresh task and report its outcome.
   */
  private async _executeRefreshTask(
    task: RefreshTask,
    shard: string,
  ): Promise<void> {
    const context: RefreshTaskContext = {
      task,
      shard,
      workerId: this._workerId,
      signal: this._abortController?.signal ?? new AbortController().signal,
    };

    let outcome: RefreshOutcome;
    try {
      // The refresh handler should return the new max concurrency
      const newMaxConcurrency = await this._refreshHandler!(context);
      outcome = {
        type: "success",
        newMaxConcurrency,
      };
    } catch (error) {
      // Handler threw an error, report as failure
      const errorObj =
        error instanceof Error ? error : new Error(String(error));
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
    });
  }

  /**
   * Send a heartbeat for a task execution and handle cancellation if detected.
   */
  private async _sendHeartbeatForTask(execution: TaskExecution): Promise<void> {
    // Don't send heartbeats for already-cancelled tasks
    if (execution.isCancelled) {
      return;
    }

    const result: HeartbeatResult = await this._client.heartbeat(
      this._workerId,
      execution.task.id,
      execution.task.shard,
    );

    // If the server reports cancellation, mark the execution as cancelled
    if (result.cancelled) {
      execution.markCancelledByServer();
    }
  }

  /**
   * Send a heartbeat for a task (for refresh tasks that don't need TaskExecution).
   */
  private async _sendHeartbeat(taskId: string, shard: string): Promise<void> {
    await this._client.heartbeat(this._workerId, taskId, shard);
  }

  /**
   * Sleep for the specified duration.
   */
  private _sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timeout = setTimeout(resolve, ms);
      // If we're stopping, resolve immediately
      this._abortController?.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timeout);
          resolve();
        },
        { once: true },
      );
    });
  }
}
