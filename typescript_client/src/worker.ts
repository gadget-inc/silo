import PQueue from "p-queue";
import { serializeError } from "serialize-error";
import type { Task } from "./pb/silo";
import type {
  SiloGRPCClient,
  TaskOutcome,
  RefreshTask,
  RefreshOutcome,
  HeartbeatResult,
} from "./client";

export { Task };

/**
 * Reason why a task was cancelled.
 */
export type CancellationReason = "server" | "client" | "worker_shutdown";

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
  /**
   * Signal that is aborted when the task should stop processing.
   * This signal is triggered when:
   * - The worker is stopping (graceful shutdown)
   * - The server reports the job as cancelled (via heartbeat)
   * - The handler explicitly calls cancel()
   */
  signal: AbortSignal;
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
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
class TaskExecution {
  /** The task being executed */
  public readonly task: Task;
  /** The shard this task came from */
  public readonly shard: number;
  /** The worker ID */
  public readonly workerId: string;
  /** Tenant for this task */
  public readonly tenant: string | undefined;

  /** Abort controller for this specific task */
  private readonly _taskAbortController: AbortController;
  /** Combined signal that aborts on task cancel OR worker shutdown */
  private readonly _combinedSignal: AbortSignal;
  /** Whether the task has been cancelled (by server or client) */
  private _cancelled: boolean = false;
  /** The reason for cancellation if cancelled */
  private _cancellationReason: CancellationReason | undefined;
  /** Promise resolving when cancel RPC completes (if initiated by client) */
  private _cancelPromise: Promise<void> | undefined;
  /** Reference to the client for cancel RPC */
  private readonly _client: SiloGRPCClient;

  constructor(
    task: Task,
    shard: number,
    workerId: string,
    tenant: string | undefined,
    workerAbortSignal: AbortSignal,
    client: SiloGRPCClient
  ) {
    this.task = task;
    this.shard = shard;
    this.workerId = workerId;
    this.tenant = tenant;
    this._client = client;

    this._taskAbortController = new AbortController();

    // Create a combined signal that aborts when EITHER the task or worker is aborted
    this._combinedSignal = combineAbortSignals(
      workerAbortSignal,
      this._taskAbortController.signal
    );

    // Track when worker shutdown causes abort
    workerAbortSignal.addEventListener(
      "abort",
      () => {
        if (!this._cancelled) {
          this._cancelled = true;
          this._cancellationReason = "worker_shutdown";
        }
      },
      { once: true }
    );
  }

  /** The combined abort signal for this task */
  get signal(): AbortSignal {
    return this._combinedSignal;
  }

  /** Whether this task has been cancelled */
  get isCancelled(): boolean {
    return this._cancelled;
  }

  /** The reason for cancellation, if cancelled */
  get cancellationReason(): CancellationReason | undefined {
    return this._cancellationReason;
  }

  /** Whether the cancellation was due to server-side cancel (should report Cancelled outcome) */
  get shouldReportCancelled(): boolean {
    return (
      this._cancelled &&
      (this._cancellationReason === "server" ||
        this._cancellationReason === "client")
    );
  }

  /**
   * Called when heartbeat detects server-side cancellation.
   * Aborts the task signal immediately.
   */
  public markCancelledByServer(): void {
    if (this._cancelled) return;
    this._cancelled = true;
    this._cancellationReason = "server";
    this._taskAbortController.abort();
  }

  /**
   * Cancel this task from the client side.
   * Calls the server to cancel the job and aborts the task signal.
   */
  public async cancelFromClient(): Promise<void> {
    if (this._cancelled) return;

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

/**
 * Combine multiple abort signals into one that aborts when ANY input signal aborts.
 * @internal
 */
function combineAbortSignals(...signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();

  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort();
      return controller.signal;
    }

    signal.addEventListener(
      "abort",
      () => {
        controller.abort();
      },
      { once: true }
    );
  }

  return controller.signal;
}

/**
 * Function that handles a task and returns its outcome.
 */
export type TaskHandler = (context: TaskContext) => Promise<TaskOutcome>;

/**
 * Context passed to refresh handlers for floating concurrency limit refreshes.
 */
export interface RefreshTaskContext {
  /** The refresh task being executed */
  task: RefreshTask;
  /** The shard the task is from (for reference) */
  shard: number;
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
 */
export interface SiloWorkerOptions {
  /** The silo client to use for communication */
  client: SiloGRPCClient;
  /** Unique identifier for this worker */
  workerId: string;
  /** The function that will handle each task */
  handler: TaskHandler;
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
export class SiloWorker {
  private readonly _client: SiloGRPCClient;
  private readonly _workerId: string;
  private readonly _handler: TaskHandler;
  private readonly _refreshHandler: RefreshHandler | undefined;
  private readonly _tenant: string | undefined;
  private readonly _concurrentPollers: number;
  private readonly _maxConcurrentTasks: number;
  private readonly _tasksPerPoll: number;
  private readonly _pollIntervalMs: number;
  private readonly _heartbeatIntervalMs: number;
  private readonly _onError: (
    error: Error,
    context?: { taskId?: string }
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

  public constructor(options: SiloWorkerOptions) {
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
        console.error(
          `[SiloWorker] Error${ctx?.taskId ? ` (task ${ctx.taskId})` : ""}:`,
          error
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
      setTimeout(resolve, timeoutMs)
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
            error instanceof Error ? error : new Error(String(error))
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
        throw new Error(
          `Worker received ${result.refreshTasks.length} floating limit refresh task(s) but no refreshHandler is configured. ` +
            `Configure a refreshHandler in SiloWorkerOptions to handle floating concurrency limits.`
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
  private _enqueueTask(task: Task): void {
    // Shard is now a number from the proto
    const shard = task.shard;

    // Create TaskExecution to manage this task's state
    const execution = new TaskExecution(
      task,
      shard,
      this._workerId,
      this._tenant,
      this._abortController?.signal ?? new AbortController().signal,
      this._client
    );
    this._activeExecutions.set(task.id, execution);

    // Start heartbeat for this task immediately
    const heartbeatInterval = setInterval(() => {
      this._sendHeartbeatForTask(execution).catch((error) => {
        this._onError(
          error instanceof Error ? error : new Error(String(error)),
          {
            taskId: task.id,
          }
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
          }
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
          }
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
          }
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
    execution: TaskExecution
  ): Promise<void> {
    const context: TaskContext = {
      task: execution.task,
      shard: execution.shard,
      workerId: execution.workerId,
      signal: execution.signal,
      cancel: () => execution.cancelFromClient(),
    };

    let outcome: TaskOutcome;
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
        shard: execution.shard,
        outcome: { type: "cancelled" },
        tenant: this._tenant,
      });
      return;
    }

    // Report the outcome to the correct shard
    await this._client.reportOutcome({
      taskId: execution.task.id,
      shard: execution.shard,
      outcome,
      tenant: this._tenant,
    });
  }

  /**
   * Execute a refresh task and report its outcome.
   */
  private async _executeRefreshTask(
    task: RefreshTask,
    shard: number
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
      tenant: this._tenant,
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
      execution.shard,
      this._tenant
    );

    // If the server reports cancellation, mark the execution as cancelled
    if (result.cancelled) {
      execution.markCancelledByServer();
    }
  }

  /**
   * Send a heartbeat for a task (for refresh tasks that don't need TaskExecution).
   */
  private async _sendHeartbeat(taskId: string, shard: number): Promise<void> {
    await this._client.heartbeat(this._workerId, taskId, shard, this._tenant);
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
        { once: true }
      );
    });
  }
}
