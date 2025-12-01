import PQueue from "p-queue";
import { serializeError } from "serialize-error";
import type { Task } from "./pb/silo";
import type { SiloGrpcClient, TaskOutcome } from "./client";

export { Task };

/**
 * Context passed to task handlers with utilities for the current task.
 */
export interface TaskContext {
  /** The task being executed */
  task: Task;
  /** The shard the task is from */
  shard: string;
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
  client: SiloGrpcClient;
  /** The shard to poll for tasks */
  shard: string;
  /** Unique identifier for this worker */
  workerId: string;
  /** The function that will handle each task */
  handler: TaskHandler;
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
   * Optional tenant ID when tenancy is enabled.
   */
  tenant?: string;
  /**
   * Called when an error occurs during polling or task execution.
   * If not provided, errors are logged to console.error.
   */
  onError?: (error: Error, context?: { taskId?: string }) => void;
}

/**
 * A worker that continuously polls for tasks and executes them.
 *
 * @example
 * ```typescript
 * const worker = new SiloWorker({
 *   client,
 *   shard: "0",
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
 * await worker.start();
 * // ... later
 * await worker.stop();
 * ```
 */
export class SiloWorker {
  private readonly _client: SiloGrpcClient;
  private readonly _shard: string;
  private readonly _workerId: string;
  private readonly _handler: TaskHandler;
  private readonly _concurrentPollers: number;
  private readonly _maxConcurrentTasks: number;
  private readonly _tasksPerPoll: number;
  private readonly _pollIntervalMs: number;
  private readonly _heartbeatIntervalMs: number;
  private readonly _tenant?: string;
  private readonly _onError: (error: Error, context?: { taskId?: string }) => void;

  private _running = false;
  private _abortController: AbortController | null = null;
  private _pollerPromises: Promise<void>[] = [];
  private _taskQueue: PQueue;
  private _heartbeatIntervals: Map<string, ReturnType<typeof setInterval>> = new Map();

  public constructor(options: SiloWorkerOptions) {
    this._client = options.client;
    this._shard = options.shard;
    this._workerId = options.workerId;
    this._handler = options.handler;
    this._concurrentPollers = options.concurrentPollers ?? 1;
    this._maxConcurrentTasks = options.maxConcurrentTasks ?? 10;
    this._tasksPerPoll = options.tasksPerPoll ?? Math.ceil(this._maxConcurrentTasks / 2);
    this._pollIntervalMs = options.pollIntervalMs ?? 1000;
    this._heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;
    this._tenant = options.tenant;
    this._onError =
      options.onError ??
      ((error, ctx) => {
        console.error(`[SiloWorker] Error${ctx?.taskId ? ` (task ${ctx.taskId})` : ""}:`, error);
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
      this._pollerPromises.push(this._pollLoop(i));
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
    const timeoutPromise = new Promise<void>((resolve) => setTimeout(resolve, timeoutMs));
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
  private async _pollLoop(_pollerId: number): Promise<void> {
    while (this._running) {
      try {
        await this._poll();
      } catch (error) {
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
  private async _poll(): Promise<void> {
    // Wait if the queue is too full
    // We allow queueing up to 1 task for every 1 task currently running
    // So total capacity = maxConcurrentTasks (running) + maxConcurrentTasks (queued)
    while (this._running && this._taskQueue.size >= this._maxConcurrentTasks) {
      await this._sleep(this._pollIntervalMs / 2);
    }

    if (!this._running) {
      return;
    }

    // Calculate how many tasks we can accept
    // Available queue slots = max queue depth - current queue size
    const availableQueueSlots = this._maxConcurrentTasks - this._taskQueue.size;
    if (availableQueueSlots <= 0) {
      return;
    }

    const tasksToRequest = Math.min(availableQueueSlots, this._tasksPerPoll);

    const tasks = await this._client.leaseTasks({
      shard: this._shard,
      workerId: this._workerId,
      maxTasks: tasksToRequest,
      tenant: this._tenant,
    });

    // Add each task to the queue
    for (const task of tasks) {
      this._enqueueTask(task);
    }
  }

  /**
   * Add a task to the execution queue.
   */
  private _enqueueTask(task: Task): void {
    // Start heartbeat for this task immediately
    const heartbeatInterval = setInterval(() => {
      this._sendHeartbeat(task.id).catch((error) => {
        this._onError(error instanceof Error ? error : new Error(String(error)), {
          taskId: task.id,
        });
      });
    }, this._heartbeatIntervalMs);
    this._heartbeatIntervals.set(task.id, heartbeatInterval);

    // Add task to the queue for execution
    this._taskQueue
      .add(async () => {
        await this._executeTask(task);
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
   * Execute a single task and report its outcome.
   */
  private async _executeTask(task: Task): Promise<void> {
    const context: TaskContext = {
      task,
      shard: this._shard,
      workerId: this._workerId,
      signal: this._abortController?.signal ?? new AbortController().signal,
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

    // Report the outcome
    await this._client.reportOutcome({
      shard: this._shard,
      taskId: task.id,
      tenant: this._tenant,
      outcome,
    });
  }

  /**
   * Send a heartbeat for a task.
   */
  private async _sendHeartbeat(taskId: string): Promise<void> {
    await this._client.heartbeat(this._shard, this._workerId, taskId);
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
