import type { Task, SiloGRPCClient } from "./client";
import { combineAbortSignals } from "./utils";

/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
export class TaskExecution {
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
    workerAbortSignal: AbortSignal,
    client: SiloGRPCClient
  ) {
    this.task = task;
    this.shard = shard;
    this.workerId = workerId;
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
 * Reason why a task was cancelled.
 */

export type CancellationReason = "server" | "client" | "worker_shutdown";

