import type { Task as ProtoTask } from "./pb/silo";
import { decodeBytes, type SiloGRPCClient } from "./client";
import { combineAbortSignals } from "./utils";

/**
 * Reason why a task was cancelled.
 */
export type CancellationReason = "server" | "client" | "worker_shutdown";

/**
 * A task received from Silo, with the payload decoded.
 *
 * This is a userland type that wraps the raw protobuf Task, providing:
 * - A required, decoded `payload` field (generic over T)
 * - All other task metadata fields
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 */

export interface Task<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>> {
  /** Unique task ID (different from job ID) */
  id: string;
  /** ID of the job this task belongs to */
  jobId: string;
  /** Which attempt this is (1 = first attempt) */
  attemptNumber: number;
  /** How long the lease lasts in milliseconds. Heartbeat before this expires. */
  leaseMs: bigint;
  /** The decoded job payload */
  payload: Payload;
  /** Job priority (for informational purposes) */
  priority: number;
  /** Shard this task came from (needed for reporting outcome) */
  shard: number;
  /** Task group this task belongs to */
  taskGroup: string;
  /** Tenant ID if multi-tenancy is enabled */
  tenantId?: string;
  /** True if this is the final attempt (no more retries after this) */
  isLastAttempt: boolean;
  /** Metadata key/value pairs from the job */
  metadata: Metadata;
}
/**
 * Transform a raw protobuf Task into a userland Task with decoded payload.
 */
export function transformTask<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>>(protoTask: ProtoTask): Task<Payload, Metadata> {
  return {
    id: protoTask.id,
    jobId: protoTask.jobId,
    attemptNumber: protoTask.attemptNumber,
    leaseMs: protoTask.leaseMs,
    payload: decodeBytes<Payload>(protoTask.payload?.data, "payload"),
    priority: protoTask.priority,
    shard: protoTask.shard,
    taskGroup: protoTask.taskGroup,
    tenantId: protoTask.tenantId,
    isLastAttempt: protoTask.isLastAttempt,
    metadata: protoTask.metadata as Metadata,
  };
}

/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
export class TaskExecution<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>> {
  /** The task being executed (raw proto format) */
  public readonly task: Task<Payload, Metadata>;
  /** The worker ID */
  public readonly workerId: string;

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
    task: Task<Payload, Metadata>,
    workerId: string,
    workerAbortSignal: AbortSignal,
    client: SiloGRPCClient
  ) {
    this.task = task;
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
      .cancelJob(this.task.jobId, this.task.tenantId)
      .catch(() => {
        // Ignore errors - the job may already be cancelled or completed
      });

    await this._cancelPromise;
  }
}



