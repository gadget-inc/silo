import type { Task as ProtoTask, Limit } from "./pb/silo";
import { decodeBytes, type SiloGRPCClient } from "./client";

export type { Limit } from "./pb/silo";
export type {
  ConcurrencyLimit,
  FloatingConcurrencyLimit,
  GubernatorRateLimit,
} from "./pb/silo";

/**
 * Reason why a task was cancelled.
 */
export type CancellationReason = "server" | "client";

/**
 * A task received from Silo, with the payload decoded.
 *
 * This is a userland type that wraps the raw protobuf Task, providing:
 * - A required, decoded `payload` field (generic over T)
 * - All other task metadata fields
 *
 * @typeParam T The type of the decoded payload. Defaults to `unknown`.
 */

export interface Task<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
> {
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
  /** Shard ID (UUID) this task came from (needed for reporting outcome) */
  shard: string;
  /** Task group this task belongs to */
  taskGroup: string;
  /** Tenant ID if multi-tenancy is enabled */
  tenantId?: string;
  /** True if this is the final attempt (no more retries after this) */
  isLastAttempt: boolean;
  /** Metadata key/value pairs from the job */
  metadata: Metadata;
  /** Limits declared on this job (concurrency, rate, floating) */
  limits: Limit[];
}
/**
 * Transform a raw protobuf Task into a userland Task with decoded payload.
 */
export function transformTask<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
>(protoTask: ProtoTask): Task<Payload, Metadata> {
  return {
    id: protoTask.id,
    jobId: protoTask.jobId,
    attemptNumber: protoTask.attemptNumber,
    leaseMs: protoTask.leaseMs,
    payload: decodeBytes<Payload>(
      protoTask.payload?.encoding.oneofKind === "msgpack"
        ? protoTask.payload.encoding.msgpack
        : undefined,
      "payload",
    ),
    priority: protoTask.priority,
    shard: protoTask.shard,
    taskGroup: protoTask.taskGroup,
    tenantId: protoTask.tenantId,
    isLastAttempt: protoTask.isLastAttempt,
    metadata: protoTask.metadata as Metadata,
    limits: protoTask.limits,
  };
}

/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
export class TaskExecution<
  Payload = unknown,
  Metadata extends Record<string, string> = Record<string, string>,
> {
  /** The task being executed (raw proto format) */
  public readonly task: Task<Payload, Metadata>;
  /** The worker ID */
  public readonly workerId: string;

  /** Abort controller for this specific task's cancellation signal */
  private readonly _taskAbortController: AbortController;
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
    client: SiloGRPCClient,
  ) {
    this.task = task;
    this.workerId = workerId;
    this._client = client;

    this._taskAbortController = new AbortController();
  }

  /**
   * The cancellation signal for this task.
   * Only aborts when the task is explicitly cancelled (by server or client),
   * NOT when the worker shuts down.
   */
  get signal(): AbortSignal {
    return this._taskAbortController.signal;
  }

  /** Whether this task has been cancelled */
  get isCancelled(): boolean {
    return this._cancelled;
  }

  /** The reason for cancellation, if cancelled */
  get cancellationReason(): CancellationReason | undefined {
    return this._cancellationReason;
  }

  /** Whether the task was cancelled and should report Cancelled outcome */
  get shouldReportCancelled(): boolean {
    return this._cancelled;
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
