import type { Task as ProtoTask } from "./pb/silo";
import { type SiloGRPCClient } from "./client";
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
}
/**
 * Transform a raw protobuf Task into a userland Task with decoded payload.
 */
export declare function transformTask<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>>(protoTask: ProtoTask): Task<Payload, Metadata>;
/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
export declare class TaskExecution<Payload = unknown, Metadata extends Record<string, string> = Record<string, string>> {
    /** The task being executed (raw proto format) */
    readonly task: Task<Payload, Metadata>;
    /** The worker ID */
    readonly workerId: string;
    /** Abort controller for this specific task */
    private readonly _taskAbortController;
    /** Combined signal that aborts on task cancel OR worker shutdown */
    private readonly _combinedSignal;
    /** Whether the task has been cancelled (by server or client) */
    private _cancelled;
    /** The reason for cancellation if cancelled */
    private _cancellationReason;
    /** Promise resolving when cancel RPC completes (if initiated by client) */
    private _cancelPromise;
    /** Reference to the client for cancel RPC */
    private readonly _client;
    constructor(task: Task<Payload, Metadata>, workerId: string, workerAbortSignal: AbortSignal, client: SiloGRPCClient);
    /** The combined abort signal for this task */
    get signal(): AbortSignal;
    /** Whether this task has been cancelled */
    get isCancelled(): boolean;
    /** The reason for cancellation, if cancelled */
    get cancellationReason(): CancellationReason | undefined;
    /** Whether the cancellation was due to server-side cancel (should report Cancelled outcome) */
    get shouldReportCancelled(): boolean;
    /**
     * Called when heartbeat detects server-side cancellation.
     * Aborts the task signal immediately.
     */
    markCancelledByServer(): void;
    /**
     * Cancel this task from the client side.
     * Calls the server to cancel the job and aborts the task signal.
     */
    cancelFromClient(): Promise<void>;
}
//# sourceMappingURL=TaskExecution.d.ts.map