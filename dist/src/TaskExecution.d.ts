import type { Task as ProtoTask, Limit } from "./pb/silo";
import { type SiloGRPCClient } from "./client";
export type { Limit } from "./pb/silo";
export type { ConcurrencyLimit, FloatingConcurrencyLimit, GubernatorRateLimit } from "./pb/silo";
/**
 * Reason why a task was cancelled.
 */
export type CancellationReason = "server" | "client" | "lease-lost";
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
    /** Which attempt this is (1 = first attempt). Monotonically increasing across restarts. */
    attemptNumber: number;
    /** Attempt number within the current run (1 = first attempt since last restart). Resets on restart. */
    relativeAttemptNumber: number;
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
    /** True if this is the final attempt within the current run (no more retries after this unless restarted) */
    isLastAttempt: boolean;
    /** Metadata key/value pairs from the job */
    metadata: Metadata;
    /** Limits declared on this job (concurrency, rate, floating) */
    limits: Limit[];
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
    /** Abort controller for this specific task's cancellation signal */
    private readonly _taskAbortController;
    /** Whether the task has been cancelled (by server or client) */
    private _cancelled;
    /** Whether the worker discovered mid-execution that it no longer holds the lease */
    private _leaseLost;
    /** Whether the handler has settled (outcome reporting is underway or done) */
    private _settled;
    /** The reason for cancellation if cancelled */
    private _cancellationReason;
    /** Promise resolving when cancel RPC completes (if initiated by client) */
    private _cancelPromise;
    /** Reference to the client for cancel RPC */
    private readonly _client;
    constructor(task: Task<Payload, Metadata>, workerId: string, client: SiloGRPCClient);
    /**
     * The cancellation signal for this task.
     * Only aborts when the task is explicitly cancelled (by server or client),
     * NOT when the worker shuts down.
     */
    get signal(): AbortSignal;
    /** Whether this task has been cancelled */
    get isCancelled(): boolean;
    /** The reason for cancellation, if cancelled */
    get cancellationReason(): CancellationReason | undefined;
    /** Whether the task was cancelled and should report Cancelled outcome */
    get shouldReportCancelled(): boolean;
    /** Whether the lease for this task is known to be gone (no outcome may be reported) */
    get isLeaseLost(): boolean;
    /** Whether the handler has settled (outcome reporting is underway or done) */
    get isSettled(): boolean;
    /**
     * Mark the handler as settled. Heartbeat responses that land after this
     * point are meaningless: outcome reporting releases the lease, so a late
     * lease-gone rejection is the routine completion race, not a lost lease.
     */
    markSettled(): void;
    /**
     * Mark the lease as lost and cancel the execution.
     * One-shot: returns true only for the call that performs the transition,
     * so concurrent in-flight heartbeat failures surface the event once.
     */
    markLeaseLost(): boolean;
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