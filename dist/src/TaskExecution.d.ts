import type { Task, SiloGRPCClient } from "./client";
/**
 * Internal class to manage the state of a single task execution.
 * Tracks the abort controller, cancellation state, and provides methods
 * to coordinate cancellation from various sources.
 * @internal
 */
export declare class TaskExecution {
    /** The task being executed */
    readonly task: Task;
    /** The shard this task came from */
    readonly shard: number;
    /** The worker ID */
    readonly workerId: string;
    /** Tenant for this task */
    readonly tenant: string | undefined;
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
    constructor(task: Task, shard: number, workerId: string, workerAbortSignal: AbortSignal, client: SiloGRPCClient);
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
/**
 * Reason why a task was cancelled.
 */
export type CancellationReason = "server" | "client" | "worker_shutdown";
//# sourceMappingURL=TaskExecution.d.ts.map