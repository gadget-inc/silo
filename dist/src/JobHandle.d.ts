import { SiloGRPCClient, Job, JobStatus, AwaitJobOptions, JobResult, GetJobOptions } from "./client";
/**
 * A handle to an enqueued job that provides methods to interact with the job.
 *
 * JobHandle provides a convenient interface to:
 * - Get job details and status
 * - Cancel the job
 * - Await the job result (poll until completion)
 *
 * @example
 * ```typescript
 * // Enqueue returns a handle
 * const handle = await client.enqueue({ payload: { task: "process" } });
 * console.log(`Job ID: ${handle.id}`);
 *
 * // Or create a handle from an existing job ID
 * const existingHandle = client.handle("job-123", "tenant-a");
 *
 * // Check job status
 * const status = await handle.getStatus();
 *
 * // Cancel the job
 * await handle.cancel();
 *
 * // Wait for completion with optional timeout
 * const result = await handle.awaitResult({ timeoutMs: 30000 });
 * ```
 */
export declare class JobHandle {
    /** @internal */
    private readonly _client;
    /** The job ID */
    readonly id: string;
    /** The tenant for this job (used for shard routing) */
    readonly tenant: string | undefined;
    /**
     * Create a new JobHandle.
     * @internal Use `client.handle()` or the return value from `client.enqueue()` instead.
     */
    constructor(client: SiloGRPCClient, id: string, tenant?: string);
    /**
     * Get the full job details.
     * @param options Optional settings for the request.
     * @returns The job details.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    getJob(options?: GetJobOptions): Promise<Job>;
    /**
     * Get the current status of the job.
     * @returns The job status.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    getStatus(): Promise<JobStatus>;
    /**
     * Cancel the job.
     * This marks the job for cancellation. Workers will be notified via heartbeat
     * and should stop processing and report a cancelled outcome.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    cancel(): Promise<void>;
    /**
     * Restart the job if it was cancelled or failed.
     * This allows the job to be processed again with a fresh set of retries
     * according to its retry policy.
     * @throws JobNotFoundError if the job doesn't exist.
     * @throws Error if the job is not in a restartable state (must be Cancelled or Failed).
     */
    restart(): Promise<void>;
    /**
     * Expedite a future-scheduled job to run immediately.
     * This is useful for dragging forward a job that was scheduled for the future,
     * or for skipping retry backoff delays on a mid-retry job.
     * @throws JobNotFoundError if the job doesn't exist.
     * @throws Error if the job is not in an expeditable state
     *         (already running, terminal, cancelled, or task already ready to run).
     */
    expedite(): Promise<void>;
    /**
     * Delete the job.
     * @throws JobNotFoundError if the job doesn't exist.
     */
    delete(): Promise<void>;
    /**
     * Wait for the job to complete and return the result.
     * This polls the job status until it reaches a terminal state (Succeeded, Failed, or Cancelled).
     *
     * @param options Options for polling behavior.
     * @returns The job result once complete.
     * @throws Error if timeout is reached before job completes.
     * @throws JobNotFoundError if the job doesn't exist.
     *
     * @example
     * ```typescript
     * // Wait indefinitely for completion
     * const result = await handle.awaitResult();
     *
     * // Wait with a 30 second timeout
     * const result = await handle.awaitResult({ timeoutMs: 30000 });
     *
     * // Poll every 100ms instead of the default 500ms
     * const result = await handle.awaitResult({ pollIntervalMs: 100 });
     * ```
     */
    awaitResult<T = unknown>(options?: AwaitJobOptions): Promise<JobResult<T>>;
}
//# sourceMappingURL=JobHandle.d.ts.map