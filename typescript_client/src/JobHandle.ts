import {
  SiloGRPCClient,
  Job,
  JobStatus,
  AwaitJobOptions,
  JobResult,
  JobNotTerminalError,
  sleep,
  GetJobOptions,
} from "./client";

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

export class JobHandle {
  /** @internal */
  private readonly _client: SiloGRPCClient;

  /** The job ID */
  public readonly id: string;

  /** The tenant for this job (used for shard routing) */
  public readonly tenant: string | undefined;

  /**
   * Create a new JobHandle.
   * @internal Use `client.handle()` or the return value from `client.enqueue()` instead.
   */
  constructor(client: SiloGRPCClient, id: string, tenant?: string) {
    this._client = client;
    this.id = id;
    this.tenant = tenant;
  }

  /**
   * Get the full job details.
   * @param options Optional settings for the request.
   * @returns The job details.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async getJob(options?: GetJobOptions): Promise<Job> {
    return this._client.getJob(this.id, this.tenant, options);
  }

  /**
   * Get the current status of the job.
   * @returns The job status.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async getStatus(): Promise<JobStatus> {
    return this._client.getJobStatus(this.id, this.tenant);
  }

  /**
   * Cancel the job.
   * This marks the job for cancellation. Workers will be notified via heartbeat
   * and should stop processing and report a cancelled outcome.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async cancel(): Promise<void> {
    return this._client.cancelJob(this.id, this.tenant);
  }

  /**
   * Restart the job if it was cancelled or failed.
   * This allows the job to be processed again with a fresh set of retries
   * according to its retry policy.
   * @throws JobNotFoundError if the job doesn't exist.
   * @throws Error if the job is not in a restartable state (must be Cancelled or Failed).
   */
  public async restart(): Promise<void> {
    return this._client.restartJob(this.id, this.tenant);
  }

  /**
   * Delete the job.
   * @throws JobNotFoundError if the job doesn't exist.
   */
  public async delete(): Promise<void> {
    return this._client.deleteJob(this.id, this.tenant);
  }

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
  public async awaitResult<T = unknown>(
    options?: AwaitJobOptions
  ): Promise<JobResult<T>> {
    const pollIntervalMs = options?.pollIntervalMs ?? 500;
    const timeoutMs = options?.timeoutMs;
    const startTime = Date.now();

    while (true) {
      try {
        return await this._client.getJobResult<T>(this.id, this.tenant);
      } catch (error) {
        // JobNotTerminalError means job is still running - keep polling
        if (!(error instanceof JobNotTerminalError)) {
          throw error;
        }
      }

      // Check timeout
      if (timeoutMs !== undefined) {
        const elapsed = Date.now() - startTime;
        if (elapsed >= timeoutMs) {
          throw new Error(
            `Timeout waiting for job ${this.id} to complete after ${timeoutMs}ms`
          );
        }
      }

      // Wait before polling again
      await sleep(pollIntervalMs);
    }
  }
}
