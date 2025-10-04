import { ChannelCredentials, ChannelOptions, status, Metadata } from "@grpc/grpc-js";
import type { CallOptions, ServiceError } from "@grpc/grpc-js";
import { SiloClient as GeneratedSiloClient } from "./generated/silo";
import type {
  EnqueueRequest,
  EnqueueResponse,
  GetJobRequest,
  GetJobResponse,
  DeleteJobRequest,
  DeleteJobResponse,
  LeaseTasksRequest,
  LeaseTasksResponse,
  ReportOutcomeRequest,
  ReportOutcomeResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  JsonValueBytes,
} from "./generated/silo";

export type RetryOptions = {
  enabled: boolean;
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffFactor: number;
  retryableStatusCodes?: number[];
  idempotentOnly?: boolean;
};

export type SiloClientOptions = {
  address: string;
  deadlineMs?: number;
  poolSize?: number;
  channelOptions?: ChannelOptions;
  retry?: RetryOptions;
  credentials?: ChannelCredentials;
};

const DEFAULT_RETRY: RetryOptions = {
  enabled: true,
  maxAttempts: 3,
  initialDelayMs: 50,
  maxDelayMs: 1000,
  backoffFactor: 2,
  retryableStatusCodes: [status.UNAVAILABLE, status.DEADLINE_EXCEEDED],
  idempotentOnly: true,
};

const DEFAULT_CHANNEL_OPTIONS: ChannelOptions = {
  "grpc.keepalive_time_ms": 20000,
  "grpc.keepalive_timeout_ms": 10000,
  "grpc.keepalive_permit_without_calls": 1,
  "grpc.http2.min_time_between_pings_ms": 10000,
  "grpc.http2.max_pings_without_data": 0,
  "grpc.max_reconnect_backoff_ms": 30000,
};

export class HighPerfSiloClient {
  private clients: GeneratedSiloClient[];
  private idx = 0;
  private opts: Required<Omit<SiloClientOptions, "credentials">> & { credentials?: ChannelCredentials };

  constructor(opts: SiloClientOptions) {
    const merged: HighPerfSiloClient["opts"] = {
      address: opts.address,
      deadlineMs: opts.deadlineMs ?? 5000,
      poolSize: opts.poolSize ?? 1,
      channelOptions: { ...DEFAULT_CHANNEL_OPTIONS, ...(opts.channelOptions ?? {}) },
      retry: { ...DEFAULT_RETRY, ...(opts.retry ?? {}) },
      credentials: opts.credentials ?? ChannelCredentials.createInsecure(),
    };
    this.opts = merged;
    this.clients = Array.from({ length: this.opts.poolSize }, () => {
      return new GeneratedSiloClient(
        this.opts.address,
        this.opts.credentials!,
        this.opts.channelOptions
      );
    });
  }

  private pick(): GeneratedSiloClient {
    const c = this.clients[this.idx % this.clients.length];
    this.idx++;
    return c;
  }

  private callUnary<R>(
    fn: (
      client: GeneratedSiloClient,
      md: Metadata,
      opts: CallOptions,
      cb: (err: ServiceError | null, res?: R) => void
    ) => unknown,
    optsOverride?: CallOptions
  ): Promise<R> {
    const md = new Metadata();
    const opts: CallOptions = {
      deadline: new Date(Date.now() + this.opts.deadlineMs),
      ...(optsOverride ?? {}),
    };
    return new Promise<R>((resolve, reject) => {
      fn(this.pick(), md, opts, (err, res) => {
        if (err) reject(err);
        else resolve(res as R);
      });
    });
  }


  async enqueue(req: EnqueueRequest): Promise<EnqueueResponse> {
    const call = () => this.callUnary<EnqueueResponse>((c, md, opts, cb) => c.enqueue(req, md, opts, cb));
    return this.maybeRetry(call, false);
  }

  async getJob(req: GetJobRequest): Promise<GetJobResponse> {
    const call = () => this.callUnary<GetJobResponse>((c, md, opts, cb) => c.getJob(req, md, opts, cb));
    return this.maybeRetry(call, true);
  }

  async deleteJob(req: DeleteJobRequest): Promise<DeleteJobResponse> {
    const call = () => this.callUnary<DeleteJobResponse>((c, md, opts, cb) => c.deleteJob(req, md, opts, cb));
    return this.maybeRetry(call, true);
  }

  async leaseTasks(req: LeaseTasksRequest): Promise<LeaseTasksResponse> {
    const call = () => this.callUnary<LeaseTasksResponse>((c, md, opts, cb) => c.leaseTasks(req, md, opts, cb));
    return this.maybeRetry(call, true);
  }

  async reportOutcome(req: ReportOutcomeRequest): Promise<ReportOutcomeResponse> {
    const call = () => this.callUnary<ReportOutcomeResponse>((c, md, opts, cb) => c.reportOutcome(req, md, opts, cb));
    return this.maybeRetry(call, false);
  }

  async heartbeat(req: HeartbeatRequest): Promise<HeartbeatResponse> {
    const call = () => this.callUnary<HeartbeatResponse>((c, md, opts, cb) => c.heartbeat(req, md, opts, cb));
    return this.maybeRetry(call, true);
  }

  private async maybeRetry<T>(fn: () => Promise<T>, idempotent: boolean): Promise<T> {
    const r = this.opts.retry;
    if (!r.enabled || (r.idempotentOnly && !idempotent)) {
      return fn();
    }
    let attempt = 0;
    let delay = r.initialDelayMs;
    for (;;) {
      try {
        return await fn();
      } catch (e: any) {
        attempt++;
        if (attempt >= r.maxAttempts) {
          throw e;
        }
        const code = e?.code;
        const retryable = r.retryableStatusCodes ?? [];
        if (!retryable.includes(code)) {
          throw e;
        }
        await sleep(Math.min(delay, r.maxDelayMs));
        delay = Math.min(delay * r.backoffFactor, r.maxDelayMs);
      }
    }
  }
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export function toJsonValueBytes(obj: unknown): JsonValueBytes {
  const json = JSON.stringify(obj ?? null);
  return { data: Buffer.from(json, "utf-8") } as JsonValueBytes;
}

export function fromJsonValueBytes(j?: JsonValueBytes | null): unknown | undefined {
  if (!j) return undefined;
  try {
    return JSON.parse(Buffer.from(j.data as any).toString("utf-8"));
  } catch {
    return undefined;
  }
}

export type {
  EnqueueRequest,
  EnqueueResponse,
  GetJobRequest,
  GetJobResponse,
  DeleteJobRequest,
  DeleteJobResponse,
  LeaseTasksRequest,
  LeaseTasksResponse,
  ReportOutcomeRequest,
  ReportOutcomeResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  JsonValueBytes,
} from "./generated/silo";
