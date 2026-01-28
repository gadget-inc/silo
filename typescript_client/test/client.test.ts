import { describe, it, expect, vi, afterEach } from "vitest";
import {
  SiloGRPCClient,
  JobStatus,
  JobNotFoundError,
  JobNotTerminalError,
  TaskNotFoundError,
  encodeBytes,
  decodeBytes,
  type EnqueueJobOptions,
  type SiloGRPCClientOptions,
  type AwaitJobOptions,
  type JobResult,
} from "../src/client";
import { JobHandle } from "../src/JobHandle";

describe("encodeBytes", () => {
  it("encodes a string as msgpack bytes", () => {
    const result = encodeBytes("hello");
    // Verify roundtrip works
    expect(decodeBytes<string>(result, "test")).toBe("hello");
  });

  it("encodes an object as msgpack bytes", () => {
    const result = encodeBytes({ foo: "bar", count: 42 });
    expect(decodeBytes(result, "test")).toEqual({ foo: "bar", count: 42 });
  });

  it("encodes null as msgpack bytes", () => {
    const result = encodeBytes(null);
    expect(decodeBytes(result, "test")).toBe(null);
  });

  it("encodes an array as msgpack bytes", () => {
    const result = encodeBytes([1, 2, 3]);
    expect(decodeBytes(result, "test")).toEqual([1, 2, 3]);
  });

  it("encodes nested objects as msgpack bytes", () => {
    const result = encodeBytes({ outer: { inner: "value" } });
    expect(decodeBytes(result, "test")).toEqual({ outer: { inner: "value" } });
  });
});

describe("decodeBytes", () => {
  it("decodes msgpack bytes as a string", () => {
    const bytes = encodeBytes("hello");
    const result = decodeBytes<string>(bytes, "test");
    expect(result).toBe("hello");
  });

  it("decodes msgpack bytes as an object", () => {
    const bytes = encodeBytes({ foo: "bar", count: 42 });
    const result = decodeBytes<{ foo: string; count: number }>(bytes, "test");
    expect(result).toEqual({ foo: "bar", count: 42 });
  });

  it("decodes msgpack bytes as null", () => {
    const bytes = encodeBytes(null);
    const result = decodeBytes<null>(bytes, "test");
    expect(result).toBe(null);
  });

  it("decodes msgpack bytes as an array", () => {
    const bytes = encodeBytes([1, 2, 3]);
    const result = decodeBytes<number[]>(bytes, "test");
    expect(result).toEqual([1, 2, 3]);
  });

  it("throws for undefined input", () => {
    expect(() => decodeBytes(undefined, "test")).toThrow(
      "No bytes to decode for field test"
    );
  });

  it("throws for empty byte array", () => {
    expect(() => decodeBytes(new Uint8Array(0), "test")).toThrow(
      "No bytes to decode for field test"
    );
  });
});

describe("SiloGRPCClient", () => {
  // Track clients created in tests for cleanup
  let clientsToClose: SiloGRPCClient[] = [];

  afterEach(() => {
    for (const client of clientsToClose) {
      client.close();
    }
    clientsToClose = [];
  });

  // Helper to create a client and track it for cleanup
  const createClient = (options: SiloGRPCClientOptions) => {
    const client = new SiloGRPCClient(options);
    clientsToClose.push(client);
    return client;
  };

  // Default options for tests (disable auto-refresh to avoid background timers)
  const defaultOptions = {
    useTls: false,
    shardRouting: {
      topologyRefreshIntervalMs: 0, // Disable auto-refresh in tests
    },
  };

  describe("constructor", () => {
    it("accepts a string server address", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts multiple server addresses", () => {
      const client = createClient({
        servers: ["localhost:50051", "localhost:50052"],
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a host/port server address", () => {
      const client = createClient({
        servers: { host: "localhost", port: 50051 },
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a string token", () => {
      const client = createClient({
        servers: "localhost:50051",
        token: "my-token",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a token function", () => {
      const client = createClient({
        servers: "localhost:50051",
        token: async () => "my-token",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts custom grpc client options", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
        grpcClientOptions: {
          "grpc.max_send_message_length": 1024 * 1024,
        },
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts custom rpc options", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
        rpcOptions: { timeout: 5000 },
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts rpc options as a function", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
        rpcOptions: () => ({ timeout: 5000 }),
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });
  });

  describe("EnqueueJobOptions type", () => {
    it("allows minimal options", () => {
      const options: EnqueueJobOptions = {
        payload: { task: "test" },
        taskGroup: "default",
      };
      expect(options.payload).toEqual({ task: "test" });
    });

    it("allows full options with concurrency limit", () => {
      const options: EnqueueJobOptions = {
        payload: { task: "test" },
        taskGroup: "default",
        id: "job-123",
        priority: 10,
        startAtMs: BigInt(Date.now()),
        retryPolicy: {
          retryCount: 3,
          initialIntervalMs: 1000n,
          maxIntervalMs: 30000n,
          randomizeInterval: true,
          backoffFactor: 2.0,
        },
        limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
        tenant: "tenant-a",
        metadata: { source: "api" },
      };
      expect(options.id).toBe("job-123");
      expect(options.priority).toBe(10);
      expect(options.retryPolicy?.retryCount).toBe(3);
      const limit = options.limits?.[0];
      expect(limit?.type).toBe("concurrency");
      if (limit?.type === "concurrency") {
        expect(limit.key).toBe("user:123");
        expect(limit.maxConcurrency).toBe(5);
      }
    });

    it("allows full options with rate limit", () => {
      const options: EnqueueJobOptions = {
        payload: { task: "test" },
        taskGroup: "default",
        limits: [
          {
            type: "rateLimit",
            name: "api-limit",
            uniqueKey: "user:456",
            limit: 100n,
            durationMs: 60000n,
            hits: 1,
            retryPolicy: {
              initialBackoffMs: 100n,
              maxBackoffMs: 5000n,
            },
          },
        ],
      };
      const limit = options.limits?.[0];
      expect(limit?.type).toBe("rateLimit");
      if (limit?.type === "rateLimit") {
        expect(limit.name).toBe("api-limit");
        expect(limit.uniqueKey).toBe("user:456");
        expect(limit.limit).toBe(100n);
        expect(limit.durationMs).toBe(60000n);
      }
    });

    it("allows mixed limits", () => {
      const options: EnqueueJobOptions = {
        payload: { task: "test" },
        taskGroup: "default",
        limits: [
          { type: "concurrency", key: "tenant:abc", maxConcurrency: 10 },
          {
            type: "rateLimit",
            name: "burst",
            uniqueKey: "tenant:abc",
            limit: 50n,
            durationMs: 1000n,
          },
        ],
      };
      expect(options.limits).toHaveLength(2);
      expect(options.limits?.[0].type).toBe("concurrency");
      expect(options.limits?.[1].type).toBe("rateLimit");
    });
  });

  describe("shard routing", () => {
    describe("getTopology", () => {
      it("returns the current topology", () => {
        const client = createClient({
          servers: "localhost:50051",
          useTls: false,
          shardRouting: {
            topologyRefreshIntervalMs: 0,
          },
        });

        const topology = client.getTopology();
        expect(topology.shardToServer).toBeInstanceOf(Map);
        expect(topology.shards).toBeInstanceOf(Array);
      });
    });

    describe("handle factory", () => {
      it("creates a JobHandle for an existing job ID", () => {
        const client = createClient({
          servers: "localhost:50051",
          ...defaultOptions,
        });

        const handle = client.handle("job-123", "tenant-a");

        expect(handle).toBeInstanceOf(JobHandle);
        expect(handle.id).toBe("job-123");
        expect(handle.tenant).toBe("tenant-a");
      });

      it("creates a JobHandle without tenant", () => {
        const client = createClient({
          servers: "localhost:50051",
          ...defaultOptions,
        });

        const handle = client.handle("job-456");

        expect(handle).toBeInstanceOf(JobHandle);
        expect(handle.id).toBe("job-456");
        expect(handle.tenant).toBeUndefined();
      });
    });
  });
});

describe("JobStatus enum", () => {
  it("has valid status values", () => {
    const statuses: JobStatus[] = [
      JobStatus.Scheduled,
      JobStatus.Running,
      JobStatus.Succeeded,
      JobStatus.Failed,
      JobStatus.Cancelled,
    ];
    expect(statuses).toHaveLength(5);
  });

  it("has string values matching the enum key", () => {
    expect(JobStatus.Scheduled).toBe("Scheduled");
    expect(JobStatus.Running).toBe("Running");
    expect(JobStatus.Succeeded).toBe("Succeeded");
    expect(JobStatus.Failed).toBe("Failed");
    expect(JobStatus.Cancelled).toBe("Cancelled");
  });
});

describe("JobResult type", () => {
  it("represents successful result", () => {
    const result: JobResult<{ count: number }> = {
      status: JobStatus.Succeeded,
      result: { count: 10 },
    };
    expect(result.status).toBe(JobStatus.Succeeded);
    expect(result.result?.count).toBe(10);
  });

  it("represents failed result", () => {
    const result: JobResult = {
      status: JobStatus.Failed,
      errorCode: "ERR_001",
      errorData: { details: "Something went wrong" },
    };
    expect(result.status).toBe(JobStatus.Failed);
    expect(result.errorCode).toBe("ERR_001");
  });

  it("represents cancelled result", () => {
    const result: JobResult = {
      status: JobStatus.Cancelled,
    };
    expect(result.status).toBe(JobStatus.Cancelled);
  });
});

describe("AwaitJobOptions type", () => {
  it("allows minimal options", () => {
    const options: AwaitJobOptions = {};
    expect(options.pollIntervalMs).toBeUndefined();
    expect(options.timeoutMs).toBeUndefined();
  });

  it("allows full options", () => {
    const options: AwaitJobOptions = {
      pollIntervalMs: 100,
      timeoutMs: 5000,
    };
    expect(options.pollIntervalMs).toBe(100);
    expect(options.timeoutMs).toBe(5000);
  });
});

describe("JobNotFoundError", () => {
  it("formats error message with job id and tenant", () => {
    const error = new JobNotFoundError("job-123", "tenant-abc");
    expect(error.message).toBe(
      'Job "job-123" not found in tenant "tenant-abc"'
    );
    expect(error.name).toBe("JobNotFoundError");
    expect(error.code).toBe("SILO_JOB_NOT_FOUND");
    expect(error.jobId).toBe("job-123");
    expect(error.tenant).toBe("tenant-abc");
  });

  it("formats error message without tenant", () => {
    const error = new JobNotFoundError("job-456");
    expect(error.message).toBe('Job "job-456" not found');
    expect(error.name).toBe("JobNotFoundError");
    expect(error.jobId).toBe("job-456");
    expect(error.tenant).toBeUndefined();
  });

  it("is an instance of Error", () => {
    const error = new JobNotFoundError("job-789");
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(JobNotFoundError);
  });
});

describe("JobNotTerminalError", () => {
  it("formats error message with job id, tenant, and status", () => {
    const error = new JobNotTerminalError(
      "job-123",
      "tenant-abc",
      JobStatus.Running
    );
    expect(error.message).toBe(
      'Job "job-123" in tenant "tenant-abc" is not in a terminal state (current status: Running)'
    );
    expect(error.name).toBe("JobNotTerminalError");
    expect(error.code).toBe("SILO_JOB_NOT_TERMINAL");
    expect(error.jobId).toBe("job-123");
    expect(error.tenant).toBe("tenant-abc");
    expect(error.currentStatus).toBe(JobStatus.Running);
  });

  it("formats error message without tenant or status", () => {
    const error = new JobNotTerminalError("job-456");
    expect(error.message).toBe('Job "job-456" is not in a terminal state');
    expect(error.name).toBe("JobNotTerminalError");
    expect(error.jobId).toBe("job-456");
    expect(error.tenant).toBeUndefined();
    expect(error.currentStatus).toBeUndefined();
  });

  it("is an instance of Error", () => {
    const error = new JobNotTerminalError("job-789");
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(JobNotTerminalError);
  });
});

describe("TaskNotFoundError", () => {
  it("formats error message with task id", () => {
    const error = new TaskNotFoundError("task-123");
    expect(error.message).toBe('Task "task-123" not found');
    expect(error.name).toBe("TaskNotFoundError");
    expect(error.code).toBe("SILO_TASK_NOT_FOUND");
    expect(error.taskId).toBe("task-123");
  });

  it("is an instance of Error", () => {
    const error = new TaskNotFoundError("task-789");
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(TaskNotFoundError);
  });
});
