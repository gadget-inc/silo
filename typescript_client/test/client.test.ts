import { describe, it, expect, vi, afterEach } from "vitest";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import {
  SiloGRPCClient,
  JobStatus,
  JobNotFoundError,
  JobAlreadyExistsError,
  JobNotTerminalError,
  TaskNotFoundError,
  SiloGrpcError,
  SiloNotFoundError,
  SiloAlreadyExistsError,
  SiloInvalidArgumentError,
  SiloFailedPreconditionError,
  SiloUnauthenticatedError,
  SiloPermissionDeniedError,
  SiloResourceExhaustedError,
  SiloUnavailableError,
  SiloDeadlineExceededError,
  encodeBytes,
  decodeBytes,
  type EnqueueJobOptions,
  type SiloGRPCClientOptions,
  type AwaitJobOptions,
  type JobResult,
  type QueryResult,
  type QueryColumnInfo,
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
    expect(() => decodeBytes(undefined, "test")).toThrow("No bytes to decode for field test");
  });

  it("throws for empty byte array", () => {
    expect(() => decodeBytes(new Uint8Array(0), "test")).toThrow(
      "No bytes to decode for field test",
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
        servers: "localhost:7450",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts multiple server addresses", () => {
      const client = createClient({
        servers: ["localhost:7450", "localhost:7451"],
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a host/port server address", () => {
      const client = createClient({
        servers: { host: "localhost", port: 7450 },
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a string token", () => {
      const client = createClient({
        servers: "localhost:7450",
        token: "my-token",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a token function", () => {
      const client = createClient({
        servers: "localhost:7450",
        token: async () => "my-token",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts custom grpc client options", () => {
      const client = createClient({
        servers: "localhost:7450",
        ...defaultOptions,
        grpcClientOptions: {
          "grpc.max_send_message_length": 1024 * 1024,
        },
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts custom rpc options", () => {
      const client = createClient({
        servers: "localhost:7450",
        ...defaultOptions,
        rpcOptions: { timeout: 5000 },
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts rpc options as a function", () => {
      const client = createClient({
        servers: "localhost:7450",
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
          servers: "localhost:7450",
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
          servers: "localhost:7450",
          ...defaultOptions,
        });

        const handle = client.handle("job-123", "tenant-a");

        expect(handle).toBeInstanceOf(JobHandle);
        expect(handle.id).toBe("job-123");
        expect(handle.tenant).toBe("tenant-a");
      });

      it("creates a JobHandle without tenant", () => {
        const client = createClient({
          servers: "localhost:7450",
          ...defaultOptions,
        });

        const handle = client.handle("job-456");

        expect(handle).toBeInstanceOf(JobHandle);
        expect(handle.id).toBe("job-456");
        expect(handle.tenant).toBeUndefined();
      });
    });
  });

  describe("error mapping", () => {
    const mockWithWrongShardRetryError = (
      client: SiloGRPCClient,
      code: string,
      message: string,
    ) => {
      (client as any)._withWrongShardRetry = vi
        .fn()
        .mockRejectedValue(new RpcError(message, code, {}));
    };

    it("maps ALREADY_EXISTS on enqueue to JobAlreadyExistsError", async () => {
      const client = createClient({
        servers: "localhost:7450",
        ...defaultOptions,
      });
      mockWithWrongShardRetryError(client, "ALREADY_EXISTS", "job already exists");

      const error = await client
        .enqueue({
          id: "job-123",
          tenant: "tenant-a",
          taskGroup: "default",
          payload: { test: true },
        })
        .catch((e) => e);

      expect(error).toBeInstanceOf(JobAlreadyExistsError);
      expect(error).toBeInstanceOf(SiloAlreadyExistsError);
      expect(error.jobId).toBe("job-123");
      expect(error.tenant).toBe("tenant-a");
    });

    it.each([
      ["INVALID_ARGUMENT", SiloInvalidArgumentError],
      ["FAILED_PRECONDITION", SiloFailedPreconditionError],
      ["UNAUTHENTICATED", SiloUnauthenticatedError],
      ["PERMISSION_DENIED", SiloPermissionDeniedError],
      ["RESOURCE_EXHAUSTED", SiloResourceExhaustedError],
      ["UNAVAILABLE", SiloUnavailableError],
      ["DEADLINE_EXCEEDED", SiloDeadlineExceededError],
      ["NOT_FOUND", SiloNotFoundError],
      ["ALREADY_EXISTS", SiloAlreadyExistsError],
    ] as const)(
      "maps %s from query into a dedicated client error",
      async (grpcCode, ErrorClass) => {
        const client = createClient({
          servers: "localhost:7450",
          ...defaultOptions,
        });
        mockWithWrongShardRetryError(client, grpcCode, `rpc failed: ${grpcCode}`);

        await expect(client.query("SELECT 1")).rejects.toThrow(ErrorClass);
      },
    );

    it("maps NOT_FOUND on getJob to JobNotFoundError", async () => {
      const client = createClient({
        servers: "localhost:7450",
        ...defaultOptions,
      });
      mockWithWrongShardRetryError(client, "NOT_FOUND", "job not found");

      await expect(client.getJob("job-404", "tenant-abc")).rejects.toThrow(JobNotFoundError);
    });

    it("maps NOT_FOUND on heartbeat to TaskNotFoundError", async () => {
      const client = createClient({
        servers: "localhost:7450",
        ...defaultOptions,
      });
      (client as any)._getClientForShard = vi.fn().mockReturnValue({
        heartbeat: vi.fn().mockReturnValue({
          response: Promise.reject(new RpcError("task not found", "NOT_FOUND", {})),
        }),
      });

      await expect(client.heartbeat("worker-a", "task-404", "shard-1")).rejects.toThrow(
        TaskNotFoundError,
      );
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
    expect(error.message).toBe('Job "job-123" not found in tenant "tenant-abc"');
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
    const error = new JobNotTerminalError("job-123", "tenant-abc", JobStatus.Running);
    expect(error.message).toBe(
      'Job "job-123" in tenant "tenant-abc" is not in a terminal state (current status: Running)',
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

describe("JobAlreadyExistsError", () => {
  it("formats error message with job id and tenant", () => {
    const error = new JobAlreadyExistsError("job-123", "tenant-abc");
    expect(error.message).toBe('Job "job-123" already exists in tenant "tenant-abc"');
    expect(error.name).toBe("JobAlreadyExistsError");
    expect(error.code).toBe("SILO_JOB_ALREADY_EXISTS");
    expect(error.grpcCode).toBe("ALREADY_EXISTS");
    expect(error.jobId).toBe("job-123");
    expect(error.tenant).toBe("tenant-abc");
  });
});

describe("SiloGrpcError hierarchy", () => {
  it("stores grpc code and message on specialized errors", () => {
    const error = new SiloUnavailableError("cluster is unavailable");
    expect(error).toBeInstanceOf(SiloGrpcError);
    expect(error.name).toBe("SiloUnavailableError");
    expect(error.code).toBe("SILO_UNAVAILABLE");
    expect(error.grpcCode).toBe("UNAVAILABLE");
    expect(error.grpcMessage).toBe("cluster is unavailable");
  });
});

describe("QueryResult type", () => {
  it("represents a basic query result", () => {
    const result: QueryResult = {
      columns: [
        { name: "id", dataType: "Utf8" },
        { name: "priority", dataType: "Int32" },
      ],
      rows: [
        { id: "job-1", priority: 10 },
        { id: "job-2", priority: 20 },
      ],
      rowCount: 2,
    };
    expect(result.columns).toHaveLength(2);
    expect(result.rows).toHaveLength(2);
    expect(result.rowCount).toBe(2);
    expect(result.rows[0].id).toBe("job-1");
  });

  it("supports generic row type parameter", () => {
    interface JobRow {
      id: string;
      priority: number;
    }

    const result: QueryResult<JobRow> = {
      columns: [
        { name: "id", dataType: "Utf8" },
        { name: "priority", dataType: "Int32" },
      ],
      rows: [{ id: "job-1", priority: 10 }],
      rowCount: 1,
    };
    // TypeScript knows the type of rows
    const row: JobRow = result.rows[0];
    expect(row.id).toBe("job-1");
    expect(row.priority).toBe(10);
  });

  it("represents an empty query result", () => {
    const result: QueryResult = {
      columns: [{ name: "id", dataType: "Utf8" }],
      rows: [],
      rowCount: 0,
    };
    expect(result.rows).toHaveLength(0);
    expect(result.rowCount).toBe(0);
  });
});

describe("QueryColumnInfo type", () => {
  it("has name and dataType fields", () => {
    const col: QueryColumnInfo = { name: "status", dataType: "Utf8" };
    expect(col.name).toBe("status");
    expect(col.dataType).toBe("Utf8");
  });
});

describe("SiloGRPCClient.query deserialization", () => {
  let clientsToClose: SiloGRPCClient[] = [];

  afterEach(() => {
    for (const client of clientsToClose) {
      client.close();
    }
    clientsToClose = [];
  });

  const createClient = (options: SiloGRPCClientOptions) => {
    const client = new SiloGRPCClient(options);
    clientsToClose.push(client);
    return client;
  };

  const defaultOptions = {
    useTls: false,
    shardRouting: {
      topologyRefreshIntervalMs: 0,
    },
  };

  it("deserializes msgpack rows in query response", async () => {
    const client = createClient({
      servers: "localhost:7450",
      ...defaultOptions,
    });

    const mockResponse = {
      columns: [
        { name: "id", dataType: "Utf8" },
        { name: "priority", dataType: "Int32" },
      ],
      rows: [
        {
          encoding: {
            oneofKind: "msgpack" as const,
            msgpack: encodeBytes({ id: "job-1", priority: 10 }),
          },
        },
        {
          encoding: {
            oneofKind: "msgpack" as const,
            msgpack: encodeBytes({ id: "job-2", priority: 20 }),
          },
        },
      ],
      rowCount: 2,
    };

    (client as any)._withWrongShardRetry = vi
      .fn()
      .mockImplementation(async (_tenant: any, _operation: any) => {
        // Simulate the internal deserialization that happens inside the real _withWrongShardRetry callback
        const columns = mockResponse.columns.map((c: any) => ({
          name: c.name,
          dataType: c.dataType,
        }));
        const rows = mockResponse.rows.map((row: any, index: number) => {
          if (row.encoding.oneofKind === "msgpack") {
            return decodeBytes(row.encoding.msgpack, `row[${index}]`);
          }
          throw new Error(`Unsupported encoding`);
        });
        return { columns, rows, rowCount: mockResponse.rowCount };
      });

    const result = await client.query("SELECT * FROM jobs");
    expect(result.columns).toHaveLength(2);
    expect(result.columns[0].name).toBe("id");
    expect(result.columns[0].dataType).toBe("Utf8");
    expect(result.rows).toHaveLength(2);
    expect(result.rows[0]).toEqual({ id: "job-1", priority: 10 });
    expect(result.rows[1]).toEqual({ id: "job-2", priority: 20 });
    expect(result.rowCount).toBe(2);
  });

  it("returns typed rows with generic parameter", async () => {
    const client = createClient({
      servers: "localhost:7450",
      ...defaultOptions,
    });

    interface CountRow {
      count: number;
    }

    (client as any)._withWrongShardRetry = vi.fn().mockImplementation(async () => {
      return {
        columns: [{ name: "count", dataType: "Int64" }],
        rows: [{ count: 42 }],
        rowCount: 1,
      };
    });

    const result = await client.query<CountRow>("SELECT COUNT(*) as count FROM jobs");
    const row: CountRow = result.rows[0];
    expect(row.count).toBe(42);
  });
});
