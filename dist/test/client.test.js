"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const JobHandle_1 = require("../src/JobHandle");
(0, vitest_1.describe)("encodeBytes", () => {
    (0, vitest_1.it)("encodes a string as msgpack bytes", () => {
        const result = (0, client_1.encodeBytes)("hello");
        // Verify roundtrip works
        (0, vitest_1.expect)((0, client_1.decodeBytes)(result, "test")).toBe("hello");
    });
    (0, vitest_1.it)("encodes an object as msgpack bytes", () => {
        const result = (0, client_1.encodeBytes)({ foo: "bar", count: 42 });
        (0, vitest_1.expect)((0, client_1.decodeBytes)(result, "test")).toEqual({ foo: "bar", count: 42 });
    });
    (0, vitest_1.it)("encodes null as msgpack bytes", () => {
        const result = (0, client_1.encodeBytes)(null);
        (0, vitest_1.expect)((0, client_1.decodeBytes)(result, "test")).toBe(null);
    });
    (0, vitest_1.it)("encodes an array as msgpack bytes", () => {
        const result = (0, client_1.encodeBytes)([1, 2, 3]);
        (0, vitest_1.expect)((0, client_1.decodeBytes)(result, "test")).toEqual([1, 2, 3]);
    });
    (0, vitest_1.it)("encodes nested objects as msgpack bytes", () => {
        const result = (0, client_1.encodeBytes)({ outer: { inner: "value" } });
        (0, vitest_1.expect)((0, client_1.decodeBytes)(result, "test")).toEqual({ outer: { inner: "value" } });
    });
});
(0, vitest_1.describe)("decodeBytes", () => {
    (0, vitest_1.it)("decodes msgpack bytes as a string", () => {
        const bytes = (0, client_1.encodeBytes)("hello");
        const result = (0, client_1.decodeBytes)(bytes, "test");
        (0, vitest_1.expect)(result).toBe("hello");
    });
    (0, vitest_1.it)("decodes msgpack bytes as an object", () => {
        const bytes = (0, client_1.encodeBytes)({ foo: "bar", count: 42 });
        const result = (0, client_1.decodeBytes)(bytes, "test");
        (0, vitest_1.expect)(result).toEqual({ foo: "bar", count: 42 });
    });
    (0, vitest_1.it)("decodes msgpack bytes as null", () => {
        const bytes = (0, client_1.encodeBytes)(null);
        const result = (0, client_1.decodeBytes)(bytes, "test");
        (0, vitest_1.expect)(result).toBe(null);
    });
    (0, vitest_1.it)("decodes msgpack bytes as an array", () => {
        const bytes = (0, client_1.encodeBytes)([1, 2, 3]);
        const result = (0, client_1.decodeBytes)(bytes, "test");
        (0, vitest_1.expect)(result).toEqual([1, 2, 3]);
    });
    (0, vitest_1.it)("throws for undefined input", () => {
        (0, vitest_1.expect)(() => (0, client_1.decodeBytes)(undefined, "test")).toThrow("No bytes to decode for field test");
    });
    (0, vitest_1.it)("throws for empty byte array", () => {
        (0, vitest_1.expect)(() => (0, client_1.decodeBytes)(new Uint8Array(0), "test")).toThrow("No bytes to decode for field test");
    });
});
(0, vitest_1.describe)("SiloGRPCClient", () => {
    // Track clients created in tests for cleanup
    let clientsToClose = [];
    (0, vitest_1.afterEach)(() => {
        for (const client of clientsToClose) {
            client.close();
        }
        clientsToClose = [];
    });
    // Helper to create a client and track it for cleanup
    const createClient = (options) => {
        const client = new client_1.SiloGRPCClient(options);
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
    (0, vitest_1.describe)("constructor", () => {
        (0, vitest_1.it)("accepts a string server address", () => {
            const client = createClient({
                servers: "localhost:7450",
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts multiple server addresses", () => {
            const client = createClient({
                servers: ["localhost:7450", "localhost:7451"],
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts a host/port server address", () => {
            const client = createClient({
                servers: { host: "localhost", port: 7450 },
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts a string token", () => {
            const client = createClient({
                servers: "localhost:7450",
                token: "my-token",
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts a token function", () => {
            const client = createClient({
                servers: "localhost:7450",
                token: async () => "my-token",
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts custom grpc client options", () => {
            const client = createClient({
                servers: "localhost:7450",
                ...defaultOptions,
                grpcClientOptions: {
                    "grpc.max_send_message_length": 1024 * 1024,
                },
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts custom rpc options", () => {
            const client = createClient({
                servers: "localhost:7450",
                ...defaultOptions,
                rpcOptions: { timeout: 5000 },
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts rpc options as a function", () => {
            const client = createClient({
                servers: "localhost:7450",
                ...defaultOptions,
                rpcOptions: () => ({ timeout: 5000 }),
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
    });
    (0, vitest_1.describe)("EnqueueJobOptions type", () => {
        (0, vitest_1.it)("allows minimal options", () => {
            const options = {
                payload: { task: "test" },
                taskGroup: "default",
            };
            (0, vitest_1.expect)(options.payload).toEqual({ task: "test" });
        });
        (0, vitest_1.it)("allows full options with concurrency limit", () => {
            const options = {
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
            (0, vitest_1.expect)(options.id).toBe("job-123");
            (0, vitest_1.expect)(options.priority).toBe(10);
            (0, vitest_1.expect)(options.retryPolicy?.retryCount).toBe(3);
            const limit = options.limits?.[0];
            (0, vitest_1.expect)(limit?.type).toBe("concurrency");
            if (limit?.type === "concurrency") {
                (0, vitest_1.expect)(limit.key).toBe("user:123");
                (0, vitest_1.expect)(limit.maxConcurrency).toBe(5);
            }
        });
        (0, vitest_1.it)("allows full options with rate limit", () => {
            const options = {
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
            (0, vitest_1.expect)(limit?.type).toBe("rateLimit");
            if (limit?.type === "rateLimit") {
                (0, vitest_1.expect)(limit.name).toBe("api-limit");
                (0, vitest_1.expect)(limit.uniqueKey).toBe("user:456");
                (0, vitest_1.expect)(limit.limit).toBe(100n);
                (0, vitest_1.expect)(limit.durationMs).toBe(60000n);
            }
        });
        (0, vitest_1.it)("allows mixed limits", () => {
            const options = {
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
            (0, vitest_1.expect)(options.limits).toHaveLength(2);
            (0, vitest_1.expect)(options.limits?.[0].type).toBe("concurrency");
            (0, vitest_1.expect)(options.limits?.[1].type).toBe("rateLimit");
        });
    });
    (0, vitest_1.describe)("shard routing", () => {
        (0, vitest_1.describe)("getTopology", () => {
            (0, vitest_1.it)("returns the current topology", () => {
                const client = createClient({
                    servers: "localhost:7450",
                    useTls: false,
                    shardRouting: {
                        topologyRefreshIntervalMs: 0,
                    },
                });
                const topology = client.getTopology();
                (0, vitest_1.expect)(topology.shardToServer).toBeInstanceOf(Map);
                (0, vitest_1.expect)(topology.shards).toBeInstanceOf(Array);
            });
        });
        (0, vitest_1.describe)("handle factory", () => {
            (0, vitest_1.it)("creates a JobHandle for an existing job ID", () => {
                const client = createClient({
                    servers: "localhost:7450",
                    ...defaultOptions,
                });
                const handle = client.handle("job-123", "tenant-a");
                (0, vitest_1.expect)(handle).toBeInstanceOf(JobHandle_1.JobHandle);
                (0, vitest_1.expect)(handle.id).toBe("job-123");
                (0, vitest_1.expect)(handle.tenant).toBe("tenant-a");
            });
            (0, vitest_1.it)("creates a JobHandle without tenant", () => {
                const client = createClient({
                    servers: "localhost:7450",
                    ...defaultOptions,
                });
                const handle = client.handle("job-456");
                (0, vitest_1.expect)(handle).toBeInstanceOf(JobHandle_1.JobHandle);
                (0, vitest_1.expect)(handle.id).toBe("job-456");
                (0, vitest_1.expect)(handle.tenant).toBeUndefined();
            });
        });
    });
});
(0, vitest_1.describe)("JobStatus enum", () => {
    (0, vitest_1.it)("has valid status values", () => {
        const statuses = [
            client_1.JobStatus.Scheduled,
            client_1.JobStatus.Running,
            client_1.JobStatus.Succeeded,
            client_1.JobStatus.Failed,
            client_1.JobStatus.Cancelled,
        ];
        (0, vitest_1.expect)(statuses).toHaveLength(5);
    });
    (0, vitest_1.it)("has string values matching the enum key", () => {
        (0, vitest_1.expect)(client_1.JobStatus.Scheduled).toBe("Scheduled");
        (0, vitest_1.expect)(client_1.JobStatus.Running).toBe("Running");
        (0, vitest_1.expect)(client_1.JobStatus.Succeeded).toBe("Succeeded");
        (0, vitest_1.expect)(client_1.JobStatus.Failed).toBe("Failed");
        (0, vitest_1.expect)(client_1.JobStatus.Cancelled).toBe("Cancelled");
    });
});
(0, vitest_1.describe)("JobResult type", () => {
    (0, vitest_1.it)("represents successful result", () => {
        const result = {
            status: client_1.JobStatus.Succeeded,
            result: { count: 10 },
        };
        (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Succeeded);
        (0, vitest_1.expect)(result.result?.count).toBe(10);
    });
    (0, vitest_1.it)("represents failed result", () => {
        const result = {
            status: client_1.JobStatus.Failed,
            errorCode: "ERR_001",
            errorData: { details: "Something went wrong" },
        };
        (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Failed);
        (0, vitest_1.expect)(result.errorCode).toBe("ERR_001");
    });
    (0, vitest_1.it)("represents cancelled result", () => {
        const result = {
            status: client_1.JobStatus.Cancelled,
        };
        (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Cancelled);
    });
});
(0, vitest_1.describe)("AwaitJobOptions type", () => {
    (0, vitest_1.it)("allows minimal options", () => {
        const options = {};
        (0, vitest_1.expect)(options.pollIntervalMs).toBeUndefined();
        (0, vitest_1.expect)(options.timeoutMs).toBeUndefined();
    });
    (0, vitest_1.it)("allows full options", () => {
        const options = {
            pollIntervalMs: 100,
            timeoutMs: 5000,
        };
        (0, vitest_1.expect)(options.pollIntervalMs).toBe(100);
        (0, vitest_1.expect)(options.timeoutMs).toBe(5000);
    });
});
(0, vitest_1.describe)("JobNotFoundError", () => {
    (0, vitest_1.it)("formats error message with job id and tenant", () => {
        const error = new client_1.JobNotFoundError("job-123", "tenant-abc");
        (0, vitest_1.expect)(error.message).toBe('Job "job-123" not found in tenant "tenant-abc"');
        (0, vitest_1.expect)(error.name).toBe("JobNotFoundError");
        (0, vitest_1.expect)(error.code).toBe("SILO_JOB_NOT_FOUND");
        (0, vitest_1.expect)(error.jobId).toBe("job-123");
        (0, vitest_1.expect)(error.tenant).toBe("tenant-abc");
    });
    (0, vitest_1.it)("formats error message without tenant", () => {
        const error = new client_1.JobNotFoundError("job-456");
        (0, vitest_1.expect)(error.message).toBe('Job "job-456" not found');
        (0, vitest_1.expect)(error.name).toBe("JobNotFoundError");
        (0, vitest_1.expect)(error.jobId).toBe("job-456");
        (0, vitest_1.expect)(error.tenant).toBeUndefined();
    });
    (0, vitest_1.it)("is an instance of Error", () => {
        const error = new client_1.JobNotFoundError("job-789");
        (0, vitest_1.expect)(error).toBeInstanceOf(Error);
        (0, vitest_1.expect)(error).toBeInstanceOf(client_1.JobNotFoundError);
    });
});
(0, vitest_1.describe)("JobNotTerminalError", () => {
    (0, vitest_1.it)("formats error message with job id, tenant, and status", () => {
        const error = new client_1.JobNotTerminalError("job-123", "tenant-abc", client_1.JobStatus.Running);
        (0, vitest_1.expect)(error.message).toBe('Job "job-123" in tenant "tenant-abc" is not in a terminal state (current status: Running)');
        (0, vitest_1.expect)(error.name).toBe("JobNotTerminalError");
        (0, vitest_1.expect)(error.code).toBe("SILO_JOB_NOT_TERMINAL");
        (0, vitest_1.expect)(error.jobId).toBe("job-123");
        (0, vitest_1.expect)(error.tenant).toBe("tenant-abc");
        (0, vitest_1.expect)(error.currentStatus).toBe(client_1.JobStatus.Running);
    });
    (0, vitest_1.it)("formats error message without tenant or status", () => {
        const error = new client_1.JobNotTerminalError("job-456");
        (0, vitest_1.expect)(error.message).toBe('Job "job-456" is not in a terminal state');
        (0, vitest_1.expect)(error.name).toBe("JobNotTerminalError");
        (0, vitest_1.expect)(error.jobId).toBe("job-456");
        (0, vitest_1.expect)(error.tenant).toBeUndefined();
        (0, vitest_1.expect)(error.currentStatus).toBeUndefined();
    });
    (0, vitest_1.it)("is an instance of Error", () => {
        const error = new client_1.JobNotTerminalError("job-789");
        (0, vitest_1.expect)(error).toBeInstanceOf(Error);
        (0, vitest_1.expect)(error).toBeInstanceOf(client_1.JobNotTerminalError);
    });
});
(0, vitest_1.describe)("TaskNotFoundError", () => {
    (0, vitest_1.it)("formats error message with task id", () => {
        const error = new client_1.TaskNotFoundError("task-123");
        (0, vitest_1.expect)(error.message).toBe('Task "task-123" not found');
        (0, vitest_1.expect)(error.name).toBe("TaskNotFoundError");
        (0, vitest_1.expect)(error.code).toBe("SILO_TASK_NOT_FOUND");
        (0, vitest_1.expect)(error.taskId).toBe("task-123");
    });
    (0, vitest_1.it)("is an instance of Error", () => {
        const error = new client_1.TaskNotFoundError("task-789");
        (0, vitest_1.expect)(error).toBeInstanceOf(Error);
        (0, vitest_1.expect)(error).toBeInstanceOf(client_1.TaskNotFoundError);
    });
});
//# sourceMappingURL=client.test.js.map