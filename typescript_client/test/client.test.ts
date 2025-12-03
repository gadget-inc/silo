import { describe, it, expect } from "vitest";
import {
  SiloGrpcClient,
  encodePayload,
  decodePayload,
  type EnqueueJobOptions,
} from "../src/client";

describe("encodePayload", () => {
  it("encodes a string as JSON bytes", () => {
    const result = encodePayload("hello");
    expect(new TextDecoder().decode(result)).toBe('"hello"');
  });

  it("encodes an object as JSON bytes", () => {
    const result = encodePayload({ foo: "bar", count: 42 });
    expect(new TextDecoder().decode(result)).toBe('{"foo":"bar","count":42}');
  });

  it("encodes null as JSON bytes", () => {
    const result = encodePayload(null);
    expect(new TextDecoder().decode(result)).toBe("null");
  });

  it("encodes an array as JSON bytes", () => {
    const result = encodePayload([1, 2, 3]);
    expect(new TextDecoder().decode(result)).toBe("[1,2,3]");
  });

  it("encodes nested objects as JSON bytes", () => {
    const result = encodePayload({ outer: { inner: "value" } });
    expect(new TextDecoder().decode(result)).toBe('{"outer":{"inner":"value"}}');
  });
});

describe("decodePayload", () => {
  it("decodes JSON bytes as a string", () => {
    const bytes = new TextEncoder().encode('"hello"');
    const result = decodePayload<string>(bytes);
    expect(result).toBe("hello");
  });

  it("decodes JSON bytes as an object", () => {
    const bytes = new TextEncoder().encode('{"foo":"bar","count":42}');
    const result = decodePayload<{ foo: string; count: number }>(bytes);
    expect(result).toEqual({ foo: "bar", count: 42 });
  });

  it("decodes JSON bytes as null", () => {
    const bytes = new TextEncoder().encode("null");
    const result = decodePayload<null>(bytes);
    expect(result).toBe(null);
  });

  it("decodes JSON bytes as an array", () => {
    const bytes = new TextEncoder().encode("[1,2,3]");
    const result = decodePayload<number[]>(bytes);
    expect(result).toEqual([1, 2, 3]);
  });

  it("returns undefined for undefined input", () => {
    const result = decodePayload(undefined);
    expect(result).toBeUndefined();
  });

  it("returns undefined for empty byte array", () => {
    const result = decodePayload(new Uint8Array(0));
    expect(result).toBeUndefined();
  });
});

describe("SiloGrpcClient", () => {
  describe("constructor", () => {
    it("accepts a string server address", () => {
      const client = new SiloGrpcClient({
        server: "localhost:50051",
        useTls: false,
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });

    it("accepts a host/port server address", () => {
      const client = new SiloGrpcClient({
        server: { host: "localhost", port: 50051 },
        useTls: false,
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });

    it("accepts a string token", () => {
      const client = new SiloGrpcClient({
        server: "localhost:50051",
        token: "my-token",
        useTls: false,
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });

    it("accepts a token function", () => {
      const client = new SiloGrpcClient({
        server: "localhost:50051",
        token: async () => "my-token",
        useTls: false,
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });

    it("accepts custom grpc client options", () => {
      const client = new SiloGrpcClient({
        server: "localhost:50051",
        useTls: false,
        grpcClientOptions: {
          "grpc.max_send_message_length": 1024 * 1024,
        },
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });

    it("accepts custom rpc options", () => {
      const client = new SiloGrpcClient({
        server: "localhost:50051",
        useTls: false,
        rpcOptions: { timeout: 5000 },
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });

    it("accepts rpc options as a function", () => {
      const client = new SiloGrpcClient({
        server: "localhost:50051",
        useTls: false,
        rpcOptions: () => ({ timeout: 5000 }),
      });
      expect(client).toBeInstanceOf(SiloGrpcClient);
      client.close();
    });
  });

  describe("EnqueueJobOptions type", () => {
    it("allows minimal options", () => {
      const options: EnqueueJobOptions = {
        shard: "default",
        payload: { task: "test" },
      };
      expect(options.shard).toBe("default");
      expect(options.payload).toEqual({ task: "test" });
    });

    it("allows full options with concurrency limit", () => {
      const options: EnqueueJobOptions = {
        shard: "default",
        payload: { task: "test" },
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
        shard: "default",
        payload: { task: "test" },
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
        shard: "default",
        payload: { task: "test" },
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
});
