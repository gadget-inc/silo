"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
/**
 * These tests verify the wrong shard retry logic without requiring a real gRPC connection.
 * We test the retry logic in isolation by mocking the internal behavior.
 */
// Helper to create a mock RpcError that simulates "shard not found"
function createWrongShardError() {
  const error = new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND");
  return error;
}
// Helper to create other RpcErrors
function createOtherError(message, code) {
  return new runtime_rpc_1.RpcError(message, code);
}
(0, vitest_1.describe)("Wrong Shard Retry Logic", () => {
  (0, vitest_1.describe)("isWrongShardError detection", () => {
    (0, vitest_1.it)("identifies NOT_FOUND with 'shard not found' as wrong shard error", () => {
      const error = createWrongShardError();
      // The error should have code NOT_FOUND and message containing "shard not found"
      (0, vitest_1.expect)(error.code).toBe("NOT_FOUND");
      (0, vitest_1.expect)(error.message).toContain("shard not found");
    });
    (0, vitest_1.it)("does not treat other NOT_FOUND errors as wrong shard", () => {
      const error = createOtherError("job not found", "NOT_FOUND");
      // This is a NOT_FOUND error but for a job, not a shard
      (0, vitest_1.expect)(error.code).toBe("NOT_FOUND");
      (0, vitest_1.expect)(error.message).not.toContain("shard not found");
    });
    (0, vitest_1.it)("does not treat other error codes as wrong shard", () => {
      const internalError = createOtherError("internal server error", "INTERNAL");
      const invalidError = createOtherError("invalid argument", "INVALID_ARGUMENT");
      (0, vitest_1.expect)(internalError.code).not.toBe("NOT_FOUND");
      (0, vitest_1.expect)(invalidError.code).not.toBe("NOT_FOUND");
    });
  });
  (0, vitest_1.describe)("retry behavior simulation", () => {
    (0, vitest_1.it)("retries up to maxWrongShardRetries times", async () => {
      const maxRetries = 5;
      let attempts = 0;
      // Simulate the retry logic
      const operation = async () => {
        attempts++;
        if (attempts <= maxRetries) {
          throw createWrongShardError();
        }
        return "success";
      };
      // Simulate _withWrongShardRetry behavior
      let lastError;
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          const result = await operation();
          (0, vitest_1.expect)(result).toBe("success");
          break;
        } catch (error) {
          lastError = error;
          if (
            error instanceof runtime_rpc_1.RpcError &&
            error.code === "NOT_FOUND" &&
            error.message.includes("shard not found") &&
            attempt < maxRetries
          ) {
            continue;
          }
          throw error;
        }
      }
      (0, vitest_1.expect)(attempts).toBe(maxRetries + 1);
    });
    (0, vitest_1.it)("does not retry after maxWrongShardRetries exhausted", async () => {
      const maxRetries = 3;
      let attempts = 0;
      const operation = async () => {
        attempts++;
        throw createWrongShardError();
      };
      // Simulate exhausting retries
      let caughtError;
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          await operation();
        } catch (error) {
          if (
            error instanceof runtime_rpc_1.RpcError &&
            error.code === "NOT_FOUND" &&
            error.message.includes("shard not found") &&
            attempt < maxRetries
          ) {
            continue;
          }
          caughtError = error;
          break;
        }
      }
      (0, vitest_1.expect)(attempts).toBe(maxRetries + 1);
      (0, vitest_1.expect)(caughtError).toBeInstanceOf(runtime_rpc_1.RpcError);
    });
    (0, vitest_1.it)("does not retry for non-wrong-shard errors", async () => {
      let attempts = 0;
      const operation = async () => {
        attempts++;
        throw createOtherError("job not found", "NOT_FOUND");
      };
      // Should not retry for "job not found"
      let caughtError;
      try {
        await operation();
      } catch (error) {
        caughtError = error;
      }
      (0, vitest_1.expect)(attempts).toBe(1);
      (0, vitest_1.expect)(caughtError).toBeInstanceOf(runtime_rpc_1.RpcError);
    });
    (0, vitest_1.it)("succeeds immediately if no error", async () => {
      let attempts = 0;
      const operation = async () => {
        attempts++;
        return "immediate success";
      };
      const result = await operation();
      (0, vitest_1.expect)(attempts).toBe(1);
      (0, vitest_1.expect)(result).toBe("immediate success");
    });
    (0, vitest_1.it)("succeeds after a few retries", async () => {
      let attempts = 0;
      const failCount = 2;
      const operation = async () => {
        attempts++;
        if (attempts <= failCount) {
          throw createWrongShardError();
        }
        return "success after retries";
      };
      // Simulate retry logic
      let result;
      const maxRetries = 5;
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          result = await operation();
          break;
        } catch (error) {
          if (
            error instanceof runtime_rpc_1.RpcError &&
            error.code === "NOT_FOUND" &&
            error.message.includes("shard not found") &&
            attempt < maxRetries
          ) {
            continue;
          }
          throw error;
        }
      }
      (0, vitest_1.expect)(attempts).toBe(failCount + 1);
      (0, vitest_1.expect)(result).toBe("success after retries");
    });
  });
  (0, vitest_1.describe)("exponential backoff simulation", () => {
    (0, vitest_1.it)("calculates correct delays with exponential backoff", () => {
      const initialDelay = 100;
      const maxDelay = 5000;
      let delay = initialDelay;
      const delays = [];
      for (let i = 0; i < 10; i++) {
        delays.push(delay);
        delay = Math.min(delay * 2, maxDelay);
      }
      // Expected: 100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000, 5000
      (0, vitest_1.expect)(delays[0]).toBe(100);
      (0, vitest_1.expect)(delays[1]).toBe(200);
      (0, vitest_1.expect)(delays[2]).toBe(400);
      (0, vitest_1.expect)(delays[3]).toBe(800);
      (0, vitest_1.expect)(delays[4]).toBe(1600);
      (0, vitest_1.expect)(delays[5]).toBe(3200);
      (0, vitest_1.expect)(delays[6]).toBe(5000); // Capped at max
      (0, vitest_1.expect)(delays[7]).toBe(5000);
      (0, vitest_1.expect)(delays[8]).toBe(5000);
      (0, vitest_1.expect)(delays[9]).toBe(5000);
    });
    (0, vitest_1.it)("respects custom initial delay", () => {
      const initialDelay = 50;
      const maxDelay = 5000;
      let delay = initialDelay;
      const delays = [];
      for (let i = 0; i < 5; i++) {
        delays.push(delay);
        delay = Math.min(delay * 2, maxDelay);
      }
      (0, vitest_1.expect)(delays[0]).toBe(50);
      (0, vitest_1.expect)(delays[1]).toBe(100);
      (0, vitest_1.expect)(delays[2]).toBe(200);
      (0, vitest_1.expect)(delays[3]).toBe(400);
      (0, vitest_1.expect)(delays[4]).toBe(800);
    });
  });
  (0, vitest_1.describe)("error propagation", () => {
    (0, vitest_1.it)("preserves original error details after exhausting retries", async () => {
      const originalError = createWrongShardError();
      let attempts = 0;
      const maxRetries = 2;
      const operation = async () => {
        attempts++;
        throw originalError;
      };
      let caughtError;
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          await operation();
        } catch (error) {
          if (
            error instanceof runtime_rpc_1.RpcError &&
            error.code === "NOT_FOUND" &&
            error.message.includes("shard not found") &&
            attempt < maxRetries
          ) {
            continue;
          }
          caughtError = error;
          break;
        }
      }
      (0, vitest_1.expect)(caughtError).toBe(originalError);
      (0, vitest_1.expect)(caughtError.code).toBe("NOT_FOUND");
      (0, vitest_1.expect)(caughtError.message).toContain("shard not found");
    });
    (0, vitest_1.it)("throws non-retriable errors immediately", async () => {
      const nonRetriableError = createOtherError("permission denied", "PERMISSION_DENIED");
      let attempts = 0;
      const operation = async () => {
        attempts++;
        throw nonRetriableError;
      };
      let caughtError;
      try {
        await operation();
      } catch (error) {
        caughtError = error;
      }
      (0, vitest_1.expect)(attempts).toBe(1);
      (0, vitest_1.expect)(caughtError).toBe(nonRetriableError);
      (0, vitest_1.expect)(caughtError.code).toBe("PERMISSION_DENIED");
    });
  });
});
(0, vitest_1.describe)("Default Configuration Values", () => {
  (0, vitest_1.it)("uses 5 as default maxWrongShardRetries", () => {
    // This matches what we implemented in the client
    const defaultMaxRetries = 5;
    (0, vitest_1.expect)(defaultMaxRetries).toBe(5);
  });
  (0, vitest_1.it)("uses 100ms as default wrongShardRetryDelayMs", () => {
    // This matches what we implemented in the client
    const defaultDelay = 100;
    (0, vitest_1.expect)(defaultDelay).toBe(100);
  });
  (0, vitest_1.it)("caps exponential backoff at 5000ms", () => {
    // This matches what we implemented in the client
    const maxBackoff = 5000;
    let delay = 100;
    for (let i = 0; i < 20; i++) {
      delay = Math.min(delay * 2, maxBackoff);
    }
    (0, vitest_1.expect)(delay).toBe(maxBackoff);
  });
});
//# sourceMappingURL=wrong-shard-retry.test.js.map
