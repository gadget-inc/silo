import { describe, it, expect } from "vitest";
import { HighPerfSiloClient } from "../src";
import { status } from "@grpc/grpc-js";

describe("retry logic", async () => {
  it("retries idempotent calls on UNAVAILABLE", async () => {
    const client = new HighPerfSiloClient({
      address: "localhost:0",
      retry: {
        enabled: true,
        idempotentOnly: true,
        maxAttempts: 3,
        initialDelayMs: 1,
        maxDelayMs: 4,
        backoffFactor: 2,
        retryableStatusCodes: [status.UNAVAILABLE],
      },
    } as any);

    let calls = 0;
    const ok = await (client as any)["maybeRetry"](
      async () => {
        calls++;
        if (calls < 3) {
          const err: any = new Error("temp");
          err.code = status.UNAVAILABLE;
          throw err;
        }
        return "ok";
      },
      true
    );

    expect(ok).toBe("ok");
    expect(calls).toBe(3);
  });

  it("does not retry non-idempotent when idempotentOnly", async () => {
    const client = new HighPerfSiloClient({
      address: "localhost:0",
      retry: {
        enabled: true,
        idempotentOnly: true,
        maxAttempts: 3,
        initialDelayMs: 1,
        maxDelayMs: 4,
        backoffFactor: 2,
        retryableStatusCodes: [status.UNAVAILABLE],
      },
    } as any);

    let calls = 0;
    await expect(
      (client as any)["maybeRetry"](async () => {
        calls++;
        const err: any = new Error("temp");
        err.code = status.UNAVAILABLE;
        throw err;
      }, false)
    ).rejects.toBeInstanceOf(Error);
    expect(calls).toBe(1);
  });
});
