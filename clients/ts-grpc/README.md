# @gadgetinc/silo-grpc

High-performance TypeScript gRPC client for the Silo service.

- Generated from ../../proto/silo.proto via ts-proto
- Uses @grpc/grpc-js with keepalive, deadlines, and retry/backoff
- Connection pooling via multiple client instances sharing the same address

## Install

Inside this subdirectory:

- Requires Node.js >= 18 and protoc available on PATH
- npm i
- npm run codegen
- npm run build

## Usage

```ts
import { HighPerfSiloClient, toJsonValueBytes } from "@gadgetinc/silo-grpc";

const client = new HighPerfSiloClient({
  address: "localhost:50051",
  poolSize: 2,
});

const { id } = await client.enqueue({
  shard: "0",
  id: "",
  priority: 10,
  startAtMs: 0,
  retryPolicy: undefined,
  payload: toJsonValueBytes({ hello: "world" }),
  concurrencyLimits: [],
});
```

## Performance options

- poolSize: number of client instances for parallelism
- channelOptions: override gRPC channel args (keepalive, backoff)
- deadlineMs: per-call deadline
- retry: control retry behavior and idempotency

## Development

- npm run typecheck
- npm test
