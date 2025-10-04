import { HighPerfSiloClient, toJsonValueBytes } from "../src";
import type { EnqueueRequest } from "../src";

async function main() {
  const client = new HighPerfSiloClient({
    address: "localhost:50051",
    poolSize: 2,
  });

  const enq: EnqueueRequest = {
    shard: "0",
    id: "",
    priority: 10,
    startAtMs: 0,
    retryPolicy: undefined,
    payload: toJsonValueBytes({ hello: "world" }),
    concurrencyLimits: [],
  };

  const { id } = await client.enqueue(enq);
  console.log("enqueued", id);

  const lease = await client.leaseTasks({ shard: "0", workerId: "w1", maxTasks: 1 });
  console.log("leased", lease.tasks.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
