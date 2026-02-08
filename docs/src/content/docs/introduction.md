---
title: Introduction
description: What Silo is, core concepts, and how to get started
---

Silo is a durable, horizontally-scalable background job system built on object storage. It stores all job data in object storage (like S3 or GCS) via [SlateDB](https://slatedb.io), which means your job broker doesn't need persistent disks or a traditional database. Workers connect to Silo over gRPC, poll for tasks, and report results back. Silo handles scheduling, retries, concurrency limits, rate limits, and multi-tenant isolation.

## Core Concepts

### Jobs

A **job** is a unit of work you want done in the background. Each job has a payload (arbitrary JSON), a task group, a priority, and an optional retry policy. Jobs progress through statuses: `Scheduled` → `Running` → `Succeeded` or `Failed` or `Cancelled`.

### Task Groups

A **task group** is a named queue that workers poll from. When you enqueue a job, you assign it to a task group like `"emails"` or `"reports"`. Workers subscribe to one task group and only receive jobs from that group. This lets you scale worker pools independently and route jobs to specialized workers.

### Tenants

A **tenant** is an isolation boundary. All of a tenant's jobs live on a single shard, which keeps per-tenant operations fast and transactional. Tenants are optional — single-tenant mode uses a synthetic default tenant. When enabled, every API call requires a tenant ID, and tenants can have independent concurrency and rate limits.

### Shards

A **shard** is a SlateDB database instance that stores jobs for a range of tenants. Silo partitions the tenant keyspace into shards using lexicographic ranges. Shards can be split dynamically to handle growing tenants. In a cluster, shards are distributed across nodes and automatically rebalanced when nodes join or leave.

### Workers

A **worker** is an external process that polls Silo for tasks, executes them, and reports outcomes. Workers lease tasks for a short period (10 seconds) and must heartbeat to keep the lease alive. If a worker crashes, the lease expires and the job is automatically retried.

## How It Fits Together

1. **Enqueue**: Your application enqueues a job via the Silo client, specifying a payload and task group.
2. **Route**: Silo hashes the tenant ID to find the correct shard and stores the job.
3. **Dequeue**: A worker polling that task group receives the job as a leased task.
4. **Execute**: The worker processes the job and heartbeats to keep the lease alive.
5. **Report**: The worker reports success or failure. On failure, Silo schedules a retry according to the retry policy.

## Quick Start

Enqueue a job and run a worker to process it:

```typescript
import { SiloGRPCClient, SiloWorker } from "@silo-ai/client";

const client = new SiloGRPCClient({
  servers: ["localhost:50051"],
});

// Enqueue a job
const handle = await client.enqueue({
  payload: { to: "user@example.com", subject: "Welcome!" },
  taskGroup: "emails",
  retryPolicy: {
    retryCount: 3,
    initialIntervalMs: 1000n,
    maxIntervalMs: 30000n,
    backoffFactor: 2.0,
    randomizeInterval: true,
  },
});

console.log(`Enqueued job: ${handle.id}`);

// Run a worker that processes email jobs
const worker = new SiloWorker({
  client,
  workerId: "email-worker-1",
  taskGroup: "emails",
  handler: async (task) => {
    const { to, subject } = task.payload;
    await sendEmail(to, subject);
    return { sent: true };
  },
});

await worker.start();
```

## Next Steps

- [Enqueueing Jobs](/guides/enqueueing) — all enqueue options including priority, scheduling, metadata, and limits
- [Running Workers](/guides/running-workers) — worker configuration, concurrency, and graceful shutdown
- [Retry Policies](/guides/retries) — exponential backoff, jitter, and attempt numbering
- [Concurrency Limits](/guides/concurrency-limits) — concurrency and rate limiting
- [Data Layout](/guides/data-layout) — multitenancy, sharding, and shard splitting
- [Deployment](/guides/deployment) — production deployment with object storage, etcd, or Kubernetes
