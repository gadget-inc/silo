---
title: Introduction
description: An overview of Silo, a durable background job queue built on object storage
---

Silo is a durable, horizontally-scalable background job queue built on top of object storage via [SlateDB](https://slatedb.io). It brokers work between your application and your workers over gRPC, storing all job data in object storage for durability and low cost.

## Why Silo?

Most background job systems fall into two camps:

- **Redis-based queues** (Sidekiq, BullMQ, Celery) are simple and fast, but aren't truly durable. A Redis crash or eviction can lose jobs that were already acknowledged.
- **Workflow engines** (Temporal, Restate) are highly durable and scalable, but are complex and expensive to operate, with heavyweight client-side execution environments and multi-step workflow semantics.

Silo sits in between. It has the simplicity and speed of a traditional job queue with the durability and horizontal scalability of a workflow engine. Jobs are stored in object storage, so there are no Redis disappearing acts, and the compute layer is stateless and elastic.

## Key Features

- **Durable**: job data is stored in object storage. No data loss on crashes or scaling events.
- **Horizontally scalable**: add nodes to increase throughput. Silo automatically distributes shards across nodes.
- **Multi-tenant**: high-cardinality tenancy is built in. Each tenant's data lives on a single shard for fast, local transactions.
- **Concurrency limits**: built-in concurrency and rate limiting with high cardinality keys and dynamic floating limits.
- **Job lifecycle management**: retries with exponential backoff, future scheduling, expediting, cancellation, restart, and deletion.
- **Job results**: workers can return results that are stored and retrievable by the enqueuing process.
- **Observability**: Prometheus metrics, OpenTelemetry tracing, structured logging, and on-demand CPU profiling.
- **SQL queries**: inspect jobs with SQL via the built-in query engine.
- **Web UI**: operator-facing dashboard for cluster health, queue inspection, and debugging.

## Core Concepts

### Jobs

A **job** is a unit of work to be processed. Each job has a payload (arbitrary JSON), a task group, an optional tenant, and optional configuration for retries, priority, scheduling, and limits.

### Task Groups

A **task group** determines which workers process a job. Workers poll for tasks from a specific task group, so you can route different types of work to specialized worker pools and scale them independently.

### Tenants

A **tenant** is an isolation boundary for job data. All data for a tenant lives on a single shard, enabling fast local transactions. Tenants can be very high cardinality (millions), but each individual tenant is bounded by a single shard's throughput (roughly 4,000 jobs/second).

### Shards

Silo partitions data across **shards**. Each shard is backed by its own SlateDB instance in object storage. Shards are assigned to compute nodes, and can be split dynamically as load grows. Shard ownership is coordinated via etcd or Kubernetes.

### Workers

**Workers** are your application processes that poll Silo for tasks, execute them, and report outcomes. Workers communicate with Silo over gRPC and can be written in any language. Silo provides a TypeScript client with built-in worker support.

## How It Works

1. Your application **enqueues** a job by calling Silo's gRPC API with a payload and task group.
2. Silo stores the job in the appropriate shard (based on tenant) and makes it available for dequeue.
3. A **worker** polling that task group leases the task and processes it.
4. The worker **reports the outcome** (success with optional result, or failure) back to Silo.
5. On failure, Silo automatically retries according to the job's retry policy.

## Next Steps

- [Enqueueing Jobs](/guides/enqueueing) — learn how to create jobs with payloads, priorities, scheduling, and limits
- [Running Workers](/guides/running-workers) — set up workers to process jobs
- [Concurrency Limits](/guides/concurrency-limits) — control job execution with concurrency and rate limits
- [Deployment](/guides/deployment) — configure Silo for production with object storage and clustering
- [Server Configuration](/reference/server-configuration) — full reference for all configuration options
