# Silo

A background job queueing system built on top of object storage via [slatedb](https://slatedb.io)

Status: working prototype

### Features

- high throughput, durable job brokering server
- low cost at scale by using object storage as the source of truth
- high-cardinality, cheap multitenancy built in
- future scheduling
- concurrency limits for limiting throughput of various jobs (with high cardinality and limit change support)
- compute/storage separation for elastic compute scaling
- tracks job results so jobs can return stuff to the enqueuing process
- tracks job and attempt history for operators
- allows searching for jobs with a few different filters
- brokers work for userland workers in any language that communicate with the broker via RPCs
- simple operator-facing webui

It's like Sidekiq, BullMQ, Temporal, Restate or similar, but durable and horizontally scalable (unlike the Redis-based systems), and cheap (unlike the big chatty systems).

## Deployment architecture

To use Silo for job execution, you must set up two things:

- the Silo server, which will store your jobs and broker tasks
- some workers listening to this Silo server that actually run tasks.

Your worker instances will poll the Silo server for new tasks to run, run them locally, and report the outcome back to Silo. On failure, the job will be re-attempted in the future according to the job's retry schedule.

If you want to run multiple Silo instances in a cluster to spread out your load, you must also configure a cluster membership system for Silo. Currently, etcd and Kubernetes API based cluster membership providers are available.

## Configuration

Silo aims to work well out of the box, but when needed, has a deep set of configurations you can adjust to tune things as you see fit. See [CONFIGURATION.md](./CONFIGURATION.md) for details on supported configuration options.

## Observability

Silo is built for production and exports a healthy number of OpenTelemetry logs, metrics, and traces for consumption in your observability tool of choice. See [OBSERVABILITY.md](./OBSERVABILITY.md) for more details.

## Temporal VS Silo

Silo is heavily inspired by Temporal, but strikes a different balance for a different kind of workload.

**Key similarities**:

Silo and Temporal both are built for high throughput, low latency background execution.

Silo and Temporal are both durable and deeply concerned with dataloss -- there's no Redis disappearing acts that can lose jobs once ack'd.

Silo and Temporal are both horizontally and elastically scalable with no single point of failure.

**Key differences**:

Silo is built to run simpler single-step jobs, or short workflows, not many-month-long workflows. In spirit, Silo is similar to Sidekiq or Celery rather than Temporal.

Silo makes no distinction between workflows and activities, and there's no heavyweight client-side deterministic isolated execution environment. Silo clients just report progress as atomically committed forward steps which can be fetched on demand, instead of discrete events in a history that can never change.

Silo doesn't use an external visibility service. Instead, jobs can be searched for via some very simple predicates using Silo's built-in operator-facing SQL query capabilities.

Silo is built to be much cheaper to run -- data is stored in object storage via [slatedb](https://slatedb.io) rather than another datastore, there's no independent microservices that increase RPC overhead, and key functionality like rate limiting is built right in to minimize extra roundtrips to workers.

Silo isn't in production at Uber etc.

Silo is built to be autoscaled, with frequent cluster membership changes being just fine, and compute/storage separation baked in deeply.

## RPC reference

### `enqueue`

Adds a new job to the queue.

Accepts:

- job id (string, less than 128 characters, default: new random id)
- job priority (int, greater than 0 less than 100, priority 1 is the highest priority and will be executed first, priority 99 is the lowest priority)
- job start time (for future scheduling, default: now)
- job retry schedule (for re-enqueuing future attempts)
- job concurrency limits (accepts a concurrence queue name and the max concurrency that queue should execute with)
- job payload (opaque serialized data blob which will be passed to any workers)

Returns:

- the job's id
- or if the job already exists, a duplicate job id error

### `cancel`

Cancels a currently running job.

Accepts:

- job id

Returns:

- ok status if the job was successfully cancelled
- not found status if the job wasn't found
- failed status if the job couldn't be cancelled

### `start`

Starts or restarts a job.

If a job is currently paused before running its next attempt, starts that next attempt now. If a job has exhaused all its attempts, enqueues a new series of attempts to complete the job eventually.

Accepts:

- job id

Returns:

- ok status if the job was successfully restarted
- not found status if the job wasn't found
- failed status if the job couldn't be restarted

### `leaseTasks`

Run by workers to get a list of tasks to run next

Accepts:

- worker id
- max tasks

Returns

- a list of tasks to run
  - a task is:
  - a job descriptor with all the jobs data, including the payload
  - a heartbeat interval describing how often the worker must check in to consider the task leased still

Workers are expected to complete their leased tasks, or heartbeat every heartbeat interval at minimum to keep the lease active.

### `reportTaskOutcome`

Reports task outcomes back to the durable store.

Accepts:

- task id
- task outcome, which is either a serialized result, or a serialized failure
- TODO: figure out how to show results to operators in the UI even when serialized

Returns:

- ok status if the outcome was successfully recorded
- not found status if the task wasn't found
- error code if the outcome has already been recorded for this task

### `heartbeatTask`

Reports that a worker is still alive and working on a task

Accepts:

- task id
- optional opaque serialized heartbeat cursor data blob

Returns

- ok status if heartbeat was persisted
- not found status if task isnt found
- error status if the task has been leased out to a different worker, suggesting that this worker needs to abort the task

### `listJobs`

Returns a list of jobs that match the given filters

Accepts:

- an optional set of filters to limit which jobs are returned
- an optional cursor to continue listing from
- an optional limit of jobs to return

Returns

- a list of jobs

### `getJob`

Returns all details of one job

Accepts:

- a job id

Returns

- all details of the job if the job is found
- not found status if the job isn't found

### `deleteJob`

Deletes a job

Accepts:

- a job id to delete

Returns

- status ok if the job was found and deleted successfully
- status not found if the job wasn't found (or has already been deleted)
- status error if the job is currently running, it needs to be cancelled before it can be deleted

## Development

See `CONTRIBUTING.md` for details on how to work on the `silo` repo.
