# Silo

A background job queueing system built on top of object storage via [slatedb](https://slatedb.io)

Status: crappy experiment

### Features

- high throughput, durable job brokering server
- low cost at scale by using object storage as the source of truth
- high-cardinality, cheap multitenancy built in
- future scheduling
- concurrency limits for limiting throughput of various jobs (with high cardinality and limit change support)
- compute/storage separation for elastic compute scaling
- tracks job results
- tracks job and attempt history for operators
- allows searching for jobs with a few different filters
- brokers work for userland workers in any language that communicate with the broker via RPCs

It's like Sidekiq, BullMQ or similar, but durable.

## RPCs

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

## Development (Flakes)

This repo uses [Flakes](https://nixos.asia/en/flakes) from the get-go.

```bash
# Dev shell
nix develop

# or run via cargo
nix develop -c cargo run

# build
nix build
```
