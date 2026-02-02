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

## Docs

See https://gadget-inc.github.io/silo/ for all the documentation, including guides and reference materials.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details on setting up a development environment.
