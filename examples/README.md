# Examples

Five runnable programs, in the order they are worth reading. Each one is a
complete script with a full docstring; this file is just the map.

They are all the same fictional store: a catalogue, a stockroom, a checkout, and
a packing line. The store is only set dressing, but it is consistent across the
five, so a task name means the same thing wherever you meet it and the examples
read as one system rather than five unrelated snippets.

| file | what it shows | backend |
| --- | --- | --- |
| [`one_job_round_trip.py`](one_job_round_trip.py) | the whole API in one short file: define a task, spawn it, await a typed result, with `TaskQueue` logging at `DEBUG` so you can watch the machinery | in-memory |
| [`scope_semantics_tour.py`](scope_semantics_tour.py) | six scope behaviours back to back (round-trip, fan-out, fail-fast, cancellation, deadline, `on_error="collect"`) over catalogue lookups and a slow stocktake | in-memory |
| [`nested_scopes_cancel_on_failure.py`](nested_scopes_cancel_on_failure.py) | a job that is itself a producer: a declined card unwinds a whole subtree of stock checks | **Redis** |
| [`cancel_crosses_processes.py`](cancel_crosses_processes.py) | `handle.cancel()` here stops a catalogue reprice running over there, and still does when the notification is lost | **Redis** |
| [`worker_crash_job_reclaimed.py`](worker_crash_job_reclaimed.py) | `SIGKILL` a worker mid-order; a surviving peer reaps the lease and packs it | **Redis** |

One file is not on that list. [`typing_error.py`](typing_error.py) is not meant to run: it holds a single call that must *not* type-check, pinned by a `# pyright: ignore` that the build only tolerates while the line really is an error. Its docstring explains the mechanism.

## Before you run the Redis three

> **They call `flushdb()` on startup.** All three connect to `localhost:6379`
> and clear database 0 before doing anything, because stranded leases from an
> earlier run hide in `processing:*` rather than in `queue` and would otherwise
> make the output lie. Do not point them at a Redis you care about.

```console
$ docker run --rm -p 6379:6379 redis
```

Each example sets its own `namespace=` (`stockroom`, `storefront`, `checkout`,
`catalog`, `fulfillment`) so the three Redis ones can share one server without
their task names colliding.

The first two need no server at all. They also cannot be split across two
terminals: `MemoryBackend` keeps its queue in an `asyncio.Queue`, so a second
process gets its own empty one and both sides block forever. Anything
cross-process needs Redis.

## Running them

```console
$ uv run python examples/one_job_round_trip.py
$ uv run python examples/scope_semantics_tour.py
$ uv run python examples/nested_scopes_cancel_on_failure.py
$ uv run python examples/cancel_crosses_processes.py
$ uv run python examples/worker_crash_job_reclaimed.py
```

Run them from the repository root. The last two start their own worker
processes (`python -m TaskQueue worker <module>:q`) and each subprocess imports
the example module itself, so a demo's settings (a deliberately short
`worker_ttl`, a 300-second job) cannot leak into its neighbours.

## What to watch for

**`nested_scopes_cancel_on_failure.py`** runs a checkout twice. The first run is
about the wall clock: three checks that add up to 6.5s of work finish in about
3s, and the step that depends on them cannot start early because the scope will
not exit until all three are terminal. The second run declines the card half a
second in, and the cancellation travels *through* the check that spawned its own
warehouse jobs into the jobs it spawned. Note `CONCURRENCY = 6`: a parent
occupies a worker slot for as long as it waits on its children, so a pool that
cannot hold the parent and its children at once deadlocks; at `concurrency=1`
this file hangs outright.

**`cancel_crosses_processes.py`** is two acts, and the second is the interesting
one. It sets the cancel flag by hand and publishes nothing, which is what a
dropped `PUBLISH` looks like from the worker's side. The reprice still stops, one
poll interval later, because the waiting side re-reads the durable record
instead of trusting the message to arrive. A lost notification costs latency
rather than correctness.

**`worker_crash_job_reclaimed.py`** uses `worker_ttl=6` against a
`--heartbeat-interval 2` so a dead worker is noticed in seconds rather than at
the 30s default. Reclaim takes `worker_ttl` plus one heartbeat interval of the
*surviving* worker, so expect the requeue roughly 6-8s after the kill.
