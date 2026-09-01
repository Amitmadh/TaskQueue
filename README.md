# TaskQueue

[![CI](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml/badge.svg)](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Typed](https://img.shields.io/badge/typed-strict-success.svg)](https://peps.python.org/pep-0561/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**A Python task queue built around structured concurrency and end-to-end type safety.** Jobs have parent-child relationships, failures cancel siblings instead of orphaning them, and the type checker catches signature mismatches at `spawn()` call sites. Redis for production and an in-memory backend for tests and single-process use, behind the same API; SQLite is planned.

![A checkout run twice. The first goes through; the second is declined half a second in, and the scope cancels the two checks that are still running instead of letting them finish.](docs/media/nested_scopes_cancel_on_failure.gif)

<sub>`examples/nested_scopes_cancel_on_failure.py` — run 1 fans three checks out into one scope and waits for all of them; run 2 fails fast, and the wall clock is the point.</sub>

> **Status:** pre-alpha, in active development. The API is still moving — see the roadmap below.

---

## What's different

**Structured concurrency, distributed.** Built on the same idea as `asyncio.TaskGroup` and Trio's nurseries, extended across processes. Jobs are spawned into a *scope* (`JobGroup`). The scope's `async with` block doesn't exit until every child reaches a terminal state. If one child fails, its siblings are cancelled and the failure propagates up the scope tree. Cancellation crosses process boundaries: `handle.cancel()` here stops a job already running on a worker over there. If a *worker* process dies, a heartbeat-based reaper returns its in-flight jobs to the queue so nothing is stranded. What it deliberately does **not** do is cancel a dead *producer's* children — see [What survives a crash](#what-survives-a-crash-and-what-doesnt).

**End-to-end type safety.** `@q.task` preserves the wrapped function's signature via `ParamSpec`, so `g.spawn(add, 2, 3)` is type-checked against `add`'s signature and `await handle.result()` is correctly typed as `int`. The whole codebase runs under Pyright in strict mode.

**Pluggable backends behind a `Protocol`.** The `Backend` interface is a `typing.Protocol`, not an ABC, so a different store (Redis, SQLite, Postgres) can be slotted in without inheriting from anything. Two are built: `MemoryBackend` for tests and single-process use, and `RedisBackend` for real deployments — `BLMOVE` into a per-worker processing list for reliable delivery, pub/sub for result and cancellation notification, Lua for every check-then-act guard. Both are held to the same conformance suite, which runs twice: once against each. SQLite comes after v1.0.

## Example

Ten jobs run in parallel inside a scope. One fails — and instead of the other nine running to completion while you learn about it from the logs, the scope cancels them and raises the failure where you can catch it.

`namespace="demo"` fixes the task names to `demo.work` and `demo.boom` no matter how this file is loaded. Leave it out and names default to the module's import path, which is right for a module that is only ever imported — but a module you run as a script has no stable import path, so the decorator refuses to guess and raises `TaskNameError`.

```python
import asyncio

from TaskQueue import MemoryBackend, Queue

q = Queue(MemoryBackend(), namespace="demo")


@q.task
async def work(n: int) -> int:
    await asyncio.sleep(1)        # still running when its sibling fails
    return n * n


@q.task
async def boom() -> int:
    raise RuntimeError("job hit a wall")


async def main() -> None:
    async with q.worker(concurrency=10):
        try:
            async with q.group() as group:  # on_error="cancel_siblings" by default
                for i in range(9):
                    await group.spawn(work, i)      # nine slow jobs...
                await group.spawn(boom)             # ...and one that fails fast
        except* RuntimeError as eg:
            print(f"scope failed, siblings cancelled: {eg.exceptions}")


asyncio.run(main())
```

The queue boundary keeps your types intact, too — `@q.task` preserves the signature via `ParamSpec`:

```python
async with q.group() as g:
    handle = await g.spawn(work, 3)     # JobHandle[int]
    total: int = await handle.result()  # typed as int

    await g.spawn(work, "three")        # Pyright: Argument of type "str" cannot be assigned to "int"
```

The same class of mistake, caught in the editor rather than at runtime:

![An editor showing a spawn call passing the string "twelve" where an int is required, with the type checker reporting that Literal['twelve'] is not assignable to parameter "incoming" of type "int".](docs/media/type_hints.png)

When fail-fast isn't what you want: `group(on_error="collect")` runs every job and raises a `BaseExceptionGroup` of all failures, and `group(deadline=5.0)` cancels the whole scope if it outruns its deadline.

Each of these behaviours — fan-out, fail-fast, explicit cancellation, deadlines, `on_error="collect"` — has a runnable version in [`examples/`](examples/), alongside demonstrations of cross-process cancellation and of a worker killed mid-job while a peer reclaims its work. [`examples/README.md`](examples/README.md) maps the five scripts and says which of them need a running Redis.

## Running a worker

The example above runs its worker in-process, which is all the in-memory backend can do —
it keeps its queue in an `asyncio.Queue`, so a second process gets its own empty one. Point
the same `Queue` at Redis and workers become separate processes on separate machines, with
no change to the task or scope code above:

```python
import redis.asyncio as redis

from TaskQueue import Queue
from TaskQueue.backends.redis_backend import RedisBackend

q = Queue(RedisBackend(redis.from_url("redis://localhost:6379/0")), namespace="myapp")
```

`RedisBackend` is imported from its own module rather than the package root, because the
`redis` driver is an optional extra (`pip install TaskQueue[redis]`) and the core installs
without it. Then:

```console
$ taskqueue worker myapp.tasks:q --concurrency 4
serving 3 task(s): myapp.add, myapp.fetch, myapp.crunch
worker pool started (concurrency=4)
worker ready (concurrency=4); ctrl-c to stop
^C
signal received; draining (timeout=30.0s, ctrl-c again to cancel)
drain complete; stopping worker pool
```

Ctrl-C drains rather than kills: the pool stops claiming and lets in-flight jobs finish.
A second Ctrl-C, or `--drain-timeout` elapsing, cancels them — and a cancelled job is
returned to the queue rather than lost. If a worker dies without either (`SIGKILL`, a
power cut), its peers notice the missing heartbeat after `worker_ttl` and reclaim its
jobs, so the guarantee is at-least-once in every case.

The target is an import string — `<module>:<queue>` — not a set of `--backend` flags. A
worker has to import your task module regardless, because importing it is what runs the
`@q.task` decorators, so your module stays the single place that decides which backend and
serializer to use. That makes it impossible for a producer and a worker to disagree about
serialization. `taskqueue backends` and `taskqueue serializers` list what the package
ships.

## What survives a crash, and what doesn't

A scope cancels its children whenever Python still runs: a sibling fails, the body
raises, the deadline passes, or the owning task is cancelled. That covers every
ordinary ending, including Ctrl-C on the process that owns the scope. Cancellation is
delivered through the backend, so it reaches a job already running in another process
— and a dropped notification costs latency, not correctness, because the waiting side
re-reads the durable record rather than trusting the message to arrive.

What it does not cover is a process killed outright — `SIGKILL`, OOM, a pulled plug.
Its jobs keep running to completion and their results expire unread; you pay worker
time for work nobody is waiting on. Detecting that automatically would mean inferring
a producer's liveness from a heartbeat, and a beat that goes quiet because a process is
busy looks exactly like one that went quiet because it died — so the cost of getting it
wrong is cancelling work that is still running. Wasting dead work is the cheaper
mistake, and it is the one this library makes.

Jobs that were *in flight on a dead worker* are a different matter, and those are
reclaimed — that is the reaper, described above.

![Two workers packing an order each. One is killed outright; its heartbeat goes silent, the surviving worker reaps the lease, and the order is requeued and packed on a second attempt.](docs/media/worker_crash_job_reclaimed.gif)

<sub>`examples/worker_crash_job_reclaimed.py` — watch worker A's beat go stale, its lease return to the queue, and `attempts` for that order go from 1 to 2. Nothing re-submits it by hand.</sub>

## Comparison with existing queues

| Feature                            | Celery    | RQ        | Dramatiq  | Arq | TaskQueue           |
| -----------------------------------| --------- | --------- | --------- | --- | ------------------- |
| Async-native worker                | ✗ ¹       | ✗         | partial ² | ✓   | ✓                   |
| Type-safe enqueue (ParamSpec)      | ✗         | ✗         | ✗         | ✗   | ✓                   |
| Structured concurrency / scopes    | ✗         | ✗         | ✗         | ✗   | ✓                   |
| Cross-process cancellation         | partial ³ | partial ⁴ | ✗         | ✓   | ✓                   |
| SQLite backend                     | partial ⁵ | ✗         | ✗         | ✗   | planned (post-v1.0) |
| Redis backend                      | ✓         | ✓         | ✓         | ✓   | ✓                   |
| `ExceptionGroup` error propagation | ✗         | ✗         | ✗         | ✗   | ✓                   |
| Years of production hardening      | ✓         | ✓         | ✓         | ✓   | ✗                   |

¹ No official asyncio worker; async tasks require third-party worker pools (e.g. celery-aio-pool).
² `async def` actors are supported via the AsyncIO middleware (an event-loop thread per worker process); the worker itself is thread-based.
³ `revoke(terminate=True)` kills the process running the task; best-effort and pool-dependent.
⁴ `send_stop_job_command()` stops a running job from another process; stopped jobs go to the failed registry and are not retried.
⁵ SQLite works as a result backend via SQLAlchemy (`db+sqlite://`) and as a broker via kombu's SQLAlchemy transport, which is officially experimental.

If you're putting something in production today, use Celery. This project's value is in the design experiment.

## Design Choices

**No orphaned jobs.** Every job has a parent scope — enforced by the API, not by convention: enqueueing takes a scope id, so `spawn` is the only way in. The only way to "fire and forget" is to spawn into the explicit `root_group()`, which makes the choice visible in the code. This isn't a restriction — it's the property that makes everything else (reliable cancellation, error propagation, observability) tractable.

**Errors have somewhere to go.** Failures are exceptions, raised from the `async with` block of the owning scope. You never have to grep logs to find out a background job died.

**The Protocol is the contract.** Backends, serializers, and middleware are `typing.Protocol`s, not abstract base classes. Bring your own implementation without inheriting from anything.

**Opinionated defaults, escape hatches everywhere.** Swapping `MemoryBackend` for `RedisBackend` is one line of config, and SQLite (post-v1.0) will be another. JSON is the default serializer — dependency-free and portable — with Pickle available when you need Python-native objects. Strict scopes are the default but `on_error="collect"` exists when you need it.

## Roadmap

I'm building this in vertical slices — each phase ends with a working demo and a git tag.

- [x] **Phase 0** — Scaffolding: tooling, CI, lint, strict types, tests
- [x] **Phase 1** — In-memory queue, `@task` with `ParamSpec`, basic worker
- [x] **Phase 2** — `JobGroup` scopes, fail-fast/collect/ignore modes, deadlines, cooperative cancellation, nested scopes
- [x] **Phase 3** — Redis backend with reliable delivery, multi-process workers, graceful drain, heartbeat-based reclaim of dead workers' jobs, CLI
- [x] **Phase 4** — Cross-process cancellation, with a poll backup so a dropped notification cannot strand a waiter
- [ ] **Phase 5** — Retries, structured logging, metrics, middleware
- [ ] **Phase 6** — OpenTelemetry instrumentation
- [ ] **Phase 7** — Documentation
- [ ] **Post-v1.0** — SQLite backend (to validate the Protocol abstraction)

See the [changelog](CHANGELOG.md) for what's actually done.

## Requirements

- Python 3.12+ (for PEP 695 type-parameter syntax — `ExceptionGroup` and `TaskGroup` only need 3.11)
- Optional: Redis 6.2+ for the Redis backend — `BLMOVE` and `ZRANGE ... BYSCORE` both need 6.2
- Optional, planned (post-v1.0): SQLite 3.35+ for the SQLite backend
- Optional, planned (Phase 6): OpenTelemetry SDK for distributed tracing

## Architecture

A quick tour of the pieces, in roughly the order they execute:

- `Queue` is the user-facing facade. Holds the backend, the task registry, and creates scopes.
- `Task` is what `@q.task` produces — a callable that keeps the original signature via `ParamSpec` and adds `.submit(group_id, job_id, ...)`, which `JobGroup.spawn` calls to enqueue.
- `Job` is the serialized unit of work that crosses the wire (id, task name, args, scope id, status).
- `JobGroup` is the structured-concurrency scope. Its `__aexit__` blocks until all children finish or are cancelled.
- `Backend` is the `Protocol` for persistence. Built: `MemoryBackend` and `RedisBackend`; SQLite comes after v1.0.
- `Worker` pulls jobs from a backend and runs them, async-native, with a small executor that handles cancellation injection.
- The reaper runs inside every worker. Each process writes its own timestamp into a `workers` sorted set every few seconds; any worker whose last beat is older than `worker_ttl` is presumed dead and its processing list is drained back onto the queue. It reclaims **leases** — a scope's children are cancelled by the scope itself, in the process that owns it.

[**`docs/architecture.md`**](docs/architecture.md) is the longer version, written now
that Phase 4 has stopped the design moving. It covers the Redis-specific patterns properly:
reliable delivery via per-worker processing lists, the two-step lease and everything that
follows from it, subscribe-then-check plus the poll backup for race-free waits, the
cancellation protocol, and why the reaper needs **no** distributed lock — the reclaim is a
single atomic Lua script, so concurrent reapers cost a wasted round trip rather than a
duplicated job, and a lock that expired mid-operation would be the only way to break that.

## Inspiration

The structured-concurrency design owes an enormous debt to:

- Nathaniel J. Smith's [Notes on structured concurrency, or: Go statement considered harmful](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/)
- The Trio project, particularly its nursery model
- PEP 654 (`ExceptionGroup`) and the design of `asyncio.TaskGroup`
- Temporal's workflow-as-code model, particularly the deterministic replay ideas

Everything we got wrong is ours alone.

## License

MIT. See [LICENSE](LICENSE).
