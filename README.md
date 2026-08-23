# TaskQueue

[![CI](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml/badge.svg)](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Typed](https://img.shields.io/badge/typed-strict-success.svg)](https://peps.python.org/pep-0561/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**A Python task queue built around structured concurrency and end-to-end type safety.** Jobs have parent-child relationships, failures cancel siblings instead of orphaning them, and the type checker catches signature mismatches at `.submit()` call sites. Redis for production and an in-memory backend for tests and single-process use, behind the same API; SQLite is planned.

> **Status:** pre-alpha, in active development. The API is still moving — see the roadmap below.

---

## What's different

**Structured concurrency, distributed.** Built on the same idea as `asyncio.TaskGroup` and Trio's nurseries, extended across processes. Jobs are spawned into a *scope* (`JobGroup`). The scope's `async with` block doesn't exit until every child reaches a terminal state. If one child fails, its siblings are cancelled and the failure propagates up the scope tree. If a *worker* process dies, a heartbeat-based reaper returns its in-flight jobs to the queue so nothing is stranded. Applying the same mechanism to a dead *scope owner* — cancelling its children rather than reclaiming its leases — is Phase 4.

**End-to-end type safety.** `@q.task` preserves the wrapped function's signature via `ParamSpec`, so `add.submit(2, 3)` is type-checked against `add`'s signature and `await handle.result()` is correctly typed as `int`. The whole codebase runs under Pyright in strict mode.

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
handle = await work.submit(3)       # JobHandle[int]
total: int = await handle.result()  # typed as int

await work.submit("three")          # Pyright: Argument of type "str" cannot be assigned to "int"
```

When fail-fast isn't what you want: `group(on_error="collect")` runs every job and raises a `BaseExceptionGroup` of all failures, and `group(deadline=5.0)` cancels the whole scope if it outruns its deadline.

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

## Comparison with existing queues

| Feature                            | Celery    | RQ        | Dramatiq  | Arq | TaskQueue           |
| -----------------------------------| --------- | --------- | --------- | --- | ------------------- |
| Async-native worker                | ✗ ¹       | ✗         | partial ² | ✓   | ✓                   |
| Type-safe `.submit()` (ParamSpec)  | ✗         | ✗         | ✗         | ✗   | ✓                   |
| Structured concurrency / scopes    | ✗         | ✗         | ✗         | ✗   | ✓                   |
| Cross-process cancellation         | partial ³ | partial ⁴ | ✗         | ✓   | planned (Phase 4)   |
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

**No orphaned jobs.** Every job has a parent scope. The only way to "fire and forget" is to spawn into the explicit `root_group()`, which makes the choice visible in the code. This isn't a restriction — it's the property that makes everything else (reliable cancellation, error propagation, observability) tractable.

**Errors have somewhere to go.** Failures are exceptions, raised from the `async with` block of the owning scope. You never have to grep logs to find out a background job died.

**The Protocol is the contract.** Backends, serializers, and middleware are `typing.Protocol`s, not abstract base classes. Bring your own implementation without inheriting from anything.

**Opinionated defaults, escape hatches everywhere.** Swapping `MemoryBackend` for `RedisBackend` is one line of config, and SQLite (post-v1.0) will be another. JSON is the default serializer — dependency-free and portable — with Pickle available when you need Python-native objects. Strict scopes are the default but `on_error="collect"` exists when you need it.

## Roadmap

I'm building this in vertical slices — each phase ends with a working demo and a git tag.

- [x] **Phase 0** — Scaffolding: tooling, CI, lint, strict types, tests
- [x] **Phase 1** — In-memory queue, `@task` with `ParamSpec`, basic worker
- [x] **Phase 2** — `JobGroup` scopes, fail-fast/collect/ignore modes, deadlines, cooperative cancellation, nested scopes
- [x] **Phase 3** — Redis backend with reliable delivery, multi-process workers, graceful drain, heartbeat-based reclaim of dead workers' jobs, CLI
- [ ] **Phase 4** — Cross-process cancellation, heartbeat-based scope reaping
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
- `Task` is what `@q.task` produces — a callable that keeps the original signature via `ParamSpec` and adds `.submit()` for enqueueing.
- `Job` is the serialized unit of work that crosses the wire (id, task name, args, scope id, status).
- `JobGroup` is the structured-concurrency scope. Its `__aexit__` blocks until all children finish or are cancelled.
- `Backend` is the `Protocol` for persistence. Built: `MemoryBackend` and `RedisBackend`; SQLite comes after v1.0.
- `Worker` pulls jobs from a backend and runs them, async-native, with a small executor that handles cancellation injection.
- The reaper runs inside every worker. Each process writes its own timestamp into a `workers` sorted set every few seconds; any worker whose last beat is older than `worker_ttl` is presumed dead and its processing list is drained back onto the queue. Phase 4 points the same machinery at scope owners, cancelling their children instead of reclaiming leases.

I'll write a longer `docs/architecture.md` once the design has stopped moving — probably
after Phase 4. The Redis-specific patterns are the things I most want to document properly:
reliable delivery via per-worker processing lists, subscribe-then-check for race-free result
waits, and why the reaper needs **no** distributed lock — the reclaim is a single atomic Lua
script, so concurrent reapers cost a wasted round trip rather than a duplicated job, and a
lock that expired mid-operation would be the only way to break that.

## Inspiration

The structured-concurrency design owes an enormous debt to:

- Nathaniel J. Smith's [Notes on structured concurrency, or: Go statement considered harmful](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/)
- The Trio project, particularly its nursery model
- PEP 654 (`ExceptionGroup`) and the design of `asyncio.TaskGroup`
- Temporal's workflow-as-code model, particularly the deterministic replay ideas

Everything we got wrong is ours alone.

## License

MIT. See [LICENSE](LICENSE).
