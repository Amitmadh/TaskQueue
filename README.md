# TaskQueue

[![CI](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml/badge.svg)](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Typed](https://img.shields.io/badge/typed-strict-success.svg)](https://peps.python.org/pep-0561/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**A Python task queue built around structured concurrency and end-to-end type safety.** Jobs have parent-child relationships, failures cancel siblings instead of orphaning them, and type checker catches signature mismatches at `.delay()` call sites. SQLite for local development, Redis for production — same API.

> ⚠️ **Status:** Pre-alpha. Roadmap below.

---

## Why another task queue?

Celery, RQ, Dramatiq, and Arq all share two limitations that TaskQueue is designed to fix:

**Jobs float free.** When you `task.delay(...)` in Celery, the job is detached from the code that submitted it. If the calling process dies, the job keeps running. If the job fails, the caller has no idea. If you submit ten jobs and one fails, the other nine keep burning compute. Errors disappear into log files, cancellation is famously unreliable. the mental model is "fire and forget."

**Types are erased at the queue boundary.** `add.delay("oops", "wrong")` is happily accepted by every existing queue library. Your IDE has no idea what arguments a task takes, what `await job.result()` returns, or that you just typo'd a parameter name. The richest type system in mainstream Python is wasted the moment a job crosses the wire.

TaskQueue rejects both. Every job is owned by a *scope* that waits for it, propagates its errors, and cancels it if siblings fail. Every task preserves its full signature through `ParamSpec`, so the type checker is as strict at the call site as it is inside the function body.

## The three differentiators

**1. Structured concurrency, distributed.** Built on the same model as `asyncio.TaskGroup` and Trio's nurseries, extended across processes and machines. Scopes own jobs. Scope exit waits for all children. Sibling failures cancel the rest. Parent process death cancels children via heartbeat-based reaping. Exceptions propagate up the scope tree as `ExceptionGroup`s, the way real Python errors do.

**2. End-to-end type safety.** `ParamSpec` and `TypeVar` carry signatures through the decorator. `await job.result()` is correctly typed as the function's return type. Pyright in strict mode is the project's baseline, and the public API is designed to make `# type: ignore` unnecessary.

**3. Redis backend designed for correctness, not just throughput.** Reliable delivery via per-worker processing lists. Cross-process cancellation that actually works, via pub/sub plus polling fallback. Scope-aware reaping so a dead parent process never leaves zombie children. SQLite backend available for local development and small deployments — same Protocol, same API.

## How TaskQueue compares

| Feature                            | Celery | RQ   | Dramatiq | Arq  | TaskQueue |
| -----------------------------------| ------ | ---- | -------- | ---- | --------- |
| Async-native worker                |   v    |  x   |    x     |  v   |     v     |
| Type-safe `.delay()` (ParamSpec)   |   x    |  x   |    x     |  x   |     v     |
| Structured concurrency / scopes    |   x    |  x   |    x     |  x   |     v     |
| Reliable cross-process cancel      |   x    |  x   |    x     |  v   |     v     |
| SQLite backend                     |   x    |  x   |    x     |  x   |     v     |
| Redis backend                      |   v    |  v   |    v     |  v   |     v     |
| `ExceptionGroup` error propagation |   x    |  x   |    x     |  x   |     v     |

TaskQueue does *not* try to replace Celery for everything. It's deliberately narrower: no built-in cron, no web dashboard, no RabbitMQ. The goal is a smaller, more opinionated tool that's correct by construction.

## Design Choices

**No orphaned jobs.** Every job has a parent scope. The only way to "fire and forget" is to spawn into the explicit `root_group()`, which makes the choice visible in the code. This isn't a restriction — it's the property that makes everything else (reliable cancellation, error propagation, observability) tractable.

**Errors have somewhere to go.** Failures are exceptions, raised from the `async with` block of the owning scope. You never have to grep logs to find out a background job died.

**The Protocol is the contract.** Backends, serializers, and middleware are `typing.Protocol`s, not abstract base classes. Bring your own implementation without inheriting from anything.

**Opinionated defaults, escape hatches everywhere.** SQLite is the default but Redis is one line of config. Pickle is the default serializer but JSON and msgpack are first-class. Strict scopes are the default but `on_error="collect"` exists when you need it.

## Requirements

- Python 3.11+ (for `ExceptionGroup` and `TaskGroup`)
- SQLite 3.35+ (ships with Python 3.11)
- Optional: Redis 6+ for the Redis backend
- Optional: OpenTelemetry SDK for distributed tracing

## Architecture

A short tour of how the pieces fit together:

- **`Queue`** is the user-facing facade. Holds the backend, the task registry, and creates scopes.
- **`Task`** is what `@q.task` produces — a callable that preserves its original signature via `ParamSpec`, plus a `.delay()` that enqueues.
- **`Job`** is the serialized unit of work in transit (id, task name, args, scope id, status).
- **`JobGroup`** is the structured-concurrency scope. Owns child jobs. Its `__aexit__` waits for them.
- **`Backend`** is a `Protocol` for the persistence layer. The same Protocol is satisfied by the in-memory, SQLite, and Redis implementations.
- **`Worker`** consumes jobs from a backend, executes them, and reports results. Async-native; multiprocessing for fan-out.
- **Reaper** runs in every worker. Detects scopes whose owning processes have stopped heartbeating and cancels their children.

See [`docs/architecture.md`](docs/architecture.md) for a deeper dive, including the cancellation protocol and the heartbeat-based reaping algorithm.

## Inspiration

The structured-concurrency design owes an enormous debt to:

- Nathaniel J. Smith's [Notes on structured concurrency, or: Go statement considered harmful](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/)
- The Trio project, particularly its nursery model
- PEP 654 (`ExceptionGroup`) and the design of `asyncio.TaskGroup`
- Temporal's workflow-as-code model, particularly the deterministic replay ideas

Everything we got wrong is ours alone.

## License

MIT. See [LICENSE](LICENSE).