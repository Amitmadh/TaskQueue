# TaskQueue

[![CI](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml/badge.svg)](https://github.com/Amitmadh/TaskQueue/actions/workflows/ci.yaml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Typed](https://img.shields.io/badge/typed-strict-success.svg)](https://peps.python.org/pep-0561/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**A Python task queue built around structured concurrency and end-to-end type safety.** Jobs have parent-child relationships, failures cancel siblings instead of orphaning them, and type checker catches signature mismatches at `.delay()` call sites. SQLite for local development, Redis for production — same API.

> **Status:** pre-alpha, in active development. The API is still moving — see the roadmap below.

---

## Why I'm building this

I spent a while reading the source of celery, RQ and Arq and two things kept bothering me about all of them.

The first is that jobs are detached from the code that submits them. When you call `task.delay(...)` in Celery, the job becomes an orphan: if the caller dies it keeps running, if it fails the caller doesn't find out, and if you submit ten jobs and one fails the other nine keep going. The mental model is "fire and check the logs later." That works at scale but it's a strange default for a language whose normal concurrency model — async/await, exceptions, `with` blocks — is built around exactly the opposite assumption.

The second is that the queue boundary erases types. `add.delay("oops", "wrong")` is accepted by every existing library I tried. Modern Python has `ParamSpec`, `TypeVar`, and `Protocol`; none of those tools show up at the place where you most need them. You lose type checking precisely when you cross processes, which is when bugs hurt the most.

This project is my attempt to do both differently. It's also the thing I'm using to get comfortable with the harder parts of async Python and distributed-systems plumbing — distributed locks, pub/sub-vs-polling trade-offs, cooperative cancellation, that kind of thing.

## What's different

**Structured concurrency, distributed.** Built on the same idea as `asyncio.TaskGroup` and Trio's nurseries, extended across processes. Jobs are spawned into a *scope* (`JobGroup`). The scope's `async with` block doesn't exit until every child reaches a terminal state. If one child fails, its siblings are cancelled and the exception propagates up the scope tree. If the process holding the scope dies, a heartbeat-based reaper cancels its children so nothing is orphaned.

**End-to-end type safety.** `@q.task` preserves the wrapped function's signature via `ParamSpec`, so `add.delay(2, 3)` is type-checked against `add`'s signature and `await handle.result()` is correctly typed as `int`. The whole codebase runs under Pyright in strict mode.

**Pluggable backends behind a `Protocol`.** Redis is the first and primary backend (`BLMOVE` for reliable delivery, pub/sub for result notification, sorted sets for scheduled jobs). The `Backend` interface is a `typing.Protocol`, not an ABC, which means a different store (SQLite, Postgres, in-memory for tests) can be slotted in without inheriting from anything. The in-memory backend is the one tests run against most of the time.

## Comparison with existing queues

| Feature                            | Celery | RQ   | Dramatiq | Arq  | TaskQueue |
| -----------------------------------| ------ | ---- | -------- | ---- | --------- |
| Async-native worker                |   v    |  x   |    x     |  v   |     v     |
| Type-safe `.delay()` (ParamSpec)   |   x    |  x   |    x     |  x   |     v     |
| Structured concurrency / scopes    |   x    |  x   |    x     |  x   |     v     |
| Reliable cross-process cancel      |   x    |  x   |    x     |  v   |     v     |
| SQLite backend                     |   x    |  x   |    x     |  x   |     v     |
| Redis backend                      |   v    |  v   |    v     |  v   |     v     |
| `ExceptionGroup` error propagation |   x    |  x   |    x     |  x   |     v     |
| Years of production hardening      |   v    |  v   |    v     |  x   |     x     |

If you're putting something in production today, use Celery. This project's value is in the design experiment.

## Design Choices

**No orphaned jobs.** Every job has a parent scope. The only way to "fire and forget" is to spawn into the explicit `root_group()`, which makes the choice visible in the code. This isn't a restriction — it's the property that makes everything else (reliable cancellation, error propagation, observability) tractable.

**Errors have somewhere to go.** Failures are exceptions, raised from the `async with` block of the owning scope. You never have to grep logs to find out a background job died.

**The Protocol is the contract.** Backends, serializers, and middleware are `typing.Protocol`s, not abstract base classes. Bring your own implementation without inheriting from anything.

**Opinionated defaults, escape hatches everywhere.** SQLite is the default but Redis is one line of config. Pickle is the default serializer but JSON and msgpack are first-class. Strict scopes are the default but `on_error="collect"` exists when you need it.

## Roadmap

I'm building this in vertical slices — each phase ends with a working demo and a git tag.

- [ ] **Phase 0** — Scaffolding: tooling, CI, lint, strict types, tests
- [ ] **Phase 1** — In-memory queue, `@task` with `ParamSpec`, basic worker
- [ ] **Phase 2** — `JobGroup` scopes, fail-fast and collect modes, nested scopes
- [ ] **Phase 3** — Redis backend with reliable delivery, multi-process workers, CLI
- [ ] **Phase 4** — Cross-process cancellation, heartbeat-based scope reaping
- [ ] **Phase 5** — Retries, structured logging, metrics, middleware
- [ ] **Phase 6** — OpenTelemetry instrumentation
- [ ] **Phase 7** — Documentation
- [ ] **Post-v1.0** — SQLite backend (to validate the Protocol abstraction)

See the [changelog](CHANGELOG.md) for what's actually done.

## Requirements

- Python 3.12+ (for `ExceptionGroup` and `TaskGroup`)
- SQLite 3.35+ (ships with Python 3.12)
- Optional: Redis 6+ for the Redis backend
- Optional: OpenTelemetry SDK for distributed tracing

## Architecture

A quick tour of the pieces, in roughly the order they execute:

- `Queue` is the user-facing facade. Holds the backend, the task registry, and creates scopes.
- `Task` is what `@q.task` produces — a callable that keeps the original signature via `ParamSpec` and adds `.delay()` for enqueueing.
- `Job` is the serialized unit of work that crosses the wire (id, task name, args, scope id, status).
- `JobGroup` is the structured-concurrency scope. Its `__aexit__` blocks until all children finish or are cancelled.
- `Backend` is the `Protocol` for persistence. Implementations so far: `MemoryBackend`, `RedisBackend`.
- `Worker` pulls jobs from a backend and runs them, async-native, with a small executor that handles cancellation injection.
- The reaper runs inside every worker. It detects scopes whose owning processes have stopped heartbeating and cancels their children.

I'll write a longer `docs/architecture.md` once the design has stopped moving — probably after Phase 4. The Redis-specific patterns (reliable delivery via processing lists, pub/sub-plus-polling for race-free result waits, distributed lock for the reaper) are the things I most want to document properly.

## Inspiration

The structured-concurrency design owes an enormous debt to:

- Nathaniel J. Smith's [Notes on structured concurrency, or: Go statement considered harmful](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/)
- The Trio project, particularly its nursery model
- PEP 654 (`ExceptionGroup`) and the design of `asyncio.TaskGroup`
- Temporal's workflow-as-code model, particularly the deterministic replay ideas

Everything we got wrong is ours alone.

## License

MIT. See [LICENSE](LICENSE).
