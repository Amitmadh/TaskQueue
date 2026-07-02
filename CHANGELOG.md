# Changelog

All notable changes documented here.

## [0.2.0] - 2026-07-02

### Added

- **Phase 2 — structured-concurrency scopes (`JobGroup`).** `queue.group(...)` opens the
  everyday scope, used as `async with`: `await group.spawn(task, *args)` enqueues a child
  and returns its `JobHandle`, and the scope's `__aexit__` does not return until every
  child reaches a terminal state. `queue.root_group(...)` is the same scope but marks an
  explicit, detached fire-and-forget root.
- **Three failure policies** via `on_error`: `"cancel_siblings"` (default) cancels the
  still-running siblings on the first failure and raises a `BaseExceptionGroup`;
  `"collect"` runs every child and raises a `BaseExceptionGroup` of all failures;
  `"ignore"` runs every child and swallows failures.
- **Scope deadlines.** `group(deadline=<seconds>)` cancels the whole scope and
  raises `TimeoutError` if it outlives the deadline.
- **Job cancellation.** `Backend.request_cancel` / `Backend.wait_cancel` and
  `JobHandle.cancel()`, plus a terminal `JobStatus.CANCELLED`. The signal is carried as a
  durable `request_cancel` record field (read once at claim, for jobs cancelled while
  still queued) together with an event notification (to interrupt a job already running) —
  the field-plus-notify split that maps onto a Redis hash field plus pub/sub.
- **Batch cancellation.** `Backend.request_cancel_many` cancels a whole scope's children
  in one call; the group's fail-fast and deadline teardown paths use it, so cancelling N
  siblings is a single backend round-trip instead of N.
- **`JobHandle.result()`** raises `RuntimeError` on a failed job and the new
  **`JobCancelled`** exception (exported from `TaskQueue`) on a cancelled one — a plain
  `Exception`, so `except Exception` catches it and it is never confused with task
  cancellation.
- **Consume-to-free results.** `JobHandle.result()` caches the terminal outcome on the
  handle (shared by repeat and concurrent callers) and frees the backing record, so a
  backend stays bounded for results that are consumed. Fetch-and-free is a single
  **`Backend.take_result`** call (see Changed), which maps onto Redis `GETDEL`.

### Changed

- `Backend.wait_for` became **`Backend.take_result`**: it blocks until the job is
  terminal, returns the record, *and* frees it in one call — folding the former separate
  wait, read, and discard steps into a single round-trip (Redis `GETDEL`).
- `root_group()` is now a real `JobGroup` scope, replacing the Phase 1 no-op placeholder.
- `Backend.request_cancel` is an idempotent no-op on a job that is already finished or
  unknown (e.g. after its result was consumed), instead of raising.

## [0.1.0]

### Added

- **Phase 1 — in-memory queue.** `Queue` facade with the `@q.task` decorator in
  both bare (`@q.task`) and parameterized (`@q.task(name=..., max_retries=...)`)
  forms, plus an async `Worker` pool used as `async with queue.worker(concurrency=...)`.
- **End-to-end typing.** `Task.submit()` preserves the wrapped function's
  signature via `ParamSpec`, and `JobHandle[R].result()` is typed as `R`. The
  package ships `py.typed` and is checked under Pyright strict.
- **`Job` / `JobStatus` model** with serializer-based `to_record` / `from_record`.
  Control fields (id, task name, status, error, attempts, created_at) stay plain
  in the record; args/kwargs and the result are opaque serialized blobs.
- **`Backend` protocol and `MemoryBackend`** — a `typing.Protocol` (enqueue,
  claim, get_job, save, release, wait_for) with an in-memory implementation
  (FIFO claim, event-driven result waiting).
- **Pluggable serializers** behind a `Serializer` protocol: `JSONSerializer`
  (default) and `PickleSerializer`.
- **At-least-once delivery.** `Backend.release` nacks an unfinished lease so a job
  interrupted mid-flight (e.g. on worker shutdown) is redelivered instead of being
  stranded in `RUNNING`; a terminal `save(done=True)` acks it.
- **Structured logging** across the package, silent by default via a `NullHandler`
  on the `TaskQueue` logger.
- **`root_group()`** placeholder scope — a no-op async context manager that keeps
  the `Queue` API stable until `JobGroup` arrives in Phase 2.
- Initial project scaffolding: tooling, CI, lint, strict typing, and tests.

### Changed

- The default serializer is JSON (dependency-free and portable); Pickle is opt-in
  for Python-native objects.
- `Worker` is single-use — re-entering a worker context manager raises
  `RuntimeError`. Call `queue.worker()` for a fresh pool.
