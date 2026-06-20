# Changelog

All notable changes documented here.

## [Unreleased]

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
