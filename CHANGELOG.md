# Changelog

All notable changes documented here.

## [0.3.0] - 2026-08-19

### Added

- **A CLI.** `taskqueue` installs as a console script (`[project.scripts]`), with
  `taskqueue worker <module>:<queue> [-c N] [-l LEVEL]` to run a worker pool, plus
  `taskqueue backends` / `taskqueue serializers` to list what the package ships and
  `--version`. The target is an **import string, not `--backend`/`--serializer` flags**:
  a worker has to import the task module anyway (importing it is what runs the `@q.task`
  decorators that populate the registry), so letting that module construct the Queue,
  backend and serializer together makes a producer/worker serializer mismatch
  unspeakable. Only knobs that cannot desync — concurrency, log level — stay as flags.
  Exit codes are distinct: `1` for an unresolvable target, `2` for an argparse usage
  error, so a supervisor can tell "typed it wrong" from "your app failed to import".
- **Task namespaces.** `Queue(backend, namespace="myapp")` fixes task names to
  `"<namespace>.<function>"` and stops consulting `__module__` entirely, so a name is
  stable across launch modes, working directories, and file moves. Without it names
  still default to the import path, which is right for a module that is only imported.
- **`TaskNameError`** (exported from `TaskQueue`), raised at decoration time when a name
  could not be agreed on by two processes — see Changed.
- **A `serializers` package.** `Serializer` moved out of `backends/serializer.py` into
  `TaskQueue.serializers`, one module per implementation (`json_serializer`,
  `pickle_serializer`). The filename *is* the registration: `taskqueue serializers`
  reads the directory with `pkgutil.iter_modules` and never imports anything, so listing
  implementations cannot fail because an optional driver is missing.
- **`TaskQueue.logger.setup_logging(level)`** — opt-in console logging for applications,
  colourised when stderr is a TTY. The library itself stays silent by default.
- **The worker announces what it serves.** `taskqueue worker` logs
  `serving N task(s): myapp.add, myapp.fetch` before accepting work — the exact strings
  that process can resolve a job back to, so a producer/worker name mismatch is one
  glance away instead of a job that quietly never runs.
- **`examples/demo.py`** — the six scenarios (round-trip, fan-out, fail-fast, explicit
  cancellation, scope deadline, `on_error="collect"`) end to end in one process.
- **`RedisBackend` — skeleton only, not usable.** `enqueue`, `claim` and `get_job` are
  implemented against a Redis hash per job plus `BLMOVE` from `queue` to `processing`;
  `save`, `release`, `take_result`, `request_cancel`, `request_cancel_many` and
  `wait_cancel` still raise `NotImplementedError`. A worker cannot complete a single job
  against it yet. Phase 3 stays open.
- **`.gitattributes`** (`* text=auto eol=lf`). The repository had drifted into mixed line
  endings, so an editor rewriting a file end to end was showing up as a whole-file diff.

### Changed

- **A task name is now treated as a wire identifier, not a Python detail.** It is
  serialized into the job record and resolved by a worker in another process, so both
  sides must compute the same string — and `f"{func.__module__}.{func.__name__}"` does
  not, because `__module__` depends on how the program was *launched*. Deriving a name
  for a task defined in a module being run as a script now raises `TaskNameError` at
  decoration (it would register as `__main__.x` here and `<import.path>.x` in a worker,
  which would silently never match) instead of guessing. Registering two tasks under one
  name also raises, where it previously logged a warning and silently overwrote the
  first — rebinding a name points jobs already queued under it at different code.
- **`Job.to_record` now produces a Redis-shaped record.** Control fields are plain
  strings (`attempts` stringified, `request_cancel` as `"0"`/`"1"`) and only the payload
  and result are `bytes` blobs, because a Redis hash stores strings and bytes — not
  ints, bools or `None`. `error` and `result` are written only once populated, so an
  absent key means `None`; `from_record` reads them with `.get`.
- **Modules renamed to snake_case**, and submodule imports change with them:
  `jobGroup.py` → `job_group.py`, `backends/memory.py` → `backends/memory_backend.py`,
  `backends/serializer.py` → `serializers/`. Imports from the top-level `TaskQueue`
  package are unaffected.
- **`JobGroup` has an `id`**, stamped on its log lines and into the `BaseExceptionGroup`
  message so concurrent scopes can be told apart. Phase 4 will persist it.
- Package version `0.0.2` → `0.3.0`. It had never been bumped past the initial
  scaffolding, so `taskqueue --version` and the `0.1.0` / `0.2.0` entries below
  disagreed; this realigns them.

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
