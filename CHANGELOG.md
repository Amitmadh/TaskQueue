# Changelog

All notable changes documented here.

## [0.3.0] - 2026-08-23

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
- **`RedisBackend` — the whole `Backend` Protocol, against real Redis.** A hash per job,
  `BLMOVE` from `queue` into a per-worker `processing:{consumer_id}` list, pub/sub for
  result and cancellation notification. Every check-then-act guard is a Lua script rather
  than a pipeline, because `MULTI` cannot branch — `pipe.hget()` returns the pipeline, not
  a value — so "if the job still exists and is not terminal, then write" is one atomic
  step instead of a read followed by a hopeful write. Import it from
  `TaskQueue.backends.redis_backend`; the driver is an optional extra
  (`pip install TaskQueue[redis]`) so the core installs without it.
- **Reliable delivery, including for a worker that dies without warning.** Each process
  writes its own timestamp into a `workers` sorted set every `heartbeat_interval`
  seconds. Any worker whose last beat is older than the backend's `worker_ttl` is
  presumed dead, and the next reaper pass drains its processing list back onto the queue,
  resetting each job to QUEUED and incrementing `attempts`. The reaper runs in every
  worker and takes **no distributed lock**: the reclaim is a single Lua script, so
  concurrent reapers cost a wasted round trip rather than a duplicated job. A lock here
  would only add a way to be wrong — one that expired mid-operation would break the very
  property it was meant to protect.
- **`Worker.drain(timeout)`** — stop claiming, let in-flight jobs finish, and cancel
  whatever is left when the deadline passes. Returns `True` if the pool wound down with
  nothing running, `False` if it had to cancel, so a caller can log the difference.
  `taskqueue worker` now drains on the first SIGINT/SIGTERM and cancels on the second,
  which turns an ordinary restart from "re-run whatever was in flight" into "finish it".
- **`result_ttl`** — a terminal record now expires rather than living forever when nobody
  calls `take_result()`. Fire-and-forget jobs no longer grow the keyspace without bound.
  Non-positive values are rejected at construction, because `EXPIRE key 0` deletes
  immediately and would silently destroy every result the moment it was written.
- **`--drain-timeout` and `--heartbeat-interval`** on `taskqueue worker`, plus exit code
  `3` for settings that parse fine but would misbehave: the CLI refuses to start when the
  heartbeat interval leaves no margin against the backend's `worker_ttl`, since every
  worker would then be presumed dead between its own beats and have its running jobs
  reclaimed by a peer. `worker_ttl` itself stays where the backend is built — it is a
  fleet-wide agreement, and behind a per-process flag two workers with different values
  would reap each other.
- **The conformance suite runs twice.** The `backend` fixture is parameterised over
  `MemoryBackend` and `RedisBackend`, so every Phase 2 property — fail-fast sibling
  cancellation, nested scope propagation, deadlines, shutdown redelivery — is now checked
  against real Redis commands rather than live Python objects. A divergence between the
  two backends is a test failure instead of a production surprise.
- **Cross-process tests against a real server.** `test_two_worker_processes_split_jobs`
  proves `BLMOVE` is atomic across processes; `test_job_reclaimed_after_worker_crash`
  SIGKILLs a worker mid-job and asserts a peer reclaims the lease. Neither can run on
  fakeredis, which lives inside the test process, so they skip when no Redis is reachable
  and CI runs a `redis:7` service container.
- **`.gitattributes`** (`* text=auto eol=lf`). The repository had drifted into mixed line
  endings, so an editor rewriting a file end to end was showing up as a whole-file diff.
- **`python -m TaskQueue`** as an alternative to the `taskqueue` console script. On
  Windows the installed script is a launcher `.exe` that runs the interpreter as its own
  child, so a parent holding a `Popen` handle on it can only signal the stub — it cannot
  kill, or wait on, the worker itself. `-m` makes the worker a direct child, which is
  what `examples/worker_crash_job_reclaimed.py` needs to stage a crash at all.

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
- **`Backend.claim()` returns `dict | None`** and must never block indefinitely. It used
  to loop internally until it had a job, which meant a worker parked in `claim` could not
  observe a shutdown request — `drain()` waited forever on a pool that was, by definition,
  idle. Returning `None` after a bounded interval hands control back to the worker loop,
  which is what makes a graceful drain possible at all.
- **The `Backend` Protocol grew to eleven methods**, adding `heartbeat()` and
  `reap()`. `MemoryBackend` implements both as no-ops: a single process cannot outlive
  its own leases, so it has no liveness protocol to run.
- **`attempts` is counted by `claim()`, not by `reap()`,** and the protocol now says so.
  Counting a lost lease at reclaim time cannot be exact: `claim` leases in two steps
  (`BLMOVE`, then the script that marks the job `RUNNING`), and a worker killed between
  them left the job on its processing list still marked `QUEUED` — a state the reaper
  requeued without counting, so a job could be delivered twice and report `attempts=0`.
  Incrementing inside the claim script, atomically with the `RUNNING` write, makes the
  half-delivery uncountable rather than uncounted: nothing was handed to a worker that
  could act on it, and the delivery that replaces it is counted normally. `attempts` now
  means "times handed to a worker", so a job reads `1` while it first runs rather than
  `0`. `MemoryBackend.claim` counts too — it previously never touched the field, so the
  same job redelivered by `release()` reported a different number in each backend. This
  matters before retries land in Phase 5, since that is the number they will branch on.
- **A failing backend no longer spins.** The worker loop retried `claim()` with no
  backoff, so a Redis outage burned a core and flooded the log — thousands of tracebacks a
  second. It now waits a second between attempts.
- **`dev` moved from `[project.optional-dependencies]` to `[dependency-groups]`.** As an
  extra it was neither installed by a bare `uv sync` (so a changed spec left a stale venv
  behind — the symptom was fakeredis without its Lua support and 52 failing tests) nor
  something end users should be able to `pip install TaskQueue[dev]` and get pyright with.
- Package version `0.0.2` → `0.3.0`. It had never been bumped past the initial
  scaffolding, so `taskqueue --version` and the `0.1.0` / `0.2.0` entries below
  disagreed; this realigns them.

### Deferred

- **The `execute_at` sorted set and scheduler coroutine**, listed under Phase 3 in the
  build plan, are not built. Nothing schedules yet — there is no `submit(delay=...)` and
  no `execute_at` on `Job` — so it would be a mechanism with no caller and no way to demo
  it. Its real driver is retry backoff, and it lands with retries in Phase 5.

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
