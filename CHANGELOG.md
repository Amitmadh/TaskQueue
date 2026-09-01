# Changelog

All notable changes documented here.

## [Unreleased]

Phase 5, in progress.

### Added

- **A task body that runs twice no longer forks its subtree.** At-least-once delivery
  means any body can run twice, and Phase 5's retries will re-run bodies on purpose. A
  leaf task only has to be idempotent in its own effects, but a *spawning* task has an
  enqueue as a side effect: measured before this change, a redelivered parent turned 2
  children into 4, with the first pair still running and owned by nobody. Two halves
  close it, and neither works alone —
  - `TaskQueue.context` holds a `JobContext` in a `ContextVar`, set by
    `Worker._process` *before* it creates the task, because a task copies the context
    at creation and anything set afterwards is invisible to the body. `JobGroup.spawn`
    seeds each child's id on it: `sha1(f"{parent.job_id}:{n}")[:32]`, where `n` is a
    counter per **body**, not per scope — a scope's id is a fresh `uuid4` on every
    construction, so it cannot appear in a seed that has to survive a re-run, and
    counting per body is what stops two sequential scopes from both claiming ordinal 0.
    Outside a job there is nothing to derive from, so `Job` still falls back to `uuid4`.
  - `Backend.enqueue` refuses an id it already knows. On Redis it stopped being a
    plain `MULTI` and became a Lua script, for the same reason as every other guard
    in that backend: `EXISTS` then `HSET`+`RPUSH` is a check-then-act, and `MULTI`
    cannot branch.
- **`tests/test_child_ids.py`** — both halves and the consequence, on both backends.
  The guard is asserted through `claim` rather than through the record, because an
  implementation that skipped the record write but still pushed the id would leave a
  job that is claimed twice. The end-to-end test settles and asserts *before* reading
  any result: `take_result` frees the record a duplicate claim would need, so consuming
  results first hides the very fork the test exists to catch. Removing either half
  fails it on both backends.

### Known limitation

- A redelivery that *overlaps* its predecessor is still not safe: two live bodies of
  one job contend for the same children's results, and `take_result` is
  single-consumer, so one of them waits forever. The reaper only redelivers a job whose
  worker is already gone, so no path today produces it — but a retry that fires while
  the first attempt is still running would, which is a constraint on the retry design
  rather than a solved problem.

## [0.4.0] - 2026-08-30

Phase 4. Cancellation that reliably crosses processes: delivered through the backend,
with a poll backup so a dropped notification costs latency rather than correctness.

### Added

- **Cross-process cancellation, proven against a real server.** `handle.cancel()` in
  one process stops a job already running in another: the record reads `cancelled`,
  `result()` raises `JobCancelled`, and the lease is acked on the way out.
  `test_cancel_crosses_processes` asserts the job is genuinely in flight over there
  *before* cancelling, and waits for a marker the job prints from its own
  `except asyncio.CancelledError` — because a record that *says* cancelled is not the
  same claim as work that actually stopped. The conformance suite already checked
  cancellation against fakeredis, which lives inside the test process: that proves the
  logic and nothing whatever about the transport.
- **A poll backup behind both pub/sub waits, so a dropped notification costs latency
  instead of correctness.** Redis pub/sub is fire-and-forget: a connection blip during
  a long job drops the `PUBLISH` for good, and the durable state then sits unread —
  `wait_cancel` parked forever on a cancel that was requested, and `take_result`
  hanging until `result_ttl` deleted the record out from under it and turned a job
  that *succeeded* into a `KeyError`. Both waits now re-read the record every
  `_NOTIFY_POLL_SECONDS` (5s): the record is the truth, the message is only the news.
  Five seconds is a backstop, not a mechanism — one `HGET` per waiting job per
  interval, forever, against a few seconds of extra latency in a failure that should
  almost never happen. `test_cancel_crosses_processes_even_when_the_publish_is_lost`
  proves it across processes, using a cancel script *derived* from the real one with
  the `PUBLISH` removed, with the derivation asserted so a refactor cannot leave the
  test quietly checking nothing.
- **`Job.group_id`** — the scope that spawned a job, on the wire. Written only when
  populated, read with `.get()`. Nothing consumes it yet: it is the visible difference
  between a spawn into an entered scope and a deliberate detachment through
  `root_group()`, it costs one optional field, and `taskqueue inspect` is its natural
  reader.
- **`ConfigError` is exported from `TaskQueue`** and lives in `TaskQueue.exceptions`.
  It used to be private to the CLI, which is no use to anyone constructing a `Worker`
  themselves — see Changed.
- **`docs/architecture.md`** — the longer write-up the README has been promising:
  the record and key space, why every check-then-act is a Lua script, the two-step
  lease and everything that follows from it, the lock-free reaper and the two
  orderings that hold it together, subscribe-then-check plus the poll, the
  cancellation protocol, and a table of what survives which kind of crash.
- **`tests/test_lease_invariants.py`** — the orderings that make a job recoverable,
  collected in one place because they are one family and each was written after a real
  failure of it rather than from a docstring.

### Changed

- **A failure now cancels its siblings immediately, not at the end of the block.** The
  fan-out is issued from a done-callback on the first failing child's waiter, so
  cancellation starts while the scope body is still running rather than when it
  finally reaches the join. A scope that spawns work in a loop no longer lets every
  remaining sibling start after one has already failed.
- **`JobGroup.cancel_all_jobs` bounds its drain.** It waited unconditionally for every
  cancelled child to report back — on a wake-up that a dropped notification can
  swallow. It now gives up after `_CANCEL_DRAIN_TIMEOUT_SECONDS` (30s) with a warning
  and lets teardown drop the local tasks. Defence in depth rather than a substitute
  for the poll above: a hang inside `finally` outlives even `pytest-timeout`, which
  makes it nastier than the same hang in the join.
- **`Task.submit(group_id, /, *args, **kwargs)` is the only enqueue.** There is no
  group-less form, so `JobGroup.spawn` is the front door by signature rather than by
  convention. The parameter is positional-only and leading because that is the one
  shape `ParamSpec` allows next to `*args: P.args` — keyword-only would collide with
  any task that has a parameter of the same name, and trailing positional would be
  swallowed by `P.args`.
- **The liveness check moved from the CLI into `Worker.__init__`.** It refused a
  `heartbeat_interval` too close to the backend's `worker_ttl` only on the CLI path,
  so `q.worker(...)` could still be talked into a pool configured to have its own
  running jobs reclaimed by peers. The CLI *lost* its pre-check rather than keeping a
  second copy — `main` now translates `ConfigError` into exit code 3, because two call
  sites for one rule is how they drift. Note the boundary: this bounds the
  configuration only, and a CPU-bound synchronous task can still starve a beat
  whatever the ratio.
- **`Worker.__aenter__` now raises if the backend is unreachable**, as a consequence of
  registering before it claims (see Fixed). It previously started, logged
  `worker upkeep failed; continuing`, and retried in the background.
- **README:** "Cross-process cancellation" is ✓ in the comparison table, and there is a
  new *What survives a crash, and what doesn't* section — the boundary between what a
  scope guarantees and what a killed process takes with it.

### Fixed

- **A worker could claim a job before it was registered as alive.** `__aenter__`
  created the claim loops and the upkeep loop as sibling `create_task`s, so the
  `BLMOVE` was issued before the first `ZADD` and the two round trips raced; measured
  under load, the claim won by 4–17 ms. `reap` walks expired **members** of the
  `workers` set and nothing else, so a `SIGKILL` in that window left the job on a
  processing list no reaper will ever look at: not queued, not owned, never
  redelivered. `__aenter__` now `await`s one heartbeat before starting anything.
- **`CancelledError` while joining a scope orphaned every child.** `__aexit__` handled
  a body that raised and a deadline that passed, but a cancellation arriving while
  parked in the join propagated with nothing cancelled — and the join is where a scope
  spends nearly all of its life, so Ctrl-C on a waiting producer left its children
  running. Every path out of a scope has to unwind correctly, because nothing outside
  the process will do it for you.
- **A cancel watcher that *raised* was read as a cancellation.** `asyncio.wait`
  reports "done" for a task that raised exactly as for one that returned, so a dropped
  pub/sub connection wrote `cancelled` over work that was still running and threw the
  result away. The worker now distinguishes them and finishes the job unwatched.
- **A failed `release` on shutdown swallowed the worker loop's own cancellation.**
  `release` raises `KeyError` for a record that no longer exists — precisely the state
  after `take_result` consumed the result — and raised from inside the
  `except CancelledError` arm it *replaced* the `CancelledError`, so the loop exited by
  exception and every `gather(..., return_exceptions=True)` collecting it swallowed the
  error silently. The same applied to a backend that was simply down, which is the
  usual reason a pool is being torn down at all.
- **An error between the two halves of a claim kept the lease.** `BLMOVE` takes it;
  a second round trip marks the record `RUNNING`. A failure there left the id on this
  worker's processing list still `queued`, and since the worker survives the error and
  keeps beating, no reaper would ever walk that list. `claim` now returns the id to the
  queue in one `MULTI` before re-raising.
- **`Job.from_record` read optional fields by direct indexing**, so a record without
  `group_id` raised `KeyError` inside the claim path — swallowed by the poison-record
  handler, leaving a producer waiting forever with nothing in the log to explain it.
  An absent key means `None`, and `.get()` is the only correct read.
- **Test scaffolding:** `_read_until` started a fresh reader thread per call, and two
  readers compete for the same pipe — whichever wins a line owns it, so a marker
  landing in the other one's queue is lost, which looks exactly like a child that went
  silent. It had only ever been called once per child, so nothing had caught it. There
  is now one pump per process. `test_version.py` also gained the check that was missing
  when the `v0.3.0` tag shipped a tree whose `pyproject.toml` still said `0.0.3`: the
  packaged version must equal the newest heading in this file.

### Deferred

- **The v0.4 demo GIF** — two panes, a worker running a slow job and a producer that
  cancels it mid-run. The test exists; the recording does not.

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
- **`examples/scope_semantics_tour.py`** — the six scenarios (round-trip, fan-out, fail-fast, explicit
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
