"""Base tests — the library exercised the way a user (and CI) runs it.

These drive the *public* API only (`from TaskQueue import ...`): define tasks
with the decorator, run a worker as an async context manager, submit work, and
await results. They cover realistic workflows (fan-out/gather, mixed
success+failure, a chained pipeline, a streaming producer/consumer) and
concurrency limits, plus full-stack runs in a *fresh Python process*
(subprocess), and skipped scaffolding marking where the Phase 3 cross-process
(Redis) end to end tests will go.

Written against the target Phase 1 API: the module cannot be collected
until the queue<->worker import cycle (C1) is fixed.
"""

import asyncio
import contextlib
import os
import queue
import subprocess
import sys
import textwrap
import threading
import time
from collections import Counter
from collections.abc import AsyncIterator
from typing import NoReturn

import pytest
import redis.asyncio as redis

from TaskQueue import JobHandle, JobStatus, MemoryBackend, Queue
from TaskQueue.backends.redis_backend import (
    QUEUE,
    WORKERS,
    RedisBackend,
    job_key,
    processing_key,
)

pytestmark = pytest.mark.timeout(20)

REDIS_URL = os.environ.get("TASKQUEUE_TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
async def redis_url() -> AsyncIterator[str]:
    """A real Redis, flushed around the test. Skips when none is reachable.

    fakeredis lives inside the test process, so it cannot host two worker
    processes; the crash tests need a real server or nothing.
    """
    client = redis.from_url(REDIS_URL)
    try:
        info = await client.info("server")
    except Exception:  # pragma: no cover - depends on the environment
        pytest.skip(f"no Redis reachable at {REDIS_URL}")

    # BLMOVE (claim) and ZRANGE ... BYSCORE (reap) are both 6.2+. An older
    # server answers "unknown command", the worker loop logs and retries, and
    # the symptom is a worker that starts but silently never claims anything.
    version = str(info.get("redis_version", "0"))  # pyright: ignore[reportUnknownMemberType]
    if tuple(int(part) for part in version.split(".")[:2]) < (6, 2):
        pytest.skip(  # pragma: no cover - depends on the environment
            f"Redis {version} at {REDIS_URL} is too old: the backend needs 6.2+ "
            "for BLMOVE and ZRANGE ... BYSCORE"
        )
    await client.flushdb()
    try:
        yield REDIS_URL
    finally:
        await client.flushdb()
        await client.aclose()


# --------------------------------------------------------------------------- #
# In-process workflows through the public API                                 #
# --------------------------------------------------------------------------- #


async def test_fan_out_and_gather() -> None:
    q = Queue(backend=MemoryBackend())

    @q.task
    async def fetch(n: int) -> int:
        await asyncio.sleep(0.01)
        return n * 10

    async with q.worker(concurrency=8):
        handles = [await fetch.submit(i) for i in range(20)]
        results = await asyncio.wait_for(
            asyncio.gather(*(h.result() for h in handles)), 10
        )

    assert sorted(results) == [i * 10 for i in range(20)]


async def test_mixed_success_and_failure_batch() -> None:
    q = Queue(backend=MemoryBackend())

    @q.task
    async def maybe(n: int) -> int:
        if n % 2 == 0:
            return n
        raise ValueError(f"odd: {n}")

    async with q.worker(concurrency=4):
        handles = [await maybe.submit(i) for i in range(6)]
        settled = await asyncio.wait_for(
            asyncio.gather(*(h.result() for h in handles), return_exceptions=True), 10
        )

    succeeded = {r for r in settled if isinstance(r, int)}
    failed = [r for r in settled if isinstance(r, Exception)]
    assert succeeded == {0, 2, 4}
    assert len(failed) == 3
    assert all(isinstance(e, RuntimeError) for e in failed)


async def test_chained_pipeline() -> None:
    """A three-stage extract -> transform -> load pipeline (caller-orchestrated)."""
    q = Queue(backend=MemoryBackend())

    @q.task
    async def extract(source: str) -> list[int]:
        return [1, 2, 3] if source == "db" else []

    @q.task
    async def transform(nums: list[int]) -> list[int]:
        return [n * n for n in nums]

    @q.task
    async def load(nums: list[int]) -> int:
        return sum(nums)

    async with q.worker(concurrency=2):
        raw = await (await extract.submit("db")).result()
        shaped = await (await transform.submit(raw)).result()
        total = await (await load.submit(shaped)).result()

    assert total == 14  # 1 + 4 + 9


async def test_streaming_producer_consumer() -> None:
    q = Queue(backend=MemoryBackend())

    @q.task
    async def work(n: int) -> int:
        await asyncio.sleep(0.005)
        return n

    collected: list[int] = []
    async with q.worker(concurrency=3):
        handles: list[JobHandle[int]] = []
        for i in range(15):
            handles.append(await work.submit(i))
            await asyncio.sleep(0.001)  # stagger submissions like a live stream
        for h in handles:
            collected.append(await asyncio.wait_for(h.result(), 5))

    assert sorted(collected) == list(range(15))


async def test_root_group_spawn_loop() -> None:
    q = Queue(backend=MemoryBackend())

    @q.task
    async def inc(n: int) -> int:
        return n + 1

    async with q.worker(concurrency=4):
        scope = q.root_group()
        handles = [await scope.spawn(inc, i) for i in range(10)]
        results = [await asyncio.wait_for(h.result(), 5) for h in handles]

    assert results == list(range(1, 11))


async def test_status_observable_end_to_end() -> None:
    q = Queue(backend=MemoryBackend())

    @q.task
    async def slow() -> str:
        await asyncio.sleep(0.05)
        return "done"

    async with q.worker():
        handle = await slow.submit()
        assert await handle.status() in (JobStatus.QUEUED, JobStatus.RUNNING)
        assert await asyncio.wait_for(handle.result(), 5) == "done"
        assert await handle.status() == JobStatus.COMPLETED


# --------------------------------------------------------------------------- #
# Full stack in a fresh process (closest we get to "real" without Redis)       #
# --------------------------------------------------------------------------- #

_PROGRAM = textwrap.dedent(
    """
    import asyncio
    from TaskQueue import Queue, MemoryBackend

    q = Queue(backend=MemoryBackend(), namespace="smoke")

    @q.task
    async def add(x: int, y: int) -> int:
        return x + y

    async def main() -> None:
        async with q.worker(concurrency=2):
            handle = await q.root_group().spawn(add, 20, 22)
            print("BASE_RESULT", await handle.result())

    asyncio.run(main())
    """
)


@pytest.mark.timeout(60)
def test_full_stack_in_a_fresh_process() -> None:
    """Run a real program in a separate interpreter and check its output."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    proc = subprocess.run(
        [sys.executable, "-c", _PROGRAM],
        capture_output=True,
        text=True,
        timeout=45,
        env=env,
    )
    assert proc.returncode == 0, f"process failed:\n{proc.stderr}"
    assert "BASE_RESULT 42" in proc.stdout


# --------------------------------------------------------------------------- #
# Concurrency, end-to-end                                                      #
# --------------------------------------------------------------------------- #


async def test_concurrency_is_bounded_and_reached() -> None:
    """The worker runs at most `concurrency` jobs at once — and reaches it."""
    q = Queue(backend=MemoryBackend())
    state = {"current": 0, "peak": 0}

    @q.task
    async def work() -> int:
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.03)
        state["current"] -= 1
        return 1

    async with q.worker(concurrency=4):
        handles = [await work.submit() for _ in range(16)]
        results = await asyncio.wait_for(
            asyncio.gather(*(h.result() for h in handles)), 10
        )

    assert results == [1] * 16
    assert state["peak"] == 4  # bounded by, and reaches, the configured limit


async def test_every_job_runs_exactly_once_under_load() -> None:
    q = Queue(backend=MemoryBackend())
    runs: Counter[int] = Counter()

    @q.task
    async def mark(n: int) -> int:
        runs[n] += 1
        return n

    async with q.worker(concurrency=8):
        handles = [await mark.submit(i) for i in range(100)]
        results = await asyncio.wait_for(
            asyncio.gather(*(h.result() for h in handles)), 10
        )

    assert sorted(results) == list(range(100))  # nothing dropped
    assert all(runs[i] == 1 for i in range(100))  # nothing run twice


_CONCURRENCY_PROGRAM = textwrap.dedent(
    """
    import asyncio
    from TaskQueue import Queue, MemoryBackend

    q = Queue(backend=MemoryBackend(), namespace="smoke")
    peak = {"current": 0, "max": 0}

    @q.task
    async def work(n: int) -> int:
        peak["current"] += 1
        peak["max"] = max(peak["max"], peak["current"])
        await asyncio.sleep(0.02)
        peak["current"] -= 1
        return n * n

    async def main() -> None:
        async with q.worker(concurrency=4):
            handles = [await work.submit(i) for i in range(40)]
            results = [await h.result() for h in handles]
        assert results == [i * i for i in range(40)], results
        assert peak["max"] == 4, peak["max"]
        print("BASE_CONCURRENCY_OK", peak["max"])

    asyncio.run(main())
    """
)


@pytest.mark.timeout(60)
def test_concurrency_in_a_fresh_process() -> None:
    """Run many concurrent jobs through a bounded worker in a separate process."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    proc = subprocess.run(
        [sys.executable, "-c", _CONCURRENCY_PROGRAM],
        capture_output=True,
        text=True,
        timeout=45,
        env=env,
    )
    assert proc.returncode == 0, f"process failed:\n{proc.stderr}"
    assert "BASE_CONCURRENCY_OK 4" in proc.stdout


# --------------------------------------------------------------------------- #
# Phase 3 cross-process scaffolding (skipped until the Redis backend exists)   #
# --------------------------------------------------------------------------- #


_SPLIT_WORKER = textwrap.dedent(
    """
    import asyncio
    import sys

    import redis.asyncio as redis

    from TaskQueue import Queue
    from TaskQueue.logger import setup_logging
    from TaskQueue.logger import setup_logging
    from TaskQueue.backends.redis_backend import RedisBackend

    setup_logging("DEBUG")
    client = redis.from_url(sys.argv[1])
    backend = RedisBackend(client)
    q = Queue(backend=backend)


    @q.task(name="split.work")
    async def work(n: int) -> str:
        await client.incr("runs:" + str(n))
        await asyncio.sleep(0.1)
        return backend._instance_id


    async def main() -> None:
        async with q.worker(concurrency=2):
            print("BASE_READY", flush=True)
            await asyncio.Event().wait()


    asyncio.run(main())
    """
)


@pytest.mark.slow
@pytest.mark.timeout(90)
async def test_two_worker_processes_split_jobs(redis_url: str) -> None:
    """Two independent processes share one queue, each job running exactly once.

    This is the property fakeredis cannot check: that 'BLMOVE' is atomic
    *across processes*, so two workers racing for the same queue never both
    claim the same job. Results stay readable from the submitting process.
    """
    jobs = 20
    client = redis.from_url(redis_url)
    q = Queue(backend=RedisBackend(client))

    @q.task(name="split.work")
    async def work(n: int) -> str:  # pragma: no cover - runs in the children
        return ""

    workers = [_spawn_worker(_SPLIT_WORKER) for _ in range(2)]
    try:
        # Both pools block on BLMOVE before a single job exists, so neither gets
        # a head start; a lopsided split would then be a real scheduling result.
        for worker in workers:
            _read_until(worker, "BASE_READY")

        handles = [await work.submit(n) for n in range(jobs)]
        ran_by = [await asyncio.wait_for(handle.result(), 45) for handle in handles]

        assert len(ran_by) == jobs
        split = Counter(ran_by)
        assert len(split) == 2, f"one process claimed everything: {split}"

        runs = [int(await client.get(f"runs:{n}") or 0) for n in range(jobs)]
        assert runs == [1] * jobs  # nothing dropped, nothing run twice
        assert await client.lrange(QUEUE, 0, -1) == []
    finally:
        for worker in workers:
            worker.kill()
            worker.wait(timeout=30)
        await client.aclose()


_CRASH_WORKER = textwrap.dedent(
    """
    import asyncio
    import sys

    import redis.asyncio as redis

    from TaskQueue import Queue
    from TaskQueue.backends.redis_backend import RedisBackend
    from TaskQueue.logger import setup_logging

    setup_logging("DEBUG")
    url, hang = sys.argv[1], float(sys.argv[2])
    backend = RedisBackend(redis.from_url(url), worker_ttl=3)
    q = Queue(backend=backend)


    @q.task(name="crash.work")
    async def work(n: int) -> int:
        print("BASE_CLAIMED", backend._instance_id, flush=True)
        await asyncio.sleep(hang)
        return n * 2


    async def main() -> None:
        async with q.worker(heartbeat_interval=0.5):
            print("BASE_READY", flush=True)
            await asyncio.Event().wait()


    asyncio.run(main())
    """
)


def _spawn_worker(program: str, *args: str) -> subprocess.Popen[str]:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    return subprocess.Popen(
        # -u: unbuffered child I/O. Without it a piped stdout is block-buffered
        # and a marker can sit in the child's buffer while the parent waits.
        [sys.executable, "-u", "-c", program, REDIS_URL, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def _read_until(
    process: subprocess.Popen[str], marker: str, timeout: float = 30.0
) -> str:
    """Read the child's output until `marker` appears, or fail with what it said.

    A reader thread is the portable way to put a deadline on this: `readline`
    on a pipe blocks with no timeout, so a child that starts but never speaks
    would hang the suite and report nothing but a stack dump. On a timeout or
    an early exit this kills the child and surfaces everything it printed.
    """
    assert process.stdout is not None
    stdout = process.stdout
    seen: list[str] = []
    lines: queue.Queue[str | None] = queue.Queue()

    def pump() -> None:
        for line in iter(stdout.readline, ""):
            lines.put(line)
        lines.put(None)

    reader = threading.Thread(target=pump, daemon=True)
    reader.start()

    def give_up(reason: str) -> NoReturn:  # pragma: no cover - failure path
        process.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=10)
        reader.join(timeout=5)
        while not lines.empty():
            extra = lines.get_nowait()
            if extra is not None:
                seen.append(extra)
        output = "".join(seen) or "(nothing)"
        pytest.fail(f"never saw {marker!r} ({reason}).\nchild output:\n{output}")

    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            give_up(f"still silent after {timeout:g}s")
        try:
            line = lines.get(timeout=remaining)
        except queue.Empty:
            give_up(f"still silent after {timeout:g}s")
        if line is None:
            give_up(f"child exited with code {process.poll()}")
        seen.append(line)
        if marker in line:
            return line


@pytest.mark.slow
@pytest.mark.timeout(90)
async def test_job_reclaimed_after_worker_crash(redis_url: str) -> None:
    """A SIGKILLed worker's in-flight job is reclaimed and completed elsewhere.

    The whole point of the heartbeat/reaper protocol, and the one case no
    in-process test can reach: the worker never runs a shutdown handler, so
    nothing releases the lease. Only a peer noticing the missing heartbeat
    can return the job to the queue.
    """
    client = redis.from_url(redis_url)
    q = Queue(backend=RedisBackend(client))

    @q.task(name="crash.work")
    async def work(n: int) -> int:  # pragma: no cover - runs in the children
        return n * 2

    handle = await work.submit(21)

    victim = _spawn_worker(_CRASH_WORKER, "600")
    try:
        claimed = _read_until(victim, "BASE_CLAIMED")
        dead_id = claimed.split()[1]

        # SIGKILL on POSIX, TerminateProcess on Windows: either way no Python
        # cleanup runs, so `release` never happens and the lease is stranded.
        victim.kill()
        victim.wait(timeout=30)

        assert await client.lrange(QUEUE, 0, -1) == []
        assert await client.lrange(processing_key(dead_id), 0, -1) == [
            handle.job_id.encode()
        ]
        assert (await client.hget(job_key(handle.job_id), "status")) == b"running"
        assert await client.zscore(WORKERS, dead_id) is not None
    finally:
        if victim.poll() is None:  # pragma: no cover - only if kill failed
            victim.kill()

    rescuer = _spawn_worker(_CRASH_WORKER, "0")
    try:
        _read_until(rescuer, "BASE_READY")
        assert await asyncio.wait_for(handle.result(), 30) == 42
        assert await client.zscore(WORKERS, dead_id) is None
        assert await client.exists(processing_key(dead_id)) == 0
    finally:
        rescuer.kill()
        rescuer.wait(timeout=30)
        await client.aclose()
