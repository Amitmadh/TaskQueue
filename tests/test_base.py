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
import os
import subprocess
import sys
import textwrap
from collections import Counter

import pytest

from TaskQueue import JobHandle, JobStatus, MemoryBackend, Queue

pytestmark = pytest.mark.timeout(20)


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


async def test_root_group_spawn_many() -> None:
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

    q = Queue(backend=MemoryBackend())

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

    q = Queue(backend=MemoryBackend())
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


@pytest.mark.skip(
    reason="cross-process end to end arrives with the Redis backend in Phase 3"
)
def test_two_worker_processes_split_jobs() -> None:
    # With a shared RedisBackend, two independent worker processes claim from the
    # same queue and between them complete every job exactly once; results are
    # readable from the submitting process.
    ...


@pytest.mark.skip(
    reason="cross-process end to end arrives with the Redis backend in Phase 3"
)
def test_job_reclaimed_after_worker_crash() -> None:
    # A job claimed by a worker that dies mid-flight is re-claimed and completed
    # after restart (reliable delivery via the processing list).
    ...
