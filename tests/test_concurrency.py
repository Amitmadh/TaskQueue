"""Concurrency behavior of the Worker pool.

These pin the invariants a worker pool must hold: it never runs more than the
configured number of jobs at once, it *does* reach that number when work is
available, every job runs exactly once (no drops or duplicates), and the
event-driven result delivery wakes every waiter. Synchronization uses
counters/events rather than wall-clock timing, so the assertions are exact
rather than flaky.
"""

import asyncio
from collections import Counter

import pytest

from TaskQueue.queue import Queue

pytestmark = pytest.mark.timeout(15)


@pytest.mark.parametrize("limit", [1, 2, 5])
async def test_peak_in_flight_never_exceeds_limit(queue: Queue, limit: int) -> None:
    state = {"current": 0, "peak": 0}

    @queue.task
    async def track() -> None:
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.05)
        state["current"] -= 1

    async with queue.worker(concurrency=limit):
        handles = [await track.submit() for _ in range(limit * 4)]
        for h in handles:
            await asyncio.wait_for(h.result(), 5)

    assert state["peak"] <= limit  # the bound is respected
    assert state["peak"] == limit  # ...and actually reached, given enough work


async def test_concurrency_one_serializes_in_order(queue: Queue) -> None:
    order: list[int] = []

    @queue.task
    async def record(n: int) -> int:
        order.append(n)
        await asyncio.sleep(0.01)
        return n

    async with queue.worker(concurrency=1):
        handles = [await record.submit(i) for i in range(8)]
        for h in handles:
            await asyncio.wait_for(h.result(), 5)

    assert order == list(range(8))  # a single worker runs jobs FIFO


async def test_each_job_runs_exactly_once(queue: Queue) -> None:
    runs: Counter[int] = Counter()

    @queue.task
    async def mark(n: int) -> int:
        runs[n] += 1
        return n

    async with queue.worker(concurrency=4):
        handles = [await mark.submit(i) for i in range(50)]
        results = [await asyncio.wait_for(h.result(), 5) for h in handles]

    assert sorted(results) == list(range(50))  # nothing dropped
    assert all(runs[i] == 1 for i in range(50))  # nothing run twice


async def test_full_parallelism_via_rendezvous(queue: Queue) -> None:
    """With concurrency=N, N jobs must be able to run at the same instant.

    Each job blocks until all N have arrived. If the pool ran fewer than N at
    once the rendezvous could never complete and result() would time out.
    """
    n = 4
    arrived = {"count": 0}
    everyone_here = asyncio.Event()

    @queue.task
    async def rendezvous() -> int:
        arrived["count"] += 1
        if arrived["count"] == n:
            everyone_here.set()
        await asyncio.wait_for(everyone_here.wait(), 3)
        return 1

    async with queue.worker(concurrency=n):
        handles = [await rendezvous.submit() for _ in range(n)]
        results = [await asyncio.wait_for(h.result(), 5) for h in handles]

    assert results == [1] * n


async def test_many_waiters_share_one_result(queue: Queue) -> None:
    @queue.task
    async def slow() -> int:
        await asyncio.sleep(0.05)
        return 99

    async with queue.worker():
        handle = await slow.submit()
        # ten coroutines await the same job concurrently; every one must wake
        results = await asyncio.wait_for(
            asyncio.gather(*(handle.result() for _ in range(10))), 5
        )

    assert results == [99] * 10


async def test_concurrent_submitters_all_land(queue: Queue) -> None:
    @queue.task
    async def echo(n: int) -> int:
        return n

    async with queue.worker(concurrency=4):
        handles = await asyncio.gather(*(echo.submit(i) for i in range(30)))
        results = await asyncio.wait_for(
            asyncio.gather(*(h.result() for h in handles)), 5
        )

    assert sorted(results) == list(range(30))


@pytest.mark.slow
@pytest.mark.timeout(30)
async def test_throughput_under_load(queue: Queue) -> None:
    @queue.task
    async def square(n: int) -> int:
        return n * n

    async with queue.worker(concurrency=16):
        handles = [await square.submit(i) for i in range(500)]
        results = [await asyncio.wait_for(h.result(), 10) for h in handles]

    assert results == [i * i for i in range(500)]
