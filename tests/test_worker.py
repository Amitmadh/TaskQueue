"""End-to-end Worker behavior: round-trips, concurrency, resilience, lifecycle."""

import asyncio

import pytest

from TaskQueue.job import Job, JobStatus
from TaskQueue.queue import Queue

pytestmark = pytest.mark.timeout(10)


async def test_single_job_round_trip(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    async with queue.worker():
        handle = await add.submit(2, 3)
        assert await asyncio.wait_for(handle.result(), 3) == 5
        assert await handle.status() == JobStatus.COMPLETED


async def test_many_jobs_complete(queue: Queue) -> None:
    @queue.task
    async def square(n: int) -> int:
        return n * n

    async with queue.worker(concurrency=4):
        handles = [await square.submit(i) for i in range(10)]
        results = [await asyncio.wait_for(h.result(), 3) for h in handles]
    assert results == [i * i for i in range(10)]


async def test_concurrency_runs_jobs_in_parallel(queue: Queue) -> None:
    @queue.task
    async def slow() -> int:
        await asyncio.sleep(0.2)
        return 1

    async with queue.worker(concurrency=5):
        start = asyncio.get_running_loop().time()
        handles = [await slow.submit() for _ in range(5)]
        for h in handles:
            await asyncio.wait_for(h.result(), 3)
        elapsed = asyncio.get_running_loop().time() - start
    assert elapsed < 0.8


async def test_failing_task_raises_and_worker_survives(queue: Queue) -> None:
    @queue.task
    async def boom() -> int:
        raise ValueError("nope")

    @queue.task
    async def ok() -> int:
        return 7

    async with queue.worker():
        h_boom = await boom.submit()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(h_boom.result(), 3)
        assert await h_boom.status() == JobStatus.FAILED
        h_ok = await ok.submit()
        assert await asyncio.wait_for(h_ok.result(), 3) == 7


async def test_unknown_task_name_does_not_kill_worker(queue: Queue) -> None:
    @queue.task
    async def ok() -> int:
        return 1

    backend = queue.backend
    ghost = Job(task_name="does.not.exist")
    async with queue.worker():
        await backend.enqueue(ghost)

        async def until_failed() -> None:
            while (await backend.get_job(ghost.id)).status is not JobStatus.FAILED:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(until_failed(), 3)
        assert (await backend.get_job(ghost.id)).error
        h_ok = await ok.submit()
        assert await asyncio.wait_for(h_ok.result(), 3) == 1


async def test_worker_lifecycle_flags(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    worker = queue.worker()
    async with worker as entered:
        assert entered.running is True
        h = await add.submit(1, 2)
        await asyncio.wait_for(h.result(), 3)
    assert worker.running is False
    assert all(t.done() for t in worker.workers)


async def test_root_group_spawn_round_trip(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    async with queue.worker():
        handle = await queue.root_group().spawn(add, 2, 3)
        assert await asyncio.wait_for(handle.result(), 3) == 5
