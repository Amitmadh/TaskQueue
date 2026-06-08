"""JobHandle as a typed promise — event-driven waiting, raising on failure.

Unit tests: JobHandle is driven directly against a backend, no Worker involved,
isolating the handle's behavior from the run loop.
"""

import asyncio

import pytest

from TaskQueue.backends.memory import MemoryBackend
from TaskQueue.handle import JobHandle
from TaskQueue.job import Job, JobStatus

pytestmark = pytest.mark.timeout(5)


async def _enqueued() -> tuple[MemoryBackend, Job, JobHandle[object]]:
    be = MemoryBackend()
    job = Job(task_name="t")
    await be.enqueue(job)
    return be, job, JobHandle(job.id, be)


async def test_exposes_job_id() -> None:
    be, job, handle = await _enqueued()
    assert handle.job_id == job.id


async def test_result_returns_stored_value() -> None:
    be, job, handle = await _enqueued()
    await be.store_result(job.id, 42)
    assert await asyncio.wait_for(handle.result(), 1) == 42


async def test_result_none_does_not_raise() -> None:
    be, job, handle = await _enqueued()
    await be.store_result(job.id, None)
    assert await asyncio.wait_for(handle.result(), 1) is None


async def test_result_raises_on_stored_error() -> None:
    be, job, handle = await _enqueued()
    await be.store_error(job.id, "kaboom")
    with pytest.raises(RuntimeError, match="kaboom"):
        await asyncio.wait_for(handle.result(), 1)


async def test_result_waits_for_completion() -> None:
    be, job, handle = await _enqueued()

    async def finish_soon() -> None:
        await asyncio.sleep(0.05)
        await be.store_result(job.id, 7)

    asyncio.create_task(finish_soon())
    assert await asyncio.wait_for(handle.result(), 1) == 7


async def test_status_tracks_backend_state() -> None:
    be, job, handle = await _enqueued()
    assert await handle.status() == JobStatus.QUEUED
    await be.store_result(job.id, 1)
    assert await handle.status() == JobStatus.COMPLETED
