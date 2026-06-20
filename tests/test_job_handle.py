"""JobHandle as a typed promise — event-driven waiting, raising on failure.

Unit tests: JobHandle is driven directly against a backend, no Worker involved,
isolating the handle's behavior from the run loop. A handle is constructed with
(job_id, backend, serializer); a job is "finished" by writing a terminal record
via backend.save(..., done=True), exactly as the Worker does.
"""

import asyncio

import pytest

from TaskQueue.backends.memory import MemoryBackend
from TaskQueue.backends.serializer import PickleSerializer, Serializer
from TaskQueue.handle import JobHandle
from TaskQueue.job import Job, JobStatus

pytestmark = pytest.mark.timeout(5)


async def _enqueued() -> tuple[MemoryBackend, Serializer, Job, JobHandle[object]]:
    be = MemoryBackend()
    serializer = PickleSerializer()
    job = Job(task_name="t")
    await be.enqueue(job.id, job.to_record(serializer))
    return be, serializer, job, JobHandle(job.id, be, serializer)


async def _finish(
    be: MemoryBackend, serializer: Serializer, job: Job, **terminal: object
) -> None:
    for k, v in terminal.items():
        setattr(job, k, v)
    await be.save(job.id, job.to_record(serializer), done=True)


async def test_exposes_job_id() -> None:
    _, _, job, handle = await _enqueued()
    assert handle.job_id == job.id


async def test_result_returns_stored_value() -> None:
    be, serializer, job, handle = await _enqueued()
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=42)
    assert await asyncio.wait_for(handle.result(), 1) == 42


async def test_result_none_does_not_raise() -> None:
    be, serializer, job, handle = await _enqueued()
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=None)
    assert await asyncio.wait_for(handle.result(), 1) is None


async def test_result_raises_on_stored_error() -> None:
    be, serializer, job, handle = await _enqueued()
    await _finish(be, serializer, job, status=JobStatus.FAILED, error="kaboom")
    with pytest.raises(RuntimeError, match="kaboom"):
        await asyncio.wait_for(handle.result(), 1)


async def test_result_waits_for_completion() -> None:
    be, serializer, job, handle = await _enqueued()

    async def finish_soon() -> None:
        await asyncio.sleep(0.05)
        await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=7)

    asyncio.create_task(finish_soon())
    assert await asyncio.wait_for(handle.result(), 1) == 7


async def test_status_tracks_backend_state() -> None:
    be, serializer, job, handle = await _enqueued()
    assert await handle.status() == JobStatus.QUEUED
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=1)
    assert await handle.status() == JobStatus.COMPLETED
