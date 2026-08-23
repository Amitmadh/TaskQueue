"""MemoryBackend operations: async-blocking claim(), FIFO order, and the
terminal save(done=True) that wakes take_result() waiters.

A record is built with Job.to_record(serializer) and read back with
Job.from_record(record, serializer); the backend never deserializes payloads.
"""

import asyncio

import pytest

from TaskQueue.backends.memory_backend import MemoryBackend
from TaskQueue.job import Job, JobStatus
from TaskQueue.serializers import Serializer

pytestmark = pytest.mark.timeout(5)


async def _enqueue(be: MemoryBackend, serializer: Serializer, **kw: object) -> Job:
    job = Job(task_name=str(kw.pop("task_name", "t")), **kw)  # type: ignore[arg-type]
    await be.enqueue(job.id, job.to_record(serializer))
    return job


async def _finish(
    be: MemoryBackend, serializer: Serializer, job: Job, **terminal: object
) -> None:
    for k, v in terminal.items():
        setattr(job, k, v)
    await be.save(job.id, job.to_record(serializer), done=True)


async def test_enqueue_marks_queued(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    rec = await be.get_job(job.id)
    assert JobStatus(rec["status"]) is JobStatus.QUEUED


async def test_claim_returns_record_and_marks_running(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    claimed = await be.claim()
    assert claimed["id"] == job.id
    assert JobStatus(claimed["status"]) is JobStatus.RUNNING


async def test_claim_waits_for_a_job_then_yields_none(serializer: Serializer) -> None:
    be = MemoryBackend()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.claim(), timeout=0.1)
    assert await asyncio.wait_for(be.claim(), timeout=3) is None

    job = await _enqueue(be, serializer)
    claimed = await asyncio.wait_for(be.claim(), timeout=1)
    assert claimed is not None
    assert claimed["id"] == job.id


async def test_claim_is_fifo(serializer: Serializer) -> None:
    be = MemoryBackend()
    ids = [(await _enqueue(be, serializer)).id for _ in range(3)]
    assert [(await be.claim())["id"] for _ in range(3)] == ids


async def test_get_unknown_job_raises() -> None:
    be = MemoryBackend()
    with pytest.raises(KeyError):
        await be.get_job("nope")


async def test_save_unknown_job_raises(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = Job(task_name="t")
    with pytest.raises(KeyError):
        await be.save(job.id, job.to_record(serializer), done=True)


async def test_save_done_completes_and_wakes_waiter(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=99)
    rec = await asyncio.wait_for(be.take_result(job.id), timeout=1)
    got = Job.from_record(rec, serializer)
    assert got.status is JobStatus.COMPLETED
    assert got.result == 99


async def test_save_error_fails_and_wakes_waiter(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await _finish(be, serializer, job, status=JobStatus.FAILED, error="boom")
    rec = await asyncio.wait_for(be.take_result(job.id), timeout=1)
    got = Job.from_record(rec, serializer)
    assert got.status is JobStatus.FAILED
    assert got.error == "boom"


async def test_save_without_done_does_not_wake(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    job.status = JobStatus.RUNNING
    await be.save(job.id, job.to_record(serializer))  # done defaults to False
    assert JobStatus((await be.get_job(job.id))["status"]) is JobStatus.RUNNING
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.take_result(job.id), timeout=0.1)


async def test_take_result_returns_immediately_if_already_done(
    serializer: Serializer,
) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=1)
    await asyncio.wait_for(be.take_result(job.id), timeout=0.5)


async def test_take_result_blocks_until_done(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.take_result(job.id), timeout=0.1)


async def test_take_result_unknown_job_raises() -> None:
    be = MemoryBackend()
    with pytest.raises(KeyError):
        await be.take_result("nope")


async def test_concurrent_claims_split_the_jobs(serializer: Serializer) -> None:
    be = MemoryBackend()
    jobs = [await _enqueue(be, serializer) for _ in range(2)]
    a, b = await asyncio.gather(be.claim(), be.claim())
    assert {a["id"], b["id"]} == {j.id for j in jobs}


async def test_release_requeues_for_redelivery(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    claimed = await be.claim()
    assert JobStatus(claimed["status"]) is JobStatus.RUNNING
    await be.release(job.id)
    assert JobStatus((await be.get_job(job.id))["status"]) is JobStatus.QUEUED
    again = await asyncio.wait_for(be.claim(), 1)
    assert again["id"] == job.id  # re-claimable after release


async def test_release_unknown_job_raises() -> None:
    be = MemoryBackend()
    with pytest.raises(KeyError):
        await be.release("nope")


async def test_release_after_completion_is_noop(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.claim()
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=1)
    await be.release(job.id)  # already terminal -> no-op
    assert JobStatus((await be.get_job(job.id))["status"]) is JobStatus.COMPLETED
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.claim(), 0.1)  # not re-queued
