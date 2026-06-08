"""MemoryBackend operations, including async-blocking claim() and event waking."""

import asyncio

import pytest

from TaskQueue.backends.memory import MemoryBackend
from TaskQueue.job import Job, JobStatus

pytestmark = pytest.mark.timeout(5)


async def test_enqueue_marks_queued() -> None:
    be = MemoryBackend()
    j = Job(task_name="t")
    await be.enqueue(j)
    assert (await be.get_job(j.id)).status is JobStatus.QUEUED


async def test_claim_returns_job_and_marks_running() -> None:
    be = MemoryBackend()
    j = Job(task_name="t")
    await be.enqueue(j)
    claimed = await be.claim()
    assert claimed.id == j.id
    assert claimed.status is JobStatus.RUNNING


async def test_claim_blocks_until_a_job_is_available() -> None:
    be = MemoryBackend()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.claim(), timeout=0.1)
    j = Job(task_name="t")
    await be.enqueue(j)
    assert (await asyncio.wait_for(be.claim(), timeout=1)).id == j.id


async def test_claim_is_fifo() -> None:
    be = MemoryBackend()
    ids = []
    for _ in range(3):
        j = Job(task_name="t")
        await be.enqueue(j)
        ids.append(j.id)
    assert [(await be.claim()).id for _ in range(3)] == ids


async def test_get_unknown_job_raises() -> None:
    be = MemoryBackend()
    with pytest.raises(KeyError):
        await be.get_job("nope")


async def test_store_result_completes_and_wakes_waiter() -> None:
    be = MemoryBackend()
    j = Job(task_name="t")
    await be.enqueue(j)
    await be.store_result(j.id, 99)
    await asyncio.wait_for(be.wait_for(j.id), timeout=1)
    got = await be.get_job(j.id)
    assert got.status is JobStatus.COMPLETED
    assert got.result == 99


async def test_store_error_fails_and_wakes_waiter() -> None:
    be = MemoryBackend()
    j = Job(task_name="t")
    await be.enqueue(j)
    await be.store_error(j.id, "boom")
    await asyncio.wait_for(be.wait_for(j.id), timeout=1)
    got = await be.get_job(j.id)
    assert got.status is JobStatus.FAILED
    assert got.error == "boom"


async def test_wait_for_returns_immediately_if_already_done() -> None:
    be = MemoryBackend()
    j = Job(task_name="t")
    await be.enqueue(j)
    await be.store_result(j.id, 1)
    await asyncio.wait_for(be.wait_for(j.id), timeout=0.5)


async def test_wait_for_blocks_until_done() -> None:
    be = MemoryBackend()
    j = Job(task_name="t")
    await be.enqueue(j)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.wait_for(j.id), timeout=0.1)


async def test_concurrent_claims_split_the_jobs() -> None:
    be = MemoryBackend()
    jobs = [Job(task_name="t") for _ in range(2)]
    for j in jobs:
        await be.enqueue(j)
    a, b = await asyncio.gather(be.claim(), be.claim())
    assert {a.id, b.id} == {j.id for j in jobs}
