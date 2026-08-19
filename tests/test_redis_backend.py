"""RedisBackend operations against fakeredis: enqueue/claim/get_job round-trip
through real Redis commands (HSET/RPUSH/BLMOVE/HGETALL), unlike MemoryBackend
which keeps everything as live Python objects.

A record is built with Job.to_record(serializer) and read back with
Job.from_record(record, serializer); the backend never deserializes payloads.
"""

import asyncio

import fakeredis
import pytest

from TaskQueue.backends.redis_backend import RedisBackend
from TaskQueue.job import Job, JobStatus
from TaskQueue.serializers import Serializer

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
async def redis_backend() -> RedisBackend:
    client = fakeredis.FakeAsyncRedis()
    return RedisBackend(client)


async def _enqueue(be: RedisBackend, serializer: Serializer, **kw: object) -> Job:
    job = Job(task_name=str(kw.pop("task_name", "t")), **kw)  # type: ignore[arg-type]
    await be.enqueue(job.id, job.to_record(serializer))
    return job


async def test_enqueue_marks_queued(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    rec = await redis_backend.get_job(job.id)
    assert JobStatus(rec["status"]) is JobStatus.QUEUED


async def test_get_job_round_trips_payload(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    rec = await redis_backend.get_job(job.id)
    got = Job.from_record(rec, serializer)
    assert got.id == job.id
    assert got.task_name == job.task_name


async def test_get_unknown_job_raises(redis_backend: RedisBackend) -> None:
    with pytest.raises(KeyError):
        await redis_backend.get_job("nope")


async def test_claim_returns_record_and_marks_running(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    claimed = await redis_backend.claim()
    assert claimed["id"] == job.id
    assert JobStatus(claimed["status"]) is JobStatus.RUNNING
    # claim() writes RUNNING back to storage, not just the returned copy.
    assert (
        JobStatus((await redis_backend.get_job(job.id))["status"]) is JobStatus.RUNNING
    )


@pytest.mark.xfail(
    reason="fakeredis returns None immediately for BLMOVE timeout=0 instead of "
    "blocking forever like real Redis; see RedisBackend.claim's None guard.",
    strict=True,
)
async def test_claim_blocks_until_a_job_is_available(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(redis_backend.claim(), timeout=0.1)
    job = await _enqueue(redis_backend, serializer)
    claimed = await asyncio.wait_for(redis_backend.claim(), timeout=1)
    assert claimed["id"] == job.id


async def test_claim_is_fifo(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    ids = [(await _enqueue(redis_backend, serializer)).id for _ in range(3)]
    assert [(await redis_backend.claim())["id"] for _ in range(3)] == ids


async def test_concurrent_claims_split_the_jobs(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    jobs = [await _enqueue(redis_backend, serializer) for _ in range(2)]
    a, b = await asyncio.gather(redis_backend.claim(), redis_backend.claim())
    assert {a["id"], b["id"]} == {j.id for j in jobs}
