"""RedisBackend operations against fakeredis: enqueue/claim/get_job round-trip
through real Redis commands (HSET/RPUSH/BLMOVE/HGETALL), unlike MemoryBackend
which keeps everything as live Python objects.

A record is built with Job.to_record(serializer) and read back with
Job.from_record(record, serializer); the backend never deserializes payloads.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import fakeredis
import pytest
import redis.asyncio.client as redis_client_module

from TaskQueue.backends import redis_backend as backend_module
from TaskQueue.backends.redis_backend import (
    DEFAULT_RESULT_TTL_SECONDS,
    QUEUE,
    WORKERS,
    RedisBackend,
    cancel_channel,
    done_channel,
    job_key,
    processing_key,
)
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


async def _finish(
    be: RedisBackend,
    job: Job,
    serializer: Serializer,
    *,
    result: object = None,
    status: JobStatus = JobStatus.COMPLETED,
) -> None:
    job.status = status
    job.result = result
    await be.save(job.id, job.to_record(serializer), done=True)


@contextlib.asynccontextmanager
async def _slow_persist(be: RedisBackend, delay: float) -> AsyncIterator[None]:
    original = be.redis.pipeline

    def pipeline(**kwargs: object) -> Any:
        inner = original(**kwargs)  # type: ignore[arg-type]

        class Delayed:
            async def __aenter__(self) -> Any:
                pipe = await inner.__aenter__()
                execute = pipe.execute

                async def delayed_execute() -> Any:
                    await asyncio.sleep(delay)
                    return await execute()

                pipe.execute = delayed_execute
                return pipe

            async def __aexit__(self, *exc: object) -> Any:
                return await inner.__aexit__(*exc)

        return Delayed()

    be.redis.pipeline = pipeline  # type: ignore[assignment]
    try:
        yield
    finally:
        be.redis.pipeline = original  # type: ignore[assignment]


async def _status_seen_when_woken(
    be: RedisBackend, job_id: str, seen: list[str]
) -> None:
    async with be.redis.pubsub() as pubsub:  # pyright: ignore[reportUnknownMemberType]
        await pubsub.subscribe(done_channel(job_id))
        async for message in pubsub.listen():
            if message["type"] == "message":
                raw = await be.redis.hgetall(job_key(job_id))
                seen.append(raw[b"status"].decode())
                return


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


async def test_claim_yields_none_on_an_empty_queue(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    assert await asyncio.wait_for(redis_backend.claim(), timeout=2) is None

    job = await _enqueue(redis_backend, serializer)
    claimed = await asyncio.wait_for(redis_backend.claim(), timeout=2)
    assert claimed is not None
    assert claimed["id"] == job.id


async def test_claim_stays_cancellable(redis_backend: RedisBackend) -> None:
    claiming = asyncio.create_task(redis_backend.claim())
    claiming.cancel()
    with pytest.raises(asyncio.CancelledError):
        await claiming


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


# --------------------------------------------------------------------------
# take_result
# --------------------------------------------------------------------------


async def test_take_result_returns_the_terminal_record(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await _finish(redis_backend, job, serializer, result=[1, 2])

    record = await redis_backend.take_result(job.id)

    finished = Job.from_record(record, serializer)
    assert finished.id == job.id
    assert finished.status is JobStatus.COMPLETED
    assert finished.result == [1, 2]


async def test_take_result_frees_the_record(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    # Consuming read: the result is delivered exactly once, so a queue that
    # nobody drains cannot grow without bound.
    job = await _enqueue(redis_backend, serializer)
    await _finish(redis_backend, job, serializer)

    await redis_backend.take_result(job.id)

    with pytest.raises(KeyError):
        await redis_backend.get_job(job.id)


async def test_take_result_returns_at_once_when_already_terminal(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    # The job finished before anyone asked: no PUBLISH is coming, so this must
    # be answered from the stored status rather than waiting for a message.
    job = await _enqueue(redis_backend, serializer)
    await _finish(redis_backend, job, serializer)

    record = await asyncio.wait_for(redis_backend.take_result(job.id), timeout=1)

    assert JobStatus(record["status"]) is JobStatus.COMPLETED


async def test_take_result_blocks_until_the_job_finishes(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    waiter = asyncio.create_task(redis_backend.take_result(job.id))

    # Still QUEUED: the call must not return a non-terminal record.
    await asyncio.sleep(0.05)
    assert not waiter.done()

    await _finish(redis_backend, job, serializer, result="done")
    record = await asyncio.wait_for(waiter, timeout=2)
    assert Job.from_record(record, serializer).result == "done"


@pytest.mark.parametrize(
    "status", [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
)
async def test_every_terminal_status_wakes_a_waiter(
    redis_backend: RedisBackend, serializer: Serializer, status: JobStatus
) -> None:
    # FAILED and CANCELLED are terminal too; waking only on COMPLETED would
    # hang result() for every failed job.
    job = await _enqueue(redis_backend, serializer)
    waiter = asyncio.create_task(redis_backend.take_result(job.id))
    await asyncio.sleep(0.05)

    await _finish(redis_backend, job, serializer, status=status)
    record = await asyncio.wait_for(waiter, timeout=2)
    assert JobStatus(record["status"]) is status


async def test_take_result_of_unknown_job_raises(redis_backend: RedisBackend) -> None:
    with pytest.raises(KeyError):
        await redis_backend.take_result("nope")


async def test_take_result_is_single_consumer(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    # Two callers race the same finished job: exactly one is handed the record,
    # the other is told it is gone.
    #
    # This pins the contract, not the mechanism. A single event loop over
    # fakeredis happens not to interleave HGETALL and DEL, so this test passes
    # even against a non-atomic implementation; adding an await between the two
    # makes both callers win. The MULTI is what holds when the callers are
    # different processes, which no in-process test can reach.
    job = await _enqueue(redis_backend, serializer)
    await _finish(redis_backend, job, serializer, result=7)

    outcomes = await asyncio.gather(
        redis_backend.take_result(job.id),
        redis_backend.take_result(job.id),
        return_exceptions=True,
    )

    delivered = [o for o in outcomes if isinstance(o, dict)]
    refused = [o for o in outcomes if isinstance(o, KeyError)]
    assert len(delivered) == 1, outcomes
    assert len(refused) == 1, outcomes
    assert Job.from_record(delivered[0], serializer).result == 7


async def test_a_publish_with_no_subscriber_is_not_replayed(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    # Guards the subscribe-then-check ordering. The PUBLISH here reaches nobody
    # and Redis drops it — unlike an asyncio.Event, which would stay set. If
    # take_result checked the status before subscribing, a job finishing in that
    # gap would leave it waiting for a message that never comes again.
    job = await _enqueue(redis_backend, serializer)
    await _finish(redis_backend, job, serializer, result="early")

    record = await asyncio.wait_for(redis_backend.take_result(job.id), timeout=1)
    assert Job.from_record(record, serializer).result == "early"


async def test_take_result_wakes_from_the_status_when_the_publish_is_lost(
    redis_backend: RedisBackend,
    serializer: Serializer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The terminal record is written straight to the hash, so no message is ever
    # published -- what a dropped PUBLISH looks like from the waiter's side.
    # Redis pub/sub is fire-and-forget, so only the poll can end this wait.
    monkeypatch.setattr(backend_module, "_NOTIFY_POLL_SECONDS", 0.1)
    job = await _enqueue(redis_backend, serializer)
    waiter = asyncio.create_task(redis_backend.take_result(job.id))
    await asyncio.sleep(0.02)
    assert not waiter.done()

    job.status = JobStatus.COMPLETED
    job.result = "late"
    await redis_backend.redis.hset(  # type: ignore[misc]
        job_key(job.id), mapping=job.to_record(serializer)
    )

    record = await asyncio.wait_for(waiter, timeout=2)
    assert Job.from_record(record, serializer).result == "late"


async def test_enqueue_stores_the_record_and_makes_it_claimable(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    assert await redis_backend.redis.exists(job_key(job.id)) == 1
    assert await redis_backend.redis.lrange("queue", 0, -1) == [job.id.encode()]


async def test_enqueue_overrides_the_status_to_queued(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = Job(task_name="t", status=JobStatus.COMPLETED)
    await redis_backend.enqueue(job.id, job.to_record(serializer))
    assert (
        JobStatus((await redis_backend.get_job(job.id))["status"]) is JobStatus.QUEUED
    )


async def test_get_job_returns_a_detached_copy(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    record = await redis_backend.get_job(job.id)
    record["status"] = JobStatus.FAILED.value
    assert (
        JobStatus((await redis_backend.get_job(job.id))["status"]) is JobStatus.QUEUED
    )


async def test_claim_moves_the_job_off_the_queue(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    assert await redis_backend.redis.lrange("queue", 0, -1) == []
    assert await redis_backend.redis.lrange(redis_backend._processing_key, 0, -1) == [
        job.id.encode()
    ]


async def test_claim_returns_a_detached_copy(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    claimed = await redis_backend.claim()
    claimed["task_name"] = "mutated"
    assert (await redis_backend.get_job(job.id))["task_name"] == job.task_name


async def test_claim_counts_the_delivery(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    assert (await redis_backend.get_job(job.id))["attempts"] == "0"

    claimed = await redis_backend.claim()
    assert claimed is not None
    assert claimed["attempts"] == "1"  # visible to the worker that claimed it
    assert (await redis_backend.get_job(job.id))["attempts"] == "1"  # and persisted


async def test_a_redelivered_job_counts_each_delivery(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await redis_backend.release(job.id)

    redelivered = await redis_backend.claim()
    assert redelivered is not None
    assert redelivered["attempts"] == "2"


# --------------------------------------------------------------------------
# save
# --------------------------------------------------------------------------


async def test_save_persists_a_non_terminal_transition(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    job.attempts = 2
    await redis_backend.save(job.id, job.to_record(serializer))
    assert (await redis_backend.get_job(job.id))["attempts"] == "2"


async def test_save_without_done_does_not_wake_a_waiter(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    waiter = asyncio.create_task(redis_backend.take_result(job.id))
    await asyncio.sleep(0.05)

    job.attempts = 1
    await redis_backend.save(job.id, job.to_record(serializer))
    await asyncio.sleep(0.05)

    assert not waiter.done()
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)


async def test_terminal_save_wakes_a_waiter(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    waiter = asyncio.create_task(redis_backend.take_result(job.id))
    await asyncio.sleep(0.05)

    job.status = JobStatus.COMPLETED
    job.result = "woken"
    await redis_backend.save(job.id, job.to_record(serializer), done=True)

    record = await asyncio.wait_for(waiter, timeout=2)
    assert Job.from_record(record, serializer).result == "woken"


async def test_save_of_an_unknown_job_raises(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = Job(task_name="t")
    with pytest.raises(KeyError):
        await redis_backend.save(job.id, job.to_record(serializer))


async def test_save_of_an_unknown_job_creates_nothing(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = Job(task_name="t")
    with pytest.raises(KeyError):
        await redis_backend.save(job.id, job.to_record(serializer), done=True)
    assert await redis_backend.redis.exists(job_key(job.id)) == 0


async def test_save_after_the_result_was_taken_does_not_resurrect_it(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await _finish(redis_backend, job, serializer, result=1)
    await redis_backend.take_result(job.id)

    with pytest.raises(KeyError):
        await redis_backend.save(job.id, job.to_record(serializer), done=True)
    assert await redis_backend.redis.exists(job_key(job.id)) == 0


async def test_terminal_save_persists_before_it_notifies(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    seen: list[str] = []
    watcher = asyncio.create_task(_status_seen_when_woken(redis_backend, job.id, seen))
    await asyncio.sleep(0.05)

    job.status = JobStatus.COMPLETED
    record = job.to_record(serializer)
    async with _slow_persist(redis_backend, 0.15):
        await redis_backend.save(job.id, record, done=True)

    await asyncio.wait_for(watcher, timeout=3)
    assert seen == [JobStatus.COMPLETED.value]


async def test_release_returns_a_running_job_to_the_queue(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()

    await redis_backend.release(job.id)

    assert (
        JobStatus((await redis_backend.get_job(job.id))["status"]) is JobStatus.QUEUED
    )
    assert await redis_backend.redis.lrange("queue", 0, -1) == [job.id.encode()]
    assert await redis_backend.redis.lrange(redis_backend._processing_key, 0, -1) == []


async def test_a_released_job_is_claimable_again(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await redis_backend.release(job.id)

    reclaimed = await redis_backend.claim()

    assert reclaimed["id"] == job.id
    assert JobStatus(reclaimed["status"]) is JobStatus.RUNNING


@pytest.mark.parametrize(
    "status", [JobStatus.QUEUED, JobStatus.COMPLETED, JobStatus.FAILED]
)
async def test_release_is_a_no_op_unless_the_job_is_running(
    redis_backend: RedisBackend, serializer: Serializer, status: JobStatus
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    job.status = status
    await redis_backend.save(job.id, job.to_record(serializer))

    await redis_backend.release(job.id)

    assert JobStatus((await redis_backend.get_job(job.id))["status"]) is status
    assert await redis_backend.redis.lrange("queue", 0, -1) == []


async def test_double_release_queues_the_job_once(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()

    await redis_backend.release(job.id)
    await redis_backend.release(job.id)

    assert await redis_backend.redis.lrange("queue", 0, -1) == [job.id.encode()]


async def test_release_of_an_unknown_job_raises(redis_backend: RedisBackend) -> None:
    with pytest.raises(KeyError):
        await redis_backend.release("nope")


async def test_release_of_an_unknown_job_creates_nothing(
    redis_backend: RedisBackend,
) -> None:
    with pytest.raises(KeyError):
        await redis_backend.release("nope")
    assert await redis_backend.redis.exists(job_key("nope")) == 0


async def test_terminal_save_acks_the_lease(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await _finish(redis_backend, job, serializer)

    assert await redis_backend.redis.lrange(redis_backend._processing_key, 0, -1) == []


async def test_non_terminal_save_keeps_the_lease(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()

    job.attempts = 1
    await redis_backend.save(job.id, job.to_record(serializer))

    assert await redis_backend.redis.lrange(redis_backend._processing_key, 0, -1) == [
        job.id.encode()
    ]


async def _cancel_message(be: RedisBackend, job_id: str, seen: list[str]) -> None:
    async with be.redis.pubsub() as pubsub:  # pyright: ignore[reportUnknownMemberType]
        await pubsub.subscribe(cancel_channel(job_id))
        async for message in pubsub.listen():
            if message["type"] == "message":
                seen.append(message["channel"].decode())
                return


async def test_request_cancel_flags_a_running_job(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()

    await redis_backend.request_cancel(job.id)

    record = await redis_backend.get_job(job.id)
    assert record["request_cancel"] == "1"
    assert Job.from_record(record, serializer).request_cancel is True


async def test_request_cancel_notifies_a_waiting_worker(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    seen: list[str] = []
    watcher = asyncio.create_task(_cancel_message(redis_backend, job.id, seen))
    await asyncio.sleep(0.05)

    await redis_backend.request_cancel(job.id)

    await asyncio.wait_for(watcher, timeout=2)
    assert seen == [cancel_channel(job.id)]


async def test_request_cancel_is_idempotent(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()

    await redis_backend.request_cancel(job.id)
    await redis_backend.request_cancel(job.id)

    assert (await redis_backend.get_job(job.id))["request_cancel"] == "1"


@pytest.mark.parametrize(
    "status", [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
)
async def test_completion_wins_over_a_cancel_request(
    redis_backend: RedisBackend, serializer: Serializer, status: JobStatus
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await _finish(redis_backend, job, serializer, status=status)

    await redis_backend.request_cancel(job.id)

    assert (await redis_backend.get_job(job.id))["request_cancel"] == "0"


async def test_request_cancel_of_an_unknown_job_is_a_no_op(
    redis_backend: RedisBackend,
) -> None:
    await redis_backend.request_cancel("nope")
    assert await redis_backend.redis.exists(job_key("nope")) == 0


async def test_request_cancel_after_the_result_was_taken_does_not_resurrect_it(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await _finish(redis_backend, job, serializer, result=1)
    await redis_backend.take_result(job.id)

    await redis_backend.request_cancel(job.id)

    assert await redis_backend.redis.exists(job_key(job.id)) == 0


async def test_terminal_lua_table_tracks_the_job_status_enum() -> None:
    from TaskQueue.backends.redis_backend import _TERMINAL, _TERMINAL_LUA

    for status in _TERMINAL:
        assert f"{status} = true" in _TERMINAL_LUA


async def test_wait_cancel_blocks_until_cancellation_is_requested(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    waiter = asyncio.create_task(redis_backend.wait_cancel(job.id))

    await asyncio.sleep(0.05)
    assert not waiter.done()

    await redis_backend.request_cancel(job.id)
    await asyncio.wait_for(waiter, timeout=2)


async def test_wait_cancel_returns_at_once_when_already_requested(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await redis_backend.request_cancel(job.id)

    await asyncio.wait_for(redis_backend.wait_cancel(job.id), timeout=1)


async def test_wait_cancel_reads_the_flag_not_its_truthiness(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    assert (await redis_backend.get_job(job.id))["request_cancel"] == "0"

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(redis_backend.wait_cancel(job.id), timeout=0.2)


async def test_wait_cancel_of_an_unknown_job_raises(
    redis_backend: RedisBackend,
) -> None:
    with pytest.raises(KeyError):
        await asyncio.wait_for(redis_backend.wait_cancel("nope"), timeout=1)


async def test_wait_cancel_wakes_from_the_flag_when_the_publish_is_lost(
    redis_backend: RedisBackend,
    serializer: Serializer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The flag is set without going through request_cancel, so nothing is ever
    # published. The record is the truth and the message is only the news, so
    # the poll has to re-read the flag or this waiter never wakes.
    monkeypatch.setattr(backend_module, "_NOTIFY_POLL_SECONDS", 0.1)
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    waiter = asyncio.create_task(redis_backend.wait_cancel(job.id))
    await asyncio.sleep(0.02)
    assert not waiter.done()

    await redis_backend.redis.hset(job_key(job.id), "request_cancel", "1")  # type: ignore[misc]

    await asyncio.wait_for(waiter, timeout=2)


async def test_wait_cancel_keeps_waiting_when_the_record_disappears(
    redis_backend: RedisBackend,
    serializer: Serializer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A vanished record means the job FINISHED -- its result was taken, or the
    # TTL reaped it -- not that it was cancelled. Worker._process reads any
    # return from wait_cancel as a cancellation, so the poll must stay silent
    # here rather than mark a job CANCELLED that nobody cancelled. Waiting on is
    # safe: the worker cancels this task itself once the job settles.
    monkeypatch.setattr(backend_module, "_NOTIFY_POLL_SECONDS", 0.05)
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    waiter = asyncio.create_task(redis_backend.wait_cancel(job.id))
    await asyncio.sleep(0.02)

    await redis_backend.redis.delete(job_key(job.id))
    await asyncio.sleep(0.3)  # several poll intervals

    assert not waiter.done()
    waiter.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await waiter


async def test_request_cancel_many_flags_every_job(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    jobs = [await _enqueue(redis_backend, serializer) for _ in range(5)]
    for _ in jobs:
        await redis_backend.claim()

    await redis_backend.request_cancel_many([job.id for job in jobs])

    for job in jobs:
        assert (await redis_backend.get_job(job.id))["request_cancel"] == "1"


async def test_request_cancel_many_wakes_every_waiter(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    jobs = [await _enqueue(redis_backend, serializer) for _ in range(3)]
    for _ in jobs:
        await redis_backend.claim()
    waiters = [asyncio.create_task(redis_backend.wait_cancel(job.id)) for job in jobs]
    await asyncio.sleep(0.05)

    await redis_backend.request_cancel_many([job.id for job in jobs])

    await asyncio.wait_for(asyncio.gather(*waiters), timeout=2)


async def test_request_cancel_many_skips_terminal_and_unknown_ids(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    running = await _enqueue(redis_backend, serializer)
    finished = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await redis_backend.claim()
    await _finish(redis_backend, finished, serializer)

    await redis_backend.request_cancel_many([running.id, finished.id, "nope"])

    assert (await redis_backend.get_job(running.id))["request_cancel"] == "1"
    assert (await redis_backend.get_job(finished.id))["request_cancel"] == "0"
    assert await redis_backend.redis.exists(job_key("nope")) == 0


async def test_request_cancel_many_of_nothing_is_a_no_op(
    redis_backend: RedisBackend,
) -> None:
    await redis_backend.request_cancel_many([])


async def test_request_cancel_many_batches_into_one_round_trip(
    redis_backend: RedisBackend,
    serializer: Serializer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = [await _enqueue(redis_backend, serializer) for _ in range(5)]
    for _ in jobs:
        await redis_backend.claim()
    await redis_backend.request_cancel_many([jobs[0].id])

    issued: list[str] = []
    original = redis_client_module.Redis.execute_command

    async def traced(self: Any, *args: Any, **kwargs: Any) -> Any:
        issued.append(str(args[0]))
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(redis_client_module.Redis, "execute_command", traced)
    await redis_backend.request_cancel_many([job.id for job in jobs])

    assert issued == []


async def test_claim_discards_a_queued_job_whose_record_is_gone(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    orphan = await _enqueue(redis_backend, serializer)
    wanted = await _enqueue(redis_backend, serializer)
    await redis_backend.redis.delete(job_key(orphan.id))

    assert await asyncio.wait_for(redis_backend.claim(), timeout=2) is None
    claimed = await asyncio.wait_for(redis_backend.claim(), timeout=2)

    assert claimed is not None
    assert claimed["id"] == wanted.id
    assert await redis_backend.redis.lrange(redis_backend._processing_key, 0, -1) == [
        wanted.id.encode()
    ]


async def test_claim_does_not_resurrect_a_deleted_record(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    orphan = await _enqueue(redis_backend, serializer)
    await redis_backend.redis.delete(job_key(orphan.id))
    filler = await _enqueue(redis_backend, serializer)

    await asyncio.wait_for(redis_backend.claim(), timeout=2)

    assert await redis_backend.redis.exists(job_key(orphan.id)) == 0
    assert filler.id


# --------------------------------------------------------------------------
# result_ttl
# --------------------------------------------------------------------------


@pytest.mark.parametrize("ttl", [0, -1])
def test_non_positive_result_ttl_is_rejected(ttl: int) -> None:
    with pytest.raises(ValueError, match="result_ttl"):
        RedisBackend(fakeredis.FakeAsyncRedis(), result_ttl=ttl)


def test_default_result_ttl_is_one_day() -> None:
    assert DEFAULT_RESULT_TTL_SECONDS == 86_400
    assert RedisBackend(fakeredis.FakeAsyncRedis()).result_ttl == 86_400


async def test_terminal_save_expires_the_record(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await _finish(redis_backend, job, serializer, result=1)

    ttl = await redis_backend.redis.ttl(job_key(job.id))
    assert 0 < ttl <= redis_backend.result_ttl


async def test_non_terminal_save_leaves_the_record_unexpiring(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.claim()
    await redis_backend.save(job.id, job.to_record(serializer))

    assert await redis_backend.redis.ttl(job_key(job.id)) == -1


async def test_result_ttl_reaps_an_uncollected_result(serializer: Serializer) -> None:
    backend = RedisBackend(fakeredis.FakeAsyncRedis(), result_ttl=1)
    job = await _enqueue(backend, serializer)
    await backend.claim()
    await _finish(backend, job, serializer, result=1)

    assert await backend.redis.exists(job_key(job.id)) == 1
    await asyncio.sleep(1.1)
    assert await backend.redis.exists(job_key(job.id)) == 0


# --------------------------------------------------------------------------
# heartbeat + reap
# --------------------------------------------------------------------------


async def _strand(be: RedisBackend, serializer: Serializer, consumer: str) -> Job:
    """Put a job on 'consumer's processing list as if that worker had claimed it."""
    job = Job(task_name="t")
    job.status = JobStatus.RUNNING
    await be.redis.hset(  # pyright: ignore[reportUnknownMemberType]
        job_key(job.id), mapping=job.to_record(serializer)
    )
    await be.redis.rpush(processing_key(consumer), job.id)  # pyright: ignore[reportUnknownMemberType]
    return job


async def _kill(be: RedisBackend, consumer: str) -> None:
    """Register 'consumer' with a heartbeat old enough to be presumed dead."""
    await be.redis.zadd(WORKERS, {consumer: 0})  # pyright: ignore[reportUnknownMemberType]


async def test_heartbeat_registers_the_worker(redis_backend: RedisBackend) -> None:
    assert await redis_backend.redis.zscore(WORKERS, redis_backend._instance_id) is None

    await redis_backend.heartbeat()

    score = await redis_backend.redis.zscore(WORKERS, redis_backend._instance_id)
    assert score is not None and score > 0


async def test_heartbeat_refreshes_the_same_entry(redis_backend: RedisBackend) -> None:
    await redis_backend.heartbeat()
    await redis_backend.heartbeat()
    assert await redis_backend.redis.zcard(WORKERS) == 1


async def test_reap_reclaims_a_dead_workers_jobs(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _strand(redis_backend, serializer, "ghost")
    await _kill(redis_backend, "ghost")

    assert await redis_backend.reap() == 1

    assert await redis_backend.redis.lrange(QUEUE, 0, -1) == [job.id.encode()]
    assert await redis_backend.redis.exists(processing_key("ghost")) == 0
    assert await redis_backend.redis.zscore(WORKERS, "ghost") is None


async def test_reap_resets_status_and_leaves_counting_to_the_redelivery(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _strand(redis_backend, serializer, "ghost")
    await _kill(redis_backend, "ghost")

    await redis_backend.reap()

    record = await redis_backend.get_job(job.id)
    assert JobStatus(record["status"]) is JobStatus.QUEUED
    assert record["attempts"] == "0"  # the reap itself is not an attempt

    claimed = await redis_backend.claim()
    assert claimed is not None
    assert claimed["attempts"] == "1"  # the redelivery is


async def test_reap_leaves_a_live_worker_alone(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    job = await _strand(redis_backend, serializer, redis_backend._instance_id)
    await redis_backend.heartbeat()

    assert await redis_backend.reap() == 0

    assert await redis_backend.redis.lrange(QUEUE, 0, -1) == []
    assert await redis_backend.redis.lrange(redis_backend._processing_key, 0, -1) == [
        job.id.encode()
    ]


async def test_reap_drops_a_job_whose_record_is_gone(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    kept = await _strand(redis_backend, serializer, "ghost")
    gone = await _strand(redis_backend, serializer, "ghost")
    await redis_backend.redis.delete(job_key(gone.id))
    await _kill(redis_backend, "ghost")

    assert await redis_backend.reap() == 1

    assert await redis_backend.redis.lrange(QUEUE, 0, -1) == [kept.id.encode()]


async def test_repeated_reaps_stay_clean(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    # A reaper that forgets to ZREM the dead worker re-processes an emptied list
    # on the next pass and errors from then on.
    await _strand(redis_backend, serializer, "ghost")
    await _kill(redis_backend, "ghost")

    assert await redis_backend.reap() == 1
    for _ in range(3):
        assert await redis_backend.reap() == 0
    assert await redis_backend.redis.llen(QUEUE) == 1


async def test_concurrent_reaps_requeue_each_job_once(serializer: Serializer) -> None:
    client = fakeredis.FakeAsyncRedis()
    reapers = [RedisBackend(client) for _ in range(4)]
    jobs = [await _strand(reapers[0], serializer, "ghost") for _ in range(5)]
    await _kill(reapers[0], "ghost")

    # Nothing coordinates these: correctness comes from the reclaim being one
    # atomic step, so exactly one caller finds work and the rest find none.
    counts = await asyncio.gather(*(r.reap() for r in reapers))

    assert sorted(counts) == [0, 0, 0, len(jobs)]
    queued = [x.decode() for x in await client.lrange(QUEUE, 0, -1)]
    assert sorted(queued) == sorted(j.id for j in jobs)


@pytest.mark.parametrize("ttl", [0, -1])
def test_non_positive_worker_ttl_is_rejected(ttl: int) -> None:
    with pytest.raises(ValueError, match="worker_ttl"):
        RedisBackend(fakeredis.FakeAsyncRedis(), worker_ttl=ttl)


async def test_a_lease_stranded_before_the_status_write_is_still_counted(
    redis_backend: RedisBackend, serializer: Serializer
) -> None:
    """'claim' leases in two steps: BLMOVE, then the script that marks RUNNING.

    A worker killed between them leaves the job on its processing list while the
    record still says QUEUED. That half-delivery must not let the eventual real
    delivery go uncounted.
    """
    job = await _enqueue(redis_backend, serializer)
    await redis_backend.redis.lmove(QUEUE, processing_key("ghost"))
    half_delivered = await redis_backend.get_job(job.id)
    assert JobStatus(half_delivered["status"]) is JobStatus.QUEUED
    await _kill(redis_backend, "ghost")

    assert await redis_backend.reap() == 1

    claimed = await redis_backend.claim()
    assert claimed is not None and claimed["id"] == job.id
    assert claimed["attempts"] == "1"
