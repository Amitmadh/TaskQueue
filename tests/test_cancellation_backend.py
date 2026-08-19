"""Phase 2 backend cancellation primitives — the executable spec for cancel.

Adds two methods to the Phase 1 Backend contract:
  request_cancel(job_id) : flag a job for cancellation (idempotent). The signal
                           must be observable on the stored record AND wake any
                           wait_cancel() waiter.
  wait_cancel(job_id)    : block until the job is flagged; return immediately if
                           it already is.

Several tests here also guard review findings against the MemoryBackend. Each is
tagged  # guards #N  with the finding it protects; the code satisfies all of them
today, so a failure means a regression, not a TODO.

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


# --------------------------------------------------------------------------- #
# wait_cancel / request_cancel signalling                                     #
# --------------------------------------------------------------------------- #
async def test_wait_cancel_blocks_until_requested(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.wait_cancel(job.id), timeout=0.1)
    await be.request_cancel(job.id)
    await asyncio.wait_for(be.wait_cancel(job.id), timeout=1)  # now returns


async def test_wait_cancel_returns_immediately_if_already_requested(
    serializer: Serializer,
) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.request_cancel(job.id)
    await asyncio.wait_for(be.wait_cancel(job.id), timeout=0.5)


async def test_request_cancel_wakes_a_pending_waiter(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    waiter = asyncio.create_task(be.wait_cancel(job.id))
    await asyncio.sleep(0)  # let the waiter start blocking
    assert not waiter.done()
    await be.request_cancel(job.id)
    await asyncio.wait_for(waiter, timeout=1)


async def test_wait_cancel_unknown_job_raises() -> None:
    be = MemoryBackend()
    with pytest.raises(RuntimeError):  # mirrors wait()'s contract for unknown ids
        await be.wait_cancel("nope")


async def test_request_cancel_is_idempotent(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.request_cancel(job.id)
    await be.request_cancel(job.id)  # second call must not raise
    await asyncio.wait_for(be.wait_cancel(job.id), timeout=0.5)


# --------------------------------------------------------------------------- #
# the cancel signal must be observable on the record (not only in-memory)      #
# --------------------------------------------------------------------------- #
async def test_request_cancel_persists_to_record(serializer: Serializer) -> None:
    # guards #4: request_cancel sets record["request_cancel"] = "1", not just an
    # asyncio.Event — otherwise status()/another process could never see it.
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    assert (await be.get_job(job.id))["request_cancel"] == "0"
    await be.request_cancel(job.id)
    assert (await be.get_job(job.id))["request_cancel"] == "1"


async def test_request_cancel_before_claim_is_observable(
    serializer: Serializer,
) -> None:
    # guards #4: a still-QUEUED job that is cancelled stays observably cancelled
    # after it is claimed, so the worker can skip running it.
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.request_cancel(job.id)
    claimed = await be.claim()
    assert claimed["request_cancel"] == "1"


async def test_request_cancel_unknown_job_is_noop() -> None:
    # Under consume-to-free a missing record is a normal post-consumption state,
    # so cancelling an unknown / already-gone job is an idempotent no-op.
    be = MemoryBackend()
    await be.request_cancel("nope")  # must not raise


async def test_request_cancel_after_completion_is_noop(serializer: Serializer) -> None:
    # guards #7: completion wins. A job that already reached a terminal state must
    # not be flipped/flagged by a late cancel.
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.claim()
    await _finish(be, serializer, job, status=JobStatus.COMPLETED, result=1)
    await be.request_cancel(job.id)  # arrives too late
    rec = await be.get_job(job.id)
    assert JobStatus(rec["status"]) is JobStatus.COMPLETED
    assert rec["request_cancel"] == "0"


# --------------------------------------------------------------------------- #
# detached-copy invariant: backend reads must not alias the stored record      #
# --------------------------------------------------------------------------- #
async def test_claim_returns_detached_copy(serializer: Serializer) -> None:
    # guards #3: claim() must return a copy. Mutating it must not corrupt storage.
    be = MemoryBackend()
    await _enqueue(be, serializer)
    claimed = await be.claim()
    claimed["status"] = "tampered"
    claimed["error"] = "injected"
    stored = await be.get_job(claimed["id"])
    assert stored["status"] == JobStatus.RUNNING.value
    assert "error" not in stored  # injected error must not reach storage


async def test_get_job_returns_detached_copy(serializer: Serializer) -> None:
    # guards #3: get_job() must return a copy too.
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    leaked = await be.get_job(job.id)
    leaked["status"] = "tampered"
    assert (await be.get_job(job.id))["status"] == JobStatus.QUEUED.value


# --------------------------------------------------------------------------- #
# the cancel signal is independent of the done/result signal                   #
# --------------------------------------------------------------------------- #
async def test_request_cancel_does_not_wake_result_waiter(
    serializer: Serializer,
) -> None:
    # Cancelling must not, by itself, wake wait() (the terminal/result signal).
    # Only a terminal save(done=True) may do that.
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.request_cancel(job.id)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(be.take_result(job.id), timeout=0.1)


async def test_terminal_save_after_cancel_wakes_result_waiter(
    serializer: Serializer,
) -> None:
    # The worker reacts to a cancel by writing a terminal CANCELLED record; that
    # write (done=True) is what releases result() waiters.
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    await be.claim()
    await be.request_cancel(job.id)
    await _finish(be, serializer, job, status=JobStatus.CANCELLED)
    rec = await asyncio.wait_for(be.take_result(job.id), timeout=1)
    assert JobStatus(rec["status"]) is JobStatus.CANCELLED


async def test_request_cancel_many_flags_all(serializer: Serializer) -> None:
    be = MemoryBackend()
    jobs = [await _enqueue(be, serializer) for _ in range(3)]
    await be.request_cancel_many([j.id for j in jobs])
    for j in jobs:
        assert (await be.get_job(j.id))["request_cancel"] == "1"


async def test_request_cancel_many_empty_is_noop() -> None:
    be = MemoryBackend()
    await be.request_cancel_many([])  # must not raise


async def test_request_cancel_many_tolerates_terminal_and_unknown(
    serializer: Serializer,
) -> None:
    # A batch may mix a live job, an already-finished job, and an id that never
    # existed: the live one is flagged, completion wins for the finished one, and
    # the unknown id is a silent no-op — none of them raise.
    be = MemoryBackend()
    live = await _enqueue(be, serializer)
    finished = await _enqueue(be, serializer)
    await _finish(be, serializer, finished, status=JobStatus.COMPLETED, result=1)

    await be.request_cancel_many([live.id, finished.id, "ghost"])  # must not raise

    assert (await be.get_job(live.id))["request_cancel"] == "1"
    assert (await be.get_job(finished.id))["request_cancel"] == "0"
    assert JobStatus((await be.get_job(finished.id))["status"]) is JobStatus.COMPLETED


async def test_request_cancel_many_wakes_all_waiters(serializer: Serializer) -> None:
    be = MemoryBackend()
    jobs = [await _enqueue(be, serializer) for _ in range(2)]
    waiters = [asyncio.create_task(be.wait_cancel(j.id)) for j in jobs]
    await asyncio.sleep(0)  # let them start blocking
    assert all(not w.done() for w in waiters)
    await be.request_cancel_many([j.id for j in jobs])
    await asyncio.wait_for(asyncio.gather(*waiters), timeout=1)


async def test_request_cancel_many_flags_a_running_job(serializer: Serializer) -> None:
    be = MemoryBackend()
    job = await _enqueue(be, serializer)
    claimed = await be.claim()  # QUEUED -> RUNNING
    assert JobStatus(claimed["status"]) is JobStatus.RUNNING
    await be.request_cancel_many([job.id])
    assert (await be.get_job(job.id))["request_cancel"] == "1"
