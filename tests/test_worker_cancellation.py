"""Phase 2 worker-side cancellation — the executable spec for how a running job
reacts to request_cancel, plus the related JobHandle behaviour.

Contract encoded here:
  * A RUNNING job that is cancelled reaches terminal CANCELLED, its done-event
    fires (result() waiters wake, they never hang), and the worker keeps serving.
  * Completion wins: a job that finishes before the cancel keeps its real result.
  * A job cancelled before it is claimed does not run its body to completion.
  * JobHandle.result() raises on a cancelled job; .status() reports CANCELLED.

Regression guards (each once pinned an open review finding; the code now passes
all of them, so a failure here means the bug was re-introduced):
  #1  the terminal write stays inside _process's try/except, so a non-serialisable
      result ends the job FAILED (result() raises) instead of stranding it.
  #2  the cancel-waiter task is cancelled on every path, including failures.
  #5  JobHandle.result() signals CANCELLED (raises JobCancelled), never None.
"""

import asyncio

import pytest

from TaskQueue import JobCancelled
from TaskQueue.job import JobStatus
from TaskQueue.queue import Queue

pytestmark = pytest.mark.timeout(10)


async def _wait_status(
    queue: Queue, job_id: str, target: JobStatus, timeout: float = 3
) -> None:
    async def loop() -> None:
        be = queue.backend
        while JobStatus((await be.get_job(job_id))["status"]) is not target:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(loop(), timeout)


# --------------------------------------------------------------------------- #
# cancelling a running job                                                     #
# --------------------------------------------------------------------------- #
async def test_cancel_running_job_reaches_cancelled(queue: Queue) -> None:
    started = asyncio.Event()
    forever = asyncio.Event()  # never set: the job only ends via cancellation

    @queue.task
    async def blocker() -> int:
        started.set()
        await forever.wait()
        return 1

    async with queue.worker():
        handle = await blocker.submit()
        await asyncio.wait_for(started.wait(), 2)  # ensure it is RUNNING
        await queue.backend.request_cancel(handle.job_id)
        await _wait_status(queue, handle.job_id, JobStatus.CANCELLED)


async def test_cancel_wakes_result_waiter(queue: Queue) -> None:
    # result() must not hang once the job is cancelled — the CANCELLED write is
    # terminal and fires the done-event.
    started = asyncio.Event()
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        started.set()
        await forever.wait()
        return 1

    async with queue.worker():
        handle = await blocker.submit()
        await asyncio.wait_for(started.wait(), 2)
        await queue.backend.request_cancel(handle.job_id)
        with pytest.raises(JobCancelled):  # guards #5
            await asyncio.wait_for(handle.result(), 3)


async def test_worker_survives_after_a_cancel(queue: Queue) -> None:
    started = asyncio.Event()
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        started.set()
        await forever.wait()
        return 1

    @queue.task
    async def ok() -> int:
        return 7

    async with queue.worker():
        h_block = await blocker.submit()
        await asyncio.wait_for(started.wait(), 2)
        await queue.backend.request_cancel(h_block.job_id)
        await _wait_status(queue, h_block.job_id, JobStatus.CANCELLED)
        # pool keeps serving
        assert await asyncio.wait_for((await ok.submit()).result(), 3) == 7


async def test_cancelled_job_does_not_finish_its_body(queue: Queue) -> None:
    # The body must not run past its first suspension point once cancelled.
    started = asyncio.Event()
    gate = asyncio.Event()  # never set
    completed = {"flag": False}

    @queue.task
    async def two_phase() -> int:
        started.set()
        await gate.wait()
        completed["flag"] = True  # must never run
        return 1

    async with queue.worker():
        handle = await two_phase.submit()
        await asyncio.wait_for(started.wait(), 2)
        await queue.backend.request_cancel(handle.job_id)
        await _wait_status(queue, handle.job_id, JobStatus.CANCELLED)
    assert completed["flag"] is False


# --------------------------------------------------------------------------- #
# completion wins                                                              #
# --------------------------------------------------------------------------- #
async def test_completed_job_is_not_overwritten_by_late_cancel(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    async with queue.worker():
        handle = await add.submit(2, 3)
        assert await asyncio.wait_for(handle.result(), 3) == 5
        # cancel arrives after the job already completed
        await queue.backend.request_cancel(handle.job_id)
        assert await handle.status() == JobStatus.COMPLETED
        assert await asyncio.wait_for(handle.result(), 1) == 5


# --------------------------------------------------------------------------- #
# JobHandle behaviour on a cancelled job                                       #
# --------------------------------------------------------------------------- #
async def test_status_reports_cancelled(queue: Queue) -> None:
    started = asyncio.Event()
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        started.set()
        await forever.wait()
        return 1

    async with queue.worker():
        handle = await blocker.submit()
        await asyncio.wait_for(started.wait(), 2)
        await queue.backend.request_cancel(handle.job_id)
        await _wait_status(queue, handle.job_id, JobStatus.CANCELLED)
        assert await handle.status() == JobStatus.CANCELLED


async def test_result_on_cancelled_job_raises(queue: Queue) -> None:
    # guards #5: result() signals cancellation (raises), never silently None.
    started = asyncio.Event()
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        started.set()
        await forever.wait()
        return 1

    async with queue.worker():
        handle = await blocker.submit()
        await asyncio.wait_for(started.wait(), 2)
        await queue.backend.request_cancel(handle.job_id)
        await _wait_status(queue, handle.job_id, JobStatus.CANCELLED)
        with pytest.raises(JobCancelled):
            await asyncio.wait_for(handle.result(), 2)


# --------------------------------------------------------------------------- #
# guards #1 — terminal write must not strand a job                            #
# --------------------------------------------------------------------------- #
async def test_unserializable_result_fails_job_instead_of_hanging(
    queue: Queue,
) -> None:
    # The default JSONSerializer cannot encode a set, so to_record() raises during
    # the terminal write. Because _save runs INSIDE _process's try/except, that
    # error is caught and the job ends FAILED, so result() raises promptly instead
    # of hanging forever.
    @queue.task
    async def bad() -> set[int]:
        return {1, 2, 3}

    @queue.task
    async def ok() -> int:
        return 1

    async with queue.worker():
        handle = await bad.submit()
        with pytest.raises(RuntimeError):  # not a TimeoutError from a hang
            await asyncio.wait_for(handle.result(), 3)
        # and the worker must still be alive afterwards
        assert await asyncio.wait_for((await ok.submit()).result(), 3) == 1


# --------------------------------------------------------------------------- #
# guards #2 — the cancel-waiter task must not leak on the failure path        #
# --------------------------------------------------------------------------- #
def _pending_wait_cancel_tasks() -> list[asyncio.Task[object]]:
    out: list[asyncio.Task[object]] = []
    for t in asyncio.all_tasks():
        if t.done():
            continue
        coro = t.get_coro()
        if getattr(coro, "__qualname__", "").endswith("wait_cancel"):
            out.append(t)
    return out


async def test_failing_job_does_not_leak_cancel_waiter(queue: Queue) -> None:
    # On the failure path, _process's finally cancels the cancel-waiter task, so
    # it is not leaked. With one worker and one job, any pending wait_cancel task
    # left behind would be that leak.
    @queue.task
    async def boom() -> int:
        raise ValueError("nope")

    async with queue.worker(concurrency=1):
        handle = await boom.submit()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(handle.result(), 3)
        await asyncio.sleep(0.05)  # let any cleanup run
        assert _pending_wait_cancel_tasks() == []
