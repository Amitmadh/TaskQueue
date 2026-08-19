"""Phase 2 JobGroup — the executable spec for structured-concurrency scopes.

This is the headline of Phase 2. These tests are the spec for the scope
behaviour and pass against the current implementation ("tests are the spec").

API (as implemented):
  group = queue.group(on_error=..., deadline=...)
      on_error : "cancel_siblings" (default, fail-fast) | "collect" | "ignore"
      deadline : seconds; the whole scope is cancelled when it elapses
  async with group as g:
      handle = await g.spawn(task, *args, **kwargs)   # -> JobHandle
  __aexit__ does NOT return until every spawned child is terminal (the join).
  Failures surface as a builtin ExceptionGroup raised out of the `async with`.
  A cancelled child ends in JobStatus.CANCELLED (see test_worker_cancellation).

Invariant under test throughout: no orphans. When the scope exits — cleanly,
by failure, by body exception, or by deadline — every child it spawned must be
in a terminal state, never left RUNNING.
"""

import asyncio

import pytest

from TaskQueue.job import JobStatus
from TaskQueue.queue import Queue

pytestmark = pytest.mark.timeout(6)


# --------------------------------------------------------------------------- #
# basics that should already hold                                             #
# --------------------------------------------------------------------------- #
async def test_empty_group_exits_cleanly(queue: Queue) -> None:
    async with queue.group():
        pass  # spawning nothing must be a clean no-op


async def test_spawn_returns_a_working_handle(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    async with queue.worker():
        async with queue.group() as g:
            handle = await g.spawn(add, 2, 3)
        assert await asyncio.wait_for(handle.result(), 3) == 5


# --------------------------------------------------------------------------- #
# the join: __aexit__ waits for every child                                    #
# --------------------------------------------------------------------------- #
async def test_join_waits_for_all_children(queue: Queue) -> None:
    @queue.task
    async def slow(n: int) -> int:
        await asyncio.sleep(0.05)
        return n

    async with queue.worker(concurrency=4):
        async with queue.group(on_error="collect") as g:
            handles = [await g.spawn(slow, i) for i in range(5)]
        # the moment the scope exits, every child must already be terminal
        for h in handles:
            assert await h.status() == JobStatus.COMPLETED


async def test_join_results_available_after_scope(queue: Queue) -> None:
    @queue.task
    async def square(n: int) -> int:
        await asyncio.sleep(0.02)
        return n * n

    async with queue.worker(concurrency=4):
        async with queue.group(on_error="collect") as g:
            handles = [await g.spawn(square, i) for i in range(6)]
        results = [await h.result() for h in handles]
    assert results == [i * i for i in range(6)]


async def test_group_runs_children_in_parallel(queue: Queue) -> None:
    n = 4
    arrived = {"count": 0}
    everyone = asyncio.Event()

    @queue.task
    async def rendezvous() -> int:
        arrived["count"] += 1
        if arrived["count"] == n:
            everyone.set()
        await asyncio.wait_for(everyone.wait(), 3)
        return 1

    async with queue.worker(concurrency=n):
        async with queue.group(on_error="collect") as g:
            handles = [await g.spawn(rendezvous) for _ in range(n)]
        assert [await h.result() for h in handles] == [1] * n


# --------------------------------------------------------------------------- #
# on_error="collect": run everyone, then raise an ExceptionGroup               #
# --------------------------------------------------------------------------- #
async def test_collect_all_success_does_not_raise(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    async with queue.worker(concurrency=4):
        async with queue.group(on_error="collect") as g:
            handles = [await g.spawn(add, i, 1) for i in range(5)]
        assert [await h.result() for h in handles] == [i + 1 for i in range(5)]


async def test_collect_gathers_all_failures(queue: Queue) -> None:
    @queue.task
    async def boom(tag: int) -> int:
        raise ValueError(f"fail-{tag}")

    @queue.task
    async def ok() -> int:
        return 1

    async with queue.worker(concurrency=4):
        h_ok = None
        with pytest.raises(BaseExceptionGroup) as ei:
            async with queue.group(on_error="collect") as g:
                h_ok = await g.spawn(ok)
                await g.spawn(boom, 1)
                await g.spawn(boom, 2)
        assert len(ei.value.exceptions) == 2  # both failures collected
        assert h_ok is not None
        assert await h_ok.status() == JobStatus.COMPLETED  # success still ran


# --------------------------------------------------------------------------- #
# on_error="ignore": swallow failures                                          #
# --------------------------------------------------------------------------- #
async def test_ignore_swallows_failures(queue: Queue) -> None:
    @queue.task
    async def boom() -> int:
        raise ValueError("nope")

    async with queue.worker():
        async with queue.group(on_error="ignore") as g:
            handle = await g.spawn(boom)
        # no raise; the job still ran and recorded its failure
        assert await handle.status() == JobStatus.FAILED


# --------------------------------------------------------------------------- #
# on_error="cancel_siblings": the money shot                                   #
# --------------------------------------------------------------------------- #
async def test_cancel_siblings_cancels_inflight_on_first_failure(
    queue: Queue,
) -> None:
    forever = asyncio.Event()  # never set: siblings only end via cancellation

    @queue.task
    async def boom() -> int:
        raise ValueError("trigger")

    @queue.task
    async def sibling() -> int:
        await forever.wait()
        return 1

    h1 = h2 = None
    async with queue.worker(concurrency=5):
        with pytest.raises(BaseExceptionGroup):
            async with queue.group(on_error="cancel_siblings") as g:
                h1 = await g.spawn(sibling)
                h2 = await g.spawn(sibling)
                await g.spawn(boom)
        assert h1 is not None and h2 is not None
        assert await h1.status() == JobStatus.CANCELLED
        assert await h2.status() == JobStatus.CANCELLED


async def test_cancel_siblings_is_the_default_mode(queue: Queue) -> None:
    forever = asyncio.Event()

    @queue.task
    async def boom() -> int:
        raise ValueError("trigger")

    @queue.task
    async def sibling() -> int:
        await forever.wait()
        return 1

    h_sib = None
    async with queue.worker(concurrency=3):
        with pytest.raises(BaseExceptionGroup):
            async with queue.group() as g:  # no on_error -> cancel_siblings
                h_sib = await g.spawn(sibling)
                await g.spawn(boom)
        assert h_sib is not None
        assert await h_sib.status() == JobStatus.CANCELLED


async def test_cancel_siblings_keeps_already_completed_result(queue: Queue) -> None:
    # Completion wins: a sibling that finished before the failure keeps its result.
    forever = asyncio.Event()

    @queue.task
    async def quick() -> int:
        return 42

    @queue.task
    async def boom() -> int:
        raise ValueError("trigger")

    @queue.task
    async def blocker() -> int:
        await forever.wait()
        return 1

    h_quick = h_block = None
    async with queue.worker(concurrency=4):
        with pytest.raises(BaseExceptionGroup):
            async with queue.group(on_error="cancel_siblings") as g:
                h_quick = await g.spawn(quick)
                await h_quick.result()  # ensure it has completed
                await g.spawn(boom)
                h_block = await g.spawn(blocker)
        assert h_quick is not None and h_block is not None
        assert await h_quick.status() == JobStatus.COMPLETED
        assert await h_quick.result() == 42
        assert await h_block.status() == JobStatus.CANCELLED


# --------------------------------------------------------------------------- #
# the body itself raising must still join/cancel children (no orphan)          #
# --------------------------------------------------------------------------- #
async def test_body_exception_cancels_children_and_propagates(queue: Queue) -> None:
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        await forever.wait()
        return 1

    h = None
    async with queue.worker(concurrency=2):
        with pytest.raises(BaseException):  # noqa: B017 - ValueError or ExceptionGroup
            async with queue.group() as g:
                h = await g.spawn(blocker)
                raise ValueError("body blew up")
        assert h is not None
        assert await h.status() == JobStatus.CANCELLED  # not left RUNNING


# --------------------------------------------------------------------------- #
# nested scopes: an inner failure propagates to the outer scope                 #
# --------------------------------------------------------------------------- #
async def test_nested_inner_failure_cancels_outer_siblings(queue: Queue) -> None:
    forever = asyncio.Event()

    @queue.task
    async def boom() -> int:
        raise ValueError("inner failure")

    @queue.task
    async def outer_child() -> int:
        await forever.wait()
        return 1

    h_outer = None
    async with queue.worker(concurrency=5):
        with pytest.raises(BaseException):  # noqa: B017
            async with queue.group() as outer:
                h_outer = await outer.spawn(outer_child)
                async with queue.group() as inner:
                    await inner.spawn(boom)
        assert h_outer is not None
        assert await h_outer.status() == JobStatus.CANCELLED


# --------------------------------------------------------------------------- #
# deadline: the whole scope is cancelled when it elapses                        #
# --------------------------------------------------------------------------- #
async def test_deadline_cancels_the_scope(queue: Queue) -> None:
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        await forever.wait()
        return 1

    h = None
    async with queue.worker():
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            async with queue.group(deadline=0.2) as g:
                h = await g.spawn(blocker)
        assert h is not None
        assert await h.status() == JobStatus.CANCELLED
