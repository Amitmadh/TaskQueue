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
import contextlib
from typing import TYPE_CHECKING

import pytest

from TaskQueue import job_group
from TaskQueue.exceptions import JobCancelled
from TaskQueue.job import JobStatus
from TaskQueue.queue import Queue

if TYPE_CHECKING:
    from TaskQueue.handle import JobHandle

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


async def test_spawned_job_carries_the_group_id(queue: Queue) -> None:
    # The scope id on the job record is what lets a process other than the
    # owner find a dead scope's children.
    @queue.task
    async def noop() -> None:
        return None

    async with queue.group() as g:
        handle = await g.spawn(noop)
        record = await queue.backend.get_job(handle.job_id)
        assert record["group_id"] == g.id
        async with queue.worker():
            await asyncio.wait_for(handle.result(), 3)


async def test_a_detached_root_group_stamps_no_group_id(queue: Queue) -> None:
    """A scope that was never entered supervises nothing.

    'q.root_group().spawn(...)' without 'async with' is the one sanctioned
    detachment, so its jobs must carry no group id -- that absence is what
    keeps a reaper from cancelling them, and it is what lets a producer
    enqueue a batch and exit without its own work being torn down behind it.
    """

    @queue.task
    async def noop() -> None:
        return None

    handle = await queue.root_group().spawn(noop)  # deliberately not entered
    record = await queue.backend.get_job(handle.job_id)
    assert "group_id" not in record


async def test_owner_cancelled_while_joining_cancels_children(queue: Queue) -> None:
    """Cancelling the task that owns a scope must not orphan its children.

    __aexit__ spends nearly all of a scope's life parked in the join, so that
    -- not the body -- is where a Ctrl-C on the owning task lands. Without the
    CancelledError arm the children keep running with nobody waiting, and no
    reaper can save them: the process is alive and still heartbeating.
    """
    forever = asyncio.Event()

    @queue.task
    async def blocker() -> int:
        await forever.wait()
        return 1

    handles: list[JobHandle[int]] = []

    async def owner() -> None:
        async with queue.group() as g:
            for _ in range(3):
                handles.append(await g.spawn(blocker))

    async def all_running() -> bool:
        if len(handles) < 3:
            return False
        for handle in handles:
            if await handle.status() is not JobStatus.RUNNING:
                return False
        return True

    async with queue.worker(concurrency=3):
        task = asyncio.create_task(owner())
        while not await all_running():
            await asyncio.sleep(0.01)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        for handle in handles:
            assert await handle.status() is JobStatus.CANCELLED


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


async def test_a_task_can_open_its_own_scope(queue: Queue) -> None:
    """A job that is itself a producer.

    Nothing in the worker distinguishes a task that spawns from one that does
    not: 'async with queue.group()' is the same construct inside a job as inside
    a script, and that nesting is what "jobs have parent-child relationships"
    means. Untested until now, despite being the headline of the README.

    Note the pool width. A parent occupies a worker slot for as long as it waits
    on its children, so the pool has to hold the parent AND at least one child —
    at 'concurrency=1' this deadlocks outright, with the parent holding the only
    slot and no child able to be claimed.
    """

    @queue.task
    async def child(n: int) -> int:
        return n * 10

    @queue.task
    async def parent() -> int:
        async with queue.group() as inner:
            counts = [await inner.spawn(child, n) for n in range(3)]
        return sum([await count.result() for count in counts])

    async with queue.worker(concurrency=4):
        handle = await queue.root_group().spawn(parent)
        assert await asyncio.wait_for(handle.result(), 10) == 30


async def test_cancelling_a_parent_job_cancels_the_children_it_spawned(
    queue: Queue,
) -> None:
    """The guarantee crosses the job boundary, not just the process boundary.

    Cancelling the parent interrupts its task, which lands as a 'CancelledError'
    inside its scope's join — the one arm of '__aexit__' that exists for this —
    and that arm is what tears the children down. Without it the parent reports
    cancelled while the work it started runs on, which is precisely the orphan
    this library claims not to produce.
    """
    running: list[int] = []
    stopped: list[int] = []
    forever = asyncio.Event()  # never set: children only end via cancellation

    @queue.task
    async def child(n: int) -> int:
        running.append(n)
        try:
            await forever.wait()
        except asyncio.CancelledError:
            stopped.append(n)  # printed from the child's own unwinding
            raise
        return n

    @queue.task
    async def parent() -> int:
        async with queue.group() as inner:
            handles = [await inner.spawn(child, n) for n in range(3)]
        return sum([await handle.result() for handle in handles])

    async with queue.worker(concurrency=4):
        handle = await queue.root_group().spawn(parent)

        # Wait for the children to be RUNNING, not merely spawned: a cancel that
        # lands while they are still queued proves nothing about reaching into a
        # job that is already executing.
        for _ in range(150):
            if len(running) == 3:
                break
            await asyncio.sleep(0.02)
        assert sorted(running) == [0, 1, 2], f"children never started: {running}"

        await handle.cancel()
        with pytest.raises(JobCancelled):
            await asyncio.wait_for(handle.result(), 3)

        # Waited for, not assumed. The parent's record goes terminal as soon as
        # its own task is cancelled, which is BEFORE the children it spawned have
        # finished unwinding — on Redis the extra round trips hide that gap, on
        # MemoryBackend they do not, and an assertion here read straight after
        # 'result()' sees an empty list on one backend and a full one on the
        # other.
        for _ in range(150):
            if len(stopped) == 3:
                break
            await asyncio.sleep(0.02)

        # Asserted INSIDE the pool. Leaving the 'async with' cancels the worker
        # loops, which cancels the children's tasks and fills this list for a
        # reason that has nothing to do with the scope — the test would then
        # pass with the guard it exists for removed.
        assert sorted(stopped) == [0, 1, 2], (
            f"cancelling the parent did not reach its children: {stopped}"
        )


async def test_a_fail_fast_scope_reports_only_what_broke_it(queue: Queue) -> None:
    """'except* RuntimeError' must fully handle a fail-fast scope.

    The siblings a scope cancels raise 'JobCancelled' in their waiters. Recorded
    as failures they land in the same 'BaseExceptionGroup' as the error that
    actually broke the scope — where an 'except*' on the real exception type
    cannot match them, so they escape as an unhandled residual group and crash
    the very 'try' that was written to catch the failure. 'asyncio.TaskGroup'
    propagates what broke it, not the cancellations it issued in response, and a
    scope that claims to work the same way has to do the same.

    Written as a user would write it rather than by inspecting the group: if
    anything other than the failure is in there, the 'except*' below does not
    match it and this test errors out on the residual instead of failing an
    assertion. That is the README's own example, and it crashed.
    """
    forever = asyncio.Event()  # never set: siblings only end via cancellation

    @queue.task
    async def boom() -> int:
        raise RuntimeError("job hit a wall")

    @queue.task
    async def sibling() -> int:
        await forever.wait()
        return 1

    caught: list[BaseException] = []
    async with queue.worker(concurrency=4):
        try:
            async with queue.group() as g:
                for _ in range(3):
                    await g.spawn(sibling)
                await g.spawn(boom)
        except* RuntimeError as failures:
            caught.extend(failures.exceptions)

    assert len(caught) == 1, caught
    assert "job hit a wall" in str(caught[0])


async def test_a_cancellation_the_scope_did_not_cause_is_still_a_failure(
    queue: Queue,
) -> None:
    """The gate is 'did we ask for this', not 'is it a JobCancelled'.

    Suppressing every 'JobCancelled' would hide a real outcome: a job somebody
    else cancelled is not collateral, and a scope that swallowed it would exit
    cleanly having never run the work.
    """
    forever = asyncio.Event()

    @queue.task
    async def sibling() -> int:
        await forever.wait()
        return 1

    async with queue.worker(concurrency=2):
        with pytest.raises(BaseExceptionGroup) as raised:
            async with queue.group() as g:
                handle = await g.spawn(sibling)
                await handle.cancel()  # nothing has failed; this is not teardown

    assert [type(exc) for exc in raised.value.exceptions] == [JobCancelled], (
        raised.value.exceptions
    )


async def test_a_failure_cancels_siblings_before_the_body_finishes(
    queue: Queue,
) -> None:
    """Fail-fast must not wait for the body to reach the join.

    'asyncio.TaskGroup' cancels the remaining children the moment one raises,
    even while the body is still running. A scope that only starts watching in
    '__aexit__' lets its siblings burn worker time for as long as the body
    takes -- and not paying for work that is already doomed is the whole point
    of fail-fast.
    """
    cancelled_in_body = asyncio.Event()
    forever = asyncio.Event()  # never set: the sibling only ends via cancellation

    @queue.task
    async def boom() -> int:
        raise ValueError("trigger")

    @queue.task
    async def sibling() -> int:
        try:
            await forever.wait()
        except asyncio.CancelledError:
            cancelled_in_body.set()
            raise
        return 1

    stopped_while_body_ran = False
    async with queue.worker(concurrency=4):
        with pytest.raises(BaseExceptionGroup):
            async with queue.group() as g:
                await g.spawn(sibling)
                await g.spawn(boom)

                # Still inside the body. Swallow the timeout rather than
                # raising here: a regression would otherwise surface as a body
                # exception, which the 'raises' guard above would absorb and
                # the test would pass for the wrong reason.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(cancelled_in_body.wait(), 2)
                stopped_while_body_ran = cancelled_in_body.is_set()

    assert stopped_while_body_ran, (
        "the sibling was still running when the scope body finished: "
        "fail-fast only fires once __aexit__ starts joining"
    )


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


async def test_teardown_does_not_hang_on_a_wake_that_never_arrives(
    queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dropped notification must not turn a deadline into a permanent hang.

    Teardown waits for every cancelled child to reach a terminal state -- that
    wait is the scope's promise. But it rides on a wake-up, and pub/sub can
    drop one, in which case the child is already finished and only the news is
    missing. An unbounded wait would move the hang from the join into the
    teardown and never come back.
    """
    monkeypatch.setattr(job_group, "_CANCEL_DRAIN_TIMEOUT_SECONDS", 0.2)

    @queue.task
    async def slow() -> int:
        await asyncio.sleep(30)
        return 1

    async def never_wakes(job_id: str) -> dict[str, object]:
        await asyncio.Event().wait()  # the notification that never comes
        raise AssertionError("unreachable")

    monkeypatch.setattr(queue.backend, "take_result", never_wakes)

    async with queue.worker():
        with pytest.raises(TimeoutError):
            async with queue.group(deadline=0.2) as g:
                await g.spawn(slow)
