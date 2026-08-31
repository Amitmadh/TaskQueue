"""A task body that runs twice must not fork its subtree.

At-least-once delivery means any task body can run twice — a reaper redelivers
after a worker dies, and Phase 5's retries will re-run bodies on purpose. A leaf
task only has to be idempotent in its own effects, but **a spawning task has an
enqueue as a side effect**, and an enqueue that is not deduped creates a second
set of children while the first set is still running, owned by nobody.

Two halves close it, and both are tested here:

* `JobGroup._child_id` seeds a child's id on the job doing the spawning, so a
  re-run of one body recomputes the same ids;
* `Backend.enqueue` refuses an id it already knows, so the second attempt is a
  no-op instead of a duplicate.

Measured before the fix: a redelivered parent turned 2 children into 4.
"""

import asyncio
from hashlib import sha1

import pytest

from TaskQueue.context import JobContext, current_job
from TaskQueue.handle import JobHandle
from TaskQueue.job import Job, JobStatus
from TaskQueue.queue import Queue
from TaskQueue.serializers import Serializer

pytestmark = pytest.mark.timeout(10)


def _running(job_id: str) -> JobContext:
    """A fresh context for the same job — one body, run once."""
    return JobContext(job_id, None, "parent", attempts=1)


# --- the derivation -----------------------------------------------------------
async def test_a_re_run_body_spawns_the_same_child_ids(queue: Queue) -> None:
    """The whole point: run the same body twice, get the same children.

    Two scopes, deliberately. The seed cannot contain the scope's id — 'group()'
    mints a fresh uuid4 on every construction, so a scope inside a re-running
    body is not the scope it was — and once the ordinal is per-BODY rather than
    per-scope, the second scope must not restart it at zero either. Both halves
    of that are asserted below: four distinct ids, and the same four again.
    """

    @queue.task
    async def noop() -> int:
        return 1

    async def body() -> list[str]:
        spawned: list[str] = []
        for _ in range(2):  # two separate scopes in one body
            scope = queue.root_group()
            spawned += [(await scope.spawn(noop)).job_id for _ in range(2)]
        return spawned

    token = current_job.set(_running("parent-job"))
    try:
        first = await body()
    finally:
        current_job.reset(token)

    token = current_job.set(_running("parent-job"))  # the same job, running again
    try:
        second = await body()
    finally:
        current_job.reset(token)

    assert len(set(first)) == 4, f"ids collided within one body: {first}"
    assert first == second, "a re-run of the same body produced different children"


async def test_two_different_jobs_do_not_share_child_ids(queue: Queue) -> None:
    """The other direction: the seed has to separate parents, not just runs."""

    @queue.task
    async def noop() -> int:
        return 1

    async def spawn_one(job_id: str) -> str:
        token = current_job.set(_running(job_id))
        try:
            return (await queue.root_group().spawn(noop)).job_id
        finally:
            current_job.reset(token)

    assert await spawn_one("job-a") != await spawn_one("job-b")


async def test_a_spawn_outside_a_job_gets_a_generated_id(queue: Queue) -> None:
    """A script has no parent to derive from, so 'Job' falls back to uuid4.

    Without this branch two identical spawns from a producer would collide, and
    the idempotent enqueue below would silently drop the second one.
    """

    @queue.task
    async def noop() -> int:
        return 1

    assert current_job.get() is None
    scope = queue.root_group()
    first = (await scope.spawn(noop)).job_id
    second = (await scope.spawn(noop)).job_id
    assert first != second


# --- the enqueue guard --------------------------------------------------------
async def test_enqueue_ignores_an_id_it_already_knows(
    queue: Queue, serializer: Serializer
) -> None:
    """The second half. Without it, derived ids just overwrite each other.

    Asserted through 'claim', because "did not enqueue" has to mean *no second
    queue entry* — an implementation that skipped the record write but still
    pushed the id would leave a job that is claimed twice.
    """
    backend = queue.backend
    job = Job(task_name="t")
    await backend.enqueue(job.id, job.to_record(serializer))
    await backend.enqueue(job.id, job.to_record(serializer))

    first = await backend.claim()
    assert first is not None and first["id"] == job.id
    assert await backend.claim() is None, "the duplicate enqueue was queued too"


# --- end to end ---------------------------------------------------------------
async def test_a_re_run_body_does_not_run_its_children_twice(queue: Queue) -> None:
    """The consequence, end to end: the same children, executed once.

    The derivation test above proves the ids match; this proves that matching
    ids mean the work is not done twice — which is the whole point, and which
    fails the moment either half of the slice is removed.

    The body is run twice directly rather than by redelivering a live job. A
    'release' on a parent that is still executing leaves TWO bodies of one job
    alive at once, and they then contend for the same children's results —
    'take_result' is single-consumer — which is a real open question about this
    design but not the thing under test here. What a reaper actually produces is
    a body whose predecessor is *gone*, and that is what this models.
    """
    runs: list[int] = []

    @queue.task
    async def child(n: int) -> int:
        runs.append(n)
        return n

    async def body() -> list[JobHandle[int]]:
        scope = queue.root_group()
        return [await scope.spawn(child, n) for n in range(2)]

    handles: list[JobHandle[int]] = []
    for _ in range(2):  # the same job's body, run twice
        token = current_job.set(_running("parent-job"))
        try:
            handles = await body()
        finally:
            current_job.reset(token)

    async with queue.worker(concurrency=2):
        # Settle and assert INSIDE the pool, and BEFORE reading any result.
        # Leaving the pool cancels whatever is still queued, and 'take_result'
        # frees the record a duplicate claim would need — either one turns a
        # second execution into a no-op and lets this pass with the guard it
        # exists for removed.
        await asyncio.sleep(0.3)
        assert sorted(runs) == [0, 1], f"the children ran more than once: {runs}"
        for handle in handles:
            assert await asyncio.wait_for(handle.result(), 5) in (0, 1)


async def test_two_jobs_running_at_once_each_see_their_own(queue: Queue) -> None:
    """Concurrency is why the context is a 'ContextVar' and not a module global.

    A pool runs 'concurrency' bodies in ONE event loop, so a global would hold
    whichever job started most recently: a body that spawns after an await would
    derive its children from a SIBLING's id, and the ids would still look
    perfectly deterministic while belonging to the wrong parent. Both parents
    here are deliberately parked at a barrier so neither can spawn until the
    other is also live.

    The expected id is spelled out rather than computed, so this pins the seed
    itself as well as the isolation.
    """
    both_live = asyncio.Barrier(2)
    spawned: dict[str, str] = {}

    @queue.task
    async def child(n: int) -> int:
        return n

    @queue.task
    async def parent(tag: str) -> str:
        me = current_job.get()
        assert me is not None, "no job context inside the task"
        await both_live.wait()  # the interleaving a global cannot survive
        spawned[tag] = (await queue.root_group().spawn(child, 1)).job_id
        return me.job_id

    async with queue.worker(concurrency=4):
        async with queue.group() as g:
            handles = {tag: await g.spawn(parent, tag) for tag in ("a", "b")}
        parents = {tag: await handle.result() for tag, handle in handles.items()}

    assert parents["a"] != parents["b"]
    for tag, parent_id in parents.items():
        expected = sha1(f"{parent_id}:0".encode()).hexdigest()[:32]
        assert spawned[tag] == expected, f"{tag}'s child derived from another job"


async def test_a_task_can_see_the_job_it_is_running_as(queue: Queue) -> None:
    """The context is set, and it is set before the job body starts.

    'Worker._process' has to set it before 'create_task', because a task copies
    the context at creation — set it afterwards and the body sees nothing.
    """
    seen: list[JobContext | None] = []

    @queue.task
    async def look() -> str:
        seen.append(current_job.get())
        return "done"

    async with queue.worker():
        handle = await queue.root_group().spawn(look)
        assert await asyncio.wait_for(handle.result(), 5) == "done"

    assert seen and seen[0] is not None, "no job context inside the task"
    assert seen[0].job_id == handle.job_id
    assert seen[0].task_name.endswith("look")
    assert seen[0].attempts == 1
    assert await handle.status() is JobStatus.COMPLETED
