"""Lease and watch bookkeeping — the orderings that make a job recoverable.

A worker holds two kinds of claim on a job: a *lease* (the id sitting on its
`processing:{id}` list) and a *watch* (`wait_cancel`, telling it to stop). Both
depend on bookkeeping that has to be in place before the thing it protects, and
has to survive that thing going wrong. `reap` is the reason: it walks expired
MEMBERS of the workers set and nothing else, so a lease its owner never
registered for, or one held by a process that is alive and beating, is invisible
to every reaper there will ever be — the job is not queued, not owned, and never
redelivered.

They live together rather than in `test_worker.py` because they are one family
and each was written after a real failure of it, not from the docstring:

* the pool registered itself with `create_task`, racing the first ZADD against
  the BLMOVE — measured claiming 4–17ms too early under load;
* `asyncio.wait` cannot tell a watcher that RETURNED from one that RAISED, so a
  dropped pubsub connection was read as a cancellation;
* the shutdown `release` raised for a record whose result had been taken, and
  that exception replaced the CancelledError, letting the loop escape its own
  cancellation;
* an error between the BLMOVE and the claim script left the lease held by a
  worker that then stayed alive and beating.
"""

import asyncio
from typing import Any

import fakeredis
import pytest

from TaskQueue.backends.memory_backend import MemoryBackend
from TaskQueue.backends.redis_backend import (
    DEFAULT_WORKER_TTL_SECONDS,
    QUEUE,
    RedisBackend,
)
from TaskQueue.exceptions import ConfigError
from TaskQueue.job import JobStatus
from TaskQueue.queue import Queue
from TaskQueue.worker import _MIN_BEATS_PER_TTL, DEFAULT_HEARTBEAT_INTERVAL_SECONDS

pytestmark = pytest.mark.timeout(15)


# --- the lease is registered before it can be taken ----------------------------
async def test_a_worker_registers_before_it_can_claim(queue: Queue) -> None:
    """No claim without a heartbeat first.

    Asserted as call ORDER rather than as a zscore: a zscore has to be awaited,
    and that await is itself the yield that lets the upkeep loop beat, so the
    check would pass on timing rather than on the guarantee.
    """
    calls: list[str] = []
    backend = queue.backend
    beat, claim = backend.heartbeat, backend.claim

    async def traced_beat() -> None:
        calls.append("heartbeat")
        await beat()

    async def traced_claim() -> dict[str, Any] | None:
        calls.append("claim")
        return await claim()

    backend.heartbeat = traced_beat  # type: ignore[method-assign]
    backend.claim = traced_claim  # type: ignore[method-assign]

    async with queue.worker(concurrency=2):
        await asyncio.sleep(0.05)

    assert calls, "the worker made no backend calls at all"
    assert calls[0] == "heartbeat", calls


async def test_the_pool_keeps_beating_until_the_last_lease_is_handed_back(
    queue: Queue,
) -> None:
    """Deregistration is the mirror image of registration, and comes last.

    `__aexit__` cancels the claim loops, waits for them, and only THEN stops the
    upkeep loop. Stopping upkeep first would let this worker's beat lapse while
    it is still handing leases back, and a peer would reclaim jobs that are
    still running here.
    """
    backend = queue.backend
    holder: dict[str, Any] = {}
    upkeep_alive: list[bool] = []
    original = backend.release

    async def traced_release(job_id: str) -> None:
        worker = holder["worker"]
        upkeep_alive.append(worker._upkeep is not None and not worker._upkeep.done())
        await original(job_id)

    backend.release = traced_release  # type: ignore[method-assign]

    started = asyncio.Event()

    @queue.task
    async def slow() -> str:
        started.set()
        await asyncio.sleep(30)
        return "never"

    scope = queue.root_group()
    async with queue.worker() as worker:
        holder["worker"] = worker
        await scope.spawn(slow)
        await asyncio.wait_for(started.wait(), 3)

    assert upkeep_alive == [True], (
        "the in-flight lease was released after upkeep had already stopped"
    )


# --- a watch that BREAKS is not a watch that FIRED -----------------------------
async def test_a_broken_cancel_watcher_does_not_cancel_the_job(queue: Queue) -> None:
    """A watcher that raises must not be read as a cancellation.

    `asyncio.wait(FIRST_COMPLETED)` reports "done" for a task that raised just
    as it does for one that returned, so a dropped pubsub connection would write
    CANCELLED over work that is still running and throw the result away. Exactly
    the failure `RedisBackend._cancel_requested` is careful to avoid on the
    other side, reached through a different door.
    """
    backend = queue.backend

    async def exploding_wait_cancel(job_id: str) -> None:
        await asyncio.sleep(0.05)
        raise ConnectionError("pubsub connection dropped")

    backend.wait_cancel = exploding_wait_cancel  # type: ignore[method-assign]

    started = asyncio.Event()

    @queue.task
    async def slow() -> str:
        started.set()
        await asyncio.sleep(0.3)
        return "done"

    scope = queue.root_group()
    async with queue.worker():
        handle = await scope.spawn(slow)
        await asyncio.wait_for(started.wait(), 3)
        assert await asyncio.wait_for(handle.result(), 5) == "done"
        assert await handle.status() is JobStatus.COMPLETED


async def test_a_returning_cancel_watcher_still_cancels_the_job(
    queue: Queue,
) -> None:
    """The other half of the pair, so the test above cannot be satisfied by
    ignoring the watcher altogether. A watcher that RETURNS is a cancellation."""
    backend = queue.backend

    async def prompt_wait_cancel(job_id: str) -> None:
        await asyncio.sleep(0.05)

    backend.wait_cancel = prompt_wait_cancel  # type: ignore[method-assign]

    started = asyncio.Event()

    @queue.task
    async def slow() -> str:
        started.set()
        await asyncio.sleep(30)
        return "never"

    scope = queue.root_group()
    async with queue.worker():
        handle = await scope.spawn(slow)
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.sleep(0.3)
        assert await handle.status() is JobStatus.CANCELLED


# --- handing the lease back must not cost the cancellation ---------------------
async def test_a_failed_release_does_not_swallow_the_shutdown_cancel(
    queue: Queue,
) -> None:
    """A loop cancelled after its job's record is gone must still end CANCELLED.

    `release` raises `KeyError` for a record that no longer exists, and once
    `take_result` has consumed the result that is precisely the state. Raised
    from inside the `except CancelledError` arm it REPLACES the CancelledError,
    so the loop exits by exception: `task.cancelled()` is False and every
    `gather(..., return_exceptions=True)` that collects it swallows the error
    silently. The same applies when the backend is simply down — which is the
    usual reason a pool is being torn down in the first place.
    """
    backend = queue.backend
    original_save = backend.save

    async def lingering_save(
        job_id: str, record: dict[str, Any], *, done: bool = False
    ) -> None:
        await original_save(job_id, record, done=done)
        if done:
            # Hold the loop inside _process past its terminal write, so the
            # cancellation lands where release() is reachable.
            await asyncio.sleep(0.5)

    backend.save = lingering_save  # type: ignore[method-assign]

    @queue.task
    async def quick() -> int:
        return 1

    scope = queue.root_group()
    async with queue.worker() as worker:
        handle = await scope.spawn(quick)
        # Consumes the record: take_result deletes it on the way out.
        assert await asyncio.wait_for(handle.result(), 5) == 1
        loops = list(worker.workers)
        for loop in loops:
            loop.cancel()
        await asyncio.gather(*loops, return_exceptions=True)

    escaped = [repr(loop.exception()) for loop in loops if not loop.cancelled()]
    assert not escaped, f"a claim loop escaped its cancellation: {escaped}"


# --- a lease taken but never marked must not be kept ---------------------------
def _flaky_claim_script(backend: RedisBackend) -> None:
    """Make the NEXT claim-script round trip fail, once."""
    script = backend._claim_script
    boom = {"n": 1}

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        if boom["n"]:
            boom["n"] = 0
            raise ConnectionError("claim script round trip failed")
        return await script(*args, **kwargs)

    backend._claim_script = flaky


async def test_a_failed_claim_returns_the_lease_to_the_queue() -> None:
    """`claim` leases in two steps, and the second one can fail on its own.

    BLMOVE moves the id onto this worker's processing list; a SECOND round trip
    marks the record RUNNING. An error there used to leave the id on the list
    with the record still QUEUED — and since the worker survives the error and
    keeps beating, no reaper ever walks that list. Redis-only: MemoryBackend has
    no lease to lose.
    """
    backend = RedisBackend(fakeredis.FakeAsyncRedis())
    q = Queue(backend=backend, namespace="lease")

    @q.task
    async def quick() -> int:
        return 7

    handle = await q.root_group().spawn(quick)
    _flaky_claim_script(backend)

    with pytest.raises(ConnectionError):
        await backend.claim()

    assert await backend.redis.lrange(backend._processing_key, 0, -1) == [], (
        "the lease was kept by a worker that never marked the job RUNNING"
    )
    assert await backend.redis.lrange(QUEUE, 0, -1) == [handle.job_id.encode()]


async def test_a_worker_survives_a_failed_claim_and_still_runs_the_job() -> None:
    """The consequence, end to end: the job comes back rather than stranding."""
    backend = RedisBackend(fakeredis.FakeAsyncRedis())
    q = Queue(backend=backend, namespace="lease")

    @q.task
    async def quick() -> int:
        return 7

    handle = await q.root_group().spawn(quick)
    _flaky_claim_script(backend)

    async with q.worker():
        assert await asyncio.wait_for(handle.result(), 8) == 7


# --- a worker must not be configured to lose its own leases -------------------
def _redis_queue(worker_ttl: int) -> Queue:
    return Queue(RedisBackend(fakeredis.FakeAsyncRedis(), worker_ttl=worker_ttl))


@pytest.mark.parametrize("interval", [10.0, 5.0, 1.0])
def test_a_worker_accepts_a_heartbeat_with_margin(interval: float) -> None:
    _redis_queue(30).worker(heartbeat_interval=interval)


@pytest.mark.parametrize("interval", [10.1, 15.0, 60.0])
def test_a_worker_refuses_a_heartbeat_too_close_to_the_ttl(interval: float) -> None:
    """The one failure in this family that is reached by settings, not by luck.

    'reap' presumes a worker dead once its beat is older than 'worker_ttl'. A
    pool beating no more often than the TTL is configured to have its own
    running jobs handed to somebody else — the failure that got supervision cut,
    arrived at deliberately. Checked in 'Worker.__init__' rather than only in
    the CLI, so 'q.worker(...)' cannot be talked into it either.
    """
    with pytest.raises(ConfigError, match="worker_ttl"):
        _redis_queue(30).worker(heartbeat_interval=interval)


def test_a_backend_without_a_ttl_is_not_checked() -> None:
    """'worker_ttl' is a RedisBackend property, not part of the Protocol, and
    MemoryBackend has no reaper for the answer to matter to."""
    Queue(MemoryBackend()).worker(heartbeat_interval=3600.0)


def test_the_shipped_defaults_are_a_valid_pair() -> None:
    """Nothing else would catch a default that refuses its own worker.

    The two constants live in different modules and sit exactly on the boundary,
    so raising either one alone makes 'q.worker()' — the no-argument call in
    every quickstart — fail at construction.
    """
    assert (
        DEFAULT_WORKER_TTL_SECONDS
        >= DEFAULT_HEARTBEAT_INTERVAL_SECONDS * _MIN_BEATS_PER_TTL
    )
    _redis_queue(DEFAULT_WORKER_TTL_SECONDS).worker()
