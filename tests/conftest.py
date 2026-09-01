"""Shared fixtures + the contract these tests encode.

Every test taking `backend` (or `queue`, which is built from it) runs twice:
once against MemoryBackend and once against RedisBackend on fakeredis. The
contract below is what both must satisfy, so a divergence between the two is a
test failure rather than something discovered in production.

Target contract (current API):
  exports     : Queue, Task, Job, JobStatus, JobHandle, JobCancelled, Backend,
                MemoryBackend, Worker, Serializer, JSONSerializer, PickleSerializer
  JobStatus   : str-enum {CREATED, QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED}
  Job         : keyword-constructable; to_record/from_record(serializer) round-trip;
                identity equality (by id)
  Serializer  : dumps/loads protocol; JSONSerializer default (Pickle too)
  Backend     : {enqueue(job_id, record), claim() -> record | None (None once the
                claim interval lapses), get_job(job_id) -> record,
                save(job_id, record, *, done), release(job_id),
                take_result(job_id) -> record, request_cancel(job_id),
                wait_cancel(job_id)}
  @q.task     : bare and @q.task(name=..., max_retries=...); bare max_retries == 0
  JobHandle   : (job_id, backend, serializer); .job_id, await .result() (raises
                RuntimeError on failure, JobCancelled on cancel), await .status(),
                await .cancel()
  Queue       : .group(...) / .root_group(...) open structured-concurrency scopes
  Worker      : async context manager; survives unknown task names and task errors;
                cancels a RUNNING job on request_cancel; redelivers an in-flight
                lease on shutdown (at-least-once); drain(timeout=None) stops
                claiming and awaits in-flight jobs, cancelling past the deadline
  JobGroup    : async-with scope; await g.spawn(task, *args) -> JobHandle;
                on_error in {cancel_siblings (default), collect, ignore}; deadline=

Records are opaque dicts: control fields (id/task_name/status/error/attempts/
created_at/request_cancel) are plain, while args/kwargs (under "payload") and
"result" are serializer blobs. Tests that drive a backend directly build records
via Job.to_record(serializer) and read them back via Job.from_record(record,
serializer).
"""

from collections.abc import Awaitable, Callable

import fakeredis
import pytest

from TaskQueue import Backend, JSONSerializer, MemoryBackend, Queue, Serializer
from TaskQueue.backends.redis_backend import (
    JOBS,
    PROCESSING,
    QUEUE,
    WORKERS,
    RedisBackend,
)


@pytest.fixture
def serializer() -> Serializer:
    return JSONSerializer()


@pytest.fixture(params=["memory", "redis"])
def backend(request: pytest.FixtureRequest) -> Backend:
    if request.param == "redis":
        return RedisBackend(fakeredis.FakeAsyncRedis())
    return MemoryBackend()


@pytest.fixture
def queue(backend: Backend) -> Queue:
    return Queue(backend=backend)


def _text(key: bytes | str) -> str:
    return key.decode() if isinstance(key, bytes) else key


@pytest.fixture
def assert_backend_clean() -> Callable[..., Awaitable[None]]:
    """Assert a backend was left with nothing stranded behind it.

    Results prove a job came back, not that the bookkeeping which made it
    recoverable was cleaned up - every bug in the lease/watch family had that
    shape: correct answers, dirty store.

    Call it once the pool has exited and every handle has resolved, and loosen
    a flag where a test means to leave that state behind.
    """

    async def _assert(
        backend: Backend,
        *,
        results_collected: bool = True,
        workers_deregistered: bool = True,
    ) -> None:
        left: list[str] = []

        if isinstance(backend, RedisBackend):
            client = backend.redis
            if queued := await client.llen(QUEUE):
                left.append(f"{queued} job(s) still on the queue")
            for key in await client.keys(f"{PROCESSING}:*"):
                if held := await client.llen(key):
                    left.append(f"{_text(key)} still holds {held} lease(s)")
            if workers_deregistered and (beating := await client.zcard(WORKERS)):
                left.append(f"{beating} worker(s) still in the liveness set")
            if results_collected and (records := await client.keys(f"{JOBS}:*")):
                left.append(f"{len(records)} job record(s) left behind")
        else:
            # MemoryBackend keeps its whole state in plain attributes and has no
            # lease to strand, so the queue and the records are the surface.
            memory = backend
            assert isinstance(memory, MemoryBackend)
            if queued := memory._queue.qsize():
                left.append(f"{queued} job(s) still on the queue")
            if results_collected and memory._jobs:
                left.append(f"{len(memory._jobs)} job record(s) left behind")

        assert not left, "backend left dirty: " + "; ".join(left)

    return _assert
