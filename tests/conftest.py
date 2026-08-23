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
  @q.task     : bare AND @q.task(name=..., max_retries=...); bare max_retries == 0
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

import fakeredis
import pytest

from TaskQueue import Backend, JSONSerializer, MemoryBackend, Queue, Serializer
from TaskQueue.backends.redis_backend import RedisBackend


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
