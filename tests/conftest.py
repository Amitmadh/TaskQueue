"""Phase 1 test suite — the executable spec for the in-memory queue.

Target contract these tests encode (current API):
  exports     : Queue, Task, Job, JobStatus, JobHandle, Backend, MemoryBackend, Worker
  JobStatus   : str-enum {CREATED, QUEUED, RUNNING, COMPLETED, FAILED}
  Job         : keyword-constructable; to_record/from_record(serializer) round-trip;
                identity equality (by id)
  Serializer  : dumps/loads protocol; JSONSerializer default (Pickle too)
  Backend     : {enqueue(job_id, record), claim() -> record, get_job(job_id) -> record,
                save(job_id, record, *, done), wait_for(job_id)}
  @q.task     : bare AND @q.task(name=..., max_retries=...); bare max_retries == 0
  JobHandle   : (job_id, backend, serializer); .job_id, await .result() (raises on
                failure), await .status()
  Worker      : async context manager; survives unknown task names and task errors

Records are opaque dicts: control fields (id/task_name/status/error/attempts/created_at)
are plain, while args/kwargs (under "payload") and "result" are serializer blobs. Tests
that drive a backend directly build records via Job.to_record(serializer) and read them
back via Job.from_record(record, serializer).
"""

import pytest

from TaskQueue import MemoryBackend, Queue
from TaskQueue.backends.serializer import JSONSerializer, Serializer


@pytest.fixture
def serializer() -> Serializer:
    return JSONSerializer()


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend()


@pytest.fixture
def queue(backend: MemoryBackend) -> Queue:
    return Queue(backend=backend)
