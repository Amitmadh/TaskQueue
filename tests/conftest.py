"""Phase 1 test suite — a TDD spec for the *intended* API.

These tests are written against the target design described in
PHASE1_CODE_REVIEW_v2.md, not the code as it currently stands. Expect several
to fail until the documented fixes land. In particular, the entire suite cannot
be collected until the queue<->worker circular import (C1) is fixed.

Target contract these tests encode:
  exports     : Queue, Task, Job, JobStatus, JobHandle, Backend, MemoryBackend, Worker
  JobStatus   : str-enum {QUEUED, RUNNING, COMPLETED, FAILED}
  Job         : keyword-constructable; to_dict()/from_dict() round-trip; equality
  Backend     : {enqueue, claim, get_job, store_result, store_error, wait_for}
  @q.task     : bare AND @q.task(name=..., max_retries=...); bare max_retries == 0
  JobHandle   : .job_id, await .result() (raises on failure), await .status()
  Worker      : async context manager; survives unknown task names and task errors
"""

import pytest

from TaskQueue import MemoryBackend, Queue


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend()


@pytest.fixture
def queue(backend: MemoryBackend) -> Queue:
    return Queue(backend=backend)
