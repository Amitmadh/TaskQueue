"""Task: __call__ delegation and submit() job construction."""

import pytest

from TaskQueue.handle import JobHandle
from TaskQueue.job import JobStatus
from TaskQueue.queue import Queue
from TaskQueue.task import Task

pytestmark = pytest.mark.timeout(5)


def test_decorator_produces_a_task(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    assert isinstance(add, Task)
    assert add.name.endswith("add")


async def test_call_delegates_to_wrapped_function(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    assert await add(2, 3) == 5


async def test_submit_returns_handle_with_job_id(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    handle = await add.submit(2, 3)
    assert isinstance(handle, JobHandle)
    assert isinstance(handle.job_id, str) and handle.job_id


async def test_submit_builds_job_from_call(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    handle = await add.submit(2, 3)
    job = await queue.backend.get_job(handle.job_id)
    assert job.task_name == add.name
    assert job.args == (2, 3)
    assert job.kwargs == {}
    assert job.status is JobStatus.QUEUED


async def test_submit_captures_kwargs(queue: Queue) -> None:
    @queue.task
    async def greet(name: str, *, loud: bool = False) -> str:
        return name

    handle = await greet.submit("amit", loud=True)
    job = await queue.backend.get_job(handle.job_id)
    assert job.args == ("amit",)
    assert job.kwargs == {"loud": True}


async def test_each_submit_is_a_distinct_job(queue: Queue) -> None:
    @queue.task
    async def add(x: int, y: int) -> int:
        return x + y

    h1 = await add.submit(1, 1)
    h2 = await add.submit(2, 2)
    assert h1.job_id != h2.job_id
