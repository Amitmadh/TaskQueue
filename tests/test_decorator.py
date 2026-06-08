"""@q.task in both bare and parameterized forms, and the task registry."""

import pytest

from TaskQueue.queue import Queue
from TaskQueue.task import Task

pytestmark = pytest.mark.timeout(5)


def test_bare_form_registers_task(queue: Queue) -> None:
    @queue.task
    async def my_task() -> None: ...

    assert isinstance(my_task, Task)
    assert queue.task_registry[my_task.name] is my_task


def test_default_name_is_module_qualified(queue: Queue) -> None:
    @queue.task
    async def my_task() -> None: ...

    assert my_task.name == f"{__name__}.my_task"


def test_bare_default_max_retries_is_zero(queue: Queue) -> None:
    @queue.task
    async def my_task() -> None: ...

    assert my_task.max_retries == 0


def test_parameterized_custom_name(queue: Queue) -> None:
    @queue.task(name="emails.send")
    async def send_email(addr: str) -> None: ...

    assert send_email.name == "emails.send"
    assert queue.task_registry["emails.send"] is send_email


def test_parameterized_max_retries(queue: Queue) -> None:
    @queue.task(name="x", max_retries=5)
    async def t() -> None: ...

    assert t.max_retries == 5


async def test_parameterized_form_is_still_callable(queue: Queue) -> None:
    @queue.task(name="adder")
    async def add(x: int, y: int) -> int:
        return x + y

    assert await add(2, 3) == 5


def test_distinct_tasks_register_separately(queue: Queue) -> None:
    @queue.task
    async def a() -> None: ...
    @queue.task
    async def b() -> None: ...

    assert a.name in queue.task_registry
    assert b.name in queue.task_registry
    assert a.name != b.name
