"""@q.task in both bare and parameterized forms, and the task registry."""

import os
import pathlib
import subprocess
import sys
import textwrap

import pytest

from TaskQueue.backends.memory_backend import MemoryBackend
from TaskQueue.exceptions import TaskNameError
from TaskQueue.queue import Queue
from TaskQueue.task import Task

pytestmark = pytest.mark.timeout(5)


def _run(
    args: list[str], cwd: pathlib.Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a fresh interpreter that can import TaskQueue, like tests/test_base.py."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    return subprocess.run(
        [sys.executable, *args],
        capture_output=True,
        text=True,
        timeout=45,
        env=env,
        cwd=None if cwd is None else str(cwd),
    )


_TASK_MODULE = textwrap.dedent(
    """
    from TaskQueue import MemoryBackend, Queue

    q = Queue(backend=MemoryBackend(), namespace="myapp")

    @q.task
    async def add(x: int, y: int) -> int:
        return x + y

    if __name__ == "__main__":
        print(sorted(q.task_registry))
    """
)


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


# --------------------------------------------------------------------------- #
# Task names are a wire identifier: both processes must compute the same string #
# --------------------------------------------------------------------------- #


def test_namespace_replaces_the_module_path() -> None:
    q = Queue(backend=MemoryBackend(), namespace="myapp")

    @q.task
    async def my_task() -> None: ...

    assert my_task.name == "myapp.my_task"
    assert __name__ not in my_task.name  # the module path is not consulted at all


def test_explicit_name_still_wins_over_the_namespace() -> None:
    q = Queue(backend=MemoryBackend(), namespace="myapp")

    @q.task(name="emails.send")
    async def send_email(addr: str) -> None: ...

    assert send_email.name == "emails.send"


def test_duplicate_name_is_rejected(queue: Queue) -> None:
    @queue.task(name="dupe")
    async def first() -> None: ...

    with pytest.raises(TaskNameError, match="already registered"):

        @queue.task(name="dupe")
        async def second() -> None: ...

    # The first registration is intact -- the second never replaced it.
    assert queue.task_registry["dupe"] is first


@pytest.mark.timeout(60)
def test_task_defined_in_main_without_a_namespace_is_rejected() -> None:
    """A script-run module would register '__main__.x'; no worker could match it."""
    program = textwrap.dedent(
        """
        from TaskQueue import MemoryBackend, Queue

        q = Queue(backend=MemoryBackend())

        @q.task
        async def add(x: int, y: int) -> int:
            return x + y
        """
    )
    proc = _run(["-c", program])
    assert proc.returncode != 0
    assert "TaskNameError" in proc.stderr
    assert "__main__.add" in proc.stderr


@pytest.mark.timeout(60)
def test_namespaced_names_match_whether_run_as_a_script_or_imported(
    tmp_path: pathlib.Path,
) -> None:
    """The regression test for the two-terminal hang: one file, two launch modes."""
    module = tmp_path / "tasks_mod.py"
    module.write_text(_TASK_MODULE, encoding="utf-8")

    as_script = _run([str(module)])
    assert as_script.returncode == 0, as_script.stderr
    as_import = _run(
        ["-c", "import tasks_mod; print(sorted(tasks_mod.q.task_registry))"],
        cwd=tmp_path,
    )
    assert as_import.returncode == 0, as_import.stderr

    assert as_script.stdout.strip() == "['myapp.add']"
    assert as_script.stdout.strip() == as_import.stdout.strip()
