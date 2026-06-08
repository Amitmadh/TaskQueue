"""Static type-safety spec — verified by PYRIGHT, not pytest.

The point of the library is that the queue boundary preserves types. These
functions are never executed; pyright checks the assert_type calls. Run with:

    pyright tests/test_typing.py

(or add "tests" to [tool.pyright].include). The one pytest test here just
confirms the module imports.
"""

from __future__ import annotations

# Type-only spec: the _check_* fns are read by pyright, never executed at runtime.
# pyright: reportUnusedFunction=false

try:
    from typing import assert_type
except ImportError:  # Python < 3.11 fallback for the runner
    from typing_extensions import assert_type  # noqa: UP035

from TaskQueue import MemoryBackend, Queue
from TaskQueue.handle import JobHandle

q = Queue(backend=MemoryBackend())


@q.task
async def add(x: int, y: int) -> int:
    return x + y


@q.task
async def make_name(first: str, last: str) -> str:
    return f"{first} {last}"


async def _check_submit_preserves_types() -> None:
    handle = await add.submit(2, 3)
    assert_type(handle, JobHandle[int])
    assert_type(await handle.result(), int)


async def _check_str_return_type() -> None:
    handle = await make_name.submit("a", "b")
    assert_type(await handle.result(), str)


async def _check_spawn_preserves_types() -> None:
    handle = await q.root_group().spawn(add, 2, 3)
    assert_type(handle, JobHandle[int])
    assert_type(await handle.result(), int)


# The following SHOULD be type errors. Uncomment to confirm pyright rejects them:
# async def _check_rejects_wrong_arg_types() -> None:
#     await add.submit("nope", "wrong")   # reportArgumentType
# async def _check_rejects_missing_args() -> None:
#     await add.submit(1)                 # reportCallIssue


def test_typing_module_importable() -> None:
    assert add.name and make_name.name
