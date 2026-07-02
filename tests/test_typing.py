"""Static type-safety spec — verified by PYRIGHT, not pytest.

The whole point of the library is that the queue boundary preserves types, so
this file is the executable spec for that guarantee. These functions are never
run; pyright checks the ``assert_type`` calls (the positive cases) and the
``# pyright: ignore`` assertions (the negative cases).

This file is type-checked by the project's default ``pyright`` run: pyproject's
``[tool.pyright]`` lists it explicitly in ``include`` under strict mode with
``reportUnnecessaryTypeIgnoreComment = "error"``. That makes the negative cases
self-enforcing: every ``# pyright: ignore`` below is load-bearing, so if a
wrong-typed ``.submit()`` call ever STOPS being a type error, the now-unnecessary
ignore fails the build — the regression guard for the ParamSpec guarantee. (The
rest of ``tests/`` is intentionally not type-checked.)

The one pytest test here just confirms the module imports.
"""

from __future__ import annotations

# Type-only spec: the _check_* fns are read by pyright, never executed at runtime.
# pyright: reportUnusedFunction=false
from typing import assert_type

from TaskQueue import MemoryBackend, Queue
from TaskQueue.handle import JobHandle

q = Queue(backend=MemoryBackend())


@q.task
async def add(x: int, y: int) -> int:
    return x + y


@q.task
async def make_name(first: str, last: str) -> str:
    return f"{first} {last}"


# --- positive cases: types must flow through the queue boundary ----------------
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


# --- negative cases: these MUST stay type errors -------------------------------
# Each `pyright: ignore` is load-bearing under reportUnnecessaryTypeIgnoreComment:
# if any of these calls ever type-checks, its ignore becomes unnecessary and the
# typing pass fails — catching a regression in the ParamSpec guarantee.
async def _check_rejects_wrong_arg_types() -> None:
    await add.submit("nope", "wrong")  # pyright: ignore[reportArgumentType]


async def _check_rejects_missing_args() -> None:
    await add.submit(1)  # pyright: ignore[reportCallIssue]


async def _check_rejects_extra_args() -> None:
    await add.submit(1, 2, 3)  # pyright: ignore[reportCallIssue]


def test_typing_module_importable() -> None:
    assert add.name and make_name.name
