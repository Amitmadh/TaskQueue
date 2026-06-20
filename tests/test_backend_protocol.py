"""Backend is a runtime_checkable Protocol with a fixed, minimal surface."""

import inspect

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.memory import MemoryBackend

REQUIRED = {"enqueue", "claim", "get_job", "save", "release", "wait_for"}


def test_memory_backend_is_structural_instance() -> None:
    assert isinstance(MemoryBackend(), Backend)


def test_required_methods_present() -> None:
    for name in REQUIRED:
        assert hasattr(MemoryBackend, name), f"MemoryBackend missing {name!r}"


def test_surface_has_not_silently_grown() -> None:
    surface = {n for n in dir(Backend) if not n.startswith("_")}
    assert surface == REQUIRED


def test_incomplete_implementation_is_not_an_instance() -> None:
    class Partial:
        async def enqueue(self, job_id: str, record: object) -> None: ...
        async def claim(self) -> object: ...

    assert not isinstance(Partial(), Backend)


def test_backend_methods_are_async() -> None:
    be = MemoryBackend()
    for name in REQUIRED:
        assert inspect.iscoroutinefunction(getattr(be, name)), f"{name} should be async"
