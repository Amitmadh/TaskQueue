"""The package must import without a cycle, and expose its public surface."""

import importlib

PUBLIC = [
    "Queue",
    "Task",
    "Job",
    "JobStatus",
    "JobHandle",
    "Backend",
    "MemoryBackend",
    "Worker",
    "Serializer",
    "JSONSerializer",
    "PickleSerializer",
]


def test_package_imports() -> None:
    import TaskQueue

    assert TaskQueue is not None


def test_public_exports_present() -> None:
    import TaskQueue

    missing = [n for n in PUBLIC if not hasattr(TaskQueue, n)]
    assert not missing, f"missing exports: {missing}"


def test_all_is_consistent() -> None:
    import TaskQueue

    for name in TaskQueue.__all__:
        assert hasattr(TaskQueue, name), f"__all__ lists undefined name: {name}"


def test_submodules_import_without_cycle() -> None:
    for mod in (
        "TaskQueue.queue",
        "TaskQueue.worker",
        "TaskQueue.task",
        "TaskQueue.handle",
        "TaskQueue.job",
        "TaskQueue.backends.interface",
        "TaskQueue.backends.memory",
        "TaskQueue.backends.serializer",
    ):
        assert importlib.import_module(mod) is not None
