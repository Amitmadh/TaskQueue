import logging
from collections.abc import Awaitable, Callable
from typing import Any, overload

from TaskQueue.backends.interface import Backend
from TaskQueue.exceptions import TaskNameError
from TaskQueue.job_group import JobGroup, OnError
from TaskQueue.serializers import JSONSerializer, Serializer
from TaskQueue.task import Task
from TaskQueue.worker import DEFAULT_HEARTBEAT_INTERVAL_SECONDS, Worker

logger = logging.getLogger(__name__)


class Queue:
    def __init__(
        self,
        backend: Backend,
        serializer: Serializer | None = None,
        *,
        namespace: str | None = None,
    ) -> None:
        """A queue, optionally with a fixed namespace for its task names."""
        self._task_registry: dict[str, Task[Any, Any]] = {}
        self._backend = backend
        self._serializer: Serializer = serializer or JSONSerializer()
        self._namespace = namespace

    @property
    def namespace(self) -> str | None:
        return self._namespace

    def _derive_name(self, func: Callable[..., Any]) -> str:
        """The default task name"""
        if self._namespace is not None:
            return f"{self._namespace}.{func.__name__}"
        module = func.__module__
        if module == "__main__":
            raise TaskNameError(
                f"cannot derive a name for task {func.__name__!r}: as a script "
                f"it registers as '__main__.{func.__name__}', but a worker that "
                f"imports the module registers '<import.path>.{func.__name__}'. "
                f"Pass Queue(..., namespace='myapp') or "
                f"@q.task(name='myapp.{func.__name__}')."
            )
        return f"{module}.{func.__name__}"

    @property
    def backend(self) -> Backend:
        return self._backend

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def task_registry(self) -> dict[str, Task[Any, Any]]:
        return self._task_registry

    @overload
    def task[**P, R](
        self,
        func: Callable[P, Awaitable[R]],
        *,
        name: str | None = None,
        max_retries: int = 0,
    ) -> Task[P, R]: ...

    @overload
    def task[**P, R](
        self,
        func: None = None,
        *,
        name: str | None = None,
        max_retries: int = 0,
    ) -> Callable[[Callable[P, Awaitable[R]]], Task[P, R]]: ...

    def task[**P, R](
        self,
        func: Callable[P, Awaitable[R]] | None = None,
        *,
        name: str | None = None,
        max_retries: int = 0,
    ) -> Task[P, R] | Callable[[Callable[P, Awaitable[R]]], Task[P, R]]:
        def decorator(f: Callable[P, Awaitable[R]]) -> Task[P, R]:
            task_name = name or self._derive_name(f)
            if task_name in self._task_registry:
                # Rebinding a name silently would point jobs already queued
                # under it at different code, so this is an error, not a warning.
                raise TaskNameError(
                    f"task {task_name!r} is already registered on this queue: "
                    f"Give one an explicit name (@q.task(name=...))."
                )
            instance = Task(
                func=f,
                name=task_name,
                backend=self._backend,
                max_retries=max_retries,
                serializer=self._serializer,
            )
            self._task_registry[task_name] = instance
            logger.debug("registered task %r (max_retries=%d)", task_name, max_retries)
            return instance

        if func is not None:
            return decorator(func)
        return decorator

    def worker(
        self,
        concurrency: int = 1,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> Worker:
        return Worker(
            self, concurrency=concurrency, heartbeat_interval=heartbeat_interval
        )

    def group(
        self,
        *,
        on_error: OnError | str = OnError.CANCEL_SIBLINGS,
        deadline: float | None = None,
    ) -> JobGroup:
        """Open a structured-concurrency scope, entered with 'async with'.

        The block does not exit until every job spawned into the scope reaches a
        terminal state. a group opened inside another is
        effectively its child, since the outer 'async with' cannot exit until
        the inner one has.
        """
        return JobGroup(self._backend, on_error=on_error, deadline=deadline)

    def root_group(
        self,
        *,
        on_error: OnError | str = OnError.CANCEL_SIBLINGS,
        deadline: float | None = None,
    ) -> JobGroup:
        """Open a detached, top-level scope — the explicit fire-and-forget entry.

        Behaves like 'group()' but documents intent: a root group stands on its
        own instead of nesting. Spawning into it without 'async with'
        (``await q.root_group().spawn(...)``) is the one sanctioned way to detach
        work from any enclosing scope, and it is the unit a heartbeat reaper will
        watch in a later phase.
        """
        return JobGroup(self._backend, on_error=on_error, deadline=deadline)
