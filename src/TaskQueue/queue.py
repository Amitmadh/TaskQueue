import logging
from collections.abc import Awaitable, Callable
from typing import Any, overload

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.serializer import JSONSerializer, Serializer
from TaskQueue.jobGroup import JobGroup, OnError
from TaskQueue.task import Task
from TaskQueue.worker import Worker

logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, backend: Backend, serializer: Serializer | None = None) -> None:
        self._task_registry: dict[str, Task[Any, Any]] = {}
        self._backend = backend
        self._serializer: Serializer = serializer or JSONSerializer()

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
            task_name = name or f"{f.__module__}.{f.__name__}"
            if task_name in self._task_registry:
                logger.warning(
                    "task %r is already registered; "
                    "overwriting the previous registration",
                    task_name,
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

    def worker(self, concurrency: int = 1) -> Worker:
        return Worker(self, concurrency=concurrency)

    def group(
        self,
        *,
        on_error: OnError | str = OnError.CANCEL_SIBLINGS,
        deadline: float | None = None,
    ) -> JobGroup:
        """Open a structured-concurrency scope, entered with ``async with``.

        The block does not exit until every job spawned into the scope reaches a
        terminal state, applying the ``on_error`` policy (and ``deadline`` if
        set). This is the everyday scope: a group opened inside another is
        effectively its child, since the outer ``async with`` cannot exit until
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

        Behaves like ``group()`` but documents intent: a root group stands on its
        own instead of nesting. Spawning into it without ``async with``
        (``await q.root_group().spawn(...)``) is the one sanctioned way to detach
        work from any enclosing scope, and it is the unit a heartbeat reaper will
        watch in a later phase.
        """
        return JobGroup(self._backend, on_error=on_error, deadline=deadline)
