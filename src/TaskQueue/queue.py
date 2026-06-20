import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, overload

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.serializer import JSONSerializer, Serializer
from TaskQueue.task import Task
from TaskQueue.worker import Worker

if TYPE_CHECKING:
    from TaskQueue.handle import JobHandle

logger = logging.getLogger(__name__)


class _NoOpScope:
    """Phase 1 placeholder. Phase 2 replaces this with JobGroup."""

    async def __aenter__(self) -> "_NoOpScope":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None  # owns nothing, cancels nothing, waits for nothing

    async def spawn[**P, R](
        self, task: Task[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> "JobHandle[R]":
        return await task.submit(*args, **kwargs)


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
                    "task name %r already registered; overwriting", task_name
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

    def root_group(self) -> _NoOpScope:
        return _NoOpScope()
