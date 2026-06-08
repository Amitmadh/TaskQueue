from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, overload

from TaskQueue.backends.interface import Backend
from TaskQueue.task import Task
from TaskQueue.worker import Worker

if TYPE_CHECKING:
    from TaskQueue.handle import JobHandle


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
    def __init__(self, backend: Backend) -> None:
        self._task_registry: dict[str, Task[Any, Any]] = {}
        self._backend = backend

    @property
    def backend(self):
        return self._backend

    @property
    def task_registry(self):
        return self._task_registry

    #    def task[**P, R](self,
    #            func: Callable[P, Awaitable[R]],
    #            *,
    #            name: str | None = None,
    #            ) -> Task[P, R]:
    #
    #        task_name = name or f"{func.__module__}.{func.__name__}"
    #        task_instance = Task(func=func, name=task_name, backend=self._backend)
    #        self._task_registry[task_name] = task_instance
    #        return task_instance

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
            instance = Task(
                func=f, name=task_name, backend=self._backend, max_retries=max_retries
            )
            self._task_registry[task_name] = instance
            return instance

        if func is not None:
            return decorator(func)
        return decorator

    def worker(self, concurrency: int = 1) -> Worker:
        return Worker(self, concurrency=concurrency)

    def root_group(self) -> _NoOpScope:
        return _NoOpScope()
