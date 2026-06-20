import logging
from collections.abc import Awaitable, Callable

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.serializer import JSONSerializer, Serializer
from TaskQueue.handle import JobHandle
from TaskQueue.job import Job

logger = logging.getLogger(__name__)


class Task[**P, R]:
    def __init__(
        self,
        func: Callable[P, Awaitable[R]],
        name: str,
        backend: Backend,
        max_retries: int = 0,
        *,
        serializer: Serializer | None = None,
    ) -> None:
        self.func: Callable[P, Awaitable[R]] = func
        self.name: str = name
        self._backend: Backend = backend
        self.max_retries: int = max_retries
        self._serializer: Serializer = serializer or JSONSerializer()

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return await self.func(*args, **kwargs)

    async def submit(self, *args: P.args, **kwargs: P.kwargs) -> JobHandle[R]:
        job = Job(task_name=self.name, args=args, kwargs=kwargs)
        await self._backend.enqueue(job.id, job.to_record(self._serializer))
        logger.debug("submitted job %s (task=%s)", job.id, self.name)
        return JobHandle[R](job.id, self._backend, self._serializer)
