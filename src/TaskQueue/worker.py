from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from TaskQueue.backends.interface import Backend
    from TaskQueue.queue import Queue
    from TaskQueue.task import Task


class Worker:
    def __init__(self, queue: Queue, concurrency: int = 1):
        self.queue: Queue = queue
        self._backend: Backend = queue.backend
        self._task_registry: dict[str, Task[Any, Any]] = queue.task_registry
        self.concurrency = concurrency
        self.workers: list[asyncio.Task[None]] = []
        self.running = False

    async def __aenter__(self):
        self.running = True

        for _ in range(self.concurrency):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)

        return self

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        self.running = False

        for worker in self.workers:
            worker.cancel()

        await asyncio.gather(*self.workers, return_exceptions=True)

    async def _worker_loop(self):
        while self.running:
            job = await self._backend.claim()
            task_name = job.task_name
            task = self._task_registry.get(task_name)
            if task is None:
                error = f"no task named {task_name}"
                await self._backend.store_error(job_id=job.id, error=error)
                continue
            try:
                result = await task(*job.args, **job.kwargs)
                await self._backend.store_result(job_id=job.id, result=result)
            except Exception as e:
                await self._backend.store_error(job_id=job.id, error=str(e))
