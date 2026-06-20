from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from TaskQueue.job import Job, JobStatus

if TYPE_CHECKING:
    from TaskQueue.backends.interface import Backend
    from TaskQueue.backends.serializer import Serializer
    from TaskQueue.queue import Queue
    from TaskQueue.task import Task

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, queue: Queue, concurrency: int = 1) -> None:
        self.queue: Queue = queue
        self._backend: Backend = queue.backend
        self._serializer: Serializer = queue.serializer
        self._task_registry: dict[str, Task[Any, Any]] = queue.task_registry
        self.concurrency = concurrency
        self.workers: list[asyncio.Task[None]] = []
        self._running = False
        self._entered = False

    @property
    def running(self) -> bool:
        """Whether the pool is currently serving jobs (read-only)."""
        return self._running

    async def __aenter__(self) -> Worker:
        if self._entered:
            raise RuntimeError(
                "Worker is single-use; call queue.worker() for a fresh one"
            )
        self._entered = True
        self._running = True

        for _ in range(self.concurrency):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)
        logger.info("worker pool started (concurrency=%d)", self.concurrency)

        return self

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        self._running = False

        for worker in self.workers:
            worker.cancel()

        await asyncio.gather(*self.workers, return_exceptions=True)
        logger.info("worker pool stopped")

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                record = await self._backend.claim()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("claim failed; continuing")
                continue

            try:
                await self._process(record)
            except asyncio.CancelledError:
                # Interrupted before a terminal write (shutdown/cancel): hand the
                # lease back so the job is redelivered, never stranded in RUNNING.
                await asyncio.shield(self._backend.release(record["id"]))
                logger.info(
                    "job %s released on shutdown; will be redelivered",
                    record["id"],
                )
                raise
            except Exception:
                # A poison record (failed deserialize/save) must not kill the
                # worker and silently shrink the pool. Log and keep serving.
                logger.exception("worker loop iteration failed; continuing")

    async def _process(self, record: dict[str, Any]) -> None:
        job = Job.from_record(record, self._serializer)
        logger.debug("claimed job %s (task=%s)", job.id, job.task_name)

        task = self._task_registry.get(job.task_name)
        if task is None:
            job.error = f"no task named {job.task_name}"
            job.status = JobStatus.FAILED
            logger.warning("job %s failed: unknown task %r", job.id, job.task_name)
        else:
            try:
                job.result = await task(*job.args, **job.kwargs)
                job.status = JobStatus.COMPLETED
                logger.debug("job %s completed", job.id)
            except Exception as e:
                job.error = str(e)
                job.status = JobStatus.FAILED
                logger.warning("job %s failed: %s", job.id, e)

        await self._save(job)

    async def _save(self, job: Job) -> None:
        # Terminal write: persist outcome and wake result() waiters.
        await self._backend.save(job.id, job.to_record(self._serializer), done=True)
