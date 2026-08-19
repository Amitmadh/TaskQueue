import asyncio
import logging
from typing import Any

from TaskQueue.backends.interface import Backend
from TaskQueue.job import JobStatus

logger = logging.getLogger(__name__)


class MemoryBackend(Backend):
    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._cancels: dict[str, asyncio.Event] = {}

    async def enqueue(self, job_id: str, record: dict[str, Any]) -> None:
        self._jobs[job_id] = record
        self._events[job_id] = asyncio.Event()
        self._cancels[job_id] = asyncio.Event()
        record["status"] = JobStatus.QUEUED.value
        await self._queue.put(job_id)
        logger.debug("enqueued job %s", job_id)

    async def claim(self) -> dict[str, Any]:
        job_id = await self._queue.get()
        record = self._jobs[job_id]
        # The RUNNING transition is a single-field write — no deserialize.
        record["status"] = JobStatus.RUNNING.value
        logger.debug("claimed job %s", job_id)
        return dict(record)

    async def get_job(self, job_id: str) -> dict[str, Any]:
        record = self._jobs.get(job_id)
        if record is None:
            raise KeyError(f"job {job_id!r} not found")
        return dict(record)

    async def save(
        self, job_id: str, record: dict[str, Any], *, done: bool = False
    ) -> None:
        if job_id not in self._jobs:
            raise KeyError(f"job {job_id!r} not found")
        self._jobs[job_id] = record
        logger.debug("saved job %s (done=%s)", job_id, done)
        # Persist every transition (status stays observable), but only wake
        # result() waiters once the job has reached a terminal state.
        if done:
            # Terminal state reached: wake any result() waiters, then drop the
            # per-job synchronization primitives — neither the done-event nor the
            # cancel-event is ever needed again. The record is kept as the result.
            event = self._events.pop(job_id, None)
            if event is not None:
                event.set()
            self._cancels.pop(job_id, None)

    async def release(self, job_id: str) -> None:
        record = self._jobs.get(job_id)
        if record is None:
            raise KeyError(f"job {job_id!r} not found")
        # Only an in-flight (RUNNING) lease is redeliverable; ignore otherwise so
        # a double release (e.g. graceful shutdown racing a future reaper) is a
        # harmless no-op rather than re-queuing a finished job.
        if record["status"] == JobStatus.RUNNING.value:
            record["status"] = JobStatus.QUEUED.value
            await self._queue.put(job_id)
            logger.debug("released job %s for redelivery", job_id)

    async def take_result(self, job_id: str) -> dict[str, Any]:
        event = self._events.get(job_id)
        if event is not None:
            await event.wait()
        # Terminal now (or already was): return the record and free it entirely.
        self._events.pop(job_id, None)
        self._cancels.pop(job_id, None)
        record = self._jobs.pop(job_id, None)
        if record is None:
            raise RuntimeError(f"cannot take result of unknown job {job_id!r}")
        return dict(record)

    async def request_cancel(self, job_id: str) -> None:
        record = self._jobs.get(job_id)
        if record is None:
            return
        if record["status"] in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        ):
            return
        record["request_cancel"] = "1"
        logger.debug("cancel requested for job %s", job_id)
        cancel = self._cancels.get(job_id)
        if cancel is not None:
            cancel.set()

    async def request_cancel_many(self, job_ids: list[str]) -> None:
        for job_id in job_ids:
            await self.request_cancel(job_id)

    async def wait_cancel(self, job_id: str) -> None:
        cancel = self._cancels.get(job_id)
        if cancel is None:
            raise RuntimeError(f"cannot watch cancellation for unknown job {job_id!r}")
        await cancel.wait()
