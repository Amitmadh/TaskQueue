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

    async def enqueue(self, job_id: str, record: dict[str, Any]) -> None:
        self._jobs[job_id] = record
        self._events[job_id] = asyncio.Event()
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
            raise KeyError(f"job not found: {job_id}")
        return dict(record)

    async def save(
        self, job_id: str, record: dict[str, Any], *, done: bool = False
    ) -> None:
        if job_id not in self._jobs:
            raise KeyError(f"job not found: {job_id}")
        self._jobs[job_id] = record
        logger.debug("saved job %s (done=%s)", job_id, done)
        # Persist every transition (status stays observable), but only wake
        # result() waiters once the job has reached a terminal state.
        if done:
            event = self._events.get(job_id)
            if event is not None:
                event.set()

    async def release(self, job_id: str) -> None:
        record = self._jobs.get(job_id)
        if record is None:
            raise KeyError(f"job not found: {job_id}")
        # Only an in-flight (RUNNING) lease is redeliverable; ignore otherwise so
        # a double release (e.g. graceful shutdown racing a future reaper) is a
        # harmless no-op rather than re-queuing a finished job.
        if record["status"] == JobStatus.RUNNING.value:
            record["status"] = JobStatus.QUEUED.value
            await self._queue.put(job_id)
            logger.debug("released job %s for redelivery", job_id)

    async def wait_for(self, job_id: str) -> None:
        event = self._events.get(job_id)
        if event is None:
            raise RuntimeError(f"no event found for job '{job_id}'")
        await event.wait()
