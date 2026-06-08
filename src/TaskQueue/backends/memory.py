import asyncio
from typing import Any

from TaskQueue.backends.interface import Backend
from TaskQueue.job import Job, JobStatus


class MemoryBackend(Backend):
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._queue: asyncio.Queue[Job] = asyncio.Queue()

    async def enqueue(self, job: Job) -> None:
        self._jobs[job.id] = job
        self._events[job.id] = asyncio.Event()
        job.status = JobStatus.QUEUED
        await self._queue.put(job)

    async def claim(self) -> Job:
        job = await self._queue.get()
        job.status = JobStatus.RUNNING
        return job

    async def get_job(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError("job not found")
        return job

    async def store_result(self, job_id: str, result: Any) -> None:
        job = await self.get_job(job_id=job_id)
        job.result = result
        job.status = JobStatus.COMPLETED

        event = self._events.get(job_id)
        if event is None:
            raise RuntimeError(f"no event found for job '{job_id}'")
        event.set()

    async def store_error(self, job_id: str, error: str) -> None:
        job = await self.get_job(job_id=job_id)
        job.error = error
        job.status = JobStatus.FAILED

        event = self._events.get(job_id)
        if event is None:
            raise RuntimeError(f"no event found for job '{job_id}'")
        event.set()

    async def wait_for(self, job_id: str) -> None:
        event = self._events.get(job_id)
        if event is None:
            raise RuntimeError(f"no event found for job '{job_id}'")
        await event.wait()
