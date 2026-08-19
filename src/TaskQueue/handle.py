import asyncio

from TaskQueue.backends.interface import Backend
from TaskQueue.exceptions import JobCancelled
from TaskQueue.job import Job, JobStatus
from TaskQueue.serializers import Serializer


class JobHandle[R]:
    def __init__(self, job_id: str, backend: Backend, serializer: Serializer) -> None:
        self._job_id = job_id
        self._backend = backend
        self._serializer = serializer
        self._lock = asyncio.Lock()
        self._job: Job | None = None  # cached terminal job, set once consumed

    @property
    def job_id(self) -> str:
        return self._job_id

    async def result(self) -> R:
        job = await self._consume()
        if job.status is JobStatus.FAILED:
            raise RuntimeError(f"job {self._job_id!r} failed: {job.error}")
        if job.status is JobStatus.CANCELLED:
            raise JobCancelled(f"job {self._job_id!r} was cancelled")
        return job.result

    async def status(self) -> JobStatus:
        # Once consumed the record is gone from the backend, so serve from cache.
        if self._job is not None:
            return self._job.status
        record = await self._backend.get_job(self._job_id)
        return JobStatus(record["status"])

    async def cancel(self) -> None:
        await self._backend.request_cancel(self._job_id)

    async def _consume(self) -> Job:
        # Await the terminal outcome once, cache it on the handle, and free the
        # backing record. Repeat and concurrent callers share the cached job;
        if self._job is not None:
            return self._job
        async with self._lock:
            if self._job is not None:
                return self._job
            record = await self._backend.take_result(self._job_id)
            self._job = Job.from_record(record, self._serializer)
            return self._job
