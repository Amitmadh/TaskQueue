import logging

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.serializer import Serializer
from TaskQueue.job import Job, JobStatus

logger = logging.getLogger(__name__)


class JobHandle[R]:
    def __init__(self, job_id: str, backend: Backend, serializer: Serializer) -> None:
        self._job_id = job_id
        self._backend = backend
        self._serializer = serializer

    @property
    def job_id(self) -> str:
        return self._job_id

    async def result(self) -> R:
        logger.debug("awaiting result for job %s", self._job_id)
        await self._backend.wait_for(self._job_id)
        record = await self._backend.get_job(self._job_id)
        job = Job.from_record(record, self._serializer)
        if job.error is not None:
            raise RuntimeError(job.error)
        return job.result

    async def status(self) -> JobStatus:
        record = await self._backend.get_job(self._job_id)
        return JobStatus(record["status"])
