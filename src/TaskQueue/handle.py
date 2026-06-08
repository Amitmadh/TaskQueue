from TaskQueue.backends.interface import Backend
from TaskQueue.job import JobStatus


class JobHandle[R]:
    def __init__(self, job_id: str, backend: Backend):
        self._job_id = job_id
        self._backend = backend

    @property
    def job_id(self) -> str:
        return self._job_id

    async def result(self) -> R:
        await self._backend.wait_for(self._job_id)
        job = await self._backend.get_job(self._job_id)
        if job.error is not None:
            raise RuntimeError(job.error)
        return job.result

    async def status(self) -> JobStatus:
        job = await self._backend.get_job(job_id=self._job_id)
        return job.status
