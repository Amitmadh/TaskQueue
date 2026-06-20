from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Backend(Protocol):
    """Cross-process system of record. Stores one record per job (== a Redis
    hash): control/envelope fields are plain, the user payload and result are
    serialized blobs.

    Delivery is at-least-once: `claim` leases a job (marks RUNNING), a terminal
    `save(done=True)` acks it, and `release` nacks an unfinished lease so the job
    is redelivered instead of stranded. The backend owns the QUEUED/RUNNING
    transitions; the worker owns the terminal COMPLETED/FAILED write.
    """

    async def enqueue(self, job_id: str, record: dict[str, Any]) -> None: ...
    async def claim(self) -> dict[str, Any]: ...
    async def get_job(self, job_id: str) -> dict[str, Any]: ...
    async def save(
        self, job_id: str, record: dict[str, Any], *, done: bool = False
    ) -> None: ...
    async def release(self, job_id: str) -> None: ...
    async def wait_for(self, job_id: str) -> None: ...
