from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import uuid4

from TaskQueue.backends.serializer import Serializer


class JobStatus(StrEnum):
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Job:
    def __init__(
        self,
        task_name: str,
        id: str | None = None,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        status: JobStatus = JobStatus.CREATED,
        result: Any = None,
        error: str | None = None,
        attempts: int = 0,
    ) -> None:
        self.id = str(id or uuid4().hex)
        self.task_name = task_name
        self.created_at = datetime.now(UTC)
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.status = status
        self.result: Any = result
        self.error: str | None = error
        self.attempts = attempts

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Job):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def to_record(self, serializer: Serializer) -> dict[str, Any]:
        """Backend storage form (a record, == a Redis hash).

        Control/envelope fields stay plain so the backend can set them
        (e.g. `status` on claim) without deserializing. Only the user payload
        (args/kwargs) and the result are opaque serialized blobs.
        """
        return {
            "id": self.id,
            "task_name": self.task_name,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "error": self.error,
            "attempts": self.attempts,
            "payload": serializer.dumps(
                {"args": list(self.args), "kwargs": self.kwargs}
            ),
            "result": (
                serializer.dumps(self.result)
                if self.status is JobStatus.COMPLETED
                else None
            ),
        }

    @classmethod
    def from_record(cls, record: dict[str, Any], serializer: Serializer) -> Self:
        payload = serializer.loads(record["payload"])
        job = object.__new__(cls)
        job.id = record["id"]
        job.task_name = record["task_name"]
        job.created_at = datetime.fromisoformat(record["created_at"])
        job.args = tuple(payload["args"])
        job.kwargs = payload["kwargs"]
        job.status = JobStatus(record["status"])
        job.result = (
            serializer.loads(record["result"]) if record["result"] is not None else None
        )
        job.error = record["error"]
        job.attempts = record["attempts"]
        return job
