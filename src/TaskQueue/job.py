from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import uuid4

from TaskQueue.serializers import Serializer


class JobStatus(StrEnum):
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


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
        self.request_cancel = False

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Job):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def to_record(self, serializer: Serializer) -> dict[str, str | bytes]:
        """Backend storage form.

        Control fields stay plain *strings* (ints stringified,
        'request_cancel' as "0"/"1") so the backend can set them without deserializing;
        only the user payload (args/kwargs) and the result are opaque
        'bytes' blobs from the serializer.
        'error' and 'result' are written only once populated — an absent key
        means None.
        """
        record: dict[str, str | bytes] = {
            "id": self.id,
            "task_name": self.task_name,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "attempts": str(self.attempts),
            "request_cancel": str(int(self.request_cancel)),
            "payload": serializer.dumps(
                {"args": list(self.args), "kwargs": self.kwargs}
            ),
        }
        if self.error is not None:
            record["error"] = self.error
        if self.status is JobStatus.COMPLETED:
            record["result"] = serializer.dumps(self.result)
        return record

    @classmethod
    def from_record(cls, record: dict[str, Any], serializer: Serializer) -> Self:
        job = object.__new__(cls)
        raw_payload = record.get("payload")
        payload = serializer.loads(raw_payload) if raw_payload is not None else None
        raw_result = record.get("result")
        job.result = serializer.loads(raw_result) if raw_result is not None else None
        job.id = record["id"]
        job.task_name = record["task_name"]
        job.created_at = datetime.fromisoformat(record["created_at"])
        job.status = JobStatus(record["status"])
        job.error = record.get("error")
        job.args = tuple(payload["args"]) if payload is not None else ()
        job.kwargs = dict(payload["kwargs"]) if payload is not None else {}
        job.request_cancel = bool(int(record.get("request_cancel", 0)))
        job.attempts = int(record.get("attempts", 0))
        return job
