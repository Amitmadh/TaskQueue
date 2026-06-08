from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import uuid4


class JobStatus(StrEnum):
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
        status: JobStatus = JobStatus.QUEUED,
        result: Any = None,
        error: str | None = None,
        attempts: int = 3,
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

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        job = object.__new__(cls)
        job.__dict__.update(d)
        return job
