from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    task_name: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: JobStatus = field(default=JobStatus.PENDING)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
