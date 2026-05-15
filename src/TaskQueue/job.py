from dataclasses import dataclass
from enum import Enum


@dataclass
class Job:
    id: int
    username: str
    email: str
    is_active: bool = True  # Default value

class JobStatus(Enum):
    pass
