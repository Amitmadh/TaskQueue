from importlib.metadata import version

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.memory import MemoryBackend
from TaskQueue.handle import JobHandle
from TaskQueue.job import Job, JobStatus
from TaskQueue.queue import Queue
from TaskQueue.task import Task
from TaskQueue.worker import Worker

__version__ = version("taskQueue")

__all__ = [
    "__version__",
    "Queue",
    "Task",
    "Job",
    "JobHandle",
    "JobStatus",
    "Backend",
    "MemoryBackend",
    "Worker",
]
