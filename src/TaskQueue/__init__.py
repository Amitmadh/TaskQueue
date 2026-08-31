import logging
from importlib.metadata import version

from TaskQueue.backends.interface import Backend
from TaskQueue.backends.memory_backend import MemoryBackend
from TaskQueue.exceptions import ConfigError, JobCancelled, TaskNameError
from TaskQueue.handle import JobHandle
from TaskQueue.job import Job, JobStatus
from TaskQueue.logger import setup_logging
from TaskQueue.queue import Queue
from TaskQueue.serializers import JSONSerializer, PickleSerializer, Serializer
from TaskQueue.task import Task
from TaskQueue.worker import Worker

__version__ = version("TaskQueue")

logging.getLogger("TaskQueue").addHandler(logging.NullHandler())

__all__ = [
    "__version__",
    "Queue",
    "Task",
    "Job",
    "ConfigError",
    "JobCancelled",
    "JobHandle",
    "JobStatus",
    "TaskNameError",
    "Backend",
    "MemoryBackend",
    "Worker",
    "Serializer",
    "JSONSerializer",
    "PickleSerializer",
    "setup_logging",
]
