class JobCancelled(Exception):
    """The job was cancelled before it produced a result."""


class TaskNameError(RuntimeError):
    """A task name is unusable as a wire identifier."""
