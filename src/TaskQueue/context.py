from contextvars import ContextVar


class JobContext:
    """Identity of the job running in this context."""

    def __init__(
        self, job_id: str, group_id: str | None, task_name: str, attempts: int
    ) -> None:
        self.job_id = job_id
        self.group_id = group_id
        self.task_name = task_name
        self.attempts = attempts
        self._spawns = 0

    def next_spawn(self) -> int:
        """Ordinal of the next spawn made by this job body.

        The count is per body rather than per scope: a scope's id is a fresh
        uuid4 on every re-run, so it cannot appear in a seed that has to
        survive one, and counting per scope would let two sequential scopes
        both claim 0.
        """
        ordinal = self._spawns
        self._spawns += 1
        return ordinal


current_job: ContextVar[JobContext | None] = ContextVar("current_job", default=None)
