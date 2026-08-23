from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Backend(Protocol):
    """Cross-process system of record. Stores one record per job (== a Redis
    hash): control/envelope fields are plain, the user payload and result are
    serialized blobs.

    Delivery is at-least-once: 'claim' leases a job (marks RUNNING), a terminal
    'save(done=True)' acks it, and 'release' nacks an unfinished lease so the job
    is redelivered instead of stranded. The backend owns the QUEUED/RUNNING
    transitions; the worker owns the terminal COMPLETED/FAILED/CANCELLED write.
    'take_result' returns a finished job's record and frees it in a single call.
    """

    async def enqueue(self, job_id: str, record: dict[str, Any]) -> None:
        """Store a newly created (QUEUED) job and make it claimable.

        The record must be persisted before the job is exposed to 'claim', so a
        worker can never claim a job whose record does not yet exist.
        """
        ...

    async def claim(self) -> dict[str, Any] | None:
        """Wait a bounded interval for a job, lease it, and return its record.

        Returns 'None' if nothing became claimable within that interval, handing
        control back so the caller can observe a shutdown request instead of
        blocking forever. Callers loop; an implementation must never block
        indefinitely.

        Leasing transitions the job QUEUED -> RUNNING. The returned record is a
        detached copy — mutating it must not corrupt stored state. The lease is
        acked by a terminal 'save(done=True)' or nacked by 'release'.
        """
        ...

    async def get_job(self, job_id: str) -> dict[str, Any]:
        """Return a detached copy of the job's stored record.

        Raises 'KeyError' if the job is unknown — never enqueued, or already
        taken.
        """
        ...

    async def save(
        self, job_id: str, record: dict[str, Any], *, done: bool = False
    ) -> None:
        """Persist a record update.

        'done=True' is the terminal write: it acks the lease and wakes
        'take_result' waiters. `done=False` persists an intermediate transition
        without waking them (waking early would let 'result()' return
        prematurely). Raises 'KeyError' if the job is unknown.
        """
        ...

    async def release(self, job_id: str) -> None:
        """Nack an in-flight lease: return a RUNNING job to QUEUED for redelivery.

        A no-op if the job is not currently leased (e.g. already terminal), so a
        double release — a graceful shutdown racing a reaper — is harmless.
        """
        ...

    async def take_result(self, job_id: str) -> dict[str, Any]:
        """Block until the job is terminal, then return its record AND free it.

        Wakes on the signal raised by a terminal 'save(done=True)' (returning
        immediately if the job is already terminal), returns a detached copy of
        the terminal record, and removes it from the store. so a networked
        backend delivers the result and frees it in one round-trip.
        Single-consumer: the record is gone afterward. Raises if the job is unknown.
        """
        ...

    async def request_cancel(self, job_id: str) -> None:
        """Request cancellation of a job.

        Durably flags the job (so a worker that claims it later can skip it) and
        notifies any worker already running it via 'wait_cancel'. An idempotent
        no-op on a job that is already finished or unknown — completion wins.
        """
        ...

    async def request_cancel_many(self, job_ids: list[str]) -> None:
        """Request cancellation of several jobs in one call — a batch
        'request_cancel'.

        Each id follows the 'request_cancel' contract (idempotent; a no-op on a
        finished or unknown job), batched into a single round-trip.
        """
        ...

    async def wait_cancel(self, job_id: str) -> None:
        """Block until cancellation is requested for the job.

        Wakes on the signal raised by 'request_cancel'; the worker races this
        against the running job. Returns immediately if cancellation was already
        requested.
        """
        ...

    async def heartbeat(self) -> None:
        """Record that this worker process is still alive.

        Called on a timer by the pool. Liveness is push-based: a worker that
        stops calling this is presumed dead once its last beat ages past the
        backend's worker TTL. Nothing asks and nothing replies, so a dropped
        message can never be mistaken for a dead worker.
        """
        ...

    async def reap(self) -> int:
        """Return the leases of presumed-dead workers to the queue.

        Returns the number of jobs requeued. Every worker calls this on a timer
        and no lock coordinates them: the reclaim is one atomic step, so the
        first caller takes the work and the rest find nothing left to do. A
        no-op for single-process backends, which cannot outlive their own
        leases.
        """
        ...
