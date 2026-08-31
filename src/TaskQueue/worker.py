from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from TaskQueue.exceptions import ConfigError
from TaskQueue.job import Job, JobStatus

if TYPE_CHECKING:
    from TaskQueue.backends.interface import Backend
    from TaskQueue.queue import Queue
    from TaskQueue.serializers import Serializer
    from TaskQueue.task import Task

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0
_CLAIM_ERROR_BACKOFF_SECONDS = 1.0
_MIN_BEATS_PER_TTL = 3


def check_liveness(backend: Backend, heartbeat_interval: float) -> None:
    """Refuse a heartbeat interval too close to the backend's worker TTL."""
    worker_ttl: object = getattr(backend, "worker_ttl", None)
    if not isinstance(worker_ttl, int):
        return
    if worker_ttl < heartbeat_interval * _MIN_BEATS_PER_TTL:
        raise ConfigError(
            f"heartbeat_interval of {heartbeat_interval:g}s is too long for "
            f"this backend's worker_ttl of {worker_ttl}s: every worker would be "
            f"presumed dead between its own beats, and peers would reclaim jobs "
            f"that are still running. Use an interval of at most "
            f"{worker_ttl / _MIN_BEATS_PER_TTL:g}s, or build the backend with a "
            f"worker_ttl of at least {heartbeat_interval * _MIN_BEATS_PER_TTL:g}s."
        )


class Worker:
    def __init__(
        self,
        queue: Queue,
        concurrency: int = 1,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self.queue: Queue = queue
        self._backend: Backend = queue.backend
        check_liveness(self._backend, heartbeat_interval)
        self._serializer: Serializer = queue.serializer
        self._task_registry: dict[str, Task[Any, Any]] = queue.task_registry
        self.concurrency = concurrency
        self.workers: list[asyncio.Task[None]] = []
        self._running = False
        self._entered = False
        self._heartbeat_interval = heartbeat_interval
        self._upkeep: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._running

    async def __aenter__(self) -> Worker:
        if self._entered:
            raise RuntimeError(
                "this worker pool has already been used; "
                "call queue.worker() to create a fresh one"
            )
        self._entered = True
        self._running = True

        # Register before any loop can claim, and await it.
        await self._backend.heartbeat()

        for _ in range(self.concurrency):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)
        self._upkeep = asyncio.create_task(self._upkeep_loop())
        logger.info("worker pool started (concurrency=%d)", self.concurrency)

        return self

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        self._running = False

        for worker in self.workers:
            worker.cancel()

        await asyncio.gather(*self.workers, return_exceptions=True)
        await self._stop_upkeep()
        logger.info("worker pool stopped")

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                record = await self._backend.claim()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "failed to claim a job from the backend; retrying in %ss",
                    _CLAIM_ERROR_BACKOFF_SECONDS,
                )
                await asyncio.sleep(_CLAIM_ERROR_BACKOFF_SECONDS)
                continue

            if record is None:
                continue

            try:
                await self._process(record)
            except asyncio.CancelledError:
                # Interrupted before a terminal write (shutdown/cancel): hand the
                # lease back so the job is redelivered, never stranded in RUNNING.
                # Guarded, because the release can legitimately fail.
                try:
                    await asyncio.shield(self._backend.release(record["id"]))
                    logger.info(
                        "job %s released on shutdown; will be redelivered",
                        record["id"],
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning(
                        "job %s could not be released on shutdown; "
                        "leaving it to the reaper",
                        record["id"],
                        exc_info=True,
                    )
                raise
            except Exception:
                # A poison record (failed deserialize/save) must not kill the
                # worker and silently shrink the pool. Log and keep serving.
                logger.exception("unexpected error while processing job; continuing")
                continue

    async def _process(self, record: dict[str, Any]) -> None:
        job = Job.from_record(record, self._serializer)
        logger.debug("processing job %s (task=%s)", job.id, job.task_name)
        if job.request_cancel:
            job.status = JobStatus.CANCELLED
            logger.debug("job %s cancelled", job.id)
            await self._save(job)
            return

        task = self._task_registry.get(job.task_name)
        if task is None:
            job.error = f"no task is registered under the name {job.task_name!r}"
            job.status = JobStatus.FAILED
            logger.warning(
                "job %s failed: no task registered under name %r",
                job.id,
                job.task_name,
            )
            await self._save(job)
            return

        cancel_task = asyncio.create_task(self._backend.wait_cancel(job.id))
        job_task = asyncio.create_task(task(*job.args, **job.kwargs))
        pending: set[asyncio.Task[None] | asyncio.Task[Any]] = {job_task, cancel_task}
        try:
            done, pending = await asyncio.wait(
                [cancel_task, job_task], return_when=asyncio.FIRST_COMPLETED
            )

            if (
                cancel_task in done
                and job_task not in done
                and cancel_task.exception() is not None
            ):
                logger.warning(
                    "job %s: cancel watcher failed (%r); running unwatched",
                    job.id,
                    cancel_task.exception(),
                )
                await job_task
                done = {job_task}

            if job_task in done:
                job.result = job_task.result()
                job.status = JobStatus.COMPLETED
                logger.debug("job %s completed", job.id)
            else:
                job.status = JobStatus.CANCELLED
                logger.debug("job %s cancelled", job.id)
            await self._save(job)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            job.error = str(e)
            job.status = JobStatus.FAILED
            logger.debug("job %s failed: %s", job.id, e)
            await self._save(job)
        finally:
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def _save(self, job: Job) -> None:
        # Terminal write: persist outcome and wake result() waiters.
        await self._backend.save(job.id, job.to_record(self._serializer), done=True)

    async def drain(self, timeout: float | None = None) -> bool:
        """Stop claiming new jobs and wait for in-flight ones to finish.

        Returns True if the pool wound down with nothing left running, False if
        the deadline passed and in-flight jobs were cancelled for redelivery.
        """
        self._running = False
        if not self.workers:
            return True

        _, pending = await asyncio.wait(self.workers, timeout=timeout)
        if pending:
            logger.warning(
                "drain timed out after %ss; cancelling %d in-flight job(s)",
                timeout,
                len(pending),
            )
            for worker in pending:
                worker.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            return False

        logger.info("worker pool drained")
        return True

    async def _upkeep_loop(self) -> None:
        """Publish this process's liveness and reclaim leases of dead ones."""
        while True:
            try:
                await self._backend.heartbeat()
                await self._backend.reap()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker upkeep failed; continuing")
            await asyncio.sleep(self._heartbeat_interval)

    async def _stop_upkeep(self) -> None:
        upkeep, self._upkeep = self._upkeep, None
        if upkeep is None:
            return
        upkeep.cancel()
        await asyncio.gather(upkeep, return_exceptions=True)
