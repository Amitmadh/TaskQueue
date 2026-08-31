from __future__ import annotations

import asyncio
import logging
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from TaskQueue.exceptions import JobCancelled

if TYPE_CHECKING:
    from types import TracebackType

    from TaskQueue.backends.interface import Backend
    from TaskQueue.handle import JobHandle
    from TaskQueue.task import Task

logger = logging.getLogger(__name__)


_CANCEL_DRAIN_TIMEOUT_SECONDS = 30.0


class OnError(StrEnum):
    CANCEL_SIBLINGS = "cancel_siblings"
    IGNORE = "ignore"
    COLLECT = "collect"


class JobGroup:
    def __init__(
        self,
        backend: Backend,
        on_error: OnError | str = OnError.CANCEL_SIBLINGS,
        deadline: float | None = None,
    ) -> None:
        self.id: str = uuid4().hex
        self._backend: Backend = backend
        self._on_error: OnError = OnError(on_error)
        self._deadline: float | None = deadline
        self._deadline_at: float | None = None
        self._handles: dict[str, JobHandle[Any]] = {}
        self._waiters: dict[asyncio.Task[Any], JobHandle[Any]] = {}
        self._failures: list[BaseException] = []
        self._entered: bool = False
        self._cancelling: bool = False
        self._fanout: asyncio.Task[None] | None = None

    async def __aenter__(self) -> JobGroup:
        self._entered = True
        if self._deadline is not None:
            self._deadline_at = asyncio.get_running_loop().time() + self._deadline
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # Local tasks now outlive every path through this method -- the
        # per-child waiters, and possibly an in-flight cancel fan-out -- so the
        # net that settles them wraps the whole body. Attached to the join
        # alone, the two early returns below would step straight past it.
        try:
            if exc_val is not None:
                logger.debug(
                    "group %s: scope body raised; cancelling all children", self.id
                )
                await self.cancel_all_jobs()
                return

            if not self._handles:
                return

            try:
                if self._deadline_at is not None:
                    async with asyncio.timeout_at(self._deadline_at):
                        failures = await self._join()
                else:
                    failures = await self._join()
            except TimeoutError:
                logger.debug(
                    "group %s: deadline exceeded; cancelling all children", self.id
                )
                await self.cancel_all_jobs()
                raise
            except asyncio.CancelledError:
                logger.debug("group %s: cancelled; cancelling all children", self.id)
                await self.cancel_all_jobs()
                raise

            if failures:
                logger.debug(
                    "group %s: %d of %d job(s) failed; raising ExceptionGroup",
                    self.id,
                    len(failures),
                    len(self._handles),
                )
                raise BaseExceptionGroup(
                    f"one or more jobs in group {self.id} failed", failures
                )
            logger.debug(
                "group %s: all %d job(s) finished", self.id, len(self._handles)
            )
        finally:
            await self._settle()

    async def _join(self) -> list[BaseException]:
        if self._on_error is OnError.IGNORE:
            await self.ignore_mode()
            return []
        if self._on_error is OnError.COLLECT:
            return await self.collect_mode()
        return await self.cancel_siblings_mode()

    def _job_finished(self, waiter: asyncio.Task[Any]) -> None:
        if waiter.cancelled():
            return
        failure = waiter.exception()
        if failure is None:
            return  # it finished fine

        if self._cancelling and isinstance(failure, JobCancelled):
            return

        self._failures.append(failure)

        if self._cancelling:
            return
        self._cancelling = True
        still_running = [h.job_id for w, h in self._waiters.items() if not w.done()]
        self._fanout = asyncio.create_task(
            self._backend.request_cancel_many(still_running)
        )

    async def _settle(self) -> None:
        """Leave no local task behind, whichever way the scope ended.

        The fan-out belongs here too: it is scheduled from a callback that
        cannot await it, so this is the only place that knows it finished.
        """
        for waiter in self._waiters:
            if not waiter.done():
                waiter.cancel()
        pending: list[asyncio.Task[Any]] = list(self._waiters)
        if self._fanout is not None:
            pending.append(self._fanout)
        await asyncio.gather(*pending, return_exceptions=True)

    async def spawn[**P, R](
        self, task: Task[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> JobHandle[R]:
        handle = await task.submit(self.id if self._entered else None, *args, **kwargs)
        self._handles[handle.job_id] = handle
        if self._entered:
            waiter = asyncio.create_task(handle.result())
            self._waiters[waiter] = handle
            if self._on_error is OnError.CANCEL_SIBLINGS:
                waiter.add_done_callback(self._job_finished)
        logger.debug("group %s: spawned job %s", self.id, handle.job_id)
        return handle

    async def cancel_all_jobs(self) -> None:
        # Request cancellation on every job in one batch call, then wait for each
        # to reach a terminal state. Errors are swallowed: this is teardown.
        handles = list(self._handles.values())
        await self._backend.request_cancel_many([h.job_id for h in handles])
        if not self._waiters:
            return
        _, not_cancelled = await asyncio.wait(
            self._waiters, timeout=_CANCEL_DRAIN_TIMEOUT_SECONDS
        )
        if not_cancelled:
            logger.warning(
                "group %s: %d child(ren) did not report back within %gs; "
                "abandoning the wait",
                self.id,
                len(not_cancelled),
                _CANCEL_DRAIN_TIMEOUT_SECONDS,
            )

    async def cancel_siblings_mode(self) -> list[BaseException]:
        # Wait until the first child fails (or all finish). On a failure, cancel the
        # still-running siblings — the JOBS, via their handles, not just the local
        # result() waiters — and drain them to terminal so none outlive the scope.
        _, pending = await asyncio.wait(
            self._waiters, return_when=asyncio.FIRST_EXCEPTION
        )
        if pending:
            # The watcher fans out the instant a child fails, so by now it has
            # usually happened already. This is the backstop for a failure that
            # lands exactly as we arrive.
            if not self._cancelling:
                self._cancelling = True
                logger.debug(
                    "group %s: a child failed; cancelling %d sibling(s)",
                    self.id,
                    len(pending),
                )
                await self._backend.request_cancel_many(
                    [self._waiters[task].job_id for task in pending]
                )
            await asyncio.gather(*pending, return_exceptions=True)
        return self._failures

    async def ignore_mode(self) -> None:
        results = await asyncio.gather(
            *self._waiters,
            return_exceptions=True,
        )
        ignored = sum(1 for r in results if isinstance(r, BaseException))
        if ignored:
            logger.debug("group %s: ignored %d failure(s)", self.id, ignored)

    async def collect_mode(self) -> list[BaseException]:
        results = await asyncio.gather(
            *self._waiters,
            return_exceptions=True,
        )
        return [res for res in results if isinstance(res, BaseException)]
