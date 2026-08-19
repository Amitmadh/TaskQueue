from __future__ import annotations

import asyncio
import logging
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import TracebackType

    from TaskQueue.backends.interface import Backend
    from TaskQueue.handle import JobHandle
    from TaskQueue.task import Task

logger = logging.getLogger(__name__)


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
        self._handles: dict[str, JobHandle[Any]] = {}
        self._on_error: OnError = OnError(on_error)
        self._deadline: float | None = deadline
        self._deadline_at: float | None = None

    async def __aenter__(self) -> JobGroup:
        if self._deadline is not None:
            self._deadline_at = asyncio.get_running_loop().time() + self._deadline
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_val is not None:
            logger.debug(
                "group %s: scope body raised; cancelling all children", self.id
            )
            await self.cancel_all_jobs(self._handles.values())
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
            await self.cancel_all_jobs(self._handles.values())
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
        logger.debug("group %s: all %d job(s) finished", self.id, len(self._handles))

    async def _join(self) -> list[BaseException]:
        if self._on_error is OnError.IGNORE:
            await self.ignore_mode()
            return []
        if self._on_error is OnError.COLLECT:
            return await self.collect_mode()
        return await self.cancel_siblings_mode()

    async def spawn[**P, R](
        self, task: Task[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> JobHandle[R]:
        handle = await task.submit(*args, **kwargs)
        self._handles[handle.job_id] = handle
        logger.debug("group %s: spawned job %s", self.id, handle.job_id)
        return handle

    async def cancel_all_jobs(self, handles: Iterable[JobHandle[Any]]) -> None:
        # Request cancellation on every job in one batch call, then wait for each
        # to reach a terminal state. Errors are swallowed: this is teardown.
        handles = list(handles)
        await self._backend.request_cancel_many([h.job_id for h in handles])
        await asyncio.gather(
            *(handle.result() for handle in handles), return_exceptions=True
        )

    async def cancel_siblings_mode(self) -> list[BaseException]:
        # Wait until the first child fails (or all finish). On a failure, cancel the
        # still-running siblings — the JOBS, via their handles, not just the local
        # result() waiters — and drain them to terminal so none outlive the scope.
        waiters = {
            asyncio.create_task(handle.result()): handle
            for handle in self._handles.values()
        }
        try:
            done, pending = await asyncio.wait(
                waiters, return_when=asyncio.FIRST_EXCEPTION
            )
            if pending:
                logger.debug(
                    "group %s: a child failed; cancelling %d sibling(s)",
                    self.id,
                    len(pending),
                )
                await self._backend.request_cancel_many(
                    [waiters[task].job_id for task in pending]
                )
                await asyncio.gather(*pending, return_exceptions=True)
            return [exception for t in done if (exception := t.exception()) is not None]
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def ignore_mode(self) -> None:
        results = await asyncio.gather(
            *(handle.result() for handle in self._handles.values()),
            return_exceptions=True,
        )
        ignored = sum(1 for r in results if isinstance(r, BaseException))
        if ignored:
            logger.debug("group %s: ignored %d failure(s)", self.id, ignored)

    async def collect_mode(self) -> list[BaseException]:
        results = await asyncio.gather(
            *(h.result() for h in self._handles.values()),
            return_exceptions=True,
        )
        return [res for res in results if isinstance(res, BaseException)]
