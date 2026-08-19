import logging
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import redis.asyncio as redis

from TaskQueue.backends.interface import Backend
from TaskQueue.job import JobStatus

if TYPE_CHECKING:
    from collections.abc import Mapping

    from redis.typing import EncodableT, FieldT

logger = logging.getLogger(__name__)


class RedisBackend(Backend):
    _BLOBS = {"payload", "result"}

    def __init__(self, redis_client: redis.Redis) -> None:
        self.redis: redis.Redis = redis_client
        self._pubsub = self.redis.pubsub()  # pyright: ignore[reportUnknownMemberType]
        self._instance_id = uuid4().hex

    def _decode(self, raw: dict[bytes | str, bytes | str]) -> dict[str, str | bytes]:
        """Normalize an HGETALL reply (bytes->bytes, or str->str if the client
        was built with decode_responses=True) into a record: str keys,
        str envelope values, blob values left as bytes for the serializer."""
        return {
            k: (v if k in self._BLOBS else (v.decode() if isinstance(v, bytes) else v))
            for kb, v in raw.items()
            if (k := kb.decode() if isinstance(kb, bytes) else kb)
        }

    async def enqueue(self, job_id: str, record: dict[str, str | bytes]) -> None:
        # self._jobs[job_id] = record
        # self._events[job_id] = asyncio.Event()
        # self._cancels[job_id] = asyncio.Event()
        # record["status"] = JobStatus.QUEUED.value
        # await self._queue.put(job_id)
        # logger.debug("enqueued job %s", job_id)
        record["status"] = JobStatus.QUEUED.value
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                f"jobs:{job_id}", mapping=cast("Mapping[FieldT, EncodableT]", record)
            )
            pipe.rpush("queue", job_id)
            await pipe.execute()
        logger.debug("enqueued job %s", job_id)

    async def claim(self) -> dict[str, Any]:
        raw_job_id = await self.redis.blmove("queue", "processing", 0)
        if raw_job_id is None:
            raise RuntimeError("blmove returned no job despite blocking indefinitely")
        job_id = raw_job_id.decode() if isinstance(raw_job_id, bytes) else raw_job_id
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(f"jobs:{job_id}", "status", JobStatus.RUNNING.value)
            pipe.hgetall(f"jobs:{job_id}")
            _, raw_record = await pipe.execute()
        logger.debug("claimed job %s", job_id)
        return self._decode(raw_record)

    async def get_job(self, job_id: str) -> dict[str, Any]:
        raw_record = await self.redis.hgetall(f"jobs:{job_id}")
        if not raw_record:
            raise KeyError(f"job {job_id!r} not found")
        return self._decode(raw_record)

    async def save(
        self, job_id: str, record: dict[str, Any], *, done: bool = False
    ) -> None:
        # if job_id not in self._jobs:
        #     raise KeyError(f"job {job_id!r} not found")
        # self._jobs[job_id] = record
        # logger.debug("saved job %s (done=%s)", job_id, done)
        # # Persist every transition (status stays observable), but only wake
        # # result() waiters once the job has reached a terminal state.
        # if done:
        #     # Terminal state reached: wake any result() waiters, then drop the
        #     # per-job synchronization primitives — neither the done-event nor the
        #     # cancel-event is ever needed again. The record is kept as the result.
        #     event = self._events.pop(job_id, None)
        #     if event is not None:
        #         event.set()
        #     self._cancels.pop(job_id, None)
        raise NotImplementedError("RedisBackend.save is not implemented yet")

    async def release(self, job_id: str) -> None:
        # record = self._jobs.get(job_id)
        # if record is None:
        #     raise KeyError(f"job {job_id!r} not found")
        # # Only an in-flight (RUNNING) lease is redeliverable; ignore otherwise so
        # # a double release (e.g. graceful shutdown racing a future reaper) is a
        # # harmless no-op rather than re-queuing a finished job.
        # if record["status"] == JobStatus.RUNNING.value:
        #     record["status"] = JobStatus.QUEUED.value
        #     await self._queue.put(job_id)
        #     logger.debug("released job %s for redelivery", job_id)
        raise NotImplementedError("RedisBackend.release is not implemented yet")

    async def take_result(self, job_id: str) -> dict[str, Any]:
        # event = self._events.get(job_id)
        # if event is not None:
        #     await event.wait()
        # # Terminal now (or already was): return the record and free it entirely.
        # self._events.pop(job_id, None)
        # self._cancels.pop(job_id, None)
        # record = self._jobs.pop(job_id, None)
        # if record is None:
        #     raise RuntimeError(f"cannot take result of unknown job {job_id!r}")
        # return dict(record)
        raise NotImplementedError("RedisBackend.take_result is not implemented yet")

    async def request_cancel(self, job_id: str) -> None:
        # record = self._jobs.get(job_id)
        # if record is None:
        #     return
        # if record["status"] in (
        #     JobStatus.COMPLETED.value,
        #     JobStatus.FAILED.value,
        #     JobStatus.CANCELLED.value,
        # ):
        #     return
        # record["request_cancel"] = True
        # logger.debug("cancel requested for job %s", job_id)
        # cancel = self._cancels.get(job_id)
        # if cancel is not None:
        #     cancel.set()
        raise NotImplementedError("RedisBackend.request_cancel is not implemented yet")

    async def request_cancel_many(self, job_ids: list[str]) -> None:
        # for job_id in job_ids:
        #     await self.request_cancel(job_id)
        raise NotImplementedError(
            "RedisBackend.request_cancel_many is not implemented yet"
        )

    async def wait_cancel(self, job_id: str) -> None:
        # cancel = self._cancels.get(job_id)
        # if cancel is None:
        #     raise RuntimeError(
        #         f"cannot watch cancellation for unknown job {job_id!r}"
        #     )
        # await cancel.wait()
        raise NotImplementedError("RedisBackend.wait_cancel is not implemented yet")
