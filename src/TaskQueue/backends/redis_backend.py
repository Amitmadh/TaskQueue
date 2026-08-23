import asyncio
import logging
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import redis.asyncio as redis

from TaskQueue.backends.interface import Backend
from TaskQueue.job import JobStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from redis.typing import EncodableT, FieldT

logger = logging.getLogger(__name__)

_TERMINAL = {
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.CANCELLED.value,
}

QUEUE = "queue"
JOBS = "jobs"
PROCESSING = "processing"
WORKERS = "workers"

DEFAULT_RESULT_TTL_SECONDS = 86400
DEFAULT_WORKER_TTL_SECONDS = 30
_CLAIM_TIMEOUT_SECONDS = 1


def job_key(job_id: str) -> str:
    """Hash holding one job's record."""
    return f"{JOBS}:{job_id}"


def processing_key(consumer_id: str) -> str:
    """List of the leases one worker process currently holds."""
    return f"{PROCESSING}:{consumer_id}"


def done_channel(job_id: str) -> str:
    """Channel a terminal 'save(done=True)' publishes to, waking 'take_result'."""
    return f"done:{job_id}"


def cancel_channel(job_id: str) -> str:
    """Channel a cancel request publishes to, waking 'wait_cancel'."""
    return f"cancel:{job_id}"


_TERMINAL_LUA = (
    "{" + ", ".join(f"{status} = true" for status in sorted(_TERMINAL)) + "}"
)

_CLAIM_SCRIPT = """
local job = KEYS[1]
local running = ARGV[1]

if redis.call('EXISTS', job) == 0 then
    return nil
end

redis.call('HSET', job, 'status', running)

return redis.call('HGETALL', job)
"""

_SAVE_SCRIPT = """
local job = KEYS[1]
local processing = KEYS[2]
local channel = KEYS[3]
local done = ARGV[1]
local job_id = ARGV[2]
local result_ttl = ARGV[3]

if redis.call('EXISTS', job) == 0 then
    return 0
end

redis.call('HSET', job, unpack(ARGV, 4))

if done == '1' then
    redis.call('LREM', processing, 1, job_id)
    redis.call('EXPIRE', job, result_ttl)
    redis.call('PUBLISH', channel, '')
end

return 1
"""

_RELEASE_SCRIPT = """
local job = KEYS[1]
local processing = KEYS[2]
local queue = KEYS[3]
local job_id = ARGV[1]
local running = ARGV[2]
local queued = ARGV[3]

if redis.call('EXISTS', job) == 0 then
    return -1
end

if redis.call('HGET', job, 'status') ~= running then
    return 0
end

redis.call('HSET', job, 'status', queued)
redis.call('LREM', processing, 1, job_id)
redis.call('RPUSH', queue, job_id)

return 1
"""

_REQUEST_CANCEL_SCRIPT = f"""
local job = KEYS[1]
local channel = KEYS[2]
local terminals = {_TERMINAL_LUA}

if redis.call('EXISTS', job) == 0 then
    return 0
end

if terminals[redis.call('HGET', job, 'status')] then
    return 0
end

redis.call('HSET', job, 'request_cancel', '1')
redis.call('PUBLISH', channel, '')

return 1
"""

_HEARTBEAT_SCRIPT = """
local now =  tonumber(redis.call('TIME')[1])
redis.call('ZADD', KEYS[1], now, ARGV[1])
return 1
"""

_REAPER_SCRIPT = """
local workers = KEYS[1]
local queue = KEYS[2]
local ttl = tonumber(ARGV[1])
local queued = ARGV[2]
local running = ARGV[3]
local processing_prefix = ARGV[4]
local job_prefix = ARGV[5]

local now = tonumber(redis.call('TIME')[1])
local expired = redis.call('ZRANGE', workers, '-inf', now - ttl, 'BYSCORE')
local reclaimed = 0
local dropped = 0

for _, worker in ipairs(expired) do
    local processing = processing_prefix .. worker
    while true do
        local job_id = redis.call('LPOP', processing)
        if not job_id then
            break
        end
        local job = job_prefix .. job_id
        if redis.call('EXISTS', job) == 1 then
            if redis.call('HGET', job, 'status') == running then
                redis.call('HSET', job, 'status', queued)
                redis.call('HINCRBY', job, 'attempts', 1)
            end
            redis.call('RPUSH', queue, job_id)
            reclaimed = reclaimed + 1
        else
            dropped = dropped + 1
        end
    end
    redis.call('ZREM', workers, worker)
end

return {reclaimed, dropped}
"""


class RedisBackend(Backend):
    _BLOBS = {"payload", "result"}

    def __init__(
        self,
        redis_client: redis.Redis,
        result_ttl: int = DEFAULT_RESULT_TTL_SECONDS,
        worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS,
    ) -> None:
        self.redis: redis.Redis = redis_client
        self._instance_id: str = uuid4().hex
        if result_ttl <= 0:
            raise ValueError(f"result_ttl (={result_ttl}) need to be greater than 0")
        if worker_ttl <= 0:
            raise ValueError(f"worker_ttl (={worker_ttl}) need to be greater than 0")
        self._result_ttl: int = result_ttl
        self._worker_ttl: int = worker_ttl
        self._claim_script = redis_client.register_script(_CLAIM_SCRIPT)
        self._save_script = redis_client.register_script(_SAVE_SCRIPT)
        self._release_script = redis_client.register_script(_RELEASE_SCRIPT)
        self._request_cancel_script = redis_client.register_script(
            _REQUEST_CANCEL_SCRIPT
        )
        self._heartbeat_script = redis_client.register_script(_HEARTBEAT_SCRIPT)
        self._reaper_script = redis_client.register_script(_REAPER_SCRIPT)

        self._processing_key: str = processing_key(self._instance_id)

    @property
    def result_ttl(self) -> int:
        """Seconds a terminal record survives if nobody calls 'take_result'."""
        return self._result_ttl

    @property
    def worker_ttl(self) -> int:
        """Seconds without a heartbeat after which a worker is presumed dead.

        Must be comfortably larger than the pool's heartbeat interval — three
        missed beats is the usual margin — or a live worker that pauses for a
        garbage collection or a network blip has its leases reclaimed under it.
        """
        return self._worker_ttl

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
        record["status"] = JobStatus.QUEUED.value
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                job_key(job_id), mapping=cast("Mapping[FieldT, EncodableT]", record)
            )
            pipe.rpush(QUEUE, job_id)
            await pipe.execute()
        logger.debug("enqueued job %s", job_id)

    async def claim(self) -> dict[str, str | bytes] | None:
        raw_job_id = await self.redis.blmove(
            QUEUE, self._processing_key, _CLAIM_TIMEOUT_SECONDS
        )
        if raw_job_id is None:
            await asyncio.sleep(0)
            return None

        job_id = raw_job_id.decode() if isinstance(raw_job_id, bytes) else raw_job_id
        flat = await self._claim_script(
            keys=[job_key(job_id)], args=[JobStatus.RUNNING.value]
        )
        if not flat:
            await self.redis.lrem(self._processing_key, 1, job_id)
            logger.warning(
                "discarded queued job %s: its record no longer exists", job_id
            )
            return None

        logger.debug("claimed job %s", job_id)
        return self._decode(dict(zip(flat[::2], flat[1::2], strict=True)))

    async def get_job(self, job_id: str) -> dict[str, str | bytes]:
        raw_record = await self.redis.hgetall(job_key(job_id))
        if not raw_record:
            raise KeyError(f"job {job_id!r} not found")
        return self._decode(raw_record)

    async def save(
        self, job_id: str, record: dict[str, str | bytes], *, done: bool = False
    ) -> None:
        flattened_record: list[str | bytes] = []
        for field, value in record.items():
            flattened_record.extend([field, value])

        return_value = await self._save_script(
            keys=[job_key(job_id), self._processing_key, done_channel(job_id)],
            args=["1" if done else "0", job_id, self._result_ttl, *flattened_record],
        )
        if return_value == 0:
            raise KeyError(f"job {job_id!r} not found")
        logger.debug("saved job %s (done=%s)", job_id, done)

    async def release(self, job_id: str) -> None:
        return_value = await self._release_script(
            keys=[job_key(job_id), self._processing_key, QUEUE],
            args=[job_id, JobStatus.RUNNING.value, JobStatus.QUEUED.value],
        )
        if return_value == -1:
            raise KeyError(f"job {job_id!r} not found")
        if return_value == 1:
            logger.debug("released job %s for redelivery", job_id)

    async def take_result(self, job_id: str) -> dict[str, str | bytes]:
        key = job_key(job_id)
        # wait for job to reach a terminal status
        async with self.redis.pubsub() as pubsub:  # pyright: ignore[reportUnknownMemberType]
            await pubsub.subscribe(done_channel(job_id))
            record = await self.get_job(job_id=job_id)
            if record["status"] not in _TERMINAL:
                messages = cast(
                    "AsyncIterator[dict[str, Any]]",
                    pubsub.listen(),  # pyright: ignore[reportUnknownMemberType]
                )
                async for message in messages:
                    if message["type"] == "message":
                        break

        # return the record and remove from queue
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hgetall(key)
            pipe.delete(key)
            raw_record, _ = await pipe.execute()
        if not raw_record:
            raise KeyError(f"job {job_id!r} not found")
        logger.debug("took result of job %s", job_id)
        return self._decode(raw_record)

    async def request_cancel(self, job_id: str) -> None:
        return_value = await self._request_cancel_script(
            keys=[job_key(job_id), cancel_channel(job_id)],
        )
        if return_value == 1:
            logger.debug("cancel requested for job %s", job_id)

    async def request_cancel_many(self, job_ids: list[str]) -> None:
        if not job_ids:
            return
        async with self.redis.pipeline(transaction=False) as pipe:
            for job_id in job_ids:
                await self._request_cancel_script(
                    keys=[job_key(job_id), cancel_channel(job_id)], client=pipe
                )
            await pipe.execute()

    async def wait_cancel(self, job_id: str) -> None:
        async with self.redis.pubsub() as pubsub:  # pyright: ignore[reportUnknownMemberType]
            await pubsub.subscribe(cancel_channel(job_id))
            flag = await self.redis.hget(job_key(job_id), "request_cancel")
            if flag is None:
                raise KeyError(f"job {job_id!r} not found")
            if int(flag):
                return
            messages = cast(
                "AsyncIterator[dict[str, Any]]",
                pubsub.listen(),  # pyright: ignore[reportUnknownMemberType]
            )
            async for message in messages:
                if message["type"] == "message":
                    return

    async def heartbeat(self) -> None:
        await self._heartbeat_script(keys=[WORKERS], args=[self._instance_id])

    async def reap(self) -> int:
        reclaimed, dropped = await self._reaper_script(
            keys=[WORKERS, QUEUE],
            args=[
                self._worker_ttl,
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                f"{PROCESSING}:",
                f"{JOBS}:",
            ],
        )
        if reclaimed or dropped:
            logger.info(
                "reaped a dead worker: %d job(s) requeued, %d discarded",
                reclaimed,
                dropped,
            )
        return int(reclaimed)
