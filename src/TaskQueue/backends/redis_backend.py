import asyncio
import contextlib
import logging
from typing import Any, cast
from uuid import uuid4

import redis.asyncio as redis

from TaskQueue.backends.interface import Backend
from TaskQueue.job import JobStatus

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
_NOTIFY_POLL_SECONDS = 5.0


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


def _flatten(record: dict[str, str | bytes]) -> list[str | bytes]:
    """A record as an alternating field/value list, for HSET's varargs."""
    flattened: list[str | bytes] = []
    for field, value in record.items():
        flattened.extend([field, value])
    return flattened


_TERMINAL_LUA = (
    "{" + ", ".join(f"{status} = true" for status in sorted(_TERMINAL)) + "}"
)

_ENQUEUE_SCRIPT = """
local job = KEYS[1]
local queue = KEYS[2]
local job_id = ARGV[1]

if redis.call('EXISTS', job) == 1 then
    return 0
end

redis.call('HSET', job, unpack(ARGV, 2))
redis.call('RPUSH', queue, job_id)

return 1
"""

_CLAIM_SCRIPT = """
local job = KEYS[1]
local running = ARGV[1]

if redis.call('EXISTS', job) == 0 then
    return nil
end

redis.call('HSET', job, 'status', running)
redis.call('HINCRBY', job, 'attempts', 1)

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

_DEREGISTER_SCRIPT = """
local workers = KEYS[1]
local processing = KEYS[2]
local worker = ARGV[1]

-- The liveness entry is the only pointer to this worker's processing list.
-- Dropping it while leases remain would hide them from later reapers.
if redis.call('LLEN', processing) > 0 then
    return 0
end

return redis.call('ZREM', workers, worker)
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


async def _wait_for_message(woken: asyncio.Event, timeout: float) -> None:
    """Wait for the next message on a watched channel, or for the poll to lapse."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(woken.wait(), timeout)
    woken.clear()


class _Notifier:
    """One pubsub connection shared by every waiter on this backend.
    Lives only while somebody is waiting.
    """

    def __init__(self, client: redis.Redis) -> None:
        self._redis = client
        self._lock = asyncio.Lock()
        self._waiters: dict[str, set[asyncio.Event]] = {}
        self._pubsub: Any = None
        self._reader: asyncio.Task[None] | None = None

    async def join(self, channel: str) -> asyncio.Event:
        """Subscribe to 'channel'; the event is set on every message it carries."""
        woken = asyncio.Event()
        async with self._lock:
            if self._pubsub is None:
                self._pubsub = self._redis.pubsub()  # pyright: ignore[reportUnknownMemberType]
            watchers = self._waiters.setdefault(channel, set())
            watchers.add(woken)
            if len(watchers) == 1:
                try:
                    await self._pubsub.subscribe(channel)
                except BaseException:
                    self._forget(channel, woken)
                    raise
            if self._reader is None:
                self._reader = asyncio.create_task(self._read())
        return woken

    def _forget(self, channel: str, woken: asyncio.Event) -> None:
        """Drop one watcher, and the channel with it once it has none."""
        watchers = self._waiters.get(channel)
        if watchers is None:
            return
        watchers.discard(woken)
        if not watchers:
            del self._waiters[channel]

    async def leave(self, channel: str, woken: asyncio.Event) -> None:
        """Release a watcher, completing even if its caller is being cancelled."""

        async def release() -> None:
            async with self._lock:
                self._forget(channel, woken)
                if channel in self._waiters:
                    return
                if self._waiters:
                    with contextlib.suppress(Exception):
                        await self._pubsub.unsubscribe(channel)
                    return
                reader, self._reader = self._reader, None
                pubsub, self._pubsub = self._pubsub, None
            if reader is not None:
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await reader
            with contextlib.suppress(Exception):
                await pubsub.aclose()

        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.shield(release())

    async def _read(self) -> None:
        while (pubsub := self._pubsub) is not None:
            try:
                message = cast(
                    "dict[str, Any] | None",
                    await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("pubsub reader stopped", exc_info=True)
                return
            if message is None:
                continue
            raw = cast("bytes | str", message["channel"])
            for event in self._waiters.get(
                raw.decode() if isinstance(raw, bytes) else raw, ()
            ):
                event.set()


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
            raise ValueError(f"result_ttl (={result_ttl}) must be greater than 0")
        if worker_ttl <= 0:
            raise ValueError(f"worker_ttl (={worker_ttl}) must be greater than 0")
        self._result_ttl: int = result_ttl
        self._worker_ttl: int = worker_ttl
        self._enqueue_script = redis_client.register_script(_ENQUEUE_SCRIPT)
        self._claim_script = redis_client.register_script(_CLAIM_SCRIPT)
        self._save_script = redis_client.register_script(_SAVE_SCRIPT)
        self._release_script = redis_client.register_script(_RELEASE_SCRIPT)
        self._request_cancel_script = redis_client.register_script(
            _REQUEST_CANCEL_SCRIPT
        )
        self._heartbeat_script = redis_client.register_script(_HEARTBEAT_SCRIPT)
        self._reaper_script = redis_client.register_script(_REAPER_SCRIPT)
        self._deregister_script = redis_client.register_script(_DEREGISTER_SCRIPT)

        self._processing_key: str = processing_key(self._instance_id)
        self._notifier = _Notifier(redis_client)

    @property
    def result_ttl(self) -> int:
        """Seconds a terminal record survives if nobody calls 'take_result'."""
        return self._result_ttl

    @property
    def worker_ttl(self) -> int:
        """Seconds without a heartbeat after which a worker is presumed dead."""
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
        created = await self._enqueue_script(
            keys=[job_key(job_id), QUEUE], args=[job_id, *_flatten(record)]
        )
        if created == 0:
            logger.debug("enqueue of job %s ignored: already known", job_id)
            return
        logger.debug("enqueued job %s", job_id)

    async def claim(self) -> dict[str, str | bytes] | None:
        raw_job_id = await self.redis.blmove(
            QUEUE, self._processing_key, _CLAIM_TIMEOUT_SECONDS
        )
        if raw_job_id is None:
            await asyncio.sleep(0)
            return None

        job_id = raw_job_id.decode() if isinstance(raw_job_id, bytes) else raw_job_id
        try:
            flat = await self._claim_script(
                keys=[job_key(job_id)], args=[JobStatus.RUNNING.value]
            )
        except Exception:
            # The lease is already taken.
            logger.warning("claim failed for job %s; returning it to the queue", job_id)
            with contextlib.suppress(Exception):
                async with self.redis.pipeline(transaction=True) as pipe:
                    pipe.lrem(self._processing_key, 1, job_id)
                    pipe.rpush(QUEUE, job_id)
                    await pipe.execute()
            raise
        if not flat:
            await self.redis.lrem(self._processing_key, 1, job_id)
            logger.warning(
                "discarded queued job %s: its record no longer exists", job_id
            )
            return None

        record = self._decode(dict(zip(flat[::2], flat[1::2], strict=True)))
        logger.debug("claimed job %s (attempt %s)", job_id, record.get("attempts"))
        return record

    async def get_job(self, job_id: str) -> dict[str, str | bytes]:
        raw_record = await self.redis.hgetall(job_key(job_id))
        if not raw_record:
            raise KeyError(f"job {job_id!r} not found")
        return self._decode(raw_record)

    async def save(
        self, job_id: str, record: dict[str, str | bytes], *, done: bool = False
    ) -> None:
        return_value = await self._save_script(
            keys=[job_key(job_id), self._processing_key, done_channel(job_id)],
            args=["1" if done else "0", job_id, self._result_ttl, *_flatten(record)],
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
        # Subscribe before the first read, or a publish landing in between is
        # missed. The stored record is authoritative and the message only says
        # that something changed, so every wake re-reads the record.
        channel = done_channel(job_id)
        woken = await self._notifier.join(channel)
        try:
            while not await self._is_terminal(job_id):
                await _wait_for_message(woken, _NOTIFY_POLL_SECONDS)
        finally:
            await self._notifier.leave(channel, woken)

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
        channel = cancel_channel(job_id)
        woken = await self._notifier.join(channel)
        try:
            flag = await self.redis.hget(job_key(job_id), "request_cancel")
            if flag is None:
                raise KeyError(f"job {job_id!r} not found")
            if int(flag):
                return
            while not await self._cancel_requested(job_id):
                await _wait_for_message(woken, _NOTIFY_POLL_SECONDS)
        finally:
            await self._notifier.leave(channel, woken)

    async def _is_terminal(self, job_id: str) -> bool:
        """Read the job's status. Raises 'KeyError' if the record is gone."""
        raw = await self.redis.hget(job_key(job_id), "status")
        if raw is None:
            raise KeyError(f"job {job_id!r} not found")
        status = raw.decode() if isinstance(raw, bytes) else raw
        return status in _TERMINAL

    async def _cancel_requested(self, job_id: str) -> bool:
        """Read the durable cancel flag. A vanished record reports False."""
        raw = await self.redis.hget(job_key(job_id), "request_cancel")
        return raw is not None and bool(int(raw))

    async def heartbeat(self) -> None:
        await self._heartbeat_script(keys=[WORKERS], args=[self._instance_id])

    async def deregister(self) -> None:
        withdrawn = await self._deregister_script(
            keys=[WORKERS, self._processing_key], args=[self._instance_id]
        )
        if withdrawn:
            logger.debug("worker %s withdrawn from the liveness set", self._instance_id)
        else:
            logger.info("worker %s still holds leases", self._instance_id)

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
