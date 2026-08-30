"""A worker dies mid-job; a surviving worker reaps its lease and finishes the job.

Self-contained: the worker subprocesses import THIS module (see TARGET), so the
fast worker_ttl used here cannot leak into the other examples.

Run a real Redis first:  docker run --rm -p 6379:6379 redis
"""

# redis-py annotates most client methods with '**kwargs: Unknown', so under a
# strict type checker every single call to them is a partially-unknown type.
# That is the library's typing, not this file's: switch the rule off here rather
# than hang an ignore comment on each line (src/ does the latter, per call).
# pyright: reportUnknownMemberType=false

import asyncio
import logging
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import redis.asyncio as redis

from TaskQueue import JobHandle, JobStatus, Queue
from TaskQueue.backends.redis_backend import (
    JOBS,
    PROCESSING,
    QUEUE,
    WORKERS,
    RedisBackend,
)

EXAMPLES_DIR = Path(__file__).resolve().parent
TARGET = f"{Path(__file__).stem}:q"

# worker_ttl=6 with --heartbeat-interval 2 keeps the demo short: a dead worker
# is noticed ~6-8s after it stops beating instead of the 30s default.
# check_liveness() enforces worker_ttl >= 3 * heartbeat_interval.
HEARTBEAT_INTERVAL = 2
WORKER_TTL = 6

# Held by name as well as by the queue: 'Queue.backend' is typed as the Backend
# protocol, which has no 'redis' attribute, and the monitor below reads the raw
# keys that protocol deliberately hides.
client = redis.Redis(host="localhost", port=6379)

q = Queue(
    backend=RedisBackend(client, worker_ttl=WORKER_TTL),
    namespace="crash_demo",
)


@q.task
async def crunch(seconds: float) -> str:
    await asyncio.sleep(seconds)
    return f"crunched for {seconds}s"


T0 = time.monotonic()


def say(message: str) -> None:
    print(f"[{time.monotonic() - T0:5.1f}s] >>> {message}", flush=True)


def spawn_worker() -> "subprocess.Popen[bytes]":
    """Start a worker as a direct child of this process.

    Deliberately NOT the 'taskqueue' console script: on Windows that is a
    launcher .exe that runs the interpreter as its own child, so Popen.kill()
    would kill the stub and leave the real worker alive - still heartbeating,
    so never reaped, and this demo would hang forever. '-m TaskQueue'
    makes the worker the process we actually hold a handle to.
    """
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "TaskQueue",
            "worker",
            TARGET,
            "--log-level",
            "debug",
            "--heartbeat-interval",
            str(HEARTBEAT_INTERVAL),
        ],
        cwd=EXAMPLES_DIR,
    )


async def snapshot() -> str:
    """One line of the backend's bookkeeping, read straight from the raw keys.

    Every reply type below is a union covering both client configurations
    ('decode_responses' on or off); the casts pin them to what this client,
    built without it, actually returns.
    """
    now, _microseconds = await client.time()

    beats = cast(
        "list[tuple[bytes, float]]",
        await client.zrange(WORKERS, 0, -1, withscores=True),
    )
    workers = ", ".join(
        f"{worker.decode()[:6]}(beat {now - int(score)}s ago)"
        for worker, score in beats
    )

    lease_keys = cast("list[bytes]", await client.keys(f"{PROCESSING}:*"))
    leases = {
        key.decode().removeprefix(f"{PROCESSING}:")[:6]: await client.llen(key)
        for key in lease_keys
    }

    # 'attempts' is bumped by the claim, so a redelivered job reads 2.
    job_keys = cast("list[bytes]", await client.keys(f"{JOBS}:*"))
    attempts = {
        key.decode().removeprefix(f"{JOBS}:")[:6]: int(raw)
        for key in job_keys
        if (raw := await client.hget(key, "attempts")) is not None
    }

    return (
        f"queued={await client.llen(QUEUE)} leases={leases} "
        f"attempts={attempts} workers=[{workers}]"
    )


async def monitor(stop: asyncio.Event) -> None:
    """Print the bookkeeping every 2s so the reclaim is visible, not inferred."""
    while not stop.is_set():
        say(f"redis | {await snapshot()}")
        await asyncio.sleep(2)


async def kill_when_both_running(
    proc: "subprocess.Popen[bytes]", handles: Sequence[JobHandle[str]]
) -> None:
    """Wait until both jobs are actually leased, then SIGKILL one worker."""
    while True:
        statuses = [await handle.status() for handle in handles]
        if all(status is JobStatus.RUNNING for status in statuses):
            break
        await asyncio.sleep(0.1)
    proc.kill()
    proc.wait()
    say(f"KILLED worker A (pid {proc.pid}) mid-job - its lease is now orphaned")


async def main() -> None:
    await client.flushdb()

    worker_a = spawn_worker()
    worker_b = spawn_worker()
    say(f"spawned worker A (pid {worker_a.pid}) and worker B (pid {worker_b.pid})")

    stop = asyncio.Event()
    watcher = asyncio.create_task(monitor(stop))
    killer: asyncio.Task[None] | None = None
    try:
        async with q.group() as g:
            handles: list[JobHandle[str]] = [
                await g.spawn(crunch, 10) for _ in range(2)
            ]
            say(f"spawned jobs {[handle.job_id[:8] for handle in handles]}")
            # The scope's __aexit__ joins both children, so the kill has to be
            # scheduled from inside the body - it fires while the join blocks.
            killer = asyncio.create_task(kill_when_both_running(worker_a, handles))
        say("group scope exited: every child reached a terminal state")

        for handle in handles:
            say(f"result: {await handle.result()}")
    finally:
        stop.set()
        for task in (killer, watcher):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        for worker in (worker_a, worker_b):
            worker.kill()
            worker.wait()
        say("workers stopped")
        await client.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
