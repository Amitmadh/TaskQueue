"""Cancel a job from here; it stops mid-run in a worker process over there.

Two acts. The first is the ordinary path: `handle.cancel()` writes a flag and
publishes, the worker's watcher wakes, and the job stops. The second removes the
message — the flag is set by hand and nothing is published, which is what a
dropped `PUBLISH` looks like from the worker's side — and the job still stops, one
poll interval later, because the waiting side re-reads the record instead of
trusting the message to arrive.

Self-contained: the worker subprocess imports THIS module (see TARGET), so the
task registry and namespace used here cannot leak into the other examples.

Run a real Redis first:  docker run --rm -p 6379:6379 redis
Then:                    python examples/cancel_crosses_processes.py
"""

# redis-py annotates most client methods with '**kwargs: Unknown', so under a
# strict type checker every single call to them is a partially-unknown type.
# That is the library's typing, not this file's: switch the rule off here rather
# than hang an ignore comment on each line (src/ does the latter, per call).
# pyright: reportUnknownMemberType=false

import asyncio
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import cast

import redis.asyncio as redis

from TaskQueue import JobCancelled, JobHandle, JobStatus, Queue
from TaskQueue.backends.redis_backend import (
    RedisBackend,
    cancel_channel,
    job_key,
    processing_key,
)

EXAMPLES_DIR = Path(__file__).resolve().parent
TARGET = f"{Path(__file__).stem}:q"

# How long the job would run if nobody stopped it. Long enough that "it stopped
# early" is unmistakable in the timeline below.
JOB_SECONDS = 300

# Held by name as well as by the queue: 'Queue.backend' is typed as the Backend
# protocol, which has no 'redis' attribute, and this file reads the raw keys that
# protocol deliberately hides.
client = redis.Redis(host="localhost", port=6379)

q = Queue(
    backend=RedisBackend(client),
    namespace="cancel_demo",
)


@q.task
async def slow_job(seconds: float) -> str:
    """Runs in the WORKER process. Its prints are the proof the work stopped."""
    say(f"worker  | job started in pid {os.getpid()}; sleeping {seconds:g}s")
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        # A record that SAYS cancelled is a weaker claim than work that stopped.
        # This line is the strong one, and it is printed from the other process.
        say(f"worker  | job CANCELLED mid-run in pid {os.getpid()}; unwinding")
        raise
    return "finished (nobody stopped me)"


# Both processes stamp their lines against the same epoch, so the interleaved
# output reads as one timeline. Wall clock rather than 'monotonic': only the
# former is guaranteed to mean the same thing in a child process.
_EPOCH_ENV = "TQ_DEMO_EPOCH"
T0 = float(os.environ.get(_EPOCH_ENV) or time.time())


def say(message: str) -> None:
    print(f"[{time.time() - T0:5.1f}s] {message}", flush=True)


def spawn_worker() -> "subprocess.Popen[bytes]":
    """Start a worker as a direct child of this process.

    Deliberately NOT the 'taskqueue' console script: on Windows that is a
    launcher .exe that runs the interpreter as its own child, so a handle on it
    is a handle on the stub rather than on the worker. '-m TaskQueue' makes the
    worker the process we actually hold.
    """
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "TaskQueue",
            "worker",
            TARGET,
            "--log-level",
            "warning",
        ],
        cwd=EXAMPLES_DIR,
        env={**os.environ, _EPOCH_ENV: str(T0)},
    )


async def until_running(handle: JobHandle[str]) -> None:
    """Block until the job is leased and marked RUNNING by a worker's claim."""
    while await handle.status() is not JobStatus.RUNNING:
        await asyncio.sleep(0.05)


async def until_watching(handle: JobHandle[str]) -> None:
    """Block until the worker's cancel subscription exists on the server.

    'wait_cancel' subscribes, reads 'request_cancel' ONCE, and only then parks on
    the message. The job body starts running during that subscribe round trip, so
    a flag set the instant the job starts can be answered by that one-shot read —
    act two would then prove nothing about the poll. 'PUBSUB NUMSUB' is the
    server confirming the subscription is live; the short settle after it covers
    the single await between 'subscribe' returning and the flag being read.
    """
    channel = cancel_channel(handle.job_id)
    while True:
        subscribers = cast(
            "list[tuple[bytes, int]]", await client.pubsub_numsub(channel)
        )
        if subscribers[0][1]:
            break
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.2)


async def until_cancelled(handle: JobHandle[str]) -> float:
    """Block until the worker writes the terminal status. Returns seconds waited.

    Timed here rather than around 'result()': this is the moment the cancel
    actually landed in the other process, without the round trip that hands the
    record back.
    """
    started = time.time()
    while await handle.status() is not JobStatus.CANCELLED:
        await asyncio.sleep(0.05)
    return time.time() - started


async def report(handle: JobHandle[str], worker_id: str | None) -> None:
    """The backend's own bookkeeping, read before 'result()' frees the record."""
    record = cast("dict[bytes, bytes]", await client.hgetall(job_key(handle.job_id)))
    status = record.get(b"status", b"(gone)").decode()
    attempts = record.get(b"attempts", b"?").decode()
    leases = (
        await client.llen(processing_key(worker_id)) if worker_id is not None else 0
    )
    say(f"redis   | status={status} attempts={attempts} leases_held={leases}")


async def current_worker_id() -> str | None:
    """The one worker's id, read off the heartbeat set."""
    workers = cast("list[bytes]", await client.zrange("workers", 0, -1))
    return workers[0].decode() if workers else None


async def act_one() -> None:
    say("--- act 1: handle.cancel() ------------------------------------------")
    # Detached on purpose. Inside 'async with q.group()' the scope's __aexit__
    # joins its children, and a child that ends CANCELLED is a scope failure —
    # correct behaviour, but it would raise a BaseExceptionGroup here and bury
    # the one thing this act is about.
    handle = await q.root_group().spawn(slow_job, JOB_SECONDS)
    say(f"producer| spawned job {handle.job_id[:8]} (would run for {JOB_SECONDS}s)")

    await until_running(handle)
    worker_id = await current_worker_id()
    say(f"producer| it is RUNNING, leased by worker {(worker_id or '?')[:8]}")
    await until_watching(handle)

    say("producer| calling handle.cancel() -- flag written AND published")
    await handle.cancel()

    waited = await until_cancelled(handle)
    say(f"producer| the record went terminal {waited:.1f}s after the cancel")
    await report(handle, worker_id)

    try:
        await handle.result()
    except JobCancelled:
        say("producer| result() raised JobCancelled, and freed the record")
    else:  # pragma: no cover - only if cancellation regressed
        say("producer| !! result() returned normally; the cancel did not land")


async def act_two() -> None:
    say("--- act 2: the same thing, with the message thrown away -------------")
    handle = await q.root_group().spawn(slow_job, JOB_SECONDS)
    say(f"producer| spawned job {handle.job_id[:8]}")

    await until_running(handle)
    worker_id = await current_worker_id()
    await until_watching(handle)
    say(
        f"producer| worker {(worker_id or '?')[:8]} is subscribed to its cancel channel"
    )

    # 'request_cancel' would set this flag and PUBLISH in one script. Setting the
    # field by hand does the first half and skips the second: the record is
    # correct, and the worker is never told. Nothing will wake it but its own
    # re-read of the record.
    await client.hset(job_key(handle.job_id), "request_cancel", "1")
    say("producer| flag set directly -- NOTHING published; the worker is parked")
    say("producer| waiting for it to notice on its own (up to one poll interval)")

    waited = await until_cancelled(handle)
    say(f"producer| the worker noticed on its own after {waited:.1f}s -- the poll")
    await report(handle, worker_id)

    try:
        await handle.result()
    except JobCancelled:
        say("producer| result() raised JobCancelled, exactly as in act 1")
    else:  # pragma: no cover - only if the poll regressed
        say("producer| !! result() returned normally; the poll did not fire")


async def main() -> None:
    await client.flushdb()

    worker = spawn_worker()
    say(f"producer| spawned worker (pid {worker.pid}); waiting for it to claim")
    try:
        await act_one()
        print()
        await act_two()
        print()
        say("both jobs stopped in the worker process -- once by message, once by poll")
    finally:
        worker.kill()
        worker.wait()
        say("producer| worker stopped")
        await client.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
