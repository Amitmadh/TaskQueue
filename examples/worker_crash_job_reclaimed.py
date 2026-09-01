"""A worker dies mid-job; a surviving worker reaps its lease and finishes the job.

The store's packing line: two orders are being packed when the machine doing one
of them dies. Nobody re-submits anything -- a peer notices the stopped
heartbeat, takes the lease back, and the order is packed by the other worker.

Self-contained: the worker subprocesses import THIS module (see TARGET), so the
fast worker_ttl used here cannot leak into the other examples.

Two processes, so two logging controls: 'basicConfig' at the bottom sets the
level for the narration this file prints, and WORKER_LOG_LEVEL sets it for the
workers, which are separate processes and inherit nothing from it.

Run a real Redis first:  docker run --rm -p 6379:6379 redis
Then:                    uv run python examples/worker_crash_job_reclaimed.py

This file spawns its own workers with ``--heartbeat-interval 1``. Pass it too if
you run one by hand -- the short ``worker_ttl`` below rejects the CLI's 10s
default, because a beat has to fit into the TTL three times over.
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
from typing import NamedTuple, cast

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

# worker_ttl=4 with --heartbeat-interval 1 keeps the demo short: a dead worker
# is noticed ~5s after it stops beating instead of the 30s default.
# check_liveness() enforces worker_ttl >= 3 * heartbeat_interval, so this is
# one second off the floor.
HEARTBEAT_INTERVAL = 1
WORKER_TTL = 4

# Passed on the workers' command line: they are separate processes, so nothing
# this file does to its own root logger reaches them. Note the CLI applies this
# to the ROOT logger in the worker, so 'debug' also turns on redis-py's and
# asyncio's own chatter, not just TaskQueue's.
# 'warning', not 'info': at 'info' two worker processes narrate their own
# startup over this file's story, on a wall clock while everything else here is
# relative seconds. The one line worth keeping - the reaper announcing the
# requeue - is replaced by the monitor below, which sees the same event from
# outside and reports it on this demo's clock.
WORKER_LOG_LEVEL = "warning"

# Long enough that both orders are still being packed when worker A is killed,
# short enough that re-running the reclaimed one does not outlast a recording.
PACK_SECONDS = 3

# How often the monitor reads the bookkeeping. It prints only when something
# changed, so polling faster than the heartbeat buys precision, not noise.
POLL_SECONDS = 0.4

# A beat between the startup banner and the first narrated line. It sits before
# the clock starts, so no time this demo reports includes it. It exists because
# the first four narrated lines land within milliseconds of each other: a screen
# recorder told to start on the first of them has already missed the kill by the
# time it reacts, so it needs something quieter to cut in on.
SETTLE_SECONDS = 1.0

# Held by name as well as by the queue: 'Queue.backend' is typed as the Backend
# protocol, which has no 'redis' attribute, and the monitor below reads the raw
# keys that protocol deliberately hides.
client = redis.Redis(host="localhost", port=6379)

q = Queue(
    backend=RedisBackend(client, worker_ttl=WORKER_TTL),
    namespace="fulfillment",
)


@q.task
async def pack_order(order_id: str, seconds: float) -> str:
    await asyncio.sleep(seconds)
    return f"{order_id} packed in {seconds:g}s"


t0 = time.monotonic()


def say(message: str) -> None:
    print(f"[{time.monotonic() - t0:5.1f}s] >>> {message}", flush=True)


# Redis knows workers and orders only by their ids, and both are hex strings of
# the same shape. The whole story here is which worker's lease moved where, and
# that is unreadable as four indistinguishable ids, so everything printed below
# goes through these.
WORKER_LABELS: dict[str, str] = {}
ORDER_LABELS: dict[str, str] = {}


def worker_label(worker_id: str) -> str:
    return WORKER_LABELS.get(worker_id, worker_id[:6])


def order_label(job_id: str) -> str:
    return ORDER_LABELS.get(job_id, job_id[:6])


def as_fields(values: dict[str, int]) -> str:
    """Render a mapping sorted by key.

    'keys' returns matches in no particular order, so an unsorted dict can
    reorder itself between two readings of identical state - which reads as a
    change, and counts as one to the monitor's change detection.
    """
    body = ", ".join(f"{key}: {value}" for key, value in sorted(values.items()))
    return "{" + body + "}"


async def register_worker(label: str, known: set[str]) -> str:
    """Wait for one more worker to register a heartbeat, and name it.

    A worker generates its own instance id and never tells this process what it
    is. Starting them one at a time and seeing which id appears is the only way
    to say 'A' and mean a specific one of them - and without that, a lease
    moving from one worker to the other is just hex.
    """
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        members = {
            member.decode()
            for member in cast("list[bytes]", await client.zrange(WORKERS, 0, -1))
        }
        if len(new := members - known) == 1:
            worker_id = new.pop()
            WORKER_LABELS[worker_id] = label
            known.add(worker_id)
            return worker_id
        await asyncio.sleep(0.05)
    raise RuntimeError(f"worker {label} never registered a heartbeat")


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
            WORKER_LOG_LEVEL,
            "--heartbeat-interval",
            str(HEARTBEAT_INTERVAL),
        ],
        cwd=EXAMPLES_DIR,
    )


class Bookkeeping(NamedTuple):
    """One reading of the raw keys: the line to print, and who was alive in it."""

    line: str
    workers: frozenset[str]


async def snapshot() -> Bookkeeping:
    """One reading of the backend's bookkeeping, straight from the raw keys.

    Every reply type below is a union covering both client configurations
    ('decode_responses' on or off); the casts pin them to what this client,
    built without it, actually returns.
    """
    now, _microseconds = await client.time()

    beats = cast(
        "list[tuple[bytes, float]]",
        await client.zrange(WORKERS, 0, -1, withscores=True),
    )

    def render(worker_id: str, score: float) -> str:
        # A worker beating on schedule is just its label. One that has gone
        # quiet carries how long for, so the TTL running out is something you
        # watch happen rather than infer from the moment the line changes.
        age = now - int(score)
        label = worker_label(worker_id)
        return label if age <= HEARTBEAT_INTERVAL else f"{label}(silent {age}s)"

    workers = ", ".join(
        sorted(render(worker.decode(), score) for worker, score in beats)
    )

    leases: dict[str, int] = {}
    for key in cast("list[bytes]", await client.keys(f"{PROCESSING}:*")):
        holder = key.decode().removeprefix(f"{PROCESSING}:")
        leases[worker_label(holder)] = await client.llen(key)

    # 'attempts' is bumped by the claim, so a redelivered order reads 2.
    attempts: dict[str, int] = {}
    for key in cast("list[bytes]", await client.keys(f"{JOBS}:*")):
        raw = await client.hget(key, "attempts")
        if raw is not None:
            attempts[order_label(key.decode().removeprefix(f"{JOBS}:"))] = int(raw)

    line = (
        f"queued={await client.llen(QUEUE)}  leases={as_fields(leases)}  "
        f"attempts={as_fields(attempts)}  workers=[{workers}]"
    )
    return Bookkeeping(line, frozenset(worker.decode() for worker, _score in beats))


async def holders() -> str:
    """Who is holding which order, read from the per-worker processing lists."""
    held: list[str] = []
    for key in cast("list[bytes]", await client.keys(f"{PROCESSING}:*")):
        worker_id = key.decode().removeprefix(f"{PROCESSING}:")
        job_ids = cast("list[bytes]", await client.lrange(key, 0, -1))
        held.extend(
            f"{worker_label(worker_id)} holds {order_label(job_id.decode())}"
            for job_id in job_ids
        )
    return ", ".join(sorted(held))


async def monitor(stop: asyncio.Event) -> None:
    """Report the bookkeeping, but only when it actually changes.

    A ticker that reprints an unchanged line is noise, and most of this line is
    unchanged most of the time: what matters is a handful of transitions.
    Dropping out of the heartbeat set is what being reaped looks like from
    outside the worker, so it is called out here, on this demo's clock, rather
    than left to a log line from inside a process that is about to be killed.
    """
    previous_line = ""
    previous_workers: frozenset[str] = frozenset()
    while not stop.is_set():
        reading = await snapshot()
        for worker_id in sorted(previous_workers - reading.workers):
            say(
                f"REAPED - {worker_label(worker_id)}'s heartbeat expired; "
                "its lease went back on the queue"
            )
        if reading.line != previous_line:
            say(f"redis | {reading.line}")
        previous_line = reading.line
        previous_workers = reading.workers
        await asyncio.sleep(POLL_SECONDS)


async def kill_when_both_running(
    proc: "subprocess.Popen[bytes]",
    worker_id: str,
    handles: Sequence[JobHandle[str]],
) -> None:
    """Wait until both orders are actually leased, then SIGKILL one worker."""
    while True:
        statuses = [await handle.status() for handle in handles]
        if all(status is JobStatus.RUNNING for status in statuses):
            break
        await asyncio.sleep(0.1)
    # The 'before' frame: without it the first thing anyone sees of the leases
    # is the state they were left in by the kill.
    say(f"both orders leased - {await holders()}")
    proc.kill()
    proc.wait()
    say(
        f"KILLED worker {worker_label(worker_id)} (pid {proc.pid}) mid-order - "
        "its lease is now orphaned"
    )


async def main() -> None:
    global t0

    await client.flushdb()

    # One at a time, so each new heartbeat can be matched to the process that
    # was just started; see register_worker. The cost is two interpreter
    # startups back to back, which is several seconds on a cold cache, so this
    # says what it is waiting for instead of sitting on a blank screen. The two
    # lines are deliberately un-timestamped: the clock starts after them.
    known: set[str] = set()
    print("starting worker A ...", flush=True)
    worker_a = spawn_worker()
    worker_a_id = await register_worker("A", known)
    print(f"worker A is up ({worker_a_id[:6]}); starting worker B ...", flush=True)
    worker_b = spawn_worker()
    worker_b_id = await register_worker("B", known)
    print(f"worker B is up ({worker_b_id[:6]})", flush=True)
    await asyncio.sleep(SETTLE_SECONDS)

    # Both are up: start the clock at the part worth watching rather than at
    # two interpreter startups.
    t0 = time.monotonic()
    say(
        f"worker A = {worker_a_id[:6]} (pid {worker_a.pid}), "
        f"worker B = {worker_b_id[:6]} (pid {worker_b.pid})"
    )

    stop = asyncio.Event()
    watcher = asyncio.create_task(monitor(stop))
    killer: asyncio.Task[None] | None = None
    try:
        async with q.group() as g:
            handles: list[JobHandle[str]] = [
                await g.spawn(pack_order, f"ORDER-{1234 + i}", PACK_SECONDS)
                for i in range(2)
            ]
            for index, handle in enumerate(handles, start=1):
                ORDER_LABELS[handle.job_id] = f"order-{index}"
            say(f"packing two orders: {', '.join(ORDER_LABELS.values())}")
            # The scope's __aexit__ joins both children, so the kill has to be
            # scheduled from inside the body - it fires while the join blocks.
            killer = asyncio.create_task(
                kill_when_both_running(worker_a, worker_a_id, handles)
            )
        say("group scope exited: every child reached a terminal state")

        for handle in handles:
            say(f"{order_label(handle.job_id)}: {await handle.result()}")
        say("nothing was re-submitted by hand, and no order was lost")
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
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
