from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import logging
import pkgutil
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from TaskQueue import __version__, backends, serializers
from TaskQueue.exceptions import ConfigError
from TaskQueue.logger import setup_logging
from TaskQueue.queue import Queue
from TaskQueue.worker import DEFAULT_HEARTBEAT_INTERVAL_SECONDS

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_BAD_TARGET = 1
EXIT_BAD_CONFIG = 3

_BACKEND_SUFFIX = "_backend"
_SERIALIZER_SUFFIX = "_serializer"


class TargetError(Exception):
    """The '<module>:<attr>' target could not be resolved to a 'Queue'."""


def available(package_path: Iterable[str], suffix: str) -> list[str]:
    """Implementation names a package ships, read from filenames."""
    return sorted(
        module.name.removesuffix(suffix)
        for module in pkgutil.iter_modules(package_path)
        if module.name.endswith(suffix) and not module.ispkg
    )


def resolve_queue(spec: str) -> Queue:
    """Import 'spec' ('<module>:<attr>') and return its Queue.

    Importing the module and runs the '@q.task' decorators.
    """
    module_name, _, queue_name = spec.partition(":")
    if not (module_name and queue_name):
        raise TargetError(f"invalid target {spec!r}: expected '<module>:<queue>'")

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise TargetError(f"cannot import {module_name!r}: {exc}.") from exc

    try:
        target: Any = getattr(module, queue_name)
    except AttributeError:
        raise TargetError(f"{module_name!r} has no attribute {queue_name!r}") from None
    if not isinstance(target, Queue):
        raise TargetError(
            f"{module_name}:{queue_name} is a {type(target).__name__}, expected a Queue"
        )
    return target


async def run_worker(
    queue: Queue,
    concurrency: int,
    drain_timeout: float | None = None,
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> None:
    """Serve jobs until SIGINT/SIGTERM, then drain the pool.

    Shutdown is two-stage. The first signal stops claiming and waits for
    in-flight jobs to finish. A second signal — or 'drain_timeout' elapsing —
    cancels them, and a job cancelled mid-run is 'release'd back to QUEUED for
    redelivery. That fallback is correct at-least-once behaviour but discards
    in-flight progress, which is exactly what the drain exists to avoid paying
    for on an ordinary restart.
    """
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        # Windows has no add_signal_handler for SIGTERM; there Ctrl-C arrives as
        # KeyboardInterrupt out of asyncio.run instead, which main() handles.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    # Printed so a producer/worker task-name mismatch is one glance away: the
    # names here are the exact strings this process can resolve a job back to.
    names = sorted(queue.task_registry)
    if names:
        logger.info("serving %d task(s): %s", len(names), ", ".join(names))
    else:
        logger.warning("no tasks are registered on this queue; every job will fail")

    async with queue.worker(
        concurrency=concurrency, heartbeat_interval=heartbeat_interval
    ) as workers:
        logger.info("worker ready (concurrency=%d); ctrl-c to stop", concurrency)
        await stop.wait()

        stop.clear()
        logger.info(
            "signal received; draining (timeout=%ss, ctrl-c again to cancel)",
            drain_timeout,
        )

        drain_task = asyncio.create_task(workers.drain(drain_timeout))
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            (drain_task, stop_task), return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending)

        if drain_task not in done:
            logger.warning("second signal; cancelling in-flight jobs for redelivery")
        elif drain_task.result():
            logger.info("drain complete; stopping worker pool")
        else:
            logger.warning("drain deadline passed; in-flight jobs will be redelivered")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskqueue", description="Run and inspect TaskQueue workers."
    )
    parser.add_argument(
        "--version", action="version", version=f"TaskQueue {__version__}"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser(
        "worker", help="run a worker pool against the Queue named by <module>:<attr>"
    )
    worker.add_argument(
        "target",
        help="import string naming a Queue, e.g. 'myapp.tasks:q'. Both halves "
        "are required.",
    )
    worker.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=1,
        help="jobs served in parallel by this process (default: 1)",
    )
    worker.add_argument(
        "--drain-timeout",
        type=float,
        default=30.0,
        help="seconds to let in-flight jobs finish on shutdown before "
        "cancelling them for redelivery (default: 30)",
    )
    worker.add_argument(
        "--heartbeat-interval",
        type=float,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="seconds between liveness beats; must be at most a third of the "
        f"backend's worker_ttl (default: {DEFAULT_HEARTBEAT_INTERVAL_SECONDS:g})",
    )
    worker.add_argument(
        "-l",
        "--log-level",
        default="info",
        choices=("debug", "info", "warning", "error", "critical"),
        help="(default: info)",
    )

    sub.add_parser("backends", help="list registered backend names")
    sub.add_parser("serializers", help="list registered serializer names")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "backends":
        print("\n".join(available(backends.__path__, _BACKEND_SUFFIX)))
        return EXIT_OK
    if args.command == "serializers":
        print("\n".join(available(serializers.__path__, _SERIALIZER_SUFFIX)))
        return EXIT_OK

    setup_logging(args.log_level.upper())

    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        queue = resolve_queue(args.target)
    except TargetError as exc:
        print(f"taskqueue: {exc}", file=sys.stderr)
        return EXIT_BAD_TARGET

    try:
        asyncio.run(
            run_worker(
                queue,
                args.concurrency,
                args.drain_timeout,
                args.heartbeat_interval,
            )
        )
    except ConfigError as exc:
        print(f"taskqueue: {exc}", file=sys.stderr)
        return EXIT_BAD_CONFIG
    except KeyboardInterrupt:
        logger.info("interrupted; worker pool stopped")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
