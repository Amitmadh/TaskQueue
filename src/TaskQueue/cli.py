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
from TaskQueue.logger import setup_logging
from TaskQueue.queue import Queue

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_BAD_TARGET = 1

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


async def run_worker(queue: Queue, concurrency: int) -> None:
    """Serve jobs until SIGINT/SIGTERM, then stop the pool.

    Shutdown today is *cancel*, not drain: leaving the ``async with`` cancels
    the worker loops, and a job interrupted mid-run is `release`d back to
    QUEUED for redelivery. Correct at-least-once behaviour, but it discards
    in-flight progress, so a rolling restart re-runs whatever was running. A
    graceful drain (stop claiming, await in-flight, cancel on a deadline) needs
    `claim` to yield periodically — it currently blocks indefinitely — so it
    lands together with the short-timeout BLMOVE loop in the Redis backend.
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

    async with queue.worker(concurrency=concurrency):
        logger.info("worker ready (concurrency=%d); ctrl-c to stop", concurrency)
        await stop.wait()
        logger.info("signal received; stopping worker pool")


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

    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        queue = resolve_queue(args.target)
    except TargetError as exc:
        print(f"taskqueue: {exc}", file=sys.stderr)
        return EXIT_BAD_TARGET

    setup_logging(args.log_level.upper())

    try:
        asyncio.run(run_worker(queue, args.concurrency))
    except KeyboardInterrupt:
        logger.info("interrupted; worker pool stopped")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
