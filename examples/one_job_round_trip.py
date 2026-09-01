"""One job, start to finish: define a task, spawn it, await a typed result.

The smallest complete program the library allows. `q.worker()` runs the pool in
this process, `root_group()` is the explicit escape hatch from the rule that
every job is joined by an enclosing scope, and `handle.result()` comes back
typed as `int` rather than as `Any`.

TaskQueue's own logger is turned up to DEBUG and given its own handler, so the
output is the machinery and not just the answer. `propagate = False` stops the
root logger printing every line a second time.

`namespace="stockroom"` fixes the task name to `stockroom.units_in_stock`. A
module run as a script has no stable import path, so without a namespace the
decorator refuses to guess and raises `TaskNameError`.

Run from the repo root:

    uv run python examples/one_job_round_trip.py
"""

import asyncio
import logging

from TaskQueue import MemoryBackend, Queue

logging.basicConfig(level=logging.DEBUG)
tq_logger = logging.getLogger("TaskQueue")
tq_logger.setLevel(logging.DEBUG)
tq_logger.addHandler(logging.StreamHandler())
tq_logger.propagate = False  # stop bubbling up to root


q = Queue(backend=MemoryBackend(), namespace="stockroom")


@q.task
async def units_in_stock(on_hand: int, incoming: int) -> int:
    return on_hand + incoming


async def main() -> None:
    async with q.worker(concurrency=2):
        handle = await q.root_group().spawn(units_in_stock, 4, 11)
        result = await handle.result()
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
