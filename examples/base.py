######################
# The API end to end #
######################
import asyncio
import logging

from TaskQueue import Queue
from TaskQueue.backends.memory import MemoryBackend

logging.basicConfig(level=logging.WARNING)
tq_logger = logging.getLogger("TaskQueue")
tq_logger.setLevel(logging.DEBUG)
tq_logger.addHandler(logging.StreamHandler())
tq_logger.propagate = False  # stop bubbling up to root


q = Queue(backend=MemoryBackend())


@q.task
async def add(x: int, y: int) -> int:
    return x + y


async def main() -> None:
    async with q.worker(concurrency=2):
        handle = await q.root_group().spawn(add, 2, 3)
        result = await handle.result()
        print(result)


asyncio.run(main())
