######################
# The API end to end #
######################
import asyncio

from TaskQueue import Queue
from TaskQueue.backends.memory import MemoryBackend

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
