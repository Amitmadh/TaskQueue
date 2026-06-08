######################
# The API end to end #
######################

import asyncio

from TaskQueue import Queue
from TaskQueue.backends.memory import MemoryBackend

q = Queue(backend=MemoryBackend())


@q.task
async def foo() -> str:
    await asyncio.sleep(5)
    return "finished waiting!"


@q.task
async def goo() -> str:
    for i in range(1000000):
        i = i
    return "finished waiting!"
