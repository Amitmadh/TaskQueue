######################
# The API end to end #
######################
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
