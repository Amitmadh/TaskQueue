######################
# The API end to end #
######################
import asyncio
import logging

import redis.asyncio as redis

from TaskQueue import Queue
from TaskQueue.backends.redis_backend import RedisBackend

logging.basicConfig(level=logging.DEBUG)
tq_logger = logging.getLogger("TaskQueue")
tq_logger.setLevel(logging.DEBUG)
tq_logger.addHandler(logging.StreamHandler())
tq_logger.propagate = False  # stop bubbling up to root


client = redis.Redis(host="localhost", port=6379)

q = Queue(backend=RedisBackend(client), namespace="stockroom")


@q.task
async def units_in_stock(on_hand: int, incoming: int) -> int:
    return on_hand + incoming


async def main() -> None:
    async with q.root_group(deadline=0.3) as g:
        await g.spawn(units_in_stock, 4, "twelve")


if __name__ == "__main__":
    asyncio.run(main())
