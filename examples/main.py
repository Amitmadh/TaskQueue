import asyncio
import logging
from pathlib import Path

import redis.asyncio as redis

from TaskQueue import Queue
from TaskQueue.backends.redis_backend import RedisBackend

EXAMPLES_DIR = Path(__file__).resolve().parent
TARGET = f"{Path(__file__).stem}:q"

client = redis.Redis(host="localhost", port=6379)

q = Queue(
    backend=RedisBackend(client),
    namespace="crash_demo",
)


@q.task
async def check_inventory(item: str) -> str:
    print(f"Checking inventory for {item}...")
    await asyncio.sleep(2)
    print("Inventory OK")
    return f"{item} available"


@q.task
async def validate_payment(amount: float) -> str:
    print(f"Validating payment of ${amount:.2f}...")
    await asyncio.sleep(2)
    if amount > 0:
        print("Payment approved")
        return "payment approved"
    raise RuntimeError("Card declined")


@q.task
async def calculate_shipping(address: str) -> str:
    print(f"Calculating shipping to {address}...")
    await asyncio.sleep(2)
    print("Shipping calculated")
    return "shipping calculated"


@q.task
async def create_order(
    inventory: str,
    payment: str,
    shipping: str,
) -> str:
    print("Creating order...")
    await asyncio.sleep(0.5)
    return "ORDER-1234"


async def checkout() -> None:
    try:
        async with q.group() as group:
            inventory = await group.spawn(check_inventory, "Laptop")
            payment = await group.spawn(validate_payment, 1299.99)
            shipping = await group.spawn(calculate_shipping, "Tel Aviv")

        # All three have completed successfully here.
        order = await create_order.submit(
            await inventory.result(),
            await payment.result(),
            await shipping.result(),
        )

        print(f"\nCheckout complete: {order.result()}")

    except* Exception as exc:
        print(f"\nCheckout failed: {exc}")


async def main() -> None:
    await client.flushdb()  # type: ignore
    asyncio.run(checkout())


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
