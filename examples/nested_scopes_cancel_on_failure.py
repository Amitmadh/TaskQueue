"""A checkout, run twice: once it goes through, once the card is declined.

The Redis twin of ``examples/scope_semantics_tour.py``. Same scope code, same
task decorator; the only difference is the backend on line one of the setup,
which is what the ``Backend`` Protocol is for. The worker pool runs in
this process for the sake of a single command; point
``taskqueue worker nested_scopes_cancel_on_failure:q`` at the same Redis from
another terminal and the producer code below does not change at all.

What each run is here to show:

  run 1  three independent checks fan out into one scope and run concurrently,
         3s of wall clock for 6.5s of work. The scope will not exit until all
         three are terminal, so the dependent step that needs their results
         cannot start early.

  run 2  the payment fails half a second in. The scope cancels the two checks
         that are still running instead of letting them finish, raises the
         failure where the caller can catch it, and never reaches the order at
         all. Watch the wall clock: the run ends in well under a second, not the
         3s the doomed work would have taken.

``check_inventory`` is a task that is itself a producer: it opens its own scope
and spawns one job per warehouse. The same ``async with`` works whether the
code around it is your script or a job already running on a worker, and that
nesting is what run 2 exercises. The cancellation does not stop at the job it
was aimed at: it goes through ``check_inventory`` into the warehouse jobs *it*
spawned, and each one unwinds.

**A pool running nested tasks has to be wide enough to hold the parent and its
children at once.** A parent occupies a worker slot for as long as it waits, so
with ``concurrency=1`` this file deadlocks outright: the parent holds the only
slot and no child can ever be claimed. Two is enough to finish; the peak here is
three outer checks plus three warehouse jobs, so the pool is six.

``namespace="checkout"`` fixes the task names to ``checkout.*`` whether this file
is run as a script or imported by a worker, and keeps them from colliding with
the other examples on a shared Redis.

Run a real Redis first:  docker run --rm -p 6379:6379 redis
Then:                    uv run python examples/nested_scopes_cancel_on_failure.py
"""

import asyncio
import logging
import time

import redis.asyncio as redis

from TaskQueue import JobHandle, Queue
from TaskQueue.backends.redis_backend import RedisBackend

# Held by name as well as by the queue: 'Queue.backend' is typed as the Backend
# protocol, and flushing is not something that protocol exposes.
client = redis.Redis(host="localhost", port=6379)

q = Queue(backend=RedisBackend(client), namespace="checkout")

# A card check is quick; stock and shipping lookups are not. The gap is what
# makes run 2 legible: the failure lands while the other two are still working.
PAYMENT_SECONDS = 0.5
LOOKUP_SECONDS = 3.0
CARD_LIMIT = 2000.0

# One job each, spawned by check_inventory rather than by this file.
STOCK = {"New York City": 4, "Los Angeles": 11, "Chicago": 2}

# Three outer checks + three warehouse jobs, all live at the same instant.
CONCURRENCY = 6


@q.task
async def check_warehouse(item: str, warehouse: str) -> int:
    try:
        await asyncio.sleep(LOOKUP_SECONDS)
    except asyncio.CancelledError:
        # Printed from the job itself: the cancellation reached this far down.
        say(f"    warehouse {warehouse} stopped mid-lookup")
        raise
    return STOCK[warehouse]


@q.task
async def check_inventory(item: str) -> str:
    """A task that is itself a producer.

    Nothing here knows it is running on a worker rather than in a script:
    'async with q.group()' is the same construct either way. Cancel this job
    and its '__aexit__' unwinds, cancelling the warehouse jobs it spawned, so
    the guarantee crosses the job boundary as well as the process boundary.
    """
    async with q.group() as warehouses:
        counts = [await warehouses.spawn(check_warehouse, item, name) for name in STOCK]
    say(f"    inventory fanned out to {len(counts)} warehouses and gathered them")
    total = sum([await count.result() for count in counts])
    return f"{item}: {total} units across {len(counts)} warehouses"


@q.task
async def validate_payment(amount: float) -> str:
    await asyncio.sleep(PAYMENT_SECONDS)
    if amount > CARD_LIMIT:
        raise RuntimeError(f"card declined: ${amount:,.2f} is over the limit")
    return f"${amount:,.2f} authorised"


@q.task
async def calculate_shipping(address: str) -> str:
    await asyncio.sleep(LOOKUP_SECONDS)
    return f"shipping to {address}: $9.99"


@q.task
async def create_order(inventory: str, payment: str, shipping: str) -> str:
    await asyncio.sleep(0.2)
    return f"ORDER-1234 [{inventory} | {payment} | {shipping}]"


# Reset at the top of each run so the timestamps below are per-checkout. The
# tasks use it too, which works because the pool runs in this process.
_run_started = time.monotonic()


def say(message: str) -> None:
    print(f"  [{time.monotonic() - _run_started:4.1f}s] {message}", flush=True)


async def checkout(amount: float) -> None:
    """One checkout. Identical code both times; only the amount differs."""
    global _run_started
    _run_started = time.monotonic()
    handles: dict[str, JobHandle[str]] = {}

    try:
        # on_error defaults to "cancel_siblings": the first failure cancels the
        # rest instead of leaving them to run on regardless.
        async with q.group() as g:
            handles["inventory"] = await g.spawn(check_inventory, "Laptop")
            handles["payment"] = await g.spawn(validate_payment, amount)
            handles["shipping"] = await g.spawn(calculate_shipping, "Tel Aviv")
            say(f"spawned {len(handles)} checks; the scope now waits")

        # Past this line every child is terminal and succeeded; reaching here at
        # all is the guarantee. Only now is it safe to use their results.
        say("all checks passed")
        order = await q.root_group().spawn(
            create_order,
            await handles["inventory"].result(),
            await handles["payment"].result(),
            await handles["shipping"].result(),
        )
        say(f"order created: {await order.result()}")

    except* RuntimeError as failures:
        # Exactly one exception here: the scope reports what broke it, not the
        # cancellations it issued in response.
        for failure in failures.exceptions:
            say(f"checkout failed: {failure}")

        for name, handle in handles.items():
            say(f"  {name:<9} -> {(await handle.status()).value}")
        say("no order was created, and nothing was left running")


async def main() -> None:
    await client.flushdb()  # pyright: ignore[reportUnknownMemberType]
    try:
        async with q.worker(concurrency=CONCURRENCY):
            print(f"\nrun 1: ${1299.99:,.2f}, under the ${CARD_LIMIT:,.0f} limit")
            await checkout(1299.99)

            print(f"\nrun 2: ${2499.00:,.2f}, over the limit")
            await checkout(2499.00)
        print("\nworker pool stopped; done")
    finally:
        await client.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
