"""End-to-end TaskQueue demo: one process, in-memory backend, real workers.

Six scope behaviours in a row, run over the jobs a small online store keeps
sending to a queue: product lookups against a catalogue service that answers
with a 500 for anything discontinued, and a stocktake slow enough to still be
running when something cancels it. The other examples are the same store under
Redis; this one needs no server at all.

Self-contained on purpose -- it runs its own worker pool. It cannot be split
across two terminals against a separate ``taskqueue worker``: MemoryBackend
keeps its queue in an ``asyncio.Queue``, so a second process gets its own empty
one and both sides block forever. Cross-process producer/worker needs Redis.

Six scenarios, each a few lines:

  1. single job round-trip           submit -> await result
  2. fan-out scope                   ``async with q.group()`` waits for all
  3. fail-fast                       one job fails, siblings are cancelled
  4. explicit cancellation           ``handle.cancel()`` -> ``JobCancelled``
  5. scope deadline                  timeout cancels every unfinished child
  6. on_error="collect"              run all, then report failures together

``namespace="storefront"`` fixes the task names to ``storefront.*`` whether this
file is run as a script or imported, so they stay stable once a real backend is
behind them, and keeps them from colliding with the other examples.

Run from the repo root:

    uv run python examples/scope_semantics_tour.py
"""

import asyncio

from TaskQueue import (
    JobCancelled,
    JobHandle,
    JobStatus,
    MemoryBackend,
    Queue,
)
from TaskQueue.logger import DEBUG, setup_logging

setup_logging(DEBUG)

q = Queue(backend=MemoryBackend(), namespace="storefront")


# --- task definitions -------------------------------------------------------


@q.task
async def units_in_stock(on_hand: int, incoming: int) -> int:
    await asyncio.sleep(0.05)
    return on_hand + incoming


@q.task
async def fetch_product(sku: str) -> str:
    """Read a product record; a discontinued SKU answers with an HTTP 500."""
    await asyncio.sleep(0.1)
    if "DISCONTINUED" in sku:
        raise ValueError(f"HTTP 500 from the catalogue for {sku}")
    return f"<catalogue record for {sku}>"


@q.task(name="slow.stocktake")
async def stocktake(seconds: float) -> str:
    await asyncio.sleep(seconds)
    return f"counted the shelves for {seconds}s"


# --- scenarios ---------------------------------------------------------------


async def single_job() -> None:
    print("\n[1] single job round-trip")
    handle = await q.root_group().spawn(units_in_stock, 4, 11)
    status = await handle.status()
    print(f"    status right after submit: {status}")
    print(f"    units_in_stock(4, 11) = {await handle.result()}")


async def fan_out() -> None:
    print("\n[2] fan-out: scope exit waits for every child")
    async with q.group() as g:
        handles = [await g.spawn(fetch_product, f"SKU-100{i}") for i in range(5)]
    # Past this line every child is terminal -- results are ready.
    for handle in handles:
        print(f"    {await handle.result()}")


async def fail_fast() -> None:
    print("\n[3] fail-fast: one job fails, siblings are cancelled")
    handles: list[JobHandle[str]] = []
    try:
        async with q.group() as g:  # on_error defaults to "cancel_siblings"
            handles.append(await g.spawn(fetch_product, "SKU-DISCONTINUED"))
            for _ in range(4):
                handles.append(await g.spawn(stocktake, 30.0))
    except* RuntimeError as eg:
        print(f"    scope raised: {eg.exceptions[0]}")
    for handle in handles:
        print(f"    job {handle.job_id[:8]} -> {await handle.status()}")


async def explicit_cancel() -> None:
    print("\n[4] explicit cancellation")
    handle = await q.root_group().spawn(stocktake, 30.0)
    await asyncio.sleep(0.1)  # let a worker claim it
    await handle.cancel()
    try:
        await handle.result()
    except JobCancelled as e:
        print(f"    {e}")


async def scope_deadline() -> None:
    print("\n[5] deadline belongs to the scope, not to the job")
    try:
        async with q.group(deadline=0.3) as g:
            await g.spawn(stocktake, 30.0)
    except TimeoutError:
        print("    deadline hit: unfinished children were cancelled")


async def collect_errors() -> None:
    print('\n[6] on_error="collect": run everything, then report failures')
    skus = [
        "SKU-1001",
        "SKU-DISCONTINUED-A",
        "SKU-1002",
        "SKU-DISCONTINUED-B",
    ]
    handles: list[JobHandle[str]] = []
    try:
        async with q.group(on_error="collect") as g:
            for sku in skus:
                handles.append(await g.spawn(fetch_product, sku))
    except* RuntimeError as eg:
        print(f"    {len(eg.exceptions)} job(s) failed, successes kept:")
    for handle in handles:
        if await handle.status() is JobStatus.COMPLETED:
            print(f"    {await handle.result()}")


async def main() -> None:
    async with q.worker(concurrency=4):
        await single_job()
        await fan_out()
        await fail_fast()
        await explicit_cancel()
        await scope_deadline()
        await collect_errors()
    print("\nworker pool stopped; done")


if __name__ == "__main__":
    asyncio.run(main())
