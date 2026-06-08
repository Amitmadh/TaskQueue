######################
# The API end to end #
######################
"""
import asyncio

from TaskQueue import Queue
from TaskQueue.backends.memory import MemoryBackend

q = Queue(backend=MemoryBackend())


@q.task
async def fetch() -> str:
    await asyncio.sleep(5)
    return "finished waiting!"


@q.task
async def long_running_task() -> str:
    for i in range(1000000):
        i = i
    return "finished waiting!"


######################################################
# Basic scope — wait for all, fail-fast on any error #
######################################################

async with q.group() as g:
    h1 = await g.spawn(fetch, "https://a.com")
    h2 = await g.spawn(fetch, "https://b.com")
    h3 = await g.spawn(fetch, "https://c.com")

# After the `async with` exits, all three are guaranteed complete.
# If any raised, the others were cancelled and the exception is re-raised here.

results = [await h.result() for h in (h1, h2, h3)]

###################################
# Nested scopes — natural fan-out #
###################################

async with q.group() as outer:

    async def crawl(url):
        html = await (await outer.spawn(fetch, url)).result()
        async with q.group() as inner:
            for link in extract_links(html):
                await inner.spawn(fetch, link)
        # inner scope guarantees all link-fetches done before crawl returns

    for seed in seeds:
        await outer.spawn_callable(crawl, seed)

###########################################################
# Explicit lifetime for fire-and-forget — but still owned #
###########################################################

# The "root" group lives as long as the queue process.
# Use it deliberately when you really do want fire-and-forget.
await q.root_group().spawn(send_welcome_email, user_id)

###########################################################
# Error collection mode for when you don't want fail-fast #
###########################################################

async with q.group(on_error="collect") as g:
    for url in urls:
        await g.spawn(fetch, url)

# Exits when all done regardless of failures.
# Raises ExceptionGroup if any failed; successes are still retrievable.
for h in g.handles:
    if h.succeeded:
        print(await h.result())

####################################
# Cancellation that actually works #
####################################

async with q.group() as g:
    handle = await g.spawn(long_running_task, data)
    await asyncio.sleep(5)
    await handle.cancel()  # delivered as CancelledError inside the job

#################################################
# Timeouts as scope-level concerns, not per-job #
#################################################

async with q.group(deadline=30) as g:  # 30s for the whole scope
    for url in urls:
        await g.spawn(fetch, url)
# At 30s, all unfinished jobs are cancelled.
"""
