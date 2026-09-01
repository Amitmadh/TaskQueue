"""A call that must not type-check: a `str` where the task wants an `int`.

`@q.task` preserves the wrapped function's signature with `ParamSpec`, so
`spawn` is checked against `units_in_stock`'s parameters rather than accepting
whatever it is handed. `"twelve"` is rejected before the job is enqueued, let
alone run on a worker.

The `# pyright: ignore` below is not hiding the problem, it is asserting it.
This project sets `reportUnnecessaryTypeIgnoreComment = "error"`, so that
comment is only allowed to stay while the line really is an error: if the
ParamSpec guarantee ever regresses and this call starts type-checking, the
ignore becomes unnecessary and the typing pass fails. Delete the comment to see
the error itself; `docs/media/type_hints.png` is that view.

Not meant to be run. There is no worker here, so the scope would only reach its
deadline. `tests/test_typing.py` makes the same assertion for a missing
argument and an extra one, alongside the positive `assert_type` cases.
"""

import asyncio

import redis.asyncio as redis

from TaskQueue import Queue
from TaskQueue.backends.redis_backend import RedisBackend

client = redis.Redis(host="localhost", port=6379)

q = Queue(backend=RedisBackend(client), namespace="stockroom")


@q.task
async def units_in_stock(on_hand: int, incoming: int) -> int:
    return on_hand + incoming


async def main() -> None:
    async with q.root_group(deadline=0.3) as g:
        await g.spawn(
            units_in_stock,
            4,
            "twelve",  # pyright: ignore[reportArgumentType]
        )


if __name__ == "__main__":
    asyncio.run(main())
