"""加练·watcher:消费死信池 q4,打印消息体和它的死亡档案(x-death)。

uv run python "rabbitmq教程/加练-消息流转基本功/watcher.py"
"""

import asyncio
from datetime import datetime

from common import RABBITMQ_URL, declare


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        topo = await declare(channel)

        print(f"{now()} watcher 已启动,守着死信池")
        async for message in topo["q4"].iterator():  # type: ignore[attr-defined]
            print(f"{now()} 死信到达: {message.body.decode('utf-8')}")
            for death in message.headers.get("x-death", []):
                reason = death.get("reason")
                reason = reason.decode() if isinstance(reason, bytes) else reason
                origin = death.get("queue")
                origin = origin.decode() if isinstance(origin, bytes) else origin
                print(f"    档案: 死于 {reason},原队列 {origin},count={death.get('count')}")
            await message.ack()


asyncio.run(main())
