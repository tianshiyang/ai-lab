"""加练·observer:消费观察队列 q2,打印看到每条事件的路由键和内容。

自始至终只应看到 task.run 和 task.done 两类——重试和死信不走广播,它看不见。

uv run python "rabbitmq教程/加练-消息流转基本功/observer.py"
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
        await channel.set_qos(prefetch_count=1)
        topo = await declare(channel)

        print(f"{now()} observer 已启动")
        async for message in topo["q2"].iterator():  # type: ignore[attr-defined]
            print(f"{now()} [{message.routing_key}] {message.body.decode('utf-8')}")
            await message.ack()


asyncio.run(main())
