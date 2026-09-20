"""结业项目·死信观察员:守着死信池,消息一到就告警(生产里 print 换成钉钉/飞书机器人)。

uv run python "rabbitmq教程/09-结业项目-售后退款中心/watcher.py"
"""

import asyncio
from datetime import datetime

import aio_pika
from common import RABBITMQ_URL, get_config


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    config = await get_config(channel)
    dead_queue: aio_pika.abc.AbstractQueue = config["dead_queue"]

    print(f"{now()} 死信观察员已启动")
    async for message in dead_queue.iterator():
        print(f"{now()} 死信到达,推送告警:")
        print(f"    消息体: {message.body.decode('utf-8')}")
        for death in message.headers.get("x-death", []):
            reason = death.get("reason")
            reason = reason.decode() if isinstance(reason, bytes) else reason
            origin = death.get("queue")
            origin = origin.decode() if isinstance(origin, bytes) else origin
            print(f"    档案: 死于 {reason},原队列 {origin}")
        await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
