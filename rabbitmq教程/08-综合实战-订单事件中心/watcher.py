import asyncio
from datetime import datetime

import aio_pika
from common import RABBITMQ_URL, declare_topology


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        topo = await declare_topology(channel)
        dead: aio_pika.Queue = topo["dead"]
        print(f"{now()} 死信观察员已启动")
        async for message in dead.iterator():
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
