import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        dq1: aio_pika.abc.AbstractQueue = config["dq1"]

        async for message in dq1.iterator():
            for death in message.headers.get("x-death", []):
                reason = death.get("reason")
                reason = reason.decode() if isinstance(reason, bytes) else reason
                origin = death.get("queue")
                origin = origin.decode() if isinstance(origin, bytes) else origin
                print(f"    档案: 死于 {reason},原队列 {origin}")
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
