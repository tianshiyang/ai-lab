import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        notify_queue: aio_pika.abc.AbstractQueue = config["notify_queue"]
        async for message in notify_queue.iterator():
            msg = json.loads(message.body.decode())
            print(msg.get("message", "彻底死亡"))
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
