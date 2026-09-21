import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        q4: aio_pika.abc.AbstractQueue = config["q4"]

        async for message in q4.iterator():
            await asyncio.sleep(15)
            msg = json.loads(message.body.decode())
            retry = message.headers.get("retry")
            print(f"retry_15: {msg.get('id')} 的 第 {retry}次失败")


if __name__ == "__main__":
    asyncio.run(main())
