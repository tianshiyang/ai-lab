import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        q3: aio_pika.abc.AbstractQueue = config["q3"]

        async for message in q3.iterator():
            await asyncio.sleep(5)
            msg = json.loads(message.body.decode())
            retry = message.headers.get("retry")
            print(f"retry_5: {msg.get('id')} 的 第 {retry}次失败")


if __name__ == "__main__":
    asyncio.run(main())
