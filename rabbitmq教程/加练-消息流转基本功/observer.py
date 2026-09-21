import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        q2: aio_pika.abc.AbstractQueue = config["q2"]

        async for message in q2.iterator():
            msg = json.loads(message.body.decode())
            print(f"任务信息已广播：{msg['id']} - {msg['msg']}")
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
