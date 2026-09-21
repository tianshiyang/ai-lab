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
            msg = json.loads(message.body.decode())
            retry = message.headers.get("retry")
            print(f"{msg.get('id')} 的 第 {retry}次失败 -> 彻底失败")
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
