import asyncio
import os
from pathlib import Path
from typing import cast

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = cast(str, os.getenv("RABBITMQ_URL"))


async def main():
    """主函数"""
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("ch1.hello", durable=True)

        received = 0
        async for message in queue.iterator():
            text = message.body.decode()
            received += 1
            print(f"收到第{received}条: {text}")
            await message.ack()
            if received == 6:
                break


if __name__ == "__main__":
    asyncio.run(main())
