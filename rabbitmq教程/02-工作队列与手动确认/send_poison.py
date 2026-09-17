import asyncio
import json
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main():
    """主函数"""
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("ch2.tasks", durable=True)

        body = json.dumps({"name": "会出事的订单-666", "seconds": 1}).encode("utf-8")
        await channel.default_exchange.publish(aio_pika.Message(body), routing_key=queue.name)


if __name__ == "__main__":
    asyncio.run(main())
